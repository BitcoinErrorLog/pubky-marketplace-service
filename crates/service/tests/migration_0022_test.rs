//! Migration 0022 test: the `#[sqlx::test]` harness applies every migration
//! to a fresh database, so it cannot express "seed rows on the pre-0022
//! schema". This test drives the migration files directly against the
//! throwaway Postgres from `DATABASE_URL`: apply 0001..=0021, apply 0022
//! twice (idempotent), and assert the two-phase paykit columns, the widened
//! `paykit_request_state` check (`preparing` admitted, unknown values
//! rejected), the `paykit_activation_state` vocabulary, and that the
//! `orders_paykit_pending` partial index predicate is UNCHANGED (a
//! `preparing` order stays unclaimed by the poll). The scratch database is
//! dropped at the end.

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::str::FromStr;
use uuid::Uuid;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

fn migration_path(number: u32) -> String {
    let prefix = format!("{number:04}_");
    let mut matches: Vec<String> = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            (name.starts_with(&prefix) && name.ends_with(".sql"))
                .then(|| format!("{MIGRATIONS_DIR}/{name}"))
        })
        .collect();
    matches.sort();
    matches
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no migration {prefix}*.sql"))
}

fn migration_sql(number: u32) -> String {
    let path = migration_path(number);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

async fn apply(pool: &PgPool, number: u32) {
    let sql = migration_sql(number);
    sqlx::raw_sql(&sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("apply migration {number:04}: {e}"));
}

async fn scratch_pool(admin: &PgPool, base: &PgConnectOptions, name: &str) -> PgPool {
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(admin)
        .await
        .expect("create scratch database");
    let options = base.clone().database(name);
    PgPoolOptions::new()
        .connect_with(options)
        .await
        .expect("connect to scratch database")
}

async fn drop_scratch(admin: &PgPool, name: &str) {
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(admin)
        .await
        .expect("drop scratch database");
}

#[tokio::test]
async fn migration_0022_adds_the_two_phase_paykit_schema() {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");

    let name = format!("mig0022_{}", Uuid::new_v4().simple());
    let pool = scratch_pool(&admin, &base, &name).await;

    // Pre-0022 schema.
    for number in 1..=21 {
        apply(&pool, number).await;
    }

    // The migration applies twice (idempotent).
    apply(&pool, 22).await;
    apply(&pool, 22).await;

    // Seed an order and exercise the new columns and checks.
    let order_id = Uuid::new_v4();
    let seeded_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("seed timestamp")
        .to_utc();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 1, 'pending_payment', '[]', NULL, 50000, 1200, 51200, \
         'SAT', 0, 1, $2, $3, $3)",
    )
    .bind(order_id)
    .bind(Uuid::new_v4())
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("seed order");

    // `preparing` is admitted by the widened request-state check, alongside
    // the existing vocabulary.
    for state in ["preparing", "pending", "detected", "confirmed"] {
        sqlx::query("UPDATE orders SET paykit_request_state = $2 WHERE id = $1")
            .bind(order_id)
            .bind(state)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("paykit_request_state '{state}' must be admitted: {e}"));
    }
    // An unknown value is rejected.
    let rejected =
        sqlx::query("UPDATE orders SET paykit_request_state = 'observing' WHERE id = $1")
            .bind(order_id)
            .execute(&pool)
            .await;
    assert!(
        rejected.is_err(),
        "an unknown paykit_request_state must be rejected"
    );

    // The activation-state vocabulary, likewise.
    for state in ["preparing", "active", "voided"] {
        sqlx::query("UPDATE orders SET paykit_activation_state = $2 WHERE id = $1")
            .bind(order_id)
            .bind(state)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("paykit_activation_state '{state}' must be admitted: {e}"));
    }
    let rejected =
        sqlx::query("UPDATE orders SET paykit_activation_state = 'primed' WHERE id = $1")
            .bind(order_id)
            .execute(&pool)
            .await;
    assert!(
        rejected.is_err(),
        "an unknown paykit_activation_state must be rejected"
    );

    // The persisted pin: every phase-1 field round-trips, and the attempt
    // counter defaults to 0.
    let invoice_id = Uuid::new_v4();
    sqlx::query(
        "UPDATE orders SET paykit_invoice_id = $2, paykit_stack_id = 'production:abc', \
         paykit_stack_endpoint = 'http://paykit.internal', paykit_total_sats = 51637, \
         paykit_expires_at = $3, paykit_prepare_expires_at = $3, \
         paykit_allocation_mode = 'shared_manual', \
         paykit_address_fingerprint = '3f7a1c9e5b204d86' WHERE id = $1",
    )
    .bind(order_id)
    .bind(invoice_id)
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("the phase-1 pin persists");
    let row = sqlx::query(
        "SELECT paykit_invoice_id, paykit_stack_id, paykit_total_sats, paykit_bind_attempt \
         FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("order row exists");
    assert_eq!(row.get::<Uuid, _>("paykit_invoice_id"), invoice_id);
    assert_eq!(row.get::<String, _>("paykit_stack_id"), "production:abc");
    assert_eq!(row.get::<i64, _>("paykit_total_sats"), 51637);
    assert_eq!(row.get::<i32, _>("paykit_bind_attempt"), 0);

    // The partial index predicate is UNCHANGED: `preparing` is not part of
    // the poll's claim set.
    let indexdef: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'orders_paykit_pending'",
    )
    .fetch_one(&pool)
    .await
    .expect("partial index exists");
    assert!(
        indexdef.contains("'pending'") && indexdef.contains("'detected'"),
        "index predicate keeps its shape: {indexdef}"
    );
    assert!(
        !indexdef.contains("preparing"),
        "a preparing order must stay unclaimed: {indexdef}"
    );

    drop_scratch(&admin, &name).await;
}
