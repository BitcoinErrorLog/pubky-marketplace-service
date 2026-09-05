//! Migration 0018 remediation test: the existing `#[sqlx::test]` harness
//! applies every migration to a fresh database, so it cannot express
//! "seed rows on the pre-0018 schema". This test drives the migration files
//! directly against the throwaway Postgres from `DATABASE_URL`:
//! apply 0001..=0017, seed a `disputed` order plus one annotation of each
//! outcome, apply 0018, and assert the remediation and the narrowed
//! constraints. The scratch database is dropped at the end.

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
async fn migration_0018_remediates_disputed_orders_and_operator_annotations() {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");

    let name = format!("mig0018_{}", Uuid::new_v4().simple());
    let pool = scratch_pool(&admin, &base, &name).await;

    // Pre-0018 schema.
    for number in 1..=17 {
        apply(&pool, number).await;
    }

    // Seed on the pre-0018 schema: one disputed order, plus one annotation
    // per outcome (three operator outcomes that must go, one refund that
    // must stay).
    let order_id = Uuid::new_v4();
    let seeded_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("seed timestamp")
        .to_utc();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, tax_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 1, 'disputed', '[]', '{}', 100, 10, 0, 110, 'EUR', 2, \
         1, $2, $3, $3)",
    )
    .bind(order_id)
    .bind(Uuid::new_v4())
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("seed disputed order");

    for outcome in [
        "refunded",
        "dispute_resolved_for_buyer",
        "dispute_resolved_for_seller",
        "attestation_disavowed",
    ] {
        sqlx::query(
            "INSERT INTO attestation_annotations (id, order_ref, outcome, reason, annotated_at) \
             VALUES ($1, 'order-ref', $2, NULL, $3)",
        )
        .bind(Uuid::new_v4())
        .bind(outcome)
        .bind(seeded_at)
        .execute(&pool)
        .await
        .expect("seed annotation");
    }

    // The migration under test.
    apply(&pool, 18).await;

    let order = sqlx::query("SELECT state, revision, updated_at FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("fetch remediated order");
    assert_eq!(order.get::<String, _>("state"), "completed");
    assert_eq!(order.get::<i64, _>("revision"), 2);
    let updated_at: chrono::DateTime<chrono::Utc> = order.get("updated_at");
    assert!(
        updated_at > seeded_at,
        "updated_at must move with the state change"
    );

    let remaining: Vec<String> = sqlx::query_scalar("SELECT outcome FROM attestation_annotations")
        .fetch_all(&pool)
        .await
        .expect("fetch remaining annotations");
    assert_eq!(remaining, ["refunded"]);

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint WHERE conname IN \
         ('orders_state_check', 'attestation_annotations_outcome_check') ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("fetch check constraints");
    assert_eq!(
        constraints,
        [
            "attestation_annotations_outcome_check".to_string(),
            "orders_state_check".to_string()
        ]
    );

    // The narrowed constraints actually reject the removed vocabulary.
    let rejected = sqlx::query("UPDATE orders SET state = 'disputed' WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await;
    assert!(
        rejected.is_err(),
        "disputed must violate orders_state_check"
    );

    // The append-only trigger was lifted for the DELETE and re-created.
    let trigger_blocked = sqlx::query("DELETE FROM attestation_annotations")
        .execute(&pool)
        .await;
    assert!(
        trigger_blocked.is_err(),
        "annotations must be append-only again"
    );

    // Idempotent: a second application is a no-op.
    apply(&pool, 18).await;
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("fetch order after re-apply");
    assert_eq!(state, "completed");

    drop(pool);
    drop_scratch(&admin, &name).await;
}
