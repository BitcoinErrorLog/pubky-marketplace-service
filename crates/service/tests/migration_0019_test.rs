//! Migration 0019 test: the `#[sqlx::test]` harness applies every migration
//! to a fresh database, so it cannot express "seed rows on the pre-0019
//! schema". This test drives the migration files directly against the
//! throwaway Postgres from `DATABASE_URL`: apply 0001..=0018, seed a
//! `shipped` order, apply 0019 twice (idempotent), and assert the additive
//! `delivery_assumed` column defaults to FALSE on pre-existing rows while
//! accepting the TRUE the `delivery_assume` worker transition writes. The
//! scratch database is dropped at the end.

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
async fn migration_0019_adds_delivery_assumed_without_touching_existing_rows() {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");

    let name = format!("mig0019_{}", Uuid::new_v4().simple());
    let pool = scratch_pool(&admin, &base, &name).await;

    // Pre-0019 schema.
    for number in 1..=18 {
        apply(&pool, number).await;
    }

    // Seed a shipped order on the pre-0019 schema (buyer-confirmed and
    // assumed deliveries do not exist yet, so nothing is flagged).
    let order_id = Uuid::new_v4();
    let seeded_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("seed timestamp")
        .to_utc();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, shipment, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 3, 'shipped', '[]', '{}', 100, 10, 110, 'EUR', 2, \
         1, $2, $4, $3, $3)",
    )
    .bind(order_id)
    .bind(Uuid::new_v4())
    .bind(seeded_at)
    .bind(serde_json::json!({
        "carrier": "Sandbox Post",
        "tracking_number": "TRACK-MIG",
        "state": "shipped",
        "shipped_at": "2026-01-01T00:00:00.000Z",
        "delivered_at": null,
    }))
    .execute(&pool)
    .await
    .expect("seed shipped order");

    // The migration applies twice (idempotent).
    apply(&pool, 19).await;
    apply(&pool, 19).await;

    // Pre-existing rows default to FALSE and remain valid.
    let row = sqlx::query("SELECT delivery_assumed FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert!(!row.get::<bool, _>("delivery_assumed"));

    // The column is NOT NULL and accepts the TRUE the worker writes.
    let (flagged,): (bool,) = sqlx::query_as(
        "UPDATE orders SET delivery_assumed = TRUE WHERE id = $1 RETURNING delivery_assumed",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("flag update");
    assert!(flagged);
    let not_null_violation = sqlx::query("UPDATE orders SET delivery_assumed = NULL WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await;
    assert!(
        not_null_violation.is_err(),
        "delivery_assumed must stay NOT NULL"
    );

    drop_scratch(&admin, &name).await;
}
