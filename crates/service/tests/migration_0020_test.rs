//! Migration 0020 test: the `#[sqlx::test]` harness applies every migration
//! to a fresh database, so it cannot express "seed rows on the pre-0020
//! schema". This test drives the migration files directly against the
//! throwaway Postgres from `DATABASE_URL`: apply 0001..=0019, seed a listing
//! and a shipped order on the pre-0020 schema, apply 0020 twice
//! (idempotent), and assert the backfill (`fulfillment = 'shipping'` on
//! pre-existing rows, `fulfillment_methods = '{shipping}'` on pre-existing
//! listings), the widened state vocabulary, and the new pickup tables. The
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
async fn migration_0020_backfills_shipping_and_adds_the_pickup_schema() {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");

    let name = format!("mig0020_{}", Uuid::new_v4().simple());
    let pool = scratch_pool(&admin, &base, &name).await;

    // Pre-0020 schema.
    for number in 1..=19 {
        apply(&pool, number).await;
    }

    // Seed a listing and a shipped order on the pre-0020 schema.
    let seller = "s".repeat(52);
    let seeded_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("seed timestamp")
        .to_utc();
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
         listing_revision, content_hash, server_revision, state, total_quantity, \
         available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
         unit_price_currency, unit_price_exponent, shipping_minor, sale_format, updated_at) \
         VALUES ($1, $2, 'boots_01', 'Boots', 1, $3, 1, 'available', 2, 2, 0, 0, 12500, \
         'USD', 2, 1200, 'fixed_price', $4)",
    )
    .bind(format!("listing:{seller}_boots_01"))
    .bind(&seller)
    .bind("a".repeat(64))
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("seed listing");
    let order_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', $2, 2, 'paid', '[]', NULL, 12500, 1200, 13700, 'USD', 2, \
         1, $3, $4, $4)",
    )
    .bind(order_id)
    .bind(&seller)
    .bind(Uuid::new_v4())
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("seed paid order");

    // The migration applies twice (idempotent).
    apply(&pool, 20).await;
    apply(&pool, 20).await;

    // Pre-existing rows backfill to shipping.
    let order = sqlx::query("SELECT fulfillment, first_revealed_at FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert_eq!(order.get::<String, _>("fulfillment"), "shipping");
    assert!(order
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("first_revealed_at")
        .is_none());
    let listing = sqlx::query("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
        .bind(format!("listing:{seller}_boots_01"))
        .fetch_one(&pool)
        .await
        .expect("listing row exists");
    assert_eq!(
        listing.get::<Vec<String>, _>("fulfillment_methods"),
        vec!["shipping".to_string()]
    );

    // The new columns are NOT NULL / checked as the handlers require.
    let bad_fulfillment = sqlx::query("UPDATE orders SET fulfillment = 'drone' WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await;
    assert!(
        bad_fulfillment.is_err(),
        "fulfillment vocabulary is checked"
    );
    let null_fulfillment = sqlx::query("UPDATE orders SET fulfillment = NULL WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await;
    assert!(null_fulfillment.is_err(), "fulfillment stays NOT NULL");
    let bad_methods = sqlx::query(
        "UPDATE listings SET fulfillment_methods = '{shipping,drone}' WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{seller}_boots_01"))
    .execute(&pool)
    .await;
    assert!(
        bad_methods.is_err(),
        "fulfillment methods vocabulary is checked"
    );

    // The widened order state vocabulary accepts ready_for_pickup.
    sqlx::query("UPDATE orders SET state = 'ready_for_pickup' WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("ready_for_pickup is a declared order state");

    // The pickup tables exist with their keys: one handover per order, the
    // surviving per-listing version counter, and both sealed families.
    sqlx::query(
        "INSERT INTO listing_pickup_version_counters (aggregate_id, seller_pubky, last_version, \
         updated_at) VALUES ($1, $2, 7, $3)",
    )
    .bind(format!("listing:{seller}_boots_01"))
    .bind(&seller)
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("counter row inserts");
    sqlx::query(
        "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
         details_ciphertext, created_at, updated_at) \
         VALUES ($1, $2, 7, '\\x0102'::bytea, $3, $3)",
    )
    .bind(format!("listing:{seller}_boots_01"))
    .bind(&seller)
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("details row inserts");
    sqlx::query(
        "INSERT INTO pickup_handovers (order_id, confirmed_by, confirmed_at) \
         VALUES ($1, 'buyer', $2)",
    )
    .bind(order_id)
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("handover row inserts");
    let duplicate_handover = sqlx::query(
        "INSERT INTO pickup_handovers (order_id, confirmed_by, confirmed_at) \
         VALUES ($1, 'seller', $2)",
    )
    .bind(order_id)
    .bind(seeded_at)
    .execute(&pool)
    .await;
    assert!(
        duplicate_handover.is_err(),
        "one handover per order (PRIMARY KEY on order_id)"
    );
    sqlx::query(
        "INSERT INTO pickup_line_snapshots (order_id, line_index, listing_aggregate_id, \
         version, snapshot_ciphertext, confirming_adapter, created_at) \
         VALUES ($1, 0, $2, 7, '\\x0304'::bytea, 'locks', $3)",
    )
    .bind(order_id)
    .bind(format!("listing:{seller}_boots_01"))
    .bind(seeded_at)
    .execute(&pool)
    .await
    .expect("snapshot row inserts");

    drop_scratch(&admin, &name).await;
}
