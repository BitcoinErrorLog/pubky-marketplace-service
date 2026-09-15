use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

fn migration_sql(number: u32) -> String {
    let prefix = format!("{number:04}_");
    let path = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations directory")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            (name.starts_with(&prefix) && name.ends_with(".sql")).then_some(path)
        })
        .next()
        .unwrap_or_else(|| panic!("missing migration {number:04}"));
    std::fs::read_to_string(path).expect("migration readable")
}

async fn apply(pool: &PgPool, number: u32) {
    sqlx::raw_sql(&migration_sql(number))
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("apply migration {number:04}: {error}"));
}

async fn scratch() -> (PgPool, PgPool, String) {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("DATABASE_URL parses");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect admin");
    let name = format!("mig0025_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create scratch database");
    let pool = PgPoolOptions::new()
        .connect_with(base.database(&name))
        .await
        .expect("connect scratch database");
    (admin, pool, name)
}

fn constraint_name(error: &sqlx::Error) -> Option<&str> {
    match error {
        sqlx::Error::Database(database) => database.constraint(),
        _ => None,
    }
}

#[tokio::test]
async fn migration_0025_preserves_legacy_release_and_enforces_non_null_money() {
    let (admin, pool, name) = scratch().await;
    apply(&pool, 1).await;
    let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("timestamp")
        .to_utc();
    let listing = "listing:seller_boots_01";
    let offer_id = Uuid::new_v4();
    let reservation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, sale_format, auction, updated_at) \
         VALUES ($1, 'seller', 'boots_01', 'Boots', 1, $2, 1, 'reserved', 1, 0, 1, 0, \
                 1000, 'USD', 2, 'fixed_price', NULL, $3)",
    )
    .bind(listing)
    .bind("a".repeat(64))
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy listing");
    sqlx::query(
        "INSERT INTO reservations (id, listing_aggregate_id, buyer_pubky, quantity, status, \
         expires_at, created_at, updated_at) VALUES ($1, $2, 'buyer', 1, 'active', $3, $4, $4)",
    )
    .bind(reservation_id)
    .bind(listing)
    .bind(now + chrono::Duration::minutes(30))
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy reservation");
    sqlx::query(
        "INSERT INTO offers (id, aggregate_id, listing_aggregate_id, buyer_pubky, seller_pubky, \
         revision, state, offered_by, amount_minor, currency, exponent, quantity, message, \
         history, expires_at, created_at, updated_at) \
         VALUES ($1, $2, $3, 'buyer', 'seller', 2, 'accepted', 'seller', 800, 'USD', 2, 1, \
                 '', '[]', $4, $5, $5)",
    )
    .bind(offer_id)
    .bind(format!("offer:{offer_id}"))
    .bind(listing)
    .bind(now + chrono::Duration::hours(1))
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy accepted offer");
    for number in 2..=24 {
        apply(&pool, number).await;
    }
    apply(&pool, 25).await;

    let migrated: (String, Option<String>) =
        sqlx::query_as("SELECT state, expiry_reason FROM offers WHERE id = $1")
            .bind(offer_id)
            .fetch_one(&pool)
            .await
            .expect("migrated offer");
    assert_eq!(
        migrated,
        (
            "expired".to_string(),
            Some("legacy_unconvertible".to_string())
        )
    );
    let award_link: Option<Uuid> =
        sqlx::query_scalar("SELECT offer_award_id FROM reservations WHERE id = $1")
            .bind(reservation_id)
            .fetch_one(&pool)
            .await
            .expect("legacy reservation");
    assert_eq!(award_link, None);
    let released = marketplace_service::expiry::expire_due_reservations(
        &pool,
        now + chrono::Duration::hours(2),
    )
    .await
    .expect("legacy reservation release");
    assert_eq!(released, 1);
    let quantities: (i64, i64) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing)
    .fetch_one(&pool)
    .await
    .expect("listing quantities");
    assert_eq!(quantities, (1, 0));

    let invalid_offer = sqlx::query(
        "UPDATE offers SET state = 'accepted', award_id = $2, accepted_at = $3, \
         award_expires_at = $3, reservation_id = $4, accepted_unit_price_minor = 800, \
         accepted_currency = 'USD', accepted_exponent = 2, accepted_quantity = NULL, \
         accepted_listing_aggregate_id = $5, accepted_listing_title = 'Boots', \
         accepted_listing_revision = 1, accepted_listing_record_sha256 = $6, \
         accepted_variant_id = 'boots_01', accepted_variant_options = '[]', \
         accepted_shipping_minor = 0, accepted_subtotal_minor = 800, \
         accepted_total_minor = 800, accepted_fulfillment = 'shipping' WHERE id = $1",
    )
    .bind(offer_id)
    .bind(Uuid::new_v4())
    .bind(now)
    .bind(Uuid::new_v4())
    .bind(listing)
    .bind("b".repeat(64))
    .execute(&pool)
    .await
    .expect_err("NULL accepted quantity must fail");
    assert_eq!(
        constraint_name(&invalid_offer),
        Some("offers_award_snapshot_check")
    );

    let invalid_receipt = sqlx::query(
        "INSERT INTO receipts (id, order_id, payment_id, issuer_pubky, recipient_pubky, \
         total_minor, currency, exponent, merchandise_total_minor, merchandise_currency, \
         merchandise_exponent, content_hash, issued_at) \
         VALUES ($1, $2, $3, 'seller', 'buyer', 1, 'USD', 2, NULL, 'USD', NULL, 'hash', $4)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(now)
    .execute(&pool)
    .await
    .expect_err("partial merchandise money must fail");
    assert_eq!(
        constraint_name(&invalid_receipt),
        Some("receipts_merchandise_money_check")
    );

    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop scratch database");
}
