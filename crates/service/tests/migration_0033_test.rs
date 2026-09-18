//! Real-catalog proof for the additive secret auction reserve migration.

mod common;

use sqlx::PgPool;
use uuid::Uuid;

async fn seed_listing(pool: &PgPool, aggregate_id: &str) {
    sqlx::query(
        "INSERT INTO listings (
            aggregate_id, seller_pubky, listing_id, title, listing_revision, content_hash,
            server_revision, state, total_quantity, available_quantity, reserved_quantity,
            sold_quantity, unit_price_amount_minor, unit_price_currency, unit_price_exponent,
            shipping_minor, sale_format, auction, fulfillment_methods, updated_at
         ) VALUES (
            $1, $2, $3, 'Catalog proof', 1, repeat('a', 64),
            1, 'available', 1, 1, 0, 0, 100, 'USD', 2, 0, 'auction',
            jsonb_build_object(
                'starts_at', '2026-08-19T22:00:00.000Z',
                'ends_at', '2026-08-19T23:00:00.000Z',
                'minimum_increment', jsonb_build_object(
                    'amount_minor', 10, 'currency', 'USD', 'exponent', 2
                ),
                'anti_sniping_window_seconds', 0,
                'anti_sniping_extension_seconds', 0,
                'status', 'active',
                'current_price', jsonb_build_object(
                    'amount_minor', 100, 'currency', 'USD', 'exponent', 2
                ),
                'leader_pubky', NULL,
                'bid_count', 0
            ),
            ARRAY['shipping'], now()
         )",
    )
    .bind(aggregate_id)
    .bind("y".repeat(52))
    .bind(aggregate_id.replace(':', "_"))
    .execute(pool)
    .await
    .expect("auction listing seeds");
}

#[sqlx::test(migrations = "./migrations")]
async fn migration_0033_has_exact_secret_reserve_catalog_and_constraints(pool: PgPool) {
    let columns: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'listing_auction_reserves'
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("reserve catalog reads");
    assert_eq!(
        columns,
        vec![
            ("listing_aggregate_id".into(), "text".into(), "NO".into()),
            ("listing_revision".into(), "bigint".into(), "NO".into()),
            ("record_revision".into(), "bigint".into(), "NO".into()),
            ("reserve_amount_minor".into(), "bigint".into(), "YES".into()),
            ("reserve_currency".into(), "text".into(), "YES".into()),
            ("reserve_exponent".into(), "integer".into(), "YES".into()),
            ("last_command_id".into(), "uuid".into(), "NO".into()),
            (
                "updated_at".into(),
                "timestamp with time zone".into(),
                "NO".into()
            ),
        ]
    );

    let constraints: Vec<(String, String)> = sqlx::query_as(
        "SELECT conname, contype::text
         FROM pg_constraint
         WHERE conrelid = 'listing_auction_reserves'::regclass
         ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("constraint catalog reads");
    for expected in [
        "listing_auction_reserves_amount_bounds",
        "listing_auction_reserves_currency_format",
        "listing_auction_reserves_exponent_bounds",
        "listing_auction_reserves_listing_aggregate_id_fkey",
        "listing_auction_reserves_money_presence",
        "listing_auction_reserves_pkey",
        "listing_auction_reserves_record_revision_check",
        "listing_auction_reserves_listing_revision_check",
    ] {
        assert!(
            constraints.iter().any(|(name, _)| name == expected),
            "missing catalog constraint {expected}: {constraints:?}"
        );
    }

    let aggregate_id = "listing:catalog-proof";
    seed_listing(&pool, aggregate_id).await;
    sqlx::query(
        "INSERT INTO listing_auction_reserves
         (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor,
          reserve_currency, reserve_exponent, last_command_id, updated_at)
         VALUES ($1, 1, 1, NULL, NULL, NULL, $2, now())",
    )
    .bind(aggregate_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("null reserve row is valid");

    let valued_aggregate_id = "listing:valid-money";
    seed_listing(&pool, valued_aggregate_id).await;
    sqlx::query(
        "INSERT INTO listing_auction_reserves
         (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor,
          reserve_currency, reserve_exponent, last_command_id, updated_at)
         VALUES ($1, 1, 1, 100, 'USD', 2, $2, now())",
    )
    .bind(valued_aggregate_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("complete bounded reserve Money is valid");

    for (aggregate_id, amount, currency, exponent) in [
        ("listing:partial", Some(100_i64), None, None),
        ("listing:amount-low", Some(0), Some("USD"), Some(2)),
        (
            "listing:amount-high",
            Some(9_007_199_254_740_992),
            Some("USD"),
            Some(2),
        ),
        ("listing:currency", Some(100), Some("usd"), Some(2)),
        ("listing:exponent", Some(100), Some("USD"), Some(19)),
    ] {
        seed_listing(&pool, aggregate_id).await;
        sqlx::query(
            "INSERT INTO listing_auction_reserves
             (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor,
              reserve_currency, reserve_exponent, last_command_id, updated_at)
             VALUES ($1, 1, 1, $2, $3, $4, $5, now())",
        )
        .bind(aggregate_id)
        .bind(amount)
        .bind(currency)
        .bind(exponent)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect_err("invalid reserve tuple must violate a database check");
    }

    sqlx::query(
        "INSERT INTO listing_auction_reserves
         (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor,
          reserve_currency, reserve_exponent, last_command_id, updated_at)
         VALUES ('listing:missing', 1, 1, NULL, NULL, NULL, $1, now())",
    )
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect_err("reserve row requires a listing foreign key");

    sqlx::query(
        "INSERT INTO listing_auction_reserves
         (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor,
          reserve_currency, reserve_exponent, last_command_id, updated_at)
         VALUES ($1, 1, 2, NULL, NULL, NULL, $2, now())",
    )
    .bind(aggregate_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect_err("one reserve row per listing is enforced by the primary key");
}
