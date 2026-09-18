//! Real-catalog proof for the additive secret auction reserve migration.

mod common;

use axum::http::StatusCode;
use common::{execute, indexed_command_id, new_actor, test_app};
use marketplace_service::clock::Clock;
use marketplace_service::workers::{run_once, AuctionCloseBatchSummary};
use serde_json::{json, Value};
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use std::borrow::Cow;
use uuid::Uuid;

const CAPTURED_SELLER: &str = "n3pfudgxncn8i1e6icuq7umoczemjuyi6xdfrfczk3o8ej3e55my";
const CAPTURED_LISTING: &str = "7dd7e4279c2745df8b174656b9ee0670";
const MIGRATION_COMMAND_ID: &str = "00000000-0000-0033-0000-000000000001";
static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

async fn migrate_through_0031(pool: &PgPool) {
    let migrator = Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= 31)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    migrator.run(pool).await.expect("pre-0033 catalog migrates");
}

async fn apply_0033(pool: &PgPool) {
    ALL_MIGRATIONS
        .run(pool)
        .await
        .expect("real 0033 migration applies");
}

fn aggregate_id(seller: &str, listing_id: &str) -> String {
    format!("listing:{seller}_{listing_id}")
}

async fn seed_production_shaped_legacy_auction(
    pool: &PgPool,
    seller: &str,
    listing_id: &str,
    title: &str,
) -> String {
    let aggregate_id = aggregate_id(seller, listing_id);
    sqlx::query(
        "INSERT INTO listings (
            aggregate_id, seller_pubky, listing_id, title, listing_revision, content_hash,
            server_revision, state, total_quantity, available_quantity, reserved_quantity,
            sold_quantity, unit_price_amount_minor, unit_price_currency, unit_price_exponent,
            shipping_minor, sale_format, auction, fulfillment_methods, updated_at
         ) VALUES (
            $1, $2, $3, $4, 1, $5, 1, 'available', 1, 1, 0, 0,
            100, 'USD', 2, 100, 'auction',
            jsonb_build_object(
                'starts_at', '2026-09-13T14:34:39.384Z',
                'ends_at', '2026-09-20T14:34:39.384Z',
                'minimum_increment', jsonb_build_object(
                    'amount_minor', 100, 'currency', 'USD', 'exponent', 2
                ),
                'anti_sniping_window_seconds', 120,
                'anti_sniping_extension_seconds', 120,
                'status', 'active',
                'current_price', jsonb_build_object(
                    'amount_minor', 100, 'currency', 'USD', 'exponent', 2
                ),
                'leader_pubky', NULL,
                'bid_count', 0
            ),
            ARRAY['shipping'], '2026-09-13T14:34:39.384Z'
         )",
    )
    .bind(&aggregate_id)
    .bind(seller)
    .bind(listing_id)
    .bind(title)
    .bind("9b01f6662f5c7dee117c7bf1a224948e3c3886f206bafcf60c657e59d69dca0f")
    .execute(pool)
    .await
    .expect("Wave 1-shaped pre-0033 auction seeds");
    aggregate_id
}

fn bid_command(aggregate_id: &str, index: u64, expected_revision: i64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x9330, index),
        "aggregate_id": aggregate_id,
        "expected_revision": expected_revision,
        "issued_at": "2026-09-18T12:00:00.000Z",
        "kind": "auction.place_bid",
        "payload": {
            "maximum_amount": { "amount_minor": 500, "currency": "USD", "exponent": 2 }
        }
    })
}

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

#[sqlx::test(migrations = false)]
async fn legacy_auction_migrates_to_no_reserve_then_bids_and_closes_sold(pool: PgPool) {
    migrate_through_0031(&pool).await;
    let aggregate_id = seed_production_shaped_legacy_auction(
        &pool,
        CAPTURED_SELLER,
        CAPTURED_LISTING,
        "V26 capture test — do not bid",
    )
    .await;
    apply_0033(&pool).await;

    let authority: (i64, i64, Option<i64>, String, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT listing_revision, record_revision, reserve_amount_minor,
                    last_command_id::text, updated_at
             FROM listing_auction_reserves WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&pool)
    .await
    .expect("migration created explicit authority");
    assert_eq!(authority.0, 1);
    assert_eq!(authority.1, 1);
    assert_eq!(
        authority.2, None,
        "legacy public JSON never supplies reserve"
    );
    assert_eq!(authority.3, MIGRATION_COMMAND_ID);
    assert_eq!(
        authority.4.to_rfc3339(),
        "2026-09-13T14:34:39.384+00:00",
        "migration provenance preserves the listing timestamp"
    );

    let app = test_app(pool).await;
    app.clock.advance_seconds(2_572_480);
    let bidder = new_actor(&app).await;
    let (status, bid) = execute(&app, &bidder.token, &bid_command(&aggregate_id, 1, 1)).await;
    assert_eq!(status, StatusCode::OK, "{bid}");

    app.clock.advance_seconds(200_000);
    let closed = marketplace_service::workers::close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("worker closes migrated auction");
    assert_eq!(
        closed,
        AuctionCloseBatchSummary {
            closed: 1,
            failed: 0
        }
    );
    let (status,): (String,) =
        sqlx::query_as("SELECT auction->>'status' FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("migrated auction remains");
    assert_eq!(status, "sold");
}

#[sqlx::test(migrations = false)]
async fn failing_due_auction_is_isolated_and_reported_while_valid_peer_closes(pool: PgPool) {
    common::install_log_capture();
    migrate_through_0031(&pool).await;
    let failing = seed_production_shaped_legacy_auction(
        &pool,
        &"a".repeat(52),
        "legacy_fail",
        "Production-shaped legacy failure fixture",
    )
    .await;
    let valid = seed_production_shaped_legacy_auction(
        &pool,
        &"b".repeat(52),
        "legacy_valid",
        "Production-shaped legacy valid fixture",
    )
    .await;
    apply_0033(&pool).await;

    let app = test_app(pool).await;
    app.clock.advance_seconds(2_572_480);
    let bidder = new_actor(&app).await;
    let (status, body) = execute(&app, &bidder.token, &bid_command(&valid, 2, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    sqlx::query("DELETE FROM listing_auction_reserves WHERE listing_aggregate_id = $1")
        .bind(&failing)
        .execute(&app.pool)
        .await
        .expect("test creates one fail-closed legacy authority defect");

    app.clock.advance_seconds(200_000);
    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass continues after isolated auction failure");
    assert_eq!(summary.auctions_closed, 1);
    assert_eq!(summary.auction_close_failures, 1);

    let states: Vec<(String, String)> = sqlx::query_as(
        "SELECT aggregate_id, auction->>'status' FROM listings
         WHERE aggregate_id = ANY($1) ORDER BY aggregate_id",
    )
    .bind(vec![failing.clone(), valid.clone()])
    .fetch_all(&app.pool)
    .await
    .expect("both fixture states read");
    assert_eq!(
        states,
        vec![(failing.clone(), "active".into()), (valid, "sold".into())]
    );
    let logs = common::captured_logs();
    assert!(
        logs.contains("auction close failed; continuing batch"),
        "{logs}"
    );
    assert!(logs.contains("auction_close_failed"), "{logs}");
    assert!(logs.contains(&failing), "{logs}");
    for forbidden in ["reserve_price", "reservePrice", "reserve_met", "reserveMet"] {
        assert!(!logs.contains(forbidden), "{logs}");
    }
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
