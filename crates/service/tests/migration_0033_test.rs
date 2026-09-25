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

#[derive(sqlx::FromRow)]
struct LegacyAuthority {
    listing_revision: i64,
    record_revision: i64,
    reserve_amount_minor: Option<i64>,
    reserve_currency: Option<String>,
    reserve_exponent: Option<i32>,
    last_command_id: String,
    updated_at: chrono::DateTime<chrono::Utc>,
}

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
    common::restore_0032_retention_connlimit(pool).await;
    ALL_MIGRATIONS
        .run(pool)
        .await
        .expect("real 0033 migration applies");
}

#[sqlx::test(migrations = false)]
async fn migration_0032_applies_after_already_applied_0033(pool: PgPool) {
    let through_0033_without_0032 = Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= 33 && migration.version != 32)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    through_0033_without_0032
        .run(&pool)
        .await
        .expect("0001..0031 and 0033 migrate without 0032");

    let initially_applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("initial migration versions");
    let expected_without_0032: Vec<i64> = (1..=31).chain([33]).collect();
    assert_eq!(initially_applied, expected_without_0032);

    common::restore_0032_retention_connlimit(&pool).await;
    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("missing 0032 and pending 0034..=0045 apply after already-applied 0033");

    let finally_applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("final migration versions");
    assert_eq!(finally_applied, (1..=45).collect::<Vec<_>>());
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
                'bid_count', 0,
                'reserve_price', jsonb_build_object(
                    'amount_minor', 900, 'currency', 'USD', 'exponent', 2
                ),
                'reservePrice', jsonb_build_object(
                    'amount_minor', 901, 'currency', 'USD', 'exponent', 2
                ),
                'reserve_met', true,
                'reserveMet', false
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

async fn overwrite_auction(pool: &PgPool, aggregate_id: &str, auction: Value) {
    sqlx::query("UPDATE listings SET auction = $2 WHERE aggregate_id = $1")
        .bind(aggregate_id)
        .bind(auction)
        .execute(pool)
        .await
        .expect("test auction document updates");
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

    let authority: LegacyAuthority = sqlx::query_as(
        "SELECT listing_revision, record_revision, reserve_amount_minor,
                    reserve_currency, reserve_exponent,
                    last_command_id::text AS last_command_id, updated_at
             FROM listing_auction_reserves WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&pool)
    .await
    .expect("migration created explicit authority");
    assert_eq!(authority.listing_revision, 1);
    assert_eq!(authority.record_revision, 1);
    assert_eq!(
        authority.reserve_amount_minor, None,
        "legacy public JSON never supplies reserve"
    );
    assert_eq!(authority.reserve_currency, None);
    assert_eq!(authority.reserve_exponent, None);
    assert_eq!(authority.last_command_id, MIGRATION_COMMAND_ID);
    assert_eq!(
        authority.updated_at.to_rfc3339(),
        "2026-09-13T14:34:39.384+00:00",
        "migration provenance preserves the listing timestamp"
    );
    let migrated_auction: Value =
        sqlx::query_scalar("SELECT auction FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&pool)
            .await
            .expect("migrated public auction reads");
    for legacy_key in ["reserve_price", "reservePrice", "reserve_met", "reserveMet"] {
        assert!(
            migrated_auction.get(legacy_key).is_none(),
            "0033 must scrub the top-level legacy key {legacy_key}"
        );
    }

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

#[sqlx::test(migrations = false)]
async fn malformed_auction_documents_are_isolated_from_a_due_peer(pool: PgPool) {
    common::install_log_capture();
    migrate_through_0031(&pool).await;
    let malformed_document = seed_production_shaped_legacy_auction(
        &pool,
        &"c".repeat(52),
        "malformed_document",
        "Malformed auction document fixture",
    )
    .await;
    let malformed_end = seed_production_shaped_legacy_auction(
        &pool,
        &"d".repeat(52),
        "malformed_end",
        "Malformed end timestamp fixture",
    )
    .await;
    let missing_end = seed_production_shaped_legacy_auction(
        &pool,
        &"e".repeat(52),
        "missing_end",
        "Missing end timestamp fixture",
    )
    .await;
    let valid = seed_production_shaped_legacy_auction(
        &pool,
        &"f".repeat(52),
        "valid_due",
        "Valid due peer fixture",
    )
    .await;
    apply_0033(&pool).await;

    overwrite_auction(
        &pool,
        &malformed_document,
        json!({
            "status": "active",
            "ends_at": "2026-09-20T14:34:39.384Z",
            "reservePrice": {"amount_minor": 777, "private_marker": "do-not-log"}
        }),
    )
    .await;
    let malformed_end_value: Value =
        sqlx::query_scalar("SELECT auction FROM listings WHERE aggregate_id = $1")
            .bind(&malformed_end)
            .fetch_one(&pool)
            .await
            .expect("malformed-end source reads");
    let mut malformed_end_value = malformed_end_value;
    malformed_end_value["ends_at"] = json!("not-a-timestamp-do-not-log");
    overwrite_auction(&pool, &malformed_end, malformed_end_value).await;
    let missing_end_value: Value =
        sqlx::query_scalar("SELECT auction - 'ends_at' FROM listings WHERE aggregate_id = $1")
            .bind(&missing_end)
            .fetch_one(&pool)
            .await
            .expect("missing-end source reads");
    overwrite_auction(&pool, &missing_end, missing_end_value).await;

    let app = test_app(pool).await;
    app.clock.advance_seconds(2_772_480);
    let summary = marketplace_service::workers::close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("malformed rows do not abort the bounded batch");
    assert_eq!(
        summary,
        AuctionCloseBatchSummary {
            closed: 1,
            failed: 3
        }
    );

    let states: Vec<(String, String)> = sqlx::query_as(
        "SELECT aggregate_id, auction->>'status' FROM listings
         WHERE aggregate_id = ANY($1) ORDER BY aggregate_id",
    )
    .bind(vec![
        malformed_document.clone(),
        malformed_end.clone(),
        missing_end.clone(),
        valid.clone(),
    ])
    .fetch_all(&app.pool)
    .await
    .expect("malformed and valid states read");
    assert_eq!(
        states,
        vec![
            (malformed_document.clone(), "active".into()),
            (malformed_end.clone(), "active".into()),
            (missing_end.clone(), "active".into()),
            (valid, "unsold".into()),
        ]
    );

    let logs = common::captured_logs();
    let failure_lines: Vec<&str> = logs
        .lines()
        .filter(|line| {
            [&malformed_document, &malformed_end, &missing_end]
                .iter()
                .any(|aggregate_id| line.contains(aggregate_id.as_str()))
        })
        .collect();
    assert_eq!(failure_lines.len(), 3, "{failure_lines:?}");
    for line in failure_lines {
        assert!(line.contains("auction_close_failed"), "{line}");
        assert!(
            line.contains("auction close failed; continuing batch"),
            "{line}"
        );
        for forbidden in [
            "reserve_price",
            "reservePrice",
            "reserve_met",
            "reserveMet",
            "do-not-log",
            "not-a-timestamp",
        ] {
            assert!(!line.contains(forbidden), "{line}");
        }
    }
}

#[sqlx::test(migrations = false)]
async fn calendar_invalid_timestamps_cannot_starve_a_due_peer(pool: PgPool) {
    common::install_log_capture();
    migrate_through_0031(&pool).await;
    let seller = "h".repeat(52);
    let mut invalid_ids = Vec::new();
    for index in 0..100 {
        invalid_ids.push(
            seed_production_shaped_legacy_auction(
                &pool,
                &seller,
                &format!("calendar_invalid_{index:03}"),
                "Calendar-invalid close fixture",
            )
            .await,
        );
    }
    let valid = seed_production_shaped_legacy_auction(
        &pool,
        &"i".repeat(52),
        "valid_due_after_calendar_invalid",
        "Valid due peer after calendar-invalid fixtures",
    )
    .await;
    apply_0033(&pool).await;

    let invalid_timestamp = "2026-02-30T00:00:00.000Z";
    sqlx::query(
        "UPDATE listings
         SET auction = jsonb_set(
             jsonb_set(
                 auction,
                 '{ends_at}',
                 to_jsonb($2::text)
             ),
             '{telemetry_secret}',
             to_jsonb($3::text)
         )
         WHERE aggregate_id = ANY($1)",
    )
    .bind(&invalid_ids)
    .bind(invalid_timestamp)
    .bind("calendar-invalid-payload-do-not-log")
    .execute(&pool)
    .await
    .expect("calendar-invalid timestamps seed");

    let app = test_app(pool).await;
    app.clock.advance_seconds(2_772_480);
    let summary = marketplace_service::workers::close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("calendar-invalid rows cannot abort or starve the bounded batch");
    assert_eq!(
        summary,
        AuctionCloseBatchSummary {
            closed: 1,
            failed: 100
        }
    );
    assert!(summary.closed + summary.failed <= 200);

    let invalid_active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM listings
         WHERE aggregate_id = ANY($1) AND auction->>'status' = 'active'",
    )
    .bind(&invalid_ids)
    .fetch_one(&app.pool)
    .await
    .expect("calendar-invalid states read");
    assert_eq!(invalid_active, 100);
    let valid_status: String =
        sqlx::query_scalar("SELECT auction->>'status' FROM listings WHERE aggregate_id = $1")
            .bind(&valid)
            .fetch_one(&app.pool)
            .await
            .expect("valid due peer state reads");
    assert_eq!(valid_status, "unsold");

    let logs = common::captured_logs();
    let invalid_failure_lines: Vec<&str> = logs
        .lines()
        .filter(|line| {
            invalid_ids
                .iter()
                .any(|aggregate_id| line.contains(aggregate_id))
        })
        .collect();
    assert_eq!(invalid_failure_lines.len(), 100);
    for line in invalid_failure_lines {
        assert!(line.contains("auction_close_failed"), "{line}");
        assert!(
            line.contains("auction close failed; continuing batch"),
            "{line}"
        );
    }
    for forbidden in [
        invalid_timestamp,
        "calendar-invalid-payload-do-not-log",
        "reserve_price",
        "reservePrice",
        "reserve_met",
        "reserveMet",
    ] {
        assert!(!logs.contains(forbidden), "{logs}");
    }
}

#[sqlx::test(migrations = false)]
async fn due_auction_selection_is_bounded_and_drains_oldest_first(pool: PgPool) {
    migrate_through_0031(&pool).await;
    let seller = "g".repeat(52);
    for index in 0..101 {
        seed_production_shaped_legacy_auction(
            &pool,
            &seller,
            &format!("bounded_{index:03}"),
            "Bounded close fixture",
        )
        .await;
    }
    apply_0033(&pool).await;

    let app = test_app(pool).await;
    app.clock.advance_seconds(2_772_480);
    let first = marketplace_service::workers::close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("first bounded close batch runs");
    assert_eq!(
        first,
        AuctionCloseBatchSummary {
            closed: 100,
            failed: 0
        }
    );

    let prefix = format!("listing:{seller}_bounded_%");
    let first_states: (i64, i64) = sqlx::query_as(
        "SELECT
             COUNT(*) FILTER (WHERE auction->>'status' = 'unsold'),
             COUNT(*) FILTER (WHERE auction->>'status' = 'active')
         FROM listings WHERE aggregate_id LIKE $1",
    )
    .bind(&prefix)
    .fetch_one(&app.pool)
    .await
    .expect("first bounded batch states read");
    assert_eq!(first_states, (100, 1));
    let last_status: String = sqlx::query_scalar(
        "SELECT auction->>'status' FROM listings WHERE aggregate_id LIKE $1
         ORDER BY aggregate_id DESC LIMIT 1",
    )
    .bind(&prefix)
    .fetch_one(&app.pool)
    .await
    .expect("last ordered due auction reads");
    assert_eq!(last_status, "active");

    let second = marketplace_service::workers::close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("remaining due auction drains next pass");
    assert_eq!(
        second,
        AuctionCloseBatchSummary {
            closed: 1,
            failed: 0
        }
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
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
