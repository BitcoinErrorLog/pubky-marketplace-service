//! Drop aggregate tests (ADR-0026): convergent `drop.sync` registration from
//! the seller's homeserver record (through the real HTTP client against a
//! local homeserver double), term locking at launch, schedule gating with
//! pinned refusal copy, the single-line cart shape rule, oversell- and
//! per-buyer-cap-proofing under 100-way concurrency, expiry restock through
//! the worker, seller cancellation, and the server-time sweep.
//!
//! Layer 2: gapless edition assignment under concurrent confirms, the
//! terminal sell-out transition with its seller notification, the public
//! projection's server-side stock redaction and lazy read transitions, the
//! seller projection and buyer ready-check, and `drop.release_listings`.

mod common;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use marketplace_service::workers::run_once;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    cancel_drop_command, count, drop_aggregate, drop_record_json, execute, indexed_command_id,
    listing_aggregate, new_actor, register_command, register_listing_command, release_drop_command,
    reserve_command, sync_drop_command, test_app_with_homeserver, ts_after, TestApp,
};

const DROP_ID: &str = "winter_drop";

fn line(listing_aggregate_id: &str, expected_revision: i64, quantity: i64) -> Value {
    json!({
        "listing_aggregate_id": listing_aggregate_id,
        "expected_revision": expected_revision,
        "quantity": quantity,
    })
}

fn checkout_lines_command(command_id: &str, lines: Vec<Value>) -> Value {
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": format!("checkout:{command_id}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "checkout.create",
        "payload": {
            "lines": lines,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US",
            },
            "guarantee_policy_version": 1,
        },
    })
}

async fn drop_state(pool: &PgPool, aggregate_id: &str) -> (String, i64, i64) {
    sqlx::query_as("SELECT state, revision, remaining_quantity FROM drops WHERE aggregate_id = $1")
        .bind(aggregate_id)
        .fetch_one(pool)
        .await
        .expect("drop row exists")
}

async fn buyer_purchases(pool: &PgPool, aggregate_id: &str, buyer_pubky: &str) -> i64 {
    let quantity: Option<(i64,)> = sqlx::query_as(
        "SELECT quantity::BIGINT FROM drop_purchases \
         WHERE drop_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(aggregate_id)
    .bind(buyer_pubky)
    .fetch_optional(pool)
    .await
    .expect("purchase query succeeds");
    quantity.map(|(value,)| value).unwrap_or(0)
}

/// Registers the shared `boots_01` listing and publishes + syncs one drop
/// over it, returning the drop aggregate id. `starts_in`/`ends_in` are
/// offsets from the fixture instant.
// Nine fixture knobs the drop tests all need to steer independently.
#[allow(clippy::too_many_arguments)]
async fn sync_drop_over_boots_01(
    app: &TestApp,
    homeserver: &common::FakeHomeserver,
    seller: &common::TestActor,
    actor: &common::TestActor,
    listing_quantity: i64,
    starts_in: i64,
    ends_in: Option<i64>,
    total_quantity: i64,
    per_buyer_limit: i64,
) -> String {
    let (status, body) = execute(
        app,
        &seller.token,
        &register_command(&seller.pubky, listing_quantity),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let ends_at = ends_in.map(ts_after);
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01"],
            &ts_after(starts_in),
            ends_at.as_deref(),
            total_quantity,
            per_buyer_limit,
        ),
    );
    let (status, body) = execute(
        app,
        &actor.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 900),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync fixture failed: {body}");
    drop_aggregate(&seller.pubky, DROP_ID)
}

// === 1. drop.sync: registration, missing listings, announced-only updates ===

#[sqlx::test]
async fn drop_sync_registers_updates_while_announced_and_locks_terms_at_launch(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id = drop_aggregate(&seller.pubky, DROP_ID);
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "boots_02", 5, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second register failed: {body}");

    // Listings that have no registered aggregate are named in the failure;
    // the sync never auto-registers them.
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01", "ghost_a", "ghost_b"],
            &ts_after(600),
            Some(&ts_after(4_200)),
            10,
            2,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        body["error"]["message"],
        json!("The drop references unregistered listings: ghost_a, ghost_b.")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM drops").await, 0);

    // The buyer — not the seller — registers the drop from the record.
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01", "boots_02"],
            &ts_after(600),
            Some(&ts_after(4_200)),
            10,
            2,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync failed: {body}");
    assert_eq!(body["revision"], json!(1));
    let drop = &body["result"]["drop"];
    assert_eq!(drop["state"], json!("announced"));
    assert_eq!(drop["total_quantity"], json!(10));
    assert_eq!(drop["remaining_quantity"], json!(10));
    assert_eq!(drop["per_buyer_limit"], json!(2));
    assert_eq!(drop["listing_ids"], json!(["boots_01", "boots_02"]));
    let (kind, actor_pubky): (String, String) =
        sqlx::query_as("SELECT kind, actor_pubky FROM events WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("one drop event recorded");
    assert_eq!(kind, "drop.synced");
    assert_eq!(actor_pubky, buyer.pubky);

    // A fresh command id with the same record revision converges: success,
    // current revision, no new event.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "no-op re-sync failed: {body}");
    assert_eq!(body["revision"], json!(1));
    assert_eq!(body["event_ids"], json!([]));

    // While announced, an advancing record updates the enforcement terms.
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            2,
            &["boots_01"],
            &ts_after(900),
            Some(&ts_after(4_200)),
            6,
            3,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "announced re-sync failed: {body}");
    assert_eq!(body["revision"], json!(2));
    let drop = &body["result"]["drop"];
    assert_eq!(drop["record_revision"], json!(2));
    assert_eq!(drop["total_quantity"], json!(6));
    assert_eq!(drop["remaining_quantity"], json!(6));
    assert_eq!(drop["per_buyer_limit"], json!(3));
    assert_eq!(drop["listing_ids"], json!(["boots_01"]));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM drop_listings WHERE listing_id = 'boots_02'"
        )
        .await,
        0,
        "the dropped listing binding is removed"
    );

    // Once live on server time — even with the transition not yet persisted
    // by any command or sweep — terms are locked at launch.
    app.clock.advance_seconds(901);
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            3,
            &["boots_01"],
            &ts_after(900),
            Some(&ts_after(4_200)),
            8,
            3,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 5),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The drop's terms are locked at launch.")
    );
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 6, "the refused re-sync changed nothing");
}

// === 2. Schedule gating with pinned copy ====================================

#[sqlx::test]
async fn gating_refuses_before_start_after_end_and_after_cancel(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    sync_drop_over_boots_01(
        &app,
        &homeserver,
        &seller,
        &buyer,
        5,
        600,
        Some(1_200),
        5,
        2,
    )
    .await;

    // Before startsAt: checkout and reserve are both refused.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9000, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["message"], json!("The drop has not started."));

    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has not started."));

    // After endsAt: the drop ended, and the listing does not quietly fall
    // back to open sale.
    app.clock.advance_seconds(1_201);
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9000, 2),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["message"], json!("The drop has ended."));

    // A cancelled drop refuses identically.
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "boots_02", 5, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second register failed: {body}");
    homeserver.put_drop_record(
        &seller.pubky,
        "flash_drop",
        drop_record_json(
            &seller.pubky,
            "flash_drop",
            1,
            &["boots_02"],
            &ts_after(0),
            None,
            5,
            2,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, "flash_drop", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "flash drop sync failed: {body}");
    assert_eq!(body["result"]["drop"]["state"], json!("live"));
    let (status, body) = execute(
        &app,
        &seller.token,
        &cancel_drop_command(&seller.pubky, "flash_drop", 1, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9000, 3),
        vec![line(&format!("listing:{}_boots_02", seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has ended."));
}

// === 3. Cart shape: a drop order is exactly one line of one unit ============

#[sqlx::test]
async fn mixed_carts_and_multi_unit_lines_are_refused(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, None, 5, 2).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "plain_tee", 5, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second register failed: {body}");

    let shape_copy = json!("A drop order is one unit of one listing per checkout.");

    let mixed = checkout_lines_command(
        &indexed_command_id(0x9001, 1),
        vec![
            line(&listing_aggregate(&seller.pubky), 1, 1),
            line(&format!("listing:{}_plain_tee", seller.pubky), 1, 1),
        ],
    );
    let (status, body) = execute(&app, &buyer.token, &mixed).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(body["error"]["message"], shape_copy);

    let multi_unit = checkout_lines_command(
        &indexed_command_id(0x9001, 2),
        vec![line(&listing_aggregate(&seller.pubky), 1, 2)],
    );
    let (status, body) = execute(&app, &buyer.token, &multi_unit).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["message"], shape_copy);

    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 2, 1)).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(
        body["error"]["message"],
        json!("A drop-bound listing can be reserved one unit at a time.")
    );

    // Nothing was debited by the refusals.
    let (_, _, remaining) = drop_state(&app.pool, &drop_aggregate(&seller.pubky, DROP_ID)).await;
    assert_eq!(remaining, 5);
}

#[sqlx::test]
async fn a_two_line_drop_checkout_is_refused(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "boots_02", 5, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second register failed: {body}");
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01", "boots_02"],
            &ts_after(0),
            None,
            10,
            2,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync failed: {body}");

    // Two single-unit lines of the SAME drop: editions map one order to one
    // unit, so the cart must be exactly one drop-bound line.
    let two_lines = checkout_lines_command(
        &indexed_command_id(0x9900, 1),
        vec![
            line(&listing_aggregate(&seller.pubky), 1, 1),
            line(&format!("listing:{}_boots_02", seller.pubky), 1, 1),
        ],
    );
    let (status, body) = execute(&app, &buyer.token, &two_lines).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        body["error"]["message"],
        json!("A drop order is one unit of one listing per checkout.")
    );
    let (_, _, remaining) = drop_state(&app.pool, &drop_aggregate(&seller.pubky, DROP_ID)).await;
    assert_eq!(remaining, 10, "the refusal debited nothing");

    // The single-line checkout of the same drop proceeds.
    let single = checkout_lines_command(
        &indexed_command_id(0x9900, 2),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &single).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "single-line checkout failed: {body}"
    );
}

// === 4. Oversell proof: 100 concurrent checkouts, total 10 ==================

#[sqlx::test]
async fn hundred_concurrent_checkouts_sell_exactly_the_drop_total(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &seller, 100, 0, None, 10, 2).await;

    let mut buyers = Vec::with_capacity(100);
    for _ in 0..100 {
        buyers.push(new_actor(&app).await);
    }

    let mut handles = Vec::with_capacity(100);
    for (index, buyer) in buyers.into_iter().enumerate() {
        let router = app.router.clone();
        let seller_pubky = seller.pubky.clone();
        handles.push(tokio::spawn(async move {
            let mut expected_revision = 1i64;
            for attempt in 0..300u64 {
                let command = checkout_lines_command(
                    &indexed_command_id(0x9100, (index as u64 + 1) * 1_000 + attempt),
                    vec![line(
                        &listing_aggregate(&seller_pubky),
                        expected_revision,
                        1,
                    )],
                );
                let (_, body) = common::send(
                    router.clone(),
                    "POST",
                    "/v1/commands",
                    Some(&buyer.token),
                    &command,
                )
                .await;
                if body["ok"] == json!(true) {
                    return "accepted";
                }
                match body["error"]["code"].as_str() {
                    Some("REVISION_CONFLICT") => {
                        expected_revision = body["error"]["current_revision"]
                            .as_i64()
                            .expect("revision conflicts carry current_revision");
                    }
                    Some("INSUFFICIENT_INVENTORY") => {
                        assert_eq!(
                            body["error"]["message"],
                            json!("The drop is sold out."),
                            "sold-out copy drifted: {body}"
                        );
                        return "sold_out";
                    }
                    other => panic!("unexpected checkout rejection {other:?}: {body}"),
                }
            }
            panic!("checkout did not reach a terminal outcome in 300 attempts");
        }));
    }

    let mut accepted = 0;
    let mut sold_out = 0;
    for handle in handles {
        match handle.await.expect("checkout task completes") {
            "accepted" => accepted += 1,
            "sold_out" => sold_out += 1,
            outcome => panic!("unexpected outcome {outcome}"),
        }
    }
    assert_eq!(accepted, 10, "exactly the drop total is sold");
    assert_eq!(sold_out, 90, "everyone else fails clean");

    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 10);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE drop_aggregate_id IS NOT NULL"
        )
        .await,
        10,
        "every accepted order is stamped with its drop"
    );
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("live", 0));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COALESCE(SUM(quantity), 0)::BIGINT FROM drop_purchases"
        )
        .await,
        10
    );
}

// === 5. Per-buyer cap: one buyer, limit 2, 10 parallel attempts =============

#[sqlx::test]
async fn one_buyer_cannot_exceed_the_per_buyer_limit_under_concurrency(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 10, 0, None, 10, 2).await;

    let mut handles = Vec::with_capacity(10);
    for index in 0..10u64 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let seller_pubky = seller.pubky.clone();
        handles.push(tokio::spawn(async move {
            let mut expected_revision = 1i64;
            for attempt in 0..300u64 {
                let command = checkout_lines_command(
                    &indexed_command_id(0x9200, (index + 1) * 1_000 + attempt),
                    vec![line(
                        &listing_aggregate(&seller_pubky),
                        expected_revision,
                        1,
                    )],
                );
                let (_, body) = common::send(
                    router.clone(),
                    "POST",
                    "/v1/commands",
                    Some(&token),
                    &command,
                )
                .await;
                if body["ok"] == json!(true) {
                    return "accepted";
                }
                match body["error"]["code"].as_str() {
                    Some("REVISION_CONFLICT") => {
                        expected_revision = body["error"]["current_revision"]
                            .as_i64()
                            .expect("revision conflicts carry current_revision");
                    }
                    Some("INVALID_STATE") => {
                        assert_eq!(
                            body["error"]["message"],
                            json!("You have reached this drop's per-buyer limit."),
                            "per-buyer copy drifted: {body}"
                        );
                        return "limited";
                    }
                    other => panic!("unexpected checkout rejection {other:?}: {body}"),
                }
            }
            panic!("checkout did not reach a terminal outcome in 300 attempts");
        }));
    }

    let mut accepted = 0;
    let mut limited = 0;
    for handle in handles {
        match handle.await.expect("checkout task completes") {
            "accepted" => accepted += 1,
            "limited" => limited += 1,
            outcome => panic!("unexpected outcome {outcome}"),
        }
    }
    assert_eq!(accepted, 2, "exactly the per-buyer limit is sold");
    assert_eq!(limited, 8);
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 8);
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer.pubky).await,
        2
    );
}

// === 6. Expiry restock: a lapsed hold returns its drop unit =================

#[sqlx::test]
async fn an_expired_reservation_restocks_the_drop_and_frees_the_buyer_cap(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, None, 5, 1).await;

    // Reservation TTL 600 s against the live drop.
    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::OK, "reserve failed: {body}");
    let (stamp,): (Option<String>,) =
        sqlx::query_as("SELECT drop_aggregate_id FROM reservations LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(stamp.as_deref(), Some(aggregate_id.as_str()));
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 4);
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer.pubky).await,
        1
    );

    // With per_buyer_limit 1, the same buyer is capped while the hold lives.
    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 2, 1, 2)).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(
        body["error"]["message"],
        json!("You have reached this drop's per-buyer limit.")
    );

    // The worker expires the hold on server time and credits the drop.
    app.clock.advance_seconds(601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.reservations_expired, 1);
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("live", 5));
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer.pubky).await,
        0
    );

    // A new buyer can hold the restocked unit (listing revision advanced:
    // register 1, reserve 2, expiry 3).
    let other_buyer = new_actor(&app).await;
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9300, 1),
        vec![line(&listing_aggregate(&seller.pubky), 3, 1)],
    );
    let (status, body) = execute(&app, &other_buyer.token, &checkout).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-restock checkout failed: {body}"
    );
    let (order_stamp,): (Option<String>,) =
        sqlx::query_as("SELECT drop_aggregate_id FROM orders LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    assert_eq!(order_stamp.as_deref(), Some(aggregate_id.as_str()));
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 4);
}

// === 7. drop.cancel ==========================================================

#[sqlx::test]
async fn the_seller_cancels_a_live_drop_and_new_holds_are_refused(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, None, 5, 2).await;

    // One real hold before the cancel; its unit stays with the buyer.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9400, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let (_, revision, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 4);

    // A non-seller cannot cancel.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &cancel_drop_command(&seller.pubky, DROP_ID, revision, 1),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
    assert_eq!(
        body["error"]["message"],
        json!("Only the seller may cancel a drop.")
    );

    // A stale revision is a conflict carrying the current revision.
    let (status, body) = execute(
        &app,
        &seller.token,
        &cancel_drop_command(&seller.pubky, DROP_ID, revision - 1, 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(revision));

    let (status, body) = execute(
        &app,
        &seller.token,
        &cancel_drop_command(&seller.pubky, DROP_ID, revision, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    assert_eq!(body["result"]["drop"]["state"], json!("ended_cancelled"));

    // New holds are refused; the outstanding hold was NOT released.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9400, 2),
        vec![line(&listing_aggregate(&seller.pubky), 2, 1)],
    );
    let other_buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &other_buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has ended."));
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("ended_cancelled", 4));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.cancelled'"
        )
        .await,
        1
    );
}

// === 8. Worker sweep: server-time transitions without traffic ===============

#[sqlx::test]
async fn the_sweep_worker_starts_and_closes_untouched_drops_on_server_time(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "boots_02", 5, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second register failed: {body}");

    // Drop A: announced, starts in 600 s, open-ended.
    homeserver.put_drop_record(
        &seller.pubky,
        "drop_a",
        drop_record_json(
            &seller.pubky,
            "drop_a",
            1,
            &["boots_01"],
            &ts_after(600),
            None,
            5,
            2,
        ),
    );
    // Drop B: live now, ends in 600 s.
    homeserver.put_drop_record(
        &seller.pubky,
        "drop_b",
        drop_record_json(
            &seller.pubky,
            "drop_b",
            1,
            &["boots_02"],
            &ts_after(0),
            Some(&ts_after(600)),
            5,
            2,
        ),
    );
    for (drop_id, index, expected_state) in [("drop_a", 1, "announced"), ("drop_b", 2, "live")] {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &sync_drop_command(&seller.pubky, drop_id, index),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{drop_id} sync failed: {body}");
        assert_eq!(body["result"]["drop"]["state"], json!(expected_state));
    }

    // Nothing is due yet on server time.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.drops_transitioned, 0);

    // +601 s: A starts, B closes — no command ever touched either drop.
    app.clock.advance_seconds(601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.drops_transitioned, 2);

    let (state_a, revision_a, _) =
        drop_state(&app.pool, &drop_aggregate(&seller.pubky, "drop_a")).await;
    assert_eq!((state_a.as_str(), revision_a), ("live", 2));
    let (state_b, revision_b, _) =
        drop_state(&app.pool, &drop_aggregate(&seller.pubky, "drop_b")).await;
    assert_eq!((state_b.as_str(), revision_b), ("ended_closed", 2));
    let (active_a,): (bool,) =
        sqlx::query_as("SELECT active FROM drop_listings WHERE drop_aggregate_id = $1")
            .bind(drop_aggregate(&seller.pubky, "drop_a"))
            .fetch_one(&app.pool)
            .await
            .expect("drop_a binding exists");
    assert!(active_a, "a started drop keeps its bindings active");
    let (active_b,): (bool,) =
        sqlx::query_as("SELECT active FROM drop_listings WHERE drop_aggregate_id = $1")
            .bind(drop_aggregate(&seller.pubky, "drop_b"))
            .fetch_one(&app.pool)
            .await
            .expect("drop_b binding exists");
    assert!(!active_b, "a closed drop deactivates its bindings");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.started'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.ended'"
        )
        .await,
        1
    );

    // The sweep converged: a second pass finds nothing due.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.drops_transitioned, 0);

    // Gating and the sweep agree: the started drop sells, the closed one
    // refuses.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines_command(
            &indexed_command_id(0x9500, 1),
            vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "post-start checkout failed: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines_command(
            &indexed_command_id(0x9500, 2),
            vec![line(&format!("listing:{}_boots_02", seller.pubky), 1, 1)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has ended."));
}

// === 9. Cancellation release paths credit the drop ==========================

#[sqlx::test]
async fn cancelling_orders_credits_the_drop_for_unpaid_and_paid_holds(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, None, 5, 2).await;

    // Unpaid order: the buyer's immediate cancel restocks the drop.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9600, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 4);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Changed mind" }),
            1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "immediate cancel failed: {body}");
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 5, "the unpaid cancel restocked the drop");
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer.pubky).await,
        0
    );

    // Paid order: cancel_request + the seller's approval restocks the drop.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9600, 2),
        vec![line(&listing_aggregate(&seller.pubky), 3, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "second checkout failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment failed: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::order_command(
            "order.cancel_request",
            &order_id,
            2,
            json!({ "reason": "No longer needed" }),
            2,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel request failed: {body}");
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(
        remaining, 4,
        "a pending cancel request releases nothing yet"
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::order_command("order.cancel_approve", &order_id, 3, json!({}), 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel approve failed: {body}");
    let (_, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!(remaining, 5, "the approved cancel restocked the drop");
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer.pubky).await,
        0
    );
}

// === 10. Editions: gapless 1..=N under concurrent confirms ===================

#[sqlx::test]
async fn concurrent_confirms_assign_gapless_editions(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &seller, 25, 0, None, 25, 1).await;

    // Twenty pending drop orders, one per buyer (sequential holds; the
    // listing revision advances by one per checkout).
    let mut pending = Vec::with_capacity(20);
    for index in 0..20u64 {
        let buyer = new_actor(&app).await;
        let command = checkout_lines_command(
            &indexed_command_id(0x9800, index + 1),
            vec![line(&listing_aggregate(&seller.pubky), 1 + index as i64, 1)],
        );
        let (status, body) = execute(&app, &buyer.token, &command).await;
        assert_eq!(status, StatusCode::OK, "checkout {index} failed: {body}");
        let payment_id = body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id present")
            .to_string();
        pending.push((buyer, payment_id));
    }

    // Twenty-way concurrent confirms: assignment happens only in the
    // confirmation transaction, under the drop row lock, so the editions
    // come out 1..=20 with no duplicates and no gaps.
    let mut handles = Vec::with_capacity(20);
    for (index, (buyer, payment_id)) in pending.into_iter().enumerate() {
        let router = app.router.clone();
        handles.push(tokio::spawn(async move {
            let command =
                common::payment_command(&payment_id, 1, "confirmed", 1, 5_000 + index as u64);
            let (_, body) =
                common::send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await;
            assert_eq!(body["ok"], json!(true), "confirm failed: {body}");
            body["result"]["order"]["edition"]
                .as_i64()
                .expect("a confirmed drop order carries its edition")
        }));
    }
    let mut editions = Vec::with_capacity(20);
    for handle in handles {
        editions.push(handle.await.expect("confirm task completes"));
    }
    editions.sort_unstable();
    assert_eq!(editions, (1..=20).collect::<Vec<i64>>());

    let stored: Vec<(i64,)> = sqlx::query_as(
        "SELECT edition::BIGINT FROM orders \
         WHERE drop_aggregate_id = $1 AND edition IS NOT NULL ORDER BY edition",
    )
    .bind(&aggregate_id)
    .fetch_all(&app.pool)
    .await
    .expect("editions readable");
    assert_eq!(
        stored
            .into_iter()
            .map(|(edition,)| edition)
            .collect::<Vec<_>>(),
        (1..=20).collect::<Vec<i64>>()
    );

    // Twenty paid of twenty-five: the drop stays live.
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("live", 5));
}

// === 11. Sell-out: terminal ended_sold_out + seller notification =============

#[sqlx::test]
async fn selling_out_ends_the_drop_terminally_and_notifies_the_seller(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer_a = new_actor(&app).await;
    let buyer_b = new_actor(&app).await;
    let buyer_c = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer_a, 5, 0, None, 2, 1).await;

    // A pays for edition 1, then cancels: the paid-cancel credit restocks
    // remaining without decrementing the paid count.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9a00, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer_a.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout A failed: {body}");
    let order_a = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_a = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, confirmed) = execute(
        &app,
        &buyer_a.token,
        &common::payment_command(&payment_a, 1, "confirmed", 1, 2_001),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirm A failed: {confirmed}");
    // The order projection carries the edition and the drop stamp.
    assert_eq!(confirmed["result"]["order"]["edition"], json!(1));
    assert_eq!(
        confirmed["result"]["order"]["drop_aggregate_id"],
        json!(aggregate_id)
    );
    let (status, body) = execute(
        &app,
        &buyer_a.token,
        &common::order_command(
            "order.cancel_request",
            &order_a,
            2,
            json!({ "reason": "Changed mind" }),
            2_002,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel request failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::order_command("order.cancel_approve", &order_a, 3, json!({}), 2_003),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel approve failed: {body}");

    // B holds one unit through a standalone reservation (TTL 600 s).
    let (status, body) = execute(
        &app,
        &buyer_b.token,
        &reserve_command(&seller.pubky, 1, 1, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reserve B failed: {body}");

    // C pays the last unit: the paid count reaches the total and the drop
    // ends sold out inside the confirmation transaction.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9a00, 2),
        vec![line(&listing_aggregate(&seller.pubky), 5, 1)],
    );
    let (status, body) = execute(&app, &buyer_c.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout C failed: {body}");
    let order_c = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_c = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, confirmed) = execute(
        &app,
        &buyer_c.token,
        &common::payment_command(&payment_c, 1, "confirmed", 1, 2_004),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirm C failed: {confirmed}");
    assert_eq!(confirmed["result"]["order"]["edition"], json!(2));

    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("ended_sold_out", 0));
    let (active,): (bool,) =
        sqlx::query_as("SELECT active FROM drop_listings WHERE drop_aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("binding exists");
    assert!(!active, "a sold-out drop deactivates its bindings");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.ended'"
        )
        .await,
        1
    );
    // The seller's notification intent is in the outbox; the worker
    // delivers it below.
    let (recipient,): (String,) = sqlx::query_as(
        "SELECT payload->>'recipient_pubky' FROM outbox \
         WHERE kind = 'notification.drop_sold_out'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("sold-out intent enqueued");
    assert_eq!(recipient, seller.pubky);

    // The single order read carries the edition too.
    let (status, order_view) = common::send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_c}"),
        Some(&buyer_c.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {order_view}");
    assert_eq!(order_view["edition"], json!(2));
    assert_eq!(order_view["drop_aggregate_id"], json!(aggregate_id));

    // Subsequent checkout is refused with the ended copy.
    let refused = checkout_lines_command(
        &indexed_command_id(0x9a00, 3),
        vec![line(&listing_aggregate(&seller.pubky), 7, 1)],
    );
    let other_buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &other_buyer.token, &refused).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has ended."));

    // B's unpaid hold lapses AFTER the sell-out: the credit keeps honest
    // books but the state is terminal — nothing reopens.
    app.clock.advance_seconds(601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.reservations_expired, 1);
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("ended_sold_out", 1));
    assert_eq!(
        buyer_purchases(&app.pool, &aggregate_id, &buyer_b.pubky).await,
        0
    );
    let still_refused = checkout_lines_command(
        &indexed_command_id(0x9a00, 4),
        vec![line(&listing_aggregate(&seller.pubky), 8, 1)],
    );
    let (status, body) = execute(&app, &other_buyer.token, &still_refused).await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["message"], json!("The drop has ended."));

    // The worker delivered the seller's sold-out notification.
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM notifications WHERE type = 'drop_sold_out'"
        )
        .await,
        1
    );
}

// === 12. Public projection: server-side redaction + lazy transitions =========

async fn public_drop(app: &TestApp, seller_pubky: &str, drop_id: &str) -> (StatusCode, Value) {
    common::send(
        app.router.clone(),
        "GET",
        &format!("/v0/drops/{seller_pubky}/{drop_id}"),
        None,
        &json!(null),
    )
    .await
}

async fn set_remaining(pool: &PgPool, aggregate_id: &str, remaining: i64) {
    sqlx::query("UPDATE drops SET remaining_quantity = $2 WHERE aggregate_id = $1")
        .bind(aggregate_id)
        .bind(remaining)
        .execute(pool)
        .await
        .expect("remaining update succeeds");
}

#[sqlx::test]
async fn the_public_drop_projection_redacts_stock_server_side(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    for (listing_id, index) in [
        ("boots_02", 1),
        ("boots_03", 2),
        ("boots_04", 3),
        ("boots_05", 4),
    ] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &register_listing_command(&seller.pubky, listing_id, 5, index),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{listing_id} register failed: {body}"
        );
    }
    // One drop per display mode plus an announced one for the lazy read.
    for (drop_id, listing_id, display, starts_in, total, index) in [
        ("drop_exact", "boots_01", "exact", 0, 5, 1),
        ("drop_bands", "boots_02", "bands", 0, 100, 2),
        ("drop_hidden", "boots_03", "hidden", 0, 5, 3),
        ("drop_tiny", "boots_04", "bands", 0, 10, 4),
        ("drop_later", "boots_05", "exact", 600, 5, 5),
    ] {
        let mut record = drop_record_json(
            &seller.pubky,
            drop_id,
            1,
            &[listing_id],
            &ts_after(starts_in),
            None,
            total,
            2,
        );
        record["stockDisplay"] = json!(display);
        homeserver.put_drop_record(&seller.pubky, drop_id, record);
        let (status, body) = execute(
            &app,
            &buyer.token,
            &sync_drop_command(&seller.pubky, drop_id, index),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{drop_id} sync failed: {body}");
    }

    // Exact: the number is public; no band.
    let (status, body) = public_drop(&app, &seller.pubky, "drop_exact").await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    let drop = &body["drop"];
    assert_eq!(drop["seller_pubky"], json!(seller.pubky));
    assert_eq!(drop["drop_id"], json!("drop_exact"));
    assert_eq!(
        drop["aggregate_id"],
        json!(drop_aggregate(&seller.pubky, "drop_exact"))
    );
    assert_eq!(drop["state"], json!("live"));
    assert_eq!(drop["format"], json!("fcfs"));
    assert_eq!(drop["starts_at"], json!(ts_after(0)));
    assert_eq!(drop["ends_at"], json!(null));
    assert_eq!(drop["stock_display"], json!("exact"));
    assert_eq!(drop["total_quantity"], json!(5));
    assert_eq!(drop["per_buyer_limit"], json!(2));
    assert_eq!(drop["remaining"], json!(5));
    assert_eq!(drop["remaining_band"], json!(null));
    assert_eq!(drop["revision"], json!(1));
    assert_eq!(drop["server_time"], json!(ts_after(0)));

    // Bands: never the exact number. Pin the band edges on total 100.
    let bands_aggregate = drop_aggregate(&seller.pubky, "drop_bands");
    for (remaining, band) in [(26, "plenty"), (25, "low"), (6, "low"), (5, "last_few")] {
        set_remaining(&app.pool, &bands_aggregate, remaining).await;
        let (status, body) = public_drop(&app, &seller.pubky, "drop_bands").await;
        assert_eq!(status, StatusCode::OK, "bands read failed: {body}");
        assert_eq!(
            body["drop"]["remaining_band"],
            json!(band),
            "remaining {remaining} banded wrong"
        );
        assert_eq!(
            body["drop"]["remaining"],
            json!(null),
            "bands must never expose the exact count"
        );
    }
    // The last_few floor is one unit even where 5% of the total rounds to 0.
    let tiny_aggregate = drop_aggregate(&seller.pubky, "drop_tiny");
    set_remaining(&app.pool, &tiny_aggregate, 1).await;
    let (status, body) = public_drop(&app, &seller.pubky, "drop_tiny").await;
    assert_eq!(status, StatusCode::OK, "tiny read failed: {body}");
    assert_eq!(body["drop"]["remaining_band"], json!("last_few"));
    assert_eq!(body["drop"]["remaining"], json!(null));

    // Hidden: neither the number nor a band; the totals stay visible.
    let (status, body) = public_drop(&app, &seller.pubky, "drop_hidden").await;
    assert_eq!(status, StatusCode::OK, "hidden read failed: {body}");
    assert_eq!(body["drop"]["remaining"], json!(null));
    assert_eq!(body["drop"]["remaining_band"], json!(null));
    assert_eq!(body["drop"]["total_quantity"], json!(5));

    // Lazy transition on read: the public projection never shows announced
    // after startsAt, even with no command or sweep in between.
    let (status, body) = public_drop(&app, &seller.pubky, "drop_later").await;
    assert_eq!(status, StatusCode::OK, "announced read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("announced"));
    app.clock.advance_seconds(601);
    let (status, body) = public_drop(&app, &seller.pubky, "drop_later").await;
    assert_eq!(status, StatusCode::OK, "post-start read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));
    assert_eq!(body["drop"]["server_time"], json!(ts_after(601)));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.started'"
        )
        .await,
        1
    );

    // Unknown drops are 404 without a session.
    let (status, body) = public_drop(&app, &seller.pubky, "no_such_drop").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
}

// === 13. Seller projection + buyer ready-check ===============================

#[sqlx::test]
async fn the_seller_projection_and_ready_check_are_role_scoped(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, None, 5, 2).await;

    let checkout = checkout_lines_command(
        &indexed_command_id(0x9b00, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");

    // The seller reads the full facts.
    let uri = format!("/v1/drops/{aggregate_id}");
    let (status, body) = common::send(
        app.router.clone(),
        "GET",
        &uri,
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seller read failed: {body}");
    let drop = &body["drop"];
    assert_eq!(drop["state"], json!("live"));
    assert_eq!(drop["total_quantity"], json!(5));
    assert_eq!(drop["per_buyer_limit"], json!(2));
    assert_eq!(drop["remaining_quantity"], json!(4));
    assert_eq!(drop["paid_quantity"], json!(0));
    assert_eq!(drop["buyer_count"], json!(1));
    assert_eq!(drop["stock_display"], json!("exact"));
    assert_eq!(drop["listing_ids"], json!(["boots_01"]));
    assert_eq!(drop["starts_at"], json!(ts_after(0)));
    assert_eq!(drop["server_time"], json!(ts_after(0)));
    assert!(drop["revision"].is_i64());

    // Everyone else — the buyer included — is 404, indistinguishable from a
    // drop that does not exist.
    for outsider in [&buyer, &stranger] {
        let (status, foreign) = common::send(
            app.router.clone(),
            "GET",
            &uri,
            Some(&outsider.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {foreign}");
        assert_eq!(foreign["error"]["code"], json!("NOT_FOUND"));
    }

    // The ready-check is any-authenticated and per-buyer.
    let me_uri = format!("/v1/drops/{aggregate_id}/me");
    let (status, body) = common::send(
        app.router.clone(),
        "GET",
        &me_uri,
        Some(&buyer.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ready-check failed: {body}");
    assert_eq!(
        body,
        json!({ "purchased": 1, "per_buyer_limit": 2, "remaining_allowance": 1 })
    );
    let (status, body) = common::send(
        app.router.clone(),
        "GET",
        &me_uri,
        Some(&stranger.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fresh ready-check failed: {body}");
    assert_eq!(
        body,
        json!({ "purchased": 0, "per_buyer_limit": 2, "remaining_allowance": 2 })
    );
    let (status, body) = common::send(
        app.router.clone(),
        "GET",
        &format!("/v1/drops/{}/me", drop_aggregate(&seller.pubky, "ghost")),
        Some(&buyer.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
}

// === 14. drop.release_listings ================================================

#[sqlx::test]
async fn release_listings_returns_ended_drop_listings_to_open_sale(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id =
        sync_drop_over_boots_01(&app, &homeserver, &seller, &buyer, 5, 0, Some(600), 5, 2).await;

    // Refused while the drop is live.
    let (status, body) = execute(
        &app,
        &seller.token,
        &release_drop_command(&seller.pubky, DROP_ID, 1, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("Listings release only after the drop ends.")
    );

    // A non-seller cannot release.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &release_drop_command(&seller.pubky, DROP_ID, 1, 2),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
    assert_eq!(
        body["error"]["message"],
        json!("Only the seller may release a drop's listings.")
    );

    // The window elapses on server time (not yet persisted by any sweep).
    app.clock.advance_seconds(601);

    // A stale revision is a conflict carrying the current revision.
    let (status, body) = execute(
        &app,
        &seller.token,
        &release_drop_command(&seller.pubky, DROP_ID, 99, 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));

    // The release persists the lazy ended_closed transition first, then
    // removes the bindings from gating consideration.
    let (status, body) = execute(
        &app,
        &seller.token,
        &release_drop_command(&seller.pubky, DROP_ID, 1, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "release failed: {body}");
    assert_eq!(body["revision"], json!(3));
    assert_eq!(body["result"]["drop"]["state"], json!("ended_closed"));
    let (active, released): (bool, bool) =
        sqlx::query_as("SELECT active, released FROM drop_listings WHERE drop_aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("binding exists");
    assert!(!active);
    assert!(released);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'drop.listings_released'"
        )
        .await,
        1
    );

    // The released listing checks out as ordinary open inventory: no drop
    // gating, no drop stamp.
    let checkout = checkout_lines_command(
        &indexed_command_id(0x9c00, 1),
        vec![line(&listing_aggregate(&seller.pubky), 1, 1)],
    );
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-release checkout failed: {body}"
    );
    assert_eq!(
        body["result"]["orders"][0]["drop_aggregate_id"],
        json!(null)
    );
    let (stamp,): (Option<String>,) =
        sqlx::query_as("SELECT drop_aggregate_id FROM orders LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    assert_eq!(stamp, None, "a released listing sells outside the drop");
    // The drop's own books are untouched by the release.
    let (state, _, remaining) = drop_state(&app.pool, &aggregate_id).await;
    assert_eq!((state.as_str(), remaining), ("ended_closed", 5));
}
