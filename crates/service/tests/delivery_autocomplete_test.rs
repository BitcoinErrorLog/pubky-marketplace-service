//! Delivery autocomplete worker tests (ADR-0019): with no carrier tracking
//! feed, post-purchase liveness is server time — `shipped → delivered`
//! after `DELIVERY_ASSUME_DAYS` (flagged `delivery_assumed` on the
//! projection) and `delivered → completed` after `AUTO_COMPLETE_DAYS`
//! unless a return/cancel request is open. Both transitions respect the
//! domain state machine's extra server triggers on the existing edges,
//! attribute their events to the system actor (never a peer), and are
//! idempotent under double-claim.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    count, create_paid_order, execute, new_actor, order_command, send, test_app, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::{
    assume_due_deliveries, complete_due_delivered_orders, run_once,
};

const DAY_SECONDS: i64 = 24 * 60 * 60;

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

async fn ship(app: &TestApp, seller_token: &str, order_id: &str, command_number: u64) {
    let (status, body) = execute(
        app,
        seller_token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-AUTO" }),
            command_number,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship fixture failed: {body}");
}

async fn confirm_delivery(app: &TestApp, buyer_token: &str, order_id: &str, command_number: u64) {
    let (status, body) = execute(
        app,
        buyer_token,
        &order_command(
            "fulfillment.confirm_delivery",
            order_id,
            3,
            json!({}),
            command_number,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery fixture failed: {body}");
}

async fn order_state(pool: &PgPool, order_id: &str) -> (String, bool, i64) {
    sqlx::query_as("SELECT state, delivery_assumed, revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row exists")
}

// The shipped → delivered assumption: due only after DELIVERY_ASSUME_DAYS,
// flagged on the projection, system-attributed, both participants notified.
#[sqlx::test]
async fn assumes_delivery_after_the_configured_window_and_flags_the_projection(pool: PgPool) {
    let app = test_app(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    ship(&app, &seller.token, order_id, 1_400).await;

    // Inside the window nothing is due (run through the full leased pass to
    // prove the TASK_DELIVERY_AUTOCOMPLETE wiring).
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.deliveries_assumed, 0);

    // 14 days (the test-config DELIVERY_ASSUME_DAYS) after shipment the
    // order is due.
    app.clock.advance_seconds(14 * DAY_SECONDS);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.deliveries_assumed, 1);
    assert_eq!(summary.orders_auto_completed, 0);

    let (state, delivery_assumed, revision) = order_state(&app.pool, order_id).await;
    assert_eq!(
        (state.as_str(), delivery_assumed, revision),
        ("delivered", true, 4)
    );

    // The projection tells the UI the delivery was assumed, not confirmed,
    // and the buyer acts next (review or return request).
    let (status, projected) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "projection failed: {projected}");
    assert_eq!(projected["delivery_assumed"], json!(true));
    assert_eq!(projected["shipment"]["state"], json!("delivered"));
    assert!(projected["shipment"]["delivered_at"].is_string());
    assert_eq!(projected["next_actor"], json!("buyer"));

    // One system-attributed delivery event; a buyer confirmation writes the
    // same event kind with the buyer as actor.
    let (actors,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events \
         WHERE aggregate_id = $1 AND kind = 'fulfillment.delivered' AND actor_pubky = 'system'",
    )
    .bind(format!("order:{order_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("event count");
    assert_eq!(actors, 1);

    // The buyer gets the assumption prompt ("tell us if it hasn't
    // arrived"); the seller hears the delivery as usual.
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_delivery_assumed'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_delivered'"
        )
        .await,
        1
    );
}

// The delivered → completed auto-completion: due only after
// AUTO_COMPLETE_DAYS, system-attributed, both participants notified.
#[sqlx::test]
async fn auto_completes_delivered_orders_after_the_configured_window(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    ship(&app, &seller.token, order_id, 1_410).await;
    confirm_delivery(&app, &buyer.token, order_id, 1_411).await;

    // Inside the window nothing completes.
    let completed = complete_due_delivered_orders(&app.pool, app.clock.now(), 14)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 0);

    app.clock.advance_seconds(14 * DAY_SECONDS);
    let completed = complete_due_delivered_orders(&app.pool, app.clock.now(), 14)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 1);

    let (state, _, revision) = order_state(&app.pool, order_id).await;
    assert_eq!((state.as_str(), revision), ("completed", 5));

    let (system_events,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events \
         WHERE aggregate_id = $1 AND kind = 'order.completed' AND actor_pubky = 'system'",
    )
    .bind(format!("order:{order_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("event count");
    assert_eq!(system_events, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_completed'"
        )
        .await,
        2,
        "buyer and seller are both notified"
    );

    // A buyer-confirmed delivery is never flagged as assumed.
    let (status, projected) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "projection failed: {projected}");
    assert_eq!(projected["delivery_assumed"], json!(false));
    assert_eq!(projected["next_actor"], Value::Null);
}

// An open return blocks auto-completion; an open cancel request keeps the
// order out of both sweeps. Both are their own order states, so the block
// is structural, not a best-effort check.
#[sqlx::test]
async fn open_return_or_cancel_requests_block_auto_complete(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // Delivered order whose buyer opens a return the next day.
    let returned = create_paid_order(&app, &seller, &buyer).await;
    ship(&app, &seller.token, &returned.order_id, 1_420).await;
    confirm_delivery(&app, &buyer.token, &returned.order_id, 1_421).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "return.request",
            &returned.order_id,
            4,
            json!({ "reason": "Not as described", "requested_amount_minor": returned.total_minor }),
            1_422,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return request failed: {body}");

    // A second order (fresh participants, so no command-id replay) whose
    // buyer asks to cancel while it is still paid.
    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let cancelled = create_paid_order(&app, &other_seller, &other_buyer).await;
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &order_command(
            "order.cancel_request",
            &cancelled.order_id,
            2,
            json!({ "reason": "Changed mind" }),
            1_423,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel request failed: {body}");

    // Well past both windows: neither order moves.
    app.clock.advance_seconds(30 * DAY_SECONDS);
    let assumed = assume_due_deliveries(&app.pool, app.clock.now(), 14)
        .await
        .expect("sweep runs");
    let completed = complete_due_delivered_orders(&app.pool, app.clock.now(), 14)
        .await
        .expect("sweep runs");
    assert_eq!((assumed, completed), (0, 0));

    let (state, _, _) = order_state(&app.pool, &returned.order_id).await;
    assert_eq!(state, "return_requested");
    let (state, _, _) = order_state(&app.pool, &cancelled.order_id).await;
    assert_eq!(state, "cancel_requested");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE actor_pubky = 'system'"
        )
        .await,
        0,
        "no system transition fired"
    );
}

// A completed-via-review order, and an order that completed through the
// assumption + auto-complete pipeline, stay done; nothing regresses.
#[sqlx::test]
async fn worker_transitions_are_idempotent_under_double_claim(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    ship(&app, &seller.token, order_id, 1_430).await;
    app.clock.advance_seconds(14 * DAY_SECONDS);

    // Two racing passes claim the same due order: SKIP LOCKED plus the
    // state compare-and-swap let exactly one apply the effect.
    let now = app.clock.now();
    let (first, second) = tokio::join!(
        assume_due_deliveries(&app.pool, now, 14),
        assume_due_deliveries(&app.pool, now, 14)
    );
    assert_eq!(
        first.expect("first pass") + second.expect("second pass"),
        1,
        "exactly one claimant transitions the order"
    );

    // A sequential re-run is a no-op: no extra event, no extra revision.
    let events_before = count(&app.pool, "SELECT COUNT(*) FROM events").await;
    let repeated = assume_due_deliveries(&app.pool, app.clock.now(), 14)
        .await
        .expect("re-run");
    assert_eq!(repeated, 0);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM events").await,
        events_before
    );
    let (state, delivery_assumed, revision) = order_state(&app.pool, order_id).await;
    assert_eq!(
        (state.as_str(), delivery_assumed, revision),
        ("delivered", true, 4)
    );

    // Same guarantee for the auto-completion edge.
    app.clock.advance_seconds(14 * DAY_SECONDS);
    let now = app.clock.now();
    let (first, second) = tokio::join!(
        complete_due_delivered_orders(&app.pool, now, 14),
        complete_due_delivered_orders(&app.pool, now, 14)
    );
    assert_eq!(first.expect("first pass") + second.expect("second pass"), 1);
    let repeated = complete_due_delivered_orders(&app.pool, app.clock.now(), 14)
        .await
        .expect("re-run");
    assert_eq!(repeated, 0);
    let (state, _, revision) = order_state(&app.pool, order_id).await;
    assert_eq!((state.as_str(), revision), ("completed", 5));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.completed'"
        )
        .await,
        1
    );
}
