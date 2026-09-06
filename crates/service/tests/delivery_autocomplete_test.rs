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
use marketplace_service::config::{
    DEFAULT_AUTO_COMPLETE_DAYS, DEFAULT_DELIVERY_ASSUME_DAYS, DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
};
use marketplace_service::workers::{
    assume_due_deliveries, complete_due_delivered_orders, run_once, DELIVERY_SWEEP_MAX_BATCHES,
};

const DAY_SECONDS: i64 = 24 * 60 * 60;

async fn assume_due(pool: &PgPool, now: chrono::DateTime<chrono::Utc>) -> u64 {
    assume_due_deliveries(
        pool,
        now,
        DEFAULT_DELIVERY_ASSUME_DAYS,
        DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        DELIVERY_SWEEP_MAX_BATCHES,
    )
    .await
    .expect("assume sweep runs")
}

async fn complete_due(pool: &PgPool, now: chrono::DateTime<chrono::Utc>) -> u64 {
    complete_due_delivered_orders(
        pool,
        now,
        DEFAULT_AUTO_COMPLETE_DAYS,
        DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        DELIVERY_SWEEP_MAX_BATCHES,
    )
    .await
    .expect("complete sweep runs")
}

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

    // Config-default DELIVERY_ASSUME_DAYS after shipment the order is due.
    app.clock
        .advance_seconds(DEFAULT_DELIVERY_ASSUME_DAYS * DAY_SECONDS);
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
    let completed = complete_due(&app.pool, app.clock.now()).await;
    assert_eq!(completed, 0);

    app.clock
        .advance_seconds(DEFAULT_AUTO_COMPLETE_DAYS * DAY_SECONDS);
    let completed = complete_due(&app.pool, app.clock.now()).await;
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
    let assumed = assume_due(&app.pool, app.clock.now()).await;
    let completed = complete_due(&app.pool, app.clock.now()).await;
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
    app.clock
        .advance_seconds(DEFAULT_DELIVERY_ASSUME_DAYS * DAY_SECONDS);

    // Two racing passes claim the same due order: SKIP LOCKED plus the
    // state compare-and-swap let exactly one apply the effect.
    let now = app.clock.now();
    let (first, second) = tokio::join!(
        assume_due_deliveries(
            &app.pool,
            now,
            DEFAULT_DELIVERY_ASSUME_DAYS,
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            DELIVERY_SWEEP_MAX_BATCHES
        ),
        assume_due_deliveries(
            &app.pool,
            now,
            DEFAULT_DELIVERY_ASSUME_DAYS,
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            DELIVERY_SWEEP_MAX_BATCHES
        )
    );
    assert_eq!(
        first.expect("first pass") + second.expect("second pass"),
        1,
        "exactly one claimant transitions the order"
    );

    // A sequential re-run is a no-op: no extra event, no extra revision.
    let events_before = count(&app.pool, "SELECT COUNT(*) FROM events").await;
    let repeated = assume_due(&app.pool, app.clock.now()).await;
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
    app.clock
        .advance_seconds(DEFAULT_AUTO_COMPLETE_DAYS * DAY_SECONDS);
    let now = app.clock.now();
    let (first, second) = tokio::join!(
        complete_due_delivered_orders(
            &app.pool,
            now,
            DEFAULT_AUTO_COMPLETE_DAYS,
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            DELIVERY_SWEEP_MAX_BATCHES
        ),
        complete_due_delivered_orders(
            &app.pool,
            now,
            DEFAULT_AUTO_COMPLETE_DAYS,
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            DELIVERY_SWEEP_MAX_BATCHES
        )
    );
    assert_eq!(first.expect("first pass") + second.expect("second pass"), 1);
    let repeated = complete_due(&app.pool, app.clock.now()).await;
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

// N+1 due orders with batch_size N are drained across successive passes
// (max_batches = 1 so each call is one inner claim).
#[sqlx::test]
async fn delivery_sweep_processes_a_batch_plus_one_across_passes(pool: PgPool) {
    let app = test_app(pool).await;
    for i in 0..3 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let order = create_paid_order(&app, &seller, &buyer).await;
        ship(&app, &seller.token, &order.order_id, 1_600 + i).await;
    }
    app.clock
        .advance_seconds(DEFAULT_DELIVERY_ASSUME_DAYS * DAY_SECONDS);
    let now = app.clock.now();
    let batch_size = 2i64;
    let first = assume_due_deliveries(&app.pool, now, DEFAULT_DELIVERY_ASSUME_DAYS, batch_size, 1)
        .await
        .expect("first pass");
    assert_eq!(first, 2, "first pass claims one batch");
    let remaining_shipped = count(
        &app.pool,
        "SELECT COUNT(*) FROM orders WHERE state = 'shipped'",
    )
    .await;
    assert_eq!(remaining_shipped, 1);
    let second = assume_due_deliveries(&app.pool, now, DEFAULT_DELIVERY_ASSUME_DAYS, batch_size, 1)
        .await
        .expect("second pass");
    assert_eq!(second, 1, "remainder processed on the next pass");
    let delivered = count(
        &app.pool,
        "SELECT COUNT(*) FROM orders WHERE state = 'delivered'",
    )
    .await;
    assert_eq!(delivered, 3);
}

// A malformed shipment timestamp is skipped (logged by order id) and does
// not abort valid due rows in the same claim.
#[sqlx::test]
async fn malformed_shipment_timestamp_is_skipped_and_valid_rows_still_transition(pool: PgPool) {
    let app = test_app(pool).await;
    let seller_ok = new_actor(&app).await;
    let buyer_ok = new_actor(&app).await;
    let ok = create_paid_order(&app, &seller_ok, &buyer_ok).await;
    ship(&app, &seller_ok.token, &ok.order_id, 1_610).await;

    let seller_poison = new_actor(&app).await;
    let buyer_poison = new_actor(&app).await;
    let poison = create_paid_order(&app, &seller_poison, &buyer_poison).await;
    ship(&app, &seller_poison.token, &poison.order_id, 1_611).await;

    let seller_ok2 = new_actor(&app).await;
    let buyer_ok2 = new_actor(&app).await;
    let ok2 = create_paid_order(&app, &seller_ok2, &buyer_ok2).await;
    ship(&app, &seller_ok2.token, &ok2.order_id, 1_612).await;

    sqlx::query(
        "UPDATE orders SET shipment = jsonb_set(shipment, '{shipped_at}', \
         to_jsonb('not-a-timestamp'::text)) WHERE id = $1::uuid",
    )
    .bind(&poison.order_id)
    .execute(&app.pool)
    .await
    .expect("poison shipment");

    app.clock
        .advance_seconds(DEFAULT_DELIVERY_ASSUME_DAYS * DAY_SECONDS);
    let assumed = assume_due(&app.pool, app.clock.now()).await;
    assert_eq!(assumed, 2, "valid rows still assume delivery");

    let (ok_state, ok_flag, _) = order_state(&app.pool, &ok.order_id).await;
    let (ok2_state, ok2_flag, _) = order_state(&app.pool, &ok2.order_id).await;
    let (poison_state, poison_flag, _) = order_state(&app.pool, &poison.order_id).await;
    assert_eq!((ok_state.as_str(), ok_flag), ("delivered", true));
    assert_eq!((ok2_state.as_str(), ok2_flag), ("delivered", true));
    assert_eq!(
        (poison_state.as_str(), poison_flag),
        ("shipped", false),
        "poison row stays shipped"
    );
}
