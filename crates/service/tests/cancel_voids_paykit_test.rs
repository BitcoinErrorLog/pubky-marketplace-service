//! A buyer's cancel of an unpaid Paykit-rail order ends the request: a
//! `preparing` request is voided in the cancel transaction and can never be
//! activated afterwards; an activation that already committed at paykit is
//! tracked so money reaching it takes the late-money fork; once money is
//! observed the buyer can no longer cancel. The races run against the
//! contract-verbatim local double, with its activate held open while the
//! cancel commits.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{enable_bitcoin, poll_now, status_confirmed, status_detected};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payments::order_reference;
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

fn order_uuid(order_id: &str) -> Uuid {
    Uuid::parse_str(order_id).expect("order id is a uuid")
}

async fn listing_revision(pool: &PgPool, seller_pubky: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller_pubky))
        .fetch_one(pool)
        .await
        .expect("listing revision")
}

async fn listing_available(pool: &PgPool, seller_pubky: &str) -> i64 {
    sqlx::query_scalar("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller_pubky))
        .fetch_one(pool)
        .await
        .expect("listing quantity")
}

async fn checkout_one(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] =
        json!(listing_revision(&app.pool, &seller.pubky).await);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string()
}

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    body
}

/// One SAT unit listed, checked out and bound to bitcoin: the order is
/// `preparing` with its one undelivered `paykit.activate` row.
async fn preparing_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, Uuid) {
    enable_bitcoin(app, paykit, seller).await;
    let (status, body) = execute(app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let order_id = checkout_one(app, seller, buyer).await;
    bind_bitcoin(app, &buyer.token, &order_id).await;
    let (invoice_id,): (Uuid,) =
        sqlx::query_as("SELECT paykit_invoice_id FROM orders WHERE id = $1")
            .bind(order_uuid(&order_id))
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    (order_id, invoice_id)
}

async fn cancel(app: &TestApp, buyer: &TestActor, order_id: &str) -> (StatusCode, Value) {
    let (revision,): (i64,) = sqlx::query_as("SELECT revision FROM orders WHERE id = $1")
        .bind(order_uuid(order_id))
        .fetch_one(&app.pool)
        .await
        .expect("order row exists");
    let command = order_command(
        "order.cancel_request",
        order_id,
        revision,
        json!({ "reason": "Changed mind" }),
        (Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64,
    );
    execute(app, &buyer.token, &command).await
}

async fn drain(app: &TestApp) -> u64 {
    let paykit = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit, app.clock.now(), 30)
        .await
        .expect("drain runs")
}

/// `(order state, payment state, activation state, request state)`.
async fn facts(pool: &PgPool, order_id: &str) -> (String, String, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT o.state, p.state, o.paykit_activation_state, o.paykit_request_state \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order facts")
}

fn calls_to(paykit: &FakePaykit, suffix: &str) -> Vec<FakePaykitCall> {
    paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with(suffix))
        .collect()
}

async fn undelivered(pool: &PgPool, kind: &str) -> i64 {
    count(
        pool,
        &format!("SELECT COUNT(*) FROM outbox WHERE kind = '{kind}' AND delivered_at IS NULL"),
    )
    .await
}

// Row 2 — the production bug (order c7e700de): bind, cancel, then the
// outbox pass. The request must never be activated.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_before_activation_voids_the_prepared_request(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    assert_eq!(
        listing_available(&pool, &seller.pubky).await,
        0,
        "bind holds the unit"
    );

    let (status, body) = cancel(&app, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));
    assert_eq!(body["result"]["order"]["stock_held"], json!(false));
    assert_eq!(body["result"]["order"]["paykit_request_state"], Value::Null);
    assert_eq!(listing_available(&pool, &seller.pubky).await, 1);
    assert_eq!(undelivered(&pool, "paykit.activate").await, 0);
    assert_eq!(undelivered(&pool, "paykit.void").await, 1);

    drain(&app).await;
    assert!(
        calls_to(&paykit, "/activate").is_empty(),
        "no activate reached paykit"
    );
    let voids = calls_to(&paykit, "/void");
    assert_eq!(voids.len(), 1);
    assert_eq!(voids[0].body["reason"], json!("order_cancelled"));
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "void_cancelled"
    );
    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("voided".to_string()),
            None
        )
    );
    assert_eq!(undelivered(&pool, "paykit.void").await, 0);

    // A stale copy of the activate row (a lost delivery mark) is refused.
    sqlx::query(
        "UPDATE outbox SET delivered_at = NULL, lease_until = NULL WHERE kind = 'paykit.activate'",
    )
    .execute(&pool)
    .await
    .expect("mark reset");
    drain(&app).await;
    assert!(
        calls_to(&paykit, "/activate").is_empty(),
        "still no activate"
    );
    assert_eq!(
        facts(&pool, &order_id).await.3,
        None,
        "the request never goes live"
    );
}

// Row 3a — the activate is in flight when the buyer cancels, and paykit
// applies the void first: the late activate is refused.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_during_activation_voids_first_and_refuses_the_late_activate(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    let gate = paykit.gate_activate(invoice_id, false);

    let state = app.state.clone();
    let now = app.clock.now();
    let in_flight = tokio::spawn(async move {
        let paykit = state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        drain_outbox(&state.pool, paykit, now, 30).await
    });
    gate.entered.notified().await;

    let (status, body) = cancel(&app, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    drain(&app).await;
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "void_cancelled",
        "the void reached paykit while the activate was held"
    );

    gate.release.notify_one();
    in_flight.await.expect("drain task").expect("drain runs");
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "void_cancelled",
        "the late activate was refused"
    );
    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("voided".to_string()),
            None
        )
    );
    assert_eq!(listing_available(&pool, &seller.pubky).await, 1);
    assert_eq!(undelivered(&pool, "paykit.activate").await, 0);
    assert_eq!(undelivered(&pool, "paykit.void").await, 0);
}

// Row 3b + row 10 + row 7 — paykit committed the activate before the void
// landed (its response was still in flight when the cancel committed). The
// invoice is live, so the order tracks it again, the refused void changes
// nothing, and money reaching it completes the order through the
// late-money fork while the unit is free.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_racing_a_committed_activation_keeps_the_invoice_observed(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    let gate = paykit.gate_activate(invoice_id, true);

    let state = app.state.clone();
    let now = app.clock.now();
    let in_flight = tokio::spawn(async move {
        let paykit = state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        drain_outbox(&state.pool, paykit, now, 30).await
    });
    gate.entered.notified().await;
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "observing"
    );

    let (status, body) = cancel(&app, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    gate.release.notify_one();
    in_flight.await.expect("drain task").expect("drain runs");
    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("active".to_string()),
            Some("pending".to_string())
        ),
        "the live invoice is tracked on the cancelled order"
    );

    // The void row meets a published invoice: refused, the tracking stays.
    drain(&app).await;
    assert_eq!(calls_to(&paykit, "/void").len(), 1);
    assert_eq!(undelivered(&pool, "paykit.void").await, 0);
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "observing"
    );
    assert_eq!(facts(&pool, &order_id).await.3.as_deref(), Some("pending"));

    // Money arrives anyway: the #50 fork completes the order (unit free).
    let reference = order_reference(order_uuid(&order_id));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, payment_state, ..) = facts(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    assert_eq!(payment_state, "confirmed");
    assert_eq!(listing_available(&pool, &seller.pubky).await, 0);
}

// Row 4 + row 7 — the request was already published when the buyer
// cancels: no paykit call, the payment ends, the tail keeps observing, and
// money that arrives after a second buyer took the unit is refund-required.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_after_activation_expires_the_payment_and_keeps_tail_observation(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    drain(&app).await;
    assert_eq!(
        facts(&pool, &order_id).await.3.as_deref(),
        Some("pending"),
        "the request is live"
    );
    let calls_before = paykit.calls().len();

    let (status, body) = cancel(&app, &buyer, &order_id).await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("active".to_string()),
            Some("pending".to_string())
        )
    );
    assert_eq!(undelivered(&pool, "paykit.void").await, 0);
    drain(&app).await;
    assert_eq!(
        paykit.calls().len(),
        calls_before,
        "cancel sends paykit nothing"
    );
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "observing"
    );
    assert_eq!(listing_available(&pool, &seller.pubky).await, 1);

    // A second buyer takes the released unit.
    let second = new_actor(&app).await;
    let second_order = checkout_one(&app, &seller, &second).await;
    bind_bitcoin(&app, &second.token, &second_order).await;
    assert_eq!(listing_available(&pool, &seller.pubky).await, 0);

    let reference = order_reference(order_uuid(&order_id));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, payment_state, reason): (String, String, Option<String>) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("order facts");
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(reason.as_deref(), Some("refund_required"));
}

// Rows 5 and 6 — money observed on the request: the buyer can no longer
// cancel and the unit stays held.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_is_refused_once_bitcoin_money_is_observed(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    drain(&app).await;
    let reference = order_reference(order_uuid(&order_id));
    paykit.set_status(&reference, status_detected("exclusive", 0));
    poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await;
    assert_eq!(facts(&pool, &order_id).await.3.as_deref(), Some("detected"));

    for request_state in ["detected", "confirmed"] {
        sqlx::query("UPDATE orders SET paykit_request_state = $2 WHERE id = $1")
            .bind(order_uuid(&order_id))
            .bind(request_state)
            .execute(&pool)
            .await
            .expect("request state set");
        let (status, body) = cancel(&app, &buyer, &order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{request_state}: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
        assert_eq!(
            body["error"]["message"],
            json!("This order can no longer be cancelled.")
        );
        let (order_state, payment_state, ..) = facts(&pool, &order_id).await;
        assert_eq!(order_state, "pending_payment");
        assert_eq!(payment_state, "awaiting_entitlement");
        assert_eq!(listing_available(&pool, &seller.pubky).await, 0);
    }

    // A shared-address seller's detection enters the seller-confirmation
    // window; the buyer cannot cancel out of it either.
    let shared_seller = new_actor(&app).await;
    let shared_buyer = new_actor(&app).await;
    paykit.set_allocation_mode("shared_manual");
    let (shared_order, _) = preparing_order(&app, &paykit, &shared_seller, &shared_buyer).await;
    drain(&app).await;
    let reference = order_reference(order_uuid(&shared_order));
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    assert_eq!(poll_now(&app, app.clock.now()).await, 1);
    assert_eq!(
        facts(&pool, &shared_order).await.3.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    let (status, body) = cancel(&app, &shared_buyer, &shared_order).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(facts(&pool, &shared_order).await.0, "pending_payment");
    assert_eq!(listing_available(&pool, &shared_seller.pubky).await, 0);
}

// Row 8 — an order that left `pending_payment` while its request was still
// `preparing` (a cancel from before cancel voided): the activation arm
// voids instead of publishing.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn activation_arm_refuses_an_order_that_left_pending_payment(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) = preparing_order(&app, &paykit, &seller, &buyer).await;
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1")
        .bind(order_uuid(&order_id))
        .execute(&pool)
        .await
        .expect("legacy cancel");

    drain(&app).await;
    assert!(
        calls_to(&paykit, "/activate").is_empty(),
        "no activate reached paykit"
    );
    let (_, _, activation, request) = facts(&pool, &order_id).await;
    assert_eq!(activation.as_deref(), Some("voided"));
    assert_eq!(request, None);
    assert_eq!(undelivered(&pool, "paykit.activate").await, 0);

    drain(&app).await;
    let voids = calls_to(&paykit, "/void");
    assert_eq!(voids.len(), 1);
    assert_eq!(voids[0].body["reason"], json!("order_not_payable"));
    assert_eq!(
        paykit.invoice(invoice_id).expect("invoice").state,
        "void_cancelled"
    );
}
