//! W1.15 `shared_manual` checkout (design §B.8.8 r13): observation entry
//! into `awaiting_seller_confirmation` with the 24-hour hold extension,
//! status-only polling that never auto-pays, the seller confirm endpoint
//! (authorisation before idempotency, observation-derived audit facts, one
//! atomic commit), the 24-hour seller-window reaper, the confirm/reaper
//! race with exactly one winner, and the two-business-day SLA alert.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::*;
use marketplace_service::bitcoin_review::{
    add_business_days, condition_seven_clear, route_due_seller_confirmation_windows,
    watch_manual_reviews, SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::order_reference;
use marketplace_service::workers::{
    drain_outbox, expire_due_payment_windows, verify_due_paykit_payments,
};
use serde_json::{json, Value};
use sqlx::{Acquire, PgPool};
use uuid::Uuid;

const NONCE_SATS: i64 = 437;
const TOTAL_SATS: i64 = 51_200 + NONCE_SATS;
const OBSERVED_TXID: &str = "9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";

/// Verbatim live producer capture from the W1.14 e2e test
/// `funded_during_tail_is_late_settlement_and_never_a_settlement`
/// (paykit-server-e2e `tests/expiry.rs`): the exact signed
/// `paykit.bitcoin_status/v2` status body the REAL paykit-server status
/// endpoint served for an exclusive creator whose exact settlement landed
/// in the late tail. The producer emitted NO `observed_sats`/`txid`.
const LIVE_EXCLUSIVE_LATE_STATUS: &str = r#"{"allocation_mode":"exclusive","amount_matched":true,"confirmations":6,"contract_version":"paykit.bitcoin_status/v2","late_settlement":true,"status":"confirmed"}"#;
/// Verbatim live producer capture from the W1.14 e2e test
/// `late_settlement_holds_for_a_shared_manual_creator_with_exact_amount`
/// (paykit-server-e2e `tests/expiry.rs`): the same late-tail shape for a
/// shared_manual creator.
const LIVE_SHARED_MANUAL_LATE_STATUS: &str = r#"{"allocation_mode":"shared_manual","amount_matched":true,"confirmations":6,"contract_version":"paykit.bitcoin_status/v2","late_settlement":true,"status":"confirmed"}"#;

fn captured_late_status(payload: &str) -> Value {
    serde_json::from_str(payload).expect("captured live producer payload")
}

fn status_detected(mode: &str, confirmations: u32) -> Value {
    bitcoin_status_v2(
        "detected",
        true,
        mode,
        Some(OBSERVED_TXID),
        Some(TOTAL_SATS as u64),
        Some(confirmations),
    )
}

fn status_confirmed(mode: &str, amount_matched: bool, confirmations: u32) -> Value {
    bitcoin_status_v2(
        "confirmed",
        amount_matched,
        mode,
        Some(OBSERVED_TXID),
        Some(TOTAL_SATS as u64),
        Some(confirmations),
    )
}

/// One verification pass through the REAL signed status client against the
/// local paykit double — the production transport/consumer seam, not a
/// mock of it.
async fn poll_now(app: &TestApp, now: DateTime<Utc>) -> u64 {
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client configured");
    verify_due_paykit_payments(&app.state, client, now)
        .await
        .expect("poll runs")
}

async fn enable_bitcoin(app: &TestApp, paykit: &FakePaykit, seller: &TestActor) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    paykit.set_claimed(&seller.pubky);
}

async fn create_sat_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let (status, body) =
        execute(app, &seller.token, &register_sat_command(&seller.pubky, 16)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id present")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id present")
            .to_string(),
    }
}

/// Binds bitcoin on a `shared_manual` stack and activates, leaving the
/// order `pending` and pollable. Returns (order_id, payment_id, reference).
async fn bound_shared_manual_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String, String) {
    paykit.set_allocation_mode("shared_manual");
    enable_bitcoin(app, paykit, seller).await;
    let order = create_sat_order(app, seller, buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    let delivered = drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    assert!(delivered >= 1, "the activate row delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    (order.order_id, order.payment_id, reference)
}

async fn bound_exclusive_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String, String) {
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(app, paykit, seller).await;
    let order = create_sat_order(app, seller, buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    let delivered = drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    assert!(delivered >= 1, "the activate row delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    (order.order_id, order.payment_id, reference)
}

/// Counts the `kind` events recorded against this order's payment
/// aggregate (the exactly-once ledger for paid/manual-review effects).
async fn payment_event_count(pool: &PgPool, order_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p \
         ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = $2",
    )
    .bind(Uuid::parse_str(order_id).unwrap())
    .bind(kind)
    .fetch_one(pool)
    .await
    .expect("event count")
}

/// The frozen late observation on the order preserves every fact the
/// producer reported: the state, the amount judgement, and the
/// confirmation depth; facts the producer did NOT emit stay null.
async fn assert_frozen_late_observation(pool: &PgPool, order_id: &str) {
    let observation_doc: Value =
        sqlx::query_scalar("SELECT paykit_observation FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(order_id).unwrap())
            .fetch_one(pool)
            .await
            .expect("observation frozen");
    assert_eq!(observation_doc["state"], json!("confirmed"));
    assert_eq!(observation_doc["amount_matched"], json!(true));
    assert_eq!(observation_doc["confirmations"], json!(6));
    assert_eq!(observation_doc["txid"], Value::Null);
    assert_eq!(observation_doc["observed_sats"], Value::Null);
    assert_eq!(observation_doc["disappeared"], json!(false));
}

/// Asserts the durable late manual-review terminal shape: the payment is
/// `manual_review` with the entry stamped, NO paid/receipt/fulfilment
/// effect landed, no seller-confirmation entry exists, and exactly one
/// `payment.manual_review` event was recorded.
async fn assert_late_manual_review(pool: &PgPool, order_id: &str, entered_at: DateTime<Utc>) {
    let (request_state, payment_state, _, _) = order_facts(pool, order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(
        payment_state, "manual_review",
        "never auto-pays a late settlement"
    );
    let stamped: DateTime<Utc> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(order_id).unwrap())
            .fetch_one(pool)
            .await
            .expect("manual review entry stamped");
    assert_eq!(stamped, entered_at, "the entry stamps the poll clock");
    assert_eq!(count(pool, "SELECT COUNT(*) FROM receipts").await, 0);
    assert_eq!(
        payment_event_count(pool, order_id, "payment.confirmed").await,
        0,
        "no paid event"
    );
    assert_eq!(
        payment_event_count(pool, order_id, "payment.manual_review").await,
        1,
        "exactly one manual-review event"
    );
    assert_eq!(
        count(pool, "SELECT COUNT(*) FROM paykit_seller_confirmations").await,
        0,
        "no seller-confirmation audit"
    );
    assert_eq!(
        count(pool, "SELECT COUNT(*) FROM paykit_resolve_outbox").await,
        0,
        "no resolution row"
    );
    let seller_window: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT paykit_seller_confirmation_entered_at FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).unwrap())
    .fetch_one(pool)
    .await
    .expect("seller window column");
    assert_eq!(
        seller_window, None,
        "never enters awaiting_seller_confirmation"
    );
    assert_frozen_late_observation(pool, order_id).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn captured_late_exclusive_status_enters_manual_review_without_paid_effects(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_exclusive_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    paykit.set_status(&reference, captured_late_status(LIVE_EXCLUSIVE_LATE_STATUS));
    let applied = poll_now(&app, now).await;
    assert_eq!(applied, 1);

    assert_late_manual_review(&pool, &order_id, now).await;
    let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    assert_ne!(order_state, "paid", "the order never pays");

    // Replay/restart: the same late observation polled again lands nothing
    // twice — one manual-review event, still no paid effect.
    let applied = poll_now(&app, now + chrono::Duration::seconds(60)).await;
    assert_eq!(applied, 0, "a replay has nothing left to apply");
    assert_eq!(
        payment_event_count(&pool, &order_id, "payment.manual_review").await,
        1
    );
    let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "manual_review");
}

#[sqlx::test(migrations = "./migrations")]
async fn captured_late_shared_manual_status_enters_manual_review_without_seller_confirmation(
    pool: PgPool,
) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    paykit.set_status(
        &reference,
        captured_late_status(LIVE_SHARED_MANUAL_LATE_STATUS),
    );
    let applied = poll_now(&app, now).await;
    assert_eq!(applied, 1);

    assert_late_manual_review(&pool, &order_id, now).await;

    // Replay/restart: exactly-once.
    let applied = poll_now(&app, now + chrono::Duration::seconds(60)).await;
    assert_eq!(applied, 0, "a replay has nothing left to apply");
    assert_eq!(
        payment_event_count(&pool, &order_id, "payment.manual_review").await,
        1
    );
    let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "manual_review");
}

/// The mandatory `late_settlement` member fails CLOSED (the producer's
/// strict v2 contract): absent, null, a string, or a number is never an
/// automatic transition input — the poll applies nothing and the order
/// keeps its pre-poll shape. Driven through the REAL signed status client
/// and the worker, never `PaykitStatusOutcome` directly.
#[sqlx::test(migrations = "./migrations")]
async fn a_missing_or_malformed_late_settlement_fails_closed(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    for (label, malformed) in [
        ("absent", None),
        ("null", Some(Value::Null)),
        ("string", Some(json!("true"))),
        ("number", Some(json!(1))),
    ] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_exclusive_order(&app, &paykit, &seller, &buyer).await;
        // Start from a valid strict-v2 body, then remove the member (the
        // absent case) or replace it with the wrong-typed value.
        let mut body = bitcoin_status_v2("confirmed", true, "exclusive", None, None, Some(6));
        body.as_object_mut().unwrap().remove("late_settlement");
        if let Some(value) = malformed {
            body["late_settlement"] = value;
        }
        paykit.set_status(&reference, body);
        let applied = poll_now(&app, app.clock.now()).await;
        let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
        assert_eq!(applied, 0, "{label}: nothing applies");
        assert_eq!(request_state, "pending", "{label}: no entry");
        assert_eq!(
            payment_state, "awaiting_entitlement",
            "{label}: a missing/malformed late_settlement NEVER pays"
        );
        assert_eq!(
            payment_event_count(&pool, &order_id, "payment.manual_review").await,
            0,
            "{label}: no manual-review entry either — the poll is Unavailable"
        );
    }
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);
}

/// A late DETECTION (or a late undetected report) is fail-safe: it never
/// enters `awaiting_seller_confirmation` and never pays — at most the
/// display state reflects the on-chain fact.
#[sqlx::test(migrations = "./migrations")]
async fn late_detected_and_undetected_never_take_authority(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // shared_manual: a late detection must NOT arm the seller window.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    let mut late_detected = status_detected("shared_manual", 0);
    late_detected["late_settlement"] = json!(true);
    paykit.set_status(&reference, late_detected);
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 0, "a late detection is no authority transition");
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_ne!(
        request_state, "awaiting_seller_confirmation",
        "a late detection never enters the seller-confirmation state"
    );
    assert_eq!(payment_state, "awaiting_entitlement");
    let seller_window: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT paykit_seller_confirmation_entered_at FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window column");
    assert_eq!(seller_window, None);

    // A late undetected report: nothing changes at all.
    let mut late_undetected =
        bitcoin_status_v2("undetected", false, "shared_manual", None, None, None);
    late_undetected["late_settlement"] = json!(true);
    paykit.set_status(&reference, late_undetected);
    let applied = poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await;
    assert_eq!(applied, 0);
    let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "awaiting_entitlement");

    // exclusive: a late detection is display-only, never a payment.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_exclusive_order(&app, &paykit, &seller, &buyer).await;
    let mut late_detected = status_detected("exclusive", 1);
    late_detected["late_settlement"] = json!(true);
    paykit.set_status(&reference, late_detected);
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 0);
    let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "awaiting_entitlement");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);
}

/// A terminal paid order NEVER un-pays: after an ordinary exact exclusive
/// confirmation (`late_settlement=false`), a late-settlement report has no
/// purchase on the order — it stays paid with its one receipt.
#[sqlx::test(migrations = "./migrations")]
async fn a_late_report_never_unpays_a_terminal_paid_order(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_exclusive_order(&app, &paykit, &seller, &buyer).await;

    // The ordinary exact exclusive auto-confirm (late=false) still pays.
    paykit.set_status(&reference, status_confirmed("exclusive", true, 6));
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 1);
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "confirmed");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);

    // The producer's late-tail report arrives after the fact: nothing
    // changes — no un-pay, no manual review, no second effect.
    paykit.set_status(&reference, captured_late_status(LIVE_EXCLUSIVE_LATE_STATUS));
    let applied = poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await;
    assert_eq!(applied, 0, "a terminal paid order is not re-applied");
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "confirmed", "never un-pays");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    assert_eq!(
        payment_event_count(&pool, &order_id, "payment.manual_review").await,
        0
    );
}

async fn order_facts(pool: &PgPool, order_id: &str) -> (String, String, bool, Option<String>) {
    let (request_state, payment_state, stock_held, _): (Option<String>, String, bool, i64) =
        sqlx::query_as(
            "SELECT o.paykit_request_state, p.state, o.stock_held, 1::bigint \
             FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
        )
        .bind(Uuid::parse_str(order_id).unwrap())
        .fetch_one(pool)
        .await
        .expect("order/payment row");
    let hold: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT hold_expires_at FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(order_id).unwrap())
            .fetch_one(pool)
            .await
            .expect("hold");
    (
        request_state.unwrap_or_default(),
        payment_state,
        stock_held,
        hold.map(|h| h.to_rfc3339()),
    )
}

async fn confirm_call(
    app: &TestApp,
    token: &str,
    order_id: &str,
    body: &Value,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/confirm-bitcoin-payment"),
        Some(token),
        body,
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn a_matching_observation_enters_awaiting_seller_confirmation(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    // A 0-conf detection enters deliberately: the confirmations judgement
    // is the seller's (§B.8.8), never a server precondition.
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    let applied = poll_now(&app, now).await;
    assert_eq!(applied, 1, "entry counts as an application");

    let (request_state, payment_state, stock_held, hold) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement", "never auto-pays");
    assert!(stock_held, "the hold is extended, not expired");
    let deadline: DateTime<Utc> =
        sqlx::query_scalar("SELECT paykit_seller_confirmation_deadline FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("deadline recorded");
    let entered: DateTime<Utc> = sqlx::query_scalar(
        "SELECT paykit_seller_confirmation_entered_at FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("entry recorded");
    assert_eq!(entered, now, "the server clock records the entry");
    assert_eq!(
        deadline,
        now + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
        "the 24-hour window is armed from the server clock"
    );
    assert_eq!(
        hold.as_deref(),
        Some(deadline.to_rfc3339().as_str()),
        "the hold extends to the same deadline"
    );
    let observation_doc: Value =
        sqlx::query_scalar("SELECT paykit_observation FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("observation frozen");
    assert_eq!(observation_doc["state"], json!("detected"));
    assert_eq!(observation_doc["observed_sats"], json!(TOTAL_SATS));
    assert_eq!(observation_doc["confirmations"], json!(0));
    assert_eq!(observation_doc["amount_matched"], json!(true));
    assert_eq!(observation_doc["disappeared"], json!(false));

    // Confirmations progress is a status-only refresh: the order never
    // advances on a poll, and the frozen facts update.
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 3));
    let later = now + chrono::Duration::seconds(60);
    let applied = poll_now(&app, later).await;
    assert_eq!(applied, 0, "a refresh is not an application");
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    let observation_doc: Value =
        sqlx::query_scalar("SELECT paykit_observation FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("observation refreshed");
    assert_eq!(
        observation_doc["state"],
        json!("confirmed"),
        "logs: {}",
        captured_logs()
            .lines()
            .filter(|l| l.contains("ERROR"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(observation_doc["confirmations"], json!(3));
    let confirmations: i32 =
        sqlx::query_scalar("SELECT confirmations FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("payment row");
    assert_eq!(confirmations, 3);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM receipts").await,
        0,
        "no receipt without the seller"
    );

    // A disappearance marks the observation; the order neither reverts nor
    // advances. A refreshed observation clears the mark on the next poll.
    paykit.set_status(
        &reference,
        bitcoin_status_v2("undetected", false, "shared_manual", None, None, None),
    );
    poll_now(&app, later + chrono::Duration::seconds(60)).await;
    let observation_doc: Value =
        sqlx::query_scalar("SELECT paykit_observation FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("observation marked");
    assert_eq!(observation_doc["disappeared"], json!(true));
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    paykit.set_status(&reference, status_detected("shared_manual", 1));
    poll_now(&app, later + chrono::Duration::seconds(120)).await;
    let observation_doc: Value =
        sqlx::query_scalar("SELECT paykit_observation FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("observation refreshed again");
    assert_eq!(observation_doc["disappeared"], json!(false));

    // The payment-window sweep must never touch this order: the hold
    // outlives the 3600 s window by design.
    let beyond_window = now + chrono::Duration::seconds(3700);
    let expired = expire_due_payment_windows(&app.state, beyond_window)
        .await
        .expect("sweep runs");
    assert_eq!(expired, 0, "the sweep cannot expire an awaiting order");
    let (_, payment_state, stock_held, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "awaiting_entitlement");
    assert!(stock_held);
}

#[sqlx::test(migrations = "./migrations")]
async fn a_late_observation_goes_straight_to_manual_review(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    // The hold window lapses with no payment: payment expired, order
    // cancelled, stock released — BEFORE any observation arrives.
    let after_window = app.clock.now() + chrono::Duration::seconds(3700);
    let expired = expire_due_payment_windows(&app.state, after_window)
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);
    let (request_state, payment_state, stock_held, _) = order_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "expired");
    assert!(!stock_held);
    assert_eq!(request_state, "pending");

    // A late FIRST observation (the §B.9 tail): detected flips the display
    // state only; confirmed routes straight to manual_review. Neither ever
    // enters awaiting_seller_confirmation — extending a hold on a dead
    // order would hold stock for nothing.
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    poll_now(&app, after_window + chrono::Duration::seconds(60)).await;
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "detected");
    assert_eq!(payment_state, "expired");

    paykit.set_status(&reference, status_confirmed("shared_manual", true, 2));
    let applied = poll_now(&app, after_window + chrono::Duration::seconds(120)).await;
    assert_eq!(applied, 1);
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "manual_review");
    let entered_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("entry stamp");
    assert_eq!(
        entered_at,
        after_window + chrono::Duration::seconds(120),
        "the late entry stamps the same clock the reaper uses"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_late_confirmation_clears_an_active_seller_window(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    assert_eq!(poll_now(&app, now).await, 1);
    paykit.set_status(
        &reference,
        captured_late_status(LIVE_SHARED_MANUAL_LATE_STATUS),
    );
    assert_eq!(poll_now(&app, now + chrono::Duration::seconds(60)).await, 1);

    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "manual_review");
    let (entered, deadline): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT paykit_seller_confirmation_entered_at, \
             paykit_seller_confirmation_deadline FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window columns");
    assert_eq!(entered, None);
    assert_eq!(deadline, None);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn an_exclusive_confirmation_clears_an_active_seller_window(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    assert_eq!(poll_now(&app, now).await, 1);
    paykit.set_allocation_mode("exclusive");
    paykit.set_status(&reference, status_confirmed("exclusive", true, 6));
    assert_eq!(poll_now(&app, now + chrono::Duration::seconds(60)).await, 1);

    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "confirmed");
    let (entered, deadline): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT paykit_seller_confirmation_entered_at, \
             paykit_seller_confirmation_deadline FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window columns");
    assert_eq!(entered, None);
    assert_eq!(deadline, None);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn an_amount_mismatch_clears_an_active_seller_window(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    let now = app.clock.now();
    paykit.set_status(&reference, status_detected("shared_manual", 0));
    assert_eq!(poll_now(&app, now).await, 1);
    paykit.set_allocation_mode("exclusive");
    paykit.set_status(&reference, status_confirmed("exclusive", false, 6));
    assert_eq!(poll_now(&app, now + chrono::Duration::seconds(60)).await, 1);

    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "manual_review");
    let (entered, deadline): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT paykit_seller_confirmation_entered_at, \
             paykit_seller_confirmation_deadline FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window columns");
    assert_eq!(entered, None);
    assert_eq!(deadline, None);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn a_legacy_unpinned_order_cannot_enter_w1_15_resolution(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    sqlx::query(
        "UPDATE orders SET paykit_stack_id = NULL, paykit_stack_endpoint = NULL WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .execute(&pool)
    .await
    .expect("simulate a pre-0022 row without resolution pins");

    paykit.set_status(&reference, status_detected("shared_manual", 0));
    assert_eq!(poll_now(&app, app.clock.now()).await, 0);
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "pending");
    assert_eq!(payment_state, "awaiting_entitlement");

    paykit.set_status(
        &reference,
        captured_late_status(LIVE_SHARED_MANUAL_LATE_STATUS),
    );
    assert_eq!(
        poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await,
        0
    );
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "pending");
    assert_eq!(payment_state, "awaiting_entitlement");
}

#[sqlx::test(migrations = "./migrations")]
async fn seller_confirm_pays_with_observation_derived_audit(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    let now = app.clock.now();
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 2));
    poll_now(&app, now).await;

    let (status, body) = confirm_call(
        &app,
        &seller.token,
        &order_id,
        &json!({"reason": "checked in my wallet"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirm failed: {body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    let confirmation = &body["confirmation"];
    assert_eq!(confirmation["confirmed_by_pubky"], json!(seller.pubky));
    assert_eq!(confirmation["confirmation_source"], json!("seller"));
    assert_eq!(
        confirmation["confirmation_basis"],
        json!("seller_attestation")
    );
    assert_eq!(
        confirmation["confirmed_txid"],
        json!(OBSERVED_TXID),
        "the txid derives from the stored observation"
    );
    assert_eq!(
        confirmation["confirmed_amount_sats"],
        json!(TOTAL_SATS),
        "the amount derives from the stored observation"
    );
    assert_eq!(
        confirmation["confirmed_reason"],
        json!("checked in my wallet")
    );
    assert_eq!(
        confirmation["paykit_observation"]["confirmations"],
        json!(2),
        "the observation at confirmation is frozen"
    );

    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "confirmed");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_seller_confirmations").await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'receipt.issued'"
        )
        .await,
        1
    );
    // Exactly one pinned resolve outbox row: the issuing stack's identity
    // AND the bind-time endpoint, one-hour delivery deadline.
    let (resolution, stack_id, endpoint, deadline, created): (
        String,
        String,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
    ) = sqlx::query_as(
        "SELECT resolution, stack_id, stack_endpoint, delivery_deadline, created_at \
             FROM paykit_resolve_outbox WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("one resolve row");
    assert_eq!(resolution, "paid_manually");
    assert_eq!(stack_id, paykit.stack_id());
    assert_eq!(endpoint, paykit.base_url);
    assert_eq!(deadline - created, chrono::Duration::seconds(3600));
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_resolve_outbox").await,
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn confirm_authorises_before_idempotency_and_is_idempotent(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    paykit.set_status(&reference, status_detected("shared_manual", 1));
    poll_now(&app, app.clock.now()).await;

    // The buyer, an unrelated seller, and the unauthenticated: 403/401
    // BEFORE any idempotency behaviour, no state change, no audit row.
    for (label, token) in [("buyer", &buyer.token), ("unrelated seller", &other.token)] {
        let (status, body) = confirm_call(&app, token, &order_id, &json!({})).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["reason"], json!("not_order_seller"));
    }
    let (status, _) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/confirm-bitcoin-payment"),
        None,
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_seller_confirmations").await,
        0
    );

    // The seller confirms; the second call replays the same record with no
    // second audit row, no second receipt, no second outbox row.
    let (status, first) = confirm_call(&app, &seller.token, &order_id, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let (status, second) = confirm_call(&app, &seller.token, &order_id, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(
        first["confirmation"]["confirmed_at"], second["confirmation"]["confirmed_at"],
        "the replay returns the recorded confirmation"
    );
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_seller_confirmations").await,
        1
    );
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_resolve_outbox").await,
        1
    );

    // After the confirmation, the buyer still gets 403 — NOT the stored
    // record: authorisation precedes the idempotency lookup by contract.
    let (status, body) = confirm_call(&app, &buyer.token, &order_id, &json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_order_seller"));
}

#[sqlx::test(migrations = "./migrations")]
async fn confirm_wrong_state_and_observation_mismatch(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;

    // No observation yet: the named precondition error, no side effects.
    let (status, body) = confirm_call(&app, &seller.token, &order_id, &json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("order_not_awaiting_confirmation")
    );

    paykit.set_status(&reference, status_confirmed("shared_manual", true, 1));
    poll_now(&app, app.clock.now()).await;

    // A body-supplied txid/amount that disagrees with the observation is
    // rejected with no state change and no audit row; equality is accepted.
    for body in [
        json!({ "txid": format!("{}0", "ab".repeat(16)) }),
        json!({ "confirmed_amount_sats": TOTAL_SATS + 1 }),
    ] {
        let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{response}");
        assert_eq!(
            response["error"]["reason"],
            json!("confirmation_observation_mismatch"),
            "{response}"
        );
    }
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_seller_confirmations").await,
        0
    );
    let (status, response) = confirm_call(
        &app,
        &seller.token,
        &order_id,
        &json!({ "txid": OBSERVED_TXID, "confirmed_amount_sats": TOTAL_SATS }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

#[sqlx::test(migrations = "./migrations")]
async fn the_seller_window_reaper_routes_to_manual_review_preserving_the_hold(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    let now = app.clock.now();
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 1));
    poll_now(&app, now).await;

    // deadline−1s: nothing; deadline: the reaper routes it.
    let before_deadline = now + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS - 1);
    let routed = route_due_seller_confirmation_windows(&app.state, before_deadline)
        .await
        .expect("reaper runs");
    assert_eq!(routed, 0);
    let at_deadline = now + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS);
    let routed = route_due_seller_confirmation_windows(&app.state, at_deadline)
        .await
        .expect("reaper runs");
    assert_eq!(routed, 1);

    let (request_state, payment_state, stock_held, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "manual_review");
    assert!(stock_held, "routing PRESERVES the hold");
    let entered_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("entry stamp");
    assert_eq!(entered_at, at_deadline, "the seven-day clock starts here");
    let reserved: i64 =
        sqlx::query_scalar("SELECT reserved_quantity FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing row");
    assert_eq!(reserved, 1, "the stock stays held through resolution");

    // A second pass is a no-op (the CAS lost; nothing changes).
    let routed = route_due_seller_confirmation_windows(&app.state, at_deadline)
        .await
        .expect("reaper runs");
    assert_eq!(routed, 0);

    // Condition 7 (the exact predicate) is blocked before AND after the
    // window elapses — the window closing never clears it.
    assert!(
        !condition_seven_clear(&pool, &paykit.stack_id())
            .await
            .expect("condition 7 query"),
        "a manual_review order pinned to the stack blocks the drain"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn confirm_and_reaper_race_has_exactly_one_winner(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // Deterministic orderings first: reaper-before-confirm and
    // confirm-before-reaper, each against a fresh order at its deadline.
    for reaper_first in [true, false] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
        let entered_at = app.clock.now();
        paykit.set_status(&reference, status_confirmed("shared_manual", true, 1));
        poll_now(&app, entered_at).await;
        let now = entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS);
        if reaper_first {
            let routed = route_due_seller_confirmation_windows(&app.state, now)
                .await
                .expect("reaper runs");
            assert_eq!(routed, 1, "the reaper is due and wins first");
            let (status, body) = confirm_call(&app, &seller.token, &order_id, &json!({})).await;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(
                body["error"]["reason"],
                json!("order_not_awaiting_confirmation"),
                "the losing confirm gets the named precondition error"
            );
            let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
            assert_eq!(payment_state, "manual_review");
        } else {
            let (status, body) = confirm_call(&app, &seller.token, &order_id, &json!({})).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let routed = route_due_seller_confirmation_windows(&app.state, now)
                .await
                .expect("reaper runs");
            assert_eq!(routed, 0, "the losing reaper changes nothing");
            let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
            assert_eq!(payment_state, "confirmed");
        }
        // Exactly one side's effects landed, whichever won.
        let audits: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_seller_confirmations WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("audit count");
        let manual_review_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events e JOIN payments p \
             ON e.aggregate_id = ('payment:' || p.id::text) \
             WHERE p.order_id = $1 AND e.kind = 'payment.manual_review'",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("event count");
        assert_eq!(
            (audits, manual_review_events),
            if reaper_first { (0, 1) } else { (1, 0) },
        );
    }

    // Overlapping transactions: both due at the deadline, released on a
    // barrier. The invariant — exactly one winner, the loser changing
    // nothing — is asserted per round; the deterministic orderings above
    // prove each direction, so no gate here depends on scheduler luck.
    for round in 0..6u64 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
        let entered_at = app.clock.now();
        paykit.set_status(&reference, status_confirmed("shared_manual", true, 1));
        poll_now(&app, entered_at).await;
        // The race runs exactly at the armed deadline: both the confirm
        // and the reaper are due, in overlapping transactions.
        let now = entered_at
            + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS)
            + chrono::Duration::seconds(round as i64);

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (confirm_router, confirm_token, confirm_order) =
            (app.router.clone(), seller.token.clone(), order_id.clone());
        let confirm_barrier = barrier.clone();
        let confirm_handle = tokio::spawn(async move {
            confirm_barrier.wait().await;
            send(
                confirm_router,
                "POST",
                &format!("/v0/orders/{confirm_order}/confirm-bitcoin-payment"),
                Some(&confirm_token),
                &json!({}),
            )
            .await
        });
        let reaper_state = app.state.clone();
        let reaper_barrier = barrier.clone();
        let reaper_handle = tokio::spawn(async move {
            reaper_barrier.wait().await;
            route_due_seller_confirmation_windows(&reaper_state, now).await
        });
        let ((confirm_status, confirm_body), reaper_result) = (
            confirm_handle.await.expect("confirm task"),
            reaper_handle.await.expect("reaper task"),
        );
        let reaper_routed = reaper_result.expect("reaper runs");

        let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
        let confirm_won = confirm_status == StatusCode::OK;
        let reaper_won = reaper_routed == 1;
        assert_ne!(
            confirm_won, reaper_won,
            "round {round}: exactly one winner (confirm={confirm_won}, reaper={reaper_won}): {confirm_body}"
        );
        if confirm_won {
            assert_eq!(payment_state, "confirmed", "round {round}");
        } else {
            assert_eq!(payment_state, "manual_review", "round {round}");
            assert_eq!(
                confirm_body["error"]["reason"],
                json!("order_not_awaiting_confirmation"),
                "round {round}: the loser gets the named precondition error"
            );
        }
        assert_eq!(request_state, "confirmed", "round {round}");
        // Exactly one audit-relevant fact per order, whichever won.
        let audits: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_seller_confirmations WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("audit count");
        let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM receipts WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("receipt count");
        let outbox: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .expect("outbox count");
        if confirm_won {
            assert_eq!((audits, receipts, outbox), (1, 1, 1), "round {round}");
        } else {
            assert_eq!(
                (audits, receipts, outbox),
                (0, 0, 0),
                "round {round}: the loser writes nothing"
            );
        }
    }
}

/// The F16 FAIL calibration: replace the conditional UPDATE with a
/// read-then-write and the race produces the state the CAS exists to
/// prevent — an order simultaneously `manual_review` and `paid`. Driven as
/// two overlapping real Postgres transactions.
#[sqlx::test(migrations = "./migrations")]
async fn read_then_write_calibration_produces_the_double_state(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 1));
    poll_now(&app, app.clock.now()).await;
    let order_uuid = Uuid::parse_str(&order_id).unwrap();
    let payment_uuid = Uuid::parse_str(&payment_id).unwrap();

    // Two overlapping transactions, each reading the state before writing
    // (the naive pattern the conditional UPDATE replaces): A reads, B
    // reads, A writes and commits, then B — deciding on its STALE read —
    // writes and commits. (Sequential awaits here are deliberate: B's
    // writes would block on A's row locks if A were still open.)
    let mut conn_a = pool.acquire().await.expect("conn a");
    let mut conn_b = pool.acquire().await.expect("conn b");
    let mut tx_a = conn_a.begin().await.expect("tx a");
    let mut tx_b = conn_b.begin().await.expect("tx b");
    let state_a: String =
        sqlx::query_scalar("SELECT paykit_request_state FROM orders WHERE id = $1")
            .bind(order_uuid)
            .fetch_one(&mut *tx_a)
            .await
            .expect("read a");
    let state_b: String =
        sqlx::query_scalar("SELECT paykit_request_state FROM orders WHERE id = $1")
            .bind(order_uuid)
            .fetch_one(&mut *tx_b)
            .await
            .expect("read b");
    assert_eq!(
        (state_a.as_str(), state_b.as_str()),
        (
            "awaiting_seller_confirmation",
            "awaiting_seller_confirmation"
        )
    );
    // A: the naive reaper — unconditional payment move to manual_review.
    sqlx::query("UPDATE payments SET state = 'manual_review', manual_review_entered_at = NOW() WHERE id = $1")
        .bind(payment_uuid)
        .execute(&mut *tx_a)
        .await
        .expect("naive reaper write");
    sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed', \
         paykit_seller_confirmation_entered_at = NULL, paykit_seller_confirmation_deadline = NULL \
         WHERE id = $1",
    )
    .bind(order_uuid)
    .execute(&mut *tx_a)
    .await
    .expect("naive reaper order write");
    tx_a.commit().await.expect("commit a");
    // B: the naive confirm — its read already said "awaiting", so it
    // writes paid effects over the reaper's commit. No predicate: both
    // "transitions" landed.
    sqlx::query(
        "UPDATE payments SET state = 'confirmed', manual_review_entered_at = NULL WHERE id = $1",
    )
    .bind(payment_uuid)
    .execute(&mut *tx_b)
    .await
    .expect("naive confirm write");
    sqlx::query("UPDATE orders SET state = 'paid' WHERE id = $1")
        .bind(order_uuid)
        .execute(&mut *tx_b)
        .await
        .expect("naive confirm order write");
    tx_b.commit().await.expect("commit b");

    let (order_state, payment_state): (String, String) = sqlx::query_as(
        "SELECT o.state, p.state FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid)
    .fetch_one(&pool)
    .await
    .expect("final state");
    // The defect, observed: whichever wrote last on the payment wins the
    // payment state while the order says paid — the simultaneously
    // manual_review/paid shape no reader of either path expects. (The
    // payment CHECK forces manual_review_entered_at to clear with the
    // second write, so the terminal shape is order=paid with the
    // payment's manual-review entry silently overwritten: the race is
    // lossy either way.) The production CAS above never produces it.
    assert_eq!(order_state, "paid");
    assert!(
        payment_state == "confirmed" || payment_state == "manual_review",
        "the naive race leaves the two sides disagreeing"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn seller_response_sla_alerts_once_at_two_business_days(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    paykit.set_status(&reference, status_detected("shared_manual", 1));
    poll_now(&app, app.clock.now()).await;

    // Route on a Friday (2026-09-11 is one): the two-business-day SLA lands
    // on Tuesday, not Sunday.
    let friday = DateTime::parse_from_rfc3339("2026-09-11T10:00:00Z")
        .expect("friday")
        .to_utc();
    let routed = route_due_seller_confirmation_windows(&app.state, friday)
        .await
        .expect("reaper runs");
    assert_eq!(routed, 1);
    let sla_deadline = add_business_days(friday, 2);
    assert_eq!(
        sla_deadline.to_rfc3339(),
        "2026-09-15T10:00:00+00:00",
        "weekends do not count against the seller"
    );

    let (_, abandoned) =
        watch_manual_reviews(&app.state, sla_deadline - chrono::Duration::seconds(1))
            .await
            .expect("watch runs");
    assert_eq!(abandoned, 0);
    let alerted: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT manual_review_sla_alerted_at FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("sla stamp");
    assert!(alerted.is_none(), "no alert before the breach");

    let (alerts, abandoned) = watch_manual_reviews(&app.state, sla_deadline)
        .await
        .expect("watch runs");
    assert_eq!(
        (alerts, abandoned),
        (1, 0),
        "the alert fires; nothing transitions"
    );
    assert!(
        captured_logs().contains("ALERT seller-response SLA breached"),
        "the breach is visible in the logs"
    );
    // Exactly once per entry.
    let (alerts, _) = watch_manual_reviews(&app.state, sla_deadline + chrono::Duration::days(1))
        .await
        .expect("watch runs");
    assert_eq!(alerts, 0, "the alert fires once");
    let (_, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(
        payment_state, "manual_review",
        "the SLA never moves authority"
    );
}

/// The strict status contract fails CLOSED (W1.14 cross-repo P1), proved
/// through the real signed transport/consumer seam: a missing/wrong
/// contract_version, a missing/unknown allocation_mode, or any
/// non-exclusive mode NEVER produces an automatic paid transition; a valid
/// `shared_manual` confirmed observation enters
/// `awaiting_seller_confirmation` and never auto-pays; the CURRENT mode
/// governs even against the bind-time record (§B.11.4 A3).
#[sqlx::test(migrations = "./migrations")]
async fn the_status_contract_fails_closed(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // Every malformed contract shape leaves the order untouched.
    let mut malformed = vec![
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some(OBSERVED_TXID),
            Some(TOTAL_SATS as u64),
            Some(1),
        ),
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some(OBSERVED_TXID),
            Some(TOTAL_SATS as u64),
            Some(1),
        ),
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some(OBSERVED_TXID),
            Some(TOTAL_SATS as u64),
            Some(1),
        ),
        bitcoin_status_v2(
            "confirmed",
            true,
            "pasted_auto",
            Some(OBSERVED_TXID),
            Some(TOTAL_SATS as u64),
            Some(1),
        ),
        bitcoin_status_v2(
            "confirmed",
            true,
            "exclusive",
            Some(OBSERVED_TXID),
            Some(TOTAL_SATS as u64),
            Some(1),
        ),
    ];
    malformed[0]
        .as_object_mut()
        .unwrap()
        .remove("contract_version");
    malformed[1]["contract_version"] = json!("paykit.bitcoin_status/v1");
    malformed[2]
        .as_object_mut()
        .unwrap()
        .remove("allocation_mode");
    malformed[4].as_object_mut().unwrap().remove("status");
    for (index, body) in malformed.into_iter().enumerate() {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
        paykit.set_status(&reference, body);
        let applied = poll_now(&app, app.clock.now()).await;
        let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
        assert_eq!(applied, 0, "case {index}: nothing applies");
        assert_eq!(request_state, "pending", "case {index}: no entry");
        assert_eq!(
            payment_state, "awaiting_entitlement",
            "case {index}: a malformed contract NEVER pays"
        );
    }

    // A valid shared_manual confirmed observation (exact amount, chain
    // confirmed) enters the state and NEVER auto-confirms.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(&app, &paykit, &seller, &buyer).await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 6));
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 1);
    let (request_state, payment_state, _, _) = order_facts(&pool, &order_id).await;
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);

    // §B.11.4 A3: a downgrade lands while the invoice is observing — the
    // bind-time record said `exclusive`, the CURRENT mode says
    // `shared_manual`, so the next matching observation enters
    // awaiting_seller_confirmation instead of auto-paying.
    paykit.set_allocation_mode("exclusive");
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let persisted: String =
        sqlx::query_scalar("SELECT paykit_allocation_mode FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("persisted mode");
    assert_eq!(persisted, "exclusive");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 2));
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 1);
    let (request_state, payment_state, _, _) = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        request_state, "awaiting_seller_confirmation",
        "the current mode governs, not allocation_mode_at_creation"
    );
    assert_eq!(payment_state, "awaiting_entitlement");
}
