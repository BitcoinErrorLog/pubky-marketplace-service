//! W1.15 seller manual-review resolution (design §B.9 r13): the
//! `/v0/orders/{id}/bitcoin/resolve` endpoint across the full
//! entry×outcome×inventory matrix, auth-before-idempotency, the shared
//! seller/reaper CAS, the seven-day inactivity abandonment, cross-rail and
//! missing-pin refusals, the exact condition-7 predicate, and the
//! CAS-removal and drain-interleaving calibrations — all against real
//! Postgres, with races as overlapping real transactions.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::*;
use marketplace_service::bitcoin_review::{
    condition_seven_clear, route_due_seller_confirmation_windows, watch_manual_reviews,
    MANUAL_REVIEW_INACTIVITY_DAYS, SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::order_reference;
use marketplace_service::workers::{
    close_due_auctions, drain_outbox, expire_due_payment_windows, verify_due_locks_lifecycles,
    verify_due_paykit_payments,
};
use serde_json::{json, Value};
use sqlx::{Acquire, PgPool};
use std::str::FromStr;
use uuid::Uuid;

const TOTAL_SATS: i64 = 51_200 + 437;
const OBSERVED_TXID: &str = "9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";

fn status_confirmed(mode: &str, amount_matched: bool) -> Value {
    bitcoin_status_v2(
        "confirmed",
        amount_matched,
        mode,
        Some(OBSERVED_TXID),
        Some(TOTAL_SATS as u64),
        Some(2),
    )
}

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
    // The checkout fixture pins the listing's CURRENT server revision so
    // repeated orders against one listing never go stale.
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
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

/// Binds and activates a bitcoin order on the given allocation mode,
/// returning (order_id, payment_id, reference).
async fn bound_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
    mode: &str,
) -> (String, String, String) {
    paykit.set_allocation_mode(mode);
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
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    (order.order_id, order.payment_id, reference)
}

/// The held entry class: shared_manual observation, entry, then the
/// 24-hour reaper routes the payment to `manual_review` (hold preserved).
async fn into_manual_review_held(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, _payment_id, reference) =
        bound_order(app, paykit, seller, buyer, "shared_manual").await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true));
    let entered_at = app.clock.now();
    poll_now(app, entered_at).await;
    let routed = route_due_seller_confirmation_windows(
        &app.state,
        entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("reaper runs");
    assert_eq!(routed, 1);
    (order_id, reference)
}

/// The late-settlement entry class: the hold window lapses first (order
/// cancelled, stock released), then a confirmed observation routes the
/// expired payment to `manual_review`.
async fn into_manual_review_late(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, _payment_id, reference) =
        bound_order(app, paykit, seller, buyer, "shared_manual").await;
    let after_window = app.clock.now() + chrono::Duration::seconds(3700);
    let expired = expire_due_payment_windows(&app.state, after_window)
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);
    paykit.set_status(&reference, status_confirmed("shared_manual", true));
    let applied = poll_now(app, after_window + chrono::Duration::seconds(60)).await;
    assert_eq!(applied, 1);
    (order_id, reference)
}

/// The amount-mismatch entry class (exclusive rail): a confirmed
/// observation with the wrong amount routes to `manual_review` with the
/// hold still reserved.
async fn into_manual_review_mismatch(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, _payment_id, reference) =
        bound_order(app, paykit, seller, buyer, "exclusive").await;
    paykit.set_status(&reference, status_confirmed("exclusive", false));
    let applied = poll_now(app, app.clock.now()).await;
    assert_eq!(applied, 1);
    (order_id, reference)
}

async fn resolve_call(
    app: &TestApp,
    token: &str,
    order_id: &str,
    key: Option<Uuid>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/v0/orders/{order_id}/bitcoin/resolve"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key.to_string());
    }
    let request = request
        .body(axum::body::Body::from(
            serde_json::to_vec(body).expect("body serializes"),
        ))
        .expect("request builds");
    let response = tower::util::ServiceExt::oneshot(app.router.clone(), request)
        .await
        .expect("request executes");
    let status = response.status();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct PaymentFacts {
    state: String,
    #[sqlx(rename = "resolution_outcome")]
    outcome: Option<String>,
    #[sqlx(rename = "resolution_basis")]
    basis: Option<String>,
    #[sqlx(rename = "resolved_by_pubky")]
    resolved_by: Option<String>,
    refund_reference: Option<String>,
    #[sqlx(rename = "manual_review_entered_at")]
    entered_at: Option<DateTime<Utc>>,
}

async fn payment_facts(pool: &PgPool, order_id: &str) -> PaymentFacts {
    sqlx::query_as(
        "SELECT state, resolution_outcome, resolution_basis, resolved_by_pubky, \
         refund_reference, manual_review_entered_at FROM payments WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(order_id).unwrap())
    .fetch_one(pool)
    .await
    .expect("payment row")
}

async fn order_row_state(pool: &PgPool, order_id: &str) -> (String, bool) {
    sqlx::query_as("SELECT state, stock_held FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(order_id).unwrap())
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn outbox_facts(pool: &PgPool, order_id: &str) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT resolution, stack_id, stack_endpoint FROM paykit_resolve_outbox WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(order_id).unwrap())
    .fetch_all(pool)
    .await
    .expect("outbox rows")
}

// ---------------------------------------------------------------------------
// The entry×outcome matrix, held entry (24-hour seller-window route)
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn held_entry_resolves_paid_refunded_and_abandoned(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // --- paid ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let key = Uuid::new_v4();
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(key),
        &json!({ "outcome": "paid", "reason": "checked my wallet" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["resolution"]["outcome"], json!("paid"));
    assert_eq!(body["resolution"]["basis"], json!("seller_attestation"));
    assert_eq!(body["resolution"]["resolved_by_pubky"], json!(seller.pubky));
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(
        facts,
        PaymentFacts {
            state: "confirmed".to_string(),
            outcome: Some("paid".to_string()),
            basis: Some("seller_attestation".to_string()),
            resolved_by: Some(seller.pubky.clone()),
            refund_reference: None,
            entered_at: None,
        },
        "a resolved paid payment leaves manual_review with its stamp cleared"
    );
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    let outbox = outbox_facts(&pool, &order_id).await;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "paid_manually");
    assert_eq!(outbox[0].1, paykit.stack_id());
    assert_eq!(outbox[0].2, paykit.base_url);
    let resolved_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = 'payment.resolved'",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("event count");
    assert_eq!(
        resolved_events, 0,
        "a paid resolution emits payment.confirmed, not payment.resolved"
    );
    // Condition 7 clears once the resolution commits.
    assert!(
        condition_seven_clear(&pool, &paykit.stack_id())
            .await
            .expect("condition 7 query"),
        "no order can still create a resolution row after resolving"
    );

    // --- refunded ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-12345" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "confirmed");
    assert_eq!(facts.outcome.as_deref(), Some("refunded"));
    assert_eq!(facts.refund_reference.as_deref(), Some("tx-12345"));
    let (order_state, stock_held) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "refunded_external");
    assert!(!stock_held, "the hold released");
    let external_refund: Value =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("refund record");
    assert_eq!(external_refund["transaction_id"], json!("tx-12345"));
    assert_eq!(external_refund["amount_minor"], json!(TOTAL_SATS));
    let outbox = outbox_facts(&pool, &order_id).await;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "refunded");
    let resolved_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = 'payment.resolved'",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("event count");
    assert_eq!(
        resolved_events, 1,
        "a refunded resolution emits payment.resolved, not payment.confirmed"
    );
    let available: i64 =
        sqlx::query_scalar("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing row");
    assert_eq!(available, 16, "the released unit restocked");

    // --- abandoned ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned", "reason": "buyer unreachable" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "expired");
    assert_eq!(facts.outcome.as_deref(), Some("abandoned"));
    let (order_state, stock_held) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "cancelled");
    assert!(!stock_held);
    let outbox = outbox_facts(&pool, &order_id).await;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "abandoned");
}

// ---------------------------------------------------------------------------
// Late-settlement entry: cancelled order, stock released
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn late_entry_resolves_paid_refunded_and_abandoned(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // --- paid: reacquire the released stock, then cancelled -> paid ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "confirmed");
    assert_eq!(facts.outcome.as_deref(), Some("paid"));
    let (order_state, stock_held) = order_row_state(&pool, &order_id).await;
    assert_eq!(
        order_state, "paid",
        "the resolution edge is cancelled -> paid"
    );
    assert!(!stock_held, "consumed by the sale");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    let (available, sold): (i64, i64) = sqlx::query_as(
        "SELECT available_quantity, sold_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{}_boots_01", seller.pubky))
    .fetch_one(&pool)
    .await
    .expect("listing row");
    assert_eq!(
        (available, sold),
        (15, 1),
        "reacquired then sold, never invented"
    );

    // --- refunded: cancelled -> refunded_external, nothing reacquired ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-late-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "refunded_external");
    let (available, sold): (i64, i64) = sqlx::query_as(
        "SELECT available_quantity, sold_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{}_boots_01", seller.pubky))
    .fetch_one(&pool)
    .await
    .expect("listing row");
    assert_eq!(
        (available, sold),
        (16, 0),
        "nothing reacquired for a refund"
    );

    // --- abandoned: the cancelled order stays cancelled; payment expires ---
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "expired");
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "cancelled");
}

#[sqlx::test(migrations = "./migrations")]
async fn late_paid_against_sold_out_stock_is_named_stock_unavailable(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;

    // Another buyer takes the released stock (all 16 units) through real
    // checkouts and sandbox payment confirmations.
    for index in 0..16u64 {
        let other = new_actor(&app).await;
        let revision: i64 =
            sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(format!("listing:{}_boots_01", seller.pubky))
                .fetch_one(&pool)
                .await
                .expect("listing row");
        let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
        checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
        let (status, body) = execute(&app, &other.token, &checkout).await;
        assert_eq!(status, StatusCode::OK, "checkout {index}: {body}");
        let payment_id = body["result"]["payments"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, body) = execute(
            &app,
            &other.token,
            &payment_command(&payment_id, 1, "confirmed", 1, 2_000 + index),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "payment {index}: {body}");
    }
    let available: i64 =
        sqlx::query_scalar("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing row");
    assert_eq!(available, 0, "the stock is gone");

    // The paid resolution must NOT invent inventory: named 409, and
    // nothing about the payment or order changed.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("stock_unavailable"));
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "manual_review");
    assert!(facts.outcome.is_none());
    assert!(
        facts.entered_at.is_some(),
        "the CAS rolled back with the effects"
    );
    // The seller's exit is refunded or abandoned — still available.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-back" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrations = "./migrations")]
async fn mismatch_entry_resolves_paid_with_the_held_stock(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_mismatch(&app, &paykit, &seller, &buyer).await;
    let (_, stock_held) = order_row_state(&pool, &order_id).await;
    assert!(
        stock_held,
        "the mismatch entry holds stock normally reserved"
    );

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "confirmed");
    assert_eq!(facts.outcome.as_deref(), Some("paid"));
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
    // The immutable snapshot is server-stored fact, never the body: the
    // charged amount and the stored observation (the exclusive rail keeps
    // no live observation document, so the payment row is the anchor).
    let snapshot: Value = sqlx::query_scalar(
        "SELECT observed_payment_snapshot FROM paykit_manual_resolutions WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("audit row");
    assert_eq!(snapshot["payment_amount_minor"], json!(TOTAL_SATS));
    assert_eq!(snapshot["paykit_total_sats"], json!(TOTAL_SATS));
}

// ---------------------------------------------------------------------------
// Idempotency: same key+body replays, same key+other body conflicts,
// different key after resolution is already_resolved
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn resolve_idempotency_replay_conflict_and_already_resolved(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;

    let key = Uuid::new_v4();
    let body = json!({ "outcome": "refunded", "reason": "buyer asked", "external_refund_reference": "tx-9" });
    let (status, first) = resolve_call(&app, &seller.token, &order_id, Some(key), &body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{first}; logs: {}",
        captured_logs()
            .lines()
            .filter(|l| l.contains("ERROR"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // Same key + same body: the winner's immutable result, byte-identical,
    // and nothing duplicated.
    let (status, replay) = resolve_call(&app, &seller.token, &order_id, Some(key), &body).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(first, replay, "the stored response replays verbatim");
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_manual_resolutions").await,
        1
    );
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_resolve_outbox").await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.resolved'"
        )
        .await,
        1
    );

    // Authorisation STILL precedes the idempotency lookup after the
    // resolution exists: the buyer and an unrelated seller with the
    // winner's key get 403, never the stored response.
    let other = new_actor(&app).await;
    for (label, token) in [("buyer", &buyer.token), ("unrelated seller", &other.token)] {
        let (status, body) = resolve_call(&app, token, &order_id, Some(key), &body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["reason"], json!("not_order_seller"));
    }

    // Same key + different body: 409 conflict, nothing new written.
    let (status, conflict) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(key),
        &json!({ "outcome": "abandoned" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["reason"], json!("conflict"));

    // Different key after resolution: 409 already_resolved.
    let (status, again) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{again}");
    assert_eq!(again["error"]["reason"], json!("already_resolved"));
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_manual_resolutions").await,
        1
    );
}

// ---------------------------------------------------------------------------
// Authorisation and validation
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn resolve_authorises_before_idempotency_and_validates(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;

    // Buyer and unrelated seller: 403 BEFORE any idempotency behaviour,
    // with a well-formed key and body — nothing is looked up or written.
    for (label, token) in [("buyer", &buyer.token), ("unrelated seller", &other.token)] {
        let (status, body) = resolve_call(
            &app,
            token,
            &order_id,
            Some(Uuid::new_v4()),
            &json!({ "outcome": "paid" }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["reason"], json!("not_order_seller"));
    }
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM paykit_manual_resolutions").await,
        0
    );

    // Missing / malformed Idempotency-Key.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        None,
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("invalid_idempotency_key"));

    // Invalid outcome; refund rules; actor/seller/txid/amount fields are
    // rejected by the body shape (deny_unknown_fields).
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "chargeback" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("invalid_outcome"));
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("invalid_refund_reference"));
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "external_refund_reference": "tx-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("invalid_refund_reference"));
    let (status, _body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "actor": seller.pubky, "txid": "abc", "amount_sats": 1 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a body carrying actor/txid/amount is refused"
    );
}

// ---------------------------------------------------------------------------
// Cross-rail and pin scope
// ---------------------------------------------------------------------------

/// A Locks-correlated payment driven to manual_review through the REAL
/// lifecycle worker (verified completion after the window elapsed).
#[sqlx::test(migrations = "./migrations")]
async fn resolve_refuses_a_locks_manual_review(pool: PgPool) {
    let (app, locks) = test_app_with_locks(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            700,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "locks register failed: {body}");
    // The window lapses, then the completion verifies late: manual_review.
    let after_window = app.clock.now() + chrono::Duration::seconds(3700);
    let expired = expire_due_payment_windows(&app.state, after_window)
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);
    locks.set_outcome(
        TEST_BUNDLE_ID,
        marketplace_service::locks::LocksLookupOutcome::Status(
            marketplace_service::locks::LocksTaskStatus::Completed,
        ),
    );
    let applied = verify_due_locks_lifecycles(
        &app.state,
        app.state.locks.as_ref().expect("locks runtime"),
        after_window + chrono::Duration::seconds(60),
    )
    .await
    .expect("locks verification runs");
    assert_eq!(applied, 1);
    let facts = payment_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "manual_review");

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("resolution_not_applicable"));
}

/// A PayPal payment driven to manual_review through the REAL
/// seller-attested path (confirm-received after the window elapsed).
#[sqlx::test(migrations = "./migrations")]
async fn resolve_refuses_a_paypal_manual_review(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "seller@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    app.clock.advance_seconds(3700);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/confirm-received", order.order_id),
        Some(&seller.token),
        &json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "late attestation routes to review: {body}"
    );
    let facts = payment_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "manual_review");

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("resolution_not_applicable"));
}

/// A Stripe payment driven to manual_review through the REAL processor
/// verification path (a paid session matched after the window elapsed).
#[sqlx::test(migrations = "./migrations")]
async fn resolve_refuses_a_stripe_manual_review(pool: PgPool) {
    let (app, stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let restricted_key = "rk_test_seller_key_1";
    stripe.accept_key(restricted_key);
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({
            "bitcoin_enabled": false,
            "stripe_payment_link": "https://buy.stripe.com/test_link",
            "stripe_restricted_key": restricted_key,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "stripe" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    app.clock.advance_seconds(3700);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);
    let total_minor: i64 = sqlx::query_scalar("SELECT total_minor FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order.order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    stripe.add_session(FakeStripeSession {
        id: "cs_test_late".to_string(),
        client_reference_id: order.order_id.clone(),
        payment_status: "paid".to_string(),
        amount_total: total_minor,
        currency: "usd".to_string(),
    });
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/verify", order.order_id),
        Some(&buyer.token),
        &json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "late verification routes to review: {body}"
    );
    let facts = payment_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "manual_review");

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("resolution_not_applicable"));
}

#[sqlx::test(migrations = "./migrations")]
async fn resolve_refuses_a_missing_pin_and_a_non_review_payment(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;

    // Missing pin: the stack identity is gone — no stack to resolve
    // against, named refusal.
    sqlx::query("UPDATE orders SET paykit_stack_id = NULL WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .execute(&pool)
        .await
        .expect("pin cleared");
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("missing_pin"));
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "manual_review");

    // A payment that never entered manual_review: the named precondition,
    // distinct from already_resolved.
    let (other_order_id, _reference) =
        into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    sqlx::query(
        "UPDATE payments SET state = 'awaiting_entitlement', manual_review_entered_at = NULL          WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(&other_order_id).unwrap())
    .execute(&pool)
    .await
    .expect("payment reset");
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &other_order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_in_manual_review"));
}

// ---------------------------------------------------------------------------
// The seven-day inactivity reaper
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn seven_day_inactivity_records_seller_unresponsive(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let entered_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("entry stamp");

    // deadline−1s: nothing. deadline: the abandoned branch, shared CAS.
    let before = entered_at + chrono::Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS)
        - chrono::Duration::seconds(1);
    let (_, abandoned) = watch_manual_reviews(&app.state, before)
        .await
        .expect("watch runs");
    assert_eq!(abandoned, 0);
    let at = entered_at + chrono::Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS);
    let (_, abandoned) = watch_manual_reviews(&app.state, at)
        .await
        .expect("watch runs");
    assert_eq!(abandoned, 1);

    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(
        facts,
        PaymentFacts {
            state: "expired".to_string(),
            outcome: Some("abandoned".to_string()),
            basis: Some("seller_unresponsive".to_string()),
            resolved_by: None,
            refund_reference: None,
            entered_at: None,
        },
        "the reaper records seller_unresponsive with NO resolver"
    );
    let (order_state, stock_held) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "cancelled");
    assert!(!stock_held, "the hold released");
    let audit: (String, Option<String>) = sqlx::query_as(
        "SELECT basis, resolved_by_pubky FROM paykit_manual_resolutions WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("audit row");
    assert_eq!(audit.0, "seller_unresponsive");
    assert!(audit.1.is_none());
    let outbox = outbox_facts(&pool, &order_id).await;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "abandoned");

    // A later seller resolve is 409; a second reaper pass is a no-op.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("already_resolved"));
    let (_, abandoned) = watch_manual_reviews(&app.state, at + chrono::Duration::days(1))
        .await
        .expect("watch runs");
    assert_eq!(abandoned, 0);
    // Condition 7 clears only now — the reaper, not the window, released it.
    assert!(condition_seven_clear(&pool, &paykit.stack_id())
        .await
        .expect("condition 7 query"));
}

/// Seller and seven-day reaper at exactly T+7d: deterministic orderings
/// plus barrier-overlapped transactions, exactly one winner throughout.
#[sqlx::test(migrations = "./migrations")]
async fn seller_and_reaper_share_one_cas(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // Deterministic orderings.
    for reaper_first in [true, false] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
        let entered_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .expect("entry stamp");
        let at = entered_at + chrono::Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS);
        if reaper_first {
            let (_, abandoned) = watch_manual_reviews(&app.state, at)
                .await
                .expect("watch runs");
            assert_eq!(abandoned, 1);
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["reason"], json!("already_resolved"));
            let facts = payment_facts(&pool, &order_id).await;
            assert_eq!(facts.basis.as_deref(), Some("seller_unresponsive"));
        } else {
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let (_, abandoned) = watch_manual_reviews(&app.state, at)
                .await
                .expect("watch runs");
            assert_eq!(abandoned, 0, "the losing reaper changes nothing");
            let facts = payment_facts(&pool, &order_id).await;
            assert_eq!(facts.basis.as_deref(), Some("seller_attestation"));
        }
        let resolutions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_manual_resolutions WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("resolution count");
        assert_eq!(resolutions, 1, "exactly one resolution row, whichever won");
    }

    // Barrier-overlapped: seller resolve and the reaper released together
    // at T+7d; exactly one winner per round (the invariant, not the
    // outcome, is the gate — the deterministic orderings above prove both
    // directions).
    for round in 0..4u64 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
        let entered_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .expect("entry stamp");
        let at = entered_at
            + chrono::Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS)
            + chrono::Duration::seconds(round as i64);
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (router, token, oid) = (app.router.clone(), seller.token.clone(), order_id.clone());
        let resolve_barrier = barrier.clone();
        let resolve_handle = tokio::spawn(async move {
            resolve_barrier.wait().await;
            let key = Uuid::new_v4();
            let request = axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v0/orders/{oid}/bitcoin/resolve"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .header("Idempotency-Key", key.to_string())
                .body(axum::body::Body::from(
                    serde_json::to_vec(&json!({ "outcome": "paid" })).expect("body"),
                ))
                .expect("request builds");
            let response = tower::util::ServiceExt::oneshot(router, request)
                .await
                .expect("request executes");
            response.status()
        });
        let watch_state = app.state.clone();
        let watch_barrier = barrier.clone();
        let watch_handle = tokio::spawn(async move {
            watch_barrier.wait().await;
            watch_manual_reviews(&watch_state, at).await
        });
        let (resolve_status, watch_result) = (
            resolve_handle.await.expect("resolve task"),
            watch_handle.await.expect("watch task"),
        );
        let abandoned = watch_result.expect("watch runs").1;
        let seller_won = resolve_status == StatusCode::OK;
        let reaper_won = abandoned == 1;
        assert_ne!(
            seller_won, reaper_won,
            "round {round}: exactly one winner (seller={seller_won}, reaper={reaper_won}, status={resolve_status})"
        );
        let resolutions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_manual_resolutions WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("resolution count");
        let outbox: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .expect("outbox count");
        assert_eq!(
            (resolutions, outbox),
            (1, 1),
            "round {round}: exactly one of each"
        );
        let facts = payment_facts(&pool, &order_id).await;
        assert!(
            facts.outcome.is_some(),
            "round {round}: exactly one outcome committed"
        );
    }
}

// ---------------------------------------------------------------------------
// Condition 7 lifecycle and the omit-payment-update calibration
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn condition_seven_blocks_until_resolution_and_needs_the_payment_side(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true));

    // Before the window: awaiting seller confirmation blocks.
    let entered_at = app.clock.now();
    poll_now(&app, entered_at).await;
    assert!(!condition_seven_clear(&pool, &paykit.stack_id())
        .await
        .unwrap());

    // After the 24h route: manual_review blocks.
    let at_deadline = entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS);
    let routed = route_due_seller_confirmation_windows(&app.state, at_deadline)
        .await
        .expect("reaper runs");
    assert_eq!(routed, 1);
    assert!(
        !condition_seven_clear(&pool, &paykit.stack_id())
            .await
            .unwrap(),
        "an elapsed window does NOT clear condition 7"
    );

    // After the seller's resolution: clear.
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(condition_seven_clear(&pool, &paykit.stack_id())
        .await
        .unwrap());

    // FAIL CALIBRATION: omit the payment-state update (order request state
    // moved to 'confirmed' but the payment left awaiting_entitlement).
    // Condition 7 reports CLEAR while the seller can still act — the exact
    // gap the payment side of the predicate closes.
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true));
    poll_now(&app, app.clock.now()).await;
    sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed',          paykit_seller_confirmation_entered_at = NULL, paykit_seller_confirmation_deadline = NULL          WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .execute(&pool)
    .await
    .expect("the buggy intermediate state");
    assert!(
        condition_seven_clear(&pool, &paykit.stack_id())
            .await
            .unwrap(),
        "the calibration proves the payment side is load-bearing: without it the drain reads clear"
    );
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "awaiting_entitlement");
}

// ---------------------------------------------------------------------------
// The CAS-removal calibration: naive read-then-write duplicates outcomes
// ---------------------------------------------------------------------------

/// Two overlapping naive resolution transactions (read-then-write, no CAS)
/// against a scratch database whose backstop uniques are DROPPED: both
/// commit, producing duplicate resolution and outbox rows — the defect the
/// shared CAS (`state='manual_review' AND resolution_outcome IS NULL`)
/// exists to prevent. Against the same schema WITH the CAS, the second
/// writer affects zero rows.
#[tokio::test]
async fn cas_removal_calibration_yields_duplicate_outcomes() {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base =
        sqlx::postgres::PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");
    let name = format!("cascalib_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create scratch database");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(base.clone().database(&name))
        .await
        .expect("connect to scratch database");
    // Full schema, then the backstops REMOVED (the calibration isolates
    // the CAS predicate's role).
    let migrations_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
    let mut files: Vec<_> = std::fs::read_dir(migrations_dir)
        .expect("migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.ends_with(".sql")
                .then(|| format!("{migrations_dir}/{name}"))
        })
        .collect();
    files.sort();
    for file in files {
        let sql = std::fs::read_to_string(&file).expect("migration readable");
        sqlx::raw_sql(&sql)
            .execute(&pool)
            .await
            .expect("migration applies");
    }
    sqlx::query(
        "ALTER TABLE paykit_manual_resolutions DROP CONSTRAINT paykit_manual_resolutions_pkey",
    )
    .execute(&pool)
    .await
    .expect("drop resolution pk");
    sqlx::query(
        "ALTER TABLE paykit_resolve_outbox DROP CONSTRAINT paykit_resolve_outbox_order_id_key",
    )
    .execute(&pool)
    .await
    .expect("drop outbox unique");

    // Seed an order in manual_review.
    let order_id = Uuid::new_v4();
    let payment_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines,          delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent,          guarantee_policy_version, payment_id, created_at, updated_at)          VALUES ($1, 'buyer', 'seller', 1, 'pending_payment', '[]', NULL, 50000, 1200, 51200,          'SAT', 0, 1, $2, NOW(), NOW())",
    )
    .bind(order_id)
    .bind(payment_id)
    .execute(&pool)
    .await
    .expect("seed order");
    sqlx::query(
        "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, state,          confirmations, amount_minor, currency, exponent, manual_review_entered_at, created_at,          updated_at)          VALUES ($1, $2, 'buyer', 'seller', 1, 'paykit', 'manual_review', 0, 51200, 'SAT', 0,          NOW(), NOW(), NOW())",
    )
    .bind(payment_id)
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("seed payment");

    // The naive pattern: both transactions read "manual_review,
    // unresolved", then each writes its own outcome (A commits before B's
    // writes, which B decided on its stale read).
    let mut conn_a = pool.acquire().await.expect("conn a");
    let mut conn_b = pool.acquire().await.expect("conn b");
    let mut tx_a = conn_a.begin().await.expect("tx a");
    let mut tx_b = conn_b.begin().await.expect("tx b");
    for (label, tx) in [("a", &mut tx_a), ("b", &mut tx_b)] {
        let state: String = sqlx::query_scalar("SELECT state FROM payments WHERE id = $1")
            .bind(payment_id)
            .fetch_one(&mut **tx)
            .await
            .unwrap_or_else(|_| panic!("read {label}"));
        assert_eq!(state, "manual_review");
    }
    naive_resolution_write(
        &mut tx_a,
        payment_id,
        order_id,
        "confirmed",
        "paid",
        "seller_attestation",
        "paid_manually",
        2,
    )
    .await;
    tx_a.commit().await.expect("commit a");
    naive_resolution_write(
        &mut tx_b,
        payment_id,
        order_id,
        "expired",
        "abandoned",
        "seller_unresponsive",
        "abandoned",
        3,
    )
    .await;
    tx_b.commit().await.expect("commit b");

    // The defect, observed: two resolutions and two outbox rows for one
    // order, with disagreeing outcomes.
    let resolutions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM paykit_manual_resolutions WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("resolution count");
    let outbox: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("outbox count");
    assert_eq!(
        (resolutions, outbox),
        (2, 2),
        "the naive pattern duplicates outcomes and outbox rows"
    );

    // The CAS predicate, same schema: the second writer affects zero rows.
    let cas_a = sqlx::query(
        "UPDATE payments SET state = 'confirmed', resolution_id = gen_random_uuid(),          resolution_outcome = 'paid', resolution_basis = 'seller_attestation', resolved_at = NOW(),          manual_review_entered_at = NULL          WHERE id = $1 AND state = 'manual_review' AND resolution_outcome IS NULL",
    )
    .bind(payment_id)
    .execute(&pool)
    .await
    .expect("cas write");
    assert_eq!(
        cas_a.rows_affected(),
        0,
        "the CAS refuses the second transition (the row already left manual_review)"
    );

    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop scratch database");
}

// ---------------------------------------------------------------------------
// The resolution/late-observer race
// ---------------------------------------------------------------------------

/// The late observer's manual_review entry and the seller's resolve are
/// overlapping real transactions: the resolve either lands after the entry
/// (and resolves) or before it (the named not_in_manual_review, with the
/// seller free to retry) — never a double outcome, never a lost one.
#[sqlx::test(migrations = "./migrations")]
async fn resolution_racing_the_late_observer_is_consistent(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    // Deterministic orderings.
    for observer_first in [true, false] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
        let after_window = app.clock.now() + chrono::Duration::seconds(3700);
        let expired = expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("sweep runs");
        assert_eq!(expired, 1);
        paykit.set_status(&reference, status_confirmed("shared_manual", true));
        let observe_at = after_window + chrono::Duration::seconds(60);
        if observer_first {
            let applied = poll_now(&app, observe_at).await;
            assert_eq!(applied, 1);
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        } else {
            // The resolve runs BEFORE the observer commits: the named
            // precondition, no side effects; the observer's entry then
            // lands and the seller's retry resolves.
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["reason"], json!("not_in_manual_review"));
            let applied = poll_now(&app, observe_at).await;
            assert_eq!(applied, 1);
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let resolutions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_manual_resolutions WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("resolution count");
        assert_eq!(resolutions, 1, "exactly one resolution either way");
    }

    // Barrier-overlapped: the observer's poll and the seller's resolve
    // released together; the outcome is either (observer won → resolve
    // succeeds) or (resolve saw the pre-entry state → named error), and
    // after a settle + one retry there is exactly one resolution.
    for _round in 0..3 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
        let after_window = app.clock.now() + chrono::Duration::seconds(3700);
        expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("sweep runs");
        paykit.set_status(&reference, status_confirmed("shared_manual", true));
        let observe_at = after_window + chrono::Duration::seconds(60);
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (router, token, oid) = (app.router.clone(), seller.token.clone(), order_id.clone());
        let resolve_barrier = barrier.clone();
        let resolve_handle = tokio::spawn(async move {
            resolve_barrier.wait().await;
            let key = Uuid::new_v4();
            let request = axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v0/orders/{oid}/bitcoin/resolve"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .header("Idempotency-Key", key.to_string())
                .body(axum::body::Body::from(
                    serde_json::to_vec(&json!({ "outcome": "paid" })).expect("body"),
                ))
                .expect("request builds");
            let response = tower::util::ServiceExt::oneshot(router, request)
                .await
                .expect("request executes");
            response.status()
        });
        let poll_app_state = app.state.clone();
        let poll_payments = app.state.payments.clone().expect("payments runtime");
        let poll_barrier = barrier.clone();
        let poll_handle = tokio::spawn(async move {
            poll_barrier.wait().await;
            let client = poll_payments.paykit.as_ref().expect("client");
            verify_due_paykit_payments(&poll_app_state, client, observe_at).await
        });
        let (resolve_status, poll_result) = (
            resolve_handle.await.expect("resolve task"),
            poll_handle.await.expect("poll task"),
        );
        poll_result.expect("poll runs");
        if resolve_status != StatusCode::OK {
            // The resolve saw the pre-entry state; the observer's entry is
            // durable now, so the retry resolves.
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let resolutions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM paykit_manual_resolutions WHERE order_id = $1",
        )
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("resolution count");
        let outbox: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = $1")
                .bind(Uuid::parse_str(&order_id).unwrap())
                .fetch_one(&pool)
                .await
                .expect("outbox count");
        assert_eq!((resolutions, outbox), (1, 1), "exactly one of each");
    }
}

// ---------------------------------------------------------------------------
// Nothing un-pays an already-paid order
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn no_path_unpays_a_paid_order(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    // An exclusive order paid the ordinary way, then a resolve attempt:
    // named precondition, the order stays paid with its receipt.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    paykit.set_status(&reference, status_confirmed("exclusive", true));
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 1);
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_in_manual_review"));
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "confirmed");

    // A resolved-paid order likewise stays paid through every later call:
    // the seven-day reaper never touches it and a re-resolve is refused.
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let distant = app.clock.now() + chrono::Duration::days(30);
    let (_, abandoned) = watch_manual_reviews(&app.state, distant)
        .await
        .expect("watch runs");
    assert_eq!(abandoned, 0);
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-x" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("already_resolved"));
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid", "no case un-pays an already paid order");
}

/// The naive (CAS-removed) resolution write used ONLY by the calibration:
/// read-then-write with no predicate, so two overlapping transactions both
/// "win". The production path's conditional UPDATE is what makes this
/// impossible with the backstop uniques present.
#[allow(clippy::too_many_arguments)]
async fn naive_resolution_write(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    payment_id: Uuid,
    order_id: Uuid,
    state: &'static str,
    outcome: &'static str,
    basis: &'static str,
    resolution: &'static str,
    event_revision: i64,
) {
    sqlx::query(
        "UPDATE payments SET state = $2, revision = revision + 1, resolution_id = $3, \
         resolution_outcome = $4, resolution_basis = $5, resolved_at = NOW(), \
         resolved_by_pubky = CASE WHEN $5 = 'seller_attestation' THEN 'seller' ELSE NULL END, \
         manual_review_entered_at = NULL, updated_at = NOW() WHERE id = $1",
    )
    .bind(payment_id)
    .bind(state)
    .bind(Uuid::new_v4())
    .bind(outcome)
    .bind(basis)
    .execute(&mut **tx)
    .await
    .expect("naive payment write");
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, $4, 'seller', 'payment.resolved', NOW())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(format!("payment:{payment_id}"))
    .bind(event_revision)
    .execute(&mut **tx)
    .await
    .expect("naive event");
    sqlx::query(
        "INSERT INTO paykit_manual_resolutions (order_id, payment_id, resolution_id, \
         outcome, basis, resolved_at, resolved_by_pubky, request_hash, response, event_id, \
         created_at) VALUES ($1, $2, $3, $4, $5, NOW(), \
         CASE WHEN $5 = 'seller_attestation' THEN 'seller' ELSE NULL END, 'h', '{}', $6, NOW())",
    )
    .bind(order_id)
    .bind(payment_id)
    .bind(Uuid::new_v4())
    .bind(outcome)
    .bind(basis)
    .bind(event_id)
    .execute(&mut **tx)
    .await
    .expect("naive resolution row");
    sqlx::query(
        "INSERT INTO paykit_resolve_outbox (order_id, payment_id, event_id, invoice_id, \
         resolution, resolved_at, stack_id, stack_endpoint, next_attempt_at, \
         delivery_deadline, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, NOW(), 'proof:x', 'http://a', NOW(), \
         NOW() + INTERVAL '1 hour', NOW(), NOW())",
    )
    .bind(order_id)
    .bind(payment_id)
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(resolution)
    .execute(&mut **tx)
    .await
    .expect("naive outbox row");
}

// ---------------------------------------------------------------------------
// Drop and auction inventory cells
// ---------------------------------------------------------------------------

/// A drop-stamped order late-paid: reacquisition re-debits the drop only
/// when units remain; an exhausted drop is the named `stock_unavailable`.
#[sqlx::test(migrations = "./migrations")]
async fn drop_late_paid_reacquires_or_refuses(pool: PgPool) {
    let (app, _stripe, paykit, _ipn, _shippo, homeserver) = {
        let stripe = spawn_fake_stripe().await;
        let paykit = spawn_fake_paykit().await;
        let ipn = spawn_fake_paypal_ipn().await;
        let shippo = spawn_fake_shippo().await;
        let homeserver = spawn_fake_homeserver().await;
        let runtime = std::sync::Arc::new(marketplace_service::payments::PaymentsRuntime {
            stripe_key_cipher: marketplace_service::payments::StripeKeyCipher::from_hex(
                TEST_STRIPE_ENCRYPTION_KEY,
            )
            .expect("key"),
            stripe: marketplace_service::payments::StripeClient::new(&stripe.base_url)
                .expect("stripe"),
            paykit: Some(
                marketplace_service::payments::PaykitClient::new(
                    &paykit.base_url,
                    TEST_PAYKIT_SIGNING_SEED,
                )
                .expect("paykit"),
            ),
            paypal_ipn: marketplace_service::payments::PaypalIpnVerifier::new(&ipn.base_url)
                .expect("ipn"),
            shippo: marketplace_service::payments::ShippoClient::new(&shippo.base_url)
                .expect("shippo"),
        });
        let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
        let clock = std::sync::Arc::new(marketplace_service::clock::AdjustableClock::new(now));
        let state = marketplace_service::AppState::new(
            pool.clone(),
            clock.clone(),
            marketplace_service::config::Config::for_tests(),
        )
        .with_payments(Some(runtime))
        .with_homeserver(Some(homeserver.client()));
        let app = TestApp {
            router: marketplace_service::http::build_router(state.clone()),
            pool: pool.clone(),
            clock,
            state,
        };
        (app, stripe, paykit, ipn, shippo, homeserver)
    };

    for exhaust_drop in [true, false] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        // SAT listing under a live drop (2 units, limit 2).
        let (status, body) = execute(
            &app,
            &seller.token,
            &register_sat_command(&seller.pubky, 16),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "register: {body}");
        let drop_id = "drop_late";
        homeserver.put_drop_record(
            &seller.pubky,
            drop_id,
            drop_record_json(
                &seller.pubky,
                drop_id,
                1,
                &["boots_01"],
                "2026-08-19T21:00:00.000Z",
                None,
                2,
                2,
            ),
        );
        let (status, body) = execute(
            &app,
            &seller.token,
            &sync_drop_command(&seller.pubky, drop_id, 900),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "drop sync: {body}");
        let drop_agg = drop_aggregate(&seller.pubky, drop_id);

        // The buyer's drop checkout (one unit, lock-at-claim), then a
        // bitcoin bind, then the hold lapses: drop credited, order
        // cancelled.
        let checkout_command_id = Uuid::new_v4().to_string();
        let checkout = json!({
            "version": 1,
            "command_id": checkout_command_id,
            "aggregate_id": format!("checkout:{checkout_command_id}"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "checkout.create",
            "payload": {
                "lines": [{
                    "listing_aggregate_id": listing_aggregate(&seller.pubky),
                    "expected_revision": 1,
                    "quantity": 1,
                }],
                "delivery_address": {
                    "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                    "city": "New York", "region": "NY", "postal_code": "10001",
                    "country_code": "US",
                },
                "guarantee_policy_version": 1,
            },
        });
        let (status, body) = execute(&app, &buyer.token, &checkout).await;
        assert_eq!(status, StatusCode::OK, "drop checkout: {body}");
        let order_id = body["result"]["orders"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        paykit.set_allocation_mode("shared_manual");
        enable_bitcoin(&app, &paykit, &seller).await;
        let (status, body) = send(
            app.router.clone(),
            "POST",
            &format!("/v0/orders/{order_id}/payment-method"),
            Some(&buyer.token),
            &json!({ "method": "bitcoin" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "bind: {body}");
        let client = app
            .state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        drain_outbox(&app.pool, client, app.clock.now(), 30)
            .await
            .expect("activation delivers");
        let after_window = app.clock.now() + chrono::Duration::seconds(3700);
        let expired = expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("sweep runs");
        assert_eq!(expired, 1, "the claim window lapsed and released the unit");
        let remaining: i64 =
            sqlx::query_scalar("SELECT remaining_quantity FROM drops WHERE aggregate_id = $1")
                .bind(&drop_agg)
                .fetch_one(&pool)
                .await
                .expect("drop row");
        assert_eq!(remaining, 2, "the release credited the drop");

        // Late settlement -> manual_review.
        let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
        paykit.set_status(&reference, status_confirmed("shared_manual", true));
        let applied = poll_now(&app, after_window + chrono::Duration::seconds(60)).await;
        assert_eq!(applied, 1);
        let facts = payment_facts(&pool, &order_id).await;
        assert_eq!(facts.state, "manual_review");

        if exhaust_drop {
            // Two other buyers claim and pay both remaining units: the drop
            // ends sold out, so reacquisition must refuse.
            for _ in 0..2 {
                let other = new_actor(&app).await;
                let revision: i64 = sqlx::query_scalar(
                    "SELECT server_revision FROM listings WHERE aggregate_id = $1",
                )
                .bind(listing_aggregate(&seller.pubky))
                .fetch_one(&pool)
                .await
                .expect("listing row");
                let other_command_id = Uuid::new_v4().to_string();
                let checkout = json!({
                    "version": 1,
                    "command_id": other_command_id,
                    "aggregate_id": format!("checkout:{other_command_id}"),
                    "expected_revision": 0,
                    "issued_at": "2026-08-19T22:00:00.000Z",
                    "kind": "checkout.create",
                    "payload": {
                        "lines": [{
                            "listing_aggregate_id": listing_aggregate(&seller.pubky),
                            "expected_revision": revision,
                            "quantity": 1,
                        }],
                        "delivery_address": {
                            "name": "Bob Buyer", "line1": "2 Market Street", "line2": "",
                            "city": "New York", "region": "NY", "postal_code": "10001",
                            "country_code": "US",
                        },
                        "guarantee_policy_version": 1,
                    },
                });
                let (status, body) = execute(&app, &other.token, &checkout).await;
                assert_eq!(status, StatusCode::OK, "other checkout: {body}");
                let payment_id = body["result"]["payments"][0]["id"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let (status, body) = execute(
                    &app,
                    &other.token,
                    &payment_command(&payment_id, 1, "confirmed", 1, 9_000),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "other payment: {body}");
            }
            let remaining: i64 =
                sqlx::query_scalar("SELECT remaining_quantity FROM drops WHERE aggregate_id = $1")
                    .bind(&drop_agg)
                    .fetch_one(&pool)
                    .await
                    .expect("drop row");
            assert_eq!(remaining, 0, "the drop is exhausted");

            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["reason"], json!("stock_unavailable"));
            let facts = payment_facts(&pool, &order_id).await;
            assert_eq!(facts.state, "manual_review", "nothing committed");
        } else {
            let (status, body) = resolve_call(
                &app,
                &seller.token,
                &order_id,
                Some(Uuid::new_v4()),
                &json!({ "outcome": "paid" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let (order_state, _) = order_row_state(&pool, &order_id).await;
            assert_eq!(order_state, "paid");
            let (remaining, paid): (i64, i64) = sqlx::query_as(
                "SELECT remaining_quantity, paid_quantity FROM drops WHERE aggregate_id = $1",
            )
            .bind(&drop_agg)
            .fetch_one(&pool)
            .await
            .expect("drop row");
            assert_eq!(
                (remaining, paid),
                (1, 1),
                "reacquired then paid exactly one unit"
            );
        }
    }
}

/// A SAT auction command fixture (the USD fixture's shape, priced in sats
/// so the winner can bind bitcoin). Each auction gets a distinct listing id
/// and command id, so several auctions can run in one test.
fn register_sat_auction_command(
    seller_pubky: &str,
    listing_id: &str,
    index: u64,
    starts_at: DateTime<Utc>,
) -> Value {
    let mut command = register_listing_command(seller_pubky, listing_id, 1, index + 100);
    command["payload"]["unit_price"] =
        json!({ "amount_minor": 45_000, "currency": "SAT", "exponent": 0 });
    command["payload"]["sale_format"] = json!("auction");
    command["payload"]["auction_terms"] = json!({
        "starts_at": marketplace_service::clock::format_timestamp(starts_at),
        "ends_at": marketplace_service::clock::format_timestamp(
            starts_at + chrono::Duration::seconds(600),
        ),
        "minimum_increment": { "amount_minor": 5_000, "currency": "SAT", "exponent": 0 },
        "reserve_price": { "amount_minor": 45_000, "currency": "SAT", "exponent": 0 },
        "anti_sniping_window_seconds": 60,
        "anti_sniping_extension_seconds": 120,
    });
    command
}

fn auction_listing(seller_pubky: &str, index: u64) -> (String, String) {
    let listing_id = format!("auction_{index}");
    (
        listing_id.clone(),
        format!("listing:{seller_pubky}_{listing_id}"),
    )
}

/// Closes a SAT auction with one winning bid and returns the winner's
/// order id plus the listing aggregate id.
async fn auction_winning_order(
    app: &TestApp,
    seller: &TestActor,
    winner: &TestActor,
    index: u64,
) -> (String, String) {
    let (listing_id, aggregate) = auction_listing(&seller.pubky, index);
    let (status, body) = execute(
        app,
        &seller.token,
        &register_sat_auction_command(&seller.pubky, &listing_id, index, app.clock.now()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register auction: {body}");
    let bid = json!({
        "version": 1,
        "command_id": indexed_command_id(0x8001, index),
        "aggregate_id": aggregate,
        "expected_revision": 1,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "auction.place_bid",
        "payload": {
            "maximum_amount": { "amount_minor": 100_000, "currency": "SAT", "exponent": 0 },
        },
    });
    let (status, body) = execute(app, &winner.token, &bid).await;
    assert_eq!(status, StatusCode::OK, "bid: {body}");
    app.clock.advance_seconds(660);
    let closed = close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("close runs");
    assert_eq!(closed, 1, "the auction closed");
    let (order_id,): (String,) = sqlx::query_as(
        "SELECT id::text FROM orders WHERE auction_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("winning order");
    (order_id, aggregate)
}

/// Auction cells: the reservation is extended at entry and PRESERVED
/// through manual_review; a paid resolution converts it; an abandoned one
/// releases it. The lapsed-reservation entry class reacquires or 409s.
#[sqlx::test(migrations = "./migrations")]
async fn auction_entries_resolve_through_the_reservation(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    // --- paid after a held entry: the reservation converts ---
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    let (order_id, aggregate) = auction_winning_order(&app, &seller, &winner, 1).await;
    paykit.set_allocation_mode("shared_manual");
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("shared_manual", true));
    let entered_at = app.clock.now();
    poll_now(&app, entered_at).await;
    let reservation_expiry: DateTime<Utc> = sqlx::query_scalar(
        "SELECT expires_at FROM reservations WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("reservation row");
    assert_eq!(
        reservation_expiry,
        entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
        "entry extended the winning reservation to the 24-hour window"
    );
    // The reservation sweep cannot touch it mid-window or in manual_review.
    let routed = route_due_seller_confirmation_windows(
        &app.state,
        entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("reaper runs");
    assert_eq!(routed, 1);
    marketplace_service::expiry::expire_due_reservations(
        &pool,
        entered_at + chrono::Duration::days(3),
    )
    .await
    .expect("reservation sweep runs");
    let reservation_status: String = sqlx::query_scalar(
        "SELECT status FROM reservations WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("reservation row");
    assert_eq!(
        reservation_status, "active",
        "preserved through manual_review"
    );

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    let reservation_status: String = sqlx::query_scalar(
        "SELECT status FROM reservations WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("reservation row");
    assert_eq!(reservation_status, "converted");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);

    // --- abandoned after a held entry: the reservation releases ---
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    let (order_id, aggregate) = auction_winning_order(&app, &seller, &winner, 2).await;
    paykit.set_allocation_mode("shared_manual");
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("shared_manual", true));
    let entered_at = app.clock.now();
    poll_now(&app, entered_at).await;
    let routed = route_due_seller_confirmation_windows(
        &app.state,
        entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("reaper runs");
    assert_eq!(routed, 1);
    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reservation_status: String = sqlx::query_scalar(
        "SELECT status FROM reservations WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("reservation row");
    assert_eq!(reservation_status, "released");
    let available: i64 =
        sqlx::query_scalar("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate)
            .fetch_one(&pool)
            .await
            .expect("listing row");
    assert_eq!(available, 1, "the unit restocked");

    // --- the lapsed-reservation entry class (confirmation fallback):
    // exclusive winner whose reservation expired before the observation;
    // manual_review via the confirm-failure path, then reacquire-or-409 ---
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    let (order_id, aggregate) = auction_winning_order(&app, &seller, &winner, 3).await;
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    // The 30-minute reservation hold lapses before any observation.
    app.clock.advance_seconds(31 * 60);
    let expired = marketplace_service::expiry::expire_due_reservations(&pool, app.clock.now())
        .await
        .expect("reservation sweep runs");
    assert_eq!(expired, 1, "the winning reservation lapsed");
    let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("exclusive", true));
    let applied = poll_now(&app, app.clock.now()).await;
    assert_eq!(applied, 1, "the confirm failure routes to manual_review");
    let facts = payment_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "manual_review");

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_state, _) = order_row_state(&pool, &order_id).await;
    assert_eq!(order_state, "paid");
    let reservation_status: String = sqlx::query_scalar(
        "SELECT status FROM reservations WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&pool)
    .await
    .expect("reservation row");
    assert_eq!(
        reservation_status, "converted",
        "the lapsed reservation was reacquired then converted"
    );
}
