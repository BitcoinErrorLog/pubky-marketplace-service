//! The Bitcoin bind on the upstream paykit API (`PAYKIT_SERVER_API=upstream`,
//! `pubky/paykit-server` #66): phase 1 prepares an unpublished payment
//! request named by a deterministic operation id and payment reference, the
//! attempt is persisted with its activation deadline, and a refused
//! preparation leaves the order unbound. Activation is a later slice, so the
//! bind writes no activation row, and releasing a prepared attempt is local.
//!
//! The paykit double answers the way the captured server exchanges do
//! (`paykit_upstream_prepare_test.rs` pins it to them); the fork's bind
//! suites (`paykit_two_phase_test.rs` and the rest) keep covering the fork.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{create_sat_order, enable_bitcoin};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payment_attempt::{
    operation_id, payment_reference, PaymentAsset, PreparedAttempt,
};
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// The order total the SAT listing fixture checks out (price plus shipping).
const AMOUNT_SATS: i64 = 51_200;

fn order_uuid(order_id: &str) -> Uuid {
    Uuid::parse_str(order_id).expect("order id is a uuid")
}

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await
}

/// An upstream app whose clock is the real instant (the double stamps
/// activation deadlines from the real clock), a seller with Bitcoin on, a
/// buyer, and one pending SAT order.
async fn upstream_order(pool: PgPool) -> (TestApp, FakePaykit, TestActor, TestActor, String) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    // Microsecond precision, as the database stores it.
    app.clock
        .set(DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).expect("now"));
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    (app, paykit, seller, buyer, order.order_id)
}

#[derive(Debug, sqlx::FromRow)]
struct AttemptRow {
    stock_held: bool,
    hold_expires_at: Option<DateTime<Utc>>,
    payment_method: Option<String>,
    paykit_request_reference: Option<String>,
    paykit_request_state: Option<String>,
    paykit_activation_state: Option<String>,
    paykit_bind_attempt: i32,
    paykit_invoice_id: Option<Uuid>,
    paykit_stack_id: Option<String>,
    paykit_stack_endpoint: Option<String>,
    paykit_allocation_mode: Option<String>,
    paykit_address_fingerprint: Option<String>,
    paykit_total_sats: Option<i64>,
    paykit_expires_at: Option<DateTime<Utc>>,
    paykit_prepare_expires_at: Option<DateTime<Utc>>,
    paykit_payment_reference: Option<Uuid>,
    paykit_operation_id: Option<String>,
    paykit_payment_window_seconds: Option<i32>,
    paykit_asset: Option<String>,
}

async fn attempt_row(pool: &PgPool, order_id: &str) -> AttemptRow {
    sqlx::query_as(
        "SELECT state, stock_held, hold_expires_at, payment_method, paykit_request_reference, \
         paykit_request_state, paykit_activation_state, paykit_bind_attempt, paykit_invoice_id, \
         paykit_stack_id, paykit_stack_endpoint, paykit_allocation_mode, \
         paykit_address_fingerprint, paykit_total_sats, paykit_expires_at, \
         paykit_prepare_expires_at, paykit_payment_reference, paykit_operation_id, \
         paykit_payment_window_seconds, paykit_asset FROM orders WHERE id = $1",
    )
    .bind(order_uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn activation_rows(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate'",
    )
    .await
}

fn paths_called(paykit: &FakePaykit) -> Vec<String> {
    paykit.calls().into_iter().map(|call| call.path).collect()
}

// ---------------------------------------------------------------------------
// The prepared attempt
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_bind_prepares_the_attempt_by_its_derived_identity(pool: PgPool) {
    let (app, paykit, seller, buyer, order_id) = upstream_order(pool).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let order = order_uuid(&order_id);
    let reference = attempt_reference(order, 1);
    let requests = paykit.prepare_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0],
        json!({
            "amount_sats": AMOUNT_SATS,
            "creator": format!("pubky{}", seller.pubky),
            "operation_id": format!("marketplace-payment:{reference}:1"),
            "payment_window_seconds": app.state.config.bitcoin_payment_window_seconds,
            "reader": format!("pubky{}", buyer.pubky),
            "reference": payment_reference(order, 1).to_string(),
        }),
        "exactly the closed request: no expires_at, idempotency key or fork field"
    );
    assert_eq!(
        paths_called(&paykit),
        ["/marketplace/payment-requests/prepare"],
        "the fork route is never dialed"
    );
    assert!(paykit.requests().is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_attempt_is_persisted_with_its_activation_deadline_and_no_fork_pins(pool: PgPool) {
    let (app, paykit, seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let bound_at = app.clock.now();
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let order = order_uuid(&order_id);
    let operation = operation_id(&attempt_reference(order, 1), 1);
    let answer = paykit
        .prepared_answer(&format!("pubky{}", seller.pubky), &operation)
        .expect("prepared");
    let row = attempt_row(&pool, &order_id).await;

    assert_eq!(row.payment_method.as_deref(), Some("bitcoin"));
    assert_eq!(row.paykit_bind_attempt, 1);
    assert_eq!(
        row.paykit_request_reference.as_deref(),
        Some(attempt_reference(order, 1).as_str())
    );
    assert_eq!(row.paykit_activation_state.as_deref(), Some("preparing"));
    assert_eq!(row.paykit_request_state.as_deref(), Some("preparing"));

    assert_eq!(
        row.paykit_invoice_id,
        Some(
            answer["invoice_id"]
                .as_str()
                .expect("id")
                .parse()
                .expect("uuid")
        )
    );
    assert_eq!(
        row.paykit_payment_reference,
        Some(payment_reference(order, 1))
    );
    assert_eq!(row.paykit_operation_id.as_deref(), Some(operation.as_str()));
    assert_eq!(row.paykit_asset.as_deref(), Some("BTC"));
    assert_eq!(
        row.paykit_total_sats,
        Some(AMOUNT_SATS),
        "no nonce: total == amount"
    );
    assert_eq!(
        row.paykit_payment_window_seconds,
        Some(i32::try_from(app.state.config.bitcoin_payment_window_seconds).expect("fits"))
    );

    assert_eq!(row.paykit_stack_id, None);
    assert_eq!(row.paykit_stack_endpoint, None);
    assert_eq!(row.paykit_allocation_mode, None);
    assert_eq!(row.paykit_address_fingerprint, None);

    let activate_by = row.paykit_prepare_expires_at.expect("activation deadline");
    let answered: DateTime<Utc> = answer["prepare_expires_at"]
        .as_str()
        .expect("deadline")
        .parse()
        .expect("RFC 3339");
    assert_eq!(activate_by, answered, "the deadline is the server's own");
    let lead = (activate_by - bound_at).num_seconds();
    assert!(
        (14 * 60..=16 * 60).contains(&lead),
        "fifteen minutes to activate, got {lead}s"
    );
    assert_eq!(
        activation_rows(&pool).await,
        0,
        "activation is a later slice: nothing is queued for it"
    );

    let mut conn = pool.acquire().await.expect("connection");
    let attempt = PreparedAttempt::load(&mut conn, order)
        .await
        .expect("attempt reads")
        .expect("an upstream attempt");
    assert_eq!(attempt.payment_asset, PaymentAsset::Btc);
    assert_eq!(attempt.reference, payment_reference(order, 1));
    assert_eq!(attempt.operation_id, operation);
    assert_eq!(attempt.total_sats, AMOUNT_SATS);
    assert_eq!(attempt.activate_by, answered);
    assert_eq!(attempt.hold_expires_at, row.hold_expires_at);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_hold_and_the_payment_window_agree_at_the_bind(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let bound_at = app.clock.now();
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let window = app.state.config.bitcoin_payment_window_seconds;
    let row = attempt_row(&pool, &order_id).await;
    let hold = bound_at + chrono::Duration::seconds(window);
    assert!(row.stock_held);
    assert_eq!(row.hold_expires_at, Some(hold), "the bind arms the hold");
    assert_eq!(
        row.paykit_expires_at,
        Some(hold),
        "the persisted expiry is the local hold, never a server timestamp"
    );
    assert_eq!(
        paykit.prepare_requests()[0]["payment_window_seconds"],
        json!(window),
        "the window sent is the remaining hold: it starts only at activation"
    );
    assert!(
        !paykit.prepare_requests()[0]
            .as_object()
            .expect("object")
            .contains_key("expires_at"),
        "no absolute deadline is sent, and none is checked on the answer"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_new_bind_attempt_is_a_new_operation_and_reference(pool: PgPool) {
    let (app, paykit, seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let seller_key = format!("pubky{}", seller.pubky);
    paykit.refuse_prepare_for(&seller_key, 503, "dependency_unavailable");
    let (status, _) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    paykit.clear_prepare_refusals();
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let order = order_uuid(&order_id);
    let requests = paykit.prepare_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["operation_id"],
        json!(operation_id(&attempt_reference(order, 1), 1))
    );
    assert_eq!(
        requests[1]["operation_id"],
        json!(operation_id(&attempt_reference(order, 2), 2))
    );
    assert_eq!(
        requests[0]["reference"],
        json!(payment_reference(order, 1).to_string())
    );
    assert_eq!(
        requests[1]["reference"],
        json!(payment_reference(order, 2).to_string())
    );
    let row = attempt_row(&pool, &order_id).await;
    assert_eq!(row.paykit_bind_attempt, 2);
    assert_eq!(
        row.paykit_payment_reference,
        Some(payment_reference(order, 2))
    );
}

// ---------------------------------------------------------------------------
// Refusals leave the order unbound
// ---------------------------------------------------------------------------

async fn assert_unbound(app: &TestApp, order_id: &str) {
    let row = attempt_row(&app.pool, order_id).await;
    assert_eq!(row.payment_method, None, "nothing is bound");
    assert_eq!(row.paykit_invoice_id, None);
    assert_eq!(row.paykit_payment_reference, None);
    assert_eq!(row.paykit_operation_id, None);
    assert_eq!(row.paykit_activation_state, None);
    assert_eq!(row.paykit_request_state, None);
    assert!(!row.stock_held, "the rollback released the hold");
    assert_eq!(activation_rows(&app.pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_server_refusal_maps_to_its_bind_answer_and_binds_nothing(pool: PgPool) {
    for (server_status, code, expected_status, reason) in [
        (
            409,
            "operation_conflict",
            StatusCode::CONFLICT,
            "paykit_rejected",
        ),
        (
            401,
            "invalid_signature",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            400,
            "invalid_request",
            StatusCode::CONFLICT,
            "paykit_rejected",
        ),
        (
            409,
            "creator_session_invalid",
            StatusCode::CONFLICT,
            "seller_account_unclaimed",
        ),
        (
            503,
            "seller_setup_pending",
            StatusCode::CONFLICT,
            "seller_account_unclaimed",
        ),
        (
            409,
            "reader_not_payable",
            StatusCode::CONFLICT,
            "buyer_paykit_wallet_required",
        ),
        (
            503,
            "reader_setup_pending",
            StatusCode::CONFLICT,
            "buyer_paykit_wallet_setup_needed",
        ),
        (
            503,
            "creator_session_unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            503,
            "dependency_unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            503,
            "dependency_timeout",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            503,
            "reader_registry_unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            502,
            "reader_registry_malformed",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
        (
            429,
            "rate_limited",
            StatusCode::SERVICE_UNAVAILABLE,
            "paykit_unavailable",
        ),
    ] {
        let (app, paykit, seller, buyer, order_id) = upstream_order(pool.clone()).await;
        let party = if code == "reader_not_payable" || code == "reader_setup_pending" {
            format!("pubky{}", buyer.pubky)
        } else {
            format!("pubky{}", seller.pubky)
        };
        paykit.refuse_prepare_for(&party, server_status, code);
        let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
        assert_eq!(status, expected_status, "{code}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason), "{code}: {body}");
        assert_unbound(&app, &order_id).await;
        assert_eq!(paykit.prepare_calls(), 1, "{code}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_timeout_after_a_late_commit_makes_the_next_bind_a_new_attempt(pool: PgPool) {
    let (app, paykit, seller, buyer, order_id) = upstream_order(pool.clone()).await;
    paykit.set_prepare_late_commits(1);
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["reason"], json!("paykit_unavailable"));
    assert_unbound(&app, &order_id).await;

    let order = order_uuid(&order_id);
    let seller_key = format!("pubky{}", seller.pubky);
    let first = operation_id(&attempt_reference(order, 1), 1);
    let orphan = paykit
        .prepared_answer(&seller_key, &first)
        .expect("paykit committed the first preparation although the answer was lost");

    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let second = operation_id(&attempt_reference(order, 2), 2);
    let kept = paykit
        .prepared_answer(&seller_key, &second)
        .expect("second preparation");
    assert_ne!(orphan["invoice_id"], kept["invoice_id"]);

    let requests = paykit.prepare_requests();
    assert_eq!(
        requests.len(),
        2,
        "the service never retries the lost attempt"
    );
    assert_eq!(requests[0]["operation_id"], json!(first));
    assert_eq!(requests[1]["operation_id"], json!(second));
    assert_ne!(requests[0]["reference"], requests[1]["reference"]);
    let row = attempt_row(&pool, &order_id).await;
    assert_eq!(
        row.paykit_invoice_id,
        Some(
            kept["invoice_id"]
                .as_str()
                .expect("id")
                .parse()
                .expect("uuid")
        ),
        "the order is pinned to the new attempt, never to the orphan"
    );
    assert_eq!(
        paths_called(&paykit),
        vec!["/marketplace/payment-requests/prepare"; 2]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_total_that_is_not_the_amount_refuses_the_bind(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool).await;
    paykit.set_prepare_total_delta(437);
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("paykit_total_inconsistent"));
    assert_unbound(&app, &order_id).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_answer_outside_the_closed_contract_refuses_the_bind(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool).await;
    paykit.set_prepare_body(Some(json!({
        "invoice_id": Uuid::new_v4(),
        "state": "prepared",
        "stack_id": "fork-era",
        "total_sats": AMOUNT_SATS,
        "prepare_expires_at": "2099-01-01T00:00:00.000000Z",
    })));
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("paykit_rejected"));
    assert_unbound(&app, &order_id).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_activation_deadline_that_already_passed_refuses_the_bind(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool).await;
    paykit.set_prepare_expiry_offset(-60);
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("paykit_expiry_inconsistent"));
    assert_unbound(&app, &order_id).await;
}

// ---------------------------------------------------------------------------
// Releasing a prepared attempt is local
// ---------------------------------------------------------------------------

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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_buyer_cancel_releases_the_prepared_attempt_without_calling_paykit(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let calls_before = paykit.calls().len();

    let (revision,): (i64,) = sqlx::query_as("SELECT revision FROM orders WHERE id = $1")
        .bind(order_uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("order row");
    let command = order_command(
        "order.cancel_request",
        &order_id,
        revision,
        json!({ "reason": "Changed mind" }),
        (Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64,
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("voided".to_string()),
            None
        )
    );
    assert_eq!(
        paykit.calls().len(),
        calls_before,
        "an unpublished attempt has nothing to void at paykit-server"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.void'"
        )
        .await,
        0
    );
    let released: i64 = count(&pool, "SELECT COUNT(*) FROM paykit_superseded_attempts").await;
    assert_eq!(released, 0, "nothing was published, so nothing is watched");
    let row = attempt_row(&pool, &order_id).await;
    assert!(!row.stock_held, "the cancel released the hold");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_hold_expiring_releases_the_prepared_attempt_without_calling_paykit(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let calls_before = paykit.calls().len();

    app.clock
        .advance_seconds(app.state.config.bitcoin_payment_window_seconds - 1);
    assert_eq!(
        expire_due_payment_windows(&app.state, app.clock.now())
            .await
            .expect("expiry runs"),
        0,
        "the hold has not lapsed yet"
    );
    assert_eq!(
        facts(&pool, &order_id).await.2.as_deref(),
        Some("preparing")
    );

    app.clock.advance_seconds(2);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 1);
    assert_eq!(
        facts(&pool, &order_id).await,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("voided".to_string()),
            None
        )
    );
    let row = attempt_row(&pool, &order_id).await;
    assert!(!row.stock_held);
    assert_eq!(paykit.calls().len(), calls_before, "no paykit call");
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.void'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_prepare_voided'"
        )
        .await,
        1,
        "the buyer is told the request was voided"
    );
    assert_eq!(
        expire_due_payment_windows(&app.state, app.clock.now())
            .await
            .expect("expiry runs"),
        0,
        "a second pass finds nothing"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn draining_the_outbox_never_touches_a_prepared_upstream_attempt(pool: PgPool) {
    let (app, paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let calls_before = paykit.calls().len();
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&pool, client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(paykit.calls().len(), calls_before);
    assert_eq!(
        facts(&pool, &order_id).await.2.as_deref(),
        Some("preparing"),
        "only activation, a later slice, moves a prepared attempt"
    );
}

// ---------------------------------------------------------------------------
// Fiat rails and the fork are untouched
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_fork_bind_still_writes_the_fork_pins_and_no_attempt_columns(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let row = attempt_row(&pool, &order.order_id).await;
    assert_eq!(
        row.paykit_stack_id.as_deref(),
        Some(paykit.stack_id().as_str())
    );
    assert!(row.paykit_stack_endpoint.is_some());
    assert!(row.paykit_allocation_mode.is_some());
    assert!(row.paykit_address_fingerprint.is_some());
    assert_eq!(row.paykit_payment_reference, None);
    assert_eq!(row.paykit_operation_id, None);
    assert_eq!(row.paykit_payment_window_seconds, None);
    assert_eq!(row.paykit_asset, None);
    assert_eq!(
        activation_rows(&pool).await,
        1,
        "the fork still queues activation"
    );
    assert_eq!(
        paykit.prepare_calls(),
        0,
        "the fork never prepares upstream"
    );
    let mut conn = pool.acquire().await.expect("connection");
    assert!(
        PreparedAttempt::load(&mut conn, order_uuid(&order.order_id))
            .await
            .expect("reads")
            .is_none()
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_stored_asset_this_build_does_not_know_is_refused_not_read_as_bitcoin(pool: PgPool) {
    let (app, _paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    sqlx::query("UPDATE orders SET paykit_asset = 'USDT' WHERE id = $1")
        .bind(order_uuid(&order_id))
        .execute(&pool)
        .await
        .expect("a later build stored another asset");
    let mut conn = pool.acquire().await.expect("connection");
    let error = PreparedAttempt::load(&mut conn, order_uuid(&order_id))
        .await
        .expect_err("the asset is unknown here");
    assert!(error.to_string().contains("USDT"), "{error}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_attempt_columns_are_all_or_nothing(pool: PgPool) {
    let (app, _paykit, _seller, buyer, order_id) = upstream_order(pool.clone()).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for column in [
        "paykit_operation_id",
        "paykit_asset",
        "paykit_payment_window_seconds",
        "paykit_payment_reference",
    ] {
        let result = sqlx::query(&format!("UPDATE orders SET {column} = NULL WHERE id = $1"))
            .bind(order_uuid(&order_id))
            .execute(&pool)
            .await;
        assert!(result.is_err(), "{column} cannot be cleared alone");
    }
    let zero = sqlx::query("UPDATE orders SET paykit_payment_window_seconds = 0 WHERE id = $1")
        .bind(order_uuid(&order_id))
        .execute(&pool)
        .await;
    assert!(zero.is_err(), "a window is positive");
}
