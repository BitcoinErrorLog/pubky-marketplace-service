//! W1.15 pinned `paykit.resolve` delivery (design §B.8.8 r13): the
//! delivery arm dials the ROW'S pinned endpoint, checks the readiness
//! stack identity before sending, maps every response class to exactly one
//! outcome through the REAL local HTTP harness (408/425/429/5xx and
//! Retry-After are served by the double, never invented), honours
//! Retry-After as a floor under the hard one-hour deadline, and gates the
//! §C.16 drain on acknowledgement. The operator escape terminates DELIVERY
//! only — no case un-pays the order.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::*;
use marketplace_service::bitcoin_review::{
    condition_seven_clear, condition_six_blocking_rows, route_due_seller_confirmation_windows,
    SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::{order_reference, PaykitClient};
use marketplace_service::resolve_delivery::{
    acknowledge_resolve_row, deliver_due_resolve_rows, terminate_resolve_delivery,
};
use marketplace_service::workers::{drain_outbox, verify_due_paykit_payments};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const TOTAL_SATS: i64 = 51_200 + 437;
const OBSERVED_TXID: &str = "9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";

fn status_confirmed() -> Value {
    bitcoin_status_v2(
        "confirmed",
        true,
        "shared_manual",
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

/// Binds + activates + observes + seller-confirms a shared_manual order,
/// leaving one pinned `paid_manually` resolve outbox row. Returns
/// (order_id, invoice_id).
async fn confirmed_order_with_resolve_row(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, Uuid) {
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
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    paykit.set_status(&reference, status_confirmed());
    poll_now(app, app.clock.now()).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/confirm-bitcoin-payment", order.order_id),
        Some(&seller.token),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirm failed: {body}");
    let (invoice_id,): (Uuid,) =
        sqlx::query_as("SELECT invoice_id FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&app.pool)
            .await
            .expect("resolve row exists");
    (order.order_id, invoice_id)
}

fn paykit_client(app: &TestApp) -> &PaykitClient {
    // The arm is driven with the deployment's configured client; the ROW'S
    // pinned endpoint decides where traffic goes.
    app.state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client")
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct RowState {
    delivery_state: String,
    terminal_reason: Option<String>,
    terminal_detail: Option<String>,
    attempt_count: i32,
    next_attempt_at: DateTime<Utc>,
    delivered_at: Option<DateTime<Utc>>,
    auth_alerted: bool,
}

async fn row_state(pool: &PgPool, order_id: &str) -> RowState {
    sqlx::query_as(
        "SELECT delivery_state, terminal_reason, terminal_detail, attempt_count, \
         next_attempt_at, delivered_at, auth_alerted \
         FROM paykit_resolve_outbox WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(order_id).unwrap())
    .fetch_one(pool)
    .await
    .expect("resolve row")
}

async fn deliver(app: &TestApp, paykit: &PaykitClient, now: DateTime<Utc>) -> u64 {
    deliver_due_resolve_rows(&app.state, paykit, now, 30)
        .await
        .expect("delivery pass runs")
}

#[sqlx::test(migrations = "./migrations")]
async fn delivery_sends_the_canonical_signed_resolve_and_stamps(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    // The fake's contract finalizes observing invoices; the invoice was
    // activated by the bind's outbox.
    paykit.set_invoice_state(invoice_id, "observing");
    let client = paykit_client(&app);
    let now = app.clock.now();
    let finished = deliver(&app, client, now).await;
    assert_eq!(finished, 1);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");
    assert_eq!(row.delivered_at, Some(now));
    // The canonical signed body, exactly the captured shape, went to the
    // PINNED endpoint.
    let calls = paykit.calls();
    let resolve_calls: Vec<_> = calls
        .iter()
        .filter(|call| call.path.ends_with("/resolve"))
        .collect();
    assert_eq!(resolve_calls.len(), 1);
    let body = &resolve_calls[0].body;
    let keys: Vec<&str> = body
        .as_object()
        .expect("body object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec!["invoice_id", "resolution", "resolved_at", "stack_id"],
        "the canonical body key set, exactly"
    );
    assert_eq!(body["invoice_id"], json!(invoice_id));
    assert_eq!(body["resolution"], json!("paid_manually"));
    assert_eq!(body["stack_id"], json!(paykit.stack_id()));
    let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    assert_eq!(order_state, "paid", "delivery never touches the outcome");
}

#[sqlx::test(migrations = "./migrations")]
async fn permanent_refusals_terminate_visibly_without_retries(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let cases: Vec<(&str, u16, &str, &str)> = vec![
        ("unknown_invoice", 404, "unknown_invoice", "unknown_invoice"),
        (
            "invoice_not_activated",
            409,
            "invoice_not_activated",
            "invoice_not_activated",
        ),
        (
            "invoice_already_resolved",
            409,
            "invoice_already_resolved",
            "invoice_already_resolved",
        ),
        (
            "invoice_finalized",
            409,
            "invoice_finalized",
            "invoice_finalized",
        ),
        (
            "stack_identity_mismatch",
            409,
            "stack_identity_mismatch",
            "stack_identity_mismatch",
        ),
    ];
    let client = paykit_client(&app);
    for (label, status, code, expected_reason) in cases {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, invoice_id) =
            confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
        paykit.script_resolve(
            invoice_id,
            vec![FakePaykitReply::Error(status, code.to_string())],
        );
        let before = paykit.calls().len();
        let now = app.clock.now();
        let finished = deliver(&app, client, now).await;
        assert_eq!(finished, 1, "{label}: terminates on the first response");
        let row = row_state(&pool, &order_id).await;
        assert_eq!(row.delivery_state, "terminal_unresolved", "{label}");
        assert_eq!(
            row.terminal_reason.as_deref(),
            Some(expected_reason),
            "{label}"
        );
        assert_eq!(row.attempt_count, 1, "{label}: exactly one attempt");
        let after = paykit.calls().len();
        assert_eq!(after - before, 1, "{label}: no retry");
        let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("order row");
        assert_eq!(order_state, "paid", "{label}: the outcome stands");
        assert!(
            condition_six_blocking_rows(&pool, &paykit.stack_id())
                .await
                .expect("condition 6 query")
                >= 1,
            "{label}: a terminal row blocks the drain until acknowledged"
        );
    }
    assert!(
        captured_logs().contains("ALERT paykit resolve delivery terminated unresolved"),
        "every termination alerts"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn unmapped_classes_terminate_with_status_and_code_recorded(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.script_resolve(
        invoice_id,
        vec![FakePaykitReply::Error(418, "teapot_class".to_string())],
    );
    let client = paykit_client(&app);
    deliver(&app, client, app.clock.now()).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(
        row.terminal_reason.as_deref(),
        Some("unmapped_resolve_error")
    );
    assert_eq!(
        row.terminal_detail.as_deref(),
        Some("status=418 code=teapot_class"),
        "the status and code are recorded for the next round"
    );

    // A 2xx answering a DIFFERENT resolution is a contract violation, not
    // a success.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.script_resolve(invoice_id, vec![FakePaykitReply::Contract]);
    // Pre-record a conflicting resolution on the double: the replay rule
    // answers 409 invoice_already_resolved; a same-resolution replay would
    // be a delivered no-op (covered by the redelivery test).
    paykit.record_resolution(invoice_id, "abandoned", "2027-01-01T00:00:00Z");
    deliver(&app, client, app.clock.now()).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(
        row.terminal_reason.as_deref(),
        Some("invoice_already_resolved")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn transient_statuses_retry_then_deliver(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);
    // Every named transient status, through the real HTTP harness.
    for status in [408u16, 425, 429, 500, 502, 503, 504] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, invoice_id) =
            confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
        paykit.set_invoice_state(invoice_id, "observing");
        paykit.script_resolve(
            invoice_id,
            vec![
                FakePaykitReply::Error(status, "transient".to_string()),
                FakePaykitReply::Contract,
            ],
        );
        let now = app.clock.now();
        let finished = deliver(&app, client, now).await;
        assert_eq!(finished, 0, "{status}: retries, does not finish");
        let row = row_state(&pool, &order_id).await;
        assert_eq!(row.delivery_state, "queued", "{status}");
        assert_eq!(row.attempt_count, 1, "{status}");
        let backoff_due = now + chrono::Duration::seconds(30);
        assert_eq!(row.next_attempt_at, backoff_due, "{status}: normal backoff");
        // The retry delivers.
        let finished = deliver(&app, client, backoff_due).await;
        assert_eq!(finished, 1, "{status}: the next attempt delivers");
        let row = row_state(&pool, &order_id).await;
        assert_eq!(row.delivery_state, "delivered", "{status}");
    }
    // Transport failure (a dead endpoint) retries identically: point the
    // row's pin at a closed port. The readiness read fails first.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    sqlx::query(
        "UPDATE paykit_resolve_outbox SET stack_endpoint = 'http://127.0.0.1:1' WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .execute(&pool)
    .await
    .expect("repoint row");
    let now = app.clock.now();
    let finished = deliver(&app, client, now).await;
    assert_eq!(finished, 0);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "queued");
    assert_eq!(row.next_attempt_at, now + chrono::Duration::seconds(30));
}

#[sqlx::test(migrations = "./migrations")]
async fn baseline_in_progress_retries_and_401_alerts_once(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);

    // invoice_baseline_in_progress: the one transient refusal.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.set_invoice_state(invoice_id, "observing");
    paykit.script_resolve(
        invoice_id,
        vec![
            FakePaykitReply::Error(409, "invoice_baseline_in_progress".to_string()),
            FakePaykitReply::Contract,
        ],
    );
    let now = app.clock.now();
    deliver(&app, client, now).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "queued");
    deliver(&app, client, now + chrono::Duration::seconds(30)).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");

    // 401: retry under the deadline WITH an immediate first alert; the
    // second 401 does not re-alert.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.set_invoice_state(invoice_id, "observing");
    paykit.script_resolve(
        invoice_id,
        vec![
            FakePaykitReply::Error(401, "invalid_signature".to_string()),
            FakePaykitReply::Error(401, "invalid_signature".to_string()),
            FakePaykitReply::Contract,
        ],
    );
    let before = captured_logs()
        .matches("ALERT paykit resolve refused on authentication")
        .count();
    deliver(&app, client, now).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "queued");
    assert!(row.auth_alerted, "the first 401 alerts");
    let mid = captured_logs()
        .matches("ALERT paykit resolve refused on authentication")
        .count();
    assert_eq!(mid - before, 1, "exactly one alert on the first occurrence");
    deliver(&app, client, now + chrono::Duration::seconds(30)).await;
    let after = captured_logs()
        .matches("ALERT paykit resolve refused on authentication")
        .count();
    assert_eq!(after - mid, 0, "no second alert for the same row");
    deliver(&app, client, now + chrono::Duration::seconds(90)).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");
}

#[sqlx::test(migrations = "./migrations")]
async fn retry_after_is_a_floor_never_an_undercut(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);

    // 429 with a Retry-After LONGER than the backoff: honoured as the next
    // attempt, clamped by the deadline.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.script_resolve(
        invoice_id,
        vec![FakePaykitReply::ErrorWithRetryAfter(
            429,
            "rate_limited".to_string(),
            "120".to_string(),
        )],
    );
    let now = app.clock.now();
    deliver(&app, client, now).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "queued");
    assert_eq!(
        row.next_attempt_at,
        now + chrono::Duration::seconds(120),
        "the server's floor wins when it is later than the backoff"
    );

    // Retry-After: 0, a past HTTP-date, and a malformed value CANNOT
    // undercut the normal backoff.
    for (label, header) in [
        ("zero", "0"),
        ("past http-date", "Thu, 01 Jan 1970 00:00:00 GMT"),
        ("malformed", "soon-ish"),
    ] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, invoice_id) =
            confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
        paykit.script_resolve(
            invoice_id,
            vec![FakePaykitReply::ErrorWithRetryAfter(
                429,
                "rate_limited".to_string(),
                header.to_string(),
            )],
        );
        deliver(&app, client, now).await;
        let row = row_state(&pool, &order_id).await;
        assert_eq!(row.delivery_state, "queued", "{label}");
        assert_eq!(
            row.next_attempt_at,
            now + chrono::Duration::seconds(30),
            "{label}: the backoff floor holds"
        );
        // The row is NOT claimable before the backoff: no hot loop.
        let finished = deliver(&app, client, now + chrono::Duration::seconds(1)).await;
        assert_eq!(finished, 0, "{label}: no early re-claim");
    }

    // A Retry-After beyond the remaining deadline terminates AT the
    // deadline with no final attempt.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    let deadline = now + chrono::Duration::seconds(300);
    sqlx::query("UPDATE paykit_resolve_outbox SET delivery_deadline = $2 WHERE order_id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .bind(deadline)
        .execute(&pool)
        .await
        .expect("compress deadline");
    paykit.script_resolve(
        invoice_id,
        vec![FakePaykitReply::ErrorWithRetryAfter(
            429,
            "rate_limited".to_string(),
            "3600".to_string(),
        )],
    );
    let finished = deliver(&app, client, now).await;
    assert_eq!(finished, 1, "the clamp terminates at the deadline");
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(
        row.terminal_reason.as_deref(),
        Some("delivery_deadline_exceeded")
    );

    // ANTI-SPIN CALIBRATION: what the floor prevents. Stamping the row the
    // way the floor-less formula would (next_attempt = retry_after_due_at =
    // now) makes it immediately claimable — repeated claims before the
    // normal backoff is due. The production max() floor is what stops it.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    sqlx::query("UPDATE paykit_resolve_outbox SET next_attempt_at = $2 WHERE order_id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .bind(now)
        .execute(&pool)
        .await
        .expect("simulate the floor-less stamp");
    let claims: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM paykit_resolve_outbox \
         WHERE delivery_state = 'queued' AND next_attempt_at <= $1 \
         AND (lease_until IS NULL OR lease_until <= $1)",
    )
    .bind(now + chrono::Duration::seconds(1))
    .fetch_one(&pool)
    .await
    .expect("claimable count");
    assert!(
        claims >= 1,
        "floor removed: the row spins (claimable long before the 30s backoff)"
    );
    // With the floor intact (the earlier cases), next_attempt_at never
    // precedes the backoff due instant.
}

#[sqlx::test(migrations = "./migrations")]
async fn the_deadline_terminates_regardless_of_class(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    // A class that would otherwise retry forever (transport-level 503),
    // compressed deadline: retry at deadline−1s, terminate AT the deadline.
    paykit.script_resolve(
        invoice_id,
        vec![
            FakePaykitReply::Error(503, "down".to_string()),
            FakePaykitReply::Error(503, "down".to_string()),
        ],
    );
    let now = app.clock.now();
    let deadline = now + chrono::Duration::seconds(45);
    sqlx::query("UPDATE paykit_resolve_outbox SET delivery_deadline = $2 WHERE order_id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .bind(deadline)
        .execute(&pool)
        .await
        .expect("compress deadline");
    deliver(&app, client, now).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "queued", "deadline−1s: still retrying");
    assert_eq!(row.next_attempt_at, now + chrono::Duration::seconds(30));
    // At the deadline the row terminates WITHOUT a final attempt (the
    // second scripted 503 is never consumed).
    let finished = deliver(&app, client, deadline).await;
    assert_eq!(finished, 1);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(
        row.terminal_reason.as_deref(),
        Some("delivery_deadline_exceeded")
    );
    let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    assert_eq!(
        order_state, "paid",
        "the deadline never touches the outcome"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn pinned_a_then_repoint_b_delivers_only_to_a(pool: PgPool) {
    let (app, _stripe, paykit_a) = test_app_with_payments(pool.clone()).await;
    let paykit_b = spawn_fake_paykit().await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit_a, &seller, &buyer).await;
    paykit_a.set_invoice_state(invoice_id, "observing");

    // The "current default" is now B (a new client built against B): the
    // row pinned to A must still deliver ONLY to A.
    let client_b = PaykitClient::new(&paykit_b.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("client for the repointed default");
    let now = app.clock.now();
    let finished = deliver(&app, &client_b, now).await;
    assert_eq!(finished, 1);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");
    let a_resolve_calls = paykit_a
        .calls()
        .iter()
        .filter(|call| call.path.ends_with("/resolve"))
        .count();
    assert_eq!(a_resolve_calls, 1, "delivered to the pinned stack A");
    assert!(
        paykit_b.calls().is_empty(),
        "zero requests reached the current-default stack B"
    );

    // The decisive same-role/wrong-instance case: the row's pinned
    // endpoint now answers with a DIFFERENT stack identity (same role
    // prefix, fresh instance). The row is never sent; it terminates
    // stack_pin_mismatch outside the acknowledgement gate.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit_a, &seller, &buyer).await;
    sqlx::query("UPDATE paykit_resolve_outbox SET stack_endpoint = $2 WHERE order_id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .bind(&paykit_b.base_url)
        .execute(&pool)
        .await
        .expect("rebind the endpoint to the wrong instance");
    assert!(
        paykit_a.stack_id().starts_with("test-stack:")
            && paykit_b.stack_id().starts_with("test-stack:")
            && paykit_a.stack_id() != paykit_b.stack_id(),
        "same role, wrong instance"
    );
    let before = paykit_b.calls().len();
    let finished = deliver(&app, &client_b, now).await;
    assert_eq!(finished, 1);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(row.terminal_reason.as_deref(), Some("stack_pin_mismatch"));
    let b_calls = &paykit_b.calls()[before..];
    assert!(
        b_calls
            .iter()
            .all(|call| call.path == "/health/ready" || !call.path.ends_with("/resolve")),
        "no resolve request was sent to the wrong stack"
    );
    assert!(
        condition_six_blocking_rows(&pool, &paykit_a.stack_id())
            .await
            .expect("condition 6 query")
            >= 1,
        "the mismatched row blocks the drain until acknowledged"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn redelivery_is_a_terminal_ok_noop(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.set_invoice_state(invoice_id, "observing");
    let client = paykit_client(&app);
    deliver(&app, client, app.clock.now()).await;
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");
    assert_eq!(
        paykit
            .resolution(invoice_id)
            .map(|(resolution, _)| resolution),
        Some("paid_manually".to_string())
    );

    // A second row naming the SAME invoice (the redelivery shape): the
    // stack replays its recorded resolution with a 200, and the arm treats
    // it as delivered — a no-op, never a rejection.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _other_invoice) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    sqlx::query("UPDATE paykit_resolve_outbox SET invoice_id = $2 WHERE order_id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .bind(invoice_id)
        .execute(&pool)
        .await
        .expect("re-point the row at the resolved invoice");
    let before = paykit.calls().len();
    let finished = deliver(&app, client, app.clock.now()).await;
    assert_eq!(finished, 1);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "delivered");
    let calls = &paykit.calls()[before..];
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.path.ends_with("/resolve"))
            .count(),
        1,
        "the redelivery was sent once and accepted idempotently"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn acknowledgement_gates_the_drain_and_the_escape_is_delivery_only(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);

    // A terminally-failed row blocks condition 6 until acknowledged.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    paykit.script_resolve(
        invoice_id,
        vec![FakePaykitReply::Error(404, "unknown_invoice".to_string())],
    );
    deliver(&app, client, app.clock.now()).await;
    assert!(
        condition_six_blocking_rows(&pool, &paykit.stack_id())
            .await
            .expect("condition 6")
            >= 1
    );
    let row_id: i64 =
        sqlx::query_scalar("SELECT id FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("row id");
    let acked = acknowledge_resolve_row(
        &pool,
        row_id,
        "infra-oncall",
        "stack was rebuilt; audit copy unrecoverable, order outcome verified",
        app.clock.now(),
    )
    .await
    .expect("acknowledge runs");
    assert!(acked);
    assert_eq!(
        condition_six_blocking_rows(&pool, &paykit.stack_id())
            .await
            .expect("condition 6"),
        0,
        "the acknowledged terminal row no longer blocks"
    );
    let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    assert_eq!(
        order_state, "paid",
        "acknowledgement never touches the order"
    );

    // The operator escape: a queued row terminates as operator_terminated
    // WITH its acknowledgement recorded, alerting — delivery only.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _invoice_id) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    let row_id: i64 =
        sqlx::query_scalar("SELECT id FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("row id");
    let terminated = terminate_resolve_delivery(
        &pool,
        row_id,
        "infra-oncall",
        "rollback window; accepting the audit-copy loss for this row",
        app.clock.now(),
    )
    .await
    .expect("terminate runs");
    assert!(terminated);
    let row = row_state(&pool, &order_id).await;
    assert_eq!(row.delivery_state, "terminal_unresolved");
    assert_eq!(row.terminal_reason.as_deref(), Some("operator_terminated"));
    assert!(
        captured_logs().contains("ALERT an infrastructure operator terminated"),
        "the escape alerts"
    );
    let order_state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("order row");
    assert_eq!(order_state, "paid", "the escape never un-pays the order");
    let payment_state: String =
        sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
            .bind(Uuid::parse_str(&order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("payment row");
    assert_eq!(payment_state, "confirmed");
}

#[sqlx::test(migrations = "./migrations")]
async fn drain_gate_7_6_7_interleavings(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let client = paykit_client(&app);
    let stack = paykit.stack_id();

    // Order awaiting seller confirmation: 7 blocks immediately.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("shared_manual");
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
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    let paykit_client_ref = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client_ref, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    paykit.set_status(&reference, status_confirmed());
    poll_now(&app, app.clock.now()).await;

    // 7a: BLOCKED (awaiting seller confirmation can still create a row).
    assert!(!condition_seven_clear(&pool, &stack).await.unwrap());

    // The seller resolves (a resolution COMMITS between 7a and the 6
    // check): 6 now sees the queued row — the calibrated gate FAILS.
    let entered_at = app.clock.now();
    let routed = route_due_seller_confirmation_windows(
        &app.state,
        entered_at + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("reaper runs");
    assert_eq!(routed, 1);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/v0/orders/{}/bitcoin/resolve", order.order_id))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", seller.token))
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .body(axum::body::Body::from(
            serde_json::to_vec(&json!({ "outcome": "abandoned" })).expect("body"),
        ))
        .expect("request builds");
    let response = tower::util::ServiceExt::oneshot(app.router.clone(), request)
        .await
        .expect("request executes");
    assert_eq!(response.status(), StatusCode::OK);
    let gate_7a = condition_seven_clear(&pool, &stack).await.unwrap();
    let gate_6 = condition_six_blocking_rows(&pool, &stack).await.unwrap();
    assert!(gate_7a, "the resolution clears 7");
    assert!(gate_6 >= 1, "the fresh row FAILS the gate at 6");

    // Deliver the row, then the full sequence passes.
    let (invoice_id,): (Uuid,) =
        sqlx::query_as("SELECT invoice_id FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("row");
    paykit.set_invoice_state(invoice_id, "observing");
    deliver(&app, client, app.clock.now()).await;
    let gate_7b = condition_seven_clear(&pool, &stack).await.unwrap();
    let gate_6 = condition_six_blocking_rows(&pool, &stack).await.unwrap();
    assert!(gate_7b && gate_6 == 0, "7 -> 6 -> 7 passes after delivery");

    // The other interleaving: a NEW order enters awaiting AFTER 6 passed —
    // the final fresh 7 read FAILS the gate (the runbook records PASS only
    // from the final condition-7 read).
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (_order_id, _invoice) =
        confirmed_order_with_resolve_row(&app, &paykit, &seller, &buyer).await;
    // (An unresolved resolve row exists again; deliver it so 6 is clean.)
    deliver(&app, client, app.clock.now()).await;
    assert_eq!(
        condition_six_blocking_rows(&pool, &stack).await.unwrap(),
        0,
        "6 clean"
    );
    // Between the 6 read and the final 7 read, a new order enters
    // awaiting_seller_confirmation.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("shared_manual");
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
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    drain_outbox(&app.pool, paykit_client_ref, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());
    paykit.set_status(&reference, status_confirmed());
    poll_now(&app, app.clock.now()).await;
    assert!(
        !condition_seven_clear(&pool, &stack).await.unwrap(),
        "the final fresh condition-7 read FAILS the gate"
    );
}
