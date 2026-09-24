//! Verified PayPal refund and reversal IPNs (docs/paypal-refund-ipn.md),
//! posted as the raw fixture bodies in `tests/fixtures/paypal_ipn` through
//! the real router and the real postback verifier.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::StatusCode;
use common::*;
use marketplace_service::clock::{format_timestamp, Clock};
use marketplace_service::http::build_router;
use serde_json::{json, Value};
use sqlx::PgPool;

/// `txn_id` of `completed.ipn`, and `parent_txn_id` of every refund fixture.
const PAYMENT_TXN: &str = "7XP31449AB123456C";
/// The fixture order total (137.00 USD), the gross of `completed.ipn`.
const TOTAL_MINOR: i64 = 13_700;
const FULL_REFUND_TXN: &str = "2WF58163VJ0384519";
const PARTIAL_1_TXN: &str = "3KA71027LM5520841";
const PARTIAL_2_TXN: &str = "4LB82138MN6631952";
const REVERSAL_TXN: &str = "5MC93249NP7742063";

type Fields = Vec<(String, String)>;

fn fixture(name: &str, order_id: &str) -> Fields {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    url::form_urlencoded::parse(text.trim_end().as_bytes())
        .map(|(name, value)| {
            let value = if value == "{{ORDER_ID}}" {
                order_id.to_string()
            } else {
                value.into_owned()
            };
            (name.into_owned(), value)
        })
        .collect()
}

fn with(mut fields: Fields, name: &str, value: &str) -> Fields {
    let entry = fields
        .iter_mut()
        .find(|(key, _)| key == name)
        .unwrap_or_else(|| panic!("fixture carries {name}"));
    entry.1 = value.to_string();
    fields
}

fn without(mut fields: Fields, name: &str) -> Fields {
    fields.retain(|(key, _)| key != name);
    fields
}

/// A refund fixture pointed at the given original payment.
fn refund(name: &str, order_id: &str, parent_txn_id: &str) -> Fields {
    with(fixture(name, order_id), "parent_txn_id", parent_txn_id)
}

fn encode(fields: &Fields) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in fields {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

async fn post_ipn(app: &TestApp, fields: &Fields) -> StatusCode {
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        encode(fields).into_bytes(),
    )
    .await;
    status
}

async fn read_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

static COMMAND_NUMBER: AtomicU64 = AtomicU64::new(40_000);

/// Sends an order command at the order's current revision.
async fn act(app: &TestApp, token: &str, kind: &str, order_id: &str, payload: Value) -> Value {
    let revision = read_order(app, token, order_id).await["revision"]
        .as_i64()
        .expect("revision");
    let number = COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed);
    let (status, body) = execute(
        app,
        token,
        &order_command(kind, order_id, revision, payload, number),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{kind} failed: {body}");
    body["result"]["order"].clone()
}

struct Order {
    seller: TestActor,
    buyer: TestActor,
    id: String,
}

/// A fresh seller and buyer, the seller's PayPal email configured, and a
/// PayPal-bound pending order of 137.00 USD.
async fn pending_paypal_order(app: &TestApp) -> Order {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({
            "bitcoin_enabled": false,
            "paypal_merchant_email": "merchant@example.com",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    Order {
        seller,
        buyer,
        id: order.order_id,
    }
}

/// [`pending_paypal_order`] paid by `completed.ipn` carrying `payment_txn`.
async fn paid_paypal_order(app: &TestApp, payment_txn: &str) -> Order {
    let order = pending_paypal_order(app).await;
    let status = post_ipn(
        app,
        &with(fixture("completed.ipn", &order.id), "txn_id", payment_txn),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("paid"), "{view}");
    assert_eq!(view["fiat_verification"], json!("gateway-notified"));
    assert_eq!(view["total"]["amount_minor"], json!(TOTAL_MINOR));
    order
}

async fn ship(app: &TestApp, order: &Order) {
    act(
        app,
        &order.seller.token,
        "fulfillment.ship",
        &order.id,
        json!({ "carrier": "USPS", "tracking_number": "9400100000000000000001" }),
    )
    .await;
}

async fn deliver(app: &TestApp, order: &Order) {
    ship(app, order).await;
    act(
        app,
        &order.buyer.token,
        "fulfillment.confirm_delivery",
        &order.id,
        json!({}),
    )
    .await;
}

async fn complete(app: &TestApp, order: &Order) {
    deliver(app, order).await;
    act(
        app,
        &order.buyer.token,
        "review.create",
        &order.id,
        json!({ "rating": 5, "text": "Arrived as described." }),
    )
    .await;
}

async fn request_return(app: &TestApp, order: &Order) {
    deliver(app, order).await;
    act(
        app,
        &order.buyer.token,
        "return.request",
        &order.id,
        json!({ "reason": "Wrong size", "requested_amount_minor": TOTAL_MINOR }),
    )
    .await;
}

async fn receive_return(app: &TestApp, order: &Order) {
    request_return(app, order).await;
    act(
        app,
        &order.seller.token,
        "return.approve",
        &order.id,
        json!({}),
    )
    .await;
    act(
        app,
        &order.seller.token,
        "return.receive",
        &order.id,
        json!({}),
    )
    .await;
}

async fn ledger_rows(pool: &PgPool, order_id: &str) -> Vec<(String, String, i64)> {
    sqlx::query_as(
        "SELECT refund_txn_id, payment_status, amount_minor FROM order_gateway_refunds \
         WHERE order_id = $1::uuid ORDER BY recorded_at, refund_txn_id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("ledger read")
}

async fn order_events(pool: &PgPool, order_id: &str, kind: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT actor_pubky FROM events WHERE aggregate_id = 'order:' || $1 AND kind = $2 \
         ORDER BY sequence",
    )
    .bind(order_id)
    .bind(kind)
    .fetch_all(pool)
    .await
    .expect("event read")
}

async fn refund_notification_recipients(pool: &PgPool, order_id: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT payload->>'recipient_pubky' FROM outbox \
         WHERE kind = 'notification.refund_recorded' AND payload->>'aggregate_id' = 'order:' || $1 \
         ORDER BY id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("outbox read")
}

async fn paypal_txn_id(pool: &PgPool, order_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT paypal_txn_id FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

/// Everything a dropped notification must leave untouched.
async fn assert_untouched(app: &TestApp, order: &Order, before: &Value) {
    let after = read_order(app, &order.buyer.token, &order.id).await;
    assert_eq!(after["revision"], before["revision"], "{after}");
    assert_eq!(after["state"], before["state"]);
    assert_eq!(after["external_refund"], before["external_refund"]);
    assert_eq!(after["payment_reversed_at"], before["payment_reversed_at"]);
    assert_eq!(
        ledger_rows(&app.pool, &order.id).await.len() as i64,
        before["__ledger"].as_i64().unwrap_or(0)
    );
}

async fn snapshot(app: &TestApp, order: &Order) -> Value {
    let mut view = read_order(app, &order.buyer.token, &order.id).await;
    view["__ledger"] = json!(ledger_rows(&app.pool, &order.id).await.len());
    view
}

/// The same app with the trust attestor configured, so a full refund's
/// `refunded` annotation is observable.
fn with_attestor(app: TestApp) -> TestApp {
    let state = app.state.clone().with_attestor(Some(test_attestor()));
    TestApp {
        router: build_router(state.clone()),
        pool: app.pool,
        clock: app.clock,
        state,
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn full_refund_ipn_moves_a_paid_order_to_refunded_external(pool: PgPool) {
    let (app, _stripe, _paykit, ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let app = with_attestor(app);
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = read_order(&app, &order.buyer.token, &order.id).await;

    let body = refund("refund-full.ipn", &order.id, PAYMENT_TXN);
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);

    // The exact body went back to the validation endpoint.
    let postbacks = ipn.postbacks();
    assert_eq!(
        postbacks.last().expect("postback"),
        &format!("cmd=_notify-validate&{}", encode(&body))
    );

    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(
        view["revision"].as_i64().unwrap(),
        before["revision"].as_i64().unwrap() + 1
    );
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!(FULL_REFUND_TXN)
    );
    assert_eq!(
        view["external_refund"]["recorded_at"],
        json!(format_timestamp(app.clock.now()))
    );
    assert_eq!(view["payment_reversed_at"], Value::Null);
    assert_eq!(view["next_actor"], Value::Null);
    assert_eq!(view["fiat_transaction_ref"], json!(PAYMENT_TXN));

    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "Refunded".to_string(),
            TOTAL_MINOR
        )]
    );
    assert_eq!(
        order_events(&pool, &order.id, "refund.recorded_external").await,
        vec!["paypal-ipn".to_string()]
    );
    assert!(order_events(&pool, &order.id, "refund.recorded_partial")
        .await
        .is_empty());
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await,
        vec![order.buyer.pubky.clone(), order.seller.pubky.clone()]
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM attestation_annotations WHERE outcome = 'refunded'"
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn partial_refund_ipn_keeps_the_state_and_records_the_amount(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let app = with_attestor(app);
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    deliver(&app, &order).await;
    let before = read_order(&app, &order.buyer.token, &order.id).await;

    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("delivered"), "{view}");
    assert_eq!(
        view["revision"].as_i64().unwrap(),
        before["revision"].as_i64().unwrap() + 1
    );
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!(PARTIAL_1_TXN)
    );
    assert_eq!(view["next_actor"], before["next_actor"]);
    assert_eq!(
        order_events(&pool, &order.id, "refund.recorded_partial").await,
        vec!["paypal-ipn".to_string()]
    );
    // A partial refund is not a terminal refund: the reputation worker
    // counts only `refund.recorded_external`, and no annotation is written.
    assert!(order_events(&pool, &order.id, "refund.recorded_external")
        .await
        .is_empty());
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM attestation_annotations").await,
        0
    );
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await,
        vec![order.buyer.pubky.clone(), order.seller.pubky.clone()]
    );

    // The partially refunded order still completes, carrying the refund.
    let completed = act(
        &app,
        &order.buyer.token,
        "review.create",
        &order.id,
        json!({ "rating": 4, "text": "Partial refund for a scuff." }),
    )
    .await;
    assert_eq!(completed["state"], json!("completed"));
    assert_eq!(completed["external_refund"]["amount_minor"], json!(4_000));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_partial_refund_ipns_sum_to_a_full_refund(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;

    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));

    let status = post_ipn(
        &app,
        &refund("refund-partial-2.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!(PARTIAL_2_TXN)
    );
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![
            (PARTIAL_1_TXN.to_string(), "Refunded".to_string(), 4_000),
            (PARTIAL_2_TXN.to_string(), "Refunded".to_string(), 9_700),
        ]
    );
    assert_eq!(
        order_events(&pool, &order.id, "refund.recorded_partial")
            .await
            .len(),
        1
    );
    assert_eq!(
        order_events(&pool, &order.id, "refund.recorded_external")
            .await
            .len(),
        1
    );
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await.len(),
        4
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn duplicate_refund_ipn_is_a_no_op(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let partial = refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN);

    assert_eq!(post_ipn(&app, &partial).await, StatusCode::OK);
    let recorded = snapshot(&app, &order).await;
    for _ in 0..3 {
        assert_eq!(post_ipn(&app, &partial).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &recorded).await;
    assert_eq!(recorded["external_refund"]["amount_minor"], json!(4_000));
    assert_eq!(
        order_events(&pool, &order.id, "refund.recorded_partial")
            .await
            .len(),
        1
    );
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await.len(),
        2
    );

    // The same holds for the notification that completed the refund.
    let rest = refund("refund-partial-2.ipn", &order.id, PAYMENT_TXN);
    assert_eq!(post_ipn(&app, &rest).await, StatusCode::OK);
    let full = snapshot(&app, &order).await;
    assert_eq!(full["state"], json!("refunded_external"));
    assert_eq!(post_ipn(&app, &rest).await, StatusCode::OK);
    assert_untouched(&app, &order, &full).await;
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await.len(),
        4
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_unknown_parent_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let unknown = "9ZZ99999ZZ9999999";
    for body in [
        // No custom: resolved by parent, which matches no order.
        without(refund("refund-full.ipn", &order.id, unknown), "custom"),
        // The right order, but a parent it never received.
        refund("refund-full.ipn", &order.id, unknown),
        // A custom order id that does not exist.
        refund(
            "refund-full.ipn",
            &uuid::Uuid::new_v4().to_string(),
            PAYMENT_TXN,
        ),
    ] {
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &before).await;
    assert!(refund_notification_recipients(&pool, &order.id)
        .await
        .is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_whose_custom_order_does_not_own_the_parent_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let first = paid_paypal_order(&app, "1AA00000AA0000001").await;
    let second = paid_paypal_order(&app, "1AA00000AA0000002").await;
    let first_before = snapshot(&app, &first).await;
    let second_before = snapshot(&app, &second).await;

    let status = post_ipn(
        &app,
        &refund("refund-full.ipn", &first.id, "1AA00000AA0000002"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_untouched(&app, &first, &first_before).await;
    assert_untouched(&app, &second, &second_before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_without_custom_resolves_by_parent(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let _other = paid_paypal_order(&app, "1AA00000AA0000001").await;
    let order = paid_paypal_order(&app, "1AA00000AA0000002").await;

    let status = post_ipn(
        &app,
        &without(
            refund("refund-full.ipn", &order.id, "1AA00000AA0000002"),
            "custom",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    let other_view = read_order(&app, &_other.buyer.token, &_other.id).await;
    assert_eq!(other_view["state"], json!("paid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_currency_mismatch_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    for currency in ["EUR", "usd", ""] {
        let body = with(
            refund("refund-full.ipn", &order.id, PAYMENT_TXN),
            "mc_currency",
            currency,
        );
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_to_another_receiver_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let body = with(
        with(
            refund("refund-full.ipn", &order.id, PAYMENT_TXN),
            "receiver_email",
            "attacker@example.com",
        ),
        "business",
        "attacker@example.com",
    );
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_a_non_negative_or_malformed_gross_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    for gross in [
        "137.00", "0.00", "-0.00", "-137", "-137.0", "-137.000", "-1,37", "", "--137.00",
    ] {
        let body = with(
            refund("refund-full.ipn", &order.id, PAYMENT_TXN),
            "mc_gross",
            gross,
        );
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK, "{gross}");
    }
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_malformed_transaction_ids_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let full = || refund("refund-full.ipn", &order.id, PAYMENT_TXN);
    let too_long = "A".repeat(65);
    for body in [
        with(full(), "txn_id", ""),
        with(full(), "txn_id", &too_long),
        with(full(), "txn_id", "2WF58163 VJ0384519"),
        without(full(), "txn_id"),
        with(full(), "parent_txn_id", ""),
        without(full(), "parent_txn_id"),
    ] {
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_exceeding_the_remaining_total_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;

    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let partial = snapshot(&app, &order).await;

    // 40.00 recorded; a further 137.00 would exceed the total.
    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_untouched(&app, &order, &partial).await;

    // The exact remainder still completes the refund.
    let status = post_ipn(
        &app,
        &refund("refund-partial-2.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn reversed_ipn_records_the_refund_and_flags_the_order(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;

    // A full chargeback on a shipped order.
    let order = paid_paypal_order(&app, "1AA00000AA0000001").await;
    ship(&app, &order).await;
    let status = post_ipn(
        &app,
        &refund("reversal-full.ipn", &order.id, "1AA00000AA0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let now = json!(format_timestamp(app.clock.now()));
    let seller_view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(
        seller_view["state"],
        json!("refunded_external"),
        "{seller_view}"
    );
    assert_eq!(seller_view["payment_reversed_at"], now);
    assert_eq!(
        seller_view["external_refund"]["transaction_id"],
        json!(REVERSAL_TXN)
    );
    let buyer_view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(buyer_view["payment_reversed_at"], now);
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            REVERSAL_TXN.to_string(),
            "Reversed".to_string(),
            TOTAL_MINOR
        )]
    );
    assert!(refund_notification_recipients(&pool, &order.id)
        .await
        .contains(&order.seller.pubky));

    // A partial reversal flags the order and keeps its state; a later
    // refund of the rest completes it and keeps the first flag instant.
    let order = paid_paypal_order(&app, "1AA00000AA0000002").await;
    let partial_reversal = with(
        with(
            refund("reversal-full.ipn", &order.id, "1AA00000AA0000002"),
            "mc_gross",
            "-40.00",
        ),
        "txn_id",
        "5MC93249NP7742999",
    );
    assert_eq!(post_ipn(&app, &partial_reversal).await, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("paid"), "{view}");
    assert_eq!(view["payment_reversed_at"], now);
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));

    app.clock.advance_seconds(300);
    let status = post_ipn(
        &app,
        &refund("refund-partial-2.ipn", &order.id, "1AA00000AA0000002"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["payment_reversed_at"], now);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn canceled_reversal_ipn_changes_nothing(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let partial_reversal = with(
        refund("reversal-full.ipn", &order.id, PAYMENT_TXN),
        "mc_gross",
        "-40.00",
    );
    assert_eq!(post_ipn(&app, &partial_reversal).await, StatusCode::OK);
    let flagged = snapshot(&app, &order).await;
    assert!(flagged["payment_reversed_at"].is_string());

    let status = post_ipn(&app, &fixture("canceled-reversal.ipn", &order.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_untouched(&app, &order, &flagged).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_applies_in_every_allowed_state(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, target) in [
        "paid",
        "ready_for_pickup",
        "shipped",
        "delivered",
        "completed",
        "return_received",
    ]
    .into_iter()
    .enumerate()
    {
        let payment_txn = format!("2BB00000BB000000{index}");
        let order = paid_paypal_order(&app, &payment_txn).await;
        match target {
            "paid" => {}
            // The pickup flow needs sealed pickup details and a pickup
            // listing; the refund path reads only the state.
            "ready_for_pickup" => {
                sqlx::query("UPDATE orders SET state = 'ready_for_pickup' WHERE id = $1::uuid")
                    .bind(&order.id)
                    .execute(&pool)
                    .await
                    .expect("state set");
            }
            "shipped" => ship(&app, &order).await,
            "delivered" => deliver(&app, &order).await,
            "completed" => complete(&app, &order).await,
            "return_received" => receive_return(&app, &order).await,
            _ => unreachable!(),
        }
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(view["state"], json!(target));

        let partial = with(
            refund("refund-partial-1.ipn", &order.id, &payment_txn),
            "txn_id",
            &format!("3CC00000CC000000{index}"),
        );
        assert_eq!(post_ipn(&app, &partial).await, StatusCode::OK);
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(view["state"], json!(target), "partial in {target}: {view}");
        assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));

        let rest = with(
            refund("refund-partial-2.ipn", &order.id, &payment_txn),
            "txn_id",
            &format!("4DD00000DD000000{index}"),
        );
        assert_eq!(post_ipn(&app, &rest).await, StatusCode::OK);
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(
            view["state"],
            json!("refunded_external"),
            "full in {target}: {view}"
        );
        assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn full_refund_ipn_resolves_a_received_return(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    receive_return(&app, &order).await;

    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(view["return_request"]["state"], json!("refunded"));
    assert_eq!(
        view["return_request"]["updated_at"],
        json!(format_timestamp(app.clock.now()))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_in_a_refused_state_records_nothing(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, target) in [
        "cancel_requested",
        "cancelled",
        "return_requested",
        "return_approved",
        "refunded_external",
    ]
    .into_iter()
    .enumerate()
    {
        let payment_txn = format!("5EE00000EE000000{index}");
        let order = paid_paypal_order(&app, &payment_txn).await;
        match target {
            "cancel_requested" | "cancelled" => {
                act(
                    &app,
                    &order.buyer.token,
                    "order.cancel_request",
                    &order.id,
                    json!({ "reason": "No longer needed" }),
                )
                .await;
                if target == "cancelled" {
                    act(
                        &app,
                        &order.seller.token,
                        "order.cancel_approve",
                        &order.id,
                        json!({}),
                    )
                    .await;
                }
            }
            "return_requested" => request_return(&app, &order).await,
            "return_approved" => {
                request_return(&app, &order).await;
                act(
                    &app,
                    &order.seller.token,
                    "return.approve",
                    &order.id,
                    json!({}),
                )
                .await;
            }
            "refunded_external" => {
                let status = post_ipn(
                    &app,
                    &with(
                        refund("refund-full.ipn", &order.id, &payment_txn),
                        "txn_id",
                        &format!("6FF00000FF000000{index}"),
                    ),
                )
                .await;
                assert_eq!(status, StatusCode::OK);
            }
            _ => unreachable!(),
        }
        let before = snapshot(&app, &order).await;
        assert_eq!(before["state"], json!(target));

        let reversal = with(
            refund("reversal-full.ipn", &order.id, &payment_txn),
            "mc_gross",
            "-40.00",
        );
        assert_eq!(post_ipn(&app, &reversal).await, StatusCode::OK);
        let partial = refund("refund-partial-1.ipn", &order.id, &payment_txn);
        assert_eq!(post_ipn(&app, &partial).await, StatusCode::OK);
        assert_untouched(&app, &order, &before).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn manual_record_after_a_partial_ipn_refund_is_refused(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    receive_return(&app, &order).await;
    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("return_received"));

    let (status, body) = execute(
        &app,
        &order.seller.token,
        &order_command(
            "refund.record_external",
            &order.id,
            view["revision"].as_i64().unwrap(),
            json!({ "amount_minor": 9_700, "transaction_id": "manual-evidence-123" }),
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The external refund cannot be recorded.")
    );

    let status = post_ipn(
        &app,
        &refund("refund-partial-2.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["return_request"]["state"], json!("refunded"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refunded_order_does_not_restock(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let quantities = || async {
        let row: (i64, i64, i64, String) = sqlx::query_as(
            "SELECT available_quantity, reserved_quantity, sold_quantity, state \
             FROM listings WHERE aggregate_id = $1",
        )
        .bind(listing_aggregate(&order.seller.pubky))
        .fetch_one(&pool)
        .await
        .expect("listing row");
        row
    };
    let sold = quantities().await;
    assert_eq!(sold, (0, 0, 1, "sold".to_string()));

    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(quantities().await, sold);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn completed_ipn_after_seller_confirmation_arms_refund_matching(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/confirm-received", order.id),
        Some(&order.seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(paypal_txn_id(&pool, &order.id).await, None);

    // PayPal's notification lands after the seller's confirmation.
    let status = post_ipn(&app, &fixture("completed.ipn", &order.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        paypal_txn_id(&pool, &order.id).await.as_deref(),
        Some(PAYMENT_TXN)
    );
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["fiat_verification"], json!("seller-attested"));
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);

    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");

    // A second verified payment id never replaces the first.
    let status = post_ipn(
        &app,
        &with(
            fixture("completed.ipn", &order.id),
            "txn_id",
            "8GG00000GG0000000",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        paypal_txn_id(&pool, &order.id).await.as_deref(),
        Some(PAYMENT_TXN)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_buyer_reported_reference_never_matches_a_refund(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/mark-paid", order.id),
        Some(&order.buyer.token),
        &json!({ "transaction_ref": PAYMENT_TXN }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/confirm-received", order.id),
        Some(&order.seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let before = snapshot(&app, &order).await;
    assert_eq!(before["fiat_transaction_ref"], json!(PAYMENT_TXN));
    assert_eq!(before["state"], json!("paid"));

    for body in [
        refund("refund-full.ipn", &order.id, PAYMENT_TXN),
        without(refund("refund-full.ipn", &order.id, PAYMENT_TXN), "custom"),
    ] {
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_ipn_that_fails_postback_validation_is_dropped(pool: PgPool) {
    let (app, _stripe, _paykit, ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    ipn.reject();
    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_untouched(&app, &order, &before).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreachable_verifier_asks_paypal_to_retry_a_refund(pool: PgPool) {
    let (app, _stripe, _paykit, ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;
    let body = refund("refund-full.ipn", &order.id, PAYMENT_TXN);

    ipn.set_unavailable(true);
    assert_eq!(post_ipn(&app, &body).await, StatusCode::SERVICE_UNAVAILABLE);
    assert_untouched(&app, &order, &before).await;

    ipn.set_unavailable(false);
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_database_failure_asks_paypal_to_retry(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;
    let body = refund("refund-full.ipn", &order.id, PAYMENT_TXN);

    sqlx::raw_sql(
        "CREATE FUNCTION refuse_gateway_refund() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'injected refund ledger failure'; END $$; \
         CREATE TRIGGER refuse_gateway_refund BEFORE INSERT ON order_gateway_refunds \
         FOR EACH ROW EXECUTE FUNCTION refuse_gateway_refund();",
    )
    .execute(&pool)
    .await
    .expect("failure trigger installs");
    assert_eq!(
        post_ipn(&app, &body).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_untouched(&app, &order, &before).await;
    assert!(refund_notification_recipients(&pool, &order.id)
        .await
        .is_empty());

    sqlx::raw_sql(
        "DROP TRIGGER refuse_gateway_refund ON order_gateway_refunds; \
         DROP FUNCTION refuse_gateway_refund();",
    )
    .execute(&pool)
    .await
    .expect("failure trigger drops");
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        refund_notification_recipients(&pool, &order.id).await.len(),
        2
    );
}
