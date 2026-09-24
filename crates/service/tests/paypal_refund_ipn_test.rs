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

async fn notification_recipients(pool: &PgPool, order_id: &str, kind: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT payload->>'recipient_pubky' FROM outbox \
         WHERE kind = 'notification.' || $2 AND payload->>'aggregate_id' = 'order:' || $1 \
         ORDER BY id",
    )
    .bind(order_id)
    .bind(kind)
    .fetch_all(pool)
    .await
    .expect("outbox read")
}

/// `(txn_id, reason, order_id, resolved)` for every inbox row.
async fn inbox_rows(pool: &PgPool) -> Vec<(String, String, Option<String>, bool)> {
    sqlx::query_as(
        "SELECT txn_id, reason, order_id::text, resolved_at IS NOT NULL \
         FROM gateway_refund_inbox ORDER BY received_at, txn_id",
    )
    .fetch_all(pool)
    .await
    .expect("inbox read")
}

async fn put_paypal_email(app: &TestApp, seller: &TestActor, email: Option<&str>) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": email.is_none(), "paypal_merchant_email": email }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
}

/// Holds the order row lock while `contenders` start, waits until `waiting`
/// sessions block on it, then releases it so they run against each other.
async fn race_on_order_lock(
    pool: &PgPool,
    order_id: &str,
    waiting: i64,
    contenders: Vec<tokio::task::JoinHandle<StatusCode>>,
) -> Vec<StatusCode> {
    let mut locker = pool.begin().await.expect("lock transaction");
    sqlx::query("SELECT id FROM orders WHERE id = $1::uuid FOR UPDATE")
        .bind(order_id)
        .execute(&mut *locker)
        .await
        .expect("order lock");
    let mut blocked = 0;
    for _ in 0..500 {
        blocked = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .expect("lock waiters");
        if blocked >= waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        blocked, waiting,
        "contenders did not block on the order lock"
    );
    locker.rollback().await.expect("lock released");
    let mut statuses = Vec::new();
    for contender in contenders {
        statuses.push(contender.await.expect("contender joins"));
    }
    statuses
}

fn spawn_ipn(app: &TestApp, fields: Fields) -> tokio::task::JoinHandle<StatusCode> {
    let router = app.router.clone();
    tokio::spawn(async move {
        send_bytes(
            router,
            "POST",
            "/v0/paypal/ipn",
            encode(&fields).into_bytes(),
        )
        .await
        .0
    })
}

async fn spawn_command(
    app: &TestApp,
    token: &str,
    kind: &str,
    order_id: &str,
    payload: Value,
) -> tokio::task::JoinHandle<StatusCode> {
    let revision = read_order(app, token, order_id).await["revision"]
        .as_i64()
        .expect("revision");
    let command = order_command(
        kind,
        order_id,
        revision,
        payload,
        COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
    );
    let router = app.router.clone();
    let token = token.to_string();
    tokio::spawn(async move {
        send(router, "POST", "/v1/commands", Some(&token), &command)
            .await
            .0
    })
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
    assert_eq!(view["gateway_refund_review_at"], Value::Null);
    assert_eq!(view["gateway_refund_unmatched"], json!(false));
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
    assert_eq!(view["gateway_refund_review_at"], Value::Null);
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

    // And for a held notification.
    let stray = without(
        refund("refund-full.ipn", &order.id, "9ZZ99999ZZ9999999"),
        "custom",
    );
    for _ in 0..2 {
        assert_eq!(post_ipn(&app, &stray).await, StatusCode::OK);
    }
    assert_eq!(inbox_rows(&pool).await.len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_unknown_parent_is_held_in_the_inbox(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let unknown = "9ZZ99999ZZ9999999";
    let missing_order = uuid::Uuid::new_v4().to_string();
    for (txn, body) in [
        // No custom: resolved by parent, which matches no order.
        (
            "1II00000II0000001",
            without(refund("refund-full.ipn", &order.id, unknown), "custom"),
        ),
        // The right order, but a parent it never received.
        (
            "1II00000II0000002",
            refund("refund-full.ipn", &order.id, unknown),
        ),
        // A custom order id that does not exist.
        (
            "1II00000II0000003",
            refund("refund-full.ipn", &missing_order, unknown),
        ),
    ] {
        assert_eq!(
            post_ipn(&app, &with(body, "txn_id", txn)).await,
            StatusCode::OK
        );
    }
    assert_untouched(&app, &order, &before).await;
    assert!(refund_notification_recipients(&pool, &order.id)
        .await
        .is_empty());
    assert_eq!(
        inbox_rows(&pool).await,
        vec![
            (
                "1II00000II0000001".to_string(),
                "unknown_parent".to_string(),
                None,
                false
            ),
            (
                "1II00000II0000002".to_string(),
                "unknown_parent".to_string(),
                Some(order.id.clone()),
                false
            ),
            (
                "1II00000II0000003".to_string(),
                "unknown_parent".to_string(),
                None,
                false
            ),
        ]
    );
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["gateway_refund_unmatched"], json!(true));
    // The inbox keeps what applying the notification needs, not the payer.
    let stored: Value =
        sqlx::query_scalar("SELECT fields FROM gateway_refund_inbox WHERE txn_id = $1")
            .bind("1II00000II0000002")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored["mc_gross"], json!("-137.00"));
    assert!(stored.get("payer_email").is_none());
    assert!(stored.get("first_name").is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_that_arrives_before_its_payment_is_applied_when_the_payment_lands(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;

    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "unknown_parent".to_string(),
            Some(order.id.clone()),
            false
        )]
    );
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("pending_payment"));

    // PayPal's retry of the payment notification lands afterwards.
    let status = post_ipn(&app, &fixture("completed.ipn", &order.id)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert!(view["receipt_id"].is_string());
    assert_eq!(view["gateway_refund_unmatched"], json!(false));
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "unknown_parent".to_string(),
            Some(order.id.clone()),
            true
        )]
    );
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "Refunded".to_string(),
            TOTAL_MINOR
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_whose_custom_order_does_not_own_the_parent_is_held(pool: PgPool) {
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
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "custom_mismatch".to_string(),
            Some(second.id.clone()),
            false
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_without_custom_resolves_by_parent(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let other = paid_paypal_order(&app, "1AA00000AA0000001").await;
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
    let other_view = read_order(&app, &other.buyer.token, &other.id).await;
    assert_eq!(other_view["state"], json!("paid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_currency_mismatch_is_held(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    for (index, currency) in ["EUR", "usd", ""].into_iter().enumerate() {
        let body = with(
            with(
                refund("refund-full.ipn", &order.id, PAYMENT_TXN),
                "mc_currency",
                currency,
            ),
            "txn_id",
            &format!("1JJ00000JJ000000{index}"),
        );
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert_untouched(&app, &order, &before).await;
    let rows = inbox_rows(&pool).await;
    assert_eq!(rows.len(), 3);
    assert!(rows
        .iter()
        .all(|row| row.1 == "currency_mismatch" && row.2.as_deref() == Some(order.id.as_str())));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_to_another_receiver_is_held(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let body = with(
        with(
            with(
                refund("refund-full.ipn", &order.id, PAYMENT_TXN),
                "receiver_email",
                "attacker@example.com",
            ),
            "business",
            "attacker@example.com",
        ),
        "receiver_id",
        "ATTACKER00001",
    );
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    assert_untouched(&app, &order, &before).await;
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "receiver_mismatch".to_string(),
            Some(order.id.clone()),
            false
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refunds_validate_against_the_receiver_snapshot_not_current_config(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let snapshot_of = |order_id: String| {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (Option<String>, Option<String>)>(
                "SELECT paypal_receiver_email, paypal_receiver_id FROM orders WHERE id = $1::uuid",
            )
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };

    // The seller changes the configured email after the payment.
    let changed = paid_paypal_order(&app, "1KK00000KK0000001").await;
    assert_eq!(
        snapshot_of(changed.id.clone()).await,
        (
            Some("merchant@example.com".to_string()),
            Some("S8XGHLYDW9T3S".to_string())
        )
    );
    put_paypal_email(&app, &changed.seller, Some("new-merchant@example.com")).await;
    let status = post_ipn(
        &app,
        &refund("refund-full.ipn", &changed.id, "1KK00000KK0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &changed.buyer.token, &changed.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");

    // The seller removes PayPal entirely.
    let cleared = paid_paypal_order(&app, "1KK00000KK0000002").await;
    put_paypal_email(&app, &cleared.seller, None).await;
    let status = post_ipn(
        &app,
        &with(
            refund("refund-full.ipn", &cleared.id, "1KK00000KK0000002"),
            "txn_id",
            "1KK00000KK0000003",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &cleared.buyer.token, &cleared.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");

    // The PayPal account renamed its email: the account id still matches.
    let renamed = paid_paypal_order(&app, "1KK00000KK0000004").await;
    let body = with(
        with(
            with(
                refund("refund-full.ipn", &renamed.id, "1KK00000KK0000004"),
                "receiver_email",
                "renamed@example.com",
            ),
            "business",
            "renamed@example.com",
        ),
        "txn_id",
        "1KK00000KK0000005",
    );
    assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    let view = read_order(&app, &renamed.buyer.token, &renamed.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert!(inbox_rows(&pool).await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_with_a_non_negative_or_malformed_gross_is_held(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    let grosses = [
        "137.00", "0.00", "-0.00", "-137", "-137.0", "-137.000", "-1,37", "", "--137.00",
    ];
    for (index, gross) in grosses.into_iter().enumerate() {
        let body = with(
            with(
                refund("refund-full.ipn", &order.id, PAYMENT_TXN),
                "mc_gross",
                gross,
            ),
            "txn_id",
            &format!("1LL00000LL000000{index}"),
        );
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK, "{gross}");
    }
    assert_untouched(&app, &order, &before).await;
    let rows = inbox_rows(&pool).await;
    assert_eq!(rows.len(), grosses.len());
    assert!(rows.iter().all(|row| row.1 == "amount_invalid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_transaction_ids(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let before = snapshot(&app, &order).await;

    // No usable `txn_id`: nothing can key a record (PayPal always sends one).
    let full = || refund("refund-full.ipn", &order.id, PAYMENT_TXN);
    let too_long = "A".repeat(65);
    for body in [
        with(full(), "txn_id", ""),
        with(full(), "txn_id", &too_long),
        with(full(), "txn_id", "2WF58163 VJ0384519"),
        without(full(), "txn_id"),
    ] {
        assert_eq!(post_ipn(&app, &body).await, StatusCode::OK);
    }
    assert!(inbox_rows(&pool).await.is_empty());

    // No usable parent: held under the refund's own id.
    for (txn, body) in [
        ("1MM00000MM0000001", with(full(), "parent_txn_id", "")),
        ("1MM00000MM0000002", without(full(), "parent_txn_id")),
    ] {
        assert_eq!(
            post_ipn(&app, &with(body, "txn_id", txn)).await,
            StatusCode::OK
        );
    }
    assert_untouched(&app, &order, &before).await;
    let rows = inbox_rows(&pool).await;
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row.1 == "missing_parent" && row.2.as_deref() == Some(order.id.as_str())));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_exceeding_the_total_is_recorded_and_flagged(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;

    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 40.00 recorded; a further 137.00 is past the total.
    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(
        view["gateway_refund_review_at"],
        json!(format_timestamp(app.clock.now()))
    );
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![
            (
                FULL_REFUND_TXN.to_string(),
                "Refunded".to_string(),
                TOTAL_MINOR
            ),
            (PARTIAL_1_TXN.to_string(), "Refunded".to_string(), 4_000),
        ]
    );
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
async fn canceled_reversal_restores_the_order_and_clears_the_flag(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let app = with_attestor(app);

    // A full chargeback on a shipped order, then PayPal cancels it.
    let order = paid_paypal_order(&app, "1NN00000NN0000001").await;
    ship(&app, &order).await;
    let status = post_ipn(
        &app,
        &refund("reversal-full.ipn", &order.id, "1NN00000NN0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        read_order(&app, &order.buyer.token, &order.id).await["state"],
        json!("refunded_external")
    );
    app.clock.advance_seconds(3_600);
    let status = post_ipn(
        &app,
        &refund("canceled-reversal.ipn", &order.id, "1NN00000NN0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let now = json!(format_timestamp(app.clock.now()));
    for token in [&order.buyer.token, &order.seller.token] {
        let view = read_order(&app, token, &order.id).await;
        assert_eq!(view["state"], json!("shipped"), "{view}");
        assert_eq!(view["payment_reversed_at"], Value::Null);
        assert_eq!(view["payment_reversal_cancelled_at"], now);
        assert_eq!(view["external_refund"], Value::Null);
        assert_eq!(view["gateway_refund_review_at"], Value::Null);
    }
    assert_eq!(
        order_events(&pool, &order.id, "refund.reversal_cancelled").await,
        vec!["paypal-ipn".to_string()]
    );
    assert_eq!(
        notification_recipients(&pool, &order.id, "payment_reversal_cancelled").await,
        vec![order.buyer.pubky.clone(), order.seller.pubky.clone()]
    );
    let outcomes: Vec<String> = sqlx::query_scalar(
        "SELECT outcome FROM attestation_annotations ORDER BY annotated_at, outcome",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(outcomes, vec!["refunded", "refund_reversal_cancelled"]);
    // The restored order continues normally.
    act(
        &app,
        &order.buyer.token,
        "fulfillment.confirm_delivery",
        &order.id,
        json!({}),
    )
    .await;

    // A partial refund and a partial reversal: cancelling the reversal
    // keeps the refund and reopens the order in its prior state.
    let order = paid_paypal_order(&app, "1NN00000NN0000002").await;
    receive_return(&app, &order).await;
    let status = post_ipn(
        &app,
        &refund("refund-partial-1.ipn", &order.id, "1NN00000NN0000002"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reversal = with(
        with(
            refund("reversal-full.ipn", &order.id, "1NN00000NN0000002"),
            "mc_gross",
            "-97.00",
        ),
        "txn_id",
        "5MC93249NP7742888",
    );
    assert_eq!(post_ipn(&app, &reversal).await, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["return_request"]["state"], json!("refunded"));
    let cancel = with(
        with(
            refund("canceled-reversal.ipn", &order.id, "1NN00000NN0000002"),
            "mc_gross",
            "97.00",
        ),
        "txn_id",
        "6ND04350PQ8853888",
    );
    assert_eq!(post_ipn(&app, &cancel).await, StatusCode::OK);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("return_received"), "{view}");
    assert_eq!(view["return_request"]["state"], json!("received"));
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!(PARTIAL_1_TXN)
    );
    assert_eq!(view["payment_reversed_at"], Value::Null);

    // A canceled reversal with nothing reversed is recorded for review.
    let order = paid_paypal_order(&app, "1NN00000NN0000003").await;
    let stray = with(
        refund("canceled-reversal.ipn", &order.id, "1NN00000NN0000003"),
        "txn_id",
        "6ND04350PQ8853777",
    );
    assert_eq!(post_ipn(&app, &stray).await, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["external_refund"], Value::Null);
    assert!(view["gateway_refund_review_at"].is_string());
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            "6ND04350PQ8853777".to_string(),
            "Canceled_Reversal".to_string(),
            TOTAL_MINOR
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_canceled_reversal_removes_the_reputation_penalty(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let attestor = test_attestor();

    // Seller A: a delivered order reversed, then the reversal canceled.
    let restored = paid_paypal_order(&app, "1OO00000OO0000001").await;
    deliver(&app, &restored).await;
    let status = post_ipn(
        &app,
        &refund("reversal-full.ipn", &restored.id, "1OO00000OO0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let status = post_ipn(
        &app,
        &refund("canceled-reversal.ipn", &restored.id, "1OO00000OO0000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Seller B: the same reversal, still standing.
    let reversed = paid_paypal_order(&app, "1OO00000OO0000002").await;
    deliver(&app, &reversed).await;
    let status = post_ipn(
        &app,
        &with(
            refund("reversal-full.ipn", &reversed.id, "1OO00000OO0000002"),
            "txn_id",
            "5MC93249NP7742666",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let signed = marketplace_service::workers::generate_due_stat_attestations(
        &app.pool,
        &attestor,
        app.clock.now(),
    )
    .await
    .expect("stat job runs");
    assert_eq!(signed, 2);
    let rate = |seller: String| {
        let pool = pool.clone();
        async move {
            let body: Value = sqlx::query_scalar(
                "SELECT body FROM seller_stat_attestations WHERE seller_pubky = $1",
            )
            .bind(seller)
            .fetch_one(&pool)
            .await
            .unwrap();
            body["completionRatePermille"].clone()
        }
    };
    assert_eq!(rate(restored.seller.pubky.clone()).await, json!(1000));
    assert_eq!(rate(reversed.seller.pubky.clone()).await, json!(500));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_ipn_is_recorded_in_every_paid_state(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, target) in [
        "paid",
        "ready_for_pickup",
        "shipped",
        "delivered",
        "completed",
        "return_received",
        "cancel_requested",
        "cancelled",
        "return_requested",
        "return_approved",
    ]
    .into_iter()
    .enumerate()
    {
        let payment_txn = format!("2BB00000BB00000{index:02}");
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
            _ => unreachable!(),
        }
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(view["state"], json!(target));
        let return_before = view["return_request"]["state"].clone();

        let partial = with(
            refund("refund-partial-1.ipn", &order.id, &payment_txn),
            "txn_id",
            &format!("3CC00000CC00000{index:02}"),
        );
        assert_eq!(post_ipn(&app, &partial).await, StatusCode::OK);
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(view["state"], json!(target), "partial in {target}: {view}");
        assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));
        assert_eq!(view["return_request"]["state"], return_before);
        assert_eq!(view["gateway_refund_review_at"], Value::Null);

        let rest = with(
            refund("refund-partial-2.ipn", &order.id, &payment_txn),
            "txn_id",
            &format!("4DD00000DD00000{index:02}"),
        );
        assert_eq!(post_ipn(&app, &rest).await, StatusCode::OK);
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        assert_eq!(
            view["state"],
            json!("refunded_external"),
            "full in {target}: {view}"
        );
        assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
        if return_before.is_string() {
            assert_eq!(view["return_request"]["state"], json!("refunded"));
        }
        assert_eq!(view["gateway_refund_review_at"], Value::Null);
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
async fn refund_ipn_on_a_manually_refunded_order_is_recorded_for_review(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    receive_return(&app, &order).await;
    act(
        &app,
        &order.seller.token,
        "refund.record_external",
        &order.id,
        json!({ "amount_minor": TOTAL_MINOR, "transaction_id": "manual-evidence-123" }),
    )
    .await;

    // PayPal then reports the refund the seller already recorded by hand.
    let status = post_ipn(&app, &refund("refund-full.ipn", &order.id, PAYMENT_TXN)).await;
    assert_eq!(status, StatusCode::OK);
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!("manual-evidence-123")
    );
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    assert!(view["gateway_refund_review_at"].is_string(), "{view}");
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "Refunded".to_string(),
            TOTAL_MINOR
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_manual_record_closes_a_return_alongside_ipn_partials(pool: PgPool) {
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
    assert_eq!(view["return_request"]["state"], json!("received"));

    let record = |amount: i64, number: u64, revision: i64| {
        order_command(
            "refund.record_external",
            &order.id,
            revision,
            json!({ "amount_minor": amount, "transaction_id": "manual-evidence-123" }),
            number,
        )
    };
    // A record below what PayPal already returned is refused.
    let revision = view["revision"].as_i64().unwrap();
    let (status, body) = execute(
        &app,
        &order.seller.token,
        &record(
            3_999,
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
            revision,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The external refund cannot be recorded.")
    );

    // Settling at the PayPal amount closes the return.
    let (status, body) = execute(
        &app,
        &order.seller.token,
        &record(
            4_000,
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
            revision,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["return_request"]["state"], json!("refunded"));
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));
    assert_eq!(
        view["external_refund"]["transaction_id"],
        json!("manual-evidence-123")
    );

    // A manual record already in place still refuses a second one.
    let (status, _) = execute(
        &app,
        &order.seller.token,
        &record(
            TOTAL_MINOR,
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
            view["revision"].as_i64().unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
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
    // Held, not applied, and visible on the order.
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "unknown_parent".to_string(),
            Some(order.id.clone()),
            false
        )]
    );
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["gateway_refund_unmatched"], json!(true));
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
    assert!(inbox_rows(&pool).await.is_empty());
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_simultaneous_partial_refunds_both_record(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    let statuses = race_on_order_lock(
        &pool,
        &order.id,
        2,
        vec![
            spawn_ipn(&app, refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN)),
            spawn_ipn(&app, refund("refund-partial-2.ipn", &order.id, PAYMENT_TXN)),
        ],
    )
    .await;
    assert_eq!(statuses, vec![StatusCode::OK, StatusCode::OK]);
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(ledger_rows(&pool, &order.id).await.len(), 2);
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
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_racing_a_cancel_approval_is_recorded(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = paid_paypal_order(&app, PAYMENT_TXN).await;
    act(
        &app,
        &order.buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "No longer needed" }),
    )
    .await;
    let approve = spawn_command(
        &app,
        &order.seller.token,
        "order.cancel_approve",
        &order.id,
        json!({}),
    )
    .await;
    let statuses = race_on_order_lock(
        &pool,
        &order.id,
        2,
        vec![
            spawn_ipn(&app, refund("refund-partial-1.ipn", &order.id, PAYMENT_TXN)),
            approve,
        ],
    )
    .await;
    assert_eq!(statuses[0], StatusCode::OK);
    // Whichever took the lock first, the refund is on the order.
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["external_refund"]["amount_minor"], json!(4_000));
    assert_eq!(ledger_rows(&pool, &order.id).await.len(), 1);
    match statuses[1] {
        StatusCode::OK => assert_eq!(view["state"], json!("cancelled")),
        StatusCode::CONFLICT => assert_eq!(view["state"], json!("cancel_requested")),
        other => panic!("unexpected approval status {other}"),
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_racing_each_return_step_is_recorded(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    for (index, (kind, before)) in [
        ("return.approve", "return_requested"),
        ("return.receive", "return_approved"),
    ]
    .into_iter()
    .enumerate()
    {
        let payment_txn = format!("2PP00000PP000000{index}");
        let order = paid_paypal_order(&app, &payment_txn).await;
        request_return(&app, &order).await;
        if before == "return_approved" {
            act(
                &app,
                &order.seller.token,
                "return.approve",
                &order.id,
                json!({}),
            )
            .await;
        }
        let step = spawn_command(&app, &order.seller.token, kind, &order.id, json!({})).await;
        let full = with(
            refund("refund-full.ipn", &order.id, &payment_txn),
            "txn_id",
            &format!("2QQ00000QQ000000{index}"),
        );
        let statuses =
            race_on_order_lock(&pool, &order.id, 2, vec![spawn_ipn(&app, full), step]).await;
        assert_eq!(statuses[0], StatusCode::OK);
        let view = read_order(&app, &order.buyer.token, &order.id).await;
        // The full refund resolves the return from either side of the step.
        assert_eq!(view["state"], json!("refunded_external"), "{kind}: {view}");
        assert_eq!(view["return_request"]["state"], json!("refunded"));
        assert_eq!(ledger_rows(&pool, &order.id).await.len(), 1);
        assert!(
            matches!(statuses[1], StatusCode::OK | StatusCode::CONFLICT),
            "{kind}: {}",
            statuses[1]
        );
    }
}

/// Waits until at least `waiting` sessions block on a lock, up to five
/// seconds, and returns how many did.
async fn lock_waiters(pool: &PgPool, waiting: i64) -> i64 {
    let mut blocked = 0;
    for _ in 0..250 {
        blocked = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .expect("lock waiters");
        if blocked >= waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    blocked
}

/// Review schedule 1: the refund finds no payment yet and is about to hold
/// itself when the payment settles. The payment's settlement must see the
/// held refund, or the refund must see the payment.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_racing_its_payment_settlement_is_never_stranded(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;

    // An uncommitted row with the refund's key parks the refund at its inbox
    // write, after it found no owner for the payment.
    let mut locker = pool.begin().await.expect("lock transaction");
    sqlx::query(
        "INSERT INTO gateway_refund_inbox \
         (txn_id, payment_status, reason, fields, received_at) \
         VALUES ($1, 'Refunded', 'unknown_parent', '{}'::jsonb, now())",
    )
    .bind(FULL_REFUND_TXN)
    .execute(&mut *locker)
    .await
    .expect("parking row");
    let refund_ipn = spawn_ipn(&app, refund("refund-full.ipn", &order.id, PAYMENT_TXN));
    let parked = lock_waiters(&pool, 1).await;
    let payment_ipn = spawn_ipn(&app, fixture("completed.ipn", &order.id));
    let both = lock_waiters(&pool, 2).await;
    locker.rollback().await.expect("release");
    assert_eq!(refund_ipn.await.expect("refund joins"), StatusCode::OK);
    assert_eq!(payment_ipn.await.expect("payment joins"), StatusCode::OK);

    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert_eq!(view["gateway_refund_unmatched"], json!(false));
    assert_eq!(
        ledger_rows(&pool, &order.id).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "Refunded".to_string(),
            TOTAL_MINOR
        )]
    );
    assert!(inbox_rows(&pool).await.iter().all(|row| row.3));
    // The payment waited on the refund's payment lock.
    assert_eq!((parked, both), (1, 2));
}

/// Review schedule 2: a full refund arrives while the payment is settling.
/// It must not see the payment before the order is paid, so it moves the
/// paid order to `refunded_external` instead of flagging a pending one.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_full_refund_racing_payment_confirmation_moves_the_paid_order(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;

    // Hold the payment row: the payment IPN parks at its payment lock.
    let mut locker = pool.begin().await.expect("lock transaction");
    sqlx::query("SELECT id FROM payments WHERE order_id = $1::uuid FOR UPDATE")
        .bind(&order.id)
        .execute(&mut *locker)
        .await
        .expect("payment lock");
    let payment_ipn = spawn_ipn(&app, fixture("completed.ipn", &order.id));
    let parked = lock_waiters(&pool, 1).await;
    let refund_ipn = spawn_ipn(&app, refund("refund-full.ipn", &order.id, PAYMENT_TXN));
    let both = lock_waiters(&pool, 2).await;
    locker.rollback().await.expect("release");
    assert_eq!(payment_ipn.await.expect("payment joins"), StatusCode::OK);
    assert_eq!(refund_ipn.await.expect("refund joins"), StatusCode::OK);

    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
    assert!(view["receipt_id"].is_string());
    assert_eq!(view["gateway_refund_review_at"], Value::Null);
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_MINOR));
    // The refund waited on the payment's lock.
    assert_eq!((parked, both), (1, 2));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_held_refund_is_re_evaluated_when_its_payment_lands(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order = pending_paypal_order(&app).await;

    let wrong_currency = with(
        refund("refund-full.ipn", &order.id, PAYMENT_TXN),
        "mc_currency",
        "EUR",
    );
    assert_eq!(post_ipn(&app, &wrong_currency).await, StatusCode::OK);
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "unknown_parent".to_string(),
            Some(order.id.clone()),
            false
        )]
    );

    // Once the payment is settled, the held refund's real problem shows.
    let status = post_ipn(&app, &fixture("completed.ipn", &order.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        inbox_rows(&pool).await,
        vec![(
            FULL_REFUND_TXN.to_string(),
            "currency_mismatch".to_string(),
            Some(order.id.clone()),
            false
        )]
    );
    let view = read_order(&app, &order.seller.token, &order.id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["gateway_refund_unmatched"], json!(true));
    // A payment retry re-evaluates the same row; nothing multiplies.
    let status = post_ipn(&app, &fixture("completed.ipn", &order.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(inbox_rows(&pool).await.len(), 1);
    assert!(ledger_rows(&pool, &order.id).await.is_empty());
}
