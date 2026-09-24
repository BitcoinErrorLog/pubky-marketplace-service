//! Digital delivery, service Wave 1 slice 2 (digital-delivery-design.md
//! §3.4–§3.6, §4.2, §6 rows B3, B4, C4, C5, D1, D3–D11, E1–E4, E6, E9, G1):
//! digital checkout, the confirm-time pin on every confirming rail, the
//! buyer's download read, and the physical edges a digital order refuses.
//! Payments are confirmed through the real sandbox command, the Paykit
//! worker poll against the Paykit double, and verified PayPal IPN fixtures.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::paykit_review::{poll_now, status_confirmed};
use common::*;
use marketplace_domain::commands::DigitalDeliveryKind;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::digital::{version_aad, DigitalKeys};
use marketplace_service::http::build_router;
use marketplace_service::payments::order_reference;
use marketplace_service::workers::{
    assume_due_deliveries, complete_due_delivered_orders, drain_outbox, expire_due_payment_windows,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use tower::util::ServiceExt;
use uuid::Uuid;

const TEXT_V1: &str = "LICENCE-V1-SENTINEL";
const TEXT_V2: &str = "LICENCE-V2-SENTINEL";
const DELIVERABLE_ID: &str = "4f1c0e7a2b6d4c85a9e3f1027b5d6c38";
const FILE_KEY: &str = "b7c1d2e3f4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0f";
const FILE_IV: &str = "0a1b2c3d4e5f60718293a4b5";
const PLAINTEXT_BLAKE3: &str = "1111111111111111111111111111111111111111111111111111111111111111";
/// The gross of `tests/fixtures/paypal_ipn/completed.ipn`.
const PAYPAL_TOTAL_MINOR: i64 = 13_700;

static NUMBER: AtomicU64 = AtomicU64::new(1);

fn next() -> u64 {
    NUMBER.fetch_add(1, Ordering::Relaxed)
}

fn with_digital(app: TestApp) -> TestApp {
    let state = app.state.clone().with_digital(Some(test_digital_keys()));
    TestApp {
        router: build_router(state.clone()),
        pool: app.pool,
        clock: app.clock,
        state,
    }
}

fn aggregate(seller: &str, listing_id: &str) -> String {
    format!("listing:{seller}_{listing_id}")
}

/// Registers `listing_id` for `seller` with the given methods and price.
async fn register(
    app: &TestApp,
    seller: &TestActor,
    listing_id: &str,
    methods: Value,
    unit_price: Value,
    quantity: i64,
) {
    let mut command = register_command(&seller.pubky, quantity);
    command["command_id"] = json!(indexed_command_id(0xd400, next()));
    command["aggregate_id"] = json!(aggregate(&seller.pubky, listing_id));
    command["payload"]["listing_id"] = json!(listing_id);
    command["payload"]["fulfillment_methods"] = methods;
    command["payload"]["unit_price"] = unit_price;
    let (status, body) = execute(app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
}

fn usd(amount_minor: i64) -> Value {
    json!({ "amount_minor": amount_minor, "currency": "USD", "exponent": 2 })
}

fn sat(amount_minor: i64) -> Value {
    json!({ "amount_minor": amount_minor, "currency": "SAT", "exponent": 0 })
}

async fn set_delivery(
    app: &TestApp,
    seller: &TestActor,
    listing_id: &str,
    expected_version: i64,
    delivery: Value,
) -> (StatusCode, Value) {
    execute(
        app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": indexed_command_id(0xd500, next()),
            "aggregate_id": aggregate(&seller.pubky, listing_id),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "digital_delivery.set",
            "payload": { "expected_version": expected_version, "delivery": delivery },
        }),
    )
    .await
}

async fn set_text(app: &TestApp, seller: &TestActor, listing_id: &str, version: i64, text: &str) {
    let (status, body) = set_delivery(
        app,
        seller,
        listing_id,
        version,
        json!({ "kind": "text", "text": text }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set failed: {body}");
}

async fn listing_revision(app: &TestApp, seller: &str, listing_id: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(aggregate(seller, listing_id))
        .fetch_one(&app.pool)
        .await
        .expect("listing row")
}

async fn checkout(
    app: &TestApp,
    buyer: &TestActor,
    lines: Vec<(String, String, Option<&str>)>,
    with_address: bool,
) -> (StatusCode, Value) {
    let command_id = indexed_command_id(0xd600, next());
    let mut resolved = Vec::new();
    for (seller, listing_id, fulfillment) in lines {
        let mut line = json!({
            "listing_aggregate_id": aggregate(&seller, &listing_id),
            "expected_revision": listing_revision(app, &seller, &listing_id).await,
            "quantity": 1,
        });
        if let Some(fulfillment) = fulfillment {
            line["fulfillment"] = json!(fulfillment);
        }
        resolved.push(line);
    }
    let mut payload = json!({ "lines": resolved, "guarantee_policy_version": 1 });
    if with_address {
        payload["delivery_address"] = json!({
            "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
            "city": "New York", "region": "NY", "postal_code": "10001", "country_code": "US",
        });
    }
    execute(
        app,
        &buyer.token,
        &json!({
            "version": 1,
            "command_id": command_id,
            "aggregate_id": format!("checkout:{command_id}"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "checkout.create",
            "payload": payload,
        }),
    )
    .await
}

struct DigitalOrder {
    order_id: String,
    payment_id: String,
}

async fn digital_checkout(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listing_id: &str,
) -> DigitalOrder {
    let (status, body) = checkout(
        app,
        buyer,
        vec![(
            seller.pubky.clone(),
            listing_id.to_string(),
            Some("digital"),
        )],
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "digital checkout failed: {body}");
    DigitalOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    }
}

async fn sandbox_confirm(app: &TestApp, buyer: &TestActor, order: &DigitalOrder) -> Value {
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "confirmed", 1, next()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sandbox confirm failed: {body}");
    body
}

async fn order_facts(app: &TestApp, order_id: &str) -> (String, bool, String, Option<String>) {
    sqlx::query_as(
        "SELECT o.state, o.receipt_id IS NOT NULL, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order facts")
}

async fn read_delivery(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, String, Value) {
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/orders/{order_id}/digital-delivery"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request executes");
    let status = response.status();
    let cache = response
        .headers()
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("body")
        .to_bytes();
    (
        status,
        cache,
        serde_json::from_slice(&bytes).expect("json body"),
    )
}

async fn access_rows(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM order_digital_access WHERE order_id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("access rows")
}

async fn order_revision(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order revision")
}

async fn order_action(
    app: &TestApp,
    token: &str,
    kind: &str,
    order_id: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let revision = order_revision(app, order_id).await;
    execute(
        app,
        token,
        &order_command(kind, order_id, revision, payload, next()),
    )
    .await
}

async fn stock(app: &TestApp, seller: &str, listing_id: &str) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity FROM listings \
         WHERE aggregate_id = $1",
    )
    .bind(aggregate(seller, listing_id))
    .fetch_one(&app.pool)
    .await
    .expect("stock")
}

/// A text listing, a paid-by-sandbox all-instant order on it.
async fn delivered_text_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> DigitalOrder {
    register(app, seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(app, seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(app, seller, buyer, "guide_01").await;
    sandbox_confirm(app, buyer, &order).await;
    order
}

async fn keyed_app(pool: PgPool) -> (TestApp, DeliverableServer) {
    test_app_with_digital(pool, Some(test_digital_keys()), Config::for_tests()).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mixed_cart_splits_digital_into_own_order(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    register(
        &app,
        &seller,
        "boots_02",
        json!(["shipping"]),
        usd(4_000),
        5,
    )
    .await;

    // Without an address the shipped group refuses the whole checkout.
    let lines = || {
        vec![
            (
                seller.pubky.clone(),
                "guide_01".to_string(),
                Some("digital"),
            ),
            (seller.pubky.clone(), "boots_02".to_string(), None),
        ]
    };
    let (status, body) = checkout(&app, &buyer, lines(), false).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let (status, body) = checkout(&app, &buyer, lines(), true).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let orders = body["result"]["orders"].as_array().expect("orders");
    assert_eq!(orders.len(), 2, "one order per (seller, method)");
    let digital = orders
        .iter()
        .find(|order| order["fulfillment"] == json!("digital"))
        .expect("digital order");
    let shipped = orders
        .iter()
        .find(|order| order["fulfillment"] == json!("shipping"))
        .expect("shipped order");
    assert_eq!(digital["lines"][0]["digital_kind"], json!("text"));
    assert!(shipped["lines"][0].get("digital_kind").is_none());
    let rows: Vec<(String, bool, i64)> = sqlx::query_as(
        "SELECT fulfillment, delivery_address IS NULL, shipping_minor FROM orders \
         ORDER BY fulfillment",
    )
    .fetch_all(&app.pool)
    .await
    .expect("orders");
    assert_eq!(
        rows,
        vec![
            ("digital".to_string(), true, 0),
            ("shipping".to_string(), false, 1_200)
        ],
        "only the shipped order carries the address and the shipping charge"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_refused_when_deliverable_missing(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    let lines = vec![(
        seller.pubky.clone(),
        "guide_01".to_string(),
        Some("digital"),
    )];
    let (status, body) = checkout(&app, &buyer, lines.clone(), false).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_delivery_not_ready"));
    // Manual kinds are not sold until the email and mark-delivered paths
    // exist.
    let (status, body) =
        set_delivery(&app, &seller, "guide_01", 0, json!({ "kind": "email" })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = checkout(&app, &buyer, lines.clone(), false).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("digital_delivery_unavailable")
    );
    set_text(&app, &seller, "guide_01", 1, TEXT_V1).await;
    let (status, body) = checkout(&app, &buyer, lines, false).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn clear_refused_while_pending_or_live(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let clear = |version: i64| {
        json!({
            "version": 1,
            "command_id": indexed_command_id(0xd700, next()),
            "aggregate_id": aggregate(&seller.pubky, "guide_01"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "digital_delivery.clear",
            "payload": { "expected_version": version },
        })
    };
    let order = digital_checkout(&app, &seller, &buyer, "guide_01").await;
    // Pending payment.
    let (status, body) = execute(&app, &seller.token, &clear(1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_delivery_in_use"));
    // Paid and still downloading.
    sandbox_confirm(&app, &buyer, &order).await;
    let (status, body) = execute(&app, &seller.token, &clear(1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_delivery_in_use"));
    // Ended: the refund is recorded, and the listing can be cleared.
    let (status, body) = order_action(
        &app,
        &seller.token,
        "refund.record_external",
        &order.order_id,
        json!({ "amount_minor": 900, "transaction_id": "manual-refund-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(&app, &seller.token, &clear(1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn kind_change_refused_while_pending_or_live(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(&app, &seller, &buyer, "guide_01").await;
    let link = json!({ "kind": "link", "url": "https://example.com/course" });
    let (status, body) = set_delivery(&app, &seller, "guide_01", 1, link.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_delivery_in_use"));
    sandbox_confirm(&app, &buyer, &order).await;
    let (status, body) = set_delivery(&app, &seller, "guide_01", 1, link.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    // A same-kind new version is always allowed.
    set_text(&app, &seller, "guide_01", 1, TEXT_V2).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn new_version_keeps_existing_pins(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let early = new_actor(&app).await;
    let late = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let paid_v1 = digital_checkout(&app, &seller, &early, "guide_01").await;
    sandbox_confirm(&app, &early, &paid_v1).await;
    let pending = digital_checkout(&app, &seller, &late, "guide_01").await;
    set_text(&app, &seller, "guide_01", 1, TEXT_V2).await;
    sandbox_confirm(&app, &late, &pending).await;

    let pin = |order_id: String| {
        let pool = app.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM order_digital_pins WHERE order_id = $1::uuid",
            )
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("pin")
        }
    };
    assert_eq!(
        pin(paid_v1.order_id.clone()).await,
        1,
        "live orders keep their pin"
    );
    assert_eq!(
        pin(pending.order_id.clone()).await,
        2,
        "pending orders pin the current version"
    );
    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM listing_digital_versions WHERE listing_aggregate_id = $1 ORDER BY version",
    )
    .bind(aggregate(&seller.pubky, "guide_01"))
    .fetch_all(&app.pool)
    .await
    .expect("versions");
    assert_eq!(versions, vec![1, 2], "a pinned version is never deleted");
    let (_, owner) = {
        let (status, body) = send(
            app.router.clone(),
            "GET",
            &format!(
                "/v1/listings/{}/digital-delivery",
                aggregate(&seller.pubky, "guide_01")
            ),
            Some(&seller.token),
            &Value::Null,
        )
        .await;
        (status, body)
    };
    assert_eq!(
        owner["pinned_versions"],
        json!([{ "version": 1, "live_orders": 1 }, { "version": 2, "live_orders": 1 }])
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn instant_confirm_delivers_on_every_path(pool: PgPool) {
    let (app, _stripe, paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let app = with_digital(app);

    // Sandbox.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = delivered_text_order(&app, &seller, &buyer).await;
    let (state, receipt, payment, _) = order_facts(&app, &order.order_id).await;
    assert_eq!(
        (state.as_str(), receipt, payment.as_str()),
        ("delivered", true, "confirmed")
    );
    assert_eq!(
        stock(&app, &seller.pubky, "guide_01").await,
        (4, 0, 1),
        "one stock conversion"
    );

    // Paykit (Bitcoin).
    let btc_seller = new_actor(&app).await;
    let btc_buyer = new_actor(&app).await;
    let (order_id, _reference) =
        bitcoin_digital_order(&app, &paykit, &btc_seller, &btc_buyer).await;
    let reference = order_reference(Uuid::parse_str(&order_id).expect("order uuid"));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let (state, receipt, payment, _) = order_facts(&app, &order_id).await;
    assert_eq!(
        (state.as_str(), receipt, payment.as_str()),
        ("delivered", true, "confirmed")
    );
    assert_eq!(stock(&app, &btc_seller.pubky, "guide_01").await, (4, 0, 1));

    // PayPal IPN.
    let pp_seller = new_actor(&app).await;
    let pp_buyer = new_actor(&app).await;
    let pp_order = paypal_digital_order(&app, &pp_seller, &pp_buyer).await;
    assert_eq!(
        post_completed_ipn(&app, &pp_order.order_id).await,
        StatusCode::OK
    );
    let (state, receipt, _, _) = order_facts(&app, &pp_order.order_id).await;
    assert_eq!((state.as_str(), receipt), ("delivered", true));

    for (buyer, order_id) in [
        (&buyer, order.order_id.as_str()),
        (&btc_buyer, order_id.as_str()),
        (&pp_buyer, pp_order.order_id.as_str()),
    ] {
        let delivered: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_delivered' \
             AND payload->>'aggregate_id' = 'order:' || $1 AND payload->>'recipient_pubky' = $2",
        )
        .bind(order_id)
        .bind(&buyer.pubky)
        .fetch_one(&app.pool)
        .await
        .expect("notification");
        assert_eq!(delivered, 1, "the buyer is told once for {order_id}");
    }
}

async fn bitcoin_digital_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    paykit.set_allocation_mode("exclusive");
    common::paykit_review::enable_bitcoin(app, paykit, seller).await;
    register(app, seller, "guide_01", json!(["digital"]), sat(50_000), 5).await;
    set_text(app, seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(app, seller, buyer, "guide_01").await;
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
    (order.order_id, order.payment_id)
}

async fn paypal_digital_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> DigitalOrder {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    register(
        app,
        seller,
        "guide_01",
        json!(["digital"]),
        usd(PAYPAL_TOTAL_MINOR),
        5,
    )
    .await;
    set_text(app, seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(app, seller, buyer, "guide_01").await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    order
}

async fn post_completed_ipn(app: &TestApp, order_id: &str) -> StatusCode {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/completed.ipn",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).expect("completed.ipn fixture");
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in url::form_urlencoded::parse(text.trim_end().as_bytes()) {
        let value = if value == "{{ORDER_ID}}" {
            order_id.into()
        } else {
            value
        };
        serializer.append_pair(&name, &value);
    }
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        serializer.finish().into_bytes(),
    )
    .await;
    status
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn late_completion_instant_ends_delivered(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let app = with_digital(app);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = bitcoin_digital_order(&app, &paykit, &seller, &buyer).await;
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    assert_eq!(order_facts(&app, &order_id).await.0, "cancelled");
    let reference = order_reference(Uuid::parse_str(&order_id).expect("order uuid"));
    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);
    let (state, receipt, payment, reason) = order_facts(&app, &order_id).await;
    assert_eq!(
        (state.as_str(), receipt, payment.as_str()),
        ("delivered", true, "confirmed")
    );
    assert!(reason.is_none());
    let pins: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_digital_pins WHERE order_id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .expect("pins");
    assert_eq!(pins, 1, "late completion pins the line");
    assert_eq!(stock(&app, &seller.pubky, "guide_01").await, (4, 0, 1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refund_required_never_delivers(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let app = with_digital(app);
    let seller = new_actor(&app).await;
    let first = new_actor(&app).await;
    paykit.set_allocation_mode("exclusive");
    common::paykit_review::enable_bitcoin(&app, &paykit, &seller).await;
    register(
        &app,
        &seller,
        "guide_01",
        json!(["digital"]),
        sat(50_000),
        1,
    )
    .await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let first_order = digital_checkout(&app, &seller, &first, "guide_01").await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", first_order.order_id),
        Some(&first.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation");
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    expire_due_payment_windows(&app.state, after)
        .await
        .expect("expire");
    // The last copy sells to a second buyer.
    app.clock.set(after);
    let second = new_actor(&app).await;
    let second_order = digital_checkout(&app, &seller, &second, "guide_01").await;
    sandbox_confirm(&app, &second, &second_order).await;
    // The first buyer's money arrives late: nothing left to deliver.
    let reference = order_reference(Uuid::parse_str(&first_order.order_id).expect("uuid"));
    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);
    let (state, receipt, payment, reason) = order_facts(&app, &first_order.order_id).await;
    assert_eq!((state.as_str(), receipt), ("cancelled", false));
    assert_eq!(
        (payment.as_str(), reason.as_deref()),
        ("manual_review", Some("refund_required"))
    );
    let pins: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_digital_pins WHERE order_id = $1::uuid")
            .bind(&first_order.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("pins");
    assert_eq!(pins, 0);
    let (status, _, body) = read_delivery(&app, &first.token, &first_order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_paid"));
}

/// Removes the listing's current version behind the command guard, the
/// only way a confirm can find nothing to pin.
async fn drop_current_version(app: &TestApp, seller: &str) {
    sqlx::query(
        "DELETE FROM listing_digital_versions WHERE listing_aggregate_id = $1 \
         AND superseded_at IS NULL",
    )
    .bind(aggregate(seller, "guide_01"))
    .execute(&app.pool)
    .await
    .expect("drop current version");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn confirm_with_nothing_to_pin_refund_required(pool: PgPool) {
    let (app, _stripe, paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let app = with_digital(app);

    // Paykit.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = bitcoin_digital_order(&app, &paykit, &seller, &buyer).await;
    assert_eq!(
        stock(&app, &seller.pubky, "guide_01").await,
        (4, 1, 0),
        "held at bind"
    );
    drop_current_version(&app, &seller.pubky).await;
    let reference = order_reference(Uuid::parse_str(&order_id).expect("uuid"));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let (state, receipt, payment, reason) = order_facts(&app, &order_id).await;
    assert_eq!((state.as_str(), receipt), ("cancelled", false));
    assert_eq!(
        (payment.as_str(), reason.as_deref()),
        ("manual_review", Some("refund_required"))
    );
    assert_eq!(
        stock(&app, &seller.pubky, "guide_01").await,
        (5, 0, 0),
        "hold released"
    );
    for recipient in [&buyer.pubky, &seller.pubky] {
        let notified: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.payment_refund_required' \
             AND payload->>'aggregate_id' = 'order:' || $1 AND payload->>'recipient_pubky' = $2",
        )
        .bind(&order_id)
        .bind(recipient)
        .fetch_one(&app.pool)
        .await
        .expect("notification");
        assert_eq!(notified, 1, "{recipient} notified");
    }

    // PayPal IPN.
    let pp_seller = new_actor(&app).await;
    let pp_buyer = new_actor(&app).await;
    let pp_order = paypal_digital_order(&app, &pp_seller, &pp_buyer).await;
    drop_current_version(&app, &pp_seller.pubky).await;
    assert_eq!(
        post_completed_ipn(&app, &pp_order.order_id).await,
        StatusCode::OK
    );
    let (state, receipt, payment, reason) = order_facts(&app, &pp_order.order_id).await;
    assert_eq!((state.as_str(), receipt), ("cancelled", false));
    assert_eq!(
        (payment.as_str(), reason.as_deref()),
        ("manual_review", Some("refund_required"))
    );

    // Sandbox has no money to route: the confirm is refused before any write.
    let sb_seller = new_actor(&app).await;
    let sb_buyer = new_actor(&app).await;
    register(
        &app,
        &sb_seller,
        "guide_01",
        json!(["digital"]),
        usd(900),
        5,
    )
    .await;
    set_text(&app, &sb_seller, "guide_01", 0, TEXT_V1).await;
    let sb_order = digital_checkout(&app, &sb_seller, &sb_buyer, "guide_01").await;
    drop_current_version(&app, &sb_seller.pubky).await;
    let (status, body) = execute(
        &app,
        &sb_buyer.token,
        &payment_command(&sb_order.payment_id, 1, "confirmed", 1, next()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_delivery_not_ready"));
    assert!(!order_facts(&app, &sb_order.order_id).await.1, "no receipt");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn nothing_to_pin_not_retried(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let app = with_digital(app);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = bitcoin_digital_order(&app, &paykit, &seller, &buyer).await;
    drop_current_version(&app, &seller.pubky).await;
    let reference = order_reference(Uuid::parse_str(&order_id).expect("uuid"));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let payment_id = order_facts_payment_id(&app.pool, &order_id).await;
    let count_events = || async {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE aggregate_id IN ($1, $2)")
            .bind(format!("order:{order_id}"))
            .bind(format!("payment:{payment_id}"))
            .fetch_one(&app.pool)
            .await
            .expect("events")
    };
    let before = count_events().await;
    for minutes in [1, 5, 30] {
        poll_now(&app, app.clock.now() + chrono::Duration::minutes(minutes)).await;
    }
    assert_eq!(
        count_events().await,
        before,
        "the observation was consumed; nothing re-runs"
    );
    let (state, receipt, payment, reason) = order_facts(&app, &order_id).await;
    assert_eq!((state.as_str(), receipt), ("cancelled", false));
    assert_eq!(
        (payment.as_str(), reason.as_deref()),
        ("manual_review", Some("refund_required"))
    );
}

async fn order_facts_payment_id(pool: &PgPool, order_id: &str) -> Uuid {
    sqlx::query_scalar("SELECT payment_id FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("payment id")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn version_open_failure_aborts_and_retries(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let app = with_digital(app);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = bitcoin_digital_order(&app, &paykit, &seller, &buyer).await;
    let listing = aggregate(&seller.pubky, "guide_01");
    let (deliverable_id, good): (String, Vec<u8>) = sqlx::query_as(
        "SELECT deliverable_id, payload_ciphertext FROM listing_digital_versions \
         WHERE listing_aggregate_id = $1",
    )
    .bind(&listing)
    .fetch_one(&app.pool)
    .await
    .expect("version");
    let wrong = DigitalKeys::from_hex(&"a".repeat(64), None).expect("wrong key");
    let bad = wrong.seal(
        &version_aad(&listing, &deliverable_id, 1, DigitalDeliveryKind::Text),
        br#"{"text":"x"}"#,
    );
    sqlx::query("UPDATE listing_digital_versions SET payload_ciphertext = $2 WHERE listing_aggregate_id = $1")
        .bind(&listing)
        .bind(&bad)
        .execute(&app.pool)
        .await
        .expect("corrupt version");
    let reference = order_reference(Uuid::parse_str(&order_id).expect("uuid"));
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    poll_now(&app, app.clock.now()).await;
    let (state, receipt, payment, _) = order_facts(&app, &order_id).await;
    assert_eq!(
        (state.as_str(), receipt, payment.as_str()),
        ("pending_payment", false, "awaiting_entitlement"),
        "the receipt transaction aborted"
    );
    sqlx::query("UPDATE listing_digital_versions SET payload_ciphertext = $2 WHERE listing_aggregate_id = $1")
        .bind(&listing)
        .bind(&good)
        .execute(&app.pool)
        .await
        .expect("restore version");
    assert!(poll_now(&app, app.clock.now() + chrono::Duration::minutes(1)).await >= 1);
    assert_eq!(
        order_facts(&app, &order_id).await.0,
        "delivered",
        "the retry delivers"
    );
}

fn ciphertext(plaintext_len: usize) -> Vec<u8> {
    (0..plaintext_len + 16).map(|i| (i % 251) as u8).collect()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyer_reads_pinned_deliverable_after_receipt(pool: PgPool) {
    let (app, server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    let bytes = ciphertext(4_096);
    server.put(&seller.pubky, DELIVERABLE_ID, 1, bytes.clone());
    let (status, body) = set_delivery(
        &app,
        &seller,
        "guide_01",
        0,
        json!({
            "kind": "file",
            "deliverable_id": DELIVERABLE_ID,
            "version": 1,
            "key": FILE_KEY,
            "iv": FILE_IV,
            "ciphertext_blake3": blake3::hash(&bytes).to_hex().to_string(),
            "plaintext_blake3": PLAINTEXT_BLAKE3,
            "size_bytes": 4_096,
            "content_type": "application/pdf",
            "file_name": "field-guide.pdf",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = digital_checkout(&app, &seller, &buyer, "guide_01").await;
    sandbox_confirm(&app, &buyer, &order).await;
    // Pins record the confirming adapter; this read is the real-rail shape,
    // so mark the pin as a Paykit confirmation (the sandbox refusal is D11).
    sqlx::query(
        "UPDATE order_digital_pins SET confirming_adapter = 'paykit' WHERE order_id = $1::uuid",
    )
    .bind(&order.order_id)
    .execute(&app.pool)
    .await
    .expect("adapter");

    let (status, cache, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(
        body,
        json!({
            "order_id": order.order_id,
            "lines": [{
                "line_index": 0,
                "listing_aggregate_id": aggregate(&seller.pubky, "guide_01"),
                "kind": "file",
                "seller_pubky": seller.pubky,
                "deliverable_id": DELIVERABLE_ID,
                "version": 1,
                "key": FILE_KEY,
                "iv": FILE_IV,
                "ciphertext_blake3": blake3::hash(&bytes).to_hex().to_string(),
                "plaintext_blake3": PLAINTEXT_BLAKE3,
                "content_type": "application/pdf",
                "file_name": "field-guide.pdf",
                "size_bytes": 4_096,
            }],
        })
    );
    assert_eq!(access_rows(&app, &order.order_id).await, 1);
    read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(
        access_rows(&app, &order.order_id).await,
        2,
        "every open is logged"
    );

    // Link and text lines return just their payload.
    let text_seller = new_actor(&app).await;
    let text_buyer = new_actor(&app).await;
    let text_order = delivered_text_order(&app, &text_seller, &text_buyer).await;
    sqlx::query(
        "UPDATE order_digital_pins SET confirming_adapter = 'paykit' WHERE order_id = $1::uuid",
    )
    .bind(&text_order.order_id)
    .execute(&app.pool)
    .await
    .expect("adapter");
    let (status, _, body) = read_delivery(&app, &text_buyer.token, &text_order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"][0]["text"], json!(TEXT_V1));
    assert!(body["lines"][0].get("key").is_none());
}

async fn paykit_delivered_order(app: &TestApp) -> (TestActor, TestActor, DigitalOrder) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let order = delivered_text_order(app, &seller, &buyer).await;
    sqlx::query(
        "UPDATE order_digital_pins SET confirming_adapter = 'paykit' WHERE order_id = $1::uuid",
    )
    .bind(&order.order_id)
    .execute(&app.pool)
    .await
    .expect("adapter");
    (seller, buyer, order)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_cannot_read_buyer_delivery(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (seller, _buyer, order) = paykit_delivered_order(&app).await;
    let (status, _, body) = read_delivery(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(access_rows(&app, &order.order_id).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn outsider_cannot_read_digital_delivery(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, _buyer, order) = paykit_delivered_order(&app).await;
    let outsider = new_actor(&app).await;
    let (status, _, body) = read_delivery(&app, &outsider.token, &order.order_id).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.to_string().contains(TEXT_V1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unpaid_order_has_no_delivery(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(&app, &seller, &buyer, "guide_01").await;
    let (status, cache, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(body["error"]["reason"], json!("not_paid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ended_order_delivery_refused(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = paykit_delivered_order(&app).await;
    let (status, body) = order_action(
        &app,
        &seller.token,
        "refund.record_external",
        &order.order_id,
        json!({ "amount_minor": 900, "transaction_id": "manual-refund-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("delivery_ended"));
    for ended in ["cancelled", "closed"] {
        sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
            .bind(&order.order_id)
            .bind(ended)
            .execute(&app.pool)
            .await
            .expect("state");
        let (status, _, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{ended}: {body}");
        assert_eq!(body["error"]["reason"], json!("delivery_ended"));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn completed_digital_order_still_downloads(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = paykit_delivered_order(&app).await;
    let later = app.clock.now() + chrono::Duration::days(app.state.config.auto_complete_days + 1);
    let completed = complete_due_delivered_orders(
        &app.pool,
        later,
        app.state.config.auto_complete_days,
        100,
        10,
    )
    .await
    .expect("auto-complete");
    assert_eq!(completed, 1);
    assert_eq!(order_facts(&app, &order.order_id).await.0, "completed");
    let (status, _, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"][0]["text"], json!(TEXT_V1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sandbox_confirmed_never_delivers(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = delivered_text_order(&app, &seller, &buyer).await;
    assert_eq!(order_facts(&app, &order.order_id).await.0, "delivered");
    let (status, _, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("sandbox_confirmed"));
    assert!(!body.to_string().contains(TEXT_V1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ship_refused_for_digital(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(&app, &seller, "guide_01", json!(["digital"]), usd(900), 5).await;
    set_text(&app, &seller, "guide_01", 0, TEXT_V1).await;
    let order = digital_checkout(&app, &seller, &buyer, "guide_01").await;
    sandbox_confirm(&app, &buyer, &order).await;
    // Force the order back to `paid` to prove the method refusal, not the
    // state check, is what stops it.
    sqlx::query("UPDATE orders SET state = 'paid' WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&app.pool)
        .await
        .expect("state");
    let (status, body) = order_action(
        &app,
        &seller.token,
        "fulfillment.ship",
        &order.order_id,
        json!({ "carrier": "USPS", "tracking_number": "9400100000000000000000" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert!(body["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("digital"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn confirm_delivery_refused_for_digital(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = paykit_delivered_order(&app).await;
    sqlx::query("UPDATE orders SET state = 'shipped' WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&app.pool)
        .await
        .expect("state");
    let (status, body) = order_action(
        &app,
        &buyer.token,
        "fulfillment.confirm_delivery",
        &order.order_id,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("digital"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_assume_skips_digital(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, _buyer, order) = paykit_delivered_order(&app).await;
    sqlx::query(
        "UPDATE orders SET state = 'shipped', \
         shipment = jsonb_build_object('state', 'shipped', 'shipped_at', '2026-01-01T00:00:00.000Z') \
         WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .execute(&app.pool)
    .await
    .expect("state");
    let later = app.clock.now() + chrono::Duration::days(365);
    let assumed = assume_due_deliveries(&app.pool, later, 1, 100, 10)
        .await
        .expect("sweep");
    assert_eq!(assumed, 0);
    assert_eq!(order_facts(&app, &order.order_id).await.0, "shipped");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn return_request_refused_for_digital(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = paykit_delivered_order(&app).await;
    for state in ["delivered", "completed"] {
        sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
            .bind(&order.order_id)
            .bind(state)
            .execute(&app.pool)
            .await
            .expect("state");
        let (status, body) = order_action(
            &app,
            &buyer.token,
            "return.request",
            &order.order_id,
            json!({ "reason": "Changed my mind", "requested_amount_minor": 900 }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{state}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!("returns_unavailable_for_digital")
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivered_digital_cancel_refused(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = paykit_delivered_order(&app).await;
    let (status, body) = order_action(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.order_id,
        json!({ "reason": "Changed my mind" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("digital_order_delivered"));
    assert_eq!(order_facts(&app, &order.order_id).await.0, "delivered");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn digital_refund_from_delivered_or_completed(pool: PgPool) {
    let (app, _server) = keyed_app(pool).await;
    for state in ["delivered", "completed"] {
        let (seller, buyer, order) = paykit_delivered_order(&app).await;
        sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
            .bind(&order.order_id)
            .bind(state)
            .execute(&app.pool)
            .await
            .expect("state");
        // Seller only.
        let (status, _) = order_action(
            &app,
            &buyer.token,
            "refund.record_external",
            &order.order_id,
            json!({ "amount_minor": 900, "transaction_id": "manual-refund-1" }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{state}");
        let (status, body) = order_action(
            &app,
            &seller.token,
            "refund.record_external",
            &order.order_id,
            json!({ "amount_minor": 900, "transaction_id": "manual-refund-1" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{state}: {body}");
        assert_eq!(body["result"]["order"]["state"], json!("refunded_external"));
        let (status, _, body) = read_delivery(&app, &buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["reason"], json!("delivery_ended"));
        assert_eq!(
            stock(&app, &seller.pubky, "guide_01").await,
            (4, 0, 1),
            "no restock"
        );
    }
    // A shipped order still cannot record a refund from `delivered`.
    let shipper = new_actor(&app).await;
    let shopper = new_actor(&app).await;
    register(
        &app,
        &shipper,
        "boots_02",
        json!(["shipping"]),
        usd(4_000),
        5,
    )
    .await;
    let (status, body) = checkout(
        &app,
        &shopper,
        vec![(shipper.pubky.clone(), "boots_02".to_string(), None)],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order")
        .to_string();
    sqlx::query("UPDATE orders SET state = 'delivered' WHERE id = $1::uuid")
        .bind(&order_id)
        .execute(&app.pool)
        .await
        .expect("state");
    let (status, _) = order_action(
        &app,
        &shipper.token,
        "refund.record_external",
        &order_id,
        json!({ "amount_minor": 4_000, "transaction_id": "manual-refund-2" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}
