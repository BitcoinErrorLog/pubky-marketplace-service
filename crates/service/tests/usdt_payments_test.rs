//! The USDT asset model and its flag (`USDT_PAYMENTS_ENABLED`): the 0053
//! schema, the order projection of an order that carries USDT terms, the
//! `/health` capability, and the `usdt` bind method, which exists only while
//! the flag is on and is refused until the upstream Marketplace prepare can
//! carry USDT. Bitcoin and PayPal behave the same with the flag on or off.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{create_sat_order, enable_bitcoin};
use common::*;
use marketplace_domain::ErrorCode;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::{MarketplaceAssets, PaymentTerms};
use marketplace_service::payments::PaykitApi;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// The USD fixture order total: 12,500 plus 1,200 shipping.
const ORDER_TOTAL_CENTS: i64 = 13_700;
const ORDER_TOTAL_MILLIONTHS: i64 = 137_000_000;

const HEALTH_KEYS: [&str; 7] = [
    "digital_delivery_available",
    "digital_delivery_max_bytes",
    "offer_checkout",
    "paykit_rail",
    "pickup_available",
    "priv_keys_available",
    "status",
];

const PAYMENT_TERMS_KEYS: [&str; 5] = [
    "payment_asset",
    "payment_network",
    "payment_amount_minor",
    "payment_exponent",
    "payment_quote_basis",
];

fn usdt_config(assets: Option<&str>, api: PaykitApi) -> Config {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    if let Some(assets) = assets {
        config.paykit_marketplace_assets =
            MarketplaceAssets::parse(Some(assets), api).expect("assets parse");
    }
    config
}

/// A pending USD/2 order (no listing lock), the price a USDT quote needs.
async fn usd_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, 4)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
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

/// An upstream app whose clock is the real instant: the Paykit double stamps
/// activation deadlines from the real clock.
async fn upstream_app(pool: PgPool, config: Config) -> (TestApp, FakePaykit) {
    let (app, paykit) = test_app_with_paykit_api_config(pool, config, PaykitApi::Upstream).await;
    app.clock
        .set(DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).expect("now"));
    (app, paykit)
}

fn uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).expect("uuid")
}

async fn bind(app: &TestApp, token: &str, order_id: &str, method: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": method }),
    )
    .await
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

async fn list_orders(app: &TestApp, token: &str) -> Vec<Value> {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/orders",
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order list failed: {body}");
    body["orders"].as_array().expect("orders array").clone()
}

async fn health(app: &TestApp) -> Value {
    let (status, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "health failed: {body}");
    body
}

fn assert_no_payment_terms(order: &Value) {
    for key in PAYMENT_TERMS_KEYS {
        assert!(
            order.get(key).is_none(),
            "{key} must be absent from {}",
            order["payment_method"]
        );
    }
}

fn assert_refusal(status: StatusCode, body: &Value, code: ErrorCode, reason: &str) {
    assert_eq!(
        status.as_u16(),
        code.http_status(),
        "unexpected status: {body}"
    );
    assert_eq!(body["ok"], json!(false), "{body}");
    assert_eq!(body["error"]["reason"], json!(reason), "{body}");
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
struct BindFacts {
    state: String,
    payment_method: Option<String>,
    stock_held: bool,
    hold_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    paykit_bind_attempt: i32,
    paykit_request_reference: Option<String>,
    fiat_checkout_url: Option<String>,
    payment_asset: Option<String>,
}

async fn bind_facts(pool: &PgPool, order_id: &str) -> BindFacts {
    sqlx::query_as(
        "SELECT state, payment_method, stock_held, hold_expires_at, paykit_bind_attempt, \
         paykit_request_reference, fiat_checkout_url, payment_asset FROM orders WHERE id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn event_count(pool: &PgPool, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_id = $1")
        .bind(format!("order:{order_id}"))
        .fetch_one(pool)
        .await
        .expect("event count")
}

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_with_the_flag_off_has_no_usdt_key(pool: PgPool) {
    let app = test_app(pool).await;
    let body = health(&app).await;
    let mut keys: Vec<&str> = body
        .as_object()
        .expect("health object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, HEALTH_KEYS);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_with_the_flag_on_reports_availability_and_nothing_else_changes(pool: PgPool) {
    let off = health(&test_app(pool.clone()).await).await;
    let on = health(&test_app_with_config(pool, usdt_config(None, PaykitApi::Fork)).await).await;
    assert_eq!(on["usdt_payments"], json!({ "available": true }));
    let mut without_usdt = on.clone();
    without_usdt
        .as_object_mut()
        .expect("health object")
        .remove("usdt_payments");
    assert_eq!(without_usdt, off, "every other health field is unchanged");
}

// ---------------------------------------------------------------------------
// Bind
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_is_an_invalid_method_with_the_flag_off(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = usd_order(&app, &seller, &buyer).await;
    let before = bind_facts(&pool, &order.order_id).await;
    let events = event_count(&pool, &order.order_id).await;

    let (status, body) = bind(&app, &buyer.token, &order.order_id, "usdt").await;
    assert_refusal(status, &body, ErrorCode::InvalidCommand, "invalid_method");
    assert_eq!(
        body["error"]["message"],
        json!("The payment method must be bitcoin, stripe, or paypal.")
    );
    assert_eq!(bind_facts(&pool, &order.order_id).await, before);
    assert_eq!(event_count(&pool, &order.order_id).await, events);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unknown_method_is_refused_with_or_without_the_flag(pool: PgPool) {
    let (off, _stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let (on, _paykit) = test_app_with_paykit_api_config(
        pool.clone(),
        usdt_config(None, PaykitApi::Fork),
        PaykitApi::Fork,
    )
    .await;
    let seller = new_actor(&off).await;
    let buyer = new_actor(&off).await;
    let order = usd_order(&off, &seller, &buyer).await;
    let buyer_on = authenticate(&on, &buyer.keypair).await;

    for method in ["lightning", "USDT", "Bitcoin", ""] {
        let (status, body) = bind(&off, &buyer.token, &order.order_id, method).await;
        assert_refusal(status, &body, ErrorCode::InvalidCommand, "invalid_method");
        assert_eq!(
            body["error"]["message"],
            json!("The payment method must be bitcoin, stripe, or paypal.")
        );
        let (status, body) = bind(&on, &buyer_on, &order.order_id, method).await;
        assert_refusal(status, &body, ErrorCode::InvalidCommand, "invalid_method");
        assert_eq!(
            body["error"]["message"],
            json!("The payment method must be bitcoin, usdt, stripe, or paypal.")
        );
    }
}

/// The USDT bind is refused on every deployment shape this build can be in,
/// and the refusal leaves the order exactly as it was: unbound, unheld, no
/// attempt spent, no event.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_bind_is_refused_and_leaves_the_order_untouched(pool: PgPool) {
    let deployments = [
        ("fork", PaykitApi::Fork, None),
        ("upstream, bitcoin-only contract", PaykitApi::Upstream, None),
        (
            "upstream, contract lists USDT",
            PaykitApi::Upstream,
            Some("BTC,USDT"),
        ),
    ];
    for (name, api, assets) in deployments {
        let (app, _paykit) =
            test_app_with_paykit_api_config(pool.clone(), usdt_config(assets, api), api).await;
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let order = usd_order(&app, &seller, &buyer).await;
        let before = bind_facts(&pool, &order.order_id).await;
        let events = event_count(&pool, &order.order_id).await;

        let (status, body) = bind(&app, &buyer.token, &order.order_id, "usdt").await;
        assert_refusal(
            status,
            &body,
            ErrorCode::UpstreamUnavailable,
            "usdt_unavailable",
        );
        assert_eq!(
            body["error"]["message"],
            json!("USDT payments are not available right now. Choose another payment method."),
            "{name}"
        );
        assert_eq!(bind_facts(&pool, &order.order_id).await, before, "{name}");
        assert_eq!(before.payment_method, None, "{name}");
        assert!(!before.stock_held, "{name}");
        assert_eq!(before.paykit_bind_attempt, 0, "{name}");
        assert_eq!(event_count(&pool, &order.order_id).await, events, "{name}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refused_usdt_bind_does_not_stop_the_buyer_choosing_bitcoin(pool: PgPool) {
    let (app, paykit) = upstream_app(pool.clone(), usdt_config(None, PaykitApi::Upstream)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;

    let (status, body) = bind(&app, &buyer.token, &order.order_id, "usdt").await;
    assert_refusal(
        status,
        &body,
        ErrorCode::UpstreamUnavailable,
        "usdt_unavailable",
    );

    let (status, body) = bind(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    assert_eq!(body["order"]["payment_method"], json!("bitcoin"));
    assert_no_payment_terms(&body["order"]);
    let facts = bind_facts(&pool, &order.order_id).await;
    assert_eq!(facts.payment_method.as_deref(), Some("bitcoin"));
    assert_eq!(facts.payment_asset, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paypal_bind_is_the_same_with_the_flag_on_and_off(pool: PgPool) {
    let mut checkout_urls = Vec::new();
    for flag in [false, true] {
        let mut config = Config::for_tests();
        config.usdt_payments_enabled = flag;
        let (app, _stripe, _paykit, _ipn, _shippo) =
            test_app_with_payments_config(pool.clone(), config).await;
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (status, body) = send(
            app.router.clone(),
            "PUT",
            "/v0/sellers/me/payment-config",
            Some(&seller.token),
            &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "config put failed: {body}");
        let order = usd_order(&app, &seller, &buyer).await;

        let (status, body) = bind(&app, &buyer.token, &order.order_id, "paypal").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "paypal bind failed (flag {flag}): {body}"
        );
        assert_eq!(body["order"]["payment_method"], json!("paypal"));
        assert_eq!(body["order"]["fiat_verification"], json!("seller-attested"));
        assert_no_payment_terms(&body["order"]);
        let url = body["order"]["fiat_checkout_url"]
            .as_str()
            .expect("checkout url")
            .to_string();
        checkout_urls.push(url.replace(&order.order_id, "ORDER"));

        // Rebind of the same method is idempotent, as before.
        let (status, again) = bind(&app, &buyer.token, &order.order_id, "paypal").await;
        assert_eq!(status, StatusCode::OK, "{again}");
        // A bound order refuses a different method, USDT included.
        let (status, refused) = bind(&app, &buyer.token, &order.order_id, "bitcoin").await;
        assert_refusal(
            status,
            &refused,
            ErrorCode::InvalidState,
            "payment_method_already_bound",
        );
    }
    assert_eq!(checkout_urls[0], checkout_urls[1]);
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

async fn store_usdt_terms(pool: &PgPool, order_id: &str) {
    let terms = PaymentTerms::usdt_at_parity("USD", 2, ORDER_TOTAL_CENTS).expect("parity quote");
    let mut conn = pool.acquire().await.expect("connection");
    assert!(terms
        .store(&mut conn, uuid(order_id))
        .await
        .expect("terms store"));
    sqlx::query("UPDATE orders SET payment_method = 'usdt' WHERE id = $1")
        .bind(uuid(order_id))
        .execute(&mut *conn)
        .await
        .expect("usdt method");
}

fn assert_usdt_projection(order: &Value) {
    assert_eq!(order["payment_method"], json!("usdt"));
    assert_eq!(order["payment_asset"], json!("USDT"));
    assert_eq!(order["payment_network"], json!("arbitrum-one"));
    assert_eq!(order["payment_amount_minor"], json!(ORDER_TOTAL_MILLIONTHS));
    assert_eq!(order["payment_exponent"], json!(6));
    assert_eq!(order["payment_quote_basis"], json!("parity"));
    assert_eq!(
        order["total"],
        json!({ "amount_minor": ORDER_TOTAL_CENTS, "currency": "USD", "exponent": 2 }),
        "the price of record is unchanged"
    );
    assert_eq!(
        order["merchandise_total"]["amount_minor"],
        json!(ORDER_TOTAL_CENTS)
    );
    assert_eq!(order["fiat_verification"], Value::Null);
    assert_eq!(order["paykit_total_sats"], Value::Null);
    assert_eq!(order["bitcoin_payable"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_with_usdt_terms_projects_them_to_both_participants(pool: PgPool) {
    let app = test_app_with_config(pool.clone(), usdt_config(None, PaykitApi::Fork)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = usd_order(&app, &seller, &buyer).await;
    store_usdt_terms(&pool, &order.order_id).await;

    for token in [&buyer.token, &seller.token] {
        assert_usdt_projection(&read_order(&app, token, &order.order_id).await);
        let listed = list_orders(&app, token).await;
        assert_eq!(listed.len(), 1);
        assert_usdt_projection(&listed[0]);
    }
}

/// An order bound while the flag was on is still shown as USDT after the flag
/// is turned off: the flag gates offers, never an order that exists.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_terms_are_projected_whatever_the_flag_says(pool: PgPool) {
    let on = test_app_with_config(pool.clone(), usdt_config(None, PaykitApi::Fork)).await;
    let seller = new_actor(&on).await;
    let buyer = new_actor(&on).await;
    let order = usd_order(&on, &seller, &buyer).await;
    store_usdt_terms(&pool, &order.order_id).await;
    let on_view = read_order(&on, &buyer.token, &order.order_id).await;

    let off = test_app(pool).await;
    let buyer_off = authenticate(&off, &buyer.keypair).await;
    let off_view = read_order(&off, &buyer_off, &order.order_id).await;
    assert_usdt_projection(&off_view);
    assert_eq!(off_view, on_view);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_without_usdt_terms_projects_no_payment_terms(pool: PgPool) {
    let (app, paykit) = upstream_app(pool.clone(), usdt_config(None, PaykitApi::Upstream)).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;

    let unbound = create_sat_order(&app, &seller, &buyer).await;
    let view = read_order(&app, &buyer.token, &unbound.order_id).await;
    assert_eq!(view["payment_method"], Value::Null);
    assert_no_payment_terms(&view);

    let (status, body) = bind(&app, &buyer.token, &unbound.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    for token in [&buyer.token, &seller.token] {
        let view = read_order(&app, token, &unbound.order_id).await;
        assert_eq!(view["payment_method"], json!("bitcoin"));
        assert_no_payment_terms(&view);
        for listed in list_orders(&app, token).await {
            assert_no_payment_terms(&listed);
        }
    }
}

/// The projection of a Bitcoin order is the same bytes with the flag on or
/// off: the flag adds no key and moves no value.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_orders_projection_is_identical_with_the_flag_on_or_off(pool: PgPool) {
    let (off, paykit) = upstream_app(pool.clone(), Config::for_tests()).await;
    let seller = new_actor(&off).await;
    let buyer = new_actor(&off).await;
    enable_bitcoin(&off, &paykit, &seller).await;
    let order = create_sat_order(&off, &seller, &buyer).await;
    let (status, body) = bind(&off, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    let off_buyer = read_order(&off, &buyer.token, &order.order_id).await;
    let off_seller = read_order(&off, &seller.token, &order.order_id).await;

    let on = test_app_with_config(pool, usdt_config(None, PaykitApi::Upstream)).await;
    let on_buyer = authenticate(&on, &buyer.keypair).await;
    let on_seller = authenticate(&on, &seller.keypair).await;
    assert_eq!(read_order(&on, &on_buyer, &order.order_id).await, off_buyer);
    assert_eq!(
        read_order(&on, &on_seller, &order.order_id).await,
        off_seller
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_first_terms_stored_on_an_order_are_kept(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = usd_order(&app, &seller, &buyer).await;
    let mut conn = pool.acquire().await.expect("connection");

    let first = PaymentTerms::usdt_at_parity("USD", 2, ORDER_TOTAL_CENTS).expect("quote");
    let other = PaymentTerms::usdt_at_parity("USD", 2, 100).expect("quote");
    assert!(first
        .store(&mut conn, uuid(&order.order_id))
        .await
        .expect("store"));
    assert!(!other
        .store(&mut conn, uuid(&order.order_id))
        .await
        .expect("store"));
    assert!(!first
        .store(&mut conn, uuid(&order.order_id))
        .await
        .expect("store"));
    let amount: i64 = sqlx::query_scalar("SELECT payment_amount_minor FROM orders WHERE id = $1")
        .bind(uuid(&order.order_id))
        .fetch_one(&mut *conn)
        .await
        .expect("amount");
    assert_eq!(amount, ORDER_TOTAL_MILLIONTHS);

    let missing = first.store(&mut conn, Uuid::new_v4()).await.expect("store");
    assert!(!missing, "no order, no terms");
}

// ---------------------------------------------------------------------------
// Migration 0053
// ---------------------------------------------------------------------------

async fn set_terms(
    pool: &PgPool,
    order_id: &str,
    columns: [(&str, Value); 5],
) -> Result<(), String> {
    let mut assignments = Vec::new();
    for (index, (column, _)) in columns.iter().enumerate() {
        assignments.push(format!("{column} = ${}", index + 2));
    }
    let sql = format!("UPDATE orders SET {} WHERE id = $1", assignments.join(", "));
    let text = |value: &Value| value.as_str().map(str::to_string);
    let number = |value: &Value| value.as_i64();
    sqlx::query(&sql)
        .bind(uuid(order_id))
        .bind(text(&columns[0].1))
        .bind(text(&columns[1].1))
        .bind(number(&columns[2].1))
        .bind(number(&columns[3].1).map(|value| value as i16))
        .bind(text(&columns[4].1))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn terms(
    asset: Value,
    network: Value,
    amount: Value,
    exponent: Value,
    basis: Value,
) -> [(&'static str, Value); 5] {
    [
        ("payment_asset", asset),
        ("payment_network", network),
        ("payment_amount_minor", amount),
        ("payment_exponent", exponent),
        ("payment_quote_basis", basis),
    ]
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0053_keeps_the_terms_all_or_nothing_and_the_values_open(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = usd_order(&app, &seller, &buyer).await;
    let null = || Value::Null;

    set_terms(
        &pool,
        &order.order_id,
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(1),
            json!(6),
            json!("parity"),
        ),
    )
    .await
    .expect("a full set of terms is accepted");
    set_terms(
        &pool,
        &order.order_id,
        terms(null(), null(), null(), null(), null()),
    )
    .await
    .expect("terms can be cleared together");

    // No CHECK on the values: a later asset, network or basis needs no
    // migration.
    set_terms(
        &pool,
        &order.order_id,
        terms(
            json!("DAI"),
            json!("base"),
            json!(5),
            json!(18),
            json!("oracle"),
        ),
    )
    .await
    .expect("a later asset is only a new value");
    set_terms(
        &pool,
        &order.order_id,
        terms(null(), null(), null(), null(), null()),
    )
    .await
    .expect("cleared");

    let partial = [
        terms(json!("USDT"), null(), null(), null(), null()),
        terms(null(), json!("arbitrum-one"), null(), null(), null()),
        terms(null(), null(), json!(1), null(), null()),
        terms(null(), null(), null(), json!(6), null()),
        terms(null(), null(), null(), null(), json!("parity")),
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(1),
            json!(6),
            null(),
        ),
        terms(
            null(),
            json!("arbitrum-one"),
            json!(1),
            json!(6),
            json!("parity"),
        ),
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(1),
            null(),
            json!("parity"),
        ),
    ];
    for columns in partial {
        let refused = set_terms(&pool, &order.order_id, columns)
            .await
            .expect_err("half-filled terms are refused");
        assert!(
            refused.contains("orders_payment_asset_terms_check"),
            "{refused}"
        );
    }
    for columns in [
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(0),
            json!(6),
            json!("parity"),
        ),
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(-5),
            json!(6),
            json!("parity"),
        ),
        terms(
            json!("USDT"),
            json!("arbitrum-one"),
            json!(5),
            json!(-1),
            json!("parity"),
        ),
    ] {
        let refused = set_terms(&pool, &order.order_id, columns)
            .await
            .expect_err("a non-positive amount or negative exponent is refused");
        assert!(
            refused.contains("orders_payment_asset_terms_check"),
            "{refused}"
        );
    }
    let asset: Option<String> =
        sqlx::query_scalar("SELECT payment_asset FROM orders WHERE id = $1")
            .bind(uuid(&order.order_id))
            .fetch_one(&pool)
            .await
            .expect("asset");
    assert_eq!(asset, None, "every refused write left the order alone");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0053_widens_the_method_check_to_usdt_only(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = usd_order(&app, &seller, &buyer).await;
    for method in ["bitcoin", "stripe", "paypal", "usdt"] {
        sqlx::query("UPDATE orders SET payment_method = $2 WHERE id = $1")
            .bind(uuid(&order.order_id))
            .bind(method)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{method} must be accepted: {error}"));
    }
    for method in ["USDT", "lightning", ""] {
        let refused = sqlx::query("UPDATE orders SET payment_method = $2 WHERE id = $1")
            .bind(uuid(&order.order_id))
            .bind(method)
            .execute(&pool)
            .await
            .expect_err("an unknown method is refused");
        assert!(
            refused.to_string().contains("orders_payment_method_check"),
            "{method}: {refused}"
        );
    }
    sqlx::query("UPDATE orders SET payment_method = NULL WHERE id = $1")
        .bind(uuid(&order.order_id))
        .execute(&pool)
        .await
        .expect("an unbound order stays valid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0053_is_rerunnable_and_existing_orders_satisfy_it(pool: PgPool) {
    let (app, paykit) = upstream_app(pool.clone(), Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let bound = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = bind(&app, &buyer.token, &bound.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    let before = read_order(&app, &buyer.token, &bound.order_id).await;

    sqlx::raw_sql(include_str!("../migrations/0053_payment_assets.sql"))
        .execute(&pool)
        .await
        .expect("0053 must be directly rerunnable");
    assert_eq!(
        read_order(&app, &buyer.token, &bound.order_id).await,
        before
    );

    // The constraints were added NOT VALID so the migration never scans
    // `orders` under a lock; every row that exists satisfies them.
    for constraint in [
        "orders_payment_method_check",
        "orders_payment_asset_terms_check",
    ] {
        sqlx::query(&format!(
            "ALTER TABLE orders VALIDATE CONSTRAINT {constraint}"
        ))
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("{constraint} must validate: {error}"));
    }
    let constraints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'orders'::regclass \
         AND conname IN ('orders_payment_method_check', 'orders_payment_asset_terms_check')",
    )
    .fetch_one(&pool)
    .await
    .expect("constraint count");
    assert_eq!(constraints, 2, "a rerun keeps one of each constraint");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_accepted_payment_options_hold_one_row_per_seller_and_known_option(pool: PgPool) {
    let insert = |seller: &'static str, option: &'static str, enabled: bool| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO seller_accepted_payment_options \
                 (seller_pubky, option_id, enabled, updated_at) VALUES ($1, $2, $3, now())",
            )
            .bind(seller)
            .bind(option)
            .bind(enabled)
            .execute(&pool)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        }
    };
    insert("seller_a", "paykit.usdt.arbitrum-one", true)
        .await
        .expect("USDT option inserts");
    insert("seller_b", "paykit.usdt.arbitrum-one", false)
        .await
        .expect("another seller inserts");
    let duplicate = insert("seller_a", "paykit.usdt.arbitrum-one", false)
        .await
        .expect_err("one row per seller and option");
    assert!(
        duplicate.contains("seller_accepted_payment_options_pkey"),
        "{duplicate}"
    );
    for option in ["paykit.btc.bitcoin", "paypal.fiat", "paykit.usdt", ""] {
        let refused = insert("seller_c", option, true)
            .await
            .expect_err("only the USDT option is stored");
        assert!(
            refused.contains("seller_accepted_payment_options_option_id_check"),
            "{option}: {refused}"
        );
    }
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM seller_accepted_payment_options")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(stored, 2);
}
