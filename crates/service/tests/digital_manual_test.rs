//! Digital delivery, service Wave 1 slice 3 (digital-delivery-design.md
//! §3.3, §3.6, §4.3, §6 rows D2, E7, E8, F1–F17, G1): the buyer's delivery
//! email, the seller's Mark emailed / Mark delivered, the purge, and the
//! mixed-kind order rules. Driven through the HTTP command surface and the
//! real Paykit confirmation path against Postgres.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::paykit_review::{poll_now, status_confirmed, status_detected};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::handlers::digital_manual::{
    overdue_delivery_emails, purge_delivery_emails, read_delivery_email_with_hook,
    DeliveryEmailReadHook,
};
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tower::util::ServiceExt;
use uuid::Uuid;

const EMAIL: &str = "buyer.sentinel+digital@example.com";
const EMAIL_2: &str = "second.sentinel@example.org";
const TEXT: &str = "LICENCE-MANUAL-SENTINEL";

static NUMBER: AtomicU64 = AtomicU64::new(1);

fn next() -> u64 {
    NUMBER.fetch_add(1, Ordering::Relaxed)
}

fn aggregate(seller: &str, listing_id: &str) -> String {
    format!("listing:{seller}_{listing_id}")
}

async fn keyed_app(pool: PgPool) -> (TestApp, FakePaykit, DeliverableServer) {
    let (app, paykit, _ipn, server) = test_app_with_payments_and_digital(pool).await;
    (app, paykit, server)
}
async fn register(
    app: &TestApp,
    seller: &TestActor,
    listing_id: &str,
    price: Value,
    quantity: i64,
) {
    let mut command = register_command(&seller.pubky, quantity);
    command["command_id"] = json!(indexed_command_id(0xe400, next()));
    command["aggregate_id"] = json!(aggregate(&seller.pubky, listing_id));
    command["payload"]["listing_id"] = json!(listing_id);
    command["payload"]["fulfillment_methods"] = json!(["digital"]);
    command["payload"]["unit_price"] = price;
    let (status, body) = execute(app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
}

fn sat(amount_minor: i64) -> Value {
    json!({ "amount_minor": amount_minor, "currency": "SAT", "exponent": 0 })
}

async fn set_kind(app: &TestApp, seller: &TestActor, listing_id: &str, delivery: Value) {
    let (status, body) = execute(
        app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": indexed_command_id(0xe500, next()),
            "aggregate_id": aggregate(&seller.pubky, listing_id),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "digital_delivery.set",
            "payload": { "expected_version": 0, "delivery": delivery },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set failed: {body}");
}

/// A SAT digital listing of the given kind, for Bitcoin orders.
async fn listing(app: &TestApp, seller: &TestActor, listing_id: &str, delivery: Value) {
    register(app, seller, listing_id, sat(50_000), 5).await;
    set_kind(app, seller, listing_id, delivery).await;
}

fn email_kind() -> Value {
    json!({ "kind": "email" })
}

fn text_kind() -> Value {
    json!({ "kind": "text", "text": TEXT })
}

async fn checkout(
    app: &TestApp,
    buyer: &TestActor,
    lines: &[(&TestActor, &str)],
    delivery_email: Option<&str>,
) -> (StatusCode, Value) {
    let command_id = indexed_command_id(0xe600, next());
    let mut resolved = Vec::new();
    for (seller, listing_id) in lines {
        let revision: i64 =
            sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(aggregate(&seller.pubky, listing_id))
                .fetch_one(&app.pool)
                .await
                .expect("listing row");
        resolved.push(json!({
            "listing_aggregate_id": aggregate(&seller.pubky, listing_id),
            "expected_revision": revision,
            "quantity": 1,
            "fulfillment": "digital",
        }));
    }
    let mut payload = json!({ "lines": resolved, "guarantee_policy_version": 1 });
    if let Some(email) = delivery_email {
        payload["delivery_email"] = json!(email);
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

struct Order {
    id: String,
}

fn orders_of(body: &Value) -> Vec<Order> {
    let orders = body["result"]["orders"].as_array().expect("orders");
    orders
        .iter()
        .map(|order| Order {
            id: order["id"].as_str().expect("order id").to_string(),
        })
        .collect()
}

async fn one_order(
    app: &TestApp,
    buyer: &TestActor,
    lines: &[(&TestActor, &str)],
    email: Option<&str>,
) -> Order {
    let (status, body) = checkout(app, buyer, lines, email).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let mut orders = orders_of(&body);
    assert_eq!(orders.len(), 1);
    orders.remove(0)
}

async fn state_of(app: &TestApp, order_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order state")
}

async fn revision_of(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order revision")
}

fn order_cmd(kind: &str, order_id: &str, revision: i64, payload: Value) -> Value {
    order_command(kind, order_id, revision, payload, next())
}

async fn act(
    app: &TestApp,
    token: &str,
    kind: &str,
    order_id: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let revision = revision_of(app, order_id).await;
    execute(app, token, &order_cmd(kind, order_id, revision, payload)).await
}

async fn mark(
    app: &TestApp,
    seller: &TestActor,
    order_id: &str,
    channel: &str,
) -> (StatusCode, Value) {
    act(
        app,
        &seller.token,
        "fulfillment.deliver_digital",
        order_id,
        json!({ "channel": channel }),
    )
    .await
}

async fn get_raw(app: &TestApp, token: &str, uri: &str) -> (StatusCode, String, Value) {
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
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

async fn read_email(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, String, Value) {
    get_raw(app, token, &format!("/v1/orders/{order_id}/delivery-email")).await
}

/// One Bitcoin seller with an email-kind listing, one buyer, one order.
async fn email_order(app: &TestApp, paykit: &FakePaykit) -> (TestActor, TestActor, Order) {
    let seller = bitcoin_seller(app, paykit).await;
    let buyer = new_actor(app).await;
    listing(app, &seller, "guide_01", email_kind()).await;
    let order = one_order(app, &buyer, &[(&seller, "guide_01")], Some(EMAIL)).await;
    (seller, buyer, order)
}

async fn email_rows(app: &TestApp) -> Vec<(String, bool)> {
    sqlx::query_as(
        "SELECT order_id::text, email_ciphertext IS NOT NULL FROM order_delivery_emails \
         ORDER BY order_id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("email rows")
}

async fn columns_containing(pool: &PgPool, needle: &str) -> Vec<String> {
    let columns: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.table_name, c.column_name, c.data_type FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
         WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' \
           AND c.data_type IN ('text', 'character varying', 'jsonb', 'json', 'bytea', 'ARRAY')",
    )
    .fetch_all(pool)
    .await
    .expect("column catalog");
    let mut hits = Vec::new();
    for (table, column, data_type) in columns {
        let predicate = if data_type == "bytea" {
            format!("position($1::bytea IN \"{column}\") > 0")
        } else {
            format!("strpos(\"{column}\"::text, $2) > 0")
        };
        let (found,): (bool,) = sqlx::query_as(&format!(
            "SELECT EXISTS (SELECT 1 FROM \"{table}\" WHERE {predicate})"
        ))
        .bind(needle.as_bytes())
        .bind(needle)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("scan {table}.{column}: {error}"));
        if found {
            hits.push(format!("{table}.{column}"));
        }
    }
    hits
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn email_kind_checkout_requires_email(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", email_kind()).await;
    let (status, body) = checkout(&app, &buyer, &[(&seller, "guide_01")], None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(body["error"]["reason"], json!("delivery_email_required"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_email_rejected_without_email_kind(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", text_kind()).await;
    let (status, body) = checkout(&app, &buyer, &[(&seller, "guide_01")], Some(EMAIL)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("delivery_email_not_needed"));
    assert!(!body.to_string().contains(EMAIL));
    assert!(email_rows(&app).await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_email_validation(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", email_kind()).await;
    let long = format!("{}@example.com", "a".repeat(250));
    for bad in [
        "buyer",
        "buyer@",
        "@example.com",
        "a@b@c",
        "buyer @example.com",
        long.as_str(),
    ] {
        let (status, body) = checkout(&app, &buyer, &[(&seller, "guide_01")], Some(bad)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["reason"], json!("invalid_delivery_email"));
    }
    assert!(email_rows(&app).await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_email_sealed_only_on_email_orders(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let first = bitcoin_seller(&app, &paykit).await;
    let second = bitcoin_seller(&app, &paykit).await;
    let instant = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &first, "guide_01", email_kind()).await;
    listing(&app, &second, "guide_01", email_kind()).await;
    listing(&app, &instant, "guide_01", text_kind()).await;
    let (status, body) = checkout(
        &app,
        &buyer,
        &[
            (&first, "guide_01"),
            (&second, "guide_01"),
            (&instant, "guide_01"),
        ],
        Some(EMAIL),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let orders = body["result"]["orders"].as_array().expect("orders");
    assert_eq!(orders.len(), 3);
    let order_for = |seller: &TestActor| {
        orders
            .iter()
            .find(|order| order["seller_pubky"] == json!(seller.pubky))
            .and_then(|order| order["id"].as_str())
            .expect("order")
            .to_string()
    };
    let mut expected = vec![(order_for(&first), true), (order_for(&second), true)];
    expected.sort();
    assert_eq!(
        email_rows(&app).await,
        expected,
        "one sealed row per email-kind order"
    );
    let hits = columns_containing(&app.pool, EMAIL).await;
    assert!(hits.is_empty(), "plaintext email stored at {hits:?}");
    assert!(!body.to_string().contains(EMAIL));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_email_sentinel_absent_everywhere(pool: PgPool) {
    install_log_capture();
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    let mut bodies = Vec::new();
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (_, list) = send(
        app.router.clone(),
        "GET",
        "/v1/orders",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    bodies.push(list);
    let (_, one) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order.id),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    bodies.push(one);
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    bodies.push(body);
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    bodies.push(body);
    // A refused change carrying the address.
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    bodies.push(body);
    for email in [EMAIL, EMAIL_2] {
        for body in &bodies {
            assert!(!body.to_string().contains(email), "{email} in {body}");
        }
        let hits = columns_containing(&app.pool, email).await;
        assert!(hits.is_empty(), "{email} stored in plaintext at {hits:?}");
        assert!(!captured_logs().contains(email), "{email} reached the logs");
    }
    // Only the entitled reads return it.
    let (_, _, read) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(read["delivery_email"], json!(EMAIL_2));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyer_reads_own_delivery_email(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = email_order(&app, &paykit).await;
    let (status, cache, body) = read_email(&app, &buyer.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(body["delivery_email"], json!(EMAIL));
    assert_eq!(body["emailed_at"], Value::Null);
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, _, body) = read_email(&app, &buyer.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_reads_email_after_receipt(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, cache, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(body["delivery_email"], json!(EMAIL));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_reads_email_in_cancel_requested(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "Wrong item" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(state_of(&app, &order.id).await, "cancel_requested");
    let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["delivery_email"], json!(EMAIL));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_cannot_read_email_before_payment(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, _buyer, order) = email_order(&app, &paykit).await;
    let (status, cache, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(cache, "no-store");
    assert_eq!(body["error"]["reason"], json!("not_paid"));
    assert!(!body.to_string().contains(EMAIL));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_cannot_read_email_after_end(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    for ended in ["cancelled", "refunded_external", "closed"] {
        sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
            .bind(&order.id)
            .bind(ended)
            .execute(&app.pool)
            .await
            .expect("state");
        let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{ended}: {body}");
        assert_eq!(body["error"]["reason"], json!("delivery_ended"));
        // The buyer still sees their own address until it is purged.
        let (status, _, _) = read_email(&app, &buyer.token, &order.id).await;
        assert_eq!(status, StatusCode::OK, "{ended}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn purged_email_prompts_buyer_reentry(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    sqlx::query(
        "UPDATE order_delivery_emails SET email_ciphertext = NULL, purged_at = now() \
         WHERE order_id = $1::uuid",
    )
    .bind(&order.id)
    .execute(&app.pool)
    .await
    .expect("purge");
    for token in [&buyer.token, &seller.token] {
        let (status, _, body) = read_email(&app, token, &order.id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["reason"], json!("email_missing"));
    }
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("email_missing"));
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["delivery_email"], json!(EMAIL_2));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn outsider_cannot_read_delivery_email(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let outsider = new_actor(&app).await;
    let (status, _, body) = read_email(&app, &outsider.token, &order.id).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.to_string().contains(EMAIL));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyer_changes_email_before_emailed(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    // Before payment.
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (_, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(body["delivery_email"], json!(EMAIL_2));
    // After payment, the seller is told (without the address).
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(body["delivery_email"], json!(EMAIL));
    let notified: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.delivery_email_updated' \
         AND payload->>'recipient_pubky' = $1",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("notification");
    assert_eq!(notified, 1, "only the post-payment change is notified");
    // A malformed replacement is refused.
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": "not-an-email" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("invalid_delivery_email"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn email_change_refused_after_emailed(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    // The order is delivered now, so the state check answers first; force it
    // back to `paid` to prove the emailed stamp alone refuses.
    sqlx::query("UPDATE orders SET state = 'paid' WHERE id = $1::uuid")
        .bind(&order.id)
        .execute(&app.pool)
        .await
        .expect("state");
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("already_emailed"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn email_change_buyer_only(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, _buyer, order) = email_order(&app, &paykit).await;
    let (status, body) = act(
        &app,
        &seller.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let outsider = new_actor(&app).await;
    let (status, _) = act(
        &app,
        &outsider.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mark_emailed_moves_to_delivered(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    assert_eq!(
        state_of(&app, &order.id).await,
        "paid",
        "an email-only order stays paid"
    );
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
    let (emailed, delivered): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT e.emailed_at, o.digital_delivered_at FROM orders o \
             JOIN order_delivery_emails e ON e.order_id = o.id WHERE o.id = $1::uuid",
    )
    .bind(&order.id)
    .fetch_one(&app.pool)
    .await
    .expect("stamps");
    assert!(emailed.is_some() && delivered.is_some());
    let (_, _, body) = read_email(&app, &buyer.token, &order.id).await;
    assert!(body["emailed_at"].is_string(), "{body}");
    let notified: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_delivered' \
         AND payload->>'recipient_pubky' = $1",
    )
    .bind(&buyer.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("notification");
    assert_eq!(notified, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mark_delivered_message_channel(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", json!({ "kind": "message" })).await;
    let order = one_order(&app, &buyer, &[(&seller, "guide_01")], None).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    assert_eq!(state_of(&app, &order.id).await, "paid");
    let (status, body) = mark(&app, &seller, &order.id, "message").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
    assert!(
        email_rows(&app).await.is_empty(),
        "a message order stores no email"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn deliver_digital_refusals(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    // Not paid yet.
    let (status, _) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::CONFLICT);
    paykit_pay(&app, &paykit, &buyer, &order).await;
    // Buyer cannot mark.
    let (status, _) = mark(&app, &buyer, &order.id, "email").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Wrong channel.
    let (status, body) = mark(&app, &seller, &order.id, "message").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("wrong_delivery_channel"));
    // Cancellation requested: resolve it first.
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "Wrong item" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("cancellation request"));
    // A non-digital order has no manual channel.
    let shipper = new_actor(&app).await;
    let (status, _) = execute(&app, &shipper.token, &register_command(&shipper.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&shipper.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let shipped = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order")
        .to_string();
    let (status, body) = mark(&app, &shipper, &shipped, "email").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("wrong_delivery_channel"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_vs_mark_emailed_single_winner(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    for _ in 0..3 {
        let (seller, buyer, order) = email_order(&app, &paykit).await;
        paykit_pay(&app, &paykit, &buyer, &order).await;
        let revision = revision_of(&app, &order.id).await;
        let cancel = order_cmd(
            "order.cancel_request",
            &order.id,
            revision,
            json!({ "reason": "Changed my mind" }),
        );
        let deliver = order_cmd(
            "fulfillment.deliver_digital",
            &order.id,
            revision,
            json!({ "channel": "email" }),
        );
        let (cancelled, delivered) = tokio::join!(
            execute(&app, &buyer.token, &cancel),
            execute(&app, &seller.token, &deliver),
        );
        let winners = [&cancelled, &delivered]
            .iter()
            .filter(|(status, _)| *status == StatusCode::OK)
            .count();
        assert_eq!(winners, 1, "{cancelled:?} {delivered:?}");
        let loser = if cancelled.0 == StatusCode::OK {
            &delivered
        } else {
            &cancelled
        };
        assert_eq!(loser.0, StatusCode::CONFLICT, "{loser:?}");
        let state = state_of(&app, &order.id).await;
        assert!(
            matches!(state.as_str(), "cancel_requested" | "delivered"),
            "{state}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_before_emailed_purges_on_schedule(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "Changed my mind" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = act(
        &app,
        &seller.token,
        "order.cancel_approve",
        &order.id,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(state_of(&app, &order.id).await, "cancelled");
    let now = app.clock.now();
    assert_eq!(
        purge_delivery_emails(&app.pool, now + chrono::Duration::days(29), 30, 7)
            .await
            .expect("purge"),
        0
    );
    assert_eq!(
        purge_delivery_emails(&app.pool, now + chrono::Duration::days(31), 30, 7)
            .await
            .expect("purge"),
        1
    );
    assert_eq!(email_rows(&app).await, vec![(order.id.clone(), false)]);
}

async fn ready(app: &TestApp) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", "/ready", None, &Value::Null).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn delivery_email_retention_sweep(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let start = app.clock.now();
    // Completed after an emailed delivery.
    let (seller, buyer, completed) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &completed).await;
    let (status, body) = mark(&app, &seller, &completed.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    sqlx::query("UPDATE orders SET state = 'completed' WHERE id = $1::uuid")
        .bind(&completed.id)
        .execute(&app.pool)
        .await
        .expect("complete");
    // An unpaid checkout that expired.
    let (_seller2, _buyer2, unpaid) = email_order(&app, &paykit).await;
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1::uuid")
        .bind(&unpaid.id)
        .execute(&app.pool)
        .await
        .expect("cancel");
    // A live paid order is never purged.
    let (_seller3, buyer3, live) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer3, &live).await;

    let (status, _) = ready(&app).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        purge_delivery_emails(&app.pool, start + chrono::Duration::days(6), 30, 7)
            .await
            .expect("purge"),
        0
    );
    assert_eq!(
        purge_delivery_emails(&app.pool, start + chrono::Duration::days(8), 30, 7)
            .await
            .expect("purge"),
        1
    );
    // Past retention plus grace with the purge not run: readiness fails.
    app.clock.set(start + chrono::Duration::days(40));
    let (status, body) = ready(&app).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["reason"], json!("delivery_email_purge_overdue"));
    assert_eq!(
        purge_delivery_emails(&app.pool, start + chrono::Duration::days(40), 30, 7)
            .await
            .expect("purge"),
        1
    );
    let (status, _) = ready(&app).await;
    assert_eq!(status, StatusCode::OK);

    let kept: Vec<(String, bool, bool, bool)> = sqlx::query_as(
        "SELECT order_id::text, email_ciphertext IS NOT NULL, emailed_at IS NOT NULL, \
         purged_at IS NOT NULL FROM order_delivery_emails ORDER BY order_id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("rows");
    let row = |id: &str| kept.iter().find(|row| row.0 == id).cloned().expect("row");
    assert_eq!(
        row(&completed.id),
        (completed.id.clone(), false, true, true),
        "emailed_at kept"
    );
    assert_eq!(row(&unpaid.id), (unpaid.id.clone(), false, false, true));
    assert_eq!(row(&live.id), (live.id.clone(), true, false, false));
    // The worker pass runs the purge.
    let summary = marketplace_service::workers::run_once(
        &app.state,
        Uuid::new_v4(),
        start + chrono::Duration::days(41),
    )
    .await
    .expect("worker pass");
    assert_eq!(summary.delivery_emails_purged, 0);
}

/// Pays an order through the real Paykit rail: bind Bitcoin, deliver the
/// activation, and let the worker observe the confirmed payment.
async fn paykit_pay(app: &TestApp, paykit: &FakePaykit, buyer: &TestActor, order: &Order) {
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.id),
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
    let reference = attempt_reference(Uuid::parse_str(&order.id).expect("order uuid"), 1);
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(app, app.clock.now()).await >= 1);
}

async fn bitcoin_seller(app: &TestApp, paykit: &FakePaykit) -> TestActor {
    let seller = new_actor(app).await;
    paykit.set_allocation_mode("exclusive");
    common::paykit_review::enable_bitcoin(app, paykit, &seller).await;
    seller
}

async fn read_download(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, String, Value) {
    read_download_line(app, token, order_id, 0).await
}

async fn read_download_line(
    app: &TestApp,
    token: &str,
    order_id: &str,
    line_index: i32,
) -> (StatusCode, String, Value) {
    get_raw(
        app,
        token,
        &format!("/v1/orders/{order_id}/digital-delivery/{line_index}"),
    )
    .await
}

async fn access_lines(app: &TestApp, order_id: &str) -> Vec<i32> {
    sqlx::query_scalar(
        "SELECT DISTINCT line_index FROM order_digital_access WHERE order_id = $1::uuid ORDER BY line_index",
    )
    .bind(order_id)
    .fetch_all(&app.pool)
    .await
    .expect("access lines")
}

async fn stock_of(app: &TestApp, seller: &TestActor, listing_id: &str) -> (i64, i64) {
    sqlx::query_as("SELECT available_quantity, sold_quantity FROM listings WHERE aggregate_id = $1")
        .bind(aggregate(&seller.pubky, listing_id))
        .fetch_one(&app.pool)
        .await
        .expect("stock")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mixed_kind_order_stays_paid(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", text_kind()).await;
    listing(&app, &seller, "guide_02", email_kind()).await;
    let order = one_order(
        &app,
        &buyer,
        &[(&seller, "guide_01"), (&seller, "guide_02")],
        Some(EMAIL),
    )
    .await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    assert_eq!(
        state_of(&app, &order.id).await,
        "paid",
        "the email line is still to deliver"
    );
    let pins: Vec<i32> =
        sqlx::query_scalar("SELECT line_index FROM order_digital_pins WHERE order_id = $1::uuid")
            .bind(&order.id)
            .fetch_all(&app.pool)
            .await
            .expect("pins");
    assert_eq!(pins, vec![0], "only the instant line is pinned");
    let (status, _, body) = read_download(&app, &buyer.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["lines"][0]["text"], json!(TEXT));
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paid_digital_cancel_request(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (_seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "Changed my mind" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancel_requested"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_approve_keeps_opened_instant_line_sold(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    listing(&app, &seller, "text_01", text_kind()).await;
    listing(&app, &seller, "email_01", email_kind()).await;
    let lines: &[(&TestActor, &str)] = &[(&seller, "text_01"), (&seller, "email_01")];
    let cancel = |order_id: String, buyer_token: String| {
        let app = &app;
        let seller_token = seller.token.clone();
        async move {
            let (status, body) = act(
                app,
                &buyer_token,
                "order.cancel_request",
                &order_id,
                json!({ "reason": "Changed my mind" }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let (status, body) = act(
                app,
                &seller_token,
                "order.cancel_approve",
                &order_id,
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
    };

    // Opened: the buyer read the text line, so it stays sold; the email line
    // restocks.
    let opener = new_actor(&app).await;
    let opened = one_order(&app, &opener, lines, Some(EMAIL)).await;
    paykit_pay(&app, &paykit, &opener, &opened).await;
    let (status, _, body) = read_download(&app, &opener.token, &opened.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    cancel(opened.id.clone(), opener.token.clone()).await;
    assert_eq!(
        stock_of(&app, &seller, "text_01").await,
        (4, 1),
        "an opened instant line stays sold"
    );
    assert_eq!(
        stock_of(&app, &seller, "email_01").await,
        (5, 0),
        "an email line restocks"
    );
    let (status, _, body) = read_download(&app, &opener.token, &opened.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("delivery_ended"));

    // Never opened: every line restocks.
    let holder = new_actor(&app).await;
    let unopened = one_order(&app, &holder, lines, Some(EMAIL)).await;
    paykit_pay(&app, &paykit, &holder, &unopened).await;
    cancel(unopened.id.clone(), holder.token.clone()).await;
    assert_eq!(
        stock_of(&app, &seller, "text_01").await,
        (4, 1),
        "the unopened text restocked"
    );
    assert_eq!(stock_of(&app, &seller, "email_01").await, (5, 0));
}

// Review P1 (Shop Wave 2): opening one instant line releases and logs only
// that line, so an unopened instant line on the same order still restocks
// when a cancel is approved (§3.6 E8).
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn opening_one_line_releases_and_logs_only_that_line(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    listing(&app, &seller, "text_01", text_kind()).await;
    listing(&app, &seller, "text_02", text_kind()).await;
    listing(&app, &seller, "email_01", email_kind()).await;
    let lines: &[(&TestActor, &str)] = &[
        (&seller, "text_01"),
        (&seller, "text_02"),
        (&seller, "email_01"),
    ];
    let buyer = new_actor(&app).await;
    let order = one_order(&app, &buyer, lines, Some(EMAIL)).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;

    let (status, cache, body) = read_download_line(&app, &buyer.token, &order.id, 0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(cache, "no-store");
    let released = body["lines"].as_array().expect("lines");
    assert_eq!(released.len(), 1, "{body}");
    assert_eq!(released[0]["line_index"], json!(0));
    assert_eq!(access_lines(&app, &order.id).await, vec![0]);

    // A manual line and an index the order does not have release nothing.
    for missing in [2, 7] {
        let (status, _, body) = read_download_line(&app, &buyer.token, &order.id, missing).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }
    assert_eq!(access_lines(&app, &order.id).await, vec![0]);

    let (status, body) = act(
        &app,
        &buyer.token,
        "order.cancel_request",
        &order.id,
        json!({ "reason": "Changed my mind" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = act(
        &app,
        &seller.token,
        "order.cancel_approve",
        &order.id,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        stock_of(&app, &seller, "text_01").await,
        (4, 1),
        "the opened line stays sold"
    );
    assert_eq!(
        stock_of(&app, &seller, "text_02").await,
        (5, 0),
        "the unopened instant line restocks"
    );
    assert_eq!(stock_of(&app, &seller, "email_01").await, (5, 0));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn late_completion_after_email_purge(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.id),
        Some(&buyer.token),
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
    // Detected inside the window: the poller keeps watching after expiry.
    let reference = attempt_reference(Uuid::parse_str(&order.id).expect("uuid"), 1);
    paykit.set_status(&reference, status_detected("exclusive", 1));
    poll_now(&app, app.clock.now()).await;
    let (request_state,): (String,) =
        sqlx::query_as("SELECT paykit_request_state FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.id).expect("uuid"))
            .fetch_one(&app.pool)
            .await
            .expect("request state");
    assert_eq!(request_state, "detected");
    let expired_at = app.clock.now() + chrono::Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&app.state, expired_at)
            .await
            .expect("expire")
            >= 1
    );
    // The unpaid checkout's address purges after 7 days.
    let later = expired_at + chrono::Duration::days(8);
    assert_eq!(
        purge_delivery_emails(&app.pool, later, 30, 7)
            .await
            .expect("purge"),
        1
    );
    // The detected payment confirms late: the unit is free, the order
    // completes.
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, later + chrono::Duration::seconds(60)).await >= 1);
    assert_eq!(
        state_of(&app, &order.id).await,
        "paid",
        "an email order stays paid"
    );
    let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("email_missing"));
    let (status, body) = act(
        &app,
        &buyer.token,
        "order.set_delivery_email",
        &order.id,
        json!({ "delivery_email": EMAIL_2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["delivery_email"], json!(EMAIL_2));
}

// The seller's email read checks the entitlement and opens the address in
// one transaction holding the order: a cancel that is committing while the
// read is in flight is seen, and the address is not revealed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancel_racing_seller_email_read_never_reveals(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool.clone()).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let mut cancel = pool.begin().await.expect("cancel transaction");
    sqlx::query("SELECT id FROM orders WHERE id = $1::uuid FOR UPDATE")
        .bind(&order.id)
        .execute(&mut *cancel)
        .await
        .expect("lock order");
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1::uuid")
        .bind(&order.id)
        .execute(&mut *cancel)
        .await
        .expect("cancel");
    let read = {
        let router = app.router.clone();
        let token = seller.token.clone();
        let order_id = order.id.clone();
        tokio::spawn(async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(format!("/v1/orders/{order_id}/delivery-email"))
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::empty())
                        .expect("request builds"),
                )
                .await
                .expect("request executes");
            let status = response.status();
            let bytes = http_body_util::BodyExt::collect(response.into_body())
                .await
                .expect("body")
                .to_bytes();
            (status, String::from_utf8_lossy(&bytes).into_owned())
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    cancel.commit().await.expect("cancel commits");
    let (status, body) = read.await.expect("read task");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.contains("delivery_ended"), "{body}");
    assert!(!body.contains(EMAIL), "the address was revealed: {body}");
}

/// What a write attempted while a delivery-email read was in flight did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attempt {
    /// The write waited on the read's locks and timed out.
    Blocked,
    /// The write committed while the read still held the address.
    Committed,
}

#[derive(Clone, Copy)]
enum ConcurrentWrite {
    /// The cancel's order-row write.
    Cancel,
    /// The real purge SQL, run past the retention window.
    Purge { now: chrono::DateTime<chrono::Utc> },
}

/// At the read's release point, tries one concurrent write on another
/// connection under a short lock timeout and records what happened.
struct WriteDuringRead {
    pool: PgPool,
    write: ConcurrentWrite,
    outcome: Mutex<Option<Attempt>>,
}

impl WriteDuringRead {
    fn new(pool: &PgPool, write: ConcurrentWrite) -> Self {
        Self {
            pool: pool.clone(),
            write,
            outcome: Mutex::new(None),
        }
    }

    fn outcome(&self) -> Attempt {
        self.outcome
            .lock()
            .expect("outcome")
            .expect("the read reached its release point")
    }
}

impl DeliveryEmailReadHook for WriteDuringRead {
    fn before_release<'a>(
        &'a self,
        order_id: Uuid,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.expect("write transaction");
            sqlx::query("SET LOCAL lock_timeout = '300ms'")
                .execute(&mut *tx)
                .await
                .expect("lock timeout");
            let wrote = match self.write {
                ConcurrentWrite::Cancel => {
                    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1")
                        .bind(order_id)
                        .execute(&mut *tx)
                        .await
                        .map(|done| done.rows_affected())
                }
                ConcurrentWrite::Purge { now } => purge_delivery_emails(&mut *tx, now, 30, 7).await,
            };
            let outcome = match wrote {
                Ok(1) => {
                    tx.commit().await.expect("write commits");
                    Attempt::Committed
                }
                Ok(rows) => panic!("the concurrent write matched {rows} rows"),
                Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => {
                    Attempt::Blocked
                }
                Err(error) => panic!("the concurrent write failed: {error}"),
            };
            *self.outcome.lock().expect("outcome") = Some(outcome);
        })
    }
}

async fn read_with(
    app: &TestApp,
    actor: &TestActor,
    order_id: &str,
    hook: &WriteDuringRead,
) -> (StatusCode, String) {
    let response = read_delivery_email_with_hook(
        &app.state,
        &actor.pubky,
        Uuid::parse_str(order_id).expect("order uuid"),
        hook,
    )
    .await;
    let status = response.status();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

// A cancel that arrives while the seller's read holds the address waits for
// the read to finish: it cannot commit an ended state underneath a read that
// then reveals the address.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_read_in_flight_holds_off_cancel(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool.clone()).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let hook = WriteDuringRead::new(&pool, ConcurrentWrite::Cancel);
    let (status, body) = read_with(&app, &seller, &order.id, &hook).await;
    assert_eq!(
        hook.outcome(),
        Attempt::Blocked,
        "the cancel committed during the read; the read returned {status}: {body}"
    );
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(EMAIL), "{body}");
    // Once the read is done the cancel goes through and ends the seller's access.
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1::uuid")
        .bind(&order.id)
        .execute(&pool)
        .await
        .expect("cancel");
    let (status, _, body) = read_email(&app, &seller.token, &order.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("delivery_ended"));
}

// A purge that arrives while the buyer's read holds the address waits for
// the read: it cannot delete the ciphertext underneath a read that then
// reveals it.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyer_read_in_flight_holds_off_purge(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool.clone()).await;
    let (_seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1::uuid")
        .bind(&order.id)
        .execute(&pool)
        .await
        .expect("ended");
    let past_retention = app.clock.now() + chrono::Duration::days(31);
    let hook = WriteDuringRead::new(
        &pool,
        ConcurrentWrite::Purge {
            now: past_retention,
        },
    );
    let (status, body) = read_with(&app, &buyer, &order.id, &hook).await;
    assert_eq!(
        hook.outcome(),
        Attempt::Blocked,
        "the purge committed during the read; the read returned {status}: {body}"
    );
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(EMAIL), "{body}");
    // Once the read is done the purge deletes the address.
    assert_eq!(
        purge_delivery_emails(&pool, past_retention, 30, 7)
            .await
            .expect("purge"),
        1
    );
    let (status, _, body) = read_email(&app, &buyer.token, &order.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("email_missing"));
}

async fn delivered_notifications(app: &TestApp, buyer: &TestActor) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_delivered' \
         AND payload->>'recipient_pubky' = $1",
    )
    .bind(&buyer.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("notifications")
}

// An order with an email line and a message line is delivered, and the
// buyer told once, only when the second channel is marked.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_manual_channels_notify_once_when_both_marked(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let seller = bitcoin_seller(&app, &paykit).await;
    let buyer = new_actor(&app).await;
    listing(&app, &seller, "guide_01", email_kind()).await;
    listing(&app, &seller, "guide_02", json!({ "kind": "message" })).await;
    let order = one_order(
        &app,
        &buyer,
        &[(&seller, "guide_01"), (&seller, "guide_02")],
        Some(EMAIL),
    )
    .await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let stamps = |app: &TestApp| {
        let pool = app.pool.clone();
        let id = order.id.clone();
        async move {
            sqlx::query_as::<_, (bool, bool, bool)>(
                "SELECT e.emailed_at IS NOT NULL, o.digital_message_delivered_at IS NOT NULL, \
                 o.digital_delivered_at IS NOT NULL FROM orders o \
                 JOIN order_delivery_emails e ON e.order_id = o.id WHERE o.id = $1::uuid",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("stamps")
        }
    };

    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));
    assert_eq!(stamps(&app).await, (true, false, false));
    assert_eq!(
        delivered_notifications(&app, &buyer).await,
        0,
        "a partial mark does not tell the buyer the order is delivered"
    );

    let (status, body) = mark(&app, &seller, &order.id, "message").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
    assert_eq!(stamps(&app).await, (true, true, true));
    assert_eq!(delivered_notifications(&app, &buyer).await, 1);
}

// The purge clock is when the order ended, not its last write: a review
// that lands 20 days after completion does not extend the address's life.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn later_review_does_not_extend_email_retention(pool: PgPool) {
    let (app, paykit, _server) = keyed_app(pool).await;
    let (seller, buyer, order) = email_order(&app, &paykit).await;
    paykit_pay(&app, &paykit, &buyer, &order).await;
    let (status, body) = mark(&app, &seller, &order.id, "email").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let completed_at = app.clock.now();
    let review = json!({ "rating": 5, "text": "Arrived by email." });
    let (status, body) = act(
        &app,
        &buyer.token,
        "review.create",
        &order.id,
        review.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(state_of(&app, &order.id).await, "completed");
    app.clock.set(completed_at + chrono::Duration::days(20));
    let (status, body) = act(&app, &seller.token, "review.create", &order.id, review).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(state_of(&app, &order.id).await, "completed");

    let past_retention = completed_at + chrono::Duration::days(32);
    assert_eq!(
        overdue_delivery_emails(&app.pool, past_retention, 30, 7)
            .await
            .expect("overdue"),
        1,
        "readiness counts the address as overdue from completion"
    );
    assert_eq!(
        purge_delivery_emails(&app.pool, completed_at + chrono::Duration::days(31), 30, 7)
            .await
            .expect("purge"),
        1,
        "the address is purged 30 days after completion"
    );
    assert_eq!(email_rows(&app).await, vec![(order.id.clone(), false)]);
}
