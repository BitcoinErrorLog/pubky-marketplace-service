//! Seller shipping configuration and Shippo label purchase: the write-only
//! sealed API token, ship-from persistence, seller-scoped rate quotes and
//! label purchase against a local Shippo double through the real client,
//! label idempotency, and the ADR-0019 §8 privacy boundary (the label never
//! appears in any shared order projection).

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::{json, Value};
use sqlx::PgPool;

const SHIPPO_KEY: &str = "shippo_test_1234567890abcdef";

fn ship_from_body() -> Value {
    json!({
        "name": "Igor's Olive Farm",
        "line1": "Maslinska 1",
        "line2": "",
        "city": "Split",
        "region": "",
        "postal_code": "21000",
        "country_code": "HR",
        "phone": "",
        "email": "",
    })
}

async fn put_shipping_config(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/shipping-config",
        Some(token),
        body,
    )
    .await
}

async fn configure_shipping(app: &TestApp, token: &str) {
    let (status, body) = put_shipping_config(
        app,
        token,
        &json!({ "shippo_api_key": SHIPPO_KEY, "ship_from": ship_from_body() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "shipping config failed: {body}");
}

async fn quote_rates(
    app: &TestApp,
    token: &str,
    order_id: &str,
    parcel: &Value,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/shipping/rates"),
        Some(token),
        parcel,
    )
    .await
}

async fn buy_label(
    app: &TestApp,
    token: &str,
    order_id: &str,
    rate_id: &str,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/shipping/label"),
        Some(token),
        &json!({ "rate_id": rate_id }),
    )
    .await
}

fn parcel_body() -> Value {
    json!({ "weight_grams": 900, "length_mm": 300, "width_mm": 200, "height_mm": 150 })
}

#[sqlx::test(migrations = "./migrations")]
async fn shipping_config_upserts_and_never_returns_the_token(pool: PgPool) {
    let (app, _shippo) = test_app_with_shippo(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = put_shipping_config(
        &app,
        &seller.token,
        &json!({ "shippo_api_key": SHIPPO_KEY, "ship_from": ship_from_body() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shipping_config"]["shippo_api_key_set"], json!(true));
    assert_eq!(body["shipping_config"]["ship_from"]["city"], json!("Split"));
    assert!(
        !body.to_string().contains(SHIPPO_KEY),
        "the token must never be echoed"
    );

    // Omitting the key preserves it; the address is replaced.
    let mut moved = ship_from_body();
    moved["city"] = json!("Zagreb");
    let (status, body) =
        put_shipping_config(&app, &seller.token, &json!({ "ship_from": moved })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shipping_config"]["shippo_api_key_set"], json!(true));
    assert_eq!(
        body["shipping_config"]["ship_from"]["city"],
        json!("Zagreb")
    );

    // An empty string clears the key.
    let (status, body) = put_shipping_config(
        &app,
        &seller.token,
        &json!({ "shippo_api_key": "", "ship_from": ship_from_body() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["shipping_config"]["shippo_api_key_set"], json!(false));

    // Malformed inputs are refused.
    let (status, body) = put_shipping_config(
        &app,
        &seller.token,
        &json!({ "shippo_api_key": "sk_live_wrong" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["reason"], json!("invalid_shippo_key"));
    let mut bad_address = ship_from_body();
    bad_address["country_code"] = json!("Croatia");
    let (status, body) =
        put_shipping_config(&app, &seller.token, &json!({ "ship_from": bad_address })).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["reason"], json!("invalid_ship_from"));
}

#[sqlx::test(migrations = "./migrations")]
async fn rates_are_quoted_with_the_sellers_token_and_real_order_address(pool: PgPool) {
    let (app, shippo) = test_app_with_shippo(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    shippo.accept_key(SHIPPO_KEY);
    shippo.add_rate("rate_ground", "USPS", "7.85");
    shippo.add_rate("rate_express", "UPS", "24.10");
    configure_shipping(&app, &seller.token).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    // Only the seller quotes.
    let (status, _) = quote_rates(&app, &buyer.token, &order.order_id, &parcel_body()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = quote_rates(&app, &seller.token, &order.order_id, &parcel_body()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rates = body["rates"].as_array().expect("rates array");
    assert_eq!(rates.len(), 2);
    assert_eq!(rates[0]["provider"], json!("USPS"));
    assert_eq!(rates[0]["amount"], json!("7.85"));

    // The quote carried the seller's ship-from, the ORDER's delivery
    // address, and the metric parcel.
    let requests = shippo.shipment_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["address_from"]["city"], json!("Split"));
    assert_eq!(requests[0]["address_to"]["zip"], json!("10001"));
    assert_eq!(requests[0]["parcels"][0]["weight"], json!("900"));
    assert_eq!(requests[0]["parcels"][0]["mass_unit"], json!("g"));
    assert_eq!(requests[0]["parcels"][0]["length"], json!("30.0"));
}

#[sqlx::test(migrations = "./migrations")]
async fn label_purchase_stores_the_label_seller_only_and_is_idempotent(pool: PgPool) {
    let (app, shippo) = test_app_with_shippo(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    shippo.accept_key(SHIPPO_KEY);
    shippo.add_rate("rate_ground", "USPS", "7.85");
    configure_shipping(&app, &seller.token).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    let (status, body) = buy_label(&app, &seller.token, &order.order_id, "rate_ground").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let label = &body["label"];
    assert_eq!(label["carrier"], json!("USPS"));
    assert_eq!(label["amount"], json!("7.85"));
    assert_eq!(label["tracking_number"], json!("SHIPPO_TRACK_123"));
    assert!(label["label_url"].as_str().unwrap().ends_with(".pdf"));

    // Idempotent: a second purchase returns the stored label, buying nothing.
    let (status, body) = buy_label(&app, &seller.token, &order.order_id, "rate_ground").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["label"]["transaction_id"], json!("txn_1"));
    assert_eq!(shippo.purchases().len(), 1);

    // The label is seller-readable...
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/orders/{}/shipping/label", order.order_id),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // ...never buyer-readable (the PDF embeds the delivery address)...
    let (status, _) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/orders/{}/shipping/label", order.order_id),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // ...and never present in the shared order projection for either side.
    for token in [&seller.token, &buyer.token] {
        let (status, body) = send(
            app.router.clone(),
            "GET",
            &format!("/v1/orders/{}", order.order_id),
            Some(token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !body.to_string().contains("label_url"),
            "the shared projection must not leak the label: {body}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn shippo_failures_surface_honestly(pool: PgPool) {
    let (app, shippo) = test_app_with_shippo(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    configure_shipping(&app, &seller.token).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    // The double does not accept the token yet: key invalid.
    let (status, body) = quote_rates(&app, &seller.token, &order.order_id, &parcel_body()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("shippo_key_invalid"));

    // A refused purchase carries Shippo's own message.
    shippo.accept_key(SHIPPO_KEY);
    shippo.add_rate("rate_ground", "USPS", "7.85");
    shippo.reject_purchase("Insufficient funds in your Shippo account.");
    let (status, body) = buy_label(&app, &seller.token, &order.order_id, "rate_ground").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("shippo_rejected"));
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Insufficient funds"));
}

#[sqlx::test(migrations = "./migrations")]
async fn shipping_requires_a_paid_order(pool: PgPool) {
    let (app, shippo) = test_app_with_shippo(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    shippo.accept_key(SHIPPO_KEY);
    shippo.add_rate("rate_ground", "USPS", "7.85");
    configure_shipping(&app, &seller.token).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let pending_order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = quote_rates(&app, &seller.token, &pending_order_id, &parcel_body()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_shippable"));
}

#[sqlx::test(migrations = "./migrations")]
async fn shipping_requires_seller_configuration(pool: PgPool) {
    let (app, shippo) = test_app_with_shippo(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    shippo.accept_key(SHIPPO_KEY);
    shippo.add_rate("rate_ground", "USPS", "7.85");

    // A paid order, but the seller never configured shipping: the exact
    // missing piece is named.
    let order = create_paid_order(&app, &seller, &buyer).await;
    let (status, body) = quote_rates(&app, &seller.token, &order.order_id, &parcel_body()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("shipping_not_configured"));
}
