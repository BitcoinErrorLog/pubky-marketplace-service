//! Seller payment-method configuration, per-order method binding, and the
//! fiat verification legs (processor-verified Stripe, seller-attested
//! PayPal), plus the paykit verification worker for physical bitcoin orders.

mod common;

use axum::http::StatusCode;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payments::{order_reference, PaykitStatusOutcome};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const RESTRICTED_KEY: &str = "rk_test_51NxyzMarketplace";

async fn put_config(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        body,
    )
    .await
}

async fn get_own_config(app: &TestApp, token: Option<&str>) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        "/v0/sellers/me/payment-config",
        token,
        &Value::Null,
    )
    .await
}

async fn get_public_config(app: &TestApp, seller_pubky: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v0/sellers/{seller_pubky}/payment-config"),
        None,
        &Value::Null,
    )
    .await
}

async fn bind_method(
    app: &TestApp,
    token: &str,
    order_id: &str,
    method: &str,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": method }),
    )
    .await
}

fn full_config_body() -> Value {
    json!({
        "bitcoin_enabled": true,
        "stripe_payment_link": "https://buy.stripe.com/test_abc123",
        "stripe_restricted_key": RESTRICTED_KEY,
        "paypal_merchant_email": "merchant@example.com",
    })
}

/// Registers a SAT-priced listing and checks out, leaving a
/// satoshi-denominated pending order.
async fn create_pending_sat_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> PendingOrder {
    let (status, body) = execute(app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
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

// ---------------------------------------------------------------------------
// Seller payment configuration
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn payment_config_upserts_and_never_returns_the_restricted_key(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;

    let (status, body) = put_config(&app, &seller.token, &full_config_body()).await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let config = &body["payment_config"];
    assert_eq!(config["bitcoin_enabled"], json!(true));
    assert_eq!(
        config["stripe_payment_link"],
        json!("https://buy.stripe.com/test_abc123")
    );
    assert_eq!(
        config["paypal_merchant_email"],
        json!("merchant@example.com")
    );
    assert_eq!(config["stripe_restricted_key_set"], json!(true));
    assert!(
        !body.to_string().contains(RESTRICTED_KEY),
        "the restricted key must never appear in a response"
    );

    // The key is sealed at rest: the raw value never reaches the table.
    let (ciphertext,): (Vec<u8>,) = sqlx::query_as(
        "SELECT stripe_restricted_key_ciphertext FROM seller_payment_configs \
         WHERE seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .fetch_one(&pool)
    .await
    .expect("config row exists");
    assert!(!ciphertext
        .windows(RESTRICTED_KEY.len())
        .any(|window| window == RESTRICTED_KEY.as_bytes()));

    // Omitting the write-only key preserves it; other fields replace.
    let (status, body) = put_config(
        &app,
        &seller.token,
        &json!({
            "bitcoin_enabled": false,
            "paypal_merchant_email": "other@example.com",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config update failed: {body}");
    let config = &body["payment_config"];
    assert_eq!(config["bitcoin_enabled"], json!(false));
    assert_eq!(config["stripe_payment_link"], Value::Null);
    assert_eq!(config["paypal_merchant_email"], json!("other@example.com"));
    assert_eq!(config["stripe_restricted_key_set"], json!(true));

    // An explicit empty string clears the stored key.
    let (status, body) = put_config(
        &app,
        &seller.token,
        &json!({ "bitcoin_enabled": false, "stripe_restricted_key": "" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config clear failed: {body}");
    assert_eq!(
        body["payment_config"]["stripe_restricted_key_set"],
        json!(false)
    );

    // The public projection never carries key material at all.
    paykit.set_claimed(&seller.pubky);
    let (status, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_object().map(|object| {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort();
            keys.iter().map(|key| key.as_str()).collect::<Vec<_>>()
        }),
        Some(vec![
            "bitcoin_available",
            "paypal_merchant_email",
            "stripe_payment_link",
        ])
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn payment_config_validation_refuses_bad_inputs(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    for (body, reason) in [
        (
            json!({ "bitcoin_enabled": false, "stripe_payment_link": "https://evil.example/x" }),
            "invalid_payment_link",
        ),
        (
            json!({ "bitcoin_enabled": false, "stripe_payment_link": "http://buy.stripe.com/x" }),
            "invalid_payment_link",
        ),
        (
            json!({ "bitcoin_enabled": false, "paypal_merchant_email": "not-an-email" }),
            "invalid_paypal_email",
        ),
        (
            json!({ "bitcoin_enabled": false, "stripe_restricted_key": "sk_test_secret12345" }),
            "invalid_restricted_key",
        ),
    ] {
        let (status, response) = put_config(&app, &seller.token, &body).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body}: {response}"
        );
        assert_eq!(response["error"]["reason"], json!(reason), "{body}");
    }
    // Nothing was stored by the rejected writes.
    let (status, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["stripe_payment_link"], Value::Null);
}

#[sqlx::test(migrations = "./migrations")]
async fn public_config_reports_bitcoin_availability_from_paykit(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;

    // No config at all: everything unavailable.
    let (status, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bitcoin_available"], json!(false));

    let (status, _) = put_config(&app, &seller.token, &full_config_body()).await;
    assert_eq!(status, StatusCode::OK);

    // Enabled but unclaimed on paykit-server: not available.
    let (status, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bitcoin_available"], json!(false));

    // Claimed: available.
    paykit.set_claimed(&seller.pubky);
    let (status, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bitcoin_available"], json!(true));

    let (status, body) = get_public_config(&app, "not-a-pubky").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["reason"], json!("invalid_pubky"));
}

#[sqlx::test(migrations = "./migrations")]
async fn own_config_returns_the_stored_row_verbatim(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;

    let (status, put_body) = put_config(&app, &seller.token, &full_config_body()).await;
    assert_eq!(status, StatusCode::OK, "config put failed: {put_body}");

    // The seller has NOT claimed a watch-only account on paykit-server, yet
    // the own view still reports bitcoin_enabled as stored: this endpoint is
    // the raw row, not availability, and performs no paykit lookup.
    let (status, body) = get_own_config(&app, Some(&seller.token)).await;
    assert_eq!(status, StatusCode::OK, "own config read failed: {body}");
    assert_eq!(body["payment_config"], put_body["payment_config"]);
    let config = &body["payment_config"];
    assert_eq!(config["bitcoin_enabled"], json!(true));
    assert_eq!(
        config["stripe_payment_link"],
        json!("https://buy.stripe.com/test_abc123")
    );
    assert_eq!(
        config["paypal_merchant_email"],
        json!("merchant@example.com")
    );
    assert_eq!(config["stripe_restricted_key_set"], json!(true));
    assert!(
        config["updated_at"].as_str().is_some(),
        "updated_at must be present: {config}"
    );
    assert!(
        !body.to_string().contains(RESTRICTED_KEY),
        "the restricted key must never appear in a response"
    );

    // The row is scoped to the session identity: another seller sees nothing.
    let stranger = new_actor(&app).await;
    let (status, body) = get_own_config(&app, Some(&stranger.token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["payment_config"], Value::Null);
}

#[sqlx::test(migrations = "./migrations")]
async fn own_config_is_null_before_the_first_save(pool: PgPool) {
    // Deliberately without the payments runtime: reading the stored row must
    // not be gated on payment rails being configured.
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = get_own_config(&app, Some(&seller.token)).await;
    assert_eq!(status, StatusCode::OK, "own config read failed: {body}");
    assert_eq!(body, json!({ "payment_config": Value::Null }));
}

#[sqlx::test(migrations = "./migrations")]
async fn own_config_requires_a_session(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let (status, _) = get_own_config(&app, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn the_payment_surface_is_refused_without_the_runtime(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = put_config(&app, &seller.token, &full_config_body()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["reason"], json!("payments_disabled"));
}

// ---------------------------------------------------------------------------
// Method binding
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn stripe_binding_snapshots_the_checkout_url(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK, "stripe bind failed: {body}");
    let bound = &body["order"];
    assert_eq!(bound["payment_method"], json!("stripe"));
    assert_eq!(bound["fiat_verification"], json!("processor"));
    assert_eq!(
        bound["fiat_checkout_url"],
        json!(format!(
            "https://buy.stripe.com/test_abc123?client_reference_id={}",
            order.order_id
        ))
    );
    // The payment adapter leaves the sandbox path permanently.
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["payment"]["adapter"], json!("stripe"));

    // Re-binding the same method is idempotent; a different method refused.
    let (status, _) = bind_method(&app, &buyer.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "paypal").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"]["reason"],
        json!("payment_method_already_bound")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn binding_rejections_cover_role_availability_and_currency(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // Seller config with NO rails at all.
    put_config(&app, &seller.token, &json!({ "bitcoin_enabled": false })).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    // Only the buyer binds.
    let (status, body) = bind_method(&app, &seller.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // Unknown method.
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "wire").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["reason"], json!("invalid_method"));

    // Unconfigured rails are refused per method.
    for method in ["stripe", "paypal", "bitcoin"] {
        let (status, body) = bind_method(&app, &buyer.token, &order.order_id, method).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!("method_unavailable"),
            "{method}"
        );
    }

    // Bitcoin on a USD order: currency unsupported even when enabled.
    put_config(&app, &seller.token, &json!({ "bitcoin_enabled": true })).await;
    paykit.set_claimed(&seller.pubky);
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"]["reason"],
        json!("currency_unsupported"),
        "{body}"
    );

    // An unknown order is 404.
    let (status, _) = bind_method(&app, &buyer.token, &Uuid::new_v4().to_string(), "stripe").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(paykit.requests().is_empty(), "nothing reached paykit");
}

#[sqlx::test(migrations = "./migrations")]
async fn paypal_binding_builds_the_seller_direct_checkout_url(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "paypal").await;
    assert_eq!(status, StatusCode::OK, "paypal bind failed: {body}");
    let bound = &body["order"];
    assert_eq!(bound["payment_method"], json!("paypal"));
    assert_eq!(bound["fiat_verification"], json!("seller-attested"));
    let url = bound["fiat_checkout_url"].as_str().expect("checkout url");
    assert!(url.starts_with("https://www.paypal.com/cgi-bin/webscr?cmd=_xclick"));
    assert!(url.contains("business=merchant%40example.com"), "{url}");
    // The 12_500 + 1_200 shipping + 1_096 tax fixture totals 147.96 USD.
    assert!(url.contains("amount=147.96"), "{url}");
    assert!(url.contains("currency_code=USD"), "{url}");
    assert!(url.contains(&format!("custom={}", order.order_id)), "{url}");
    // The buyer must be sent back to their orders page after commit —
    // without a `return` URL PayPal's legacy WPS flow dead-ends post-payment.
    assert!(
        url.contains("return=https%3A%2F%2Fapp.test%2Fmarketplace%2Forders"),
        "{url}"
    );
    assert!(
        url.contains("cancel_return=https%3A%2F%2Fapp.test%2Fmarketplace%2Forders"),
        "{url}"
    );
    assert!(url.contains("no_shipping=1"), "{url}");
    assert!(url.contains("no_note=1"), "{url}");
}

#[sqlx::test(migrations = "./migrations")]
async fn bitcoin_binding_creates_the_signed_paykit_request(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    paykit.set_claimed(&seller.pubky);
    let order = create_pending_sat_order(&app, &seller, &buyer).await;

    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    let bound = &body["order"];
    let order_uuid = Uuid::parse_str(&order.order_id).expect("order id is a uuid");
    let reference = order_reference(order_uuid);
    assert_eq!(bound["payment_method"], json!("bitcoin"));
    assert_eq!(bound["fiat_verification"], Value::Null);
    assert_eq!(bound["fiat_checkout_url"], Value::Null);
    assert_eq!(bound["paykit_request_reference"], json!(reference));
    assert_eq!(bound["paykit_request_state"], json!("pending"));

    // The fake paykit-server verified the request signature before
    // recording it; the amount is the order's satoshi total.
    let total_sats = bound["total"]["amount_minor"].as_u64().expect("sat total");
    assert_eq!(
        paykit.requests(),
        vec![common::FakePaykitRequest {
            creator: format!("pubky{}", seller.pubky),
            reader: format!("pubky{}", buyer.pubky),
            reference: reference.clone(),
            amount_sats: total_sats,
        }]
    );
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["payment"]["adapter"], json!("paykit"));

    // Idempotent re-bind replays the request (same reference) harmlessly.
    let (status, _) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        paykit.requests().len(),
        1,
        "an idempotent re-bind is a read"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_refused_paykit_request_leaves_the_order_unbound(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    paykit.set_claimed(&seller.pubky);
    paykit.fail_creation_with("creator_session_invalid");
    let order = create_pending_sat_order(&app, &seller, &buyer).await;

    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["reason"], json!("seller_account_unclaimed"));

    // Nothing persisted: the order can still bind once the seller fixes
    // their account.
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["payment_method"], Value::Null);
    assert_eq!(order_view["paykit_request_reference"], Value::Null);
    assert_eq!(order_view["payment"]["adapter"], json!("sandbox"));
    paykit.clear_creation_failure();
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

// ---------------------------------------------------------------------------
// Stripe verification (processor-verified)
// ---------------------------------------------------------------------------

async fn stripe_bound_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> (PendingOrder, i64) {
    put_config(app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(app, seller, buyer).await;
    let (status, body) = bind_method(app, &buyer.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK, "stripe bind failed: {body}");
    let total = body["order"]["total"]["amount_minor"]
        .as_i64()
        .expect("order total");
    (order, total)
}

async fn verify(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/verify"),
        Some(token),
        &Value::Null,
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn stripe_verification_pays_the_order_on_a_matching_paid_session(pool: PgPool) {
    let (app, stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order, total) = stripe_bound_order(&app, &seller, &buyer).await;
    stripe.accept_key(RESTRICTED_KEY);

    // No matching session yet: honestly unverified, nothing advanced.
    let (status, body) = verify(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["verified"], json!(false));

    // A paid session with the wrong amount never matches.
    stripe.add_session(FakeStripeSession {
        id: "cs_wrong_amount".into(),
        client_reference_id: order.order_id.clone(),
        payment_status: "paid".into(),
        amount_total: total - 1,
        currency: "usd".into(),
    });
    // An unpaid session with the right amount never matches.
    stripe.add_session(FakeStripeSession {
        id: "cs_unpaid".into(),
        client_reference_id: order.order_id.clone(),
        payment_status: "unpaid".into(),
        amount_total: total,
        currency: "usd".into(),
    });
    let (status, body) = verify(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["verified"], json!(false), "{body}");

    // The exact match pays the order (either participant may trigger it).
    stripe.add_session(FakeStripeSession {
        id: "cs_paid_match".into(),
        client_reference_id: order.order_id.clone(),
        payment_status: "paid".into(),
        amount_total: total,
        currency: "usd".into(),
    });
    let (status, body) = verify(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["verified"], json!(true));
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(
        body["order"]["fiat_transaction_ref"],
        json!("cs_paid_match")
    );
    assert!(body["order"]["receipt_id"].is_string());

    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["payment"]["state"], json!("confirmed"));

    // Re-verification is an idempotent success: exactly one receipt.
    let (status, body) = verify(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["verified"], json!(true));
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM receipts").await,
        1,
        "confirmation effects applied exactly once"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn stripe_verification_surfaces_key_problems_honestly(pool: PgPool) {
    let (app, stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (order, _total) = stripe_bound_order(&app, &seller, &buyer).await;

    // Only participants verify.
    let (status, _) = verify(&app, &stranger.token, &order.order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The fake accepts no keys, so the stored key is rejected upstream.
    let (status, body) = verify(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"]["reason"],
        json!("stripe_key_invalid"),
        "{body}"
    );
    assert!(stripe.request_count() >= 1, "the real key reached Stripe");

    // A seller who clears their key leaves the order unverifiable, stated
    // as exactly that.
    let (status, _) = put_config(
        &app,
        &seller.token,
        &json!({
            "bitcoin_enabled": false,
            "stripe_payment_link": "https://buy.stripe.com/test_abc123",
            "stripe_restricted_key": "",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = verify(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["reason"], json!("stripe_key_missing"));

    // Verification never applies to orders without a bound Stripe method.
    let seller2 = new_actor(&app).await;
    let order2 = create_pending_order(&app, &seller2, &stranger).await;
    let (status, body) = verify(&app, &stranger.token, &order2.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["reason"], json!("method_mismatch"), "{body}");
}

// ---------------------------------------------------------------------------
// PayPal attestation (seller-attested)
// ---------------------------------------------------------------------------

async fn mark_paid(
    app: &TestApp,
    token: &str,
    order_id: &str,
    body: &Value,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/mark-paid"),
        Some(token),
        body,
    )
    .await
}

async fn confirm_received(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/confirm-received"),
        Some(token),
        &Value::Null,
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn paypal_two_step_attestation_pays_the_order(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    bind_method(&app, &buyer.token, &order.order_id, "paypal").await;

    // Only the buyer reports; only the seller confirms.
    let (status, _) = mark_paid(&app, &seller.token, &order.order_id, &json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = confirm_received(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = mark_paid(
        &app,
        &buyer.token,
        &order.order_id,
        &json!({ "transaction_ref": "5TY05013RG002845M" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reported_at = body["order"]["payment_reported_at"].clone();
    assert!(reported_at.is_string());
    assert_eq!(
        body["order"]["fiat_transaction_ref"],
        json!("5TY05013RG002845M")
    );
    // Reporting alone never advances the payment.
    assert_eq!(body["order"]["state"], json!("pending_payment"));

    // Duplicate reports keep the original timestamp.
    let (status, body) = mark_paid(&app, &buyer.token, &order.order_id, &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["order"]["payment_reported_at"], reported_at);

    let (status, body) = confirm_received(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert!(body["order"]["receipt_id"].is_string());
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["payment"]["state"], json!("confirmed"));

    // Both attestation events are on the order/payment timeline.
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.fiat_payment_reported'"
        )
        .await,
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

    // Confirmation is idempotent: one receipt, ever.
    let (status, _) = confirm_received(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn paypal_attestation_applies_only_to_paypal_orders(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order, _total) = stripe_bound_order(&app, &seller, &buyer).await;
    let (status, body) = mark_paid(&app, &buyer.token, &order.order_id, &json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["reason"], json!("method_mismatch"));
    let (status, body) = confirm_received(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["reason"], json!("method_mismatch"));
}

// ---------------------------------------------------------------------------
// Paykit verification worker (physical bitcoin orders)
// ---------------------------------------------------------------------------

#[sqlx::test(migrations = "./migrations")]
async fn the_paykit_worker_confirms_a_settled_bitcoin_order(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    paykit.set_claimed(&seller.pubky);
    let order = create_pending_sat_order(&app, &seller, &buyer).await;
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());

    let source = FakePaykitStatus::default();
    let now = app.clock.now();

    // Undetected: nothing advances, the claim stamp defers the next poll.
    source.set_outcome(&reference, PaykitStatusOutcome::Undetected);
    let applied =
        marketplace_service::workers::verify_due_paykit_payments(&app.state, &source, now)
            .await
            .unwrap();
    assert_eq!(applied, 0);
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["paykit_request_state"], json!("pending"));

    // Detected: the projection reflects it; the payment still awaits.
    source.set_outcome(&reference, PaykitStatusOutcome::Detected);
    let later = now + chrono::Duration::seconds(60);
    marketplace_service::workers::verify_due_paykit_payments(&app.state, &source, later)
        .await
        .unwrap();
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["paykit_request_state"], json!("detected"));
    assert_eq!(order_view["state"], json!("pending_payment"));

    // Confirmed with the required amount: the order is paid exactly once.
    source.set_outcome(
        &reference,
        PaykitStatusOutcome::Confirmed {
            amount_matched: true,
        },
    );
    let even_later = later + chrono::Duration::seconds(60);
    let applied =
        marketplace_service::workers::verify_due_paykit_payments(&app.state, &source, even_later)
            .await
            .unwrap();
    assert_eq!(applied, 1);
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["state"], json!("paid"));
    assert_eq!(order_view["paykit_request_state"], json!("confirmed"));
    assert_eq!(order_view["payment"]["state"], json!("confirmed"));
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);

    // A repeated pass has nothing left to claim.
    let applied = marketplace_service::workers::verify_due_paykit_payments(
        &app.state,
        &source,
        even_later + chrono::Duration::seconds(60),
    )
    .await
    .unwrap();
    assert_eq!(applied, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn a_confirmed_but_mismatched_amount_routes_to_manual_review(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    paykit.set_claimed(&seller.pubky);
    let order = create_pending_sat_order(&app, &seller, &buyer).await;
    bind_method(&app, &buyer.token, &order.order_id, "bitcoin").await;
    let reference = order_reference(Uuid::parse_str(&order.order_id).unwrap());

    let source = FakePaykitStatus::default();
    source.set_outcome(
        &reference,
        PaykitStatusOutcome::Confirmed {
            amount_matched: false,
        },
    );
    let applied = marketplace_service::workers::verify_due_paykit_payments(
        &app.state,
        &source,
        app.clock.now(),
    )
    .await
    .unwrap();
    assert_eq!(applied, 1);
    let order_view = read_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(order_view["state"], json!("pending_payment"));
    assert_eq!(order_view["payment"]["state"], json!("manual_review"));
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM receipts").await, 0);
}
