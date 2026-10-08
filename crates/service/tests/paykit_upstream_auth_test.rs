//! The Paykit client against upstream `pubky/paykit-server` (rc9 plus #55):
//! the request-preimage signature and the signed `POST /setup/status`
//! seller-readiness call, selected by `PaykitApi::Upstream`. The fork API
//! stays the default and is covered by the existing suites.
//!
//! Upstream references (`pubky/paykit-server` PR #55 head `a109148`):
//! - verifier and preimage: `paykit-server/src/http/auth.rs`
//! - `/setup/status`: `paykit-server/src/http/setup_status.rs` and
//!   `paykit-server/tests/setup_status.rs` (the test vector below)

mod common;

use axum::http::StatusCode;
use base64::Engine;
use common::*;
use ed25519_dalek::{Signer, SigningKey, Verifier};
use marketplace_service::payments::{
    paykit_signature_preimage, PaykitApi, PaykitClient, PaykitRequestError, PaykitStatusOutcome,
};
use serde_json::{json, Value};
use sqlx::PgPool;

const SELLER: &str = "gy1wnkhfwezwdnawnur1bc3kw1x3jf5ggjj3cm37e31i5ntq3pco";
const REFERENCE: &str = "0R8Y7ZQ3M5N9K2VJ6W4X1T8S0P";

/// `paykit-server/tests/setup_status.rs`: `SigningKey::from_bytes(&[7; 32])`,
/// `CREATOR`, `canonical_body()`, and `signed_request()` over
/// `signature_preimage("POST", "/setup/status", &body)`.
const UPSTREAM_CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const UPSTREAM_BODY: &str =
    r#"{"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"}"#;
/// `signature_preimage("POST", "/setup/status", UPSTREAM_BODY)` and its
/// Ed25519 signature under that key, produced by upstream's own
/// `paykit_server::http::auth::signature_preimage` at PR #55 head `a109148`.
const UPSTREAM_PREIMAGE_HEX: &str = "7061796b69742d687474702d7369676e61747572652d763100504f5354002f73657475702f737461747573007b2263726561746f72223a227075626b79746b7271387a6d77623861336d396b313563737533713137716d6667716e703964736b6272673975713172796470797870377179227d";
const UPSTREAM_SIGNATURE: &str =
    "uGwZVTdCxIRugXQo2gui7dAz7Ude3mobvcretytsMx55Iyr3rCzLEm-rwpfJcphCTPOj6VAzD4sNMr2wT60OAw";

fn upstream_client(paykit: &FakePaykit) -> PaykitClient {
    PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream)
}

async fn upstream_paykit() -> FakePaykit {
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    paykit
}

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn preimage_and_signature_match_the_bytes_upstream_signs_in_its_own_test() {
    assert_eq!(
        UPSTREAM_BODY,
        format!(r#"{{"creator":"{UPSTREAM_CREATOR}"}}"#)
    );
    let preimage = paykit_signature_preimage("POST", "/setup/status", UPSTREAM_BODY.as_bytes());
    assert_eq!(hex_of(&preimage), UPSTREAM_PREIMAGE_HEX);
    assert!(
        preimage.starts_with(b"paykit-http-signature-v1\0POST\0/setup/status\0{"),
        "domain, upper-case method, path and raw body, NUL separated"
    );

    let key = SigningKey::from_bytes(&[7; 32]);
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes());
    assert_eq!(signature, UPSTREAM_SIGNATURE);
}

#[test]
fn the_preimage_binds_method_path_and_body() {
    let body = UPSTREAM_BODY.as_bytes();
    let base = paykit_signature_preimage("POST", "/setup/status", body);
    assert_eq!(
        paykit_signature_preimage("post", "/setup/status", body),
        base,
        "the method is upper-cased"
    );
    for other in [
        paykit_signature_preimage("GET", "/setup/status", body),
        paykit_signature_preimage("POST", "/transactions/status", body),
        paykit_signature_preimage("POST", "/setup/status", br#"{"creator":"x"}"#),
        body.to_vec(),
    ] {
        assert_ne!(other, base);
    }
    let key = SigningKey::from_bytes(&[7; 32]);
    let signature = key.sign(&base);
    assert!(
        key.verifying_key().verify(body, &signature).is_err(),
        "a signature over the preimage never verifies over the bare body"
    );
}

#[test]
fn the_api_setting_parses_fork_and_upstream_only() {
    assert_eq!(PaykitApi::default(), PaykitApi::Fork);
    assert_eq!(PaykitApi::parse("fork").unwrap(), PaykitApi::Fork);
    assert_eq!(
        PaykitApi::parse(" upstream\n").unwrap(),
        PaykitApi::Upstream
    );
    for bad in ["", "Upstream", "rc9", "both"] {
        assert!(PaykitApi::parse(bad).is_err(), "{bad:?}");
    }
}

#[tokio::test]
async fn setup_status_asks_for_btc_with_a_signed_closed_canonical_body() {
    let paykit = upstream_paykit().await;
    paykit.set_claimed(SELLER);
    assert_eq!(
        upstream_client(&paykit).seller_ready(SELLER).await,
        Ok(true)
    );

    let calls = paykit.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "POST");
    assert_eq!(calls[0].path, "/setup/status");
    assert_eq!(
        calls[0].body,
        json!({ "asset": "BTC", "creator": format!("pubky{SELLER}") })
    );
}

#[tokio::test]
async fn bitcoin_is_offered_only_on_exactly_ready() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    for (status, expected) in [
        ("ready", Ok(true)),
        ("setup_required", Ok(false)),
        ("unavailable", Err(PaykitRequestError::Unavailable)),
        ("Ready", Err(PaykitRequestError::Unavailable)),
        ("degraded", Err(PaykitRequestError::Unavailable)),
        ("", Err(PaykitRequestError::Unavailable)),
    ] {
        paykit.set_setup_status(SELLER, status);
        assert_eq!(client.seller_ready(SELLER).await, expected, "{status:?}");
    }
}

#[tokio::test]
async fn a_non_2xx_or_off_contract_answer_is_an_outage_never_ready() {
    let paykit = upstream_paykit().await;
    paykit.set_claimed(SELLER);
    let client = upstream_client(&paykit);
    for status in [400, 401, 404, 429, 500, 503] {
        paykit.fail_setup_status_with(status);
        assert_eq!(
            client.seller_ready(SELLER).await,
            Err(PaykitRequestError::Unavailable),
            "{status}"
        );
    }
    paykit.clear_setup_status_failure();
    for body in [
        json!({}),
        json!({ "status": 7 }),
        json!({ "status": "ready", "extra": true }),
        json!(["ready"]),
    ] {
        paykit.set_setup_status_body(body.clone());
        assert_eq!(
            client.seller_ready(SELLER).await,
            Err(PaykitRequestError::Unavailable),
            "{body}"
        );
    }
}

#[tokio::test]
async fn an_unreachable_paykit_server_is_an_outage() {
    let client = PaykitClient::new("http://127.0.0.1:1", TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream);
    assert_eq!(
        client.seller_ready(SELLER).await,
        Err(PaykitRequestError::Unavailable)
    );
}

#[tokio::test]
async fn upstream_signs_every_request_with_the_preimage() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    paykit.set_status(
        REFERENCE,
        json!({ "status": "undetected", "confirmations": 0, "amount_matched": false }),
    );
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Undetected
    );
    assert_eq!(paykit.calls().len(), 1, "the status call authenticated");
}

#[tokio::test]
async fn the_two_signature_schemes_do_not_verify_against_each_other() {
    let upstream_server = upstream_paykit().await;
    let fork_client = PaykitClient::new(&upstream_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client");
    upstream_server.set_status(
        REFERENCE,
        json!({ "status": "undetected", "confirmations": 0, "amount_matched": false }),
    );
    assert_eq!(
        fork_client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Unavailable,
        "a body-only signature is a 401 on upstream"
    );
    assert!(upstream_server.calls().is_empty());

    let fork_server = spawn_fake_paykit().await;
    fork_server.set_claimed(SELLER);
    let upstream_client = upstream_client(&fork_server);
    assert_eq!(
        upstream_client.seller_ready(SELLER).await,
        Err(PaykitRequestError::Unavailable),
        "the fork neither serves /setup/status nor accepts the preimage"
    );
    assert!(fork_server.calls().is_empty());
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

async fn enable_bitcoin(app: &TestApp, token: &str) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_config_offers_bitcoin_only_when_setup_status_is_ready(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;

    for (status, expected) in [
        ("ready", true),
        ("setup_required", false),
        ("ready", true),
        ("unavailable", true),
    ] {
        paykit.set_setup_status(&seller.pubky, status);
        app.clock.advance_seconds(16);
        let (http, body) = get_public_config(&app, &seller.pubky).await;
        assert_eq!(http, StatusCode::OK);
        assert_eq!(body["bitcoin_available"], json!(expected), "{status}");
        assert_eq!(
            body["bitcoin_offer_available"],
            json!(true),
            "the rail-wide gate is independent of the seller state"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unavailable_serves_the_last_known_value_until_stale_then_closes(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(body["bitcoin_available"], json!(true));

    paykit.set_setup_status(&seller.pubky, "unavailable");
    app.clock.advance_seconds(16);
    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(
        body["bitcoin_available"],
        json!(true),
        "within the stale window the last answer holds"
    );

    app.clock.advance_seconds(
        app.state.config.paykit_rail_stale_seconds + app.state.config.paykit_poll_seconds,
    );
    let (http, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(http, StatusCode::OK);
    assert_eq!(body["bitcoin_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_non_2xx_setup_status_never_offers_bitcoin_to_a_new_seller(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;
    paykit.set_claimed(&seller.pubky);
    paykit.fail_setup_status_with(503);

    let (http, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(
        http,
        StatusCode::OK,
        "the public read never turns into a 503"
    );
    assert_eq!(body["bitcoin_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn setup_status_is_not_called_for_a_seller_who_disabled_bitcoin(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    paykit.set_claimed(&seller.pubky);

    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(body["bitcoin_available"], json!(false));
    assert_eq!(body["paypal_available"], json!(true));
    assert!(
        paykit.calls().is_empty(),
        "PayPal-only sellers never reach paykit-server"
    );
}
