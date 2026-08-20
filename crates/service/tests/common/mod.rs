//! Shared integration test harness: builds the real router against a real
//! Postgres database (provisioned per test by `#[sqlx::test]`) and drives it
//! through the full HTTP stack, including Pubky challenge–response auth.
#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
use http_body_util::BodyExt;
use rand::rngs::OsRng;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;

use marketplace_service::auth::CHALLENGE_CONTEXT;
use marketplace_service::clock::AdjustableClock;
use marketplace_service::config::Config;
use marketplace_service::http::build_router;
use marketplace_service::AppState;

/// The fixed test instant used by the TypeScript prototype suite.
pub const NOW: &str = "2026-08-19T22:00:00Z";
pub const REGISTER_COMMAND_ID: &str = "018f47d2-6a27-7c23-a49d-6b21bb770120";

pub struct TestApp {
    pub router: Router,
    pub pool: PgPool,
    pub clock: Arc<AdjustableClock>,
}

pub async fn test_app(pool: PgPool) -> TestApp {
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests());
    TestApp {
        router: build_router(state),
        pool,
        clock,
    }
}

pub struct TestActor {
    pub signing: SigningKey,
    pub pubky: String,
    pub token: String,
}

pub fn random_keypair() -> (SigningKey, String) {
    let signing = SigningKey::generate(&mut OsRng);
    let pubky = z32::encode(signing.verifying_key().as_bytes());
    (signing, pubky)
}

pub async fn send(
    router: Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let request = request
        .body(Body::from(
            serde_json::to_vec(body).expect("body serializes"),
        ))
        .expect("request builds");
    let response = router.oneshot(request).await.expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("body is JSON")
    };
    (status, value)
}

pub fn sign_challenge(signing: &SigningKey, nonce: &[u8]) -> String {
    let mut message = CHALLENGE_CONTEXT.to_vec();
    message.extend_from_slice(nonce);
    URL_SAFE_NO_PAD.encode(signing.sign(&message).to_bytes())
}

/// Full challenge–response authentication over HTTP with a real keypair.
pub async fn authenticate(app: &TestApp, signing: &SigningKey, pubky: &str) -> String {
    let (status, challenge) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/challenges",
        None,
        &json!({ "pubky": pubky }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "challenge issue failed: {challenge}"
    );
    let nonce = URL_SAFE_NO_PAD
        .decode(challenge["nonce"].as_str().expect("nonce present"))
        .expect("nonce decodes");
    let (status, session) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        None,
        &json!({
            "pubky": pubky,
            "challenge_id": challenge["challenge_id"],
            "signature": sign_challenge(signing, &nonce),
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "session issue failed: {session}"
    );
    session["token"]
        .as_str()
        .expect("token present")
        .to_string()
}

pub async fn new_actor(app: &TestApp) -> TestActor {
    let (signing, pubky) = random_keypair();
    let token = authenticate(app, &signing, &pubky).await;
    TestActor {
        signing,
        pubky,
        token,
    }
}

pub async fn execute(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some(token),
        body,
    )
    .await
}

pub fn listing_aggregate(seller_pubky: &str) -> String {
    format!("listing:{seller_pubky}_boots_01")
}

pub fn indexed_command_id(prefix: u16, index: u64) -> String {
    format!("00000000-0000-4000-{prefix:04x}-{index:012}")
}

pub fn register_command(seller_pubky: &str, quantity: i64) -> Value {
    json!({
        "version": 1,
        "command_id": REGISTER_COMMAND_ID,
        "aggregate_id": listing_aggregate(seller_pubky),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "listing.register",
        "payload": {
            "seller_pubky": seller_pubky,
            "listing_id": "boots_01",
            "listing_revision": 1,
            "content_hash": "a".repeat(64),
            "quantity": quantity,
            "unit_price": { "amount_minor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

pub fn reserve_command(
    seller_pubky: &str,
    index: u64,
    quantity: i64,
    expected_revision: i64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8000, index),
        "aggregate_id": listing_aggregate(seller_pubky),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "inventory.reserve",
        "payload": {
            "quantity": quantity,
            "reservation_ttl_seconds": 600,
        },
    })
}

pub fn checkout_command_with_id(seller_pubky: &str, command_id: &str) -> Value {
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": format!("checkout:{command_id}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "checkout.create",
        "payload": {
            "lines": [{
                "listing_aggregate_id": listing_aggregate(seller_pubky),
                "expected_revision": 1,
                "quantity": 1,
            }],
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US",
            },
            "guarantee_policy_version": 1,
        },
    })
}

pub fn checkout_command(seller_pubky: &str) -> Value {
    checkout_command_with_id(seller_pubky, "00000000-0000-4000-8000-000000001000")
}

pub async fn count(pool: &PgPool, sql: &str) -> i64 {
    let (value,): (i64,) = sqlx::query_as(sql)
        .fetch_one(pool)
        .await
        .expect("count query succeeds");
    value
}
