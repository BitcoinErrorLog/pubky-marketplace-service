//! Shared integration test harness: builds the real router against a real
//! Postgres database (provisioned per test by `#[sqlx::test]`) and drives it
//! through the full HTTP stack, including Pubky AuthToken auth.
#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::Capability;
use pubky_common::crypto::Keypair;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;

use marketplace_service::clock::{AdjustableClock, Clock};
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
    pub state: AppState,
}

pub async fn test_app(pool: PgPool) -> TestApp {
    test_app_with_config(pool, Config::for_tests()).await
}

pub async fn test_app_with_moderators(pool: PgPool, moderator_pubkys: Vec<String>) -> TestApp {
    let mut config = Config::for_tests();
    config.moderator_pubkys = moderator_pubkys;
    test_app_with_config(pool, config).await
}

pub async fn test_app_with_config(pool: PgPool, config: Config) -> TestApp {
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), config);
    TestApp {
        router: build_router(state.clone()),
        pool,
        clock,
        state,
    }
}

pub struct TestActor {
    pub keypair: Keypair,
    pub pubky: String,
    pub token: String,
}

pub fn random_keypair() -> (Keypair, String) {
    let keypair = Keypair::random();
    let pubky = keypair.public_key().z32();
    (keypair, pubky)
}

/// Signs a genuine AuthToken with the library the service verifies against
/// and returns its canonical postcard bytes.
pub fn auth_token_bytes(keypair: &Keypair) -> Vec<u8> {
    AuthToken::sign(keypair, vec![Capability::root()]).serialize()
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

/// Sends a request whose body is raw bytes (the AuthToken wire form).
pub async fn send_bytes(
    router: Router,
    method: &str,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body))
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

/// Establishes a session by signing a genuine AuthToken and posting its
/// bytes to `/v1/auth/sessions`.
///
/// `AuthToken::sign` stamps the token with real system time, so the
/// adjustable test clock is aligned with system time for the exchange (the
/// service's acceptance window is measured against its clock) and restored
/// to the fixture instant afterwards.
pub async fn authenticate(app: &TestApp, keypair: &Keypair) -> String {
    let fixture_now = app.clock.now();
    app.clock.set(Utc::now());
    let (status, session) = send_bytes(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        auth_token_bytes(keypair),
    )
    .await;
    app.clock.set(fixture_now);
    assert_eq!(
        status,
        StatusCode::CREATED,
        "session issue failed: {session}"
    );
    assert_eq!(session["pubky"], json!(keypair.public_key().z32()));
    session["token"]
        .as_str()
        .expect("token present")
        .to_string()
}

pub async fn new_actor(app: &TestApp) -> TestActor {
    let (keypair, pubky) = random_keypair();
    let token = authenticate(app, &keypair).await;
    TestActor {
        keypair,
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

/// The prototype's auction fixture: one unit at 45.00 USD, ten-minute
/// runtime, 5.00 minimum increment, 60.00 reserve, 60 s anti-sniping window
/// with a 120 s extension.
pub fn register_auction_command(seller_pubky: &str) -> Value {
    let mut command = register_command(seller_pubky, 1);
    command["command_id"] = json!("00000000-0000-4000-8000-000000000600");
    command["payload"]["unit_price"] =
        json!({ "amount_minor": 4_500, "currency": "USD", "exponent": 2 });
    command["payload"]["sale_format"] = json!("auction");
    command["payload"]["auction_terms"] = json!({
        "starts_at": "2026-08-19T22:00:00.000Z",
        "ends_at": "2026-08-19T22:10:00.000Z",
        "minimum_increment": { "amount_minor": 500, "currency": "USD", "exponent": 2 },
        "reserve_price": { "amount_minor": 6_000, "currency": "USD", "exponent": 2 },
        "anti_sniping_window_seconds": 60,
        "anti_sniping_extension_seconds": 120,
    });
    command
}

pub const OFFER_COMMAND_ID: &str = "00000000-0000-4000-8000-000000000500";

pub fn offer_aggregate() -> String {
    format!("offer:{OFFER_COMMAND_ID}")
}

pub fn create_offer_command(seller_pubky: &str, quantity: i64) -> Value {
    json!({
        "version": 1,
        "command_id": OFFER_COMMAND_ID,
        "aggregate_id": listing_aggregate(seller_pubky),
        "expected_revision": 1,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.create",
        "payload": {
            "amount": { "amount_minor": 10_000, "currency": "USD", "exponent": 2 },
            "quantity": quantity,
            "expires_in_seconds": 3_600,
            "message": "Would you take this?",
        },
    })
}

pub fn counter_offer_command(expected_revision: i64) -> Value {
    json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000000501",
        "aggregate_id": offer_aggregate(),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.counter",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "amount": { "amount_minor": 11_000, "currency": "USD", "exponent": 2 },
            "quantity": 1,
            "expires_in_seconds": 3_600,
            "message": "Meet me here.",
        },
    })
}

pub fn offer_action(kind: &str, expected_revision: i64, command_id: &str) -> Value {
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": offer_aggregate(),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": kind,
        "payload": { "offer_id": OFFER_COMMAND_ID },
    })
}

pub fn place_bid_command(
    seller_pubky: &str,
    index: u64,
    maximum_minor: i64,
    expected_revision: i64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8001, index),
        "aggregate_id": listing_aggregate(seller_pubky),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "auction.place_bid",
        "payload": {
            "maximum_amount": { "amount_minor": maximum_minor, "currency": "USD", "exponent": 2 },
        },
    })
}

pub fn close_auction_command(
    seller_pubky: &str,
    expected_revision: i64,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8000, command_number),
        "aggregate_id": listing_aggregate(seller_pubky),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "auction.close",
        "payload": {},
    })
}

pub fn report_command(command_id: &str, target_id: &str) -> Value {
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": format!("report:{command_id}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "trust.report",
        "payload": {
            "target_type": "listing",
            "target_id": target_id,
            "reason": "counterfeit",
            "details": "Brand markings appear inconsistent.",
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
