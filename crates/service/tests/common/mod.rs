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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use marketplace_service::attestor::Attestor;
use marketplace_service::clock::{AdjustableClock, Clock};
use marketplace_service::config::Config;
use marketplace_service::homeserver::{HomeserverListingClient, HttpHomeserverClient};
use marketplace_service::http::build_router;
use marketplace_service::locks::{
    LocksKeys, LocksLifecycleClient, LocksLookupOutcome, LocksRuntime,
};
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
    test_app_full(pool, config, None).await
}

/// Deterministic attestor identity for tests (secret and order salt as the
/// production env vars would carry them, hex).
pub const TEST_ATTESTOR_SECRET: &str =
    "0707070707070707070707070707070707070707070707070707070707070707";
pub const TEST_ATTESTOR_SALT: &str =
    "4242424242424242424242424242424242424242424242424242424242424242";

pub fn test_attestor() -> Arc<Attestor> {
    Arc::new(Attestor::from_hex(TEST_ATTESTOR_SECRET, TEST_ATTESTOR_SALT).expect("test attestor"))
}

pub async fn test_app_with_attestor(pool: PgPool) -> TestApp {
    test_app_full(pool, Config::for_tests(), Some(test_attestor())).await
}

pub async fn test_app_with_attestor_and_moderators(
    pool: PgPool,
    moderator_pubkys: Vec<String>,
) -> TestApp {
    let mut config = Config::for_tests();
    config.moderator_pubkys = moderator_pubkys;
    test_app_full(pool, config, Some(test_attestor())).await
}

pub async fn test_app_full(
    pool: PgPool,
    config: Config,
    attestor: Option<Arc<Attestor>>,
) -> TestApp {
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), config).with_attestor(attestor);
    TestApp {
        router: build_router(state.clone()),
        pool,
        clock,
        state,
    }
}

/// Deterministic test keys for the Locks correlation store (distinct
/// encryption and HMAC keys, as production requires).
pub const TEST_LOCKS_ENCRYPTION_KEY: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";
pub const TEST_LOCKS_HMAC_KEY: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";

/// A canonical Locks bundle id (26-character Crockford base32 of 16 bytes).
pub const TEST_BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
/// A canonical Locks lock id (52-character Crockford base32).
pub const TEST_LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

pub fn test_locks_keys() -> LocksKeys {
    LocksKeys::from_hex(TEST_LOCKS_ENCRYPTION_KEY, TEST_LOCKS_HMAC_KEY)
        .expect("test locks keys parse")
}

/// Programmable Lock Server lifecycle double for tests. Outcomes are keyed
/// by bundle id; a lifecycle the test never announced is `NotFound`, exactly
/// what the real route reports for an unsubmitted bundle. This type exists
/// only in the test harness — production constructs the HTTP client alone.
#[derive(Default)]
pub struct FakeLocksClient {
    outcomes: Mutex<HashMap<String, LocksLookupOutcome>>,
    lookups: Mutex<Vec<(String, String)>>,
}

impl FakeLocksClient {
    pub fn set_outcome(&self, bundle_id: &str, outcome: LocksLookupOutcome) {
        self.outcomes
            .lock()
            .expect("fake outcomes lock")
            .insert(bundle_id.to_string(), outcome);
    }

    pub fn lookup_count(&self) -> usize {
        self.lookups.lock().expect("fake lookups lock").len()
    }

    /// The `(creator, bundle_id)` pairs the service actually sent upstream.
    pub fn lookups(&self) -> Vec<(String, String)> {
        self.lookups.lock().expect("fake lookups lock").clone()
    }
}

impl LocksLifecycleClient for FakeLocksClient {
    fn lookup<'a>(
        &'a self,
        creator: &'a str,
        bundle_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = LocksLookupOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.lookups
                .lock()
                .expect("fake lookups lock")
                .push((creator.to_string(), bundle_id.to_string()));
            *self
                .outcomes
                .lock()
                .expect("fake outcomes lock")
                .get(bundle_id)
                .unwrap_or(&LocksLookupOutcome::NotFound)
        })
    }
}

/// A test app with Locks verification enabled, driven by the programmable
/// fake lifecycle client.
pub async fn test_app_with_locks(pool: PgPool) -> (TestApp, Arc<FakeLocksClient>) {
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let fake = Arc::new(FakeLocksClient::default());
    let runtime = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: fake.clone(),
    });
    let state =
        AppState::new(pool.clone(), clock.clone(), Config::for_tests()).with_locks(Some(runtime));
    (
        TestApp {
            router: build_router(state.clone()),
            pool,
            clock,
            state,
        },
        fake,
    )
}

/// The canonical addressed lock resource for a seller's test lock.
pub fn lock_resource_for(seller_pubky: &str) -> String {
    format!("{seller_pubky}/pub/locks.app/{TEST_LOCK_ID}.json")
}

type HomeserverRecordMap = Arc<Mutex<HashMap<(String, String), Value>>>;

/// A minimal local homeserver double: a real axum listener serving canonical
/// camelCase listing records at the production path, keyed by the
/// `pubky-host` header and listing id — exactly how the real homeserver
/// addresses seller-owned records. Tests reach it through the REAL
/// [`HttpHomeserverClient`], so header handling, status mapping, and JSON
/// parsing are exercised end to end.
pub struct FakeHomeserver {
    records: HomeserverRecordMap,
    pub base_url: String,
}

impl FakeHomeserver {
    pub fn put_record(&self, seller_pubky: &str, listing_id: &str, record: Value) {
        self.records
            .lock()
            .expect("fake homeserver records lock")
            .insert((seller_pubky.to_string(), listing_id.to_string()), record);
    }

    pub fn client(&self) -> Arc<dyn HomeserverListingClient> {
        Arc::new(HttpHomeserverClient::new(&self.base_url).expect("fake homeserver client builds"))
    }
}

async fn serve_homeserver_record(
    axum::extract::State(records): axum::extract::State<HomeserverRecordMap>,
    axum::extract::Path(listing_id): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let seller = headers
        .get("pubky-host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let stored = records
        .lock()
        .expect("fake homeserver records lock")
        .get(&(seller, listing_id))
        .cloned();
    match stored {
        Some(record) => (StatusCode::OK, axum::Json(record)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn spawn_fake_homeserver() -> FakeHomeserver {
    let records: HomeserverRecordMap = Arc::default();
    let router = Router::new()
        .route(
            "/pub/pubky.app/marketplace/v1/listings/{listing_id}",
            axum::routing::get(serve_homeserver_record),
        )
        .with_state(records.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake homeserver binds");
    let addr = listener.local_addr().expect("fake homeserver address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("fake homeserver serves");
    });
    FakeHomeserver {
        records,
        base_url: format!("http://{addr}"),
    }
}

/// A test app whose `listing.sync` fetches from the given homeserver client.
pub async fn test_app_with_homeserver_client(
    pool: PgPool,
    homeserver: Arc<dyn HomeserverListingClient>,
) -> TestApp {
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
        .with_homeserver(Some(homeserver));
    TestApp {
        router: build_router(state.clone()),
        pool,
        clock,
        state,
    }
}

/// A test app wired to a freshly spawned fake homeserver.
pub async fn test_app_with_homeserver(pool: PgPool) -> (TestApp, FakeHomeserver) {
    let homeserver = spawn_fake_homeserver().await;
    let app = test_app_with_homeserver_client(pool, homeserver.client()).await;
    (app, homeserver)
}

/// The `listing.sync` envelope: any authenticated actor, `expected_revision`
/// always 0 (sync is convergent — the caller never knows the revision).
pub fn sync_command(seller_pubky: &str, listing_id: &str, command_number: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8003, command_number),
        "aggregate_id": format!("listing:{seller_pubky}_{listing_id}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "listing.sync",
        "payload": {
            "seller_pubky": seller_pubky,
            "listing_id": listing_id,
        },
    })
}

/// The `payment.register_locks` envelope (buyer-authored).
pub fn register_locks_command(
    payment_id: &str,
    expected_revision: i64,
    bundle_id: &str,
    pubky_lock_resource: &str,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8002, command_number),
        "aggregate_id": format!("payment:{payment_id}"),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "payment.register_locks",
        "payload": {
            "payment_id": payment_id,
            "bundle_id": bundle_id,
            "pubky_lock_resource": pubky_lock_resource,
        },
    })
}

pub struct PendingOrder {
    pub order_id: String,
    pub payment_id: String,
}

/// Registers one unit and checks out as the buyer, leaving the order in
/// `pending_payment` with its payment awaiting entitlement at revision 1.
pub async fn create_pending_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> PendingOrder {
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, 1)).await;
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

/// The prototype's `paymentCommand` fixture.
pub fn payment_command(
    payment_id: &str,
    expected_revision: i64,
    target: &str,
    confirmations: i64,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8000, command_number),
        "aggregate_id": format!("payment:{payment_id}"),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "payment.sandbox_advance",
        "payload": {
            "payment_id": payment_id,
            "target": target,
            "confirmations": confirmations,
        },
    })
}

/// The prototype's `orderCommand` fixture: an order-aggregate command whose
/// payload carries the order id plus command-specific fields.
pub fn order_command(
    kind: &str,
    order_id: &str,
    expected_revision: i64,
    mut payload: Value,
    command_number: u64,
) -> Value {
    payload["order_id"] = json!(order_id);
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8000, command_number),
        "aggregate_id": format!("order:{order_id}"),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": kind,
        "payload": payload,
    })
}

pub struct PaidOrder {
    pub order_id: String,
    pub payment_id: String,
    pub receipt_id: String,
    pub total_minor: i64,
}

/// The prototype's `createPaidOrder` fixture: register one unit, check out
/// as the buyer, and confirm the sandbox payment (which issues the receipt
/// and moves the order to `paid` at revision 2).
pub async fn create_paid_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PaidOrder {
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let total_minor = body["result"]["orders"][0]["total"]["amount_minor"]
        .as_i64()
        .expect("order total present");
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment fixture failed: {body}");
    let receipt_id = body["result"]["receipt"]["id"]
        .as_str()
        .expect("receipt id present")
        .to_string();
    PaidOrder {
        order_id,
        payment_id,
        receipt_id,
        total_minor,
    }
}

pub async fn count(pool: &PgPool, sql: &str) -> i64 {
    let (value,): (i64,) = sqlx::query_as(sql)
        .fetch_one(pool)
        .await
        .expect("count query succeeds");
    value
}
