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
use marketplace_service::payments::{
    PaykitClient, PaykitStatusOutcome, PaykitStatusSource, PaymentsRuntime, StripeClient,
    StripeKeyCipher,
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
/// camelCase listing and drop records at the production paths, keyed by the
/// `pubky-host` header and record id — exactly how the real homeserver
/// addresses seller-owned records. Tests reach it through the REAL
/// [`HttpHomeserverClient`], so header handling, status mapping, and JSON
/// parsing are exercised end to end.
pub struct FakeHomeserver {
    records: HomeserverRecordMap,
    drop_records: HomeserverRecordMap,
    pub base_url: String,
}

impl FakeHomeserver {
    pub fn put_record(&self, seller_pubky: &str, listing_id: &str, record: Value) {
        self.records
            .lock()
            .expect("fake homeserver records lock")
            .insert((seller_pubky.to_string(), listing_id.to_string()), record);
    }

    pub fn put_drop_record(&self, seller_pubky: &str, drop_id: &str, record: Value) {
        self.drop_records
            .lock()
            .expect("fake homeserver drop records lock")
            .insert((seller_pubky.to_string(), drop_id.to_string()), record);
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
    let drop_records: HomeserverRecordMap = Arc::default();
    let router = Router::new()
        .route(
            "/pub/pubky.app/marketplace/v1/listings/{listing_id}",
            axum::routing::get(serve_homeserver_record),
        )
        .with_state(records.clone())
        .merge(
            Router::new()
                .route(
                    "/pub/pubky.app/marketplace/v1/drops/{drop_id}",
                    axum::routing::get(serve_homeserver_record),
                )
                .with_state(drop_records.clone()),
        );
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
        drop_records,
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

/// A test app wired to a fresh fake homeserver AND the deterministic test
/// attestor (drop fixtures that need edition attestations).
pub async fn test_app_with_homeserver_and_attestor(pool: PgPool) -> (TestApp, FakeHomeserver) {
    let homeserver = spawn_fake_homeserver().await;
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
        .with_homeserver(Some(homeserver.client()))
        .with_attestor(Some(test_attestor()));
    (
        TestApp {
            router: build_router(state.clone()),
            pool,
            clock,
            state,
        },
        homeserver,
    )
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

/// A wire timestamp `seconds` after the fixed test instant [`NOW`].
pub fn ts_after(seconds: i64) -> String {
    let base: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    marketplace_service::clock::format_timestamp(base + chrono::Duration::seconds(seconds))
}

pub fn drop_aggregate(seller_pubky: &str, drop_id: &str) -> String {
    format!("drop:{seller_pubky}_{drop_id}")
}

/// A canonical camelCase homeserver drop record (ADR-0026 shape).
// Eight fixture knobs the drop tests all need to steer independently.
#[allow(clippy::too_many_arguments)]
pub fn drop_record_json(
    seller_pubky: &str,
    drop_id: &str,
    revision: i64,
    listing_ids: &[&str],
    starts_at: &str,
    ends_at: Option<&str>,
    total_quantity: i64,
    per_buyer_limit: i64,
) -> Value {
    json!({
        "schemaVersion": 1,
        "recordType": "drop",
        "ownerPubky": seller_pubky,
        "revision": revision,
        "createdAt": "2026-08-19T21:00:00.000Z",
        "updatedAt": "2026-08-19T21:00:00.000Z",
        "dropId": drop_id,
        "title": "Winter capsule drop",
        "description": "Limited winter release, first come first served.",
        "media": [{ "id": "m1", "contentHash": "f".repeat(64) }],
        "format": "fcfs",
        "startsAt": starts_at,
        "endsAt": ends_at,
        "listingIds": listing_ids,
        "totalQuantity": total_quantity,
        "perBuyerLimit": per_buyer_limit,
        "stockDisplay": "exact",
    })
}

/// The `drop.sync` envelope: any authenticated actor, `expected_revision`
/// always 0 (sync is convergent — the caller never knows the revision).
pub fn sync_drop_command(seller_pubky: &str, drop_id: &str, command_number: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8005, command_number),
        "aggregate_id": drop_aggregate(seller_pubky, drop_id),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "drop.sync",
        "payload": {
            "seller_pubky": seller_pubky,
            "drop_id": drop_id,
        },
    })
}

/// The `drop.cancel` envelope (seller-authored, revision CAS).
pub fn cancel_drop_command(
    seller_pubky: &str,
    drop_id: &str,
    expected_revision: i64,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8006, command_number),
        "aggregate_id": drop_aggregate(seller_pubky, drop_id),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "drop.cancel",
        "payload": {},
    })
}

/// The `drop.release_listings` envelope (seller-authored, revision CAS).
pub fn release_drop_command(
    seller_pubky: &str,
    drop_id: &str,
    expected_revision: i64,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8008, command_number),
        "aggregate_id": drop_aggregate(seller_pubky, drop_id),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "drop.release_listings",
        "payload": {},
    })
}

/// A `listing.register` envelope for an arbitrary listing id (the shared
/// [`register_command`] fixture is pinned to `boots_01`).
pub fn register_listing_command(
    seller_pubky: &str,
    listing_id: &str,
    quantity: i64,
    command_number: u64,
) -> Value {
    let mut command = register_command(seller_pubky, quantity);
    command["command_id"] = json!(indexed_command_id(0x8007, command_number));
    command["aggregate_id"] = json!(format!("listing:{seller_pubky}_{listing_id}"));
    command["payload"]["listing_id"] = json!(listing_id);
    command
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

/// Deterministic key sealing Stripe restricted keys in tests (production:
/// `STRIPE_KEY_ENCRYPTION_KEY`).
pub const TEST_STRIPE_ENCRYPTION_KEY: &str =
    "5555555555555555555555555555555555555555555555555555555555555555";
/// Deterministic ed25519 seed for the signed Paykit client (production:
/// `PAYKIT_REQUEST_SIGNING_KEY`).
pub const TEST_PAYKIT_SIGNING_SEED: &str =
    "6666666666666666666666666666666666666666666666666666666666666666";

/// One recorded Stripe Checkout Session served by [`FakeStripe`].
#[derive(Clone)]
pub struct FakeStripeSession {
    pub id: String,
    pub client_reference_id: String,
    pub payment_status: String,
    pub amount_total: i64,
    pub currency: String,
}

#[derive(Default)]
struct FakeStripeState {
    /// Restricted keys accepted as valid bearers; anything else is 401.
    valid_keys: Vec<String>,
    sessions: Vec<FakeStripeSession>,
    requests: Vec<String>,
}

/// A local Stripe API double serving `GET /v1/checkout/sessions` exactly as
/// tests configure it, reached through the REAL [`StripeClient`], so bearer
/// auth, pagination parameters, and status mapping are exercised end to end.
pub struct FakeStripe {
    state: Arc<Mutex<FakeStripeState>>,
    pub base_url: String,
}

impl FakeStripe {
    pub fn accept_key(&self, key: &str) {
        self.state
            .lock()
            .expect("fake stripe lock")
            .valid_keys
            .push(key.to_string());
    }

    pub fn add_session(&self, session: FakeStripeSession) {
        self.state
            .lock()
            .expect("fake stripe lock")
            .sessions
            .push(session);
    }

    pub fn request_count(&self) -> usize {
        self.state.lock().expect("fake stripe lock").requests.len()
    }
}

async fn serve_stripe_sessions(
    axum::extract::State(state): axum::extract::State<Arc<Mutex<FakeStripeState>>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_string();
    let mut guard = state.lock().expect("fake stripe lock");
    guard.requests.push(bearer.clone());
    if !guard.valid_keys.iter().any(|key| key == &bearer) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({ "error": { "type": "invalid_request_error" } })),
        )
            .into_response();
    }
    let data: Vec<Value> = guard
        .sessions
        .iter()
        .map(|session| {
            json!({
                "id": session.id,
                "client_reference_id": session.client_reference_id,
                "payment_status": session.payment_status,
                "amount_total": session.amount_total,
                "currency": session.currency,
            })
        })
        .collect();
    (
        StatusCode::OK,
        axum::Json(json!({ "object": "list", "data": data, "has_more": false })),
    )
        .into_response()
}

pub async fn spawn_fake_stripe() -> FakeStripe {
    let state: Arc<Mutex<FakeStripeState>> = Arc::default();
    let router = Router::new()
        .route(
            "/v1/checkout/sessions",
            axum::routing::get(serve_stripe_sessions),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake stripe binds");
    let addr = listener.local_addr().expect("fake stripe address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("fake stripe serves");
    });
    FakeStripe {
        state,
        base_url: format!("http://{addr}"),
    }
}

/// One payment-request the fake paykit-server accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakePaykitRequest {
    pub creator: String,
    pub reader: String,
    pub reference: String,
    pub amount_sats: u64,
}

#[derive(Default)]
struct FakePaykitState {
    /// Sellers (bare z32) with claimed watch-only accounts.
    claimed: Vec<String>,
    /// Error code returned for payment-request creation, when forced.
    create_error: Option<String>,
    requests: Vec<FakePaykitRequest>,
}

/// A local paykit-server double serving the fork's marketplace surface
/// (`GET /v0/accounts/{creator}`, signed `POST /v0/payment-requests`). The
/// signature of every payment request is verified against the test signing
/// key — exactly what the deployed server does — so the client's canonical
/// JSON and header handling are covered end to end.
pub struct FakePaykit {
    state: Arc<Mutex<FakePaykitState>>,
    pub base_url: String,
}

impl FakePaykit {
    pub fn set_claimed(&self, seller_pubky: &str) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .claimed
            .push(seller_pubky.to_string());
    }

    /// Force payment-request creation to fail with a paykit error code
    /// (e.g. `creator_session_invalid`).
    pub fn fail_creation_with(&self, code: &str) {
        self.state.lock().expect("fake paykit lock").create_error = Some(code.to_string());
    }

    pub fn clear_creation_failure(&self) {
        self.state.lock().expect("fake paykit lock").create_error = None;
    }

    pub fn requests(&self) -> Vec<FakePaykitRequest> {
        self.state
            .lock()
            .expect("fake paykit lock")
            .requests
            .clone()
    }
}

fn paykit_test_verifying_key() -> ed25519_dalek::VerifyingKey {
    let seed: [u8; 32] = hex::decode(TEST_PAYKIT_SIGNING_SEED)
        .expect("test seed decodes")
        .try_into()
        .expect("test seed is 32 bytes");
    ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key()
}

async fn serve_paykit_account(
    axum::extract::State(state): axum::extract::State<Arc<Mutex<FakePaykitState>>>,
    axum::extract::Path(creator): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let claimed = state
        .lock()
        .expect("fake paykit lock")
        .claimed
        .iter()
        .any(|seller| format!("pubky{seller}") == creator);
    (StatusCode::OK, axum::Json(json!({ "claimed": claimed }))).into_response()
}

async fn serve_paykit_payment_request(
    axum::extract::State(state): axum::extract::State<Arc<Mutex<FakePaykitState>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use ed25519_dalek::Verifier;
    let unauthorized = || {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({ "error": { "code": "invalid_signature" } })),
        )
            .into_response()
    };
    let Some(signature) = headers
        .get("x-paykit-signature")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, value).ok()
        })
        .and_then(|bytes| <[u8; 64]>::try_from(bytes).ok())
    else {
        return unauthorized();
    };
    if paykit_test_verifying_key()
        .verify(&body, &ed25519_dalek::Signature::from_bytes(&signature))
        .is_err()
    {
        return unauthorized();
    }
    let Ok(parsed) = serde_json::from_slice::<Value>(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": { "code": "invalid_request" } })),
        )
            .into_response();
    };
    let mut guard = state.lock().expect("fake paykit lock");
    if let Some(code) = guard.create_error.clone() {
        return (
            StatusCode::CONFLICT,
            axum::Json(json!({ "error": { "code": code } })),
        )
            .into_response();
    }
    guard.requests.push(FakePaykitRequest {
        creator: parsed["creator"].as_str().unwrap_or_default().to_string(),
        reader: parsed["reader"].as_str().unwrap_or_default().to_string(),
        reference: parsed["reference"].as_str().unwrap_or_default().to_string(),
        amount_sats: parsed["amount_sats"].as_u64().unwrap_or_default(),
    });
    StatusCode::NO_CONTENT.into_response()
}

pub async fn spawn_fake_paykit() -> FakePaykit {
    let state: Arc<Mutex<FakePaykitState>> = Arc::default();
    let router = Router::new()
        .route(
            "/v0/accounts/{creator}",
            axum::routing::get(serve_paykit_account),
        )
        .route(
            "/v0/payment-requests",
            axum::routing::post(serve_paykit_payment_request),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake paykit binds");
    let addr = listener.local_addr().expect("fake paykit address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("fake paykit serves");
    });
    FakePaykit {
        state,
        base_url: format!("http://{addr}"),
    }
}

/// Programmable paykit status source for worker tests, keyed by reference.
#[derive(Default)]
pub struct FakePaykitStatus {
    outcomes: Mutex<HashMap<String, PaykitStatusOutcome>>,
}

impl FakePaykitStatus {
    pub fn set_outcome(&self, reference: &str, outcome: PaykitStatusOutcome) {
        self.outcomes
            .lock()
            .expect("fake paykit status lock")
            .insert(reference.to_string(), outcome);
    }
}

impl PaykitStatusSource for FakePaykitStatus {
    fn status<'a>(
        &'a self,
        _seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = PaykitStatusOutcome> + Send + 'a>> {
        Box::pin(async move {
            *self
                .outcomes
                .lock()
                .expect("fake paykit status lock")
                .get(reference)
                .unwrap_or(&PaykitStatusOutcome::NotFound)
        })
    }
}

/// A test app with the payment-methods runtime enabled, wired to fresh
/// Stripe and paykit-server doubles through the real HTTP clients.
pub async fn test_app_with_payments(pool: PgPool) -> (TestApp, FakeStripe, FakePaykit) {
    let stripe = spawn_fake_stripe().await;
    let paykit = spawn_fake_paykit().await;
    let runtime = Arc::new(PaymentsRuntime {
        stripe_key_cipher: StripeKeyCipher::from_hex(TEST_STRIPE_ENCRYPTION_KEY)
            .expect("test stripe key parses"),
        stripe: StripeClient::new(&stripe.base_url).expect("fake stripe client builds"),
        paykit: Some(
            PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
                .expect("fake paykit client builds"),
        ),
    });
    let now: DateTime<Utc> = NOW.parse().expect("valid test timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
        .with_payments(Some(runtime));
    (
        TestApp {
            router: build_router(state.clone()),
            pool,
            clock,
            state,
        },
        stripe,
        paykit,
    )
}

/// A SAT-denominated register command so bitcoin binding has a
/// satoshi-priced order to work with.
pub fn register_sat_command(seller_pubky: &str, quantity: i64) -> Value {
    let mut command = register_command(seller_pubky, quantity);
    command["payload"]["unit_price"] =
        json!({ "amount_minor": 50_000, "currency": "SAT", "exponent": 0 });
    command
}
