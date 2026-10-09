//! The upstream preparation client (`pubky/paykit-server` #66, head
//! `3eff693`) against exchanges captured from a running server.
//!
//! The fixtures in `tests/fixtures/paykit-server-66/` are real requests and
//! answers (see `capture/README.md`). Three layers use them:
//!
//! 1. the fixtures themselves: the request signature verifies over this
//!    service's preimage, the body is the closed canonical request, and the
//!    attempt identities derive from the captured inputs;
//! 2. the real client against a server that replays the captured answers: it
//!    must produce the captured request byte for byte, signature included,
//!    and map each captured answer to the right outcome;
//! 3. the local paykit double the bind tests use, replayed with the same
//!    requests: it must answer with the same status, error code and body
//!    shape, so the double cannot drift from the server it stands for.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use base64::Engine;
use common::paykit_server_66::{self as fixtures, Fixture};
use common::*;
use ed25519_dalek::{Signature, SigningKey, Verifier};
use marketplace_service::payment_attempt::{operation_id, payment_reference, UpstreamPrepared};
use marketplace_service::payments::{
    attempt_reference, paykit_signature_preimage, upstream_prepare_error, PaykitApi, PaykitClient,
    PaykitRequestError, UPSTREAM_PREPARE_PATH,
};
use serde_json::{json, Value};

fn verifying_key() -> ed25519_dalek::VerifyingKey {
    let seed: [u8; 32] = hex::decode(TEST_PAYKIT_SIGNING_SEED)
        .expect("seed decodes")
        .try_into()
        .expect("seed is 32 bytes");
    SigningKey::from_bytes(&seed).verifying_key()
}

fn decode_signature(signature: &str) -> Signature {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature)
        .expect("signature is base64url");
    Signature::from_bytes(&bytes.try_into().expect("signature is 64 bytes"))
}

fn fixture_signature_verifies(fixture: &Fixture) -> bool {
    verifying_key()
        .verify(
            &paykit_signature_preimage(
                &fixture.method,
                &fixture.path,
                fixture.request_body.as_bytes(),
            ),
            &decode_signature(&fixture.signature),
        )
        .is_ok()
}

// ---------------------------------------------------------------------------
// 1. The fixtures
// ---------------------------------------------------------------------------

#[test]
fn every_fixture_comes_from_the_pinned_server_revision() {
    let all = fixtures::load_all();
    assert_eq!(all.len(), fixtures::FIXTURE_NAMES.len());
    for fixture in &all {
        assert_eq!(fixture.server_revision, fixtures::SERVER_REVISION);
        assert_eq!(fixture.method, "POST");
        assert_eq!(fixture.path, UPSTREAM_PREPARE_PATH);
    }
}

#[test]
fn the_captured_signatures_verify_over_this_services_preimage() {
    for fixture in fixtures::load_all() {
        let verifies = fixture_signature_verifies(&fixture);
        if fixture.name == "prepare_invalid_signature" {
            assert!(!verifies, "signed by a key the server does not trust");
            assert_eq!(fixture.status, 401);
        } else {
            assert!(
                verifies,
                "{}: the server accepted a request this service's preimage signs",
                fixture.name
            );
        }
    }
}

#[test]
fn the_captured_requests_are_the_closed_canonical_body() {
    let closed = [
        "amount_sats",
        "creator",
        "operation_id",
        "payment_window_seconds",
        "reader",
        "reference",
    ];
    for fixture in fixtures::load_all() {
        let request = fixture.request();
        let canonical = serde_json_canonicalizer::to_string(&request).expect("canonicalizes");
        assert_eq!(canonical, fixture.request_body, "{}", fixture.name);
        let mut keys: Vec<&str> = request
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        if fixture.name == "prepare_forbidden_fork_field" {
            let mut expected = closed.to_vec();
            expected.push("stack_id");
            expected.sort_unstable();
            assert_eq!(keys, expected);
        } else {
            assert_eq!(keys, closed, "{}", fixture.name);
        }
        for field in [
            "expires_at",
            "idempotency_key",
            "nonce_sats",
            "allocation_mode",
        ] {
            assert!(request.get(field).is_none(), "{}: {field}", fixture.name);
        }
    }
}

#[test]
fn the_derivation_reproduces_the_captured_attempt_identities() {
    let inputs = fixtures::inputs();
    let order: uuid::Uuid = inputs["order_id"]
        .as_str()
        .expect("order id")
        .parse()
        .expect("uuid");
    for attempt in inputs["attempts"].as_array().expect("attempts") {
        let number = i32::try_from(attempt["attempt"].as_i64().expect("attempt")).expect("fits");
        let reference = attempt_reference(order, number);
        assert_eq!(attempt["attempt_reference"], json!(reference));
        assert_eq!(
            attempt["operation_id"],
            json!(operation_id(&reference, number))
        );
        assert_eq!(
            attempt["reference"],
            json!(payment_reference(order, number).to_string())
        );
    }
    for (name, number) in [("prepare_new", 1), ("prepare_next_attempt", 2)] {
        let fixture = fixtures::load(name);
        let reference = attempt_reference(order, number);
        assert_eq!(fixture.operation_id(), operation_id(&reference, number));
        assert_eq!(fixture.reference(), payment_reference(order, number));
    }
}

#[test]
fn the_default_activation_deadline_is_fifteen_minutes_on_the_servers_clock() {
    for name in ["prepare_new", "prepare_next_attempt"] {
        let fixture = fixtures::load(name);
        let prepared: UpstreamPrepared =
            serde_json::from_str(&fixture.response_body).expect("closed answer");
        let lead = (prepared.prepare_expires_at - fixture.sent_at).num_milliseconds();
        assert!(
            (15 * 60 * 1000 - 1_000..=15 * 60 * 1000 + 1_000).contains(&lead),
            "{name}: {lead} ms between the request and its activation deadline"
        );
    }
    let short = fixtures::load("prepare_new_short_ttl");
    let prepared: UpstreamPrepared =
        serde_json::from_str(&short.response_body).expect("closed answer");
    let lead = (prepared.prepare_expires_at - short.sent_at).num_milliseconds();
    assert!((0..=2_000).contains(&lead), "1s TTL: {lead} ms");
}

#[test]
fn a_prepared_answer_has_no_stack_nonce_fingerprint_or_payment_deadline() {
    for name in [
        "prepare_new",
        "prepare_replay",
        "prepare_next_attempt",
        "prepare_new_short_ttl",
        "prepare_replay_after_ttl",
    ] {
        let fixture = fixtures::load(name);
        assert_eq!(fixture.status, 200);
        assert_eq!(fixture.content_type.as_deref(), Some("application/json"));
        let response = fixture.response();
        let mut keys: Vec<&str> = response
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["invoice_id", "prepare_expires_at", "state", "total_sats"],
            "{name}"
        );
        assert_eq!(response["state"], json!("prepared"));
        assert_eq!(
            response["total_sats"].as_u64(),
            Some(fixture.amount_sats()),
            "{name}: no nonce, so the total is the amount"
        );
    }
}

#[test]
fn the_replays_answer_exactly_the_stored_body() {
    assert_eq!(
        fixtures::load("prepare_replay").response_body,
        fixtures::load("prepare_new").response_body
    );
    assert_eq!(
        fixtures::load("prepare_replay").request_body,
        fixtures::load("prepare_new").request_body
    );
    assert_eq!(
        fixtures::load("prepare_replay_after_ttl").response_body,
        fixtures::load("prepare_new_short_ttl").response_body,
        "a replay survives the preparation TTL"
    );
    assert_ne!(
        fixtures::load("prepare_next_attempt").response()["invoice_id"],
        fixtures::load("prepare_new").response()["invoice_id"],
        "a new operation id is a new invoice"
    );
}

// ---------------------------------------------------------------------------
// 2. The client against a server that replays the captured answers
// ---------------------------------------------------------------------------

struct ReplayServer {
    base_url: String,
    matched: Arc<Mutex<Vec<String>>>,
    unmatched: Arc<Mutex<Vec<(String, String)>>>,
}

/// Answers a request only when its signature and body equal a captured
/// request, with that exchange's captured status, content type and body.
/// Anything else is a `599` and is recorded: the client sent something the
/// real server was never shown.
async fn replay_server() -> ReplayServer {
    #[derive(Clone)]
    struct Shared {
        by_request: Arc<HashMap<(String, String), Fixture>>,
        matched: Arc<Mutex<Vec<String>>>,
        unmatched: Arc<Mutex<Vec<(String, String)>>>,
    }
    async fn serve(
        axum::extract::State(shared): axum::extract::State<Shared>,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        let signature = headers
            .get("x-paykit-signature")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = String::from_utf8_lossy(&body).to_string();
        match shared.by_request.get(&(signature.clone(), body.clone())) {
            Some(fixture) => {
                shared
                    .matched
                    .lock()
                    .expect("lock")
                    .push(fixture.name.clone());
                (
                    StatusCode::from_u16(fixture.status).expect("status"),
                    [(
                        axum::http::header::CONTENT_TYPE,
                        fixture
                            .content_type
                            .clone()
                            .unwrap_or_else(|| "application/json".to_string()),
                    )],
                    fixture.response_body.clone(),
                )
                    .into_response()
            }
            None => {
                shared
                    .unmatched
                    .lock()
                    .expect("lock")
                    .push((signature, body));
                StatusCode::from_u16(599).expect("status").into_response()
            }
        }
    }
    let mut by_request = HashMap::new();
    for fixture in fixtures::load_all() {
        // Identical requests (`prepare_replay`) answer identically.
        by_request
            .entry((fixture.signature.clone(), fixture.request_body.clone()))
            .or_insert(fixture);
    }
    let matched = Arc::new(Mutex::new(Vec::new()));
    let unmatched = Arc::new(Mutex::new(Vec::new()));
    let shared = Shared {
        by_request: Arc::new(by_request),
        matched: matched.clone(),
        unmatched: unmatched.clone(),
    };
    let router = axum::Router::new()
        .route(UPSTREAM_PREPARE_PATH, axum::routing::post(serve))
        .with_state(shared);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("replay server binds");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("replay serves");
    });
    ReplayServer {
        base_url: format!("http://{address}"),
        matched,
        unmatched,
    }
}

fn client_for(base_url: &str) -> PaykitClient {
    PaykitClient::new(base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream)
}

/// Calls the client with the captured inputs of `fixture`, optionally
/// overriding the amount and window.
async fn prepare_like(
    server: &ReplayServer,
    fixture: &Fixture,
    amount_sats: Option<u64>,
    window_seconds: Option<u64>,
) -> Result<UpstreamPrepared, PaykitRequestError> {
    client_for(&server.base_url)
        .prepare_marketplace_payment(
            &fixture.seller(),
            &fixture.buyer(),
            fixture.reference(),
            amount_sats.unwrap_or_else(|| fixture.amount_sats()),
            &fixture.operation_id(),
            window_seconds.unwrap_or_else(|| fixture.window_seconds()),
        )
        .await
}

fn parsed(fixture: &Fixture) -> UpstreamPrepared {
    serde_json::from_str(&fixture.response_body).expect("a prepared answer")
}

#[tokio::test]
async fn a_new_attempt_sends_the_captured_request_and_reads_the_captured_answer() {
    let server = replay_server().await;
    let fixture = fixtures::load("prepare_new");
    let prepared = prepare_like(&server, &fixture, None, None)
        .await
        .expect("prepared");
    assert_eq!(prepared, parsed(&fixture));
    assert_eq!(prepared.total_sats, fixture.amount_sats());
    assert!(
        server.unmatched.lock().expect("lock").is_empty(),
        "the client's request and signature are byte for byte the captured ones"
    );
    assert_eq!(*server.matched.lock().expect("lock"), ["prepare_new"]);
}

#[tokio::test]
async fn the_same_attempt_again_replays_the_stored_answer() {
    let server = replay_server().await;
    let fixture = fixtures::load("prepare_new");
    let first = prepare_like(&server, &fixture, None, None).await;
    let second = prepare_like(&server, &fixture, None, None).await;
    assert_eq!(first, second);
    assert_eq!(second, Ok(parsed(&fixture)));
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn the_next_attempt_is_a_new_invoice() {
    let server = replay_server().await;
    let first = fixtures::load("prepare_new");
    let next = fixtures::load("prepare_next_attempt");
    let first = prepare_like(&server, &first, None, None)
        .await
        .expect("first");
    let next = prepare_like(&server, &next, None, None)
        .await
        .expect("next");
    assert_ne!(first.invoice_id, next.invoice_id);
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_replay_after_the_preparation_ttl_still_prepares() {
    let server = replay_server().await;
    let fixture = fixtures::load("prepare_replay_after_ttl");
    assert_eq!(
        prepare_like(&server, &fixture, None, None).await,
        Ok(parsed(&fixture))
    );
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_changed_binding_is_refused_and_never_a_buyer_error() {
    let server = replay_server().await;
    let fixture = fixtures::load("prepare_conflict_changed_binding");
    assert_eq!(fixture.status, 409);
    assert_eq!(fixture.error_code(), "conflict");
    let first = fixtures::load("prepare_new");
    assert_eq!(
        prepare_like(&server, &fixture, Some(first.amount_sats() + 1), None).await,
        Err(PaykitRequestError::Rejected)
    );
    assert_eq!(
        *server.matched.lock().expect("lock"),
        ["prepare_conflict_changed_binding"]
    );
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn windows_the_server_refuses_are_refused_without_clamping() {
    let server = replay_server().await;
    for (name, window) in [
        ("prepare_window_over_cap", 86_401),
        ("prepare_window_zero", 0),
    ] {
        let fixture = fixtures::load(name);
        assert_eq!(
            (fixture.status, fixture.error_code().as_str()),
            (400, "invalid_request")
        );
        assert_eq!(fixture.window_seconds(), window);
        assert_eq!(
            prepare_like(&server, &fixture, None, None).await,
            Err(PaykitRequestError::Rejected),
            "{name}"
        );
    }
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn each_captured_admission_refusal_maps_to_its_bind_class() {
    let server = replay_server().await;
    for (name, status, code, expected) in [
        (
            "prepare_reader_setup_pending",
            503,
            "reader_setup_pending",
            PaykitRequestError::ReaderSetupPending,
        ),
        (
            "prepare_reader_not_payable",
            409,
            "reader_not_payable",
            PaykitRequestError::ReaderNotPayable,
        ),
        (
            "prepare_creator_session_invalid",
            409,
            "creator_session_invalid",
            PaykitRequestError::SellerAccountUnavailable,
        ),
        (
            "prepare_seller_without_bitcoin",
            400,
            "invalid_request",
            PaykitRequestError::Rejected,
        ),
    ] {
        let fixture = fixtures::load(name);
        assert_eq!(
            (fixture.status, fixture.error_code().as_str()),
            (status, code),
            "{name}"
        );
        assert_eq!(
            prepare_like(&server, &fixture, None, None).await,
            Err(expected),
            "{name}"
        );
    }
    assert!(server.unmatched.lock().expect("lock").is_empty());
}

#[test]
fn refusals_the_client_never_provokes_still_map_from_the_captured_answers() {
    // The typed client always sends a UUID reference and the closed body, and
    // signs with the trusted key, so these three answers are exercised
    // through the mapping alone.
    for (name, status, code, expected) in [
        (
            "prepare_reference_not_uuid",
            400,
            "invalid_request",
            PaykitRequestError::Rejected,
        ),
        (
            "prepare_forbidden_fork_field",
            400,
            "invalid_request",
            PaykitRequestError::Rejected,
        ),
        (
            "prepare_invalid_signature",
            401,
            "invalid_signature",
            PaykitRequestError::Unavailable,
        ),
    ] {
        let fixture = fixtures::load(name);
        assert_eq!(
            (fixture.status, fixture.error_code().as_str()),
            (status, code),
            "{name}"
        );
        assert_eq!(
            upstream_prepare_error(
                reqwest::StatusCode::from_u16(fixture.status).expect("status"),
                &fixture.error_code()
            ),
            expected,
            "{name}"
        );
    }
}

#[test]
fn every_named_server_refusal_maps_and_unknown_ones_are_outages() {
    use PaykitRequestError::*;
    for (status, code, expected) in [
        (409, "conflict", Rejected),
        (401, "invalid_signature", Unavailable),
        (409, "creator_session_invalid", SellerAccountUnavailable),
        (400, "invalid_request", Rejected),
        (409, "reader_not_payable", ReaderNotPayable),
        (503, "reader_setup_pending", ReaderSetupPending),
        (503, "creator_session_unavailable", Unavailable),
        (503, "dependency_unavailable", Unavailable),
        (503, "dependency_timeout", Unavailable),
        (503, "reader_registry_unavailable", Unavailable),
        (502, "reader_registry_malformed", Unavailable),
        (429, "rate_limited", Unavailable),
        (413, "payload_too_large", Unavailable),
        (500, "internal_error", Unavailable),
        (404, "not_found", Unavailable),
        (409, "invoice_conflict", Unavailable),
        (409, "unheard_of", Unavailable),
        (400, "", Unavailable),
    ] {
        assert_eq!(
            upstream_prepare_error(reqwest::StatusCode::from_u16(status).expect("status"), code),
            expected,
            "{status} {code}"
        );
    }
}

// ---------------------------------------------------------------------------
// The client against the local double: contract violations
// ---------------------------------------------------------------------------

const SELLER: &str = "tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const BUYER: &str = "ykrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

async fn prepare_on(
    paykit: &FakePaykit,
    amount_sats: u64,
) -> Result<UpstreamPrepared, PaykitRequestError> {
    client_for(&paykit.base_url)
        .prepare_marketplace_payment(
            SELLER,
            BUYER,
            uuid::Uuid::parse_str("a59ea622-851b-4eab-b7dc-0e5da7f73cea").expect("uuid"),
            amount_sats,
            "marketplace-payment:RCSYZPCY72B6PT0HDQ7SR225NC:1",
            1_800,
        )
        .await
}

#[tokio::test]
async fn a_total_that_differs_from_the_amount_is_refused() {
    for delta in [1, -1] {
        let paykit = spawn_fake_paykit().await;
        paykit.use_upstream_api();
        paykit.set_prepare_total_delta(delta);
        assert_eq!(
            prepare_on(&paykit, 50_000).await,
            Err(PaykitRequestError::TotalInconsistent),
            "{delta}"
        );
    }
}

#[tokio::test]
async fn an_answer_outside_the_closed_contract_is_refused() {
    let id = "6f9619ff-8b86-4d11-b42d-00c04fc964ff";
    let expires = "2026-10-09T12:15:00.000000Z";
    for (label, body) in [
        (
            "a fork-era field",
            json!({ "invoice_id": id, "state": "prepared", "total_sats": 50_000,
                    "prepare_expires_at": expires, "nonce_sats": 0 }),
        ),
        (
            "a payment deadline",
            json!({ "invoice_id": id, "state": "prepared", "total_sats": 50_000,
                    "prepare_expires_at": expires, "expires_at": expires }),
        ),
        (
            "a missing field",
            json!({ "invoice_id": id, "state": "prepared", "total_sats": 50_000 }),
        ),
        (
            "another state",
            json!({ "invoice_id": id, "state": "observing", "total_sats": 50_000,
                    "prepare_expires_at": expires }),
        ),
        ("not an object", json!(["prepared"])),
    ] {
        let paykit = spawn_fake_paykit().await;
        paykit.use_upstream_api();
        paykit.set_prepare_body(Some(body));
        assert_eq!(
            prepare_on(&paykit, 50_000).await,
            Err(PaykitRequestError::Rejected),
            "{label}"
        );
    }
}

#[tokio::test]
async fn an_unreachable_server_is_an_outage() {
    assert_eq!(
        client_for("http://127.0.0.1:1")
            .prepare_marketplace_payment(
                SELLER,
                BUYER,
                uuid::Uuid::from_u128(4),
                1,
                "marketplace-payment:x:1",
                60
            )
            .await,
        Err(PaykitRequestError::Unavailable)
    );
}

#[tokio::test]
async fn the_fork_client_never_calls_the_upstream_route() {
    let paykit = spawn_fake_paykit().await;
    let fork = PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED).expect("client");
    assert_eq!(fork.api(), PaykitApi::Fork);
    assert_eq!(
        fork.prepare_marketplace_payment(
            SELLER,
            BUYER,
            uuid::Uuid::from_u128(4),
            1,
            "marketplace-payment:x:1",
            60
        )
        .await,
        Err(PaykitRequestError::Rejected)
    );
    assert!(paykit.calls().is_empty());
}

// ---------------------------------------------------------------------------
// 3. The local double against the captured exchanges
// ---------------------------------------------------------------------------

async fn post_raw(paykit: &FakePaykit, fixture: &Fixture) -> (u16, Option<String>, String) {
    let response = reqwest::Client::new()
        .post(format!("{}{}", paykit.base_url, fixture.path))
        .header("x-paykit-signature", &fixture.signature)
        .body(fixture.request_body.clone())
        .send()
        .await
        .expect("double answers");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    (status, content_type, response.text().await.expect("body"))
}

fn assert_prepared_shape(name: &str, fixture: &Fixture, double: &Value) {
    let real = fixture.response();
    let keys = |value: &Value| {
        let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
        keys.sort();
        keys
    };
    assert_eq!(keys(double), keys(&real), "{name}");
    assert_eq!(double["state"], real["state"], "{name}");
    assert_eq!(double["total_sats"], real["total_sats"], "{name}");
    double["invoice_id"]
        .as_str()
        .and_then(|id| id.parse::<uuid::Uuid>().ok())
        .unwrap_or_else(|| panic!("{name}: invoice_id is a uuid"));
    for value in [double, &real] {
        let expires = value["prepare_expires_at"].as_str().expect("expiry");
        let (_, fraction) = expires
            .strip_suffix('Z')
            .and_then(|text| text.split_once('.'))
            .unwrap_or_else(|| panic!("{name}: microsecond UTC timestamp, got {expires}"));
        assert_eq!(fraction.len(), 6, "{name}: {expires}");
        chrono::DateTime::parse_from_rfc3339(expires).expect("RFC 3339");
    }
}

#[tokio::test]
async fn the_double_answers_every_captured_request_like_the_real_server() {
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    let mut stored: HashMap<String, String> = HashMap::new();
    for fixture in fixtures::load_all() {
        let name = fixture.name.as_str();
        match name {
            "prepare_reader_setup_pending"
            | "prepare_reader_not_payable"
            | "prepare_seller_without_bitcoin"
            | "prepare_creator_session_invalid" => {
                let party = match name {
                    "prepare_reader_setup_pending" | "prepare_reader_not_payable" => fixture
                        .request()["reader"]
                        .as_str()
                        .expect("reader")
                        .to_string(),
                    _ => fixture.request()["creator"]
                        .as_str()
                        .expect("creator")
                        .to_string(),
                };
                paykit.refuse_prepare_for(&party, fixture.status, &fixture.error_code());
            }
            _ => {}
        }
        let (status, content_type, body) = post_raw(&paykit, &fixture).await;
        assert_eq!(status, fixture.status, "{name}: {body}");
        assert_eq!(content_type, fixture.content_type, "{name}");
        match fixture.status {
            200 => {
                let double: Value = serde_json::from_str(&body).expect("JSON");
                assert_prepared_shape(name, &fixture, &double);
                match name {
                    "prepare_replay" => assert_eq!(body, stored["prepare_new"], "a replay"),
                    "prepare_replay_after_ttl" => {
                        assert_eq!(body, stored["prepare_new_short_ttl"], "a replay");
                    }
                    _ => {}
                }
                stored.insert(name.to_string(), body);
            }
            _ => assert_eq!(body, fixture.response_body, "{name}: the same error body"),
        }
    }
    assert_eq!(
        paykit.prepare_calls(),
        fixtures::FIXTURE_NAMES.len() - 2,
        "every request but the badly signed one and the one with a forbidden field \
         reached the preparation logic"
    );
    let new = fixtures::load("prepare_new");
    let creator = new.request()["creator"]
        .as_str()
        .expect("creator")
        .to_string();
    let answer = paykit
        .prepared_answer(&creator, &new.operation_id())
        .expect("the double stored the preparation");
    assert_eq!(answer.to_string(), stored["prepare_new"]);
}
