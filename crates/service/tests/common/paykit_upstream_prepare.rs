//! The upstream `POST /marketplace/payment-requests/prepare` route of the
//! local paykit-server double (`pubky/paykit-server` #66, head `3eff693`).
//!
//! The double implements the preparation contract the way the server does:
//! the request signature covers the preimage, the body is closed and
//! canonical, a stored operation replays or conflicts before any validation
//! or dependency, and a new preparation is validated, admitted and stored
//! with an activation deadline fifteen minutes out. Its answers are pinned to
//! real server exchanges: `paykit_upstream_prepare_test.rs` replays every
//! fixture of `tests/fixtures/paykit-server-66/` through it and requires the
//! same status, error code and body shape.
//!
//! Admission (the buyer's registry, the seller's session and receiving
//! details) is outside the double: a test names the refusal a buyer or seller
//! gets with `refuse_prepare_for`, using the status and code the server
//! answered in the fixtures.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::{json, Value};

use super::{header_host, paykit_record_call, paykit_verify_signed, FakePaykit, FakePaykitState};

const PREPARE_PATH: &str = "/marketplace/payment-requests/prepare";
const PAYMENT_WINDOW_CAP_SECONDS: u64 = 24 * 60 * 60;
const PREPARE_TTL_SECONDS: i64 = 15 * 60;
const BODY_FIELDS: [&str; 6] = [
    "amount_sats",
    "creator",
    "operation_id",
    "payment_window_seconds",
    "reader",
    "reference",
];

/// What the double remembers of preparations and how a test shapes them.
pub struct PrepareState {
    /// Every authenticated, well-formed request body, in arrival order.
    requests: Vec<Value>,
    /// `(creator, operation_id)` -> the binding and the stored answer.
    operations: HashMap<(String, String), (Vec<u8>, Value)>,
    /// A buyer or seller (`pubky…` app key) -> the refusal admission gives.
    refusals: HashMap<String, (u16, String)>,
    /// Added to `total_sats` of a new preparation (a contract violation).
    total_delta: i64,
    /// Seconds from now to `prepare_expires_at` of a new preparation.
    expiry_offset_seconds: i64,
    /// Served verbatim as the `200` body of a new preparation.
    body_override: Option<Value>,
    /// Requests that reached the preparation logic (past authentication).
    calls: usize,
    /// New preparations that still store their operation, then answer
    /// `503 dependency_timeout`: a commit that lands after the deadline.
    late_commits: usize,
}

impl Default for PrepareState {
    fn default() -> Self {
        Self {
            requests: Vec::new(),
            operations: HashMap::new(),
            refusals: HashMap::new(),
            total_delta: 0,
            expiry_offset_seconds: PREPARE_TTL_SECONDS,
            body_override: None,
            calls: 0,
            late_commits: 0,
        }
    }
}

/// The message the real server sends with each error code it answered in the
/// fixtures; any other code carries a generic message (the client reads only
/// the code).
fn error_message(code: &str) -> &'static str {
    match code {
        "operation_conflict" => "operation binding conflicts with persisted payment state",
        "seller_setup_pending" => "seller Bitcoin receiving setup is needed",
        "dependency_timeout" => "request deadline exceeded",
        "invalid_request" => "request is invalid",
        "invalid_signature" => "request authentication failed",
        "reader_setup_pending" => "reader wallet setup needed",
        "reader_not_payable" => "reader has no Paykit app able to pay requests",
        "creator_session_invalid" => "creator session is invalid",
        _ => "request refused",
    }
}

fn refusal(status: u16, code: &str) -> axum::response::Response {
    (
        StatusCode::from_u16(status).expect("valid status"),
        axum::Json(json!({ "error": { "code": code, "message": error_message(code) } })),
    )
        .into_response()
}

/// The server's UUIDv4 rule: lowercase hyphenated, version 4, RFC 4122.
fn is_lowercase_uuid_v4(text: &str) -> bool {
    uuid::Uuid::parse_str(text).is_ok_and(|uuid| {
        uuid.get_version_num() == 4
            && uuid.get_variant() == uuid::Variant::RFC4122
            && uuid.hyphenated().to_string() == text
    })
}

fn microsecond_timestamp(offset_seconds: i64) -> String {
    let now = chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
        .expect("microsecond timestamp");
    (now + chrono::Duration::seconds(offset_seconds))
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

pub(super) async fn serve_upstream_prepare(
    axum::extract::State(state): axum::extract::State<Arc<Mutex<FakePaykitState>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let host = header_host(&headers);
    let Some(parsed) = paykit_verify_signed(&state, "POST", PREPARE_PATH, &headers, &body) else {
        return refusal(401, "invalid_signature");
    };
    let closed = parsed.as_object().is_some_and(|object| {
        object.len() == BODY_FIELDS.len()
            && BODY_FIELDS.iter().all(|field| object.contains_key(*field))
            && object["amount_sats"].is_u64()
            && object["payment_window_seconds"].is_u64()
            && ["creator", "operation_id", "reader", "reference"]
                .iter()
                .all(|field| object[*field].is_string())
    });
    let canonical = serde_json_canonicalizer::to_vec(&parsed).expect("canonicalizes");
    if !closed || canonical != body.as_ref() {
        return refusal(400, "invalid_request");
    }
    paykit_record_call(&state, "POST", PREPARE_PATH, &host, &parsed);

    let mut guard = state.lock().expect("fake paykit lock");
    let prepare = &mut guard.prepare;
    prepare.calls += 1;
    prepare.requests.push(parsed.clone());

    let creator = parsed["creator"].as_str().expect("closed").to_string();
    let operation_id = parsed["operation_id"].as_str().expect("closed").to_string();
    let key = (creator.clone(), operation_id);
    if let Some((binding, stored)) = prepare.operations.get(&key) {
        return if *binding == canonical {
            (StatusCode::OK, axum::Json(stored.clone())).into_response()
        } else {
            refusal(409, "operation_conflict")
        };
    }

    let amount_sats = parsed["amount_sats"].as_u64().expect("closed");
    let window = parsed["payment_window_seconds"].as_u64().expect("closed");
    let reader = parsed["reader"].as_str().expect("closed").to_string();
    if key.1.is_empty()
        || amount_sats == 0
        || window == 0
        || window > PAYMENT_WINDOW_CAP_SECONDS
        || !is_lowercase_uuid_v4(parsed["reference"].as_str().expect("closed"))
    {
        return refusal(400, "invalid_request");
    }
    for party in [&creator, &reader] {
        if let Some((status, code)) = prepare.refusals.get(party) {
            return refusal(*status, code);
        }
    }

    let total_sats = u64::try_from(i64::try_from(amount_sats).expect("fits") + prepare.total_delta)
        .expect("total is positive");
    let answer = prepare.body_override.clone().unwrap_or_else(|| {
        json!({
            "invoice_id": uuid::Uuid::new_v4(),
            "state": "prepared",
            "total_sats": total_sats,
            "prepare_expires_at": microsecond_timestamp(prepare.expiry_offset_seconds),
        })
    });
    prepare.operations.insert(key, (canonical, answer.clone()));
    if prepare.late_commits > 0 {
        prepare.late_commits -= 1;
        return refusal(503, "dependency_timeout");
    }
    (StatusCode::OK, axum::Json(answer)).into_response()
}

impl FakePaykit {
    /// Makes a buyer or seller (the `pubky…` app key the request carries) get
    /// this refusal from admission, after request validation.
    pub fn refuse_prepare_for(&self, app_key: &str, status: u16, code: &str) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .refusals
            .insert(app_key.to_string(), (status, code.to_string()));
    }

    pub fn clear_prepare_refusals(&self) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .refusals
            .clear();
    }

    /// Every authenticated, well-formed preparation request, in order.
    pub fn prepare_requests(&self) -> Vec<Value> {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .requests
            .clone()
    }

    /// How many requests reached the preparation logic.
    pub fn prepare_calls(&self) -> usize {
        self.state.lock().expect("fake paykit lock").prepare.calls
    }

    /// The stored answer for one `(creator, operation_id)`.
    pub fn prepared_answer(&self, creator: &str, operation_id: &str) -> Option<Value> {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .operations
            .get(&(creator.to_string(), operation_id.to_string()))
            .map(|(_, answer)| answer.clone())
    }

    /// A contract violation: the next new preparations answer a `total_sats`
    /// that differs from the amount by `delta`.
    pub fn set_prepare_total_delta(&self, delta: i64) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .total_delta = delta;
    }

    /// Seconds from now to the `prepare_expires_at` the next new preparations
    /// answer (the server's default is 900).
    pub fn set_prepare_expiry_offset(&self, seconds: i64) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .expiry_offset_seconds = seconds;
    }

    /// The next `count` new preparations commit and then answer
    /// `503 dependency_timeout`, as a request that outlives paykit-server's
    /// deadline does; the exact retry replays the stored answer.
    pub fn set_prepare_late_commits(&self, count: usize) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .late_commits = count;
    }

    /// A contract violation: new preparations answer exactly this `200` body.
    pub fn set_prepare_body(&self, body: Option<Value>) {
        self.state
            .lock()
            .expect("fake paykit lock")
            .prepare
            .body_override = body;
    }
}
