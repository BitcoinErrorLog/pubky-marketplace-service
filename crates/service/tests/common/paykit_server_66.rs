//! Exchanges captured from a running paykit-server at `pubky/paykit-server`
//! #66, head `f9079d5` (`tests/fixtures/paykit-server-66/`). Nothing in a
//! fixture is written by hand: `capture/README.md` says how they were
//! captured, and `capture/capture-harness.patch` is the harness.

use serde_json::Value;

pub const SERVER_REVISION: &str = "f9079d50424f31ff0a7ca3df3a12ddc43c398ea8";

/// Every captured exchange, in capture order.
pub const FIXTURE_NAMES: [&str; 17] = [
    "prepare_new",
    "prepare_replay",
    "prepare_conflict_changed_binding",
    "prepare_next_attempt",
    "prepare_window_over_cap",
    "prepare_window_zero",
    "prepare_reference_not_uuid",
    "prepare_forbidden_fork_field",
    "prepare_invalid_signature",
    "prepare_reader_setup_pending",
    "prepare_reader_not_payable",
    "prepare_seller_without_bitcoin",
    "prepare_creator_session_invalid",
    "prepare_new_short_ttl",
    "prepare_replay_after_ttl",
    "prepare_deadline_exceeded",
    "prepare_replay_after_deadline_exceeded",
];

#[derive(Debug, Clone)]
pub struct Fixture {
    pub name: String,
    pub server_revision: String,
    pub method: String,
    pub path: String,
    pub signature: String,
    pub request_body: String,
    pub sent_at: chrono::DateTime<chrono::Utc>,
    pub status: u16,
    pub content_type: Option<String>,
    pub response_body: String,
}

fn directory() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/paykit-server-66")
}

pub fn load(name: &str) -> Fixture {
    let path = directory().join(format!("{name}.json"));
    let document: Value = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("fixture {path:?}: {error}")),
    )
    .expect("fixture is JSON");
    let text = |value: &Value| value.as_str().expect("fixture string").to_string();
    Fixture {
        name: text(&document["name"]),
        server_revision: text(&document["server_revision"]),
        method: text(&document["request"]["method"]),
        path: text(&document["request"]["path"]),
        signature: text(&document["request"]["signature"]),
        request_body: text(&document["request"]["body"]),
        sent_at: text(&document["sent_at"])
            .parse()
            .expect("fixture sent_at is RFC 3339"),
        status: u16::try_from(document["response"]["status"].as_u64().expect("status"))
            .expect("status fits"),
        content_type: document["response"]["content_type"]
            .as_str()
            .map(str::to_string),
        response_body: text(&document["response"]["body"]),
    }
}

pub fn load_all() -> Vec<Fixture> {
    FIXTURE_NAMES.iter().map(|name| load(name)).collect()
}

/// The attempt identities the Marketplace service derived for the capture
/// (`capture/inputs.json`).
pub fn inputs() -> Value {
    serde_json::from_slice(
        &std::fs::read(directory().join("capture/inputs.json")).expect("capture inputs"),
    )
    .expect("capture inputs are JSON")
}

impl Fixture {
    pub fn request(&self) -> Value {
        serde_json::from_str(&self.request_body).expect("request body is JSON")
    }

    pub fn response(&self) -> Value {
        serde_json::from_str(&self.response_body).expect("response body is JSON")
    }

    /// The error code of an error answer.
    pub fn error_code(&self) -> String {
        self.response()["error"]["code"]
            .as_str()
            .expect("an error answer carries a code")
            .to_string()
    }

    fn field(&self, name: &str) -> String {
        self.request()[name]
            .as_str()
            .unwrap_or_else(|| panic!("request field {name}"))
            .to_string()
    }

    /// The seller as the order stores it (no `pubky` prefix).
    pub fn seller(&self) -> String {
        self.field("creator")
            .strip_prefix("pubky")
            .expect("app key")
            .to_string()
    }

    /// The buyer as the order stores it (no `pubky` prefix).
    pub fn buyer(&self) -> String {
        self.field("reader")
            .strip_prefix("pubky")
            .expect("app key")
            .to_string()
    }

    pub fn operation_id(&self) -> String {
        self.field("operation_id")
    }

    pub fn reference(&self) -> uuid::Uuid {
        self.field("reference")
            .parse()
            .expect("reference is a uuid")
    }

    pub fn amount_sats(&self) -> u64 {
        self.request()["amount_sats"].as_u64().expect("amount")
    }

    pub fn window_seconds(&self) -> u64 {
        self.request()["payment_window_seconds"]
            .as_u64()
            .expect("window")
    }
}
