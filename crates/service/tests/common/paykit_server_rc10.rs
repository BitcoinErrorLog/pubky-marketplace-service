//! Exchanges captured from a running paykit-server at `pubky/paykit-server`
//! `v0.1.0-rc10` (`tests/fixtures/paykit-server-rc10/`), all of
//! `POST /setup/status`. Nothing in a fixture is written by hand:
//! `capture/README.md` says how they were captured, and
//! `capture/capture-harness.patch` is the harness.

use serde_json::Value;

pub const SERVER_REVISION: &str = "7326a3f9a035d79d8b6977b7c1a5fb742327ff37";

/// Every captured exchange, in capture order.
pub const FIXTURE_NAMES: [&str; 20] = [
    "setup_status_authority_ready",
    "setup_status_authority_ready_bitcoin_only",
    "setup_status_authority_never_set_up",
    "setup_status_btc_dual",
    "setup_status_btc_bitcoin_only",
    "setup_status_usdt_dual",
    "setup_status_usdt_bitcoin_only",
    "setup_status_usdt_never_set_up",
    "setup_status_usdt_before_reconnect",
    "setup_status_usdt_after_reconnect",
    "setup_status_btc_after_reconnect",
    "setup_status_usdt_without_usdt_config",
    "setup_status_authority_without_usdt_config",
    "setup_status_usdt_without_usdt_config_bitcoin_wallet",
    "setup_status_btc_without_usdt_config",
    "setup_status_invalid_asset",
    "setup_status_unknown_field",
    "setup_status_invalid_signature",
    "setup_status_authority_homeserver_down",
    "setup_status_usdt_homeserver_down",
];

#[derive(Debug, Clone)]
pub struct Fixture {
    pub name: String,
    pub server: String,
    pub server_revision: String,
    pub method: String,
    pub path: String,
    pub signature: String,
    pub request_body: String,
    pub status: u16,
    pub content_type: Option<String>,
    pub response_body: String,
}

fn directory() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/paykit-server-rc10")
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
        server: text(&document["server"]),
        server_revision: text(&document["server_revision"]),
        method: text(&document["request"]["method"]),
        path: text(&document["request"]["path"]),
        signature: text(&document["request"]["signature"]),
        request_body: text(&document["request"]["body"]),
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

impl Fixture {
    pub fn request(&self) -> Value {
        serde_json::from_str(&self.request_body).expect("request body is JSON")
    }

    /// The `asset` the captured request asked about; `None` for the
    /// authority-only body.
    pub fn asset(&self) -> Option<String> {
        self.request()["asset"].as_str().map(str::to_string)
    }

    /// The `status` of a 200 answer.
    pub fn status_value(&self) -> String {
        let body: Value = serde_json::from_str(&self.response_body).expect("answer is JSON");
        body["status"]
            .as_str()
            .expect("a 200 answer carries a status")
            .to_string()
    }
}
