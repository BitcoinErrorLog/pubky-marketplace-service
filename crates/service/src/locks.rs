//! Server-side Locks verification support (plan task 4.5, ADR-0019 §7/§8).
//!
//! The bundle id registered for a payment is a bearer secret. This module
//! keeps it secret end to end:
//!
//! - **Encryption at rest.** The bundle id is sealed with XChaCha20-Poly1305
//!   under a configured key, with the payment id as associated data, so a
//!   ciphertext copied onto another correlation row will not decrypt.
//! - **HMAC lookup token.** Queries and uniqueness use
//!   HMAC-SHA256(lookup key, creator ‖ bundle id) — the raw value is never
//!   stored, indexed, logged, or placed in a URL.
//! - **Independent verification.** [`LocksLifecycleClient`] is the lifecycle
//!   lookup (`POST /verification-task-lookups` on the Lock Server, pinned
//!   contract commit `ba49a777`). The production implementation
//!   ([`HttpLocksClient`]) performs a real HTTP call; the trait exists so
//!   tests can exercise every lifecycle outcome without a live Lock Server,
//!   and no configuration path substitutes a fake in production
//!   ([`runtime_from_env`] only ever constructs the HTTP client).
//!
//! Verification is a pure function of what Locks reports: nothing in this
//! module accepts a client-supplied status.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::Deserialize;
use sha2::Sha256;
use uuid::Uuid;

/// Environment variable naming the Lock Server base URL. Setting it enables
/// Locks verification and makes both keys mandatory (fail closed).
pub const ENV_LOCKS_SERVER_URL: &str = "LOCKS_SERVER_URL";
/// Environment variable holding the 32-byte hex bundle-id encryption key.
pub const ENV_BUNDLE_ENCRYPTION_KEY: &str = "LOCKS_BUNDLE_ENCRYPTION_KEY";
/// Environment variable holding the 32-byte hex HMAC lookup-token key.
pub const ENV_LOOKUP_HMAC_KEY: &str = "LOCKS_LOOKUP_HMAC_KEY";

const XNONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// The configured Locks secret material. Both keys are required together
/// and must differ; the service refuses to start otherwise.
pub struct LocksKeys {
    encryption: [u8; KEY_LEN],
    lookup_hmac: [u8; KEY_LEN],
}

/// Key material never appears in logs, not even truncated.
impl std::fmt::Debug for LocksKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocksKeys(<redacted>)")
    }
}

impl LocksKeys {
    pub fn from_hex(encryption_hex: &str, lookup_hmac_hex: &str) -> anyhow::Result<Self> {
        let encryption = parse_key(ENV_BUNDLE_ENCRYPTION_KEY, encryption_hex)?;
        let lookup_hmac = parse_key(ENV_LOOKUP_HMAC_KEY, lookup_hmac_hex)?;
        if encryption == lookup_hmac {
            anyhow::bail!(
                "{ENV_BUNDLE_ENCRYPTION_KEY} and {ENV_LOOKUP_HMAC_KEY} must be distinct keys"
            );
        }
        Ok(Self {
            encryption,
            lookup_hmac,
        })
    }

    /// Seals a bundle id for storage: a random 24-byte nonce followed by the
    /// XChaCha20-Poly1305 ciphertext, with the owning payment id as
    /// associated data so ciphertexts cannot be transplanted between rows.
    pub fn encrypt_bundle_id(&self, payment_id: Uuid, bundle_id: &str) -> Vec<u8> {
        let cipher = XChaCha20Poly1305::new((&self.encryption).into());
        let mut nonce_bytes = [0u8; XNONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: bundle_id.as_bytes(),
                    aad: payment_id.as_bytes(),
                },
            )
            .expect("XChaCha20-Poly1305 encryption is infallible for in-memory buffers");
        let mut sealed = Vec::with_capacity(XNONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(&nonce_bytes);
        sealed.extend_from_slice(&ciphertext);
        sealed
    }

    /// Opens a sealed bundle id. Fails when the ciphertext was not produced
    /// under this key for this payment (wrong key configured, or tampering).
    pub fn decrypt_bundle_id(&self, payment_id: Uuid, sealed: &[u8]) -> anyhow::Result<String> {
        if sealed.len() <= XNONCE_LEN {
            anyhow::bail!("sealed bundle id is too short");
        }
        let (nonce_bytes, ciphertext) = sealed.split_at(XNONCE_LEN);
        let cipher = XChaCha20Poly1305::new((&self.encryption).into());
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(nonce_bytes),
                Payload {
                    msg: ciphertext,
                    aad: payment_id.as_bytes(),
                },
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "bundle id ciphertext did not authenticate under the configured key"
                )
            })?;
        String::from_utf8(plaintext).map_err(|_| anyhow::anyhow!("bundle id is not valid UTF-8"))
    }

    /// The deterministic lookup/uniqueness token for a lifecycle identity:
    /// HMAC-SHA256(lookup key, creator ‖ 0x0A ‖ bundle id).
    pub fn lookup_token(&self, creator: &str, bundle_id: &str) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.lookup_hmac)
            .expect("HMAC accepts any key length");
        mac.update(creator.as_bytes());
        mac.update(b"\n");
        mac.update(bundle_id.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
}

fn parse_key(name: &str, hex_value: &str) -> anyhow::Result<[u8; KEY_LEN]> {
    let bytes = hex::decode(hex_value.trim())
        .map_err(|_| anyhow::anyhow!("{name} must be 64 hexadecimal characters"))?;
    <[u8; KEY_LEN]>::try_from(bytes)
        .map_err(|_| anyhow::anyhow!("{name} must decode to exactly 32 bytes"))
}

/// The Locks verification-task lifecycle statuses (Locks `docs/API.md` at
/// pinned commit `ba49a777`; wire values are snake_case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocksTaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Expired,
}

impl LocksTaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            LocksTaskStatus::Pending => "pending",
            LocksTaskStatus::InProgress => "in_progress",
            LocksTaskStatus::Completed => "completed",
            LocksTaskStatus::Failed => "failed",
            LocksTaskStatus::Expired => "expired",
        }
    }
}

/// One lifecycle lookup result. Transport failures, non-2xx responses other
/// than a definitive not-found, and malformed bodies are `Unavailable`: the
/// correlation stays pending and is retried on the next worker pass,
/// matching Locks v1 semantics where transport/status failures remain
/// pending rather than terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocksLookupOutcome {
    Status(LocksTaskStatus),
    /// `404 verification_task_not_found`: the buyer has not (yet) submitted
    /// this bundle to the Lock Server.
    NotFound,
    Unavailable,
}

/// The Lock Server lifecycle lookup. Implementations must treat the bundle
/// id as bearer material: it may appear only in the outbound request body,
/// never in URLs, logs, or errors.
pub trait LocksLifecycleClient: Send + Sync + 'static {
    fn lookup<'a>(
        &'a self,
        creator: &'a str,
        bundle_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = LocksLookupOutcome> + Send + 'a>>;
}

/// The production client: a real `POST /verification-task-lookups` against
/// the configured Lock Server. The bundle id travels in the JSON body only
/// (it is bearer-secret-like and never belongs in a URL, per the Locks API).
pub struct HttpLocksClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct LifecycleBody {
    status: String,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    code: String,
}

impl HttpLocksClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            anyhow::bail!("{ENV_LOCKS_SERVER_URL} must be an http(s) URL");
        }
        let http = reqwest::Client::builder().timeout(LOOKUP_TIMEOUT).build()?;
        Ok(Self { base_url, http })
    }

    async fn lookup_inner(&self, creator: &str, bundle_id: &str) -> LocksLookupOutcome {
        let response = self
            .http
            .post(format!("{}/verification-task-lookups", self.base_url))
            .json(&serde_json::json!({ "creator": creator, "bundle_id": bundle_id }))
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tracing::warn!("locks lifecycle lookup transport failure");
                return LocksLookupOutcome::Unavailable;
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            // Only the documented error code is a definitive not-found; an
            // unmounted route or proxy 404 stays retryable.
            return match response.json::<ErrorBody>().await {
                Ok(body) if body.error.code == "verification_task_not_found" => {
                    LocksLookupOutcome::NotFound
                }
                _ => LocksLookupOutcome::Unavailable,
            };
        }
        if !status.is_success() {
            tracing::warn!(status = %status, "locks lifecycle lookup rejected");
            return LocksLookupOutcome::Unavailable;
        }
        match response.json::<LifecycleBody>().await {
            Ok(body) => match body.status.as_str() {
                "pending" => LocksLookupOutcome::Status(LocksTaskStatus::Pending),
                "in_progress" => LocksLookupOutcome::Status(LocksTaskStatus::InProgress),
                "completed" => LocksLookupOutcome::Status(LocksTaskStatus::Completed),
                "failed" => LocksLookupOutcome::Status(LocksTaskStatus::Failed),
                "expired" => LocksLookupOutcome::Status(LocksTaskStatus::Expired),
                _ => {
                    tracing::warn!("locks lifecycle lookup returned an unknown status");
                    LocksLookupOutcome::Unavailable
                }
            },
            Err(_) => {
                tracing::warn!("locks lifecycle lookup returned a malformed body");
                LocksLookupOutcome::Unavailable
            }
        }
    }
}

impl LocksLifecycleClient for HttpLocksClient {
    fn lookup<'a>(
        &'a self,
        creator: &'a str,
        bundle_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = LocksLookupOutcome> + Send + 'a>> {
        Box::pin(self.lookup_inner(creator, bundle_id))
    }
}

/// The Locks verification runtime carried on [`crate::AppState`]: absent on
/// sandbox-only deployments (where `payment.register_locks` is refused), and
/// required complete otherwise.
pub struct LocksRuntime {
    pub keys: LocksKeys,
    pub client: Arc<dyn LocksLifecycleClient>,
}

impl std::fmt::Debug for LocksRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocksRuntime(<redacted>)")
    }
}

/// Builds the production runtime from the environment, failing closed:
/// either all of `LOCKS_SERVER_URL`, `LOCKS_BUNDLE_ENCRYPTION_KEY`, and
/// `LOCKS_LOOKUP_HMAC_KEY` are present and valid (Locks mode), or none are
/// (sandbox-only mode). A partial configuration refuses to start rather
/// than run with verification silently disabled or secrets unprotected.
/// Only the real HTTP client is ever constructed here.
pub fn runtime_from_env() -> anyhow::Result<Option<Arc<LocksRuntime>>> {
    let url = std::env::var(ENV_LOCKS_SERVER_URL).ok();
    let encryption_key = std::env::var(ENV_BUNDLE_ENCRYPTION_KEY).ok();
    let hmac_key = std::env::var(ENV_LOOKUP_HMAC_KEY).ok();
    match (url, encryption_key, hmac_key) {
        (None, None, None) => Ok(None),
        (Some(url), Some(encryption_key), Some(hmac_key)) => {
            let keys = LocksKeys::from_hex(&encryption_key, &hmac_key)?;
            let client = Arc::new(HttpLocksClient::new(&url)?);
            Ok(Some(Arc::new(LocksRuntime { keys, client })))
        }
        _ => anyhow::bail!(
            "Locks verification is partially configured: set all of \
             {ENV_LOCKS_SERVER_URL}, {ENV_BUNDLE_ENCRYPTION_KEY}, and \
             {ENV_LOOKUP_HMAC_KEY}, or none of them"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENC_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const MAC_KEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

    fn keys() -> LocksKeys {
        LocksKeys::from_hex(ENC_KEY, MAC_KEY).expect("test keys parse")
    }

    #[test]
    fn bundle_id_round_trips_under_fresh_nonces() {
        let keys = keys();
        let payment_id = Uuid::new_v4();
        let first = keys.encrypt_bundle_id(payment_id, BUNDLE);
        let second = keys.encrypt_bundle_id(payment_id, BUNDLE);
        assert_ne!(first, second, "each seal uses a fresh random nonce");
        assert_eq!(keys.decrypt_bundle_id(payment_id, &first).unwrap(), BUNDLE);
        assert_eq!(keys.decrypt_bundle_id(payment_id, &second).unwrap(), BUNDLE);
    }

    #[test]
    fn ciphertext_never_contains_the_plaintext_and_binds_the_payment() {
        let keys = keys();
        let payment_id = Uuid::new_v4();
        let sealed = keys.encrypt_bundle_id(payment_id, BUNDLE);
        assert!(!sealed
            .windows(BUNDLE.len())
            .any(|window| window == BUNDLE.as_bytes()));
        keys.decrypt_bundle_id(Uuid::new_v4(), &sealed)
            .expect_err("a transplanted ciphertext must not decrypt");
        keys.decrypt_bundle_id(payment_id, &sealed[..XNONCE_LEN])
            .expect_err("a truncated ciphertext must not decrypt");
        let other_keys = LocksKeys::from_hex(MAC_KEY, ENC_KEY).expect("swapped keys parse");
        other_keys
            .decrypt_bundle_id(payment_id, &sealed)
            .expect_err("a different key must not decrypt");
    }

    #[test]
    fn lookup_tokens_are_deterministic_and_identity_scoped() {
        let keys = keys();
        let creator = "y".repeat(52);
        let token = keys.lookup_token(&creator, BUNDLE);
        assert_eq!(token, keys.lookup_token(&creator, BUNDLE));
        assert_ne!(token, keys.lookup_token(&"o".repeat(52), BUNDLE));
        assert_ne!(
            token,
            keys.lookup_token(&creator, "111111111111111111111111111")
        );
        assert!(!token
            .windows(BUNDLE.len())
            .any(|window| window == BUNDLE.as_bytes()));
    }

    #[test]
    fn key_parsing_fails_closed() {
        LocksKeys::from_hex("not-hex", MAC_KEY).expect_err("non-hex encryption key rejected");
        LocksKeys::from_hex(ENC_KEY, "abcd").expect_err("short HMAC key rejected");
        LocksKeys::from_hex(ENC_KEY, ENC_KEY).expect_err("identical keys rejected");
        let debug = format!("{:?}", keys());
        assert!(!debug.contains(ENC_KEY) && !debug.contains(MAC_KEY));
    }

    #[test]
    fn http_client_requires_an_http_base_url() {
        assert!(
            HttpLocksClient::new("ftp://locks.example").is_err(),
            "non-http scheme rejected"
        );
        assert!(
            HttpLocksClient::new("https://locks.example/").is_ok(),
            "https URL accepted"
        );
    }
}
