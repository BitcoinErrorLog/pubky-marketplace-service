//! Durable Pubky grant authentication and one-use BFF result handoff.
//!
//! Browser-visible flow IDs are correlation handles only. The authority to
//! retrieve a marketplace bearer is the conjunction of a pinned Shop-BFF
//! workload signature, the immutable delivery context, a fresh result PoP
//! proof, and (for claim) a one-use result token.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, VerifyingKey};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use pubky::{
    AuthFlowKind, Capabilities, ClientId, GrantAuthFlowState, PubkyGrantAuthFlow, PubkyHttpClient,
    PublicKey,
};
use rand::RngCore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Notify;
use url::Url;
use uuid::Uuid;

use crate::auth;
use crate::clock::format_timestamp;
use crate::seal;
use crate::AppState;

const ASSERTION_AUDIENCE: &str = "marketplace-service";
const ASSERTION_BOOTSTRAP_PURPOSE: &str = "marketplace-grant-flow";
const ASSERTION_DELIVERY_PURPOSE: &str = "marketplace-result-delivery";
const RESULT_HASH_SALT: &[u8] = b"marketplace/grant-result-hash/hkdf-salt/v1";
const DELIVERY_KEY_INFO: &[u8] = b"marketplace/grant-result-hash/delivery-key/v1";
const TOKEN_KEY_INFO: &[u8] = b"marketplace/grant-result-hash/token-key/v1";
const DELIVERY_HASH_DOMAIN: &[u8] = b"marketplace/grant-result-hash/delivery-id/v1";
const TOKEN_HASH_DOMAIN: &[u8] = b"marketplace/grant-result-hash/result-token/v1";
const RATE_HASH_DOMAIN: &[u8] = b"marketplace/grant-rate-limit/bucket/v1";
const STATE_AAD_DOMAIN: &[u8] = b"marketplace/grant-flow-state/v1";
const RESULT_AAD_DOMAIN: &[u8] = b"marketplace/grant-flow-result/v1";
const POP_DOMAIN: &str = "marketplace/grant-result-pop/v1";
const RESULT_DENIED: &str = "result_denied";
const GRANT_STATE_VERSION: u8 = 1;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct GrantConfig {
    pub client_id: String,
    pub relay_url: Url,
    pub flow_ttl_seconds: i64,
    pub verify_lease_seconds: i64,
    pub relay_poll_milliseconds: u64,
    pub max_live_flows: i64,
    pub worker_batch_size: i64,
    pub reaper_batch_size: i64,
    pub terminal_retention_seconds: i64,
    pub create_per_ip_per_minute: i64,
    pub create_per_pubky_per_minute: i64,
    pub status_per_flow_per_minute: i64,
    pub result_per_principal_per_minute: i64,
    pub assertion_issuer: String,
    pub session_ttl_seconds: i64,
}

#[derive(Clone)]
struct KeySlot {
    epoch: i16,
    key: [u8; 32],
}

impl std::fmt::Debug for KeySlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeySlot")
            .field("epoch", &self.epoch)
            .field("key", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug)]
struct SecretKeyRing {
    active: KeySlot,
    previous: Option<KeySlot>,
}

impl SecretKeyRing {
    fn select(&self, epoch: i16) -> Option<&[u8; 32]> {
        if self.active.epoch == epoch {
            Some(&self.active.key)
        } else {
            self.previous
                .as_ref()
                .filter(|slot| slot.epoch == epoch)
                .map(|slot| &slot.key)
        }
    }
}

#[derive(Clone)]
struct VerifySlot {
    epoch: i16,
    kid: String,
    key: VerifyingKey,
}

impl std::fmt::Debug for VerifySlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifySlot")
            .field("epoch", &self.epoch)
            .field("kid", &self.kid)
            .field("key", &"<public-key>")
            .finish()
    }
}

#[derive(Clone, Debug)]
struct VerifyKeyRing {
    active: VerifySlot,
    previous: Option<VerifySlot>,
}

impl VerifyKeyRing {
    fn by_kid(&self, kid: &str) -> Option<&VerifySlot> {
        if self.active.kid == kid {
            Some(&self.active)
        } else {
            self.previous.as_ref().filter(|slot| slot.kid == kid)
        }
    }

    fn verify_any(&self, message: &[u8], signature: &Signature) -> Option<&str> {
        [&self.active]
            .into_iter()
            .chain(self.previous.as_ref())
            .find(|slot| slot.key.verify_strict(message, signature).is_ok())
            .map(|slot| slot.kid.as_str())
    }
}

#[derive(Clone)]
pub struct GrantRuntime {
    pub config: GrantConfig,
    encryption_keys: SecretKeyRing,
    result_hash_keys: SecretKeyRing,
    assertion_keys: VerifyKeyRing,
    request_keys: VerifyKeyRing,
    client: PubkyHttpClient,
    notify: Arc<Notify>,
}

impl std::fmt::Debug for GrantRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantRuntime")
            .field("config", &self.config)
            .field("encryption_keys", &self.encryption_keys)
            .field("result_hash_keys", &self.result_hash_keys)
            .field("assertion_keys", &self.assertion_keys)
            .field("request_keys", &self.request_keys)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyKeySet {
    active: VerifyKeyConfig,
    previous: Option<VerifyKeyConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyKeyConfig {
    epoch: i16,
    kid: String,
    public_key: String,
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Ok(value) => match value.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => anyhow::bail!("{name} must be true, false, 1, or 0"),
        },
        Err(_) => Ok(default),
    }
}

fn env_bounded_i64(name: &str, default: i64, min: i64, max: i64) -> anyhow::Result<i64> {
    let value = std::env::var(name)
        .ok()
        .map(|raw| raw.parse::<i64>())
        .transpose()
        .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?
        .unwrap_or(default);
    if !(min..=max).contains(&value) {
        anyhow::bail!("{name} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_epoch(name: &str, value: &str) -> anyhow::Result<i16> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        anyhow::bail!("{name} must be a canonical positive decimal smallint");
    }
    let epoch: i16 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("{name} must be a positive smallint"))?;
    if epoch < 1 {
        anyhow::bail!("{name} must be a positive smallint");
    }
    Ok(epoch)
}

fn parse_standard_key(name: &str, value: &str) -> anyhow::Result<[u8; 32]> {
    if value.trim() != value {
        anyhow::bail!("{name} must be canonical padded standard Base64");
    }
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| anyhow::anyhow!("{name} must be canonical padded standard Base64"))?;
    if decoded.len() != 32 || STANDARD.encode(&decoded) != value {
        anyhow::bail!("{name} must canonically encode exactly 32 bytes");
    }
    decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("{name} must encode exactly 32 bytes"))
}

fn parse_secret_ring(
    active_key_name: &str,
    active_epoch_name: &str,
    previous_key_name: &str,
    previous_epoch_name: &str,
) -> anyhow::Result<SecretKeyRing> {
    let active = KeySlot {
        key: parse_standard_key(
            active_key_name,
            &std::env::var(active_key_name)
                .map_err(|_| anyhow::anyhow!("{active_key_name} must be set"))?,
        )?,
        epoch: parse_epoch(
            active_epoch_name,
            &std::env::var(active_epoch_name)
                .map_err(|_| anyhow::anyhow!("{active_epoch_name} must be set"))?,
        )?,
    };
    let previous_key = std::env::var(previous_key_name).ok();
    let previous_epoch = std::env::var(previous_epoch_name).ok();
    let previous = match (previous_key, previous_epoch) {
        (None, None) => None,
        (Some(key), Some(epoch)) => {
            let slot = KeySlot {
                key: parse_standard_key(previous_key_name, &key)?,
                epoch: parse_epoch(previous_epoch_name, &epoch)?,
            };
            if slot.epoch != active.epoch - 1 || slot.key == active.key {
                anyhow::bail!("{previous_epoch_name} must be active minus one with a distinct key");
            }
            Some(slot)
        }
        _ => anyhow::bail!(
            "{previous_key_name} and {previous_epoch_name} must be configured together"
        ),
    };
    Ok(SecretKeyRing { active, previous })
}

fn valid_kid(kid: &str) -> bool {
    (1..=64).contains(&kid.len())
        && kid.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' => true,
            b'.' | b'_' | b'-' if index > 0 => true,
            _ => false,
        })
}

fn parse_verify_ring(name: &str) -> anyhow::Result<VerifyKeyRing> {
    let raw = std::env::var(name).map_err(|_| anyhow::anyhow!("{name} must be set"))?;
    let parsed: VerifyKeySet =
        serde_json::from_str(&raw).map_err(|_| anyhow::anyhow!("{name} has invalid shape"))?;
    let parse_slot = |config: VerifyKeyConfig| -> anyhow::Result<VerifySlot> {
        if !(1..=32767).contains(&config.epoch) || !valid_kid(&config.kid) {
            anyhow::bail!("{name} contains an invalid epoch or kid");
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(&config.public_key)
            .map_err(|_| anyhow::anyhow!("{name} contains an invalid public key"))?;
        if decoded.len() != 32 || URL_SAFE_NO_PAD.encode(&decoded) != config.public_key {
            anyhow::bail!("{name} public keys must canonically encode 32 bytes");
        }
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| anyhow::anyhow!("{name} public keys must be 32 bytes"))?;
        Ok(VerifySlot {
            epoch: config.epoch,
            kid: config.kid,
            key: VerifyingKey::from_bytes(&bytes)?,
        })
    };
    let active = parse_slot(parsed.active)?;
    let previous = parsed.previous.map(parse_slot).transpose()?;
    if let Some(previous) = &previous {
        if previous.epoch != active.epoch - 1
            || previous.kid == active.kid
            || previous.key == active.key
        {
            anyhow::bail!("{name} previous key must be distinct and active minus one");
        }
    }
    Ok(VerifyKeyRing { active, previous })
}

impl GrantRuntime {
    pub fn from_env(session_ttl_seconds: i64) -> anyhow::Result<Option<Arc<Self>>> {
        if !env_bool("MARKETPLACE_GRANT_FLOW_ENABLED", false)? {
            return Ok(None);
        }
        let client_id = std::env::var("MARKETPLACE_GRANT_CLIENT_ID")
            .map_err(|_| anyhow::anyhow!("MARKETPLACE_GRANT_CLIENT_ID must be set"))?;
        ClientId::new(&client_id)
            .map_err(|_| anyhow::anyhow!("MARKETPLACE_GRANT_CLIENT_ID is invalid"))?;
        let relay_url = Url::parse(
            &std::env::var("MARKETPLACE_GRANT_RELAY_URL")
                .map_err(|_| anyhow::anyhow!("MARKETPLACE_GRANT_RELAY_URL must be set"))?,
        )?;
        if relay_url.scheme() != "https"
            || !relay_url.username().is_empty()
            || relay_url.password().is_some()
            || relay_url.query().is_some()
            || relay_url.fragment().is_some()
        {
            anyhow::bail!("MARKETPLACE_GRANT_RELAY_URL must be a clean HTTPS URL");
        }
        let flow_ttl_seconds = env_bounded_i64("MARKETPLACE_GRANT_FLOW_TTL_SECONDS", 300, 60, 600)?;
        let verify_lease_seconds =
            env_bounded_i64("MARKETPLACE_GRANT_VERIFY_LEASE_SECONDS", 30, 10, 60)?;
        if verify_lease_seconds >= flow_ttl_seconds {
            anyhow::bail!("grant verification lease must be shorter than flow TTL");
        }
        let config = GrantConfig {
            client_id,
            relay_url,
            flow_ttl_seconds,
            verify_lease_seconds,
            relay_poll_milliseconds: env_bounded_i64(
                "MARKETPLACE_GRANT_RELAY_POLL_MILLISECONDS",
                1000,
                250,
                5000,
            )? as u64,
            max_live_flows: env_bounded_i64("MARKETPLACE_GRANT_MAX_LIVE_FLOWS", 1000, 1, i64::MAX)?,
            worker_batch_size: env_bounded_i64("MARKETPLACE_GRANT_WORKER_BATCH_SIZE", 25, 1, 100)?,
            reaper_batch_size: env_bounded_i64("MARKETPLACE_GRANT_REAPER_BATCH_SIZE", 100, 1, 500)?,
            terminal_retention_seconds: env_bounded_i64(
                "MARKETPLACE_GRANT_TERMINAL_RETENTION_SECONDS",
                86_400,
                600,
                i64::MAX,
            )?,
            create_per_ip_per_minute: env_bounded_i64(
                "MARKETPLACE_GRANT_CREATE_PER_IP_PER_MINUTE",
                10,
                1,
                10_000,
            )?,
            create_per_pubky_per_minute: env_bounded_i64(
                "MARKETPLACE_GRANT_CREATE_PER_PUBKY_PER_MINUTE",
                5,
                1,
                10_000,
            )?,
            status_per_flow_per_minute: env_bounded_i64(
                "MARKETPLACE_GRANT_STATUS_PER_FLOW_PER_MINUTE",
                60,
                1,
                100_000,
            )?,
            result_per_principal_per_minute: env_bounded_i64(
                "MARKETPLACE_GRANT_RESULT_PER_PRINCIPAL_PER_MINUTE",
                30,
                1,
                10_000,
            )?,
            assertion_issuer: std::env::var("SHOP_GRANT_ASSERTION_ISSUER")
                .unwrap_or_else(|_| "https://shop.pubky.app".to_string()),
            session_ttl_seconds,
        };
        let runtime = Self {
            config,
            encryption_keys: parse_secret_ring(
                "GRANT_FLOW_ENCRYPTION_KEY_B64",
                "GRANT_FLOW_KEY_EPOCH",
                "GRANT_FLOW_PREVIOUS_ENCRYPTION_KEY_B64",
                "GRANT_FLOW_PREVIOUS_KEY_EPOCH",
            )?,
            result_hash_keys: parse_secret_ring(
                "GRANT_RESULT_HMAC_ROOT_B64",
                "GRANT_RESULT_HMAC_KEY_EPOCH",
                "GRANT_RESULT_HMAC_PREVIOUS_ROOT_B64",
                "GRANT_RESULT_HMAC_PREVIOUS_KEY_EPOCH",
            )?,
            assertion_keys: parse_verify_ring("SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON")?,
            request_keys: parse_verify_ring("SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON")?,
            client: PubkyHttpClient::new()?,
            notify: Arc::new(Notify::new()),
        };
        Ok(Some(Arc::new(runtime)))
    }

    pub fn active_key_epoch(&self) -> i16 {
        self.encryption_keys.active.epoch
    }

    pub fn active_hash_epoch(&self) -> i16 {
        self.result_hash_keys.active.epoch
    }

    fn encryption_key(&self, epoch: i16) -> anyhow::Result<&[u8; 32]> {
        self.encryption_keys
            .select(epoch)
            .ok_or_else(|| anyhow::anyhow!("grant flow key epoch is unavailable"))
    }

    fn result_keys(&self, epoch: i16) -> anyhow::Result<([u8; 32], [u8; 32])> {
        let root = self
            .result_hash_keys
            .select(epoch)
            .ok_or_else(|| anyhow::anyhow!("grant result hash epoch is unavailable"))?;
        let hkdf = Hkdf::<Sha256>::new(Some(RESULT_HASH_SALT), root);
        let mut delivery_info = DELIVERY_KEY_INFO.to_vec();
        delivery_info.extend_from_slice(&epoch.to_be_bytes());
        let mut token_info = TOKEN_KEY_INFO.to_vec();
        token_info.extend_from_slice(&epoch.to_be_bytes());
        let mut delivery = [0u8; 32];
        let mut token = [0u8; 32];
        hkdf.expand(&delivery_info, &mut delivery)
            .map_err(|_| anyhow::anyhow!("grant delivery HKDF expansion failed"))?;
        hkdf.expand(&token_info, &mut token)
            .map_err(|_| anyhow::anyhow!("grant token HKDF expansion failed"))?;
        Ok((delivery, token))
    }

    fn result_hash(
        &self,
        flow_id: Uuid,
        epoch: i16,
        value: &[u8; 32],
        token: bool,
    ) -> anyhow::Result<[u8; 32]> {
        let (delivery_key, token_key) = self.result_keys(epoch)?;
        let (key, domain) = if token {
            (&token_key, TOKEN_HASH_DOMAIN)
        } else {
            (&delivery_key, DELIVERY_HASH_DOMAIN)
        };
        let mut mac = <HmacSha256 as Mac>::new_from_slice(key)
            .map_err(|_| anyhow::anyhow!("grant result HMAC initialization failed"))?;
        mac.update(domain);
        mac.update(flow_id.as_bytes());
        mac.update(value);
        Ok(mac.finalize().into_bytes().into())
    }
}

#[cfg(any(test, feature = "test-faults"))]
pub mod test_support {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    pub struct GrantTestAuthority {
        pub runtime: Arc<GrantRuntime>,
        assertion_key: SigningKey,
        request_key: SigningKey,
    }

    pub struct SeedCompletedFlow<'a> {
        pub expected_pubky: &'a str,
        pub result_cpk: &'a str,
        pub delivery_id: [u8; 32],
        pub bearer: [u8; 32],
        pub result_token: [u8; 32],
        pub now: DateTime<Utc>,
    }

    impl GrantTestAuthority {
        pub fn generate(session_ttl_seconds: i64) -> Self {
            let mut assertion_seed = [0u8; 32];
            let mut request_seed = [0u8; 32];
            let mut encryption_key = [0u8; 32];
            let mut hash_root = [0u8; 32];
            let mut bff_state_key = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut assertion_seed);
            rand::rngs::OsRng.fill_bytes(&mut request_seed);
            rand::rngs::OsRng.fill_bytes(&mut encryption_key);
            rand::rngs::OsRng.fill_bytes(&mut hash_root);
            // Generated in-process to exercise the fifth-input inventory.
            // It belongs to Shop BFF and is deliberately not installed in
            // marketplace-service.
            rand::rngs::OsRng.fill_bytes(&mut bff_state_key);
            let assertion_key = SigningKey::from_bytes(&assertion_seed);
            let request_key = SigningKey::from_bytes(&request_seed);
            let runtime = GrantRuntime {
                config: GrantConfig {
                    client_id: "marketplace.localhost".to_string(),
                    relay_url: Url::parse("http://127.0.0.1:1/inbox").expect("test relay URL"),
                    flow_ttl_seconds: 300,
                    verify_lease_seconds: 30,
                    relay_poll_milliseconds: 1000,
                    max_live_flows: 1000,
                    worker_batch_size: 25,
                    reaper_batch_size: 100,
                    terminal_retention_seconds: 86_400,
                    create_per_ip_per_minute: 10,
                    create_per_pubky_per_minute: 5,
                    status_per_flow_per_minute: 60,
                    result_per_principal_per_minute: 30,
                    assertion_issuer: "https://shop.test".to_string(),
                    session_ttl_seconds,
                },
                encryption_keys: SecretKeyRing {
                    active: KeySlot {
                        epoch: 1,
                        key: encryption_key,
                    },
                    previous: None,
                },
                result_hash_keys: SecretKeyRing {
                    active: KeySlot {
                        epoch: 1,
                        key: hash_root,
                    },
                    previous: None,
                },
                assertion_keys: VerifyKeyRing {
                    active: VerifySlot {
                        epoch: 1,
                        kid: "shop-test-0001".to_string(),
                        key: assertion_key.verifying_key(),
                    },
                    previous: None,
                },
                request_keys: VerifyKeyRing {
                    active: VerifySlot {
                        epoch: 1,
                        kid: "bff-test-0001".to_string(),
                        key: request_key.verifying_key(),
                    },
                    previous: None,
                },
                client: PubkyHttpClient::new().expect("test Pubky client"),
                notify: Arc::new(Notify::new()),
            };
            bff_state_key.fill(0);
            Self {
                runtime: Arc::new(runtime),
                assertion_key,
                request_key,
            }
        }

        pub fn sign_bootstrap_assertion(
            &self,
            sub: &str,
            result_delivery_id: &[u8; 32],
            result_cpk: &str,
            now: DateTime<Utc>,
            jti: Uuid,
        ) -> String {
            self.sign_assertion(json!({
                "aud": ASSERTION_AUDIENCE,
                "exp": now.timestamp() + 60,
                "iat": now.timestamp(),
                "iss": self.runtime.config.assertion_issuer,
                "jti": jti.to_string(),
                "purpose": ASSERTION_BOOTSTRAP_PURPOSE,
                "result_cpk": result_cpk,
                "result_delivery_id": URL_SAFE_NO_PAD.encode(result_delivery_id),
                "sub": sub,
            }))
        }

        pub fn sign_delivery_assertion(
            &self,
            sub: &str,
            result_delivery_id: &[u8; 32],
            result_cpk: &str,
            now: DateTime<Utc>,
            jti: Uuid,
        ) -> String {
            self.sign_assertion(json!({
                "aud": ASSERTION_AUDIENCE,
                "exp": now.timestamp() + 60,
                "iat": now.timestamp(),
                "iss": self.runtime.config.assertion_issuer,
                "jti": jti.to_string(),
                "purpose": ASSERTION_DELIVERY_PURPOSE,
                "result_cpk": result_cpk,
                "result_delivery_id": URL_SAFE_NO_PAD.encode(result_delivery_id),
                "sub": sub,
            }))
        }

        fn sign_assertion(&self, claims: Value) -> String {
            let header = json!({"alg":"EdDSA","kid":"shop-test-0001","typ":"JWT"});
            let header = URL_SAFE_NO_PAD.encode(canonical_json(&header).expect("header JCS"));
            let claims = URL_SAFE_NO_PAD.encode(canonical_json(&claims).expect("claims JCS"));
            let signing_input = format!("{header}.{claims}");
            let signature = self.assertion_key.sign(signing_input.as_bytes());
            format!(
                "{signing_input}.{}",
                URL_SAFE_NO_PAD.encode(signature.to_bytes())
            )
        }

        pub fn sign_service_body(&self, value: &Value) -> (Vec<u8>, String) {
            let body = canonical_json(value).expect("service body JCS");
            let signature = self.request_key.sign(&body);
            (body, URL_SAFE_NO_PAD.encode(signature.to_bytes()))
        }

        pub async fn seed_completed_flow(
            &self,
            pool: &sqlx::PgPool,
            input: SeedCompletedFlow<'_>,
        ) -> Uuid {
            let SeedCompletedFlow {
                expected_pubky,
                result_cpk,
                delivery_id,
                bearer,
                result_token,
                now,
            } = input;
            let flow_id = Uuid::new_v4();
            let key_epoch = self.runtime.active_key_epoch();
            let hash_epoch = self.runtime.active_hash_epoch();
            let delivery_hash = self
                .runtime
                .result_hash(flow_id, hash_epoch, &delivery_id, false)
                .expect("delivery hash");
            let token_hash = self
                .runtime
                .result_hash(flow_id, hash_epoch, &result_token, true)
                .expect("token hash");
            let expires_at = now + Duration::seconds(300);
            let session_expires_at = now + Duration::seconds(86_400);
            let result_expires_at = now + Duration::seconds(60);
            let payload = SealedResult {
                version: GRANT_STATE_VERSION,
                bearer,
                result_token,
                session_expires_at,
            };
            let aad = result_aad(flow_id, result_cpk, key_epoch).expect("result AAD");
            let sealed = seal::seal(
                self.runtime.encryption_key(key_epoch).expect("test key"),
                &aad,
                &canonical_json(&payload).expect("result JSON"),
            );
            let session_id: Uuid = sqlx::query_scalar(
                "INSERT INTO auth_sessions (token_hash,pubky,capabilities,created_at,expires_at) \
                 VALUES ($1,$2,'',$3,$4) RETURNING session_id",
            )
            .bind(auth::hash_token(&bearer))
            .bind(expected_pubky)
            .bind(now)
            .bind(session_expires_at)
            .fetch_one(pool)
            .await
            .expect("seed auth session");
            sqlx::query(
                "INSERT INTO grant_flows (flow_id,expected_pubky,assertion_jti,client_id,cpk,\
                 relay_url,key_epoch,result_hash_epoch,status,approved_pubky,\
                 result_delivery_id_hash,result_cpk,result_token_hash,result_token_expires_at,\
                 result_payload_sealed,result_auth_session_id,created_at,expires_at,terminal_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'complete',$2,$9,$10,$11,$12,$13,$14,$15,$16,$15)",
            )
            .bind(flow_id)
            .bind(expected_pubky)
            .bind(Uuid::new_v4())
            .bind(&self.runtime.config.client_id)
            .bind(
                PublicKey::try_from_z32(result_cpk)
                    .expect("result key")
                    .z32(),
            )
            .bind(self.runtime.config.relay_url.as_str())
            .bind(key_epoch)
            .bind(hash_epoch)
            .bind(delivery_hash.as_slice())
            .bind(result_cpk)
            .bind(token_hash.as_slice())
            .bind(result_expires_at)
            .bind(sealed)
            .bind(session_id)
            .bind(now)
            .bind(expires_at)
            .execute(pool)
            .await
            .expect("seed completed flow");
            flow_id
        }

        pub async fn settle_verified_identity(
            &self,
            state: &AppState,
            expected_pubky: &str,
            approved_pubky: &str,
        ) -> (Uuid, bool, bool) {
            let flow_id = Uuid::new_v4();
            let lease_owner = Uuid::new_v4();
            let now = state.clock.now();
            let cpk = "y".repeat(52);
            sqlx::query(
                "INSERT INTO grant_flows (flow_id,expected_pubky,assertion_jti,client_id,cpk,\
                 relay_url,grant_state_sealed,key_epoch,result_hash_epoch,status,\
                 version,lease_owner,lease_until,result_delivery_id_hash,result_cpk,created_at,expires_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,1,1,'verifying',1,$8,$9,$10,$11,$12,$13)",
            )
            .bind(flow_id)
            .bind(expected_pubky)
            .bind(Uuid::new_v4())
            .bind(&self.runtime.config.client_id)
            .bind(&cpk)
            .bind(self.runtime.config.relay_url.as_str())
            .bind(vec![1u8; 80])
            .bind(lease_owner)
            .bind(now + Duration::seconds(30))
            .bind(vec![2u8; 32])
            .bind(&cpk)
            .bind(now)
            .bind(now + Duration::seconds(300))
            .execute(&state.pool)
            .await
            .expect("seed verifying flow");
            let lease = FlowLease {
                flow_id,
                expected_pubky: expected_pubky.to_string(),
                client_id: self.runtime.config.client_id.clone(),
                cpk,
                grant_state_sealed: vec![1u8; 80],
                key_epoch: 1,
                result_hash_epoch: 1,
                result_cpk: "y".repeat(52),
                lease_owner,
                version: 1,
            };
            let first = settle_verified(state, &self.runtime, &lease, approved_pubky, now)
                .await
                .expect("first settlement");
            let replay = settle_verified(state, &self.runtime, &lease, approved_pubky, now)
                .await
                .expect("replay settlement");
            (flow_id, first, replay)
        }
    }
}

fn append_len_prefixed(output: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    let length: u16 = value
        .len()
        .try_into()
        .map_err(|_| anyhow::anyhow!("grant AAD field is too long"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn state_aad(flow_id: Uuid, client_id: &str, cpk: &str, epoch: i16) -> anyhow::Result<Vec<u8>> {
    let mut aad = STATE_AAD_DOMAIN.to_vec();
    aad.extend_from_slice(flow_id.as_bytes());
    append_len_prefixed(&mut aad, client_id)?;
    append_len_prefixed(&mut aad, cpk)?;
    aad.extend_from_slice(&epoch.to_be_bytes());
    Ok(aad)
}

fn result_aad(flow_id: Uuid, result_cpk: &str, epoch: i16) -> anyhow::Result<Vec<u8>> {
    let mut aad = RESULT_AAD_DOMAIN.to_vec();
    aad.extend_from_slice(flow_id.as_bytes());
    append_len_prefixed(&mut aad, result_cpk)?;
    aad.extend_from_slice(&epoch.to_be_bytes());
    Ok(aad)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredGrantState {
    version: u8,
    state: GrantAuthFlowState,
}

fn canonical_json<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json_canonicalizer::to_string(value)?.into_bytes())
}

fn decode_canonical_b64(name: &str, value: &str, expected: usize) -> anyhow::Result<Vec<u8>> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow::anyhow!("{name} is not canonical Base64url"))?;
    if decoded.len() != expected || URL_SAFE_NO_PAD.encode(&decoded) != value {
        anyhow::bail!("{name} must canonically encode {expected} bytes");
    }
    Ok(decoded)
}

fn canonical_uuid(value: &str) -> anyhow::Result<Uuid> {
    let parsed = Uuid::parse_str(value).map_err(|_| anyhow::anyhow!("UUID is invalid"))?;
    if parsed.to_string() != value {
        anyhow::bail!("UUID is not canonical");
    }
    Ok(parsed)
}

fn canonical_pubky(value: &str) -> anyhow::Result<PublicKey> {
    let parsed = PublicKey::from_str(value).map_err(|_| anyhow::anyhow!("pubky is invalid"))?;
    if parsed.z32() != value || value.len() != 52 {
        anyhow::bail!("pubky is not canonical");
    }
    Ok(parsed)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AssertionHeader {
    alg: String,
    kid: String,
    typ: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BootstrapAssertion {
    aud: String,
    exp: i64,
    iat: i64,
    iss: String,
    jti: String,
    purpose: String,
    result_cpk: String,
    result_delivery_id: String,
    sub: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeliveryAssertion {
    aud: String,
    exp: i64,
    iat: i64,
    iss: String,
    jti: String,
    purpose: String,
    result_cpk: String,
    result_delivery_id: String,
    sub: String,
}

#[derive(Debug)]
struct VerifiedAssertion {
    jti: Uuid,
    expected_pubky: Option<String>,
    result_cpk: String,
    delivery_id: [u8; 32],
}

fn decode_jcs_segment<T: DeserializeOwned + Serialize>(segment: &str) -> anyhow::Result<T> {
    let decoded = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| anyhow::anyhow!("assertion segment is invalid"))?;
    if URL_SAFE_NO_PAD.encode(&decoded) != segment {
        anyhow::bail!("assertion segment is not canonical Base64url");
    }
    let parsed: T = serde_json::from_slice(&decoded)
        .map_err(|_| anyhow::anyhow!("assertion JSON is invalid"))?;
    if canonical_json(&parsed)? != decoded {
        anyhow::bail!("assertion JSON is not RFC 8785 canonical");
    }
    Ok(parsed)
}

fn validate_assertion_times(iat: i64, exp: i64, now: DateTime<Utc>) -> anyhow::Result<()> {
    if exp <= iat || exp - iat > 60 || iat > now.timestamp() + 5 || now.timestamp() >= exp {
        anyhow::bail!("assertion time bounds are invalid");
    }
    Ok(())
}

fn verify_assertion(
    runtime: &GrantRuntime,
    compact: &str,
    bootstrap: bool,
    now: DateTime<Utc>,
) -> anyhow::Result<VerifiedAssertion> {
    let segments: Vec<&str> = compact.split('.').collect();
    if segments.len() != 3 {
        anyhow::bail!("assertion must be compact JWS");
    }
    let header: AssertionHeader = decode_jcs_segment(segments[0])?;
    if header.alg != "EdDSA" || header.typ != "JWT" || !valid_kid(&header.kid) {
        anyhow::bail!("assertion protected header is invalid");
    }
    let slot = runtime
        .assertion_keys
        .by_kid(&header.kid)
        .ok_or_else(|| anyhow::anyhow!("assertion kid is unknown"))?;
    let signature_bytes = decode_canonical_b64("assertion signature", segments[2], 64)?;
    let signature = Signature::from_slice(&signature_bytes)?;
    slot.key.verify_strict(
        format!("{}.{}", segments[0], segments[1]).as_bytes(),
        &signature,
    )?;

    let (aud, exp, iat, iss, jti, purpose, result_cpk, result_delivery_id, sub) = if bootstrap {
        let claims: BootstrapAssertion = decode_jcs_segment(segments[1])?;
        (
            claims.aud,
            claims.exp,
            claims.iat,
            claims.iss,
            claims.jti,
            claims.purpose,
            claims.result_cpk,
            claims.result_delivery_id,
            Some(claims.sub),
        )
    } else {
        let claims: DeliveryAssertion = decode_jcs_segment(segments[1])?;
        (
            claims.aud,
            claims.exp,
            claims.iat,
            claims.iss,
            claims.jti,
            claims.purpose,
            claims.result_cpk,
            claims.result_delivery_id,
            Some(claims.sub),
        )
    };
    let expected_purpose = if bootstrap {
        ASSERTION_BOOTSTRAP_PURPOSE
    } else {
        ASSERTION_DELIVERY_PURPOSE
    };
    if aud != ASSERTION_AUDIENCE
        || iss != runtime.config.assertion_issuer
        || purpose != expected_purpose
    {
        anyhow::bail!("assertion audience, issuer, or purpose is invalid");
    }
    validate_assertion_times(iat, exp, now)?;
    let expected_pubky = sub
        .map(|value| canonical_pubky(&value).map(|_| value))
        .transpose()?;
    canonical_pubky(&result_cpk)?;
    let delivery_id = decode_canonical_b64("result_delivery_id", &result_delivery_id, 32)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("result_delivery_id must be 32 bytes"))?;
    Ok(VerifiedAssertion {
        jti: canonical_uuid(&jti)?,
        expected_pubky,
        result_cpk,
        delivery_id,
    })
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );
    response
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    no_store((status, Json(json!({ "error": code }))).into_response())
}

async fn admit_rate(
    state: &AppState,
    runtime: &GrantRuntime,
    endpoint_class: &str,
    bucket: &str,
    limit: i64,
) -> anyhow::Result<bool> {
    let (rate_key, _) = runtime.result_keys(runtime.active_hash_epoch())?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(&rate_key)
        .map_err(|_| anyhow::anyhow!("grant rate HMAC initialization failed"))?;
    mac.update(RATE_HASH_DOMAIN);
    mac.update(endpoint_class.as_bytes());
    mac.update(&[0]);
    mac.update(bucket.as_bytes());
    let bucket_hash = mac.finalize().into_bytes();
    let now = state.clock.now();
    let window_floor = now - Duration::seconds(60);
    let count: i32 = sqlx::query_scalar(
        "INSERT INTO grant_rate_limits \
         (bucket_hash, endpoint_class, window_started_at, request_count) \
         VALUES ($1,$2,$3,1) \
         ON CONFLICT (bucket_hash, endpoint_class) DO UPDATE SET \
           window_started_at = CASE \
             WHEN grant_rate_limits.window_started_at <= $4 THEN EXCLUDED.window_started_at \
             ELSE grant_rate_limits.window_started_at END, \
           request_count = CASE \
             WHEN grant_rate_limits.window_started_at <= $4 THEN 1 \
             ELSE grant_rate_limits.request_count + 1 END \
         RETURNING request_count",
    )
    .bind(bucket_hash.as_slice())
    .bind(endpoint_class)
    .bind(now)
    .bind(window_floor)
    .fetch_one(&state.pool)
    .await?;
    Ok(i64::from(count) <= limit)
}

fn request_ip(ConnectInfo(address): ConnectInfo<SocketAddr>) -> String {
    address.ip().to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateGrantFlowRequest {
    assertion: Option<String>,
    delivery_assertion: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedResult {
    version: u8,
    bearer: [u8; 32],
    result_token: [u8; 32],
    session_expires_at: DateTime<Utc>,
}

fn grant_runtime(state: &AppState) -> Option<&Arc<GrantRuntime>> {
    state.grant.as_ref()
}

fn authorization_cpk(url: &Url) -> anyhow::Result<String> {
    let pairs: HashMap<_, _> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let cpk = pairs
        .get("cpk")
        .ok_or_else(|| anyhow::anyhow!("SDK grant URL omitted cpk"))?;
    canonical_pubky(cpk)?;
    Ok(cpk.clone())
}

pub async fn create_flow(
    State(state): State<AppState>,
    connect: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match grant_runtime(&state) {
        Some(runtime) => runtime,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
        }
    };
    match admit_rate(
        &state,
        runtime,
        "create_ip",
        &request_ip(connect),
        runtime.config.create_per_ip_per_minute,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let request: CreateGrantFlowRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    let now = state.clock.now();
    let actor = match auth::actor_from_authorization(&state.pool, &headers, now).await {
        Ok(actor) => actor,
        Err(_) => return error_response(StatusCode::UNAUTHORIZED, "invalid_session"),
    };
    let (compact, bootstrap) = match (
        actor.as_ref(),
        request.assertion.as_deref(),
        request.delivery_assertion.as_deref(),
    ) {
        (None, Some(assertion), None) => (assertion, true),
        (Some(_), None, Some(assertion)) => (assertion, false),
        _ => return error_response(StatusCode::BAD_REQUEST, "ambiguous_principal"),
    };
    let assertion = match verify_assertion(runtime, compact, bootstrap, now) {
        Ok(assertion) => assertion,
        Err(_) => return error_response(StatusCode::UNAUTHORIZED, "invalid_assertion"),
    };
    let expected_pubky = if bootstrap {
        assertion
            .expected_pubky
            .clone()
            .expect("bootstrap verifier requires sub")
    } else {
        let actor_pubky = actor.expect("reconnect mode requires actor").0;
        if assertion.expected_pubky.as_deref() != Some(actor_pubky.as_str()) {
            return error_response(StatusCode::UNAUTHORIZED, "invalid_assertion");
        }
        actor_pubky
    };
    if !marketplace_domain::pubky::is_valid_pubky(&expected_pubky) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid_assertion");
    }
    match admit_rate(
        &state,
        runtime,
        "create_pubky",
        &expected_pubky,
        runtime.config.create_per_pubky_per_minute,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }

    let client_id = match ClientId::new(&runtime.config.client_id) {
        Ok(client_id) => client_id,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let flow = match PubkyGrantAuthFlow::builder(
        &Capabilities::default(),
        AuthFlowKind::signin(),
        client_id,
    )
    .relay(runtime.config.relay_url.clone())
    .client(runtime.client.clone())
    .start()
    {
        Ok(flow) => flow,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let authorization_url = flow.authorization_url();
    let cpk = match authorization_cpk(&authorization_url) {
        Ok(cpk) => cpk,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let saved = match flow.save_local() {
        Some(saved) => saved,
        None => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    drop(flow);

    let flow_id = Uuid::new_v4();
    let key_epoch = runtime.active_key_epoch();
    let hash_epoch = runtime.active_hash_epoch();
    let expires_at = now + Duration::seconds(runtime.config.flow_ttl_seconds);
    let stored = StoredGrantState {
        version: GRANT_STATE_VERSION,
        state: saved,
    };
    let plaintext = match canonical_json(&stored) {
        Ok(plaintext) => plaintext,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let aad = match state_aad(flow_id, &runtime.config.client_id, &cpk, key_epoch) {
        Ok(aad) => aad,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let sealed = seal::seal(&runtime.encryption_keys.active.key, &aad, &plaintext);
    let delivery_hash =
        match runtime.result_hash(flow_id, hash_epoch, &assertion.delivery_id, false) {
            Ok(hash) => hash,
            Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
        };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    if sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(6_747_261_680_359_579_715_i64)
        .execute(&mut *tx)
        .await
        .is_err()
    {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
    }
    let live: i64 = match sqlx::query_scalar(
        "SELECT COUNT(*) FROM grant_flows \
         WHERE status IN ('awaiting','verifying','complete') \
           AND expires_at > $1 AND result_claimed_at IS NULL",
    )
    .bind(now)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(count) => count,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    if live >= runtime.config.max_live_flows {
        return error_response(StatusCode::TOO_MANY_REQUESTS, "capacity_exhausted");
    }
    let inserted = sqlx::query(
        "INSERT INTO grant_flows (flow_id, expected_pubky, assertion_jti, client_id, cpk, \
         relay_url, grant_state_sealed, key_epoch, result_hash_epoch, status, \
         result_delivery_id_hash, result_cpk, created_at, expires_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'awaiting',$10,$11,$12,$13)",
    )
    .bind(flow_id)
    .bind(&expected_pubky)
    .bind(assertion.jti)
    .bind(&runtime.config.client_id)
    .bind(&cpk)
    .bind(runtime.config.relay_url.as_str())
    .bind(sealed)
    .bind(key_epoch)
    .bind(hash_epoch)
    .bind(delivery_hash.as_slice())
    .bind(&assertion.result_cpk)
    .bind(now)
    .bind(expires_at)
    .execute(&mut *tx)
    .await;
    match inserted {
        Ok(_) => {
            if tx.commit().await.is_err() {
                return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
            }
            runtime.notify.notify_one();
            no_store(
                (
                    StatusCode::CREATED,
                    Json(json!({
                        "authorization_url": authorization_url.as_str(),
                        "expires_at": format_timestamp(expires_at),
                        "flow_id": flow_id,
                        "status": "awaiting",
                    })),
                )
                    .into_response(),
            )
        }
        Err(error)
            if error
                .as_database_error()
                .is_some_and(|error| error.is_unique_violation()) =>
        {
            error_response(StatusCode::CONFLICT, "assertion_replayed")
        }
        Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
}

#[derive(Debug, sqlx::FromRow)]
struct StatusRow {
    flow_id: Uuid,
    status: String,
    expires_at: DateTime<Utc>,
}

pub async fn get_status(
    State(state): State<AppState>,
    connect: ConnectInfo<SocketAddr>,
    Path(flow_id): Path<Uuid>,
) -> Response {
    let Some(runtime) = grant_runtime(&state) else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
    };
    let bucket = format!("{}:{flow_id}", request_ip(connect));
    match admit_rate(
        &state,
        runtime,
        "status_flow",
        &bucket,
        runtime.config.status_per_flow_per_minute,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let row: Option<StatusRow> = match sqlx::query_as(
        "SELECT flow_id, status, expires_at FROM grant_flows WHERE flow_id = $1",
    )
    .bind(flow_id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    };
    let Some(row) = row else {
        return error_response(StatusCode::NOT_FOUND, "flow_not_found");
    };
    if matches!(
        row.status.as_str(),
        "mismatch" | "expired" | "cancelled" | "invalid" | "failed"
    ) {
        return no_store(
            (
                StatusCode::GONE,
                Json(json!({
                    "status": "terminal",
                })),
            )
                .into_response(),
        );
    }
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "expires_at": format_timestamp(row.expires_at),
                "flow_id": row.flow_id,
                "status": row.status,
            })),
        )
            .into_response(),
    )
}

fn verify_service_request<'a>(
    runtime: &'a GrantRuntime,
    headers: &HeaderMap,
    body: &[u8],
    expected_path: &str,
) -> anyhow::Result<(&'a str, Value)> {
    let signatures: Vec<_> = headers.get_all("x-marketplace-signature").iter().collect();
    if signatures.len() != 1 {
        anyhow::bail!("exactly one service signature is required");
    }
    let signature_text = signatures[0]
        .to_str()
        .map_err(|_| anyhow::anyhow!("service signature is invalid"))?;
    let signature = Signature::from_slice(&decode_canonical_b64(
        "service signature",
        signature_text,
        64,
    )?)?;
    let principal = runtime
        .request_keys
        .verify_any(body, &signature)
        .ok_or_else(|| anyhow::anyhow!("service signature is invalid"))?;
    let value: Value =
        serde_json::from_slice(body).map_err(|_| anyhow::anyhow!("request JSON is invalid"))?;
    if canonical_json(&value)? != body {
        anyhow::bail!("service request body is not RFC 8785 canonical");
    }
    if value.get("method").and_then(Value::as_str) != Some("POST")
        || value.get("path").and_then(Value::as_str) != Some(expected_path)
    {
        anyhow::bail!("signed method or path does not match request");
    }
    Ok((principal, value))
}

async fn admit_result_rates(
    state: &AppState,
    runtime: &GrantRuntime,
    principal: &str,
    flow_id: Uuid,
) -> anyhow::Result<bool> {
    if !admit_rate(
        state,
        runtime,
        "result_principal",
        principal,
        runtime.config.result_per_principal_per_minute,
    )
    .await?
    {
        return Ok(false);
    }
    admit_rate(state, runtime, "result_flow", &flow_id.to_string(), 10).await
}

async fn consume_service_request(
    state: &AppState,
    principal: &str,
    endpoint_class: &str,
    request_id: &str,
) -> anyhow::Result<bool> {
    let request_id = canonical_uuid(request_id)?;
    let inserted = sqlx::query(
        "INSERT INTO grant_service_requests \
         (request_id, bff_principal, endpoint_class, used_at) \
         VALUES ($1,$2,$3,$4) ON CONFLICT (request_id) DO NOTHING",
    )
    .bind(request_id)
    .bind(principal)
    .bind(endpoint_class)
    .bind(state.clock.now())
    .execute(&state.pool)
    .await?;
    Ok(inserted.rows_affected() == 1)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NonceRequest {
    method: String,
    path: String,
    purpose: String,
    request_id: String,
}

pub async fn issue_result_nonce(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match grant_runtime(&state) {
        Some(runtime) => runtime,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
        }
    };
    let path = format!("/v1/auth/grant-flows/{flow_id}/result-nonces");
    let (principal, value) = match verify_service_request(runtime, &headers, &body, &path) {
        Ok(verified) => verified,
        Err(_) => {
            return error_response(StatusCode::UNAUTHORIZED, "invalid_service_signature");
        }
    };
    match admit_result_rates(&state, runtime, principal, flow_id).await {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let request: NonceRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    if request.method != "POST"
        || request.path != path
        || !matches!(request.purpose.as_str(), "ticket" | "claim")
        || canonical_uuid(&request.request_id).is_err()
    {
        return error_response(StatusCode::BAD_REQUEST, "invalid_request");
    }
    match consume_service_request(&state, principal, "nonce", &request.request_id).await {
        Ok(true) => {}
        Ok(false) => return result_denied(flow_id, principal),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let exists: bool =
        match sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM grant_flows WHERE flow_id = $1)")
            .bind(flow_id)
            .fetch_one(&state.pool)
            .await
        {
            Ok(exists) => exists,
            Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
        };
    if !exists {
        return error_response(StatusCode::FORBIDDEN, RESULT_DENIED);
    }
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let nonce_id = Uuid::new_v4();
    let now = state.clock.now();
    let expires_at = now + Duration::seconds(30);
    let digest = Sha256::digest(nonce);
    let inserted = sqlx::query(
        "INSERT INTO grant_result_nonces \
         (nonce_id, flow_id, purpose, bff_principal, nonce_hash, created_at, expires_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(nonce_id)
    .bind(flow_id)
    .bind(&request.purpose)
    .bind(principal)
    .bind(digest.as_slice())
    .bind(now)
    .bind(expires_at)
    .execute(&state.pool)
    .await;
    if inserted.is_err() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
    }
    no_store(
        (
            StatusCode::CREATED,
            Json(json!({
                "expires_at": format_timestamp(expires_at),
                "nonce": URL_SAFE_NO_PAD.encode(nonce),
                "nonce_id": nonce_id,
            })),
        )
            .into_response(),
    )
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResultProof {
    issued_at: i64,
    nonce: String,
    nonce_id: String,
    signature: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TicketRequest {
    method: String,
    path: String,
    proof: ResultProof,
    request_id: String,
    result_delivery_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimRequest {
    method: String,
    path: String,
    proof: ResultProof,
    request_id: String,
    result_delivery_id: String,
    result_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelRequest {
    method: String,
    path: String,
    request_id: String,
    result_delivery_id: String,
}

#[derive(Debug, Serialize)]
struct PopMessage<'a> {
    domain: &'static str,
    flow_id: String,
    issued_at: i64,
    method: &'static str,
    nonce: &'a str,
    nonce_id: &'a str,
    path: &'a str,
    purpose: &'a str,
    result_delivery_id: &'a str,
}

#[derive(Debug, sqlx::FromRow)]
struct ResultFlowRow {
    status: String,
    result_delivery_id_hash: Vec<u8>,
    result_cpk: String,
    result_token_hash: Option<Vec<u8>>,
    result_token_expires_at: Option<DateTime<Utc>>,
    result_token_delivered_at: Option<DateTime<Utc>>,
    result_payload_sealed: Option<Vec<u8>>,
    result_claimed_at: Option<DateTime<Utc>>,
    key_epoch: i16,
    result_hash_epoch: i16,
    expected_pubky: String,
}

fn flow_prefix(flow_id: Uuid) -> String {
    flow_id.to_string()[..8].to_string()
}

fn result_denied(flow_id: Uuid, principal: &str) -> Response {
    tracing::warn!(
        flow_prefix = flow_prefix(flow_id),
        bff_principal = principal,
        result = RESULT_DENIED,
        "grant.result_denied"
    );
    error_response(StatusCode::FORBIDDEN, RESULT_DENIED)
}

fn decode_32(name: &str, value: &str) -> anyhow::Result<[u8; 32]> {
    decode_canonical_b64(name, value, 32)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{name} must be 32 bytes"))
}

fn fixed_digest_matches(stored: &[u8], expected: &[u8; 32]) -> bool {
    stored.len() == 32 && stored.ct_eq(expected).into()
}

struct PopVerification<'a> {
    flow_id: Uuid,
    principal: &'a str,
    purpose: &'a str,
    path: &'a str,
    delivery_id_text: &'a str,
    result_cpk: &'a str,
    proof: &'a ResultProof,
    now: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct NonceRow {
    nonce_hash: Vec<u8>,
    expires_at: DateTime<Utc>,
    used_at: Option<DateTime<Utc>>,
}

async fn verify_pop_and_consume(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    input: PopVerification<'_>,
) -> anyhow::Result<()> {
    let PopVerification {
        flow_id,
        principal,
        purpose,
        path,
        delivery_id_text,
        result_cpk,
        proof,
        now,
    } = input;
    if (now.timestamp() - proof.issued_at).abs() > 30 {
        anyhow::bail!("result proof is outside its age window");
    }
    let nonce_id = canonical_uuid(&proof.nonce_id)?;
    let nonce = decode_32("nonce", &proof.nonce)?;
    let signature = Signature::from_slice(&decode_canonical_b64(
        "proof signature",
        &proof.signature,
        64,
    )?)?;
    let public_key = canonical_pubky(result_cpk)?;
    let verifying_key = VerifyingKey::from_bytes(public_key.as_inner().as_bytes())?;
    let message = PopMessage {
        domain: POP_DOMAIN,
        flow_id: flow_id.to_string(),
        issued_at: proof.issued_at,
        method: "POST",
        nonce: &proof.nonce,
        nonce_id: &proof.nonce_id,
        path,
        purpose,
        result_delivery_id: delivery_id_text,
    };
    verifying_key.verify_strict(&canonical_json(&message)?, &signature)?;

    let nonce_row: Option<NonceRow> = sqlx::query_as(
        "SELECT nonce_hash, expires_at, used_at FROM grant_result_nonces \
         WHERE nonce_id = $1 AND flow_id = $2 AND purpose = $3 AND bff_principal = $4 \
         FOR UPDATE",
    )
    .bind(nonce_id)
    .bind(flow_id)
    .bind(purpose)
    .bind(principal)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(nonce_row) = nonce_row else {
        anyhow::bail!("result nonce was not found");
    };
    let nonce_hash: [u8; 32] = Sha256::digest(nonce).into();
    if nonce_row.expires_at <= now
        || nonce_row.used_at.is_some()
        || !fixed_digest_matches(&nonce_row.nonce_hash, &nonce_hash)
    {
        anyhow::bail!("result nonce is unavailable");
    }
    let consumed = sqlx::query(
        "UPDATE grant_result_nonces SET used_at = $2 \
         WHERE nonce_id = $1 AND used_at IS NULL AND expires_at > $2",
    )
    .bind(nonce_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if consumed.rows_affected() != 1 {
        anyhow::bail!("result nonce lost its one-use race");
    }
    Ok(())
}

fn open_result(
    runtime: &GrantRuntime,
    flow_id: Uuid,
    result_cpk: &str,
    key_epoch: i16,
    sealed: &[u8],
) -> anyhow::Result<SealedResult> {
    let aad = result_aad(flow_id, result_cpk, key_epoch)?;
    let plaintext = seal::open(runtime.encryption_key(key_epoch)?, &aad, sealed)?;
    let result: SealedResult = serde_json::from_slice(&plaintext)?;
    if result.version != GRANT_STATE_VERSION || canonical_json(&result)? != plaintext {
        anyhow::bail!("sealed grant result is not canonical");
    }
    Ok(result)
}

pub async fn result_ticket(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match grant_runtime(&state) {
        Some(runtime) => runtime,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
        }
    };
    let path = format!("/v1/auth/grant-flows/{flow_id}/result-ticket");
    let (principal, value) = match verify_service_request(runtime, &headers, &body, &path) {
        Ok(verified) => verified,
        Err(_) => {
            return error_response(StatusCode::UNAUTHORIZED, "invalid_service_signature");
        }
    };
    match admit_result_rates(&state, runtime, principal, flow_id).await {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let request: TicketRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    if request.method != "POST"
        || request.path != path
        || canonical_uuid(&request.request_id).is_err()
    {
        return error_response(StatusCode::BAD_REQUEST, "invalid_request");
    }
    match consume_service_request(&state, principal, "ticket", &request.request_id).await {
        Ok(true) => {}
        Ok(false) => return result_denied(flow_id, principal),
        Err(_) => return result_denied(flow_id, principal),
    }
    let delivery_id = match decode_32("result_delivery_id", &request.result_delivery_id) {
        Ok(value) => value,
        Err(_) => return result_denied(flow_id, principal),
    };
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return result_denied(flow_id, principal),
    };
    let row: Option<ResultFlowRow> = match sqlx::query_as(
        "SELECT status, result_delivery_id_hash, result_cpk, result_token_hash, \
         result_token_expires_at, result_token_delivered_at, result_payload_sealed, \
         result_claimed_at, key_epoch, result_hash_epoch, expected_pubky \
         FROM grant_flows WHERE flow_id = $1 FOR UPDATE",
    )
    .bind(flow_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(_) => return result_denied(flow_id, principal),
    };
    let Some(row) = row else {
        return result_denied(flow_id, principal);
    };
    let expected_delivery_hash =
        match runtime.result_hash(flow_id, row.result_hash_epoch, &delivery_id, false) {
            Ok(hash) => hash,
            Err(_) => return result_denied(flow_id, principal),
        };
    if row.status != "complete"
        || row.result_claimed_at.is_some()
        || row.result_token_delivered_at.is_some()
        || row
            .result_token_expires_at
            .is_none_or(|expires| expires <= now)
        || !fixed_digest_matches(&row.result_delivery_id_hash, &expected_delivery_hash)
    {
        return result_denied(flow_id, principal);
    }
    if verify_pop_and_consume(
        &mut tx,
        PopVerification {
            flow_id,
            principal,
            purpose: "ticket",
            path: &path,
            delivery_id_text: &request.result_delivery_id,
            result_cpk: &row.result_cpk,
            proof: &request.proof,
            now,
        },
    )
    .await
    .is_err()
    {
        return result_denied(flow_id, principal);
    }
    let sealed = match row.result_payload_sealed.as_deref() {
        Some(sealed) => sealed,
        None => return result_denied(flow_id, principal),
    };
    let result = match open_result(runtime, flow_id, &row.result_cpk, row.key_epoch, sealed) {
        Ok(result) => result,
        Err(_) => return result_denied(flow_id, principal),
    };
    let stamped = match sqlx::query(
        "UPDATE grant_flows SET result_token_delivered_at = $2 \
         WHERE flow_id = $1 AND status = 'complete' \
           AND result_token_delivered_at IS NULL AND result_claimed_at IS NULL",
    )
    .bind(flow_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result,
        Err(_) => return result_denied(flow_id, principal),
    };
    if stamped.rows_affected() != 1 || tx.commit().await.is_err() {
        return result_denied(flow_id, principal);
    }
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "expires_at": format_timestamp(row.result_token_expires_at.expect("checked")),
                "result_token": URL_SAFE_NO_PAD.encode(result.result_token),
            })),
        )
            .into_response(),
    )
}

pub async fn claim_result(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match grant_runtime(&state) {
        Some(runtime) => runtime,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
        }
    };
    let path = format!("/v1/auth/grant-flows/{flow_id}/claim");
    let (principal, value) = match verify_service_request(runtime, &headers, &body, &path) {
        Ok(verified) => verified,
        Err(_) => {
            return error_response(StatusCode::UNAUTHORIZED, "invalid_service_signature");
        }
    };
    match admit_result_rates(&state, runtime, principal, flow_id).await {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let request: ClaimRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    if request.method != "POST"
        || request.path != path
        || canonical_uuid(&request.request_id).is_err()
    {
        return error_response(StatusCode::BAD_REQUEST, "invalid_request");
    }
    match consume_service_request(&state, principal, "claim", &request.request_id).await {
        Ok(true) => {}
        Ok(false) => return result_denied(flow_id, principal),
        Err(_) => return result_denied(flow_id, principal),
    }
    let delivery_id = match decode_32("result_delivery_id", &request.result_delivery_id) {
        Ok(value) => value,
        Err(_) => return result_denied(flow_id, principal),
    };
    let presented_token = match decode_32("result_token", &request.result_token) {
        Ok(value) => value,
        Err(_) => return result_denied(flow_id, principal),
    };
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return result_denied(flow_id, principal),
    };
    let row: Option<ResultFlowRow> = match sqlx::query_as(
        "SELECT status, result_delivery_id_hash, result_cpk, result_token_hash, \
         result_token_expires_at, result_token_delivered_at, result_payload_sealed, \
         result_claimed_at, key_epoch, result_hash_epoch, expected_pubky \
         FROM grant_flows WHERE flow_id = $1 FOR UPDATE",
    )
    .bind(flow_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(_) => return result_denied(flow_id, principal),
    };
    let Some(row) = row else {
        return result_denied(flow_id, principal);
    };
    let expected_delivery_hash =
        match runtime.result_hash(flow_id, row.result_hash_epoch, &delivery_id, false) {
            Ok(hash) => hash,
            Err(_) => return result_denied(flow_id, principal),
        };
    let expected_token_hash =
        match runtime.result_hash(flow_id, row.result_hash_epoch, &presented_token, true) {
            Ok(hash) => hash,
            Err(_) => return result_denied(flow_id, principal),
        };
    if row.status != "complete"
        || row.result_claimed_at.is_some()
        || row.result_token_delivered_at.is_none()
        || row
            .result_token_expires_at
            .is_none_or(|expires| expires <= now)
        || !fixed_digest_matches(&row.result_delivery_id_hash, &expected_delivery_hash)
        || row
            .result_token_hash
            .as_deref()
            .is_none_or(|stored| !fixed_digest_matches(stored, &expected_token_hash))
    {
        return result_denied(flow_id, principal);
    }
    if verify_pop_and_consume(
        &mut tx,
        PopVerification {
            flow_id,
            principal,
            purpose: "claim",
            path: &path,
            delivery_id_text: &request.result_delivery_id,
            result_cpk: &row.result_cpk,
            proof: &request.proof,
            now,
        },
    )
    .await
    .is_err()
    {
        return result_denied(flow_id, principal);
    }
    let sealed = match row.result_payload_sealed.as_deref() {
        Some(sealed) => sealed,
        None => return result_denied(flow_id, principal),
    };
    let result = match open_result(runtime, flow_id, &row.result_cpk, row.key_epoch, sealed) {
        Ok(result) => result,
        Err(_) => return result_denied(flow_id, principal),
    };
    let consumed = match sqlx::query(
        "UPDATE grant_flows SET result_claimed_at = $2, result_token_hash = NULL, \
         result_payload_sealed = NULL WHERE flow_id = $1 AND status = 'complete' \
         AND result_token_delivered_at IS NOT NULL AND result_claimed_at IS NULL",
    )
    .bind(flow_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result,
        Err(_) => return result_denied(flow_id, principal),
    };
    if consumed.rows_affected() != 1 || tx.commit().await.is_err() {
        return result_denied(flow_id, principal);
    }
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "capabilities": "",
                "expires_at": format_timestamp(result.session_expires_at),
                "pubky": row.expected_pubky,
                "token": URL_SAFE_NO_PAD.encode(result.bearer),
            })),
        )
            .into_response(),
    )
}

pub async fn cancel_flow(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let runtime = match grant_runtime(&state) {
        Some(runtime) => runtime,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable");
        }
    };
    let path = format!("/v1/auth/grant-flows/{flow_id}/cancel");
    let (principal, value) = match verify_service_request(runtime, &headers, &body, &path) {
        Ok(verified) => verified,
        Err(_) => {
            return error_response(StatusCode::UNAUTHORIZED, "invalid_service_signature");
        }
    };
    match admit_result_rates(&state, runtime, principal, flow_id).await {
        Ok(true) => {}
        Ok(false) => return error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        Err(_) => return error_response(StatusCode::SERVICE_UNAVAILABLE, "grant_unavailable"),
    }
    let request: CancelRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    if request.method != "POST"
        || request.path != path
        || canonical_uuid(&request.request_id).is_err()
    {
        return error_response(StatusCode::BAD_REQUEST, "invalid_request");
    }
    match consume_service_request(&state, principal, "cancel", &request.request_id).await {
        Ok(true) => {}
        Ok(false) => return result_denied(flow_id, principal),
        Err(_) => return result_denied(flow_id, principal),
    }
    let delivery_id = match decode_32("result_delivery_id", &request.result_delivery_id) {
        Ok(value) => value,
        Err(_) => return result_denied(flow_id, principal),
    };
    let row: Option<(String, Vec<u8>, i16)> = match sqlx::query_as(
        "SELECT status, result_delivery_id_hash, result_hash_epoch \
         FROM grant_flows WHERE flow_id = $1",
    )
    .bind(flow_id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(_) => return result_denied(flow_id, principal),
    };
    let Some((status, stored_hash, epoch)) = row else {
        return result_denied(flow_id, principal);
    };
    let expected = match runtime.result_hash(flow_id, epoch, &delivery_id, false) {
        Ok(hash) => hash,
        Err(_) => return result_denied(flow_id, principal),
    };
    if !fixed_digest_matches(&stored_hash, &expected) {
        return result_denied(flow_id, principal);
    }
    if status != "awaiting" {
        return error_response(StatusCode::CONFLICT, "flow_not_cancellable");
    }
    let now = state.clock.now();
    let updated = sqlx::query(
        "UPDATE grant_flows SET status = 'cancelled', terminal_code = 'cancelled', \
         terminal_at = $2, grant_state_sealed = NULL, result_payload_sealed = NULL, \
         result_token_hash = NULL WHERE flow_id = $1 AND status = 'awaiting'",
    )
    .bind(flow_id)
    .bind(now)
    .execute(&state.pool)
    .await;
    match updated {
        Ok(result) if result.rows_affected() == 1 => {
            no_store(StatusCode::NO_CONTENT.into_response())
        }
        _ => error_response(StatusCode::CONFLICT, "flow_not_cancellable"),
    }
}

#[derive(Debug, sqlx::FromRow)]
struct FlowLease {
    flow_id: Uuid,
    expected_pubky: String,
    client_id: String,
    cpk: String,
    grant_state_sealed: Vec<u8>,
    key_epoch: i16,
    result_hash_epoch: i16,
    result_cpk: String,
    lease_owner: Uuid,
    version: i64,
}

async fn acquire_flows(
    state: &AppState,
    runtime: &GrantRuntime,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<FlowLease>> {
    let lease_owner = Uuid::new_v4();
    let lease_until = now + Duration::seconds(runtime.config.verify_lease_seconds);
    let mut tx = state.pool.begin().await?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT flow_id FROM grant_flows \
         WHERE status = 'awaiting' AND expires_at > $1 \
         ORDER BY expires_at, created_at FOR UPDATE SKIP LOCKED LIMIT $2",
    )
    .bind(now)
    .bind(runtime.config.worker_batch_size)
    .fetch_all(&mut *tx)
    .await?;
    let mut leases = Vec::with_capacity(ids.len());
    for flow_id in ids {
        let lease: Option<FlowLease> = sqlx::query_as(
            "UPDATE grant_flows SET status = 'verifying', lease_owner = $2, \
             lease_until = $3, version = version + 1 \
             WHERE flow_id = $1 AND status = 'awaiting' AND expires_at > $4 \
             RETURNING flow_id, expected_pubky, client_id, cpk, grant_state_sealed, \
             key_epoch, result_hash_epoch, result_cpk, lease_owner, version",
        )
        .bind(flow_id)
        .bind(lease_owner)
        .bind(lease_until)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(lease) = lease {
            leases.push(lease);
        }
    }
    tx.commit().await?;
    Ok(leases)
}

async fn terminalize_owned(
    state: &AppState,
    lease: &FlowLease,
    status: &str,
    terminal_code: &str,
    approved_pubky: Option<&str>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE grant_flows SET status = $4, terminal_code = $5, approved_pubky = $6, \
         terminal_at = $7, grant_state_sealed = NULL, result_payload_sealed = NULL, \
         result_token_hash = NULL, lease_owner = NULL, lease_until = NULL \
         WHERE flow_id = $1 AND status = 'verifying' AND lease_owner = $2 AND version = $3",
    )
    .bind(lease.flow_id)
    .bind(lease.lease_owner)
    .bind(lease.version)
    .bind(status)
    .bind(terminal_code)
    .bind(approved_pubky)
    .bind(now)
    .execute(&state.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn return_owned_to_awaiting(state: &AppState, lease: &FlowLease) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE grant_flows SET status = 'awaiting', lease_owner = NULL, lease_until = NULL \
         WHERE flow_id = $1 AND status = 'verifying' AND lease_owner = $2 AND version = $3",
    )
    .bind(lease.flow_id)
    .bind(lease.lease_owner)
    .bind(lease.version)
    .execute(&state.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn complete_owned(
    state: &AppState,
    runtime: &GrantRuntime,
    lease: &FlowLease,
    approved_pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut bearer = [0u8; 32];
    let mut result_token = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bearer);
    rand::rngs::OsRng.fill_bytes(&mut result_token);
    let session_expires_at = now + Duration::seconds(runtime.config.session_ttl_seconds);
    let result_token_expires_at = now + Duration::seconds(60);
    let token_hash =
        runtime.result_hash(lease.flow_id, lease.result_hash_epoch, &result_token, true)?;
    let payload = SealedResult {
        version: GRANT_STATE_VERSION,
        bearer,
        result_token,
        session_expires_at,
    };
    let plaintext = canonical_json(&payload)?;
    let aad = result_aad(lease.flow_id, &lease.result_cpk, lease.key_epoch)?;
    let sealed = seal::seal(runtime.encryption_key(lease.key_epoch)?, &aad, &plaintext);

    let mut tx = state.pool.begin().await?;
    let owns: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM grant_flows WHERE flow_id = $1 \
         AND status = 'verifying' AND lease_owner = $2 AND version = $3 FOR UPDATE)",
    )
    .bind(lease.flow_id)
    .bind(lease.lease_owner)
    .bind(lease.version)
    .fetch_one(&mut *tx)
    .await?;
    if !owns {
        tx.rollback().await?;
        return Ok(false);
    }
    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1,$2,'',$3,$4) RETURNING session_id",
    )
    .bind(auth::hash_token(&bearer))
    .bind(approved_pubky)
    .bind(now)
    .bind(session_expires_at)
    .fetch_one(&mut *tx)
    .await?;
    let updated = sqlx::query(
        "UPDATE grant_flows SET status = 'complete', approved_pubky = $4, \
         terminal_at = $5, grant_state_sealed = NULL, result_token_hash = $6, \
         result_token_expires_at = $7, result_payload_sealed = $8, \
         result_auth_session_id = $9, lease_owner = NULL, lease_until = NULL \
         WHERE flow_id = $1 AND status = 'verifying' AND lease_owner = $2 AND version = $3",
    )
    .bind(lease.flow_id)
    .bind(lease.lease_owner)
    .bind(lease.version)
    .bind(approved_pubky)
    .bind(now)
    .bind(token_hash.as_slice())
    .bind(result_token_expires_at)
    .bind(sealed)
    .bind(session_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        tx.rollback().await?;
        return Ok(false);
    }
    tx.commit().await?;
    Ok(true)
}

async fn settle_verified(
    state: &AppState,
    runtime: &GrantRuntime,
    lease: &FlowLease,
    approved_pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    if approved_pubky != lease.expected_pubky {
        terminalize_owned(
            state,
            lease,
            "mismatch",
            "identity_mismatch",
            Some(approved_pubky),
            now,
        )
        .await
    } else {
        complete_owned(state, runtime, lease, approved_pubky, now).await
    }
}

async fn process_lease(
    state: &AppState,
    runtime: &GrantRuntime,
    lease: FlowLease,
) -> anyhow::Result<()> {
    let now = state.clock.now();
    let aad = state_aad(lease.flow_id, &lease.client_id, &lease.cpk, lease.key_epoch)?;
    let plaintext = match seal::open(
        runtime.encryption_key(lease.key_epoch)?,
        &aad,
        &lease.grant_state_sealed,
    ) {
        Ok(plaintext) => plaintext,
        Err(_) => {
            terminalize_owned(state, &lease, "failed", "storage_failure", None, now).await?;
            return Ok(());
        }
    };
    let stored: StoredGrantState = match serde_json::from_slice::<StoredGrantState>(&plaintext) {
        Ok(stored)
            if stored.version == GRANT_STATE_VERSION
                && canonical_json(&stored).is_ok_and(|canonical| canonical == plaintext) =>
        {
            stored
        }
        _ => {
            terminalize_owned(state, &lease, "failed", "storage_failure", None, now).await?;
            return Ok(());
        }
    };
    let flow = match PubkyGrantAuthFlow::restore(stored.state, runtime.client.clone()) {
        Ok(flow) => flow,
        Err(_) => {
            terminalize_owned(state, &lease, "invalid", "grant_invalid", None, now).await?;
            return Ok(());
        }
    };
    match flow.try_poll_once().await {
        Ok(None) => {
            return_owned_to_awaiting(state, &lease).await?;
        }
        Ok(Some(session)) => {
            let approved_pubky = session.public_key().z32();
            settle_verified(state, runtime, &lease, &approved_pubky, now).await?;
        }
        Err(error) => {
            let (status, code) = match error {
                pubky::Error::Authentication(_) | pubky::Error::Parse(_) => {
                    ("invalid", "grant_invalid")
                }
                pubky::Error::Pkarr(_) | pubky::Error::Request(_) => ("failed", "grant_exchange"),
                pubky::Error::Build(_) => ("failed", "relay_transport"),
            };
            terminalize_owned(state, &lease, status, code, None, now).await?;
        }
    }
    Ok(())
}

pub async fn poll_once(state: &AppState) -> anyhow::Result<u64> {
    let Some(runtime) = state.grant.as_ref() else {
        return Ok(0);
    };
    let leases = acquire_flows(state, runtime, state.clock.now()).await?;
    let count = leases.len() as u64;
    for lease in leases {
        if let Err(error) = process_lease(state, runtime, lease).await {
            tracing::error!(error = %error, "grant flow worker failed closed");
        }
    }
    Ok(count)
}

pub async fn reap_once(state: &AppState) -> anyhow::Result<u64> {
    let Some(runtime) = state.grant.as_ref() else {
        return Ok(0);
    };
    let now = state.clock.now();
    let batch = runtime.config.reaper_batch_size;
    let mut affected = 0u64;

    let expired = sqlx::query(
        "WITH due AS (SELECT flow_id FROM grant_flows WHERE status = 'awaiting' \
         AND expires_at <= $1 ORDER BY expires_at FOR UPDATE SKIP LOCKED LIMIT $2) \
         UPDATE grant_flows g SET status = 'expired', terminal_code = 'flow_expired', \
         terminal_at = $1, grant_state_sealed = NULL, result_payload_sealed = NULL, \
         result_token_hash = NULL FROM due WHERE g.flow_id = due.flow_id",
    )
    .bind(now)
    .bind(batch)
    .execute(&state.pool)
    .await?;
    affected += expired.rows_affected();

    let stale = sqlx::query(
        "WITH due AS (SELECT flow_id FROM grant_flows WHERE status = 'verifying' \
         AND lease_until <= $1 ORDER BY lease_until FOR UPDATE SKIP LOCKED LIMIT $2) \
         UPDATE grant_flows g SET status = 'failed', terminal_code = 'lease_lost', \
         terminal_at = $1, grant_state_sealed = NULL, result_payload_sealed = NULL, \
         result_token_hash = NULL, lease_owner = NULL, lease_until = NULL \
         FROM due WHERE g.flow_id = due.flow_id",
    )
    .bind(now)
    .bind(batch)
    .execute(&state.pool)
    .await?;
    affected += stale.rows_affected();

    let mut tx = state.pool.begin().await?;
    let result_rows: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT flow_id, result_auth_session_id FROM grant_flows \
         WHERE status = 'complete' AND result_claimed_at IS NULL \
           AND result_token_expires_at <= $1 \
           AND (result_payload_sealed IS NOT NULL OR result_auth_session_id IS NOT NULL) \
         ORDER BY result_token_expires_at FOR UPDATE SKIP LOCKED LIMIT $2",
    )
    .bind(now)
    .bind(batch)
    .fetch_all(&mut *tx)
    .await?;
    for (flow_id, session_id) in &result_rows {
        if let Some(session_id) = session_id {
            sqlx::query("DELETE FROM auth_sessions WHERE session_id = $1")
                .bind(session_id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "UPDATE grant_flows SET result_token_hash = NULL, result_payload_sealed = NULL, \
             result_token_expires_at = NULL, result_auth_session_id = NULL, \
             terminal_code = 'flow_expired' WHERE flow_id = $1",
        )
        .bind(flow_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    affected += result_rows.len() as u64;

    sqlx::query("DELETE FROM grant_result_nonces WHERE expires_at <= $1")
        .bind(now)
        .execute(&state.pool)
        .await?;
    sqlx::query("DELETE FROM grant_rate_limits WHERE window_started_at <= $1")
        .bind(now - Duration::seconds(120))
        .execute(&state.pool)
        .await?;
    sqlx::query("DELETE FROM grant_service_requests WHERE used_at <= $1")
        .bind(now - Duration::seconds(runtime.config.terminal_retention_seconds))
        .execute(&state.pool)
        .await?;
    let purged = sqlx::query(
        "WITH due AS (SELECT flow_id FROM grant_flows WHERE terminal_at <= $1 \
         ORDER BY terminal_at LIMIT $2) DELETE FROM grant_flows g USING due \
         WHERE g.flow_id = due.flow_id",
    )
    .bind(now - Duration::seconds(runtime.config.terminal_retention_seconds))
    .bind(batch)
    .execute(&state.pool)
    .await?;
    affected += purged.rows_affected();
    Ok(affected)
}

pub fn spawn(state: AppState) {
    let Some(runtime) = state.grant.clone() else {
        return;
    };
    tokio::spawn(async move {
        let mut reaper_ticks = 0u8;
        loop {
            tokio::select! {
                () = runtime.notify.notified() => {}
                () = tokio::time::sleep(std::time::Duration::from_millis(
                    runtime.config.relay_poll_milliseconds
                )) => {}
            }
            if let Err(error) = poll_once(&state).await {
                tracing::error!(error = %error, "grant relay scan failed closed");
            }
            reaper_ticks = reaper_ticks.saturating_add(1);
            if reaper_ticks >= 10 {
                if let Err(error) = reap_once(&state).await {
                    tracing::error!(error = %error, "grant reaper failed closed");
                }
                reaper_ticks = 0;
            }
        }
    });
}
