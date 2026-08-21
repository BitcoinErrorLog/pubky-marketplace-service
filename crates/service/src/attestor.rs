//! The marketplace attestor identity (ADR 0024, ratified D6).
//!
//! The attestor is a Pubky identity: an Ed25519 key whose z-base-32 encoding
//! is the attestor's pubky. It signs two kinds of compact JWS (RFC 7515,
//! `alg: EdDSA` per RFC 8037):
//!
//! - **Purchase attestations** issued inside the `review.create`
//!   transaction and embedded by the client in the published review record's
//!   `eligibilityAttestation` field. Durable, no expiry; verifiable offline
//!   by decoding `iss` from z-base-32 (the pubky *is* the verification key).
//! - **Seller stat attestations** computed weekly from the private order
//!   book (D3: median time-to-ship, dispute rate, completion rate) and
//!   signed for later publication on the attestor's own homeserver
//!   (publication is Phase 3 of the trust & reputation plan; this service
//!   currently signs and stores them).
//!
//! Claim privacy follows ADR 0019 §8 verbatim: no exact amounts, no
//! addresses, no payment identifiers, no bundle ids. `order_ref` is an
//! attestor-salted Blake3 hash of the private order UUID — nobody without
//! the salt can link it back to an order. `completed_on` is day-granularity.
//! The optional `amount_band` log-decade band is emitted only under
//! both-sides consent (ratified D2): the seller's standing preference AND
//! the reviewer's per-review opt-in.
//!
//! Key custody (ratified D6): the secret lives in the `ATTESTOR_SECRET_KEY`
//! environment variable (KMS/env-held, same process as the service), to be
//! revisited before real funds move. The salt (`ATTESTOR_ORDER_SALT`) must
//! stay stable for the life of the attestor identity — changing it silently
//! unlinks every previously issued `order_ref` from its annotations.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
use marketplace_domain::pubky::encode_pubky;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub const ENV_ATTESTOR_SECRET_KEY: &str = "ATTESTOR_SECRET_KEY";
pub const ENV_ATTESTOR_ORDER_SALT: &str = "ATTESTOR_ORDER_SALT";

/// JOSE `typ` of a v1 purchase attestation (spec fork: normative reference
/// in `pubky-app-specs` `marketplace_attestation.rs`).
pub const PURCHASE_ATTESTATION_TYP: &str = "pubky-purchase-attestation+v1";
/// JOSE `typ` of a v1 seller stat attestation.
pub const SELLER_STATS_TYP: &str = "pubky-seller-stats+v1";

pub struct Attestor {
    signing_key: SigningKey,
    pubky: String,
    order_salt: [u8; 32],
}

/// A freshly issued purchase attestation: the compact JWS plus its claims
/// (returned in the command result and stored for idempotent re-fetch).
#[derive(Debug, Clone)]
pub struct IssuedAttestation {
    pub jws: String,
    pub claims: Value,
    pub order_ref: String,
}

impl Attestor {
    /// Builds the attestor from `ATTESTOR_SECRET_KEY` and
    /// `ATTESTOR_ORDER_SALT` (both 64 hex chars). Fail closed on partial
    /// configuration: either both are set or attestation support is off.
    pub fn from_env() -> anyhow::Result<Option<Arc<Attestor>>> {
        let secret = std::env::var(ENV_ATTESTOR_SECRET_KEY).ok();
        let salt = std::env::var(ENV_ATTESTOR_ORDER_SALT).ok();
        match (secret, salt) {
            (None, None) => Ok(None),
            (Some(secret), Some(salt)) => Ok(Some(Arc::new(Self::from_hex(&secret, &salt)?))),
            _ => anyhow::bail!(
                "Attestation is partially configured: set both \
                 {ENV_ATTESTOR_SECRET_KEY} and {ENV_ATTESTOR_ORDER_SALT}, or neither"
            ),
        }
    }

    pub fn from_hex(secret_hex: &str, salt_hex: &str) -> anyhow::Result<Self> {
        let secret: [u8; 32] = hex::decode(secret_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                anyhow::anyhow!("{ENV_ATTESTOR_SECRET_KEY} must be 64 hex characters")
            })?;
        let order_salt: [u8; 32] = hex::decode(salt_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                anyhow::anyhow!("{ENV_ATTESTOR_ORDER_SALT} must be 64 hex characters")
            })?;
        let signing_key = SigningKey::from_bytes(&secret);
        let pubky = encode_pubky(signing_key.verifying_key().as_bytes());
        Ok(Self {
            signing_key,
            pubky,
            order_salt,
        })
    }

    /// The attestor's pubky: the z-base-32 encoding of its Ed25519
    /// verification key. Verifiers decode this to check signatures — no key
    /// server involved.
    pub fn pubky(&self) -> &str {
        &self.pubky
    }

    /// The opaque, attestor-salted reference for an order: lowercase hex
    /// Blake3 of the order UUID bytes concatenated with the private salt.
    /// Lets annotations and repeat-purchase attestations reference an order
    /// without exposing service identifiers (ADR 0019 §8).
    pub fn order_ref(&self, order_id: Uuid) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(order_id.as_bytes());
        hasher.update(&self.order_salt);
        hasher.finalize().to_hex().to_string()
    }

    /// Issues the durable purchase attestation for one (order, reviewer)
    /// pair. `role` is the record-vocabulary direction
    /// (`buyer_reviewing_seller` / `seller_reviewing_buyer`); `amount_band`
    /// must already have passed the D2 both-sides consent gate.
    #[allow(clippy::too_many_arguments)]
    pub fn issue_purchase_attestation(
        &self,
        order_id: Uuid,
        reviewer_pubky: &str,
        counterparty_pubky: &str,
        role: &str,
        listing_uri: &str,
        completed_on: &str,
        amount_band: Option<String>,
        now: DateTime<Utc>,
    ) -> IssuedAttestation {
        let order_ref = self.order_ref(order_id);
        // Claim order matches the design doc example (§5.3); serde_json is
        // built with preserve_order, so serialization is stable.
        let mut claims = json!({
            "v": 1,
            "iss": self.pubky,
            "sub": reviewer_pubky,
            "cpk": counterparty_pubky,
            "role": role,
            "listing": listing_uri,
            "order_ref": order_ref,
            "completed_on": completed_on,
        });
        if let Some(band) = amount_band {
            claims["amount_band"] = json!(band);
        }
        claims["iat"] = json!(now.timestamp());
        let jws = self.sign_compact(PURCHASE_ATTESTATION_TYP, &claims);
        IssuedAttestation {
            jws,
            claims,
            order_ref,
        }
    }

    /// Signs a seller stat attestation body (already banded/per-mille per
    /// design §7.2) as a compact JWS.
    pub fn sign_seller_stats(&self, body: &Value) -> String {
        self.sign_compact(SELLER_STATS_TYP, body)
    }

    fn sign_compact(&self, typ: &str, payload: &Value) -> String {
        let header = json!({ "alg": "EdDSA", "typ": typ });
        let header_b64 = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&header).expect("header serializes infallibly"));
        let payload_b64 = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(payload).expect("claims serialize infallibly"));
        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = self.signing_key.sign(signing_input.as_bytes());
        format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        )
    }
}

/// Maps the service's stored reviewer role (`buyer` / `seller`) to the
/// public record vocabulary the attestation claims use.
pub fn claim_role(reviewer_role: &str) -> &'static str {
    if reviewer_role == "buyer" {
        "buyer_reviewing_seller"
    } else {
        "seller_reviewing_buyer"
    }
}

/// The log-decade amount band for an order total in minor units, e.g.
/// 850_000 sats -> `SAT:5`. `None` for non-positive totals (nothing honest
/// to band).
pub fn amount_band(currency: &str, total_minor: i64) -> Option<String> {
    if total_minor < 1 {
        return None;
    }
    Some(format!("{currency}:{}", total_minor.ilog10()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    const SECRET: &str = "0707070707070707070707070707070707070707070707070707070707070707";
    const SALT: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn attestor() -> Attestor {
        Attestor::from_hex(SECRET, SALT).expect("test attestor builds")
    }

    /// Decodes a z-base-32 pubky back to key bytes (verifier-side recipe).
    fn decode_pubky(pubky: &str) -> [u8; 32] {
        const ALPHABET: &str = "ybndrfg8ejkmcpqxot1uwisza345h769";
        let mut accumulator: u64 = 0;
        let mut bits: u32 = 0;
        let mut out = Vec::new();
        for c in pubky.chars() {
            let value = ALPHABET.find(c).expect("z-base-32 char") as u64;
            accumulator = (accumulator << 5) | value;
            bits += 5;
            if bits >= 8 {
                bits -= 8;
                out.push((accumulator >> bits) as u8);
            }
        }
        out.truncate(32);
        out.try_into().expect("32 bytes")
    }

    #[test]
    fn pubky_round_trips_to_the_verifying_key() {
        let attestor = attestor();
        assert_eq!(attestor.pubky().len(), 52);
        let decoded = decode_pubky(attestor.pubky());
        assert_eq!(&decoded, attestor.signing_key.verifying_key().as_bytes());
    }

    #[test]
    fn order_ref_is_deterministic_and_salted() {
        let attestor = attestor();
        let order = Uuid::from_u128(42);
        assert_eq!(attestor.order_ref(order), attestor.order_ref(order));
        assert_eq!(attestor.order_ref(order).len(), 64);

        let other_salt = Attestor::from_hex(
            SECRET,
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        assert_ne!(attestor.order_ref(order), other_salt.order_ref(order));
    }

    #[test]
    fn issued_attestation_verifies_with_the_attestor_pubky() {
        let attestor = attestor();
        let now = "2026-08-21T12:00:00Z".parse().unwrap();
        let issued = attestor.issue_purchase_attestation(
            Uuid::from_u128(7),
            &"o".repeat(52),
            &"p".repeat(52),
            "buyer_reviewing_seller",
            "pubky://ppppp/pub/pubky.app/marketplace/v1/listings/x",
            "2026-08-20",
            Some("SAT:5".to_string()),
            now,
        );
        let mut parts = issued.jws.split('.');
        let header_b64 = parts.next().unwrap();
        let payload_b64 = parts.next().unwrap();
        let signature_b64 = parts.next().unwrap();
        assert!(parts.next().is_none());

        let header: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header_b64).unwrap()).unwrap();
        assert_eq!(header["alg"], "EdDSA");
        assert_eq!(header["typ"], PURCHASE_ATTESTATION_TYP);

        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload_b64).unwrap()).unwrap();
        assert_eq!(claims, issued.claims);
        assert_eq!(claims["order_ref"], json!(issued.order_ref));

        // Verifier recipe: decode iss -> Ed25519 key -> verify.
        let key = VerifyingKey::from_bytes(&decode_pubky(claims["iss"].as_str().unwrap()))
            .expect("valid key");
        let signature = Signature::from_bytes(
            &URL_SAFE_NO_PAD
                .decode(signature_b64)
                .unwrap()
                .try_into()
                .expect("64 bytes"),
        );
        key.verify(format!("{header_b64}.{payload_b64}").as_bytes(), &signature)
            .expect("signature verifies");
    }

    #[test]
    fn amount_band_is_a_log_decade() {
        assert_eq!(amount_band("SAT", 850_000), Some("SAT:5".to_string()));
        assert_eq!(amount_band("USD", 1), Some("USD:0".to_string()));
        assert_eq!(amount_band("USD", 99), Some("USD:1".to_string()));
        assert_eq!(amount_band("USD", 0), None);
    }

    #[test]
    fn claim_role_maps_the_stored_vocabulary() {
        assert_eq!(claim_role("buyer"), "buyer_reviewing_seller");
        assert_eq!(claim_role("seller"), "seller_reviewing_buyer");
    }

    #[test]
    fn partial_hex_configuration_is_rejected() {
        assert!(Attestor::from_hex("zz", SALT).is_err());
        assert!(Attestor::from_hex(SECRET, "short").is_err());
    }
}
