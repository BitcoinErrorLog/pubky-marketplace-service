//! Seller-configurable payment rails: config secrecy, Stripe verification,
//! and the signed Paykit client for physical bitcoin orders.
//!
//! Ownership decision (Task C): Stripe verification lives HERE, in the
//! transaction service, not in the fiat-verifier. The verifier is a
//! platform-credential gateway for Locks-guarded fiat criteria; per-seller
//! restricted keys belong with the per-seller payment config this service
//! owns, and the order state transition the verification drives must happen
//! in the same database transaction domain as the order itself. One owner,
//! one ledger.
//!
//! Secrecy rules mirror the Locks bundle-id handling: the Stripe restricted
//! key is sealed with XChaCha20-Poly1305 under `STRIPE_KEY_ENCRYPTION_KEY`
//! with the seller pubky as associated data, is never returned by any read,
//! and appears in outbound traffic only as the Authorization header of the
//! server-side Stripe API call.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use serde::Deserialize;

/// Environment variable holding the 32-byte hex key sealing Stripe
/// restricted keys at rest. Setting it enables the payment-methods surface.
pub const ENV_STRIPE_KEY_ENCRYPTION_KEY: &str = "STRIPE_KEY_ENCRYPTION_KEY";
/// Environment variable overriding the Stripe API base URL. Production
/// deployments leave it unset (`https://api.stripe.com`); tests point it at
/// a local double so the real HTTP client is exercised end to end.
pub const ENV_STRIPE_API_BASE: &str = "STRIPE_API_BASE";
/// Environment variable naming the paykit-server base URL. Setting it
/// enables the bitcoin method and makes the signing key mandatory.
pub const ENV_PAYKIT_SERVER_URL: &str = "PAYKIT_SERVER_URL";
/// Environment variable holding the 32-byte hex ed25519 seed whose public
/// key paykit-server trusts as `marketplace.trusted_public_key`.
pub const ENV_PAYKIT_REQUEST_SIGNING_KEY: &str = "PAYKIT_REQUEST_SIGNING_KEY";

const XNONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// How many 100-item Checkout Session pages a verification scans before
/// honestly reporting "not found". Payment Links have no server-side
/// `client_reference_id` filter, so recent sessions are listed and matched.
const STRIPE_MAX_PAGES: usize = 3;

/// Seals and opens Stripe restricted keys.
pub struct StripeKeyCipher {
    key: [u8; KEY_LEN],
}

impl std::fmt::Debug for StripeKeyCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StripeKeyCipher(<redacted>)")
    }
}

impl StripeKeyCipher {
    pub fn from_hex(hex_value: &str) -> anyhow::Result<Self> {
        let bytes = hex::decode(hex_value.trim()).map_err(|_| {
            anyhow::anyhow!("{ENV_STRIPE_KEY_ENCRYPTION_KEY} must be 64 hexadecimal characters")
        })?;
        let key = <[u8; KEY_LEN]>::try_from(bytes).map_err(|_| {
            anyhow::anyhow!("{ENV_STRIPE_KEY_ENCRYPTION_KEY} must decode to exactly 32 bytes")
        })?;
        Ok(Self { key })
    }

    /// Seals a restricted key: random 24-byte nonce followed by the
    /// ciphertext, with the seller pubky as associated data so ciphertexts
    /// cannot be transplanted between sellers.
    pub fn encrypt(&self, seller_pubky: &str, restricted_key: &str) -> Vec<u8> {
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        let mut nonce_bytes = [0u8; XNONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce_bytes),
                Payload {
                    msg: restricted_key.as_bytes(),
                    aad: seller_pubky.as_bytes(),
                },
            )
            .expect("XChaCha20-Poly1305 encryption is infallible for in-memory buffers");
        let mut sealed = Vec::with_capacity(XNONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(&nonce_bytes);
        sealed.extend_from_slice(&ciphertext);
        sealed
    }

    pub fn decrypt(&self, seller_pubky: &str, sealed: &[u8]) -> anyhow::Result<String> {
        if sealed.len() <= XNONCE_LEN {
            anyhow::bail!("sealed restricted key is too short");
        }
        let (nonce_bytes, ciphertext) = sealed.split_at(XNONCE_LEN);
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(nonce_bytes),
                Payload {
                    msg: ciphertext,
                    aad: seller_pubky.as_bytes(),
                },
            )
            .map_err(|_| {
                anyhow::anyhow!("restricted key ciphertext did not authenticate under this key")
            })?;
        String::from_utf8(plaintext)
            .map_err(|_| anyhow::anyhow!("restricted key is not valid UTF-8"))
    }
}

/// A matched, paid Stripe Checkout Session for an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeMatch {
    pub session_id: String,
}

/// How a Stripe verification attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StripeError {
    /// Stripe rejected the seller's restricted key (401/403): the seller
    /// must fix their configuration.
    KeyInvalid,
    /// Transport trouble or an unexpected Stripe response; retryable.
    Unavailable,
}

#[derive(Deserialize)]
struct StripeSessionList {
    data: Vec<StripeSession>,
    #[serde(default)]
    has_more: bool,
}

#[derive(Deserialize)]
struct StripeSession {
    id: String,
    #[serde(default)]
    client_reference_id: Option<String>,
    #[serde(default)]
    payment_status: Option<String>,
    #[serde(default)]
    amount_total: Option<i64>,
    #[serde(default)]
    currency: Option<String>,
}

/// The real Stripe API client. The trait seam used elsewhere in this
/// codebase is deliberately absent: tests exercise this exact client against
/// a local HTTP double (`STRIPE_API_BASE`), so header handling, pagination,
/// and status mapping are covered end to end.
pub struct StripeClient {
    base_url: String,
    http: reqwest::Client,
}

impl std::fmt::Debug for StripeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StripeClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl StripeClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            anyhow::bail!("{ENV_STRIPE_API_BASE} must be an http(s) URL");
        }
        Ok(Self {
            base_url,
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        })
    }

    /// Lists recent Checkout Sessions with the seller's restricted key and
    /// returns the first PAID session whose `client_reference_id` is this
    /// order and whose amount and currency match the order total exactly.
    pub async fn find_paid_session(
        &self,
        restricted_key: &str,
        order_id: &str,
        amount_minor: i64,
        currency: &str,
    ) -> Result<Option<StripeMatch>, StripeError> {
        let wanted_currency = currency.to_ascii_lowercase();
        let mut starting_after: Option<String> = None;
        for _ in 0..STRIPE_MAX_PAGES {
            let mut request = self
                .http
                .get(format!("{}/v1/checkout/sessions", self.base_url))
                .bearer_auth(restricted_key)
                .query(&[("limit", "100")]);
            if let Some(cursor) = &starting_after {
                request = request.query(&[("starting_after", cursor.as_str())]);
            }
            let response = request.send().await.map_err(|_| {
                tracing::warn!("stripe checkout session listing transport failure");
                StripeError::Unavailable
            })?;
            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::FORBIDDEN
            {
                return Err(StripeError::KeyInvalid);
            }
            if !status.is_success() {
                tracing::warn!(status = %status, "stripe checkout session listing rejected");
                return Err(StripeError::Unavailable);
            }
            let page: StripeSessionList = response.json().await.map_err(|_| {
                tracing::warn!("stripe checkout session listing returned a malformed body");
                StripeError::Unavailable
            })?;
            for session in &page.data {
                if session.client_reference_id.as_deref() == Some(order_id)
                    && session.payment_status.as_deref() == Some("paid")
                    && session.amount_total == Some(amount_minor)
                    && session.currency.as_deref() == Some(wanted_currency.as_str())
                {
                    return Ok(Some(StripeMatch {
                        session_id: session.id.clone(),
                    }));
                }
            }
            match (page.has_more, page.data.last()) {
                (true, Some(last)) => starting_after = Some(last.id.clone()),
                _ => break,
            }
        }
        Ok(None)
    }
}

/// Paykit payment-request status as this service consumes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitStatusOutcome {
    Undetected,
    Detected,
    /// Confirmed on-chain; `amount_matched` is paykit-server's own
    /// observation of the required satoshi amount.
    Confirmed {
        amount_matched: bool,
    },
    NotFound,
    Unavailable,
}

/// How a Paykit payment-request creation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitRequestError {
    /// The seller has no claimed watch-only account (or its homeserver
    /// session lapsed); the seller must (re-)claim through Paykit setup.
    SellerAccountUnavailable,
    /// The buyer has no Paykit receiver marker (no Bitkit wallet published
    /// one) or the request was otherwise refused.
    Rejected,
    /// paykit-server is unreachable or timed out; retryable.
    Unavailable,
}

/// The signed Paykit client: `x-paykit-signature` over the canonical JSON
/// body, verified by paykit-server against `marketplace.trusted_public_key`.
pub struct PaykitClient {
    base_url: String,
    signing_key: SigningKey,
    http: reqwest::Client,
}

impl std::fmt::Debug for PaykitClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaykitClient")
            .field("base_url", &self.base_url)
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

/// The canonical pubky-prefixed app-key form paykit-server's identifier
/// parsers require.
pub fn pubky_app_key(pubky: &str) -> String {
    format!("pubky{pubky}")
}

/// Crockford base32 (uppercase, no padding) of the order UUID's 16 bytes:
/// the same encoding `locks-core` bundle identifiers use, so the reference
/// is accepted verbatim as a `bundle_id` by paykit-server's status lookup.
pub fn order_reference(order_id: uuid::Uuid) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let bytes = order_id.as_bytes();
    let mut output = String::with_capacity(26);
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    for byte in bytes {
        buffer = (buffer << 8) | u64::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            output.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        output.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    output
}

impl PaykitClient {
    pub fn new(base_url: &str, signing_seed_hex: &str) -> anyhow::Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            anyhow::bail!("{ENV_PAYKIT_SERVER_URL} must be an http(s) URL");
        }
        let seed = hex::decode(signing_seed_hex.trim()).map_err(|_| {
            anyhow::anyhow!("{ENV_PAYKIT_REQUEST_SIGNING_KEY} must be 64 hexadecimal characters")
        })?;
        let seed = <[u8; 32]>::try_from(seed).map_err(|_| {
            anyhow::anyhow!("{ENV_PAYKIT_REQUEST_SIGNING_KEY} must decode to exactly 32 bytes")
        })?;
        Ok(Self {
            base_url,
            signing_key: SigningKey::from_bytes(&seed),
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        })
    }

    /// Whether the seller has a claimed watch-only account on paykit-server.
    pub async fn account_exists(&self, seller_pubky: &str) -> Result<bool, PaykitRequestError> {
        let response = self
            .http
            .get(format!(
                "{}/v0/accounts/{}",
                self.base_url,
                pubky_app_key(seller_pubky)
            ))
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        if !response.status().is_success() {
            return Err(PaykitRequestError::Unavailable);
        }
        #[derive(Deserialize)]
        struct Existence {
            claimed: bool,
        }
        response
            .json::<Existence>()
            .await
            .map(|existence| existence.claimed)
            .map_err(|_| PaykitRequestError::Unavailable)
    }

    fn signed_body(&self, value: &serde_json::Value) -> anyhow::Result<(String, String)> {
        let body = serde_json_canonicalizer::to_string(value)?;
        let signature = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            self.signing_key.sign(body.as_bytes()).to_bytes(),
        );
        Ok((body, signature))
    }

    /// Creates (or idempotently replays) the Paykit payment request for a
    /// physical bitcoin order.
    pub async fn create_payment_request(
        &self,
        seller_pubky: &str,
        buyer_pubky: &str,
        reference: &str,
        amount_sats: u64,
    ) -> Result<(), PaykitRequestError> {
        let (body, signature) = self
            .signed_body(&serde_json::json!({
                "amount_sats": amount_sats,
                "creator": pubky_app_key(seller_pubky),
                "reader": pubky_app_key(buyer_pubky),
                "reference": reference,
            }))
            .map_err(|_| PaykitRequestError::Rejected)?;
        let response = self
            .http
            .post(format!("{}/v0/payment-requests", self.base_url))
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        match code.as_str() {
            "creator_session_invalid" => Err(PaykitRequestError::SellerAccountUnavailable),
            "invalid_request" | "invoice_conflict" => Err(PaykitRequestError::Rejected),
            _ => Err(PaykitRequestError::Unavailable),
        }
    }

    /// Polls the payment status for one order reference.
    pub async fn payment_status(&self, seller_pubky: &str, reference: &str) -> PaykitStatusOutcome {
        let Ok((body, signature)) = self.signed_body(&serde_json::json!({
            "bundle_id": reference,
            "creator": pubky_app_key(seller_pubky),
        })) else {
            return PaykitStatusOutcome::Unavailable;
        };
        let response = self
            .http
            .post(format!("{}/transactions/status", self.base_url))
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tracing::warn!("paykit payment status transport failure");
                return PaykitStatusOutcome::Unavailable;
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return PaykitStatusOutcome::NotFound;
        }
        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), "paykit payment status rejected");
            return PaykitStatusOutcome::Unavailable;
        }
        #[derive(Deserialize)]
        struct StatusBody {
            status: String,
            amount_matched: bool,
        }
        match response.json::<StatusBody>().await {
            Ok(body) => match body.status.as_str() {
                "undetected" => PaykitStatusOutcome::Undetected,
                "detected" => PaykitStatusOutcome::Detected,
                "confirmed" => PaykitStatusOutcome::Confirmed {
                    amount_matched: body.amount_matched,
                },
                _ => PaykitStatusOutcome::Unavailable,
            },
            Err(_) => PaykitStatusOutcome::Unavailable,
        }
    }
}

/// The payment-methods runtime carried on [`crate::AppState`]: absent when
/// `STRIPE_KEY_ENCRYPTION_KEY` is unset (the whole surface is refused), with
/// the Paykit leg additionally gated on its own pair of variables.
pub struct PaymentsRuntime {
    pub stripe_key_cipher: StripeKeyCipher,
    pub stripe: StripeClient,
    pub paykit: Option<PaykitClient>,
}

impl std::fmt::Debug for PaymentsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PaymentsRuntime(<redacted>)")
    }
}

/// Builds the production runtime from the environment, failing closed:
/// `STRIPE_KEY_ENCRYPTION_KEY` enables the surface; `PAYKIT_SERVER_URL` and
/// `PAYKIT_REQUEST_SIGNING_KEY` must be set together or not at all.
pub fn payments_runtime_from_env() -> anyhow::Result<Option<Arc<PaymentsRuntime>>> {
    let Some(encryption_key) = std::env::var(ENV_STRIPE_KEY_ENCRYPTION_KEY).ok() else {
        let partial_paykit = std::env::var(ENV_PAYKIT_SERVER_URL).is_ok()
            || std::env::var(ENV_PAYKIT_REQUEST_SIGNING_KEY).is_ok();
        if partial_paykit {
            anyhow::bail!(
                "payment methods are partially configured: {ENV_PAYKIT_SERVER_URL} is set \
                 without {ENV_STRIPE_KEY_ENCRYPTION_KEY}"
            );
        }
        return Ok(None);
    };
    let stripe_key_cipher = StripeKeyCipher::from_hex(&encryption_key)?;
    let stripe_base =
        std::env::var(ENV_STRIPE_API_BASE).unwrap_or_else(|_| "https://api.stripe.com".to_string());
    let stripe = StripeClient::new(&stripe_base)?;
    let paykit = match (
        std::env::var(ENV_PAYKIT_SERVER_URL).ok(),
        std::env::var(ENV_PAYKIT_REQUEST_SIGNING_KEY).ok(),
    ) {
        (None, None) => None,
        (Some(url), Some(seed)) => Some(PaykitClient::new(&url, &seed)?),
        _ => anyhow::bail!(
            "Paykit is partially configured: set both {ENV_PAYKIT_SERVER_URL} and \
             {ENV_PAYKIT_REQUEST_SIGNING_KEY}, or neither"
        ),
    };
    Ok(Some(Arc::new(PaymentsRuntime {
        stripe_key_cipher,
        stripe,
        paykit,
    })))
}

/// The lifecycle poller boundary for tests mirrors the Locks pattern: the
/// worker consumes this trait so tests can drive every status outcome; the
/// production implementation is [`PaykitClient`] alone.
pub trait PaykitStatusSource: Send + Sync + 'static {
    fn status<'a>(
        &'a self,
        seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = PaykitStatusOutcome> + Send + 'a>>;
}

impl PaykitStatusSource for PaykitClient {
    fn status<'a>(
        &'a self,
        seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = PaykitStatusOutcome> + Send + 'a>> {
        Box::pin(self.payment_status(seller_pubky, reference))
    }
}

/// Validates a Stripe Payment Link URL: HTTPS on `buy.stripe.com` or
/// `book.stripe.com`, no credentials, no fragment.
pub fn validate_stripe_payment_link(value: &str) -> Result<(), &'static str> {
    let parsed: url::Url = value
        .parse()
        .map_err(|_| "stripe_payment_link must be a valid URL")?;
    if parsed.scheme() != "https" {
        return Err("stripe_payment_link must use https");
    }
    if !matches!(
        parsed.host_str(),
        Some("buy.stripe.com") | Some("book.stripe.com")
    ) {
        return Err("stripe_payment_link must be a buy.stripe.com or book.stripe.com URL");
    }
    if parsed.username() != "" || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err("stripe_payment_link must not carry credentials or fragments");
    }
    Ok(())
}

/// Validates the shape of a PayPal merchant email (single `@`, non-empty
/// local part, dotted domain, no whitespace or control characters).
pub fn validate_paypal_email(value: &str) -> Result<(), &'static str> {
    let error = "paypal_merchant_email must be a valid email address";
    if value.len() > 254 || value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(error);
    }
    let Some((local, domain)) = value.split_once('@') else {
        return Err(error);
    };
    if local.is_empty() || domain.is_empty() || !domain.contains('.') {
        return Err(error);
    }
    if domain.starts_with('.') || domain.ends_with('.') || domain.contains("..") {
        return Err(error);
    }
    Ok(())
}

/// Validates a Stripe restricted key: only `rk_`-prefixed keys are accepted
/// so full secret keys (`sk_`) are never stored.
pub fn validate_stripe_restricted_key(value: &str) -> Result<(), &'static str> {
    if !value.starts_with("rk_") {
        return Err("stripe_restricted_key must be a restricted key (rk_...)");
    }
    if value.len() < 12 || value.len() > 255 || !value.chars().all(|c| c.is_ascii_graphic()) {
        return Err("stripe_restricted_key is malformed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "3333333333333333333333333333333333333333333333333333333333333333";

    #[test]
    fn restricted_keys_round_trip_and_bind_the_seller() {
        let cipher = StripeKeyCipher::from_hex(KEY).unwrap();
        let sealed = cipher.encrypt(&"y".repeat(52), "rk_test_abc123456789");
        assert!(!sealed
            .windows(b"rk_test".len())
            .any(|window| window == b"rk_test"));
        assert_eq!(
            cipher.decrypt(&"y".repeat(52), &sealed).unwrap(),
            "rk_test_abc123456789"
        );
        cipher
            .decrypt(&"o".repeat(52), &sealed)
            .expect_err("a transplanted ciphertext must not decrypt");
        StripeKeyCipher::from_hex("abcd").expect_err("short key rejected");
    }

    #[test]
    fn order_references_are_canonical_crockford_bundle_identifiers() {
        // The all-zero UUID encodes to 26 zeros — the canonical fixture shape.
        assert_eq!(order_reference(uuid::Uuid::nil()), "0".repeat(26));
        let reference =
            order_reference(uuid::Uuid::parse_str("018f47d2-6a27-7c23-a49d-6b21bb770120").unwrap());
        assert_eq!(reference.len(), 26);
        assert!(reference
            .chars()
            .all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)));
        // Distinct UUIDs must map to distinct references.
        assert_ne!(reference, order_reference(uuid::Uuid::new_v4()));
    }

    #[test]
    fn payment_link_validation_accepts_stripe_hosted_links_only() {
        validate_stripe_payment_link("https://buy.stripe.com/test_abc").unwrap();
        validate_stripe_payment_link("https://book.stripe.com/abc").unwrap();
        for rejected in [
            "http://buy.stripe.com/test_abc",
            "https://evil.example/buy.stripe.com",
            "https://buy.stripe.com.evil.example/x",
            "not a url",
            "https://buy.stripe.com/x#fragment",
        ] {
            validate_stripe_payment_link(rejected).expect_err(rejected);
        }
    }

    #[test]
    fn paypal_email_validation_accepts_plain_addresses_only() {
        validate_paypal_email("merchant@example.com").unwrap();
        validate_paypal_email("a.b+c@sub.example.co").unwrap();
        for rejected in [
            "",
            "no-at-sign",
            "@example.com",
            "user@",
            "user@nodot",
            "user@ex..ample.com",
            "user name@example.com",
            "user@.example.com",
        ] {
            validate_paypal_email(rejected).expect_err(rejected);
        }
    }

    #[test]
    fn restricted_key_validation_refuses_secret_keys() {
        validate_stripe_restricted_key("rk_test_abc123456789").unwrap();
        validate_stripe_restricted_key("sk_test_abc123456789")
            .expect_err("secret keys are never stored");
        validate_stripe_restricted_key("rk_short").expect_err("too short");
    }
}
