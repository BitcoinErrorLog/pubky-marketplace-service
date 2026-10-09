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
use serde::{Deserialize, Serialize};

use crate::payment_attempt::UpstreamPrepared;

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
/// key paykit-server trusts: `marketplace.trusted_public_key` on the fork,
/// an entry of `[signed_services] trusted_public_keys` upstream.
pub const ENV_PAYKIT_REQUEST_SIGNING_KEY: &str = "PAYKIT_REQUEST_SIGNING_KEY";
/// Environment variable selecting the paykit-server API the client speaks:
/// `fork` (the default) or `upstream`. See [`PaykitApi`].
pub const ENV_PAYKIT_SERVER_API: &str = "PAYKIT_SERVER_API";
/// Environment variable overriding the Shippo API base URL. Production
/// leaves it unset (`https://api.goshippo.com`); tests point it at a local
/// double so the real HTTP client is exercised end to end.
pub const ENV_SHIPPO_API_BASE: &str = "SHIPPO_API_BASE";
/// Environment variable overriding PayPal's IPN validation endpoint.
/// Production leaves it unset (`https://ipnpb.paypal.com/cgi-bin/webscr`);
/// tests point it at a local double so the real postback client is
/// exercised end to end.
pub const ENV_PAYPAL_IPN_VERIFY_URL: &str = "PAYPAL_IPN_VERIFY_URL";

const XNONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const AVAILABILITY_HTTP_TIMEOUT: Duration = Duration::from_secs(3);
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

/// How a Shippo API call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShippoError {
    /// Shippo rejected the seller's API token (401/403).
    KeyInvalid,
    /// Shippo refused the request with actionable messages (bad address,
    /// unpurchasable rate, ...). The joined messages are seller-facing.
    Rejected(String),
    /// Shippo is unreachable or answered malformed; retryable.
    Unavailable,
}

pub(crate) struct ShippoDestination {
    name: String,
    street1: String,
    street2: String,
    city: String,
    state: String,
    zip: String,
    country: String,
    phone: String,
    email: String,
}

impl ShippoDestination {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: String,
        street1: String,
        street2: String,
        city: String,
        state: String,
        zip: String,
        country: String,
        phone: String,
        email: String,
    ) -> Self {
        Self {
            name,
            street1,
            street2,
            city,
            state,
            zip,
            country,
            phone,
            email,
        }
    }

    fn as_json(&self) -> serde_json::Value {
        let mut address = serde_json::json!({
            "name": self.name,
            "street1": self.street1,
            "city": self.city,
            "zip": self.zip,
            "country": self.country,
        });
        for (key, value) in [
            ("street2", self.street2.as_str()),
            ("state", self.state.as_str()),
            ("phone", self.phone.as_str()),
            ("email", self.email.as_str()),
        ] {
            if !value.is_empty() {
                address[key] = serde_json::Value::String(value.to_string());
            }
        }
        address
    }
}

/// One purchasable rate from a Shippo shipment quote.
#[derive(Debug, Clone, Serialize)]
pub struct ShippoRate {
    pub rate_id: String,
    pub provider: String,
    pub servicelevel: String,
    /// Decimal amount string exactly as Shippo quotes it (e.g. `"7.85"`).
    pub amount: String,
    pub currency: String,
    pub estimated_days: Option<i64>,
    pub duration_terms: Option<String>,
}

/// A purchased Shippo label.
#[derive(Debug, Clone, Serialize)]
pub struct ShippoLabel {
    pub transaction_id: String,
    pub carrier: String,
    pub servicelevel: String,
    pub amount: String,
    pub currency: String,
    pub tracking_number: String,
    pub tracking_url: Option<String>,
    pub label_url: String,
}

#[derive(Debug, Deserialize)]
struct ShippoRateWire {
    object_id: String,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    servicelevel: Option<ShippoServiceLevelWire>,
    #[serde(default)]
    amount: Option<String>,
    #[serde(default)]
    currency: Option<String>,
    #[serde(default)]
    estimated_days: Option<i64>,
    #[serde(default)]
    duration_terms: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ShippoServiceLevelWire {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ShippoMessageWire {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ShippoShipmentWire {
    #[serde(default)]
    rates: Vec<ShippoRateWire>,
    #[serde(default)]
    messages: Vec<ShippoMessageWire>,
}

#[derive(Debug, Deserialize)]
struct ShippoTransactionWire {
    object_id: String,
    status: String,
    #[serde(default)]
    tracking_number: Option<String>,
    #[serde(default)]
    tracking_url_provider: Option<String>,
    #[serde(default)]
    label_url: Option<String>,
    #[serde(default)]
    messages: Vec<ShippoMessageWire>,
}

impl ShippoRateWire {
    fn into_rate(self) -> ShippoRate {
        ShippoRate {
            rate_id: self.object_id,
            provider: self.provider.unwrap_or_default(),
            servicelevel: self
                .servicelevel
                .and_then(|level| level.name)
                .unwrap_or_default(),
            amount: self.amount.unwrap_or_default(),
            currency: self.currency.unwrap_or_default(),
            estimated_days: self.estimated_days,
            duration_terms: self.duration_terms,
        }
    }
}

fn joined_messages(messages: &[ShippoMessageWire]) -> String {
    let joined: Vec<&str> = messages
        .iter()
        .filter_map(|message| message.text.as_deref())
        .collect();
    if joined.is_empty() {
        "Shippo refused the request.".to_string()
    } else {
        joined.join(" ")
    }
}

/// The real Shippo API client, reached with each SELLER's own API token —
/// the service holds no platform shipping credentials (same trust shape as
/// the Stripe restricted key). Tests exercise this exact client against a
/// local double (`SHIPPO_API_BASE`).
pub struct ShippoClient {
    base_url: String,
    http: reqwest::Client,
}

impl std::fmt::Debug for ShippoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShippoClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl ShippoClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            anyhow::bail!("{ENV_SHIPPO_API_BASE} must be an http(s) URL");
        }
        Ok(Self {
            base_url,
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        })
    }

    async fn post(
        &self,
        api_key: &str,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<reqwest::Response, ShippoError> {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("ShippoToken {api_key}"),
            )
            .json(body)
            .send()
            .await
            .map_err(|_| {
                tracing::warn!("shippo transport failure");
                ShippoError::Unavailable
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(ShippoError::KeyInvalid);
        }
        Ok(response)
    }

    /// Quotes a synchronous shipment and returns its purchasable rates.
    pub(crate) async fn shipment_rates(
        &self,
        api_key: &str,
        address_from: &serde_json::Value,
        address_to: &ShippoDestination,
        parcel: &serde_json::Value,
    ) -> Result<Vec<ShippoRate>, ShippoError> {
        let body = serde_json::json!({
            "address_from": address_from,
            "address_to": address_to.as_json(),
            "parcels": [parcel],
            "async": false,
        });
        let response = self.post(api_key, "/shipments/", &body).await?;
        let status = response.status();
        let shipment: ShippoShipmentWire = response.json().await.map_err(|_| {
            tracing::warn!("shippo shipment response was malformed");
            ShippoError::Unavailable
        })?;
        if !status.is_success() {
            return Err(ShippoError::Rejected(joined_messages(&shipment.messages)));
        }
        let rates: Vec<ShippoRate> = shipment
            .rates
            .into_iter()
            .map(ShippoRateWire::into_rate)
            .collect();
        if rates.is_empty() {
            return Err(ShippoError::Rejected(joined_messages(&shipment.messages)));
        }
        Ok(rates)
    }

    /// Purchases a label for a previously quoted rate. The rate is re-read
    /// first so the stored label carries the carrier and price the seller
    /// actually bought.
    pub async fn purchase_label(
        &self,
        api_key: &str,
        rate_id: &str,
    ) -> Result<ShippoLabel, ShippoError> {
        let rate_response = self
            .http
            .get(format!("{}/rates/{rate_id}", self.base_url))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("ShippoToken {api_key}"),
            )
            .send()
            .await
            .map_err(|_| ShippoError::Unavailable)?;
        let rate_status = rate_response.status();
        if rate_status == reqwest::StatusCode::UNAUTHORIZED
            || rate_status == reqwest::StatusCode::FORBIDDEN
        {
            return Err(ShippoError::KeyInvalid);
        }
        if rate_status == reqwest::StatusCode::NOT_FOUND {
            return Err(ShippoError::Rejected(
                "The selected rate no longer exists; quote rates again.".to_string(),
            ));
        }
        if !rate_status.is_success() {
            return Err(ShippoError::Unavailable);
        }
        let rate: ShippoRateWire = rate_response
            .json()
            .await
            .map_err(|_| ShippoError::Unavailable)?;
        let rate = rate.into_rate();

        let body = serde_json::json!({
            "rate": rate_id,
            "label_file_type": "PDF",
            "async": false,
        });
        let response = self.post(api_key, "/transactions/", &body).await?;
        let status = response.status();
        let transaction: ShippoTransactionWire = response.json().await.map_err(|_| {
            tracing::warn!("shippo transaction response was malformed");
            ShippoError::Unavailable
        })?;
        if !status.is_success() || transaction.status != "SUCCESS" {
            return Err(ShippoError::Rejected(joined_messages(
                &transaction.messages,
            )));
        }
        let (Some(tracking_number), Some(label_url)) =
            (transaction.tracking_number, transaction.label_url)
        else {
            tracing::warn!("shippo SUCCESS transaction lacked tracking or label url");
            return Err(ShippoError::Unavailable);
        };
        Ok(ShippoLabel {
            transaction_id: transaction.object_id,
            carrier: rate.provider,
            servicelevel: rate.servicelevel,
            amount: rate.amount,
            currency: rate.currency,
            tracking_number,
            tracking_url: transaction.tracking_url_provider,
            label_url,
        })
    }
}

/// The outcome of echoing an IPN message back to PayPal for validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpnVerdict {
    /// PayPal answered `VERIFIED`: the message is authentic.
    Verified,
    /// PayPal answered `INVALID` (or anything else): the message is not
    /// PayPal's and must be dropped.
    Invalid,
    /// PayPal could not be reached; the caller answers non-2xx so PayPal
    /// retries the notification later.
    Unavailable,
}

/// The PayPal IPN postback client: authenticity of an inbound notification
/// is established the only way the protocol offers — echoing the exact
/// received body back to PayPal prefixed with `cmd=_notify-validate` and
/// trusting only a literal `VERIFIED` answer. No seller credentials are
/// involved.
pub struct PaypalIpnVerifier {
    verify_url: String,
    http: reqwest::Client,
}

impl std::fmt::Debug for PaypalIpnVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaypalIpnVerifier")
            .field("verify_url", &self.verify_url)
            .finish()
    }
}

impl PaypalIpnVerifier {
    pub fn new(verify_url: &str) -> anyhow::Result<Self> {
        let verify_url = verify_url.trim_end_matches('/').to_string();
        if !verify_url.starts_with("http://") && !verify_url.starts_with("https://") {
            anyhow::bail!("{ENV_PAYPAL_IPN_VERIFY_URL} must be an http(s) URL");
        }
        Ok(Self {
            verify_url,
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        })
    }

    pub async fn verify(&self, raw_body: &str) -> IpnVerdict {
        let postback = format!("cmd=_notify-validate&{raw_body}");
        let response = self
            .http
            .post(&self.verify_url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(postback)
            .send()
            .await;
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::warn!(status = %response.status(), "paypal ipn postback rejected");
                return IpnVerdict::Unavailable;
            }
            Err(_) => {
                tracing::warn!("paypal ipn postback transport failure");
                return IpnVerdict::Unavailable;
            }
        };
        match response.text().await {
            Ok(text) if text.trim() == "VERIFIED" => IpnVerdict::Verified,
            Ok(_) => IpnVerdict::Invalid,
            Err(_) => {
                tracing::warn!("paypal ipn postback returned an unreadable body");
                IpnVerdict::Unavailable
            }
        }
    }
}

/// The rail-wide Bitcoin offer gate from a `/health/ready` body.
///
/// - The fork publishes `bitcoin_offer_available`; when present it is the
///   answer.
/// - Upstream paykit-server (and the fork before Hop 1) publishes
///   `{status, postgres, electrum, paykit_delivery, outbox}` with string
///   components. Only `status == "ready"` permits new Bitcoin offers there;
///   upstream's `status` is `ready` only when every component is.
/// - The fork's Hop 1 rollout body carries an `electrum` object and no
///   aggregate gate; its `state` decides.
///
/// Anything else is `false`.
pub fn rail_offer_available(body: &serde_json::Value) -> bool {
    body.get("bitcoin_offer_available")
        .and_then(serde_json::Value::as_bool)
        .or_else(|| {
            let status = body.get("status")?.as_str()?;
            let electrum = body.get("electrum")?.as_str()?;
            Some(status == "ready" && electrum == "ready")
        })
        .or_else(|| {
            body.get("electrum").and_then(|electrum| match electrum {
                serde_json::Value::String(state) => Some(state == "ready"),
                serde_json::Value::Object(object) => object
                    .get("state")
                    .and_then(serde_json::Value::as_str)
                    .map(|state| state == "ready"),
                _ => None,
            })
        })
        .unwrap_or(false)
}

/// The strict transaction-status contract paykit-server emits
/// (`paykit.bitcoin_status/v2`, W1.14). The consumer fails CLOSED: a
/// missing/wrong `contract_version`, a missing/unknown `allocation_mode`,
/// an unknown status, or a missing/mistyped mandatory `late_settlement`
/// boolean is never an automatic transition input.
pub const PAYKIT_STATUS_CONTRACT: &str = "paykit.bitcoin_status/v2";

/// The observation facts paykit-server reported for one status poll:
/// the outpoint txid, the observed satoshi total, and the confirmation
/// depth at that instant. Every field is optional on the wire (an older
/// paykit-server reports none of them); what is present is frozen into the
/// order's live observation and, at seller confirmation, into the audit
/// record — never supplied by any peer (design §B.8.8).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct PaykitObservation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_sats: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmations: Option<u32>,
}

/// The validated per-poll facts: the creator's CURRENT allocation mode —
/// which decides automatic versus seller-confirmed handling (§B.11.4 A3)
/// — the producer's mandatory late-settlement flag, plus the reported
/// observation. `late_settlement` is the producer's authoritative
/// statement that the settlement landed outside the invoice's settlement
/// window: a late observation NEVER auto-pays and NEVER enters
/// `awaiting_seller_confirmation`; a confirmed one routes to durable
/// `manual_review`. None of this applies to an order already inside
/// `awaiting_seller_confirmation`: whatever mode, lateness, or amount a
/// later report carries, it only refreshes the facts, and the seller or
/// the seller-window reaper decides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PaykitStatusFacts {
    pub allocation_mode: String,
    pub late_settlement: bool,
    pub observation: PaykitObservation,
}

/// Whether the payment request reached the buyer's wallet, normalized from
/// paykit-server's `paykit_delivery_state`. `cancelled` and
/// `contract_error` are not delivery facts about a live request and map to
/// nothing (the stored state stays).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitDeliveryState {
    /// Paykit is still establishing the Encrypted Link or sending.
    Pending,
    /// The request and endpoint were sent over the Encrypted Link.
    Delivered,
    /// Paykit gave up delivering (for example the wallet never answered
    /// the link).
    Failed,
}

impl PaykitDeliveryState {
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "pending_delivery" => Some(Self::Pending),
            "delivered" => Some(Self::Delivered),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }
}

/// Paykit payment-request status as this service consumes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaykitStatusOutcome {
    Undetected,
    Detected {
        facts: PaykitStatusFacts,
    },
    /// Confirmed on-chain; `amount_matched` is paykit-server's own
    /// observation of the required satoshi amount.
    Confirmed {
        amount_matched: bool,
        facts: PaykitStatusFacts,
    },
    NotFound,
    Unavailable,
}

/// Reads upstream paykit-server's `/transactions/status` body, which is
/// exactly `{status, confirmations, amount_matched}`. `None` means the bytes
/// are not that shape, and the caller fails closed as before.
///
/// Upstream reports no late-settlement flag and no allocation mode, and every
/// automatic transition on a detected or confirmed payment depends on both.
/// Only `undetected` is therefore an outcome; `detected` and `confirmed` fail
/// closed as `Unavailable` until upstream defines those facts.
pub fn upstream_payment_status(bytes: &[u8]) -> Option<PaykitStatusOutcome> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct UpstreamStatusBody {
        status: String,
        #[allow(dead_code)]
        confirmations: u32,
        #[allow(dead_code)]
        amount_matched: bool,
    }
    let body = serde_json::from_slice::<UpstreamStatusBody>(bytes).ok()?;
    match body.status.as_str() {
        "undetected" => Some(PaykitStatusOutcome::Undetected),
        "detected" | "confirmed" => {
            tracing::warn!(
                status = %body.status,
                "upstream paykit payment status carries no late-settlement flag or \
                 allocation mode; failing closed"
            );
            Some(PaykitStatusOutcome::Unavailable)
        }
        _ => None,
    }
}

/// How a Paykit payment-request creation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaykitRequestError {
    /// The seller has no claimed watch-only account (or its homeserver
    /// session lapsed); the seller must (re-)claim through Paykit setup.
    SellerAccountUnavailable,
    /// The request was refused, or the 200 body violated the two-phase
    /// contract shape (missing field, a legacy 204).
    Rejected,
    /// The buyer publishes no Paykit receiver that takes payment requests
    /// (`reader_not_payable`): they must connect a Paykit wallet such as
    /// Bitkit. Not retryable as is.
    ReaderNotPayable,
    /// The buyer has no Paykit App Registry yet (`503 reader_setup_pending`,
    /// sent without `Retry-After` after paykit-server's own bounded reads):
    /// they must finish setting up a Paykit wallet. Retried only when the
    /// buyer acts, never automatically.
    ReaderSetupPending,
    /// Phase 1 returned `total_sats != amount_sats + nonce_sats`: the two
    /// services disagree about money. Refused, alerted, never retried.
    TotalInconsistent,
    /// paykit-server is unreachable or timed out; retryable.
    Unavailable,
}

/// The phase-1 prepared invoice, parsed from the verbatim §B.11.3 200 body.
/// Every field is required: a body missing any of them is a contract-shape
/// violation and the bind is refused.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaykitPrepared {
    pub invoice_id: uuid::Uuid,
    pub state: String,
    pub stack_id: String,
    pub allocation_mode: String,
    pub nonce_sats: u64,
    pub total_sats: u64,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub prepare_expires_at: chrono::DateTime<chrono::Utc>,
    pub derived_address_fingerprint: String,
}

/// The phase-2 activation 200 body (`state` is `"observing"`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaykitActivated {
    pub invoice_id: uuid::Uuid,
    pub state: String,
    pub activated_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub total_sats: u64,
}

/// The void 200 body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaykitVoided {
    pub invoice_id: uuid::Uuid,
    pub state: String,
    pub voided_at: chrono::DateTime<chrono::Utc>,
}

/// The resolve success body (§B.9): the recorded resolution, echoing the
/// invoice, the resolution, and the instant, with the finalized state
/// (`resolved_paid_manually` / `resolved_closed`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaykitResolved {
    pub invoice_id: uuid::Uuid,
    pub resolution: String,
    pub resolved_at: chrono::DateTime<chrono::Utc>,
    pub state: String,
}

/// The verbatim outcome of one signed `resolve` call, for the delivery
/// arm's response-class mapping (§B.8.8). Every HTTP status — success,
/// named refusal, or unmapped — reaches the caller; only a transport
/// failure (connect/TLS/timeout) is an `Err`.
#[derive(Debug)]
pub struct PaykitResolveResponse {
    pub status: reqwest::StatusCode,
    /// The application error code from an `{"error": {"code": ...}}`
    /// envelope, when one was sent.
    pub code: Option<String>,
    /// The raw `Retry-After` header, when present (delay-seconds or
    /// HTTP-date; parsed by the caller, malformed falls back to backoff).
    pub retry_after: Option<String>,
    /// The parsed success body on a 2xx (None when the body violates the
    /// contract shape — the caller treats that as a malformed success).
    pub resolved: Option<PaykitResolved>,
}

/// How a signed `activate` or `void` call failed (§B.11.3 named errors).
/// The terminal variants are per-contract final; `Unavailable` is the only
/// retryable outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaykitCommandError {
    /// 409: the prepare TTL reaped the invoice. Void the bind.
    PrepareExpired,
    /// 409: the invoice is in a finalized state. On `activate`: void the
    /// bind; on `void` after a lost 2xx it means the invoice activated.
    InvoiceFinalized,
    /// 404: no such invoice on this stack — a stack mixup; alert.
    UnknownInvoice,
    /// 409: the echoed `total_sats` disagrees with paykit's stored total.
    ActivationTotalMismatch,
    /// 409: the echoed `stack_id` is not this stack's identity.
    StackIdentityMismatch,
    /// Any other refusal (unknown contract state): terminal, alert.
    UnexpectedRejection(String),
    /// 5xx, transport error, timeout, or a malformed success body:
    /// retryable under the ordinary lease.
    Unavailable,
}

impl PaykitCommandError {
    fn from_status(status: reqwest::StatusCode, code: String) -> Self {
        match (status.as_u16(), code.as_str()) {
            (409, "prepare_expired") => PaykitCommandError::PrepareExpired,
            (409, "invoice_finalized") => PaykitCommandError::InvoiceFinalized,
            (404, "unknown_invoice") => PaykitCommandError::UnknownInvoice,
            (409, "activation_total_mismatch") => PaykitCommandError::ActivationTotalMismatch,
            (409, "stack_identity_mismatch") => PaykitCommandError::StackIdentityMismatch,
            _ if status.is_server_error() => PaykitCommandError::Unavailable,
            _ if status.is_client_error() => PaykitCommandError::UnexpectedRejection(code),
            _ => PaykitCommandError::Unavailable,
        }
    }
}

/// Which paykit-server API the client speaks. One setting decides both the
/// signature preimage and the seller-readiness call, so a cutover (and its
/// rollback) is a single configuration change.
///
/// - `Fork` (default): `x-paykit-signature` signs the canonical JSON body
///   alone, and seller readiness is the public `GET /v0/accounts/{creator}`.
/// - `Upstream` (`pubky/paykit-server` rc9 and #55): the signature covers
///   [`paykit_signature_preimage`] (method, path and body), and seller
///   readiness is the signed `POST /setup/status`.
///
/// Under `Upstream` the Bitcoin bind prepares through
/// [`PaykitClient::prepare_marketplace_payment`]. Activate, void and resolve
/// are fork routes until the later upstream slices land: they are signed like
/// every other request, but a prepared upstream attempt is not yet
/// activated (see [`crate::payment_attempt`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PaykitApi {
    #[default]
    Fork,
    Upstream,
}

impl PaykitApi {
    /// Parses the [`ENV_PAYKIT_SERVER_API`] value.
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim() {
            "fork" => Ok(Self::Fork),
            "upstream" => Ok(Self::Upstream),
            _ => anyhow::bail!("{ENV_PAYKIT_SERVER_API} must be `fork` or `upstream`"),
        }
    }
}

/// The bytes paykit-server verifies the `x-paykit-signature` over:
/// `"paykit-http-signature-v1\0" + METHOD + "\0" + path + "\0" + raw_body`,
/// with the method upper-cased and `path` the query-free request path
/// exactly as the server receives it.
pub fn paykit_signature_preimage(method: &str, path: &str, raw_body: &[u8]) -> Vec<u8> {
    const DOMAIN: &[u8] = b"paykit-http-signature-v1\0";
    let method = method.to_ascii_uppercase();
    let mut preimage =
        Vec::with_capacity(DOMAIN.len() + method.len() + 1 + path.len() + 1 + raw_body.len());
    preimage.extend_from_slice(DOMAIN);
    preimage.extend_from_slice(method.as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(path.as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(raw_body);
    preimage
}

/// The signed Paykit client: `x-paykit-signature` verified by paykit-server
/// against its trusted service key. What the signature covers depends on
/// the configured [`PaykitApi`].
pub struct PaykitClient {
    base_url: String,
    signing_key: SigningKey,
    api: PaykitApi,
    http: reqwest::Client,
}

impl std::fmt::Debug for PaykitClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaykitClient")
            .field("base_url", &self.base_url)
            .field("api", &self.api)
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

/// The upstream preparation route (`pubky/paykit-server` #66).
pub const UPSTREAM_PREPARE_PATH: &str = "/marketplace/payment-requests/prepare";

/// Maps one refusal of the upstream preparation route to the bind's error
/// classes. Only the codes #66 can answer are named; anything else is an
/// outage the buyer may retry.
///
/// - `409 operation_conflict`: the operation was prepared with a different
///   binding. The service derives every field from its own attempt, so this
///   is a bug here, never a buyer error: refused and alerted.
/// - `401 invalid_signature`: the signing key is not in paykit-server's
///   `[signed_services] trusted_public_keys`. Nothing the buyer can fix:
///   alerted, and shown as an outage.
/// - `409 creator_session_invalid` and `503 seller_setup_pending` (the seller
///   has no Bitcoin receiving details): the seller must (re)do Paykit setup,
///   the outcome the fork gives a seller without a claimed account.
/// - `400 invalid_request`: a malformed request or an over-cap window, never
///   a buyer or seller state.
/// - `409 reader_not_payable` and `503 reader_setup_pending`: the buyer's
///   wallet cannot pay, or is not set up yet.
/// - `503 dependency_timeout`: the request outlived paykit-server's deadline.
///   The preparation may still have committed; that is harmless, because it
///   is unpublished and lapses at its activation deadline, and the buyer's
///   retry is a new bind attempt (a new operation id), never a replay.
pub fn upstream_prepare_error(status: reqwest::StatusCode, code: &str) -> PaykitRequestError {
    match (status.as_u16(), code) {
        (409, "operation_conflict") => {
            tracing::error!(
                "ALERT paykit prepare refused a changed binding for one operation; \
                 the service derived a different request for the same attempt"
            );
            PaykitRequestError::Rejected
        }
        (401, "invalid_signature") => {
            tracing::error!(
                "ALERT paykit prepare rejected the request signature; the service key is not \
                 in paykit-server's signed_services trusted_public_keys"
            );
            PaykitRequestError::Unavailable
        }
        (409, "creator_session_invalid") | (503, "seller_setup_pending") => {
            PaykitRequestError::SellerAccountUnavailable
        }
        (400, "invalid_request") => PaykitRequestError::Rejected,
        (409, "reader_not_payable") => PaykitRequestError::ReaderNotPayable,
        (503, "reader_setup_pending") => PaykitRequestError::ReaderSetupPending,
        _ => PaykitRequestError::Unavailable,
    }
}

/// The canonical pubky-prefixed app-key form paykit-server's identifier
/// parsers require.
pub fn pubky_app_key(pubky: &str) -> String {
    format!("pubky{pubky}")
}

/// The Paykit `reference` of one bind attempt on an order: Crockford base32
/// of the first 16 bytes of SHA-256 over a domain tag, the order UUID, and
/// the attempt number. paykit-server holds one invoice per
/// `(creator, reference)` and refuses any other binding against it, so each
/// attempt needs its own reference; the same `(order, attempt)` always
/// derives the same one, so a transport retry of an attempt replays it.
pub fn attempt_reference(order_id: uuid::Uuid, attempt: i32) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::new()
        .chain_update(b"pubky-marketplace/paykit-attempt-reference/v1")
        .chain_update(order_id.as_bytes())
        .chain_update(attempt.to_be_bytes())
        .finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    crockford_bundle_id(&bytes)
}

/// The single per-order reference every bind used before per-attempt
/// references: Crockford base32 of the order UUID. Attempts released
/// under it are polled by it.
pub fn legacy_order_reference(order_id: uuid::Uuid) -> String {
    crockford_bundle_id(order_id.as_bytes())
}

/// Crockford base32 (uppercase, no padding) of 16 bytes: the encoding
/// `locks-core` bundle identifiers use, so the value is accepted verbatim
/// as a `bundle_id` by paykit-server.
fn crockford_bundle_id(bytes: &[u8; 16]) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
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
            api: PaykitApi::Fork,
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
        })
    }

    /// Selects the paykit-server API this client speaks (default
    /// [`PaykitApi::Fork`]).
    pub fn with_api(mut self, api: PaykitApi) -> Self {
        self.api = api;
        self
    }

    /// Whether the seller can receive Bitcoin right now: the only input to
    /// `bitcoin_available`. `Ok(true)` only for a ready seller; `Ok(false)`
    /// when the seller has to (re)do Paykit setup; `Err(Unavailable)` when
    /// paykit-server cannot say, so the availability cache retries and serves
    /// the last known value until it goes stale. Nothing here ever starts a
    /// Paykit authorization flow.
    pub async fn seller_ready(&self, seller_pubky: &str) -> Result<bool, PaykitRequestError> {
        match self.api {
            PaykitApi::Fork => self.fork_account_claimed(seller_pubky).await,
            PaykitApi::Upstream => self.upstream_setup_ready(seller_pubky).await,
        }
    }

    /// Upstream seller readiness: signed `POST /setup/status` for BTC. Only
    /// `ready` permits Bitcoin offers; `setup_required` means the seller
    /// must set up Paykit; `unavailable`, a non-2xx answer, a transport
    /// failure and any body outside the closed `{"status": ...}` contract
    /// are all retryable outages.
    async fn upstream_setup_ready(&self, seller_pubky: &str) -> Result<bool, PaykitRequestError> {
        let url = format!("{}/setup/status", self.base_url);
        let (body, signature) = self
            .signed_body(
                &url,
                &serde_json::json!({
                    "asset": "BTC",
                    "creator": pubky_app_key(seller_pubky),
                }),
            )
            .map_err(|error| {
                tracing::error!(error = %error, "paykit setup status request could not be signed");
                PaykitRequestError::Unavailable
            })?;
        let response = self
            .http
            .post(url)
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), "paykit setup status rejected");
            return Err(PaykitRequestError::Unavailable);
        }
        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        let status = body
            .as_object()
            .filter(|object| object.len() == 1)
            .and_then(|object| object.get("status"))
            .and_then(serde_json::Value::as_str);
        match status {
            Some("ready") => Ok(true),
            Some("setup_required") => Ok(false),
            Some("unavailable") => Err(PaykitRequestError::Unavailable),
            _ => {
                tracing::warn!("paykit setup status answered outside its contract");
                Err(PaykitRequestError::Unavailable)
            }
        }
    }

    /// Fork seller readiness: whether the seller has a claimed watch-only
    /// account (`GET /v0/accounts/{creator}`).
    async fn fork_account_claimed(&self, seller_pubky: &str) -> Result<bool, PaykitRequestError> {
        let response = self
            .http
            .get(format!(
                "{}/v0/accounts/{}",
                self.base_url,
                pubky_app_key(seller_pubky)
            ))
            .timeout(AVAILABILITY_HTTP_TIMEOUT)
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

    /// Reads Paykit's rail-wide Bitcoin offer gate (see
    /// [`rail_offer_available`] for the accepted readiness shapes). It is
    /// the public, unsigned `GET /health/ready`, which fork and upstream
    /// both serve, so it does not depend on [`PaykitApi`].
    pub async fn rail_health(&self) -> Result<bool, PaykitRequestError> {
        let response = self
            .http
            .get(format!("{}/health/ready", self.base_url))
            .timeout(AVAILABILITY_HTTP_TIMEOUT)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        if !response.status().is_success() {
            return Err(PaykitRequestError::Unavailable);
        }
        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        Ok(rail_offer_available(&body))
    }

    /// Canonicalizes `value` and signs it for a `POST` to `url`. The fork
    /// signs the body alone; upstream signs the request preimage, whose path
    /// is the URL's path (including any base-URL prefix the server sees).
    fn signed_body(
        &self,
        url: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<(String, String)> {
        let body = serde_json_canonicalizer::to_string(value)?;
        let message = match self.api {
            PaykitApi::Fork => body.as_bytes().to_vec(),
            PaykitApi::Upstream => {
                let url = url::Url::parse(url)?;
                paykit_signature_preimage("POST", url.path(), body.as_bytes())
            }
        };
        let signature = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            self.signing_key.sign(&message).to_bytes(),
        );
        Ok((body, signature))
    }

    /// The base URL this client dials. The bind persists this value as the
    /// order's `paykit_stack_endpoint` in the same transaction as phase 1,
    /// so later activate/void calls route to the issuing stack even after
    /// `PAYKIT_SERVER_URL` is repointed.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The paykit-server API this client speaks.
    pub fn api(&self) -> PaykitApi {
        self.api
    }

    /// Upstream preparation (`pubky/paykit-server` #66): prepares, or
    /// replays, the unpublished payment request of one bind attempt with a
    /// signed `POST /marketplace/payment-requests/prepare`.
    ///
    /// The closed body is `{amount_sats, creator, operation_id,
    /// payment_window_seconds, reader, reference}`: `operation_id` and
    /// `reference` come from [`crate::payment_attempt`], and the window is a
    /// duration that starts at activation, never an absolute deadline. A
    /// `200` carries `total_sats == amount_sats` (the upstream path adds no
    /// nonce); any other total is refused as
    /// [`PaykitRequestError::TotalInconsistent`]. Callable only on a
    /// [`PaykitApi::Upstream`] client.
    pub async fn prepare_marketplace_payment(
        &self,
        seller_pubky: &str,
        buyer_pubky: &str,
        reference: uuid::Uuid,
        amount_sats: u64,
        operation_id: &str,
        payment_window_seconds: u64,
    ) -> Result<UpstreamPrepared, PaykitRequestError> {
        if self.api != PaykitApi::Upstream {
            tracing::error!("the upstream prepare was called on a fork paykit client");
            return Err(PaykitRequestError::Rejected);
        }
        let url = format!("{}{UPSTREAM_PREPARE_PATH}", self.base_url);
        let (body, signature) = self
            .signed_body(
                &url,
                &serde_json::json!({
                    "amount_sats": amount_sats,
                    "creator": pubky_app_key(seller_pubky),
                    "operation_id": operation_id,
                    "payment_window_seconds": payment_window_seconds,
                    "reader": pubky_app_key(buyer_pubky),
                    "reference": reference.hyphenated().to_string(),
                }),
            )
            .map_err(|_| PaykitRequestError::Rejected)?;
        let response = self
            .http
            .post(url)
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        let status = response.status();
        if status.is_success() {
            let prepared = match status == reqwest::StatusCode::OK {
                true => response.json::<UpstreamPrepared>().await.ok(),
                false => None,
            };
            let Some(prepared) = prepared.filter(|prepared| prepared.state == "prepared") else {
                tracing::error!(
                    status = %status,
                    "paykit prepare answered outside its contract; refusing the bind"
                );
                return Err(PaykitRequestError::Rejected);
            };
            if prepared.total_sats != amount_sats {
                tracing::error!(
                    amount_sats,
                    total_sats = prepared.total_sats,
                    "ALERT paykit prepare total_sats != amount_sats; refusing the bind"
                );
                return Err(PaykitRequestError::TotalInconsistent);
            }
            return Ok(prepared);
        }
        let code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        Err(upstream_prepare_error(status, &code))
    }

    /// Phase 1 (§B.11.3): prepares (or idempotently replays) the Paykit
    /// payment request for a physical bitcoin order. `expires_at` is the
    /// exact hold deadline the bind armed; `reference` is the bind
    /// attempt's [`attempt_reference`] and `idempotency_key` is
    /// `{reference}:{bind_attempt}`. The 200 body is the verbatim
    /// prepared shape; a legacy 204 or a body missing any field is a
    /// hard refusal.
    pub async fn create_payment_request(
        &self,
        seller_pubky: &str,
        buyer_pubky: &str,
        reference: &str,
        amount_sats: u64,
        expires_at: chrono::DateTime<chrono::Utc>,
        idempotency_key: &str,
    ) -> Result<PaykitPrepared, PaykitRequestError> {
        let url = format!("{}/v0/payment-requests", self.base_url);
        let (body, signature) = self
            .signed_body(
                &url,
                &serde_json::json!({
                    "amount_sats": amount_sats,
                    "creator": pubky_app_key(seller_pubky),
                    "reader": pubky_app_key(buyer_pubky),
                    "reference": reference,
                    "expires_at": expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "idempotency_key": idempotency_key,
                }),
            )
            .map_err(|_| PaykitRequestError::Rejected)?;
        let response = self
            .http
            .post(url)
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        let status = response.status();
        if status.is_success() {
            if status != reqwest::StatusCode::OK {
                tracing::error!(
                    status = %status,
                    "paykit phase 1 answered a bodyless success; refusing the bind"
                );
                return Err(PaykitRequestError::Rejected);
            }
            return match response.json::<PaykitPrepared>().await {
                Ok(prepared) => Ok(prepared),
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "paykit phase 1 returned a body violating the prepared shape"
                    );
                    Err(PaykitRequestError::Rejected)
                }
            };
        }
        let code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        match code.as_str() {
            "creator_session_invalid" => Err(PaykitRequestError::SellerAccountUnavailable),
            "reader_not_payable" => Err(PaykitRequestError::ReaderNotPayable),
            "reader_setup_pending" => Err(PaykitRequestError::ReaderSetupPending),
            "invalid_request" | "invoice_conflict" => Err(PaykitRequestError::Rejected),
            // `bitcoin_creation_disabled` (503, §C.16) and everything else:
            // refused cleanly as an availability failure, as today.
            _ => Err(PaykitRequestError::Unavailable),
        }
    }

    /// One signed POST to an explicit endpoint (the persisted per-order
    /// stack endpoint, which may predate the configured base URL).
    async fn post_signed_to(
        &self,
        endpoint: &str,
        path: &str,
        body: serde_json::Value,
    ) -> Result<reqwest::Response, PaykitCommandError> {
        let url = format!("{}{path}", endpoint.trim_end_matches('/'));
        let (body, signature) = self.signed_body(&url, &body).map_err(|_| {
            PaykitCommandError::UnexpectedRejection("body did not canonicalize".to_string())
        })?;
        self.http
            .post(url)
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await
            .map_err(|_| PaykitCommandError::Unavailable)
    }

    async fn error_code(response: reqwest::Response) -> (reqwest::StatusCode, String) {
        let status = response.status();
        let code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| body["error"]["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        (status, code)
    }

    /// Phase 2 (§B.11.3): activates a prepared invoice, idempotently.
    /// `invoice_id` is repeated in the signed body; `stack_id` and
    /// `total_sats` are echoed as guards.
    pub async fn activate_payment_request(
        &self,
        endpoint: &str,
        invoice_id: uuid::Uuid,
        stack_id: &str,
        total_sats: u64,
        activation_attempt: u64,
    ) -> Result<PaykitActivated, PaykitCommandError> {
        let response = self
            .post_signed_to(
                endpoint,
                &format!("/v0/payment-requests/{invoice_id}/activate"),
                serde_json::json!({
                    "invoice_id": invoice_id,
                    "stack_id": stack_id,
                    "total_sats": total_sats,
                    "activation_attempt": activation_attempt,
                }),
            )
            .await?;
        let status = response.status();
        if status.is_success() {
            return response.json::<PaykitActivated>().await.map_err(|error| {
                tracing::warn!(
                    error = %error,
                    "paykit activate returned a malformed success body; retrying"
                );
                PaykitCommandError::Unavailable
            });
        }
        let (status, code) = Self::error_code(response).await;
        Err(PaykitCommandError::from_status(status, code))
    }

    /// Voids a prepared invoice, idempotently (§B.11.3). `stack_id` is the
    /// value phase 1 returned, never one re-derived from configuration.
    pub async fn void_payment_request(
        &self,
        endpoint: &str,
        invoice_id: uuid::Uuid,
        stack_id: &str,
        reason: &str,
    ) -> Result<PaykitVoided, PaykitCommandError> {
        let response = self
            .post_signed_to(
                endpoint,
                &format!("/v0/payment-requests/{invoice_id}/void"),
                serde_json::json!({
                    "invoice_id": invoice_id,
                    "stack_id": stack_id,
                    "reason": reason,
                }),
            )
            .await?;
        let status = response.status();
        if status.is_success() {
            return response.json::<PaykitVoided>().await.map_err(|error| {
                tracing::warn!(
                    error = %error,
                    "paykit void returned a malformed success body; retrying"
                );
                PaykitCommandError::Unavailable
            });
        }
        let (status, code) = Self::error_code(response).await;
        Err(PaykitCommandError::from_status(status, code))
    }

    /// §B.9 resolve: records the marketplace-authoritative money outcome on
    /// the issuing stack, idempotent on `(invoice_id, resolution)`. The
    /// canonical body is exactly `{invoice_id, resolution, resolved_at,
    /// stack_id}` (the captured contract shape); `stack_id` is the value
    /// phase 1 returned, and the call dials the ROW's pinned endpoint.
    pub async fn resolve_payment_request(
        &self,
        endpoint: &str,
        invoice_id: uuid::Uuid,
        stack_id: &str,
        resolution: &str,
        resolved_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<PaykitResolveResponse, PaykitCommandError> {
        let response = self
            .post_signed_to(
                endpoint,
                &format!("/v0/payment-requests/{invoice_id}/resolve"),
                serde_json::json!({
                    "invoice_id": invoice_id,
                    "resolution": resolution,
                    "resolved_at": resolved_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "stack_id": stack_id,
                }),
            )
            .await?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response.text().await.unwrap_or_default();
        if status.is_success() {
            let resolved = serde_json::from_str::<PaykitResolved>(&body)
                .map_err(|error| {
                    tracing::warn!(
                        error = %error,
                        "paykit resolve returned a malformed success body"
                    );
                    error
                })
                .ok();
            return Ok(PaykitResolveResponse {
                status,
                code: None,
                retry_after,
                resolved,
            });
        }
        let code = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|body| body["error"]["code"].as_str().map(str::to_owned));
        Ok(PaykitResolveResponse {
            status,
            code,
            retry_after,
            resolved: None,
        })
    }

    /// Reads `stack_id` from an EXPLICIT endpoint's `/health/ready` (the
    /// pinned-endpoint half of the resolve pin, §B.8.8): the delivery arm
    /// compares the row's persisted identity against what that address
    /// currently answers, per endpoint rather than per process. Transport
    /// or shape failures are `Err`; a well-formed body without the field
    /// answers `None`.
    pub async fn stack_identity_at(
        &self,
        endpoint: &str,
    ) -> Result<Option<String>, PaykitRequestError> {
        let response = self
            .http
            .get(format!("{}/health/ready", endpoint.trim_end_matches('/')))
            .timeout(AVAILABILITY_HTTP_TIMEOUT)
            .send()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        if !response.status().is_success() {
            return Err(PaykitRequestError::Unavailable);
        }
        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|_| PaykitRequestError::Unavailable)?;
        Ok(body["stack_id"].as_str().map(str::to_owned))
    }

    /// Polls the payment status for one order reference against the strict
    /// `paykit.bitcoin_status/v2` contract (W1.14): a missing/wrong
    /// `contract_version`, a missing/unknown `allocation_mode`, an unknown
    /// status, a missing/mistyped mandatory `late_settlement` boolean, or
    /// a malformed body all fail CLOSED as `Unavailable` — never an
    /// automatic transition input.
    pub async fn payment_status(&self, seller_pubky: &str, reference: &str) -> PaykitStatusOutcome {
        self.payment_status_with_delivery(seller_pubky, reference)
            .await
            .0
    }

    /// [`Self::payment_status`] plus the request's delivery state from the
    /// same response (`paykit_delivery_state`), when the body carries a
    /// known one. The delivery state is reported even when the status
    /// itself fails closed on the payment contract.
    pub async fn payment_status_with_delivery(
        &self,
        seller_pubky: &str,
        reference: &str,
    ) -> (PaykitStatusOutcome, Option<PaykitDeliveryState>) {
        let url = format!("{}/transactions/status", self.base_url);
        let Ok((body, signature)) = self.signed_body(
            &url,
            &serde_json::json!({
                "bundle_id": reference,
                "creator": pubky_app_key(seller_pubky),
            }),
        ) else {
            return (PaykitStatusOutcome::Unavailable, None);
        };
        let response = self
            .http
            .post(url)
            .header("x-paykit-signature", signature)
            .body(body)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tracing::warn!("paykit payment status transport failure");
                return (PaykitStatusOutcome::Unavailable, None);
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return (PaykitStatusOutcome::NotFound, None);
        }
        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), "paykit payment status rejected");
            return (PaykitStatusOutcome::Unavailable, None);
        }
        #[derive(Deserialize)]
        struct StatusBody {
            contract_version: Option<String>,
            status: Option<String>,
            allocation_mode: Option<String>,
            // Mandatory strict boolean (no default): an absent, null, or
            // wrong-typed `late_settlement` fails the whole body parse and
            // the poll fails CLOSED as `Unavailable`.
            late_settlement: bool,
            #[serde(default)]
            amount_matched: bool,
            #[serde(default)]
            observed_sats: Option<u64>,
            #[serde(default)]
            confirmations: Option<u32>,
            #[serde(default)]
            txid: Option<String>,
            #[serde(default)]
            paykit_delivery_state: Option<String>,
        }
        let Ok(bytes) = response.bytes().await else {
            tracing::warn!("paykit payment status returned a malformed body; failing closed");
            return (PaykitStatusOutcome::Unavailable, None);
        };
        let body = match serde_json::from_slice::<StatusBody>(&bytes) {
            Ok(body) => body,
            Err(_) => {
                if let Some(outcome) = upstream_payment_status(&bytes) {
                    return (outcome, None);
                }
                tracing::warn!("paykit payment status returned a malformed body; failing closed");
                return (PaykitStatusOutcome::Unavailable, None);
            }
        };
        let delivery = body
            .paykit_delivery_state
            .as_deref()
            .and_then(PaykitDeliveryState::from_wire);
        // Contract validation, fail closed: the version must be the strict
        // v2 marker and the mode exactly one of the two known values.
        if body.contract_version.as_deref() != Some(PAYKIT_STATUS_CONTRACT) {
            tracing::warn!(
                "paykit payment status violated the status contract version; failing closed"
            );
            return (PaykitStatusOutcome::Unavailable, delivery);
        }
        let Some(allocation_mode) = body
            .allocation_mode
            .filter(|mode| matches!(mode.as_str(), "exclusive" | "shared_manual"))
        else {
            tracing::warn!(
                "paykit payment status carried a missing or unknown allocation_mode; failing closed"
            );
            return (PaykitStatusOutcome::Unavailable, delivery);
        };
        let facts = PaykitStatusFacts {
            allocation_mode,
            late_settlement: body.late_settlement,
            observation: PaykitObservation {
                txid: body.txid,
                observed_sats: body.observed_sats,
                confirmations: body.confirmations,
            },
        };
        let outcome = match body.status.as_deref() {
            Some("undetected") => PaykitStatusOutcome::Undetected,
            Some("detected") => PaykitStatusOutcome::Detected { facts },
            Some("confirmed") => PaykitStatusOutcome::Confirmed {
                amount_matched: body.amount_matched,
                facts,
            },
            _ => PaykitStatusOutcome::Unavailable,
        };
        (outcome, delivery)
    }
}

/// The payment-methods runtime carried on [`crate::AppState`]: absent when
/// `STRIPE_KEY_ENCRYPTION_KEY` is unset (the whole surface is refused), with
/// the Paykit leg additionally gated on its own pair of variables.
pub struct PaymentsRuntime {
    pub stripe_key_cipher: StripeKeyCipher,
    pub stripe: StripeClient,
    pub paykit: Option<PaykitClient>,
    pub paypal_ipn: PaypalIpnVerifier,
    pub shippo: ShippoClient,
}

impl std::fmt::Debug for PaymentsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PaymentsRuntime(<redacted>)")
    }
}

/// The Paykit client from its three settings. URL and signing key come
/// together or not at all; the API selector is meaningless without them, so
/// setting it alone is refused rather than silently ignored.
fn paykit_client_from_settings(
    url: Option<String>,
    signing_seed: Option<String>,
    api: Option<String>,
) -> anyhow::Result<Option<PaykitClient>> {
    match (url, signing_seed) {
        (None, None) => {
            if api.is_some() {
                anyhow::bail!(
                    "{ENV_PAYKIT_SERVER_API} is set without {ENV_PAYKIT_SERVER_URL} and \
                     {ENV_PAYKIT_REQUEST_SIGNING_KEY}"
                );
            }
            Ok(None)
        }
        (Some(url), Some(seed)) => {
            let api = api
                .map(|value| PaykitApi::parse(&value))
                .transpose()?
                .unwrap_or_default();
            Ok(Some(PaykitClient::new(&url, &seed)?.with_api(api)))
        }
        _ => anyhow::bail!(
            "Paykit is partially configured: set both {ENV_PAYKIT_SERVER_URL} and \
             {ENV_PAYKIT_REQUEST_SIGNING_KEY}, or neither"
        ),
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
    let paykit = paykit_client_from_settings(
        std::env::var(ENV_PAYKIT_SERVER_URL).ok(),
        std::env::var(ENV_PAYKIT_REQUEST_SIGNING_KEY).ok(),
        std::env::var(ENV_PAYKIT_SERVER_API).ok(),
    )?;
    let ipn_verify_url = std::env::var(ENV_PAYPAL_IPN_VERIFY_URL)
        .unwrap_or_else(|_| "https://ipnpb.paypal.com/cgi-bin/webscr".to_string());
    let paypal_ipn = PaypalIpnVerifier::new(&ipn_verify_url)?;
    let shippo_base = std::env::var(ENV_SHIPPO_API_BASE)
        .unwrap_or_else(|_| "https://api.goshippo.com".to_string());
    let shippo = ShippoClient::new(&shippo_base)?;
    Ok(Some(Arc::new(PaymentsRuntime {
        stripe_key_cipher,
        stripe,
        paykit,
        paypal_ipn,
        shippo,
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

    /// The status plus the request's delivery state, when the source knows
    /// it. Sources without delivery facts report none.
    fn status_with_delivery<'a>(
        &'a self,
        seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = (PaykitStatusOutcome, Option<PaykitDeliveryState>)> + Send + 'a>>
    {
        Box::pin(async move { (self.status(seller_pubky, reference).await, None) })
    }
}

impl PaykitStatusSource for PaykitClient {
    fn status<'a>(
        &'a self,
        seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = PaykitStatusOutcome> + Send + 'a>> {
        Box::pin(self.payment_status(seller_pubky, reference))
    }

    fn status_with_delivery<'a>(
        &'a self,
        seller_pubky: &'a str,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = (PaykitStatusOutcome, Option<PaykitDeliveryState>)> + Send + 'a>>
    {
        Box::pin(self.payment_status_with_delivery(seller_pubky, reference))
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

    /// The signing seed of `paykit-server/tests/setup_status.rs`
    /// (`SigningKey::from_bytes(&[7; 32])`).
    const UPSTREAM_TEST_SEED: &str =
        "0707070707070707070707070707070707070707070707070707070707070707";
    const UPSTREAM_CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

    fn upstream_test_client(api: PaykitApi) -> PaykitClient {
        PaykitClient::new("https://paykit.example", UPSTREAM_TEST_SEED)
            .unwrap()
            .with_api(api)
    }

    /// Signatures produced by upstream's own `signature_preimage` (PR #55
    /// head `a109148`) over `POST /setup/status`: the bare-creator body is
    /// upstream's test body, the BTC body is what this client sends.
    #[test]
    fn the_upstream_client_signs_what_upstream_verifies() {
        let client = upstream_test_client(PaykitApi::Upstream);
        let url = "https://paykit.example/setup/status";
        let (body, signature) = client
            .signed_body(url, &serde_json::json!({ "creator": UPSTREAM_CREATOR }))
            .unwrap();
        assert_eq!(
            body,
            r#"{"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"}"#
        );
        assert_eq!(
            signature,
            "uGwZVTdCxIRugXQo2gui7dAz7Ude3mobvcretytsMx55Iyr3rCzLEm-rwpfJcphCTPOj6VAzD4sNMr2wT60OAw"
        );
        let (body, signature) = client
            .signed_body(
                url,
                &serde_json::json!({ "creator": UPSTREAM_CREATOR, "asset": "BTC" }),
            )
            .unwrap();
        assert_eq!(
            body,
            r#"{"asset":"BTC","creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"}"#
        );
        assert_eq!(
            signature,
            "iJiAIXxTD2HUUwDus4BykJGxIrq7tG1wvzQvcxufWJutnh500DNkImHqx5rk-BNt0_gXJ8bRexIJdaS254oECw"
        );
    }

    #[test]
    fn the_api_selector_needs_the_paykit_url_and_key_and_defaults_to_the_fork() {
        let url = Some("https://paykit.example".to_string());
        let seed = Some(UPSTREAM_TEST_SEED.to_string());
        let api = |client: Option<PaykitClient>| client.expect("configured").api;

        assert_eq!(
            api(paykit_client_from_settings(url.clone(), seed.clone(), None).unwrap()),
            PaykitApi::Fork
        );
        assert_eq!(
            api(
                paykit_client_from_settings(url.clone(), seed.clone(), Some("upstream".into()))
                    .unwrap()
            ),
            PaykitApi::Upstream
        );
        assert!(paykit_client_from_settings(None, None, None)
            .unwrap()
            .is_none());
        assert!(
            paykit_client_from_settings(url.clone(), seed.clone(), Some("rc9".into())).is_err()
        );
        assert!(paykit_client_from_settings(None, None, Some("upstream".into())).is_err());
        assert!(paykit_client_from_settings(url, None, Some("upstream".into())).is_err());
        assert!(paykit_client_from_settings(None, seed, None).is_err());
    }

    #[test]
    fn the_fork_client_still_signs_the_bare_canonical_body() {
        let fork = upstream_test_client(PaykitApi::Fork);
        let upstream = upstream_test_client(PaykitApi::Upstream);
        let value = serde_json::json!({ "creator": UPSTREAM_CREATOR });
        let url = "https://paykit.example/setup/status";
        let (body, signature) = fork.signed_body(url, &value).unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        assert_eq!(
            signature,
            base64::Engine::encode(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                key.sign(body.as_bytes()).to_bytes(),
            )
        );
        assert_ne!(signature, upstream.signed_body(url, &value).unwrap().1);
    }

    #[test]
    fn the_signed_path_is_the_request_path_including_a_base_url_prefix() {
        let client = upstream_test_client(PaykitApi::Upstream);
        let value = serde_json::json!({ "creator": UPSTREAM_CREATOR });
        let plain = client
            .signed_body("https://paykit.example/setup/status", &value)
            .unwrap();
        let prefixed = client
            .signed_body("https://paykit.example/paykit/setup/status", &value)
            .unwrap();
        assert_ne!(plain.1, prefixed.1);
        let (body, signature) = prefixed;
        let signature =
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, signature)
                .unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        use ed25519_dalek::Verifier;
        key.verifying_key()
            .verify(
                &paykit_signature_preimage("POST", "/paykit/setup/status", body.as_bytes()),
                &ed25519_dalek::Signature::from_slice(&signature).unwrap(),
            )
            .unwrap();
    }

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
    fn delivery_states_normalize_only_live_request_facts() {
        assert_eq!(
            PaykitDeliveryState::from_wire("pending_delivery"),
            Some(PaykitDeliveryState::Pending)
        );
        assert_eq!(
            PaykitDeliveryState::from_wire("delivered"),
            Some(PaykitDeliveryState::Delivered)
        );
        assert_eq!(
            PaykitDeliveryState::from_wire("failed"),
            Some(PaykitDeliveryState::Failed)
        );
        for other in ["cancelled", "contract_error", "pending", ""] {
            assert_eq!(PaykitDeliveryState::from_wire(other), None, "{other}");
        }
        assert_eq!(PaykitDeliveryState::Pending.as_str(), "pending");
    }

    #[test]
    fn attempt_references_are_canonical_crockford_bundle_identifiers() {
        // All-zero bytes encode to 26 zeros — the canonical fixture shape.
        assert_eq!(crockford_bundle_id(&[0; 16]), "0".repeat(26));
        assert_eq!(
            crockford_bundle_id(&[0xff; 16]),
            format!("{}W", "Z".repeat(25))
        );
        let order = uuid::Uuid::parse_str("018f47d2-6a27-7c23-a49d-6b21bb770120").unwrap();
        let reference = attempt_reference(order, 1);
        assert_eq!(reference.len(), 26);
        assert!(reference
            .chars()
            .all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)));
        assert_eq!(reference, attempt_reference(order, 1), "deterministic");
        assert_ne!(reference, attempt_reference(order, 2), "per attempt");
        assert_ne!(
            reference,
            attempt_reference(uuid::Uuid::new_v4(), 1),
            "per order"
        );
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
