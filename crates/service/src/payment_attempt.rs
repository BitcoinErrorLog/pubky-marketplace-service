//! The Paykit payment attempt as the upstream API prepares it
//! (`PAYKIT_SERVER_API=upstream`, `pubky/paykit-server` #66).
//!
//! One module owns every type of the prepared attempt (its identity, its
//! asset, the server's prepared answer and the persisted row), so the
//! denomination of an attempt has exactly one home. Today an attempt is
//! always Bitcoin: its amount is satoshis and [`PaymentAsset`] has one
//! variant. A later asset is a new variant plus a value in
//! `orders.paykit_asset`, a text column with no CHECK, so adding it needs no
//! migration; [`PreparedAttempt::load`] refuses a stored asset this build
//! does not know instead of reading it as Bitcoin.
//!
//! # Identity
//!
//! An attempt is one `(order, bind attempt)` pair and is named twice on the
//! wire, both derived from the pair so a retry replays:
//!
//! - `operation_id` = `marketplace-payment:{attempt_reference}:{attempt}`
//!   (the fork's `idempotency_key` with a namespace prefix) scopes the
//!   preparation at paykit-server, which replays the stored answer for an
//!   identical request and answers `409 conflict` for a changed one;
//! - `reference` is a lowercase hyphenated UUIDv4 derived from the order id
//!   and the attempt number ([`payment_reference`]); it is part of the
//!   binding, so it must be the same on every retry of an attempt and is
//!   persisted with the attempt in `orders.paykit_payment_reference`.
//!
//! # Lifecycle in this slice
//!
//! Preparation creates an unpublished request: paykit-server writes neither
//! an invoice nor an outbox row until activation, which a later slice adds.
//! An attempt prepared here therefore has no remote state to void: it lapses
//! at its 15-minute activation deadline (`orders.paykit_prepare_expires_at`)
//! on its own, and releasing it (a buyer cancel, the hold expiring) is a
//! local transition.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

/// The namespace prefix of every Marketplace operation id.
pub const OPERATION_NAMESPACE: &str = "marketplace-payment";

/// What an attempt is denominated in. Bitcoin is the only asset the
/// Marketplace prepares today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentAsset {
    Btc,
}

impl PaymentAsset {
    /// The value stored in `orders.paykit_asset`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
        }
    }

    /// Reads the stored value; an asset this build does not know is `None`.
    pub fn from_column(value: &str) -> Option<Self> {
        match value {
            "BTC" => Some(Self::Btc),
            _ => None,
        }
    }
}

/// `marketplace-payment:{attempt_reference}:{attempt}`, the operation
/// identity paykit-server scopes a preparation by.
pub fn operation_id(attempt_reference: &str, attempt: i32) -> String {
    format!("{OPERATION_NAMESPACE}:{attempt_reference}:{attempt}")
}

/// The payment `reference` of one bind attempt: a UUIDv4 (version nibble 4,
/// RFC 4122 variant) built from the first 16 bytes of SHA-256 over a domain
/// tag, the order id and the attempt number. The same `(order, attempt)`
/// always derives the same reference, and distinct attempts derive distinct
/// ones, so paykit-server's binding of an operation never changes between
/// retries. Displayed lowercase and hyphenated, the only form it accepts.
pub fn payment_reference(order_id: Uuid, attempt: i32) -> Uuid {
    let digest = Sha256::new()
        .chain_update(b"pubky-marketplace/paykit-payment-reference/v1")
        .chain_update(order_id.as_bytes())
        .chain_update(attempt.to_be_bytes())
        .finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// The payment window to bind at preparation, in whole seconds: the time
/// left on the hold, rounded up, and at least one. The window starts when
/// activation commits, so it is a duration here, never a deadline.
pub fn payment_window_seconds(hold_expires_at: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    let remaining_ms = (hold_expires_at - now).num_milliseconds().max(0);
    ((remaining_ms + 999) / 1000).max(1)
}

/// The `200` body of `POST /marketplace/payment-requests/prepare`: closed,
/// every field required. The payment deadline is deliberately absent: the
/// payment window starts when activation commits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamPrepared {
    pub invoice_id: Uuid,
    pub state: String,
    pub total_sats: u64,
    pub prepare_expires_at: DateTime<Utc>,
}

/// What identifies and bounds an attempt about to be prepared: everything
/// the bind writes to the `paykit_payment_reference`, `paykit_operation_id`,
/// `paykit_payment_window_seconds` and `paykit_asset` columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptIdentity {
    pub reference: Uuid,
    pub operation_id: String,
    pub asset: PaymentAsset,
    pub payment_window_seconds: i32,
}

/// What the bind persists of a prepared attempt, beyond the pins both APIs
/// share (`paykit_invoice_id`, `paykit_total_sats`, `paykit_expires_at`,
/// `paykit_prepare_expires_at`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAttempt {
    pub order_id: Uuid,
    pub invoice_id: Uuid,
    pub reference: Uuid,
    pub operation_id: String,
    pub asset: PaymentAsset,
    pub total_sats: i64,
    /// The payment window bound at preparation, in seconds. It starts when
    /// activation commits, never at preparation.
    pub payment_window_seconds: i32,
    /// The activation deadline: paykit-server's `prepare_expires_at`, on its
    /// database clock.
    pub activate_by: DateTime<Utc>,
    /// The local hold deadline the window was sized to.
    pub hold_expires_at: Option<DateTime<Utc>>,
}

/// Why a stored attempt cannot be read.
#[derive(Debug)]
pub enum AttemptReadError {
    Database(sqlx::Error),
    /// The row carries an asset this build does not know.
    UnknownAsset(String),
}

impl std::fmt::Display for AttemptReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(f, "{error}"),
            Self::UnknownAsset(asset) => {
                write!(f, "the stored paykit attempt asset {asset:?} is not known")
            }
        }
    }
}

impl std::error::Error for AttemptReadError {}

impl From<sqlx::Error> for AttemptReadError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<AttemptReadError> for sqlx::Error {
    fn from(error: AttemptReadError) -> Self {
        match error {
            AttemptReadError::Database(error) => error,
            unknown @ AttemptReadError::UnknownAsset(_) => sqlx::Error::Decode(Box::new(unknown)),
        }
    }
}

impl PreparedAttempt {
    /// The order's current attempt when it was prepared through the upstream
    /// API: the row has a payment reference and no stack pin. The fork never
    /// writes a payment reference, and always writes a stack id.
    pub async fn load(
        conn: &mut PgConnection,
        order_id: Uuid,
    ) -> Result<Option<Self>, AttemptReadError> {
        let row = sqlx::query(
            "SELECT paykit_invoice_id, paykit_payment_reference, paykit_operation_id, \
             paykit_asset, paykit_total_sats, paykit_payment_window_seconds, \
             paykit_prepare_expires_at, hold_expires_at \
             FROM orders WHERE id = $1 AND paykit_payment_reference IS NOT NULL \
             AND paykit_stack_id IS NULL AND paykit_invoice_id IS NOT NULL",
        )
        .bind(order_id)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let asset: String = row.try_get("paykit_asset")?;
        let asset =
            PaymentAsset::from_column(&asset).ok_or(AttemptReadError::UnknownAsset(asset))?;
        Ok(Some(Self {
            order_id,
            invoice_id: row.try_get("paykit_invoice_id")?,
            reference: row.try_get("paykit_payment_reference")?,
            operation_id: row.try_get("paykit_operation_id")?,
            asset,
            total_sats: row.try_get("paykit_total_sats")?,
            payment_window_seconds: row.try_get("paykit_payment_window_seconds")?,
            activate_by: row.try_get("paykit_prepare_expires_at")?,
            hold_expires_at: row.try_get("hold_expires_at")?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reference_is_a_deterministic_lowercase_uuid_v4() {
        let order = Uuid::from_u128(0x0192_8f4e_7a6b_7c3d_8e1f_2a3b_4c5d_6e7f);
        let reference = payment_reference(order, 1);
        assert_eq!(reference, payment_reference(order, 1));
        assert_eq!(reference.get_version_num(), 4);
        assert_eq!(reference.get_variant(), uuid::Variant::RFC4122);
        let text = reference.hyphenated().to_string();
        assert_eq!(text, text.to_lowercase());
        assert_eq!(reference.to_string(), text);
    }

    #[test]
    fn every_attempt_and_every_order_derives_its_own_reference() {
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let mut seen = std::collections::HashSet::new();
        for order in [first, second] {
            for attempt in 1..=32 {
                assert!(seen.insert(payment_reference(order, attempt)));
            }
        }
    }

    #[test]
    fn the_operation_id_carries_the_namespace_reference_and_attempt() {
        assert_eq!(
            operation_id("0R8Y7ZQ3M5N9K2VJ6W4X1T8S0P", 3),
            "marketplace-payment:0R8Y7ZQ3M5N9K2VJ6W4X1T8S0P:3"
        );
    }

    #[test]
    fn the_window_is_the_remaining_hold_rounded_up_and_at_least_a_second() {
        let now: DateTime<Utc> = "2026-10-09T12:00:00Z".parse().expect("timestamp");
        let after = |ms: i64| now + chrono::Duration::milliseconds(ms);
        assert_eq!(payment_window_seconds(after(1_800_000), now), 1_800);
        assert_eq!(payment_window_seconds(after(120_500), now), 121);
        assert_eq!(payment_window_seconds(after(120_001), now), 121);
        assert_eq!(payment_window_seconds(after(1), now), 1);
        assert_eq!(payment_window_seconds(after(0), now), 1);
        assert_eq!(payment_window_seconds(after(-5_000), now), 1);
    }

    #[test]
    fn only_bitcoin_is_a_known_asset() {
        assert_eq!(PaymentAsset::from_column("BTC"), Some(PaymentAsset::Btc));
        assert_eq!(PaymentAsset::Btc.as_str(), "BTC");
        for other in ["", "btc", "USDT", "USD"] {
            assert_eq!(PaymentAsset::from_column(other), None, "{other:?}");
        }
    }

    #[test]
    fn the_prepared_body_is_closed_and_complete() {
        let body = r#"{"invoice_id":"6f9619ff-8b86-4d11-b42d-00c04fc964ff","state":"prepared","total_sats":1500,"prepare_expires_at":"2026-10-09T12:15:00Z"}"#;
        let prepared: UpstreamPrepared = serde_json::from_str(body).expect("closed body parses");
        assert_eq!(prepared.total_sats, 1500);
        for bad in [
            r#"{"invoice_id":"6f9619ff-8b86-4d11-b42d-00c04fc964ff","state":"prepared","total_sats":1500}"#,
            r#"{"invoice_id":"6f9619ff-8b86-4d11-b42d-00c04fc964ff","state":"prepared","total_sats":1500,"prepare_expires_at":"2026-10-09T12:15:00Z","expires_at":"2026-10-09T12:45:00Z"}"#,
            r#"{"invoice_id":"6f9619ff-8b86-4d11-b42d-00c04fc964ff","state":"prepared","total_sats":1500,"prepare_expires_at":"2026-10-09T12:15:00Z","nonce_sats":0}"#,
        ] {
            assert!(
                serde_json::from_str::<UpstreamPrepared>(bad).is_err(),
                "{bad}"
            );
        }
    }
}
