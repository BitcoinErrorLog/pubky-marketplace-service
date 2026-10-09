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
//!   identical request and answers `409 operation_conflict` for a changed one;
//! - `reference` is a lowercase hyphenated UUIDv4 derived from the order id
//!   and the attempt number ([`payment_reference`]); it is part of the
//!   binding, so it must be the same on every retry of an attempt and is
//!   persisted with the attempt in `orders.paykit_payment_reference`.
//!
//! A bind request prepares once. A refused, failed or timed-out call is not
//! retried within the request: its attempt number is spent (reserved in its
//! own statement before the bind transaction) and the buyer's next bind
//! derives the next attempt, a new operation and a new payment reference.
//! paykit-server may have committed a preparation whose answer was lost (a
//! `dependency_timeout` after the commit); an exact retry would replay it, but
//! the new attempt never asks, and the orphan lapses at its activation
//! deadline without ever being published.
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

use crate::payments::PaykitApi;

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

/// What the buyer picks at checkout: one `{method}.{asset}.{network}` option
/// (`fiat` stands for "the order's own currency"). It is distinct from
/// [`PaymentAsset`], which is what a Paykit request is *denominated* in: the
/// lock or request asset is the price denomination, not necessarily what the
/// buyer sends, so a USD-denominated request may be paid in USDT.
///
/// `payment_method` stays the buyer-facing label ([`Self::method`]); every
/// existing wire value is unchanged and `usdt` is the only new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentOption {
    PaykitBtcBitcoin,
    PaykitUsdtArbitrumOne,
    PaypalFiat,
    StripeFiat,
}

impl PaymentOption {
    pub const ALL: [Self; 4] = [
        Self::PaykitBtcBitcoin,
        Self::PaykitUsdtArbitrumOne,
        Self::PaypalFiat,
        Self::StripeFiat,
    ];

    /// The canonical option id, as stored in
    /// `seller_accepted_payment_options.option_id`.
    pub const fn id(self) -> &'static str {
        match self {
            Self::PaykitBtcBitcoin => "paykit.btc.bitcoin",
            Self::PaykitUsdtArbitrumOne => "paykit.usdt.arbitrum-one",
            Self::PaypalFiat => "paypal.fiat",
            Self::StripeFiat => "stripe.fiat",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|option| option.id() == id)
    }

    /// The buyer-facing label carried by `orders.payment_method`.
    pub const fn method(self) -> &'static str {
        match self {
            Self::PaykitBtcBitcoin => "bitcoin",
            Self::PaykitUsdtArbitrumOne => "usdt",
            Self::PaypalFiat => "paypal",
            Self::StripeFiat => "stripe",
        }
    }

    pub fn from_method(method: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|option| option.method() == method)
    }

    /// Where the option settles; fiat methods settle off-chain.
    pub const fn network(self) -> Option<PaymentNetwork> {
        match self {
            Self::PaykitBtcBitcoin => Some(PaymentNetwork::Bitcoin),
            Self::PaykitUsdtArbitrumOne => Some(PaymentNetwork::ArbitrumOne),
            Self::PaypalFiat | Self::StripeFiat => None,
        }
    }

    /// Whether the option exists only while `USDT_PAYMENTS_ENABLED` is on.
    pub const fn requires_usdt_flag(self) -> bool {
        matches!(self, Self::PaykitUsdtArbitrumOne)
    }
}

/// Where a payment asset settles (`orders.payment_network`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentNetwork {
    Bitcoin,
    /// USDT0 on Arbitrum One, chain 42161.
    ArbitrumOne,
}

impl PaymentNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bitcoin => "bitcoin",
            Self::ArbitrumOne => "arbitrum-one",
        }
    }
}

/// What the buyer sends (`orders.payment_asset`). Only USDT carries these
/// columns today; Bitcoin, PayPal and Stripe orders leave them NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementAsset {
    Usdt,
}

impl SettlementAsset {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Usdt => "USDT",
        }
    }

    /// The decimals of the amount in `orders.payment_amount_minor`: USDT0
    /// counts millionths.
    pub const fn exponent(self) -> i16 {
        match self {
            Self::Usdt => 6,
        }
    }
}

/// How the amount sent was derived from the order's price of record
/// (`orders.payment_quote_basis`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteBasis {
    /// Exact 1:1 USD to USDT, with no rate source and no rounding.
    Parity,
}

impl QuoteBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parity => "parity",
        }
    }
}

/// Why an order's price cannot be quoted in USDT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParityQuoteError {
    /// Only `USD/2` prices convert at parity.
    UnsupportedPrice,
    /// The total is not a positive amount.
    NotPositive,
    /// The amount in millionths does not fit the column.
    Overflow,
}

/// The amount a buyer sends in `asset`, recorded on the order when the
/// payment option is bound. The order's `currency` and `total_minor` stay the
/// price of record; this is what the buyer pays (`$25.00` as `25.000000`
/// USDT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaymentTerms {
    pub asset: SettlementAsset,
    pub network: PaymentNetwork,
    pub amount_minor: i64,
    pub quote_basis: QuoteBasis,
}

/// USDT millionths per USD cent at parity.
const USDT_MILLIONTHS_PER_CENT: i64 = 10_000;

impl PaymentTerms {
    /// USDT0 on Arbitrum One at exact parity with a `USD/2` price:
    /// `usdt_millionths = total_cents x 10_000`, the rule paykit-server
    /// applies to USD to USDT. Any other price is refused: listings are never
    /// priced in USDT, and a rate-based quote needs Paykit to own it.
    pub fn usdt_at_parity(
        currency: &str,
        exponent: i32,
        total_minor: i64,
    ) -> Result<Self, ParityQuoteError> {
        if currency != "USD" || exponent != 2 {
            return Err(ParityQuoteError::UnsupportedPrice);
        }
        if total_minor <= 0 {
            return Err(ParityQuoteError::NotPositive);
        }
        let amount_minor = total_minor
            .checked_mul(USDT_MILLIONTHS_PER_CENT)
            .ok_or(ParityQuoteError::Overflow)?;
        Ok(Self {
            asset: SettlementAsset::Usdt,
            network: PaymentNetwork::ArbitrumOne,
            amount_minor,
            quote_basis: QuoteBasis::Parity,
        })
    }

    /// Records the terms on the order in one statement, so the five columns
    /// are written together or not at all. Returns whether the order took
    /// them: an order that already carries terms keeps them.
    pub async fn store(
        &self,
        conn: &mut PgConnection,
        order_id: Uuid,
    ) -> Result<bool, sqlx::Error> {
        let updated = sqlx::query(
            "UPDATE orders SET payment_asset = $2, payment_network = $3, \
             payment_amount_minor = $4, payment_exponent = $5, payment_quote_basis = $6 \
             WHERE id = $1 AND payment_asset IS NULL",
        )
        .bind(order_id)
        .bind(self.asset.as_str())
        .bind(self.network.as_str())
        .bind(self.amount_minor)
        .bind(self.asset.exponent())
        .bind(self.quote_basis.as_str())
        .execute(&mut *conn)
        .await?;
        Ok(updated.rows_affected() == 1)
    }
}

/// The assets the deployed paykit-server's Marketplace prepare accepts
/// (`PAYKIT_MARKETPLACE_ASSETS`). It is configuration, not a probe: an upstream
/// that cannot carry USDT must never be sent a body it would reject, so USDT
/// is listed only once the deployed contract has it. Bitcoin is always
/// present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketplaceAssets {
    usdt: bool,
}

impl MarketplaceAssets {
    pub const BTC_ONLY: Self = Self { usdt: false };

    pub const fn includes_usdt(self) -> bool {
        self.usdt
    }

    /// Parses the comma-separated variable: unset or blank is Bitcoin only;
    /// otherwise every entry is `BTC` or `USDT`, `BTC` is required, nothing
    /// repeats, and `USDT` needs the upstream API (the fork has no USDT).
    pub fn parse(value: Option<&str>, api: PaykitApi) -> anyhow::Result<Self> {
        const NAME: &str = "PAYKIT_MARKETPLACE_ASSETS";
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::BTC_ONLY);
        };
        let (mut btc, mut usdt) = (false, false);
        for entry in value.split(',').map(str::trim) {
            let seen = match entry {
                "BTC" => &mut btc,
                "USDT" => &mut usdt,
                "" => anyhow::bail!("{NAME} must not contain an empty entry"),
                other => anyhow::bail!("{NAME} entry {other:?} must be BTC or USDT"),
            };
            if std::mem::replace(seen, true) {
                anyhow::bail!("{NAME} must not repeat {entry}");
            }
        }
        if !btc {
            anyhow::bail!("{NAME} must include BTC");
        }
        if usdt && api != PaykitApi::Upstream {
            anyhow::bail!("{NAME} may include USDT only with PAYKIT_SERVER_API=upstream");
        }
        Ok(Self { usdt })
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
    pub payment_asset: PaymentAsset,
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
    pub payment_asset: PaymentAsset,
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
        let stored: String = row.try_get("paykit_asset")?;
        let payment_asset =
            PaymentAsset::from_column(&stored).ok_or(AttemptReadError::UnknownAsset(stored))?;
        Ok(Some(Self {
            order_id,
            invoice_id: row.try_get("paykit_invoice_id")?,
            reference: row.try_get("paykit_payment_reference")?,
            operation_id: row.try_get("paykit_operation_id")?,
            payment_asset,
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
    fn every_option_has_one_id_and_one_method_and_both_round_trip() {
        let ids: Vec<&str> = PaymentOption::ALL
            .into_iter()
            .map(PaymentOption::id)
            .collect();
        assert_eq!(
            ids,
            [
                "paykit.btc.bitcoin",
                "paykit.usdt.arbitrum-one",
                "paypal.fiat",
                "stripe.fiat"
            ]
        );
        let methods: Vec<&str> = PaymentOption::ALL
            .into_iter()
            .map(PaymentOption::method)
            .collect();
        assert_eq!(methods, ["bitcoin", "usdt", "paypal", "stripe"]);
        for option in PaymentOption::ALL {
            assert_eq!(PaymentOption::from_id(option.id()), Some(option));
            assert_eq!(PaymentOption::from_method(option.method()), Some(option));
        }
        for unknown in ["", "BITCOIN", "USDT", "paykit.usdt", "paypal", "lightning"] {
            assert_eq!(PaymentOption::from_id(unknown), None, "{unknown:?}");
        }
        for unknown in [
            "",
            "BITCOIN",
            "USDT",
            "paykit.usdt.arbitrum-one",
            "lightning",
        ] {
            assert_eq!(PaymentOption::from_method(unknown), None, "{unknown:?}");
        }
    }

    #[test]
    fn only_usdt_needs_the_flag_and_only_onchain_options_have_a_network() {
        let flagged: Vec<PaymentOption> = PaymentOption::ALL
            .into_iter()
            .filter(|option| option.requires_usdt_flag())
            .collect();
        assert_eq!(flagged, [PaymentOption::PaykitUsdtArbitrumOne]);
        assert_eq!(
            PaymentOption::PaykitBtcBitcoin.network(),
            Some(PaymentNetwork::Bitcoin)
        );
        assert_eq!(
            PaymentOption::PaykitUsdtArbitrumOne.network(),
            Some(PaymentNetwork::ArbitrumOne)
        );
        assert_eq!(PaymentOption::PaypalFiat.network(), None);
        assert_eq!(PaymentOption::StripeFiat.network(), None);
        assert_eq!(PaymentNetwork::Bitcoin.as_str(), "bitcoin");
        assert_eq!(PaymentNetwork::ArbitrumOne.as_str(), "arbitrum-one");
    }

    #[test]
    fn a_usd_total_quotes_exact_parity_in_millionths() {
        for (cents, millionths) in [
            (1, 10_000),
            (100, 1_000_000),
            (2_500, 25_000_000),
            (12_345, 123_450_000),
            (i64::MAX / 10_000, (i64::MAX / 10_000) * 10_000),
        ] {
            let terms = PaymentTerms::usdt_at_parity("USD", 2, cents).expect("parity quote");
            assert_eq!(terms.amount_minor, millionths, "{cents} cents");
            assert_eq!(terms.asset, SettlementAsset::Usdt);
            assert_eq!(terms.asset.as_str(), "USDT");
            assert_eq!(terms.asset.exponent(), 6);
            assert_eq!(terms.network, PaymentNetwork::ArbitrumOne);
            assert_eq!(terms.quote_basis, QuoteBasis::Parity);
            assert_eq!(terms.quote_basis.as_str(), "parity");
        }
    }

    #[test]
    fn a_price_that_is_not_usd_cents_has_no_parity_quote() {
        for (currency, exponent) in [
            ("USD", 0),
            ("USD", 3),
            ("EUR", 2),
            ("SAT", 0),
            ("BTC", 8),
            ("USDT", 6),
            ("usd", 2),
        ] {
            assert_eq!(
                PaymentTerms::usdt_at_parity(currency, exponent, 2_500),
                Err(ParityQuoteError::UnsupportedPrice),
                "{currency}/{exponent}"
            );
        }
    }

    #[test]
    fn a_total_that_is_not_positive_or_overflows_has_no_parity_quote() {
        for total in [0, -1, i64::MIN] {
            assert_eq!(
                PaymentTerms::usdt_at_parity("USD", 2, total),
                Err(ParityQuoteError::NotPositive),
                "{total}"
            );
        }
        for total in [i64::MAX / 10_000 + 1, i64::MAX] {
            assert_eq!(
                PaymentTerms::usdt_at_parity("USD", 2, total),
                Err(ParityQuoteError::Overflow),
                "{total}"
            );
        }
    }

    #[test]
    fn marketplace_assets_default_to_bitcoin_only() {
        for unset in [None, Some(""), Some("   ")] {
            for api in [PaykitApi::Fork, PaykitApi::Upstream] {
                let assets = MarketplaceAssets::parse(unset, api).expect("default");
                assert_eq!(assets, MarketplaceAssets::BTC_ONLY, "{unset:?}");
                assert!(!assets.includes_usdt());
            }
        }
    }

    #[test]
    fn marketplace_assets_list_usdt_only_on_the_upstream_api() {
        for value in ["BTC,USDT", " BTC , USDT ", "USDT,BTC"] {
            let assets = MarketplaceAssets::parse(Some(value), PaykitApi::Upstream).expect(value);
            assert!(assets.includes_usdt(), "{value:?}");
            let refused = MarketplaceAssets::parse(Some(value), PaykitApi::Fork)
                .expect_err("the fork has no USDT");
            assert!(refused.to_string().contains("upstream"), "{refused}");
        }
        for api in [PaykitApi::Fork, PaykitApi::Upstream] {
            let assets = MarketplaceAssets::parse(Some("BTC"), api).expect("BTC");
            assert!(!assets.includes_usdt());
        }
    }

    #[test]
    fn marketplace_assets_refuse_a_malformed_list() {
        for (value, reason) in [
            ("USDT", "must include BTC"),
            ("BTC,", "empty entry"),
            (",BTC", "empty entry"),
            ("BTC,,USDT", "empty entry"),
            ("BTC,BTC", "repeat BTC"),
            ("BTC,USDT,USDT", "repeat USDT"),
            ("BTC,DAI", "must be BTC or USDT"),
            ("btc", "must be BTC or USDT"),
            ("BTC USDT", "must be BTC or USDT"),
        ] {
            let refused =
                MarketplaceAssets::parse(Some(value), PaykitApi::Upstream).expect_err(value);
            assert!(refused.to_string().contains(reason), "{value:?}: {refused}");
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
