use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use uuid::Uuid;

use crate::money::{Money, MAX_SAFE_INTEGER};
use crate::pubky::is_valid_pubky;

/// Command envelope contract version per ADR-0019 §3.
pub const COMMERCE_CONTRACT_VERSION: u64 = 1;

/// One redacted validation issue: field path and message, never values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub path: String,
    pub message: String,
}

fn issue(path: &str, message: &str) -> ValidationIssue {
    ValidationIssue {
        path: path.to_string(),
        message: message.to_string(),
    }
}

/// Raw wire envelope. Unknown fields are rejected (`deny_unknown_fields`);
/// the version is validated after parsing so unsupported versions produce a
/// dedicated issue instead of a generic type error.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvelope {
    version: u64,
    command_id: Uuid,
    aggregate_id: String,
    expected_revision: i64,
    issued_at: DateTime<Utc>,
    kind: String,
    payload: Value,
}

/// A fully validated, normalized command.
#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    pub command_id: Uuid,
    pub aggregate_id: String,
    pub expected_revision: i64,
    pub issued_at: DateTime<Utc>,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommandPayload {
    RegisterListing(RegisterListingPayload),
    ReserveInventory(ReserveInventoryPayload),
    CreateCheckout(CreateCheckoutPayload),
}

impl Command {
    pub fn kind(&self) -> &'static str {
        match self.payload {
            CommandPayload::RegisterListing(_) => "listing.register",
            CommandPayload::ReserveInventory(_) => "inventory.reserve",
            CommandPayload::CreateCheckout(_) => "checkout.create",
        }
    }

    /// Canonical JSON of the parsed, normalized command. Two wire texts that
    /// parse to the same command (key order, timestamp formatting, defaults)
    /// canonicalize identically.
    pub fn canonical_json(&self) -> Value {
        let payload = match &self.payload {
            CommandPayload::RegisterListing(p) => serde_json::to_value(p),
            CommandPayload::ReserveInventory(p) => serde_json::to_value(p),
            CommandPayload::CreateCheckout(p) => serde_json::to_value(p),
        }
        .expect("command payloads serialize infallibly");
        json!({
            "version": COMMERCE_CONTRACT_VERSION,
            "command_id": self.command_id,
            "aggregate_id": self.aggregate_id,
            "expected_revision": self.expected_revision,
            "issued_at": self.issued_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "kind": self.kind(),
            "payload": payload,
        })
    }

    /// SHA-256 hex digest of the canonical JSON; the idempotency comparison key.
    pub fn request_hash(&self) -> String {
        let canonical =
            serde_json::to_vec(&self.canonical_json()).expect("canonical JSON serializes");
        let digest = Sha256::digest(&canonical);
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SaleFormat {
    #[default]
    FixedPrice,
    Auction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuctionTerms {
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub minimum_increment: Money,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserve_price: Option<Money>,
    pub anti_sniping_window_seconds: i64,
    pub anti_sniping_extension_seconds: i64,
}

fn default_title() -> String {
    "Marketplace item".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterListingPayload {
    pub seller_pubky: String,
    pub listing_id: String,
    #[serde(default = "default_title")]
    pub title: String,
    pub listing_revision: i64,
    pub content_hash: String,
    pub quantity: i64,
    pub unit_price: Money,
    #[serde(default)]
    pub sale_format: SaleFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auction_terms: Option<AuctionTerms>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveInventoryPayload {
    pub quantity: i64,
    pub reservation_ttl_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckoutLine {
    pub listing_aggregate_id: String,
    pub expected_revision: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryAddress {
    pub name: String,
    pub line1: String,
    pub line2: String,
    pub city: String,
    pub region: String,
    pub postal_code: String,
    pub country_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCheckoutPayload {
    pub lines: Vec<CheckoutLine>,
    pub delivery_address: DeliveryAddress,
    pub guarantee_policy_version: u32,
}

fn aggregate_id_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^[a-z][a-z0-9_-]{1,31}:[A-Za-z0-9_-]{1,256}$").expect("valid regex")
    })
}

fn entity_id_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9_-]{1,128}$").expect("valid regex"))
}

fn content_hash_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-f0-9]{64}$").expect("valid regex"))
}

fn currency_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Z][A-Z0-9]{2,11}$").expect("valid regex"))
}

fn country_code_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Z]{2}$").expect("valid regex"))
}

/// Validates the actor value supplied by the authentication layer.
pub fn validate_actor(actor: &str) -> Result<(), Vec<ValidationIssue>> {
    if is_valid_pubky(actor) {
        Ok(())
    } else {
        Err(vec![issue(
            "actor",
            "Expected a 52-character z-base-32 Pubky",
        )])
    }
}

/// Parses and validates a wire command. Issues never echo payload values.
pub fn parse_command(raw: &Value) -> Result<Command, Vec<ValidationIssue>> {
    let envelope: RawEnvelope = match serde_json::from_value(raw.clone()) {
        Ok(envelope) => envelope,
        Err(error) => return Err(vec![issue("", &format!("Invalid envelope: {error}"))]),
    };

    let mut issues = Vec::new();
    if envelope.version != COMMERCE_CONTRACT_VERSION {
        issues.push(issue("version", "Unsupported command version"));
    }
    if !aggregate_id_regex().is_match(&envelope.aggregate_id) {
        issues.push(issue(
            "aggregate_id",
            "Expected type:identifier aggregate format",
        ));
    }
    if !(0..=MAX_SAFE_INTEGER).contains(&envelope.expected_revision) {
        issues.push(issue(
            "expected_revision",
            "Expected a non-negative revision",
        ));
    }
    if !issues.is_empty() {
        return Err(issues);
    }

    let payload = match envelope.kind.as_str() {
        "listing.register" => {
            parse_payload(&envelope.payload).and_then(validate_register_listing)?
        }
        "inventory.reserve" => {
            parse_payload(&envelope.payload).and_then(validate_reserve_inventory)?
        }
        "checkout.create" => parse_payload(&envelope.payload).and_then(validate_create_checkout)?,
        _ => return Err(vec![issue("kind", "Unsupported command kind")]),
    };

    Ok(Command {
        command_id: envelope.command_id,
        aggregate_id: envelope.aggregate_id,
        expected_revision: envelope.expected_revision,
        issued_at: envelope.issued_at,
        payload,
    })
}

fn parse_payload<T: for<'de> Deserialize<'de>>(value: &Value) -> Result<T, Vec<ValidationIssue>> {
    serde_json::from_value(value.clone())
        .map_err(|error| vec![issue("payload", &format!("Invalid payload: {error}"))])
}

fn validate_money(path: &str, money: &Money, issues: &mut Vec<ValidationIssue>) {
    if !(0..=MAX_SAFE_INTEGER).contains(&money.amount_minor) {
        issues.push(issue(
            &format!("{path}.amount_minor"),
            "Expected a non-negative safe integer amount",
        ));
    }
    if !currency_regex().is_match(&money.currency) {
        issues.push(issue(
            &format!("{path}.currency"),
            "Expected an uppercase asset code",
        ));
    }
    if !(0..=18).contains(&money.exponent) {
        issues.push(issue(
            &format!("{path}.exponent"),
            "Expected an exponent between 0 and 18",
        ));
    }
}

fn validate_positive_money(path: &str, money: &Money, issues: &mut Vec<ValidationIssue>) {
    validate_money(path, money, issues);
    if money.amount_minor <= 0 {
        issues.push(issue(
            &format!("{path}.amount_minor"),
            "Expected a positive monetary amount",
        ));
    }
}

fn validate_trimmed(
    path: &str,
    value: &mut String,
    min: usize,
    max: usize,
    issues: &mut Vec<ValidationIssue>,
) {
    *value = value.trim().to_string();
    if value.chars().count() < min || value.chars().count() > max {
        issues.push(issue(
            path,
            &format!("Expected between {min} and {max} characters"),
        ));
    }
}

fn validate_register_listing(
    mut payload: RegisterListingPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !is_valid_pubky(&payload.seller_pubky) {
        issues.push(issue(
            "payload.seller_pubky",
            "Expected a 52-character z-base-32 Pubky",
        ));
    }
    if !entity_id_regex().is_match(&payload.listing_id) {
        issues.push(issue(
            "payload.listing_id",
            "Expected a path-safe commerce identifier",
        ));
    }
    validate_trimmed("payload.title", &mut payload.title, 1, 80, &mut issues);
    if !(1..=MAX_SAFE_INTEGER).contains(&payload.listing_revision) {
        issues.push(issue(
            "payload.listing_revision",
            "Expected a positive listing revision",
        ));
    }
    if !content_hash_regex().is_match(&payload.content_hash) {
        issues.push(issue(
            "payload.content_hash",
            "Expected a 64-character lowercase hex hash",
        ));
    }
    if !(1..=1_000_000).contains(&payload.quantity) {
        issues.push(issue(
            "payload.quantity",
            "Expected a quantity between 1 and 1000000",
        ));
    }
    validate_positive_money("payload.unit_price", &payload.unit_price, &mut issues);

    let is_auction = payload.sale_format == SaleFormat::Auction;
    if is_auction != payload.auction_terms.is_some() {
        issues.push(issue(
            "payload.auction_terms",
            "Auction format and terms must be configured together",
        ));
    }
    if let Some(terms) = &payload.auction_terms {
        if terms.ends_at <= terms.starts_at {
            issues.push(issue(
                "payload.auction_terms.ends_at",
                "Auction end must follow start",
            ));
        }
        validate_positive_money(
            "payload.auction_terms.minimum_increment",
            &terms.minimum_increment,
            &mut issues,
        );
        if let Some(reserve) = &terms.reserve_price {
            validate_positive_money("payload.auction_terms.reserve_price", reserve, &mut issues);
        }
        for (path, amount) in [
            ("minimum_increment", Some(&terms.minimum_increment)),
            ("reserve_price", terms.reserve_price.as_ref()),
        ] {
            if let Some(amount) = amount {
                if !amount.same_asset(&payload.unit_price) {
                    issues.push(issue(
                        &format!("payload.auction_terms.{path}"),
                        "Auction amounts must use the listing asset and exponent",
                    ));
                }
            }
        }
        if !(0..=3_600).contains(&terms.anti_sniping_window_seconds) {
            issues.push(issue(
                "payload.auction_terms.anti_sniping_window_seconds",
                "Expected a window between 0 and 3600 seconds",
            ));
        }
        if !(0..=3_600).contains(&terms.anti_sniping_extension_seconds) {
            issues.push(issue(
                "payload.auction_terms.anti_sniping_extension_seconds",
                "Expected an extension between 0 and 3600 seconds",
            ));
        }
    }

    if issues.is_empty() {
        Ok(CommandPayload::RegisterListing(payload))
    } else {
        Err(issues)
    }
}

fn validate_reserve_inventory(
    payload: ReserveInventoryPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !(1..=1_000_000).contains(&payload.quantity) {
        issues.push(issue(
            "payload.quantity",
            "Expected a quantity between 1 and 1000000",
        ));
    }
    if !(60..=1_800).contains(&payload.reservation_ttl_seconds) {
        issues.push(issue(
            "payload.reservation_ttl_seconds",
            "Expected a reservation TTL between 60 and 1800 seconds",
        ));
    }
    if issues.is_empty() {
        Ok(CommandPayload::ReserveInventory(payload))
    } else {
        Err(issues)
    }
}

fn validate_create_checkout(
    mut payload: CreateCheckoutPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if payload.lines.is_empty() || payload.lines.len() > 50 {
        issues.push(issue(
            "payload.lines",
            "Expected between 1 and 50 checkout lines",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (index, line) in payload.lines.iter().enumerate() {
        if !aggregate_id_regex().is_match(&line.listing_aggregate_id) {
            issues.push(issue(
                &format!("payload.lines.{index}.listing_aggregate_id"),
                "Expected type:identifier aggregate format",
            ));
        }
        if !(1..=MAX_SAFE_INTEGER).contains(&line.expected_revision) {
            issues.push(issue(
                &format!("payload.lines.{index}.expected_revision"),
                "Expected a positive expected revision",
            ));
        }
        if !(1..=1_000_000).contains(&line.quantity) {
            issues.push(issue(
                &format!("payload.lines.{index}.quantity"),
                "Expected a quantity between 1 and 1000000",
            ));
        }
        if !seen.insert(line.listing_aggregate_id.clone()) {
            issues.push(issue(
                "payload.lines",
                "Checkout listing lines must be unique",
            ));
        }
    }

    let address = &mut payload.delivery_address;
    validate_trimmed(
        "payload.delivery_address.name",
        &mut address.name,
        1,
        100,
        &mut issues,
    );
    validate_trimmed(
        "payload.delivery_address.line1",
        &mut address.line1,
        1,
        200,
        &mut issues,
    );
    validate_trimmed(
        "payload.delivery_address.line2",
        &mut address.line2,
        0,
        200,
        &mut issues,
    );
    validate_trimmed(
        "payload.delivery_address.city",
        &mut address.city,
        1,
        100,
        &mut issues,
    );
    validate_trimmed(
        "payload.delivery_address.region",
        &mut address.region,
        1,
        100,
        &mut issues,
    );
    validate_trimmed(
        "payload.delivery_address.postal_code",
        &mut address.postal_code,
        1,
        32,
        &mut issues,
    );
    if !country_code_regex().is_match(&address.country_code) {
        issues.push(issue(
            "payload.delivery_address.country_code",
            "Expected an ISO 3166-1 alpha-2 country code",
        ));
    }
    if payload.guarantee_policy_version != 1 {
        issues.push(issue(
            "payload.guarantee_policy_version",
            "Expected guarantee policy version 1",
        ));
    }

    if issues.is_empty() {
        Ok(CommandPayload::CreateCheckout(payload))
    } else {
        Err(issues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register_command_json() -> Value {
        json!({
            "version": 1,
            "command_id": "018f47d2-6a27-7c23-a49d-6b21bb770120",
            "aggregate_id": format!("listing:{}_boots_01", "y".repeat(52)),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "listing.register",
            "payload": {
                "seller_pubky": "y".repeat(52),
                "listing_id": "boots_01",
                "listing_revision": 1,
                "content_hash": "a".repeat(64),
                "quantity": 1,
                "unit_price": { "amount_minor": 12_500, "currency": "USD", "exponent": 2 },
            },
        })
    }

    #[test]
    fn parses_a_valid_register_command_with_defaults() {
        let command = parse_command(&register_command_json()).expect("valid command");
        assert_eq!(command.kind(), "listing.register");
        let CommandPayload::RegisterListing(payload) = &command.payload else {
            panic!("expected register payload");
        };
        assert_eq!(payload.title, "Marketplace item");
        assert_eq!(payload.sale_format, SaleFormat::FixedPrice);
    }

    #[test]
    fn rejects_unknown_envelope_fields_without_echoing_values() {
        let mut raw = register_command_json();
        raw["private_address"] = json!("secret-address");
        let issues = parse_command(&raw).expect_err("unknown field must be rejected");
        let serialized = serde_json::to_string(&issues).expect("issues serialize");
        assert!(!serialized.contains("secret-address"));
    }

    #[test]
    fn rejects_unknown_payload_fields() {
        let mut raw = register_command_json();
        raw["payload"]["private_address"] = json!("secret-address");
        let issues = parse_command(&raw).expect_err("unknown payload field must be rejected");
        let serialized = serde_json::to_string(&issues).expect("issues serialize");
        assert!(!serialized.contains("secret-address"));
    }

    #[test]
    fn rejects_unsupported_versions_and_kinds() {
        let mut raw = register_command_json();
        raw["version"] = json!(2);
        let issues = parse_command(&raw).expect_err("version 2 unsupported");
        assert_eq!(issues[0].path, "version");

        let mut raw = register_command_json();
        raw["kind"] = json!("offer.create");
        let issues = parse_command(&raw).expect_err("kind not yet supported");
        assert_eq!(issues[0].path, "kind");
    }

    #[test]
    fn canonical_hash_ignores_key_order_and_timestamp_formatting() {
        let ordered = parse_command(&register_command_json()).expect("valid");

        let mut reordered = json!({
            "kind": "listing.register",
            "issued_at": "2026-08-19T22:00:00+00:00",
            "payload": register_command_json()["payload"].clone(),
            "expected_revision": 0,
            "aggregate_id": register_command_json()["aggregate_id"].clone(),
            "command_id": "018f47d2-6a27-7c23-a49d-6b21bb770120",
            "version": 1,
        });
        reordered["payload"]["title"] = json!("  Marketplace item  ");
        let equivalent = parse_command(&reordered).expect("valid");

        assert_eq!(ordered.request_hash(), equivalent.request_hash());
    }

    #[test]
    fn canonical_hash_differs_for_changed_payloads() {
        let original = parse_command(&register_command_json()).expect("valid");
        let mut changed_raw = register_command_json();
        changed_raw["payload"]["quantity"] = json!(2);
        let changed = parse_command(&changed_raw).expect("valid");
        assert_ne!(original.request_hash(), changed.request_hash());
    }

    #[test]
    fn validates_auction_terms_pairing() {
        let mut raw = register_command_json();
        raw["payload"]["sale_format"] = json!("auction");
        let issues = parse_command(&raw).expect_err("auction without terms invalid");
        assert!(issues.iter().any(|i| i.path == "payload.auction_terms"));

        let mut raw = register_command_json();
        raw["payload"]["auction_terms"] = json!({
            "starts_at": "2026-08-19T22:00:00.000Z",
            "ends_at": "2026-08-19T22:10:00.000Z",
            "minimum_increment": { "amount_minor": 500, "currency": "USD", "exponent": 2 },
            "anti_sniping_window_seconds": 60,
            "anti_sniping_extension_seconds": 120,
        });
        let issues = parse_command(&raw).expect_err("terms without auction format invalid");
        assert!(issues.iter().any(|i| i.path == "payload.auction_terms"));
    }
}
