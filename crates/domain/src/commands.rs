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
    CreateOffer(OfferTermsPayload),
    CounterOffer(CounterOfferPayload),
    AcceptOffer(OfferActionPayload),
    RejectOffer(OfferActionPayload),
    WithdrawOffer(OfferActionPayload),
    PlaceBid(PlaceBidPayload),
    CloseAuction(CloseAuctionPayload),
    AdvanceSandboxPayment(AdvanceSandboxPaymentPayload),
    RegisterLocks(RegisterLocksPayload),
    RequestCancellation(RequestCancellationPayload),
    ApproveCancellation(OrderActionPayload),
    ShipOrder(ShipOrderPayload),
    ConfirmDelivery(OrderActionPayload),
    RequestReturn(RequestReturnPayload),
    ApproveReturn(OrderActionPayload),
    ReceiveReturn(OrderActionPayload),
    RecordExternalRefund(RecordExternalRefundPayload),
    OpenDispute(OpenDisputePayload),
    AddDisputeEvidence(DisputeEvidencePayload),
    ResolveDispute(ResolveDisputePayload),
    CreateReview(ReviewTermsPayload),
    UpdateReview(ReviewTermsPayload),
    CreateReport(CreateReportPayload),
    DecideReport(DecideReportPayload),
}

impl Command {
    pub fn kind(&self) -> &'static str {
        match self.payload {
            CommandPayload::RegisterListing(_) => "listing.register",
            CommandPayload::ReserveInventory(_) => "inventory.reserve",
            CommandPayload::CreateCheckout(_) => "checkout.create",
            CommandPayload::CreateOffer(_) => "offer.create",
            CommandPayload::CounterOffer(_) => "offer.counter",
            CommandPayload::AcceptOffer(_) => "offer.accept",
            CommandPayload::RejectOffer(_) => "offer.reject",
            CommandPayload::WithdrawOffer(_) => "offer.withdraw",
            CommandPayload::PlaceBid(_) => "auction.place_bid",
            CommandPayload::CloseAuction(_) => "auction.close",
            CommandPayload::AdvanceSandboxPayment(_) => "payment.sandbox_advance",
            CommandPayload::RegisterLocks(_) => "payment.register_locks",
            CommandPayload::RequestCancellation(_) => "order.cancel_request",
            CommandPayload::ApproveCancellation(_) => "order.cancel_approve",
            CommandPayload::ShipOrder(_) => "fulfillment.ship",
            CommandPayload::ConfirmDelivery(_) => "fulfillment.confirm_delivery",
            CommandPayload::RequestReturn(_) => "return.request",
            CommandPayload::ApproveReturn(_) => "return.approve",
            CommandPayload::ReceiveReturn(_) => "return.receive",
            CommandPayload::RecordExternalRefund(_) => "refund.record_external",
            CommandPayload::OpenDispute(_) => "dispute.open",
            CommandPayload::AddDisputeEvidence(_) => "dispute.evidence",
            CommandPayload::ResolveDispute(_) => "dispute.resolve",
            CommandPayload::CreateReview(_) => "review.create",
            CommandPayload::UpdateReview(_) => "review.update",
            CommandPayload::CreateReport(_) => "trust.report",
            CommandPayload::DecideReport(_) => "trust.decide",
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
            CommandPayload::CreateOffer(p) => serde_json::to_value(p),
            CommandPayload::CounterOffer(p) => serde_json::to_value(p),
            CommandPayload::AcceptOffer(p)
            | CommandPayload::RejectOffer(p)
            | CommandPayload::WithdrawOffer(p) => serde_json::to_value(p),
            CommandPayload::PlaceBid(p) => serde_json::to_value(p),
            CommandPayload::CloseAuction(p) => serde_json::to_value(p),
            CommandPayload::AdvanceSandboxPayment(p) => serde_json::to_value(p),
            CommandPayload::RegisterLocks(p) => serde_json::to_value(p),
            CommandPayload::RequestCancellation(p) => serde_json::to_value(p),
            CommandPayload::ShipOrder(p) => serde_json::to_value(p),
            CommandPayload::ConfirmDelivery(p)
            | CommandPayload::ApproveCancellation(p)
            | CommandPayload::ApproveReturn(p)
            | CommandPayload::ReceiveReturn(p) => serde_json::to_value(p),
            CommandPayload::RequestReturn(p) => serde_json::to_value(p),
            CommandPayload::RecordExternalRefund(p) => serde_json::to_value(p),
            CommandPayload::OpenDispute(p) => serde_json::to_value(p),
            CommandPayload::AddDisputeEvidence(p) => serde_json::to_value(p),
            CommandPayload::ResolveDispute(p) => serde_json::to_value(p),
            CommandPayload::CreateReview(p) | CommandPayload::UpdateReview(p) => {
                serde_json::to_value(p)
            }
            CommandPayload::CreateReport(p) => serde_json::to_value(p),
            CommandPayload::DecideReport(p) => serde_json::to_value(p),
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

/// Offer terms shared by `offer.create` and `offer.counter`
/// (`offerTermsSchema` in the prototype contracts).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferTermsPayload {
    pub amount: Money,
    pub quantity: i64,
    pub expires_in_seconds: i64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CounterOfferPayload {
    pub offer_id: Uuid,
    pub amount: Money,
    pub quantity: i64,
    pub expires_in_seconds: i64,
    pub message: String,
}

/// Payload for `offer.accept`, `offer.reject`, and `offer.withdraw`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferActionPayload {
    pub offer_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceBidPayload {
    pub maximum_amount: Money,
}

/// `auction.close` carries an empty payload; unknown fields are rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseAuctionPayload {}

/// Sandbox payment target states (`payment.sandbox_advance`). The sandbox
/// adapter records these transitions; it never observes or moves funds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPaymentTarget {
    Detected,
    Confirmed,
    Expired,
    ManualReview,
}

impl SandboxPaymentTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            SandboxPaymentTarget::Detected => "detected",
            SandboxPaymentTarget::Confirmed => "confirmed",
            SandboxPaymentTarget::Expired => "expired",
            SandboxPaymentTarget::ManualReview => "manual_review",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvanceSandboxPaymentPayload {
    pub payment_id: Uuid,
    pub target: SandboxPaymentTarget,
    pub confirmations: i64,
}

/// Registers the Locks lifecycle correlation for a payment
/// (`payment.register_locks`, buyer only). The bundle id is the buyer's
/// cryptographically random lifecycle handle, a bearer secret: the service
/// encrypts it at rest, never returns it in any response, and uses it only
/// to independently verify the Locks lifecycle server-side. Registration
/// never advances the payment — only verified Locks completion does.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterLocksPayload {
    pub payment_id: Uuid,
    /// Canonical 26-character uppercase Crockford-base32 encoding of the
    /// 128-bit viewer-generated bundle identity (the Locks `BundleId` wire
    /// form).
    pub bundle_id: String,
    /// The addressed public lock resource,
    /// `<creator>/pub/locks.app/<lock_id>.json`, whose creator must be the
    /// order's seller.
    pub pubky_lock_resource: String,
}

/// The bundle id is a bearer secret and the lock resource is correlation
/// material (ADR-0019 §8): neither may reach logs through a derived Debug.
impl std::fmt::Debug for RegisterLocksPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterLocksPayload")
            .field("payment_id", &self.payment_id)
            .field("bundle_id", &"<redacted>")
            .field("pubky_lock_resource", &"<redacted>")
            .finish()
    }
}

/// The Locks content-lock path prefix inside a pubky lock resource.
pub const LOCKS_CONTENT_LOCK_PREFIX: &str = "/pub/locks.app/";

/// Splits an addressed lock resource into `(creator, lock_id)` when it has
/// the canonical form `<z-base-32 creator>/pub/locks.app/<lock_id>.json`
/// with a 52-character canonical Crockford lock id.
pub fn parse_lock_resource(resource: &str) -> Option<(&str, &str)> {
    let (creator, path) = resource.split_at(resource.find(LOCKS_CONTENT_LOCK_PREFIX)?);
    let lock_id = path
        .strip_prefix(LOCKS_CONTENT_LOCK_PREFIX)?
        .strip_suffix(".json")?;
    if is_valid_pubky(creator) && crockford_id_regex_52().is_match(lock_id) {
        Some((creator, lock_id))
    } else {
        None
    }
}

/// Payload shared by the order commands that carry only the order id
/// (`order.cancel_approve`, `fulfillment.confirm_delivery`,
/// `return.approve`, `return.receive`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderActionPayload {
    pub order_id: Uuid,
}

/// Buyer cancellation request (`order.cancel_request`). From
/// `pending_payment` the cancel applies immediately; from `paid` or
/// `processing` it moves the order to `cancel_requested` awaiting the
/// seller's `order.cancel_approve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestCancellationPayload {
    pub order_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShipOrderPayload {
    pub order_id: Uuid,
    pub carrier: String,
    pub tracking_number: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestReturnPayload {
    pub order_id: Uuid,
    pub reason: String,
    pub requested_amount_minor: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordExternalRefundPayload {
    pub order_id: Uuid,
    pub amount_minor: i64,
    pub transaction_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisputeRemedy {
    Refund,
    PartialRefund,
    Replacement,
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenDisputePayload {
    pub order_id: Uuid,
    pub reason: String,
    pub requested_remedy: DisputeRemedy,
}

/// Evidence attached to an open dispute (`dispute.evidence`, this service
/// only). The body is stored append-only and is never echoed back in any
/// response (ADR-0019 §8: order evidence stays private).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisputeEvidencePayload {
    pub order_id: Uuid,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisputeResolution {
    BuyerRefund,
    PartialRefund,
    SellerFavor,
    Replacement,
}

impl DisputeResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            DisputeResolution::BuyerRefund => "buyer_refund",
            DisputeResolution::PartialRefund => "partial_refund",
            DisputeResolution::SellerFavor => "seller_favor",
            DisputeResolution::Replacement => "replacement",
        }
    }

    /// Buyer remedies leave the order disputed awaiting the external refund;
    /// the others complete the order (prototype engine semantics).
    pub fn favors_buyer(self) -> bool {
        matches!(
            self,
            DisputeResolution::BuyerRefund | DisputeResolution::PartialRefund
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveDisputePayload {
    pub order_id: Uuid,
    pub resolution: DisputeResolution,
    pub rationale: String,
}

/// Review terms shared by `review.create` and `review.update` (the update
/// command is this service only; the prototype had no review editing).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewTermsPayload {
    pub order_id: Uuid,
    pub rating: i64,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportTargetType {
    Listing,
    User,
    Message,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportReason {
    ProhibitedItem,
    Counterfeit,
    Scam,
    Harassment,
    Unsafe,
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateReportPayload {
    pub target_type: ReportTargetType,
    pub target_id: String,
    pub reason: ReportReason,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportDecision {
    Dismissed,
    Actioned,
}

impl ReportDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            ReportDecision::Dismissed => "dismissed",
            ReportDecision::Actioned => "actioned",
        }
    }
}

/// Moderator decision on a report (`trust.decide`, this service only; the
/// prototype engine has no report decisions).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecideReportPayload {
    pub report_id: Uuid,
    pub decision: ReportDecision,
    pub rationale: String,
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

/// Canonical uppercase Crockford base32 (no I, L, O, U), 26 characters: the
/// Locks `BundleId` wire encoding of 16 bytes. Only the canonical form is
/// accepted so one bundle identity cannot alias two lookup tokens.
fn crockford_bundle_id_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[0-9ABCDEFGHJKMNPQRSTVWXYZ]{26}$").expect("valid regex"))
}

/// Canonical uppercase Crockford base32, 52 characters: the Locks `LockId`
/// wire encoding of a 32-byte lock hash.
fn crockford_id_regex_52() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[0-9ABCDEFGHJKMNPQRSTVWXYZ]{52}$").expect("valid regex"))
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
        "offer.create" => parse_payload(&envelope.payload).and_then(validate_create_offer)?,
        "offer.counter" => parse_payload(&envelope.payload).and_then(validate_counter_offer)?,
        "offer.accept" => parse_payload(&envelope.payload).map(CommandPayload::AcceptOffer)?,
        "offer.reject" => parse_payload(&envelope.payload).map(CommandPayload::RejectOffer)?,
        "offer.withdraw" => parse_payload(&envelope.payload).map(CommandPayload::WithdrawOffer)?,
        "auction.place_bid" => parse_payload(&envelope.payload).and_then(validate_place_bid)?,
        "auction.close" => parse_payload(&envelope.payload).map(CommandPayload::CloseAuction)?,
        "payment.sandbox_advance" => {
            parse_payload(&envelope.payload).and_then(validate_advance_sandbox_payment)?
        }
        "payment.register_locks" => {
            parse_payload(&envelope.payload).and_then(validate_register_locks)?
        }
        "order.cancel_request" => {
            parse_payload(&envelope.payload).and_then(validate_request_cancellation)?
        }
        "order.cancel_approve" => {
            parse_payload(&envelope.payload).map(CommandPayload::ApproveCancellation)?
        }
        "fulfillment.ship" => parse_payload(&envelope.payload).and_then(validate_ship_order)?,
        "fulfillment.confirm_delivery" => {
            parse_payload(&envelope.payload).map(CommandPayload::ConfirmDelivery)?
        }
        "return.request" => parse_payload(&envelope.payload).and_then(validate_request_return)?,
        "return.approve" => parse_payload(&envelope.payload).map(CommandPayload::ApproveReturn)?,
        "return.receive" => parse_payload(&envelope.payload).map(CommandPayload::ReceiveReturn)?,
        "refund.record_external" => {
            parse_payload(&envelope.payload).and_then(validate_record_external_refund)?
        }
        "dispute.open" => parse_payload(&envelope.payload).and_then(validate_open_dispute)?,
        "dispute.evidence" => {
            parse_payload(&envelope.payload).and_then(validate_dispute_evidence)?
        }
        "dispute.resolve" => parse_payload(&envelope.payload).and_then(validate_resolve_dispute)?,
        "review.create" => parse_payload(&envelope.payload)
            .and_then(|payload| validate_review_terms(payload, CommandPayload::CreateReview))?,
        "review.update" => parse_payload(&envelope.payload)
            .and_then(|payload| validate_review_terms(payload, CommandPayload::UpdateReview))?,
        "trust.report" => parse_payload(&envelope.payload).and_then(validate_create_report)?,
        "trust.decide" => parse_payload(&envelope.payload).and_then(validate_decide_report)?,
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

/// One week, the maximum offer lifetime accepted by the prototype contracts.
const MAX_OFFER_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;

fn validate_offer_terms(
    amount: &Money,
    quantity: i64,
    expires_in_seconds: i64,
    message: &mut String,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_positive_money("payload.amount", amount, issues);
    if !(1..=1_000_000).contains(&quantity) {
        issues.push(issue(
            "payload.quantity",
            "Expected a quantity between 1 and 1000000",
        ));
    }
    if !(300..=MAX_OFFER_TTL_SECONDS).contains(&expires_in_seconds) {
        issues.push(issue(
            "payload.expires_in_seconds",
            "Expected an offer lifetime between 300 and 604800 seconds",
        ));
    }
    validate_trimmed("payload.message", message, 0, 500, issues);
}

fn validate_create_offer(
    mut payload: OfferTermsPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_offer_terms(
        &payload.amount,
        payload.quantity,
        payload.expires_in_seconds,
        &mut payload.message,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::CreateOffer(payload))
    } else {
        Err(issues)
    }
}

fn validate_counter_offer(
    mut payload: CounterOfferPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_offer_terms(
        &payload.amount,
        payload.quantity,
        payload.expires_in_seconds,
        &mut payload.message,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::CounterOffer(payload))
    } else {
        Err(issues)
    }
}

fn validate_place_bid(payload: PlaceBidPayload) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_positive_money(
        "payload.maximum_amount",
        &payload.maximum_amount,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::PlaceBid(payload))
    } else {
        Err(issues)
    }
}

fn validate_advance_sandbox_payment(
    payload: AdvanceSandboxPaymentPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !(0..=6).contains(&payload.confirmations) {
        issues.push(issue(
            "payload.confirmations",
            "Expected between 0 and 6 confirmations",
        ));
    }
    if issues.is_empty() {
        Ok(CommandPayload::AdvanceSandboxPayment(payload))
    } else {
        Err(issues)
    }
}

/// Issues never echo payload values; for this payload that rule protects
/// bearer material (the bundle id), so messages describe only the expected
/// shape.
fn validate_register_locks(
    payload: RegisterLocksPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !crockford_bundle_id_regex().is_match(&payload.bundle_id) {
        issues.push(issue(
            "payload.bundle_id",
            "Expected a canonical 26-character Crockford-base32 bundle id",
        ));
    }
    if parse_lock_resource(&payload.pubky_lock_resource).is_none() {
        issues.push(issue(
            "payload.pubky_lock_resource",
            "Expected <creator>/pub/locks.app/<lock_id>.json with a z-base-32 \
             creator and a canonical 52-character Crockford lock id",
        ));
    }
    if issues.is_empty() {
        Ok(CommandPayload::RegisterLocks(payload))
    } else {
        Err(issues)
    }
}

fn validate_request_cancellation(
    mut payload: RequestCancellationPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed("payload.reason", &mut payload.reason, 1, 500, &mut issues);
    if issues.is_empty() {
        Ok(CommandPayload::RequestCancellation(payload))
    } else {
        Err(issues)
    }
}

fn validate_ship_order(
    mut payload: ShipOrderPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed("payload.carrier", &mut payload.carrier, 1, 100, &mut issues);
    validate_trimmed(
        "payload.tracking_number",
        &mut payload.tracking_number,
        1,
        200,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::ShipOrder(payload))
    } else {
        Err(issues)
    }
}

fn validate_request_return(
    mut payload: RequestReturnPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed("payload.reason", &mut payload.reason, 1, 1_000, &mut issues);
    if !(1..=MAX_SAFE_INTEGER).contains(&payload.requested_amount_minor) {
        issues.push(issue(
            "payload.requested_amount_minor",
            "Expected a positive requested amount",
        ));
    }
    if issues.is_empty() {
        Ok(CommandPayload::RequestReturn(payload))
    } else {
        Err(issues)
    }
}

fn validate_record_external_refund(
    mut payload: RecordExternalRefundPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !(1..=MAX_SAFE_INTEGER).contains(&payload.amount_minor) {
        issues.push(issue(
            "payload.amount_minor",
            "Expected a positive refund amount",
        ));
    }
    // The transaction id is the independently supplied external evidence; a
    // refund cannot be recorded without it (ADR-0019 §7).
    validate_trimmed(
        "payload.transaction_id",
        &mut payload.transaction_id,
        8,
        200,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::RecordExternalRefund(payload))
    } else {
        Err(issues)
    }
}

fn validate_open_dispute(
    mut payload: OpenDisputePayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed("payload.reason", &mut payload.reason, 1, 2_000, &mut issues);
    if issues.is_empty() {
        Ok(CommandPayload::OpenDispute(payload))
    } else {
        Err(issues)
    }
}

fn validate_dispute_evidence(
    mut payload: DisputeEvidencePayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed("payload.body", &mut payload.body, 1, 2_000, &mut issues);
    if issues.is_empty() {
        Ok(CommandPayload::AddDisputeEvidence(payload))
    } else {
        Err(issues)
    }
}

fn validate_resolve_dispute(
    mut payload: ResolveDisputePayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed(
        "payload.rationale",
        &mut payload.rationale,
        1,
        2_000,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::ResolveDispute(payload))
    } else {
        Err(issues)
    }
}

fn validate_review_terms(
    mut payload: ReviewTermsPayload,
    wrap: fn(ReviewTermsPayload) -> CommandPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if !(1..=5).contains(&payload.rating) {
        issues.push(issue("payload.rating", "Expected a rating between 1 and 5"));
    }
    validate_trimmed("payload.text", &mut payload.text, 1, 5_000, &mut issues);
    if issues.is_empty() {
        Ok(wrap(payload))
    } else {
        Err(issues)
    }
}

fn validate_create_report(
    mut payload: CreateReportPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    if payload.target_id.is_empty() || payload.target_id.chars().count() > 300 {
        issues.push(issue(
            "payload.target_id",
            "Expected between 1 and 300 characters",
        ));
    }
    validate_trimmed(
        "payload.details",
        &mut payload.details,
        1,
        2_000,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::CreateReport(payload))
    } else {
        Err(issues)
    }
}

fn validate_decide_report(
    mut payload: DecideReportPayload,
) -> Result<CommandPayload, Vec<ValidationIssue>> {
    let mut issues = Vec::new();
    validate_trimmed(
        "payload.rationale",
        &mut payload.rationale,
        1,
        2_000,
        &mut issues,
    );
    if issues.is_empty() {
        Ok(CommandPayload::DecideReport(payload))
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
        raw["kind"] = json!("message.send");
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

    fn offer_command_json() -> Value {
        json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000000500",
            "aggregate_id": format!("listing:{}_boots_01", "y".repeat(52)),
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.create",
            "payload": {
                "amount": { "amount_minor": 10_000, "currency": "USD", "exponent": 2 },
                "quantity": 1,
                "expires_in_seconds": 3_600,
                "message": "Would you take this?",
            },
        })
    }

    #[test]
    fn parses_offer_lifecycle_commands() {
        let created = parse_command(&offer_command_json()).expect("valid offer.create");
        assert_eq!(created.kind(), "offer.create");

        let mut counter = offer_command_json();
        counter["kind"] = json!("offer.counter");
        counter["aggregate_id"] = json!("offer:00000000-0000-4000-8000-000000000500");
        counter["payload"]["offer_id"] = json!("00000000-0000-4000-8000-000000000500");
        assert_eq!(
            parse_command(&counter).expect("valid offer.counter").kind(),
            "offer.counter"
        );

        for kind in ["offer.accept", "offer.reject", "offer.withdraw"] {
            let mut action = offer_command_json();
            action["kind"] = json!(kind);
            action["aggregate_id"] = json!("offer:00000000-0000-4000-8000-000000000500");
            action["payload"] = json!({ "offer_id": "00000000-0000-4000-8000-000000000500" });
            assert_eq!(parse_command(&action).expect("valid action").kind(), kind);
        }
    }

    #[test]
    fn rejects_out_of_range_offer_terms() {
        let mut short = offer_command_json();
        short["payload"]["expires_in_seconds"] = json!(299);
        let issues = parse_command(&short).expect_err("lifetime below 300s invalid");
        assert!(issues
            .iter()
            .any(|i| i.path == "payload.expires_in_seconds"));

        let mut long_message = offer_command_json();
        long_message["payload"]["message"] = json!("x".repeat(501));
        let issues = parse_command(&long_message).expect_err("501-char message invalid");
        assert!(issues.iter().any(|i| i.path == "payload.message"));

        let mut negative = offer_command_json();
        negative["payload"]["amount"]["amount_minor"] = json!(0);
        let issues = parse_command(&negative).expect_err("zero amount invalid");
        assert!(issues
            .iter()
            .any(|i| i.path == "payload.amount.amount_minor"));
    }

    #[test]
    fn parses_auction_bid_and_close_commands() {
        let mut bid = offer_command_json();
        bid["kind"] = json!("auction.place_bid");
        bid["payload"] = json!({
            "maximum_amount": { "amount_minor": 10_000, "currency": "USD", "exponent": 2 },
        });
        assert_eq!(
            parse_command(&bid).expect("valid bid").kind(),
            "auction.place_bid"
        );

        let mut close = offer_command_json();
        close["kind"] = json!("auction.close");
        close["payload"] = json!({});
        assert_eq!(
            parse_command(&close).expect("valid close").kind(),
            "auction.close"
        );

        let mut close_extra = offer_command_json();
        close_extra["kind"] = json!("auction.close");
        close_extra["payload"] = json!({ "private_address": "secret-address" });
        let issues = parse_command(&close_extra).expect_err("close payload must be empty");
        let serialized = serde_json::to_string(&issues).expect("issues serialize");
        assert!(!serialized.contains("secret-address"));
    }

    #[test]
    fn parses_trust_report_and_decide_commands() {
        let mut report = offer_command_json();
        report["kind"] = json!("trust.report");
        report["aggregate_id"] = json!("report:00000000-0000-4000-8000-000000000500");
        report["expected_revision"] = json!(0);
        report["payload"] = json!({
            "target_type": "listing",
            "target_id": format!("listing:{}_boots_01", "y".repeat(52)),
            "reason": "counterfeit",
            "details": "Brand markings appear inconsistent.",
        });
        assert_eq!(
            parse_command(&report).expect("valid report").kind(),
            "trust.report"
        );

        let mut bad_reason = report.clone();
        bad_reason["payload"]["reason"] = json!("not-a-reason");
        parse_command(&bad_reason).expect_err("unknown reason invalid");

        let mut decide = offer_command_json();
        decide["kind"] = json!("trust.decide");
        decide["aggregate_id"] = json!("report:00000000-0000-4000-8000-000000000500");
        decide["payload"] = json!({
            "report_id": "00000000-0000-4000-8000-000000000500",
            "decision": "actioned",
            "rationale": "Listing removed after review.",
        });
        assert_eq!(
            parse_command(&decide).expect("valid decide").kind(),
            "trust.decide"
        );
    }

    fn order_command_json(kind: &str, payload: Value) -> Value {
        json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000001201",
            "aggregate_id": "order:00000000-0000-4000-8000-00000000aaaa",
            "expected_revision": 2,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": kind,
            "payload": payload,
        })
    }

    #[test]
    fn parses_payment_and_fulfillment_commands() {
        let advance = order_command_json(
            "payment.sandbox_advance",
            json!({
                "payment_id": "00000000-0000-4000-8000-00000000bbbb",
                "target": "confirmed",
                "confirmations": 1,
            }),
        );
        assert_eq!(
            parse_command(&advance).expect("valid advance").kind(),
            "payment.sandbox_advance"
        );

        let mut too_many = advance.clone();
        too_many["payload"]["confirmations"] = json!(7);
        let issues = parse_command(&too_many).expect_err("7 confirmations invalid");
        assert!(issues.iter().any(|i| i.path == "payload.confirmations"));

        let ship = order_command_json(
            "fulfillment.ship",
            json!({
                "order_id": "00000000-0000-4000-8000-00000000aaaa",
                "carrier": " Sandbox Post ",
                "tracking_number": "TRACK-123",
            }),
        );
        let command = parse_command(&ship).expect("valid ship");
        let CommandPayload::ShipOrder(payload) = &command.payload else {
            panic!("expected ship payload");
        };
        assert_eq!(payload.carrier, "Sandbox Post");

        let confirm = order_command_json(
            "fulfillment.confirm_delivery",
            json!({ "order_id": "00000000-0000-4000-8000-00000000aaaa" }),
        );
        assert_eq!(
            parse_command(&confirm).expect("valid confirm").kind(),
            "fulfillment.confirm_delivery"
        );
    }

    #[test]
    fn parses_cancellation_return_refund_dispute_and_review_commands() {
        let order_id = "00000000-0000-4000-8000-00000000aaaa";
        for (kind, payload) in [
            (
                "order.cancel_request",
                json!({ "order_id": order_id, "reason": "Changed mind" }),
            ),
            ("order.cancel_approve", json!({ "order_id": order_id })),
            (
                "return.request",
                json!({ "order_id": order_id, "reason": "Differs", "requested_amount_minor": 14_796 }),
            ),
            ("return.approve", json!({ "order_id": order_id })),
            ("return.receive", json!({ "order_id": order_id })),
            (
                "refund.record_external",
                json!({ "order_id": order_id, "amount_minor": 14_796, "transaction_id": "bitcoin-tx-evidence-123" }),
            ),
            (
                "dispute.open",
                json!({ "order_id": order_id, "reason": "No response", "requested_remedy": "refund" }),
            ),
            (
                "dispute.evidence",
                json!({ "order_id": order_id, "body": "Carrier photo reference 42." }),
            ),
            (
                "dispute.resolve",
                json!({ "order_id": order_id, "resolution": "buyer_refund", "rationale": "Evidence supports the buyer." }),
            ),
            (
                "review.create",
                json!({ "order_id": order_id, "rating": 5, "text": "Accurate and fast." }),
            ),
            (
                "review.update",
                json!({ "order_id": order_id, "rating": 4, "text": "Revised after wear." }),
            ),
        ] {
            let command = order_command_json(kind, payload);
            assert_eq!(parse_command(&command).expect("valid command").kind(), kind);
        }
    }

    #[test]
    fn rejects_refunds_without_evidence_and_out_of_range_reviews() {
        let order_id = "00000000-0000-4000-8000-00000000aaaa";
        let missing_evidence = order_command_json(
            "refund.record_external",
            json!({ "order_id": order_id, "amount_minor": 100, "transaction_id": "short" }),
        );
        let issues = parse_command(&missing_evidence).expect_err("evidence under 8 chars invalid");
        assert!(issues.iter().any(|i| i.path == "payload.transaction_id"));

        let zero_rating = order_command_json(
            "review.create",
            json!({ "order_id": order_id, "rating": 0, "text": "x" }),
        );
        let issues = parse_command(&zero_rating).expect_err("rating 0 invalid");
        assert!(issues.iter().any(|i| i.path == "payload.rating"));

        let bad_remedy = order_command_json(
            "dispute.open",
            json!({ "order_id": order_id, "reason": "r", "requested_remedy": "escrow" }),
        );
        parse_command(&bad_remedy).expect_err("unknown remedy invalid");
    }

    const TEST_BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TEST_LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    fn register_locks_command_json() -> Value {
        order_command_json(
            "payment.register_locks",
            json!({
                "payment_id": "00000000-0000-4000-8000-00000000bbbb",
                "bundle_id": TEST_BUNDLE_ID,
                "pubky_lock_resource":
                    format!("{}/pub/locks.app/{TEST_LOCK_ID}.json", "y".repeat(52)),
            }),
        )
    }

    #[test]
    fn parses_a_valid_locks_registration() {
        let command = parse_command(&register_locks_command_json()).expect("valid registration");
        assert_eq!(command.kind(), "payment.register_locks");
        let CommandPayload::RegisterLocks(payload) = &command.payload else {
            panic!("expected register-locks payload");
        };
        let (creator, lock_id) =
            parse_lock_resource(&payload.pubky_lock_resource).expect("resource parses");
        assert_eq!(creator, "y".repeat(52));
        assert_eq!(lock_id, TEST_LOCK_ID);
    }

    #[test]
    fn rejects_non_canonical_locks_identifiers_without_echoing_them() {
        for bundle in [
            "",
            "short",
            &TEST_BUNDLE_ID.to_lowercase(), // lowercase is non-canonical
            &format!("{}L", &TEST_BUNDLE_ID[..25]), // L is not Crockford
            &format!("{}X", TEST_BUNDLE_ID), // wrong length
        ] {
            let mut raw = register_locks_command_json();
            raw["payload"]["bundle_id"] = json!(bundle);
            let issues = parse_command(&raw).expect_err("non-canonical bundle id invalid");
            assert!(issues.iter().any(|i| i.path == "payload.bundle_id"));
            let serialized = serde_json::to_string(&issues).expect("issues serialize");
            assert!(!serialized.contains(TEST_BUNDLE_ID));
        }

        for resource in [
            "",
            "not-a-resource",
            &format!("{}/pub/other.app/{TEST_LOCK_ID}.json", "y".repeat(52)),
            &format!("{}/pub/locks.app/{TEST_LOCK_ID}", "y".repeat(52)),
            &format!("{}/pub/locks.app/short.json", "y".repeat(52)),
            &format!("UPPER/pub/locks.app/{TEST_LOCK_ID}.json"),
        ] {
            let mut raw = register_locks_command_json();
            raw["payload"]["pubky_lock_resource"] = json!(resource);
            let issues = parse_command(&raw).expect_err("malformed lock resource invalid");
            assert!(issues
                .iter()
                .any(|i| i.path == "payload.pubky_lock_resource"));
        }
    }

    #[test]
    fn register_locks_debug_redacts_bearer_material() {
        let command = parse_command(&register_locks_command_json()).expect("valid registration");
        let debug = format!("{:?}", command.payload);
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(TEST_BUNDLE_ID));
        assert!(!debug.contains(TEST_LOCK_ID));
    }

    #[test]
    fn rejects_out_of_range_cancellation_reasons() {
        let order_id = "00000000-0000-4000-8000-00000000aaaa";
        for reason in ["", "  ", &"x".repeat(501)] {
            let command = order_command_json(
                "order.cancel_request",
                json!({ "order_id": order_id, "reason": reason }),
            );
            let issues = parse_command(&command).expect_err("out-of-range reason invalid");
            assert!(issues.iter().any(|i| i.path == "payload.reason"));
        }

        let extra_field = order_command_json(
            "order.cancel_approve",
            json!({ "order_id": order_id, "private_address": "secret-address" }),
        );
        let issues = parse_command(&extra_field).expect_err("unknown payload field invalid");
        let serialized = serde_json::to_string(&issues).expect("issues serialize");
        assert!(!serialized.contains("secret-address"));
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
