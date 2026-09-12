use chrono::{DateTime, Utc};
use marketplace_domain::Money;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::FromRow;
use uuid::Uuid;

use crate::clock::format_timestamp;

/// Serde adapter for the canonical wire timestamp format (RFC 3339 with
/// milliseconds and `Z`), used inside the auction JSONB document.
mod ts_millis {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&crate::clock::format_timestamp(*value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// The auction sub-document stored in `listings.auction` (JSONB), matching
/// the prototype engine's listing aggregate shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuctionState {
    #[serde(with = "ts_millis")]
    pub starts_at: DateTime<Utc>,
    #[serde(with = "ts_millis")]
    pub ends_at: DateTime<Utc>,
    pub minimum_increment: Money,
    pub reserve_price: Option<Money>,
    pub anti_sniping_window_seconds: i64,
    pub anti_sniping_extension_seconds: i64,
    pub status: String,
    pub current_price: Money,
    pub leader_pubky: Option<String>,
    pub bid_count: i64,
    pub reserve_met: bool,
}

impl AuctionState {
    pub fn from_value(value: &Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value.clone())
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("auction state serializes infallibly")
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct ListingRow {
    pub aggregate_id: String,
    pub seller_pubky: String,
    pub listing_id: String,
    pub title: String,
    pub listing_revision: i64,
    pub content_hash: String,
    pub server_revision: i64,
    pub state: String,
    pub total_quantity: i64,
    pub available_quantity: i64,
    pub reserved_quantity: i64,
    pub sold_quantity: i64,
    pub unit_price_amount_minor: i64,
    pub unit_price_currency: String,
    pub unit_price_exponent: i32,
    /// Flat seller-signed shipping per order line, in the listing currency's
    /// minor units (0 = free / not configured).
    pub shipping_minor: i64,
    pub sale_format: String,
    pub auction: Option<Value>,
    /// The fulfillment methods the owner-signed listing record publishes
    /// (`fulfillmentMethods`, §A1): any of `shipping`/`pickup`, at least one.
    /// Public catalog data, like the rest of the listing view.
    pub fulfillment_methods: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

impl ListingRow {
    pub fn unit_price_json(&self) -> Value {
        money_json(
            self.unit_price_amount_minor,
            &self.unit_price_currency,
            self.unit_price_exponent,
        )
    }

    pub fn view(&self) -> Value {
        json!({
            "aggregate_id": self.aggregate_id,
            "seller_pubky": self.seller_pubky,
            "listing_id": self.listing_id,
            "title": self.title,
            "listing_revision": self.listing_revision,
            "content_hash": self.content_hash,
            "server_revision": self.server_revision,
            "state": self.state,
            "total_quantity": self.total_quantity,
            "available_quantity": self.available_quantity,
            "reserved_quantity": self.reserved_quantity,
            "sold_quantity": self.sold_quantity,
            "unit_price": self.unit_price_json(),
            "shipping": money_json(
                self.shipping_minor,
                &self.unit_price_currency,
                self.unit_price_exponent,
            ),
            "sale_format": self.sale_format,
            "auction": self.auction.clone().unwrap_or(Value::Null),
            "fulfillment_methods": self.fulfillment_methods,
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

/// A drop aggregate (ADR-0026): one timed, limited release synced from the
/// seller-signed homeserver record. `record_revision` tracks the record for
/// `drop.sync` convergence; `revision` is the server aggregate revision.
#[derive(Debug, Clone, FromRow)]
pub struct DropRow {
    pub aggregate_id: String,
    pub seller_pubky: String,
    pub drop_id: String,
    pub record_revision: i64,
    pub revision: i64,
    pub state: String,
    pub format: String,
    pub starts_at: DateTime<Utc>,
    pub ends_at: Option<DateTime<Utc>>,
    pub total_quantity: i64,
    pub per_buyer_limit: i64,
    pub remaining_quantity: i64,
    pub paid_quantity: i64,
    pub stock_display: String,
    pub listing_ids: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DropRow {
    pub fn view(&self) -> Value {
        json!({
            "aggregate_id": self.aggregate_id,
            "seller_pubky": self.seller_pubky,
            "drop_id": self.drop_id,
            "record_revision": self.record_revision,
            "revision": self.revision,
            "state": self.state,
            "format": self.format,
            "starts_at": format_timestamp(self.starts_at),
            "ends_at": self.ends_at.map(format_timestamp),
            "total_quantity": self.total_quantity,
            "per_buyer_limit": self.per_buyer_limit,
            "remaining_quantity": self.remaining_quantity,
            "paid_quantity": self.paid_quantity,
            "stock_display": self.stock_display,
            "listing_ids": self.listing_ids,
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct ReservationRow {
    pub id: Uuid,
    pub listing_aggregate_id: String,
    pub buyer_pubky: String,
    pub quantity: i64,
    pub status: String,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl ReservationRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "aggregate_id": self.listing_aggregate_id,
            "buyer_pubky": self.buyer_pubky,
            "quantity": self.quantity,
            "status": self.status,
            "expires_at": format_timestamp(self.expires_at),
            "created_at": format_timestamp(self.created_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct OfferRow {
    pub id: Uuid,
    pub aggregate_id: String,
    pub listing_aggregate_id: String,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub revision: i64,
    pub state: String,
    pub offered_by: String,
    pub amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub quantity: i64,
    pub message: String,
    pub history: Value,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OfferRow {
    pub fn amount_json(&self) -> Value {
        money_json(self.amount_minor, &self.currency, self.exponent)
    }

    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "aggregate_id": self.aggregate_id,
            "listing_aggregate_id": self.listing_aggregate_id,
            "buyer_pubky": self.buyer_pubky,
            "seller_pubky": self.seller_pubky,
            "revision": self.revision,
            "state": self.state,
            "offered_by": self.offered_by,
            "amount": self.amount_json(),
            "quantity": self.quantity,
            "message": self.message,
            "history": self.history,
            "expires_at": format_timestamp(self.expires_at),
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct BidRow {
    pub id: Uuid,
    pub listing_aggregate_id: String,
    pub bidder_pubky: String,
    pub maximum_amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub sequence: i64,
    pub created_at: DateTime<Utc>,
}

impl BidRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "listing_aggregate_id": self.listing_aggregate_id,
            "bidder_pubky": self.bidder_pubky,
            "maximum_amount": money_json(self.maximum_amount_minor, &self.currency, self.exponent),
            "sequence": self.sequence,
            "created_at": format_timestamp(self.created_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    /// Internal correlation to the winning auction, never serialized.
    pub auction_aggregate_id: Option<String>,
    /// Correlation to the drop this order's units were debited from (`NULL`
    /// for non-drop orders); the release paths credit exactly this drop.
    /// Serialized on order views/projections so both participants can tie
    /// the order to its drop.
    pub drop_aggregate_id: Option<String>,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub seller_has_rail: bool,
    pub revision: i64,
    pub state: String,
    pub lines: Value,
    pub delivery_address: Option<Value>,
    pub subtotal_minor: i64,
    pub shipping_minor: i64,
    pub total_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub guarantee_policy_version: i32,
    pub payment_id: Uuid,
    pub receipt_id: Option<Uuid>,
    /// The order's edition inside its drop (ADR-0026 layer 2): the value
    /// `paid_quantity` reached when this order's payment confirmed, assigned
    /// exactly once under the drop row lock. 1-based and gapless over paid
    /// orders; `NULL` for non-drop orders and for drop orders not yet paid.
    pub edition: Option<i32>,
    pub cancellation_reason: Option<String>,
    /// Whether this order currently holds `reserved` listing stock of its
    /// own ("only a payment locks an item"): set by a payment lock point —
    /// or at checkout for drop-bound orders (lock-at-claim) — cleared when
    /// confirmation converts the hold to sold, when a cancellation releases
    /// it, and when the hold window lapses. Auction orders never set it:
    /// their hold is the winning `reservations` row.
    pub stock_held: bool,
    /// Server-time bound on the hold: elapsing while the order is still
    /// pending cancels the order, expires the payment, and restocks.
    pub hold_expires_at: Option<DateTime<Utc>>,
    pub shipment: Option<Value>,
    /// TRUE only when the server-time `delivery_assume` transition marked
    /// the order delivered (DELIVERY_ASSUME_DAYS after shipment; there is
    /// no carrier tracking feed). A buyer-confirmed delivery stays FALSE,
    /// so the UI can tell an assumed delivery apart and ask the buyer to
    /// report a non-arrival.
    pub delivery_assumed: bool,
    pub return_request: Option<Value>,
    pub external_refund: Option<Value>,
    /// The buyer-bound payment method (`bitcoin` | `stripe` | `paypal`),
    /// NULL until bound.
    pub payment_method: Option<String>,
    /// Snapshot of the fiat checkout URL taken at binding time.
    pub fiat_checkout_url: Option<String>,
    /// When the buyer reported an out-of-band fiat payment (PayPal leg).
    pub payment_reported_at: Option<DateTime<Utc>>,
    /// Optional processor/transaction reference: the buyer-supplied PayPal
    /// transaction id, or the matched Stripe Checkout Session id.
    pub fiat_transaction_ref: Option<String>,
    /// Who verified the fiat payment that paid the order: `processor`
    /// (Stripe key lookup), `gateway` (verified PayPal IPN), or `seller`
    /// (manual confirm-received). NULL until verified.
    pub fiat_verified_by: Option<String>,
    /// A purchased Shippo label: SELLER-ONLY (the PDF embeds the buyer's
    /// address), deliberately absent from [`Self::view`] and served
    /// exclusively through the seller-scoped label endpoints.
    pub shipping_label: Option<Value>,
    /// Paykit payment-request reference for physical bitcoin orders
    /// (Crockford base32 of the order UUID; the status-lookup bundle id).
    pub paykit_request_reference: Option<String>,
    pub paykit_request_state: Option<String>,
    /// Poll stamp for the paykit verification worker; never serialized.
    pub paykit_last_checked_at: Option<DateTime<Utc>>,
    /// Two-phase paykit protocol (§B.11): the invoice phase 1 prepared.
    /// Internal correlation, never serialized.
    pub paykit_invoice_id: Option<Uuid>,
    /// The issuing stack's identity, from the phase-1 response body (never
    /// configuration); every later message on this invoice is checked
    /// against it. Never serialized.
    pub paykit_stack_id: Option<String>,
    /// The base URL the phase-1 call used, persisted in the bind
    /// transaction so activate/void route back to the issuing stack after a
    /// repoint. Never serialized.
    pub paykit_stack_endpoint: Option<String>,
    /// The figure the marketplace charges, displays and records:
    /// `amount_sats + nonce_sats` from phase 1 (R3-3).
    pub paykit_total_sats: Option<i64>,
    pub paykit_expires_at: Option<DateTime<Utc>>,
    pub paykit_prepare_expires_at: Option<DateTime<Utc>>,
    /// §B.8.8 allocation mode, persisted for W1.15/W1.16. Never serialized.
    pub paykit_allocation_mode: Option<String>,
    /// First 8 bytes of SHA-256 over the derived address, hex — never the
    /// address itself (§B.0). For the §D proofs; never serialized.
    pub paykit_address_fingerprint: Option<String>,
    /// Per-order phase-1 attempt counter (`{reference}:{attempt}` is the
    /// idempotency key). Never serialized.
    pub paykit_bind_attempt: i32,
    /// Two-phase activation state: `preparing` until the activation outbox
    /// row delivers, then `active`; `voided` when the prepare was voided or
    /// reaped and the bind released.
    pub paykit_activation_state: Option<String>,
    /// The latest Paykit observation as the status-only poll refreshed it
    /// (`{state, observed_sats, confirmations, amount_matched, txid,
    /// observed_at, disappeared}`). Facts for the seller's decision, never
    /// a transition input. Internal, never serialized.
    pub paykit_observation: Option<Value>,
    /// Server-clock entry into `awaiting_seller_confirmation` and the armed
    /// 24-hour seller-confirmation deadline (design §B.8.8). Cleared when
    /// the order leaves the state (the schema CHECK is a biconditional).
    /// Never serialized.
    pub paykit_seller_confirmation_entered_at: Option<DateTime<Utc>>,
    pub paykit_seller_confirmation_deadline: Option<DateTime<Utc>>,
    /// How this order reaches the buyer (§A2): exactly one of `shipping` |
    /// `pickup`. Required, not derivable: checkout splits one order per
    /// (seller, fulfillment), so reveal, shipping charge, and packing slip
    /// logic stay uniform per order.
    pub fulfillment: String,
    /// The first successful buyer reveal of the pickup details stamps the
    /// bounded withdrawal window (§A3); NULL until then.
    pub first_revealed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OrderRow {
    /// The participant-facing read projection: [`Self::view`] minus the
    /// delivery address, which is a private delivery detail that read
    /// projections must not expose (ADR-0019 §8). The buyer receives the
    /// address back once, in the checkout command result they authored.
    pub fn projection(&self) -> Value {
        let mut view = self.view();
        view.as_object_mut()
            .expect("order view is an object")
            .remove("delivery_address");
        view
    }

    /// How the bound fiat method is verified: `processor` (Stripe, via the
    /// seller's restricted key against the Stripe API), `gateway-notified`
    /// (PayPal, a postback-verified IPN from PayPal's servers paid the
    /// order), or `seller-attested` (PayPal fallback: buyer reports + seller
    /// confirms). The provenance asymmetry is deliberate and must stay
    /// visible to both parties.
    pub fn fiat_verification(&self) -> Value {
        match self.payment_method.as_deref() {
            Some("stripe") => json!("processor"),
            Some("paypal") if self.fiat_verified_by.as_deref() == Some("gateway") => {
                json!("gateway-notified")
            }
            Some("paypal") => json!("seller-attested"),
            _ => Value::Null,
        }
    }

    /// Which participant drives the next recorded transition, derived from
    /// the order state so a projection always shows who acts next (the
    /// current return step itself is `return_request.state`). Server-time
    /// transitions (delivery assumption, auto-completion, hold expiry) fire
    /// without either participant; `"delivered"` therefore has no pending
    /// actor. A pending payment with no bound rail is waiting on the seller
    /// to configure one, while a reported payment is waiting on seller
    /// confirmation.
    pub fn next_actor(&self) -> Option<&'static str> {
        next_actor_for_order(
            &self.state,
            self.payment_method.is_some(),
            self.payment_reported_at.is_some(),
            self.seller_has_rail,
        )
    }

    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "buyer_pubky": self.buyer_pubky,
            "seller_pubky": self.seller_pubky,
            "revision": self.revision,
            "state": self.state,
            "lines": self.lines,
            "delivery_address": self.delivery_address.clone().unwrap_or(Value::Null),
            "subtotal": money_json(self.subtotal_minor, &self.currency, self.exponent),
            "shipping": money_json(self.shipping_minor, &self.currency, self.exponent),
            // The buyer-facing total of a bitcoin-bound order is the
            // paykit total (price + nonce, §B.11.3): the figure the
            // marketplace must charge, display and record. A `preparing` or
            // `active` order always has it; an unbound order falls back to
            // the listing total.
            "total": money_json(
                self.paykit_total_sats.unwrap_or(self.total_minor),
                &self.currency,
                self.exponent,
            ),
            "guarantee_policy_version": self.guarantee_policy_version,
            "payment_id": self.payment_id,
            "receipt_id": self.receipt_id,
            "edition": self.edition,
            "drop_aggregate_id": self.drop_aggregate_id,
            "cancellation_reason": self.cancellation_reason,
            "stock_held": self.stock_held,
            "hold_expires_at": self.hold_expires_at.map(format_timestamp),
            "shipment": self.shipment.clone().unwrap_or(Value::Null),
            "delivery_assumed": self.delivery_assumed,
            "next_actor": self.next_actor(),
            "return_request": self.return_request.clone().unwrap_or(Value::Null),
            "external_refund": self.external_refund.clone().unwrap_or(Value::Null),
            "payment_method": self.payment_method,
            "fiat_checkout_url": self.fiat_checkout_url,
            "fiat_verification": self.fiat_verification(),
            "payment_reported_at": self.payment_reported_at.map(format_timestamp),
            "fiat_transaction_ref": self.fiat_transaction_ref,
            "paykit_request_reference": self.paykit_request_reference,
            "paykit_request_state": self.paykit_request_state,
            "paykit_activation_state": self.paykit_activation_state,
            "paykit_total_sats": self.paykit_total_sats,
            "fulfillment": self.fulfillment,
            "first_revealed_at": self.first_revealed_at.map(format_timestamp),
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

fn next_actor_for_order(
    state: &str,
    payment_method_bound: bool,
    payment_reported: bool,
    seller_has_rail: bool,
) -> Option<&'static str> {
    match state {
        "pending_payment" if payment_reported => Some("seller"),
        "pending_payment" if !payment_method_bound && !seller_has_rail => Some("seller"),
        "pending_payment" | "shipped" => Some("buyer"),
        // A pickup order in `paid` waits on the seller (mark ready, or
        // confirm the handover); in `ready_for_pickup` on the buyer
        // (confirm on receipt). The value set stays 'buyer' | 'seller'.
        "paid" | "processing" | "cancel_requested" | "return_requested" | "return_approved"
        | "return_received" => Some("seller"),
        "ready_for_pickup" => Some("buyer"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::next_actor_for_order;

    #[test]
    fn next_actor_matches_order_state_and_payment_facts() {
        assert_eq!(
            next_actor_for_order("pending_payment", false, false, false),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("pending_payment", false, false, true),
            Some("buyer")
        );
        assert_eq!(
            next_actor_for_order("pending_payment", true, false, false),
            Some("buyer")
        );
        assert_eq!(
            next_actor_for_order("pending_payment", true, true, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("pending_payment", false, true, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("shipped", true, false, true),
            Some("buyer")
        );
        assert_eq!(next_actor_for_order("delivered", true, false, true), None);
        assert_eq!(
            next_actor_for_order("paid", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("processing", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("return_requested", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("cancel_requested", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("return_approved", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("return_received", true, false, true),
            Some("seller")
        );
        assert_eq!(
            next_actor_for_order("ready_for_pickup", true, false, true),
            Some("buyer")
        );
        assert_eq!(next_actor_for_order("completed", true, false, true), None);
        assert_eq!(next_actor_for_order("cancelled", true, false, true), None);
        assert_eq!(
            next_actor_for_order("refunded_external", true, false, true),
            None
        );
        assert_eq!(next_actor_for_order("closed", true, false, true), None);
    }
}

/// A sealed pickup-details version row (§A1/§A3). Like the Locks
/// correlation row, this is internal state: it has NO `view()` — the
/// plaintext exists only inside `details_ciphertext`, and only the two
/// entitled reads (the seller's owner read and the paying buyer's reveal)
/// ever open the seal. A derived Debug prints bytes, never the secret.
#[derive(Debug, Clone, FromRow)]
pub struct PickupDetailsRow {
    pub aggregate_id: String,
    pub seller_pubky: String,
    pub version: i64,
    pub details_ciphertext: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The per-listing monotonic version counter in its own row (§A3): it
/// survives `pickup_details.clear`, so versions never restart and
/// terms-change detection cannot be fooled by a delete-and-recreate.
#[derive(Debug, Clone, FromRow)]
pub struct PickupVersionCounterRow {
    pub aggregate_id: String,
    pub seller_pubky: String,
    pub last_version: i64,
    /// Set by `pickup_details.clear`, reset by the next `set`: while set,
    /// retained versions are dispute exhibits, not current details.
    pub cleared_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// A sealed per-line pinned snapshot written inside `confirm_order`'s
/// receipt transaction (§A3): the details as shown at payment plus the
/// adapter that confirmed the payment. No `view()`: the buyer's reveal read
/// is the only open path, and it refuses snapshots pinned under
/// `payment.sandbox_advance` regardless of the current sandbox flag.
#[derive(Debug, Clone, FromRow)]
pub struct PickupLineSnapshotRow {
    pub order_id: Uuid,
    pub line_index: i32,
    pub listing_aggregate_id: String,
    pub version: i64,
    pub snapshot_ciphertext: Vec<u8>,
    pub confirming_adapter: String,
    pub created_at: DateTime<Utc>,
}

/// The pickup handover record (§A6): one row per order (PRIMARY KEY on
/// `order_id`), carrying who confirmed and the server instant. A
/// `confirmed_by = 'seller'` row is a seller-attested handover: reputation
/// counts the completion only on a buyer confirm or a dispute-free
/// auto-complete.
#[derive(Debug, Clone, FromRow)]
pub struct PickupHandoverRow {
    pub order_id: Uuid,
    pub confirmed_by: String,
    pub confirmed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ReceiptRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub payment_id: Uuid,
    pub issuer_pubky: String,
    pub recipient_pubky: String,
    pub total_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub content_hash: String,
    pub issued_at: DateTime<Utc>,
}

impl ReceiptRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "order_id": self.order_id,
            "payment_id": self.payment_id,
            "issuer_pubky": self.issuer_pubky,
            "recipient_pubky": self.recipient_pubky,
            "total": money_json(self.total_minor, &self.currency, self.exponent),
            "content_hash": self.content_hash,
            "issued_at": format_timestamp(self.issued_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct ReviewRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub reviewer_pubky: String,
    pub reviewer_role: String,
    pub subject_pubky: String,
    pub rating: i32,
    pub text: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ReviewRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "reviewer_pubky": self.reviewer_pubky,
            "reviewer_role": self.reviewer_role,
            "subject_pubky": self.subject_pubky,
            "rating": self.rating,
            "text": self.text,
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct PaymentRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub revision: i64,
    pub adapter: String,
    pub state: String,
    pub confirmations: i32,
    pub amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
    /// Stamped by EVERY transition into `manual_review` (schema CHECK);
    /// cleared when the payment leaves the state. The two-business-day
    /// seller-response SLA and the seven-day inactivity clock read it.
    pub manual_review_entered_at: Option<DateTime<Utc>>,
    /// The SLA breach alert fires once per manual-review entry.
    pub manual_review_sla_alerted_at: Option<DateTime<Utc>>,
    /// The resolution record (all NULL until resolved, schema CHECK):
    /// the Idempotency-Key / reaper-minted key, the outcome, the basis,
    /// who resolved, and the validated external refund reference.
    pub resolution_id: Option<Uuid>,
    pub resolution_outcome: Option<String>,
    pub resolution_basis: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_by_pubky: Option<String>,
    /// The validated external refund reference. Never serialized on the
    /// payment projection (the order's `external_refund` carries it).
    pub refund_reference: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PaymentRow {
    /// The participant-facing payment projection. The Locks bundle
    /// correlation, which ADR-0019 §8 forbids in exposed records (`access
    /// credentials or bundle_id`), no longer exists on the payment row at
    /// all: it lives encrypted in `payment_locks_correlations` and has no
    /// serialization path. The resolution outcome/basis ARE rendered
    /// (design §B.9 r12: projections distinguish a resolved payment from a
    /// naturally confirmed/expired one); the refund reference is not — the
    /// order's `external_refund` carries it to both participants.
    pub fn projection(&self) -> Value {
        json!({
            "id": self.id,
            "order_id": self.order_id,
            "buyer_pubky": self.buyer_pubky,
            "seller_pubky": self.seller_pubky,
            "revision": self.revision,
            "adapter": self.adapter,
            "state": self.state,
            "confirmations": self.confirmations,
            "amount": money_json(self.amount_minor, &self.currency, self.exponent),
            "resolution_outcome": self.resolution_outcome,
            "resolution_basis": self.resolution_basis,
            "resolved_at": self.resolved_at.map(format_timestamp),
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

/// The encrypted correlation between a payment/order and a Locks
/// verification lifecycle (ADR-0019 §7). This row is internal state: it has
/// no `view()`/`projection()` on purpose — no read projection, command
/// result, log, or metric serializes it, and the bundle id exists only as
/// `bundle_id_ciphertext`.
#[derive(Debug, Clone, FromRow)]
pub struct LocksCorrelationRow {
    pub id: Uuid,
    pub payment_id: Uuid,
    pub order_id: Uuid,
    pub buyer_pubky: String,
    pub creator_pubky: String,
    pub lock_resource_hash: String,
    pub amount_minor: i64,
    pub asset: String,
    pub exponent: i32,
    pub policy_version: i32,
    pub bundle_id_ciphertext: Vec<u8>,
    pub bundle_lookup_token: Vec<u8>,
    pub verification_state: String,
    pub window_expires_at: DateTime<Utc>,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_observed_status: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A delivered notification (outbox consumer row). Notifications are
/// immutable delivery records, not revisioned aggregates: there is no
/// notification command surface yet, so the projection carries no revision.
#[derive(Debug, Clone, FromRow)]
pub struct NotificationRow {
    pub id: Uuid,
    pub recipient_pubky: String,
    pub actor_pubky: String,
    #[sqlx(rename = "type")]
    pub notification_type: String,
    pub aggregate_id: String,
    /// Optional monetary context in the projections' money JSON shape,
    /// present only where the recipient already sees the figure in a
    /// role-scoped projection (ADR-0019 §8). NULL on rows delivered before
    /// amounts existed.
    pub amount: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

impl NotificationRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "recipient_pubky": self.recipient_pubky,
            "actor_pubky": self.actor_pubky,
            "type": self.notification_type,
            "aggregate_id": self.aggregate_id,
            "amount": self.amount,
            "created_at": format_timestamp(self.created_at),
            "read_at": self.read_at.map(format_timestamp),
        })
    }
}

pub fn money_json(amount_minor: i64, currency: &str, exponent: i32) -> Value {
    json!({
        "amount_minor": amount_minor,
        "currency": currency,
        "exponent": exponent,
    })
}
