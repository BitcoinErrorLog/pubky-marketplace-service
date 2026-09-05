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
            "total": money_json(self.total_minor, &self.currency, self.exponent),
            "guarantee_policy_version": self.guarantee_policy_version,
            "payment_id": self.payment_id,
            "receipt_id": self.receipt_id,
            "edition": self.edition,
            "drop_aggregate_id": self.drop_aggregate_id,
            "cancellation_reason": self.cancellation_reason,
            "stock_held": self.stock_held,
            "hold_expires_at": self.hold_expires_at.map(format_timestamp),
            "shipment": self.shipment.clone().unwrap_or(Value::Null),
            "return_request": self.return_request.clone().unwrap_or(Value::Null),
            "external_refund": self.external_refund.clone().unwrap_or(Value::Null),
            "payment_method": self.payment_method,
            "fiat_checkout_url": self.fiat_checkout_url,
            "fiat_verification": self.fiat_verification(),
            "payment_reported_at": self.payment_reported_at.map(format_timestamp),
            "fiat_transaction_ref": self.fiat_transaction_ref,
            "paykit_request_reference": self.paykit_request_reference,
            "paykit_request_state": self.paykit_request_state,
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PaymentRow {
    /// The participant-facing payment projection. The Locks bundle
    /// correlation, which ADR-0019 §8 forbids in exposed records (`access
    /// credentials or bundle_id`), no longer exists on the payment row at
    /// all: it lives encrypted in `payment_locks_correlations` and has no
    /// serialization path.
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
