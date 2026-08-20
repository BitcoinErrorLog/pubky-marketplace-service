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
            "sale_format": self.sale_format,
            "auction": self.auction.clone().unwrap_or(Value::Null),
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
pub struct ReportRow {
    pub id: Uuid,
    pub reporter_pubky: String,
    pub target_type: String,
    pub target_id: String,
    pub reason: String,
    pub details: String,
    pub state: String,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ReportRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "reporter_pubky": self.reporter_pubky,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "reason": self.reason,
            "details": self.details,
            "state": self.state,
            "revision": self.revision,
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        })
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    /// Internal correlation to the winning auction, never serialized.
    pub auction_aggregate_id: Option<String>,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub revision: i64,
    pub state: String,
    pub lines: Value,
    pub delivery_address: Option<Value>,
    pub subtotal_minor: i64,
    pub shipping_minor: i64,
    pub tax_minor: i64,
    pub total_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub guarantee_policy_version: i32,
    pub payment_id: Uuid,
    pub receipt_id: Option<Uuid>,
    pub cancellation_reason: Option<String>,
    pub shipment: Option<Value>,
    pub return_request: Option<Value>,
    pub dispute: Option<Value>,
    pub external_refund: Option<Value>,
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
            "tax": money_json(self.tax_minor, &self.currency, self.exponent),
            "total": money_json(self.total_minor, &self.currency, self.exponent),
            "guarantee_policy_version": self.guarantee_policy_version,
            "payment_id": self.payment_id,
            "receipt_id": self.receipt_id,
            "cancellation_reason": self.cancellation_reason,
            "shipment": self.shipment.clone().unwrap_or(Value::Null),
            "return_request": self.return_request.clone().unwrap_or(Value::Null),
            "dispute": self.dispute.clone().unwrap_or(Value::Null),
            "external_refund": self.external_refund.clone().unwrap_or(Value::Null),
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

/// An append-only dispute evidence row. The body is private order evidence
/// (ADR-0019 §8): it is served only through the scoped case-file read
/// (`GET /v1/orders/{id}/evidence`, dispute participants and configured
/// moderators), never through general projections or command results.
#[derive(Debug, Clone, FromRow)]
pub struct DisputeEvidenceRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub submitter_pubky: String,
    pub body: String,
    /// Byte size of the body (`octet_length`), so metadata-level views can
    /// describe the item without repeating it.
    pub body_bytes: i32,
    pub created_at: DateTime<Utc>,
}

impl DisputeEvidenceRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "submitter_pubky": self.submitter_pubky,
            "body": self.body,
            "body_bytes": self.body_bytes,
            "created_at": format_timestamp(self.created_at),
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
