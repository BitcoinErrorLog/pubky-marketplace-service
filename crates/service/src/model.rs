use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::FromRow;
use uuid::Uuid;

use crate::clock::format_timestamp;

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
pub struct OrderRow {
    pub id: Uuid,
    pub buyer_pubky: String,
    pub seller_pubky: String,
    pub revision: i64,
    pub state: String,
    pub lines: Value,
    pub delivery_address: Value,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl OrderRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "buyer_pubky": self.buyer_pubky,
            "seller_pubky": self.seller_pubky,
            "revision": self.revision,
            "state": self.state,
            "lines": self.lines,
            "delivery_address": self.delivery_address,
            "subtotal": money_json(self.subtotal_minor, &self.currency, self.exponent),
            "shipping": money_json(self.shipping_minor, &self.currency, self.exponent),
            "tax": money_json(self.tax_minor, &self.currency, self.exponent),
            "total": money_json(self.total_minor, &self.currency, self.exponent),
            "guarantee_policy_version": self.guarantee_policy_version,
            "payment_id": self.payment_id,
            "receipt_id": self.receipt_id,
            "cancellation_reason": self.cancellation_reason,
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
    pub locks_bundle_id: Uuid,
    pub amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PaymentRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "order_id": self.order_id,
            "buyer_pubky": self.buyer_pubky,
            "seller_pubky": self.seller_pubky,
            "revision": self.revision,
            "adapter": self.adapter,
            "state": self.state,
            "confirmations": self.confirmations,
            "locks_bundle_id": self.locks_bundle_id,
            "amount": money_json(self.amount_minor, &self.currency, self.exponent),
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
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
