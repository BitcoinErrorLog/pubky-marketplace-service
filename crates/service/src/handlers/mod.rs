pub mod auction;
pub mod checkout;
pub mod offers;
pub mod register_listing;
pub mod report;
pub mod reserve_inventory;

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::model::ListingRow;

pub const LISTING_COLUMNS: &str = "aggregate_id, seller_pubky, listing_id, title, \
     listing_revision, content_hash, server_revision, state, total_quantity, \
     available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
     unit_price_currency, unit_price_exponent, sale_format, auction, updated_at";

/// Flat sandbox shipping per seller order, identical to the prototype engine.
pub const SHIPPING_MINOR: i64 = 1_200;

/// Sandbox tax: 8% of subtotal + shipping, rounded half up in integer math
/// (`Math.round((subtotal + shipping) * 0.08)` in the prototype engine).
pub fn sandbox_tax_minor(subtotal_minor: i64, shipping_minor: i64) -> i64 {
    (8 * (subtotal_minor + shipping_minor) + 50) / 100
}

pub async fn fetch_listing(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

pub async fn fetch_listing_for_update(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1 FOR UPDATE"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

pub async fn current_listing_revision(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<i64, sqlx::Error> {
    let revision: Option<(i64,)> =
        sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(aggregate_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(revision.map(|(value,)| value).unwrap_or(0))
}

/// Writes a complete notification intent to the outbox in the same
/// transaction as the command (ADR-0019 §4). The worker delivers intents at
/// least once; the notifications table dedups by (event id, recipient).
pub async fn insert_notification_intent(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    notification_type: &str,
    recipient_pubky: &str,
    actor_pubky: &str,
    aggregate_id: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO outbox (event_id, kind, payload, created_at) VALUES ($1, $2, $3, $4)")
        .bind(event_id)
        .bind(format!("notification.{notification_type}"))
        .bind(json!({
            "event_id": event_id,
            "recipient_pubky": recipient_pubky,
            "actor_pubky": actor_pubky,
            "aggregate_id": aggregate_id,
        }))
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
