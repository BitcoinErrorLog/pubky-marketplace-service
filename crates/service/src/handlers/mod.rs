pub mod attestation;
pub mod auction;
pub mod cancellation;
pub mod checkout;
pub mod digital;
pub mod digital_manual;
pub mod digital_orders;
pub mod drops;
pub mod fulfillment;
pub mod holds;
pub mod locks;
pub mod offer_checkout;
pub mod offers;
pub mod payment;
pub mod pickup;
pub mod register_listing;
pub mod reserve_inventory;
pub mod returns;
pub mod reviews;
pub mod sync_listing;

use chrono::{DateTime, Utc};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::model::{AuctionReserveRow, ListingRow, OrderRow, ProjectionContext, ReviewRow};
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub const LISTING_COLUMNS: &str = "aggregate_id, seller_pubky, listing_id, title, \
     listing_revision, content_hash, server_revision, state, total_quantity, \
     available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
     unit_price_currency, unit_price_exponent, shipping_minor, sale_format, auction, \
     fulfillment_methods, digital_lock_policy_uri, digital_lock_criterion_id, updated_at, \
     digital_delivery_kind, digital_delivery_content_type, digital_delivery_size_bytes, \
     deleted_at, deleted_event_cursor, recreated_at";

pub const AUCTION_RESERVE_COLUMNS: &str = "listing_aggregate_id, listing_revision, \
     record_revision, reserve_amount_minor, reserve_currency, reserve_exponent, \
     last_command_id, updated_at";

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

/// [`fetch_listing`] for paths that create a new commitment (checkout,
/// offers, reserves, bids): a tombstoned listing reads as absent.
pub async fn fetch_live_listing(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    Ok(fetch_listing(tx, aggregate_id)
        .await?
        .filter(|listing| !listing.is_deleted()))
}

/// [`fetch_listing_for_update`] for commitment paths. The row lock is still
/// taken on a tombstoned listing, so the deletion cannot race the caller.
pub async fn fetch_live_listing_for_update(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    Ok(fetch_listing_for_update(tx, aggregate_id)
        .await?
        .filter(|listing| !listing.is_deleted()))
}

pub async fn fetch_auction_reserve(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<AuctionReserveRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {AUCTION_RESERVE_COLUMNS} FROM listing_auction_reserves \
         WHERE listing_aggregate_id = $1"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

pub async fn fetch_auction_reserve_for_update(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<AuctionReserveRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {AUCTION_RESERVE_COLUMNS} FROM listing_auction_reserves \
         WHERE listing_aggregate_id = $1 FOR UPDATE"
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

pub async fn fetch_order(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<OrderRow>, sqlx::Error> {
    sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1"))
        .bind(order_id)
        .fetch_optional(&mut **tx)
        .await
}

pub async fn fetch_order_for_update(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<OrderRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1 FOR UPDATE"
    ))
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
}

/// The prototype engine's `getOrderAction` guard sequence: participant
/// check, aggregate identity check, then the revision compare. The caller
/// handles the not-found case (the fetch).
pub fn guard_order_action(
    actor: &str,
    command: &Command,
    order: &OrderRow,
) -> Option<CommandFailure> {
    if actor != order.buyer_pubky && actor != order.seller_pubky {
        return Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only order participants may act on it.",
        ));
    }
    if command.aggregate_id != ids::order_aggregate_id(order.id) {
        return Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The order aggregate id is invalid.",
        ));
    }
    if command.expected_revision != order.revision {
        return Some(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The order revision is stale.",
            order.revision,
        ));
    }
    None
}

pub const REVIEW_COLUMNS: &str = "id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
     rating, text, created_at, updated_at";

pub async fn fetch_order_reviews(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Vec<ReviewRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {REVIEW_COLUMNS} FROM reviews WHERE order_id = $1 ORDER BY created_at, id"
    ))
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await
}

/// The redacted order projection with its reviews attached, the shape every
/// order-action command result returns (never the delivery address).
pub fn order_json_with_reviews(order: &OrderRow, reviews: &[ReviewRow]) -> Value {
    let mut view = order.project(ProjectionContext::CommandResult {
        authenticated_actor: "",
    });
    view["reviews"] = Value::Array(reviews.iter().map(ReviewRow::view).collect());
    view
}

/// Seller-scoped variant used by the Paykit review commands. Keeping this
/// separate prevents seller-only evidence from widening ordinary command
/// result projections.
pub fn seller_order_json_with_reviews(
    order: &OrderRow,
    reviews: &[ReviewRow],
    authenticated_actor: &str,
) -> Value {
    let mut view = order.projection_for_actor_with_payment(authenticated_actor, None);
    view["reviews"] = Value::Array(reviews.iter().map(ReviewRow::view).collect());
    view
}

/// Persists the shared tail of an order action, mirroring the prototype's
/// `persistOrderAction`: one immutable event on the order aggregate plus one
/// outbox notification intent (`notification` is `(type, recipient)`),
/// returning `{ kind: "order", order }`.
pub async fn finish_order_action(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    order: &OrderRow,
    event_kind: &str,
    notification: (&str, &str),
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let (notification_type, notification_recipient) = notification;
    let order_aggregate_id = ids::order_aggregate_id(order.id);
    let event_id = insert_event(
        tx,
        command.command_id,
        &order_aggregate_id,
        order.revision,
        actor,
        event_kind,
        now,
    )
    .await?;
    insert_notification_intent(
        tx,
        event_id,
        notification_type,
        notification_recipient,
        actor,
        &order_aggregate_id,
        None,
        now,
    )
    .await?;
    let reviews = fetch_order_reviews(tx, order.id).await?;
    Ok(Ok(HandlerSuccess {
        revision: order.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "order",
            "order": order_json_with_reviews(order, &reviews),
        }),
    }))
}

/// Writes a complete notification intent to the outbox in the same
/// transaction as the command (ADR-0019 §4). The worker delivers intents at
/// least once; the notifications table dedups by (event id, recipient).
///
/// `amount` is optional monetary context (`money_json` shape) rendered in
/// notification copy. ADR-0019 §8 compatibility: an amount may ride a
/// notification only when its recipient already sees that exact figure in a
/// role-scoped projection they can read — the offer amount is on the offer
/// projection both parties fetch, and an auction's visible price is on the
/// listing projection every bidder fetches. Never pass anything address- or
/// payment-bearing here (no addresses, correlations, or payment ids).
// Eight positional facts of one insert; a params struct would only rename
// the call sites without reducing what each caller must supply.
#[allow(clippy::too_many_arguments)]
pub async fn insert_notification_intent(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    notification_type: &str,
    recipient_pubky: &str,
    actor_pubky: &str,
    aggregate_id: &str,
    amount: Option<&Value>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut payload = json!({
        "event_id": event_id,
        "recipient_pubky": recipient_pubky,
        "actor_pubky": actor_pubky,
        "aggregate_id": aggregate_id,
    });
    // Additive payload evolution: rows written before amounts existed have
    // no `amount` key, and the delivery worker treats absence as null.
    if let Some(amount) = amount {
        payload["amount"] = amount.clone();
    }
    enqueue_notification(tx, event_id, notification_type, payload, now).await
}

/// Why a Bitcoin payment waits for the seller's decision (the
/// `notifications_review_reason_check` vocabulary, migration 0048).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitcoinReviewNotice {
    LateSettlement,
    AmountMismatch,
    ConfirmationFailed,
    SellerConfirmationWindowElapsed,
    SellerResponseOverdue,
}

impl BitcoinReviewNotice {
    pub const ALL: [Self; 5] = [
        Self::LateSettlement,
        Self::AmountMismatch,
        Self::ConfirmationFailed,
        Self::SellerConfirmationWindowElapsed,
        Self::SellerResponseOverdue,
    ];

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|notice| notice.as_str() == value)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LateSettlement => "late_settlement",
            Self::AmountMismatch => "amount_mismatch",
            Self::ConfirmationFailed => "confirmation_failed",
            Self::SellerConfirmationWindowElapsed => "seller_confirmation_window_elapsed",
            Self::SellerResponseOverdue => "seller_response_overdue",
        }
    }
}

/// Tells the order's seller that a Paykit Bitcoin payment is in
/// `manual_review` and needs their resolution, with the reason. The seller
/// already sees the payment state and its review reason on the order.
pub async fn insert_bitcoin_manual_review_intent(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    seller_pubky: &str,
    order_id: Uuid,
    reason: BitcoinReviewNotice,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let payload = json!({
        "event_id": event_id,
        "recipient_pubky": seller_pubky,
        "actor_pubky": crate::workers::SYSTEM_ACTOR,
        "aggregate_id": ids::order_aggregate_id(order_id),
        "review_reason": reason.as_str(),
    });
    enqueue_notification(tx, event_id, "bitcoin_manual_review", payload, now).await
}

async fn enqueue_notification(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    notification_type: &str,
    payload: Value,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    crate::reserve_secrecy::ensure_reserve_free(&payload)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    sqlx::query("INSERT INTO outbox (event_id, kind, payload, created_at) VALUES ($1, $2, $3, $4)")
        .bind(event_id)
        .bind(format!("notification.{notification_type}"))
        .bind(payload)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
