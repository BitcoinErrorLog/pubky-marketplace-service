//! Manual digital delivery: the buyer's delivery email and the seller's
//! Mark emailed / Mark delivered (digital-delivery-design.md §3.3, §4.3,
//! §6 rows F1–F17).
//!
//! The buyer's address is sealed per email-kind order under
//! `buyer-email/v1|{order_id}|{buyer_pubky}` in the order-create
//! transaction. It is opened only for the order's buyer (any state, until
//! purged) and for its seller once a receipt exists and while the order is
//! not `cancelled`, `refunded_external` or `closed`. It appears in no
//! projection, event, notification, outbox payload, log line or command
//! result. The purge deletes the ciphertext 30 days after the order ends
//! (7 days after an unpaid checkout ends) and keeps `emailed_at` and
//! `purged_at`.

use std::collections::BTreeSet;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    DeliverDigitalPayload, DeliveryEmail, DigitalDeliveryChannel, DigitalDeliveryKind,
    SetDeliveryEmailPayload,
};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::digital::{email_aad, DigitalKeys};
use crate::executor::insert_event;
use crate::handlers::digital::{no_store, unavailable, DIGITAL_ENDED_ORDER_STATES};
use crate::handlers::digital_orders::line_kind;
use crate::handlers::{
    fetch_order_for_update, fetch_order_reviews, guard_order_action, insert_notification_intent,
    order_json_with_reviews,
};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::refusal_audit::RefusalKind;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};
use crate::AppState;

pub const REASON_EMAIL_REQUIRED: &str = "delivery_email_required";
pub const REASON_EMAIL_NOT_NEEDED: &str = "delivery_email_not_needed";
pub const REASON_INVALID_EMAIL: &str = "invalid_delivery_email";
pub const REASON_ALREADY_EMAILED: &str = "already_emailed";
pub const REASON_WRONG_CHANNEL: &str = "wrong_delivery_channel";
pub const REASON_EMAIL_MISSING: &str = "email_missing";

pub(crate) fn invalid_email() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "The delivery email address is not valid.",
        REASON_INVALID_EMAIL,
    )
}

/// The manual kinds (email, message) among a digital order's lines.
pub(crate) fn manual_kinds(order: &OrderRow) -> BTreeSet<&'static str> {
    order
        .lines
        .as_array()
        .map(|lines| {
            lines
                .iter()
                .filter_map(line_kind)
                .filter(|kind| !kind.is_instant())
                .map(DigitalDeliveryKind::as_str)
                .collect()
        })
        .unwrap_or_default()
}

fn has_email_line(order: &OrderRow) -> bool {
    order.fulfillment == "digital" && manual_kinds(order).contains("email")
}

/// Seals and stores (or replaces) the buyer's delivery address for one
/// order. A replacement after a purge clears `purged_at`.
pub(crate) async fn store_delivery_email(
    tx: &mut Transaction<'_, Postgres>,
    keys: &DigitalKeys,
    order_id: Uuid,
    buyer_pubky: &str,
    email: &DeliveryEmail,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sealed = keys.seal(&email_aad(order_id, buyer_pubky), email.0.as_bytes());
    sqlx::query(
        "INSERT INTO order_delivery_emails (order_id, buyer_pubky, email_ciphertext, created_at, \
         updated_at) VALUES ($1, $2, $3, $4, $4) \
         ON CONFLICT (order_id) DO UPDATE SET email_ciphertext = EXCLUDED.email_ciphertext, \
         updated_at = EXCLUDED.updated_at, purged_at = NULL",
    )
    .bind(order_id)
    .bind(buyer_pubky)
    .bind(&sealed)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct EmailRow {
    buyer_pubky: String,
    email_ciphertext: Option<Vec<u8>>,
    emailed_at: Option<DateTime<Utc>>,
}

async fn email_row(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<EmailRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT buyer_pubky, email_ciphertext, emailed_at FROM order_delivery_emails \
         WHERE order_id = $1 FOR UPDATE",
    )
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
}

async fn order_action_success(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    order: &OrderRow,
    event_kind: &str,
    notify: Option<(&str, &str)>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let aggregate = ids::order_aggregate_id(order.id);
    let event_id = insert_event(
        tx,
        command.command_id,
        &aggregate,
        order.revision,
        actor,
        event_kind,
        now,
    )
    .await?;
    if let Some((notification, recipient)) = notify {
        insert_notification_intent(
            tx,
            event_id,
            notification,
            recipient,
            actor,
            &aggregate,
            None,
            now,
        )
        .await?;
    }
    let reviews = fetch_order_reviews(tx, order.id).await?;
    Ok(Ok(HandlerSuccess {
        revision: order.revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "order", "order": order_json_with_reviews(order, &reviews) }),
    }))
}

/// `order.set_delivery_email` (§6 F11, F12): the buyer replaces the sealed
/// address while the order is `pending_payment` or `paid` and not yet
/// marked emailed. Also how a buyer re-enters an address after a purge.
pub async fn set_delivery_email(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SetDeliveryEmailPayload,
    keys: Option<&DigitalKeys>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(keys) = keys else {
        return Ok(Err(unavailable()));
    };
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the buyer may change the delivery email.",
        )));
    }
    if !has_email_line(&order) {
        return Ok(Err(CommandFailure::refused_with_reason(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "No item in this order is delivered by email.",
            REASON_EMAIL_NOT_NEEDED,
        )));
    }
    if !matches!(order.state.as_str(), "pending_payment" | "paid") {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The delivery email can no longer be changed.",
        )));
    }
    if email_row(tx, order.id)
        .await?
        .is_some_and(|row| row.emailed_at.is_some())
    {
        return Ok(Err(CommandFailure::refused_with_reason(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller already emailed the purchase.",
            REASON_ALREADY_EMAILED,
        )));
    }
    if !payload.delivery_email.is_well_formed() {
        return Ok(Err(invalid_email()));
    }
    store_delivery_email(
        tx,
        keys,
        order.id,
        &order.buyer_pubky,
        &payload.delivery_email,
        now,
    )
    .await?;
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    // The seller learns of a change only once they can read the address.
    let seller = updated.seller_pubky.clone();
    let notify = updated
        .receipt_id
        .is_some()
        .then_some(("delivery_email_updated", seller.as_str()));
    order_action_success(
        tx,
        actor,
        command,
        &updated,
        "order.delivery_email_set",
        notify,
        now,
    )
    .await
}

/// `fulfillment.deliver_digital` (§6 F13–F15): the seller marks the
/// order's email or message lines delivered. When every manual kind on the
/// order is marked, the order moves `paid → delivered`. The order-revision
/// compare-and-swap makes a racing buyer cancel request and this command
/// single-winner.
pub async fn deliver_digital(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &DeliverDigitalPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the seller may mark this delivered.",
        )));
    }
    let kinds = manual_kinds(&order);
    let channel_kind = payload.channel.kind().as_str();
    if order.fulfillment != "digital" || !kinds.contains(channel_kind) {
        return Ok(Err(CommandFailure::refused_with_reason(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "No item in this order is delivered that way.",
            REASON_WRONG_CHANNEL,
        )));
    }
    if order.state != "paid" {
        let message = if order.state == "cancel_requested" {
            "This order has a cancellation request; approve or decline it first."
        } else {
            "The order is not awaiting delivery."
        };
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            message,
        )));
    }
    let email_row = email_row(tx, order.id).await?;
    let (message_delivered_at,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT digital_message_delivered_at FROM orders WHERE id = $1")
            .bind(order.id)
            .fetch_one(&mut **tx)
            .await?;
    let email_marked = email_row
        .as_ref()
        .is_some_and(|row| row.emailed_at.is_some());
    let message_marked = message_delivered_at.is_some();
    match payload.channel {
        DigitalDeliveryChannel::Email => {
            if email_marked {
                return Ok(Err(CommandFailure::refused_with_reason(
                    RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The purchase was already marked emailed.",
                    REASON_ALREADY_EMAILED,
                )));
            }
            if email_row
                .as_ref()
                .is_none_or(|row| row.email_ciphertext.is_none())
            {
                return Ok(Err(CommandFailure::refused_with_reason(
                    RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The buyer has not given a delivery email.",
                    REASON_EMAIL_MISSING,
                )));
            }
            sqlx::query("UPDATE order_delivery_emails SET emailed_at = $2 WHERE order_id = $1")
                .bind(order.id)
                .bind(now)
                .execute(&mut **tx)
                .await?;
        }
        DigitalDeliveryChannel::Message => {
            if message_marked {
                return Ok(Err(CommandFailure::refused(
                    RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The purchase was already marked delivered.",
                )));
            }
        }
    }
    let email_done = !kinds.contains("email")
        || email_marked
        || payload.channel == DigitalDeliveryChannel::Email;
    let message_done = !kinds.contains("message")
        || message_marked
        || payload.channel == DigitalDeliveryChannel::Message;
    let delivered = email_done && message_done;
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3, \
         state = CASE WHEN $4 THEN 'delivered' ELSE state END, \
         digital_delivered_at = CASE WHEN $4 THEN $3 ELSE digital_delivered_at END, \
         digital_message_delivered_at = CASE WHEN $5 THEN $3 ELSE digital_message_delivered_at END \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .bind(delivered)
    .bind(payload.channel == DigitalDeliveryChannel::Message)
    .fetch_one(&mut **tx)
    .await?;
    let buyer = updated.buyer_pubky.clone();
    order_action_success(
        tx,
        actor,
        command,
        &updated,
        if delivered {
            "fulfillment.digital_delivered"
        } else {
            "fulfillment.digital_channel_delivered"
        },
        Some(("order_delivered", buyer.as_str())),
        now,
    )
    .await
}

fn read_error(code: ErrorCode, message: &str, reason: Option<&str>) -> Response {
    let mut error = json!({ "code": code, "message": message });
    if let Some(reason) = reason {
        error["reason"] = json!(reason);
    }
    no_store(
        (
            StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
            Json(json!({ "ok": false, "error": error })),
        )
            .into_response(),
    )
}

fn internal_error(context: &str) -> Response {
    tracing::error!("{context} failed");
    no_store(
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "ok": false,
                "error": { "code": "INTERNAL", "message": "The delivery email could not be read." },
            })),
        )
            .into_response(),
    )
}

/// `GET /v1/orders/{id}/delivery-email` (§6 F5–F10). The entitlement check
/// and the open run in one transaction holding the order `FOR SHARE`, so a
/// cancel or refund that commits first ends the seller's read, and one that
/// arrives later waits for it.
pub async fn get_order_delivery_email(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error("delivery email read"),
    };
    let order: Result<Option<OrderRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders \
         WHERE id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2) FOR SHARE"
    ))
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&mut *tx)
    .await;
    let order = match order {
        Ok(Some(order)) if has_email_line(&order) => order,
        Ok(_) => return read_error(ErrorCode::NotFound, "The order was not found.", None),
        Err(_) => return internal_error("delivery email order read"),
    };
    let is_buyer = order.buyer_pubky == actor.0;
    if !is_buyer {
        if order.receipt_id.is_none() {
            return read_error(
                ErrorCode::InvalidState,
                "The buyer's email appears once payment is confirmed.",
                Some("not_paid"),
            );
        }
        if DIGITAL_ENDED_ORDER_STATES.contains(&order.state.as_str()) {
            return read_error(
                ErrorCode::InvalidState,
                "This order was cancelled or refunded.",
                Some("delivery_ended"),
            );
        }
    }
    let Some(keys) = state.digital.as_deref() else {
        return read_error(
            ErrorCode::InvalidState,
            "Digital delivery is unavailable on this deployment.",
            Some(crate::handlers::digital::REASON_UNAVAILABLE),
        );
    };
    let row: Result<Option<EmailRow>, sqlx::Error> = sqlx::query_as(
        "SELECT buyer_pubky, email_ciphertext, emailed_at FROM order_delivery_emails \
         WHERE order_id = $1",
    )
    .bind(order.id)
    .fetch_optional(&mut *tx)
    .await;
    let row = match row {
        Ok(row) => row,
        Err(_) => return internal_error("delivery email row read"),
    };
    if tx.commit().await.is_err() {
        return internal_error("delivery email read commit");
    }
    let emailed_at = row.as_ref().and_then(|row| row.emailed_at);
    let Some((buyer, ciphertext)) =
        row.and_then(|row| Some((row.buyer_pubky, row.email_ciphertext?)))
    else {
        return read_error(
            ErrorCode::InvalidState,
            "No delivery email is on file for this order.",
            Some(REASON_EMAIL_MISSING),
        );
    };
    let Ok(plaintext) = keys.open(&email_aad(order.id, &buyer), &ciphertext) else {
        return internal_error("delivery email open");
    };
    let Ok(email) = String::from_utf8(plaintext) else {
        return internal_error("delivery email decode");
    };
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "order_id": order.id,
                "delivery_email": email,
                "emailed_at": emailed_at.map(format_timestamp),
            })),
        )
            .into_response(),
    )
}

/// The purge (§4.3 step 7): deletes the sealed address 30 days after the
/// order ends and 7 days after an unpaid checkout ends, keeping
/// `emailed_at` and `purged_at`.
pub async fn purge_delivery_emails(
    pool: &PgPool,
    now: DateTime<Utc>,
    retention_days: i64,
    unpaid_retention_days: i64,
) -> Result<u64, sqlx::Error> {
    let purged = sqlx::query(
        "UPDATE order_delivery_emails e SET email_ciphertext = NULL, purged_at = $1, \
         updated_at = $1 FROM orders o \
         WHERE o.id = e.order_id AND e.email_ciphertext IS NOT NULL AND ( \
           (o.receipt_id IS NOT NULL \
            AND o.state IN ('completed', 'cancelled', 'refunded_external', 'closed') \
            AND o.updated_at <= $2) \
           OR (o.receipt_id IS NULL AND o.state = 'cancelled' AND o.updated_at <= $3))",
    )
    .bind(now)
    .bind(now - chrono::Duration::days(retention_days))
    .bind(now - chrono::Duration::days(unpaid_retention_days))
    .execute(pool)
    .await?
    .rows_affected();
    Ok(purged)
}

/// How long past its retention a sealed address may sit before `/ready`
/// reports the purge as not running.
pub const PURGE_GRACE_DAYS: i64 = 1;

/// Sealed addresses the purge should have deleted more than
/// [`PURGE_GRACE_DAYS`] ago.
pub async fn overdue_delivery_emails(
    pool: &PgPool,
    now: DateTime<Utc>,
    retention_days: i64,
    unpaid_retention_days: i64,
) -> Result<i64, sqlx::Error> {
    let (overdue,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM order_delivery_emails e JOIN orders o ON o.id = e.order_id \
         WHERE e.email_ciphertext IS NOT NULL AND ( \
           (o.receipt_id IS NOT NULL \
            AND o.state IN ('completed', 'cancelled', 'refunded_external', 'closed') \
            AND o.updated_at <= $1) \
           OR (o.receipt_id IS NULL AND o.state = 'cancelled' AND o.updated_at <= $2))",
    )
    .bind(now - chrono::Duration::days(retention_days + PURGE_GRACE_DAYS))
    .bind(now - chrono::Duration::days(unpaid_retention_days + PURGE_GRACE_DAYS))
    .fetch_one(pool)
    .await?;
    Ok(overdue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_email_shape() {
        for good in ["buyer@example.com", "a@b", "first.last+tag@sub.example.org"] {
            assert!(DeliveryEmail(good.into()).is_well_formed(), "{good}");
        }
        for bad in [
            "",
            "buyer",
            "@example.com",
            "buyer@",
            "a@b@c",
            "buyer @example.com",
            "buyer@exa\tmple.com",
            "buyer@example.com\n",
        ] {
            assert!(!DeliveryEmail(bad.into()).is_well_formed(), "{bad:?}");
        }
        let long = format!("{}@example.com", "a".repeat(250));
        assert!(!DeliveryEmail(long).is_well_formed());
        assert_eq!(
            format!("{:?}", DeliveryEmail("buyer@example.com".into())),
            "DeliveryEmail(<redacted>)"
        );
    }
}
