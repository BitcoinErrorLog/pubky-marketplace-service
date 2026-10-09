//! `refund.confirm_destination`: the buyer of a USDT order confirms the
//! Arbitrum One address the seller refunds to (see
//! [`crate::refund_destination`]).
//!
//! The buyer may replace the address until a refund is recorded; every
//! confirmation is an event and tells the seller. The marketplace never moves
//! funds: this only stores what the buyer confirmed.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::ConfirmRefundDestinationPayload;
use marketplace_domain::{Command, ErrorCode};
use sqlx::{Postgres, Transaction};

use crate::handlers::{fetch_order_for_update, finish_order_action, guard_order_action};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::refund_destination::{is_usdt_order, is_valid_address, upsert};
use crate::refusal_audit::RefusalKind;
use crate::result::{CommandFailure, HandlerResult};

pub const REASON_INVALID_REFUND_DESTINATION: &str = "invalid_refund_destination";

/// A USDT payment the seller holds (paid) or is reviewing: the two states a
/// refund can follow. A pending, expired or abandoned payment owes nothing.
const REFUNDABLE_PAYMENT_STATES: [&str; 2] = ["confirmed", "manual_review"];

pub async fn confirm(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ConfirmRefundDestinationPayload,
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
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the buyer may confirm the refund address.",
        )));
    }
    if !is_usdt_order(&order) {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only an order paid in USDT has a refund address.",
        )));
    }
    if !is_valid_address(&payload.address) {
        return Ok(Err(CommandFailure::refused_with_reason(
            RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "That is not a valid Arbitrum address.",
            REASON_INVALID_REFUND_DESTINATION,
        )));
    }
    let payment_state: Option<String> =
        sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
            .bind(order.id)
            .fetch_optional(&mut **tx)
            .await?;
    if !payment_state
        .as_deref()
        .is_some_and(|state| REFUNDABLE_PAYMENT_STATES.contains(&state))
    {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The USDT payment has not been received, so there is nothing to refund.",
        )));
    }
    if order.external_refund.is_some() || order.state == "refunded_external" {
        return Ok(Err(CommandFailure::refused(
            RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "A refund was already recorded for this order.",
        )));
    }

    let destination = upsert(tx, order.id, &order.buyer_pubky, &payload.address, now).await?;
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let seller = updated.seller_pubky.clone();
    let mut result = finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "refund.destination_confirmed",
        ("refund_destination_confirmed", &seller),
        now,
    )
    .await?;
    if let Ok(success) = &mut result {
        success.result["order"]["refund_destination"] = destination.view();
    }
    Ok(result)
}
