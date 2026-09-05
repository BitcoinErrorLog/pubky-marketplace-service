//! Return commands (`return.request`, `return.approve`, `return.receive`)
//! and the return resolution (`refund.record_external`), ported from the
//! TypeScript prototype engine.
//!
//! `refund.record_external` records independently supplied seller evidence
//! of a refund executed outside this service (an external transaction id).
//! The service never claims to spend, custody, escrow, release, or refund
//! funds (ADR-0019 §7); it records the evidence and advances the order to
//! `refunded_external`.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    OrderActionPayload, RecordExternalRefundPayload, RequestReturnPayload,
};
use marketplace_domain::state_machines::{can_transition, order_machine, return_machine};
use marketplace_domain::{Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::clock::format_timestamp;
use crate::handlers::{fetch_order_for_update, finish_order_action, guard_order_action};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult};

pub async fn request(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RequestReturnPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the buyer may request a return.",
        )));
    }
    if !matches!(order.state.as_str(), "delivered" | "completed")
        || payload.requested_amount_minor > order.total_minor
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not eligible for this return amount.",
        )));
    }
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        "return_requested"
    ));

    let return_request = json!({
        "state": "requested",
        "reason": payload.reason,
        "requested_amount_minor": payload.requested_amount_minor,
        "requested_at": format_timestamp(now),
        "updated_at": format_timestamp(now),
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'return_requested', \
         return_request = $3, updated_at = $4 WHERE id = $1 AND revision = $2 \
         RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.seller_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "return.requested",
        ("return_updated", &recipient),
        now,
    )
    .await
}

pub async fn approve(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    advance_return(
        tx,
        actor,
        command,
        payload,
        ReturnStep {
            from_order_state: "return_requested",
            to_order_state: "return_approved",
            to_return_state: "approved",
            invalid_message: "No return is pending approval.",
            event_kind: "return.approved",
        },
        now,
    )
    .await
}

pub async fn receive(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    advance_return(
        tx,
        actor,
        command,
        payload,
        ReturnStep {
            from_order_state: "return_approved",
            to_order_state: "return_received",
            to_return_state: "received",
            invalid_message: "The return is not approved.",
            event_kind: "return.received",
        },
        now,
    )
    .await
}

struct ReturnStep {
    from_order_state: &'static str,
    to_order_state: &'static str,
    to_return_state: &'static str,
    invalid_message: &'static str,
    event_kind: &'static str,
}

/// Seller-driven return progression shared by approve and receive: the order
/// state and the return sub-state advance together under the canonical
/// order and return machines, and the buyer is notified.
async fn advance_return(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    step: ReturnStep,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            if step.to_return_state == "approved" {
                "Only the seller may approve this return."
            } else {
                "Only the seller may receive this return."
            },
        )));
    }
    let Some(mut return_request) = order
        .return_request
        .clone()
        .filter(|_| order.state == step.from_order_state)
    else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            step.invalid_message,
        )));
    };
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        step.to_order_state
    ));
    debug_assert!(can_transition(
        &return_machine(),
        return_request["state"]
            .as_str()
            .expect("return has a state"),
        step.to_return_state
    ));

    return_request["state"] = json!(step.to_return_state);
    return_request["updated_at"] = json!(format_timestamp(now));
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, return_request = $4, \
         updated_at = $5 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(step.to_order_state)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        step.event_kind,
        ("return_updated", &recipient),
        now,
    )
    .await
}

pub async fn record_external_refund(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RecordExternalRefundPayload,
    attestor: Option<&crate::attestor::Attestor>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the seller may record a refund.",
        )));
    }
    if !matches!(order.state.as_str(), "return_received" | "cancelled")
        || payload.amount_minor > order.total_minor
        || order.external_refund.is_some()
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The external refund cannot be recorded.",
        )));
    }
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        "refunded_external"
    ));

    let external_refund = json!({
        "amount_minor": payload.amount_minor,
        "transaction_id": payload.transaction_id,
        "recorded_at": format_timestamp(now),
    });
    let return_request = order.return_request.clone().map(|mut request| {
        request["state"] = json!("refunded");
        request["updated_at"] = json!(format_timestamp(now));
        request
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'refunded_external', \
         external_refund = $3, return_request = $4, updated_at = $5 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&external_refund)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    // Attestor annotation (ADR 0024 §5): the refund annotates the
    // order_ref; the attestation itself is never revoked.
    if let Some(attestor) = attestor {
        crate::handlers::attestation::insert_annotation(
            tx, attestor, updated.id, "refunded", None, now,
        )
        .await?;
    }

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "refund.recorded_external",
        ("refund_recorded", &recipient),
        now,
    )
    .await
}
