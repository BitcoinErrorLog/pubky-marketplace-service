//! `payment.sandbox_advance`, ported from the TypeScript prototype engine.
//!
//! The sandbox payment adapter records state transitions the buyer drives
//! explicitly; the service never observes, holds, or moves funds (ADR-0019
//! §7). Confirmation is the transition that issues the durable receipt,
//! moves the order to `paid`, converts the held inventory from reserved to
//! sold, and marks the winning auction reservation converted — the
//! `payment_confirmation` triggers declared in the state machine contract.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{AdvanceSandboxPaymentPayload, SandboxPaymentTarget};
use marketplace_domain::state_machines::{
    can_transition, listing_machine, order_machine, payment_machine,
};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    fetch_listing_for_update, fetch_order_for_update, fetch_order_reviews,
    insert_notification_intent, order_json_with_reviews,
};
use crate::model::{money_json, OrderRow, PaymentRow, ReceiptRow};
use crate::queries::{ORDER_COLUMNS, PAYMENT_COLUMNS};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &AdvanceSandboxPaymentPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(payload.payment_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(payment) = payment else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The sandbox payment was not found.",
        )));
    };
    if payment.buyer_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the buyer may advance a sandbox payment.",
        )));
    }
    // A payment correlated to a Locks lifecycle is advanced exclusively by
    // server-side verification (ADR-0019 §7): no client claim — including
    // this explicitly sandbox-only command — may advance it.
    if payment.adapter != "sandbox" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "A Locks-correlated payment advances only by server-side verification.",
        )));
    }
    if command.aggregate_id != ids::payment_aggregate_id(payment.id) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The payment aggregate id is invalid.",
        )));
    }
    if command.expected_revision != payment.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The payment revision is stale.",
            payment.revision,
        )));
    }
    let target = payload.target.as_str();
    // The prototype's transition table is a strict subset of the payment
    // machine (the machine also carries the server-driven payment window).
    let allowed = match payment.state.as_str() {
        "awaiting_entitlement" => ["detected", "confirmed", "expired", "manual_review"].as_slice(),
        "detected" => ["confirmed", "manual_review"].as_slice(),
        _ => [].as_slice(),
    };
    if !allowed.contains(&target) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The sandbox payment transition is invalid.",
        )));
    }
    debug_assert!(can_transition(&payment_machine(), &payment.state, target));
    if payload.target == SandboxPaymentTarget::Confirmed && payload.confirmations < 1 {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Confirmed payment requires at least one confirmation.",
        )));
    }
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };

    let updated_payment: PaymentRow = sqlx::query_as(&format!(
        "UPDATE payments SET revision = revision + 1, state = $2, confirmations = $3, \
         updated_at = $4 WHERE id = $1 RETURNING {PAYMENT_COLUMNS}"
    ))
    .bind(payment.id)
    .bind(target)
    .bind(i32::try_from(payload.confirmations).expect("confirmations validated to 0..=6"))
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let payment_event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        updated_payment.revision,
        actor,
        &format!("payment.{target}"),
        now,
    )
    .await?;
    let mut event_ids = vec![payment_event_id];

    let (updated_order, receipt) = if payload.target == SandboxPaymentTarget::Confirmed {
        let (order, receipt, receipt_event_id) =
            match confirm_order(tx, actor, command.command_id, &payment, order, now).await? {
                Ok(confirmed) => confirmed,
                Err(failure) => return Ok(Err(failure)),
            };
        event_ids.push(receipt_event_id);
        insert_notification_intent(
            tx,
            payment_event_id,
            "payment_confirmed",
            &order.seller_pubky,
            actor,
            &ids::order_aggregate_id(order.id),
            None,
            now,
        )
        .await?;
        (order, Some(receipt))
    } else {
        (order, None)
    };

    let reviews = fetch_order_reviews(tx, updated_order.id).await?;
    Ok(Ok(HandlerSuccess {
        revision: updated_payment.revision,
        event_ids,
        result: json!({
            "kind": "payment",
            "payment": updated_payment.projection(),
            "order": order_json_with_reviews(&updated_order, &reviews),
            "receipt": receipt.as_ref().map(ReceiptRow::view).unwrap_or(Value::Null),
        }),
    }))
}

/// Applies the confirmation effects: the order moves to `paid` with its
/// receipt issued exactly once, the held inventory converts from reserved
/// to sold, and the winning auction reservation (when one exists) is marked
/// converted. Shared by the sandbox command and the worker's Locks
/// verification (`command_id` is the sandbox command id or the correlation
/// id, for event traceability).
pub(crate) async fn confirm_order(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command_id: Uuid,
    payment: &PaymentRow,
    order: OrderRow,
    now: DateTime<Utc>,
) -> Result<Result<(OrderRow, ReceiptRow, Uuid), CommandFailure>, sqlx::Error> {
    if !can_transition(&order_machine(), &order.state, "paid") {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order can no longer be paid.",
        )));
    }

    // A lapsed winning reservation means the hold was already released back
    // to the listing; confirming then would sell inventory the order no
    // longer holds.
    if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
        let converted = sqlx::query(
            "UPDATE reservations SET status = 'converted', updated_at = $3 \
             WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 AND status = 'active'",
        )
        .bind(auction_aggregate_id)
        .bind(&order.buyer_pubky)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        if converted.rows_affected() == 0 {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "The reserved inventory for this order is no longer held.",
            )));
        }
    }

    // Move each line's quantity from reserved to sold under the quantity
    // balance constraint; the listing machine declares reserved -> sold for
    // exactly this payment confirmation.
    let lines = order.lines.as_array().expect("order lines are an array");
    for line in lines {
        let aggregate_id = line["listing_aggregate_id"]
            .as_str()
            .expect("order line carries its listing aggregate id");
        let quantity = line["quantity"]
            .as_i64()
            .expect("order line carries its quantity");
        let Some(listing) = fetch_listing_for_update(tx, aggregate_id).await? else {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvariantViolation,
                "An order line's listing is missing.",
            )));
        };
        if listing.reserved_quantity < quantity {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "The reserved inventory for this order is no longer held.",
            )));
        }
        let new_state = if listing.available_quantity > 0 {
            "available"
        } else if listing.reserved_quantity > quantity {
            "reserved"
        } else {
            "sold"
        };
        debug_assert!(can_transition(
            &listing_machine(),
            &listing.state,
            new_state
        ));
        sqlx::query(
            "UPDATE listings SET server_revision = server_revision + 1, state = $2, \
             reserved_quantity = reserved_quantity - $3, sold_quantity = sold_quantity + $3, \
             updated_at = $4 WHERE aggregate_id = $1",
        )
        .bind(aggregate_id)
        .bind(new_state)
        .bind(quantity)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    }

    // The receipt hash covers the canonical snake_case receipt payload in
    // fixed field order, BLAKE3 like the prototype engine.
    let receipt_id = Uuid::new_v4();
    let total = money_json(order.total_minor, &order.currency, order.exponent);
    let canonical = serde_json::to_vec(&json!({
        "order_id": order.id,
        "payment_id": payment.id,
        "total": total,
        "issued_at": format_timestamp(now),
    }))
    .expect("receipt payload serializes infallibly");
    let content_hash = blake3::hash(&canonical).to_hex().to_string();
    let receipt: ReceiptRow = sqlx::query_as(
        "INSERT INTO receipts (id, order_id, payment_id, issuer_pubky, recipient_pubky, \
         total_minor, currency, exponent, content_hash, issued_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         RETURNING id, order_id, payment_id, issuer_pubky, recipient_pubky, total_minor, \
         currency, exponent, content_hash, issued_at",
    )
    .bind(receipt_id)
    .bind(order.id)
    .bind(payment.id)
    .bind(&order.seller_pubky)
    .bind(&order.buyer_pubky)
    .bind(order.total_minor)
    .bind(&order.currency)
    .bind(order.exponent)
    .bind(&content_hash)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let updated_order: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'paid', receipt_id = $2, \
         updated_at = $3 WHERE id = $1 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(receipt_id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let receipt_event_id = insert_event(
        tx,
        command_id,
        &ids::order_aggregate_id(order.id),
        updated_order.revision,
        actor,
        "receipt.issued",
        now,
    )
    .await?;

    Ok(Ok((updated_order, receipt, receipt_event_id)))
}
