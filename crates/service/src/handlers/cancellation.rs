//! Order cancellation commands (`order.cancel_request`,
//! `order.cancel_approve`), ported from the TypeScript prototype engine.
//!
//! The buyer requests cancellation: an unpaid order (`pending_payment`)
//! cancels immediately and returns its held stock to the listing; a paid
//! order moves to `cancel_requested` awaiting the seller. The seller's
//! approval moves the order to `cancelled` and returns the sold quantities
//! to available under the listings quantity-balance constraint.
//!
//! Cancellation never touches the payment record: a confirmed payment stays
//! confirmed with its receipt intact, and the only money path out of a
//! cancelled order is the externally evidenced `refund.record_external`
//! (ADR-0019 §7 — the service never claims to move funds).

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{OrderActionPayload, RequestCancellationPayload};
use marketplace_domain::state_machines::{can_transition, listing_machine, order_machine};
use marketplace_domain::{Command, ErrorCode};
use sqlx::{Postgres, Transaction};

use crate::handlers::{
    fetch_listing_for_update, fetch_order_for_update, finish_order_action, guard_order_action,
};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult};

pub async fn request(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RequestCancellationPayload,
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
            "Only the buyer may request cancellation.",
        )));
    }
    if !matches!(
        order.state.as_str(),
        "pending_payment" | "paid" | "processing"
    ) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "This order can no longer be cancelled.",
        )));
    }

    // An unpaid order cancels immediately and releases its hold; a paid
    // order awaits the seller's approval (prototype engine semantics).
    let immediate = order.state == "pending_payment";
    let (to_state, event_kind) = if immediate {
        ("cancelled", "order.cancelled")
    } else {
        ("cancel_requested", "order.cancel_requested")
    };
    debug_assert!(can_transition(&order_machine(), &order.state, to_state));
    if immediate {
        if let Err(failure) = release_reserved_hold(tx, &order, now).await? {
            return Ok(Err(failure));
        }
    }

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, cancellation_reason = $4, \
         updated_at = $5 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(to_state)
    .bind(&payload.reason)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.seller_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        event_kind,
        ("order_cancelled", &recipient),
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
            "Only the seller may approve cancellation.",
        )));
    }
    if order.state != "cancel_requested" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "No cancellation is pending.",
        )));
    }
    debug_assert!(can_transition(&order_machine(), &order.state, "cancelled"));

    // A cancel_requested order came from paid/processing, where payment
    // confirmation had already moved the quantities reserved -> sold; the
    // reversal returns them sold -> available (the listing machine declares
    // this transition for order.cancel_approve). The payment and receipt
    // stay untouched: the refund path is refund.record_external.
    if let Err(failure) = release_lines(tx, &order, HeldQuantity::Sold, now).await? {
        return Ok(Err(failure));
    }

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'cancelled', updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "order.cancelled",
        ("order_cancelled", &recipient),
        now,
    )
    .await
}

/// Returns an immediately cancelled unpaid order's held stock to its
/// listings, mirroring the prototype's `releaseOrderInventory`.
///
/// An auction winner's hold is released through the reservation
/// compare-and-swap: only the transition that flips the reservation from
/// `active` to `released` moves the quantities, so when the 30-minute hold
/// already lapsed on server time (the expiry sweep returned the unit and
/// marked the reservation `expired`), the cancel still succeeds without
/// releasing the same unit twice.
async fn release_reserved_hold(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    now: DateTime<Utc>,
) -> Result<Result<(), CommandFailure>, sqlx::Error> {
    if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
        let released = sqlx::query(
            "UPDATE reservations SET status = 'released', updated_at = $3 \
             WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 AND status = 'active'",
        )
        .bind(auction_aggregate_id)
        .bind(&order.buyer_pubky)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        if released.rows_affected() == 0 {
            return Ok(Ok(()));
        }
    }
    release_lines(tx, order, HeldQuantity::Reserved, now).await
}

/// Which listing quantity column an order's lines currently occupy:
/// `reserved` before payment confirmation, `sold` after it.
#[derive(Clone, Copy)]
enum HeldQuantity {
    Reserved,
    Sold,
}

impl HeldQuantity {
    fn column(self) -> &'static str {
        match self {
            HeldQuantity::Reserved => "reserved_quantity",
            HeldQuantity::Sold => "sold_quantity",
        }
    }
}

/// Moves each order line's quantity from the held column back to available
/// under the listings quantity-balance constraint. The held-quantity guard
/// is a compare-and-swap against the ledger: nothing else can remove this
/// order's contribution, so a shortfall is an invariant violation, not a
/// race to tolerate.
async fn release_lines(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    held: HeldQuantity,
    now: DateTime<Utc>,
) -> Result<Result<(), CommandFailure>, sqlx::Error> {
    let column = held.column();
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
        debug_assert!(can_transition(
            &listing_machine(),
            &listing.state,
            "available"
        ));
        let updated = sqlx::query(&format!(
            "UPDATE listings SET server_revision = server_revision + 1, state = 'available', \
             available_quantity = available_quantity + $2, {column} = {column} - $2, \
             updated_at = $3 WHERE aggregate_id = $1 AND {column} >= $2",
        ))
        .bind(aggregate_id)
        .bind(quantity)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvariantViolation,
                "The inventory held by this order is no longer accounted to its listing.",
            )));
        }
    }
    Ok(Ok(()))
}
