//! Payment-time inventory holds: "only a payment should lock an item."
//!
//! Checkout no longer moves inventory for ordinary listings — it creates
//! the immutable order snapshot with NO hold. The hold is acquired at a
//! **payment lock point**, each of which calls [`acquire_payment_hold`]
//! inside its own command/request transaction:
//!
//! - `payment.register_locks` — window `LOCKS_PAYMENT_WINDOW_SECONDS` (the
//!   correlation window IS the hold window; one window concept, not two);
//! - the payment-method bind (`POST /v0/orders/{id}/payment-method`, all
//!   three rails) — window `FIAT_PAYMENT_WINDOW_SECONDS`;
//! - `payment.sandbox_advance` transitioning OUT of `awaiting_entitlement`
//!   (the first transition) — window `SANDBOX_PAYMENT_WINDOW_SECONDS`.
//!
//! The acquisition atomically moves `available → reserved` for the order's
//! quantities under the listing row lock, failing with
//! `INSUFFICIENT_INVENTORY` ([`SOLD_OUT_BEFORE_PAYMENT`]) when stock is
//! gone, and arms `orders.hold_expires_at`. A lock point on an order that
//! ALREADY holds stock never double-decrements: it re-arms the window for a
//! drop-bound order (the claim window extends to the payment window once a
//! payment starts) and is a no-op otherwise.
//!
//! Auction orders are excluded entirely: their hold is the winning
//! `reservations` row, taken at close and governed by reservation expiry.
//! Drop-bound checkout keeps lock-at-claim (the FCFS race is the product)
//! and arms `DROP_CLAIM_WINDOW_SECONDS` at checkout.
//!
//! The payment-window worker ([`crate::workers::expire_due_payment_windows`])
//! releases a lapsed hold, expires the payment, and cancels the order.

use chrono::{DateTime, Utc};
use marketplace_domain::state_machines::{can_transition, listing_machine};
use marketplace_domain::ErrorCode;
use sqlx::{Postgres, Transaction};

use crate::handlers::fetch_listing_for_update;
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::CommandFailure;

/// Refusal copy for a lock point that finds the stock already gone, pinned
/// by the client contract tests.
pub const SOLD_OUT_BEFORE_PAYMENT: &str = "The listing sold out before this payment started.";

/// Which listing quantity column an order's lines currently occupy:
/// `reserved` before payment confirmation, `sold` after it.
#[derive(Clone, Copy)]
pub(crate) enum HeldQuantity {
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
pub(crate) async fn release_lines(
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

/// The payment lock point: acquires (or re-arms) the order's inventory hold
/// inside the caller's transaction. Returns the order with its hold columns
/// current.
///
/// - An auction order is untouched: its hold is the winning reservation.
/// - An order that is no longer `pending_payment` refuses the lock point —
///   a cancelled or already-paid order must never grab stock.
/// - An order that already holds stock never double-decrements; a
///   drop-bound one re-arms its window to this lock point's (longer) span,
///   an ordinary one re-arms nothing.
/// - Otherwise each line atomically moves `available → reserved` under the
///   listing row lock (`INSUFFICIENT_INVENTORY` with
///   [`SOLD_OUT_BEFORE_PAYMENT`] when the stock is gone) and the hold
///   window arms at `now + window_seconds`.
pub async fn acquire_payment_hold(
    tx: &mut Transaction<'_, Postgres>,
    order: OrderRow,
    window_seconds: i64,
    now: DateTime<Utc>,
) -> Result<Result<OrderRow, CommandFailure>, sqlx::Error> {
    if order.auction_aggregate_id.is_some() {
        return Ok(Ok(order));
    }
    if order.state != "pending_payment" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Only an order pending payment can start a payment.",
        )));
    }
    let hold_expires_at = now + chrono::Duration::seconds(window_seconds);
    if order.stock_held {
        if order.drop_aggregate_id.is_none() {
            return Ok(Ok(order));
        }
        // A drop claim's window extends to the payment window once a
        // payment starts; the stock itself was already debited at claim.
        let rearmed: OrderRow = sqlx::query_as(&format!(
            "UPDATE orders SET hold_expires_at = $2, updated_at = $3 \
             WHERE id = $1 RETURNING {ORDER_COLUMNS}"
        ))
        .bind(order.id)
        .bind(hold_expires_at)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        return Ok(Ok(rearmed));
    }

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
        if listing.available_quantity < quantity {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InsufficientInventory,
                SOLD_OUT_BEFORE_PAYMENT,
            )));
        }
        let new_state = if listing.available_quantity == quantity {
            "reserved"
        } else {
            "available"
        };
        debug_assert!(can_transition(
            &listing_machine(),
            &listing.state,
            new_state
        ));
        // The row is locked above, so the guarded UPDATE cannot lose a
        // race; the guard plus the CHECK constraints are the backstop.
        let updated = sqlx::query(
            "UPDATE listings SET server_revision = server_revision + 1, state = $2, \
             available_quantity = available_quantity - $3, \
             reserved_quantity = reserved_quantity + $3, updated_at = $4 \
             WHERE aggregate_id = $1 AND available_quantity >= $3",
        )
        .bind(aggregate_id)
        .bind(new_state)
        .bind(quantity)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InsufficientInventory,
                SOLD_OUT_BEFORE_PAYMENT,
            )));
        }
    }

    let held: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET stock_held = true, hold_expires_at = $2, updated_at = $3 \
         WHERE id = $1 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(hold_expires_at)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Ok(held))
}
