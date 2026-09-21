//! Exclusive inventory holds: checkout parks a unit; a payment re-arms the
//! window.
//!
//! Ordinary `checkout.create` acquires a bounded hold
//! (`CHECKOUT_HOLD_WINDOW_SECONDS`) inside the checkout transaction. Sibling
//! checkout is refused until that hold pays, cancels, or expires. Bind,
//! Locks registration, and sandbox first-advance **re-arm**
//! `hold_expires_at` to the rail window (they never double-decrement).
//!
//! - `checkout.create` — window `CHECKOUT_HOLD_WINDOW_SECONDS` (default 900);
//! - `payment.register_locks` — window `LOCKS_PAYMENT_WINDOW_SECONDS`;
//! - payment-method bind — fiat `FIAT_PAYMENT_WINDOW_SECONDS`, bitcoin
//!   `BITCOIN_PAYMENT_WINDOW_SECONDS`;
//! - `payment.sandbox_advance` leaving `awaiting_entitlement` — window
//!   `SANDBOX_PAYMENT_WINDOW_SECONDS`.
//!
//! Live-test allow-list, `PAYMENT_RAILS_DISABLED`, and amount caps still run
//! **before** the bind re-arm. Auction orders are excluded: their hold is
//! the winning `reservations` row. Drop-bound checkout keeps lock-at-claim
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

pub const HOLD_SOURCE_CHECKOUT: &str = "checkout";
pub const HOLD_SOURCE_LOCKS: &str = "locks";
pub const HOLD_SOURCE_BIND: &str = "bind";
pub const HOLD_SOURCE_SANDBOX: &str = "sandbox";
pub const HOLD_SOURCE_DROP_CLAIM: &str = "drop_claim";

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
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
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
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "The inventory held by this order is no longer accounted to its listing.",
            )));
        }
    }
    Ok(Ok(()))
}

/// Acquires (or re-arms) the order's inventory hold inside the caller's
/// transaction. Returns the order with its hold columns current.
///
/// - An auction order is untouched: its hold is the winning reservation.
/// - An order that is no longer `pending_payment` refuses the lock point —
///   a cancelled or already-paid order must never grab stock.
/// - An order that already holds stock never double-decrements; it re-arms
///   `hold_expires_at` and `hold_source` to this lock point's window.
/// - Otherwise each line atomically moves `available → reserved` under the
///   listing row lock (`INSUFFICIENT_INVENTORY` with
///   [`SOLD_OUT_BEFORE_PAYMENT`] when the stock is gone) and the hold
///   window arms at `now + window_seconds`.
pub async fn acquire_payment_hold(
    tx: &mut Transaction<'_, Postgres>,
    order: OrderRow,
    window_seconds: i64,
    hold_source: &str,
    now: DateTime<Utc>,
) -> Result<Result<OrderRow, CommandFailure>, sqlx::Error> {
    if order.auction_aggregate_id.is_some() {
        return Ok(Ok(order));
    }
    if order.state != "pending_payment" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only an order pending payment can start a payment.",
        )));
    }
    let hold_expires_at = now + chrono::Duration::seconds(window_seconds);
    if order.stock_held {
        let rearmed: OrderRow = sqlx::query_as(&format!(
            "UPDATE orders SET hold_expires_at = $2, hold_source = $3, updated_at = $4 \
             WHERE id = $1 RETURNING {ORDER_COLUMNS}"
        ))
        .bind(order.id)
        .bind(hold_expires_at)
        .bind(hold_source)
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
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "An order line's listing is missing.",
            )));
        };
        if listing.available_quantity < quantity {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InsufficientInventory,
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
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InsufficientInventory,
                ErrorCode::InsufficientInventory,
                SOLD_OUT_BEFORE_PAYMENT,
            )));
        }
    }

    let held: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET stock_held = true, hold_expires_at = $2, hold_source = $3, \
         updated_at = $4 WHERE id = $1 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(hold_expires_at)
    .bind(hold_source)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Ok(held))
}
