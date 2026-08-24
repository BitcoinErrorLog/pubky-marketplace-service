//! Order cancellation commands (`order.cancel_request`,
//! `order.cancel_approve`), ported from the TypeScript prototype engine.
//!
//! The buyer requests cancellation: an unpaid order (`pending_payment`)
//! cancels immediately and returns its held stock to the listing — but ONLY
//! when a hold exists ("only a payment locks an item": an ordinary pending
//! order holds nothing until a payment lock point runs, so cancelling it
//! releases nothing). A paid order moves to `cancel_requested` awaiting the
//! seller. The seller's approval moves the order to `cancelled` and returns
//! the sold quantities to available under the listings quantity-balance
//! constraint.
//!
//! Cancellation never touches the payment record: a confirmed payment stays
//! confirmed with its receipt intact, and the only money path out of a
//! cancelled order is the externally evidenced `refund.record_external`
//! (ADR-0019 §7 — the service never claims to move funds).
//!
//! Drop-stamped orders (ADR-0026): both release paths — the immediate
//! cancel of an unpaid order and the seller's approval of a paid one — also
//! credit the stamped drop's counters, in the same transaction and with the
//! drop row locked BEFORE any listing row (the shared lock order). The
//! credit restocks a live drop; an ended drop keeps honest books but
//! nothing reopens.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{OrderActionPayload, RequestCancellationPayload};
use marketplace_domain::state_machines::{can_transition, order_machine};
use marketplace_domain::{Command, ErrorCode};
use sqlx::{Postgres, Transaction};

use crate::handlers::holds::{release_lines, HeldQuantity};
use crate::handlers::{fetch_order_for_update, finish_order_action, guard_order_action};
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

    // An unpaid order cancels immediately and releases its hold — when one
    // exists; a paid order awaits the seller's approval (prototype engine
    // semantics). Under "only a payment locks an item" an ordinary pending
    // order holds nothing until a payment lock point runs, so cancelling it
    // releases nothing.
    let immediate = order.state == "pending_payment";
    let (to_state, event_kind) = if immediate {
        ("cancelled", "order.cancelled")
    } else {
        ("cancel_requested", "order.cancel_requested")
    };
    debug_assert!(can_transition(&order_machine(), &order.state, to_state));
    if immediate {
        if order.auction_aggregate_id.is_some() {
            // An auction winner's hold lives in the winning reservation and
            // releases through its compare-and-swap.
            if let Err(failure) = release_reserved_hold(tx, &order, now).await? {
                return Ok(Err(failure));
            }
        } else if order.stock_held {
            if let Err(failure) = credit_order_drop(tx, &order, now).await? {
                return Ok(Err(failure));
            }
            if let Err(failure) = release_lines(tx, &order, HeldQuantity::Reserved, now).await? {
                return Ok(Err(failure));
            }
        }
    }

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, cancellation_reason = $4, \
         stock_held = false, hold_expires_at = NULL, \
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
    if let Err(failure) = credit_order_drop(tx, &order, now).await? {
        return Ok(Err(failure));
    }
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

/// Credits a drop-stamped order's units back to its drop before the listing
/// releases below run (drop lock before listing locks). A no-op for orders
/// without a drop stamp — including auction and offer orders, which never
/// debited a drop.
async fn credit_order_drop(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    now: DateTime<Utc>,
) -> Result<Result<(), CommandFailure>, sqlx::Error> {
    let Some(drop_aggregate_id) = &order.drop_aggregate_id else {
        return Ok(Ok(()));
    };
    let units: i64 = order
        .lines
        .as_array()
        .expect("order lines are an array")
        .iter()
        .map(|line| {
            line["quantity"]
                .as_i64()
                .expect("order line carries its quantity")
        })
        .sum();
    if crate::handlers::drops::credit_drop_release(
        tx,
        drop_aggregate_id,
        &order.buyer_pubky,
        units,
        now,
    )
    .await?
    {
        Ok(Ok(()))
    } else {
        Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "The drop units held by this order are no longer accounted to its drop.",
        )))
    }
}

/// Returns a cancelled auction winner's held stock through the reservation
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
    let auction_aggregate_id = order
        .auction_aggregate_id
        .as_ref()
        .expect("only auction orders release through the reservation");
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
    release_lines(tx, order, HeldQuantity::Reserved, now).await
}
