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
//! Cancellation never touches a confirmed payment: it stays confirmed with
//! its receipt intact, and the only money path out of a
//! cancelled order is the externally evidenced `refund.record_external`
//! (ADR-0019 §7 — the service never claims to move funds).
//!
//! Drop-stamped orders (ADR-0026): both release paths — the immediate
//! cancel of an unpaid order and the seller's approval of a paid one — also
//! credit the stamped drop's counters, in the same transaction and with the
//! drop row locked BEFORE any listing row (the shared lock order). The
//! credit restocks a live drop; an ended drop keeps honest books but
//! nothing reopens.
//!
//! Paykit-rail orders: the immediate cancel ends the unpaid payment
//! (`awaiting_entitlement → expired`) so money that still reaches the
//! request takes the late-money fork, and a `preparing` request is voided
//! in the same transaction — activation state `voided`, the undelivered
//! `paykit.activate` row stamped, one `paykit.void` row enqueued — so no
//! activation can publish it afterwards. Once money is observed on the
//! request (`detected`, `awaiting_seller_confirmation`, `confirmed`) the
//! buyer can no longer cancel: the hold stays with the payment it backs.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{OrderActionPayload, RequestCancellationPayload};
use marketplace_domain::state_machines::{can_transition, order_machine};
use marketplace_domain::{ids, Command, ErrorCode};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
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
    // Payment before order: the settlement paths lock in that order, so a
    // cancel racing a confirmation waits instead of deadlocking.
    let payment = lock_payment(tx, payload.order_id).await?;
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the buyer may request cancellation.",
        )));
    }
    if !matches!(
        order.state.as_str(),
        "pending_payment" | "paid" | "processing" | "ready_for_pickup"
    ) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "This order can no longer be cancelled.",
        )));
    }
    if order.state == "pending_payment" && paykit_money_observed(&order) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "This order can no longer be cancelled.",
        )));
    }

    // The unilateral buyer exits of the pickup design (§A3/§A6). From
    // `paid` or `ready_for_pickup` on a PICKUP order, the request moves the
    // order straight to `cancelled` — no seller approval — while either:
    //   * a post-payment terms change exists (any line's current details
    //     version > `version_at_payment`, or the details were cleared); or
    //   * the bounded withdrawal window is open (`first_revealed_at`
    //     stamped, no handover confirm yet — `fulfillment.mark_ready` does
    //     NOT close it, and a handover confirm has already moved the order
    //     out of these states, closing the window by construction).
    // Outside those conditions — including a request that races
    // `mark_ready` before any first reveal — the same command yields the
    // ordinary `cancel_requested` awaiting the seller, as today.
    let unilateral = if order.fulfillment == "pickup"
        && matches!(order.state.as_str(), "paid" | "ready_for_pickup")
    {
        let terms_changed =
            crate::handlers::pickup::order_has_unresolved_terms_change(tx, &order).await?;
        let withdrawal_open = order.first_revealed_at.is_some();
        terms_changed || withdrawal_open
    } else {
        false
    };

    // An unpaid order cancels immediately and releases its hold — when one
    // exists; a paid order awaits the seller's approval (prototype engine
    // semantics), unless a unilateral pickup exit applies. Under "only a
    // payment locks an item" an ordinary pending order holds nothing until
    // a payment lock point runs, so cancelling it releases nothing.
    let immediate = order.state == "pending_payment";
    let (to_state, event_kind) = if immediate {
        ("cancelled", "order.cancelled")
    } else if unilateral {
        // The distinct event kind (§A3): the reputation worker's
        // `terminated_badly` aggregation excludes the WHOLE order when its
        // terminal cancel is `order.cancelled_terms_change`, including any
        // `refund.recorded_external` leg on it.
        ("cancelled", "order.cancelled_terms_change")
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
                if !legacy_null_expiry_unaccounted(&order, &failure) {
                    return Ok(Err(failure));
                }
                tracing::warn!(
                    order_id = %order.id,
                    "legacy null-expiry hold: drop units unaccounted; still cancelling"
                );
            }
            if let Err(failure) = release_lines(tx, &order, HeldQuantity::Reserved, now).await? {
                if !legacy_null_expiry_unaccounted(&order, &failure) {
                    return Ok(Err(failure));
                }
                tracing::warn!(
                    order_id = %order.id,
                    "legacy null-expiry hold: listing unaccounted; still cancelling"
                );
            }
        }
    } else if unilateral {
        // The unilateral exit reuses `approve`'s release path VERBATIM
        // (§A8): the payment confirmation moved the quantities
        // reserved -> sold, so the reversal credits a stamped drop first
        // and then returns them sold -> available — the listing machine
        // declares this edge for `order.cancel_request` exactly as for
        // `order.cancel_approve`. The payment and receipt stay untouched:
        // the refund remains seller-recorded external evidence
        // (`refund.record_external`, ADR-0019); cancelling moves no money.
        if let Err(failure) = credit_order_drop(tx, &order, now).await? {
            return Ok(Err(failure));
        }
        if let Err(failure) = release_lines(tx, &order, HeldQuantity::Sold, now).await? {
            return Ok(Err(failure));
        }
    }

    let paykit_void = match (&payment, immediate) {
        (Some(payment), true) => {
            end_paykit_request(tx, &order, payment, actor, command.command_id, now).await?
        }
        _ => None,
    };

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
    let result = finish_order_action(
        tx,
        actor,
        command,
        &updated,
        event_kind,
        ("order_cancelled", &recipient),
        now,
    )
    .await?;
    if let (Some(pin), Ok(success)) = (&paykit_void, &result) {
        let event_id = *success
            .event_ids
            .first()
            .expect("an order action records its event");
        enqueue_paykit_void(tx, event_id, order.id, pin, "order_cancelled", now).await?;
    }
    Ok(result)
}

/// Money was observed on the order's Paykit request: the payment it backs
/// is confirming (or confirmed into review), so the hold must stay.
fn paykit_money_observed(order: &OrderRow) -> bool {
    matches!(
        order.paykit_request_state.as_deref(),
        Some("detected" | "awaiting_seller_confirmation" | "confirmed")
    )
}

/// The order's payment row, locked: `(id, adapter, state)`.
struct LockedPayment {
    id: Uuid,
    adapter: String,
    state: String,
}

async fn lock_payment(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<LockedPayment>, sqlx::Error> {
    let row: Option<(Uuid, String, String)> =
        sqlx::query_as("SELECT id, adapter, state FROM payments WHERE order_id = $1 FOR UPDATE")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(row.map(|(id, adapter, state)| LockedPayment { id, adapter, state }))
}

/// The persisted phase-1 pin a `paykit.void` row is dialed with.
pub(crate) struct PaykitVoidPin {
    pub invoice_id: Uuid,
    pub stack_id: String,
    pub stack_endpoint: String,
}

/// Ends the Paykit leg of an unpaid order that is leaving `pending_payment`
/// by the buyer's hand, inside the cancel transaction and before the order
/// row's revision bump. The `awaiting_entitlement` payment expires, so a
/// settlement that still arrives is late money (`apply_late_money`), never
/// an ordinary confirmation of a cancelled order. A `preparing` request is
/// voided locally and its undelivered activate row stamped; the returned pin
/// is the void the caller enqueues once the order event exists. An `active`
/// request stays `pending` so the observation tail keeps polling it.
async fn end_paykit_request(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    payment: &LockedPayment,
    actor: &str,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Option<PaykitVoidPin>, sqlx::Error> {
    if payment.adapter != "paykit" {
        return Ok(None);
    }
    if payment.state == "awaiting_entitlement" {
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'expired', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        insert_event(
            tx,
            command_id,
            &ids::payment_aggregate_id(payment.id),
            revision,
            actor,
            "payment.expired",
            now,
        )
        .await?;
    }
    if order.paykit_activation_state.as_deref() != Some("preparing") {
        return Ok(None);
    }
    let (Some(invoice_id), Some(stack_id), Some(stack_endpoint)) = (
        order.paykit_invoice_id,
        order.paykit_stack_id.clone(),
        order.paykit_stack_endpoint.clone(),
    ) else {
        tracing::error!(
            order_id = %order.id,
            "ALERT a preparing order is missing its persisted paykit pin at cancel"
        );
        return Ok(None);
    };
    void_preparing_request(tx, order.id, now).await?;
    Ok(Some(PaykitVoidPin {
        invoice_id,
        stack_id,
        stack_endpoint,
    }))
}

/// `preparing → voided` for an order that is no longer payable: the request
/// state clears and the undelivered `paykit.activate` row is stamped, so no
/// outbox pass can publish the invoice. The remote void is the caller's
/// `paykit.void` row.
pub(crate) async fn void_preparing_request(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE orders SET paykit_activation_state = 'voided', paykit_request_state = NULL, \
         updated_at = $2 WHERE id = $1 AND paykit_activation_state = 'preparing'",
    )
    .bind(order_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE outbox SET delivered_at = $2 WHERE kind = 'paykit.activate' \
         AND payload->>'order_id' = $1 AND delivered_at IS NULL",
    )
    .bind(order_id.to_string())
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// One `paykit.void` row for the order's prepared invoice, retried by the
/// outbox arm under its 24-hour bound.
pub(crate) async fn enqueue_paykit_void(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    order_id: Uuid,
    pin: &PaykitVoidPin,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO outbox (event_id, kind, payload, created_at) \
         VALUES ($1, 'paykit.void', $2, $3)",
    )
    .bind(event_id)
    .bind(serde_json::json!({
        "invoice_id": pin.invoice_id,
        "order_id": order_id,
        "stack_id": pin.stack_id,
        "stack_endpoint": pin.stack_endpoint,
        "reason": reason,
    }))
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn approve(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the seller may approve cancellation.",
        )));
    }
    if order.state != "cancel_requested" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
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

/// Pre-#50 held rows can carry `stock_held` with a NULL window. Releasing
/// them is best-effort: an unaccounted listing must not park the cancel in
/// `cancel_requested`.
fn legacy_null_expiry_unaccounted(order: &OrderRow, failure: &CommandFailure) -> bool {
    order.hold_expires_at.is_none() && failure.code() == ErrorCode::InvariantViolation
}

/// Credits a drop-stamped order's units back to its drop before the listing
/// releases below run (drop lock before listing locks). A no-op for orders
/// without a drop stamp — including auction and offer orders, which never
/// debited a drop.
pub(crate) async fn credit_order_drop(
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
        Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
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
pub(crate) async fn release_reserved_hold(
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
