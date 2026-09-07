//! Fulfillment commands (`fulfillment.ship`, `fulfillment.confirm_delivery`),
//! ported from the TypeScript prototype engine. The seller ships with a
//! carrier and tracking number; the buyer confirms receipt, which marks the
//! shipment delivered. Tracking numbers are participant-visible; the
//! delivery address never appears in any response (ADR-0019 §8).

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{OrderActionPayload, ShipOrderPayload};
use marketplace_domain::state_machines::{can_transition, order_machine};
use marketplace_domain::{Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::clock::format_timestamp;
use crate::handlers::{fetch_order_for_update, finish_order_action, guard_order_action};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult};

pub async fn ship(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ShipOrderPayload,
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
            "Only the seller may ship this order.",
        )));
    }
    // A pickup order has no shipment (§A6): the handover commands are its
    // fulfillment path.
    if order.fulfillment == "pickup" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "A pickup order cannot be shipped; use the pickup handover commands.",
        )));
    }
    if !matches!(order.state.as_str(), "paid" | "processing") {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not ready to ship.",
        )));
    }
    debug_assert!(can_transition(&order_machine(), &order.state, "shipped"));

    let shipment = json!({
        "carrier": payload.carrier,
        "tracking_number": payload.tracking_number,
        "state": "shipped",
        "shipped_at": format_timestamp(now),
        "delivered_at": null,
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'shipped', shipment = $3, \
         updated_at = $4 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&shipment)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "fulfillment.shipped",
        ("order_shipped", &recipient),
        now,
    )
    .await
}

pub async fn confirm_delivery(
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
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the buyer may confirm delivery.",
        )));
    }
    if order.fulfillment == "pickup" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "A pickup order has no shipment to confirm; use fulfillment.confirm_pickup.",
        )));
    }
    let Some(shipment) = order.shipment.clone().filter(|_| order.state == "shipped") else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not awaiting delivery confirmation.",
        )));
    };
    debug_assert!(can_transition(&order_machine(), &order.state, "delivered"));

    let mut delivered = shipment;
    delivered["state"] = json!("delivered");
    delivered["delivered_at"] = json!(format_timestamp(now));
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'delivered', shipment = $3, \
         updated_at = $4 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&delivered)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.seller_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "fulfillment.delivered",
        ("order_delivered", &recipient),
        now,
    )
    .await
}

/// Guards a pickup-path command (`fulfillment.mark_ready`,
/// `fulfillment.confirm_pickup`): the order must be a pickup order (the
/// shipped-order commands are refused for it, and vice versa — §A6).
fn guard_pickup_order(order: &OrderRow) -> Option<CommandFailure> {
    if order.fulfillment != "pickup" {
        return Some(CommandFailure::new(
            ErrorCode::InvalidState,
            "This command applies only to pickup orders.",
        ));
    }
    None
}

/// `fulfillment.mark_ready` (seller, own order, state `paid`): the seller
/// arms pickup readiness and the buyer is notified (`pickup_ready`, §A6/A7).
/// Marking ready deliberately does NOT close the buyer's bounded withdrawal
/// window — it is seller-controlled and instant, so a window that closed on
/// it would let the seller delete the buyer's exit at will (§A3).
pub async fn mark_ready(
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
            "Only the seller may mark the order ready for pickup.",
        )));
    }
    if let Some(failure) = guard_pickup_order(&order) {
        return Ok(Err(failure));
    }
    if order.state != "paid" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not awaiting pickup readiness.",
        )));
    }
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        "ready_for_pickup"
    ));

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'ready_for_pickup', updated_at = $3 \
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
        "fulfillment.ready_for_pickup",
        ("pickup_ready", &recipient),
        now,
    )
    .await
}

/// `fulfillment.confirm_pickup` (buyer OR seller, own order, state `paid`
/// or `ready_for_pickup`): records the handover and moves the order to
/// `delivered`, emitting the same `fulfillment.delivered` kind a shipped
/// order's confirmation emits, so reputation and feed consumers see one
/// delivery fact (§A6).
///
/// The handover record carries who confirmed and the server instant, one
/// row per order (PRIMARY KEY on `order_id`): a duplicate or replayed
/// confirm cannot write a second row. One asymmetry is binding: a
/// SELLER-actor confirm is refused while an unresolved post-payment terms
/// change exists on the order, so a seller cannot edit the meeting point
/// and immediately self-confirm the handover to delete the buyer's
/// unilateral-cancel exit before the buyer saw the change; a buyer-actor
/// confirm stays allowed (the buyer may accept the new terms by showing
/// up). A seller-only confirm is a seller-ATTESTED handover: reputation
/// counts it only on a buyer confirm or a dispute-free auto-complete.
pub async fn confirm_pickup(
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
    if let Some(failure) = guard_pickup_order(&order) {
        return Ok(Err(failure));
    }
    if !matches!(order.state.as_str(), "paid" | "ready_for_pickup") {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not awaiting a pickup handover.",
        )));
    }
    let confirming_role = if actor == order.buyer_pubky {
        "buyer"
    } else {
        "seller"
    };
    if confirming_role == "seller"
        && crate::handlers::pickup::order_has_unresolved_terms_change(tx, &order).await?
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The pickup terms changed after payment; the seller cannot confirm the handover \
             until the buyer has seen the change.",
        )));
    }
    debug_assert!(can_transition(&order_machine(), &order.state, "delivered"));

    // One handover per order: the PRIMARY KEY on order_id makes a duplicate
    // or replayed confirm a no-row (the state guard above rejects the
    // repeat command before this point in every non-concurrent interleave).
    sqlx::query(
        "INSERT INTO pickup_handovers (order_id, confirmed_by, confirmed_at) \
         VALUES ($1, $2, $3) ON CONFLICT (order_id) DO NOTHING",
    )
    .bind(order.id)
    .bind(confirming_role)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'delivered', updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = if confirming_role == "buyer" {
        updated.seller_pubky.clone()
    } else {
        updated.buyer_pubky.clone()
    };
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "fulfillment.delivered",
        ("order_delivered", &recipient),
        now,
    )
    .await
}
