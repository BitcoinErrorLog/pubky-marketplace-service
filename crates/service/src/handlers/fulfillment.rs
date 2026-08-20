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
