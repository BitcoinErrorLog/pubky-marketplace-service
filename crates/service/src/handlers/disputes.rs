//! Dispute commands. `dispute.open` and `dispute.resolve` are ported from
//! the TypeScript prototype engine; `dispute.evidence` is this service only
//! (the prototype had no evidence records). Moderator authority for
//! `dispute.resolve` is the configured `MODERATOR_PUBKYS` role, exactly as
//! for `trust.decide`.
//!
//! Evidence bodies are private order evidence (ADR-0019 §8): they are stored
//! append-only in `dispute_evidence` and never appear in general read
//! projections or command results — the dispute sub-document carries only a
//! content-free `evidence_count`. The case file is served exclusively by the
//! scoped read `GET /v1/orders/{id}/evidence` (the two dispute participants
//! plus configured moderators, with moderator reads audited append-only);
//! see [`crate::queries::list_evidence`].

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    DisputeEvidencePayload, OpenDisputePayload, ResolveDisputePayload,
};
use marketplace_domain::state_machines::{can_transition, dispute_machine, order_machine};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    fetch_order_for_update, fetch_order_reviews, finish_order_action, guard_order_action,
    insert_notification_intent, order_json_with_reviews,
};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// Order states from which a participant may open a dispute (prototype
/// engine table).
const DISPUTABLE_STATES: [&str; 7] = [
    "paid",
    "processing",
    "shipped",
    "delivered",
    "completed",
    "return_requested",
    "return_approved",
];

pub async fn open(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OpenDisputePayload,
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
    if !DISPUTABLE_STATES.contains(&order.state.as_str()) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "This order cannot enter dispute.",
        )));
    }
    if order.dispute.is_some() {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "A dispute already exists.",
        )));
    }
    debug_assert!(can_transition(&order_machine(), &order.state, "disputed"));

    let remedy = serde_json::to_value(payload.requested_remedy).expect("enum serializes");
    let dispute = json!({
        "state": "open",
        "opened_by": actor,
        "reason": payload.reason,
        "requested_remedy": remedy,
        "resolution": null,
        "rationale": null,
        "opened_at": format_timestamp(now),
        "resolved_at": null,
        "evidence_count": 0,
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'disputed', dispute = $3, \
         updated_at = $4 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&dispute)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = if actor == updated.buyer_pubky {
        updated.seller_pubky.clone()
    } else {
        updated.buyer_pubky.clone()
    };
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "dispute.opened",
        ("dispute_updated", &recipient),
        now,
    )
    .await
}

pub async fn add_evidence(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &DisputeEvidencePayload,
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
    let Some(mut dispute) = order
        .dispute
        .clone()
        .filter(|dispute| order.state == "disputed" && dispute["state"] == json!("open"))
    else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "No open dispute accepts evidence.",
        )));
    };

    sqlx::query(
        "INSERT INTO dispute_evidence (id, order_id, submitter_pubky, body, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(order.id)
    .bind(actor)
    .bind(&payload.body)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let count = dispute["evidence_count"].as_i64().unwrap_or(0) + 1;
    dispute["evidence_count"] = json!(count);
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, dispute = $3, updated_at = $4 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&dispute)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &ids::order_aggregate_id(updated.id),
        updated.revision,
        actor,
        "dispute.evidence_added",
        now,
    )
    .await?;
    let reviews = fetch_order_reviews(tx, updated.id).await?;
    // No notification: the prototype emitted none for evidence (it had no
    // evidence records), and the body itself is never echoed back.
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "order",
            "order": order_json_with_reviews(&updated, &reviews),
        }),
    }))
}

pub async fn resolve(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ResolveDisputePayload,
    moderator_pubkys: &[String],
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The dispute order was not found.",
        )));
    };
    if !moderator_pubkys.iter().any(|entry| entry == actor) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only a configured moderator may resolve disputes.",
        )));
    }
    // The prototype folds the aggregate identity check into the revision
    // conflict, so both mismatches answer identically.
    if command.aggregate_id != ids::order_aggregate_id(order.id)
        || command.expected_revision != order.revision
    {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The dispute order revision is stale.",
            order.revision,
        )));
    }
    let Some(mut dispute) = order
        .dispute
        .clone()
        .filter(|dispute| order.state == "disputed" && dispute["state"] == json!("open"))
    else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "No open dispute can be resolved.",
        )));
    };
    debug_assert!(can_transition(&dispute_machine(), "open", "resolved"));

    // Buyer remedies leave the order disputed awaiting the externally
    // evidenced refund; the others complete it (prototype engine).
    let new_state = if payload.resolution.favors_buyer() {
        "disputed"
    } else {
        "completed"
    };
    debug_assert!(can_transition(&order_machine(), &order.state, new_state));
    dispute["state"] = json!("resolved");
    dispute["resolution"] = json!(payload.resolution.as_str());
    dispute["rationale"] = json!(payload.rationale);
    dispute["resolved_at"] = json!(format_timestamp(now));
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, dispute = $4, updated_at = $5 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(new_state)
    .bind(&dispute)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let order_aggregate_id = ids::order_aggregate_id(updated.id);
    let event_id = insert_event(
        tx,
        command.command_id,
        &order_aggregate_id,
        updated.revision,
        actor,
        "dispute.resolved",
        now,
    )
    .await?;
    for recipient in [&updated.buyer_pubky, &updated.seller_pubky] {
        insert_notification_intent(
            tx,
            event_id,
            "dispute_updated",
            recipient,
            actor,
            &order_aggregate_id,
            now,
        )
        .await?;
    }
    let reviews = fetch_order_reviews(tx, updated.id).await?;
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "order",
            "order": order_json_with_reviews(&updated, &reviews),
        }),
    }))
}
