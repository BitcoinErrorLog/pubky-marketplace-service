//! Review commands. `review.create` is ported from the TypeScript prototype
//! engine: one review per participant per order per role, with the
//! uniqueness enforced by the `reviews_one_per_order_role` database
//! constraint rather than application logic — the review row is inserted
//! before the order's revision compare-and-swap, so a same-role race is
//! decided by the constraint, not by code. `review.update` is this service
//! only: the reviewer may revise their rating and text within a bounded
//! window of the review's creation.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::ReviewTermsPayload;
use marketplace_domain::state_machines::{can_transition, order_machine};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::executor::insert_event;
use crate::handlers::{
    fetch_order, fetch_order_for_update, fetch_order_reviews, guard_order_action,
    insert_notification_intent, order_json_with_reviews, REVIEW_COLUMNS,
};
use crate::model::{OrderRow, ReviewRow};
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// How long a reviewer may edit their review after creating it. This
/// service only — the prototype had no review editing, so the bound is a
/// deliberate policy choice documented in the README.
pub const REVIEW_EDIT_WINDOW_SECONDS: i64 = 24 * 60 * 60;

/// Order states eligible for review (prototype engine table).
const REVIEWABLE_STATES: [&str; 3] = ["delivered", "completed", "closed"];

pub async fn create(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ReviewTermsPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Deliberately no FOR UPDATE: the review insert happens before the
    // order's compare-and-swap, so two same-role commands racing past this
    // read are decided by the unique constraint.
    let Some(order) = fetch_order(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if !REVIEWABLE_STATES.contains(&order.state.as_str()) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order is not eligible for review.",
        )));
    }
    let existing: Option<(sqlx::types::Uuid,)> =
        sqlx::query_as("SELECT id FROM reviews WHERE order_id = $1 AND reviewer_pubky = $2")
            .bind(order.id)
            .bind(actor)
            .fetch_optional(&mut **tx)
            .await?;
    if existing.is_some() {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "This participant already reviewed the order.",
        )));
    }

    let (role, subject) = if actor == order.buyer_pubky {
        ("buyer", order.seller_pubky.clone())
    } else {
        ("seller", order.buyer_pubky.clone())
    };
    let review: ReviewRow = sqlx::query_as(&format!(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         rating, text, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) \
         RETURNING {REVIEW_COLUMNS}"
    ))
    .bind(command.command_id)
    .bind(order.id)
    .bind(actor)
    .bind(role)
    .bind(&subject)
    .bind(i32::try_from(payload.rating).expect("rating validated to 1..=5"))
    .bind(&payload.text)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let new_state = if order.state == "delivered" {
        "completed"
    } else {
        order.state.as_str()
    };
    debug_assert!(can_transition(&order_machine(), &order.state, new_state));
    let updated: Option<OrderRow> = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, updated_at = $4 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(new_state)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(updated) = updated else {
        let current: (i64,) = sqlx::query_as("SELECT revision FROM orders WHERE id = $1")
            .bind(order.id)
            .fetch_one(&mut **tx)
            .await?;
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The order revision is stale.",
            current.0,
        )));
    };

    let order_aggregate_id = ids::order_aggregate_id(updated.id);
    let event_id = insert_event(
        tx,
        command.command_id,
        &order_aggregate_id,
        updated.revision,
        actor,
        "review.created",
        now,
    )
    .await?;
    insert_notification_intent(
        tx,
        event_id,
        "review_received",
        &subject,
        actor,
        &order_aggregate_id,
        now,
    )
    .await?;
    let reviews = fetch_order_reviews(tx, updated.id).await?;
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "review",
            "order": order_json_with_reviews(&updated, &reviews),
            "review": review.view(),
        }),
    }))
}

pub async fn update(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ReviewTermsPayload,
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
    let review: Option<ReviewRow> = sqlx::query_as(&format!(
        "SELECT {REVIEW_COLUMNS} FROM reviews \
         WHERE order_id = $1 AND reviewer_pubky = $2 FOR UPDATE"
    ))
    .bind(order.id)
    .bind(actor)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(review) = review else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "This participant has not reviewed the order.",
        )));
    };
    if now > review.created_at + chrono::Duration::seconds(REVIEW_EDIT_WINDOW_SECONDS) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The review edit window has closed.",
        )));
    }

    let updated_review: ReviewRow = sqlx::query_as(&format!(
        "UPDATE reviews SET rating = $2, text = $3, updated_at = $4 \
         WHERE id = $1 RETURNING {REVIEW_COLUMNS}"
    ))
    .bind(review.id)
    .bind(i32::try_from(payload.rating).expect("rating validated to 1..=5"))
    .bind(&payload.text)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &ids::order_aggregate_id(updated.id),
        updated.revision,
        actor,
        "review.updated",
        now,
    )
    .await?;
    let reviews = fetch_order_reviews(tx, updated.id).await?;
    // No notification: the prototype emitted none (it had no review edits).
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "review",
            "order": order_json_with_reviews(&updated, &reviews),
            "review": updated_review.view(),
        }),
    }))
}
