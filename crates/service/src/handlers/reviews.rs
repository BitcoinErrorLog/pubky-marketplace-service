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
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::attestor::{amount_band, claim_role, Attestor};
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
    attestor: Option<&Attestor>,
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

    // Purchase attestation (ADR 0024): issued inside this same transaction,
    // stored append-only for idempotent re-fetch, and returned in the
    // command result for the client to embed in the published record.
    let attestation = match attestor {
        Some(attestor) => Some(
            issue_attestation(
                tx,
                attestor,
                &order,
                actor,
                &subject,
                role,
                payload.allow_amount_band,
                now,
            )
            .await?,
        ),
        None => None,
    };

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
        None,
        now,
    )
    .await?;
    let reviews = fetch_order_reviews(tx, updated.id).await?;
    let mut result = json!({
        "kind": "review",
        "order": order_json_with_reviews(&updated, &reviews),
        "review": review.view(),
    });
    if let Some(attestation) = attestation {
        result["attestation"] = attestation;
    }
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result,
    }))
}

/// Issues and stores the purchase attestation for one (order, reviewer)
/// pair, applying the D2 both-sides amount-band consent gate. Returns the
/// `{ jws, claims }` object the command result carries.
#[allow(clippy::too_many_arguments)]
async fn issue_attestation(
    tx: &mut Transaction<'_, Postgres>,
    attestor: &Attestor,
    order: &OrderRow,
    reviewer: &str,
    subject: &str,
    role: &str,
    reviewer_allows_band: bool,
    now: DateTime<Utc>,
) -> Result<Value, sqlx::Error> {
    let listing_uri = listing_uri_from_order(order);

    // Day-granularity completion date: the delivery confirmation when the
    // order was delivered, otherwise the issuance day (orders completed via
    // dispute resolution never carry a delivery event).
    let delivered: Option<(DateTime<Utc>,)> = sqlx::query_as(
        "SELECT occurred_at FROM events WHERE aggregate_id = $1 AND kind = 'fulfillment.delivered' \
         ORDER BY occurred_at DESC LIMIT 1",
    )
    .bind(ids::order_aggregate_id(order.id))
    .fetch_optional(&mut **tx)
    .await?;
    let completed_on = delivered
        .map(|(occurred_at,)| occurred_at)
        .unwrap_or(now)
        .format("%Y-%m-%d")
        .to_string();

    // D2 both-sides consent: the band is included only when the seller's
    // standing preference allows it AND the reviewer opted in.
    let seller_allows: Option<(bool,)> = sqlx::query_as(
        "SELECT allows_amount_band FROM attestation_band_consents WHERE seller_pubky = $1",
    )
    .bind(&order.seller_pubky)
    .fetch_optional(&mut **tx)
    .await?;
    let band = if reviewer_allows_band && seller_allows.map(|(allows,)| allows).unwrap_or(false) {
        amount_band(&order.currency, order.total_minor)
    } else {
        None
    };

    let issued = attestor.issue_purchase_attestation(
        order.id,
        reviewer,
        subject,
        claim_role(role),
        &listing_uri,
        &completed_on,
        band,
        now,
    );
    sqlx::query(
        "INSERT INTO review_attestations (order_id, reviewer_pubky, order_ref, jws, claims, issued_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(order.id)
    .bind(reviewer)
    .bind(&issued.order_ref)
    .bind(&issued.jws)
    .bind(&issued.claims)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(json!({ "jws": issued.jws, "claims": issued.claims }))
}

/// The canonical public listing URI for the order's (single-listing) line
/// set: every order row carries `lines[0].listing_aggregate_id` in the
/// `listing:{seller}_{listing_id}` format, and the seller is the order's
/// seller by construction (checkout groups lines per seller).
fn listing_uri_from_order(order: &OrderRow) -> String {
    let aggregate_id = order.lines[0]["listing_aggregate_id"]
        .as_str()
        .expect("order lines always carry a listing aggregate id");
    let listing_id = aggregate_id
        .strip_prefix(&format!("listing:{}_", order.seller_pubky))
        .expect("listing aggregate ids follow the listing:{seller}_{id} format");
    format!(
        "pubky://{}/pub/pubky.app/marketplace/v1/listings/{}",
        order.seller_pubky, listing_id
    )
}

/// Fetches the stored attestation for one (order, reviewer) pair as the
/// `{ jws, claims }` result object, if one was issued.
pub async fn fetch_attestation_json(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    reviewer_pubky: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let row: Option<(String, Value)> = sqlx::query_as(
        "SELECT jws, claims FROM review_attestations WHERE order_id = $1 AND reviewer_pubky = $2",
    )
    .bind(order_id)
    .bind(reviewer_pubky)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|(jws, claims)| json!({ "jws": jws, "claims": claims })))
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
    // The attestation is unchanged by edits (it attests the purchase, not
    // the text); it is echoed back so the client can republish the revised
    // record without a second read.
    let attestation = fetch_attestation_json(tx, updated.id, actor).await?;
    let mut result = json!({
        "kind": "review",
        "order": order_json_with_reviews(&updated, &reviews),
        "review": updated_review.view(),
    });
    if let Some(attestation) = attestation {
        result["attestation"] = attestation;
    }
    // No notification: the prototype emitted none (it had no review edits).
    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result,
    }))
}
