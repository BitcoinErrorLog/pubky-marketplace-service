//! Attestation-adjacent commands (ADR 0024): the seller's standing
//! amount-band consent (ratified D2) and the moderator-only disavowal
//! escape hatch, plus the shared annotation writer the dispute/refund
//! handlers use.
//!
//! Annotations are stored append-only keyed by the salted `order_ref`; the
//! attestor-homeserver publisher that makes them public is Phase 3 of the
//! trust & reputation plan (documented there), so today they accumulate as
//! the ground truth that publisher will read.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{DisavowAttestationPayload, SetBandConsentPayload};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::attestor::Attestor;
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_order_for_update, fetch_order_reviews, order_json_with_reviews};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// `attestation.set_band_consent`: upserts the actor's own standing
/// amount-band preference. The aggregate is the seller's settings row;
/// `expected_revision` is 0 for the first write and the stored revision
/// afterwards, so concurrent toggles surface as ordinary revision conflicts.
pub async fn set_band_consent(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SetBandConsentPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if command.aggregate_id != ids::seller_settings_aggregate_id(actor) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Band consent may only be set on the actor's own settings aggregate.",
        )));
    }
    let current: Option<(i64,)> = sqlx::query_as(
        "SELECT revision FROM attestation_band_consents WHERE seller_pubky = $1 FOR UPDATE",
    )
    .bind(actor)
    .fetch_optional(&mut **tx)
    .await?;
    let current_revision = current.map(|(revision,)| revision).unwrap_or(0);
    if command.expected_revision != current_revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The band consent revision is stale.",
            current_revision,
        )));
    }

    let new_revision = current_revision + 1;
    sqlx::query(
        "INSERT INTO attestation_band_consents (seller_pubky, allows_amount_band, revision, updated_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (seller_pubky) DO UPDATE \
         SET allows_amount_band = EXCLUDED.allows_amount_band, \
             revision = EXCLUDED.revision, updated_at = EXCLUDED.updated_at",
    )
    .bind(actor)
    .bind(payload.allows_amount_band)
    .bind(new_revision)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        new_revision,
        actor,
        "attestation.band_consent_set",
        now,
    )
    .await?;
    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "band_consent",
            "band_consent": {
                "seller_pubky": actor,
                "allows_amount_band": payload.allows_amount_band,
                "revision": new_revision,
                "updated_at": format_timestamp(now),
            },
        }),
    }))
}

/// `attestation.disavow`: the moderator-only fraud/collusion escape hatch
/// (design §5.6). Records an `attestation_disavowed` annotation keyed by
/// the order's salted `order_ref`; the reason stays internal. Refused when
/// the deployment carries no attestor (no salt means no `order_ref`).
pub async fn disavow(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &DisavowAttestationPayload,
    attestor: Option<&Attestor>,
    moderator_pubkys: &[String],
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(attestor) = attestor else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Attestation support is not configured on this deployment.",
        )));
    };
    if !moderator_pubkys.iter().any(|entry| entry == actor) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only a configured moderator may disavow attestations.",
        )));
    }
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if command.aggregate_id != ids::order_aggregate_id(order.id)
        || command.expected_revision != order.revision
    {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The order revision is stale.",
            order.revision,
        )));
    }

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    insert_annotation(
        tx,
        attestor,
        order.id,
        "attestation_disavowed",
        Some(&payload.reason),
        now,
    )
    .await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &ids::order_aggregate_id(updated.id),
        updated.revision,
        actor,
        "attestation.disavowed",
        now,
    )
    .await?;
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

/// Appends one attestor outcome annotation for an order. The Phase 3
/// publisher maps outcomes to the reviewer-relative record vocabulary per
/// review role.
pub async fn insert_annotation(
    tx: &mut Transaction<'_, Postgres>,
    attestor: &Attestor,
    order_id: Uuid,
    outcome: &str,
    reason: Option<&str>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO attestation_annotations (id, order_ref, outcome, reason, annotated_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(attestor.order_ref(order_id))
    .bind(outcome)
    .bind(reason)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
