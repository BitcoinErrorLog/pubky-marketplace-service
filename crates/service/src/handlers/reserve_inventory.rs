use chrono::{DateTime, Utc};
use marketplace_domain::commands::{Command, ReserveInventoryPayload};
use marketplace_domain::state_machines::{can_transition, listing_machine};
use marketplace_domain::ErrorCode;
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::executor::insert_event;
use crate::handlers::{current_listing_revision, fetch_listing, LISTING_COLUMNS};
use crate::model::{ListingRow, ReservationRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ReserveInventoryPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(listing) = fetch_listing(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The listing is not registered.",
        )));
    };
    if listing.seller_pubky == actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "A seller cannot reserve their own listing.",
        )));
    }
    if command.expected_revision != listing.server_revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            listing.server_revision,
        )));
    }
    if listing.available_quantity < payload.quantity {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InsufficientInventory,
            "The requested quantity is unavailable.",
            listing.server_revision,
        )));
    }

    let new_state = if listing.available_quantity == payload.quantity {
        "reserved"
    } else {
        "available"
    };
    if !can_transition(&listing_machine(), &listing.state, new_state) {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InvariantViolation,
            "The listing cannot enter the reserved state.",
            listing.server_revision,
        )));
    }

    // Compare-and-swap on the expected revision; the CHECK constraints keep
    // inventory non-negative even if this guard were bypassed.
    let updated: Option<ListingRow> = sqlx::query_as(&format!(
        "UPDATE listings SET server_revision = server_revision + 1, state = $3, \
         available_quantity = available_quantity - $4, \
         reserved_quantity = reserved_quantity + $4, updated_at = $5 \
         WHERE aggregate_id = $1 AND server_revision = $2 AND available_quantity >= $4 \
         RETURNING {LISTING_COLUMNS}"
    ))
    .bind(&command.aggregate_id)
    .bind(command.expected_revision)
    .bind(new_state)
    .bind(payload.quantity)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(listing) = updated else {
        let latest = current_listing_revision(tx, &command.aggregate_id).await?;
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            latest,
        )));
    };

    let expires_at = now + chrono::Duration::seconds(payload.reservation_ttl_seconds);
    sqlx::query(
        "INSERT INTO reservations (id, listing_aggregate_id, buyer_pubky, quantity, status, \
         expires_at, created_at, updated_at) VALUES ($1, $2, $3, $4, 'active', $5, $6, $6)",
    )
    .bind(command.command_id)
    .bind(&command.aggregate_id)
    .bind(actor)
    .bind(payload.quantity)
    .bind(expires_at)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let reservation = ReservationRow {
        id: command.command_id,
        listing_aggregate_id: command.aggregate_id.clone(),
        buyer_pubky: actor.to_string(),
        quantity: payload.quantity,
        status: "active".to_string(),
        expires_at,
        created_at: now,
    };

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        listing.server_revision,
        actor,
        "inventory.reserved",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: listing.server_revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "reservation",
            "listing": listing.view(),
            "reservation": reservation.view(),
        }),
    }))
}
