//! `listing.sync`: service-side registration from the canonical homeserver
//! record, issuable by ANY authenticated actor.
//!
//! `listing.register` requires the seller because the client supplies the
//! registration fields. Here the service fetches the seller-signed record
//! from the seller-owned homeserver path itself, so the record's provenance
//! substitutes for the actor check: whoever asks, the data registered is
//! exactly what the seller published. This heals listings published before
//! durable-mode registration existed (no aggregate → buyers dead-end at
//! checkout, and only the seller could previously fix it).
//!
//! Sync is convergent, not optimistic: callers pass `expected_revision` 0
//! without knowing the aggregate's state, a pre-existing aggregate is never
//! a conflict, and a record whose revision does not advance past the
//! aggregate's is a successful no-op returning the current server revision.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    validate_register_listing_payload, Command, SyncListingPayload,
};
use marketplace_domain::{ids, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::handlers::fetch_listing_for_update;
use crate::handlers::register_listing::apply_registration;
use crate::homeserver::{
    registration_payload_from_record, HomeserverFetchOutcome, HomeserverListingClient,
};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SyncListingPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Fail closed: without a configured homeserver there is no canonical
    // record to derive registration from.
    let Some(homeserver) = homeserver else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Listing sync is not enabled on this deployment.",
        )));
    };

    let expected_aggregate_id =
        ids::listing_aggregate_id(&payload.seller_pubky, &payload.listing_id);
    if command.aggregate_id != expected_aggregate_id {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The listing aggregate id does not match its seller and listing.",
        )));
    }

    let record = match homeserver
        .fetch_listing(&payload.seller_pubky, &payload.listing_id)
        .await
    {
        HomeserverFetchOutcome::Found(record) => record,
        HomeserverFetchOutcome::NotFound => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::NotFound,
                "The seller's homeserver has no such listing record.",
            )));
        }
        HomeserverFetchOutcome::Unavailable => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::UpstreamUnavailable,
                "The seller's homeserver could not be reached. Try again shortly.",
            )));
        }
    };

    let Some(registration) =
        registration_payload_from_record(&payload.seller_pubky, &payload.listing_id, &record)
    else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The seller's listing record could not be interpreted for registration.",
        )));
    };
    // The derived payload must satisfy exactly the invariants
    // `listing.register` enforces; a record that fails them cannot back a
    // registered aggregate.
    let registration = match validate_register_listing_payload(registration) {
        Ok(registration) => registration,
        Err(issues) => {
            return Ok(Err(CommandFailure {
                issues: Some(issues),
                ..CommandFailure::new(
                    ErrorCode::InvalidState,
                    "The seller's listing record does not satisfy registration invariants.",
                )
            }));
        }
    };

    let current = fetch_listing_for_update(tx, &command.aggregate_id).await?;
    if let Some(current) = &current {
        // Convergent no-op: the aggregate already reflects this record
        // revision (or a newer one) — with ONE narrow healing exception.
        // When the service's own derivation evolves (shipping was added
        // after listings were first registered), the record hasn't changed
        // but what the service reads out of it has. An equal-revision sync
        // heals exactly that derived field: shipping is inventory-neutral,
        // so nothing else (quantity, price, state) is touched.
        if registration.listing_revision <= current.listing_revision {
            if registration.listing_revision == current.listing_revision
                && registration.shipping_minor != current.shipping_minor
            {
                let healed: crate::model::ListingRow = sqlx::query_as(&format!(
                    "UPDATE listings SET server_revision = server_revision + 1, \
                     shipping_minor = $2, updated_at = $3 WHERE aggregate_id = $1 \
                     RETURNING {}",
                    crate::handlers::LISTING_COLUMNS
                ))
                .bind(&command.aggregate_id)
                .bind(registration.shipping_minor)
                .bind(now)
                .fetch_one(&mut **tx)
                .await?;
                let event_id = crate::executor::insert_event(
                    tx,
                    command.command_id,
                    &command.aggregate_id,
                    healed.server_revision,
                    actor,
                    "listing.synced",
                    now,
                )
                .await?;
                return Ok(Ok(HandlerSuccess {
                    revision: healed.server_revision,
                    event_ids: vec![event_id],
                    result: json!({ "kind": "listing", "listing": healed.view() }),
                }));
            }
            return Ok(Ok(HandlerSuccess {
                revision: current.server_revision,
                event_ids: vec![],
                result: json!({ "kind": "listing", "listing": current.view() }),
            }));
        }
    }

    apply_registration(
        tx,
        actor,
        command.command_id,
        &command.aggregate_id,
        &registration,
        current.as_ref(),
        "listing.synced",
        now,
    )
    .await
}
