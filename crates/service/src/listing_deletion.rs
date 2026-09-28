//! Listing tombstones: the service follows a seller's homeserver delete.
//!
//! A listing is deleted when BOTH hold on the configured homeserver:
//!
//! 1. the record fetch at `/pub/pubky.app/marketplace/v1/listings/{id}`
//!    answers 404, and
//! 2. the seller's event stream (`/events-stream?user={seller}`) names a
//!    `DEL` as the latest event for that exact record path.
//!
//! The homeserver answers the record fetch with 404 for users it does not
//! host, so (1) alone would let any caller tombstone a listing whose seller
//! lives elsewhere. The event stream answers 404 for an unhosted user and
//! only ever records a `DEL` the seller wrote, so (2) is the proof.
//!
//! Two paths reach the tombstone: `listing.sync` (any actor, including
//! `POST /v1/listings/sync-many`) when its fetch finds the record gone, and
//! the [`follow_homeserver_deletions`] worker pass, which reads each seller's
//! listing events the way Nexus follows the homeserver. Both apply
//! [`tombstone`]: the row is kept (past orders, digital delivery evidence
//! and disputes resolve against it and the quantity ledger keeps
//! balancing), while public reads, seller lists, and every new commitment
//! skip it.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::handlers::LISTING_COLUMNS;
use crate::homeserver::{
    HomeserverEventKind, HomeserverEventsOutcome, HomeserverFetchOutcome, HomeserverListingClient,
    LISTINGS_PATH,
};
use crate::model::ListingRow;

pub const LISTING_DELETED_EVENT: &str = "listing.deleted";

/// Reverse-order entries read to find the latest event for one record. The
/// path filter is a prefix match, so sibling ids sharing the prefix can
/// occupy some of them; a record not found within the window is not
/// confirmed deleted.
const CONFIRM_WINDOW: u16 = 20;
/// Forward entries read per seller per worker pass.
const FOLLOW_PAGE: u16 = 100;
const FOLLOW_SELLERS_PER_PASS: i64 = 10;
const FOLLOW_POLL_SECONDS: i64 = 60;
/// Wall-clock budget for one follower pass. Worker tasks run in sequence,
/// so a slow homeserver must not delay payment windows or auction closes
/// past the lease.
const FOLLOW_PASS_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);

pub fn listing_record_path(listing_id: &str) -> String {
    format!("{LISTINGS_PATH}{listing_id}")
}

pub fn listing_record_uri(seller_pubky: &str, listing_id: &str) -> String {
    format!("pubky://{seller_pubky}{}", listing_record_path(listing_id))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeletionCheck {
    /// The latest event for the record is this `DEL`.
    Deleted { cursor: String },
    /// The record was re-created, never existed on this homeserver, or the
    /// seller is not hosted there.
    NotDeleted,
    /// The event stream could not be read; retry later.
    Unavailable,
}

/// Step (2) of the deletion proof. Callers have already seen step (1).
pub async fn latest_event_is_delete(
    homeserver: &dyn HomeserverListingClient,
    seller_pubky: &str,
    listing_id: &str,
) -> DeletionCheck {
    let path = listing_record_path(listing_id);
    let uri = listing_record_uri(seller_pubky, listing_id);
    match homeserver
        .listing_events(seller_pubky, &path, None, true, CONFIRM_WINDOW)
        .await
    {
        HomeserverEventsOutcome::Events(events) => {
            match events.into_iter().find(|event| event.uri == uri) {
                Some(event) if event.kind == HomeserverEventKind::Del => DeletionCheck::Deleted {
                    cursor: event.cursor,
                },
                _ => DeletionCheck::NotDeleted,
            }
        }
        HomeserverEventsOutcome::UnknownUser => DeletionCheck::NotDeleted,
        HomeserverEventsOutcome::Unavailable => DeletionCheck::Unavailable,
    }
}

/// Both steps of the deletion proof, for callers that have not fetched the
/// record.
pub async fn confirm_deleted(
    homeserver: &dyn HomeserverListingClient,
    seller_pubky: &str,
    listing_id: &str,
) -> DeletionCheck {
    match homeserver.fetch_listing(seller_pubky, listing_id).await {
        HomeserverFetchOutcome::NotFound => {
            latest_event_is_delete(homeserver, seller_pubky, listing_id).await
        }
        HomeserverFetchOutcome::Found(_) => DeletionCheck::NotDeleted,
        HomeserverFetchOutcome::Unavailable => DeletionCheck::Unavailable,
    }
}

/// Tombstones a live listing inside the caller's transaction and records
/// `listing.deleted` at the bumped revision. Stock columns are left as they
/// are: holds and paid orders still settle against them. Returns `None`
/// when the listing is missing or already tombstoned.
pub async fn tombstone(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    event_cursor: &str,
    actor: &str,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Option<(ListingRow, Uuid)>, sqlx::Error> {
    let deleted: Option<ListingRow> = sqlx::query_as(&format!(
        "UPDATE listings SET deleted_at = $2, deleted_event_cursor = $3, \
         server_revision = server_revision + 1, updated_at = $2 \
         WHERE aggregate_id = $1 AND deleted_at IS NULL \
         RETURNING {LISTING_COLUMNS}"
    ))
    .bind(aggregate_id)
    .bind(now)
    .bind(event_cursor)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(deleted) = deleted else {
        return Ok(None);
    };
    let event_id = insert_event(
        tx,
        command_id,
        aggregate_id,
        deleted.server_revision,
        actor,
        LISTING_DELETED_EVENT,
        now,
    )
    .await?;
    tracing::info!(
        seller_pubky_prefix = %deleted.seller_pubky.get(..8).unwrap_or_default(),
        "listing tombstoned after its homeserver record was deleted"
    );
    Ok(Some((deleted, event_id)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SettleOutcome {
    Tombstoned,
    NotDeleted,
    Unavailable,
}

async fn settle_deletion(
    pool: &PgPool,
    homeserver: &dyn HomeserverListingClient,
    seller_pubky: &str,
    listing_id: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<SettleOutcome> {
    let aggregate_id = marketplace_domain::ids::listing_aggregate_id(seller_pubky, listing_id);
    let live: Option<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM listings WHERE aggregate_id = $1 AND deleted_at IS NULL",
    )
    .bind(&aggregate_id)
    .fetch_optional(pool)
    .await?;
    if live.is_none() {
        return Ok(SettleOutcome::NotDeleted);
    }
    let cursor = match confirm_deleted(homeserver, seller_pubky, listing_id).await {
        DeletionCheck::Deleted { cursor } => cursor,
        DeletionCheck::NotDeleted => return Ok(SettleOutcome::NotDeleted),
        DeletionCheck::Unavailable => return Ok(SettleOutcome::Unavailable),
    };
    let mut tx = pool.begin().await?;
    let tombstoned = tombstone(
        &mut tx,
        &aggregate_id,
        &cursor,
        crate::workers::SYSTEM_ACTOR,
        Uuid::new_v4(),
        now,
    )
    .await?;
    tx.commit().await?;
    Ok(if tombstoned.is_some() {
        SettleOutcome::Tombstoned
    } else {
        SettleOutcome::NotDeleted
    })
}

/// The listing id named by an event URI for this seller's listings, if any.
fn event_listing_id<'a>(seller_pubky: &str, uri: &'a str) -> Option<&'a str> {
    let id = uri
        .strip_prefix("pubky://")?
        .strip_prefix(seller_pubky)?
        .strip_prefix(LISTINGS_PATH)?;
    (!id.is_empty() && !id.contains('/')).then_some(id)
}

async fn record_poll(
    pool: &PgPool,
    seller_pubky: &str,
    cursor: Option<&str>,
    polled_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
         VALUES ($1, $2, $3) ON CONFLICT (seller_pubky) DO UPDATE \
         SET event_cursor = COALESCE(EXCLUDED.event_cursor, listing_deletion_cursors.event_cursor), \
             polled_at = EXCLUDED.polled_at",
    )
    .bind(seller_pubky)
    .bind(cursor)
    .bind(polled_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// One worker pass of the deletion follower. For up to
/// [`FOLLOW_SELLERS_PER_PASS`] sellers with live listings whose last poll is
/// older than [`FOLLOW_POLL_SECONDS`], reads the next batch of their listing
/// events and tombstones every live listing whose latest event in the batch
/// is a `DEL` that [`confirm_deleted`] still confirms. The seller's cursor
/// advances only when every deletion in the batch settled; a transient
/// homeserver failure leaves it for the next poll. Returns the number of
/// listings tombstoned.
pub async fn follow_homeserver_deletions(
    pool: &PgPool,
    homeserver: &dyn HomeserverListingClient,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let started = std::time::Instant::now();
    let due: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT s.seller_pubky, c.event_cursor \
         FROM (SELECT DISTINCT seller_pubky FROM listings WHERE deleted_at IS NULL) s \
         LEFT JOIN listing_deletion_cursors c ON c.seller_pubky = s.seller_pubky \
         WHERE c.polled_at IS NULL OR c.polled_at <= $1 \
         ORDER BY c.polled_at NULLS FIRST, s.seller_pubky \
         LIMIT $2",
    )
    .bind(now - chrono::Duration::seconds(FOLLOW_POLL_SECONDS))
    .bind(FOLLOW_SELLERS_PER_PASS)
    .fetch_all(pool)
    .await?;

    let mut tombstoned = 0u64;
    for (seller_pubky, cursor) in due {
        if started.elapsed() >= FOLLOW_PASS_BUDGET {
            break;
        }
        let events = match homeserver
            .listing_events(
                &seller_pubky,
                LISTINGS_PATH,
                cursor.as_deref(),
                false,
                FOLLOW_PAGE,
            )
            .await
        {
            HomeserverEventsOutcome::Events(events) => events,
            HomeserverEventsOutcome::UnknownUser | HomeserverEventsOutcome::Unavailable => {
                record_poll(pool, &seller_pubky, None, now).await?;
                continue;
            }
        };

        let mut latest: Vec<(&str, HomeserverEventKind)> = Vec::new();
        for event in &events {
            let Some(listing_id) = event_listing_id(&seller_pubky, &event.uri) else {
                continue;
            };
            match latest.iter_mut().find(|(id, _)| *id == listing_id) {
                Some(entry) => entry.1 = event.kind,
                None => latest.push((listing_id, event.kind)),
            }
        }

        let mut settled = true;
        for (listing_id, kind) in latest {
            if kind != HomeserverEventKind::Del {
                continue;
            }
            match settle_deletion(pool, homeserver, &seller_pubky, listing_id, now).await? {
                SettleOutcome::Tombstoned => tombstoned += 1,
                SettleOutcome::NotDeleted => {}
                SettleOutcome::Unavailable => settled = false,
            }
        }

        let next_cursor = events
            .last()
            .filter(|_| settled)
            .map(|event| event.cursor.as_str());
        // A full page means more history is waiting: leave the seller due
        // for the next pass instead of the next poll interval.
        let polled_at = if settled && events.len() == usize::from(FOLLOW_PAGE) {
            now - chrono::Duration::seconds(FOLLOW_POLL_SECONDS)
        } else {
            now
        };
        record_poll(pool, &seller_pubky, next_cursor, polled_at).await?;
    }
    Ok(tombstoned)
}

#[cfg(test)]
mod tests {
    use super::event_listing_id;

    #[test]
    fn event_listing_ids_are_exact_seller_listing_records() {
        let seller = "7oboeqnfgtf5d6gohz1wao7rboe1q3ynexkbm5tmq4u49kxzej9y";
        let uri = |path: &str| format!("pubky://{seller}{path}");
        assert_eq!(
            event_listing_id(
                seller,
                &uri("/pub/pubky.app/marketplace/v1/listings/w4i_mujp0ovw_a")
            ),
            Some("w4i_mujp0ovw_a")
        );
        for path in [
            "/pub/pubky.app/marketplace/v1/listings/",
            "/pub/pubky.app/marketplace/v1/listings/a/b",
            "/pub/pubky.app/marketplace/v1/drops/a",
        ] {
            assert_eq!(event_listing_id(seller, &uri(path)), None, "{path}");
        }
        assert_eq!(
            event_listing_id(
                "other",
                &uri("/pub/pubky.app/marketplace/v1/listings/w4i_mujp0ovw_a")
            ),
            None
        );
    }
}
