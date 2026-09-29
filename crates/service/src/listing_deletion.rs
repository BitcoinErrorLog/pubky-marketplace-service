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
//!
//! The follower runs under a fenced lease (`worker_leases.fence`, 0050).
//! Each of its database units is one transaction whose statements the
//! server cancels at the lease deadline, and each write first share-locks
//! the lease row and proceeds only while it still names this pass. The
//! cursor it writes never moves backwards.

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
/// Deletions confirmed per pass; each costs two homeserver requests.
const FOLLOW_SETTLES_PER_PASS: usize = 20;
const FOLLOW_POLL_SECONDS: i64 = 60;

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
    let live: Option<(String, String)> = sqlx::query_as(
        "SELECT seller_pubky, listing_id FROM listings \
         WHERE aggregate_id = $1 AND deleted_at IS NULL",
    )
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((seller_pubky, listing_id)) = live else {
        return Ok(None);
    };
    // A drop bound to the deleted listing must not gate, or draw on, a
    // record later re-created at the same id. Releasing the binding takes it
    // out of gating the way `drop.release_listings` does; only an advanced
    // record of a still-announced drop can bind the re-created listing.
    // The bindings are locked before the listing row: a drop sell-out
    // payment confirmation takes them in that order too.
    sqlx::query(
        "UPDATE drop_listings SET active = FALSE, released = TRUE \
         WHERE seller_pubky = $1 AND listing_id = $2 AND NOT released",
    )
    .bind(&seller_pubky)
    .bind(&listing_id)
    .execute(&mut **tx)
    .await?;
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
    /// The homeserver could not answer, or the pass ran out of time.
    Unsettled,
}

/// Why a pass stopped before its due sellers ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Halt {
    /// The lease deadline passed before the database work finished.
    OutOfTime,
    /// The lease row no longer names this pass: it expired, another holder
    /// took it, or a later acquisition renewed it.
    LeaseLost,
}

/// Opens a transaction for work that must end by `deadline`. The server
/// cancels any of its statements still running at the deadline, one
/// waiting on a row lock included, and ends the session if it sits idle
/// inside the transaction that long, so no lock the transaction takes
/// outlives the deadline. `None` when the deadline has already passed.
async fn begin_by(
    pool: &PgPool,
    deadline: tokio::time::Instant,
) -> Result<Option<Transaction<'static, Postgres>>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Ok(None);
    }
    let millis = remaining.as_millis().max(1).to_string();
    sqlx::query(
        "SELECT set_config('statement_timeout', $1, true), \
         set_config('idle_in_transaction_session_timeout', $1, true)",
    )
    .bind(&millis)
    .execute(&mut *tx)
    .await?;
    Ok(Some(tx))
}

/// Waits for one unit of database work no longer than `deadline`, even
/// when no pool connection is free. A statement the server cancelled at
/// the deadline, or a unit still running when it passes, is `None`; that
/// unit committed nothing it had not already committed.
async fn by_deadline<T>(
    deadline: tokio::time::Instant,
    unit: impl std::future::Future<Output = Result<Option<T>, sqlx::Error>>,
) -> Result<Option<T>, sqlx::Error> {
    match tokio::time::timeout_at(deadline, unit).await {
        Ok(Err(error)) if is_statement_timeout(&error) => Ok(None),
        Ok(result) => result,
        Err(_) => Ok(None),
    }
}

fn is_statement_timeout(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("57014"))
}

/// Takes the follower's lease and returns its fence, waiting on the lease
/// row no longer than `deadline`. `None` when another live holder owns the
/// task, or when the row stayed locked by another pass's in-flight fenced
/// write until the deadline.
pub async fn acquire_follower_lease(
    pool: &PgPool,
    holder: Uuid,
    now: DateTime<Utc>,
    lease_seconds: i64,
    deadline: tokio::time::Instant,
) -> Result<Option<i64>, sqlx::Error> {
    by_deadline(deadline, async {
        let Some(mut tx) = begin_by(pool, deadline).await? else {
            return Ok(None);
        };
        let fence = crate::workers::try_acquire_fenced_lease(
            &mut *tx,
            crate::workers::TASK_LISTING_DELETIONS,
            holder,
            now,
            lease_seconds,
        )
        .await?;
        tx.commit().await?;
        Ok(fence)
    })
    .await
}

/// One follower pass: its fenced lease and its two deadlines.
pub struct FollowerPass<'a> {
    pub pool: &'a PgPool,
    pub homeserver: &'a dyn HomeserverListingClient,
    pub clock: &'a dyn crate::clock::Clock,
    pub holder: Uuid,
    /// The fence [`acquire_follower_lease`] returned for this pass.
    pub fence: i64,
    /// Homeserver work stops here.
    pub deadline: tokio::time::Instant,
    /// The lease ends here; no database work of the pass outlives it.
    pub lease_deadline: tokio::time::Instant,
}

impl FollowerPass<'_> {
    /// Runs one homeserver call within what is left of the pass. `None`
    /// means the deadline passed first.
    async fn bounded<T>(&self, call: impl std::future::Future<Output = T>) -> Option<T> {
        tokio::time::timeout_at(self.deadline, call).await.ok()
    }

    fn expired(&self) -> bool {
        tokio::time::Instant::now() >= self.deadline
    }

    async fn begin(&self) -> Result<Option<Transaction<'static, Postgres>>, sqlx::Error> {
        begin_by(self.pool, self.lease_deadline).await
    }

    async fn within_lease<T>(
        &self,
        unit: impl std::future::Future<Output = Result<Option<T>, sqlx::Error>>,
    ) -> Result<Option<T>, sqlx::Error> {
        by_deadline(self.lease_deadline, unit).await
    }

    /// Share-locks this pass's lease row and reports whether it still is
    /// the lease the pass took, unexpired. Every write of the pass checks
    /// this first in its own transaction: a takeover or renewal updates
    /// the row, so it waits until that transaction ends, and a row that
    /// names another holder or fence, or has expired, refuses the write.
    async fn holds_lease(
        &self,
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<bool, sqlx::Error> {
        let held: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM worker_leases \
             WHERE task = $1 AND holder = $2 AND fence = $3 AND lease_until > $4 \
             FOR SHARE",
        )
        .bind(crate::workers::TASK_LISTING_DELETIONS)
        .bind(self.holder)
        .bind(self.fence)
        .bind(self.clock.now())
        .fetch_optional(&mut **tx)
        .await?;
        Ok(held.is_some())
    }

    async fn settle_deletion(
        &self,
        seller_pubky: &str,
        listing_id: &str,
    ) -> anyhow::Result<Result<SettleOutcome, Halt>> {
        let aggregate_id = marketplace_domain::ids::listing_aggregate_id(seller_pubky, listing_id);
        let live = self
            .within_lease(async {
                let Some(mut tx) = self.begin().await? else {
                    return Ok(None);
                };
                let live: Option<String> = sqlx::query_scalar(
                    "SELECT aggregate_id FROM listings \
                     WHERE aggregate_id = $1 AND deleted_at IS NULL",
                )
                .bind(&aggregate_id)
                .fetch_optional(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(Some(live.is_some()))
            })
            .await?;
        match live {
            None => return Ok(Err(Halt::OutOfTime)),
            Some(false) => return Ok(Ok(SettleOutcome::NotDeleted)),
            Some(true) => {}
        }
        let cursor = match self
            .bounded(confirm_deleted(self.homeserver, seller_pubky, listing_id))
            .await
        {
            Some(DeletionCheck::Deleted { cursor }) => cursor,
            Some(DeletionCheck::NotDeleted) => return Ok(Ok(SettleOutcome::NotDeleted)),
            Some(DeletionCheck::Unavailable) | None => return Ok(Ok(SettleOutcome::Unsettled)),
        };
        let written = self
            .within_lease(async {
                let Some(mut tx) = self.begin().await? else {
                    return Ok(None);
                };
                if !self.holds_lease(&mut tx).await? {
                    return Ok(Some(Err(Halt::LeaseLost)));
                }
                let deleted = tombstone(
                    &mut tx,
                    &aggregate_id,
                    &cursor,
                    crate::workers::SYSTEM_ACTOR,
                    Uuid::new_v4(),
                    self.clock.now(),
                )
                .await?;
                tx.commit().await?;
                Ok(Some(Ok(if deleted.is_some() {
                    SettleOutcome::Tombstoned
                } else {
                    SettleOutcome::NotDeleted
                })))
            })
            .await?;
        Ok(written.unwrap_or(Err(Halt::OutOfTime)))
    }

    /// Records a poll while the pass holds its lease. The cursor only
    /// moves forward: one older than the stored cursor keeps the stored one.
    async fn record_poll(
        &self,
        seller_pubky: &str,
        cursor: Option<&str>,
        polled_at: DateTime<Utc>,
    ) -> Result<Result<(), Halt>, sqlx::Error> {
        let written = self
            .within_lease(async {
                let Some(mut tx) = self.begin().await? else {
                    return Ok(None);
                };
                if !self.holds_lease(&mut tx).await? {
                    return Ok(Some(Err(Halt::LeaseLost)));
                }
                sqlx::query(
                    "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (seller_pubky) DO UPDATE SET \
                         event_cursor = CASE \
                             WHEN listing_deletion_cursors.event_cursor IS NULL \
                                 OR EXCLUDED.event_cursor::numeric \
                                     > listing_deletion_cursors.event_cursor::numeric \
                             THEN EXCLUDED.event_cursor \
                             ELSE listing_deletion_cursors.event_cursor END, \
                         polled_at = EXCLUDED.polled_at",
                )
                .bind(seller_pubky)
                .bind(cursor)
                .bind(polled_at)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok(Some(Ok(())))
            })
            .await?;
        Ok(written.unwrap_or(Err(Halt::OutOfTime)))
    }

    /// Ends this pass's lease, unless a later acquisition already owns it.
    pub async fn release_lease(&self) -> Result<(), sqlx::Error> {
        self.within_lease(async {
            let Some(mut tx) = self.begin().await? else {
                return Ok(None);
            };
            crate::workers::release_fenced_lease(
                &mut *tx,
                crate::workers::TASK_LISTING_DELETIONS,
                self.holder,
                self.fence,
                self.clock.now(),
            )
            .await?;
            tx.commit().await?;
            Ok(Some(()))
        })
        .await?;
        Ok(())
    }
}

/// The listing id named by an event URI for this seller's listings, if any.
fn event_listing_id<'a>(seller_pubky: &str, uri: &'a str) -> Option<&'a str> {
    let id = uri
        .strip_prefix("pubky://")?
        .strip_prefix(seller_pubky)?
        .strip_prefix(LISTINGS_PATH)?;
    (!id.is_empty() && !id.contains('/')).then_some(id)
}

/// One worker pass of the deletion follower, bounded by `pass.deadline`.
/// For up to [`FOLLOW_SELLERS_PER_PASS`] sellers with live listings whose
/// last poll is older than [`FOLLOW_POLL_SECONDS`], reads the next batch of
/// their listing events in order. A `DEL` that is the batch's latest event
/// for its listing is settled through [`confirm_deleted`], at most
/// [`FOLLOW_SETTLES_PER_PASS`] per pass. The seller's cursor advances to the
/// last event before the first one that could not be settled (homeserver
/// unavailable, deadline, or settle cap), so nothing is skipped. The pass
/// stops at the deadline, when its database work reaches the lease
/// deadline, or when its lease is gone. Returns the number of listings
/// tombstoned.
pub async fn follow_homeserver_deletions(pass: &FollowerPass<'_>) -> anyhow::Result<u64> {
    let now = pass.clock.now();
    let due = pass
        .within_lease(async {
            let Some(mut tx) = pass.begin().await? else {
                return Ok(None);
            };
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
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(Some(due))
        })
        .await?;
    let Some(due) = due else {
        return Ok(halted(0, Halt::OutOfTime));
    };

    let mut tombstoned = 0u64;
    let mut settles = 0usize;
    let mut unhosted = 0usize;
    for (seller_pubky, cursor) in due {
        if pass.expired() || settles >= FOLLOW_SETTLES_PER_PASS {
            break;
        }
        let events = match pass
            .bounded(pass.homeserver.listing_events(
                &seller_pubky,
                LISTINGS_PATH,
                cursor.as_deref(),
                false,
                FOLLOW_PAGE,
            ))
            .await
        {
            Some(HomeserverEventsOutcome::Events(events)) => events,
            Some(outcome) => {
                if outcome == HomeserverEventsOutcome::UnknownUser {
                    unhosted += 1;
                }
                if let Err(halt) = pass
                    .record_poll(&seller_pubky, None, pass.clock.now())
                    .await?
                {
                    return Ok(halted(tombstoned, halt));
                }
                continue;
            }
            None => break,
        };

        let mut settled_through: Option<&str> = None;
        let mut complete = true;
        for (index, event) in events.iter().enumerate() {
            let listing_id = event_listing_id(&seller_pubky, &event.uri);
            let superseded = listing_id.is_some_and(|id| {
                events[index + 1..]
                    .iter()
                    .any(|later| event_listing_id(&seller_pubky, &later.uri) == Some(id))
            });
            if let (Some(listing_id), HomeserverEventKind::Del, false) =
                (listing_id, event.kind, superseded)
            {
                if settles >= FOLLOW_SETTLES_PER_PASS {
                    complete = false;
                    break;
                }
                settles += 1;
                match pass.settle_deletion(&seller_pubky, listing_id).await? {
                    Ok(SettleOutcome::Tombstoned) => tombstoned += 1,
                    Ok(SettleOutcome::NotDeleted) => {}
                    Ok(SettleOutcome::Unsettled) => {
                        complete = false;
                        break;
                    }
                    Err(halt) => return Ok(halted(tombstoned, halt)),
                }
            }
            settled_through = Some(event.cursor.as_str());
        }

        // A full page read to its end means more history is waiting: leave
        // the seller due for the next pass instead of the next interval.
        let polled_at = if complete && events.len() == usize::from(FOLLOW_PAGE) {
            now - chrono::Duration::seconds(FOLLOW_POLL_SECONDS)
        } else {
            pass.clock.now()
        };
        if let Err(halt) = pass
            .record_poll(&seller_pubky, settled_through, polled_at)
            .await?
        {
            return Ok(halted(tombstoned, halt));
        }
    }
    if unhosted > 0 {
        tracing::info!(
            unhosted,
            "listing deletion follower: sellers unknown to the homeserver event stream"
        );
    }
    Ok(tombstoned)
}

fn halted(tombstoned: u64, halt: Halt) -> u64 {
    match halt {
        Halt::OutOfTime => {
            tracing::warn!(
                "listing deletion follower reached its lease deadline; stopping the pass"
            )
        }
        Halt::LeaseLost => {
            tracing::warn!("listing deletion follower lost its lease; stopping the pass")
        }
    }
    tombstoned
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
