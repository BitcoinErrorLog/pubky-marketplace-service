//! Background worker runtime (plan task 3.4).
//!
//! One runtime drains four server-time tasks: (a) reservation expiry,
//! (b) offer expiry, (c) auction close, and (d) the outbox. Each task is
//! guarded by a lease row in `worker_leases`, so two service instances never
//! drain the same task concurrently; a holder that dies mid-lease is
//! recovered by any instance once the lease lapses. Within a task, due rows
//! are additionally locked with `FOR UPDATE SKIP LOCKED`, so even a lease
//! violation cannot double-process a row.
//!
//! Outbox semantics: intents are written in the command transaction
//! (ADR-0019 §4) and delivered at least once. A claim stamps `lease_until`
//! on the row; delivery inserts the notification and marks `delivered_at` in
//! one transaction. A crash between claim and delivery leaves the row leased
//! but undelivered — it is redelivered after the lease lapses. The consumer
//! side dedups by (event id, recipient), so redelivery never duplicates the
//! effect.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::handlers::auction::{close_locked_auction, parse_auction};
use crate::handlers::LISTING_COLUMNS;
use crate::model::ListingRow;
use crate::{expiry, AppState};

pub const TASK_RESERVATION_EXPIRY: &str = "reservation_expiry";
pub const TASK_OFFER_EXPIRY: &str = "offer_expiry";
pub const TASK_AUCTION_CLOSE: &str = "auction_close";
pub const TASK_OUTBOX: &str = "outbox";

const OUTBOX_BATCH_SIZE: i64 = 100;

/// Takes (or renews) the lease for one task. Returns false when another
/// live holder owns it.
pub async fn try_acquire_lease(
    pool: &PgPool,
    task: &str,
    holder: Uuid,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<bool, sqlx::Error> {
    let lease_until = now + chrono::Duration::seconds(lease_seconds);
    let result = sqlx::query(
        "INSERT INTO worker_leases (task, holder, lease_until) VALUES ($1, $2, $3) \
         ON CONFLICT (task) DO UPDATE SET holder = EXCLUDED.holder, \
         lease_until = EXCLUDED.lease_until \
         WHERE worker_leases.lease_until <= $4 OR worker_leases.holder = EXCLUDED.holder",
    )
    .bind(task)
    .bind(holder)
    .bind(lease_until)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Ends the holder's lease so any instance can take the task immediately.
pub async fn release_lease(
    pool: &PgPool,
    task: &str,
    holder: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE worker_leases SET lease_until = $3 WHERE task = $1 AND holder = $2")
        .bind(task)
        .bind(holder)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

/// Expires pending/countered offers whose server-time deadline has passed.
/// The offer moves to the terminal `expired` state with a revision bump and
/// an `offer.expired` event traceable through the offer id.
pub async fn expire_due_offers(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<(Uuid, String, i64, String)> = sqlx::query_as(
        "SELECT id, aggregate_id, revision, offered_by FROM offers \
         WHERE state IN ('pending', 'countered') AND expires_at <= $1 \
         ORDER BY expires_at FOR UPDATE SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for (offer_id, aggregate_id, revision, offered_by) in due {
        let flipped = sqlx::query(
            "UPDATE offers SET state = 'expired', revision = revision + 1, updated_at = $2 \
             WHERE id = $1 AND state IN ('pending', 'countered')",
        )
        .bind(offer_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if flipped.rows_affected() != 1 {
            continue;
        }
        sqlx::query(
            "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
             occurred_at) VALUES ($1, $2, $3, $4, $5, 'offer.expired', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(offer_id)
        .bind(&aggregate_id)
        .bind(revision + 1)
        .bind(&offered_by)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tracing::info!(offer_id = %offer_id, aggregate_id = %aggregate_id, "expired offer");
        expired += 1;
    }
    tx.commit().await?;
    Ok(expired)
}

/// Authoritatively closes active auctions whose end time has passed on
/// server time, using the same close path as the seller command. Exactly one
/// close result per auction: the status guard flips `active` exactly once
/// and `orders_one_winner_per_auction` blocks a second winning order.
pub async fn close_due_auctions(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<ListingRow> = sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings \
         WHERE sale_format = 'auction' AND auction->>'status' = 'active' \
         AND (auction->>'ends_at')::timestamptz <= $1 \
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut closed = 0u64;
    for listing in due {
        let Some(auction) = parse_auction(&listing) else {
            continue;
        };
        let seller = listing.seller_pubky.clone();
        let outcome =
            close_locked_auction(&mut tx, &listing, auction, &seller, Uuid::new_v4(), now).await?;
        tracing::info!(
            aggregate_id = %listing.aggregate_id,
            outcome = if outcome.sold { "sold" } else { "unsold" },
            "closed auction on server time"
        );
        closed += 1;
    }
    tx.commit().await?;
    Ok(closed)
}

#[derive(Debug, sqlx::FromRow)]
pub struct ClaimedOutboxRow {
    pub id: i64,
    pub event_id: Uuid,
    pub kind: String,
    pub payload: Value,
}

/// Claims a batch of undelivered outbox rows by stamping `lease_until`.
/// Rows whose previous claim lapsed (crashed deliverer) are reclaimed.
pub async fn claim_outbox_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<Vec<ClaimedOutboxRow>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE outbox SET lease_until = $2 WHERE id IN (\
             SELECT id FROM outbox \
             WHERE delivered_at IS NULL AND (lease_until IS NULL OR lease_until <= $1) \
             ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED\
         ) RETURNING id, event_id, kind, payload",
    )
    .bind(now)
    .bind(now + chrono::Duration::seconds(lease_seconds))
    .bind(OUTBOX_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Delivers claimed intents. Each row is consumed in one transaction: the
/// notification insert (deduplicated by event id + recipient) and the
/// `delivered_at` mark commit together, so a redelivered intent can never
/// apply its effect twice.
pub async fn deliver_claimed(
    pool: &PgPool,
    rows: &[ClaimedOutboxRow],
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let mut delivered = 0u64;
    for row in rows {
        let Some(notification_type) = row.kind.strip_prefix("notification.") else {
            anyhow::bail!("outbox row {} has unroutable kind {}", row.id, row.kind);
        };
        let recipient = payload_str(&row.payload, "recipient_pubky", row.id)?;
        let actor = payload_str(&row.payload, "actor_pubky", row.id)?;
        let aggregate_id = payload_str(&row.payload, "aggregate_id", row.id)?;

        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
             aggregate_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (event_id, recipient_pubky) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(row.event_id)
        .bind(recipient)
        .bind(actor)
        .bind(notification_type)
        .bind(aggregate_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
            .bind(row.id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        delivered += 1;
    }
    Ok(delivered)
}

fn payload_str<'a>(payload: &'a Value, field: &str, row_id: i64) -> anyhow::Result<&'a str> {
    payload[field]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("outbox row {row_id} payload is missing {field}"))
}

/// Claims and delivers due outbox intents.
pub async fn drain_outbox(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> anyhow::Result<u64> {
    let claimed = claim_outbox_batch(pool, now, lease_seconds).await?;
    deliver_claimed(pool, &claimed, now).await
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct WorkerSummary {
    pub reservations_expired: u64,
    pub offers_expired: u64,
    pub auctions_closed: u64,
    pub outbox_delivered: u64,
}

/// One worker pass: for each task, take the lease, drain, release. Tasks
/// whose lease is held by another live instance are skipped.
pub async fn run_once(
    state: &AppState,
    holder: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<WorkerSummary> {
    let lease_seconds = state.config.worker_lease_seconds;
    let mut summary = WorkerSummary::default();

    if try_acquire_lease(
        &state.pool,
        TASK_RESERVATION_EXPIRY,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        summary.reservations_expired = expiry::expire_due_reservations(&state.pool, now).await?;
        release_lease(&state.pool, TASK_RESERVATION_EXPIRY, holder, now).await?;
    }
    if try_acquire_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now, lease_seconds).await? {
        summary.offers_expired = expire_due_offers(&state.pool, now).await?;
        release_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now).await?;
    }
    if try_acquire_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now, lease_seconds).await? {
        summary.auctions_closed = close_due_auctions(&state.pool, now).await?;
        release_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now).await?;
    }
    if try_acquire_lease(&state.pool, TASK_OUTBOX, holder, now, lease_seconds).await? {
        summary.outbox_delivered = drain_outbox(&state.pool, now, lease_seconds).await?;
        release_lease(&state.pool, TASK_OUTBOX, holder, now).await?;
    }
    Ok(summary)
}

/// Spawns the periodic worker runtime used by the production binary. Each
/// process gets a unique holder id; multiple instances coordinate through
/// the lease table.
pub fn spawn(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let holder = Uuid::new_v4();
        let interval = std::time::Duration::from_secs(state.config.worker_interval_seconds);
        loop {
            tokio::time::sleep(interval).await;
            let now = state.clock.now();
            match run_once(&state, holder, now).await {
                Ok(summary) => {
                    if summary != WorkerSummary::default() {
                        tracing::info!(
                            reservations_expired = summary.reservations_expired,
                            offers_expired = summary.offers_expired,
                            auctions_closed = summary.auctions_closed,
                            outbox_delivered = summary.outbox_delivered,
                            "worker pass completed"
                        );
                    }
                }
                Err(error) => tracing::error!(error = %error, "worker pass failed"),
            }
        }
    })
}
