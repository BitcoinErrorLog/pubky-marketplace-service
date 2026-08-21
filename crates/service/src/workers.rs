//! Background worker runtime (plan tasks 3.4 and 4.5).
//!
//! One runtime drains six server-time tasks: (a) reservation expiry,
//! (b) offer expiry, (c) auction close, (d) the outbox, (e) Locks lifecycle
//! verification, and (f) the marketplace payment window. Each task is
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
//!
//! Locks verification semantics (ADR-0019 §7): the service independently
//! verifies each pending correlation against the Lock Server's lifecycle
//! lookup and advances the payment on a completed result exactly once — the
//! payment-state compare-and-swap plus the `events_one_payment_confirmed`
//! unique index enforce the once, not application logic. A claim only stamps
//! `last_checked_at`, so a crash between the lookup and the effect leaves
//! the correlation pending and it is re-verified after the poll interval:
//! bounded, abortable, and resumable across restarts. Marketplace
//! payment-window expiry is a separate server-time transition — Locks v1
//! leaves transport/status failures pending, so upstream trouble never
//! expires a payment by itself — and a completion verified after the window
//! goes to `manual_review`, never silently discarded.

use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::handlers::auction::{close_locked_auction, parse_auction};
use crate::handlers::payment::confirm_order;
use crate::handlers::{fetch_order_for_update, insert_notification_intent, LISTING_COLUMNS};
use crate::locks::{LocksLookupOutcome, LocksRuntime, LocksTaskStatus};
use crate::model::{ListingRow, PaymentRow};
use crate::queries::PAYMENT_COLUMNS;
use crate::{expiry, AppState};

pub const TASK_RESERVATION_EXPIRY: &str = "reservation_expiry";
pub const TASK_OFFER_EXPIRY: &str = "offer_expiry";
pub const TASK_AUCTION_CLOSE: &str = "auction_close";
pub const TASK_OUTBOX: &str = "outbox";
pub const TASK_LOCKS_VERIFICATION: &str = "locks_verification";
pub const TASK_PAYMENT_WINDOW: &str = "payment_window";
pub const TASK_STAT_ATTESTATIONS: &str = "stat_attestations";

const OUTBOX_BATCH_SIZE: i64 = 100;
const LOCKS_VERIFY_BATCH_SIZE: i64 = 25;

/// Stat attestation cadence (ratified D3: weekly) and window (trailing 90
/// days, matching the design's period example).
const STAT_ATTESTATION_INTERVAL_DAYS: i64 = 7;
const STAT_ATTESTATION_WINDOW_DAYS: i64 = 90;

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

/// One claimed pending correlation. The bundle id leaves this struct only
/// as ciphertext; a derived Debug would print bytes, not the secret.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedCorrelation {
    id: Uuid,
    payment_id: Uuid,
    creator_pubky: String,
    bundle_id_ciphertext: Vec<u8>,
    last_observed_status: Option<String>,
}

/// Claims a batch of pending correlations due for a lifecycle lookup by
/// stamping `last_checked_at`. The stamp is the only pre-effect write, so a
/// holder that dies after claiming loses nothing: the row stays `pending`
/// and is re-verified once the poll interval elapses.
async fn claim_due_correlations(
    pool: &PgPool,
    now: DateTime<Utc>,
    poll_seconds: i64,
) -> Result<Vec<ClaimedCorrelation>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE payment_locks_correlations SET last_checked_at = $1, updated_at = $1 \
         WHERE id IN (\
             SELECT id FROM payment_locks_correlations \
             WHERE verification_state = 'pending' \
             AND (last_checked_at IS NULL OR last_checked_at <= $2) \
             ORDER BY last_checked_at ASC NULLS FIRST LIMIT $3 FOR UPDATE SKIP LOCKED\
         ) RETURNING id, payment_id, creator_pubky, bundle_id_ciphertext, last_observed_status",
    )
    .bind(now)
    .bind(now - chrono::Duration::seconds(poll_seconds))
    .bind(LOCKS_VERIFY_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Appends one reconciliation-history row (append-only by trigger).
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    correlation_id: Uuid,
    observed_status: &str,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO payment_locks_observations (correlation_id, observed_status, outcome, \
         observed_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(correlation_id)
    .bind(observed_status)
    .bind(outcome)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Records a non-terminal upstream status (pending / in_progress /
/// not_found). History rows are appended only on change, so steady polling
/// does not grow the table.
async fn record_status_observation(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    observed_status: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    if row.last_observed_status.as_deref() == Some(observed_status) {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE payment_locks_correlations SET last_observed_status = $2, updated_at = $3 \
         WHERE id = $1",
    )
    .bind(row.id)
    .bind(observed_status)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    insert_observation(&mut tx, row.id, observed_status, "none", now).await?;
    tx.commit().await?;
    Ok(())
}

/// Records a terminal upstream failure (`failed`/`expired` from Locks) and
/// stops polling the lifecycle. The payment is deliberately untouched: an
/// upstream failure is not a marketplace expiry (ADR-0019 §7) — the payment
/// window moves the payment to `expired` on its own schedule.
async fn record_upstream_terminal(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    observed_status: &str,
    verification_state: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let flipped = sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = $2, \
         last_observed_status = $3, updated_at = $4 \
         WHERE id = $1 AND verification_state = 'pending'",
    )
    .bind(row.id)
    .bind(verification_state)
    .bind(observed_status)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if flipped.rows_affected() == 1 {
        insert_observation(&mut tx, row.id, observed_status, "none", now).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Marks the correlation completed inside the caller's effect transaction.
async fn mark_correlation_completed(
    tx: &mut Transaction<'_, Postgres>,
    correlation_id: Uuid,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let flipped = sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = 'completed', \
         last_observed_status = 'completed', completed_at = $2, updated_at = $2 \
         WHERE id = $1 AND verification_state = 'pending'",
    )
    .bind(correlation_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if flipped.rows_affected() == 1 {
        insert_observation(tx, correlation_id, "completed", outcome, now).await?;
    }
    Ok(())
}

/// Moves the payment to `manual_review` from `from_state` (a completion that
/// can no longer confirm the order: verified after the marketplace window,
/// or refused by the confirmation invariants). The compare-and-swap makes a
/// redelivered completion harmless, and the correlation plus its observation
/// row retain the history — a late completion is never silently discarded.
async fn apply_manual_review(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    from_state: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let updated: Option<(i64, String)> = sqlx::query_as(
        "UPDATE payments SET state = 'manual_review', revision = revision + 1, updated_at = $3 \
         WHERE id = $1 AND state = $2 RETURNING revision, buyer_pubky",
    )
    .bind(row.payment_id)
    .bind(from_state)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let applied = match updated {
        Some((revision, buyer_pubky)) => {
            crate::executor::insert_event(
                &mut tx,
                row.id,
                &ids::payment_aggregate_id(row.payment_id),
                revision,
                &buyer_pubky,
                "payment.manual_review",
                now,
            )
            .await?;
            mark_correlation_completed(&mut tx, row.id, "manual_review", now).await?;
            true
        }
        None => {
            // The payment moved concurrently; keep the completed fact.
            mark_correlation_completed(&mut tx, row.id, "none", now).await?;
            false
        }
    };
    tx.commit().await?;
    Ok(applied)
}

/// Applies one independently verified completed Locks result. Advances
/// `awaiting_entitlement → confirmed` exactly once (payment CAS + the
/// `events_one_payment_confirmed` unique index); routes a completion that
/// arrives after marketplace expiry — or one whose order can no longer be
/// confirmed — to `manual_review`; treats an already-advanced payment as a
/// harmless duplicate.
async fn apply_completed_lifecycle(
    pool: &PgPool,
    row: &ClaimedCorrelation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(row.payment_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(payment) = payment else {
        anyhow::bail!("correlation {} references a missing payment", row.id);
    };

    match payment.state.as_str() {
        "awaiting_entitlement" => {
            let Some(order) = fetch_order_for_update(&mut tx, payment.order_id).await? else {
                anyhow::bail!("correlation {} references a missing order", row.id);
            };
            match confirm_order(&mut tx, &payment.buyer_pubky, row.id, &payment, order, now).await?
            {
                Ok((order, _receipt, _receipt_event_id)) => {
                    let (revision,): (i64,) = sqlx::query_as(
                        "UPDATE payments SET state = 'confirmed', revision = revision + 1, \
                         updated_at = $2 WHERE id = $1 RETURNING revision",
                    )
                    .bind(payment.id)
                    .bind(now)
                    .fetch_one(&mut *tx)
                    .await?;
                    let event_id = crate::executor::insert_event(
                        &mut tx,
                        row.id,
                        &ids::payment_aggregate_id(payment.id),
                        revision,
                        &payment.buyer_pubky,
                        "payment.confirmed",
                        now,
                    )
                    .await?;
                    insert_notification_intent(
                        &mut tx,
                        event_id,
                        "payment_confirmed",
                        &order.seller_pubky,
                        &payment.buyer_pubky,
                        &ids::order_aggregate_id(order.id),
                        now,
                    )
                    .await?;
                    mark_correlation_completed(&mut tx, row.id, "payment_confirmed", now).await?;
                    tx.commit().await?;
                    tracing::info!(
                        payment_id = %payment.id,
                        correlation_id = %row.id,
                        "confirmed payment on verified locks completion"
                    );
                    Ok(true)
                }
                Err(failure) => {
                    // The entitlement is real but the order can no longer be
                    // confirmed (e.g. a lapsed auction hold). Roll back the
                    // partial confirmation effects, then retain the fact
                    // under manual review.
                    tx.rollback().await?;
                    tracing::warn!(
                        payment_id = %payment.id,
                        correlation_id = %row.id,
                        code = ?failure.code,
                        "verified locks completion could not confirm the order; routing to manual review"
                    );
                    apply_manual_review(pool, row, "awaiting_entitlement", now).await
                }
            }
        }
        "expired" => {
            tx.rollback().await?;
            tracing::info!(
                payment_id = %payment.id,
                correlation_id = %row.id,
                "verified locks completion arrived after the payment window; routing to manual review"
            );
            apply_manual_review(pool, row, "expired", now).await
        }
        _ => {
            // Already confirmed or under review: a duplicate or reordered
            // completion has no further effect.
            mark_correlation_completed(&mut tx, row.id, "none", now).await?;
            tx.commit().await?;
            Ok(false)
        }
    }
}

/// One Locks verification pass: claims due pending correlations, performs
/// the independent lifecycle lookup for each, and applies the outcome.
/// Returns the number of payments advanced (confirmed or manual review).
pub async fn verify_due_locks_lifecycles(
    state: &AppState,
    locks: &LocksRuntime,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let claimed = claim_due_correlations(&state.pool, now, state.config.locks_poll_seconds).await?;
    let mut applied = 0u64;
    for row in &claimed {
        let bundle_id = locks
            .keys
            .decrypt_bundle_id(row.payment_id, &row.bundle_id_ciphertext)
            .map_err(|error| {
                anyhow::anyhow!("correlation {} cannot be decrypted: {error}", row.id)
            })?;
        let outcome = locks.client.lookup(&row.creator_pubky, &bundle_id).await;
        match outcome {
            LocksLookupOutcome::Status(LocksTaskStatus::Completed) => {
                if apply_completed_lifecycle(&state.pool, row, now).await? {
                    applied += 1;
                }
            }
            LocksLookupOutcome::Status(LocksTaskStatus::Failed) => {
                record_upstream_terminal(&state.pool, row, "failed", "upstream_failed", now)
                    .await?;
            }
            LocksLookupOutcome::Status(LocksTaskStatus::Expired) => {
                record_upstream_terminal(&state.pool, row, "expired", "upstream_expired", now)
                    .await?;
            }
            LocksLookupOutcome::Status(LocksTaskStatus::Pending) => {
                record_status_observation(&state.pool, row, "pending", now).await?;
            }
            LocksLookupOutcome::Status(LocksTaskStatus::InProgress) => {
                record_status_observation(&state.pool, row, "in_progress", now).await?;
            }
            LocksLookupOutcome::NotFound => {
                record_status_observation(&state.pool, row, "not_found", now).await?;
            }
            LocksLookupOutcome::Unavailable => {
                // Transport/status trouble stays pending and retryable
                // (Locks v1 has no terminal payment failure); the claim
                // stamp already deferred the next attempt.
            }
        }
    }
    Ok(applied)
}

/// Expires Locks-correlated payments whose marketplace payment window has
/// elapsed on server time while the payment still awaits entitlement. This
/// is a marketplace policy transition, independent of upstream state; the
/// correlation keeps polling (bounded by the Lock Server's own task ageing),
/// so a completion verified later still surfaces as `manual_review`.
pub async fn expire_due_payment_windows(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT c.id, c.payment_id, p.buyer_pubky \
         FROM payment_locks_correlations c JOIN payments p ON p.id = c.payment_id \
         WHERE c.window_expires_at <= $1 AND p.state = 'awaiting_entitlement' \
         ORDER BY c.window_expires_at FOR UPDATE OF c, p SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for (correlation_id, payment_id, buyer_pubky) in due {
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE payments SET state = 'expired', revision = revision + 1, updated_at = $2 \
             WHERE id = $1 AND state = 'awaiting_entitlement' RETURNING revision",
        )
        .bind(payment_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        crate::executor::insert_event(
            &mut tx,
            correlation_id,
            &ids::payment_aggregate_id(payment_id),
            revision,
            &buyer_pubky,
            "payment.expired",
            now,
        )
        .await?;
        insert_observation(
            &mut tx,
            correlation_id,
            "window_elapsed",
            "payment_expired",
            now,
        )
        .await?;
        tracing::info!(
            payment_id = %payment_id,
            correlation_id = %correlation_id,
            "expired payment on marketplace payment window"
        );
        expired += 1;
    }
    tx.commit().await?;
    Ok(expired)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct WorkerSummary {
    pub reservations_expired: u64,
    pub offers_expired: u64,
    pub auctions_closed: u64,
    pub outbox_delivered: u64,
    pub locks_completions_applied: u64,
    pub payment_windows_expired: u64,
    pub stat_attestations_signed: u64,
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
    // Verification runs only when the deployment has Locks configured (fail
    // closed); the window sweep is a marketplace-time transition and always
    // runs, so correlations registered before a config change still expire.
    if let Some(locks) = &state.locks {
        if try_acquire_lease(
            &state.pool,
            TASK_LOCKS_VERIFICATION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            summary.locks_completions_applied =
                verify_due_locks_lifecycles(state, locks, now).await?;
            release_lease(&state.pool, TASK_LOCKS_VERIFICATION, holder, now).await?;
        }
    }
    if try_acquire_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now, lease_seconds).await? {
        summary.payment_windows_expired = expire_due_payment_windows(&state.pool, now).await?;
        release_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now).await?;
    }
    // Weekly seller stat attestations (ratified D3) run only when the
    // deployment holds the attestor key: unsigned stats would be worthless.
    if let Some(attestor) = &state.attestor {
        if try_acquire_lease(
            &state.pool,
            TASK_STAT_ATTESTATIONS,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            summary.stat_attestations_signed =
                generate_due_stat_attestations(&state.pool, attestor, now).await?;
            release_lease(&state.pool, TASK_STAT_ATTESTATIONS, holder, now).await?;
        }
    }
    Ok(summary)
}

/// One row of the per-order event aggregate the stat computation reads.
#[derive(Debug, sqlx::FromRow)]
struct SellerOrderStats {
    paid_at: Option<DateTime<Utc>>,
    shipped_at: Option<DateTime<Utc>>,
    delivered_at: Option<DateTime<Utc>>,
    disputed: Option<bool>,
    cancelled: Option<bool>,
    refunded: Option<bool>,
}

/// Computes and signs the weekly per-seller stat attestations (ratified D3:
/// median time-to-ship, dispute rate, completion rate — banded and
/// per-mille, never raw amounts). A seller is due when they have at least
/// one delivered order in the trailing window and no attestation newer than
/// the weekly cadence. Rows are stored for the Phase 3 attestor-homeserver
/// publisher; nothing is public yet.
pub async fn generate_due_stat_attestations(
    pool: &PgPool,
    attestor: &crate::attestor::Attestor,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let window_start = now - chrono::Duration::days(STAT_ATTESTATION_WINDOW_DAYS);
    let stale_before = now - chrono::Duration::days(STAT_ATTESTATION_INTERVAL_DAYS);

    let due_sellers: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT o.seller_pubky \
         FROM events e JOIN orders o ON e.aggregate_id = 'order:' || o.id::text \
         WHERE e.kind = 'fulfillment.delivered' AND e.occurred_at >= $1 AND e.occurred_at <= $2 \
           AND NOT EXISTS ( \
             SELECT 1 FROM seller_stat_attestations s \
             WHERE s.seller_pubky = o.seller_pubky AND s.created_at > $3)",
    )
    .bind(window_start)
    .bind(now)
    .bind(stale_before)
    .fetch_all(pool)
    .await?;

    let mut signed = 0u64;
    for (seller_pubky,) in due_sellers {
        // `receipt.issued` is the paid marker on the ORDER aggregate: it is
        // written in the same transaction as the payment confirmation,
        // exactly once per order (payment.confirmed lives on the payment
        // aggregate and is not visible in this per-order join).
        let per_order: Vec<SellerOrderStats> = sqlx::query_as(
            "SELECT \
               MIN(CASE WHEN e.kind = 'receipt.issued' THEN e.occurred_at END) AS paid_at, \
               MIN(CASE WHEN e.kind = 'fulfillment.shipped' THEN e.occurred_at END) AS shipped_at, \
               MIN(CASE WHEN e.kind = 'fulfillment.delivered' THEN e.occurred_at END) AS delivered_at, \
               BOOL_OR(e.kind = 'dispute.opened') AS disputed, \
               BOOL_OR(e.kind = 'order.cancelled') AS cancelled, \
               BOOL_OR(e.kind = 'refund.recorded_external') AS refunded \
             FROM orders o JOIN events e ON e.aggregate_id = 'order:' || o.id::text \
             WHERE o.seller_pubky = $1 AND e.occurred_at >= $2 AND e.occurred_at <= $3 \
             GROUP BY o.id",
        )
        .bind(&seller_pubky)
        .bind(window_start)
        .bind(now)
        .fetch_all(pool)
        .await?;

        let completed = per_order
            .iter()
            .filter(|order| order.delivered_at.is_some())
            .count() as i64;
        if completed < 1 {
            continue;
        }
        let disputed = per_order
            .iter()
            .filter(|order| order.disputed.unwrap_or(false))
            .count() as i64;
        let terminated_badly = per_order
            .iter()
            .filter(|order| order.cancelled.unwrap_or(false) || order.refunded.unwrap_or(false))
            .count() as i64;
        let mut ship_hours: Vec<i64> = per_order
            .iter()
            .filter_map(|order| match (order.paid_at, order.shipped_at) {
                (Some(paid), Some(shipped)) if shipped >= paid => {
                    Some((shipped - paid).num_hours())
                }
                _ => None,
            })
            .collect();
        ship_hours.sort_unstable();
        let median_ship_hours = median(&ship_hours);

        let dispute_rate_permille = disputed * 1_000 / completed;
        let completion_rate_permille = completed * 1_000 / (completed + terminated_badly);
        let period_from = window_start.format("%Y-%m-%d").to_string();
        let period_to = now.format("%Y-%m-%d").to_string();

        // Banded count and per-mille rates, never raw counts or amounts
        // (design §7.2): exact GMV/volume stays private while remaining
        // rankable.
        let body = serde_json::json!({
            "v": 1,
            "attestor": attestor.pubky(),
            "seller": seller_pubky,
            "period": { "from": period_from, "to": period_to },
            "ordersCompletedBand": completed.ilog10().to_string(),
            "medianTimeToShipHours": median_ship_hours,
            "disputeRatePermille": dispute_rate_permille,
            "completionRatePermille": completion_rate_permille,
        });
        let jws = attestor.sign_seller_stats(&body);
        let inserted = sqlx::query(
            "INSERT INTO seller_stat_attestations \
             (id, seller_pubky, period_from, period_to, body, jws, created_at) \
             VALUES ($1, $2, $3::date, $4::date, $5, $6, $7) \
             ON CONFLICT (seller_pubky, period_to) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(&seller_pubky)
        .bind(&period_from)
        .bind(&period_to)
        .bind(&body)
        .bind(&jws)
        .bind(now)
        .execute(pool)
        .await?;
        signed += inserted.rows_affected();
    }
    Ok(signed)
}

/// Median of a sorted slice, `None` when empty (an honest absence beats a
/// fabricated zero).
fn median(sorted: &[i64]) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2
    } else {
        sorted[middle]
    })
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
                            locks_completions_applied = summary.locks_completions_applied,
                            payment_windows_expired = summary.payment_windows_expired,
                            stat_attestations_signed = summary.stat_attestations_signed,
                            "worker pass completed"
                        );
                    }
                }
                Err(error) => tracing::error!(error = %error, "worker pass failed"),
            }
        }
    })
}
