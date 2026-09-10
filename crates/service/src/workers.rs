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
//! payment-window expiry is a separate server-time transition over the
//! ORDER's armed inventory-hold window ("only a payment locks an item"):
//! a lapsed hold restocks, the payment expires, and the order cancels.
//! Locks v1 leaves transport/status failures pending, so upstream trouble
//! never expires a payment by itself — and a completion verified after the
//! window goes to `manual_review`, never silently discarded.
//!
//! Delivery autocomplete semantics (ADR-0019): there is NO carrier
//! tracking feed, so post-purchase liveness is server time. A `shipped`
//! order is marked `delivered` once `DELIVERY_ASSUME_DAYS` has elapsed
//! since the ship timestamp, flagged `delivery_assumed = true` so the UI
//! can say "marked delivered automatically after N days; tell us if it
//! hasn't arrived" (the `delivery_assume` trigger on the machine's
//! shipped → delivered edge). A `delivered` order completes once
//! `AUTO_COMPLETE_DAYS` has elapsed (`order_auto_complete` on the
//! delivered → completed edge); an open return or cancel request is its
//! own order state, so it blocks the sweep by construction. Both events
//! are attributed to the system actor, never to a peer.

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
use crate::payments::{PaykitClient, PaykitCommandError, PaykitStatusOutcome, PaykitStatusSource};
use crate::queries::PAYMENT_COLUMNS;
use crate::{expiry, AppState};

pub const TASK_RESERVATION_EXPIRY: &str = "reservation_expiry";
pub const TASK_DROP_TRANSITIONS: &str = "drop_transitions";
pub const TASK_OFFER_EXPIRY: &str = "offer_expiry";
pub const TASK_AUCTION_CLOSE: &str = "auction_close";
pub const TASK_OUTBOX: &str = "outbox";
pub const TASK_LOCKS_VERIFICATION: &str = "locks_verification";
pub const TASK_PAYKIT_VERIFICATION: &str = "paykit_verification";
pub const TASK_PAYMENT_WINDOW: &str = "payment_window";
pub const TASK_STAT_ATTESTATIONS: &str = "stat_attestations";
pub const TASK_DELIVERY_AUTOCOMPLETE: &str = "delivery_autocomplete";
pub const TASK_PICKUP_RESEAL: &str = "pickup_reseal";
pub const TASK_PICKUP_RETENTION: &str = "pickup_retention";

/// The actor stamped on server-time post-purchase events and their
/// notifications: the system, never a peer (ADR-0019).
pub const SYSTEM_ACTOR: &str = "system";

/// Max inner claim loops per lease so a huge backlog cannot hold
/// `TASK_DELIVERY_AUTOCOMPLETE` indefinitely (next interval resumes).
pub const DELIVERY_SWEEP_MAX_BATCHES: u32 = 10;

const OUTBOX_BATCH_SIZE: i64 = 100;
const LOCKS_VERIFY_BATCH_SIZE: i64 = 25;
const PAYKIT_VERIFY_BATCH_SIZE: i64 = 25;

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

/// Applies the server-time drop transitions for drops no command has
/// touched — announced → live at `starts_at`, live (or a fully elapsed
/// announced window) → ended_closed at `ends_at` — through the same shared
/// transition path gating uses, so projections and gating agree without
/// traffic. Due rows are locked with `FOR UPDATE SKIP LOCKED`.
pub async fn transition_due_drops(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<crate::model::DropRow> = sqlx::query_as(&format!(
        "SELECT {columns} FROM drops \
         WHERE (state = 'announced' AND starts_at <= $1) \
         OR (state = 'live' AND ends_at IS NOT NULL AND ends_at <= $1) \
         ORDER BY aggregate_id FOR UPDATE SKIP LOCKED",
        columns = crate::handlers::drops::DROP_COLUMNS
    ))
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut transitioned = 0u64;
    for drop in due {
        let from = drop.state.clone();
        let after =
            crate::handlers::drops::apply_time_transitions(&mut tx, drop, Uuid::new_v4(), now)
                .await?;
        if after.state != from {
            tracing::info!(
                aggregate_id = %after.aggregate_id,
                from = %from,
                to = %after.state,
                "transitioned drop on server time"
            );
            transitioned += 1;
        }
    }
    tx.commit().await?;
    Ok(transitioned)
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
    pub created_at: DateTime<Utc>,
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
         ) RETURNING id, event_id, kind, payload, created_at",
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
/// apply its effect twice. `paykit.activate` / `paykit.void` rows drive the
/// two-phase protocol against the order's persisted stack endpoint
/// (§B.11.8); a paykit row on a deployment without the signed client is
/// skipped (logged), never head-of-line for the rest of the batch.
pub async fn deliver_claimed(
    pool: &PgPool,
    paykit: Option<&PaykitClient>,
    rows: &[ClaimedOutboxRow],
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let mut delivered = 0u64;
    for row in rows {
        if row.kind == "paykit.activate" {
            let Some(paykit) = paykit else {
                tracing::error!(
                    row_id = row.id,
                    "paykit.activate row on a deployment without the paykit client; skipping"
                );
                continue;
            };
            if deliver_paykit_activation(pool, paykit, row, now).await? {
                delivered += 1;
            }
            continue;
        }
        if row.kind == "paykit.void" {
            let Some(paykit) = paykit else {
                tracing::error!(
                    row_id = row.id,
                    "paykit.void row on a deployment without the paykit client; skipping"
                );
                continue;
            };
            if deliver_paykit_void(pool, paykit, row, now).await? {
                delivered += 1;
            }
            continue;
        }
        let Some(notification_type) = row.kind.strip_prefix("notification.") else {
            anyhow::bail!("outbox row {} has unroutable kind {}", row.id, row.kind);
        };
        let recipient = payload_str(&row.payload, "recipient_pubky", row.id)?;
        let actor = payload_str(&row.payload, "actor_pubky", row.id)?;
        let aggregate_id = payload_str(&row.payload, "aggregate_id", row.id)?;
        // Optional monetary context; intents written before amounts existed
        // have no key and deliver as NULL.
        let amount = match &row.payload["amount"] {
            Value::Null => None,
            value => Some(value.clone()),
        };

        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
             aggregate_id, amount, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (event_id, recipient_pubky) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(row.event_id)
        .bind(recipient)
        .bind(actor)
        .bind(notification_type)
        .bind(aggregate_id)
        .bind(amount)
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
    paykit: Option<&PaykitClient>,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> anyhow::Result<u64> {
    let claimed = claim_outbox_batch(pool, now, lease_seconds).await?;
    deliver_claimed(pool, paykit, &claimed, now).await
}

// ---------------------------------------------------------------------------
// Two-phase paykit activation (§B.11.2, §B.11.8)
// ---------------------------------------------------------------------------

/// The parsed `paykit.activate` payload (written in the bind transaction).
struct ActivatePayload {
    invoice_id: Uuid,
    order_id: Uuid,
    activation_attempt: i64,
}

fn parse_activate_payload(row: &ClaimedOutboxRow) -> anyhow::Result<ActivatePayload> {
    let invoice_id = payload_str(&row.payload, "invoice_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} invoice_id is not a uuid", row.id))?;
    let order_id = payload_str(&row.payload, "order_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} order_id is not a uuid", row.id))?;
    let activation_attempt = row.payload["activation_attempt"].as_i64().ok_or_else(|| {
        anyhow::anyhow!(
            "outbox row {} payload is missing activation_attempt",
            row.id
        )
    })?;
    Ok(ActivatePayload {
        invoice_id,
        order_id,
        activation_attempt,
    })
}

/// One transaction stamping the outbox row delivered and nothing else (the
/// idempotent no-op for a row whose order already left `preparing`).
async fn stamp_delivered_only(
    pool: &PgPool,
    row_id: i64,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(row_id)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

/// The terminal-void transaction shared by every `preparing → voided` edge
/// (§B.11.2): the bind is released — hold returned to the listing, the
/// payment row restored to its pre-bind state, the method cleared so the
/// buyer can choose again — and the `payment.bitcoin_prepare_voided`
/// notification intent is emitted, all atomically with the outbox mark.
/// Caller holds the outbox row's lease.
async fn void_prepare_effects(
    pool: &PgPool,
    row_id: i64,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let Some(order) = fetch_order_for_update(&mut tx, order_id).await? else {
        anyhow::bail!("paykit.activate row {row_id} references a missing order {order_id}");
    };
    if order.paykit_activation_state.as_deref() != Some("preparing") {
        // A redelivery or a concurrently resolved row: mark only.
        sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
            .bind(row_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(());
    }
    if order.stock_held {
        if let Err(failure) = crate::handlers::holds::release_lines(
            &mut tx,
            &order,
            crate::handlers::holds::HeldQuantity::Reserved,
            now,
        )
        .await?
        {
            anyhow::bail!("voided order {order_id} could not release its hold: {failure:?}");
        }
    }
    let (revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET revision = revision + 1, paykit_activation_state = 'voided', \
         paykit_request_state = NULL, payment_method = NULL, stock_held = false, \
         hold_expires_at = NULL, updated_at = $2 WHERE id = $1 RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    // The payment row returns to its pre-bind state (sandbox adapter,
    // listing-price amount), so a fresh bind starts clean.
    sqlx::query(
        "UPDATE payments SET revision = revision + 1, adapter = 'sandbox', \
         amount_minor = $2, updated_at = $3 WHERE id = $1",
    )
    .bind(order.payment_id)
    .bind(order.total_minor)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    let event_id = crate::executor::insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order_id),
        revision,
        SYSTEM_ACTOR,
        "payment.bitcoin_prepare_voided",
        now,
    )
    .await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        "bitcoin_prepare_voided",
        &order.buyer_pubky,
        SYSTEM_ACTOR,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await?;
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(row_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    tracing::info!(order_id = %order_id, "voided a prepared bitcoin bind");
    Ok(())
}

/// Delivers one `paykit.activate` row (§B.11.8): POSTs the signed activate
/// to the order's PERSISTED stack endpoint (never the current
/// configuration), then commits the state flip and the `delivered_at` mark
/// in one transaction so a redelivered row cannot apply twice.
async fn deliver_paykit_activation(
    pool: &PgPool,
    paykit: &PaykitClient,
    row: &ClaimedOutboxRow,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let payload = parse_activate_payload(row)?;
    // Read the order's persisted pin under its row lock; count this try by
    // incrementing the attempt in the payload before the call.
    let (endpoint, stack_id, total_sats, attempt) = {
        let mut tx = pool.begin().await?;
        let Some(order) = fetch_order_for_update(&mut tx, payload.order_id).await? else {
            anyhow::bail!(
                "paykit.activate row {} references a missing order {}",
                row.id,
                payload.order_id
            );
        };
        if order.paykit_activation_state.as_deref() != Some("preparing") {
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(true);
        }
        let attempt = payload.activation_attempt + 1;
        sqlx::query(
            "UPDATE outbox SET payload = jsonb_set(payload, '{activation_attempt}', $2) \
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(serde_json::json!(attempt))
        .execute(&mut *tx)
        .await?;
        let pin = (
            order.paykit_stack_endpoint.clone(),
            order.paykit_stack_id.clone(),
            order.paykit_total_sats,
        );
        tx.commit().await?;
        let (Some(endpoint), Some(stack_id), Some(total_sats)) = pin else {
            anyhow::bail!(
                "preparing order {} is missing its persisted paykit pin",
                payload.order_id
            );
        };
        (endpoint, stack_id, total_sats, attempt)
    };
    let total_sats = u64::try_from(total_sats)
        .map_err(|_| anyhow::anyhow!("order {} paykit_total_sats is negative", payload.order_id))?;
    match paykit
        .activate_payment_request(
            &endpoint,
            payload.invoice_id,
            &stack_id,
            total_sats,
            u64::try_from(attempt).unwrap_or(0),
        )
        .await
    {
        Ok(activated) => {
            if activated.total_sats != total_sats {
                tracing::error!(
                    order_id = %payload.order_id,
                    persisted_total_sats = total_sats,
                    activated_total_sats = activated.total_sats,
                    "ALERT paykit activate acknowledged a different total than persisted; \
                     voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                return Ok(true);
            }
            let mut tx = pool.begin().await?;
            // Conditional on `preparing` under the row lock: a redelivered
            // row cannot apply the flip twice.
            let flipped = sqlx::query(
                "UPDATE orders SET paykit_activation_state = 'active', \
                 paykit_request_state = 'pending', updated_at = $2 \
                 WHERE id = $1 AND paykit_activation_state = 'preparing'",
            )
            .bind(payload.order_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(row.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            if flipped.rows_affected() == 1 {
                tracing::info!(
                    order_id = %payload.order_id,
                    invoice_id = %payload.invoice_id,
                    "activated a prepared bitcoin payment request"
                );
            }
            Ok(true)
        }
        Err(error) => match error {
            PaykitCommandError::PrepareExpired | PaykitCommandError::InvoiceFinalized => {
                tracing::warn!(
                    order_id = %payload.order_id,
                    error = ?error,
                    "paykit prepare is finalized; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::UnknownInvoice => {
                tracing::error!(
                    order_id = %payload.order_id,
                    invoice_id = %payload.invoice_id,
                    stack_id = %stack_id,
                    "ALERT paykit reports an unknown invoice (stack mixup); voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::ActivationTotalMismatch => {
                tracing::error!(
                    order_id = %payload.order_id,
                    persisted_total_sats = total_sats,
                    "ALERT paykit refused activation with a total mismatch; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::StackIdentityMismatch => {
                tracing::error!(
                    order_id = %payload.order_id,
                    persisted_stack_id = %stack_id,
                    endpoint = %endpoint,
                    "ALERT the persisted paykit endpoint answered with a different stack \
                     identity; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::UnexpectedRejection(code) => {
                tracing::error!(
                    order_id = %payload.order_id,
                    code = %code,
                    "ALERT paykit activation hit an unknown contract state; voiding the bind"
                );
                void_prepare_effects(pool, row.id, payload.order_id, now).await?;
                Ok(true)
            }
            PaykitCommandError::Unavailable => {
                tracing::warn!(
                    order_id = %payload.order_id,
                    attempt,
                    "paykit activation unreachable; the row retries under its lease"
                );
                Ok(false)
            }
        },
    }
}

/// Delivers one `paykit.void` row (inserted only by the preparing-order
/// hold-expiry hard bound): an idempotent void retried under the ordinary
/// lease until delivered or 24 h old, then stamped with a `gave_up` log.
async fn deliver_paykit_void(
    pool: &PgPool,
    paykit: &PaykitClient,
    row: &ClaimedOutboxRow,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    if row.created_at + chrono::Duration::hours(24) <= now {
        tracing::warn!(
            row_id = row.id,
            "gave_up delivering a paykit.void row after 24h; stamping it delivered"
        );
        stamp_delivered_only(pool, row.id, now).await?;
        return Ok(true);
    }
    let invoice_id: Uuid = payload_str(&row.payload, "invoice_id", row.id)?
        .parse()
        .map_err(|_| anyhow::anyhow!("outbox row {} invoice_id is not a uuid", row.id))?;
    let stack_id = payload_str(&row.payload, "stack_id", row.id)?;
    let endpoint = payload_str(&row.payload, "stack_endpoint", row.id)?;
    let reason = payload_str(&row.payload, "reason", row.id)?;
    match paykit
        .void_payment_request(endpoint, invoice_id, stack_id, reason)
        .await
    {
        Ok(voided) => {
            tracing::info!(
                invoice_id = %invoice_id,
                state = %voided.state,
                "delivered a paykit void"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        // Idempotent by contract (§B.11.6): the invoice is already in a
        // final state, so the caller's intent is satisfied.
        Err(PaykitCommandError::PrepareExpired)
        | Err(PaykitCommandError::InvoiceFinalized)
        | Err(PaykitCommandError::UnknownInvoice) => {
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        Err(PaykitCommandError::StackIdentityMismatch) => {
            tracing::error!(
                row_id = row.id,
                invoice_id = %invoice_id,
                stack_id = %stack_id,
                "ALERT paykit void refused with a stack identity mismatch; stamping delivered"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        Err(PaykitCommandError::ActivationTotalMismatch)
        | Err(PaykitCommandError::UnexpectedRejection(_)) => {
            tracing::error!(
                row_id = row.id,
                invoice_id = %invoice_id,
                "ALERT paykit void hit an unknown contract state; stamping delivered"
            );
            stamp_delivered_only(pool, row.id, now).await?;
            Ok(true)
        }
        Err(PaykitCommandError::Unavailable) => {
            tracing::warn!(
                row_id = row.id,
                invoice_id = %invoice_id,
                "paykit void unreachable; the row retries under its lease"
            );
            Ok(false)
        }
    }
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
    pickup: Option<&crate::pickup::PickupKeys>,
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
            match confirm_order(
                &mut tx,
                &payment.buyer_pubky,
                row.id,
                &payment,
                order,
                pickup,
                now,
            )
            .await?
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
                        None,
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

/// Applies the independently verified lifecycle outcome for ONE claimed
/// correlation. Errors propagate to the caller's per-item handler — one
/// bad row must never stall the rest of the claimed batch.
async fn apply_locks_lookup_outcome(
    state: &AppState,
    locks: &LocksRuntime,
    row: &ClaimedCorrelation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let bundle_id = locks
        .keys
        .decrypt_bundle_id(row.payment_id, &row.bundle_id_ciphertext)
        .map_err(|error| anyhow::anyhow!("correlation {} cannot be decrypted: {error}", row.id))?;
    let outcome = locks.client.lookup(&row.creator_pubky, &bundle_id).await;
    match outcome {
        LocksLookupOutcome::Status(LocksTaskStatus::Completed) => {
            apply_completed_lifecycle(&state.pool, row, state.pickup.as_deref(), now).await
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Failed) => {
            record_upstream_terminal(&state.pool, row, "failed", "upstream_failed", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Expired) => {
            record_upstream_terminal(&state.pool, row, "expired", "upstream_expired", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::Pending) => {
            record_status_observation(&state.pool, row, "pending", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Status(LocksTaskStatus::InProgress) => {
            record_status_observation(&state.pool, row, "in_progress", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::NotFound => {
            record_status_observation(&state.pool, row, "not_found", now).await?;
            Ok(false)
        }
        LocksLookupOutcome::Unavailable => {
            // Transport/status trouble stays pending and retryable
            // (Locks v1 has no terminal payment failure); the claim
            // stamp already deferred the next attempt.
            Ok(false)
        }
    }
}

/// One Locks verification pass: claims due pending correlations, performs
/// the independent lifecycle lookup for each, and applies the outcome.
/// Returns the number of payments advanced (confirmed or manual review).
/// A per-item failure (an undecryptable bundle id, an unopenable sealed
/// pickup row, a database error mid-effect) is logged with the correlation
/// identity and the pass CONTINUES with the rest of the claimed batch —
/// one bad row is never head-of-line for the others.
pub async fn verify_due_locks_lifecycles(
    state: &AppState,
    locks: &LocksRuntime,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let claimed = claim_due_correlations(&state.pool, now, state.config.locks_poll_seconds).await?;
    let mut applied = 0u64;
    for row in &claimed {
        match apply_locks_lookup_outcome(state, locks, row, now).await {
            Ok(true) => applied += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    correlation_id = %row.id,
                    payment_id = %row.payment_id,
                    error = %error,
                    "locks verification failed for a claimed correlation; continuing the batch"
                );
            }
        }
    }
    Ok(applied)
}

/// One claimed pending paykit-verified bitcoin order.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedPaykitOrder {
    id: Uuid,
    payment_id: Uuid,
    buyer_pubky: String,
    seller_pubky: String,
    paykit_request_reference: String,
    paykit_request_state: String,
}

/// Claims a batch of bitcoin orders due for a paykit status poll by stamping
/// `paykit_last_checked_at` — the only pre-effect write, so a crashed holder
/// loses nothing.
async fn claim_due_paykit_orders(
    pool: &PgPool,
    now: DateTime<Utc>,
    poll_seconds: i64,
) -> Result<Vec<ClaimedPaykitOrder>, sqlx::Error> {
    // Expired payments keep polling only while money was already DETECTED
    // on-chain: a settlement confirmed after the hold window elapsed must
    // surface as manual_review, never vanish. An expired order that never
    // saw a detection stops polling.
    sqlx::query_as(
        "UPDATE orders SET paykit_last_checked_at = $1, updated_at = updated_at \
         WHERE id IN (\
             SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
             WHERE p.adapter = 'paykit' \
             AND ((p.state = 'awaiting_entitlement' \
                   AND o.paykit_request_state IN ('pending', 'detected')) \
                  OR (p.state = 'expired' AND o.paykit_request_state = 'detected')) \
             AND o.paykit_request_reference IS NOT NULL \
             AND (o.paykit_last_checked_at IS NULL OR o.paykit_last_checked_at <= $2) \
             ORDER BY o.paykit_last_checked_at ASC NULLS FIRST LIMIT $3 \
             FOR UPDATE OF o SKIP LOCKED\
         ) RETURNING id, payment_id, buyer_pubky, seller_pubky, \
         paykit_request_reference, paykit_request_state",
    )
    .bind(now)
    .bind(now - chrono::Duration::seconds(poll_seconds))
    .bind(PAYKIT_VERIFY_BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// Applies one confirmed paykit payment: `awaiting_entitlement → confirmed`
/// exactly once via the shared confirmation effects; a confirmation the
/// order can no longer accept (or an amount mismatch paykit observed) routes
/// the payment to `manual_review`.
async fn apply_confirmed_paykit_payment(
    pool: &PgPool,
    row: &ClaimedPaykitOrder,
    amount_matched: bool,
    pickup: Option<&crate::pickup::PickupKeys>,
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
        anyhow::bail!("paykit order {} references a missing payment", row.id);
    };
    if payment.state == "expired" {
        // The settlement is real but the hold window already elapsed (the
        // sweep released the stock and cancelled the order): retain the
        // fact under manual review, exactly like a late Locks completion.
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        crate::executor::insert_event(
            &mut tx,
            row.id,
            &ids::payment_aggregate_id(payment.id),
            revision,
            &row.buyer_pubky,
            "payment.manual_review",
            now,
        )
        .await?;
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', updated_at = $2 WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %row.id,
            "paykit settlement confirmed after the hold window; routing to manual review"
        );
        return Ok(true);
    }
    if payment.state != "awaiting_entitlement" {
        tx.rollback().await?;
        return Ok(false);
    }
    if !amount_matched {
        // Money arrived but not the required amount: never silently confirm.
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        crate::executor::insert_event(
            &mut tx,
            row.id,
            &ids::payment_aggregate_id(payment.id),
            revision,
            &row.buyer_pubky,
            "payment.manual_review",
            now,
        )
        .await?;
        sqlx::query(
            "UPDATE orders SET paykit_request_state = 'confirmed', updated_at = $2 WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %row.id,
            "confirmed paykit payment did not match the required amount; routing to manual review"
        );
        return Ok(true);
    }
    let Some(order) = fetch_order_for_update(&mut tx, row.id).await? else {
        anyhow::bail!("paykit order {} is missing", row.id);
    };
    match confirm_order(
        &mut tx,
        &row.buyer_pubky,
        row.id,
        &payment,
        order,
        pickup,
        now,
    )
    .await?
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
                &row.buyer_pubky,
                "payment.confirmed",
                now,
            )
            .await?;
            insert_notification_intent(
                &mut tx,
                event_id,
                "payment_confirmed",
                &order.seller_pubky,
                &row.buyer_pubky,
                &ids::order_aggregate_id(order.id),
                None,
                now,
            )
            .await?;
            sqlx::query(
                "UPDATE orders SET paykit_request_state = 'confirmed', updated_at = $2 \
                 WHERE id = $1",
            )
            .bind(row.id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            tracing::info!(
                order_id = %row.id,
                "confirmed payment on observed paykit settlement"
            );
            Ok(true)
        }
        Err(failure) => {
            tx.rollback().await?;
            tracing::warn!(
                order_id = %row.id,
                code = ?failure.code,
                "confirmed paykit payment could not confirm the order; routing to manual review"
            );
            let mut tx = pool.begin().await?;
            let updated: Option<(i64,)> = sqlx::query_as(
                "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
                 updated_at = $2 WHERE id = $1 AND state = 'awaiting_entitlement' \
                 RETURNING revision",
            )
            .bind(payment.id)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some((revision,)) = updated {
                crate::executor::insert_event(
                    &mut tx,
                    row.id,
                    &ids::payment_aggregate_id(payment.id),
                    revision,
                    &row.buyer_pubky,
                    "payment.manual_review",
                    now,
                )
                .await?;
            }
            sqlx::query(
                "UPDATE orders SET paykit_request_state = 'confirmed', updated_at = $2 \
                 WHERE id = $1",
            )
            .bind(row.id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(updated.is_some())
        }
    }
}

/// Applies the polled paykit status for ONE claimed order. Errors
/// propagate to the caller's per-item handler — one bad row must never
/// stall the rest of the claimed batch.
async fn apply_paykit_status_outcome(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    row: &ClaimedPaykitOrder,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    match source
        .status(&row.seller_pubky, &row.paykit_request_reference)
        .await
    {
        PaykitStatusOutcome::Confirmed { amount_matched } => {
            apply_confirmed_paykit_payment(
                &state.pool,
                row,
                amount_matched,
                state.pickup.as_deref(),
                now,
            )
            .await
        }
        PaykitStatusOutcome::Detected => {
            if row.paykit_request_state != "detected" {
                sqlx::query(
                    "UPDATE orders SET paykit_request_state = 'detected', updated_at = $2 \
                     WHERE id = $1 AND paykit_request_state = 'pending'",
                )
                .bind(row.id)
                .bind(now)
                .execute(&state.pool)
                .await?;
            }
            Ok(false)
        }
        // Not yet visible, or the request never reached paykit-server
        // (NotFound stays retryable: creation is idempotent and the
        // binding transaction only commits after a successful create).
        PaykitStatusOutcome::Undetected
        | PaykitStatusOutcome::NotFound
        | PaykitStatusOutcome::Unavailable => Ok(false),
    }
}

/// One paykit verification pass: claims due bitcoin orders, polls the
/// paykit-server status for each, and applies the outcome. Returns the
/// number of payments advanced (confirmed or manual review). A per-item
/// failure is logged with the order identity and the pass CONTINUES with
/// the rest of the claimed batch — one bad row is never head-of-line for
/// the others.
pub async fn verify_due_paykit_payments(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let claimed =
        claim_due_paykit_orders(&state.pool, now, state.config.paykit_poll_seconds).await?;
    let mut applied = 0u64;
    for row in &claimed {
        match apply_paykit_status_outcome(state, source, row, now).await {
            Ok(true) => applied += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    order_id = %row.id,
                    payment_id = %row.payment_id,
                    error = %error,
                    "paykit verification failed for a claimed order; continuing the batch"
                );
            }
        }
    }
    Ok(applied)
}

/// Expires pending orders whose armed inventory-hold window has elapsed on
/// server time ("only a payment locks an item"): the hold releases
/// (`reserved → available`, crediting a stamped drop first under the shared
/// lock order), the payment moves to `expired` (the machine's
/// `payment_window` edge from `awaiting_entitlement`), and the order is
/// cancelled with the stored reason "payment window elapsed" — post-expiry
/// the buyer simply checks out again.
///
/// This is a marketplace policy transition, independent of upstream state; a
/// Locks correlation keeps polling (bounded by the Lock Server's own task
/// ageing), so a completion verified later still surfaces as
/// `manual_review`. Payments already under `manual_review` are deliberately
/// NOT swept: a human decides those — their money may be real, so their
/// hold is never auto-released. A sandbox payment the buyer drove to
/// `detected` has no `detected → expired` edge; its lapsed order still
/// cancels and restocks, and the payment record stays untouched exactly as
/// buyer cancellation leaves it. Confirmed orders are `paid` and never
/// match the sweep.
pub async fn expire_due_payment_windows(
    state: &AppState,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let pool = &state.pool;
    let mut tx = pool.begin().await?;
    // A `preparing` bitcoin order is excluded here: its expiry must first
    // settle the prepared invoice with paykit (§B.11.2 hold-expiry rule,
    // the coordinator-decided cell), handled per order below.
    let due: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT o.id, p.id, p.state, o.buyer_pubky \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.state = 'pending_payment' AND o.stock_held AND o.hold_expires_at <= $1 \
         AND p.state IN ('awaiting_entitlement', 'detected', 'expired') \
         AND o.paykit_activation_state IS DISTINCT FROM 'preparing' \
         ORDER BY o.hold_expires_at FOR UPDATE OF o, p SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for (order_id, payment_id, payment_state, buyer_pubky) in due {
        expire_held_order(
            &mut tx,
            order_id,
            payment_id,
            &payment_state,
            &buyer_pubky,
            now,
        )
        .await?;
        expired += 1;
    }
    tx.commit().await?;

    // §B.11.2 hold-expiry rule for `preparing` orders (coordinator
    // decision; design §B.11.2 leaves this cell undefined — W9.3
    // reconcile): the prepared invoice is settled with paykit BEFORE the
    // ordinary expiry effects run, each order in its own transaction so no
    // HTTP call rides the batch transaction.
    let preparing: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.state = 'pending_payment' AND o.stock_held AND o.hold_expires_at <= $1 \
         AND p.state = 'awaiting_entitlement' AND o.paykit_activation_state = 'preparing' \
         ORDER BY o.hold_expires_at",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    for (order_id,) in preparing {
        match expire_preparing_order(state, order_id, now).await {
            Ok(true) => expired += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    order_id = %order_id,
                    error = %error,
                    "preparing-order hold expiry failed; continuing the batch"
                );
            }
        }
    }
    Ok(expired)
}

/// The shared expiry effects for one due order, inside the caller's
/// transaction: the hold releases (`reserved → available`, crediting a
/// stamped drop first under the shared lock order), an
/// `awaiting_entitlement` payment moves to `expired`, and the order is
/// cancelled with the stored reason "payment window elapsed".
async fn expire_held_order(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    payment_id: Uuid,
    payment_state: &str,
    buyer_pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let Some(order) = fetch_order_for_update(tx, order_id).await? else {
        return Ok(());
    };
    // A Locks-correlated payment's expiry keeps its reconciliation
    // trail: the correlation id is the traceable command id and the
    // observation history records the window elapsing.
    let correlation: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM payment_locks_correlations WHERE order_id = $1")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await?;
    let command_id = correlation
        .map(|(id,)| id)
        .unwrap_or_else(uuid::Uuid::new_v4);

    if payment_state == "awaiting_entitlement" {
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'expired', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 RETURNING revision",
        )
        .bind(payment_id)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        crate::executor::insert_event(
            tx,
            command_id,
            &ids::payment_aggregate_id(payment_id),
            revision,
            buyer_pubky,
            "payment.expired",
            now,
        )
        .await?;
        if let Some((correlation_id,)) = correlation {
            insert_observation(tx, correlation_id, "window_elapsed", "payment_expired", now)
                .await?;
        }
    }

    // A drop-stamped hold credits its drop first (drop lock before the
    // listing lock, the shared order): while live the unit restocks and
    // the buyer's per-drop cap frees; an ended drop keeps honest books
    // but nothing reopens.
    if let Some(drop_aggregate_id) = &order.drop_aggregate_id {
        let units: i64 = order
            .lines
            .as_array()
            .expect("order lines are an array")
            .iter()
            .map(|line| {
                line["quantity"]
                    .as_i64()
                    .expect("order line carries its quantity")
            })
            .sum();
        if !crate::handlers::drops::credit_drop_release(
            tx,
            drop_aggregate_id,
            &order.buyer_pubky,
            units,
            now,
        )
        .await?
        {
            anyhow::bail!("expired order {order_id} could not credit drop {drop_aggregate_id}");
        }
    }
    if let Err(failure) = crate::handlers::holds::release_lines(
        tx,
        &order,
        crate::handlers::holds::HeldQuantity::Reserved,
        now,
    )
    .await?
    {
        anyhow::bail!("expired order {order_id} could not release its hold: {failure:?}");
    }

    debug_assert!(marketplace_domain::state_machines::can_transition(
        &marketplace_domain::state_machines::order_machine(),
        "pending_payment",
        "cancelled"
    ));
    let (order_revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET state = 'cancelled', revision = revision + 1, \
         cancellation_reason = 'payment window elapsed', stock_held = false, \
         hold_expires_at = NULL, updated_at = $2 WHERE id = $1 RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    crate::executor::insert_event(
        tx,
        command_id,
        &ids::order_aggregate_id(order_id),
        order_revision,
        buyer_pubky,
        "order.cancelled",
        now,
    )
    .await?;

    tracing::info!(
        order_id = %order_id,
        payment_id = %payment_id,
        "expired hold window: released stock, expired payment, cancelled order"
    );
    Ok(())
}

/// How long past the hold deadline a `preparing` order may wait for an
/// unreachable paykit before the marketplace voids locally and defers the
/// void to a `paykit.void` outbox row.
const PREPARING_VOID_GRACE_SECONDS: i64 = 30 * 60;

/// §B.11.2's hold-expiry cell for a `preparing` bitcoin order (coordinator
/// decision; design §B.11.2 leaves this cell undefined — W9.3 reconcile):
/// the prepared invoice is voided at its persisted endpoint BEFORE the
/// ordinary expiry effects run. Returns true when the order expired.
///
/// - `200` / `prepare_expired` / `unknown_invoice`: the prepare is gone —
///   `voided`, the activate row stamped, ordinary expiry effects, and the
///   `payment.bitcoin_prepare_voided` intent, in one transaction.
/// - `invoice_finalized`: paykit already activated the invoice (a lost
///   2xx) — mark `active` + `paykit_request_state='pending'` and let the
///   ordinary expiry for a pending bitcoin order run next tick (paykit's
///   `expires_at` equals the hold deadline, so the invoice expires too).
/// - unreachable: the activate row's lease is released for the next tick;
///   past `hold_expires_at + 30 min` the marketplace voids locally and
///   defers the remote void to a `paykit.void` outbox row (alert
///   `paykit_unreachable_at_void`).
async fn expire_preparing_order(
    state: &AppState,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let pool = &state.pool;
    // Take the activate row's lease first: a delivery in flight settles the
    // invoice itself, so this order is skipped this tick.
    let leased: Option<(i64,)> = sqlx::query_as(
        "UPDATE outbox SET lease_until = $2 WHERE id = (\
             SELECT id FROM outbox WHERE kind = 'paykit.activate' \
             AND payload->>'order_id' = $3 AND delivered_at IS NULL \
             ORDER BY id LIMIT 1\
         ) AND delivered_at IS NULL AND (lease_until IS NULL OR lease_until <= $1) \
         RETURNING id",
    )
    .bind(now)
    .bind(now + chrono::Duration::seconds(state.config.worker_lease_seconds))
    .bind(order_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some((outbox_row_id,)) = leased else {
        return Ok(false);
    };
    #[derive(sqlx::FromRow)]
    struct PreparingPin {
        paykit_invoice_id: Option<Uuid>,
        paykit_stack_id: Option<String>,
        paykit_stack_endpoint: Option<String>,
        hold_expires_at: Option<DateTime<Utc>>,
        payment_id: Uuid,
    }
    let pin: Option<PreparingPin> = sqlx::query_as(
        "SELECT o.paykit_invoice_id, o.paykit_stack_id, o.paykit_stack_endpoint, \
             o.hold_expires_at, p.id AS payment_id \
             FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await?;
    let Some(PreparingPin {
        paykit_invoice_id: Some(invoice_id),
        paykit_stack_id: Some(stack_id),
        paykit_stack_endpoint: Some(endpoint),
        hold_expires_at,
        payment_id,
    }) = pin
    else {
        anyhow::bail!("preparing order {order_id} is missing its persisted paykit pin");
    };
    let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    else {
        tracing::error!(
            order_id = %order_id,
            "preparing-order hold expiry on a deployment without the paykit client; skipping"
        );
        return Ok(false);
    };
    match paykit
        .void_payment_request(&endpoint, invoice_id, &stack_id, "hold_expired")
        .await
    {
        Ok(voided) => {
            tracing::info!(
                order_id = %order_id,
                invoice_state = %voided.state,
                "voided a preparing order's invoice at hold expiry"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::PrepareExpired) | Err(PaykitCommandError::UnknownInvoice) => {
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::InvoiceFinalized) => {
            // Paykit already activated the invoice — a lost 2xx. The order
            // becomes a live pending bitcoin order; the ordinary expiry
            // path handles it from the next tick.
            let mut tx = pool.begin().await?;
            sqlx::query(
                "UPDATE orders SET paykit_activation_state = 'active', \
                 paykit_request_state = 'pending', updated_at = $2 \
                 WHERE id = $1 AND paykit_activation_state = 'preparing'",
            )
            .bind(order_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
                .bind(outbox_row_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::warn!(
                order_id = %order_id,
                "paykit had already activated the invoice (lost 2xx); order is now live-pending"
            );
            Ok(false)
        }
        Err(PaykitCommandError::StackIdentityMismatch) => {
            tracing::error!(
                order_id = %order_id,
                stack_id = %stack_id,
                endpoint = %endpoint,
                "ALERT the persisted paykit endpoint answered with a different stack identity \
                 at hold-expiry void; voiding locally"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::ActivationTotalMismatch)
        | Err(PaykitCommandError::UnexpectedRejection(_)) => {
            tracing::error!(
                order_id = %order_id,
                "ALERT paykit hold-expiry void hit an unknown contract state; voiding locally"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, false)
                .await?;
            Ok(true)
        }
        Err(PaykitCommandError::Unavailable) => {
            let grace_elapsed = hold_expires_at.is_some_and(|deadline| {
                now > deadline + chrono::Duration::seconds(PREPARING_VOID_GRACE_SECONDS)
            });
            if !grace_elapsed {
                // Release the lease; retried on the next tick.
                sqlx::query("UPDATE outbox SET lease_until = NULL WHERE id = $1")
                    .bind(outbox_row_id)
                    .execute(pool)
                    .await?;
                tracing::warn!(
                    order_id = %order_id,
                    "paykit unreachable at hold-expiry void; retrying next tick"
                );
                return Ok(false);
            }
            tracing::error!(
                order_id = %order_id,
                invoice_id = %invoice_id,
                endpoint = %endpoint,
                "ALERT paykit_unreachable_at_void: voiding locally past the 30-minute grace \
                 and deferring the remote void to a paykit.void outbox row"
            );
            void_and_expire_preparing_order(pool, order_id, payment_id, outbox_row_id, now, true)
                .await?;
            Ok(true)
        }
    }
}

/// One transaction for the `preparing → voided` hold-expiry edge: the
/// activation state flips to `voided`, the activate row is stamped
/// delivered, the ordinary expiry effects run (hold released, payment
/// expired, order cancelled), the `payment.bitcoin_prepare_voided` intent
/// is emitted, and — past the unreachability grace — a `paykit.void` row
/// is enqueued for the deferred remote void.
async fn void_and_expire_preparing_order(
    pool: &PgPool,
    order_id: Uuid,
    payment_id: Uuid,
    outbox_row_id: i64,
    now: DateTime<Utc>,
    enqueue_void_retry: bool,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let buyer_pubky: (String,) = sqlx::query_as("SELECT buyer_pubky FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
    expire_held_order(
        &mut tx,
        order_id,
        payment_id,
        "awaiting_entitlement",
        &buyer_pubky.0,
        now,
    )
    .await?;
    let (revision,): (i64,) = sqlx::query_as(
        "UPDATE orders SET revision = revision + 1, paykit_activation_state = 'voided', \
         paykit_request_state = NULL, updated_at = $2 \
         WHERE id = $1 AND paykit_activation_state = 'preparing' RETURNING revision",
    )
    .bind(order_id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    let event_id = crate::executor::insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order_id),
        revision,
        SYSTEM_ACTOR,
        "payment.bitcoin_prepare_voided",
        now,
    )
    .await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        "bitcoin_prepare_voided",
        &buyer_pubky.0,
        SYSTEM_ACTOR,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await?;
    if enqueue_void_retry {
        let pin: (Option<Uuid>, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT paykit_invoice_id, paykit_stack_id, paykit_stack_endpoint \
             FROM orders WHERE id = $1",
        )
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
        let (Some(invoice_id), Some(stack_id), Some(endpoint)) = pin else {
            anyhow::bail!("preparing order {order_id} is missing its persisted paykit pin");
        };
        sqlx::query(
            "INSERT INTO outbox (event_id, kind, payload, created_at) \
             VALUES ($1, 'paykit.void', $2, $3)",
        )
        .bind(event_id)
        .bind(serde_json::json!({
            "invoice_id": invoice_id,
            "order_id": order_id,
            "stack_id": stack_id,
            "stack_endpoint": endpoint,
            "reason": "hold_expired",
        }))
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("UPDATE outbox SET delivered_at = $2 WHERE id = $1")
        .bind(outbox_row_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// RFC3339-shaped prefix (`YYYY-MM-DDTHH:MM:SS`). Used only to over-include
/// candidates in SQL; due-ness and validity are decided in Rust so a
/// malformed `shipment` timestamp cannot `::timestamptz`-abort the batch.
const SHIPMENT_INSTANT_PREFIX: &str = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}";

fn parse_shipment_instant(raw: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw?)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

struct DeliverySweepClaim {
    claimed: u64,
    processed: u64,
}

/// One claimed `delivered` order for the auto-complete sweep, with both
/// candidate delivery instants (shipment JSON for shipped orders, the
/// handover row for pickups) coalesced by the claim query.
#[derive(Debug, sqlx::FromRow)]
struct DeliveredOrderClaim {
    id: Uuid,
    buyer_pubky: String,
    seller_pubky: String,
    fulfillment: String,
    delivered_at: Option<String>,
    handover_at: Option<DateTime<Utc>>,
}

/// Marks `shipped` orders `delivered` once `assume_days` have elapsed since
/// the ship timestamp — the `delivery_assume` server trigger on the
/// machine's shipped → delivered edge, standing in for the carrier
/// tracking feed this service deliberately does not have. The update is a
/// compare-and-swap on `state = 'shipped'`, so a double-claim (lease
/// violation or a racing pass) transitions the order exactly once; the
/// `delivery_assumed` flag tells the projection — and the buyer — the
/// delivery was assumed, not confirmed. Both participants are notified;
/// the event is attributed to the system actor, never a peer.
///
/// Claims are bounded (`batch_size` per inner transaction, up to
/// `max_batches` per call) so a backlog cannot hold row locks on every due
/// order at once. Malformed `shipped_at` values are skipped and logged by
/// order id (no payload/PII) so they cannot abort the rest of the batch.
pub async fn assume_due_deliveries(
    pool: &PgPool,
    now: DateTime<Utc>,
    assume_days: i64,
    batch_size: i64,
    max_batches: u32,
) -> anyhow::Result<u64> {
    let cutoff = now - chrono::Duration::days(assume_days);
    let cutoff_text = crate::clock::format_timestamp(cutoff);
    let mut assumed = 0u64;
    for _ in 0..max_batches {
        let pass = assume_due_deliveries_batch(pool, now, cutoff, &cutoff_text, batch_size).await?;
        assumed += pass.processed;
        if pass.claimed < batch_size as u64 {
            break;
        }
    }
    Ok(assumed)
}

async fn assume_due_deliveries_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    cutoff_text: &str,
    batch_size: i64,
) -> anyhow::Result<DeliverySweepClaim> {
    let mut tx = pool.begin().await?;
    let due: Vec<(Uuid, Value, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, shipment, buyer_pubky, seller_pubky, shipment->>'shipped_at' \
         FROM orders \
         WHERE state = 'shipped' \
         AND (shipment->>'shipped_at' IS NULL \
              OR shipment->>'shipped_at' !~ $2 \
              OR left(shipment->>'shipped_at', 19) <= left($1, 19)) \
         ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED",
    )
    .bind(cutoff_text)
    .bind(SHIPMENT_INSTANT_PREFIX)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let claimed = due.len() as u64;
    let mut processed = 0u64;
    for (order_id, shipment, buyer_pubky, seller_pubky, shipped_at) in due {
        let Some(shipped_at) = parse_shipment_instant(shipped_at.as_deref()) else {
            tracing::warn!(
                order_id = %order_id,
                "skipping shipped order with malformed shipment timestamp"
            );
            continue;
        };
        if shipped_at > cutoff {
            continue;
        }
        let mut delivered = shipment;
        delivered["state"] = serde_json::json!("delivered");
        // `delivered_at` is the server instant of the assumption, not
        // `shipped_at + assume_days`. This is a system event (no carrier
        // tracking feed, not peer-attested): the auto-complete clock must
        // start when the service marked delivery, not when the parcel
        // hypothetically arrived.
        delivered["delivered_at"] = serde_json::json!(crate::clock::format_timestamp(now));
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "shipped",
            "delivered"
        ));
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE orders SET revision = revision + 1, state = 'delivered', shipment = $2, \
             delivery_assumed = TRUE, updated_at = $3 \
             WHERE id = $1 AND state = 'shipped' RETURNING revision",
        )
        .bind(order_id)
        .bind(&delivered)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        let aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &aggregate_id,
            revision,
            SYSTEM_ACTOR,
            "fulfillment.delivered",
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivery_assumed",
            &buyer_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        insert_notification_intent(
            &mut tx,
            event_id,
            "order_delivered",
            &seller_pubky,
            SYSTEM_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
        tracing::info!(order_id = %order_id, "assumed delivery on server time");
        processed += 1;
    }
    tx.commit().await?;
    Ok(DeliverySweepClaim { claimed, processed })
}

/// Completes `delivered` orders once `auto_complete_days` have elapsed
/// since the delivery timestamp — the `order_auto_complete` server trigger
/// on the machine's delivered → completed edge. An open return or cancel
/// request blocks completion by construction: `return_requested`,
/// `return_approved`, `return_received`, and `cancel_requested` are their
/// own order states, so such an order never matches the `state =
/// 'delivered'` claim or the compare-and-swap. The event is attributed to
/// the system actor, never a peer.
pub async fn complete_due_delivered_orders(
    pool: &PgPool,
    now: DateTime<Utc>,
    auto_complete_days: i64,
    batch_size: i64,
    max_batches: u32,
) -> anyhow::Result<u64> {
    let cutoff = now - chrono::Duration::days(auto_complete_days);
    let cutoff_text = crate::clock::format_timestamp(cutoff);
    let mut completed = 0u64;
    for _ in 0..max_batches {
        let pass = complete_due_delivered_orders_batch(pool, now, cutoff, &cutoff_text, batch_size)
            .await?;
        completed += pass.processed;
        if pass.claimed < batch_size as u64 {
            break;
        }
    }
    Ok(completed)
}

async fn complete_due_delivered_orders_batch(
    pool: &PgPool,
    now: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    cutoff_text: &str,
    batch_size: i64,
) -> anyhow::Result<DeliverySweepClaim> {
    let mut tx = pool.begin().await?;
    // The delivery instant COALESCES the two sources (§A6): the handover
    // record's server instant for pickup orders, the shipment
    // `delivered_at` for shipped ones. The join locks `FOR UPDATE OF o`
    // (orders only — never the handover row), so the coalescing read cannot
    // deadlock against concurrent order writers.
    let due: Vec<DeliveredOrderClaim> = sqlx::query_as(
        "SELECT o.id, o.buyer_pubky, o.seller_pubky, o.fulfillment, \
                    o.shipment->>'delivered_at' AS delivered_at, \
                    h.confirmed_at AS handover_at \
             FROM orders o LEFT JOIN pickup_handovers h ON h.order_id = o.id \
             WHERE o.state = 'delivered' \
             AND ( \
               (o.fulfillment = 'pickup' AND (h.confirmed_at IS NULL OR h.confirmed_at <= $1)) \
               OR (o.fulfillment <> 'pickup' AND ( \
                    o.shipment->>'delivered_at' IS NULL \
                    OR o.shipment->>'delivered_at' !~ $3 \
                    OR left(o.shipment->>'delivered_at', 19) <= left($2, 19))) \
             ) \
             ORDER BY o.id LIMIT $4 FOR UPDATE OF o SKIP LOCKED",
    )
    .bind(cutoff)
    .bind(cutoff_text)
    .bind(SHIPMENT_INSTANT_PREFIX)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let claimed = due.len() as u64;
    let mut processed = 0u64;
    for claim in due {
        let DeliveredOrderClaim {
            id: order_id,
            buyer_pubky,
            seller_pubky,
            fulfillment,
            delivered_at: shipment_delivered_at,
            handover_at,
        } = claim;
        if fulfillment == "pickup" {
            // A delivered pickup order always carries its handover row
            // (written in the confirm transaction). A missing row is an
            // anomaly to skip — never the warn-and-skip-forever loop the
            // shipment-only sweep would have emitted for pickup rows (§A6).
            let Some(delivered_at) = handover_at else {
                continue;
            };
            if delivered_at > cutoff {
                continue;
            }
        } else {
            let Some(delivered_at) = parse_shipment_instant(shipment_delivered_at.as_deref())
            else {
                tracing::warn!(
                    order_id = %order_id,
                    "skipping delivered order with malformed shipment timestamp"
                );
                continue;
            };
            if delivered_at > cutoff {
                continue;
            }
        }
        debug_assert!(marketplace_domain::state_machines::can_transition(
            &marketplace_domain::state_machines::order_machine(),
            "delivered",
            "completed"
        ));
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE orders SET revision = revision + 1, state = 'completed', updated_at = $2 \
             WHERE id = $1 AND state = 'delivered' RETURNING revision",
        )
        .bind(order_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((revision,)) = updated else {
            continue;
        };
        let aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &aggregate_id,
            revision,
            SYSTEM_ACTOR,
            "order.completed",
            now,
        )
        .await?;
        for recipient in [&buyer_pubky, &seller_pubky] {
            insert_notification_intent(
                &mut tx,
                event_id,
                "order_completed",
                recipient,
                SYSTEM_ACTOR,
                &aggregate_id,
                None,
                now,
            )
            .await?;
        }
        tracing::info!(order_id = %order_id, "auto-completed order on server time");
        processed += 1;
    }
    tx.commit().await?;
    Ok(DeliverySweepClaim { claimed, processed })
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct WorkerSummary {
    pub reservations_expired: u64,
    pub drops_transitioned: u64,
    pub offers_expired: u64,
    pub auctions_closed: u64,
    pub outbox_delivered: u64,
    pub locks_completions_applied: u64,
    pub paykit_payments_applied: u64,
    pub payment_windows_expired: u64,
    pub stat_attestations_signed: u64,
    pub deliveries_assumed: u64,
    pub orders_auto_completed: u64,
    pub pickup_rows_resealed: u64,
    pub pickup_snapshots_purged: u64,
    pub pickup_versions_purged: u64,
}

/// One worker pass: for each task, take the lease, drain, release. Tasks
/// whose lease is held by another live instance are skipped. A drain error
/// propagates only after the release, so a failing task never squats on
/// its lease until expiry. Panics are NOT caught (there is no
/// catch_unwind): a panicking task takes the pass down and leaves its
/// lease to expire on its own.
pub async fn run_once(
    state: &AppState,
    holder: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<WorkerSummary> {
    let lease_seconds = state.config.worker_lease_seconds;
    let mut summary = WorkerSummary::default();

    if let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    {
        state
            .payment_availability
            .rail_health(
                state.clock.as_ref(),
                state.config.paykit_poll_seconds,
                state.config.paykit_rail_stale_seconds,
                || async { paykit.rail_health().await },
            )
            .await;
        state
            .payment_availability
            .evict_stale(now, state.config.paykit_rail_stale_seconds)
            .await;
    }

    if try_acquire_lease(
        &state.pool,
        TASK_RESERVATION_EXPIRY,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = expiry::expire_due_reservations(&state.pool, now).await;
        release_lease(&state.pool, TASK_RESERVATION_EXPIRY, holder, now).await?;
        summary.reservations_expired = result?;
    }
    if try_acquire_lease(
        &state.pool,
        TASK_DROP_TRANSITIONS,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = transition_due_drops(&state.pool, now).await;
        release_lease(&state.pool, TASK_DROP_TRANSITIONS, holder, now).await?;
        summary.drops_transitioned = result?;
    }
    if try_acquire_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now, lease_seconds).await? {
        let result = expire_due_offers(&state.pool, now).await;
        release_lease(&state.pool, TASK_OFFER_EXPIRY, holder, now).await?;
        summary.offers_expired = result?;
    }
    if try_acquire_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now, lease_seconds).await? {
        let result = close_due_auctions(&state.pool, now).await;
        release_lease(&state.pool, TASK_AUCTION_CLOSE, holder, now).await?;
        summary.auctions_closed = result?;
    }
    if try_acquire_lease(&state.pool, TASK_OUTBOX, holder, now, lease_seconds).await? {
        let paykit = state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        let result = drain_outbox(&state.pool, paykit, now, lease_seconds).await;
        release_lease(&state.pool, TASK_OUTBOX, holder, now).await?;
        summary.outbox_delivered = result?;
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
            let result = verify_due_locks_lifecycles(state, locks, now).await;
            release_lease(&state.pool, TASK_LOCKS_VERIFICATION, holder, now).await?;
            summary.locks_completions_applied = result?;
        }
    }
    // Paykit verification runs only when the deployment carries the signed
    // Paykit client (fail closed).
    if let Some(paykit) = state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
    {
        if try_acquire_lease(
            &state.pool,
            TASK_PAYKIT_VERIFICATION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = verify_due_paykit_payments(state, paykit, now).await;
            release_lease(&state.pool, TASK_PAYKIT_VERIFICATION, holder, now).await?;
            summary.paykit_payments_applied = result?;
        }
    }
    if try_acquire_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now, lease_seconds).await? {
        let result = expire_due_payment_windows(state, now).await;
        release_lease(&state.pool, TASK_PAYMENT_WINDOW, holder, now).await?;
        summary.payment_windows_expired = result?;
    }
    // Post-purchase liveness is server time (no carrier tracking feed):
    // assume delivery after DELIVERY_ASSUME_DAYS, then auto-complete after
    // AUTO_COMPLETE_DAYS unless a return/cancel request is open.
    if try_acquire_lease(
        &state.pool,
        TASK_DELIVERY_AUTOCOMPLETE,
        holder,
        now,
        lease_seconds,
    )
    .await?
    {
        let result = async {
            summary.deliveries_assumed = assume_due_deliveries(
                &state.pool,
                now,
                state.config.delivery_assume_days,
                state.config.delivery_sweep_batch_size,
                DELIVERY_SWEEP_MAX_BATCHES,
            )
            .await?;
            summary.orders_auto_completed = complete_due_delivered_orders(
                &state.pool,
                now,
                state.config.auto_complete_days,
                state.config.delivery_sweep_batch_size,
                DELIVERY_SWEEP_MAX_BATCHES,
            )
            .await?;
            anyhow::Ok(())
        }
        .await;
        release_lease(&state.pool, TASK_DELIVERY_AUTOCOMPLETE, holder, now).await?;
        result?;
    }
    // Pickup maintenance runs only when the sealing keys are configured:
    // the retention purge of pinned versions/snapshots whose referencing
    // orders went terminal, and — while a previous key is configured — the
    // dual-key re-seal job across BOTH sealed families (§A1/§A3).
    if let Some(pickup) = &state.pickup {
        if try_acquire_lease(
            &state.pool,
            TASK_PICKUP_RETENTION,
            holder,
            now,
            lease_seconds,
        )
        .await?
        {
            let result = crate::pickup::purge_terminal_pickup_retention(
                &state.pool,
                now,
                state.config.pickup_dispute_retention_days,
            )
            .await;
            release_lease(&state.pool, TASK_PICKUP_RETENTION, holder, now).await?;
            let (snapshots_purged, versions_purged) = result?;
            summary.pickup_snapshots_purged = snapshots_purged;
            summary.pickup_versions_purged = versions_purged;
        }
        if pickup.has_previous()
            && try_acquire_lease(&state.pool, TASK_PICKUP_RESEAL, holder, now, lease_seconds)
                .await?
        {
            let result = crate::pickup::reseal_previous_key_batch(&state.pool, pickup, now).await;
            release_lease(&state.pool, TASK_PICKUP_RESEAL, holder, now).await?;
            // A failed re-seal pass (a permanently unopenable row) is a
            // PER-TASK failure: it is logged with its unopenable count and
            // the tick continues — it must not skip the remaining tasks or
            // discard the tick summary on every pass. The pass retries on
            // the next tick; only the OTHER tasks' failures fail run_once.
            match result {
                Ok(progress) => {
                    summary.pickup_rows_resealed =
                        progress.details_resealed + progress.snapshots_resealed;
                    if progress.remaining_under_previous == 0 && summary.pickup_rows_resealed > 0 {
                        tracing::info!(
                            resealed = summary.pickup_rows_resealed,
                            "pickup key rotation complete: zero rows remain under the previous \
                             key across both sealed families"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "pickup re-seal pass failed; continuing with the remaining worker tasks"
                    );
                }
            }
        }
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
            let result = generate_due_stat_attestations(&state.pool, attestor, now).await;
            release_lease(&state.pool, TASK_STAT_ATTESTATIONS, holder, now).await?;
            summary.stat_attestations_signed = result?;
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
    cancelled: Option<bool>,
    refunded: Option<bool>,
    /// The order's terminal cancel was a buyer-protection exit (§A3):
    /// `order.cancelled_terms_change` excludes the WHOLE order from
    /// `terminated_badly`, including any `refund.recorded_external` leg.
    terms_change_cancelled: Option<bool>,
    /// The order completed on server time (the `order_auto_complete`
    /// sweep), as opposed to a buyer review.
    auto_completed: Option<bool>,
    /// The buyer opened a cancel or return request on the order.
    buyer_disputed: Option<bool>,
    fulfillment: String,
    /// Who confirmed the pickup handover (`buyer` | `seller`), when one
    /// exists; NULL for shipped orders and unconfirmed pickups.
    handover_confirmed_by: Option<String>,
}

/// Computes and signs the weekly per-seller stat attestations (ratified D3:
/// median time-to-ship, completion rate — banded and
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
               BOOL_OR(e.kind = 'order.cancelled') AS cancelled, \
               BOOL_OR(e.kind = 'refund.recorded_external') AS refunded, \
               BOOL_OR(e.kind = 'order.cancelled_terms_change') AS terms_change_cancelled, \
               BOOL_OR(e.kind = 'order.completed') AS auto_completed, \
               BOOL_OR(e.kind IN ('order.cancel_requested', 'order.cancelled', \
                                  'order.cancelled_terms_change', 'return.requested')) \
                 AS buyer_disputed, \
               o.fulfillment AS fulfillment, \
               (SELECT h.confirmed_by FROM pickup_handovers h WHERE h.order_id = o.id) \
                 AS handover_confirmed_by \
             FROM orders o JOIN events e ON e.aggregate_id = 'order:' || o.id::text \
             WHERE o.seller_pubky = $1 AND e.occurred_at >= $2 AND e.occurred_at <= $3 \
             GROUP BY o.id",
        )
        .bind(&seller_pubky)
        .bind(window_start)
        .bind(now)
        .fetch_all(pool)
        .await?;

        // The confirming-actor rule (§A6): a pickup completion counts only
        // when the BUYER confirmed the handover, or when the order
        // auto-completed with no buyer cancel or return in the window. A
        // seller-unilateral confirm is never reputation-positive on its
        // own — the seller is paid at payment time, so a self-confirm that
        // also minted completion reputation would pay a fraudulent seller
        // twice. Shipped orders count on delivery, as today.
        let completed = per_order
            .iter()
            .filter(|order| {
                if order.delivered_at.is_none() {
                    return false;
                }
                if order.fulfillment != "pickup" {
                    return true;
                }
                order.handover_confirmed_by.as_deref() == Some("buyer")
                    || (order.auto_completed.unwrap_or(false)
                        && !order.buyer_disputed.unwrap_or(false))
            })
            .count() as i64;
        if completed < 1 {
            continue;
        }
        // `order.cancelled_terms_change` excludes the WHOLE order from
        // `terminated_badly` (§A3): a buyer can never ding the seller's
        // completion rate by exercising a buyer-protection exit, even when
        // a refund was recorded on the same order.
        let terminated_badly = per_order
            .iter()
            .filter(|order| {
                (order.cancelled.unwrap_or(false) || order.refunded.unwrap_or(false))
                    && !order.terms_change_cancelled.unwrap_or(false)
            })
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
                            drops_transitioned = summary.drops_transitioned,
                            offers_expired = summary.offers_expired,
                            auctions_closed = summary.auctions_closed,
                            outbox_delivered = summary.outbox_delivered,
                            locks_completions_applied = summary.locks_completions_applied,
                            paykit_payments_applied = summary.paykit_payments_applied,
                            payment_windows_expired = summary.payment_windows_expired,
                            stat_attestations_signed = summary.stat_attestations_signed,
                            deliveries_assumed = summary.deliveries_assumed,
                            orders_auto_completed = summary.orders_auto_completed,
                            pickup_rows_resealed = summary.pickup_rows_resealed,
                            pickup_snapshots_purged = summary.pickup_snapshots_purged,
                            pickup_versions_purged = summary.pickup_versions_purged,
                            "worker pass completed"
                        );
                    }
                }
                Err(error) => tracing::error!(error = %error, "worker pass failed"),
            }
        }
    })
}
