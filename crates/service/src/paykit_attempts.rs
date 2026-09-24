//! Released Paykit attempts (migration 0042).
//!
//! An order carries one set of Paykit pins: the attempt it is bound to. When
//! `void_prepare_effects` releases that attempt the order becomes unbound,
//! and a re-bind (or a fiat bind) overwrites the pins. paykit-server keeps
//! answering status for the released invoice by `(creator, reference)`, and
//! the invoice can still be payable — an activation that committed at
//! paykit but whose response was lost. Every released attempt is therefore
//! recorded in `paykit_superseded_attempts` and polled through its
//! observation tail:
//!
//! - confirmed money routes to the late-money path, with the paid attempt
//!   restored as the order's attempt of record (so a later resolution names
//!   the invoice that was paid) and the order's other attempt released in
//!   its place, still watched;
//! - money the order can no longer take (its payment already settled or
//!   under review, or the order now bound to a fiat method) is held as
//!   `needs_review` and alerted;
//! - a detection is recorded and keeps the attempt watched past its tail;
//! - no money by the end of the tail closes the attempt `closed_unpaid`.

use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::bitcoin_review::{apply_late_money, observation_json};
use crate::handlers::fetch_order_for_update;
use crate::model::PaymentRow;
use crate::payments::{
    legacy_order_reference, PaykitObservation, PaykitStatusOutcome, PaykitStatusSource,
};
use crate::queries::PAYMENT_COLUMNS;
use crate::AppState;

/// How long after its expiry a released attempt stays watched with no
/// detection: the same observation tail the order poller keeps for an
/// expired pending request.
const OBSERVATION_TAIL_HOURS: i64 = 24;
const BATCH_SIZE: i64 = 50;

/// Records the order's current Paykit attempt as released, from the pins on
/// the order row. Idempotent per `(order, invoice)`. A row without complete
/// pins (no prepared invoice) records nothing.
pub(crate) async fn record_released_attempt(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO paykit_superseded_attempts (order_id, invoice_id, reference, stack_id, \
         stack_endpoint, total_sats, expires_at, allocation_mode, address_fingerprint, \
         released_at) \
         SELECT id, paykit_invoice_id, paykit_request_reference, paykit_stack_id, \
         paykit_stack_endpoint, paykit_total_sats, paykit_expires_at, paykit_allocation_mode, \
         paykit_address_fingerprint, $2 FROM orders \
         WHERE id = $1 AND paykit_invoice_id IS NOT NULL AND paykit_stack_id IS NOT NULL \
         AND paykit_stack_endpoint IS NOT NULL AND paykit_total_sats > 0 \
         ON CONFLICT (order_id, invoice_id) DO NOTHING",
    )
    .bind(order_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct WatchedAttempt {
    order_id: Uuid,
    invoice_id: Uuid,
    reference: Option<String>,
    stack_id: String,
    stack_endpoint: String,
    total_sats: i64,
    expires_at: Option<DateTime<Utc>>,
    allocation_mode: Option<String>,
    address_fingerprint: Option<String>,
    released_at: DateTime<Utc>,
    detected_at: Option<DateTime<Utc>>,
    seller_pubky: String,
}

impl WatchedAttempt {
    fn reference(&self) -> String {
        self.reference
            .clone()
            .unwrap_or_else(|| legacy_order_reference(self.order_id))
    }
}

/// Claims watched attempts due for a status poll by stamping
/// `last_checked_at`, the only pre-effect write. An attempt the order row
/// itself tracks again (its invoice is the order's current one with a live
/// request state) belongs to the order poller and is skipped.
async fn claim_due_attempts(
    pool: &PgPool,
    now: DateTime<Utc>,
    poll_seconds: i64,
) -> Result<Vec<WatchedAttempt>, sqlx::Error> {
    sqlx::query_as(
        "WITH due AS (\
             SELECT h.order_id, h.invoice_id FROM paykit_superseded_attempts h \
             JOIN orders o ON o.id = h.order_id \
             WHERE h.state = 'watching' \
             AND (h.last_checked_at IS NULL OR h.last_checked_at <= $2) \
             AND NOT (o.paykit_invoice_id IS NOT DISTINCT FROM h.invoice_id \
                      AND o.paykit_request_state IS NOT NULL) \
             ORDER BY h.last_checked_at ASC NULLS FIRST LIMIT $3 \
             FOR UPDATE OF h SKIP LOCKED\
         ), claimed AS (\
             UPDATE paykit_superseded_attempts h SET last_checked_at = $1 FROM due \
             WHERE h.order_id = due.order_id AND h.invoice_id = due.invoice_id \
             RETURNING h.*\
         ) SELECT c.order_id, c.invoice_id, c.reference, c.stack_id, c.stack_endpoint, \
           c.total_sats, c.expires_at, c.allocation_mode, c.address_fingerprint, \
           c.released_at, c.detected_at, o.seller_pubky \
           FROM claimed c JOIN orders o ON o.id = c.order_id",
    )
    .bind(now)
    .bind(now - chrono::Duration::seconds(poll_seconds))
    .bind(BATCH_SIZE)
    .fetch_all(pool)
    .await
}

/// One pass over released attempts: polls each due attempt's status and
/// applies it. Returns the number of attempts whose money was routed. A
/// per-attempt failure is logged and the pass continues.
pub async fn verify_due_released_attempts(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let claimed = claim_due_attempts(&state.pool, now, state.config.paykit_poll_seconds).await?;
    let mut routed = 0u64;
    for attempt in &claimed {
        match apply_attempt_status(state, source, attempt, now).await {
            Ok(true) => routed += 1,
            Ok(false) => {}
            Err(error) => tracing::error!(
                order_id = %attempt.order_id,
                invoice_id = %attempt.invoice_id,
                error = %error,
                "released paykit attempt verification failed; continuing the batch"
            ),
        }
    }
    Ok(routed)
}

async fn apply_attempt_status(
    state: &AppState,
    source: &dyn PaykitStatusSource,
    attempt: &WatchedAttempt,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    match source
        .status(&attempt.seller_pubky, &attempt.reference())
        .await
    {
        PaykitStatusOutcome::Confirmed {
            amount_matched,
            facts,
        } => {
            route_released_settlement(state, attempt, amount_matched, &facts.observation, now).await
        }
        PaykitStatusOutcome::Detected { facts } => {
            sqlx::query(
                "UPDATE paykit_superseded_attempts SET detected_at = COALESCE(detected_at, $3), \
                 observation = $4 \
                 WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching'",
            )
            .bind(attempt.order_id)
            .bind(attempt.invoice_id)
            .bind(now)
            .bind(observation_json(
                "detected",
                true,
                &facts.observation,
                now,
                false,
            ))
            .execute(&state.pool)
            .await?;
            tracing::warn!(
                order_id = %attempt.order_id,
                invoice_id = %attempt.invoice_id,
                "ALERT money detected on a released paykit attempt; watching for confirmation"
            );
            Ok(false)
        }
        PaykitStatusOutcome::Undetected | PaykitStatusOutcome::NotFound => {
            let tail_ends = attempt.expires_at.unwrap_or(attempt.released_at)
                + chrono::Duration::hours(OBSERVATION_TAIL_HOURS);
            if attempt.detected_at.is_none() && now >= tail_ends {
                sqlx::query(
                    "UPDATE paykit_superseded_attempts SET state = 'closed_unpaid', \
                     closed_at = $3 \
                     WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching' \
                     AND detected_at IS NULL",
                )
                .bind(attempt.order_id)
                .bind(attempt.invoice_id)
                .bind(now)
                .execute(&state.pool)
                .await?;
            }
            Ok(false)
        }
        PaykitStatusOutcome::Unavailable => Ok(false),
    }
}

/// Confirmed money on a released attempt, in one transaction under the
/// payment-then-order lock order. The paid attempt becomes the order's
/// attempt of record again and the settlement takes the late-money path:
/// never an ordinary confirmation, so the order's other attempt cannot
/// complete it on its own.
async fn route_released_settlement(
    state: &AppState,
    attempt: &WatchedAttempt,
    amount_matched: bool,
    observation: &PaykitObservation,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let mut tx = state.pool.begin().await?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1 FOR UPDATE"
    ))
    .bind(attempt.order_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(payment) = payment else {
        anyhow::bail!(
            "released paykit attempt names order {} without a payment",
            attempt.order_id
        );
    };
    let Some(order) = fetch_order_for_update(&mut tx, attempt.order_id).await? else {
        anyhow::bail!("released paykit attempt names a missing order");
    };
    let still_watching: Option<(String,)> = sqlx::query_as(
        "SELECT state FROM paykit_superseded_attempts \
         WHERE order_id = $1 AND invoice_id = $2 FOR UPDATE",
    )
    .bind(attempt.order_id)
    .bind(attempt.invoice_id)
    .fetch_optional(&mut *tx)
    .await?;
    if still_watching.as_ref().map(|(state,)| state.as_str()) != Some("watching") {
        tx.rollback().await?;
        return Ok(false);
    }
    let current_invoice = order.paykit_invoice_id;
    if current_invoice == Some(attempt.invoice_id) && order.paykit_request_state.is_some() {
        // The order poller tracks this invoice again.
        tx.rollback().await?;
        return Ok(false);
    }
    let frozen = observation_json("confirmed", amount_matched, observation, now, false);

    let fiat_bound = order
        .payment_method
        .as_deref()
        .is_some_and(|method| method != "bitcoin");
    if fiat_bound || !matches!(payment.state.as_str(), "awaiting_entitlement" | "expired") {
        close_attempt(&mut tx, attempt, "needs_review", &frozen, now).await?;
        tx.commit().await?;
        tracing::error!(
            order_id = %attempt.order_id,
            invoice_id = %attempt.invoice_id,
            payment_state = %payment.state,
            payment_method = ?order.payment_method,
            code = "paykit_released_attempt_paid_after_settlement",
            "ALERT money confirmed on a released paykit attempt of an order that is settled, \
             under review, or bound to another method; held for review"
        );
        return Ok(true);
    }

    // The order's other attempt, if it has one, is released in the paid
    // attempt's place and stays watched. A still-preparing one is never
    // activated: its outbox row is stamped and paykit's prepare reaper
    // voids the unpublished invoice.
    if current_invoice.is_some() && current_invoice != Some(attempt.invoice_id) {
        record_released_attempt(&mut tx, attempt.order_id, now).await?;
        if order.paykit_activation_state.as_deref() == Some("preparing") {
            crate::handlers::cancellation::void_preparing_request(&mut tx, order.id, now).await?;
        }
    }
    sqlx::query(
        "UPDATE orders SET payment_method = 'bitcoin', paykit_invoice_id = $2, \
         paykit_request_reference = $3, paykit_stack_id = $4, paykit_stack_endpoint = $5, \
         paykit_total_sats = $6, paykit_expires_at = $7, paykit_allocation_mode = $8, \
         paykit_address_fingerprint = $9, paykit_activation_state = 'active', \
         paykit_request_state = 'confirmed', paykit_observation = $10, \
         paykit_observed_sats = $11, paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, updated_at = $12 WHERE id = $1",
    )
    .bind(order.id)
    .bind(attempt.invoice_id)
    .bind(attempt.reference())
    .bind(&attempt.stack_id)
    .bind(&attempt.stack_endpoint)
    .bind(attempt.total_sats)
    .bind(attempt.expires_at)
    .bind(&attempt.allocation_mode)
    .bind(&attempt.address_fingerprint)
    .bind(&frozen)
    .bind(observation.observed_sats.map(i64::try_from).transpose()?)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE payments SET revision = revision + 1, adapter = 'paykit', amount_minor = $2, \
         currency = 'SAT', exponent = 0, updated_at = $3 WHERE id = $1",
    )
    .bind(payment.id)
    .bind(attempt.total_sats)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    let payment: PaymentRow = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1"
    ))
    .bind(payment.id)
    .fetch_one(&mut *tx)
    .await?;
    let Some(order) = fetch_order_for_update(&mut tx, attempt.order_id).await? else {
        anyhow::bail!("order vanished while routing a released paykit attempt");
    };

    if !amount_matched {
        // Not the amount the released invoice asked for: a human decides.
        let (revision,): (i64,) = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             review_reason = 'amount_mismatch', manual_review_entered_at = $2, updated_at = $2 \
             WHERE id = $1 RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        crate::executor::insert_event(
            &mut tx,
            Uuid::new_v4(),
            &ids::payment_aggregate_id(payment.id),
            revision,
            &order.buyer_pubky,
            "payment.manual_review",
            now,
        )
        .await?;
        close_attempt(&mut tx, attempt, "late_money", &frozen, now).await?;
        tx.commit().await?;
        tracing::warn!(
            order_id = %attempt.order_id,
            invoice_id = %attempt.invoice_id,
            "released paykit attempt settled with a different amount; routed to manual review"
        );
        return Ok(true);
    }

    match apply_late_money(
        &mut tx,
        state.pickup.as_deref(),
        &payment,
        &order,
        attempt.order_id,
        &order.buyer_pubky,
        now,
    )
    .await
    {
        Ok(outcome) => {
            close_attempt(&mut tx, attempt, "late_money", &frozen, now).await?;
            tx.commit().await?;
            tracing::warn!(
                order_id = %attempt.order_id,
                invoice_id = %attempt.invoice_id,
                ?outcome,
                "applied late-money fork for a settlement on a released paykit attempt"
            );
            Ok(true)
        }
        Err(failure) => {
            tx.rollback().await?;
            anyhow::bail!(
                "released paykit attempt settlement for {} failed: {:?}",
                attempt.order_id,
                std::mem::discriminant(&failure)
            );
        }
    }
}

async fn close_attempt(
    tx: &mut Transaction<'_, Postgres>,
    attempt: &WatchedAttempt,
    state: &str,
    observation: &serde_json::Value,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE paykit_superseded_attempts SET state = $3, observation = $4, closed_at = $5 \
         WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(attempt.order_id)
    .bind(attempt.invoice_id)
    .bind(state)
    .bind(observation)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
