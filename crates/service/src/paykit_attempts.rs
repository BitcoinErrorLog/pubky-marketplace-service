//! Released Paykit attempts (migration 0042).
//!
//! An order carries one set of Paykit pins: the attempt it is bound to. An
//! attempt is released when the activation worker voids the bind, a buyer
//! cancels a preparing request, or a preparing order's hold expires; the
//! order stops polling it, and a re-bind (or a fiat bind) later overwrites
//! the pins. paykit-server keeps answering status for the released invoice
//! by `(creator, reference)`, and the invoice can still be payable — an
//! activation that committed at paykit but whose response was lost. Every
//! released attempt is therefore recorded in `paykit_superseded_attempts`,
//! with its pins and its bind-time quote, and polled with backoff:
//!
//! - confirmed money routes to the late-money path, with the paid attempt
//!   (pins and quote) restored as the order's attempt of record, so a later
//!   resolution names the invoice that was paid, and the order's other
//!   attempt released in its place, still watched;
//! - money the order can no longer take (its payment settled or under
//!   review, or the order on another rail: a fiat method, or a payment
//!   managed by Locks or any adapter but paykit or sandbox) goes to
//!   `needs_review`, alerted, with the order and payment untouched;
//! - a detection keeps the attempt watched past its tail, up to
//!   [`DETECTION_WINDOW_DAYS`] by our clock, after which every poll pass
//!   moves it to `needs_review` whether or not paykit answers;
//! - no money by the end of the tail closes the attempt `closed_unpaid`.
//!
//! Checks start at the paykit poll interval and double per check up to
//! [`MAX_CHECK_INTERVAL_SECONDS`]. An operator records the outcome of a
//! `needs_review` attempt with [`resolve_needs_review`] (the
//! `paykit-attempts-admin` binary; runbook: `docs/paykit-released-attempts.md`).

use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use serde::Serialize;
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

/// How long after its expiry a released attempt with no detection stays
/// watched: the observation tail the order poller keeps for an expired
/// pending request.
const OBSERVATION_TAIL_HOURS: i64 = 24;
/// How long a detection that never confirms stays watched before it goes to
/// an operator.
pub const DETECTION_WINDOW_DAYS: i64 = 7;
/// The longest gap between two checks of one released attempt.
pub const MAX_CHECK_INTERVAL_SECONDS: i64 = 3600;
const BATCH_SIZE: i64 = 50;

/// Records the order's current Paykit attempt as released, from the pins
/// and quote on the order row. Idempotent per `(order, invoice)`. A row
/// without complete pins (no prepared invoice) records nothing.
pub(crate) async fn record_released_attempt(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO paykit_superseded_attempts (order_id, invoice_id, reference, stack_id, \
         stack_endpoint, total_sats, expires_at, prepare_expires_at, allocation_mode, \
         address_fingerprint, bitcoin_quote_rate, bitcoin_quote_source, \
         bitcoin_quote_fetched_at, bitcoin_quoted_sats, bitcoin_quote_expires_at, \
         bitcoin_quote_currency, bitcoin_quote_exponent, bitcoin_quote_spread_bps, \
         released_at) \
         SELECT id, paykit_invoice_id, paykit_request_reference, paykit_stack_id, \
         paykit_stack_endpoint, paykit_total_sats, paykit_expires_at, \
         paykit_prepare_expires_at, paykit_allocation_mode, paykit_address_fingerprint, \
         bitcoin_quote_rate, bitcoin_quote_source, bitcoin_quote_fetched_at, \
         bitcoin_quoted_sats, bitcoin_quote_expires_at, bitcoin_quote_currency, \
         bitcoin_quote_exponent, bitcoin_quote_spread_bps, $2 FROM orders \
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
    expires_at: Option<DateTime<Utc>>,
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

/// Claims watched attempts due for a status poll. The claim is the only
/// pre-effect write: it stamps `last_checked_at`, counts the check, and
/// schedules the next one at the poll interval doubled per earlier check,
/// capped at [`MAX_CHECK_INTERVAL_SECONDS`]. An attempt the order row
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
             AND (h.next_check_at IS NULL OR h.next_check_at <= $1) \
             AND NOT (o.paykit_invoice_id IS NOT DISTINCT FROM h.invoice_id \
                      AND o.paykit_request_state IS NOT NULL) \
             ORDER BY h.next_check_at ASC NULLS FIRST LIMIT $2 \
             FOR UPDATE OF h SKIP LOCKED\
         ), claimed AS (\
             UPDATE paykit_superseded_attempts h SET last_checked_at = $1, \
             check_count = h.check_count + 1, \
             next_check_at = $1 + LEAST(\
                 make_interval(secs => $3::double precision \
                     * power(2, LEAST(h.check_count, 20))), \
                 make_interval(secs => $4::double precision)) \
             FROM due \
             WHERE h.order_id = due.order_id AND h.invoice_id = due.invoice_id \
             RETURNING h.order_id, h.invoice_id, h.reference, h.expires_at, h.released_at, \
             h.detected_at\
         ) SELECT c.order_id, c.invoice_id, c.reference, c.expires_at, c.released_at, \
           c.detected_at, o.seller_pubky \
           FROM claimed c JOIN orders o ON o.id = c.order_id",
    )
    .bind(now)
    .bind(BATCH_SIZE)
    .bind(poll_seconds.max(1) as f64)
    .bind(MAX_CHECK_INTERVAL_SECONDS as f64)
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
    escalate_stale_detections(&state.pool, now).await?;
    Ok(routed)
}

/// Moves every watched attempt whose detection is older than
/// [`DETECTION_WINDOW_DAYS`] to `needs_review`, by elapsed time alone: it
/// neither polls paykit nor waits for a due check, so an unavailable status
/// endpoint cannot hold an unconfirmed detection open. The frozen
/// observation stays for the operator.
async fn escalate_stale_detections(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let escalated: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "UPDATE paykit_superseded_attempts SET state = 'needs_review', \
         review_reason = 'detected_unconfirmed', closed_at = $1 \
         WHERE state = 'watching' AND detected_at <= $2 \
         RETURNING order_id, invoice_id",
    )
    .bind(now)
    .bind(now - chrono::Duration::days(DETECTION_WINDOW_DAYS))
    .fetch_all(pool)
    .await?;
    for (order_id, invoice_id) in &escalated {
        tracing::error!(
            order_id = %order_id,
            invoice_id = %invoice_id,
            code = "paykit_released_attempt_detected_unconfirmed",
            "ALERT money detected on a released paykit attempt never confirmed; held for review"
        );
    }
    Ok(escalated.len() as u64)
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
            let observation = observation_json("detected", true, &facts.observation, now, false);
            let first = sqlx::query(
                "UPDATE paykit_superseded_attempts SET detected_at = $3, observation = $4 \
                 WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching' \
                 AND detected_at IS NULL",
            )
            .bind(attempt.order_id)
            .bind(attempt.invoice_id)
            .bind(now)
            .bind(&observation)
            .execute(&state.pool)
            .await?;
            if first.rows_affected() == 1 {
                tracing::warn!(
                    order_id = %attempt.order_id,
                    invoice_id = %attempt.invoice_id,
                    "ALERT money detected on a released paykit attempt; watching for confirmation"
                );
            } else {
                sqlx::query(
                    "UPDATE paykit_superseded_attempts SET observation = $3 \
                     WHERE order_id = $1 AND invoice_id = $2 AND state = 'watching'",
                )
                .bind(attempt.order_id)
                .bind(attempt.invoice_id)
                .bind(&observation)
                .execute(&state.pool)
                .await?;
            }
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

/// The released attempt's pins and bind-time quote, read under the row lock.
#[derive(Debug, sqlx::FromRow)]
struct ReleasedPins {
    state: String,
    reference: Option<String>,
    stack_id: String,
    stack_endpoint: String,
    total_sats: i64,
    expires_at: Option<DateTime<Utc>>,
    prepare_expires_at: Option<DateTime<Utc>>,
    allocation_mode: Option<String>,
    address_fingerprint: Option<String>,
    bitcoin_quote_rate: Option<sqlx::types::BigDecimal>,
    bitcoin_quote_source: Option<String>,
    bitcoin_quote_fetched_at: Option<DateTime<Utc>>,
    bitcoin_quoted_sats: Option<i64>,
    bitcoin_quote_expires_at: Option<DateTime<Utc>>,
    bitcoin_quote_currency: Option<String>,
    bitcoin_quote_exponent: Option<i16>,
    bitcoin_quote_spread_bps: Option<i32>,
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
    let pins: Option<ReleasedPins> = sqlx::query_as(
        "SELECT state, reference, stack_id, stack_endpoint, total_sats, expires_at, \
         prepare_expires_at, allocation_mode, address_fingerprint, bitcoin_quote_rate, \
         bitcoin_quote_source, bitcoin_quote_fetched_at, bitcoin_quoted_sats, \
         bitcoin_quote_expires_at, bitcoin_quote_currency, bitcoin_quote_exponent, \
         bitcoin_quote_spread_bps FROM paykit_superseded_attempts \
         WHERE order_id = $1 AND invoice_id = $2 FOR UPDATE",
    )
    .bind(attempt.order_id)
    .bind(attempt.invoice_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(pins) = pins.filter(|pins| pins.state == "watching") else {
        tx.rollback().await?;
        return Ok(false);
    };
    let current_invoice = order.paykit_invoice_id;
    if current_invoice == Some(attempt.invoice_id) && order.paykit_request_state.is_some() {
        // The order poller tracks this invoice again.
        tx.rollback().await?;
        return Ok(false);
    }
    let frozen = observation_json("confirmed", amount_matched, observation, now, false);

    // Another rail owns the payment: a fiat method, or an adapter other
    // than paykit (bitcoin) or sandbox (unbound). Locks pins `adapter`
    // without a `payment_method`.
    let other_rail = order
        .payment_method
        .as_deref()
        .is_some_and(|method| method != "bitcoin")
        || !matches!(payment.adapter.as_str(), "paykit" | "sandbox");
    let settled = !matches!(payment.state.as_str(), "awaiting_entitlement" | "expired");
    if other_rail || settled {
        let reason = if settled {
            "payment_settled"
        } else {
            "other_rail"
        };
        close_attempt(&mut tx, attempt, "needs_review", Some(reason), &frozen, now).await?;
        tx.commit().await?;
        tracing::error!(
            order_id = %attempt.order_id,
            invoice_id = %attempt.invoice_id,
            payment_state = %payment.state,
            payment_method = ?order.payment_method,
            payment_adapter = %payment.adapter,
            review_reason = reason,
            code = "paykit_released_attempt_paid_after_settlement",
            "ALERT money confirmed on a released paykit attempt of an order that is settled, \
             under review, or on another payment rail; held for review"
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
         paykit_total_sats = $6, paykit_expires_at = $7, paykit_prepare_expires_at = $8, \
         paykit_allocation_mode = $9, paykit_address_fingerprint = $10, \
         bitcoin_quote_rate = $11, bitcoin_quote_source = $12, bitcoin_quote_fetched_at = $13, \
         bitcoin_quoted_sats = $14, bitcoin_quote_expires_at = $15, \
         bitcoin_quote_currency = $16, bitcoin_quote_exponent = $17, \
         bitcoin_quote_spread_bps = $18, paykit_activation_state = 'active', \
         paykit_request_state = 'confirmed', paykit_delivery_state = NULL, \
         paykit_observation = $19, \
         paykit_observed_sats = $20, paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, updated_at = $21 WHERE id = $1",
    )
    .bind(order.id)
    .bind(attempt.invoice_id)
    .bind(pins.reference.unwrap_or_else(|| attempt.reference()))
    .bind(&pins.stack_id)
    .bind(&pins.stack_endpoint)
    .bind(pins.total_sats)
    .bind(pins.expires_at)
    .bind(pins.prepare_expires_at)
    .bind(&pins.allocation_mode)
    .bind(&pins.address_fingerprint)
    .bind(&pins.bitcoin_quote_rate)
    .bind(&pins.bitcoin_quote_source)
    .bind(pins.bitcoin_quote_fetched_at)
    .bind(pins.bitcoin_quoted_sats)
    .bind(pins.bitcoin_quote_expires_at)
    .bind(&pins.bitcoin_quote_currency)
    .bind(pins.bitcoin_quote_exponent)
    .bind(pins.bitcoin_quote_spread_bps)
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
    .bind(pins.total_sats)
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
        close_attempt(&mut tx, attempt, "late_money", None, &frozen, now).await?;
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
        state.confirm_keys(),
        &payment,
        &order,
        attempt.order_id,
        &order.buyer_pubky,
        now,
    )
    .await
    {
        Ok(outcome) => {
            close_attempt(&mut tx, attempt, "late_money", None, &frozen, now).await?;
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
    review_reason: Option<&str>,
    observation: &serde_json::Value,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE paykit_superseded_attempts SET state = $3, review_reason = $4, \
         observation = $5, closed_at = $6 WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(attempt.order_id)
    .bind(attempt.invoice_id)
    .bind(state)
    .bind(review_reason)
    .bind(observation)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// One released attempt waiting for an operator.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct NeedsReview {
    pub order_id: Uuid,
    pub invoice_id: Uuid,
    pub seller_pubky: String,
    pub buyer_pubky: String,
    pub review_reason: String,
    pub total_sats: i64,
    pub observation: Option<serde_json::Value>,
    pub closed_at: DateTime<Utc>,
}

/// Every released attempt waiting for an operator, oldest first.
pub async fn list_needs_review(pool: &PgPool) -> Result<Vec<NeedsReview>, sqlx::Error> {
    sqlx::query_as(
        "SELECT h.order_id, h.invoice_id, o.seller_pubky, o.buyer_pubky, h.review_reason, \
         h.total_sats, h.observation, h.closed_at \
         FROM paykit_superseded_attempts h JOIN orders o ON o.id = h.order_id \
         WHERE h.state = 'needs_review' ORDER BY h.closed_at, h.order_id, h.invoice_id",
    )
    .fetch_all(pool)
    .await
}

/// How an operator closed a `needs_review` attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewOutcome {
    /// The buyer's money was returned; the note is the external refund
    /// reference. Allowed for every review reason.
    Refunded,
    /// No money was confirmed, so no refund is due; the note is the reason.
    /// Allowed only for `detected_unconfirmed` (a detection that never
    /// confirmed). Confirmed money (`payment_settled`, `other_rail`) is
    /// closed only by a refund.
    Dismissed,
}

impl ReviewOutcome {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "refunded" => Ok(Self::Refunded),
            "dismissed" => Ok(Self::Dismissed),
            other => anyhow::bail!("the outcome must be refunded or dismissed, not {other:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Refunded => "refunded",
            Self::Dismissed => "dismissed",
        }
    }
}

/// Records an operator's outcome for one `needs_review` attempt. Returns
/// false when the attempt is not waiting for review (unknown, or already
/// resolved). The note (refund reference or reason) and the operator are
/// required, and `Dismissed` is refused unless no money was confirmed.
pub async fn resolve_needs_review(
    pool: &PgPool,
    order_id: Uuid,
    invoice_id: Uuid,
    outcome: ReviewOutcome,
    note: &str,
    operator: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let note = note.trim();
    let operator = operator.trim();
    if note.is_empty() || note.chars().count() > 500 {
        anyhow::bail!("the note must be 1 to 500 characters");
    }
    if operator.is_empty() || operator.chars().count() > 128 {
        anyhow::bail!("the operator must be 1 to 128 characters");
    }
    let mut tx = pool.begin().await?;
    let reason: Option<String> = sqlx::query_scalar(
        "SELECT review_reason FROM paykit_superseded_attempts \
         WHERE order_id = $1 AND invoice_id = $2 AND state = 'needs_review' FOR UPDATE",
    )
    .bind(order_id)
    .bind(invoice_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(reason) = reason else {
        tx.rollback().await?;
        return Ok(false);
    };
    if outcome == ReviewOutcome::Dismissed && reason != "detected_unconfirmed" {
        tx.rollback().await?;
        anyhow::bail!(
            "{reason} is confirmed money the order did not take: it closes only as refunded \
             with the refund reference"
        );
    }
    let resolved = sqlx::query(
        "UPDATE paykit_superseded_attempts SET state = 'resolved', resolution_outcome = $3, \
         resolution_note = $4, resolved_by = $5, resolved_at = $6 \
         WHERE order_id = $1 AND invoice_id = $2 AND state = 'needs_review' \
         AND ($3 = 'refunded' OR review_reason = 'detected_unconfirmed')",
    )
    .bind(order_id)
    .bind(invoice_id)
    .bind(outcome.as_str())
    .bind(note)
    .bind(operator)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if resolved.rows_affected() == 1 {
        tracing::info!(
            order_id = %order_id,
            invoice_id = %invoice_id,
            outcome = outcome.as_str(),
            "released paykit attempt review resolved"
        );
    }
    Ok(resolved.rows_affected() == 1)
}
