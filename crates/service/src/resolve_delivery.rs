//! The pinned `paykit.resolve` delivery arm (design §B.8.8 r13).
//!
//! Every seller confirmation and every manual-review resolution commits its
//! local effects with one `paykit_resolve_outbox` row pinned to the issuing
//! stack by identity AND address. This arm delivers those rows:
//!
//! - It dials the ROW'S pinned endpoint, never the current default, and
//!   compares the row's `stack_id` against what that endpoint reports on
//!   `/health/ready` (cached per endpoint for 15 s) before sending. A
//!   mismatch sends NO request: the row terminates `stack_pin_mismatch`
//!   with an alert.
//! - Every response class has exactly one outcome (the twelve-row
//!   mapping): permanent refusals terminate visibly; 401/403 retry under
//!   the deadline with an immediate first alert; 408/425/429, 5xx and
//!   transport failures retry under the hard one-hour
//!   `delivery_deadline`. `429`/`503` honour `Retry-After` as
//!   `min(deadline, max(backoff_due_at, retry_after_due_at))` — a floor,
//!   never an undercut of the normal backoff. Anything unmapped terminates
//!   `unmapped_resolve_error` with the status and code recorded.
//! - The infrastructure termination acknowledgement and the operator
//!   escape terminate DELIVERY only: they never change the order or
//!   payment outcome and confer no order authority. The order stays
//!   whatever the local transaction decided — the marketplace's record is
//!   authoritative for the money outcome.
//!
//! No delivery ever runs inside a local state transaction; the resolution
//! committed before this arm ever sees the row.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::payments::{PaykitClient, PaykitResolveResponse};
use crate::AppState;

/// The pinned-endpoint readiness cache TTL (the same 15 s the
/// `bitcoin_offer_available` rail cache uses, per endpoint rather than per
/// process — after a repoint two endpoints are in play at once).
const PIN_CACHE_TTL_SECONDS: i64 = 15;

const RESOLVE_CLAIM_BATCH: i64 = 50;
/// The normal lease-and-backoff schedule: 30 s doubling to a 15-minute
/// cap, always bounded by the row's delivery deadline.
const BACKOFF_BASE_SECONDS: i64 = 30;
const BACKOFF_CAP_SECONDS: i64 = 900;

/// Per-endpoint cache of the `stack_id` each pinned endpoint currently
/// reports on `/health/ready`. Failures are never cached (a blip retries
/// next pass rather than pinning a stale "unreachable"). Cloned handles
/// share the one cache (AppState is cloned per request).
/// One cached readiness identity: the endpoint's reported `stack_id`
/// (None = the endpoint answered without one) and when it was read.
type CachedIdentity = (Option<String>, DateTime<Utc>);

#[derive(Debug, Default, Clone)]
pub struct ResolvePinCache {
    inner: std::sync::Arc<Mutex<HashMap<String, CachedIdentity>>>,
}

impl ResolvePinCache {
    /// The cached identity for an endpoint, fresh at `now`; `None` on a
    /// cache miss (the outer `Option` — a cached "endpoint reported no
    /// stack_id" is `Some(None)`).
    pub fn get(&self, endpoint: &str, now: DateTime<Utc>) -> Option<Option<String>> {
        let guard = self.inner.lock().expect("pin cache lock");
        let (stack_id, fetched_at) = guard.get(endpoint)?;
        if now - *fetched_at > chrono::Duration::seconds(PIN_CACHE_TTL_SECONDS) {
            return None;
        }
        Some(stack_id.clone())
    }

    pub fn put(&self, endpoint: &str, stack_id: Option<String>, now: DateTime<Utc>) {
        self.inner
            .lock()
            .expect("pin cache lock")
            .insert(endpoint.to_string(), (stack_id, now));
    }
}

/// The normal backoff for a retrying row: 30 s doubling to the cap.
/// `attempt_count` is the number of attempts already made (0-based).
pub(crate) fn resolve_backoff_seconds(attempt_count: i32) -> i64 {
    let shift = u32::try_from(attempt_count).unwrap_or(0).min(5);
    (BACKOFF_BASE_SECONDS << shift).min(BACKOFF_CAP_SECONDS)
}

/// Parses a `Retry-After` header (delay-seconds or HTTP-date) into a due
/// instant. A malformed value falls back — the caller then uses the normal
/// backoff. Zero, near-zero, and past values parse fine; the `max()` floor
/// in the scheduler is what stops them undercutting the backoff.
pub(crate) fn retry_after_due_at(header: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let trimmed = header.trim();
    if let Ok(seconds) = trimmed.parse::<i64>() {
        return Some(now + chrono::Duration::seconds(seconds.max(0)));
    }
    DateTime::parse_from_rfc2822(trimmed)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

/// One claimed `paykit_resolve_outbox` row.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedResolveRow {
    id: i64,
    order_id: Uuid,
    invoice_id: Uuid,
    resolution: String,
    resolved_at: DateTime<Utc>,
    stack_id: String,
    stack_endpoint: String,
    attempt_count: i32,
    delivery_deadline: DateTime<Utc>,
    auth_alerted: bool,
}

/// Claims due queued rows by stamping their lease.
async fn claim_due_resolve_rows(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Result<Vec<ClaimedResolveRow>, sqlx::Error> {
    sqlx::query_as(
        "UPDATE paykit_resolve_outbox SET lease_until = $2 WHERE id IN (\
             SELECT id FROM paykit_resolve_outbox \
             WHERE delivery_state = 'queued' AND next_attempt_at <= $1 \
             AND (lease_until IS NULL OR lease_until <= $1) \
             ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED\
         ) RETURNING id, order_id, invoice_id, resolution, resolved_at, stack_id, \
         stack_endpoint, attempt_count, delivery_deadline, auth_alerted",
    )
    .bind(now)
    .bind(now + chrono::Duration::seconds(lease_seconds))
    .bind(RESOLVE_CLAIM_BATCH)
    .fetch_all(pool)
    .await
}

/// One delivery pass: claims due rows and delivers each against its pinned
/// endpoint. Returns the number of rows that reached a finished state
/// (delivered or terminated) this pass.
pub async fn deliver_due_resolve_rows(
    state: &AppState,
    paykit: &PaykitClient,
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> anyhow::Result<u64> {
    let claimed = claim_due_resolve_rows(&state.pool, now, lease_seconds).await?;
    let mut finished = 0u64;
    for row in &claimed {
        match deliver_one_resolve_row(state, paykit, row, now).await {
            Ok(true) => finished += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    row_id = row.id,
                    order_id = %row.order_id,
                    error = %error,
                    "resolve delivery failed for a claimed row; continuing the batch"
                );
            }
        }
    }
    Ok(finished)
}

/// Stamps a row delivered: state change and `delivered_at` in one update.
async fn stamp_delivered(pool: &PgPool, row_id: i64, now: DateTime<Utc>) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE paykit_resolve_outbox SET delivery_state = 'delivered', delivered_at = $2, \
         lease_until = NULL, attempt_count = attempt_count + 1, last_attempt_at = $2, \
         updated_at = $2 WHERE id = $1",
    )
    .bind(row_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Terminates a row visibly with its mapped reason (and optional detail),
/// alerting. The order and payment are deliberately untouched: the
/// marketplace's record is authoritative for the money outcome, and what a
/// terminal row loses is only paykit's audit copy of it.
async fn stamp_terminal(
    pool: &PgPool,
    row: &ClaimedResolveRow,
    reason: &str,
    detail: Option<String>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE paykit_resolve_outbox SET delivery_state = 'terminal_unresolved', \
         terminal_reason = $2, terminal_detail = $3, lease_until = NULL, \
         attempt_count = attempt_count + 1, last_attempt_at = $4, updated_at = $4 \
         WHERE id = $1",
    )
    .bind(row.id)
    .bind(reason)
    .bind(detail)
    .bind(now)
    .execute(pool)
    .await?;
    tracing::error!(
        row_id = row.id,
        order_id = %row.order_id,
        terminal_reason = reason,
        "ALERT paykit resolve delivery terminated unresolved; the order outcome stands \
         and the row blocks the drain until acknowledged"
    );
    Ok(())
}

/// Schedules the next attempt under the deadline: the normal backoff, with
/// a valid `Retry-After` honoured as a floor (`max`), clamped by the hard
/// deadline (`min`). When the clamp lands on the deadline the row
/// terminates THERE with no final attempt.
async fn schedule_retry(
    pool: &PgPool,
    row: &ClaimedResolveRow,
    retry_after: Option<&str>,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let backoff_due_at =
        now + chrono::Duration::seconds(resolve_backoff_seconds(row.attempt_count));
    let retry_after_due_at = retry_after.and_then(|header| retry_after_due_at(header, now));
    let mut next = backoff_due_at;
    if let Some(retry_after_due_at) = retry_after_due_at {
        if retry_after_due_at > next {
            next = retry_after_due_at;
        }
    }
    if next >= row.delivery_deadline {
        stamp_terminal(
            pool,
            row,
            "delivery_deadline_exceeded",
            None,
            row.delivery_deadline,
        )
        .await?;
        return Ok(true);
    }
    sqlx::query(
        "UPDATE paykit_resolve_outbox SET attempt_count = attempt_count + 1, \
         last_attempt_at = $2, next_attempt_at = $3, lease_until = NULL, updated_at = $2 \
         WHERE id = $1",
    )
    .bind(row.id)
    .bind(now)
    .bind(next)
    .execute(pool)
    .await?;
    Ok(false)
}

/// The pinned-endpoint identity check: the row's `stack_id` against what
/// its pinned endpoint currently reports. `Ok(true)` sends; `Ok(false)`
/// terminated `stack_pin_mismatch` without any request; `Err(())` is a
/// transient readiness failure (retry).
async fn pin_check(
    state: &AppState,
    paykit: &PaykitClient,
    row: &ClaimedResolveRow,
    now: DateTime<Utc>,
) -> Result<bool, ()> {
    let reported = match state.resolve_pin_cache.get(&row.stack_endpoint, now) {
        Some(cached) => cached,
        None => {
            let fetched = paykit.stack_identity_at(&row.stack_endpoint).await;
            match fetched {
                Ok(stack_id) => {
                    state
                        .resolve_pin_cache
                        .put(&row.stack_endpoint, stack_id.clone(), now);
                    stack_id
                }
                Err(_) => return Err(()),
            }
        }
    };
    Ok(reported.as_deref() == Some(row.stack_id.as_str()))
}

/// Delivers one claimed row. Returns true when the row reached a finished
/// state (delivered or terminated) this pass.
async fn deliver_one_resolve_row(
    state: &AppState,
    paykit: &PaykitClient,
    row: &ClaimedResolveRow,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let pool = &state.pool;
    // The hard bound, regardless of class: a row still in retry at its
    // deadline terminates there.
    if now >= row.delivery_deadline {
        stamp_terminal(
            pool,
            row,
            "delivery_deadline_exceeded",
            None,
            row.delivery_deadline,
        )
        .await?;
        return Ok(true);
    }
    // The local half of the pin: compare before sending, never send on a
    // mismatch.
    match pin_check(state, paykit, row, now).await {
        Ok(true) => {}
        Ok(false) => {
            stamp_terminal(pool, row, "stack_pin_mismatch", None, now).await?;
            return Ok(true);
        }
        Err(()) => {
            tracing::warn!(
                row_id = row.id,
                "the pinned endpoint is unreachable for the readiness read; retrying"
            );
            return schedule_retry(pool, row, None, now).await;
        }
    }

    let outcome = paykit
        .resolve_payment_request(
            &row.stack_endpoint,
            row.invoice_id,
            &row.stack_id,
            &row.resolution,
            row.resolved_at,
        )
        .await;
    let response = match outcome {
        Ok(response) => response,
        Err(_) => {
            // Transport failure: connection refused, TLS, timeout,
            // unroutable host — transient by definition.
            tracing::warn!(
                row_id = row.id,
                "resolve delivery transport failure; retrying"
            );
            return schedule_retry(pool, row, None, now).await;
        }
    };
    classify_response(state, row, response, now).await
}

/// The twelve-row response-class mapping (§B.8.8): HTTP status first,
/// application code second, every class with exactly one outcome.
async fn classify_response(
    state: &AppState,
    row: &ClaimedResolveRow,
    response: PaykitResolveResponse,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let pool = &state.pool;
    let status = response.status.as_u16();
    let code = response.code.as_deref().unwrap_or_default();

    // The two success rows: resolution recorded, or an idempotent replay
    // of a row whose effect already landed (resolve is idempotent on
    // (invoice_id, resolution)). A 2xx answering a DIFFERENT resolution is
    // a contract violation — fail visible, not a success.
    if response.status.is_success() {
        return match response.resolved {
            Some(resolved) if resolved.resolution == row.resolution => {
                stamp_delivered(pool, row.id, now).await?;
                tracing::info!(
                    row_id = row.id,
                    order_id = %row.order_id,
                    resolution = %row.resolution,
                    "delivered a pinned paykit resolution"
                );
                Ok(true)
            }
            Some(resolved) => {
                stamp_terminal(
                    pool,
                    row,
                    "unmapped_resolve_error",
                    Some(format!(
                        "status={status} resolution_mismatch:{}",
                        resolved.resolution
                    )),
                    now,
                )
                .await?;
                Ok(true)
            }
            None => {
                // A malformed success body: retryable, like a malformed
                // activate 200 — nothing may change.
                tracing::warn!(
                    row_id = row.id,
                    "paykit resolve returned a malformed success body; retrying"
                );
                schedule_retry(pool, row, None, now).await
            }
        };
    }

    // The named transient set: "not now", never "not ever".
    if matches!(status, 408 | 425 | 429) || response.status.is_server_error() {
        let retry_after = if matches!(status, 429 | 503) {
            response.retry_after.as_deref()
        } else {
            None
        };
        return schedule_retry(pool, row, retry_after, now).await;
    }

    // 401/403: transient WITH an immediate first alert — a key rotation or
    // a misconfigured trusted key is operator-fixable inside the deadline.
    if matches!(status, 401 | 403) {
        if !row.auth_alerted {
            sqlx::query("UPDATE paykit_resolve_outbox SET auth_alerted = TRUE WHERE id = $1")
                .bind(row.id)
                .execute(pool)
                .await?;
            tracing::error!(
                row_id = row.id,
                order_id = %row.order_id,
                status,
                "ALERT paykit resolve refused on authentication; an operator must fix the \
                 trusted key inside the delivery deadline"
            );
        }
        return schedule_retry(pool, row, None, now).await;
    }

    // Permanent named refusals, each terminating visibly.
    let terminal_reason = match (status, code) {
        (404, "unknown_invoice") => Some("unknown_invoice"),
        (409, "invoice_not_activated") => Some("invoice_not_activated"),
        (409, "invoice_already_resolved") => Some("invoice_already_resolved"),
        (409, "invoice_finalized") => Some("invoice_finalized"),
        (409, "void_baseline_failed") => Some("void_baseline_failed"),
        (409, "void_prepare_expired") => Some("void_prepare_expired"),
        (409, "void_cancelled") => Some("void_cancelled"),
        (409, "stack_identity_mismatch") => Some("stack_identity_mismatch"),
        // The one genuinely transient invoice state: a baseline snapshot in
        // flight clears within a tick or fails to a void error.
        (409, "invoice_baseline_in_progress") => None,
        _ => {
            // The permanent default: any other status or any unrecognised
            // application code terminates with the status and code
            // recorded — fail-visible, never fail-forever.
            let detail = format!("status={status} code={code}");
            stamp_terminal(pool, row, "unmapped_resolve_error", Some(detail), now).await?;
            return Ok(true);
        }
    };
    match terminal_reason {
        Some(reason) => {
            stamp_terminal(pool, row, reason, None, now).await?;
            Ok(true)
        }
        None => schedule_retry(pool, row, None, now).await,
    }
}

// ---------------------------------------------------------------------------
// Infrastructure acknowledgement (delivery-only; no order authority)
// ---------------------------------------------------------------------------

/// The operator escape (§C row 17): terminates a named QUEUED row as
/// `operator_terminated` with the operator's identity and reason recorded,
/// alerting. Terminates DELIVERY only — the order/payment outcome is
/// untouched, and the acknowledgement counts toward the §C.16 condition-6
/// gate. Returns false when no queued row with that id exists.
pub async fn terminate_resolve_delivery(
    pool: &PgPool,
    row_id: i64,
    operator: &str,
    reason_text: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let terminated = sqlx::query(
        "UPDATE paykit_resolve_outbox SET delivery_state = 'terminal_unresolved', \
         terminal_reason = 'operator_terminated', lease_until = NULL, \
         acknowledged_at = $2, acknowledged_by = $3, acknowledgement_note = $4, updated_at = $2 \
         WHERE id = $1 AND delivery_state = 'queued'",
    )
    .bind(row_id)
    .bind(now)
    .bind(operator)
    .bind(reason_text)
    .execute(pool)
    .await?;
    if terminated.rows_affected() == 1 {
        tracing::error!(
            row_id,
            "ALERT an infrastructure operator terminated a paykit resolve delivery; the \
             order outcome stands — only paykit's audit copy is discarded"
        );
        return Ok(true);
    }
    Ok(false)
}

/// Records the deliberate operator acknowledgement of an already-terminal
/// row (what they did about the lost audit copy before the rollback
/// proceeds). Returns false unless the row is terminal and unacknowledged.
pub async fn acknowledge_resolve_row(
    pool: &PgPool,
    row_id: i64,
    operator: &str,
    note: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let acknowledged = sqlx::query(
        "UPDATE paykit_resolve_outbox SET acknowledged_at = $2, acknowledged_by = $3, \
         acknowledgement_note = $4, updated_at = $2 \
         WHERE id = $1 AND delivery_state = 'terminal_unresolved' AND acknowledged_at IS NULL",
    )
    .bind(row_id)
    .bind(now)
    .bind(operator)
    .bind(note)
    .execute(pool)
    .await?;
    Ok(acknowledged.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid timestamp")
    }

    #[test]
    fn backoff_doubles_to_the_cap() {
        assert_eq!(resolve_backoff_seconds(0), 30);
        assert_eq!(resolve_backoff_seconds(1), 60);
        assert_eq!(resolve_backoff_seconds(2), 120);
        assert_eq!(resolve_backoff_seconds(3), 240);
        assert_eq!(resolve_backoff_seconds(4), 480);
        assert_eq!(resolve_backoff_seconds(5), 900);
        assert_eq!(resolve_backoff_seconds(6), 900);
        assert_eq!(resolve_backoff_seconds(40), 900);
    }

    #[test]
    fn retry_after_parses_both_forms() {
        let now = at(1_000_000);
        assert_eq!(
            retry_after_due_at("120", now),
            Some(now + chrono::Duration::seconds(120))
        );
        // Zero and negative-delay values parse; the scheduler's floor is
        // what stops them undercutting the backoff.
        assert_eq!(retry_after_due_at("0", now), Some(now));
        assert_eq!(retry_after_due_at("-5", now), Some(now));
        // An HTTP-date in the future and in the past.
        let future = "Fri, 02 Jan 2037 00:00:00 GMT";
        assert!(retry_after_due_at(future, now).is_some());
        let past = "Thu, 01 Jan 1970 00:00:00 GMT";
        assert_eq!(
            retry_after_due_at(past, now),
            Some(DateTime::from_timestamp(0, 0).expect("epoch"))
        );
        // Malformed falls back (None).
        assert_eq!(retry_after_due_at("soon", now), None);
        assert_eq!(retry_after_due_at("", now), None);
    }

    #[test]
    fn pin_cache_expires_per_endpoint() {
        let cache = ResolvePinCache::default();
        let now = at(1_000_000);
        cache.put("http://a", Some("proof:x".to_string()), now);
        cache.put("http://b", None, now);
        assert_eq!(
            cache.get("http://a", now + chrono::Duration::seconds(14)),
            Some(Some("proof:x".to_string()))
        );
        assert_eq!(
            cache.get("http://b", now + chrono::Duration::seconds(14)),
            Some(None)
        );
        assert_eq!(
            cache.get("http://a", now + chrono::Duration::seconds(16)),
            None
        );
        assert_eq!(cache.get("http://c", now), None);
    }
}
