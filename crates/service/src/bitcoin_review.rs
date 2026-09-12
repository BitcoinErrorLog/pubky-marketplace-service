//! `shared_manual` checkout: seller confirmation and Paykit manual-review
//! resolution (design §B.8.8/§B.9 r13).
//!
//! The rail never auto-pays a `shared_manual` order: a non-late matching
//! Paykit observation enters `orders.paykit_request_state =
//! 'awaiting_seller_confirmation'` (the poller in `workers.rs` writes that),
//! extending the inventory hold to a bounded 24-hour seller-confirmation
//! window. Status polling keeps refreshing facts and can never transition
//! the order to `paid`. Three exits exist, each decided by one CAS with
//! every side effect in the same transaction:
//!
//! - **Seller confirm** (`POST /v0/orders/{id}/confirm-bitcoin-payment`):
//!   the listing seller — and only the listing seller, authorised BEFORE
//!   any idempotency lookup — attests the payment. The audit facts
//!   (`confirmed_txid`, `confirmed_amount_sats`, the frozen observation)
//!   derive from the stored Paykit observation, never the body;
//!   `confirmation_basis = 'seller_attestation'` is a constant.
//! - **The 24-hour seller-window reaper**: CAS `awaiting_seller_confirmation
//!   -> confirmed` on the order plus `manual_review` on the payment, hold
//!   PRESERVED, starting the two-business-day SLA and the seven-day clock.
//! - **Seller resolve / the seven-day inactivity reaper**: one shared CAS
//!   (`state = 'manual_review' AND resolution_outcome IS NULL`) applies
//!   `paid` / `refunded` / `abandoned` with the frozen entry×outcome
//!   inventory matrix. The reaper is the same abandoned branch with
//!   `resolution_basis = 'seller_unresponsive'`; a later seller resolve
//!   gets 409.
//!
//! Every terminal outcome writes exactly one audit row, one event, and one
//! stack-AND-endpoint pinned `paykit.resolve` outbox row (delivery lives in
//! `resolve_delivery.rs`). No network call happens inside any local state
//! transaction. Nothing here can un-pay an order: there is no reverse edge.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Datelike, Utc, Weekday};
use marketplace_domain::{ids, ErrorCode};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::holds::{release_lines, HeldQuantity};
use crate::handlers::{fetch_listing, fetch_order_for_update, insert_notification_intent};
use crate::model::{OrderRow, PaymentRow};
use crate::payments::PaykitObservation;
use crate::queries::{ORDER_COLUMNS, PAYMENT_COLUMNS};
use crate::workers::SYSTEM_ACTOR;
use crate::AppState;

/// The bounded seller-confirmation window armed on entry to
/// `awaiting_seller_confirmation` (D6): aligned with §B.9's 24-hour
/// observation tail so an operator reasons about one number.
pub const SELLER_CONFIRMATION_WINDOW_SECONDS: i64 = 24 * 3600;
/// The inactivity bound after `manual_review` entry: at seven days the
/// reaper records `abandoned` with `resolution_basis='seller_unresponsive'`.
pub const MANUAL_REVIEW_INACTIVITY_DAYS: i64 = 7;
/// The seller-response SLA from `manual_review` entry: breach alerts (it
/// never transitions anything).
pub const SELLER_RESPONSE_SLA_BUSINESS_DAYS: i64 = 2;
/// The hard delivery bound on every `paykit.resolve` outbox row (§B.8.8):
/// one hour from row creation, regardless of response class.
pub const RESOLVE_DELIVERY_DEADLINE_SECONDS: i64 = 3600;

const RESOLUTION_OUTCOMES: [&str; 3] = ["paid", "refunded", "abandoned"];

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Machine-readable failure in the standard envelope, with a `reason`
/// sub-code the client can branch on. Static copy only — nothing about the
/// order, the observation, or any server value is interpolated.
fn review_error(code: ErrorCode, reason: &str, message: &str) -> Response {
    (
        StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
        Json(json!({
            "ok": false,
            "error": { "code": code, "message": message, "reason": reason },
        })),
    )
        .into_response()
}

fn internal(context: &str, error: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %error, "{context} failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "ok": false,
            "error": { "code": "INTERNAL", "message": "The request could not be processed." },
        })),
    )
        .into_response()
}

/// The observation document persisted on `orders.paykit_observation`: the
/// facts paykit-server reported, refreshed by the status-only poll and
/// frozen into audit records at confirmation/resolution. Never a
/// transition input.
pub fn observation_json(
    state: &str,
    amount_matched: bool,
    observation: &PaykitObservation,
    now: DateTime<Utc>,
    disappeared: bool,
) -> Value {
    json!({
        "state": state,
        "observed_sats": observation.observed_sats,
        "confirmations": observation.confirmations,
        "amount_matched": amount_matched,
        "txid": observation.txid,
        "observed_at": format_timestamp(now),
        "disappeared": disappeared,
    })
}

/// Adds `days` business days (Mon–Fri) to `start`: the two-business-day
/// seller-response SLA is measured in business days, not wall-clock days,
/// so a Friday routing gives the seller until Tuesday.
pub fn add_business_days(start: DateTime<Utc>, days: i64) -> DateTime<Utc> {
    let mut current = start;
    let mut remaining = days;
    while remaining > 0 {
        current += chrono::Duration::days(1);
        if !matches!(current.weekday(), Weekday::Sat | Weekday::Sun) {
            remaining -= 1;
        }
    }
    current
}

// ---------------------------------------------------------------------------
// §C.16 drain predicates (conditions 6 and 7)
// ---------------------------------------------------------------------------

/// §C.16 condition 7, the exact empty-set predicate: no order pinned to
/// this stack can still create a resolution row. Window elapsing does NOT
/// clear it — only seller resolution or the seven-day reaper does.
pub async fn condition_seven_clear(pool: &PgPool, old_stack_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT NOT EXISTS (\
             SELECT 1 FROM orders o LEFT JOIN payments p ON p.order_id = o.id \
             WHERE o.paykit_stack_id = $1 \
             AND (o.paykit_request_state = 'awaiting_seller_confirmation' \
                  OR p.state = 'manual_review'))",
    )
    .bind(old_stack_id)
    .fetch_one(pool)
    .await
}

/// §C.16 condition 6: every `paykit.resolve` row for this stack is
/// `delivered`, or `terminal_unresolved` WITH an operator acknowledgement.
/// Returns the number of rows still blocking the drain.
pub async fn condition_six_blocking_rows(
    pool: &PgPool,
    old_stack_id: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM paykit_resolve_outbox \
         WHERE stack_id = $1 \
         AND (delivery_state = 'queued' \
              OR (delivery_state = 'terminal_unresolved' AND acknowledged_at IS NULL))",
    )
    .bind(old_stack_id)
    .fetch_one(pool)
    .await
}

// ---------------------------------------------------------------------------
// Authorization: session pubky -> order -> listing -> seller pubky
// ---------------------------------------------------------------------------

/// Resolves the order's listing seller for the peer endpoints. The session
/// pubky is the authority; the seller identity comes from the order's OWN
/// listing record, never from the request body.
async fn listing_seller_for_order(
    pool: &PgPool,
    order_id: Uuid,
) -> Result<Option<(OrderRow, String)>, sqlx::Error> {
    let order: Option<OrderRow> =
        sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1"))
            .bind(order_id)
            .fetch_optional(pool)
            .await?;
    let Some(order) = order else {
        return Ok(None);
    };
    let Some(aggregate_id) = order
        .lines
        .as_array()
        .and_then(|lines| lines.first())
        .and_then(|line| line["listing_aggregate_id"].as_str())
    else {
        return Ok(None);
    };
    let mut tx = pool.begin().await?;
    let listing = fetch_listing(&mut tx, aggregate_id).await?;
    tx.commit().await?;
    Ok(listing.map(|listing| (order, listing.seller_pubky)))
}

fn not_order_seller() -> Response {
    review_error(
        ErrorCode::Unauthorized,
        "not_order_seller",
        "Only the seller of this order's listing may perform this action.",
    )
}

/// The stored seller-confirmation record read by the idempotency lookup.
type StoredConfirmation = (
    chrono::DateTime<Utc>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Value,
);

fn order_not_found() -> Response {
    review_error(
        ErrorCode::NotFound,
        "order_not_found",
        "The order was not found.",
    )
}

// ---------------------------------------------------------------------------
// Seller confirmation (POST /v0/orders/{id}/confirm-bitcoin-payment)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmBitcoinPaymentBody {
    /// Free-text seller note (bounded), recorded as `confirmed_reason`.
    #[serde(default)]
    reason: Option<String>,
    /// Legacy optional fields: never read as facts. When present they must
    /// EQUAL the stored observation exactly, or the call is rejected with
    /// `confirmation_observation_mismatch` (Sol P1: the audit trail says
    /// what happened, not what the seller typed).
    #[serde(default)]
    txid: Option<String>,
    #[serde(default)]
    confirmed_amount_sats: Option<i64>,
}

/// The seller's attestation that the observed payment is this buyer's.
/// One local transaction: the conditional UPDATE out of
/// `awaiting_seller_confirmation`, the audit record, the fulfilment
/// effects, and the pinned `paykit.resolve` outbox row commit together or
/// not at all. No network call rides the transaction.
pub async fn confirm_bitcoin_payment(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    body: Option<Json<ConfirmBitcoinPaymentBody>>,
) -> Response {
    let body = body
        .map(|body| body.0)
        .unwrap_or(ConfirmBitcoinPaymentBody {
            reason: None,
            txid: None,
            confirmed_amount_sats: None,
        });
    if body
        .reason
        .as_deref()
        .is_some_and(|reason| reason.len() > 500)
    {
        return review_error(
            ErrorCode::InvalidCommand,
            "invalid_reason",
            "The reason must be at most 500 characters.",
        );
    }
    // (1) Authorise FIRST: session pubky -> order -> listing -> seller
    // pubky. A buyer, an unrelated seller, or a wrong-listing seller gets
    // 403 before any idempotency behaviour (F13).
    let (order, seller) = match listing_seller_for_order(&state.pool, order_id).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return order_not_found(),
        Err(error) => return internal("order lookup", &error),
    };
    if seller != actor.0 {
        tracing::info!(order_id = %order_id, "rejected a non-seller bitcoin confirmation attempt");
        return not_order_seller();
    }
    // (2) Idempotency on the order id: the second call returns the recorded
    // confirmation unchanged and writes nothing.
    let existing: Option<StoredConfirmation> = match sqlx::query_as(
        "SELECT confirmed_at, confirmed_txid, confirmed_amount_sats, confirmed_reason, \
         paykit_observation FROM paykit_seller_confirmations WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(existing) => existing,
        Err(error) => return internal("confirmation lookup", &error),
    };
    if let Some((confirmed_at, txid, amount_sats, reason, observation)) = existing {
        return (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "confirmation": {
                    "order_id": order_id,
                    "confirmed_by_pubky": actor.0,
                    "confirmed_at": format_timestamp(confirmed_at),
                    "confirmed_txid": txid,
                    "confirmed_amount_sats": amount_sats,
                    "confirmed_reason": reason,
                    "confirmation_source": "seller",
                    "confirmation_basis": "seller_attestation",
                    "paykit_observation": observation,
                },
            })),
        )
            .into_response();
    }

    // Legacy-field equality: a supplied txid/amount must equal the stored
    // observation exactly, otherwise the client believes it recorded
    // something it did not. No state change, no audit row (F14).
    let observation = order
        .paykit_observation
        .clone()
        .unwrap_or_else(|| json!({}));
    if let Some(supplied) = &body.txid {
        let observed = observation["txid"].as_str();
        if Some(supplied.as_str()) != observed {
            tracing::info!(
                order_id = %order_id,
                "rejected a confirmation whose supplied txid disagrees with the observation"
            );
            return review_error(
                ErrorCode::InvalidCommand,
                "confirmation_observation_mismatch",
                "The supplied transaction details do not match the observed payment.",
            );
        }
    }
    if let Some(supplied) = body.confirmed_amount_sats {
        let observed = observation["observed_sats"].as_i64();
        if Some(supplied) != observed {
            tracing::info!(
                order_id = %order_id,
                "rejected a confirmation whose supplied amount disagrees with the observation"
            );
            return review_error(
                ErrorCode::InvalidCommand,
                "confirmation_observation_mismatch",
                "The supplied transaction details do not match the observed payment.",
            );
        }
    }

    let now = state.clock.now();
    let pool = state.pool.clone();
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal("confirmation transaction", &error),
    };
    // Canonical lock order: payment row before the order row (the 24-hour
    // reaper and every resolution path take them in this order).
    let payment: Option<PaymentRow> = match sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1 FOR UPDATE"
    ))
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(payment) => payment,
        Err(error) => return internal("payment lock", &error),
    };
    let Some(payment) = payment else {
        return internal("payment lock", &"payment row missing for order");
    };
    // (3) The decision: one conditional UPDATE predicated on the state.
    // Zero rows means the order was never in it or the reaper won — the
    // same named error either way (A8: first committer wins, the loser
    // changes nothing).
    let won = match sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed', \
         paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, updated_at = $2 \
         WHERE id = $1 AND paykit_request_state = 'awaiting_seller_confirmation'",
    )
    .bind(order_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result.rows_affected() == 1,
        Err(error) => return internal("confirmation decision", &error),
    };
    if !won {
        let _ = tx.rollback().await;
        return review_error(
            ErrorCode::InvalidState,
            "order_not_awaiting_confirmation",
            "This order is not awaiting seller confirmation.",
        );
    }
    let payment_cas: Option<(i64,)> = match sqlx::query_as(
        "UPDATE payments SET state = 'confirmed', revision = revision + 1, updated_at = $2 \
         WHERE id = $1 AND state = 'awaiting_entitlement' RETURNING revision",
    )
    .bind(payment.id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(cas) => cas,
        Err(error) => return internal("payment confirmation", &error),
    };
    let Some((payment_revision,)) = payment_cas else {
        let _ = tx.rollback().await;
        return internal(
            "payment confirmation",
            &"payment left awaiting_entitlement mid-confirm",
        );
    };
    // The fulfilment effects: paid, receipt exactly once, hold consumed.
    let (confirmed_order, _receipt, receipt_event_id) =
        match crate::handlers::payment::confirm_order(
            &mut tx,
            &actor.0,
            order_id,
            &payment,
            order.clone(),
            state.pickup.as_deref(),
            now,
        )
        .await
        {
            Ok(Ok(confirmed)) => confirmed,
            Ok(Err(failure)) => {
                let _ = tx.rollback().await;
                return review_error(
                    failure.code,
                    "confirmation_effects_failed",
                    &failure.message,
                );
            }
            Err(error) => return internal("confirmation effects", &error),
        };
    let payment_event_id = match insert_event(
        &mut tx,
        order_id,
        &ids::payment_aggregate_id(payment.id),
        payment_revision,
        &actor.0,
        "payment.confirmed",
        now,
    )
    .await
    {
        Ok(event_id) => event_id,
        Err(error) => return internal("payment event", &error),
    };
    if let Err(error) = insert_notification_intent(
        &mut tx,
        payment_event_id,
        "payment_confirmed",
        &confirmed_order.buyer_pubky,
        &actor.0,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await
    {
        return internal("confirmation notification", &error);
    }
    // The audit row: observation-derived facts, the constant basis, the
    // frozen observation. Inserted in the SAME transaction — an
    // unauthorised or losing call leaves no audit row behind.
    let confirmed_txid = observation["txid"].as_str().map(str::to_owned);
    let confirmed_amount_sats = observation["observed_sats"].as_i64();
    if let Err(error) = sqlx::query(
        "INSERT INTO paykit_seller_confirmations (order_id, payment_id, confirmed_by_pubky, \
         confirmed_at, confirmed_txid, confirmed_amount_sats, confirmed_reason, \
         confirmation_source, confirmation_basis, paykit_observation, event_id, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'seller', 'seller_attestation', $8, $9, $4)",
    )
    .bind(order_id)
    .bind(payment.id)
    .bind(&actor.0)
    .bind(now)
    .bind(&confirmed_txid)
    .bind(confirmed_amount_sats)
    .bind(&body.reason)
    .bind(&observation)
    .bind(receipt_event_id)
    .execute(&mut *tx)
    .await
    {
        return internal("confirmation audit", &error);
    }
    // The pinned resolve outbox row: stack identity AND address from the
    // order's bind-time pin, one-hour delivery deadline.
    if let Err(error) = enqueue_resolve_row(
        &mut tx,
        &order,
        payment.id,
        payment_event_id,
        "paid_manually",
        now,
    )
    .await
    {
        return internal("resolve outbox", &error);
    }
    let reviews = match crate::handlers::fetch_order_reviews(&mut tx, order_id).await {
        Ok(reviews) => reviews,
        Err(error) => return internal("order projection", &error),
    };
    match tx.commit().await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "order": crate::handlers::order_json_with_reviews(&confirmed_order, &reviews),
                "confirmation": {
                    "order_id": order_id,
                    "confirmed_by_pubky": actor.0,
                    "confirmed_at": format_timestamp(now),
                    "confirmed_txid": confirmed_txid,
                    "confirmed_amount_sats": confirmed_amount_sats,
                    "confirmed_reason": body.reason,
                    "confirmation_source": "seller",
                    "confirmation_basis": "seller_attestation",
                    "paykit_observation": observation,
                },
            })),
        )
            .into_response(),
        Err(error) => internal("confirmation commit", &error),
    }
}

/// Writes the one pinned `paykit.resolve` outbox row for an order, inside
/// the caller's transaction. Pins come from the order's bind-time record —
/// never from configuration read later.
async fn enqueue_resolve_row(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    payment_id: Uuid,
    event_id: Uuid,
    resolution: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let (Some(invoice_id), Some(stack_id), Some(endpoint)) = (
        order.paykit_invoice_id,
        order.paykit_stack_id.clone(),
        order.paykit_stack_endpoint.clone(),
    ) else {
        // Scope was validated by the caller (pin presence is a
        // precondition); reaching here is an invariant violation.
        return Err(sqlx::Error::Protocol(
            "resolve outbox row without a persisted paykit pin".to_string(),
        ));
    };
    sqlx::query(
        "INSERT INTO paykit_resolve_outbox (order_id, payment_id, event_id, invoice_id, \
         resolution, resolved_at, stack_id, stack_endpoint, next_attempt_at, delivery_deadline, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)",
    )
    .bind(order.id)
    .bind(payment_id)
    .bind(event_id)
    .bind(invoice_id)
    .bind(resolution)
    .bind(now)
    .bind(stack_id)
    .bind(endpoint)
    .bind(now)
    .bind(now + chrono::Duration::seconds(RESOLVE_DELIVERY_DEADLINE_SECONDS))
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Seller manual-review resolution (POST /v0/orders/{id}/bitcoin/resolve)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveBitcoinPaymentBody {
    outcome: String,
    #[serde(default)]
    reason: Option<String>,
    /// Validated external refund reference; REQUIRED for `refunded`,
    /// forbidden otherwise (the payments CHECK mirrors this).
    #[serde(default)]
    external_refund_reference: Option<String>,
}

/// The validated refund-reference shape (same rule as the fiat transaction
/// reference): 1–64 printable ASCII characters.
fn valid_refund_reference(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.chars().all(|c| c.is_ascii_graphic())
}

/// The canonical request hash behind the idempotency rule: same key + same
/// body replays the winner; same key + a different body conflicts.
fn resolve_request_hash(body: &ResolveBitcoinPaymentBody) -> String {
    let canonical = serde_json_canonicalizer::to_string(&json!({
        "outcome": body.outcome,
        "reason": body.reason,
        "external_refund_reference": body.external_refund_reference,
    }))
    .expect("resolve body canonicalizes");
    blake3::hash(canonical.as_bytes()).to_hex().to_string()
}

/// The one peer action on a `manual_review` payment. Authorisation is the
/// session pubky resolved order -> listing -> seller, BEFORE the
/// idempotency lookup; the body carries no actor, seller, txid, or amount.
pub async fn resolve_bitcoin_payment(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ResolveBitcoinPaymentBody>,
) -> Response {
    // Idempotency-Key is a UUID stored as `resolution_id`.
    let resolution_id = headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Uuid>().ok());
    let Some(resolution_id) = resolution_id else {
        return review_error(
            ErrorCode::InvalidCommand,
            "invalid_idempotency_key",
            "The Idempotency-Key header must be a UUID.",
        );
    };
    if !RESOLUTION_OUTCOMES.contains(&body.outcome.as_str()) {
        return review_error(
            ErrorCode::InvalidCommand,
            "invalid_outcome",
            "The outcome must be paid, refunded, or abandoned.",
        );
    }
    if body
        .reason
        .as_deref()
        .is_some_and(|reason| reason.len() > 500)
    {
        return review_error(
            ErrorCode::InvalidCommand,
            "invalid_reason",
            "The reason must be at most 500 characters.",
        );
    }
    match (body.outcome.as_str(), &body.external_refund_reference) {
        ("refunded", Some(reference)) if valid_refund_reference(reference) => {}
        ("refunded", _) => {
            return review_error(
                ErrorCode::InvalidCommand,
                "invalid_refund_reference",
                "A refunded resolution requires a valid external refund reference.",
            );
        }
        (_, None) => {}
        (_, Some(_)) => {
            return review_error(
                ErrorCode::InvalidCommand,
                "invalid_refund_reference",
                "Only a refunded resolution carries a refund reference.",
            );
        }
    }
    // (1) Authorise first (F13/F14 shape): buyer or unrelated seller -> 403
    // BEFORE the idempotency lookup.
    let (order, seller) = match listing_seller_for_order(&state.pool, order_id).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return order_not_found(),
        Err(error) => return internal("order lookup", &error),
    };
    if seller != actor.0 {
        tracing::info!(order_id = %order_id, "rejected a non-seller bitcoin resolve attempt");
        return not_order_seller();
    }
    // Scope (r12): Paykit Bitcoin only, with both pins persisted. Locks,
    // Stripe, PayPal, sandbox, and missing-pin rows are out of scope.
    if order.payment_method.as_deref() != Some("bitcoin") {
        return review_error(
            ErrorCode::InvalidState,
            "resolution_not_applicable",
            "Bitcoin resolution applies only to bitcoin-bound orders.",
        );
    }
    let pins_present = order.paykit_request_reference.is_some()
        && order.paykit_stack_id.is_some()
        && order.paykit_stack_endpoint.is_some();
    if !pins_present {
        return review_error(
            ErrorCode::InvalidState,
            "missing_pin",
            "This order has no pinned paykit stack to resolve against.",
        );
    }
    // (2) Idempotency: same key + same body replays the winner; same key +
    // a different body conflicts; a different key after resolution is
    // already_resolved.
    let request_hash = resolve_request_hash(&body);
    let existing: Option<(String, Value)> = match sqlx::query_as(
        "SELECT request_hash, response FROM paykit_manual_resolutions \
         WHERE order_id = $1 AND resolution_id = $2",
    )
    .bind(order_id)
    .bind(resolution_id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(existing) => existing,
        Err(error) => return internal("resolution lookup", &error),
    };
    if let Some((stored_hash, response)) = existing {
        if stored_hash == request_hash {
            return (StatusCode::OK, Json(response)).into_response();
        }
        return review_error(
            ErrorCode::IdempotencyConflict,
            "conflict",
            "The Idempotency-Key was already used with a different resolution.",
        );
    }
    let any_resolution: bool = match sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM paykit_manual_resolutions WHERE order_id = $1)",
    )
    .bind(order_id)
    .fetch_one(&state.pool)
    .await
    {
        Ok(exists) => exists,
        Err(error) => return internal("resolution lookup", &error),
    };
    if any_resolution {
        return review_error(
            ErrorCode::InvalidState,
            "already_resolved",
            "This order's payment was already resolved.",
        );
    }

    // (3) The shared CAS: the seller endpoint and the seven-day reaper
    // decide through the same predicate, so exactly one outcome exists.
    let now = state.clock.now();
    let input = ResolutionInput {
        outcome: &body.outcome,
        basis: "seller_attestation",
        resolved_by: Some(&actor.0),
        reason: body.reason.as_deref(),
        refund_reference: body.external_refund_reference.as_deref(),
        resolution_id,
        request_hash: &request_hash,
        event_actor: &actor.0,
    };
    match apply_manual_review_resolution(&state, order_id, &input, now).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(ResolutionFailure::AlreadyResolved) => review_error(
            ErrorCode::InvalidState,
            "already_resolved",
            "This order's payment was already resolved.",
        ),
        Err(ResolutionFailure::NotInManualReview) => review_error(
            ErrorCode::InvalidState,
            "not_in_manual_review",
            "This order's payment is not awaiting manual resolution.",
        ),
        Err(ResolutionFailure::StockUnavailable) => review_error(
            ErrorCode::InsufficientInventory,
            "stock_unavailable",
            "The order's inventory can no longer be reacquired; choose refunded or abandoned.",
        ),
        Err(ResolutionFailure::Internal(context, error)) => internal(&context, &error),
    }
}

/// Everything the shared resolution path needs, caller-independent: the
/// seller endpoint passes its body and identity; the inactivity reaper
/// passes the abandoned branch with a minted key and no resolver.
pub(crate) struct ResolutionInput<'a> {
    pub outcome: &'a str,
    pub basis: &'a str,
    pub resolved_by: Option<&'a str>,
    pub reason: Option<&'a str>,
    pub refund_reference: Option<&'a str>,
    pub resolution_id: Uuid,
    pub request_hash: &'a str,
    pub event_actor: &'a str,
}

pub(crate) enum ResolutionFailure {
    /// The CAS affected zero rows because a resolution already committed.
    AlreadyResolved,
    /// The payment is not an unresolved `manual_review` row (and no
    /// resolution exists): the precondition failure, distinct from the
    /// race loss above.
    NotInManualReview,
    /// The `paid` branch could not hold or reacquire the inventory
    /// (sold-out / drop / auction late cases): named 409, nothing changed.
    StockUnavailable,
    Internal(String, String),
}

/// The shared resolution transaction (seller endpoint and seven-day
/// reaper): one CAS decides the winner; the outcome effects, audit row,
/// event, and pinned outbox row commit with it. Lock order is payment ->
/// order -> listing/inventory, the canonical order every W1.15 path takes.
pub(crate) async fn apply_manual_review_resolution(
    state: &AppState,
    order_id: Uuid,
    input: &ResolutionInput<'_>,
    now: DateTime<Utc>,
) -> Result<Value, ResolutionFailure> {
    let pool = &state.pool;
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| ResolutionFailure::Internal("resolution transaction".into(), e.to_string()))?;
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1 FOR UPDATE"
    ))
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ResolutionFailure::Internal("payment lock".into(), e.to_string()))?;
    let Some(payment) = payment else {
        return Err(ResolutionFailure::Internal(
            "payment lock".into(),
            "payment row missing".into(),
        ));
    };
    if payment.adapter != "paykit" {
        return Err(ResolutionFailure::NotInManualReview);
    }
    if payment.state != "manual_review" {
        return Err(if payment.resolution_outcome.is_some() {
            ResolutionFailure::AlreadyResolved
        } else {
            ResolutionFailure::NotInManualReview
        });
    }
    let Some(order) = fetch_order_for_update(&mut tx, order_id)
        .await
        .map_err(|e| ResolutionFailure::Internal("order lock".into(), e.to_string()))?
    else {
        return Err(ResolutionFailure::Internal(
            "order lock".into(),
            "order row missing".into(),
        ));
    };
    let exit_state = match input.outcome {
        "paid" | "refunded" => "confirmed",
        _ => "expired",
    };
    // The decision: exactly one UPDATE may pass this predicate, ever.
    let cas: Option<(i64,)> = sqlx::query_as(
        "UPDATE payments SET state = $2, revision = revision + 1, \
         resolution_id = $3, resolution_outcome = $4, resolution_basis = $5, \
         resolved_at = $6, resolved_by_pubky = $7, refund_reference = $8, \
         manual_review_entered_at = NULL, updated_at = $6 \
         WHERE id = $1 AND state = 'manual_review' AND resolution_outcome IS NULL \
         RETURNING revision",
    )
    .bind(payment.id)
    .bind(exit_state)
    .bind(input.resolution_id)
    .bind(input.outcome)
    .bind(input.basis)
    .bind(now)
    .bind(input.resolved_by)
    .bind(input.refund_reference)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ResolutionFailure::Internal("resolution decision".into(), e.to_string()))?;
    let Some((payment_revision,)) = cas else {
        let _ = tx.rollback().await;
        return Err(ResolutionFailure::AlreadyResolved);
    };

    // Outcome effects (the frozen entry×outcome matrix). Any failure rolls
    // the CAS back with the rest — nothing partial commits.
    let updated_order =
        match apply_outcome_effects(&mut tx, state, &order, &payment, input, now).await {
            Ok(order) => order,
            Err(failure) => {
                let _ = tx.rollback().await;
                return Err(failure);
            }
        };

    // The payment event: `payment.confirmed` for paid (the unique index
    // enforces once); `payment.resolved` for refunded and abandoned.
    let event_kind = if input.outcome == "paid" {
        "payment.confirmed"
    } else {
        "payment.resolved"
    };
    let payment_event_id = insert_event(
        &mut tx,
        input.resolution_id,
        &ids::payment_aggregate_id(payment.id),
        payment_revision,
        input.event_actor,
        event_kind,
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("payment event".into(), e.to_string()))?;
    let notification_type = if input.outcome == "paid" {
        "payment_confirmed"
    } else {
        "payment_resolved"
    };
    insert_notification_intent(
        &mut tx,
        payment_event_id,
        notification_type,
        &order.buyer_pubky,
        input.event_actor,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("resolution notification".into(), e.to_string()))?;

    // The immutable observed-payment snapshot: what the refund amount
    // derives from — server-stored facts, never the request body.
    let snapshot = json!({
        "observation": order.paykit_observation,
        "payment_amount_minor": payment.amount_minor,
        "currency": payment.currency,
        "exponent": payment.exponent,
        "paykit_total_sats": order.paykit_total_sats,
    });

    let response = json!({
        "ok": true,
        "order": updated_order.projection(),
        "resolution": {
            "order_id": order_id,
            "resolution_id": input.resolution_id,
            "outcome": input.outcome,
            "basis": input.basis,
            "resolved_at": format_timestamp(now),
            "resolved_by_pubky": input.resolved_by,
        },
    });

    sqlx::query(
        "INSERT INTO paykit_manual_resolutions (order_id, payment_id, resolution_id, outcome, \
         basis, resolved_at, resolved_by_pubky, reason, refund_reference, \
         observed_payment_snapshot, request_hash, response, event_id, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $6)",
    )
    .bind(order_id)
    .bind(payment.id)
    .bind(input.resolution_id)
    .bind(input.outcome)
    .bind(input.basis)
    .bind(now)
    .bind(input.resolved_by)
    .bind(input.reason)
    .bind(input.refund_reference)
    .bind(&snapshot)
    .bind(input.request_hash)
    .bind(&response)
    .bind(payment_event_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ResolutionFailure::Internal("resolution audit".into(), e.to_string()))?;

    let paykit_resolution = match input.outcome {
        "paid" => "paid_manually",
        "refunded" => "refunded",
        _ => "abandoned",
    };
    enqueue_resolve_row(
        &mut tx,
        &order,
        payment.id,
        payment_event_id,
        paykit_resolution,
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("resolve outbox".into(), e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| ResolutionFailure::Internal("resolution commit".into(), e.to_string()))?;
    Ok(response)
}

/// The outcome's order/inventory effects, inside the caller's transaction.
/// Lock order after payment and order: drop row, then listing rows.
async fn apply_outcome_effects(
    tx: &mut Transaction<'_, Postgres>,
    state: &AppState,
    order: &OrderRow,
    payment: &PaymentRow,
    input: &ResolutionInput<'_>,
    now: DateTime<Utc>,
) -> Result<OrderRow, ResolutionFailure> {
    match input.outcome {
        "paid" => apply_paid_effects(tx, state, order, payment, input, now).await,
        "refunded" => apply_refunded_effects(tx, order, payment, input, now).await,
        _ => apply_abandoned_effects(tx, order, input, now).await,
    }
}

/// `paid`: payment confirmed with the existing confirmation effects. A
/// held order consumes its hold; a late-settled order (cancelled, stock
/// released) reacquires the same SKU quantity in lock order or fails with
/// `stock_unavailable` — the marketplace never invents inventory.
async fn apply_paid_effects(
    tx: &mut Transaction<'_, Postgres>,
    state: &AppState,
    order: &OrderRow,
    payment: &PaymentRow,
    input: &ResolutionInput<'_>,
    now: DateTime<Utc>,
) -> Result<OrderRow, ResolutionFailure> {
    match order.state.as_str() {
        "pending_payment" => {
            // Verify hold presence; if absent, atomically reacquire.
            if !hold_present(tx, order).await? {
                reacquire_hold(tx, order, now).await?;
            }
        }
        "cancelled" => {
            // Late settlement after hold expiry / late first observation:
            // reacquire, then the `cancelled -> paid` resolution edge.
            reacquire_hold(tx, order, now).await?;
        }
        _ => {
            return Err(ResolutionFailure::Internal(
                "paid effects".into(),
                format!("order in unexpected state {}", order.state),
            ));
        }
    }
    let order_for_confirm = fetch_order_for_update(tx, order.id)
        .await
        .map_err(|e| ResolutionFailure::Internal("order re-read".into(), e.to_string()))?
        .expect("order locked above");
    match crate::handlers::payment::confirm_order(
        tx,
        input.event_actor,
        input.resolution_id,
        payment,
        order_for_confirm,
        state.pickup.as_deref(),
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("confirmation effects".into(), e.to_string()))?
    {
        Ok((order, _receipt, _event)) => Ok(order),
        Err(failure) => {
            if failure.code == ErrorCode::InsufficientInventory
                || failure.code == ErrorCode::InvalidState
            {
                Err(ResolutionFailure::StockUnavailable)
            } else {
                Err(ResolutionFailure::Internal(
                    "confirmation effects".into(),
                    failure.message,
                ))
            }
        }
    }
}

/// Whether the order's hold is intact: its own reserved stock, or the
/// active winning reservation for an auction order.
async fn hold_present(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
) -> Result<bool, ResolutionFailure> {
    if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM reservations \
             WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 AND status = 'active')",
        )
        .bind(auction_aggregate_id)
        .bind(&order.buyer_pubky)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| ResolutionFailure::Internal("reservation check".into(), e.to_string()))?;
        Ok(active)
    } else {
        Ok(order.stock_held)
    }
}

/// Atomically reacquires the order's inventory in the normal lock order
/// (drop row, then listing rows), for the late-settlement `paid` branch.
/// Failure is always `stock_unavailable`: sold-out, drop-exhausted, and
/// auction-lapsed cases all land on the same named error.
async fn reacquire_hold(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    now: DateTime<Utc>,
) -> Result<(), ResolutionFailure> {
    // Drop-stamped orders re-debit their drop first (drop lock before
    // listing locks, the shared order).
    if let Some(drop_aggregate_id) = &order.drop_aggregate_id {
        let units: i64 = order
            .lines
            .as_array()
            .expect("order lines are an array")
            .iter()
            .map(|line| line["quantity"].as_i64().expect("line quantity"))
            .sum();
        let Some(drop) = crate::handlers::drops::fetch_drop_for_update(tx, drop_aggregate_id)
            .await
            .map_err(|e| ResolutionFailure::Internal("drop lock".into(), e.to_string()))?
        else {
            return Err(ResolutionFailure::StockUnavailable);
        };
        crate::handlers::drops::apply_time_transitions(tx, drop, inputless_command_id(), now)
            .await
            .map_err(|e| ResolutionFailure::Internal("drop transition".into(), e.to_string()))?;
        let debited = sqlx::query(
            "UPDATE drops SET remaining_quantity = remaining_quantity - $2, \
             revision = revision + 1, updated_at = $3 \
             WHERE aggregate_id = $1 AND remaining_quantity >= $2",
        )
        .bind(drop_aggregate_id)
        .bind(units)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(|e| ResolutionFailure::Internal("drop reacquire".into(), e.to_string()))?;
        if debited.rows_affected() != 1 {
            return Err(ResolutionFailure::StockUnavailable);
        }
        sqlx::query(
            "UPDATE drop_purchases SET quantity = quantity + $3 \
             WHERE drop_aggregate_id = $1 AND buyer_pubky = $2",
        )
        .bind(drop_aggregate_id)
        .bind(&order.buyer_pubky)
        .bind(units)
        .execute(&mut **tx)
        .await
        .map_err(|e| ResolutionFailure::Internal("drop counter".into(), e.to_string()))?;
    }

    if let Some(auction_aggregate_id) = &order.auction_aggregate_id {
        // Reacquire the lapsed/expired winning reservation and restore its
        // listing quantities; confirm_order then converts it.
        let reacquired = sqlx::query(
            "UPDATE reservations SET status = 'active', expires_at = $3, updated_at = $3 \
             WHERE listing_aggregate_id = $1 AND buyer_pubky = $2 AND status IN ('expired', 'released')",
        )
        .bind(auction_aggregate_id)
        .bind(&order.buyer_pubky)
        .bind(now + chrono::Duration::hours(1))
        .execute(&mut **tx)
        .await
        .map_err(|e| ResolutionFailure::Internal("reservation reacquire".into(), e.to_string()))?;
        if reacquired.rows_affected() != 1 {
            return Err(ResolutionFailure::StockUnavailable);
        }
    }

    // Each line: available -> reserved under the quantity guard (this also
    // covers the auction listing, whose quantities the reservation expiry
    // returned). The listing state follows the same rule as the ordinary
    // hold acquisition: fully reserved means `reserved`.
    let lines = order.lines.as_array().expect("order lines are an array");
    for line in lines {
        let aggregate_id = line["listing_aggregate_id"]
            .as_str()
            .expect("order line carries its listing aggregate id");
        let quantity = line["quantity"].as_i64().expect("line quantity");
        let reacquired = sqlx::query(
            "UPDATE listings SET server_revision = server_revision + 1, \
             state = CASE WHEN available_quantity = $2 THEN 'reserved' ELSE 'available' END, \
             available_quantity = available_quantity - $2, \
             reserved_quantity = reserved_quantity + $2, updated_at = $3 \
             WHERE aggregate_id = $1 AND available_quantity >= $2",
        )
        .bind(aggregate_id)
        .bind(quantity)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(|e| ResolutionFailure::Internal("listing reacquire".into(), e.to_string()))?;
        if reacquired.rows_affected() != 1 {
            return Err(ResolutionFailure::StockUnavailable);
        }
    }

    if order.auction_aggregate_id.is_none() {
        sqlx::query("UPDATE orders SET stock_held = true, updated_at = $2 WHERE id = $1")
            .bind(order.id)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(|e| ResolutionFailure::Internal("order hold flag".into(), e.to_string()))?;
    }
    Ok(())
}

/// A drop transition needs a command id for its event; reacquisition emits
/// none of its own, so a fresh id keeps the transition event traceable.
fn inputless_command_id() -> Uuid {
    Uuid::new_v4()
}

/// `refunded`: payment confirmed with `resolution_outcome='refunded'`, the
/// order moves to `refunded_external` with the validated reference
/// recorded, and the hold releases per the frozen matrix.
async fn apply_refunded_effects(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    payment: &PaymentRow,
    input: &ResolutionInput<'_>,
    now: DateTime<Utc>,
) -> Result<OrderRow, ResolutionFailure> {
    let mut current = order.clone();
    if current.state == "pending_payment" {
        current = cancel_held_order(
            tx,
            &current,
            input,
            "refunded by the seller after manual review",
            now,
        )
        .await?;
    }
    if current.state != "cancelled" {
        return Err(ResolutionFailure::Internal(
            "refund effects".into(),
            format!("order in unexpected state {}", current.state),
        ));
    }
    let amount_minor = order.paykit_total_sats.unwrap_or(payment.amount_minor);
    let external_refund = json!({
        "amount_minor": amount_minor,
        "transaction_id": input.refund_reference,
        "recorded_at": format_timestamp(now),
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'refunded_external', \
         external_refund = $3, updated_at = $4 WHERE id = $1 AND revision = $2 \
         RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(current.revision)
    .bind(&external_refund)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| ResolutionFailure::Internal("refund record".into(), e.to_string()))?;
    let event_id = insert_event(
        tx,
        input.resolution_id,
        &ids::order_aggregate_id(order.id),
        updated.revision,
        input.event_actor,
        "refund.recorded_external",
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("refund event".into(), e.to_string()))?;
    insert_notification_intent(
        tx,
        event_id,
        "refund_recorded",
        &order.buyer_pubky,
        input.event_actor,
        &ids::order_aggregate_id(order.id),
        None,
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("refund notification".into(), e.to_string()))?;
    Ok(updated)
}

/// `abandoned`: payment expired, the order cancels and releases its hold
/// (or stays cancelled/released), Paykit records `closed` via the pinned
/// resolve row.
async fn apply_abandoned_effects(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    input: &ResolutionInput<'_>,
    now: DateTime<Utc>,
) -> Result<OrderRow, ResolutionFailure> {
    match order.state.as_str() {
        "pending_payment" => {
            cancel_held_order(tx, order, input, "closed after manual review", now).await
        }
        "cancelled" => Ok(order.clone()),
        _ => Err(ResolutionFailure::Internal(
            "abandon effects".into(),
            format!("order in unexpected state {}", order.state),
        )),
    }
}

/// Cancels a held `pending_payment` order inside the caller's transaction:
/// the drop credit (stamped drop, drop lock first), the hold release (or
/// the auction reservation's compare-and-swap), and the state move with
/// its event. Mirrors `expire_held_order`'s inventory discipline.
async fn cancel_held_order(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    input: &ResolutionInput<'_>,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<OrderRow, ResolutionFailure> {
    if order.state != "pending_payment" {
        return Err(ResolutionFailure::Internal(
            "cancel effects".into(),
            "order is not pending payment".into(),
        ));
    }
    if order.auction_aggregate_id.is_some() {
        crate::handlers::cancellation::release_reserved_hold(tx, order, now)
            .await
            .map_err(|e| ResolutionFailure::Internal("reservation release".into(), e.to_string()))?
            .map_err(|failure| {
                ResolutionFailure::Internal("reservation release".into(), failure.message)
            })?;
    } else if order.stock_held {
        crate::handlers::cancellation::credit_order_drop(tx, order, now)
            .await
            .map_err(|e| ResolutionFailure::Internal("drop credit".into(), e.to_string()))?
            .map_err(|failure| {
                ResolutionFailure::Internal("drop credit".into(), failure.message)
            })?;
        release_lines(tx, order, HeldQuantity::Reserved, now)
            .await
            .map_err(|e| ResolutionFailure::Internal("hold release".into(), e.to_string()))?
            .map_err(|failure| {
                ResolutionFailure::Internal("hold release".into(), failure.message)
            })?;
    }
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'cancelled', \
         cancellation_reason = $3, stock_held = false, hold_expires_at = NULL, updated_at = $4 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(reason)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| ResolutionFailure::Internal("order cancel".into(), e.to_string()))?;
    let event_id = insert_event(
        tx,
        input.resolution_id,
        &ids::order_aggregate_id(order.id),
        updated.revision,
        input.event_actor,
        "order.cancelled",
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("cancel event".into(), e.to_string()))?;
    insert_notification_intent(
        tx,
        event_id,
        "order_cancelled",
        &order.buyer_pubky,
        input.event_actor,
        &ids::order_aggregate_id(order.id),
        None,
        now,
    )
    .await
    .map_err(|e| ResolutionFailure::Internal("cancel notification".into(), e.to_string()))?;
    Ok(updated)
}

// ---------------------------------------------------------------------------
// The 24-hour seller-confirmation window reaper
// ---------------------------------------------------------------------------

/// Routes every elapsed seller-confirmation window to `manual_review`: one
/// conditional UPDATE predicated on `awaiting_seller_confirmation` (first
/// committer wins against a seller confirm; a losing pass changes nothing),
/// then the payment CAS with the entry stamp. The hold is PRESERVED — the
/// buyer demonstrably paid on chain — and the seven-day inactivity clock
/// and two-business-day SLA start here.
pub async fn route_due_seller_confirmation_windows(
    state: &AppState,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let pool = &state.pool;
    let due: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM orders \
         WHERE paykit_request_state = 'awaiting_seller_confirmation' \
         AND paykit_seller_confirmation_deadline <= $1 \
         ORDER BY paykit_seller_confirmation_deadline LIMIT 100",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    let mut routed = 0u64;
    for (order_id,) in due {
        match route_one_seller_confirmation_window(state, order_id, now).await {
            Ok(true) => routed += 1,
            Ok(false) => {}
            Err(error) => {
                tracing::error!(
                    order_id = %order_id,
                    error = %error,
                    "seller-window routing failed for one order; continuing the batch"
                );
            }
        }
    }
    Ok(routed)
}

async fn route_one_seller_confirmation_window(
    state: &AppState,
    order_id: Uuid,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let pool = &state.pool;
    let mut tx = pool.begin().await?;
    // Canonical lock order: payment first, then the order CAS.
    let payment: Option<(Uuid, String)> =
        sqlx::query_as("SELECT id, buyer_pubky FROM payments WHERE order_id = $1 FOR UPDATE")
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((payment_id, buyer_pubky)) = payment else {
        anyhow::bail!("order {order_id} has no payment row");
    };
    // The decision: one conditional UPDATE predicated on the state. The
    // seller-confirm CAS is the same predicate, so first committer wins.
    let won = sqlx::query(
        "UPDATE orders SET paykit_request_state = 'confirmed', \
         paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL, hold_expires_at = NULL, updated_at = $2 \
         WHERE id = $1 AND paykit_request_state = 'awaiting_seller_confirmation'",
    )
    .bind(order_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if won.rows_affected() != 1 {
        tx.rollback().await?;
        return Ok(false);
    }
    let cas: Option<(i64,)> = sqlx::query_as(
        "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
         manual_review_entered_at = $2, updated_at = $2 \
         WHERE id = $1 AND state = 'awaiting_entitlement' RETURNING revision",
    )
    .bind(payment_id)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((payment_revision,)) = cas else {
        anyhow::bail!(
            "order {order_id} won the seller-window CAS but its payment was not awaiting_entitlement"
        );
    };
    let event_id = insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::payment_aggregate_id(payment_id),
        payment_revision,
        &buyer_pubky,
        "payment.manual_review",
        now,
    )
    .await?;
    // The seller must act; the buyer's item stays held.
    let seller_pubky: (String,) = sqlx::query_as("SELECT seller_pubky FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        "bitcoin_manual_review",
        &seller_pubky.0,
        SYSTEM_ACTOR,
        &ids::order_aggregate_id(order_id),
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    tracing::info!(
        order_id = %order_id,
        "seller-confirmation window elapsed; routed the payment to manual_review (hold preserved)"
    );
    Ok(true)
}

// ---------------------------------------------------------------------------
// The manual-review watch: two-business-day SLA alerts + seven-day reaper
// ---------------------------------------------------------------------------

/// One pass over unresolved Paykit `manual_review` payments: fires the
/// two-business-day seller-response SLA alert exactly once per entry, then
/// applies the seven-day inactivity `abandoned` branch through the SAME
/// CAS the seller's resolve endpoint uses. Returns (sla_alerts, abandoned).
pub async fn watch_manual_reviews(
    state: &AppState,
    now: DateTime<Utc>,
) -> anyhow::Result<(u64, u64)> {
    let pool = &state.pool;
    // Eligible scope (r12): Paykit adapter, bitcoin method, unresolved,
    // pins present. Read without row locks; each row's own CAS decides.
    let rows: Vec<(Uuid, DateTime<Utc>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT p.order_id, p.manual_review_entered_at, p.manual_review_sla_alerted_at \
         FROM payments p JOIN orders o ON o.id = p.order_id \
         WHERE p.adapter = 'paykit' AND o.payment_method = 'bitcoin' \
         AND p.state = 'manual_review' AND p.resolution_outcome IS NULL \
         AND o.paykit_request_reference IS NOT NULL \
         AND o.paykit_stack_id IS NOT NULL AND o.paykit_stack_endpoint IS NOT NULL \
         ORDER BY p.manual_review_entered_at LIMIT 100",
    )
    .fetch_all(pool)
    .await?;

    let mut alerts = 0u64;
    let mut abandoned = 0u64;
    for (order_id, entered_at, sla_alerted_at) in rows {
        // The SLA: alert on breach, once. It never transitions authority.
        if sla_alerted_at.is_none()
            && now >= add_business_days(entered_at, SELLER_RESPONSE_SLA_BUSINESS_DAYS)
        {
            let stamped = sqlx::query(
                "UPDATE payments SET manual_review_sla_alerted_at = $2 \
                 WHERE order_id = $1 AND state = 'manual_review' \
                 AND manual_review_sla_alerted_at IS NULL",
            )
            .bind(order_id)
            .bind(now)
            .execute(pool)
            .await?;
            if stamped.rows_affected() == 1 {
                tracing::error!(
                    order_id = %order_id,
                    "ALERT seller-response SLA breached: a bitcoin payment has waited in \
                     manual_review for two business days without a seller resolution"
                );
                alerts += 1;
            }
        }
        if entered_at > now - chrono::Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS) {
            continue;
        }
        let resolution_id = Uuid::new_v4();
        let request_hash = blake3::hash(b"seller_unresponsive:abandoned")
            .to_hex()
            .to_string();
        let input = ResolutionInput {
            outcome: "abandoned",
            basis: "seller_unresponsive",
            resolved_by: None,
            reason: Some("The seller did not respond within seven days."),
            refund_reference: None,
            resolution_id,
            request_hash: &request_hash,
            event_actor: SYSTEM_ACTOR,
        };
        match apply_manual_review_resolution(state, order_id, &input, now).await {
            Ok(_) => {
                abandoned += 1;
                tracing::info!(
                    order_id = %order_id,
                    "seven days without a seller response; recorded seller_unresponsive abandoned"
                );
            }
            Err(ResolutionFailure::AlreadyResolved) | Err(ResolutionFailure::NotInManualReview) => {
                // The seller resolved it between the scan and the CAS:
                // first committer won, the reaper changes nothing.
            }
            Err(failure) => {
                tracing::error!(
                    order_id = %order_id,
                    failure = ?std::mem::discriminant(&failure),
                    "inactivity abandonment failed for one order; continuing the batch"
                );
            }
        }
    }
    Ok((alerts, abandoned))
}
