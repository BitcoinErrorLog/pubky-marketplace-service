//! Verified PayPal `Refunded`, `Reversed`, and `Canceled_Reversal` IPNs
//! (docs/paypal-refund-ipn.md).
//!
//! PayPal already moved the money; this records the fact. Nothing verified
//! is discarded: a notification that matches an order's verified payment is
//! recorded on the order in every state, and one that cannot be matched or
//! validated lands in `gateway_refund_inbox`. Each PayPal `txn_id` is
//! recorded once. The order moves to `refunded_external` when the recorded
//! refunds reach its total from a state that holds a confirmed payment, and a
//! canceled reversal moves it back. A refund never restocks.

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use marketplace_domain::state_machines::{can_transition, order_machine};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{parse_gateway_amount_minor, PAYPAL_GATEWAY_ACTOR};
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_order_for_update, insert_notification_intent};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::AppState;

/// Order states holding a confirmed payment, from which recorded refunds
/// reaching the order total move the order to `refunded_external` (the
/// `paypal_refund` edges). A full refund in any other state is recorded and
/// flagged for the seller's review.
const FULL_REFUND_SOURCES: [&str; 10] = [
    "paid",
    "ready_for_pickup",
    "shipped",
    "delivered",
    "completed",
    "return_received",
    "cancel_requested",
    "cancelled",
    "return_requested",
    "return_approved",
];

/// IPN fields kept on an inbox row: enough to apply it later, and nothing
/// about the payer.
const INBOX_FIELDS: [&str; 10] = [
    "payment_status",
    "txn_id",
    "parent_txn_id",
    "custom",
    "mc_gross",
    "mc_currency",
    "receiver_email",
    "business",
    "receiver_id",
    "reason_code",
];

/// A PayPal transaction or account id as stored: 1–64 printable ASCII
/// characters.
pub(super) fn paypal_transaction_id(value: &str) -> Option<&str> {
    (!value.is_empty() && value.len() <= 64 && value.chars().all(|c| c.is_ascii_graphic()))
        .then_some(value)
}

/// A refund or reversal's `mc_gross` is negative, with exactly the
/// exponent's fraction digits.
fn negative_gross_minor(mc_gross: &str, exponent: i32) -> Option<i64> {
    let amount = parse_gateway_amount_minor(mc_gross.strip_prefix('-')?, exponent)?;
    (amount > 0).then_some(amount)
}

/// A canceled reversal's `mc_gross` is the positive amount restored.
fn positive_gross_minor(mc_gross: &str, exponent: i32) -> Option<i64> {
    parse_gateway_amount_minor(mc_gross, exponent).filter(|amount| *amount > 0)
}

/// Either address PayPal names as the payee matches the seller's configured
/// PayPal email, case-insensitively. Used when a payment is verified.
pub(super) fn paid_to_merchant(fields: &HashMap<String, String>, merchant_email: &str) -> bool {
    let merchant = merchant_email.to_ascii_lowercase();
    ["receiver_email", "business"].iter().any(|name| {
        fields
            .get(*name)
            .is_some_and(|value| value.to_ascii_lowercase() == merchant)
    })
}

/// A refund-class notification names the receiver its payment was verified
/// against: the same PayPal account id, or the same email.
fn paid_to_snapshot(
    fields: &HashMap<String, String>,
    receiver_email: &str,
    receiver_id: Option<&str>,
) -> bool {
    let same_account = receiver_id.is_some_and(|stored| {
        fields
            .get("receiver_id")
            .is_some_and(|value| !value.is_empty() && value == stored)
    });
    same_account || paid_to_merchant(fields, receiver_email)
}

/// Stores the verified payment's `txn_id` and receiver snapshot on the
/// order, once. A payment id already owned by another order is not reused.
pub(super) async fn record_verified_payment(
    pool: &PgPool,
    order_id: Uuid,
    txn_id: &str,
    merchant_email: &str,
    fields: &HashMap<String, String>,
) -> Result<(), sqlx::Error> {
    let receiver_id = fields
        .get("receiver_id")
        .and_then(|value| paypal_transaction_id(value));
    sqlx::query(
        "UPDATE orders SET paypal_txn_id = $2, paypal_receiver_email = $3, \
         paypal_receiver_id = $4 \
         WHERE id = $1 AND paypal_txn_id IS NULL \
           AND NOT EXISTS (SELECT 1 FROM orders d WHERE d.paypal_txn_id = $2)",
    )
    .bind(order_id)
    .bind(txn_id)
    .bind(merchant_email.to_ascii_lowercase())
    .bind(receiver_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Applies inbox rows that were waiting for this payment's `Completed` IPN.
pub(super) async fn apply_waiting_refunds(
    state: &AppState,
    parent_txn_id: &str,
) -> Result<(), sqlx::Error> {
    let waiting: Vec<(Value,)> = sqlx::query_as(
        "SELECT fields FROM gateway_refund_inbox \
         WHERE parent_txn_id = $1 AND reason = 'unknown_parent' AND resolved_at IS NULL \
         ORDER BY received_at, txn_id",
    )
    .bind(parent_txn_id)
    .fetch_all(&state.pool)
    .await?;
    for (stored,) in waiting {
        let fields: HashMap<String, String> = stored
            .as_object()
            .map(|object| {
                object
                    .iter()
                    .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let disposition = process(state, &fields, state.clock.now()).await?;
        tracing::info!(
            outcome = disposition.outcome(),
            "waiting paypal refund notification replayed"
        );
    }
    Ok(())
}

fn retry(context: &str, error: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %error, "{context} failed");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// Handles a postback-verified refund-class IPN. Everything verified is
/// persisted before PayPal gets its 200; a database failure answers 500 so
/// PayPal retries.
pub(super) async fn apply_refund_ipn(
    state: &AppState,
    fields: &HashMap<String, String>,
) -> Response {
    match process(state, fields, state.clock.now()).await {
        Ok(disposition) => {
            let outcome = disposition.outcome();
            match disposition {
                Disposition::Applied(_) | Disposition::Duplicate => {
                    tracing::info!(outcome, "paypal refund notification recorded");
                }
                Disposition::Inbox(_) | Disposition::Malformed => {
                    tracing::warn!(outcome, "paypal refund notification needs review");
                }
            }
            StatusCode::OK.into_response()
        }
        Err(error) => retry("paypal refund record", &error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayStatus {
    Refunded,
    Reversed,
    CanceledReversal,
}

impl GatewayStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "Refunded" => Some(Self::Refunded),
            "Reversed" => Some(Self::Reversed),
            "Canceled_Reversal" => Some(Self::CanceledReversal),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Refunded => "Refunded",
            Self::Reversed => "Reversed",
            Self::CanceledReversal => "Canceled_Reversal",
        }
    }
}

#[derive(Debug)]
enum Disposition {
    Applied(Effect),
    Duplicate,
    Inbox(&'static str),
    /// No `txn_id` to key a record on; PayPal always sends one.
    Malformed,
}

impl Disposition {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Applied(Effect::Partial) => "partial",
            Self::Applied(Effect::Full) => "full",
            Self::Applied(Effect::Restored) => "restored",
            Self::Applied(Effect::Review) => "recorded_for_review",
            Self::Duplicate => "duplicate",
            Self::Inbox(reason) => reason,
            Self::Malformed => "malformed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Partial,
    Full,
    Restored,
    Review,
}

async fn process(
    state: &AppState,
    fields: &HashMap<String, String>,
    now: DateTime<Utc>,
) -> Result<Disposition, sqlx::Error> {
    let field = |name: &str| fields.get(name).map(String::as_str).unwrap_or_default();
    let (Some(status), Some(txn_id)) = (
        GatewayStatus::parse(field("payment_status")),
        paypal_transaction_id(field("txn_id")),
    ) else {
        return Ok(Disposition::Malformed);
    };
    let recorded: Option<(Uuid,)> =
        sqlx::query_as("SELECT order_id FROM order_gateway_refunds WHERE refund_txn_id = $1")
            .bind(txn_id)
            .fetch_optional(&state.pool)
            .await?;
    if recorded.is_some() {
        return Ok(Disposition::Duplicate);
    }
    let custom_order = match field("custom").parse::<Uuid>() {
        Ok(order_id) => {
            let exists: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM orders WHERE id = $1")
                .bind(order_id)
                .fetch_optional(&state.pool)
                .await?;
            exists.map(|(id,)| id)
        }
        Err(_) => None,
    };
    let inbox = InboxEntry {
        txn_id,
        parent_txn_id: None,
        status,
        fields,
    };
    let Some(parent_txn_id) = paypal_transaction_id(field("parent_txn_id")) else {
        return inbox
            .store(&state.pool, "missing_parent", custom_order, now)
            .await;
    };
    let inbox = InboxEntry {
        parent_txn_id: Some(parent_txn_id),
        ..inbox
    };
    let owner: Option<(Uuid, String, Option<String>, String, i32)> = sqlx::query_as(
        "SELECT id, paypal_receiver_email, paypal_receiver_id, currency, exponent \
         FROM orders WHERE paypal_txn_id = $1",
    )
    .bind(parent_txn_id)
    .fetch_optional(&state.pool)
    .await?;
    let Some((order_id, receiver_email, receiver_id, currency, exponent)) = owner else {
        return inbox
            .store(&state.pool, "unknown_parent", custom_order, now)
            .await;
    };
    if custom_order.is_some_and(|custom| custom != order_id) {
        return inbox
            .store(&state.pool, "custom_mismatch", Some(order_id), now)
            .await;
    }
    if !paid_to_snapshot(fields, &receiver_email, receiver_id.as_deref()) {
        return inbox
            .store(&state.pool, "receiver_mismatch", Some(order_id), now)
            .await;
    }
    if field("mc_currency") != currency {
        return inbox
            .store(&state.pool, "currency_mismatch", Some(order_id), now)
            .await;
    }
    let amount = match status {
        GatewayStatus::CanceledReversal => positive_gross_minor(field("mc_gross"), exponent),
        GatewayStatus::Refunded | GatewayStatus::Reversed => {
            negative_gross_minor(field("mc_gross"), exponent)
        }
    };
    let Some(amount_minor) = amount else {
        return inbox
            .store(&state.pool, "amount_invalid", Some(order_id), now)
            .await;
    };
    record_on_order(
        state,
        &GatewayRecord {
            order_id,
            txn_id,
            parent_txn_id,
            status,
            amount_minor,
        },
        now,
    )
    .await
}

struct InboxEntry<'a> {
    txn_id: &'a str,
    parent_txn_id: Option<&'a str>,
    status: GatewayStatus,
    fields: &'a HashMap<String, String>,
}

impl InboxEntry<'_> {
    async fn store(
        &self,
        pool: &PgPool,
        reason: &'static str,
        order_id: Option<Uuid>,
        now: DateTime<Utc>,
    ) -> Result<Disposition, sqlx::Error> {
        let kept: serde_json::Map<String, Value> = INBOX_FIELDS
            .iter()
            .filter_map(|name| {
                self.fields
                    .get(*name)
                    .map(|value| (name.to_string(), json!(value)))
            })
            .collect();
        let inserted = sqlx::query(
            "INSERT INTO gateway_refund_inbox \
             (txn_id, parent_txn_id, payment_status, reason, order_id, fields, received_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (txn_id) DO NOTHING",
        )
        .bind(self.txn_id)
        .bind(self.parent_txn_id)
        .bind(self.status.as_str())
        .bind(reason)
        .bind(order_id)
        .bind(Value::Object(kept))
        .bind(now)
        .execute(pool)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(Disposition::Duplicate);
        }
        tracing::warn!(
            ?order_id,
            reason,
            "paypal refund notification held for review"
        );
        Ok(Disposition::Inbox(reason))
    }
}

struct GatewayRecord<'a> {
    order_id: Uuid,
    txn_id: &'a str,
    parent_txn_id: &'a str,
    status: GatewayStatus,
    amount_minor: i64,
}

#[derive(sqlx::FromRow)]
struct LedgerRow {
    refund_txn_id: String,
    payment_status: String,
    amount_minor: i64,
    from_state: Option<String>,
    from_return_state: Option<String>,
}

#[derive(Default)]
struct Totals {
    refunded: i64,
    reversed: i64,
    restored: i64,
}

impl Totals {
    fn add(&mut self, status: &str, amount: i64) {
        match status {
            "Refunded" => self.refunded += amount,
            "Reversed" => self.reversed += amount,
            _ => self.restored += amount,
        }
    }

    fn outstanding_reversal(&self) -> i64 {
        (self.reversed - self.restored).max(0)
    }

    /// Money PayPal returned to the buyer and did not take back.
    fn effective(&self) -> i64 {
        self.refunded + self.outstanding_reversal()
    }
}

/// Records one notification on its order under the order row lock, in every
/// order state: ledger row, running amount, reversal fields, state move or
/// review flag, event, and notifications.
async fn record_on_order(
    state: &AppState,
    record: &GatewayRecord<'_>,
    now: DateTime<Utc>,
) -> Result<Disposition, sqlx::Error> {
    let mut tx = state.pool.begin().await?;
    let Some(order) = fetch_order_for_update(&mut tx, record.order_id).await? else {
        return Ok(Disposition::Inbox("unknown_parent"));
    };
    let ledger: Vec<LedgerRow> = sqlx::query_as(
        "SELECT refund_txn_id, payment_status, amount_minor, from_state, from_return_state \
         FROM order_gateway_refunds WHERE order_id = $1 ORDER BY recorded_at, refund_txn_id",
    )
    .bind(order.id)
    .fetch_all(&mut *tx)
    .await?;
    if ledger.iter().any(|row| row.refund_txn_id == record.txn_id) {
        return Ok(Disposition::Duplicate);
    }
    // `external_refund` written by `refund.record_external` or a manual
    // review carries a transaction id no ledger row has.
    let manual_external = order
        .external_refund
        .as_ref()
        .and_then(|refund| refund["transaction_id"].as_str())
        .is_some_and(|txn| !ledger.iter().any(|row| row.refund_txn_id == txn));
    let mut before = Totals::default();
    for row in &ledger {
        before.add(&row.payment_status, row.amount_minor);
    }
    let mut after = Totals {
        refunded: before.refunded,
        reversed: before.reversed,
        restored: before.restored,
    };
    after.add(record.status.as_str(), record.amount_minor);

    let inserted = sqlx::query(
        "INSERT INTO order_gateway_refunds \
         (refund_txn_id, order_id, parent_txn_id, payment_status, amount_minor, recorded_at) \
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (refund_txn_id) DO NOTHING",
    )
    .bind(record.txn_id)
    .bind(order.id)
    .bind(record.parent_txn_id)
    .bind(record.status.as_str())
    .bind(record.amount_minor)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(Disposition::Duplicate);
    }
    sqlx::query(
        "UPDATE gateway_refund_inbox SET resolved_at = $2 \
         WHERE txn_id = $1 AND resolved_at IS NULL",
    )
    .bind(record.txn_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;

    let effective = after.effective();
    let running_refund = |txn: &str| {
        (effective > 0).then(|| {
            json!({
                "amount_minor": effective.min(order.total_minor),
                "transaction_id": txn,
                "recorded_at": format_timestamp(now),
            })
        })
    };
    let mut next = OrderChange {
        state: order.state.clone(),
        external_refund: order.external_refund.clone(),
        return_request: order.return_request.clone(),
        reversed_at: order.payment_reversed_at,
        reversal_cancelled_at: order.payment_reversal_cancelled_at,
        review: false,
    };
    let effect = match record.status {
        GatewayStatus::Refunded | GatewayStatus::Reversed => {
            let full = effective >= order.total_minor;
            let transition =
                full && !manual_external && FULL_REFUND_SOURCES.contains(&order.state.as_str());
            if !manual_external {
                next.external_refund = running_refund(record.txn_id);
            }
            if record.status == GatewayStatus::Reversed {
                next.reversed_at = Some(order.payment_reversed_at.unwrap_or(now));
            }
            next.review = manual_external || effective > order.total_minor || (full && !transition);
            if transition {
                debug_assert!(can_transition(
                    &order_machine(),
                    &order.state,
                    "refunded_external"
                ));
                let from_return_state = order
                    .return_request
                    .as_ref()
                    .and_then(|request| request["state"].as_str())
                    .map(str::to_string);
                sqlx::query(
                    "UPDATE order_gateway_refunds SET from_state = $2, from_return_state = $3 \
                     WHERE refund_txn_id = $1",
                )
                .bind(record.txn_id)
                .bind(&order.state)
                .bind(&from_return_state)
                .execute(&mut *tx)
                .await?;
                next.state = "refunded_external".to_string();
                next.return_request = set_return_state(&order.return_request, "refunded", now);
                Effect::Full
            } else if next.review {
                Effect::Review
            } else {
                Effect::Partial
            }
        }
        GatewayStatus::CanceledReversal => {
            let restored_something = before.outstanding_reversal() > 0;
            if restored_something {
                next.reversal_cancelled_at = Some(now);
            }
            if after.outstanding_reversal() == 0 {
                next.reversed_at = None;
            }
            if !manual_external {
                let latest = ledger
                    .iter()
                    .rev()
                    .find(|row| row.payment_status == "Refunded")
                    .or_else(|| {
                        ledger
                            .iter()
                            .rev()
                            .find(|row| row.payment_status == "Reversed")
                    })
                    .map(|row| row.refund_txn_id.as_str())
                    .unwrap_or(record.txn_id);
                next.external_refund = running_refund(latest);
            }
            let replaced = ledger.iter().rev().find(|row| row.from_state.is_some());
            let reopen = order.state == "refunded_external" && effective < order.total_minor;
            match (reopen, replaced, manual_external) {
                (true, Some(row), false) => {
                    let from_state = row.from_state.clone().unwrap_or_default();
                    debug_assert!(can_transition(
                        &order_machine(),
                        "refunded_external",
                        &from_state
                    ));
                    next.state = from_state;
                    if let Some(return_state) = &row.from_return_state {
                        next.return_request =
                            set_return_state(&order.return_request, return_state, now);
                    }
                    Effect::Restored
                }
                (true, _, _) => {
                    next.review = true;
                    Effect::Review
                }
                (false, _, _) if !restored_something => {
                    next.review = true;
                    Effect::Review
                }
                (false, _, _) => Effect::Partial,
            }
        }
    };

    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, external_refund = $4, \
         return_request = $5, payment_reversed_at = $6, payment_reversal_cancelled_at = $7, \
         gateway_refund_review_at = CASE WHEN $8 THEN COALESCE(gateway_refund_review_at, $9) \
                                         ELSE gateway_refund_review_at END, \
         updated_at = $9 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&next.state)
    .bind(&next.external_refund)
    .bind(&next.return_request)
    .bind(next.reversed_at)
    .bind(next.reversal_cancelled_at)
    .bind(next.review)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;

    let (event_kind, notification) = match (record.status, effect) {
        (GatewayStatus::CanceledReversal, _) => {
            ("refund.reversal_cancelled", "payment_reversal_cancelled")
        }
        (_, Effect::Full) => ("refund.recorded_external", "refund_recorded"),
        _ => ("refund.recorded_partial", "refund_recorded"),
    };
    finish(
        &mut tx,
        state,
        &updated,
        event_kind,
        notification,
        effect,
        now,
    )
    .await?;
    tx.commit().await?;
    Ok(Disposition::Applied(effect))
}

struct OrderChange {
    state: String,
    external_refund: Option<Value>,
    return_request: Option<Value>,
    reversed_at: Option<DateTime<Utc>>,
    reversal_cancelled_at: Option<DateTime<Utc>>,
    review: bool,
}

fn set_return_state(request: &Option<Value>, state: &str, now: DateTime<Utc>) -> Option<Value> {
    request.clone().map(|mut request| {
        request["state"] = json!(state);
        request["updated_at"] = json!(format_timestamp(now));
        request
    })
}

/// The event, the attestor annotation (`refunded` on a full refund,
/// `refund_reversal_cancelled` when a canceled reversal reopens the order),
/// and one notification to each participant.
async fn finish(
    tx: &mut Transaction<'_, Postgres>,
    state: &AppState,
    order: &OrderRow,
    event_kind: &str,
    notification: &str,
    effect: Effect,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let aggregate_id = ids::order_aggregate_id(order.id);
    let event_id = insert_event(
        tx,
        Uuid::new_v4(),
        &aggregate_id,
        order.revision,
        PAYPAL_GATEWAY_ACTOR,
        event_kind,
        now,
    )
    .await?;
    let annotation = match effect {
        Effect::Full => Some("refunded"),
        Effect::Restored => Some("refund_reversal_cancelled"),
        Effect::Partial | Effect::Review => None,
    };
    if let (Some(outcome), Some(attestor)) = (annotation, state.attestor.as_deref()) {
        crate::handlers::attestation::insert_annotation(tx, attestor, order.id, outcome, None, now)
            .await?;
    }
    for recipient in [&order.buyer_pubky, &order.seller_pubky] {
        insert_notification_intent(
            tx,
            event_id,
            notification,
            recipient,
            PAYPAL_GATEWAY_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        negative_gross_minor, paid_to_merchant, paid_to_snapshot, paypal_transaction_id,
        positive_gross_minor, Totals, FULL_REFUND_SOURCES,
    };
    use marketplace_domain::state_machines::{order_machine, Via};
    use std::collections::HashMap;

    fn fields(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    /// The handler's transition gate is exactly the contract's
    /// `paypal_refund` edges.
    #[test]
    fn full_refund_sources_are_exactly_the_paypal_refund_edges() {
        let mut edges: Vec<&str> = order_machine()
            .transitions
            .iter()
            .filter(|t| t.via.contains(&Via::Server("paypal_refund")))
            .map(|t| t.from)
            .collect();
        edges.sort_unstable();
        let mut gate = FULL_REFUND_SOURCES.to_vec();
        gate.sort_unstable();
        assert_eq!(gate, edges);
        for flagged in [
            "pending_payment",
            "processing",
            "closed",
            "refunded_external",
        ] {
            assert!(!FULL_REFUND_SOURCES.contains(&flagged), "{flagged}");
        }
    }

    #[test]
    fn a_canceled_reversal_offsets_only_reversed_money() {
        let mut totals = Totals::default();
        totals.add("Refunded", 4_000);
        totals.add("Reversed", 9_700);
        assert_eq!(totals.effective(), 13_700);
        totals.add("Canceled_Reversal", 9_700);
        assert_eq!(totals.outstanding_reversal(), 0);
        assert_eq!(totals.effective(), 4_000);
        // A restoration larger than what is reversed never offsets refunds.
        totals.add("Canceled_Reversal", 5_000);
        assert_eq!(totals.effective(), 4_000);
    }

    #[test]
    fn gross_amounts_parse_strictly_at_the_order_exponent() {
        assert_eq!(negative_gross_minor("-137.00", 2), Some(13_700));
        assert_eq!(negative_gross_minor("-0.01", 2), Some(1));
        assert_eq!(negative_gross_minor("-500", 0), Some(500));
        assert_eq!(positive_gross_minor("137.00", 2), Some(13_700));
        for invalid in [
            "137.00", "-0.00", "0.00", "--1.00", "-1.0", "-1.000", "-", "", "-1,00", "- 1.00",
            "-+1.00", "-1e2",
        ] {
            assert_eq!(negative_gross_minor(invalid, 2), None, "{invalid}");
        }
        for invalid in ["-137.00", "0.00", "1.0", ""] {
            assert_eq!(positive_gross_minor(invalid, 2), None, "{invalid}");
        }
    }

    #[test]
    fn transaction_ids_are_bounded_printable_ascii() {
        assert_eq!(
            paypal_transaction_id("7XP31449AB123456C"),
            Some("7XP31449AB123456C")
        );
        assert_eq!(paypal_transaction_id(""), None);
        assert_eq!(paypal_transaction_id("has space"), None);
        assert_eq!(paypal_transaction_id("tab\tid"), None);
        assert_eq!(paypal_transaction_id("é1234567"), None);
        assert!(paypal_transaction_id(&"A".repeat(64)).is_some());
        assert_eq!(paypal_transaction_id(&"A".repeat(65)), None);
    }

    #[test]
    fn either_payee_field_may_name_the_merchant() {
        let payee = |receiver: &str, business: &str| {
            fields(&[("receiver_email", receiver), ("business", business)])
        };
        assert!(paid_to_merchant(
            &payee("Merchant@Example.com", "other@example.com"),
            "merchant@example.com"
        ));
        assert!(paid_to_merchant(
            &payee("other@example.com", "merchant@example.com"),
            "merchant@example.com"
        ));
        assert!(!paid_to_merchant(
            &payee("other@example.com", "other@example.com"),
            "merchant@example.com"
        ));
        assert!(!paid_to_merchant(&HashMap::new(), "merchant@example.com"));
    }

    #[test]
    fn the_snapshot_matches_by_account_id_or_email() {
        let renamed = fields(&[
            ("receiver_email", "new@example.com"),
            ("receiver_id", "S8XGHLYDW9T3S"),
        ]);
        assert!(paid_to_snapshot(
            &renamed,
            "merchant@example.com",
            Some("S8XGHLYDW9T3S")
        ));
        assert!(!paid_to_snapshot(&renamed, "merchant@example.com", None));
        assert!(!paid_to_snapshot(
            &renamed,
            "merchant@example.com",
            Some("OTHERACCOUNT1")
        ));
        let empty_id = fields(&[("receiver_email", "new@example.com"), ("receiver_id", "")]);
        assert!(!paid_to_snapshot(
            &empty_id,
            "merchant@example.com",
            Some("")
        ));
        assert!(paid_to_snapshot(
            &fields(&[("business", "MERCHANT@example.com")]),
            "merchant@example.com",
            None
        ));
    }
}
