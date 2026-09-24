//! Verified PayPal `Refunded` and `Reversed` IPNs (docs/paypal-refund-ipn.md).
//!
//! The seller refunded in PayPal, or the buyer's dispute reversed the
//! payment: PayPal already moved the money, and this records the fact on the
//! order. Each refund `txn_id` is recorded once. The running sum lands on
//! `external_refund`; the order moves to `refunded_external` only when the
//! sum reaches the order total, and a partial refund keeps the order's state.
//! A refund never restocks.

use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use marketplace_domain::ids;
use marketplace_domain::state_machines::{can_transition, order_machine};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::{load_config, parse_gateway_amount_minor, PAYPAL_GATEWAY_ACTOR};
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_order_for_update, insert_notification_intent};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::AppState;

/// Order states a PayPal refund is recorded in: paid and not yet terminal,
/// with no open cancel or return request.
const REFUNDABLE_STATES: [&str; 6] = [
    "paid",
    "ready_for_pickup",
    "shipped",
    "delivered",
    "completed",
    "return_received",
];

/// A PayPal transaction id as stored: 1–64 printable ASCII characters.
pub(super) fn paypal_transaction_id(value: &str) -> Option<&str> {
    (!value.is_empty() && value.len() <= 64 && value.chars().all(|c| c.is_ascii_graphic()))
        .then_some(value)
}

/// The refunded amount in minor units: `mc_gross` is negative on a refund
/// or reversal, with exactly the exponent's fraction digits.
fn refund_amount_minor(mc_gross: &str, exponent: i32) -> Option<i64> {
    let amount = parse_gateway_amount_minor(mc_gross.strip_prefix('-')?, exponent)?;
    (amount > 0).then_some(amount)
}

/// Either address PayPal names as the payee matches the seller's configured
/// PayPal email, case-insensitively.
pub(super) fn paid_to_merchant(fields: &HashMap<String, String>, merchant_email: &str) -> bool {
    let merchant = merchant_email.to_ascii_lowercase();
    ["receiver_email", "business"].iter().any(|name| {
        fields
            .get(*name)
            .is_some_and(|value| value.to_ascii_lowercase() == merchant)
    })
}

fn acknowledge() -> Response {
    StatusCode::OK.into_response()
}

fn retry(context: &str, error: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %error, "{context} failed");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// Handles a postback-verified `Refunded` or `Reversed` IPN. Every
/// mismatch answers 200 (PayPal retries only non-2xx, and a mismatch never
/// becomes valid); a database failure answers 500 so PayPal retries.
pub(super) async fn apply_refund_ipn(
    state: &AppState,
    fields: &HashMap<String, String>,
) -> Response {
    let field = |name: &str| fields.get(name).map(String::as_str).unwrap_or_default();
    let reversal = field("payment_status") == "Reversed";
    let (Some(refund_txn_id), Some(parent_txn_id)) = (
        paypal_transaction_id(field("txn_id")),
        paypal_transaction_id(field("parent_txn_id")),
    ) else {
        tracing::warn!("paypal refund ipn dropped: malformed transaction ids");
        return acknowledge();
    };
    let order_id = match resolve_order(&state.pool, field("custom"), parent_txn_id).await {
        Ok(Some(order_id)) => order_id,
        Ok(None) => return acknowledge(),
        Err(error) => return retry("refund ipn order lookup", &error),
    };
    let order: Option<OrderRow> =
        match sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1"))
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await
        {
            Ok(order) => order,
            Err(error) => return retry("refund ipn order read", &error),
        };
    let Some(order) = order else {
        tracing::warn!(%order_id, "paypal refund ipn dropped: order not found");
        return acknowledge();
    };
    if order.payment_method.as_deref() != Some("paypal") {
        tracing::warn!(%order_id, "paypal refund ipn dropped: order is not paypal-bound");
        return acknowledge();
    }
    let config = match load_config(&state.pool, &order.seller_pubky).await {
        Ok(config) => config,
        Err(error) => return retry("refund ipn payment config read", &error),
    };
    let Some(merchant_email) = config.and_then(|config| config.paypal_merchant_email) else {
        tracing::warn!(%order_id, "paypal refund ipn dropped: seller has no paypal email configured");
        return acknowledge();
    };
    if !paid_to_merchant(fields, &merchant_email) {
        tracing::warn!(%order_id, "paypal refund ipn dropped: receiver is not the configured seller");
        return acknowledge();
    }
    if field("mc_currency") != order.currency {
        tracing::warn!(%order_id, "paypal refund ipn dropped: currency mismatch");
        return acknowledge();
    }
    let Some(amount_minor) = refund_amount_minor(field("mc_gross"), order.exponent) else {
        tracing::warn!(%order_id, "paypal refund ipn dropped: gross is not a negative amount");
        return acknowledge();
    };
    let refund = GatewayRefund {
        order_id,
        refund_txn_id,
        parent_txn_id,
        amount_minor,
        reversal,
    };
    match record_refund(state, &refund, state.clock.now()).await {
        Ok(outcome) => {
            match outcome {
                RefundOutcome::Partial | RefundOutcome::Full => {
                    tracing::info!(%order_id, ?outcome, reversal, "paypal refund recorded");
                }
                RefundOutcome::Duplicate => {
                    tracing::info!(%order_id, "paypal refund ipn already recorded");
                }
                RefundOutcome::StateRefused | RefundOutcome::ExceedsTotal => {
                    tracing::warn!(%order_id, ?outcome, "paypal refund ipn dropped");
                }
            }
            acknowledge()
        }
        Err(error) => retry("paypal refund record", &error),
    }
}

/// `custom` carries the order id on every checkout this service builds;
/// that order must own `parent_txn_id`. Without it, the one PayPal order
/// whose verified payment id is `parent_txn_id`.
async fn resolve_order(
    pool: &PgPool,
    custom: &str,
    parent_txn_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    if let Ok(order_id) = custom.parse::<Uuid>() {
        let owner: Option<(Option<String>,)> =
            sqlx::query_as("SELECT paypal_txn_id FROM orders WHERE id = $1")
                .bind(order_id)
                .fetch_optional(pool)
                .await?;
        return Ok(match owner {
            None => {
                tracing::warn!(%order_id, "paypal refund ipn dropped: order not found");
                None
            }
            Some((Some(stored),)) if stored == parent_txn_id => Some(order_id),
            Some(_) => {
                tracing::warn!(
                    %order_id,
                    "paypal refund ipn dropped: the order does not own the parent transaction"
                );
                None
            }
        });
    }
    let matches: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM orders WHERE paypal_txn_id = $1 AND payment_method = 'paypal' LIMIT 2",
    )
    .bind(parent_txn_id)
    .fetch_all(pool)
    .await?;
    Ok(match matches.as_slice() {
        [(order_id,)] => Some(*order_id),
        [] => {
            tracing::warn!("paypal refund ipn dropped: unknown parent transaction");
            None
        }
        _ => {
            tracing::warn!("paypal refund ipn dropped: parent transaction is ambiguous");
            None
        }
    })
}

struct GatewayRefund<'a> {
    order_id: Uuid,
    refund_txn_id: &'a str,
    parent_txn_id: &'a str,
    amount_minor: i64,
    reversal: bool,
}

#[derive(Debug)]
enum RefundOutcome {
    Partial,
    Full,
    Duplicate,
    StateRefused,
    ExceedsTotal,
}

/// Records one refund under the order row lock: dedup by refund `txn_id`,
/// state gate, running sum capped at the order total, then the order
/// update, its event, the attestor annotation on a full refund, and a
/// `refund_recorded` notification to each participant.
async fn record_refund(
    state: &AppState,
    refund: &GatewayRefund<'_>,
    now: DateTime<Utc>,
) -> Result<RefundOutcome, sqlx::Error> {
    let mut tx = state.pool.begin().await?;
    let Some(order) = fetch_order_for_update(&mut tx, refund.order_id).await? else {
        return Ok(RefundOutcome::StateRefused);
    };
    let recorded: Option<(Uuid,)> =
        sqlx::query_as("SELECT order_id FROM order_gateway_refunds WHERE refund_txn_id = $1")
            .bind(refund.refund_txn_id)
            .fetch_optional(&mut *tx)
            .await?;
    if recorded.is_some() {
        return Ok(RefundOutcome::Duplicate);
    }
    if !REFUNDABLE_STATES.contains(&order.state.as_str()) {
        return Ok(RefundOutcome::StateRefused);
    }
    let (prior_minor,): (i64,) = sqlx::query_as(
        "SELECT COALESCE(SUM(amount_minor), 0)::BIGINT FROM order_gateway_refunds \
         WHERE order_id = $1",
    )
    .bind(order.id)
    .fetch_one(&mut *tx)
    .await?;
    let Some(total_refunded) = prior_minor
        .checked_add(refund.amount_minor)
        .filter(|sum| *sum <= order.total_minor)
    else {
        return Ok(RefundOutcome::ExceedsTotal);
    };
    let inserted = sqlx::query(
        "INSERT INTO order_gateway_refunds \
         (refund_txn_id, order_id, parent_txn_id, payment_status, amount_minor, recorded_at) \
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (refund_txn_id) DO NOTHING",
    )
    .bind(refund.refund_txn_id)
    .bind(order.id)
    .bind(refund.parent_txn_id)
    .bind(if refund.reversal {
        "Reversed"
    } else {
        "Refunded"
    })
    .bind(refund.amount_minor)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(RefundOutcome::Duplicate);
    }

    let full = total_refunded == order.total_minor;
    let next_state = if full {
        debug_assert!(can_transition(
            &order_machine(),
            &order.state,
            "refunded_external"
        ));
        "refunded_external"
    } else {
        order.state.as_str()
    };
    let external_refund = json!({
        "amount_minor": total_refunded,
        "transaction_id": refund.refund_txn_id,
        "recorded_at": format_timestamp(now),
    });
    let return_request = order.return_request.clone().map(|mut request| {
        if full {
            request["state"] = json!("refunded");
            request["updated_at"] = json!(format_timestamp(now));
        }
        request
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, external_refund = $4, \
         return_request = $5, \
         payment_reversed_at = CASE WHEN $6 THEN COALESCE(payment_reversed_at, $7) \
                                    ELSE payment_reversed_at END, \
         updated_at = $7 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(next_state)
    .bind(&external_refund)
    .bind(&return_request)
    .bind(refund.reversal)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;

    let aggregate_id = ids::order_aggregate_id(updated.id);
    let event_id = insert_event(
        &mut tx,
        Uuid::new_v4(),
        &aggregate_id,
        updated.revision,
        PAYPAL_GATEWAY_ACTOR,
        if full {
            "refund.recorded_external"
        } else {
            "refund.recorded_partial"
        },
        now,
    )
    .await?;
    if full {
        if let Some(attestor) = state.attestor.as_deref() {
            crate::handlers::attestation::insert_annotation(
                &mut tx, attestor, updated.id, "refunded", None, now,
            )
            .await?;
        }
    }
    for recipient in [&updated.buyer_pubky, &updated.seller_pubky] {
        insert_notification_intent(
            &mut tx,
            event_id,
            "refund_recorded",
            recipient,
            PAYPAL_GATEWAY_ACTOR,
            &aggregate_id,
            None,
            now,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(if full {
        RefundOutcome::Full
    } else {
        RefundOutcome::Partial
    })
}

#[cfg(test)]
mod tests {
    use super::{paid_to_merchant, paypal_transaction_id, refund_amount_minor, REFUNDABLE_STATES};
    use marketplace_domain::state_machines::{order_machine, Via};
    use std::collections::HashMap;

    /// The handler's state gate is exactly the contract's `paypal_refund`
    /// edges, so `pending_payment`, `processing`, `closed`, and every
    /// cancel/return/terminal state are refused.
    #[test]
    fn refundable_states_are_exactly_the_paypal_refund_edges() {
        let mut edges: Vec<&str> = order_machine()
            .transitions
            .iter()
            .filter(|t| t.via.contains(&Via::Server("paypal_refund")))
            .map(|t| t.from)
            .collect();
        edges.sort_unstable();
        let mut gate = REFUNDABLE_STATES.to_vec();
        gate.sort_unstable();
        assert_eq!(gate, edges);
        for refused in [
            "pending_payment",
            "processing",
            "cancel_requested",
            "cancelled",
            "return_requested",
            "return_approved",
            "refunded_external",
            "closed",
        ] {
            assert!(!REFUNDABLE_STATES.contains(&refused), "{refused}");
        }
    }

    #[test]
    fn refund_amounts_are_negative_gross_at_the_order_exponent() {
        assert_eq!(refund_amount_minor("-137.00", 2), Some(13_700));
        assert_eq!(refund_amount_minor("-0.01", 2), Some(1));
        assert_eq!(refund_amount_minor("-500", 0), Some(500));
        for invalid in [
            "137.00", "-0.00", "0.00", "--1.00", "-1.0", "-1.000", "-", "", "-1,00", "- 1.00",
            "-+1.00", "-1e2",
        ] {
            assert_eq!(refund_amount_minor(invalid, 2), None, "{invalid}");
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
        let fields = |receiver: &str, business: &str| -> HashMap<String, String> {
            HashMap::from([
                ("receiver_email".to_string(), receiver.to_string()),
                ("business".to_string(), business.to_string()),
            ])
        };
        assert!(paid_to_merchant(
            &fields("Merchant@Example.com", "other@example.com"),
            "merchant@example.com"
        ));
        assert!(paid_to_merchant(
            &fields("other@example.com", "merchant@example.com"),
            "merchant@example.com"
        ));
        assert!(!paid_to_merchant(
            &fields("other@example.com", "other@example.com"),
            "merchant@example.com"
        ));
        assert!(!paid_to_merchant(&HashMap::new(), "merchant@example.com"));
    }
}
