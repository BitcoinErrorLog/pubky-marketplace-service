//! Seller payment-method configuration and per-order method binding
//! (`/v0/sellers/*/payment-config`, `/v0/orders/{id}/payment-method`,
//! `/v0/orders/{id}/fiat/*`).
//!
//! Money-state rules:
//! - Binding is one-shot per order: re-binding the SAME method is an
//!   idempotent no-op; a different method is refused once one is bound
//!   (switching rails after a payment request or checkout URL exists could
//!   double-collect).
//! - The Stripe leg is processor-verified: the service lists the seller's
//!   recent Checkout Sessions with their stored restricted key and matches
//!   `client_reference_id == order id` plus exact amount and currency.
//! - The PayPal leg is seller-attested: the buyer reports payment, the
//!   seller confirms receipt. The projection exposes the difference as
//!   `fiat_verification: 'processor' | 'seller-attested'`.
//! - Payment confirmation reuses the sandbox/Locks confirmation path
//!   (`confirm_order`): receipt exactly once, inventory reserved → sold,
//!   payment CAS `awaiting_entitlement → confirmed`. A confirmation the
//!   order can no longer accept routes the payment to `manual_review`,
//!   mirroring the Locks worker — real money is never silently dropped.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::{ids, ErrorCode};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    fetch_order_for_update, fetch_order_reviews, insert_notification_intent,
    order_json_with_reviews,
};
use crate::model::{OrderRow, PaymentRow};
use crate::payments::{
    order_reference, validate_paypal_email, validate_stripe_payment_link,
    validate_stripe_restricted_key, PaykitRequestError, PaymentsRuntime, StripeError,
};
use crate::queries::PAYMENT_COLUMNS;
use crate::AppState;

const CONFIG_COLUMNS: &str = "seller_pubky, bitcoin_enabled, stripe_payment_link, \
     stripe_restricted_key_ciphertext, paypal_merchant_email, created_at, updated_at";

#[derive(Debug, sqlx::FromRow)]
struct SellerPaymentConfigRow {
    #[allow(dead_code)]
    seller_pubky: String,
    bitcoin_enabled: bool,
    stripe_payment_link: Option<String>,
    stripe_restricted_key_ciphertext: Option<Vec<u8>>,
    paypal_merchant_email: Option<String>,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Machine-readable failure: the standard command error envelope plus a
/// `reason` sub-code the client can branch on (e.g. `stripe_key_invalid`).
fn method_error(code: ErrorCode, reason: &str, message: &str) -> Response {
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

/// The payment-methods surface is enabled only when the deployment carries
/// the payments runtime (`STRIPE_KEY_ENCRYPTION_KEY`); fail closed otherwise.
fn payments_runtime(state: &AppState) -> Result<std::sync::Arc<PaymentsRuntime>, Box<Response>> {
    state.payments.clone().ok_or_else(|| {
        Box::new(method_error(
            ErrorCode::UpstreamUnavailable,
            "payments_disabled",
            "Payment methods are not enabled on this deployment.",
        ))
    })
}

async fn load_config(
    pool: &sqlx::PgPool,
    seller_pubky: &str,
) -> Result<Option<SellerPaymentConfigRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONFIG_COLUMNS} FROM seller_payment_configs WHERE seller_pubky = $1"
    ))
    .bind(seller_pubky)
    .fetch_optional(pool)
    .await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutPaymentConfigBody {
    bitcoin_enabled: bool,
    /// Absent or null clears the stored link.
    #[serde(default)]
    stripe_payment_link: Option<String>,
    /// Write-only. Absent or null PRESERVES the stored key (the client can
    /// never read it back to re-send it); an empty string clears it.
    #[serde(default)]
    stripe_restricted_key: Option<String>,
    /// Absent or null clears the stored email.
    #[serde(default)]
    paypal_merchant_email: Option<String>,
}

fn config_view(
    bitcoin_enabled: bool,
    stripe_payment_link: &Option<String>,
    paypal_merchant_email: &Option<String>,
    stripe_restricted_key_set: bool,
    updated_at: DateTime<Utc>,
) -> Value {
    json!({
        "bitcoin_enabled": bitcoin_enabled,
        "stripe_payment_link": stripe_payment_link,
        "paypal_merchant_email": paypal_merchant_email,
        "stripe_restricted_key_set": stripe_restricted_key_set,
        "updated_at": format_timestamp(updated_at),
    })
}

/// `PUT /v0/sellers/me/payment-config` (seller session): full replace of the
/// seller's rail configuration, except the write-only restricted key, which
/// only an explicit value changes. The restricted key is never returned.
pub async fn put_payment_config(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(body): Json<PutPaymentConfigBody>,
) -> Response {
    let payments = match payments_runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    if let Some(link) = &body.stripe_payment_link {
        if let Err(message) = validate_stripe_payment_link(link) {
            return method_error(ErrorCode::InvalidCommand, "invalid_payment_link", message);
        }
    }
    if let Some(email) = &body.paypal_merchant_email {
        if let Err(message) = validate_paypal_email(email) {
            return method_error(ErrorCode::InvalidCommand, "invalid_paypal_email", message);
        }
    }
    // None = preserve; Some(None) = clear; Some(Some(ciphertext)) = replace.
    let restricted_key_update: Option<Option<Vec<u8>>> = match body.stripe_restricted_key.as_deref()
    {
        None => None,
        Some("") => Some(None),
        Some(key) => {
            if let Err(message) = validate_stripe_restricted_key(key) {
                return method_error(ErrorCode::InvalidCommand, "invalid_restricted_key", message);
            }
            Some(Some(payments.stripe_key_cipher.encrypt(&actor.0, key)))
        }
    };
    let now = state.clock.now();
    let stored: Result<SellerPaymentConfigRow, sqlx::Error> = match restricted_key_update {
        None => {
            sqlx::query_as(&format!(
                "INSERT INTO seller_payment_configs \
                 (seller_pubky, bitcoin_enabled, stripe_payment_link, paypal_merchant_email, \
                  created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5) \
                 ON CONFLICT (seller_pubky) DO UPDATE SET \
                 bitcoin_enabled = EXCLUDED.bitcoin_enabled, \
                 stripe_payment_link = EXCLUDED.stripe_payment_link, \
                 paypal_merchant_email = EXCLUDED.paypal_merchant_email, \
                 updated_at = EXCLUDED.updated_at \
                 RETURNING {CONFIG_COLUMNS}"
            ))
            .bind(&actor.0)
            .bind(body.bitcoin_enabled)
            .bind(&body.stripe_payment_link)
            .bind(&body.paypal_merchant_email)
            .bind(now)
            .fetch_one(&state.pool)
            .await
        }
        Some(ciphertext) => {
            sqlx::query_as(&format!(
                "INSERT INTO seller_payment_configs \
                 (seller_pubky, bitcoin_enabled, stripe_payment_link, \
                  stripe_restricted_key_ciphertext, paypal_merchant_email, created_at, \
                  updated_at) VALUES ($1, $2, $3, $4, $5, $6, $6) \
                 ON CONFLICT (seller_pubky) DO UPDATE SET \
                 bitcoin_enabled = EXCLUDED.bitcoin_enabled, \
                 stripe_payment_link = EXCLUDED.stripe_payment_link, \
                 stripe_restricted_key_ciphertext = EXCLUDED.stripe_restricted_key_ciphertext, \
                 paypal_merchant_email = EXCLUDED.paypal_merchant_email, \
                 updated_at = EXCLUDED.updated_at \
                 RETURNING {CONFIG_COLUMNS}"
            ))
            .bind(&actor.0)
            .bind(body.bitcoin_enabled)
            .bind(&body.stripe_payment_link)
            .bind(&ciphertext)
            .bind(&body.paypal_merchant_email)
            .bind(now)
            .fetch_one(&state.pool)
            .await
        }
    };
    match stored {
        Ok(row) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "payment_config": config_view(
                    row.bitcoin_enabled,
                    &row.stripe_payment_link,
                    &row.paypal_merchant_email,
                    row.stripe_restricted_key_ciphertext.is_some(),
                    row.updated_at,
                ),
            })),
        )
            .into_response(),
        Err(error) => internal("payment config upsert", &error),
    }
}

/// `GET /v0/sellers/me/payment-config` (seller session): the seller's own
/// stored configuration, exactly as the PUT returns it. This is the raw row
/// (`bitcoin_enabled` as stored, no paykit availability lookup) so the
/// settings page can load state on mount. The restricted key is never
/// returned, only `stripe_restricted_key_set`.
pub async fn get_own_payment_config(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> Response {
    match load_config(&state.pool, &actor.0).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(json!({
                "payment_config": config_view(
                    row.bitcoin_enabled,
                    &row.stripe_payment_link,
                    &row.paypal_merchant_email,
                    row.stripe_restricted_key_ciphertext.is_some(),
                    row.updated_at,
                ),
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(json!({ "payment_config": Value::Null })),
        )
            .into_response(),
        Err(error) => internal("payment config read", &error),
    }
}

/// `GET /v0/sellers/{pubky}/payment-config` (public): the buyer-facing rail
/// availability. `bitcoin_available` is true only when the seller enabled it
/// AND their watch-only account is actually claimed on paykit-server; a
/// paykit outage is reported as 503 rather than silently `false`.
pub async fn get_payment_config(
    State(state): State<AppState>,
    Path(seller_pubky): Path<String>,
) -> Response {
    if !marketplace_domain::pubky::is_valid_pubky(&seller_pubky) {
        return method_error(
            ErrorCode::InvalidCommand,
            "invalid_pubky",
            "The seller pubky is invalid.",
        );
    }
    let config = match load_config(&state.pool, &seller_pubky).await {
        Ok(config) => config,
        Err(error) => return internal("payment config read", &error),
    };
    let Some(config) = config else {
        return (
            StatusCode::OK,
            Json(json!({
                "bitcoin_available": false,
                "stripe_payment_link": Value::Null,
                "paypal_merchant_email": Value::Null,
            })),
        )
            .into_response();
    };
    let bitcoin_available =
        if config.bitcoin_enabled {
            let paykit = state
                .payments
                .as_ref()
                .and_then(|payments| payments.paykit.as_ref());
            match paykit {
                Some(paykit) => match paykit.account_exists(&seller_pubky).await {
                    Ok(claimed) => claimed,
                    Err(_) => return method_error(
                        ErrorCode::UpstreamUnavailable,
                        "paykit_unavailable",
                        "The Paykit server could not be reached to confirm Bitcoin availability.",
                    ),
                },
                None => false,
            }
        } else {
            false
        };
    (
        StatusCode::OK,
        Json(json!({
            "bitcoin_available": bitcoin_available,
            "stripe_payment_link": config.stripe_payment_link,
            "paypal_merchant_email": config.paypal_merchant_email,
        })),
    )
        .into_response()
}

async fn fetch_payment_for_order_update(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<PaymentRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1 FOR UPDATE"
    ))
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
}

async fn order_response(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    extra: Value,
) -> Result<Response, sqlx::Error> {
    let reviews = fetch_order_reviews(tx, order.id).await?;
    let mut body = json!({ "ok": true, "order": order_json_with_reviews(order, &reviews) });
    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            body[key] = value;
        }
    }
    Ok((StatusCode::OK, Json(body)).into_response())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindPaymentMethodBody {
    method: String,
}

/// Builds the PayPal "Buy Now" (`_xclick`) checkout URL: seller-direct,
/// no platform credentials, with the order id in `custom` for the buyer's
/// later report.
fn paypal_checkout_url(merchant_email: &str, order: &OrderRow) -> Result<String, &'static str> {
    if order.exponent < 0 || order.exponent > 4 {
        return Err("the order currency exponent is outside PayPal's supported range");
    }
    let scale = 10_i64.pow(order.exponent as u32);
    let amount = format!(
        "{}.{:0width$}",
        order.total_minor / scale,
        order.total_minor % scale,
        width = order.exponent as usize
    );
    let mut url =
        url::Url::parse("https://www.paypal.com/cgi-bin/webscr").expect("static PayPal URL parses");
    url.query_pairs_mut()
        .append_pair("cmd", "_xclick")
        .append_pair("business", merchant_email)
        .append_pair("item_name", &format!("Order {}", order.id))
        .append_pair("amount", &amount)
        .append_pair("currency_code", &order.currency)
        .append_pair("custom", &order.id.to_string());
    Ok(url.to_string())
}

fn is_fiat_currency(order: &OrderRow) -> bool {
    order.currency.len() == 3
        && order.currency.chars().all(|c| c.is_ascii_uppercase())
        && !matches!(order.currency.as_str(), "SAT" | "BTC" | "XBT")
}

/// The satoshi amount of a bitcoin-denominated order: `SAT` (exponent 0)
/// and `BTC` (exponent 8) totals are both already in satoshi minor units.
fn bitcoin_amount_sats(order: &OrderRow) -> Option<u64> {
    let supported = matches!(
        (order.currency.as_str(), order.exponent),
        ("SAT", 0) | ("BTC", 8)
    );
    if !supported {
        return None;
    }
    u64::try_from(order.total_minor)
        .ok()
        .filter(|sats| *sats > 0)
}

/// `POST /v0/orders/{id}/payment-method` (buyer session): binds exactly one
/// available method to a pending order. Bitcoin binding creates the Paykit
/// payment request inside the same transaction: if paykit-server refuses,
/// nothing is bound.
pub async fn bind_payment_method(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    Json(body): Json<BindPaymentMethodBody>,
) -> Response {
    let payments = match payments_runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    let method = body.method.as_str();
    if !matches!(method, "bitcoin" | "stripe" | "paypal") {
        return method_error(
            ErrorCode::InvalidCommand,
            "invalid_method",
            "The payment method must be bitcoin, stripe, or paypal.",
        );
    }
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal("payment method transaction", &error),
    };
    let payment = match fetch_payment_for_order_update(&mut tx, order_id).await {
        Ok(Some(payment)) => payment,
        Ok(None) => {
            return method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        }
        Err(error) => return internal("payment lookup", &error),
    };
    let order = match fetch_order_for_update(&mut tx, order_id).await {
        Ok(Some(order)) => order,
        Ok(None) => {
            return method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        }
        Err(error) => return internal("order lookup", &error),
    };
    if order.buyer_pubky != actor.0 {
        return method_error(
            ErrorCode::Unauthorized,
            "not_buyer",
            "Only the buyer may bind the payment method.",
        );
    }
    if let Some(bound) = &order.payment_method {
        if bound == method {
            // Idempotent re-bind of the same method.
            return match order_response(&mut tx, &order, json!({})).await {
                Ok(response) => response,
                Err(error) => internal("order projection", &error),
            };
        }
        return method_error(
            ErrorCode::InvalidState,
            "payment_method_already_bound",
            "A payment method is already bound to this order.",
        );
    }
    if order.state != "pending_payment" {
        return method_error(
            ErrorCode::InvalidState,
            "order_not_pending",
            "Only an order pending payment can bind a payment method.",
        );
    }
    if payment.state != "awaiting_entitlement" {
        return method_error(
            ErrorCode::InvalidState,
            "payment_not_awaiting",
            "The payment is no longer awaiting a method.",
        );
    }
    if payment.adapter == "locks" {
        return method_error(
            ErrorCode::InvalidState,
            "locks_managed",
            "A Locks-correlated payment advances only by server-side verification.",
        );
    }
    let config = match load_config(&state.pool, &order.seller_pubky).await {
        Ok(config) => config,
        Err(error) => return internal("payment config read", &error),
    };

    // The bind lock point: choosing a real rail is the payment start, so it
    // acquires the order's inventory hold and arms the fiat payment window
    // (all three rails; the paykit worker and both fiat verification legs
    // confirm against this hold).
    let order = match crate::handlers::holds::acquire_payment_hold(
        &mut tx,
        order,
        state.config.fiat_payment_window_seconds,
        now,
    )
    .await
    {
        Ok(Ok(order)) => order,
        Ok(Err(failure)) => {
            let reason = if failure.code == ErrorCode::InsufficientInventory {
                "sold_out"
            } else {
                "hold_unavailable"
            };
            return method_error(failure.code, reason, &failure.message);
        }
        Err(error) => return internal("payment hold", &error),
    };

    let (fiat_checkout_url, paykit_reference, adapter): (Option<String>, Option<String>, &str) =
        match method {
            "bitcoin" => {
                let enabled = config.as_ref().is_some_and(|config| config.bitcoin_enabled);
                if !enabled {
                    return method_error(
                        ErrorCode::InvalidState,
                        "method_unavailable",
                        "The seller does not accept Bitcoin.",
                    );
                }
                if payments.paykit.is_none() {
                    return method_error(
                        ErrorCode::UpstreamUnavailable,
                        "bitcoin_unavailable",
                        "Bitcoin payments are not enabled on this deployment.",
                    );
                }
                if bitcoin_amount_sats(&order).is_none() {
                    return method_error(
                        ErrorCode::InvalidState,
                        "currency_unsupported",
                        "Bitcoin payment requires a SAT- or BTC-denominated order.",
                    );
                }
                (None, Some(order_reference(order.id)), "paykit")
            }
            "stripe" => {
                let Some(link) = config
                    .as_ref()
                    .and_then(|config| config.stripe_payment_link.clone())
                else {
                    return method_error(
                        ErrorCode::InvalidState,
                        "method_unavailable",
                        "The seller has no Stripe payment link configured.",
                    );
                };
                if !is_fiat_currency(&order) {
                    return method_error(
                        ErrorCode::InvalidState,
                        "currency_unsupported",
                        "Stripe payment requires a fiat-denominated order.",
                    );
                }
                let separator = if link.contains('?') { '&' } else { '?' };
                (
                    Some(format!("{link}{separator}client_reference_id={}", order.id)),
                    None,
                    "stripe",
                )
            }
            "paypal" => {
                let Some(email) = config
                    .as_ref()
                    .and_then(|config| config.paypal_merchant_email.clone())
                else {
                    return method_error(
                        ErrorCode::InvalidState,
                        "method_unavailable",
                        "The seller has no PayPal merchant email configured.",
                    );
                };
                if !is_fiat_currency(&order) {
                    return method_error(
                        ErrorCode::InvalidState,
                        "currency_unsupported",
                        "PayPal payment requires a fiat-denominated order.",
                    );
                }
                match paypal_checkout_url(&email, &order) {
                    Ok(url) => (Some(url), None, "paypal"),
                    Err(message) => {
                        return method_error(
                            ErrorCode::InvalidState,
                            "currency_unsupported",
                            message,
                        )
                    }
                }
            }
            _ => unreachable!("method validated above"),
        };

    let updated_order: OrderRow = match sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, payment_method = $2, \
         fiat_checkout_url = $3, paykit_request_reference = $4, \
         paykit_request_state = CASE WHEN $4::text IS NULL THEN NULL ELSE 'pending' END, \
         updated_at = $5 WHERE id = $1 RETURNING {}",
        crate::queries::ORDER_COLUMNS
    ))
    .bind(order.id)
    .bind(method)
    .bind(&fiat_checkout_url)
    .bind(&paykit_reference)
    .bind(now)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(order) => order,
        Err(error) => return internal("payment method update", &error),
    };
    if let Err(error) = sqlx::query(
        "UPDATE payments SET revision = revision + 1, adapter = $2, updated_at = $3 WHERE id = $1",
    )
    .bind(payment.id)
    .bind(adapter)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        return internal("payment adapter update", &error);
    }
    let event_id = match insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order.id),
        updated_order.revision,
        &actor.0,
        "order.payment_method_bound",
        now,
    )
    .await
    {
        Ok(event_id) => event_id,
        Err(error) => return internal("payment method event", &error),
    };
    if let Err(error) = insert_notification_intent(
        &mut tx,
        event_id,
        "payment_method_bound",
        &order.seller_pubky,
        &actor.0,
        &ids::order_aggregate_id(order.id),
        None,
        now,
    )
    .await
    {
        return internal("payment method notification", &error);
    }

    // Bitcoin: the Paykit payment request is created before commit so a
    // refusal leaves the order unbound. The request itself is idempotent on
    // (creator, reference), so a retried binding replays rather than
    // double-requests.
    if let Some(reference) = &paykit_reference {
        let paykit = payments.paykit.as_ref().expect("checked above");
        let amount_sats = bitcoin_amount_sats(&order).expect("checked above");
        if let Err(error) = paykit
            .create_payment_request(
                &order.seller_pubky,
                &order.buyer_pubky,
                reference,
                amount_sats,
            )
            .await
        {
            let _ = tx.rollback().await;
            return match error {
                PaykitRequestError::SellerAccountUnavailable => method_error(
                    ErrorCode::InvalidState,
                    "seller_account_unclaimed",
                    "The seller has no claimed Bitcoin receiving account.",
                ),
                PaykitRequestError::Rejected => method_error(
                    ErrorCode::InvalidState,
                    "paykit_rejected",
                    "The Paykit payment request was refused (the buyer may have no Paykit-enabled wallet).",
                ),
                PaykitRequestError::Unavailable => method_error(
                    ErrorCode::UpstreamUnavailable,
                    "paykit_unavailable",
                    "The Paykit server could not be reached; try again.",
                ),
            };
        }
    }

    let response = match order_response(&mut tx, &updated_order, json!({})).await {
        Ok(response) => response,
        Err(error) => return internal("order projection", &error),
    };
    match tx.commit().await {
        Ok(()) => response,
        Err(error) => internal("payment method commit", &error),
    }
}

/// Applies a verified/attested fiat payment: payment CAS
/// `awaiting_entitlement → confirmed` with the shared confirmation effects,
/// or `manual_review` when the order can no longer confirm. Idempotent for
/// already-advanced payments.
async fn apply_fiat_paid(
    state: &AppState,
    actor: &str,
    order_id: Uuid,
    transaction_ref: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Response, Response> {
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|error| internal("fiat confirmation transaction", &error))?;
    let payment = fetch_payment_for_order_update(&mut tx, order_id)
        .await
        .map_err(|error| internal("payment lookup", &error))?
        .ok_or_else(|| {
            method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        })?;
    let order = fetch_order_for_update(&mut tx, order_id)
        .await
        .map_err(|error| internal("order lookup", &error))?
        .ok_or_else(|| {
            method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        })?;
    if payment.state == "expired" {
        // The money is real but the payment window already elapsed: the
        // sweep released the hold and cancelled the order, so the fact is
        // retained under manual review — never silently dropped, never a
        // confirmation of inventory the order no longer holds (exactly like
        // a late Locks completion).
        let updated: Option<(i64,)> = sqlx::query_as(
            "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
             updated_at = $2 WHERE id = $1 AND state = 'expired' RETURNING revision",
        )
        .bind(payment.id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| internal("manual review update", &error))?;
        if let Some((revision,)) = updated {
            insert_event(
                &mut tx,
                Uuid::new_v4(),
                &ids::payment_aggregate_id(payment.id),
                revision,
                actor,
                "payment.manual_review",
                now,
            )
            .await
            .map_err(|error| internal("manual review event", &error))?;
        }
        let response = order_response(&mut tx, &order, json!({ "verified": true }))
            .await
            .map_err(|error| internal("order projection", &error))?;
        tx.commit()
            .await
            .map_err(|error| internal("manual review commit", &error))?;
        return Ok(response);
    }
    if payment.state != "awaiting_entitlement" {
        // Already confirmed (or under review): a duplicate verification has
        // no further effect.
        let response = order_response(&mut tx, &order, json!({ "verified": true }))
            .await
            .map_err(|error| internal("order projection", &error))?;
        tx.commit()
            .await
            .map_err(|error| internal("fiat confirmation commit", &error))?;
        return Ok(response);
    }
    if let Some(reference) = transaction_ref {
        sqlx::query("UPDATE orders SET fiat_transaction_ref = $2 WHERE id = $1")
            .bind(order.id)
            .bind(reference)
            .execute(&mut *tx)
            .await
            .map_err(|error| internal("transaction reference update", &error))?;
    }
    let command_id = Uuid::new_v4();
    match crate::handlers::payment::confirm_order(&mut tx, actor, command_id, &payment, order, now)
        .await
        .map_err(|error| internal("order confirmation", &error))?
    {
        Ok((confirmed_order, _receipt, _receipt_event_id)) => {
            let (revision,): (i64,) = sqlx::query_as(
                "UPDATE payments SET state = 'confirmed', revision = revision + 1, \
                 updated_at = $2 WHERE id = $1 RETURNING revision",
            )
            .bind(payment.id)
            .bind(now)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| internal("payment confirmation", &error))?;
            let event_id = insert_event(
                &mut tx,
                command_id,
                &ids::payment_aggregate_id(payment.id),
                revision,
                actor,
                "payment.confirmed",
                now,
            )
            .await
            .map_err(|error| internal("payment confirmation event", &error))?;
            let counterparty = if actor == confirmed_order.buyer_pubky {
                confirmed_order.seller_pubky.clone()
            } else {
                confirmed_order.buyer_pubky.clone()
            };
            insert_notification_intent(
                &mut tx,
                event_id,
                "payment_confirmed",
                &counterparty,
                actor,
                &ids::order_aggregate_id(confirmed_order.id),
                None,
                now,
            )
            .await
            .map_err(|error| internal("payment confirmation notification", &error))?;
            let response = order_response(&mut tx, &confirmed_order, json!({ "verified": true }))
                .await
                .map_err(|error| internal("order projection", &error))?;
            tx.commit()
                .await
                .map_err(|error| internal("fiat confirmation commit", &error))?;
            Ok(response)
        }
        Err(failure) => {
            // The money is real but the order can no longer confirm (e.g. a
            // lapsed auction hold): retain the fact under manual review,
            // exactly like the Locks worker.
            tx.rollback()
                .await
                .map_err(|error| internal("fiat confirmation rollback", &error))?;
            tracing::warn!(
                order_id = %order_id,
                code = ?failure.code,
                "verified fiat payment could not confirm the order; routing to manual review"
            );
            let mut tx = state
                .pool
                .begin()
                .await
                .map_err(|error| internal("manual review transaction", &error))?;
            let updated: Option<(i64,)> = sqlx::query_as(
                "UPDATE payments SET state = 'manual_review', revision = revision + 1, \
                 updated_at = $3 WHERE id = $1 AND state = $2 RETURNING revision",
            )
            .bind(payment.id)
            .bind("awaiting_entitlement")
            .bind(now)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| internal("manual review update", &error))?;
            if let Some((revision,)) = updated {
                insert_event(
                    &mut tx,
                    command_id,
                    &ids::payment_aggregate_id(payment.id),
                    revision,
                    actor,
                    "payment.manual_review",
                    now,
                )
                .await
                .map_err(|error| internal("manual review event", &error))?;
            }
            let order = fetch_order_for_update(&mut tx, order_id)
                .await
                .map_err(|error| internal("order lookup", &error))?
                .ok_or_else(|| {
                    method_error(
                        ErrorCode::NotFound,
                        "order_not_found",
                        "The order was not found.",
                    )
                })?;
            let response = order_response(&mut tx, &order, json!({ "verified": true }))
                .await
                .map_err(|error| internal("order projection", &error))?;
            tx.commit()
                .await
                .map_err(|error| internal("manual review commit", &error))?;
            Ok(response)
        }
    }
}

/// Reads the order and payment without locks for the verification preamble.
async fn read_order_and_payment(
    state: &AppState,
    order_id: Uuid,
) -> Result<Option<(OrderRow, PaymentRow)>, sqlx::Error> {
    let order: Option<OrderRow> = sqlx::query_as(&format!(
        "SELECT {} FROM orders WHERE id = $1",
        crate::queries::ORDER_COLUMNS
    ))
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await?;
    let Some(order) = order else {
        return Ok(None);
    };
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1"
    ))
    .bind(order_id)
    .fetch_optional(&state.pool)
    .await?;
    Ok(payment.map(|payment| (order, payment)))
}

/// `POST /v0/orders/{id}/fiat/verify` (either participant): server-side
/// Stripe verification with the SELLER's stored restricted key. On a match
/// the order is paid; `verified: false` means no matching paid session was
/// found yet (the buyer may not have completed checkout).
pub async fn verify_fiat_payment(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
) -> Response {
    let payments = match payments_runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    let (order, payment) = match read_order_and_payment(&state, order_id).await {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            return method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        }
        Err(error) => return internal("order lookup", &error),
    };
    if actor.0 != order.buyer_pubky && actor.0 != order.seller_pubky {
        return method_error(
            ErrorCode::Unauthorized,
            "not_participant",
            "Only order participants may verify the payment.",
        );
    }
    if order.payment_method.as_deref() != Some("stripe") {
        return method_error(
            ErrorCode::InvalidState,
            "method_mismatch",
            "Processor verification applies only to Stripe-bound orders.",
        );
    }
    if payment.state != "awaiting_entitlement" {
        // Already advanced: idempotent success.
        return match apply_fiat_paid(&state, &actor.0, order_id, None, state.clock.now()).await {
            Ok(response) | Err(response) => response,
        };
    }
    let config = match load_config(&state.pool, &order.seller_pubky).await {
        Ok(config) => config,
        Err(error) => return internal("payment config read", &error),
    };
    let Some(sealed) = config.and_then(|config| config.stripe_restricted_key_ciphertext) else {
        return method_error(
            ErrorCode::InvalidState,
            "stripe_key_missing",
            "The seller has no Stripe restricted key configured, so this order cannot be verified.",
        );
    };
    let restricted_key = match payments
        .stripe_key_cipher
        .decrypt(&order.seller_pubky, &sealed)
    {
        Ok(key) => key,
        Err(error) => return internal("restricted key decryption", &error),
    };
    let matched = payments
        .stripe
        .find_paid_session(
            &restricted_key,
            &order.id.to_string(),
            order.total_minor,
            &order.currency,
        )
        .await;
    match matched {
        Ok(Some(matched)) => {
            match apply_fiat_paid(
                &state,
                &actor.0,
                order_id,
                Some(&matched.session_id),
                state.clock.now(),
            )
            .await
            {
                Ok(response) | Err(response) => response,
            }
        }
        Ok(None) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "verified": false, "status": "not_found" })),
        )
            .into_response(),
        Err(StripeError::KeyInvalid) => method_error(
            ErrorCode::InvalidState,
            "stripe_key_invalid",
            "Stripe rejected the seller's restricted key; the seller must update their payment configuration.",
        ),
        Err(StripeError::Unavailable) => method_error(
            ErrorCode::UpstreamUnavailable,
            "stripe_unavailable",
            "Stripe could not be reached; try again.",
        ),
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkPaidBody {
    #[serde(default)]
    transaction_ref: Option<String>,
}

/// `POST /v0/orders/{id}/fiat/mark-paid` (buyer session): the buyer reports
/// an out-of-band PayPal payment. This never advances the payment — only the
/// seller's confirmation does.
pub async fn mark_fiat_paid(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    body: Option<Json<MarkPaidBody>>,
) -> Response {
    if payments_runtime(&state).is_err() {
        return method_error(
            ErrorCode::UpstreamUnavailable,
            "payments_disabled",
            "Payment methods are not enabled on this deployment.",
        );
    }
    let transaction_ref = body.and_then(|body| body.0.transaction_ref);
    if let Some(reference) = &transaction_ref {
        if reference.is_empty()
            || reference.len() > 64
            || !reference.chars().all(|c| c.is_ascii_graphic())
        {
            return method_error(
                ErrorCode::InvalidCommand,
                "invalid_transaction_ref",
                "The transaction reference must be 1-64 printable ASCII characters.",
            );
        }
    }
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal("mark-paid transaction", &error),
    };
    let order = match fetch_order_for_update(&mut tx, order_id).await {
        Ok(Some(order)) => order,
        Ok(None) => {
            return method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        }
        Err(error) => return internal("order lookup", &error),
    };
    if order.buyer_pubky != actor.0 {
        return method_error(
            ErrorCode::Unauthorized,
            "not_buyer",
            "Only the buyer may report a fiat payment.",
        );
    }
    if order.payment_method.as_deref() != Some("paypal") {
        return method_error(
            ErrorCode::InvalidState,
            "method_mismatch",
            "Payment reporting applies only to PayPal-bound orders.",
        );
    }
    if order.payment_reported_at.is_some() {
        // Idempotent duplicate report: the original timestamp is kept.
        return match order_response(&mut tx, &order, json!({})).await {
            Ok(response) => response,
            Err(error) => internal("order projection", &error),
        };
    }
    let updated_order: OrderRow = match sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, payment_reported_at = $2, \
         fiat_transaction_ref = COALESCE($3, fiat_transaction_ref), updated_at = $2 \
         WHERE id = $1 RETURNING {}",
        crate::queries::ORDER_COLUMNS
    ))
    .bind(order.id)
    .bind(now)
    .bind(&transaction_ref)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(order) => order,
        Err(error) => return internal("mark-paid update", &error),
    };
    let event_id = match insert_event(
        &mut tx,
        Uuid::new_v4(),
        &ids::order_aggregate_id(order.id),
        updated_order.revision,
        &actor.0,
        "order.fiat_payment_reported",
        now,
    )
    .await
    {
        Ok(event_id) => event_id,
        Err(error) => return internal("mark-paid event", &error),
    };
    if let Err(error) = insert_notification_intent(
        &mut tx,
        event_id,
        "fiat_payment_reported",
        &order.seller_pubky,
        &actor.0,
        &ids::order_aggregate_id(order.id),
        None,
        now,
    )
    .await
    {
        return internal("mark-paid notification", &error);
    }
    let response = match order_response(&mut tx, &updated_order, json!({})).await {
        Ok(response) => response,
        Err(error) => return internal("order projection", &error),
    };
    match tx.commit().await {
        Ok(()) => response,
        Err(error) => internal("mark-paid commit", &error),
    }
}

/// `POST /v0/orders/{id}/fiat/confirm-received` (seller session): the seller
/// attests the PayPal payment arrived, which pays the order. This is the
/// deliberate `seller-attested` counterpart to Stripe's processor
/// verification.
pub async fn confirm_fiat_received(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
) -> Response {
    if payments_runtime(&state).is_err() {
        return method_error(
            ErrorCode::UpstreamUnavailable,
            "payments_disabled",
            "Payment methods are not enabled on this deployment.",
        );
    }
    let (order, _payment) = match read_order_and_payment(&state, order_id).await {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            return method_error(
                ErrorCode::NotFound,
                "order_not_found",
                "The order was not found.",
            )
        }
        Err(error) => return internal("order lookup", &error),
    };
    if order.seller_pubky != actor.0 {
        return method_error(
            ErrorCode::Unauthorized,
            "not_seller",
            "Only the seller may confirm a received fiat payment.",
        );
    }
    if order.payment_method.as_deref() != Some("paypal") {
        return method_error(
            ErrorCode::InvalidState,
            "method_mismatch",
            "Receipt confirmation applies only to PayPal-bound orders.",
        );
    }
    match apply_fiat_paid(&state, &actor.0, order_id, None, state.clock.now()).await {
        Ok(response) | Err(response) => response,
    }
}
