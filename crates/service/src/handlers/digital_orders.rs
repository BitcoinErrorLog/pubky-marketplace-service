//! Digital orders: the confirm-time pin and the buyer's download read
//! (digital-delivery-design.md §3.4, §3.6, §4.2, §6 rows D1–D11).
//!
//! Each order line of a digital order snapshots the listing's delivery kind
//! (`digital_kind`) at checkout. At payment confirmation every instant line
//! (file, link, text) opens the listing's current deliverable version and
//! re-seals its payload as an order pin (`order_digital_pins`) inside the
//! receipt transaction; the buyer's read serves only the pin. The
//! entitlement is the receipt, re-checked on every read, and ends at
//! `cancelled`, `refunded_external` or `closed` — `completed` still
//! downloads.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::commands::DigitalDeliveryKind;
use marketplace_domain::ErrorCode;
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::digital::{pin_aad, version_aad, DigitalKeys};
use crate::handlers::digital::{no_store, DIGITAL_ENDED_ORDER_STATES};
use crate::model::OrderRow;
use crate::AppState;

/// The delivery kind a digital order line snapshotted at checkout.
pub(crate) fn line_kind(line: &Value) -> Option<DigitalDeliveryKind> {
    line.get("digital_kind")
        .and_then(Value::as_str)
        .and_then(DigitalDeliveryKind::parse)
}

fn instant_lines(order: &OrderRow) -> Vec<(i32, String, DigitalDeliveryKind)> {
    order
        .lines
        .as_array()
        .map(|lines| {
            lines
                .iter()
                .enumerate()
                .filter_map(|(index, line)| {
                    let kind = line_kind(line).filter(|kind| kind.is_instant())?;
                    let listing = line.get("listing_aggregate_id")?.as_str()?.to_string();
                    Some((index as i32, listing, kind))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether every line of a digital order is released on the order page.
pub(crate) fn all_lines_instant(order: &OrderRow) -> bool {
    order.lines.as_array().is_some_and(|lines| {
        !lines.is_empty()
            && lines
                .iter()
                .all(|line| line_kind(line).is_some_and(DigitalDeliveryKind::is_instant))
    })
}

#[derive(Debug, FromRow)]
struct CurrentVersion {
    deliverable_id: String,
    version: i64,
    kind: String,
    payload_ciphertext: Vec<u8>,
}

async fn current_version(
    tx: &mut Transaction<'_, Postgres>,
    listing_aggregate_id: &str,
) -> Result<Option<CurrentVersion>, sqlx::Error> {
    sqlx::query_as(
        "SELECT deliverable_id, version, kind, payload_ciphertext FROM listing_digital_versions \
         WHERE listing_aggregate_id = $1 AND superseded_at IS NULL",
    )
    .bind(listing_aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

/// A digital order that cannot be delivered at confirmation: an instant
/// line whose listing has no current version of the kind the buyer paid
/// for (§6 D3). The `digital_delivery.clear` / kind-change guard makes this
/// unreachable through commands; the confirming paths take the refund
/// route instead of issuing a receipt.
pub(crate) async fn unpinnable(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
) -> Result<bool, sqlx::Error> {
    if order.fulfillment != "digital" {
        return Ok(false);
    }
    for (_, listing, kind) in instant_lines(order) {
        match current_version(tx, &listing).await? {
            Some(current) if current.kind == kind.as_str() => {}
            _ => return Ok(true),
        }
    }
    Ok(false)
}

/// Pins every instant line inside the receipt transaction. A version that
/// does not open (a wrong or half-rotated key, or no key at all) aborts the
/// transaction so the confirming path retries: that is an operator fault,
/// surfaced by the boot probe and `/ready`, never a product outcome (§6 D4).
pub(crate) async fn pin_digital_lines(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
    confirming_adapter: &str,
    keys: Option<&DigitalKeys>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let lines = instant_lines(order);
    if lines.is_empty() {
        return Ok(());
    }
    let Some(keys) = keys else {
        return Err(sqlx::Error::Protocol(
            "a digital order confirmed without DIGITAL_DELIVERY_ENCRYPTION_KEY".into(),
        ));
    };
    for (line_index, listing, kind) in lines {
        let Some(current) = current_version(tx, &listing).await? else {
            return Err(sqlx::Error::Protocol(
                "a digital order line lost its deliverable inside the receipt transaction".into(),
            ));
        };
        let plaintext = keys
            .open(
                &version_aad(&listing, &current.deliverable_id, current.version, kind),
                &current.payload_ciphertext,
            )
            .map_err(|_| {
                sqlx::Error::Protocol(
                    "a digital deliverable version did not open under the configured key".into(),
                )
            })?;
        let sealed = keys.seal(
            &pin_aad(
                order.id,
                line_index,
                &listing,
                &current.deliverable_id,
                current.version,
            ),
            &plaintext,
        );
        sqlx::query(
            "INSERT INTO order_digital_pins (order_id, line_index, listing_aggregate_id, \
             deliverable_id, version, kind, payload_ciphertext, confirming_adapter, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(order.id)
        .bind(line_index)
        .bind(&listing)
        .bind(&current.deliverable_id)
        .bind(current.version)
        .bind(kind.as_str())
        .bind(&sealed)
        .bind(confirming_adapter)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

fn read_error(code: ErrorCode, message: &str, reason: Option<&str>) -> Response {
    let mut error = json!({ "code": code, "message": message });
    if let Some(reason) = reason {
        error["reason"] = json!(reason);
    }
    no_store(
        (
            StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
            Json(json!({ "ok": false, "error": error })),
        )
            .into_response(),
    )
}

fn internal_error(context: &str) -> Response {
    tracing::error!("{context} failed");
    no_store(
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "ok": false,
                "error": { "code": "INTERNAL", "message": "The delivery could not be read." },
            })),
        )
            .into_response(),
    )
}

#[derive(Debug, FromRow)]
struct PinRow {
    line_index: i32,
    listing_aggregate_id: String,
    deliverable_id: String,
    version: i64,
    kind: String,
    payload_ciphertext: Vec<u8>,
    confirming_adapter: String,
}

/// Download reads a buyer may make per minute across all their orders, and
/// per order. Re-downloads are unlimited over time; a looping client is
/// refused with `429` and `Retry-After`.
pub const BUYER_READS_PER_MINUTE: f64 = 30.0;
pub const ORDER_READS_PER_MINUTE: f64 = 10.0;
/// Repeat opens of one line within this window add no access row: the log
/// records the first open and at most one open per line per hour.
pub const ACCESS_COALESCE_SECONDS: i64 = 3_600;

/// A token bucket in `digital_read_rate_limits`, consumed in the caller's
/// transaction. Returns the seconds to wait when the bucket is empty.
///
/// The bucket row is seeded full with `ON CONFLICT DO NOTHING` and then
/// locked before any arithmetic, so concurrent first reads (for example one
/// buyer opening several orders at once) all consume from the same row
/// rather than each computing from an absent one.
async fn consume_read_token(
    tx: &mut Transaction<'_, Postgres>,
    bucket: &str,
    per_minute: f64,
    now: DateTime<Utc>,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query(
        "INSERT INTO digital_read_rate_limits (bucket, tokens, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (bucket) DO NOTHING",
    )
    .bind(bucket)
    .bind(per_minute)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let (stored, updated_at): (f64, DateTime<Utc>) = sqlx::query_as(
        "SELECT tokens, updated_at FROM digital_read_rate_limits WHERE bucket = $1 FOR UPDATE",
    )
    .bind(bucket)
    .fetch_one(&mut **tx)
    .await?;
    let elapsed = (now - updated_at).num_milliseconds().max(0) as f64 / 1_000.0;
    let available = (stored + elapsed * per_minute / 60.0).min(per_minute);
    let (tokens, retry_after) = if available >= 1.0 {
        (available - 1.0, None)
    } else {
        let seconds = ((1.0 - available) * 60.0 / per_minute).ceil().max(1.0) as i64;
        (available, Some(seconds))
    };
    sqlx::query(
        "UPDATE digital_read_rate_limits SET tokens = $2, updated_at = GREATEST(updated_at, $3) \
         WHERE bucket = $1",
    )
    .bind(bucket)
    .bind(tokens)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(retry_after)
}

fn rate_limited(retry_after: i64) -> Response {
    let mut response = read_error(
        ErrorCode::InvalidState,
        "Too many downloads; try again shortly.",
        Some("rate_limited"),
    );
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from_str(&retry_after.to_string())
            .expect("retry-after is a safe integer"),
    );
    response
}

/// `GET /v1/orders/{id}/digital-delivery/{line_index}`: the paying buyer's
/// download of ONE line (§4.2).
///
/// A read releases only the requested line and records access only for it.
/// The access row is what keeps an opened instant line sold on cancel
/// (§3.6 E8), so opening one line must never release or log another.
///
/// The entitlement is checked and the payload released in ONE transaction
/// that holds the order row `FOR SHARE`: a refund or cancel that commits
/// first is seen here, and one that arrives later waits for this read to
/// commit, so a key is never released for an ended order.
///
/// The access row is written in that same transaction before the payload is
/// returned, and a failed write fails the read (fail closed). The access
/// log is the delivery evidence the design calls for (§4.2, §3.6 E8: an
/// opened instant line stays sold on cancel), so no key leaves without it.
pub async fn get_order_digital_delivery(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path((id, line_index)): Path<(Uuid, i32)>,
) -> Response {
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error("digital delivery read"),
    };
    let order: Result<Option<OrderRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {} FROM orders WHERE id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2) \
         FOR SHARE",
        crate::queries::ORDER_COLUMNS
    ))
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&mut *tx)
    .await;
    let order = match order {
        Ok(Some(order)) if order.fulfillment == "digital" => order,
        Ok(_) => return read_error(ErrorCode::NotFound, "The order was not found.", None),
        Err(_) => return internal_error("digital delivery order read"),
    };
    if order.buyer_pubky != actor.0 {
        return read_error(
            ErrorCode::Unauthorized,
            "Only the buyer may open the purchase.",
            None,
        );
    }
    if order.receipt_id.is_none() {
        return read_error(
            ErrorCode::InvalidState,
            "The purchase is available once payment is confirmed.",
            Some("not_paid"),
        );
    }
    if DIGITAL_ENDED_ORDER_STATES.contains(&order.state.as_str()) {
        return read_error(
            ErrorCode::InvalidState,
            "The order was refunded or cancelled; the purchase is no longer available.",
            Some("delivery_ended"),
        );
    }
    let pins: Vec<PinRow> = match sqlx::query_as(
        "SELECT line_index, listing_aggregate_id, deliverable_id, version, kind, \
         payload_ciphertext, confirming_adapter FROM order_digital_pins \
         WHERE order_id = $1 AND line_index = $2",
    )
    .bind(order.id)
    .bind(line_index)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(pins) => pins,
        Err(_) => return internal_error("digital delivery pins"),
    };
    // A manual (email or message) line, or an index the order does not
    // have, has no pin to release.
    if pins.is_empty() {
        return read_error(ErrorCode::NotFound, "The order line has no download.", None);
    }
    if pins.iter().any(|pin| pin.confirming_adapter == "sandbox") {
        return read_error(
            ErrorCode::InvalidState,
            "Sandbox orders do not deliver purchases.",
            Some("sandbox_confirmed"),
        );
    }
    let Some(keys) = state.digital.as_deref() else {
        return read_error(
            ErrorCode::InvalidState,
            "Digital delivery is unavailable on this deployment.",
            Some(crate::handlers::digital::REASON_UNAVAILABLE),
        );
    };
    let now = state.clock.now();
    for (bucket, per_minute) in [
        (
            format!("buyer:{}", order.buyer_pubky),
            BUYER_READS_PER_MINUTE,
        ),
        (format!("order:{}", order.id), ORDER_READS_PER_MINUTE),
    ] {
        match consume_read_token(&mut tx, &bucket, per_minute, now).await {
            Ok(None) => {}
            Ok(Some(retry_after)) => {
                // The spent tokens persist; the refusal itself releases nothing.
                if tx.commit().await.is_err() {
                    return internal_error("digital delivery rate limit commit");
                }
                return rate_limited(retry_after);
            }
            Err(_) => return internal_error("digital delivery rate limit"),
        }
    }
    let mut lines = Vec::with_capacity(pins.len());
    for pin in pins {
        let aad = pin_aad(
            order.id,
            pin.line_index,
            &pin.listing_aggregate_id,
            &pin.deliverable_id,
            pin.version,
        );
        let Ok(plaintext) = keys.open(&aad, &pin.payload_ciphertext) else {
            return internal_error("digital delivery pin open");
        };
        let Ok(payload) = serde_json::from_slice::<Value>(&plaintext) else {
            return internal_error("digital delivery pin parse");
        };
        let mut line = json!({
            "line_index": pin.line_index,
            "listing_aggregate_id": pin.listing_aggregate_id,
            "kind": pin.kind,
            "seller_pubky": order.seller_pubky,
            "deliverable_id": pin.deliverable_id,
            "version": pin.version,
        });
        let fields: &[&str] = match pin.kind.as_str() {
            "file" => &[
                "key",
                "iv",
                "ciphertext_blake3",
                "plaintext_blake3",
                "content_type",
                "file_name",
                "size_bytes",
            ],
            "link" => &["url"],
            "text" => &["text"],
            _ => return internal_error("digital delivery pin kind"),
        };
        for field in fields {
            line[*field] = payload[*field].clone();
        }
        lines.push(line);
        if sqlx::query(
            "INSERT INTO order_digital_access (order_id, line_index, accessed_at) \
             SELECT $1, $2, $3 WHERE NOT EXISTS ( \
                 SELECT 1 FROM order_digital_access \
                 WHERE order_id = $1 AND line_index = $2 AND accessed_at > $4)",
        )
        .bind(order.id)
        .bind(pin.line_index)
        .bind(now)
        .bind(now - chrono::Duration::seconds(ACCESS_COALESCE_SECONDS))
        .execute(&mut *tx)
        .await
        .is_err()
        {
            return internal_error("digital delivery access log");
        }
    }
    if tx.commit().await.is_err() {
        return internal_error("digital delivery access log commit");
    }
    no_store(
        (
            StatusCode::OK,
            Json(json!({ "order_id": order.id, "lines": lines })),
        )
            .into_response(),
    )
}

/// `GET /v1/orders/{id}/digital-evidence`: the seller's delivery evidence on
/// one of their digital orders (§3 "Seller's orders"): when the order was
/// delivered, when an instant line was first opened and how many opens were
/// logged (coalesced to at most one per line per hour), and when the seller
/// marked the email and message lines. Timestamps and a count only: no
/// address, key, link, text or IP. Seller only; anyone else, and a
/// non-digital order, is NOT_FOUND.
pub async fn get_order_digital_evidence(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let evidence: Result<Option<EvidenceRow>, sqlx::Error> = sqlx::query_as(
        "SELECT o.id AS order_id, o.digital_delivered_at AS delivered_at, \
                o.digital_message_delivered_at AS message_delivered_at, \
                (SELECT MIN(accessed_at) FROM order_digital_access a WHERE a.order_id = o.id) AS first_opened_at, \
                (SELECT COUNT(*) FROM order_digital_access a WHERE a.order_id = o.id) AS open_count, \
                (SELECT emailed_at FROM order_delivery_emails e WHERE e.order_id = o.id) AS emailed_at \
         FROM orders o WHERE o.id = $1 AND o.seller_pubky = $2 AND o.fulfillment = 'digital'",
    )
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    match evidence {
        Ok(Some(row)) => no_store(
            (
                StatusCode::OK,
                Json(json!({
                    "order_id": row.order_id,
                    "delivered_at": row.delivered_at.map(format_timestamp),
                    "first_opened_at": row.first_opened_at.map(format_timestamp),
                    "open_count": row.open_count,
                    "emailed_at": row.emailed_at.map(format_timestamp),
                    "message_delivered_at": row.message_delivered_at.map(format_timestamp),
                })),
            )
                .into_response(),
        ),
        Ok(None) => read_error(ErrorCode::NotFound, "The order was not found.", None),
        Err(_) => internal_error("digital evidence read"),
    }
}

#[derive(FromRow)]
struct EvidenceRow {
    order_id: Uuid,
    delivered_at: Option<DateTime<Utc>>,
    message_delivered_at: Option<DateTime<Utc>>,
    first_opened_at: Option<DateTime<Utc>>,
    open_count: i64,
    emailed_at: Option<DateTime<Utc>>,
}
