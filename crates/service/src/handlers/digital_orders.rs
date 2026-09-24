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

/// `GET /v1/orders/{id}/digital-delivery`: the paying buyer's download
/// (§4.2). Each successful read appends one access row per line served.
pub async fn get_order_digital_delivery(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let order: Result<Option<OrderRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {} FROM orders WHERE id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2)",
        crate::queries::ORDER_COLUMNS
    ))
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
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
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error("digital delivery read"),
    };
    let pins: Vec<PinRow> = match sqlx::query_as(
        "SELECT line_index, listing_aggregate_id, deliverable_id, version, kind, \
         payload_ciphertext, confirming_adapter FROM order_digital_pins \
         WHERE order_id = $1 ORDER BY line_index",
    )
    .bind(order.id)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(pins) => pins,
        Err(_) => return internal_error("digital delivery pins"),
    };
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
             VALUES ($1, $2, $3)",
        )
        .bind(order.id)
        .bind(pin.line_index)
        .bind(now)
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
