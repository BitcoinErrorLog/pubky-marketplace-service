//! Local pickup commands and reveal reads (local pickup design PART A).
//!
//! `pickup_details.set` / `pickup_details.clear` are the ONLY writes to the
//! sealed details store: `listing.sync` converges the public record and
//! carries no details, so no sync path can null them (§A4). Versions are
//! monotonic per listing through the separate
//! `listing_pickup_version_counters` row, which survives `clear` — a
//! delete-and-recreate can never restart the sequence and fool terms-change
//! detection (§A3).
//!
//! The two reveal reads are the only paths that ever open the seal: the
//! seller's owner read and the paying buyer's per-line reveal, which serves
//! the PINNED snapshot recorded at payment — never the listing's current
//! details — with its entitlement re-evaluated against the durable payment
//! fact (`orders.receipt_id IS NOT NULL`) and the terminal cutoff on every
//! read (§A3).

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::commands::{ClearPickupDetailsPayload, SetPickupDetailsPayload};
use marketplace_domain::{Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_listing_for_update, insert_notification_intent};
use crate::model::{OrderRow, PickupDetailsRow, PickupVersionCounterRow};
use crate::pickup::{details_aad, snapshot_aad, PickupKeys};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};
use crate::AppState;

/// Order states after which nothing more can happen to the order: the
/// reveal entitlement ends here (§A3), and retention keeps details only for
/// orders outside this set.
pub const TERMINAL_ORDER_STATES: [&str; 4] =
    ["completed", "cancelled", "refunded_external", "closed"];

const PICKUP_UNAVAILABLE: &str = "Pickup is unavailable on this deployment.";

/// The command/event aggregate namespace for pickup-details history, kept
/// distinct from the listing aggregate so details events never collide with
/// the listing's own revision sequence.
fn details_aggregate_id(listing_aggregate_id: &str) -> String {
    let suffix = listing_aggregate_id
        .strip_prefix("listing:")
        .unwrap_or(listing_aggregate_id);
    format!("pickup_details:{suffix}")
}

fn unavailable() -> CommandFailure {
    CommandFailure::new(ErrorCode::InvalidState, PICKUP_UNAVAILABLE)
}

/// Fetches and locks the listing the command targets, enforcing seller
/// ownership and the aggregate identity, plus the all-or-none pickup gate
/// (key configured AND sandbox payments disabled, §A7/§A8).
async fn guard_details_command(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    pickup: Option<&PickupKeys>,
    sandbox_payments_enabled: bool,
) -> Result<Result<crate::model::ListingRow, CommandFailure>, sqlx::Error> {
    if pickup.is_none() || sandbox_payments_enabled {
        return Ok(Err(unavailable()));
    }
    let Some(listing) = fetch_listing_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The listing was not found.",
        )));
    };
    if listing.seller_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the listing seller may manage its pickup details.",
        )));
    }
    Ok(Ok(listing))
}

/// Locks (creating when absent) the per-listing version counter and
/// compare-and-swaps `expected_version` against it. The counter row is the
/// single source of the next version and survives `pickup_details.clear`
/// (§A3); CAS failures return the current version for the client's retry.
async fn lock_counter_cas(
    tx: &mut Transaction<'_, Postgres>,
    listing: &crate::model::ListingRow,
    expected_version: i64,
    now: DateTime<Utc>,
) -> Result<Result<PickupVersionCounterRow, CommandFailure>, sqlx::Error> {
    sqlx::query(
        "INSERT INTO listing_pickup_version_counters (aggregate_id, seller_pubky, last_version, \
         updated_at) VALUES ($1, $2, 0, $3) ON CONFLICT (aggregate_id) DO NOTHING",
    )
    .bind(&listing.aggregate_id)
    .bind(&listing.seller_pubky)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let counter: PickupVersionCounterRow = sqlx::query_as(
        "SELECT aggregate_id, seller_pubky, last_version, updated_at \
         FROM listing_pickup_version_counters WHERE aggregate_id = $1 FOR UPDATE",
    )
    .bind(&listing.aggregate_id)
    .fetch_one(&mut **tx)
    .await?;
    if counter.last_version != expected_version {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The pickup details version is stale.",
            counter.last_version,
        )));
    }
    Ok(Ok(counter))
}

/// The current details version of a listing: the maximum version still
/// present (after a `clear`, none — the counter alone survives).
async fn current_details(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<PickupDetailsRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT aggregate_id, seller_pubky, version, details_ciphertext, created_at, updated_at \
         FROM listing_pickup_details WHERE aggregate_id = $1 \
         ORDER BY version DESC LIMIT 1",
    )
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Notifies the buyers of every PAID, non-terminal pickup order touching
/// this listing (§A3: an edit or a clear is never silent). One intent per
/// buyer per event; the outbox dedups delivery by (event id, recipient).
async fn notify_paid_buyers(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    notification_type: &str,
    listing: &crate::model::ListingRow,
    actor: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let buyers: Vec<(String, Uuid)> = sqlx::query_as(
        "SELECT DISTINCT o.buyer_pubky, o.id FROM orders o \
         WHERE o.fulfillment = 'pickup' AND o.receipt_id IS NOT NULL \
           AND o.state NOT IN ('completed', 'cancelled', 'refunded_external', 'closed') \
           AND EXISTS ( \
             SELECT 1 FROM jsonb_array_elements(o.lines) AS line \
             WHERE line->>'listing_aggregate_id' = $1)",
    )
    .bind(&listing.aggregate_id)
    .fetch_all(&mut **tx)
    .await?;
    for (buyer, order_id) in buyers {
        insert_notification_intent(
            tx,
            event_id,
            notification_type,
            &buyer,
            actor,
            &marketplace_domain::ids::order_aggregate_id(order_id),
            None,
            now,
        )
        .await?;
    }
    Ok(())
}

/// `pickup_details.set`: sealed whole-payload upsert, version + 1 via the
/// counters row CAS (post-clear CAS included — the counter survives), with
/// paid-buyer notification on change (§A3/§A7).
pub async fn set(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SetPickupDetailsPayload,
    pickup: Option<&PickupKeys>,
    sandbox_payments_enabled: bool,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let listing = match guard_details_command(tx, actor, command, pickup, sandbox_payments_enabled)
        .await?
    {
        Ok(listing) => listing,
        Err(failure) => return Ok(Err(failure)),
    };
    let keys = pickup.expect("the gate refuses the command without keys");
    let counter = match lock_counter_cas(tx, &listing, payload.expected_version, now).await? {
        Ok(counter) => counter,
        Err(failure) => return Ok(Err(failure)),
    };

    let new_version = counter.last_version + 1;
    let plaintext =
        serde_json::to_vec(&payload.details).expect("pickup details serialize infallibly");
    let ciphertext = keys.seal(&details_aad(&listing.aggregate_id, new_version), &plaintext);
    sqlx::query(
        "INSERT INTO listing_pickup_details \
         (aggregate_id, seller_pubky, version, details_ciphertext, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $5)",
    )
    .bind(&listing.aggregate_id)
    .bind(&listing.seller_pubky)
    .bind(new_version)
    .bind(&ciphertext)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE listing_pickup_version_counters SET last_version = $2, updated_at = $3 \
         WHERE aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .bind(new_version)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &details_aggregate_id(&listing.aggregate_id),
        new_version,
        actor,
        "pickup_details.set",
        now,
    )
    .await?;
    notify_paid_buyers(
        tx,
        event_id,
        "pickup_details_updated",
        &listing,
        actor,
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_version,
        event_ids: vec![event_id],
        result: json!({
            "kind": "pickup_details",
            "listing_aggregate_id": listing.aggregate_id,
            "version": new_version,
            "updated_at": format_timestamp(now),
        }),
    }))
}

/// `pickup_details.clear`: removes the details. Retention keeps only the
/// versions referenced as `version_at_payment` by a paid, non-terminal
/// order (their reveal keeps serving the pinned snapshot, flagged
/// withdrawn-by-seller); every other version is hard-deleted. The counter
/// row SURVIVES so versions never restart (§A3).
pub async fn clear(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ClearPickupDetailsPayload,
    pickup: Option<&PickupKeys>,
    sandbox_payments_enabled: bool,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let listing = match guard_details_command(tx, actor, command, pickup, sandbox_payments_enabled)
        .await?
    {
        Ok(listing) => listing,
        Err(failure) => return Ok(Err(failure)),
    };
    let counter = match lock_counter_cas(tx, &listing, payload.expected_version, now).await? {
        Ok(counter) => counter,
        Err(failure) => return Ok(Err(failure)),
    };

    // Hard-delete every version NOT referenced by a paid, non-terminal
    // order's pinned snapshot. Referenced versions survive (the reveal
    // serves the pinned snapshot regardless; the retained version is the
    // dispute exhibit) and purge once their referencing orders go terminal.
    sqlx::query(
        "DELETE FROM listing_pickup_details d \
         WHERE d.aggregate_id = $1 AND NOT EXISTS ( \
             SELECT 1 FROM pickup_line_snapshots s JOIN orders o ON o.id = s.order_id \
             WHERE s.listing_aggregate_id = d.aggregate_id AND s.version = d.version \
               AND o.receipt_id IS NOT NULL \
               AND o.state NOT IN ('completed', 'cancelled', 'refunded_external', 'closed'))",
    )
    .bind(&listing.aggregate_id)
    .execute(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &details_aggregate_id(&listing.aggregate_id),
        counter.last_version,
        actor,
        "pickup_details.cleared",
        now,
    )
    .await?;
    notify_paid_buyers(
        tx,
        event_id,
        "pickup_details_cleared",
        &listing,
        actor,
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: counter.last_version,
        event_ids: vec![event_id],
        result: json!({
            "kind": "pickup_details",
            "listing_aggregate_id": listing.aggregate_id,
            "version": counter.last_version,
            "cleared": true,
            "updated_at": format_timestamp(now),
        }),
    }))
}

/// Pins one order's pickup terms inside `confirm_order`'s receipt
/// transaction (§A3): per pickup line, the details version shown at payment
/// (`version_at_payment` on the line JSON) and a SEALED snapshot of those
/// details (AAD = order id ‖ line index ‖ version), plus the confirming
/// payment adapter. Both confirmation paths share this one writer, so
/// sandbox confirmations pin exactly like worker-confirmed payments — and
/// the reveal read later refuses a sandbox-pinned snapshot regardless of
/// the deployment's current flag.
///
/// Returns the (possibly rewritten) lines JSON to persist with the order
/// update. Lines whose listing has no current details pin nothing: an
/// absent `version_at_payment` key reads as "no terms version pinned".
pub(crate) async fn pin_pickup_lines(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    lines: &Value,
    confirming_adapter: &str,
    keys: Option<&PickupKeys>,
    now: DateTime<Utc>,
) -> Result<Value, sqlx::Error> {
    let Some(keys) = keys else {
        return Ok(lines.clone());
    };
    let mut pinned_lines = lines.clone();
    let Some(array) = pinned_lines.as_array_mut() else {
        return Ok(lines.clone());
    };
    for (index, line) in array.iter_mut().enumerate() {
        if line.get("fulfillment").and_then(Value::as_str) != Some("pickup") {
            continue;
        }
        let Some(aggregate_id) = line.get("listing_aggregate_id").and_then(Value::as_str) else {
            continue;
        };
        let aggregate_id = aggregate_id.to_string();
        let Some(details) = current_details(tx, &aggregate_id).await? else {
            continue;
        };
        let line_index = i32::try_from(index).expect("checkout caps lines at 50");
        let plaintext = keys
            .open(
                &details_aad(&aggregate_id, details.version),
                &details.details_ciphertext,
            )
            .unwrap_or_else(|_| {
                panic!("pickup details sealed by this service must open under its key")
            });
        let snapshot = keys.seal(
            &snapshot_aad(order_id, line_index, details.version),
            &plaintext,
        );
        sqlx::query(
            "INSERT INTO pickup_line_snapshots \
             (order_id, line_index, listing_aggregate_id, version, snapshot_ciphertext, \
              confirming_adapter, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (order_id, line_index) DO NOTHING",
        )
        .bind(order_id)
        .bind(line_index)
        .bind(&aggregate_id)
        .bind(details.version)
        .bind(&snapshot)
        .bind(confirming_adapter)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        line["version_at_payment"] = json!(details.version);
    }
    Ok(pinned_lines)
}

/// The unresolved post-payment terms-change test of §A3/§A6: true when any
/// pickup line's listing details were edited after payment (current version
/// above `version_at_payment`) or cleared (no current details while a
/// version was pinned). Drives the buyer's unilateral-cancel unlock and the
/// seller-actor `fulfillment.confirm_pickup` refusal.
pub(crate) async fn order_has_unresolved_terms_change(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
) -> Result<bool, sqlx::Error> {
    if order.fulfillment != "pickup" {
        return Ok(false);
    }
    let Some(lines) = order.lines.as_array() else {
        return Ok(false);
    };
    for line in lines {
        let Some(version_at_payment) = line.get("version_at_payment").and_then(Value::as_i64)
        else {
            continue;
        };
        let Some(aggregate_id) = line.get("listing_aggregate_id").and_then(Value::as_str) else {
            continue;
        };
        let (current,): (Option<i64>,) = sqlx::query_as(
            "SELECT MAX(version) FROM listing_pickup_details WHERE aggregate_id = $1",
        )
        .bind(aggregate_id)
        .fetch_one(&mut **tx)
        .await?;
        match current {
            None => return Ok(true), // cleared after payment
            Some(current_version) if current_version > version_at_payment => return Ok(true),
            Some(_) => {}
        }
    }
    Ok(false)
}

/// The batch variant for read projections (§A3: the order view flags
/// "meeting point updated since you ordered"). One query per distinct
/// listing referenced by pickup lines with a pinned version.
pub async fn pickup_terms_changed_flags(
    pool: &sqlx::PgPool,
    orders: &[OrderRow],
) -> Result<std::collections::HashMap<Uuid, bool>, sqlx::Error> {
    let mut flags = std::collections::HashMap::new();
    for order in orders {
        if order.fulfillment != "pickup" || order.receipt_id.is_none() {
            continue;
        }
        let Some(lines) = order.lines.as_array() else {
            continue;
        };
        let mut changed = false;
        for line in lines {
            let Some(version_at_payment) = line.get("version_at_payment").and_then(Value::as_i64)
            else {
                continue;
            };
            let Some(aggregate_id) = line.get("listing_aggregate_id").and_then(Value::as_str)
            else {
                continue;
            };
            let current: Option<(Option<i64>,)> = sqlx::query_as(
                "SELECT MAX(version) FROM listing_pickup_details WHERE aggregate_id = $1",
            )
            .bind(aggregate_id)
            .fetch_optional(pool)
            .await?;
            match current.and_then(|(version,)| version) {
                None => changed = true, // cleared after payment
                Some(current_version) if current_version > version_at_payment => changed = true,
                Some(_) => {}
            }
        }
        flags.insert(order.id, changed);
    }
    Ok(flags)
}

fn read_error(code: ErrorCode, message: &str) -> Response {
    (
        StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
        Json(json!({ "ok": false, "error": { "code": code, "message": message } })),
    )
        .into_response()
}

fn internal_error(context: &str, error: &sqlx::Error) -> Response {
    tracing::error!(error = %error, "{context} query failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "ok": false,
            "error": { "code": "INTERNAL", "message": "The projection could not be read." },
        })),
    )
        .into_response()
}

/// Adds `Cache-Control: no-store` to an entitled-details response: the
/// entitlement is re-evaluated against the durable fact on every read, and
/// no intermediary or browser cache may serve the meeting point (§A3,
/// threat model WEB-03).
fn no_store(response: Response) -> Response {
    let mut response = response;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, header::HeaderValue::from_static("no-store"));
    response
}

/// `GET /v1/listings/{aggregate_id}/pickup-details`: the seller's owner
/// read (§A4) — their own details opened for them, alongside the surviving
/// version counter so the client's next `pickup_details.set` can
/// compare-and-swap without a hidden second read. After a clear, `details`
/// is null and the counter still answers.
pub async fn get_listing_pickup_details(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let listing: Result<Option<crate::model::ListingRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {} FROM listings WHERE aggregate_id = $1 AND seller_pubky = $2",
        crate::handlers::LISTING_COLUMNS
    ))
    .bind(&aggregate_id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    let listing = match listing {
        Ok(Some(listing)) => listing,
        Ok(None) => return read_error(ErrorCode::NotFound, "The listing was not found."),
        Err(error) => return internal_error("listing pickup details", &error),
    };
    let Some(keys) = state.pickup.as_deref() else {
        return read_error(ErrorCode::InvalidState, PICKUP_UNAVAILABLE);
    };

    let counter: Result<Option<(i64,)>, sqlx::Error> = sqlx::query_as(
        "SELECT last_version FROM listing_pickup_version_counters WHERE aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .fetch_optional(&state.pool)
    .await;
    let last_version = match counter {
        Ok(row) => row.map(|(version,)| version).unwrap_or(0),
        Err(error) => return internal_error("pickup version counter", &error),
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal_error("pickup details read", &error),
    };
    let current = match current_details(&mut tx, &listing.aggregate_id).await {
        Ok(current) => current,
        Err(error) => return internal_error("pickup details read", &error),
    };
    let details = match current {
        Some(row) => {
            let plaintext = match keys.open(
                &details_aad(&listing.aggregate_id, row.version),
                &row.details_ciphertext,
            ) {
                Ok(plaintext) => plaintext,
                Err(_) => {
                    return internal_error(
                        "pickup details open",
                        &sqlx::Error::Protocol(
                            "sealed pickup details did not authenticate".to_string(),
                        ),
                    )
                }
            };
            let details: Value = match serde_json::from_slice(&plaintext) {
                Ok(details) => details,
                Err(_) => {
                    return internal_error(
                        "pickup details parse",
                        &sqlx::Error::Protocol(
                            "sealed pickup details are not valid JSON".to_string(),
                        ),
                    )
                }
            };
            Some(json!({
                "details": details,
                "version": row.version,
                "updated_at": format_timestamp(row.updated_at),
            }))
        }
        None => None,
    };
    no_store((
        StatusCode::OK,
        Json(json!({
            "listing_aggregate_id": listing.aggregate_id,
            "current": details,
            "last_version": last_version,
        })),
    )
        .into_response())
}

/// `GET /v1/orders/{id}/pickup-details`: the paying buyer's per-line
/// reveal (§A3). Buyer only; the entitlement is the durable payment fact
/// (`receipt_id IS NOT NULL`) plus the terminal cutoff, re-checked on every
/// read; the response serves the PINNED snapshot (kind, address-or-spot,
/// instructions, read-only availability windows with their IANA zone,
/// version, updated_at), flagged withdrawn-by-seller after a clear — never
/// the listing's current details. The first successful read stamps
/// `first_revealed_at` (the bounded withdrawal window).
pub async fn get_order_pickup_details(
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
        Ok(Some(order)) => order,
        Ok(None) => return read_error(ErrorCode::NotFound, "The order was not found."),
        Err(error) => return internal_error("order pickup details", &error),
    };
    // Buyer only: sellers read their own details through the owner read.
    if order.buyer_pubky != actor.0 {
        return read_error(
            ErrorCode::Unauthorized,
            "Only the buyer may reveal the pickup details.",
        );
    }
    // The deployment boundary (§A8): refused outright whenever sandbox
    // payments are enabled, independent of the pinned adapter.
    if state.config.sandbox_payments_enabled {
        return read_error(ErrorCode::InvalidState, PICKUP_UNAVAILABLE);
    }
    let Some(keys) = state.pickup.as_deref() else {
        return read_error(ErrorCode::InvalidState, PICKUP_UNAVAILABLE);
    };
    // The entitlement is the durable payment fact, not state membership:
    // `cancelled` is reachable from `pending_payment`, and a buyer who never
    // paid must never see the meeting point.
    if order.receipt_id.is_none() {
        return read_error(
            ErrorCode::InvalidState,
            "The order carries no payment confirmation.",
        );
    }
    // The entitlement ENDS at the terminal transition — including a cancel
    // on any path — not at some later deadline (§A3).
    if TERMINAL_ORDER_STATES.contains(&order.state.as_str()) {
        return read_error(
            ErrorCode::InvalidState,
            "The order is terminal; the pickup details are no longer revealed.",
        );
    }
    if order.fulfillment != "pickup" {
        return read_error(
            ErrorCode::InvalidState,
            "Only pickup orders carry pickup details.",
        );
    }

    let snapshots: Result<Vec<crate::model::PickupLineSnapshotRow>, sqlx::Error> =
        sqlx::query_as(
            "SELECT order_id, line_index, listing_aggregate_id, version, snapshot_ciphertext, \
             confirming_adapter, created_at FROM pickup_line_snapshots \
             WHERE order_id = $1 ORDER BY line_index",
        )
        .bind(order.id)
        .fetch_all(&state.pool)
        .await;
    let snapshots = match snapshots {
        Ok(snapshots) => snapshots,
        Err(error) => return internal_error("pickup snapshots", &error),
    };
    // A snapshot pinned under `payment.sandbox_advance` is refused on every
    // read, checked against the pin and independent of the deployment's
    // current sandbox flag (§A3): a flag toggle window can never make a
    // fake-money order's past reveal free.
    if snapshots
        .iter()
        .any(|snapshot| snapshot.confirming_adapter == "sandbox")
    {
        return read_error(
            ErrorCode::InvalidState,
            "This order was confirmed by a sandbox payment; its pickup details are never revealed.",
        );
    }

    let mut lines = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        let plaintext = match keys.open(
            &snapshot_aad(order.id, snapshot.line_index, snapshot.version),
            &snapshot.snapshot_ciphertext,
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                return internal_error(
                    "pickup snapshot open",
                    &sqlx::Error::Protocol(
                        "sealed pickup snapshot did not authenticate".to_string(),
                    ),
                )
            }
        };
        let details: Value = match serde_json::from_slice(&plaintext) {
            Ok(details) => details,
            Err(_) => {
                return internal_error(
                    "pickup snapshot parse",
                    &sqlx::Error::Protocol("sealed pickup snapshot is not valid JSON".to_string()),
                )
            }
        };
        // Withdrawn-by-seller: the pinned snapshot stands in place of the
        // current details when a `pickup_details.clear` removed them (§A3).
        let current: Result<Option<(Option<i64>,)>, sqlx::Error> = sqlx::query_as(
            "SELECT MAX(version) FROM listing_pickup_details WHERE aggregate_id = $1",
        )
        .bind(&snapshot.listing_aggregate_id)
        .fetch_optional(&state.pool)
        .await;
        let current_version = match current {
            Ok(row) => row.and_then(|(version,)| version),
            Err(error) => return internal_error("pickup current details", &error),
        };
        lines.push(json!({
            "line_index": snapshot.line_index,
            "listing_aggregate_id": snapshot.listing_aggregate_id,
            "version": snapshot.version,
            "current_version": current_version,
            "updated_since_payment": current_version.is_some_and(|v| v > snapshot.version),
            "withdrawn_by_seller": current_version.is_none(),
            "updated_at": format_timestamp(snapshot.created_at),
            "details": details,
        }));
    }

    // The first successful read stamps the bounded withdrawal window (§A3);
    // later reads are no-ops.
    let now = state.clock.now();
    let stamped = sqlx::query(
        "UPDATE orders SET first_revealed_at = $2, updated_at = updated_at \
         WHERE id = $1 AND first_revealed_at IS NULL",
    )
    .bind(order.id)
    .bind(now)
    .execute(&state.pool)
    .await;
    if let Err(error) = stamped {
        return internal_error("first reveal stamp", &error);
    }
    let first_revealed_at = order.first_revealed_at.unwrap_or(now);

    no_store((
        StatusCode::OK,
        Json(json!({
            "order_id": order.id,
            "first_revealed_at": format_timestamp(first_revealed_at),
            "lines": lines,
        })),
    )
        .into_response())
}
