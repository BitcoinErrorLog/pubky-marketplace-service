//! Digital delivery seller setup (digital-delivery-design.md §2, §4.1, §6 C).
//!
//! `digital_delivery.set` / `.clear` are the only writes to a listing's
//! deliverable. Each set issues the next version through the per-listing
//! counter row, which survives `clear`, so a version never repeats: the
//! version is part of the homeserver path and of the associated data. A file
//! set reads the seller's homeserver ciphertext once to check its length and
//! BLAKE3; the bytes are hashed as they stream and dropped.
//!
//! Results, events and logs carry the deliverable id and version only. The
//! file key, link and text exist in plaintext only in the command payload and
//! the sealed row.

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    ClearDigitalDeliveryPayload, DigitalDelivery, DigitalDeliveryKind, SetDigitalDeliveryPayload,
    AES_GCM_TAG_BYTES,
};
use marketplace_domain::{Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, Transaction};

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::digital::{version_aad, DigitalKeys};
use crate::executor::insert_event;
use crate::handlers::{fetch_listing, fetch_listing_for_update};
use crate::homeserver::{DeliverableFetchOutcome, HomeserverListingClient};
use crate::model::ListingRow;
use crate::refusal_audit::RefusalKind;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};
use crate::AppState;

/// Order states in which a digital entitlement has ended. `completed` is
/// deliberately absent: a completed digital order still downloads (§3.4).
pub const DIGITAL_ENDED_ORDER_STATES: [&str; 3] = ["cancelled", "refunded_external", "closed"];

pub const REASON_UNAVAILABLE: &str = "digital_delivery_unavailable";
pub const REASON_IN_USE: &str = "digital_delivery_in_use";
pub const REASON_UNVERIFIABLE: &str = "deliverable_unverifiable";
pub const REASON_TOO_LARGE: &str = "deliverable_too_large";
pub const REASON_NOT_READY: &str = "digital_delivery_not_ready";

pub(crate) fn unavailable() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::InvalidState,
        "Digital delivery is unavailable on this deployment.",
        REASON_UNAVAILABLE,
    )
}

fn unverifiable(code: ErrorCode, kind: RefusalKind) -> CommandFailure {
    CommandFailure::refused_with_reason(
        kind,
        code,
        "The deliverable could not be read back from the seller's homeserver.",
        REASON_UNVERIFIABLE,
    )
}

fn events_aggregate_id(listing_aggregate_id: &str) -> String {
    let suffix = listing_aggregate_id
        .strip_prefix("listing:")
        .unwrap_or(listing_aggregate_id);
    format!("digital_delivery:{suffix}")
}

fn not_seller() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::Unauthorized,
        ErrorCode::Unauthorized,
        "Only the listing seller may manage its digital delivery.",
    )
}

fn listing_not_found() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::NotFound,
        ErrorCode::NotFound,
        "The listing was not found.",
    )
}

fn publishes_digital(listing: &ListingRow) -> bool {
    listing
        .fulfillment_methods
        .iter()
        .any(|method| method == "digital")
}

fn not_digital() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::InvalidState,
        ErrorCode::InvalidState,
        "The listing does not publish digital delivery.",
    )
}

#[derive(Debug, Clone, FromRow)]
struct VersionRow {
    deliverable_id: String,
    version: i64,
    kind: String,
    payload_ciphertext: Vec<u8>,
    content_type: Option<String>,
    size_bytes: Option<i64>,
    created_at: DateTime<Utc>,
}

async fn current_version(
    tx: &mut Transaction<'_, Postgres>,
    listing_aggregate_id: &str,
) -> Result<Option<VersionRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT deliverable_id, version, kind, payload_ciphertext, content_type, size_bytes, \
                created_at \
         FROM listing_digital_versions \
         WHERE listing_aggregate_id = $1 AND superseded_at IS NULL",
    )
    .bind(listing_aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Whether any buyer is paying for, or still entitled to, this listing's
/// digital delivery: an order in `pending_payment`, or one with a receipt
/// outside the ended set (§6 C4).
pub(crate) async fn listing_digital_in_use(
    tx: &mut Transaction<'_, Postgres>,
    listing_aggregate_id: &str,
) -> Result<bool, sqlx::Error> {
    let (in_use,): (bool,) = sqlx::query_as(
        "SELECT EXISTS ( \
             SELECT 1 FROM orders o \
             WHERE o.fulfillment = 'digital' \
               AND (o.state = 'pending_payment' \
                    OR (o.receipt_id IS NOT NULL \
                        AND o.state NOT IN ('cancelled', 'refunded_external', 'closed'))) \
               AND EXISTS ( \
                   SELECT 1 FROM jsonb_array_elements(o.lines) AS line \
                   WHERE line->>'listing_aggregate_id' = $1))",
    )
    .bind(listing_aggregate_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(in_use)
}

fn in_use() -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::InvalidState,
        "Buyers are paying for or still downloading this listing's digital delivery.",
        REASON_IN_USE,
    )
}

/// Locks (creating when absent) the listing's version counter and
/// compare-and-swaps `expected_version` against it.
async fn lock_counter_cas(
    tx: &mut Transaction<'_, Postgres>,
    listing: &ListingRow,
    expected_version: i64,
    now: DateTime<Utc>,
) -> Result<Result<i64, CommandFailure>, sqlx::Error> {
    sqlx::query(
        "INSERT INTO listing_digital_counters (listing_aggregate_id, seller_pubky, last_version, \
         updated_at) VALUES ($1, $2, 0, $3) ON CONFLICT (listing_aggregate_id) DO NOTHING",
    )
    .bind(&listing.aggregate_id)
    .bind(&listing.seller_pubky)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let (last_version,): (i64,) = sqlx::query_as(
        "SELECT last_version FROM listing_digital_counters \
         WHERE listing_aggregate_id = $1 FOR UPDATE",
    )
    .bind(&listing.aggregate_id)
    .fetch_one(&mut **tx)
    .await?;
    if last_version != expected_version {
        return Ok(Err(CommandFailure::refused_with_revision(
            RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The digital delivery version is stale.",
            last_version,
        )));
    }
    Ok(Ok(last_version))
}

async fn next_event_revision(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<i64, sqlx::Error> {
    let (next,): (i64,) =
        sqlx::query_as("SELECT COALESCE(MAX(revision), 0) + 1 FROM events WHERE aggregate_id = $1")
            .bind(aggregate_id)
            .fetch_one(&mut **tx)
            .await?;
    Ok(next)
}

/// Supersedes the current version and deletes superseded versions no live
/// order pins. A pinned version stays: its pin carries its own sealed copy,
/// and the row is what tells the seller which blobs buyers still download.
async fn supersede_versions(
    tx: &mut Transaction<'_, Postgres>,
    listing_aggregate_id: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE listing_digital_versions SET superseded_at = $2 \
         WHERE listing_aggregate_id = $1 AND superseded_at IS NULL",
    )
    .bind(listing_aggregate_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM listing_digital_versions v \
         WHERE v.listing_aggregate_id = $1 AND v.superseded_at IS NOT NULL \
           AND NOT EXISTS ( \
               SELECT 1 FROM order_digital_pins p JOIN orders o ON o.id = p.order_id \
               WHERE p.listing_aggregate_id = v.listing_aggregate_id \
                 AND p.version = v.version \
                 AND o.state NOT IN ('cancelled', 'refunded_external', 'closed'))",
    )
    .bind(listing_aggregate_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn random_deliverable_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// The sealed version payload by kind (§4.1).
fn sealed_payload(delivery: &DigitalDelivery) -> Value {
    match delivery {
        DigitalDelivery::File(file) => json!({
            "key": file.key,
            "iv": file.iv,
            "ciphertext_blake3": file.ciphertext_blake3,
            "plaintext_blake3": file.plaintext_blake3,
            "size_bytes": file.size_bytes,
            "content_type": file.content_type,
            "file_name": file.file_name,
        }),
        DigitalDelivery::Link { url } => json!({ "url": url }),
        DigitalDelivery::Text { text } => json!({ "text": text }),
        DigitalDelivery::Email {} | DigitalDelivery::Message {} => json!({}),
    }
}

/// Reads the file back from the seller's homeserver and checks it against
/// the declared length and BLAKE3.
async fn verify_file(
    homeserver: Option<&dyn HomeserverListingClient>,
    seller_pubky: &str,
    file: &marketplace_domain::commands::DigitalFile,
) -> Option<CommandFailure> {
    let Some(homeserver) = homeserver else {
        return Some(unverifiable(
            ErrorCode::UpstreamUnavailable,
            RefusalKind::UpstreamUnavailable,
        ));
    };
    let expected_len = (file.size_bytes + AES_GCM_TAG_BYTES) as u64;
    match homeserver
        .fetch_deliverable_digest(
            seller_pubky,
            &file.deliverable_id,
            file.version,
            expected_len,
        )
        .await
    {
        DeliverableFetchOutcome::Found { blake3, len }
            if len == expected_len && blake3 == file.ciphertext_blake3 =>
        {
            None
        }
        DeliverableFetchOutcome::Found { .. }
        | DeliverableFetchOutcome::NotFound
        | DeliverableFetchOutcome::TooLarge => Some(unverifiable(
            ErrorCode::InvalidState,
            RefusalKind::InvalidState,
        )),
        DeliverableFetchOutcome::Unavailable => Some(unverifiable(
            ErrorCode::UpstreamUnavailable,
            RefusalKind::UpstreamUnavailable,
        )),
    }
}

/// `digital_delivery.set` (§2, §6 C1–C5).
#[allow(clippy::too_many_arguments)]
pub async fn set(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SetDigitalDeliveryPayload,
    keys: Option<&DigitalKeys>,
    homeserver: Option<&dyn HomeserverListingClient>,
    max_bytes: i64,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(keys) = keys else {
        return Ok(Err(unavailable()));
    };
    // Ownership first, so nobody but the seller can make the service read a
    // homeserver path.
    let Some(listing) = fetch_listing(tx, &command.aggregate_id).await? else {
        return Ok(Err(listing_not_found()));
    };
    if listing.seller_pubky != actor {
        return Ok(Err(not_seller()));
    }
    if !publishes_digital(&listing) {
        return Ok(Err(not_digital()));
    }
    if let DigitalDelivery::File(file) = &payload.delivery {
        if file.size_bytes > max_bytes {
            return Ok(Err(CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "The file is larger than this deployment accepts.",
                REASON_TOO_LARGE,
            )));
        }
        // The read happens before any row lock, so a slow homeserver never
        // holds up checkouts of this listing.
        if let Some(failure) = verify_file(homeserver, &listing.seller_pubky, file).await {
            return Ok(Err(failure));
        }
    }

    let Some(listing) = fetch_listing_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(listing_not_found()));
    };
    if listing.seller_pubky != actor {
        return Ok(Err(not_seller()));
    }
    if !publishes_digital(&listing) {
        return Ok(Err(not_digital()));
    }
    let last_version = match lock_counter_cas(tx, &listing, payload.expected_version, now).await? {
        Ok(version) => version,
        Err(failure) => return Ok(Err(failure)),
    };
    let kind = payload.delivery.kind();
    if let Some(current) = current_version(tx, &listing.aggregate_id).await? {
        if current.kind != kind.as_str()
            && listing_digital_in_use(tx, &listing.aggregate_id).await?
        {
            return Ok(Err(in_use()));
        }
    }

    let new_version = last_version + 1;
    let deliverable_id = match &payload.delivery {
        DigitalDelivery::File(file) => file.deliverable_id.clone(),
        _ => random_deliverable_id(),
    };
    let plaintext = serde_json::to_vec(&sealed_payload(&payload.delivery))
        .expect("deliverable payload serializes");
    let ciphertext = keys.seal(
        &version_aad(&listing.aggregate_id, &deliverable_id, new_version, kind),
        &plaintext,
    );
    let (ciphertext_blake3, size_bytes, content_type) = match &payload.delivery {
        DigitalDelivery::File(file) => (
            Some(file.ciphertext_blake3.clone()),
            Some(file.size_bytes),
            Some(file.content_type.clone()),
        ),
        _ => (None, None, None),
    };

    supersede_versions(tx, &listing.aggregate_id, now).await?;
    sqlx::query(
        "INSERT INTO listing_digital_versions (listing_aggregate_id, seller_pubky, \
         deliverable_id, version, kind, payload_ciphertext, ciphertext_blake3, size_bytes, \
         content_type, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(&listing.aggregate_id)
    .bind(&listing.seller_pubky)
    .bind(&deliverable_id)
    .bind(new_version)
    .bind(kind.as_str())
    .bind(&ciphertext)
    .bind(&ciphertext_blake3)
    .bind(size_bytes)
    .bind(&content_type)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE listing_digital_counters SET last_version = $2, cleared_at = NULL, \
         updated_at = $3 WHERE listing_aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .bind(new_version)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE listings SET digital_delivery_kind = $2, digital_delivery_content_type = $3, \
         digital_delivery_size_bytes = $4 WHERE aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .bind(kind.as_str())
    .bind(&content_type)
    .bind(size_bytes)
    .execute(&mut **tx)
    .await?;

    let events_aggregate = events_aggregate_id(&listing.aggregate_id);
    let event_revision = next_event_revision(tx, &events_aggregate).await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &events_aggregate,
        event_revision,
        actor,
        "digital_delivery.set",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_version,
        event_ids: vec![event_id],
        result: json!({
            "kind": "digital_delivery",
            "listing_aggregate_id": listing.aggregate_id,
            "delivery_kind": kind.as_str(),
            "deliverable_id": deliverable_id,
            "version": new_version,
            "updated_at": format_timestamp(now),
        }),
    }))
}

/// `digital_delivery.clear` (§6 C4): refused while anyone is paying for or
/// still entitled to the listing's delivery. The counter survives.
pub async fn clear(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &ClearDigitalDeliveryPayload,
    keys: Option<&DigitalKeys>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if keys.is_none() {
        return Ok(Err(unavailable()));
    }
    let Some(listing) = fetch_listing_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(listing_not_found()));
    };
    if listing.seller_pubky != actor {
        return Ok(Err(not_seller()));
    }
    let last_version = match lock_counter_cas(tx, &listing, payload.expected_version, now).await? {
        Ok(version) => version,
        Err(failure) => return Ok(Err(failure)),
    };
    if listing_digital_in_use(tx, &listing.aggregate_id).await? {
        return Ok(Err(in_use()));
    }
    supersede_versions(tx, &listing.aggregate_id, now).await?;
    sqlx::query(
        "UPDATE listing_digital_counters SET cleared_at = $2, updated_at = $2 \
         WHERE listing_aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE listings SET digital_delivery_kind = NULL, digital_delivery_content_type = NULL, \
         digital_delivery_size_bytes = NULL WHERE aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .execute(&mut **tx)
    .await?;

    let events_aggregate = events_aggregate_id(&listing.aggregate_id);
    let event_revision = next_event_revision(tx, &events_aggregate).await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &events_aggregate,
        event_revision,
        actor,
        "digital_delivery.cleared",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: last_version,
        event_ids: vec![event_id],
        result: json!({
            "kind": "digital_delivery",
            "listing_aggregate_id": listing.aggregate_id,
            "version": last_version,
            "cleared": true,
            "updated_at": format_timestamp(now),
        }),
    }))
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
                "error": { "code": "INTERNAL", "message": "The projection could not be read." },
            })),
        )
            .into_response(),
    )
}

pub(crate) fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

/// `GET /v1/listings/{aggregate_id}/digital-delivery`: the seller's owner
/// read. The current deliverable as the seller set it (never the file key,
/// which the seller's browser generated), the counter for the next set's
/// compare-and-swap, and how many live orders pin each version.
pub async fn get_listing_digital_delivery(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let Some(keys) = state.digital.as_deref() else {
        return read_error(
            ErrorCode::InvalidState,
            "Digital delivery is unavailable on this deployment.",
            Some(REASON_UNAVAILABLE),
        );
    };
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error("digital delivery owner read"),
    };
    let listing = match fetch_listing(&mut tx, &aggregate_id).await {
        Ok(Some(listing)) if listing.seller_pubky == actor.0 => listing,
        Ok(_) => return read_error(ErrorCode::NotFound, "The listing was not found.", None),
        Err(_) => return internal_error("digital delivery owner read"),
    };
    let last_version: i64 = match sqlx::query_as::<_, (i64,)>(
        "SELECT last_version FROM listing_digital_counters WHERE listing_aggregate_id = $1",
    )
    .bind(&listing.aggregate_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row.map(|(version,)| version).unwrap_or(0),
        Err(_) => return internal_error("digital delivery counter read"),
    };
    let current = match current_version(&mut tx, &listing.aggregate_id).await {
        Ok(current) => current,
        Err(_) => return internal_error("digital delivery owner read"),
    };
    let current = match current {
        None => Value::Null,
        Some(row) => {
            let Some(kind) = DigitalDeliveryKind::parse(&row.kind) else {
                return internal_error("digital delivery kind");
            };
            let aad = version_aad(
                &listing.aggregate_id,
                &row.deliverable_id,
                row.version,
                kind,
            );
            let Ok(plaintext) = keys.open(&aad, &row.payload_ciphertext) else {
                return internal_error("digital delivery open");
            };
            let Ok(sealed) = serde_json::from_slice::<Value>(&plaintext) else {
                return internal_error("digital delivery parse");
            };
            let mut current = json!({
                "kind": kind.as_str(),
                "deliverable_id": row.deliverable_id,
                "version": row.version,
                "created_at": format_timestamp(row.created_at),
            });
            match kind {
                DigitalDeliveryKind::File => {
                    current["content_type"] = json!(row.content_type);
                    current["size_bytes"] = json!(row.size_bytes);
                    current["file_name"] = sealed["file_name"].clone();
                }
                DigitalDeliveryKind::Link => current["url"] = sealed["url"].clone(),
                DigitalDeliveryKind::Text => current["text"] = sealed["text"].clone(),
                DigitalDeliveryKind::Email | DigitalDeliveryKind::Message => {}
            }
            current
        }
    };
    let pinned: Vec<(i64, i64)> = match sqlx::query_as(
        "SELECT p.version, COUNT(DISTINCT p.order_id) FROM order_digital_pins p \
         JOIN orders o ON o.id = p.order_id \
         WHERE p.listing_aggregate_id = $1 \
           AND o.state NOT IN ('cancelled', 'refunded_external', 'closed') \
         GROUP BY p.version ORDER BY p.version",
    )
    .bind(&listing.aggregate_id)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(rows) => rows,
        Err(_) => return internal_error("digital delivery pin counts"),
    };
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "listing_aggregate_id": listing.aggregate_id,
                "current": current,
                "last_version": last_version,
                "pinned_versions": pinned
                    .into_iter()
                    .map(|(version, orders)| json!({ "version": version, "live_orders": orders }))
                    .collect::<Vec<_>>(),
            })),
        )
            .into_response(),
    )
}
