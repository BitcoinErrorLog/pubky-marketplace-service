//! Phase 6 automation APIs: seller exports, cursor events, bulk listing
//! synchronization, and durable signed webhooks.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, Row};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::handlers::LISTING_COLUMNS;
use crate::homeserver::HomeserverRawFetchOutcome;
use crate::model::ListingRow;
use crate::AppState;

pub const DEFAULT_PAGE_SIZE: i64 = 50;
pub const MAX_PAGE_SIZE: i64 = 200;
pub const MAX_SYNC_MANY: usize = 100;
pub const WEBHOOK_BODY_LIMIT: usize = 64 * 1024;
const DELIVERY_BATCH_SIZE: i64 = 50;
const WEBHOOK_VERSION_HEADER: &str = "Pubky-Webhook-Version";
const WEBHOOK_ID_HEADER: &str = "Pubky-Webhook-Id";
const WEBHOOK_TIMESTAMP_HEADER: &str = "Pubky-Webhook-Timestamp";
const WEBHOOK_KEY_ID_HEADER: &str = "Pubky-Webhook-Key-Id";
const WEBHOOK_SIGNATURE_HEADER: &str = "Pubky-Webhook-Signature";

fn endpoint_class(path: &str) -> &'static str {
    if path.starts_with("/v1/auth/sessions") {
        "session.admin"
    } else if path == "/v1/listings/sync-many" {
        "listing.sync_many"
    } else if path.starts_with("/v1/webhooks") {
        "webhook.admin"
    } else if path.ends_with("/orders") {
        "export.orders"
    } else if path.ends_with("/events") {
        "events.read"
    } else {
        "export.listings"
    }
}

pub async fn require_automation_rate(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(actor) = request.extensions().get::<Actor>() else {
        return error(
            StatusCode::UNAUTHORIZED,
            "session_required",
            "A session bearer token is required.",
        );
    };
    let class = endpoint_class(request.uri().path());
    match consume_automation_rate(&state, &actor.0, class).await {
        Ok(Some(retry_after)) => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "The request rate limit was exceeded.",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&retry_after.to_string())
                    .expect("retry-after is a safe integer"),
            );
            response
        }
        Ok(None) => next.run(request).await,
        Err(cause) => internal("automation rate limit", &cause),
    }
}

pub async fn consume_automation_rate(
    state: &AppState,
    actor: &str,
    class: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let now = state.clock.now();
    let rate = state.config.automation_rate_limit_per_minute as f64;
    let capacity = (state.config.automation_rate_limit_per_minute
        * state.config.automation_rate_limit_burst_multiplier) as f64;
    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6353))")
        .bind(format!("{actor}:{class}"))
        .execute(&mut *tx)
        .await?;
    let row: Option<(f64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tokens, updated_at FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = $2 FOR UPDATE",
    )
    .bind(actor)
    .bind(class)
    .fetch_optional(&mut *tx)
    .await?;
    let (stored, updated_at) = row.unwrap_or((capacity, now));
    let elapsed = (now - updated_at).num_milliseconds().max(0) as f64 / 1_000.0;
    let available = (stored + elapsed * rate / 60.0).min(capacity);
    let (tokens, retry_after) = if available >= 1.0 {
        (available - 1.0, None)
    } else {
        let seconds = ((1.0 - available) * 60.0 / rate).ceil().max(1.0) as i64;
        (available, Some(seconds))
    };
    sqlx::query(
        "INSERT INTO automation_rate_limits \
         (seller_pubky, endpoint_class, tokens, updated_at) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (seller_pubky, endpoint_class) DO UPDATE \
         SET tokens = EXCLUDED.tokens, updated_at = EXCLUDED.updated_at",
    )
    .bind(actor)
    .bind(class)
    .bind(tokens)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(retry_after)
}

fn response(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    response(
        status,
        json!({"ok": false, "error": {"code": code, "message": message}}),
    )
}

fn internal(context: &str, cause: &sqlx::Error) -> Response {
    tracing::error!(error = %cause, "{context} failed");
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "The request could not be completed.",
    )
}

#[derive(Debug, Deserialize)]
pub struct PageQuery {
    limit: Option<i64>,
    cursor: Option<String>,
}

impl PageQuery {
    fn limit(&self) -> Option<i64> {
        match self.limit.unwrap_or(DEFAULT_PAGE_SIZE) {
            limit @ 1..=MAX_PAGE_SIZE => Some(limit),
            _ => None,
        }
    }

    fn sequence(&self) -> Option<Result<i64, ()>> {
        self.cursor.as_ref().map(|cursor| decode_cursor(cursor))
    }

    fn page_position(&self) -> Result<(i64, String), ()> {
        self.cursor
            .as_ref()
            .map(|cursor| decode_page_cursor(cursor))
            .unwrap_or_else(|| Ok((0, String::new())))
    }
}

fn encode_cursor(sequence: i64) -> String {
    URL_SAFE_NO_PAD.encode(sequence.to_be_bytes())
}

fn decode_cursor(cursor: &str) -> Result<i64, ()> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| ())?;
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| ())?;
    let value = i64::from_be_bytes(bytes);
    (value >= 0).then_some(value).ok_or(())
}

fn encode_page_cursor(revision: i64, identity: &str) -> String {
    URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&(revision, identity)).expect("page cursor tuple serializes"))
}

fn decode_page_cursor(cursor: &str) -> Result<(i64, String), ()> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| ())?;
    let (revision, identity): (i64, String) = serde_json::from_slice(&bytes).map_err(|_| ())?;
    if revision < 0 || identity.len() > 256 || identity.chars().any(char::is_control) {
        return Err(());
    }
    Ok((revision, identity))
}

fn etag(body: &Value) -> String {
    let bytes = serde_json::to_vec(body).expect("JSON value serializes");
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}

fn conditional_json(headers: &HeaderMap, body: Value) -> Response {
    let tag = etag(&body);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == tag)
    {
        return StatusCode::NOT_MODIFIED.into_response();
    }
    let mut response = (StatusCode::OK, Json(body)).into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&tag).expect("etag is a safe header value"),
    );
    response
}

pub async fn list_seller_listings(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(seller): Path<String>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if actor.0 != seller {
        return error(
            StatusCode::FORBIDDEN,
            "seller_required",
            "Seller access is required.",
        );
    }
    let Some(limit) = query.limit() else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_limit",
            "The page limit is invalid.",
        );
    };
    let (after_revision, after_id) = match query.page_position() {
        Ok(value) => value,
        Err(()) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_cursor",
                "The cursor is invalid.",
            )
        }
    };
    let rows: Vec<ListingRow> = match sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE seller_pubky = $1 \
         AND (server_revision > $2 OR (server_revision = $2 AND aggregate_id > $3)) \
         ORDER BY server_revision, aggregate_id LIMIT $4"
    ))
    .bind(&seller)
    .bind(after_revision)
    .bind(&after_id)
    .bind(limit + 1)
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => return internal("seller listing export", &error),
    };
    let more = rows.len() as i64 > limit;
    let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let next_cursor = more
        .then(|| {
            rows.last()
                .map(|row| encode_page_cursor(row.server_revision, &row.aggregate_id))
        })
        .flatten();
    let mut listings = Vec::with_capacity(rows.len());
    for row in rows {
        match exported_listing(&state, &row).await {
            Ok(listing) => listings.push(listing),
            Err(_) => {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "listing_record_unavailable",
                    "A canonical listing record could not be exported.",
                )
            }
        }
    }
    conditional_json(
        &headers,
        json!({
            "schema_version": 1,
            "kind": "seller_listing_export",
            "seller_pubky": seller,
            "listings": listings,
            "next_cursor": next_cursor
        }),
    )
}

pub async fn get_seller_listing(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path((seller, listing_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if actor.0 != seller {
        return error(
            StatusCode::FORBIDDEN,
            "seller_required",
            "Seller access is required.",
        );
    }
    let row: Option<ListingRow> = match sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE seller_pubky = $1 AND listing_id = $2"
    ))
    .bind(&seller)
    .bind(&listing_id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(error) => return internal("seller listing read", &error),
    };
    let Some(row) = row else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "The listing was not found.",
        );
    };
    match exported_listing(&state, &row).await {
        Ok(listing) => conditional_json(
            &headers,
            json!({"schema_version": 1, "kind": "seller_listing", "listing": listing}),
        ),
        Err(_) => error(
            StatusCode::BAD_GATEWAY,
            "listing_record_unavailable",
            "The canonical listing record could not be exported.",
        ),
    }
}

async fn exported_listing(state: &AppState, row: &ListingRow) -> Result<Value, &'static str> {
    let homeserver = state.homeserver.as_deref().ok_or("homeserver disabled")?;
    let raw = match homeserver
        .fetch_listing_raw(&row.seller_pubky, &row.listing_id)
        .await
    {
        HomeserverRawFetchOutcome::Found(raw) => raw,
        HomeserverRawFetchOutcome::NotFound
        | HomeserverRawFetchOutcome::TooLarge
        | HomeserverRawFetchOutcome::Unavailable => return Err("record unavailable"),
    };
    let record: Value = serde_json::from_slice(&raw).map_err(|_| "record malformed")?;
    let projection = row
        .public_projection()
        .map_err(|_| "projection inconsistent")?;
    let record_uri = format!(
        "pubky://{}/pub/pubky.app/marketplace/v1/listings/{}",
        row.seller_pubky, row.listing_id
    );
    Ok(json!({
        "record_uri": record_uri,
        "record": record,
        "record_bytes_base64": base64::engine::general_purpose::STANDARD.encode(&raw),
        "record_sha256": hex::encode(Sha256::digest(&raw)),
        "projection": projection
    }))
}

pub async fn list_seller_orders(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(seller): Path<String>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if actor.0 != seller {
        return error(
            StatusCode::FORBIDDEN,
            "seller_required",
            "Seller access is required.",
        );
    }
    let Some(limit) = query.limit() else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_limit",
            "The page limit is invalid.",
        );
    };
    let (after_revision, after_id) = match query.page_position() {
        Ok(value) => value,
        Err(()) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_cursor",
                "The cursor is invalid.",
            )
        }
    };
    // Dedicated orders-only export. Do not replace this with list_orders:
    // this query deliberately cannot access payments or private addresses.
    let rows = match sqlx::query(
        "SELECT id, buyer_pubky, seller_pubky, revision, state, lines, \
         subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, receipt_id, edition, drop_aggregate_id, \
         cancellation_reason, stock_held, hold_expires_at, shipment, \
         delivery_assumed, return_request, external_refund, fulfillment, \
         payment_method, created_at, updated_at \
         FROM orders WHERE seller_pubky = $1 \
         AND (revision > $2 OR (revision = $2 AND id::text > $3)) \
         ORDER BY revision, id LIMIT $4",
    )
    .bind(&seller)
    .bind(after_revision)
    .bind(&after_id)
    .bind(limit + 1)
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => return internal("seller order export", &error),
    };
    let more = rows.len() as i64 > limit;
    let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let next_cursor = more
        .then(|| {
            rows.last()
                .and_then(|row| row.try_get::<i64, _>("revision").ok())
                .zip(
                    rows.last()
                        .and_then(|row| row.try_get::<Uuid, _>("id").ok()),
                )
                .map(|(revision, id)| encode_page_cursor(revision, &id.to_string()))
        })
        .flatten();
    let orders: Result<Vec<Value>, sqlx::Error> = rows
        .iter()
        .map(|row| {
            Ok(json!({
                "id": row.try_get::<Uuid, _>("id")?,
                "buyer_pubky": row.try_get::<String, _>("buyer_pubky")?,
                "seller_pubky": row.try_get::<String, _>("seller_pubky")?,
                "revision": row.try_get::<i64, _>("revision")?,
                "state": row.try_get::<String, _>("state")?,
                "lines": row.try_get::<Value, _>("lines")?,
                "subtotal_minor": row.try_get::<i64, _>("subtotal_minor")?,
                "shipping_minor": row.try_get::<i64, _>("shipping_minor")?,
                "total_minor": row.try_get::<i64, _>("total_minor")?,
                "currency": row.try_get::<String, _>("currency")?,
                "exponent": row.try_get::<i32, _>("exponent")?,
                "guarantee_policy_version": row.try_get::<i32, _>("guarantee_policy_version")?,
                "receipt_id": row.try_get::<Option<Uuid>, _>("receipt_id")?,
                "edition": row.try_get::<Option<i32>, _>("edition")?,
                "drop_aggregate_id": row.try_get::<Option<String>, _>("drop_aggregate_id")?,
                "cancellation_reason": row.try_get::<Option<String>, _>("cancellation_reason")?,
                "stock_held": row.try_get::<bool, _>("stock_held")?,
                "hold_expires_at": row.try_get::<Option<DateTime<Utc>>, _>("hold_expires_at")?
                    .map(format_timestamp),
                "shipment": row.try_get::<Option<Value>, _>("shipment")?,
                "delivery_assumed": row.try_get::<bool, _>("delivery_assumed")?,
                "return_request": row.try_get::<Option<Value>, _>("return_request")?,
                "external_refund": row.try_get::<Option<Value>, _>("external_refund")?,
                "fulfillment": row.try_get::<String, _>("fulfillment")?,
                "payment_method": row.try_get::<Option<String>, _>("payment_method")?,
                "created_at": format_timestamp(row.try_get::<DateTime<Utc>, _>("created_at")?),
                "updated_at": format_timestamp(row.try_get::<DateTime<Utc>, _>("updated_at")?)
            }))
        })
        .collect();
    match orders {
        Ok(orders) => conditional_json(
            &headers,
            json!({
                "schema_version": 1,
                "kind": "seller_order_export",
                "seller_pubky": seller,
                "orders": orders,
                "next_cursor": next_cursor
            }),
        ),
        Err(error) => internal("seller order serialization", &error),
    }
}

#[derive(FromRow)]
struct EventRow {
    id: Uuid,
    sequence: i64,
    aggregate_id: String,
    revision: i64,
    kind: String,
    occurred_at: DateTime<Utc>,
}

const SELLER_EVENT_FROM: &str = "FROM events e \
     LEFT JOIN listings l ON l.aggregate_id = e.aggregate_id \
     LEFT JOIN orders o ON e.aggregate_id = 'order:' || o.id::text \
     LEFT JOIN offers f ON f.aggregate_id = e.aggregate_id \
     LEFT JOIN drops d ON d.aggregate_id = e.aggregate_id";

pub async fn list_seller_events(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(seller): Path<String>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Response {
    if actor.0 != seller {
        return error(
            StatusCode::FORBIDDEN,
            "seller_required",
            "Seller access is required.",
        );
    }
    let Some(limit) = query.limit() else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_limit",
            "The page limit is invalid.",
        );
    };
    let after = match query.sequence().transpose() {
        Ok(value) => value.unwrap_or(0),
        Err(()) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_cursor",
                "The cursor is invalid.",
            )
        }
    };
    let cutoff = state.clock.now() - chrono::Duration::days(state.config.event_retention_days);
    let minimum: Option<i64> = match sqlx::query_scalar(&format!(
        "SELECT MIN(e.sequence) {SELLER_EVENT_FROM} \
         WHERE COALESCE(l.seller_pubky, o.seller_pubky, f.seller_pubky, d.seller_pubky) = $1 \
         AND e.occurred_at >= $2"
    ))
    .bind(&seller)
    .bind(cutoff)
    .fetch_one(&state.pool)
    .await
    {
        Ok(value) => value,
        Err(error) => return internal("event retention boundary", &error),
    };
    if after > 0 {
        let cursor_time: Option<DateTime<Utc>> = match sqlx::query_scalar(&format!(
            "SELECT e.occurred_at {SELLER_EVENT_FROM} \
             WHERE COALESCE(l.seller_pubky, o.seller_pubky, f.seller_pubky, d.seller_pubky) = $1 \
               AND e.sequence = $2"
        ))
        .bind(&seller)
        .bind(after)
        .fetch_optional(&state.pool)
        .await
        {
            Ok(value) => value,
            Err(error) => return internal("event cursor lookup", &error),
        };
        if cursor_time.is_some_and(|occurred_at| occurred_at < cutoff)
            || (cursor_time.is_none()
                && minimum.is_some_and(|minimum| after < minimum.saturating_sub(1)))
        {
            return error(
                StatusCode::GONE,
                "cursor_expired",
                "The cursor has expired; perform a full pull.",
            );
        }
        if cursor_time.is_none() {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_cursor",
                "The cursor was not issued for this seller.",
            );
        }
    }
    let rows: Vec<EventRow> = match sqlx::query_as(&format!(
        "SELECT e.id, e.sequence, e.aggregate_id, e.revision, e.kind, e.occurred_at \
         {SELLER_EVENT_FROM} \
         WHERE COALESCE(l.seller_pubky, o.seller_pubky, f.seller_pubky, d.seller_pubky) = $1 \
         AND e.sequence > $2 AND e.occurred_at >= $3 \
         ORDER BY e.sequence LIMIT $4"
    ))
    .bind(&seller)
    .bind(after)
    .bind(cutoff)
    .bind(limit + 1)
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => return internal("seller event feed", &error),
    };
    let more = rows.len() as i64 > limit;
    let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let next_cursor = rows
        .last()
        .map(|row| encode_cursor(row.sequence))
        .filter(|_| more);
    let events = rows
        .into_iter()
        .map(|row| event_value(&seller, &row))
        .collect::<Vec<_>>();
    conditional_json(
        &headers,
        json!({
            "schema_version": 1,
            "kind": "seller_event_feed",
            "seller_pubky": seller,
            "events": events,
            "next_cursor": next_cursor
        }),
    )
}

fn event_value(seller: &str, row: &EventRow) -> Value {
    json!({
        "schema_version": 1,
        "id": row.id,
        "cursor": encode_cursor(row.sequence),
        "sequence": row.sequence,
        "seller_pubky": seller,
        "aggregate_id": row.aggregate_id,
        "revision": row.revision,
        "type": row.kind,
        "occurred_at": format_timestamp(row.occurred_at),
        "data": {}
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncManyRequest {
    listings: Vec<SyncManyItem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncManyItem {
    seller_pubky: String,
    listing_id: String,
}

pub async fn sync_many(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(request): Json<SyncManyRequest>,
) -> Response {
    if request.listings.is_empty() || request.listings.len() > MAX_SYNC_MANY {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_batch",
            "The batch must contain between 1 and 100 listings.",
        );
    }
    let mut results = Vec::with_capacity(request.listings.len());
    for item in request.listings {
        let aggregate_id =
            marketplace_domain::ids::listing_aggregate_id(&item.seller_pubky, &item.listing_id);
        let raw = json!({
            "version": 1,
            "command_id": Uuid::new_v4(),
            "aggregate_id": aggregate_id,
            "expected_revision": 0,
            "issued_at": format_timestamp(state.clock.now()),
            "kind": "listing.sync",
            "payload": {
                "seller_pubky": item.seller_pubky,
                "listing_id": item.listing_id
            }
        });
        match crate::executor::execute(&state, &actor.0, &raw).await {
            Ok((status, body)) => results.push(json!({
                "seller_pubky": item.seller_pubky,
                "listing_id": item.listing_id,
                "status": status.as_u16(),
                "result": body
            })),
            Err(error) => {
                tracing::error!(error = %error, "listing.sync_many item failed");
                results.push(json!({
                    "seller_pubky": item.seller_pubky,
                    "listing_id": item.listing_id,
                    "status": 500,
                    "result": {"ok": false, "error": {"code": "internal", "message": "The listing could not be synchronized."}}
                }));
            }
        }
    }
    response(
        StatusCode::MULTI_STATUS,
        json!({"schema_version": 1, "kind": "listing.sync_many", "results": results}),
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddWebhookRequest {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotateWebhookRequest {}

fn validate_webhook_url(raw: &str) -> Result<url::Url, &'static str> {
    let url = url::Url::parse(raw).map_err(|_| "The webhook URL is invalid.")?;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(443)
        || url.host_str().is_none()
    {
        return Err("Webhook URLs must use HTTPS on port 443 without credentials or fragments.");
    }
    let literal_ip = match url.host() {
        Some(url::Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
        _ => None,
    };
    if literal_ip.is_some_and(|ip| !public_ip(ip)) {
        return Err("Webhook URLs cannot use a private or special IP address.");
    }
    Ok(url)
}

fn new_signing_material() -> (String, Uuid, Vec<u8>) {
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    let encoded = URL_SAFE_NO_PAD.encode(secret);
    let key = Sha256::digest(secret).to_vec();
    (encoded, Uuid::new_v4(), key)
}

pub async fn add_webhook(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(request): Json<AddWebhookRequest>,
) -> Response {
    let url = match validate_webhook_url(&request.url) {
        Ok(url) => url.to_string(),
        Err(message) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_webhook_url",
                message,
            )
        }
    };
    let (secret, key_id, signing_key) = new_signing_material();
    let id = Uuid::new_v4();
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal("webhook registration transaction", &error),
    };
    if let Err(error) = sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6355))")
        .bind(&actor.0)
        .execute(&mut *tx)
        .await
    {
        return internal("webhook seller quota lock", &error);
    }
    let endpoint_count: i64 = match sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_endpoints \
         WHERE seller_pubky = $1 AND deleted_at IS NULL",
    )
    .bind(&actor.0)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(count) => count,
        Err(error) => return internal("webhook seller quota", &error),
    };
    if endpoint_count >= state.config.webhook_max_endpoints_per_seller {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "webhook_endpoint_limit",
            "The seller webhook endpoint limit was reached.",
        );
    }
    let result = sqlx::query(
        "INSERT INTO webhook_endpoints \
         (id, seller_pubky, endpoint_url, key_id, signing_key, enqueue_sequence, \
          created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, (SELECT COALESCE(MAX(sequence), 0) FROM events), $6, $6)",
    )
    .bind(id)
    .bind(&actor.0)
    .bind(&url)
    .bind(key_id)
    .bind(signing_key)
    .bind(now)
    .execute(&mut *tx)
    .await;
    match result {
        Ok(_) => {
            if let Err(error) = tx.commit().await {
                return internal("webhook registration commit", &error);
            }
            response(
                StatusCode::CREATED,
                json!({
                    "schema_version": 1,
                    "webhook": {"id": id, "url": url, "key_id": key_id, "created_at": format_timestamp(now)},
                    "secret": secret
                }),
            )
        }
        Err(sqlx::Error::Database(database)) if database.is_unique_violation() => error(
            StatusCode::CONFLICT,
            "webhook_exists",
            "That webhook endpoint already exists.",
        ),
        Err(error) => internal("webhook registration", &error),
    }
}

pub async fn rotate_webhook(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
    Json(_request): Json<RotateWebhookRequest>,
) -> Response {
    let (secret, key_id, signing_key) = new_signing_material();
    let now = state.clock.now();
    let changed = match sqlx::query(
        "UPDATE webhook_endpoints SET key_id = $3, signing_key = $4, updated_at = $5 \
         WHERE id = $1 AND seller_pubky = $2 AND deleted_at IS NULL",
    )
    .bind(id)
    .bind(&actor.0)
    .bind(key_id)
    .bind(signing_key)
    .bind(now)
    .execute(&state.pool)
    .await
    {
        Ok(result) => result.rows_affected(),
        Err(error) => return internal("webhook rotation", &error),
    };
    if changed == 0 {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "The webhook was not found.",
        );
    }
    response(
        StatusCode::OK,
        json!({"schema_version": 1, "id": id, "key_id": key_id, "secret": secret}),
    )
}

pub async fn delete_webhook(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal("webhook deletion transaction", &error),
    };
    if let Err(error) = sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6354))")
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
    {
        return internal("webhook deletion fence", &error);
    }
    let changed = match sqlx::query(
        "UPDATE webhook_endpoints SET deleted_at = $3, updated_at = $3 \
         WHERE id = $1 AND seller_pubky = $2 AND deleted_at IS NULL",
    )
    .bind(id)
    .bind(&actor.0)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result.rows_affected(),
        Err(error) => return internal("webhook deletion", &error),
    };
    if changed == 0 {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "The webhook was not found.",
        );
    }
    if let Err(error) = sqlx::query(
        "UPDATE webhook_deliveries SET dead_lettered_at = $2, lease_until = NULL \
         WHERE endpoint_id = $1 AND delivered_at IS NULL AND dead_lettered_at IS NULL",
    )
    .bind(id)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        return internal("webhook delivery cancellation", &error);
    }
    if let Err(error) = tx.commit().await {
        return internal("webhook deletion commit", &error);
    }
    StatusCode::NO_CONTENT.into_response()
}

#[derive(FromRow)]
struct Delivery {
    endpoint_id: Uuid,
    event_id: Uuid,
    endpoint_url: String,
    key_id: Uuid,
    signing_key: Vec<u8>,
    body: Value,
    attempt_count: i32,
    created_at: DateTime<Utc>,
}

struct SignedWebhookRequest {
    raw: Vec<u8>,
    timestamp: String,
    event_id: String,
    key_id: String,
    signature: String,
}

pub async fn run_webhook_pass(state: &AppState) -> anyhow::Result<u64> {
    let now = state.clock.now();
    purge_terminal_webhooks(state, now).await?;
    enqueue_webhook_deliveries(state, now).await?;
    let lease_until = now + chrono::Duration::seconds(state.config.webhook_lease_seconds);
    let deliveries: Vec<Delivery> = sqlx::query_as(
        "UPDATE webhook_deliveries d SET lease_until = $2 \
         FROM webhook_endpoints w \
         WHERE (d.endpoint_id, d.event_id) IN ( \
           SELECT endpoint_id, event_id FROM webhook_deliveries \
           WHERE delivered_at IS NULL AND dead_lettered_at IS NULL \
             AND next_attempt_at <= $1 AND (lease_until IS NULL OR lease_until <= $1) \
           ORDER BY next_attempt_at, endpoint_id, event_id \
           FOR UPDATE SKIP LOCKED LIMIT $3 \
         ) AND w.id = d.endpoint_id AND w.deleted_at IS NULL \
         RETURNING d.endpoint_id, d.event_id, w.endpoint_url, d.key_id, \
                   d.signing_key, d.body, d.attempt_count, d.created_at",
    )
    .bind(now)
    .bind(lease_until)
    .bind(DELIVERY_BATCH_SIZE)
    .fetch_all(&state.pool)
    .await?;
    let mut terminal = 0;
    for delivery in deliveries {
        let mut tx = state.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6354))")
            .bind(delivery.endpoint_id.to_string())
            .execute(&mut *tx)
            .await?;
        let still_active: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM webhook_endpoints \
             WHERE id = $1 AND deleted_at IS NULL)",
        )
        .bind(delivery.endpoint_id)
        .fetch_one(&mut *tx)
        .await?;
        if !still_active {
            tx.commit().await?;
            continue;
        }
        let status = deliver(&delivery, now).await.ok().map(i32::from);
        if persist_delivery_outcome(&mut tx, &state.config, &delivery, now, status).await? {
            terminal += 1;
        }
        tx.commit().await?;
    }
    Ok(terminal)
}

async fn persist_delivery_outcome(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::config::Config,
    delivery: &Delivery,
    now: DateTime<Utc>,
    status: Option<i32>,
) -> Result<bool, sqlx::Error> {
    let attempt = delivery.attempt_count + 1;
    let age = now - delivery.created_at;
    if status.is_some_and(|status| (200..300).contains(&status)) {
        sqlx::query(
            "UPDATE webhook_deliveries SET attempt_count = $3, last_status = $4, \
             delivered_at = $5, lease_until = NULL \
             WHERE endpoint_id = $1 AND event_id = $2 AND delivered_at IS NULL \
               AND dead_lettered_at IS NULL",
        )
        .bind(delivery.endpoint_id)
        .bind(delivery.event_id)
        .bind(attempt)
        .bind(status)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        return Ok(true);
    }
    if attempt >= config.webhook_max_attempts
        || age >= chrono::Duration::hours(config.webhook_max_age_hours)
    {
        sqlx::query(
            "UPDATE webhook_deliveries SET attempt_count = $3, last_status = $4, \
             dead_lettered_at = $5, lease_until = NULL \
             WHERE endpoint_id = $1 AND event_id = $2 AND delivered_at IS NULL \
               AND dead_lettered_at IS NULL",
        )
        .bind(delivery.endpoint_id)
        .bind(delivery.event_id)
        .bind(attempt)
        .bind(status)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO webhook_dead_letters \
             (endpoint_id, event_id, attempt_count, final_status, dead_lettered_at) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(delivery.endpoint_id)
        .bind(delivery.event_id)
        .bind(attempt)
        .bind(status)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        return Ok(true);
    }
    let shift = u32::try_from(attempt.saturating_sub(1).min(20)).unwrap_or(20);
    let backoff = config
        .webhook_retry_base_seconds
        .saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(config.webhook_retry_max_seconds);
    sqlx::query(
        "UPDATE webhook_deliveries SET attempt_count = $3, last_status = $4, \
         next_attempt_at = $5, lease_until = NULL \
         WHERE endpoint_id = $1 AND event_id = $2 AND delivered_at IS NULL \
           AND dead_lettered_at IS NULL",
    )
    .bind(delivery.endpoint_id)
    .bind(delivery.event_id)
    .bind(attempt)
    .bind(status)
    .bind(now + chrono::Duration::seconds(backoff))
    .execute(&mut **tx)
    .await?;
    Ok(false)
}

#[cfg(any(test, feature = "test-faults"))]
pub async fn record_webhook_outcome_for_proof(
    state: &AppState,
    endpoint_id: Uuid,
    event_id: Uuid,
    status: Option<i32>,
) -> anyhow::Result<bool> {
    let mut tx = state.pool.begin().await?;
    let delivery: Delivery = sqlx::query_as(
        "SELECT d.endpoint_id, d.event_id, w.endpoint_url, d.key_id, d.signing_key, \
                d.body, d.attempt_count, d.created_at \
         FROM webhook_deliveries d JOIN webhook_endpoints w ON w.id = d.endpoint_id \
         WHERE d.endpoint_id = $1 AND d.event_id = $2 FOR UPDATE",
    )
    .bind(endpoint_id)
    .bind(event_id)
    .fetch_one(&mut *tx)
    .await?;
    let terminal =
        persist_delivery_outcome(&mut tx, &state.config, &delivery, state.clock.now(), status)
            .await?;
    tx.commit().await?;
    Ok(terminal)
}

async fn purge_terminal_webhooks(state: &AppState, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let cutoff = now - chrono::Duration::days(state.config.webhook_terminal_retention_days);
    let dead_letters = sqlx::query(
        "WITH due AS ( \
           SELECT endpoint_id, event_id FROM webhook_dead_letters \
           WHERE dead_lettered_at < $1 ORDER BY dead_lettered_at \
           LIMIT $2 \
         ) DELETE FROM webhook_dead_letters d USING due \
         WHERE d.endpoint_id = due.endpoint_id AND d.event_id = due.event_id",
    )
    .bind(cutoff)
    .bind(state.config.webhook_purge_batch_size)
    .execute(&state.pool)
    .await?
    .rows_affected();
    let deliveries = sqlx::query(
        "WITH due AS ( \
           SELECT endpoint_id, event_id FROM webhook_deliveries \
           WHERE COALESCE(delivered_at, dead_lettered_at) < $1 \
           ORDER BY COALESCE(delivered_at, dead_lettered_at) \
           LIMIT $2 \
         ) DELETE FROM webhook_deliveries d USING due \
         WHERE d.endpoint_id = due.endpoint_id AND d.event_id = due.event_id",
    )
    .bind(cutoff)
    .bind(state.config.webhook_purge_batch_size)
    .execute(&state.pool)
    .await?
    .rows_affected();
    Ok(dead_letters + deliveries)
}

async fn enqueue_webhook_deliveries(state: &AppState, now: DateTime<Utc>) -> anyhow::Result<()> {
    let endpoint_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM webhook_endpoints WHERE deleted_at IS NULL \
         ORDER BY enqueue_checked_at, id LIMIT $1",
    )
    .bind(state.config.webhook_enqueue_endpoints_per_pass)
    .fetch_all(&state.pool)
    .await?;
    let cutoff = now - chrono::Duration::days(state.config.event_retention_days);
    for endpoint_id in endpoint_ids {
        let mut tx = state.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6354))")
            .bind(endpoint_id.to_string())
            .execute(&mut *tx)
            .await?;
        let endpoint: Option<(String, Uuid, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT seller_pubky, key_id, signing_key, enqueue_sequence \
             FROM webhook_endpoints WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
        )
        .bind(endpoint_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((seller, key_id, signing_key, cursor)) = endpoint else {
            tx.commit().await?;
            continue;
        };
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6355))")
            .bind(&seller)
            .execute(&mut *tx)
            .await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM webhook_deliveries d \
             JOIN webhook_endpoints w ON w.id = d.endpoint_id \
             WHERE w.seller_pubky = $1 AND d.delivered_at IS NULL \
               AND d.dead_lettered_at IS NULL",
        )
        .bind(&seller)
        .fetch_one(&mut *tx)
        .await?;
        let room = state
            .config
            .webhook_max_pending_per_seller
            .saturating_sub(pending);
        if room <= 0 {
            sqlx::query("UPDATE webhook_endpoints SET enqueue_checked_at = $2 WHERE id = $1")
                .bind(endpoint_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            continue;
        }
        let limit = room.min(state.config.webhook_enqueue_batch_size);
        let rows: Vec<EventRow> = sqlx::query_as(&format!(
            "SELECT e.id, e.sequence, e.aggregate_id, e.revision, e.kind, e.occurred_at \
             {SELLER_EVENT_FROM} \
             WHERE COALESCE(l.seller_pubky, o.seller_pubky, f.seller_pubky, d.seller_pubky) = $1 \
               AND e.sequence > $2 AND e.occurred_at >= $3 \
             ORDER BY e.sequence LIMIT $4"
        ))
        .bind(&seller)
        .bind(cursor)
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        for row in &rows {
            sqlx::query(
                "INSERT INTO webhook_deliveries \
                 (endpoint_id, event_id, key_id, signing_key, body, next_attempt_at, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $6) ON CONFLICT DO NOTHING",
            )
            .bind(endpoint_id)
            .bind(row.id)
            .bind(key_id)
            .bind(&signing_key)
            .bind(event_value(&seller, row))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        let next_cursor = if rows.len() as i64 == limit {
            rows.last().map_or(cursor, |row| row.sequence)
        } else {
            sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(sequence) FROM events")
                .fetch_one(&mut *tx)
                .await?
                .unwrap_or(cursor)
                .max(cursor)
        };
        sqlx::query(
            "UPDATE webhook_endpoints SET enqueue_sequence = $2, enqueue_checked_at = $3 \
             WHERE id = $1",
        )
        .bind(endpoint_id)
        .bind(next_cursor)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn deliver(delivery: &Delivery, now: DateTime<Utc>) -> Result<u16, ()> {
    let url = validate_webhook_url(&delivery.endpoint_url).map_err(|_| ())?;
    let host = match url.host().ok_or(())? {
        url::Host::Domain(host) => host.to_string(),
        url::Host::Ipv4(ip) => ip.to_string(),
        url::Host::Ipv6(ip) => ip.to_string(),
    };
    let addresses = tokio::net::lookup_host((host.as_str(), 443))
        .await
        .map_err(|_| ())?
        .collect::<Vec<_>>();
    let pinned = validate_resolved_addresses(&addresses).ok_or(())?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .resolve(&host, pinned)
        .build()
        .map_err(|_| ())?;
    let signed = signed_webhook_request(delivery, now).ok_or(())?;
    let mut response = client
        .post(url)
        .header("content-type", "application/json")
        .header(WEBHOOK_VERSION_HEADER, "1")
        .header(WEBHOOK_ID_HEADER, signed.event_id)
        .header(WEBHOOK_TIMESTAMP_HEADER, signed.timestamp)
        .header(WEBHOOK_KEY_ID_HEADER, signed.key_id)
        .header(WEBHOOK_SIGNATURE_HEADER, signed.signature)
        .body(signed.raw)
        .send()
        .await
        .map_err(|_| ())?;
    let status = response.status().as_u16();
    let mut received = 0usize;
    while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
        received = received.saturating_add(chunk.len());
        if received > WEBHOOK_BODY_LIMIT {
            return Err(());
        }
    }
    Ok(status)
}

fn signed_webhook_request(delivery: &Delivery, now: DateTime<Utc>) -> Option<SignedWebhookRequest> {
    let raw = serde_json::to_vec(&delivery.body).ok()?;
    let timestamp = now.timestamp().to_string();
    let signature = webhook_signature(&delivery.signing_key, &timestamp, delivery.event_id, &raw)?;
    Some(SignedWebhookRequest {
        raw,
        timestamp,
        event_id: delivery.event_id.to_string(),
        key_id: delivery.key_id.to_string(),
        signature: format!("v1={signature}"),
    })
}

fn webhook_signature(
    signing_key: &[u8],
    timestamp: &str,
    event_id: Uuid,
    raw: &[u8],
) -> Option<String> {
    let event_id = event_id.to_string();
    let signing_input = [
        b"v1.".as_slice(),
        timestamp.as_bytes(),
        b".",
        event_id.as_bytes(),
        b".",
        raw,
    ]
    .concat();
    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key).ok()?;
    mac.update(&signing_input);
    Some(hex::encode(mac.finalize().into_bytes()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedWebhook {
    pub event_id: Uuid,
    pub key_id: Uuid,
    pub payload_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookVerifyError {
    Malformed,
    Stale,
    WrongKey,
    InvalidSignature,
}

pub struct WebhookVerifyInput<'a> {
    pub secret: &'a [u8],
    pub expected_key_id: Uuid,
    pub event_id: &'a str,
    pub timestamp: &'a str,
    pub key_id: &'a str,
    pub signature: &'a str,
    pub raw: &'a [u8],
    pub receiver_now: DateTime<Utc>,
    pub max_skew_seconds: i64,
}

pub fn verify_webhook(
    input: WebhookVerifyInput<'_>,
) -> Result<VerifiedWebhook, WebhookVerifyError> {
    let event_id = Uuid::parse_str(input.event_id).map_err(|_| WebhookVerifyError::Malformed)?;
    let key_id = Uuid::parse_str(input.key_id).map_err(|_| WebhookVerifyError::Malformed)?;
    if key_id != input.expected_key_id {
        return Err(WebhookVerifyError::WrongKey);
    }
    let timestamp_seconds = input
        .timestamp
        .parse::<i64>()
        .map_err(|_| WebhookVerifyError::Malformed)?;
    if timestamp_seconds
        .checked_sub(input.receiver_now.timestamp())
        .is_none_or(|delta| delta.unsigned_abs() > input.max_skew_seconds.max(0) as u64)
    {
        return Err(WebhookVerifyError::Stale);
    }
    let supplied = input
        .signature
        .strip_prefix("v1=")
        .filter(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or(WebhookVerifyError::Malformed)?;
    let signing_key = Sha256::digest(input.secret);
    let expected = webhook_signature(&signing_key, input.timestamp, event_id, input.raw)
        .ok_or(WebhookVerifyError::Malformed)?;
    use subtle::ConstantTimeEq;
    if !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(WebhookVerifyError::InvalidSignature);
    }
    Ok(VerifiedWebhook {
        event_id,
        key_id,
        payload_sha256: hex::encode(Sha256::digest(input.raw)),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookReplayDecision {
    Apply,
    AlreadyApplied,
    QuarantineChangedPayload,
}

pub fn classify_webhook_replay(
    stored_payload_sha256: Option<&str>,
    received_payload_sha256: &str,
) -> WebhookReplayDecision {
    match stored_payload_sha256 {
        None => WebhookReplayDecision::Apply,
        Some(stored) if stored == received_payload_sha256 => WebhookReplayDecision::AlreadyApplied,
        Some(_) => WebhookReplayDecision::QuarantineChangedPayload,
    }
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.octets()[0] == 0
                || ip.octets() == [169, 254, 169, 254])
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(mapped));
            }
            let octets = ip.octets();
            let documentation = octets[0..4] == [0x20, 0x01, 0x0d, 0xb8];
            let six_to_four = octets[0..2] == [0x20, 0x02];
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || documentation
                || six_to_four)
        }
    }
}

fn validate_resolved_addresses(addresses: &[SocketAddr]) -> Option<SocketAddr> {
    if addresses.is_empty() || addresses.iter().any(|address| !public_ip(address.ip())) {
        return None;
    }
    Some(SocketAddr::new(addresses[0].ip(), 443))
}

pub fn spawn_webhook_worker(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(
            state.config.webhook_worker_interval_seconds,
        ));
        loop {
            interval.tick().await;
            if let Err(error) = run_webhook_pass(&state).await {
                tracing::error!(error = %error, "webhook worker pass failed");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_are_opaque_and_round_trip() {
        let cursor = encode_cursor(42);
        assert_ne!(cursor, "42");
        assert_eq!(decode_cursor(&cursor), Ok(42));
        assert!(decode_cursor("not-a-cursor").is_err());

        let page = encode_page_cursor(7, "listing:abc_item");
        assert_eq!(
            decode_page_cursor(&page),
            Ok((7, "listing:abc_item".to_string()))
        );
    }

    #[test]
    fn private_and_special_addresses_are_rejected() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "2001:db8::1",
            "2002:7f00:1::1",
        ] {
            assert!(!public_ip(address.parse().expect("test IP parses")));
        }
        assert!(public_ip("1.1.1.1".parse().expect("test IP parses")));
    }

    #[test]
    fn every_dns_answer_must_be_public_before_one_is_pinned() {
        let public = "1.1.1.1:443".parse().expect("public address");
        let rebinding = "[::ffff:127.0.0.1]:443"
            .parse()
            .expect("mapped loopback address");
        assert_eq!(
            validate_resolved_addresses(&[public]),
            Some(public),
            "a public-only resolution is pinned"
        );
        assert_eq!(
            validate_resolved_addresses(&[public, rebinding]),
            None,
            "a mixed DNS answer is rejected instead of selecting the public decoy"
        );
        assert_eq!(validate_resolved_addresses(&[]), None);
    }

    #[test]
    fn signature_framing_matches_the_cross_language_vector() {
        let key = (0_u8..32).collect::<Vec<_>>();
        let event_id =
            Uuid::parse_str("018f47d2-6a27-7c23-a49d-6b21bb770120").expect("test UUID parses");
        let signature = webhook_signature(&key, "1700000000", event_id, br#"{"ok":true}"#)
            .expect("valid HMAC key");
        assert_eq!(
            signature,
            "340177cd5af321108cc3b5322ae9abf0c24c093df2a60172c413a73ca9d14931"
        );
    }

    #[test]
    fn outbound_headers_and_raw_body_are_exact_and_stable() {
        let now: DateTime<Utc> = "2023-11-14T22:13:20Z".parse().expect("test time");
        let event_id = Uuid::parse_str("018f47d2-6a27-7c23-a49d-6b21bb770120").expect("event id");
        let key_id = Uuid::parse_str("018f47d2-6a27-7c23-a49d-6b21bb770121").expect("key id");
        let delivery = Delivery {
            endpoint_id: Uuid::nil(),
            event_id,
            endpoint_url: "https://example.com/hook".to_string(),
            key_id,
            signing_key: (0_u8..32).collect(),
            body: serde_json::from_str(r#"{"ok":true}"#).expect("body"),
            attempt_count: 0,
            created_at: now,
        };
        let signed = signed_webhook_request(&delivery, now).expect("signed request");
        assert_eq!(signed.raw, br#"{"ok":true}"#);
        assert_eq!(signed.timestamp, "1700000000");
        assert_eq!(signed.event_id, event_id.to_string());
        assert_eq!(signed.key_id, key_id.to_string());
        assert_eq!(
            signed.signature,
            "v1=340177cd5af321108cc3b5322ae9abf0c24c093df2a60172c413a73ca9d14931"
        );
        assert_eq!(WEBHOOK_VERSION_HEADER, "Pubky-Webhook-Version");
        assert_eq!(WEBHOOK_ID_HEADER, "Pubky-Webhook-Id");
        assert_eq!(WEBHOOK_TIMESTAMP_HEADER, "Pubky-Webhook-Timestamp");
        assert_eq!(WEBHOOK_KEY_ID_HEADER, "Pubky-Webhook-Key-Id");
        assert_eq!(WEBHOOK_SIGNATURE_HEADER, "Pubky-Webhook-Signature");
    }

    #[test]
    fn receiver_rejects_skew_and_classifies_replays_by_payload_hash() {
        let secret = [9_u8; 32];
        let key_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let now: DateTime<Utc> = "2026-09-20T08:00:00Z".parse().expect("test time");
        let timestamp = now.timestamp().to_string();
        let raw = br#"{"schema_version":1,"type":"listing.synced"}"#;
        let signing_key = Sha256::digest(secret);
        let signature = format!(
            "v1={}",
            webhook_signature(&signing_key, &timestamp, event_id, raw).expect("signature")
        );
        let event_id_text = event_id.to_string();
        let key_id_text = key_id.to_string();
        let verified = verify_webhook(WebhookVerifyInput {
            secret: &secret,
            expected_key_id: key_id,
            event_id: &event_id_text,
            timestamp: &timestamp,
            key_id: &key_id_text,
            signature: &signature,
            raw,
            receiver_now: now,
            max_skew_seconds: 300,
        })
        .expect("fresh signature verifies");
        assert_eq!(
            classify_webhook_replay(None, &verified.payload_sha256),
            WebhookReplayDecision::Apply
        );
        assert_eq!(
            classify_webhook_replay(Some(&verified.payload_sha256), &verified.payload_sha256),
            WebhookReplayDecision::AlreadyApplied
        );
        assert_eq!(
            classify_webhook_replay(Some(&"0".repeat(64)), &verified.payload_sha256),
            WebhookReplayDecision::QuarantineChangedPayload
        );
        assert_eq!(
            verify_webhook(WebhookVerifyInput {
                secret: &secret,
                expected_key_id: key_id,
                event_id: &event_id_text,
                timestamp: &timestamp,
                key_id: &key_id_text,
                signature: &signature,
                raw,
                receiver_now: now + chrono::Duration::seconds(301),
                max_skew_seconds: 300,
            }),
            Err(WebhookVerifyError::Stale)
        );
    }
}
