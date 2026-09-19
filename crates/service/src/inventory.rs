//! Phase 6 Wave 1 listing-total inventory authority.
//!
//! The service never claims per-variant availability. An optional variant is
//! only a seller-record lookup assertion, accepted when exactly one variant
//! is enabled. Stock mutation, event append, external-reference persistence,
//! and immutable idempotent result storage share one PostgreSQL transaction.

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::auth::AuthSession;
use crate::homeserver::HomeserverRawFetchOutcome;
use crate::AppState;

pub const MAX_INVENTORY_BODY_BYTES: usize = 4096;
pub const DEFAULT_AUTHENTICATED_RATE_PER_MINUTE: u32 = 120;
pub const DEFAULT_ANONYMOUS_RATE_PER_MINUTE: u32 = 20;
pub const DEFAULT_RATE_BURST_MULTIPLIER: u32 = 2;
/// At most this many distinct changed-body hashes are retained for one
/// successful seller/idempotency-key pair.
pub const MAX_CONFLICT_EVIDENCE_PER_IDEMPOTENCY_KEY: i64 = 16;

const MAX_RATE_PER_MINUTE: u32 = 10_000;
const MAX_BURST_MULTIPLIER: u32 = 10;
const MAX_LISTING_ID_BYTES: usize = 128;
const MAX_VARIANT_VALUE_BYTES: usize = 128;
const MAX_EXTERNAL_CHANNEL_BYTES: usize = 32;
const MAX_EXTERNAL_ID_BYTES: usize = 128;
const MAX_ABSOLUTE_DELTA: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryRateConfig {
    pub authenticated_per_minute: u32,
    pub anonymous_per_minute: u32,
    pub burst_multiplier: u32,
}

impl InventoryRateConfig {
    fn from_values(
        authenticated: Option<&str>,
        anonymous: Option<&str>,
        burst: Option<&str>,
    ) -> anyhow::Result<Self> {
        let authenticated_per_minute = parse_bounded_u32(
            "INVENTORY_AUTHENTICATED_RATE_LIMIT_PER_MINUTE",
            authenticated,
            DEFAULT_AUTHENTICATED_RATE_PER_MINUTE,
            1,
            MAX_RATE_PER_MINUTE,
        )?;
        let anonymous_per_minute = parse_bounded_u32(
            "INVENTORY_ANONYMOUS_RATE_LIMIT_PER_MINUTE",
            anonymous,
            DEFAULT_ANONYMOUS_RATE_PER_MINUTE,
            1,
            MAX_RATE_PER_MINUTE,
        )?;
        let burst_multiplier = parse_bounded_u32(
            "INVENTORY_RATE_LIMIT_BURST_MULTIPLIER",
            burst,
            DEFAULT_RATE_BURST_MULTIPLIER,
            1,
            MAX_BURST_MULTIPLIER,
        )?;
        let capacity = authenticated_per_minute
            .checked_mul(burst_multiplier)
            .ok_or_else(|| anyhow::anyhow!("inventory rate-limit capacity overflows"))?;
        if capacity > 20_000 {
            anyhow::bail!("inventory authenticated burst capacity must not exceed 20000");
        }
        Ok(Self {
            authenticated_per_minute,
            anonymous_per_minute,
            burst_multiplier,
        })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let authenticated = std::env::var("INVENTORY_AUTHENTICATED_RATE_LIMIT_PER_MINUTE").ok();
        let anonymous = std::env::var("INVENTORY_ANONYMOUS_RATE_LIMIT_PER_MINUTE").ok();
        let burst = std::env::var("INVENTORY_RATE_LIMIT_BURST_MULTIPLIER").ok();
        Self::from_values(
            authenticated.as_deref(),
            anonymous.as_deref(),
            burst.as_deref(),
        )
    }
}

/// Production startup hook: invalid inventory rate settings stop the service
/// before it listens. Handlers also parse the same settings so there is only
/// one parser and no hidden configuration path.
pub fn validate_rate_config_from_env() -> anyhow::Result<()> {
    InventoryRateConfig::from_env().map(|_| ())
}

fn parse_bounded_u32(
    name: &str,
    raw: Option<&str>,
    default: u32,
    minimum: u32,
    maximum: u32,
) -> anyhow::Result<u32> {
    let value = match raw {
        None => default,
        Some(raw) => raw
            .parse::<u32>()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?,
    };
    if !(minimum..=maximum).contains(&value) {
        anyhow::bail!("{name} must be between {minimum} and {maximum}");
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InventoryAdjustRequest {
    #[schemars(range(min = 1, max = 1))]
    pub schema_version: u8,
    #[schemars(regex(pattern = r"^inventory\.adjust$"))]
    pub kind: String,
    #[schemars(length(min = 1, max = 256))]
    pub aggregate_id: String,
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[A-Za-z0-9_.-]+$"))]
    pub listing_id: String,
    #[schemars(range(min = 1))]
    pub expected_revision: i64,
    #[schemars(range(min = -1000000, max = 1000000))]
    pub delta: i64,
    #[schemars(regex(
        pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    ))]
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<VariantAssertion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ref: Option<ExternalReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VariantAssertion {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[A-Za-z0-9_.-]+$"))]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128))]
    pub sku: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExternalReference {
    #[schemars(
        length(min = 1, max = 32),
        regex(pattern = r"^[a-z0-9][a-z0-9._-]{0,31}$")
    )]
    pub channel: String,
    #[schemars(length(min = 1, max = 128))]
    pub external_id: String,
}

#[derive(Debug)]
struct ValidatedAdjustRequest {
    wire: InventoryAdjustRequest,
    idempotency_key: Uuid,
    request_hash: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct StockView {
    #[schemars(regex(pattern = r"^listing_total$"))]
    authority: &'static str,
    #[schemars(range(min = 0))]
    total: i64,
    #[schemars(range(min = 0))]
    available: i64,
    #[schemars(range(min = 0))]
    reserved: i64,
    #[schemars(range(min = 0))]
    sold: i64,
}

#[derive(Debug, Serialize)]
struct AdjustmentResult {
    aggregate_id: String,
    listing_id: String,
    server_revision: i64,
    event_id: Uuid,
    stock: StockView,
}

#[derive(Debug, Serialize)]
struct SuccessEnvelope {
    schema_version: u8,
    ok: bool,
    result: AdjustmentResult,
}

#[derive(Debug, Serialize, JsonSchema)]
struct InventoryProjection {
    #[schemars(range(min = 1, max = 1))]
    schema_version: u8,
    #[schemars(regex(pattern = r"^inventory_projection$"))]
    kind: &'static str,
    #[schemars(length(min = 1, max = 256))]
    aggregate_id: String,
    #[schemars(length(min = 52, max = 52))]
    seller_pubky: String,
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[A-Za-z0-9_.-]+$"))]
    listing_id: String,
    #[schemars(range(min = 1))]
    server_revision: i64,
    stock: StockView,
}

#[derive(Debug, sqlx::FromRow)]
struct LockedListing {
    seller_pubky: String,
    listing_id: String,
    listing_revision: i64,
    server_revision: i64,
    total: i64,
    available: i64,
    reserved: i64,
    sold: i64,
}

#[derive(Debug)]
struct ValidatedVariantRecordBinding {
    seller_pubky: String,
    listing_id: String,
    listing_revision: i64,
    raw_sha256: [u8; 32],
}

#[derive(Debug)]
enum StoredAdjustment {
    Missing,
    Exact(String),
    Conflict,
}

impl StockView {
    fn new(total: i64, available: i64, reserved: i64, sold: i64) -> Self {
        Self {
            authority: "listing_total",
            total,
            available,
            reserved,
            sold,
        }
    }
}

fn inventory_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    current_revision: Option<i64>,
) -> Response {
    let mut error = serde_json::Map::new();
    error.insert("code".to_string(), json!(code));
    error.insert("message".to_string(), json!(message));
    if let Some(revision) = current_revision {
        error.insert("current_revision".to_string(), json!(revision));
    }
    (
        status,
        Json(json!({
            "schema_version": 1,
            "ok": false,
            "error": Value::Object(error),
        })),
    )
        .into_response()
}

fn invalid_request() -> Response {
    inventory_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_request",
        "The inventory request is invalid.",
        None,
    )
}

fn internal_error() -> Response {
    // Deliberately omit the database error: PostgreSQL diagnostics can echo
    // bound attacker-controlled values.
    tracing::error!("inventory database operation failed");
    inventory_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "The inventory request could not be processed.",
        None,
    )
}

fn capability_error() -> Response {
    inventory_error(
        StatusCode::FORBIDDEN,
        "capability_required",
        "The session grant does not authorize inventory access.",
        None,
    )
}

fn validate_request(bytes: &[u8]) -> Result<ValidatedAdjustRequest, ()> {
    if bytes.is_empty() || bytes.len() > MAX_INVENTORY_BODY_BYTES {
        return Err(());
    }
    let wire: InventoryAdjustRequest = serde_json::from_slice(bytes).map_err(|_| ())?;
    if wire.schema_version != 1
        || wire.kind != "inventory.adjust"
        || wire.aggregate_id.is_empty()
        || !valid_bounded_id(&wire.listing_id, MAX_LISTING_ID_BYTES)
        || wire.aggregate_id.len() > 256
        || wire.expected_revision < 1
        || wire.delta == 0
        || wire.delta.unsigned_abs() > MAX_ABSOLUTE_DELTA as u64
    {
        return Err(());
    }
    let idempotency_key = parse_strict_idempotency_uuid(&wire.idempotency_key)?;
    if let Some(variant) = &wire.variant {
        if variant.id.is_none() && variant.sku.is_none() {
            return Err(());
        }
        if variant
            .id
            .as_deref()
            .is_some_and(|value| !valid_bounded_id(value, MAX_VARIANT_VALUE_BYTES))
            || variant
                .sku
                .as_deref()
                .is_some_and(|value| !valid_printable(value, MAX_VARIANT_VALUE_BYTES))
        {
            return Err(());
        }
    }
    if let Some(reference) = &wire.external_ref {
        if !valid_channel(&reference.channel)
            || !valid_printable(&reference.external_id, MAX_EXTERNAL_ID_BYTES)
        {
            return Err(());
        }
    }
    let canonical = serde_json_canonicalizer::to_vec(&wire).map_err(|_| ())?;
    let request_hash = hex::encode(Sha256::digest(canonical));
    Ok(ValidatedAdjustRequest {
        wire,
        idempotency_key,
        request_hash,
    })
}

fn parse_strict_idempotency_uuid(value: &str) -> Result<Uuid, ()> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes.iter().enumerate().any(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte != b'-',
            _ => !byte.is_ascii_hexdigit(),
        })
        || !matches!(bytes[14], b'1'..=b'8')
        || !matches!(bytes[19], b'8' | b'9' | b'a' | b'A' | b'b' | b'B')
    {
        return Err(());
    }
    Uuid::parse_str(value).map_err(|_| ())
}

fn valid_bounded_id(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_printable(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn valid_channel(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_EXTERNAL_CHANNEL_BYTES
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

async fn consume_rate(
    tx: &mut Transaction<'_, Postgres>,
    session: &AuthSession,
    endpoint_class: &'static str,
    now: DateTime<Utc>,
    config: InventoryRateConfig,
) -> Result<bool, sqlx::Error> {
    let capacity = f64::from(config.authenticated_per_minute * config.burst_multiplier);
    let refill_per_second = f64::from(config.authenticated_per_minute) / 60.0;
    let remaining: Option<f64> = sqlx::query_scalar(
        "INSERT INTO inventory_rate_limits \
             (session_hash, endpoint_class, tokens, updated_at) \
         VALUES ($1, $2, $3 - 1, $4) \
         ON CONFLICT (session_hash, endpoint_class) DO UPDATE SET \
             tokens = LEAST($3, inventory_rate_limits.tokens + \
                 GREATEST(EXTRACT(EPOCH FROM ($4 - inventory_rate_limits.updated_at)), 0) * $5) - 1, \
             updated_at = $4 \
         WHERE LEAST($3, inventory_rate_limits.tokens + \
                 GREATEST(EXTRACT(EPOCH FROM ($4 - inventory_rate_limits.updated_at)), 0) * $5) >= 1 \
         RETURNING tokens",
    )
    .bind(&session.token_hash)
    .bind(endpoint_class)
    .bind(capacity)
    .bind(now)
    .bind(refill_per_second)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(remaining.is_some())
}

fn rate_limited(endpoint_class: &'static str) -> Response {
    let mut response = inventory_error(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "The inventory request rate limit was reached.",
        None,
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    let body = json!({
        "schema_version": 1,
        "ok": false,
        "error": {
            "code": "rate_limited",
            "message": "The inventory request rate limit was reached.",
            "limit_class": endpoint_class,
        }
    });
    *response.body_mut() = axum::body::Body::from(
        serde_json::to_vec(&body).expect("static rate-limit body serializes"),
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

async fn finish_without_mutation(tx: Transaction<'_, Postgres>, response: Response) -> Response {
    match tx.commit().await {
        Ok(()) => response,
        Err(_) => internal_error(),
    }
}

async fn classify_stored_adjustment(
    tx: &mut Transaction<'_, Postgres>,
    session: &AuthSession,
    request: &ValidatedAdjustRequest,
    observed_at: DateTime<Utc>,
) -> Result<StoredAdjustment, sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6341))")
        .bind(format!("{}:{}", session.actor.0, request.idempotency_key))
        .execute(&mut **tx)
        .await?;

    let stored: Option<(String, String, String)> = sqlx::query_as(
        "SELECT request_hash, result_json, aggregate_id \
         FROM inventory_adjustment_results \
         WHERE seller_pubky = $1 AND idempotency_key = $2",
    )
    .bind(&session.actor.0)
    .bind(request.idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((original_hash, result_json, original_aggregate)) = stored else {
        return Ok(StoredAdjustment::Missing);
    };
    if original_hash == request.request_hash {
        return Ok(StoredAdjustment::Exact(result_json));
    }

    // The unique index has seller/key as its leftmost prefix, so this bounded
    // count remains indexable. Migration 0034 independently enforces the same
    // cap for writers outside this application path.
    sqlx::query(
        "INSERT INTO inventory_adjustment_conflicts \
             (seller_pubky, idempotency_key, aggregate_id, original_request_hash, \
              conflicting_request_hash, observed_at) \
         SELECT $1, $2, $3, $4, $5, $6 \
         WHERE ( \
             SELECT COUNT(*) FROM inventory_adjustment_conflicts \
             WHERE seller_pubky = $1 AND idempotency_key = $2 \
         ) < $7 \
         ON CONFLICT (seller_pubky, idempotency_key, conflicting_request_hash) DO NOTHING",
    )
    .bind(&session.actor.0)
    .bind(request.idempotency_key)
    .bind(original_aggregate)
    .bind(original_hash)
    .bind(&request.request_hash)
    .bind(observed_at)
    .bind(MAX_CONFLICT_EVIDENCE_PER_IDEMPOTENCY_KEY)
    .execute(&mut **tx)
    .await?;
    Ok(StoredAdjustment::Conflict)
}

async fn continue_or_finish_stored<'a>(
    tx: Transaction<'a, Postgres>,
    stored: StoredAdjustment,
) -> Result<Transaction<'a, Postgres>, Response> {
    match stored {
        StoredAdjustment::Missing => Ok(tx),
        StoredAdjustment::Exact(result_json) => {
            let response: Value = match serde_json::from_str(&result_json) {
                Ok(response) => response,
                Err(_) => return Err(internal_error()),
            };
            Err(finish_without_mutation(tx, (StatusCode::OK, Json(response)).into_response()).await)
        }
        StoredAdjustment::Conflict => Err(finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "The idempotency key was already used with different input.",
                None,
            ),
        )
        .await),
    }
}

pub async fn adjust_inventory(
    State(state): State<AppState>,
    Extension(session): Extension<AuthSession>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    if !session.covers_inventory_service() {
        return capability_error();
    }
    let body = match body {
        Ok(body) => body,
        Err(_) => return invalid_request(),
    };
    let request = match validate_request(&body) {
        Ok(request) => request,
        Err(()) => return invalid_request(),
    };
    let config = match InventoryRateConfig::from_env() {
        Ok(config) => config,
        Err(_) => return internal_error(),
    };
    let preflight_now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error(),
    };

    let stored = match classify_stored_adjustment(&mut tx, &session, &request, preflight_now).await
    {
        Ok(stored) => stored,
        Err(_) => return internal_error(),
    };
    let mut tx = match continue_or_finish_stored(tx, stored).await {
        Ok(tx) => tx,
        Err(response) => return response,
    };

    let expected_aggregate =
        marketplace_domain::ids::listing_aggregate_id(&session.actor.0, &request.wire.listing_id);
    if request.wire.aggregate_id != expected_aggregate {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::FORBIDDEN,
                "seller_ownership_required",
                "Only the listing seller may adjust inventory.",
                None,
            ),
        )
        .await;
    }

    // The first short transaction preserves replay/conflict precedence under
    // the idempotency advisory lock. For a new variant-addressed request it
    // closes before remote I/O; after validation, the mutation transaction
    // re-acquires the same lock and reclassifies to close the concurrency gap.
    let (variant_binding, rate_consumed, mutation_now) =
        if let Some(assertion) = &request.wire.variant {
            let rate_allowed =
                match consume_rate(&mut tx, &session, "inventory.adjust", preflight_now, config)
                    .await
                {
                    Ok(allowed) => allowed,
                    Err(_) => return internal_error(),
                };
            if !rate_allowed {
                return finish_without_mutation(tx, rate_limited("inventory.adjust")).await;
            }
            if tx.commit().await.is_err() {
                return internal_error();
            }
            let binding = match validate_variant_assertion(
                &state,
                &session.actor.0,
                &request.wire.listing_id,
                assertion,
            )
            .await
            {
                Ok(binding) => binding,
                Err(response) => return response,
            };
            // Remote lookup latency must not stale mutation, event, result, or
            // external-reference timestamps.
            let mutation_now = state.clock.now();
            let mut mutation_tx = match state.pool.begin().await {
                Ok(tx) => tx,
                Err(_) => return internal_error(),
            };
            let stored = match classify_stored_adjustment(
                &mut mutation_tx,
                &session,
                &request,
                mutation_now,
            )
            .await
            {
                Ok(stored) => stored,
                Err(_) => return internal_error(),
            };
            tx = match continue_or_finish_stored(mutation_tx, stored).await {
                Ok(tx) => tx,
                Err(response) => return response,
            };
            (Some(binding), true, mutation_now)
        } else {
            (None, false, preflight_now)
        };

    if !rate_consumed {
        let rate_allowed =
            match consume_rate(&mut tx, &session, "inventory.adjust", mutation_now, config).await {
                Ok(allowed) => allowed,
                Err(_) => return internal_error(),
            };
        if !rate_allowed {
            return finish_without_mutation(tx, rate_limited("inventory.adjust")).await;
        }
    }

    let listing: Option<LockedListing> = match sqlx::query_as(
        "SELECT seller_pubky, listing_id, listing_revision, server_revision, \
                    total_quantity AS total, available_quantity AS available, \
                    reserved_quantity AS reserved, sold_quantity AS sold \
             FROM listings WHERE aggregate_id = $1 FOR UPDATE",
    )
    .bind(&request.wire.aggregate_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(listing) => listing,
        Err(_) => return internal_error(),
    };
    let Some(listing) = listing else {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::NOT_FOUND,
                "listing_not_found",
                "The listing was not found.",
                None,
            ),
        )
        .await;
    };
    if listing.seller_pubky != session.actor.0 || listing.listing_id != request.wire.listing_id {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::FORBIDDEN,
                "seller_ownership_required",
                "Only the listing seller may adjust inventory.",
                None,
            ),
        )
        .await;
    }
    if let Some(binding) = &variant_binding {
        // The digest binds the exact bytes validated before this transaction.
        // It is intentionally not compared with listings.content_hash, which
        // is a media hash rather than a listing-record digest. The locked
        // seller/id/revision tuple remains the service authority.
        let _validated_raw_sha256 = binding.raw_sha256;
        if binding.seller_pubky != listing.seller_pubky
            || binding.listing_id != listing.listing_id
            || binding.listing_revision != listing.listing_revision
        {
            return finish_without_mutation(
                tx,
                inventory_error(
                    StatusCode::CONFLICT,
                    "variant_record_conflict",
                    "The seller listing record does not match the registered listing.",
                    None,
                ),
            )
            .await;
        }
    }
    if listing.server_revision != request.wire.expected_revision {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "revision_conflict",
                "The listing server revision is stale.",
                Some(listing.server_revision),
            ),
        )
        .await;
    }

    if let Some(reference) = &request.wire.external_ref {
        if sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6342))")
            .bind(format!(
                "{}:{}:{}",
                session.actor.0, reference.channel, reference.external_id
            ))
            .execute(&mut *tx)
            .await
            .is_err()
        {
            return internal_error();
        }
        let exists: bool = match sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM inventory_external_refs \
             WHERE seller_pubky = $1 AND channel = $2 AND external_id = $3)",
        )
        .bind(&session.actor.0)
        .bind(&reference.channel)
        .bind(&reference.external_id)
        .fetch_one(&mut *tx)
        .await
        {
            Ok(exists) => exists,
            Err(_) => return internal_error(),
        };
        if exists {
            return finish_without_mutation(
                tx,
                inventory_error(
                    StatusCode::CONFLICT,
                    "external_reference_conflict",
                    "The external event reference was already used.",
                    None,
                ),
            )
            .await;
        }
    }

    let balanced = listing
        .reserved
        .checked_add(listing.sold)
        .and_then(|committed| listing.available.checked_add(committed))
        == Some(listing.total);
    if !balanced {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "inventory_invariant_violation",
                "The listing inventory state is inconsistent.",
                None,
            ),
        )
        .await;
    }
    let Some(new_total) = listing.total.checked_add(request.wire.delta) else {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "stock_overflow",
                "The inventory adjustment exceeds the supported stock range.",
                None,
            ),
        )
        .await;
    };
    let Some(new_available) = listing.available.checked_add(request.wire.delta) else {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "stock_overflow",
                "The inventory adjustment exceeds the supported stock range.",
                None,
            ),
        )
        .await;
    };
    if new_total < 0 || new_available < 0 {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::CONFLICT,
                "negative_stock",
                "The adjustment would make available inventory negative.",
                None,
            ),
        )
        .await;
    }
    let new_revision = match listing.server_revision.checked_add(1) {
        Some(revision) => revision,
        None => {
            return finish_without_mutation(
                tx,
                inventory_error(
                    StatusCode::CONFLICT,
                    "stock_overflow",
                    "The inventory adjustment exceeds the supported stock range.",
                    None,
                ),
            )
            .await
        }
    };
    let state_name = if new_available > 0 {
        "available"
    } else if listing.reserved > 0 {
        "reserved"
    } else {
        "sold"
    };
    let updated: Option<(i64, i64, i64, i64, i64)> = match sqlx::query_as(
        "UPDATE listings SET total_quantity = $2, available_quantity = $3, \
             server_revision = $4, state = $5, updated_at = $6 \
         WHERE aggregate_id = $1 AND server_revision = $7 \
         RETURNING server_revision, total_quantity, available_quantity, \
                   reserved_quantity, sold_quantity",
    )
    .bind(&request.wire.aggregate_id)
    .bind(new_total)
    .bind(new_available)
    .bind(new_revision)
    .bind(state_name)
    .bind(mutation_now)
    .bind(listing.server_revision)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(updated) => updated,
        Err(_) => return internal_error(),
    };
    let Some((revision, total, available, reserved, sold)) = updated else {
        return internal_error();
    };

    let event_id = Uuid::new_v4();
    if sqlx::query(
        "INSERT INTO events \
             (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, 'inventory.adjusted', $6)",
    )
    .bind(event_id)
    .bind(request.idempotency_key)
    .bind(&request.wire.aggregate_id)
    .bind(revision)
    .bind(&session.actor.0)
    .bind(mutation_now)
    .execute(&mut *tx)
    .await
    .is_err()
    {
        return internal_error();
    }

    if let Some(reference) = &request.wire.external_ref {
        if sqlx::query(
            "INSERT INTO inventory_external_refs \
                 (seller_pubky, channel, external_id, aggregate_id, event_id, request_hash, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&session.actor.0)
        .bind(&reference.channel)
        .bind(&reference.external_id)
        .bind(&request.wire.aggregate_id)
        .bind(event_id)
        .bind(&request.request_hash)
        .bind(mutation_now)
        .execute(&mut *tx)
        .await
        .is_err()
        {
            return internal_error();
        }
    }

    let response = SuccessEnvelope {
        schema_version: 1,
        ok: true,
        result: AdjustmentResult {
            aggregate_id: request.wire.aggregate_id.clone(),
            listing_id: request.wire.listing_id.clone(),
            server_revision: revision,
            event_id,
            stock: StockView::new(total, available, reserved, sold),
        },
    };
    let result_json = match serde_json::to_string(&response) {
        Ok(result) => result,
        Err(_) => return internal_error(),
    };
    if sqlx::query(
        "INSERT INTO inventory_adjustment_results \
             (seller_pubky, idempotency_key, request_hash, aggregate_id, event_id, result_json, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&session.actor.0)
    .bind(request.idempotency_key)
    .bind(&request.request_hash)
    .bind(&request.wire.aggregate_id)
    .bind(event_id)
    .bind(&result_json)
    .bind(mutation_now)
    .execute(&mut *tx)
    .await
    .is_err()
    {
        return internal_error();
    }
    let response_value = match serde_json::from_str::<Value>(&result_json) {
        Ok(value) => value,
        Err(_) => return internal_error(),
    };
    match tx.commit().await {
        Ok(()) => (StatusCode::OK, Json(response_value)).into_response(),
        Err(_) => internal_error(),
    }
}

async fn validate_variant_assertion(
    state: &AppState,
    expected_seller_pubky: &str,
    expected_listing_id: &str,
    assertion: &VariantAssertion,
) -> Result<ValidatedVariantRecordBinding, Response> {
    let Some(homeserver) = state.homeserver.as_deref() else {
        return Err(inventory_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "variant_lookup_unavailable",
            "The seller listing record is unavailable for variant validation.",
            None,
        ));
    };
    let raw = match homeserver
        .fetch_listing_raw(expected_seller_pubky, expected_listing_id)
        .await
    {
        HomeserverRawFetchOutcome::Found(raw) => raw,
        HomeserverRawFetchOutcome::NotFound
        | HomeserverRawFetchOutcome::TooLarge
        | HomeserverRawFetchOutcome::Unavailable => {
            return Err(inventory_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "variant_lookup_unavailable",
                "The seller listing record is unavailable for variant validation.",
                None,
            ))
        }
    };
    let record: Value = serde_json::from_slice(&raw).map_err(|_| {
        inventory_error(
            StatusCode::CONFLICT,
            "variant_authority_unsupported",
            "The listing does not have exactly one enabled variant.",
            None,
        )
    })?;
    let record_seller = record.get("ownerPubky").and_then(Value::as_str);
    let record_listing_id = record.get("listingId").and_then(Value::as_str);
    let record_revision = record.get("revision").and_then(Value::as_i64);
    if record_seller != Some(expected_seller_pubky)
        || record_listing_id != Some(expected_listing_id)
        || record_revision.is_none()
    {
        return Err(inventory_error(
            StatusCode::CONFLICT,
            "variant_record_conflict",
            "The seller listing record does not match the registered listing.",
            None,
        ));
    }
    let variants = record
        .get("variants")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            inventory_error(
                StatusCode::CONFLICT,
                "variant_authority_unsupported",
                "The listing does not have exactly one enabled variant.",
                None,
            )
        })?;
    let enabled = variants
        .iter()
        .filter(|variant| variant.get("enabled").and_then(Value::as_bool) == Some(true))
        .collect::<Vec<_>>();
    if enabled.len() != 1 {
        return Err(inventory_error(
            StatusCode::CONFLICT,
            "variant_authority_unsupported",
            "The listing does not have exactly one enabled variant.",
            None,
        ));
    }
    let actual_id = enabled[0].get("id").and_then(Value::as_str);
    let actual_sku = enabled[0].get("sku").and_then(Value::as_str);
    if actual_id.is_none_or(|value| !valid_bounded_id(value, MAX_VARIANT_VALUE_BYTES))
        || actual_sku.is_some_and(|value| !valid_printable(value, MAX_VARIANT_VALUE_BYTES))
    {
        return Err(inventory_error(
            StatusCode::CONFLICT,
            "variant_authority_unsupported",
            "The listing does not have exactly one enabled variant.",
            None,
        ));
    }
    if assertion
        .id
        .as_deref()
        .is_some_and(|expected| Some(expected) != actual_id)
        || assertion
            .sku
            .as_deref()
            .is_some_and(|expected| Some(expected) != actual_sku)
    {
        return Err(inventory_error(
            StatusCode::CONFLICT,
            "variant_assertion_mismatch",
            "The variant assertion does not match the sole enabled variant.",
            None,
        ));
    }
    Ok(ValidatedVariantRecordBinding {
        seller_pubky: record_seller.expect("checked above").to_string(),
        listing_id: record_listing_id.expect("checked above").to_string(),
        listing_revision: record_revision.expect("checked above"),
        raw_sha256: Sha256::digest(&raw).into(),
    })
}

pub async fn get_inventory_projection(
    State(state): State<AppState>,
    Extension(session): Extension<AuthSession>,
    Path(aggregate_id): Path<String>,
) -> Response {
    if !session.covers_inventory_service() {
        return capability_error();
    }
    if aggregate_id.is_empty() || aggregate_id.len() > 256 {
        return invalid_request();
    }
    let config = match InventoryRateConfig::from_env() {
        Ok(config) => config,
        Err(_) => return internal_error(),
    };
    let now = state.clock.now();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return internal_error(),
    };
    match consume_rate(&mut tx, &session, "inventory.read", now, config).await {
        Ok(true) => {}
        Ok(false) => return finish_without_mutation(tx, rate_limited("inventory.read")).await,
        Err(_) => return internal_error(),
    }
    let row: Option<(String, String, i64, i64, i64, i64, i64)> = match sqlx::query_as(
        "SELECT seller_pubky, listing_id, server_revision, total_quantity, \
                available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(_) => return internal_error(),
    };
    let Some((seller_pubky, listing_id, revision, total, available, reserved, sold)) = row else {
        return finish_without_mutation(
            tx,
            inventory_error(
                StatusCode::NOT_FOUND,
                "listing_not_found",
                "The listing was not found.",
                None,
            ),
        )
        .await;
    };
    let projection = InventoryProjection {
        schema_version: 1,
        kind: "inventory_projection",
        aggregate_id,
        seller_pubky,
        listing_id,
        server_revision: revision,
        stock: StockView::new(total, available, reserved, sold),
    };
    match tx.commit().await {
        Ok(()) => (StatusCode::OK, Json(projection)).into_response(),
        Err(_) => internal_error(),
    }
}

/// Deterministic schema source consumed by the executable contract test.
pub fn inventory_adjust_schema() -> Value {
    let mut schema = serde_json::to_value(schema_for!(InventoryAdjustRequest))
        .expect("inventory request schema serializes");
    schema["properties"]["delta"]["not"] = json!({"const": 0});
    schema
}

pub fn inventory_projection_schema() -> Value {
    serde_json::to_value(schema_for!(InventoryProjection))
        .expect("inventory projection schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_rate_defaults_and_bounds_are_single_source() {
        assert_eq!(
            InventoryRateConfig::from_values(None, None, None).unwrap(),
            InventoryRateConfig {
                authenticated_per_minute: 120,
                anonymous_per_minute: 20,
                burst_multiplier: 2,
            }
        );
        for (authenticated, anonymous, burst) in [
            (Some("0"), None, None),
            (Some("10001"), None, None),
            (Some("abc"), None, None),
            (None, Some("0"), None),
            (None, None, Some("0")),
            (Some("3000"), None, Some("10")),
        ] {
            assert!(
                InventoryRateConfig::from_values(authenticated, anonymous, burst).is_err(),
                "invalid settings must fail: {authenticated:?} {anonymous:?} {burst:?}"
            );
        }
    }

    #[test]
    fn request_hash_is_rfc8785_deterministic_and_changed_body_differs() {
        let first = br#"{
            "kind":"inventory.adjust",
            "schema_version":1,
            "listing_id":"boots_01",
            "aggregate_id":"listing:yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy_boots_01",
            "delta":1,
            "expected_revision":1,
            "idempotency_key":"00000000-0000-4000-8000-000000000001"
        }"#;
        let reordered = br#"{
            "idempotency_key":"00000000-0000-4000-8000-000000000001",
            "expected_revision":1,
            "delta":1,
            "aggregate_id":"listing:yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy_boots_01",
            "listing_id":"boots_01",
            "schema_version":1,
            "kind":"inventory.adjust"
        }"#;
        let a = validate_request(first).expect("valid");
        let b = validate_request(reordered).expect("valid");
        assert_eq!(a.request_hash, b.request_hash);
        let mut changed: InventoryAdjustRequest = serde_json::from_slice(first).expect("valid");
        changed.delta = 2;
        let changed =
            validate_request(&serde_json::to_vec(&changed).expect("changed request serializes"))
                .expect("changed request validates");
        assert_ne!(a.request_hash, changed.request_hash);
    }

    fn request_bytes(idempotency_key: &str, delta: i64) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "kind": "inventory.adjust",
            "aggregate_id": format!("listing:{}_boots_01", "y".repeat(52)),
            "listing_id": "boots_01",
            "expected_revision": 1,
            "delta": delta,
            "idempotency_key": idempotency_key,
        }))
        .expect("request serializes")
    }

    #[test]
    fn runtime_rejects_nil_unsupported_version_variant_and_zero_delta() {
        for (idempotency_key, delta) in [
            ("00000000-0000-0000-8000-000000000001", 1),
            ("00000000-0000-9000-8000-000000000001", 1),
            ("00000000-0000-4000-7000-000000000001", 1),
            ("00000000-0000-4000-8000-000000000001", 0),
        ] {
            assert!(
                validate_request(&request_bytes(idempotency_key, delta)).is_err(),
                "runtime accepted idempotency_key={idempotency_key}, delta={delta}"
            );
        }
    }

    #[test]
    fn request_schema_requires_rfc_variant_versions_one_through_eight_and_nonzero_delta() {
        let schema = inventory_adjust_schema();
        assert_eq!(
            schema.pointer("/properties/idempotency_key/pattern"),
            Some(&json!(
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
            ))
        );
        assert_eq!(
            schema.pointer("/properties/delta/not/const"),
            Some(&json!(0)),
            "published schema must reject the zero delta that runtime rejects"
        );
    }
}
