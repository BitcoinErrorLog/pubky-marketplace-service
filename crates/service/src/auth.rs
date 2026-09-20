//! Pubky AuthToken authentication.
//!
//! Flow:
//! 1. The client obtains an `AuthToken` through the Pubky auth flow: the
//!    user approves on their signer device (e.g. Pubky Ring), which signs a
//!    time-bound proof of key ownership. The app never holds the secret key.
//! 2. `POST /v1/auth/sessions` receives the postcard-serialized token bytes
//!    as the raw request body and verifies them with `pubky-common` — the
//!    same crate the Pubky homeserver and the `@synonymdev/pubky` SDK are
//!    built on. The token's public key becomes the authenticated actor and
//!    its capabilities the granted scope.
//! 3. Replay protection is enforced by this service, not assumed from the
//!    token: each token is single-use (its `(public key, timestamp)` identity
//!    is recorded in Postgres) and must fall within a bounded acceptance
//!    window around the authoritative server clock.
//! 4. On success the service issues an opaque 32-byte session token, stored
//!    hashed (SHA-256), presented as `Authorization: Bearer <token>`;
//!    middleware resolves the actor pubky from the stored hash. No trust-me
//!    headers.

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::{Action, Capabilities};
use pubky_common::StoragePath;
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::logging;
use crate::AppState;

/// Minimum length of a serialized v0 AuthToken: 64-byte signature, 10-byte
/// namespace, and 1 version byte precede the variable-length remainder.
/// `AuthToken::verify` indexes the version byte directly, so the length is
/// guarded before delegating to it.
const MIN_TOKEN_LENGTH: usize = 75;

/// `pubky-common` itself rejects tokens more than 3 minutes from system
/// time. Used only to size the retention of single-use records: a token
/// older than both windows can never be accepted again, so its record is
/// prunable.
const LIBRARY_TIMESTAMP_WINDOW_SECONDS: i64 = 180;

/// Canonical AuthToken directory grant required by every Phase 6 inventory
/// command and projection. Both read and write actions are required by D6.6.
pub const INVENTORY_SERVICE_SCOPE: &str = "/pub/pubky.app/marketplace-service/v1/";

/// The authenticated actor, resolved from a session token by middleware.
#[derive(Debug, Clone)]
pub struct Actor(pub String);

/// The verified persisted session facts available to route middleware and
/// command/query boundaries. The token hash is opaque and must never be
/// logged or serialized.
#[derive(Debug, Clone)]
pub struct AuthSession {
    pub actor: Actor,
    pub capabilities: String,
    pub token_hash: Vec<u8>,
}

#[derive(sqlx::FromRow)]
struct SessionAdminRow {
    session_id: Uuid,
    label: Option<String>,
    client_metadata: Value,
    capabilities: String,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
}

impl AuthSession {
    pub fn covers_inventory_service(&self) -> bool {
        capability_covers_inventory_service(&self.capabilities)
    }
}

/// Semantic capability coverage: a directory grant strictly broader than
/// the service scope (notably `/:rw`) covers it; exact string equality would
/// incorrectly reject that grant. Read-only, write-only, malformed, empty,
/// and unrelated grants fail closed.
pub fn capability_covers_inventory_service(raw: &str) -> bool {
    let Ok(capabilities) = raw.parse::<Capabilities>() else {
        return false;
    };
    let required_path =
        StoragePath::new(INVENTORY_SERVICE_SCOPE).expect("inventory service scope is canonical");
    capabilities.iter().any(|capability| {
        capability.scope_covers_path(&required_path)
            && capability.actions().contains(&Action::Read)
            && capability.actions().contains(&Action::Write)
    })
}

/// The claims extracted from a cryptographically verified AuthToken.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedAuthToken {
    /// The signer's public key as a 52-character z-base-32 pubky.
    pub pubky: String,
    /// The granted capabilities in their canonical string form.
    pub capabilities: String,
    /// The token's signing timestamp in microseconds since the Unix epoch;
    /// together with the pubky it is the token's unique identity.
    pub timestamp_micros: i64,
}

/// Why an AuthToken was not accepted. All variants map to HTTP 401.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthTokenRejection {
    /// Too short to contain the fixed-length signature/namespace/version
    /// header.
    Malformed,
    /// Rejected by `pubky-common`: parse failure, invalid signature, unknown
    /// version, or outside the library's own window against system time.
    Invalid,
    /// Outside this service's acceptance window relative to the
    /// authoritative server clock.
    OutsideAcceptanceWindow,
}

/// Verifies postcard-serialized AuthToken bytes and enforces the service's
/// acceptance window around `now`. Signature and structure verification is
/// delegated entirely to `pubky-common`; nothing about the wire format is
/// reimplemented here.
pub fn verify_auth_token(
    bytes: &[u8],
    now: DateTime<Utc>,
    window_seconds: i64,
) -> Result<VerifiedAuthToken, AuthTokenRejection> {
    if bytes.len() < MIN_TOKEN_LENGTH {
        return Err(AuthTokenRejection::Malformed);
    }
    let token = AuthToken::verify(bytes).map_err(|_| AuthTokenRejection::Invalid)?;
    let timestamp_micros =
        i64::try_from(token.timestamp().as_u64()).map_err(|_| AuthTokenRejection::Invalid)?;
    let drift_micros = timestamp_micros - now.timestamp_micros();
    if drift_micros.abs() > window_seconds.saturating_mul(1_000_000) {
        return Err(AuthTokenRejection::OutsideAcceptanceWindow);
    }
    Ok(VerifiedAuthToken {
        pubky: token.public_key().z32(),
        capabilities: token.capabilities().clone().normalize().to_string(),
        timestamp_micros,
    })
}

pub fn hash_token(token: &[u8]) -> Vec<u8> {
    Sha256::digest(token).to_vec()
}

fn auth_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": { "message": message } }))).into_response()
}

pub async fn create_session(State(state): State<AppState>, body: Bytes) -> Response {
    let now = state.clock.now();
    let verified = match verify_auth_token(&body, now, state.config.auth_token_window_seconds) {
        Ok(verified) => verified,
        Err(rejection) => {
            tracing::info!(rejection = ?rejection, "rejected auth token");
            return auth_error(StatusCode::UNAUTHORIZED, "The auth token is invalid.");
        }
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            tracing::error!(error = %error, "failed to open auth transaction");
            return auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Session could not be created.",
            );
        }
    };

    // Prune single-use records that can never match an acceptable token
    // again (older than both the service and library windows, doubled for
    // margin).
    let retention_seconds = 2 * state
        .config
        .auth_token_window_seconds
        .max(LIBRARY_TIMESTAMP_WINDOW_SECONDS);
    let prune = sqlx::query("DELETE FROM auth_token_uses WHERE used_at < $1")
        .bind(now - chrono::Duration::seconds(retention_seconds))
        .execute(&mut *tx)
        .await;
    if let Err(error) = prune {
        tracing::error!(error = %error, "failed to prune auth token uses");
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Session could not be created.",
        );
    }

    // Single use: the (pubky, timestamp) pair is the token's identity. A
    // conflict means this exact token was already accepted once.
    let recorded = match sqlx::query(
        "INSERT INTO auth_token_uses (pubky, token_timestamp_micros, used_at) \
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
    )
    .bind(&verified.pubky)
    .bind(verified.timestamp_micros)
    .bind(now)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::error!(error = %error, "failed to record auth token use");
            return auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Session could not be created.",
            );
        }
    };
    if recorded.rows_affected() == 0 {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The auth token has already been used.",
        );
    }

    let mut token = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token);
    let expires_at = now + chrono::Duration::seconds(state.config.session_ttl_seconds);
    let session_id = Uuid::new_v4();
    let stored = sqlx::query(
        "INSERT INTO auth_sessions \
         (token_hash, session_id, pubky, capabilities, created_at, expires_at, last_used_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $5)",
    )
    .bind(hash_token(&token))
    .bind(session_id)
    .bind(&verified.pubky)
    // Persist only the normalized verified grant, never the credential bytes.
    // Inventory route middleware and the handler boundary both enforce it.
    .bind(&verified.capabilities)
    .bind(now)
    .bind(expires_at)
    .execute(&mut *tx)
    .await;
    if let Err(error) = stored {
        tracing::error!(error = %error, "failed to store auth session");
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Session could not be created.",
        );
    }
    if let Err(error) = tx.commit().await {
        tracing::error!(error = %error, "failed to commit auth session");
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Session could not be created.",
        );
    }

    tracing::info!(
        actor_prefix = logging::actor_prefix(&verified.pubky),
        "issued auth session"
    );
    (
        StatusCode::CREATED,
        Json(json!({
            "token": URL_SAFE_NO_PAD.encode(token),
            "session_id": session_id,
            "pubky": verified.pubky,
            "capabilities": verified.capabilities,
            "expires_at": format_timestamp(expires_at),
        })),
    )
        .into_response()
}

/// Resolves the actor pubky from the Bearer token and injects [`Actor`].
pub async fn require_session(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| URL_SAFE_NO_PAD.decode(token).ok());
    let Some(token) = token else {
        tracing::warn!(
            route = logging::route_template(&request),
            status = StatusCode::UNAUTHORIZED.as_u16(),
            reason = "MISSING_OR_MALFORMED_BEARER",
            "auth.rejected"
        );
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "A session bearer token is required.",
        );
    };

    let now = state.clock.now();
    let token_hash = hash_token(&token);
    let session: Option<(String, String, DateTime<Utc>)> = match sqlx::query_as(
        "SELECT pubky, capabilities, expires_at FROM auth_sessions \
         WHERE token_hash = $1 AND expires_at > $2 AND revoked_at IS NULL",
    )
    .bind(&token_hash)
    .bind(now)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(error) => {
            tracing::error!(error = %error, "failed to resolve session");
            return auth_error(StatusCode::INTERNAL_SERVER_ERROR, "Session lookup failed.");
        }
    };
    let Some((pubky, capabilities, _)) = session else {
        tracing::warn!(
            route = logging::route_template(&request),
            status = StatusCode::UNAUTHORIZED.as_u16(),
            reason = "INVALID_OR_EXPIRED_SESSION",
            "auth.rejected"
        );
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The session is invalid or expired.",
        );
    };
    if let Err(error) =
        sqlx::query("UPDATE auth_sessions SET last_used_at = $2 WHERE token_hash = $1")
            .bind(&token_hash)
            .bind(now)
            .execute(&state.pool)
            .await
    {
        tracing::error!(error = %error, "failed to update session last use");
        return auth_error(StatusCode::INTERNAL_SERVER_ERROR, "Session lookup failed.");
    }

    let actor = Actor(pubky);
    request.extensions_mut().insert(actor.clone());
    request.extensions_mut().insert(AuthSession {
        actor,
        capabilities,
        token_hash,
    });
    let Some(actor) = request.extensions().get::<Actor>().cloned() else {
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Session authentication failed.",
        );
    };
    let mut response = next.run(request).await;
    response.extensions_mut().insert(actor);
    response
}

async fn resolve_header_session(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthSession, Response> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| URL_SAFE_NO_PAD.decode(token).ok())
        .ok_or_else(|| {
            auth_error(
                StatusCode::UNAUTHORIZED,
                "A session bearer token is required.",
            )
        })?;
    let token_hash = hash_token(&token);
    let now = state.clock.now();
    let row: Option<(String, String)> = sqlx::query_as(
        "UPDATE auth_sessions SET last_used_at = $2 \
         WHERE token_hash = $1 AND expires_at > $2 AND revoked_at IS NULL \
         RETURNING pubky, capabilities",
    )
    .bind(&token_hash)
    .bind(now)
    .fetch_optional(&state.pool)
    .await
    .map_err(|error| {
        tracing::error!(error = %error, "failed to resolve session");
        auth_error(StatusCode::INTERNAL_SERVER_ERROR, "Session lookup failed.")
    })?;
    let (pubky, capabilities) = row.ok_or_else(|| {
        auth_error(
            StatusCode::UNAUTHORIZED,
            "The session is invalid or expired.",
        )
    })?;
    Ok(AuthSession {
        actor: Actor(pubky),
        capabilities,
        token_hash,
    })
}

pub async fn list_sessions(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let session = match resolve_header_session(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if !session.covers_inventory_service() {
        return capability_error();
    }
    match crate::automation::consume_automation_rate(&state, &session.actor.0, "session.admin")
        .await
    {
        Ok(Some(retry_after)) => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "ok": false,
                    "error": {
                        "code": "rate_limited",
                        "message": "The request rate limit was exceeded."
                    }
                })),
            )
                .into_response();
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_str(&retry_after.to_string())
                    .expect("retry-after is a safe integer"),
            );
            return response;
        }
        Ok(None) => {}
        Err(error) => {
            tracing::error!(error = %error, "session administration rate limit failed");
            return auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Sessions could not be read.",
            );
        }
    }
    let rows: Result<Vec<SessionAdminRow>, sqlx::Error> = sqlx::query_as(
        "SELECT session_id, label, client_metadata, capabilities, created_at, expires_at, \
                    last_used_at, revoked_at \
             FROM auth_sessions WHERE pubky = $1 \
             ORDER BY created_at DESC, session_id DESC LIMIT 200",
    )
    .bind(&session.actor.0)
    .fetch_all(&state.pool)
    .await;
    match rows {
        Ok(rows) => {
            let sessions = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "id": row.session_id,
                        "label": row.label,
                        "metadata": row.client_metadata,
                        "capabilities": row.capabilities,
                        "created_at": format_timestamp(row.created_at),
                        "expires_at": format_timestamp(row.expires_at),
                        "last_used_at": row.last_used_at.map(format_timestamp),
                        "revoked_at": row.revoked_at.map(format_timestamp),
                    })
                })
                .collect::<Vec<_>>();
            (
                StatusCode::OK,
                Json(json!({"schema_version": 1, "sessions": sessions})),
            )
                .into_response()
        }
        Err(error) => {
            tracing::error!(error = %error, "failed to list sessions");
            auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Sessions could not be read.",
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionMetadataUpdate {
    label: Option<String>,
    #[serde(default)]
    metadata: Value,
}

pub async fn update_session_metadata(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
    Json(update): Json<SessionMetadataUpdate>,
) -> Response {
    let label_valid = update.label.as_ref().is_none_or(|label| {
        !label.is_empty() && label.len() <= 80 && !label.chars().any(char::is_control)
    });
    let metadata_valid = update.metadata.is_object()
        && serde_json::to_vec(&update.metadata).is_ok_and(|bytes| bytes.len() <= 2048);
    if !label_valid || !metadata_valid {
        return auth_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Session metadata is invalid.",
        );
    }
    match sqlx::query(
        "UPDATE auth_sessions SET label = $3, client_metadata = $4 \
         WHERE session_id = $1 AND pubky = $2 AND revoked_at IS NULL",
    )
    .bind(id)
    .bind(&actor.0)
    .bind(&update.label)
    .bind(&update.metadata)
    .execute(&state.pool)
    .await
    {
        Ok(result) if result.rows_affected() == 1 => (
            StatusCode::OK,
            Json(json!({"schema_version": 1, "id": id, "label": update.label, "metadata": update.metadata})),
        )
            .into_response(),
        Ok(_) => auth_error(StatusCode::NOT_FOUND, "The session was not found."),
        Err(error) => {
            tracing::error!(error = %error, "failed to update session metadata");
            auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Session metadata could not be updated.",
            )
        }
    }
}

pub async fn revoke_session(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let now = state.clock.now();
    match sqlx::query(
        "UPDATE auth_sessions SET revoked_at = $3 \
         WHERE session_id = $1 AND pubky = $2 AND revoked_at IS NULL",
    )
    .bind(id)
    .bind(&actor.0)
    .bind(now)
    .execute(&state.pool)
    .await
    {
        Ok(result) if result.rows_affected() == 1 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => auth_error(StatusCode::NOT_FOUND, "The session was not found."),
        Err(error) => {
            tracing::error!(error = %error, "failed to revoke session");
            auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "The session could not be revoked.",
            )
        }
    }
}

fn capability_error() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "schema_version": 1,
            "ok": false,
            "error": {
                "code": "capability_required",
                "message": "The session grant does not authorize inventory access."
            }
        })),
    )
        .into_response()
}

/// Inventory-only middleware. Generic legacy routes continue to receive the
/// authenticated actor, while every new Phase 6 route must also pass this
/// semantic capability gate.
pub async fn require_inventory_capability(request: Request, next: Next) -> Response {
    let Some(session) = request.extensions().get::<AuthSession>() else {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "A session bearer token is required.",
        );
    };
    if !session.covers_inventory_service() {
        return capability_error();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky_common::capabilities::Capability;
    use pubky_common::crypto::Keypair;

    const WINDOW_SECONDS: i64 = 120;

    fn genuine_token_bytes(keypair: &Keypair) -> Vec<u8> {
        AuthToken::sign(keypair, vec![Capability::root()]).serialize()
    }

    #[test]
    fn accepts_a_genuine_token_and_extracts_the_signer() {
        let keypair = Keypair::random();
        let bytes = genuine_token_bytes(&keypair);

        let verified = verify_auth_token(&bytes, Utc::now(), WINDOW_SECONDS)
            .expect("freshly signed token verifies");

        assert_eq!(verified.pubky, keypair.public_key().z32());
        assert_eq!(verified.capabilities, "/:rw");
        assert!(marketplace_domain::pubky::is_valid_pubky(&verified.pubky));
    }

    #[test]
    fn a_token_identifies_its_signer_and_no_one_else() {
        let alice = Keypair::random();
        let bob = Keypair::random();
        let bytes = genuine_token_bytes(&alice);

        let verified = verify_auth_token(&bytes, Utc::now(), WINDOW_SECONDS)
            .expect("freshly signed token verifies");

        assert_eq!(verified.pubky, alice.public_key().z32());
        assert_ne!(verified.pubky, bob.public_key().z32());
    }

    #[test]
    fn rejects_a_tampered_signature() {
        let keypair = Keypair::random();
        let mut bytes = genuine_token_bytes(&keypair);
        bytes[0] ^= 0x01;

        assert_eq!(
            verify_auth_token(&bytes, Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Invalid)
        );
    }

    #[test]
    fn rejects_a_tampered_payload() {
        let keypair = Keypair::random();
        let mut bytes = genuine_token_bytes(&keypair);
        // The capabilities live at the tail; changing them breaks the
        // signature over the signable region.
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        assert_eq!(
            verify_auth_token(&bytes, Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Invalid)
        );
    }

    #[test]
    fn rejects_truncated_and_garbage_bytes() {
        let keypair = Keypair::random();
        let bytes = genuine_token_bytes(&keypair);

        assert_eq!(
            verify_auth_token(&bytes[..MIN_TOKEN_LENGTH - 1], Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Malformed)
        );
        assert_eq!(
            verify_auth_token(&[], Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Malformed)
        );
        assert_eq!(
            verify_auth_token(&bytes[..bytes.len() - 1], Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Invalid)
        );
        assert_eq!(
            verify_auth_token(&[0u8; 128], Utc::now(), WINDOW_SECONDS),
            Err(AuthTokenRejection::Invalid)
        );
    }

    #[test]
    fn rejects_tokens_outside_the_service_acceptance_window() {
        let keypair = Keypair::random();
        let bytes = genuine_token_bytes(&keypair);
        let drift = chrono::Duration::seconds(WINDOW_SECONDS + 1);

        // Server clock far ahead of the token: the token is expired.
        assert_eq!(
            verify_auth_token(&bytes, Utc::now() + drift, WINDOW_SECONDS),
            Err(AuthTokenRejection::OutsideAcceptanceWindow)
        );
        // Server clock behind the token: the token is from the future.
        assert_eq!(
            verify_auth_token(&bytes, Utc::now() - drift, WINDOW_SECONDS),
            Err(AuthTokenRejection::OutsideAcceptanceWindow)
        );
    }
}
