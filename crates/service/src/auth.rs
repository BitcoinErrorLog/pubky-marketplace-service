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
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use pubky_common::auth::AuthToken;
use rand::RngCore;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::clock::format_timestamp;
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

/// The authenticated actor, resolved from a session token by middleware.
#[derive(Debug, Clone)]
pub struct Actor(pub String);

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
        capabilities: token.capabilities().to_string(),
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
    let stored = sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(hash_token(&token))
    .bind(&verified.pubky)
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

    tracing::info!(pubky = %verified.pubky, "issued auth session");
    (
        StatusCode::CREATED,
        Json(json!({
            "token": URL_SAFE_NO_PAD.encode(token),
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
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "A session bearer token is required.",
        );
    };

    let now = state.clock.now();
    let session: Option<(String, DateTime<Utc>)> = match sqlx::query_as(
        "SELECT pubky, expires_at FROM auth_sessions WHERE token_hash = $1 AND expires_at > $2",
    )
    .bind(hash_token(&token))
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
    let Some((pubky, _)) = session else {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The session is invalid or expired.",
        );
    };

    request.extensions_mut().insert(Actor(pubky));
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
