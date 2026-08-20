//! Pubky challenge–response authentication (plan task 3.2).
//!
//! Flow:
//! 1. `POST /v1/auth/challenges` issues a random 32-byte nonce bound to the
//!    requesting pubky, stored server-side with a short TTL, single use.
//! 2. The client signs `CHALLENGE_CONTEXT || nonce` with its ed25519 key.
//! 3. `POST /v1/auth/sessions` verifies the signature against the z-base-32
//!    pubky and issues an opaque 32-byte session token, stored hashed.
//! 4. The token is presented as `Authorization: Bearer <token>`; middleware
//!    resolves the actor pubky from the stored hash. No trust-me headers.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, VerifyingKey};
use rand::RngCore;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::AppState;

/// Domain separation for challenge signatures: binds the signature to this
/// service and protocol version so it cannot be replayed elsewhere.
pub const CHALLENGE_CONTEXT: &[u8] = b"pubky-marketplace-transaction-service:auth:v1\n";

/// The authenticated actor, resolved from a session token by middleware.
#[derive(Debug, Clone)]
pub struct Actor(pub String);

/// Verifies an ed25519 signature over `CHALLENGE_CONTEXT || nonce` against a
/// z-base-32 pubky. Pure function; all failures collapse to `false`.
pub fn verify_challenge_signature(pubky: &str, nonce: &[u8], signature: &[u8]) -> bool {
    if !marketplace_domain::pubky::is_valid_pubky(pubky) {
        return false;
    }
    let Ok(key_bytes) = z32::decode(pubky.as_bytes()) else {
        return false;
    };
    let Ok(key_bytes) = <[u8; 32]>::try_from(key_bytes.as_slice()) else {
        return false;
    };
    let Ok(verifying_key) = VerifyingKey::from_bytes(&key_bytes) else {
        return false;
    };
    let Ok(signature_bytes) = <[u8; 64]>::try_from(signature) else {
        return false;
    };
    let signature = Signature::from_bytes(&signature_bytes);
    let mut message = Vec::with_capacity(CHALLENGE_CONTEXT.len() + nonce.len());
    message.extend_from_slice(CHALLENGE_CONTEXT);
    message.extend_from_slice(nonce);
    verifying_key.verify_strict(&message, &signature).is_ok()
}

pub fn hash_token(token: &[u8]) -> Vec<u8> {
    Sha256::digest(token).to_vec()
}

fn auth_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": { "message": message } }))).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChallengeRequest {
    pub pubky: String,
}

pub async fn create_challenge(
    State(state): State<AppState>,
    Json(request): Json<ChallengeRequest>,
) -> Response {
    if !marketplace_domain::pubky::is_valid_pubky(&request.pubky) {
        return auth_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Expected a 52-character z-base-32 Pubky.",
        );
    }
    let now = state.clock.now();
    let expires_at = now + chrono::Duration::seconds(state.config.challenge_ttl_seconds);
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let challenge_id = Uuid::new_v4();

    let inserted = sqlx::query(
        "INSERT INTO auth_challenges (id, pubky, nonce, created_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(challenge_id)
    .bind(&request.pubky)
    .bind(nonce.as_slice())
    .bind(now)
    .bind(expires_at)
    .execute(&state.pool)
    .await;
    if let Err(error) = inserted {
        tracing::error!(error = %error, "failed to store auth challenge");
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Challenge could not be issued.",
        );
    }

    tracing::info!(challenge_id = %challenge_id, "issued auth challenge");
    (
        StatusCode::CREATED,
        Json(json!({
            "challenge_id": challenge_id,
            "nonce": URL_SAFE_NO_PAD.encode(nonce),
            "context": String::from_utf8_lossy(CHALLENGE_CONTEXT).trim_end(),
            "expires_at": format_timestamp(expires_at),
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRequest {
    pub pubky: String,
    pub challenge_id: Uuid,
    /// base64url (unpadded) ed25519 signature over `CHALLENGE_CONTEXT || nonce`.
    pub signature: String,
}

pub async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<SessionRequest>,
) -> Response {
    let now = state.clock.now();
    let Ok(signature) = URL_SAFE_NO_PAD.decode(&request.signature) else {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The challenge response is invalid.",
        );
    };

    // Consume the challenge atomically: bound pubky, unused, and unexpired.
    let consumed: Option<(Vec<u8>,)> = match sqlx::query_as(
        "UPDATE auth_challenges SET used_at = $1 \
         WHERE id = $2 AND pubky = $3 AND used_at IS NULL AND expires_at > $1 \
         RETURNING nonce",
    )
    .bind(now)
    .bind(request.challenge_id)
    .bind(&request.pubky)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(row) => row,
        Err(error) => {
            tracing::error!(error = %error, "failed to consume auth challenge");
            return auth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Session could not be created.",
            );
        }
    };
    let Some((nonce,)) = consumed else {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The challenge response is invalid.",
        );
    };

    if !verify_challenge_signature(&request.pubky, &nonce, &signature) {
        return auth_error(
            StatusCode::UNAUTHORIZED,
            "The challenge response is invalid.",
        );
    }

    let mut token = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token);
    let expires_at = now + chrono::Duration::seconds(state.config.session_ttl_seconds);
    let stored = sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, created_at, expires_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(hash_token(&token))
    .bind(&request.pubky)
    .bind(now)
    .bind(expires_at)
    .execute(&state.pool)
    .await;
    if let Err(error) = stored {
        tracing::error!(error = %error, "failed to store auth session");
        return auth_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Session could not be created.",
        );
    }

    tracing::info!(challenge_id = %request.challenge_id, "issued auth session");
    (
        StatusCode::CREATED,
        Json(json!({
            "token": URL_SAFE_NO_PAD.encode(token),
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
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    fn keypair() -> (SigningKey, String) {
        let signing = SigningKey::generate(&mut OsRng);
        let pubky = z32::encode(signing.verifying_key().as_bytes());
        (signing, pubky)
    }

    fn sign_challenge(signing: &SigningKey, nonce: &[u8]) -> Vec<u8> {
        let mut message = CHALLENGE_CONTEXT.to_vec();
        message.extend_from_slice(nonce);
        signing.sign(&message).to_bytes().to_vec()
    }

    #[test]
    fn accepts_a_genuine_signature_from_the_key_owner() {
        let (signing, pubky) = keypair();
        let nonce = [7u8; 32];
        let signature = sign_challenge(&signing, &nonce);
        assert!(verify_challenge_signature(&pubky, &nonce, &signature));
    }

    #[test]
    fn rejects_a_forged_signature() {
        let (_, pubky) = keypair();
        let (forger, _) = keypair();
        let nonce = [7u8; 32];
        let forged = sign_challenge(&forger, &nonce);
        assert!(!verify_challenge_signature(&pubky, &nonce, &forged));
    }

    #[test]
    fn rejects_a_mismatched_pubky() {
        let (signing, _) = keypair();
        let (_, other_pubky) = keypair();
        let nonce = [7u8; 32];
        let signature = sign_challenge(&signing, &nonce);
        assert!(!verify_challenge_signature(
            &other_pubky,
            &nonce,
            &signature
        ));
    }

    #[test]
    fn rejects_a_signature_over_a_different_nonce() {
        let (signing, pubky) = keypair();
        let signature = sign_challenge(&signing, &[1u8; 32]);
        assert!(!verify_challenge_signature(&pubky, &[2u8; 32], &signature));
    }

    #[test]
    fn rejects_a_signature_without_the_domain_context() {
        let (signing, pubky) = keypair();
        let nonce = [7u8; 32];
        let raw = signing.sign(&nonce).to_bytes().to_vec();
        assert!(!verify_challenge_signature(&pubky, &nonce, &raw));
    }

    #[test]
    fn rejects_malformed_pubkys_and_signatures() {
        let (signing, pubky) = keypair();
        let nonce = [7u8; 32];
        let signature = sign_challenge(&signing, &nonce);
        assert!(!verify_challenge_signature(
            "not-a-pubky",
            &nonce,
            &signature
        ));
        assert!(!verify_challenge_signature(
            &pubky,
            &nonce,
            &signature[..63]
        ));
    }
}
