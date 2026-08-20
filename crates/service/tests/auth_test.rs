//! HTTP-level tests for Pubky AuthToken authentication, using genuine
//! tokens signed with `pubky-common` — the same library the service
//! verifies with — end to end.

mod common;

use axum::http::StatusCode;
use chrono::Utc;
use marketplace_service::clock::Clock;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    auth_token_bytes, execute, new_actor, random_keypair, register_command, send, send_bytes,
    test_app, TestApp,
};

/// Posts raw AuthToken bytes with the test clock aligned to real system
/// time (tokens are stamped with real time by `AuthToken::sign`), restoring
/// the fixture instant afterwards.
async fn post_token(app: &TestApp, bytes: Vec<u8>) -> (StatusCode, Value) {
    let fixture_now = app.clock.now();
    app.clock.set(Utc::now());
    let result = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;
    app.clock.set(fixture_now);
    result
}

#[sqlx::test]
async fn a_valid_auth_token_mints_a_session(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, pubky) = random_keypair();

    let (status, session) = post_token(&app, auth_token_bytes(&keypair)).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "session issue failed: {session}"
    );
    assert_eq!(session["pubky"], json!(pubky));
    assert_eq!(session["capabilities"], json!("/:rw"));
    assert!(session["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert!(session["expires_at"].as_str().is_some());
}

#[sqlx::test]
async fn a_session_from_a_valid_token_authorizes_commands(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
}

#[sqlx::test]
async fn rejects_tampered_and_corrupted_tokens(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let genuine = auth_token_bytes(&keypair);

    // Tampered signature.
    let mut tampered_signature = genuine.clone();
    tampered_signature[0] ^= 0x01;
    let (status, body) = post_token(&app, tampered_signature).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "tampered signature accepted: {body}"
    );

    // Tampered payload (capabilities tail) breaks the signature.
    let mut tampered_payload = genuine.clone();
    let last = tampered_payload.len() - 1;
    tampered_payload[last] ^= 0x01;
    let (status, body) = post_token(&app, tampered_payload).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "tampered payload accepted: {body}"
    );

    // Truncated and garbage bytes.
    let (status, _) = post_token(&app, genuine[..genuine.len() - 1].to_vec()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_token(&app, vec![0u8; 16]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_token(&app, Vec::new()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // The genuine token still works: the rejections above were about the
    // corruption, not the token.
    let (status, _) = post_token(&app, genuine).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test]
async fn a_replayed_token_is_rejected(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    let (status, _) = post_token(&app, bytes.clone()).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = post_token(&app, bytes).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "replay accepted: {body}");
}

#[sqlx::test]
async fn an_expired_token_is_rejected_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    // The token was signed at real system time; the service clock is 121 s
    // later, past the 120 s acceptance window.
    app.clock.set(Utc::now() + chrono::Duration::seconds(121));
    let (status, body) = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "expired token accepted: {body}"
    );
}

#[sqlx::test]
async fn a_token_from_the_future_is_rejected_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    // The service clock is 121 s before the token's signing time.
    app.clock.set(Utc::now() - chrono::Duration::seconds(121));
    let (status, body) = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "future token accepted: {body}"
    );
}

#[sqlx::test]
async fn a_token_for_pubky_a_cannot_act_as_pubky_b(pool: PgPool) {
    let app = test_app(pool).await;
    let alice = new_actor(&app).await;
    let (_, bob_pubky) = random_keypair();

    // Alice's session is bound to the pubky inside her token; a command
    // claiming Bob as the seller is refused.
    let (status, body) = execute(&app, &alice.token, &register_command(&bob_pubky, 1)).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "cross-actor command accepted: {body}"
    );
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
}

#[sqlx::test]
async fn sessions_expire_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    // The session was minted at real system time (see `authenticate`); move
    // the clock one second past its 24 h TTL.
    app.clock
        .set(Utc::now() + chrono::Duration::seconds(86_401));
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn commands_require_a_bearer_session(pool: PgPool) {
    let app = test_app(pool).await;
    let (_, pubky) = random_keypair();

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        None,
        &register_command(&pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some("bm90LWEtcmVhbC10b2tlbg"),
        &register_command(&pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn the_challenge_endpoint_is_gone(pool: PgPool) {
    let app = test_app(pool).await;
    let (_, pubky) = random_keypair();

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/challenges",
        None,
        &json!({ "pubky": pubky }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn health_and_readiness_endpoints_respond(pool: PgPool) {
    let app = test_app(pool).await;

    let (status, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ok"));

    let (status, body) = send(app.router.clone(), "GET", "/ready", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ready"));
}
