//! HTTP-level tests for Pubky challenge–response authentication (task 3.2),
//! using real ed25519 keypairs end to end.

mod common;

use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    execute, new_actor, random_keypair, register_command, send, sign_challenge, test_app, TestApp,
};

async fn issue_challenge(app: &TestApp, pubky: &str) -> Value {
    let (status, challenge) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/challenges",
        None,
        &json!({ "pubky": pubky }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    challenge
}

fn challenge_nonce(challenge: &Value) -> Vec<u8> {
    URL_SAFE_NO_PAD
        .decode(challenge["nonce"].as_str().expect("nonce present"))
        .expect("nonce decodes")
}

#[sqlx::test]
async fn full_challenge_response_flow_authorizes_commands(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
}

#[sqlx::test]
async fn rejects_a_forged_signature(pool: PgPool) {
    let app = test_app(pool).await;
    let (_owner, pubky) = random_keypair();
    let (forger, _) = random_keypair();
    let challenge = issue_challenge(&app, &pubky).await;

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        None,
        &json!({
            "pubky": pubky,
            "challenge_id": challenge["challenge_id"],
            "signature": sign_challenge(&forger, &challenge_nonce(&challenge)),
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "forged signature accepted: {body}"
    );
}

#[sqlx::test]
async fn rejects_a_mismatched_pubky(pool: PgPool) {
    let app = test_app(pool).await;
    let (signer, signer_pubky) = random_keypair();
    let (_, other_pubky) = random_keypair();
    let challenge = issue_challenge(&app, &signer_pubky).await;
    let signature = sign_challenge(&signer, &challenge_nonce(&challenge));

    // Valid signature by the signer, presented under a different pubky: the
    // challenge is bound to the requesting pubky, so the lookup fails.
    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        None,
        &json!({
            "pubky": other_pubky,
            "challenge_id": challenge["challenge_id"],
            "signature": signature,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A challenge issued to the other pubky but signed by the signer's key
    // fails signature verification.
    let other_challenge = issue_challenge(&app, &other_pubky).await;
    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        None,
        &json!({
            "pubky": other_pubky,
            "challenge_id": other_challenge["challenge_id"],
            "signature": sign_challenge(&signer, &challenge_nonce(&other_challenge)),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn challenges_are_single_use(pool: PgPool) {
    let app = test_app(pool).await;
    let (signing, pubky) = random_keypair();
    let challenge = issue_challenge(&app, &pubky).await;
    let signature = sign_challenge(&signing, &challenge_nonce(&challenge));
    let body = json!({
        "pubky": pubky,
        "challenge_id": challenge["challenge_id"],
        "signature": signature,
    });

    let (status, _) = send(app.router.clone(), "POST", "/v1/auth/sessions", None, &body).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(app.router.clone(), "POST", "/v1/auth/sessions", None, &body).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn challenges_expire_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let (signing, pubky) = random_keypair();
    let challenge = issue_challenge(&app, &pubky).await;
    app.clock.advance_seconds(121);

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        None,
        &json!({
            "pubky": pubky,
            "challenge_id": challenge["challenge_id"],
            "signature": sign_challenge(&signing, &challenge_nonce(&challenge)),
        }),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn sessions_expire_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    app.clock.advance_seconds(86_401);

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
async fn rejects_challenge_requests_for_invalid_pubkys(pool: PgPool) {
    let app = test_app(pool).await;

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/challenges",
        None,
        &json!({ "pubky": "not-a-pubky" }),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
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
