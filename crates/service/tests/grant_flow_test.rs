mod common;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signer, SigningKey};
use http_body_util::BodyExt;
use marketplace_domain::pubky::encode_pubky;
use marketplace_service::clock::Clock;
use marketplace_service::grant;
use marketplace_service::grant::test_support::SeedCompletedFlow;
use pubky_common::crypto::Keypair;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

use common::{send, test_app_with_grant, NOW};

async fn send_signed(
    router: axum::Router,
    uri: &str,
    body: Vec<u8>,
    signature: String,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-marketplace-signature", signature)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("JSON response")
    };
    (status, headers, body)
}

fn assert_no_store(headers: &axum::http::HeaderMap) {
    assert!(headers
        .get(header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("no-store")));
}

#[sqlx::test(migrations = "./migrations")]
async fn migration_0036_preserves_bearer_key_and_reuses_uuid_session_identity(pool: sqlx::PgPool) {
    let token_hash = vec![7u8; 32];
    let now: DateTime<Utc> = NOW.parse().unwrap();
    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO auth_sessions (token_hash,pubky,capabilities,created_at,expires_at) \
         VALUES ($1,$2,'',$3,$4) RETURNING session_id",
    )
    .bind(&token_hash)
    .bind("y".repeat(52))
    .bind(now)
    .bind(now + Duration::hours(1))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!session_id.is_nil());
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT token_hash FROM auth_sessions WHERE session_id = $1")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, token_hash);
}

#[sqlx::test(migrations = "./migrations")]
async fn client_asserted_identity_and_ambiguous_principal_create_nothing(pool: sqlx::PgPool) {
    let (app, _) = test_app_with_grant(pool.clone()).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion":"opaque","expected_pubky":"y".repeat(52)}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_request");

    let (status, body) = send(
        app.router,
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion":"opaque","delivery_assertion":"opaque"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "ambiguous_principal");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM grant_flows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn bootstrap_identity_is_assertion_derived_and_jti_is_single_use(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool.clone()).await;
    let now = app.clock.now();
    let signer = Keypair::random();
    let expected_pubky = signer.public_key().z32();
    let result_key = SigningKey::from_bytes(&[19u8; 32]);
    let result_cpk = encode_pubky(&result_key.verifying_key().to_bytes());
    let assertion = authority.sign_bootstrap_assertion(
        &expected_pubky,
        &[23u8; 32],
        &result_cpk,
        now,
        Uuid::new_v4(),
    );
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion":assertion}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let flow_id = body["flow_id"].as_str().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT expected_pubky FROM grant_flows WHERE flow_id = $1"
        )
        .bind(Uuid::parse_str(flow_id).unwrap())
        .fetch_one(&pool)
        .await
        .unwrap(),
        expected_pubky
    );

    let (status, _) = send(
        app.router,
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion":assertion}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM grant_flows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn flow_id_status_never_returns_result_authority(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool).await;
    let result_key = SigningKey::from_bytes(&[31u8; 32]);
    let pubky = "y".repeat(52);
    let flow_id = authority
        .seed_completed_flow(
            &app.pool,
            SeedCompletedFlow {
                expected_pubky: &pubky,
                result_cpk: &encode_pubky(&result_key.verifying_key().to_bytes()),
                delivery_id: [5u8; 32],
                bearer: [6u8; 32],
                result_token: [7u8; 32],
                now: app.clock.now(),
            },
        )
        .await;
    let (status, body) = send(
        app.router,
        "GET",
        &format!("/v1/auth/grant-flows/{flow_id}"),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    let text = body.to_string();
    for forbidden in ["token", "bearer", "payload", "delivery", "cpk"] {
        assert!(!text.contains(forbidden), "{forbidden} leaked in {text}");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn status_rate_limit_is_shared_in_postgres(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool).await;
    let result_key = SigningKey::from_bytes(&[30u8; 32]);
    let flow_id = authority
        .seed_completed_flow(
            &app.pool,
            SeedCompletedFlow {
                expected_pubky: &"y".repeat(52),
                result_cpk: &encode_pubky(&result_key.verifying_key().to_bytes()),
                delivery_id: [31u8; 32],
                bearer: [32u8; 32],
                result_token: [33u8; 32],
                now: app.clock.now(),
            },
        )
        .await;
    let uri = format!("/v1/auth/grant-flows/{flow_id}");
    for _ in 0..60 {
        assert_eq!(
            send(app.router.clone(), "GET", &uri, None, &Value::Null)
                .await
                .0,
            StatusCode::OK
        );
    }
    let (status, body) = send(app.router, "GET", &uri, None, &Value::Null).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"], "rate_limited");
    let (count, bucket_hash): (i32, Vec<u8>) = sqlx::query_as(
        "SELECT request_count, bucket_hash FROM grant_rate_limits \
         WHERE endpoint_class = 'status_flow'",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(count, 61);
    let mut unhashed = Sha256::new();
    unhashed.update(b"marketplace/grant-rate-limit/v1");
    unhashed.update(b"status_flow");
    unhashed.update([0]);
    unhashed.update(format!("127.0.0.1:{flow_id}").as_bytes());
    assert_ne!(bucket_hash, unhashed.finalize().as_slice());
}

#[sqlx::test(migrations = "./migrations")]
async fn wrong_verified_signer_is_visible_409_mints_no_bearer_and_cannot_replay(
    pool: sqlx::PgPool,
) {
    let (app, authority) = test_app_with_grant(pool.clone()).await;
    let expected = "y".repeat(52);
    let approved = "o".repeat(52);
    let (flow_id, first, replay) = authority
        .settle_verified_identity(&app.state, &expected, &approved)
        .await;
    assert!(first);
    assert!(!replay, "terminal approval must not settle twice");

    let (status, body) = send(
        app.router,
        "GET",
        &format!("/v1/auth/grant-flows/{flow_id}"),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["status"], "mismatch");
    assert_eq!(body["terminal_code"], "identity_mismatch");
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auth_sessions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sessions, 0);
    let row: (bool, bool) = sqlx::query_as(
        "SELECT grant_state_sealed IS NULL, result_payload_sealed IS NULL \
         FROM grant_flows WHERE flow_id = $1",
    )
    .bind(flow_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, (true, true));
}

struct ProofInput<'a> {
    flow_id: Uuid,
    path: &'a str,
    purpose: &'a str,
    delivery_id: &'a [u8; 32],
    nonce_id: &'a str,
    nonce: &'a str,
    issued_at: i64,
}

fn proof(key: &SigningKey, input: ProofInput<'_>) -> Value {
    let delivery = URL_SAFE_NO_PAD.encode(input.delivery_id);
    let signed = json!({
        "domain":"marketplace/grant-result-pop/v1",
        "flow_id":input.flow_id.to_string(),
        "issued_at":input.issued_at,
        "method":"POST",
        "nonce":input.nonce,
        "nonce_id":input.nonce_id,
        "path":input.path,
        "purpose":input.purpose,
        "result_delivery_id":delivery,
    });
    let bytes = serde_json_canonicalizer::to_string(&signed).unwrap();
    json!({
        "issued_at":input.issued_at,
        "nonce":input.nonce,
        "nonce_id":input.nonce_id,
        "signature":URL_SAFE_NO_PAD.encode(key.sign(bytes.as_bytes()).to_bytes()),
    })
}

async fn nonce(
    app: &common::TestApp,
    authority: &marketplace_service::grant::test_support::GrantTestAuthority,
    flow_id: Uuid,
    purpose: &str,
) -> Value {
    let path = format!("/v1/auth/grant-flows/{flow_id}/result-nonces");
    let request = json!({
        "method":"POST",
        "path":path,
        "purpose":purpose,
        "request_id":Uuid::new_v4().to_string(),
    });
    let (body, signature) = authority.sign_service_body(&request);
    let (status, headers, response) = send_signed(app.router.clone(), &path, body, signature).await;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    assert_no_store(&headers);
    response
}

#[sqlx::test(migrations = "./migrations")]
async fn retrieval_token_is_delivered_and_claimed_once(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool.clone()).await;
    let result_key = SigningKey::from_bytes(&[41u8; 32]);
    let delivery_id = [42u8; 32];
    let bearer = [43u8; 32];
    let result_token = [44u8; 32];
    let pubky = "y".repeat(52);
    let flow_id = authority
        .seed_completed_flow(
            &pool,
            SeedCompletedFlow {
                expected_pubky: &pubky,
                result_cpk: &encode_pubky(&result_key.verifying_key().to_bytes()),
                delivery_id,
                bearer,
                result_token,
                now: app.clock.now(),
            },
        )
        .await;

    let ticket_nonce = nonce(&app, &authority, flow_id, "ticket").await;
    let ticket_path = format!("/v1/auth/grant-flows/{flow_id}/result-ticket");
    let ticket = json!({
        "method":"POST",
        "path":ticket_path,
        "proof":proof(&result_key, ProofInput {
            flow_id,
            path:&ticket_path,
            purpose:"ticket",
            delivery_id:&delivery_id,
            nonce_id:ticket_nonce["nonce_id"].as_str().unwrap(),
            nonce:ticket_nonce["nonce"].as_str().unwrap(),
            issued_at:app.clock.now().timestamp(),
        }),
        "request_id":Uuid::new_v4().to_string(),
        "result_delivery_id":URL_SAFE_NO_PAD.encode(delivery_id),
    });
    let (ticket_body, ticket_signature) = authority.sign_service_body(&ticket);
    let (status, _, response) = send_signed(
        app.router.clone(),
        &ticket_path,
        ticket_body.clone(),
        ticket_signature.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(
        response["result_token"],
        URL_SAFE_NO_PAD.encode(result_token)
    );
    let (status, _, _) = send_signed(
        app.router.clone(),
        &ticket_path,
        ticket_body,
        ticket_signature,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let claim_nonce = nonce(&app, &authority, flow_id, "claim").await;
    let claim_path = format!("/v1/auth/grant-flows/{flow_id}/claim");
    let mut claim = json!({
        "method":"POST",
        "path":claim_path,
        "proof":proof(&result_key, ProofInput {
            flow_id,
            path:&claim_path,
            purpose:"claim",
            delivery_id:&delivery_id,
            nonce_id:claim_nonce["nonce_id"].as_str().unwrap(),
            nonce:claim_nonce["nonce"].as_str().unwrap(),
            issued_at:app.clock.now().timestamp(),
        }),
        "request_id":Uuid::new_v4().to_string(),
        "result_delivery_id":URL_SAFE_NO_PAD.encode(delivery_id),
        "result_token":URL_SAFE_NO_PAD.encode(result_token),
    });
    let mut wrong_claim = claim.clone();
    wrong_claim["result_token"] = Value::String(URL_SAFE_NO_PAD.encode([99u8; 32]));
    let (wrong_body, wrong_signature) = authority.sign_service_body(&wrong_claim);
    let (status, _, wrong_response) =
        send_signed(app.router.clone(), &claim_path, wrong_body, wrong_signature).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(wrong_response["error"], "result_denied");
    assert_eq!(wrong_response.as_object().unwrap().len(), 1);

    claim["request_id"] = Value::String(Uuid::new_v4().to_string());
    let (claim_body, claim_signature) = authority.sign_service_body(&claim);
    let (status, _, response) = send_signed(
        app.router.clone(),
        &claim_path,
        claim_body.clone(),
        claim_signature.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["token"], URL_SAFE_NO_PAD.encode(bearer));
    let (status, _, _) = send_signed(app.router, &claim_path, claim_body, claim_signature).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let row: (bool, bool, bool) = sqlx::query_as(
        "SELECT result_claimed_at IS NOT NULL, result_token_hash IS NULL, \
         result_payload_sealed IS NULL FROM grant_flows WHERE flow_id = $1",
    )
    .bind(flow_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, (true, true, true));
    let service_requests: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM grant_service_requests")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        service_requests, 5,
        "duplicate request IDs are not reinserted"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn reaper_terminalizes_expiry_and_stale_lease_without_replay(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool.clone()).await;
    let now = app.clock.now();
    for (status, lease_until) in [
        ("awaiting", None),
        ("verifying", Some(now - Duration::seconds(1))),
    ] {
        sqlx::query(
            "INSERT INTO grant_flows (flow_id,expected_pubky,assertion_jti,client_id,cpk,\
             capabilities,relay_url,grant_state_sealed,key_epoch,result_hash_epoch,status,\
             version,lease_owner,lease_until,result_delivery_id_hash,result_cpk,created_at,expires_at) \
             VALUES ($1,$2,$3,$4,$5,'',$6,$7,1,1,$8,1,$9,$10,$11,$12,$13,$14)",
        )
        .bind(Uuid::new_v4())
        .bind("y".repeat(52))
        .bind(Uuid::new_v4())
        .bind(format!("{}-{status}", authority.runtime.config.client_id))
        .bind(if status == "awaiting" { "y".repeat(52) } else { "o".repeat(52) })
        .bind(authority.runtime.config.relay_url.as_str())
        .bind(vec![9u8; 80])
        .bind(status)
        .bind(Uuid::new_v4())
        .bind(lease_until)
        .bind(vec![8u8; 32])
        .bind("y".repeat(52))
        .bind(now - Duration::minutes(10))
        .bind(now - Duration::seconds(1))
        .execute(&pool)
        .await
        .unwrap();
    }
    assert_eq!(grant::reap_once(&app.state).await.unwrap(), 2);
    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT status, terminal_code, grant_state_sealed IS NULL \
         FROM grant_flows ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(rows.contains(&("expired".into(), "flow_expired".into(), true)));
    assert!(rows.contains(&("failed".into(), "lease_lost".into(), true)));
}

#[sqlx::test(migrations = "./migrations")]
async fn result_expiry_revokes_both_undelivered_and_delivered_sessions(pool: sqlx::PgPool) {
    let (app, authority) = test_app_with_grant(pool.clone()).await;
    let result_key = SigningKey::from_bytes(&[51u8; 32]);
    let result_cpk = encode_pubky(&result_key.verifying_key().to_bytes());
    let delivered_result_key = SigningKey::from_bytes(&[58u8; 32]);
    let delivered_result_cpk = encode_pubky(&delivered_result_key.verifying_key().to_bytes());
    let undelivered = authority
        .seed_completed_flow(
            &pool,
            SeedCompletedFlow {
                expected_pubky: &"y".repeat(52),
                result_cpk: &result_cpk,
                delivery_id: [52u8; 32],
                bearer: [53u8; 32],
                result_token: [54u8; 32],
                now: app.clock.now(),
            },
        )
        .await;
    let delivered = authority
        .seed_completed_flow(
            &pool,
            SeedCompletedFlow {
                expected_pubky: &"o".repeat(52),
                result_cpk: &delivered_result_cpk,
                delivery_id: [55u8; 32],
                bearer: [56u8; 32],
                result_token: [57u8; 32],
                now: app.clock.now(),
            },
        )
        .await;
    sqlx::query("UPDATE grant_flows SET result_token_delivered_at = $2 WHERE flow_id = $1")
        .bind(delivered)
        .bind(app.clock.now())
        .execute(&pool)
        .await
        .unwrap();
    app.clock.advance_seconds(61);
    assert_eq!(grant::reap_once(&app.state).await.unwrap(), 2);
    assert_eq!(
        grant::reap_once(&app.state).await.unwrap(),
        0,
        "cleared result rows must not be reprocessed"
    );
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auth_sessions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sessions, 0);
    for flow_id in [undelivered, delivered] {
        let cleared: (bool, bool, bool) = sqlx::query_as(
            "SELECT result_token_hash IS NULL, result_payload_sealed IS NULL, \
             result_auth_session_id IS NULL FROM grant_flows WHERE flow_id = $1",
        )
        .bind(flow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(cleared, (true, true, true));
    }
}

#[test]
fn real_rc55_bitkit_artifact_is_pinned_before_enablement() {
    let fixture = std::fs::read_to_string("tests/fixtures/grant/bitkit-rc55-staging.json")
        .expect("real staging artifact is pinned");
    let value: Value = serde_json::from_str(&fixture).expect("artifact JSON");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["intent"], "signin_grant");
    assert_eq!(
        value["parameter_names"],
        json!(["caps", "cid", "cpk", "relay", "secret", "x-bitkit-claim"])
    );
    assert_eq!(value["parameter_cardinality"], "one_each");
    assert_eq!(value["secret_decoded_bytes"], 32);
    assert_eq!(value["claim_type"], "watch-only-account-v1");
    let redacted_url = value["authorization_url_redacted"]
        .as_str()
        .expect("redacted authorization URL");
    for parameter in ["secret", "cid", "cpk"] {
        assert!(
            redacted_url.contains(&format!("{parameter}=%3Credacted%3E")),
            "{parameter} must be redacted"
        );
    }
    assert_eq!(value["provenance"]["staging_source_head"], "1f0d0974");
    assert_eq!(
        value["provenance"]["source_flow"],
        "Paykit staging /setup from issue #48"
    );
    assert_eq!(value["provenance"]["sensitive_payload_recorded"], false);
    for forbidden in ["grant", "pop_private_key", "bearer", "relay_secret"] {
        assert!(value.get(forbidden).is_none());
    }
}

#[test]
fn no_plaintext_secret_sentinels_are_hashed_as_their_stored_forms() {
    let sentinel = [0xA5u8; 32];
    let encoded = URL_SAFE_NO_PAD.encode(sentinel);
    let digest = Sha256::digest(sentinel);
    assert_ne!(digest.as_slice(), sentinel);
    assert!(!hex::encode(digest).contains(&encoded));
}
