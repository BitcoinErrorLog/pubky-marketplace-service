//! HTTP integration coverage for the Phase 6 Wave 3a automation surfaces.
//! Every request runs through the real router, bearer middleware, and a real
//! PostgreSQL database.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use base64::Engine;
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use common::{
    authenticate, checkout_command, checkout_command_with_id, execute, new_actor, register_command,
    send, send_with_headers, test_app, test_app_with_config,
};

async fn get_with_etag(router: Router, uri: &str, token: &str, etag: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("if-none-match", etag)
        .body(Body::empty())
        .expect("conditional request builds");
    let response = router.oneshot(request).await.expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("JSON response")
    };
    (status, body)
}

#[sqlx::test(migrations = "./migrations")]
async fn sessions_can_be_listed_labeled_and_revoked(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let target_token = authenticate(&app, &seller.keypair).await;
    let foreign = new_actor(&app).await;

    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let target_hash = marketplace_service::auth::hash_token(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&target_token)
            .expect("target bearer decodes"),
    );
    let id: Uuid = sqlx::query_scalar("SELECT session_id FROM auth_sessions WHERE token_hash = $1")
        .bind(target_hash)
        .fetch_one(&app.pool)
        .await
        .expect("target session id");

    let (status, body) = send(
        app.router.clone(),
        "PATCH",
        &format!("/v1/auth/sessions/{id}"),
        Some(&foreign.token),
        &json!({"label": "warehouse", "metadata": {"client": "pubky-shop-cli"}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let (status, body) = send(
        app.router.clone(),
        "PATCH",
        &format!("/v1/auth/sessions/{id}"),
        Some(&seller.token),
        &json!({"label": "warehouse", "metadata": {"client": "pubky-shop-cli"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["label"], json!("warehouse"));

    let (status, _) = send(
        app.router.clone(),
        "DELETE",
        &format!("/v1/auth/sessions/{id}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(
        app.router.clone(),
        "DELETE",
        &format!("/v1/auth/sessions/{id}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "revoke must be idempotent");

    let (status, body) = send(
        app.router.clone(),
        "DELETE",
        &format!("/v1/auth/sessions/{id}"),
        Some(&foreign.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let (status, _) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(&target_token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn listing_export_event_cursor_and_sync_many_are_seller_scoped(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let foreign = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut second = register_command(&seller.pubky, 4);
    second["command_id"] = json!("00000000-0000-4000-8000-000000000122");
    second["aggregate_id"] = json!(format!("listing:{}_boots_02", seller.pubky));
    second["payload"]["listing_id"] = json!("boots_02");
    let (status, body) = execute(&app, &seller.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let path = format!("/v1/sellers/{}/listings?limit=1", seller.pubky);
    let (status, headers, body) = send_with_headers(
        app.router.clone(),
        "GET",
        &path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], json!("seller_listing_export"));
    assert_eq!(
        body["listings"][0]["projection"]["available_quantity"],
        json!(3)
    );
    assert_eq!(
        body["listings"][0]["record"]["listingId"],
        json!("boots_01")
    );
    assert!(body["listings"][0]["record_bytes_base64"]
        .as_str()
        .is_some());
    assert_eq!(
        body["listings"][0]["record_sha256"]
            .as_str()
            .expect("record digest")
            .len(),
        64
    );
    let listing_cursor = body["next_cursor"]
        .as_str()
        .expect("first listing page has a cursor");
    let etag = headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("listing page etag");
    let (status, body304) = get_with_etag(app.router.clone(), &path, &seller.token, etag).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED, "{body304}");
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("{path}&cursor={listing_cursor}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["listings"].as_array().expect("listing page").len(), 1);
    assert_eq!(
        body["listings"][0]["record"]["listingId"],
        json!("boots_02")
    );

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &path,
        Some(&foreign.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let event_path = format!("/v1/sellers/{}/events?limit=1", seller.pubky);
    let (status, headers, body) = send_with_headers(
        app.router.clone(),
        "GET",
        &event_path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["type"], json!("listing.registered"));
    let event_cursor = body["events"][0]["cursor"]
        .as_str()
        .expect("event cursor")
        .to_string();
    let next_event_cursor = body["next_cursor"]
        .as_str()
        .expect("event page continuation");
    let etag = headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("event page etag");
    let (status, body304) =
        get_with_etag(app.router.clone(), &event_path, &seller.token, etag).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED, "{body304}");
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("{event_path}&cursor={next_event_cursor}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"].as_array().expect("event page").len(), 1);

    let mut batch = vec![json!({
        "seller_pubky": seller.pubky,
        "listing_id": "boots_01"
    })];
    batch.extend((1..100).map(|index| {
        json!({
            "seller_pubky": seller.pubky,
            "listing_id": format!("missing_{index:03}")
        })
    }));
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": batch}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(
        body["results"].as_array().expect("batch results").len(),
        100
    );
    assert_eq!(body["results"][0]["status"], json!(200));
    assert_eq!(
        body["results"]
            .as_array()
            .expect("batch results")
            .iter()
            .filter(|result| result["status"] == json!(404))
            .count(),
        99
    );

    app.clock.advance_seconds(31 * 24 * 60 * 60);
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("{event_path}&cursor={event_cursor}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    assert_eq!(body["error"]["code"], json!("cursor_expired"));
}

#[sqlx::test(migrations = "./migrations")]
async fn webhook_lifecycle_returns_each_secret_once_and_deletion_fences_delivery(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    for url in [
        "https://127.0.0.1/hook",
        "https://[::ffff:127.0.0.1]/hook",
        "https://[::ffff:10.0.0.1]/hook",
        "https://[fe80::1]/hook",
        "https://[fc00::1]/hook",
    ] {
        let (status, body) = send(
            app.router.clone(),
            "POST",
            "/v1/webhooks",
            Some(&seller.token),
            &json!({"url": url}),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{url}: {body}");
    }

    let (status, added) = send(
        app.router.clone(),
        "POST",
        "/v1/webhooks",
        Some(&seller.token),
        &json!({"url": "https://hooks.example.com/pubky"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{added}");
    let id = added["webhook"]["id"].as_str().expect("webhook id");
    let first_secret = added["secret"].as_str().expect("one-time secret");

    let (status, rotated) = send(
        app.router.clone(),
        "POST",
        &format!("/v1/webhooks/{id}/rotate"),
        Some(&seller.token),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    assert_ne!(
        first_secret,
        rotated["secret"].as_str().expect("rotated secret")
    );
    assert_ne!(added["webhook"]["key_id"], rotated["key_id"]);

    let mut fence = app.pool.begin().await.expect("delivery fence transaction");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6354))")
        .bind(id)
        .execute(&mut *fence)
        .await
        .expect("delivery fence lock");
    let router = app.router.clone();
    let token = seller.token.clone();
    let path = format!("/v1/webhooks/{id}");
    let mut deletion =
        tokio::spawn(
            async move { send(router, "DELETE", &path, Some(&token), &Value::Null).await },
        );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut deletion)
            .await
            .is_err(),
        "delete returned while a delivery held the linearization fence"
    );
    fence.commit().await.expect("release delivery fence");
    let (status, _) = deletion.await.expect("deletion task joins");
    assert_eq!(status, StatusCode::NO_CONTENT);

    let active: bool =
        sqlx::query_scalar("SELECT deleted_at IS NULL FROM webhook_endpoints WHERE id = $1::uuid")
            .bind(id)
            .fetch_one(&app.pool)
            .await
            .expect("webhook row");
    assert!(!active);
}

#[sqlx::test(migrations = "./migrations")]
async fn endpoint_classes_use_service_clock_rate_limits(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.automation_rate_limit_per_minute = 1;
    config.automation_rate_limit_burst_multiplier = 1;
    let app = test_app_with_config(pool, config).await;
    let seller = new_actor(&app).await;

    let (status, _) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], json!("rate_limited"));
}

#[sqlx::test(migrations = "./migrations")]
async fn seller_order_export_uses_the_private_safe_allowlist(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let path = format!("/v1/sellers/{}/orders?limit=1", seller.pubky);
    let (status, headers, body) = send_with_headers(
        app.router.clone(),
        "GET",
        &path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = &body["orders"][0];
    assert!(order["id"].is_string());
    assert!(order.get("payment_method").is_some());
    for forbidden in [
        "delivery_address",
        "payment_id",
        "fiat_transaction_ref",
        "paykit_invoice_id",
        "bitcoin_quote_rate",
        "locks_bundle_id",
    ] {
        assert!(
            order.get(forbidden).is_none(),
            "{forbidden} leaked: {order}"
        );
    }
    let cursor = body["next_cursor"]
        .as_str()
        .expect("first order page cursor");
    let etag = headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("order page etag");
    let (status, body304) = get_with_etag(app.router.clone(), &path, &seller.token, etag).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED, "{body304}");
    let first_id = order["id"].clone();
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("{path}&cursor={cursor}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["orders"].as_array().expect("second order page").len(),
        1
    );
    assert_ne!(body["orders"][0]["id"], first_id);
}

#[sqlx::test(migrations = "./migrations")]
async fn unsafe_delivery_retries_then_dead_letters_without_connecting(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.webhook_max_attempts = 2;
    config.webhook_retry_base_seconds = 1;
    config.webhook_retry_max_seconds = 1;
    let app = test_app_with_config(pool, config).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let endpoint_id = Uuid::new_v4();
    let key_id = Uuid::new_v4();
    let now: DateTime<Utc> = common::NOW.parse().expect("fixture timestamp");
    sqlx::query(
        "INSERT INTO webhook_endpoints \
         (id, seller_pubky, endpoint_url, key_id, signing_key, created_at, updated_at) \
         VALUES ($1, $2, 'https://127.0.0.1/hook', $3, $4, $5, $5)",
    )
    .bind(endpoint_id)
    .bind(&seller.pubky)
    .bind(key_id)
    .bind(vec![7_u8; 32])
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("unsafe fixture endpoint");

    marketplace_service::automation::run_webhook_pass(&app.state)
        .await
        .expect("first worker pass");
    let first_attempt: (i32, DateTime<Utc>, Vec<u8>) = sqlx::query_as(
        "SELECT attempt_count, next_attempt_at, signing_key FROM webhook_deliveries \
         WHERE endpoint_id = $1",
    )
    .bind(endpoint_id)
    .fetch_one(&app.pool)
    .await
    .expect("first delivery attempt");
    assert_eq!(first_attempt.0, 1);
    assert_eq!(first_attempt.1, now + chrono::Duration::seconds(1));
    let (status, rotated) = send(
        app.router.clone(),
        "POST",
        &format!("/v1/webhooks/{endpoint_id}/rotate"),
        Some(&seller.token),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    let endpoint_key: Vec<u8> =
        sqlx::query_scalar("SELECT signing_key FROM webhook_endpoints WHERE id = $1")
            .bind(endpoint_id)
            .fetch_one(&app.pool)
            .await
            .expect("rotated endpoint key");
    assert_ne!(endpoint_key, first_attempt.2);
    let pinned_delivery_key: Vec<u8> =
        sqlx::query_scalar("SELECT signing_key FROM webhook_deliveries WHERE endpoint_id = $1")
            .bind(endpoint_id)
            .fetch_one(&app.pool)
            .await
            .expect("pinned delivery key");
    assert_eq!(pinned_delivery_key, first_attempt.2);
    app.clock.advance_seconds(2);
    marketplace_service::automation::run_webhook_pass(&app.state)
        .await
        .expect("second worker pass");

    let dead_letters: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM webhook_dead_letters WHERE endpoint_id = $1")
            .bind(endpoint_id)
            .fetch_one(&app.pool)
            .await
            .expect("dead-letter count");
    assert_eq!(dead_letters, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn webhook_enqueue_is_cursor_bounded_and_applies_per_seller_backpressure(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.webhook_enqueue_batch_size = 1;
    config.webhook_max_pending_per_seller = 1;
    let app = test_app_with_config(pool, config).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut second = register_command(&seller.pubky, 1);
    second["command_id"] = json!("00000000-0000-4000-8000-000000000123");
    second["aggregate_id"] = json!(format!("listing:{}_boots_02", seller.pubky));
    second["payload"]["listing_id"] = json!("boots_02");
    let (status, body) = execute(&app, &seller.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let endpoint_id = Uuid::new_v4();
    let now: DateTime<Utc> = common::NOW.parse().expect("fixture timestamp");
    sqlx::query(
        "INSERT INTO webhook_endpoints \
         (id, seller_pubky, endpoint_url, key_id, signing_key, created_at, updated_at) \
         VALUES ($1, $2, 'https://127.0.0.1/hook', $3, $4, $5, $5)",
    )
    .bind(endpoint_id)
    .bind(&seller.pubky)
    .bind(Uuid::new_v4())
    .bind(vec![7_u8; 32])
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("endpoint fixture");

    marketplace_service::automation::run_webhook_pass(&app.state)
        .await
        .expect("first bounded pass");
    let first: (i64, i64) = sqlx::query_as(
        "SELECT enqueue_sequence, \
           (SELECT COUNT(*) FROM webhook_deliveries WHERE endpoint_id = $1) \
         FROM webhook_endpoints WHERE id = $1",
    )
    .bind(endpoint_id)
    .fetch_one(&app.pool)
    .await
    .expect("first cursor and count");
    assert_eq!(first.1, 1);

    marketplace_service::automation::run_webhook_pass(&app.state)
        .await
        .expect("backpressured pass");
    let second: (i64, i64) = sqlx::query_as(
        "SELECT enqueue_sequence, \
           (SELECT COUNT(*) FROM webhook_deliveries WHERE endpoint_id = $1) \
         FROM webhook_endpoints WHERE id = $1",
    )
    .bind(endpoint_id)
    .fetch_one(&app.pool)
    .await
    .expect("second cursor and count");
    assert_eq!(
        second, first,
        "a full seller backlog must stop cursor advance"
    );
}
