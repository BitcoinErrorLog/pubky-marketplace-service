//! HTTP integration coverage for the Phase 6 Wave 3a automation surfaces.
//! Every request runs through the real router, bearer middleware, and a real
//! PostgreSQL database.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, execute, new_actor, register_command, send, test_app, test_app_with_config,
};

#[sqlx::test(migrations = "./migrations")]
async fn sessions_can_be_listed_labeled_and_revoked(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let id = body["sessions"][0]["id"]
        .as_str()
        .expect("session id is present");

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
        "GET",
        "/v1/auth/sessions",
        Some(&seller.token),
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

    let path = format!("/v1/sellers/{}/listings", seller.pubky);
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], json!("seller_listing_export"));
    assert_eq!(body["listings"][0]["available_quantity"], json!(3));

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &path,
        Some(&foreign.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let event_path = format!("/v1/sellers/{}/events", seller.pubky);
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &event_path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["type"], json!("listing.registered"));
    assert!(body["events"][0]["cursor"].as_str().is_some());

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "boots_01"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(200));
}

#[sqlx::test(migrations = "./migrations")]
async fn webhook_lifecycle_returns_each_secret_once_and_deletion_fences_delivery(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

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

    let (status, _) = send(
        app.router.clone(),
        "DELETE",
        &format!("/v1/webhooks/{id}"),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
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

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/sellers/{}/orders", seller.pubky),
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
}
