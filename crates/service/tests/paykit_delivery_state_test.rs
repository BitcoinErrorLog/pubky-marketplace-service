//! Whether the Bitcoin payment request reached the buyer's wallet: the
//! status poll records paykit-server's `paykit_delivery_state` on the order
//! and the order projection carries it.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{bound_order, poll_now};
use common::*;
use marketplace_service::clock::Clock;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

fn undetected_with_delivery(delivery: &str) -> Value {
    let mut body = bitcoin_status_v2("undetected", false, "exclusive", None, None, None);
    body["paykit_delivery_state"] = json!(delivery);
    body
}

async fn stored(pool: &PgPool, order_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT paykit_delivery_state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn projected(app: &TestApp, buyer: &TestActor, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["paykit_delivery_state"].clone()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_poll_records_whether_the_request_reached_the_wallet(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    assert_eq!(
        stored(&pool, &order_id).await,
        None,
        "unknown before the poll"
    );
    assert_eq!(projected(&app, &buyer, &order_id).await, Value::Null);

    let mut now = app.clock.now();
    paykit.set_status(&reference, undetected_with_delivery("pending_delivery"));
    poll_now(&app, now).await;
    assert_eq!(stored(&pool, &order_id).await.as_deref(), Some("pending"));
    assert_eq!(projected(&app, &buyer, &order_id).await, json!("pending"));

    // Paykit gave up: the wallet never answered the link.
    now += chrono::Duration::seconds(60);
    paykit.set_status(&reference, undetected_with_delivery("failed"));
    poll_now(&app, now).await;
    assert_eq!(stored(&pool, &order_id).await.as_deref(), Some("failed"));
    assert_eq!(projected(&app, &buyer, &order_id).await, json!("failed"));

    // A final state is never moved back, and non-delivery facts change nothing.
    for wire in ["pending_delivery", "cancelled", "contract_error"] {
        now += chrono::Duration::seconds(60);
        paykit.set_status(&reference, undetected_with_delivery(wire));
        poll_now(&app, now).await;
        assert_eq!(
            stored(&pool, &order_id).await.as_deref(),
            Some("failed"),
            "{wire}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_delivered_request_is_recorded(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    paykit.set_status(&reference, undetected_with_delivery("delivered"));
    poll_now(&app, app.clock.now()).await;
    assert_eq!(stored(&pool, &order_id).await.as_deref(), Some("delivered"));
    assert_eq!(projected(&app, &buyer, &order_id).await, json!("delivered"));
}
