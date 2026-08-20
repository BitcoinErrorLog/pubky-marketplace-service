//! Concurrency proof for the vertical slice (plan task 3.6 subset):
//! 100 concurrent purchase attempts for a single-unit listing yield exactly
//! one accepted order and 99 clean rejections, and a duplicate checkout with
//! the same command id returns the identical stored result without creating
//! a second order row.

mod common;

use axum::http::StatusCode;
use serde_json::json;
use sqlx::PgPool;

use common::{
    checkout_command, checkout_command_with_id, count, execute, indexed_command_id, new_actor,
    register_command, test_app,
};

#[sqlx::test]
async fn exactly_one_of_100_concurrent_purchases_wins_a_single_unit(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK);

    let mut handles = Vec::with_capacity(100);
    for index in 1..=100u64 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x8002, index));
        handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }

    let mut accepted = 0;
    let mut rejected = 0;
    for handle in handles {
        let (status, body) = handle.await.expect("request task completes");
        if body["ok"] == json!(true) {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["result"]["kind"], json!("checkout"));
            accepted += 1;
        } else {
            // Losers that race the winner's compare-and-swap fail with
            // REVISION_CONFLICT; losers that read after the winner committed
            // see the listing already reserved and fail with INVALID_STATE
            // (the engine checks listing state before the line revision).
            assert_eq!(status, StatusCode::CONFLICT, "unexpected rejection: {body}");
            let code = body["error"]["code"].as_str().expect("error code present");
            assert!(
                code == "REVISION_CONFLICT" || code == "INVALID_STATE",
                "unexpected rejection code: {body}"
            );
            rejected += 1;
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(rejected, 99);

    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 1);
    let (available, reserved, state): (i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, state FROM listings WHERE aggregate_id = $1",
    )
    .bind(common::listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!((available, reserved, state.as_str()), (0, 1, "reserved"));
}

#[sqlx::test]
async fn duplicate_checkout_replays_the_stored_result_without_a_second_order(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let command = checkout_command(&seller.pubky);

    let (first_status, first) = execute(&app, &buyer.token, &command).await;
    let (replay_status, replay) = execute(&app, &buyer.token, &command).await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(
        replay, first,
        "replay must return the identical stored result"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.created'"
        )
        .await,
        1
    );
}

#[sqlx::test]
async fn concurrent_duplicate_checkouts_converge_on_one_stored_result(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let command = checkout_command(&seller.pubky);

    let mut handles = Vec::with_capacity(10);
    for _ in 0..10 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = command.clone();
        handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }
    let mut bodies = Vec::with_capacity(10);
    for handle in handles {
        let (status, body) = handle.await.expect("request task completes");
        assert_eq!(status, StatusCode::OK, "duplicate must replay: {body}");
        bodies.push(body);
    }
    let first = &bodies[0];
    assert_eq!(first["ok"], json!(true));
    assert!(bodies.iter().all(|body| body == first));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}
