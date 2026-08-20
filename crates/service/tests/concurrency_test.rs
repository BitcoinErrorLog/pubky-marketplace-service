//! Concurrency proof for the vertical slice (plan task 3.6 subset):
//! 100 concurrent purchase attempts for a single-unit listing yield exactly
//! one accepted order and 99 clean rejections, and a duplicate checkout with
//! the same command id returns the identical stored result without creating
//! a second order row.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, checkout_command_with_id, count, execute, indexed_command_id,
    listing_aggregate, new_actor, register_auction_command, register_command, test_app,
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

/// Auction concurrency proof: 100 concurrent proxy bidders (retrying on
/// revision conflicts, stopping on BID_TOO_LOW) converge on one
/// deterministic leader — the highest maximum — and the close produces
/// exactly one result and one winning order even when raced.
#[sqlx::test]
async fn hundred_concurrent_proxy_bids_produce_one_deterministic_leader_and_one_close(
    pool: PgPool,
) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, _) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut bidders = Vec::with_capacity(100);
    for _ in 0..100 {
        bidders.push(new_actor(&app).await);
    }
    let top_bidder_pubky = bidders[99].pubky.clone();

    let mut handles = Vec::with_capacity(100);
    for (index, bidder) in bidders.into_iter().enumerate() {
        let router = app.router.clone();
        let seller_pubky = seller.pubky.clone();
        // Bidder i's proxy maximum: 5_100 .. 15_000 in 100-minor steps.
        let maximum_minor = 5_000 + 100 * (index as i64 + 1);
        handles.push(tokio::spawn(async move {
            let mut expected_revision = 1i64;
            for attempt in 0..300u64 {
                let command = json!({
                    "version": 1,
                    "command_id": indexed_command_id(0x8003, (index as u64 + 1) * 1_000 + attempt),
                    "aggregate_id": listing_aggregate(&seller_pubky),
                    "expected_revision": expected_revision,
                    "issued_at": "2026-08-19T22:00:00.000Z",
                    "kind": "auction.place_bid",
                    "payload": {
                        "maximum_amount": {
                            "amount_minor": maximum_minor,
                            "currency": "USD",
                            "exponent": 2,
                        },
                    },
                });
                let (_, body) = common::send(
                    router.clone(),
                    "POST",
                    "/v1/commands",
                    Some(&bidder.token),
                    &command,
                )
                .await;
                if body["ok"] == json!(true) {
                    return "accepted";
                }
                match body["error"]["code"].as_str() {
                    Some("REVISION_CONFLICT") => {
                        expected_revision = body["error"]["current_revision"]
                            .as_i64()
                            .expect("revision conflicts carry current_revision");
                    }
                    Some("BID_TOO_LOW") => return "outbid",
                    other => panic!("unexpected bid rejection {other:?}: {body}"),
                }
            }
            panic!("bidder did not reach a terminal outcome in 300 attempts");
        }));
    }
    let mut accepted = 0;
    for handle in handles {
        if handle.await.expect("bid task completes") == "accepted" {
            accepted += 1;
        }
    }
    assert!(accepted >= 2, "at least the top bidders place bids");

    let (auction, revision): (Value, i64) =
        sqlx::query_as("SELECT auction, server_revision FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!(
        auction["leader_pubky"],
        json!(top_bidder_pubky),
        "the highest proxy maximum always leads"
    );
    let visible = auction["current_price"]["amount_minor"]
        .as_i64()
        .expect("visible price");
    assert!(
        (14_900..=15_000).contains(&visible),
        "visible price is runner-up bound: {visible}"
    );
    assert_eq!(auction["reserve_met"], json!(true));

    // Two concurrent closes: exactly one close result, exactly one winning
    // order (the second closer sees the auction already terminal).
    app.clock.advance_seconds(30 * 60);
    let mut close_handles = Vec::new();
    for close_index in 0..2u64 {
        let router = app.router.clone();
        let token = seller.token.clone();
        let command = json!({
            "version": 1,
            "command_id": indexed_command_id(0x8004, close_index + 1),
            "aggregate_id": listing_aggregate(&seller.pubky),
            "expected_revision": revision,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "auction.close",
            "payload": {},
        });
        close_handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }
    let mut close_accepted = 0;
    for handle in close_handles {
        let (_, body) = handle.await.expect("close task completes");
        if body["ok"] == json!(true) {
            assert_eq!(body["result"]["outcome"], json!("sold"));
            assert_eq!(body["result"]["winner_pubky"], json!(top_bidder_pubky));
            close_accepted += 1;
        } else {
            assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
        }
    }
    assert_eq!(close_accepted, 1, "exactly one close result");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE auction_aggregate_id IS NOT NULL"
        )
        .await,
        1,
        "exactly one winning order"
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'auction.closed_sold'"
        )
        .await,
        1
    );
}
