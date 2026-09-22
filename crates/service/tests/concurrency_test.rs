//! Concurrency proof: exclusive inventory hold at payment-method bind.
//! 100 concurrent checkouts against a qty-1 listing all succeed unheld;
//! 100 concurrent binds then yield exactly one held order. Losers are 409
//! `INVALID_STATE` with the holding copy. Qty-10 yields ten held binds.
//! Two binders on a one-connection pool must return 200/409, never 5xx
//! (nested pool checkout during the bind transaction used to INTERNAL 500).
//! A duplicate checkout with the same command id still returns the identical
//! stored result without creating a second order.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, checkout_command_with_id, count, execute, indexed_command_id,
    listing_aggregate, new_actor, register_auction_command, register_command, send, test_app,
    test_app_with_payments,
};

const HOLDING_COPY: &str = "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks.";

async fn put_stripe_config(app: &common::TestApp, token: &str) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &json!({
            "bitcoin_enabled": false,
            "stripe_payment_link": "https://buy.stripe.com/test_abc123",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
}

async fn bind_stripe(router: axum::Router, token: String, order_id: String) -> (StatusCode, Value) {
    send(
        router,
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&token),
        &json!({ "method": "stripe" }),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn hundred_concurrent_binds_against_qty_one_yield_one_hold(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK);
    put_stripe_config(&app, &seller.token).await;

    let mut create_handles = Vec::with_capacity(100);
    for index in 1..=100u64 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x8002, index));
        create_handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }
    let mut order_ids = Vec::with_capacity(100);
    for handle in create_handles {
        let (status, body) = handle.await.expect("request task completes");
        assert_eq!(
            status,
            StatusCode::OK,
            "checkout must succeed unheld: {body}"
        );
        assert_eq!(body["result"]["orders"][0]["stock_held"], json!(false));
        order_ids.push(
            body["result"]["orders"][0]["id"]
                .as_str()
                .expect("order id")
                .to_string(),
        );
    }
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 100);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM orders WHERE stock_held").await,
        0
    );

    let mut bind_handles = Vec::with_capacity(100);
    for order_id in order_ids {
        let router = app.router.clone();
        let token = buyer.token.clone();
        bind_handles.push(tokio::spawn(async move {
            bind_stripe(router, token, order_id).await
        }));
    }
    let mut wins = 0;
    let mut holding = 0;
    for handle in bind_handles {
        let (status, body) = handle.await.expect("bind task completes");
        if body["ok"] == json!(true) {
            assert_eq!(status, StatusCode::OK, "winner: {body}");
            assert_eq!(body["order"]["stock_held"], json!(true));
            wins += 1;
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "loser: {body}");
            assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
            assert_eq!(body["error"]["message"], json!(HOLDING_COPY));
            holding += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one bind holds the unit");
    assert_eq!(holding, 99);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM orders WHERE stock_held").await,
        1
    );
    let (available, reserved, state): (i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, state FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!((available, reserved, state.as_str()), (0, 1, "reserved"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn hundred_concurrent_binds_against_qty_ten_yield_ten_holds(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 10)).await;
    assert_eq!(status, StatusCode::OK);
    put_stripe_config(&app, &seller.token).await;

    let mut create_handles = Vec::with_capacity(100);
    for index in 1..=100u64 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x8002, index));
        create_handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }
    let mut order_ids = Vec::with_capacity(100);
    for handle in create_handles {
        let (status, body) = handle.await.expect("request task completes");
        assert_eq!(
            status,
            StatusCode::OK,
            "checkout must succeed unheld: {body}"
        );
        assert_eq!(body["result"]["orders"][0]["stock_held"], json!(false));
        order_ids.push(
            body["result"]["orders"][0]["id"]
                .as_str()
                .expect("order id")
                .to_string(),
        );
    }

    let mut bind_handles = Vec::with_capacity(100);
    for order_id in order_ids {
        let router = app.router.clone();
        let token = buyer.token.clone();
        bind_handles.push(tokio::spawn(async move {
            bind_stripe(router, token, order_id).await
        }));
    }
    let mut wins = 0;
    let mut holding = 0;
    for handle in bind_handles {
        let (status, body) = handle.await.expect("bind task completes");
        if body["ok"] == json!(true) {
            assert_eq!(status, StatusCode::OK, "winner: {body}");
            assert_eq!(body["order"]["stock_held"], json!(true));
            wins += 1;
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "loser: {body}");
            assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
            assert_eq!(body["error"]["message"], json!(HOLDING_COPY));
            holding += 1;
        }
    }
    assert_eq!(wins, 10, "exactly the available stock is held at bind");
    assert_eq!(holding, 90);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM orders WHERE stock_held").await,
        10
    );
    let (available, reserved, state): (i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, state FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!((available, reserved, state.as_str()), (0, 10, "reserved"));
}

/// Nested `state.pool` checkout while the bind transaction holds the only
/// connection used to `PoolTimedOut` → HTTP 500. Bind must stay on that
/// connection so both callers see 200 or 409.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_concurrent_binds_on_one_connection_never_return_5xx(pool: PgPool) {
    let tight = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .expect("one-connection pool");
    let (app, _stripe, _paykit) = test_app_with_payments(tight).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK);
    put_stripe_config(&app, &seller.token).await;

    let mut order_ids = Vec::with_capacity(2);
    for index in 1..=2u64 {
        let command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x8003, index));
        let (status, body) = execute(&app, &buyer.token, &command).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "checkout must succeed unheld: {body}"
        );
        assert_eq!(body["result"]["orders"][0]["stock_held"], json!(false));
        order_ids.push(
            body["result"]["orders"][0]["id"]
                .as_str()
                .expect("order id")
                .to_string(),
        );
    }

    let left = bind_stripe(
        app.router.clone(),
        buyer.token.clone(),
        order_ids[0].clone(),
    );
    let right = bind_stripe(
        app.router.clone(),
        buyer.token.clone(),
        order_ids[1].clone(),
    );
    let (left, right) = tokio::join!(left, right);
    let results = [left, right];
    let mut wins = 0;
    let mut holding = 0;
    for (status, body) in results {
        assert!(
            status.as_u16() < 500,
            "bind must not 5xx under a one-connection pool: {status} {body}"
        );
        assert!(
            status == StatusCode::OK || status == StatusCode::CONFLICT,
            "bind must be 200 or 409: {status} {body}"
        );
        if status == StatusCode::OK {
            assert_eq!(body["ok"], json!(true), "winner: {body}");
            assert_eq!(body["order"]["stock_held"], json!(true));
            wins += 1;
        } else {
            assert_eq!(
                body["error"]["code"],
                json!("INVALID_STATE"),
                "loser: {body}"
            );
            assert_eq!(body["error"]["message"], json!(HOLDING_COPY));
            holding += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one bind holds the unit");
    assert_eq!(holding, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
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
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
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
        // Bidder i's proxy maximum: 6_000 .. 105_000 in 1_000-minor steps.
        // The spacing keeps each first-time maximum above the current
        // visible-price increment as concurrent bids serialize.
        let maximum_minor = 5_000 + 1_000 * (index as i64 + 1);
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
        (104_500..=105_000).contains(&visible),
        "visible price is runner-up bound: {visible}"
    );
    assert!(
        auction.get("reserve_met").is_none() && auction.get("reserve_price").is_none(),
        "stored auction state is reserve-blind"
    );

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
