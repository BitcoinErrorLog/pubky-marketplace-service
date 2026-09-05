//! Auction tests ported from the TypeScript prototype suite: deterministic
//! proxy bidding, first-accepted-sequence tie break, anti-sniping extension,
//! reserve status, and authoritative close. The Rust service additionally
//! creates exactly one winning order and sandbox payment on a sold close
//! (see the README divergence table).

mod common;

use axum::http::StatusCode;
use serde_json::json;
use sqlx::PgPool;

use common::{
    close_auction_command, count, execute, listing_aggregate, new_actor, place_bid_command,
    register_auction_command, register_command, send, test_app, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::drain_outbox;

/// Delivered notifications as (type, recipient, amount JSON) after draining
/// the outbox, ordered for stable assertions.
async fn delivered_notifications(app: &TestApp) -> Vec<(String, String, serde_json::Value)> {
    drain_outbox(&app.pool, app.clock.now(), 50)
        .await
        .expect("outbox drains");
    sqlx::query_as::<_, (String, String, Option<serde_json::Value>)>(
        "SELECT type, recipient_pubky, amount FROM notifications ORDER BY type, recipient_pubky",
    )
    .fetch_all(&app.pool)
    .await
    .expect("notifications listed")
    .into_iter()
    .map(|(kind, recipient, amount)| (kind, recipient, amount.unwrap_or(json!(null))))
    .collect()
}

// TS case: "applies deterministic proxy bidding and reserve status"
#[sqlx::test]
async fn applies_deterministic_proxy_bidding_and_reserve_status(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;

    let (status, first) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first bid failed: {first}");
    let auction = &first["result"]["listing"]["auction"];
    assert_eq!(first["result"]["kind"], json!("bid"));
    assert_eq!(auction["current_price"]["amount_minor"], json!(4_500));
    assert_eq!(auction["leader_pubky"], json!(buyer.pubky));
    assert_eq!(auction["reserve_met"], json!(false));
    assert_eq!(auction["bid_count"], json!(1));

    let (status, second) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second bid failed: {second}");
    assert_eq!(second["revision"], json!(3));
    let auction = &second["result"]["listing"]["auction"];
    assert_eq!(auction["current_price"]["amount_minor"], json!(8_500));
    assert_eq!(auction["leader_pubky"], json!(buyer.pubky));
    assert_eq!(auction["reserve_met"], json!(true));
    assert_eq!(auction["bid_count"], json!(2));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 2);
}

#[sqlx::test]
async fn bid_history_shows_the_visible_price_progression_and_never_the_maximums(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let observer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;

    // Any authenticated user — not only participants — audits the history.
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{}/bids", listing_aggregate(&seller.pubky)),
        Some(&observer.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let bids = body["bids"].as_array().expect("bids array");
    assert_eq!(bids.len(), 2);
    // The visible price progression: the first bid opens at the start price,
    // the second (a losing 8k proxy against a 10k proxy) pushes the visible
    // price to runner-up + increment.
    assert_eq!(bids[0]["sequence"], json!(1));
    assert_eq!(bids[0]["bidder_pubky"], json!(buyer.pubky));
    assert_eq!(bids[0]["visible_amount"]["amount_minor"], json!(4_500));
    assert_eq!(bids[1]["sequence"], json!(2));
    assert_eq!(bids[1]["bidder_pubky"], json!(other_buyer.pubky));
    assert_eq!(bids[1]["visible_amount"]["amount_minor"], json!(8_500));
    assert!(bids[0]["created_at"].is_string());
    // The secret proxy maximums (10_000 / 8_000) must never appear anywhere
    // in the response.
    let serialized = body.to_string();
    assert!(!serialized.contains("maximum"), "{serialized}");
    assert!(!serialized.contains("10000"), "{serialized}");
    assert!(!serialized.contains("8000"), "{serialized}");
    // The countdown correction and the live auction terms ride along.
    assert!(body["server_time"].is_string());
    assert_eq!(body["auction"]["bid_count"], json!(2));

    // Unauthenticated reads are refused: the audit audience is the same
    // audience that can bid.
    let (status, _) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{}/bids", listing_aggregate(&seller.pubky)),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// TS case: "uses first accepted sequence as the proxy-bid tie breaker"
#[sqlx::test]
async fn uses_first_accepted_sequence_as_proxy_bid_tie_breaker(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 2, 10_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tied bid failed: {body}");

    let (auction,): (serde_json::Value,) =
        sqlx::query_as("SELECT auction FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!(auction["current_price"]["amount_minor"], json!(10_000));
    assert_eq!(auction["leader_pubky"], json!(buyer.pubky));
}

// TS case: "rejects seller, low, stale, and post-close bids"
#[sqlx::test]
async fn rejects_seller_low_stale_and_post_close_bids(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("A seller cannot bid on their own auction.")
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 2, 4_500, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("BID_TOO_LOW"));
    assert_eq!(
        body["error"]["message"],
        json!("Bid maximum must exceed the current visible price.")
    );

    let (status, _) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 3, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 4, 11_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(2));

    // A proxy maximum not exceeding the same bidder's previous maximum is
    // rejected even when it clears the visible price.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 6, 9_000, 2),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "same-bidder lower max: {body}"
    );
    assert_eq!(body["error"]["code"], json!("BID_TOO_LOW"));
    assert_eq!(
        body["error"]["message"],
        json!("A new proxy maximum must exceed the bidder previous maximum.")
    );

    app.clock.advance_seconds(11 * 60);
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 5, 11_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AUCTION_CLOSED"));
}

// The prototype rejects bids on non-auction listings as INVALID_STATE.
#[sqlx::test]
async fn rejects_bids_on_fixed_price_listings(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 1, 20_000, 1),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("This listing is not an auction.")
    );
}

// TS case: "extends an auction when a valid bid lands inside the anti-sniping window"
#[sqlx::test]
async fn extends_auction_when_bid_lands_inside_anti_sniping_window(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    app.clock.advance_seconds(9 * 60 + 30);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sniping-window bid failed: {body}");

    // 22:09:30 + the 120-second extension.
    assert_eq!(
        body["result"]["listing"]["auction"]["ends_at"],
        json!("2026-08-19T22:11:30.000Z")
    );
}

// TS case: "closes a reserve-met auction with one winner and reservation".
// The Rust service also creates exactly one winning order + sandbox payment.
#[sqlx::test]
async fn closes_reserve_met_auction_with_one_winner_and_reservation(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 20, 10_000, 1),
    )
    .await;
    execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 21, 8_000, 2),
    )
    .await;
    app.clock.advance_seconds(11 * 60);

    // Only the seller may close.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &close_auction_command(&seller.pubky, 3, 949),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "non-seller close: {body}");

    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 950),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {body}");
    assert_eq!(body["revision"], json!(4));
    assert_eq!(body["result"]["kind"], json!("auction_result"));
    assert_eq!(body["result"]["outcome"], json!("sold"));
    assert_eq!(body["result"]["winner_pubky"], json!(buyer.pubky));
    assert_eq!(body["result"]["listing"]["state"], json!("reserved"));
    assert_eq!(
        body["result"]["listing"]["auction"]["status"],
        json!("sold")
    );
    assert_eq!(
        body["result"]["reservation"]["buyer_pubky"],
        json!(buyer.pubky)
    );
    assert_eq!(body["result"]["reservation"]["quantity"], json!(1));
    // The winning order snapshots the final visible price (8_500) plus the
    // seller-signed shipping, nothing else added.
    let order = &body["result"]["order"];
    assert_eq!(order["buyer_pubky"], json!(buyer.pubky));
    assert_eq!(order["seller_pubky"], json!(seller.pubky));
    assert_eq!(order["state"], json!("pending_payment"));
    assert_eq!(order["subtotal"]["amount_minor"], json!(8_500));
    assert_eq!(order["shipping"]["amount_minor"], json!(1_200));
    assert_eq!(order["total"]["amount_minor"], json!(8_500 + 1_200));
    assert_eq!(
        body["result"]["payment"]["state"],
        json!("awaiting_entitlement")
    );

    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE auction_aggregate_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.auction_won'"
        )
        .await,
        1
    );

    // A second close is rejected: the auction is no longer active.
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 4, 951),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The auction is not active.")
    );
}

// TS case: "closes an auction without a reserve-met leader as unsold"
#[sqlx::test]
async fn closes_auction_without_reserve_met_leader_as_unsold(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    app.clock.advance_seconds(11 * 60);

    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 1, 952),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unsold close failed: {body}");
    assert_eq!(body["result"]["outcome"], json!("unsold"));
    assert_eq!(body["result"]["winner_pubky"], json!(null));
    assert_eq!(body["result"]["listing"]["state"], json!("available"));
    assert_eq!(
        body["result"]["listing"]["auction"]["status"],
        json!("unsold")
    );
    assert_eq!(body["result"]["reservation"], json!(null));
    assert_eq!(body["result"]["order"], json!(null));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM reservations").await,
        0
    );
}

// Prototype closeAuction: "The auction has not ended yet."
#[sqlx::test]
async fn rejects_closing_an_auction_before_its_end_time(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 1, 953),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AUCTION_CLOSED"));
    assert_eq!(
        body["error"]["message"],
        json!("The auction has not ended yet.")
    );
}

// Service gap closed: every distinct bidder (not just the winner and the
// displaced leader) learns the auction closed, with the closing visible
// price riding the payload per ADR-0019 §8 (the figure is already on the
// listing projection every bidder reads).
#[sqlx::test]
async fn close_notifies_every_bidder_except_the_winner(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    let runner_up = new_actor(&app).await;
    let early_bidder = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &early_bidder.token,
        &place_bid_command(&seller.pubky, 30, 7_000, 1),
    )
    .await;
    execute(
        &app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 31, 8_000, 2),
    )
    .await;
    let (status, body) = execute(
        &app,
        &winner.token,
        &place_bid_command(&seller.pubky, 32, 10_000, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "third bid failed: {body}");
    app.clock.advance_seconds(11 * 60);

    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 4, 954),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {body}");
    assert_eq!(body["result"]["outcome"], json!("sold"));

    // Final visible price: winner max 10_000 capped at runner-up 8_000 +
    // 500 increment = 8_500.
    let final_price = json!({ "amount_minor": 8_500, "currency": "USD", "exponent": 2 });
    let mut expected = vec![
        (
            "auction_ended".to_string(),
            early_bidder.pubky.clone(),
            final_price.clone(),
        ),
        (
            "auction_ended".to_string(),
            runner_up.pubky.clone(),
            final_price.clone(),
        ),
        (
            "auction_won".to_string(),
            winner.pubky.clone(),
            final_price.clone(),
        ),
        (
            "outbid".to_string(),
            early_bidder.pubky.clone(),
            json!({ "amount_minor": 7_500, "currency": "USD", "exponent": 2 }),
        ),
        (
            "outbid".to_string(),
            runner_up.pubky.clone(),
            final_price.clone(),
        ),
    ];
    expected.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let mut delivered = delivered_notifications(&app).await;
    delivered.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    assert_eq!(delivered, expected);

    // Every close notification references the auction listing aggregate.
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM notifications WHERE type IN ('auction_ended', 'auction_won') \
             AND aggregate_id LIKE 'listing:%'"
        )
        .await,
        3
    );
}

// An unsold close (reserve never met) still tells the bidders the auction
// is over — nobody "wins" silence.
#[sqlx::test]
async fn unsold_close_notifies_the_bidders(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    // Below the 6_000 reserve: the bid leads but never satisfies it.
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 33, 5_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid failed: {body}");
    app.clock.advance_seconds(11 * 60);

    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 2, 955),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {body}");
    assert_eq!(body["result"]["outcome"], json!("unsold"));

    let delivered = delivered_notifications(&app).await;
    assert_eq!(
        delivered,
        vec![(
            "auction_ended".to_string(),
            bidder.pubky.clone(),
            // A sole bid keeps the visible price at the starting price.
            json!({ "amount_minor": 4_500, "currency": "USD", "exponent": 2 }),
        )]
    );
}
