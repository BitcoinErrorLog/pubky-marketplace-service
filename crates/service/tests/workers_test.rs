//! Background worker tests (plan task 3.4): server-time expiry for
//! reservations and offers, authoritative auction close, lease exclusion
//! between instances, crash-mid-lease recovery, and at-least-once outbox
//! delivery with consumer-side dedup by event id.

mod common;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use marketplace_service::workers::{
    self, claim_outbox_batch, drain_outbox, run_once, try_acquire_lease,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    checkout_command, count, create_offer_command, execute, listing_aggregate, new_actor,
    place_bid_command, register_auction_command, register_command, reserve_command, test_app,
};

// Reservation TTL 600 s, offer TTL 3600 s, auction ends at +600 s — all on
// server time, drained by one worker pass per deadline.
#[sqlx::test]
async fn worker_expires_reservations_and_offers_and_closes_auctions_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let auction_seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    execute(
        &app,
        &auction_seller.token,
        &register_auction_command(&auction_seller.pubky),
    )
    .await;
    let (status, _) = execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::OK);
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["expected_revision"] = json!(2);
    let (status, _) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK);
    // Two bids so the runner-up lifts the visible price over the reserve.
    let (status, _) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&auction_seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&auction_seller.pubky, 2, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Nothing is due yet on server time.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.reservations_expired, 0);
    assert_eq!(summary.offers_expired, 0);
    assert_eq!(summary.auctions_closed, 0);

    // +601 s: the reservation (600 s TTL) and the auction (ends at +600 s)
    // are due; the offer (3600 s) is not.
    app.clock.advance_seconds(601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.reservations_expired, 1);
    assert_eq!(summary.auctions_closed, 1);
    assert_eq!(summary.offers_expired, 0);

    let (reservation_status,): (String,) =
        sqlx::query_as("SELECT status FROM reservations WHERE quantity = 1 AND buyer_pubky = $1 AND listing_aggregate_id = $2")
            .bind(&buyer.pubky)
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(reservation_status, "expired");
    let (auction,): (serde_json::Value,) =
        sqlx::query_as("SELECT auction FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&auction_seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("auction listing exists");
    assert_eq!(auction["status"], json!("sold"));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE auction_aggregate_id IS NOT NULL"
        )
        .await,
        1,
        "the server-time close created the winning order"
    );

    // +3601 s: the offer is now due, along with the auction winner's lapsed
    // 30-minute hold created by the close; the earlier work does not repeat.
    app.clock.advance_seconds(3_000);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.offers_expired, 1);
    assert_eq!(
        summary.reservations_expired, 1,
        "the auction winner's 30-minute hold lapsed"
    );
    assert_eq!(summary.auctions_closed, 0);

    let (offer_state, offer_revision): (String, i64) =
        sqlx::query_as("SELECT state, revision FROM offers LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("offer row exists");
    assert_eq!(offer_state, "expired");
    assert_eq!(offer_revision, 2);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'offer.expired'"
        )
        .await,
        1
    );
}

// Two instances cannot hold the same task lease at the same time; the lease
// is recoverable after it lapses.
#[sqlx::test]
async fn leases_exclude_concurrent_instances_and_lapse_over_time(pool: PgPool) {
    let app = test_app(pool).await;
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let now = app.clock.now();

    assert!(
        try_acquire_lease(&app.pool, workers::TASK_OUTBOX, instance_a, now, 30)
            .await
            .expect("lease query runs"),
        "a free lease is acquired"
    );
    assert!(
        !try_acquire_lease(&app.pool, workers::TASK_OUTBOX, instance_b, now, 30)
            .await
            .expect("lease query runs"),
        "a held lease excludes another instance"
    );
    assert!(
        try_acquire_lease(&app.pool, workers::TASK_OUTBOX, instance_a, now, 30)
            .await
            .expect("lease query runs"),
        "the holder may renew its own lease"
    );

    // A run_once by the excluded instance skips the outbox task entirely.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let summary = run_once(&app.state, instance_b, now)
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.outbox_delivered, 0,
        "excluded instance skips the task"
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        0
    );

    // After the lease lapses, instance B takes over and drains it.
    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, instance_b, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.outbox_delivered, 1);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        1
    );
}

// A worker that claims outbox rows and dies mid-lease loses nothing: the
// rows are redelivered by another instance after the claim lapses, exactly
// once in effect.
#[sqlx::test]
async fn crashed_outbox_claim_is_recovered_without_loss_or_duplication(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (status, _) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM outbox").await, 1);

    // Instance A claims the batch (stamping the row lease), then "crashes"
    // before delivering anything.
    let claimed = claim_outbox_batch(&app.pool, app.clock.now(), 30)
        .await
        .expect("claim runs");
    assert_eq!(claimed.len(), 1);

    // While the claim lease is live, another drain must not steal the row.
    let delivered = drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(delivered, 0, "a live claim excludes other deliverers");
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        0
    );

    // After the claim lapses, the row is recovered and delivered once.
    app.clock.advance_seconds(31);
    let delivered = drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(delivered, 1, "the crashed claim is recovered");
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL"
        )
        .await,
        0
    );
}

// At-least-once delivery: a redelivered intent (lost acknowledgement) marks
// the outbox row again but cannot duplicate the notification, which dedups
// by (event id, recipient).
#[sqlx::test]
async fn outbox_redelivery_does_not_duplicate_notification_effects(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;

    let delivered = drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(delivered, 1);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        1
    );

    // Simulate a lost delivery mark: the intent becomes due again, as under
    // at-least-once semantics.
    sqlx::query("UPDATE outbox SET delivered_at = NULL, lease_until = NULL")
        .execute(&app.pool)
        .await
        .expect("reset runs");
    let redelivered = drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(redelivered, 1, "the intent is delivered again");

    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM notifications").await,
        1,
        "consumer-side dedup by event id keeps exactly one notification"
    );
    let (notification_type, recipient): (String, String) =
        sqlx::query_as("SELECT type, recipient_pubky FROM notifications LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("notification row exists");
    assert_eq!(notification_type, "order_created");
    assert_eq!(recipient, seller.pubky);
}

// Offer and bid commands write their notification intents through the same
// outbox, so one worker pass delivers them all.
#[sqlx::test]
async fn worker_delivers_offer_and_auction_notifications(pool: PgPool) {
    let app = test_app(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["expected_revision"] = json!(1);
    let (status, _) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK);
    execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 10, 10_000, 1),
    )
    .await;
    let (status, _) = execute(
        &app,
        &other_buyer.token,
        &place_bid_command(&seller.pubky, 11, 12_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    // offer_received (seller) + outbid (first bidder).
    assert_eq!(summary.outbox_delivered, 2);

    // Both notifications carry monetary context the recipient already sees
    // in projections: the offer amount, and the visible price the displaced
    // leader must now beat (12_000 capped to 10_000 + 500 increment).
    let types: Vec<(String, String, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT type, recipient_pubky, amount FROM notifications ORDER BY type")
            .fetch_all(&app.pool)
            .await
            .expect("notifications listed");
    assert_eq!(
        types,
        vec![
            (
                "offer_received".to_string(),
                seller.pubky.clone(),
                Some(json!({ "amount_minor": 10_000, "currency": "USD", "exponent": 2 })),
            ),
            (
                "outbid".to_string(),
                buyer.pubky.clone(),
                Some(json!({ "amount_minor": 10_500, "currency": "USD", "exponent": 2 })),
            ),
        ]
    );
}
