//! Order cancellation tests: the prototype's unpaid-checkout cancel ported
//! by case name, plus the durable-service proofs — the paid-order
//! request/approve flow that keeps the confirmed payment intact, role and
//! participation refusals, idempotent replays that release inventory
//! exactly once, quantity-balance assertions, terminal-state refusals, and
//! the auction winner's hold released through the reservation
//! compare-and-swap (including the lapsed-hold case).

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, count, create_paid_order, execute, listing_aggregate, new_actor,
    order_command, payment_command, place_bid_command, register_auction_command, register_command,
    send, test_app, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::expiry::expire_due_reservations;
use marketplace_service::workers::drain_outbox;

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

/// The listing's quantity ledger: (available, reserved, sold, state). The
/// `listings_quantity_balance` CHECK already forces the three quantities to
/// sum to the total on every write; the tests assert the exact split.
async fn listing_quantities(app: &TestApp, seller_pubky: &str) -> (i64, i64, i64, String) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller_pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists")
}

async fn notification_types(app: &TestApp, token: &str) -> Vec<String> {
    drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (status, body) = get(app, "/v1/notifications", token).await;
    assert_eq!(status, StatusCode::OK);
    body["notifications"]
        .as_array()
        .expect("notifications array")
        .iter()
        .filter_map(|n| n["type"].as_str().map(str::to_string))
        .collect()
}

// TS case: "cancels unpaid checkout immediately and releases reserved
// inventory once" — the replay proves the "once": the stored result comes
// back without re-executing the release.
#[sqlx::test]
async fn cancels_unpaid_checkout_immediately_and_releases_reserved_inventory_once(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (_, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    let cancel = order_command(
        "order.cancel_request",
        &order_id,
        1,
        json!({ "reason": "Changed mind" }),
        1_220,
    );
    let (status, cancelled) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {cancelled}");
    assert_eq!(cancelled["result"]["kind"], json!("order"));
    assert_eq!(cancelled["result"]["order"]["state"], json!("cancelled"));
    assert_eq!(cancelled["result"]["order"]["revision"], json!(2));
    assert_eq!(
        cancelled["result"]["order"]["cancellation_reason"],
        json!("Changed mind")
    );
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancelled'"
        )
        .await,
        1
    );

    // Exact replay returns the stored result without releasing again.
    let (status, replayed) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "replay failed: {replayed}");
    assert_eq!(replayed, cancelled);
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancelled'"
        )
        .await,
        1
    );

    // The seller learns about the cancellation through the outbox.
    let types = notification_types(&app, &seller.token).await;
    assert!(types.contains(&"order_cancelled".to_string()), "{types:?}");
}

// New in the Rust service: the paid-order flow. The buyer's request parks
// the order in cancel_requested; the seller's approval cancels it and
// returns the sold quantities to available (the listing machine's
// sold -> available transition). The confirmed payment and its receipt are
// never discarded — the money path out is refund.record_external.
#[sqlx::test]
async fn approves_cancellation_of_a_paid_order_without_discarding_the_payment(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );

    let (status, requested) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            2,
            json!({ "reason": "No longer needed" }),
            1_400,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "request failed: {requested}");
    assert_eq!(
        requested["result"]["order"]["state"],
        json!("cancel_requested")
    );
    assert_eq!(requested["result"]["order"]["revision"], json!(3));
    // The request alone releases nothing: the seller has not agreed yet.
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancel_requested'"
        )
        .await,
        1
    );

    // A cancel-requested order is no longer shippable.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            3,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-CANCEL" }),
            1_401,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    let approve = order_command("order.cancel_approve", order_id, 3, json!({}), 1_402);
    let (status, approved) = execute(&app, &seller.token, &approve).await;
    assert_eq!(status, StatusCode::OK, "approve failed: {approved}");
    assert_eq!(approved["result"]["order"]["state"], json!("cancelled"));
    assert_eq!(approved["result"]["order"]["revision"], json!(4));
    assert_eq!(
        approved["result"]["order"]["cancellation_reason"],
        json!("No longer needed")
    );
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );

    // Replaying the approval returns the stored result without releasing
    // the same quantities twice.
    let (status, replayed) = execute(&app, &seller.token, &approve).await;
    assert_eq!(status, StatusCode::OK, "replay failed: {replayed}");
    assert_eq!(replayed, approved);
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancelled'"
        )
        .await,
        1
    );

    // The confirmed payment and its receipt survive the cancellation.
    let (payment_state,): (String,) = sqlx::query_as("SELECT state FROM payments WHERE id = $1")
        .bind(order.payment_id.parse::<uuid::Uuid>().expect("payment id"))
        .fetch_one(&app.pool)
        .await
        .expect("payment row exists");
    assert_eq!(payment_state, "confirmed");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 1);

    // Both parties were notified through the outbox: the seller of the
    // request, the buyer of the approval.
    let seller_types = notification_types(&app, &seller.token).await;
    assert!(
        seller_types.contains(&"order_cancelled".to_string()),
        "{seller_types:?}"
    );
    let buyer_types = notification_types(&app, &buyer.token).await;
    assert!(
        buyer_types.contains(&"order_cancelled".to_string()),
        "{buyer_types:?}"
    );

    // The refund-after-cancellation path is reachable: the seller records
    // independently evidenced external refund from the cancelled state.
    let (status, refunded) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            4,
            json!({
                "amount_minor": order.total_minor,
                "transaction_id": "bitcoin-tx-evidence-123",
            }),
            1_403,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund failed: {refunded}");
    assert_eq!(
        refunded["result"]["order"]["state"],
        json!("refunded_external")
    );
}

// New in the Rust service: role scoping. Only the buyer requests, only the
// seller approves, and a non-participant is refused outright (403), never
// handed an empty or not-found answer that hides the refusal.
#[sqlx::test]
async fn enforces_cancellation_roles_and_refuses_non_participants(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    let request = |number: u64| {
        order_command(
            "order.cancel_request",
            order_id,
            2,
            json!({ "reason": "Changed mind" }),
            number,
        )
    };
    for (actor, command) in [
        (&seller, request(1_410)),
        (&stranger, request(1_411)),
        (
            &stranger,
            order_command("order.cancel_approve", order_id, 2, json!({}), 1_412),
        ),
    ] {
        let (status, body) = execute(&app, &actor.token, &command).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    }

    let (status, body) = execute(&app, &buyer.token, &request(1_413)).await;
    assert_eq!(status, StatusCode::OK, "request failed: {body}");

    // The requester cannot approve their own cancellation.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command("order.cancel_approve", order_id, 3, json!({}), 1_414),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));

    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("order.cancel_approve", order_id, 3, json!({}), 1_415),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "approve failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));
}

// New in the Rust service: ineligible states and stale revisions. A shipped
// order can no longer be cancelled, approval without a pending request is
// refused, and a stale cancel conflicts with the current revision.
#[sqlx::test]
async fn refuses_cancellation_in_ineligible_states_and_on_stale_revisions(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    // Approval with no cancellation pending is refused.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("order.cancel_approve", order_id, 2, json!({}), 1_420),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // A stale cancel conflicts and reports the current revision.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            1,
            json!({ "reason": "Stale attempt" }),
            1_421,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(2));

    // Once shipped, the order can no longer be cancelled.
    execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-SHIPPED" }),
            1_422,
        ),
    )
    .await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            3,
            json!({ "reason": "Too late" }),
            1_423,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    // Nothing was released by the refused attempts.
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
}

// New in the Rust service: cancellation is terminal for the purchase. A
// cancelled order refuses payment confirmation, fulfillment, returns, and a
// second cancellation.
#[sqlx::test]
async fn cancelled_order_refuses_payment_fulfillment_and_return_commands(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (_, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Changed mind" }),
            1_430,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");

    // Confirming the payment would re-sell released inventory; the order
    // machine has no cancelled -> paid transition and the command is
    // refused.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_431),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    for (actor, command) in [
        (
            &seller,
            order_command(
                "fulfillment.ship",
                &order_id,
                2,
                json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-VOID" }),
                1_432,
            ),
        ),
        (
            &buyer,
            order_command(
                "return.request",
                &order_id,
                2,
                json!({ "reason": "Never arrived", "requested_amount_minor": 100 }),
                1_433,
            ),
        ),
        (
            &buyer,
            order_command(
                "order.cancel_request",
                &order_id,
                2,
                json!({ "reason": "Cancel again" }),
                1_434,
            ),
        ),
        (
            &seller,
            order_command("order.cancel_approve", &order_id, 2, json!({}), 1_435),
        ),
    ] {
        let (status, body) = execute(&app, &actor.token, &command).await;
        assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    }

    // The released unit was never re-committed by the refused commands.
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
}

// New in the Rust service: an auction winner's unpaid order releases the
// winning hold through the reservation compare-and-swap.
#[sqlx::test]
async fn cancels_an_auction_winners_unpaid_order_and_releases_the_hold(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let runner_up = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    execute(
        &app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        &app,
        &seller.token,
        &common::close_auction_command(&seller.pubky, 3, 952),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {closed}");
    let order_id = closed["result"]["order"]["id"]
        .as_str()
        .expect("winning order present")
        .to_string();
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    let (status, cancelled) = execute(
        &app,
        &bidder.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Bid regret" }),
            1_440,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {cancelled}");
    assert_eq!(cancelled["result"]["order"]["state"], json!("cancelled"));
    let (reservation_status,): (String,) =
        sqlx::query_as("SELECT status FROM reservations LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(reservation_status, "released");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
}

// New in the Rust service: when the winner's 30-minute hold has already
// lapsed on server time, the expiry sweep returned the unit — the cancel
// still succeeds but must not release the same unit twice.
#[sqlx::test]
async fn cancelling_after_the_hold_lapsed_does_not_release_twice(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let runner_up = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    execute(
        &app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        &app,
        &seller.token,
        &common::close_auction_command(&seller.pubky, 3, 953),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {closed}");
    let order_id = closed["result"]["order"]["id"]
        .as_str()
        .expect("winning order present")
        .to_string();

    app.clock.advance_seconds(31 * 60);
    let expired = expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("expiry sweep runs");
    assert_eq!(expired, 1);
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );

    let (status, cancelled) = execute(
        &app,
        &bidder.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Hold lapsed anyway" }),
            1_441,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {cancelled}");
    assert_eq!(cancelled["result"]["order"]["state"], json!("cancelled"));
    let (reservation_status,): (String,) =
        sqlx::query_as("SELECT status FROM reservations LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(reservation_status, "expired");
    // Still exactly one unit: the lapsed hold was not released twice.
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
}
