//! Post-purchase lifecycle tests: sandbox payment advancement with receipt
//! issue, fulfillment, returns, externally evidenced refunds, and reviews —
//! ported from the TypeScript prototype suite by case name, plus the
//! durable-service proofs (participant refusals, idempotent replays,
//! constraint-enforced review uniqueness).

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, count, create_paid_order, execute, listing_aggregate, new_actor,
    order_command, payment_command, place_bid_command, register_auction_command,
    register_command, send, test_app, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::expiry::expire_due_reservations;
use marketplace_service::workers::drain_outbox;

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

// TS case: "advances sandbox payment through detection to confirmation and
// issues a receipt".
#[sqlx::test]
async fn advances_sandbox_payment_through_detection_to_confirmation_and_issues_receipt(
    pool: PgPool,
) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (_, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();

    let (status, detected) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "detected", 0, 1_001),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "detection failed: {detected}");
    assert_eq!(detected["result"]["kind"], json!("payment"));
    assert_eq!(detected["result"]["payment"]["state"], json!("detected"));
    assert_eq!(detected["result"]["payment"]["revision"], json!(2));
    assert_eq!(detected["result"]["receipt"], Value::Null);
    assert_eq!(
        detected["result"]["order"]["state"],
        json!("pending_payment")
    );

    let (status, confirmed) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 2, "confirmed", 1, 1_002),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirmation failed: {confirmed}");
    assert_eq!(confirmed["result"]["payment"]["state"], json!("confirmed"));
    assert_eq!(confirmed["result"]["payment"]["confirmations"], json!(1));
    assert_eq!(confirmed["result"]["payment"]["revision"], json!(3));
    assert_eq!(confirmed["result"]["order"]["state"], json!("paid"));
    assert_eq!(confirmed["result"]["order"]["revision"], json!(2));
    let receipt = &confirmed["result"]["receipt"];
    let receipt_id = receipt["id"].as_str().expect("receipt id present");
    assert_eq!(confirmed["result"]["order"]["receipt_id"], receipt["id"]);
    assert_eq!(receipt["total"]["amount_minor"], json!(13_700));
    assert_eq!(receipt["issuer_pubky"], json!(seller.pubky));
    assert_eq!(receipt["recipient_pubky"], json!(buyer.pubky));
    let content_hash = receipt["content_hash"].as_str().expect("hash present");
    assert_eq!(content_hash.len(), 64);
    assert!(content_hash.chars().all(|c| c.is_ascii_hexdigit()));

    // Both participants read the receipt; a stranger's fetch is refused
    // without revealing that the receipt exists.
    for participant in [&buyer, &seller] {
        let (status, fetched) = get(
            &app,
            &format!("/v1/receipts/{receipt_id}"),
            &participant.token,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "receipt fetch failed: {fetched}");
        assert_eq!(fetched, *receipt);
    }
    let (status, body) = get(&app, &format!("/v1/receipts/{receipt_id}"), &stranger.token).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");

    // Confirmation converted the held inventory from reserved to sold, the
    // reserved -> sold transition the contract declares for
    // payment_confirmation.
    let (available, reserved, sold, state): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (available, reserved, sold, state.as_str()),
        (0, 0, 1, "sold")
    );

    // The seller is notified through the outbox, and the confirmed event is
    // unique per payment (events_one_payment_confirmed).
    drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (status, body) = get(&app, "/v1/notifications", &seller.token).await;
    assert_eq!(status, StatusCode::OK);
    let types: Vec<&str> = body["notifications"]
        .as_array()
        .expect("notifications array")
        .iter()
        .filter_map(|n| n["type"].as_str())
        .collect();
    assert!(types.contains(&"payment_confirmed"), "got: {types:?}");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'receipt.issued'"
        )
        .await,
        1
    );
}

// TS case: "rejects duplicate checkout lines, stale stock, self-purchase,
// and invalid payment transitions" — the payment-transition half, now that
// payment.sandbox_advance is ported (the checkout half lives in
// commands_test.rs).
#[sqlx::test]
async fn rejects_invalid_payment_transitions_and_non_buyer_advancement(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (_, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();

    // Confirmation requires at least one confirmation.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 0, 1_003),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    // Only the buyer may advance the sandbox payment — not another user,
    // and not even the seller.
    for outsider in [&other_buyer, &seller] {
        let (status, body) = execute(
            &app,
            &outsider.token,
            &payment_command(&payment_id, 1, "detected", 0, 1_004),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    }

    // A stale revision conflicts, and a terminal payment accepts nothing.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "expired", 0, 1_005),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expiry failed: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_006),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(2));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 2, "confirmed", 1, 1_007),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// TS case: "ships, confirms delivery, and allows one review per participant".
#[sqlx::test]
async fn ships_confirms_delivery_and_allows_one_review_per_participant(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    let (status, shipped) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-123" }),
            1_201,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship failed: {shipped}");
    assert_eq!(shipped["result"]["order"]["state"], json!("shipped"));
    assert_eq!(shipped["result"]["order"]["revision"], json!(3));
    // Tracking is participant-visible; the delivery address never appears
    // in an order-action result (ADR-0019 §8).
    assert_eq!(
        shipped["result"]["order"]["shipment"]["tracking_number"],
        json!("TRACK-123")
    );
    assert!(!shipped.to_string().contains("1 Market Street"));
    assert!(shipped["result"]["order"].get("delivery_address").is_none());

    let (status, delivered) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            order_id,
            3,
            json!({}),
            1_202,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery failed: {delivered}");
    assert_eq!(delivered["result"]["order"]["state"], json!("delivered"));
    assert_eq!(delivered["result"]["order"]["revision"], json!(4));
    assert_eq!(
        delivered["result"]["order"]["shipment"]["state"],
        json!("delivered")
    );
    assert!(delivered["result"]["order"]["shipment"]["delivered_at"].is_string());

    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            1_203,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    assert_eq!(reviewed["result"]["kind"], json!("review"));
    assert_eq!(reviewed["result"]["order"]["state"], json!("completed"));
    assert_eq!(
        reviewed["result"]["order"]["reviews"][0]["rating"],
        json!(5)
    );
    assert_eq!(
        reviewed["result"]["review"]["subject_pubky"],
        json!(seller.pubky)
    );

    let (status, seller_reviewed) = execute(
        &app,
        &seller.token,
        &order_command(
            "review.create",
            order_id,
            5,
            json!({ "rating": 5, "text": "Great buyer." }),
            1_204,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "seller review failed: {seller_reviewed}"
    );
    assert_eq!(seller_reviewed["result"]["order"]["revision"], json!(6));
    assert_eq!(
        seller_reviewed["result"]["order"]["reviews"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );

    let (status, duplicate) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            order_id,
            6,
            json!({ "rating": 4, "text": "Duplicate." }),
            1_205,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {duplicate}");
    assert_eq!(duplicate["error"]["code"], json!("INVALID_STATE"));

    // Fulfillment and review notifications went through the outbox.
    drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    for (kind, expected) in [
        ("notification.order_shipped", 1),
        ("notification.order_delivered", 1),
        ("notification.review_received", 2),
    ] {
        assert_eq!(
            count(
                &app.pool,
                &format!("SELECT COUNT(*) FROM outbox WHERE kind = '{kind}'")
            )
            .await,
            expected,
            "outbox intents for {kind}"
        );
    }
}

// TS case: "runs return approval, receipt, and externally verified refund
// without claiming custody".
#[sqlx::test]
async fn runs_return_approval_receipt_and_externally_verified_refund_without_claiming_custody(
    pool: PgPool,
) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-RETURN" }),
            1_210,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            order_id,
            3,
            json!({}),
            1_211,
        ),
    )
    .await;

    let (status, requested) = execute(
        &app,
        &buyer.token,
        &order_command(
            "return.request",
            order_id,
            4,
            json!({
                "reason": "Item differs from description",
                "requested_amount_minor": order.total_minor,
            }),
            1_212,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return request failed: {requested}");
    assert_eq!(
        requested["result"]["order"]["state"],
        json!("return_requested")
    );
    assert_eq!(requested["result"]["order"]["revision"], json!(5));
    assert_eq!(
        requested["result"]["order"]["return_request"]["state"],
        json!("requested")
    );

    let (status, approved) = execute(
        &app,
        &seller.token,
        &order_command("return.approve", order_id, 5, json!({}), 1_213),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "approval failed: {approved}");
    assert_eq!(
        approved["result"]["order"]["state"],
        json!("return_approved")
    );
    let (status, received) = execute(
        &app,
        &seller.token,
        &order_command("return.receive", order_id, 6, json!({}), 1_214),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "receive failed: {received}");
    assert_eq!(
        received["result"]["order"]["state"],
        json!("return_received")
    );

    let (status, refunded) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            7,
            json!({
                "amount_minor": order.total_minor,
                "transaction_id": "bitcoin-tx-evidence-123",
            }),
            1_215,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund record failed: {refunded}");
    let refunded_order = &refunded["result"]["order"];
    assert_eq!(refunded_order["state"], json!("refunded_external"));
    assert_eq!(
        refunded_order["external_refund"]["amount_minor"],
        json!(order.total_minor)
    );
    assert_eq!(
        refunded_order["external_refund"]["transaction_id"],
        json!("bitcoin-tx-evidence-123")
    );
    assert_eq!(refunded_order["return_request"]["state"], json!("refunded"));

    drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.return_updated'"
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.refund_recorded'"
        )
        .await,
        1
    );
}

// New in the Rust service: the refund record is refused without independent
// evidence, above the order value, and from ineligible states.
#[sqlx::test]
async fn refuses_external_refunds_without_independent_evidence(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    // Evidence shorter than the minimum is rejected by the contract before
    // any state is touched.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            2,
            json!({ "amount_minor": order.total_minor, "transaction_id": "short" }),
            1_216,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert!(body["error"]["issues"]
        .as_array()
        .expect("issues present")
        .iter()
        .any(|issue| issue["path"] == json!("payload.transaction_id")));

    // A paid order that has not gone through return receipt or cancellation
    // cannot be marked refunded, evidence or not.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            2,
            json!({
                "amount_minor": order.total_minor,
                "transaction_id": "bitcoin-tx-evidence-123",
            }),
            1_217,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE external_refund IS NOT NULL"
        )
        .await,
        0
    );
}

// New in the Rust service: every post-purchase command refuses a
// non-participant outright — a refusal, not an empty result — and the
// receipt endpoint hides foreign receipts.
#[sqlx::test]
async fn refuses_non_participants_on_every_post_purchase_command(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    let commands: Vec<(&str, Value)> = vec![
        (
            "fulfillment.ship",
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-999" }),
        ),
        ("fulfillment.confirm_delivery", json!({})),
        (
            "return.request",
            json!({ "reason": "Not as described", "requested_amount_minor": 100 }),
        ),
        ("return.approve", json!({})),
        ("return.receive", json!({})),
        (
            "refund.record_external",
            json!({ "amount_minor": 100, "transaction_id": "bitcoin-tx-evidence-789" }),
        ),
        (
            "review.create",
            json!({ "rating": 1, "text": "Unrelated party review" }),
        ),
        (
            "review.update",
            json!({ "rating": 1, "text": "Unrelated party edit" }),
        ),
    ];
    for (index, (kind, payload)) in commands.into_iter().enumerate() {
        let command = order_command(kind, order_id, 2, payload, 1_250 + index as u64);
        let (status, body) = execute(&app, &stranger.token, &command).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{kind} let a stranger in: {body}"
        );
        assert_eq!(
            body["error"]["code"],
            json!("UNAUTHORIZED"),
            "{kind}: {body}"
        );
    }
    let (status, body) = execute(
        &app,
        &stranger.token,
        &payment_command(&order.payment_id, 3, "manual_review", 0, 1_270),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));

    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}", order.receipt_id),
        &stranger.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");

    // Nothing a stranger sent changed the order.
    let (_, projected) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(projected["revision"], json!(2));
    assert_eq!(projected["state"], json!("paid"));
}

// New in the Rust service: exact replays of every post-purchase command
// return the stored result without re-executing (ADR-0019 §3).
#[sqlx::test]
async fn replays_each_post_purchase_command_idempotently(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (_, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();

    // Each step executes once, then replays byte-identically. The event
    // count proves the replay did not re-execute.
    let steps: Vec<(&str, Value)> = vec![
        (
            "buyer",
            payment_command(&payment_id, 1, "confirmed", 1, 1_280),
        ),
        (
            "seller",
            order_command(
                "fulfillment.ship",
                &order_id,
                2,
                json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-REPLAY" }),
                1_281,
            ),
        ),
        (
            "buyer",
            order_command(
                "fulfillment.confirm_delivery",
                &order_id,
                3,
                json!({}),
                1_282,
            ),
        ),
        (
            "buyer",
            order_command(
                "review.create",
                &order_id,
                4,
                json!({ "rating": 5, "text": "Accurate and fast." }),
                1_283,
            ),
        ),
        (
            "buyer",
            order_command(
                "review.update",
                &order_id,
                5,
                json!({ "rating": 4, "text": "Revised after wear." }),
                1_284,
            ),
        ),
        (
            "buyer",
            order_command(
                "return.request",
                &order_id,
                6,
                json!({ "reason": "Changed my mind", "requested_amount_minor": 100 }),
                1_285,
            ),
        ),
        (
            "seller",
            order_command("return.approve", &order_id, 7, json!({}), 1_286),
        ),
        (
            "seller",
            order_command("return.receive", &order_id, 8, json!({}), 1_287),
        ),
        (
            "seller",
            order_command(
                "refund.record_external",
                &order_id,
                9,
                json!({ "amount_minor": 100, "transaction_id": "bitcoin-tx-evidence-replay" }),
                1_288,
            ),
        ),
    ];
    for (role, command) in steps {
        let token = match role {
            "buyer" => &buyer.token,
            _ => &seller.token,
        };
        let (status, first) = execute(&app, token, &command).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} failed: {first}",
            command["kind"]
        );
        let events_before = count(&app.pool, "SELECT COUNT(*) FROM events").await;
        let (replay_status, replay) = execute(&app, token, &command).await;
        assert_eq!(replay_status, StatusCode::OK);
        assert_eq!(replay, first, "replay diverged for {}", command["kind"]);
        let events_after = count(&app.pool, "SELECT COUNT(*) FROM events").await;
        assert_eq!(
            events_before, events_after,
            "replay re-executed {}",
            command["kind"]
        );
    }
}

// New in the Rust service: review uniqueness is decided by the
// reviews_one_per_order_role constraint even when two same-role commands
// race — the competing uncommitted insert forces the handler through the
// constraint instead of its sequential pre-check.
#[sqlx::test]
async fn review_uniqueness_is_enforced_by_the_database_constraint_under_concurrency(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.clone();
    execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-RACE" }),
            1_300,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order_id,
            3,
            json!({}),
            1_301,
        ),
    )
    .await;

    // A racing command's uncommitted review insert, held open while the
    // real command executes through the full HTTP stack.
    let mut racing_tx = app.pool.begin().await.expect("competing tx begins");
    sqlx::query(
        "INSERT INTO reviews (id, order_id, reviewer_pubky, reviewer_role, subject_pubky, \
         rating, text, created_at, updated_at) \
         VALUES (gen_random_uuid(), $1::uuid, $2, 'buyer', $3, 5, 'Racing review.', now(), now())",
    )
    .bind(&order_id)
    .bind(&buyer.pubky)
    .bind(&seller.pubky)
    .execute(&mut *racing_tx)
    .await
    .expect("competing insert succeeds");

    let command = order_command(
        "review.create",
        &order_id,
        4,
        json!({ "rating": 5, "text": "Blocked by the race." }),
        1_302,
    );
    let router = app.router.clone();
    let token = buyer.token.clone();
    let handle = tokio::spawn(async move {
        common::send(router, "POST", "/v1/commands", Some(&token), &command).await
    });
    // Wait until the command's insert is provably blocked on the competing
    // uncommitted row before releasing it, so the constraint — not the
    // sequential pre-check — decides the race.
    let mut blocked = false;
    for _ in 0..200 {
        let waiting = count(
            &app.pool,
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND query LIKE 'INSERT INTO reviews%'",
        )
        .await;
        if waiting > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(blocked, "the racing insert never reached the unique index");
    racing_tx.commit().await.expect("competing tx commits");

    let (status, body) = handle.await.expect("command task completes");
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM reviews").await,
        1,
        "exactly one review row survives the race"
    );
}

// New in the Rust service (review.update has no prototype counterpart):
// the reviewer may revise within the bounded window, and only the reviewer.
#[sqlx::test]
async fn reviews_are_editable_by_their_author_within_the_bounded_window(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-EDIT" }),
            1_310,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            order_id,
            3,
            json!({}),
            1_311,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            1_312,
        ),
    )
    .await;

    // A participant who has not reviewed cannot edit.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "review.update",
            order_id,
            5,
            json!({ "rating": 1, "text": "Not my review." }),
            1_313,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");

    let (status, updated) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            order_id,
            5,
            json!({ "rating": 4, "text": "Revised after a week of wear." }),
            1_314,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "edit failed: {updated}");
    assert_eq!(updated["result"]["review"]["rating"], json!(4));
    assert_eq!(updated["result"]["order"]["revision"], json!(6));
    assert_eq!(updated["result"]["order"]["reviews"][0]["rating"], json!(4));

    // Past the 24-hour window the review is frozen.
    app.clock.advance_seconds(24 * 60 * 60 + 1);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            order_id,
            6,
            json!({ "rating": 1, "text": "Too late to change." }),
            1_315,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// New in the Rust service: confirming an auction winner's payment converts
// the winning reservation and sells the held unit; once the hold lapses on
// server time, confirmation is refused instead of overselling.
#[sqlx::test]
async fn auction_payment_confirmation_converts_the_winning_reservation(pool: PgPool) {
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
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid failed: {body}");
    // A runner-up lifts the visible price past the reserve so the close
    // sells (the prototype's reserve-met fixture).
    let (status, body) = execute(
        &app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second bid failed: {body}");
    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        &app,
        &seller.token,
        &common::close_auction_command(&seller.pubky, 3, 950),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {closed}");
    // The close result is redacted like every other response: no Locks
    // bundle id, no delivery address key.
    assert!(closed["result"]["payment"].get("locks_bundle_id").is_none());
    assert!(closed["result"]["order"].get("delivery_address").is_none());
    let payment_id = closed["result"]["payment"]["id"]
        .as_str()
        .expect("winning payment present")
        .to_string();

    let (status, confirmed) = execute(
        &app,
        &bidder.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_320),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirmation failed: {confirmed}");
    assert_eq!(confirmed["result"]["order"]["state"], json!("paid"));

    let (reservation_status,): (String,) =
        sqlx::query_as("SELECT status FROM reservations LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(reservation_status, "converted");
    let (available, reserved, sold, state): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (available, reserved, sold, state.as_str()),
        (0, 0, 1, "sold")
    );
}

#[sqlx::test]
async fn refuses_payment_confirmation_after_the_winning_hold_lapses(pool: PgPool) {
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
        &common::close_auction_command(&seller.pubky, 3, 951),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {closed}");
    let payment_id = closed["result"]["payment"]["id"]
        .as_str()
        .expect("winning payment present")
        .to_string();

    // The winner's 30-minute hold expires on server time and the worker
    // releases the unit; a late confirmation must not re-sell it.
    app.clock.advance_seconds(31 * 60);
    let expired = expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("expiry sweep runs");
    assert_eq!(expired, 1);
    let (status, body) = execute(
        &app,
        &bidder.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_321),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    let (available, reserved, sold): (i64, i64, i64) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!((available, reserved, sold), (1, 0, 0));
}

// New in the Rust service: the order read projections now carry the
// post-purchase sub-objects for participants, matching the client's
// orderSchema field names, still without the delivery address.
#[sqlx::test]
async fn order_projections_carry_post_purchase_sub_objects_for_participants(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-PROJ" }),
            1_330,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            order_id,
            3,
            json!({}),
            1_331,
        ),
    )
    .await;
    execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            1_332,
        ),
    )
    .await;

    for participant in [&buyer, &seller] {
        let (status, projected) =
            get(&app, &format!("/v1/orders/{order_id}"), &participant.token).await;
        assert_eq!(status, StatusCode::OK, "projection failed: {projected}");
        assert_eq!(projected["state"], json!("completed"));
        assert_eq!(projected["receipt_id"], json!(order.receipt_id));
        assert_eq!(projected["shipment"]["carrier"], json!("Sandbox Post"));
        assert_eq!(
            projected["shipment"]["tracking_number"],
            json!("TRACK-PROJ")
        );
        assert_eq!(projected["shipment"]["state"], json!("delivered"));
        assert_eq!(projected["return_request"], Value::Null);
        assert_eq!(projected["external_refund"], Value::Null);
        assert_eq!(projected["reviews"][0]["rating"], json!(5));
        assert_eq!(
            projected["reviews"][0]["reviewer_pubky"],
            json!(buyer.pubky)
        );
        assert!(projected.get("delivery_address").is_none());
        assert!(!projected.to_string().contains("1 Market Street"));
    }

    // The list projection carries the same sub-objects.
    let (status, listed) = get(&app, "/v1/orders", &buyer.token).await;
    assert_eq!(status, StatusCode::OK);
    let listed_order = &listed["orders"][0];
    assert_eq!(
        listed_order["shipment"]["tracking_number"],
        json!("TRACK-PROJ")
    );
    assert_eq!(listed_order["reviews"].as_array().map(Vec::len), Some(1));
}
