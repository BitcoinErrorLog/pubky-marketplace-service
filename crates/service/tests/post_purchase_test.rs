//! Post-purchase lifecycle tests: sandbox payment advancement with receipt
//! issue, fulfillment, returns, disputes, externally evidenced refunds, and
//! reviews — ported from the TypeScript prototype suite by case name, plus
//! the durable-service proofs (participant refusals, idempotent replays,
//! constraint-enforced review uniqueness, evidence redaction).

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, count, create_paid_order, execute, indexed_command_id, listing_aggregate,
    new_actor, order_command, payment_command, place_bid_command, register_auction_command,
    register_command, send, test_app, test_app_with_moderators, TestApp,
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
    assert_eq!(receipt["total"]["amount_minor"], json!(14_796));
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

    // A paid order that has not gone through return receipt, dispute, or
    // cancellation cannot be marked refunded, evidence or not.
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

// TS case: "opens participant disputes and restricts resolution to the
// sandbox moderator" — moderator identity adapted to the configured
// MODERATOR_PUBKYS role.
#[sqlx::test]
async fn opens_participant_disputes_and_restricts_resolution_to_configured_moderators(
    pool: PgPool,
) {
    let (moderator_keypair, moderator_pubky) = common::random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky.clone()]).await;
    let moderator_token = common::authenticate(&app, &moderator_keypair).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    let (status, opened) = execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.open",
            order_id,
            2,
            json!({ "reason": "Seller stopped responding", "requested_remedy": "refund" }),
            1_230,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "dispute open failed: {opened}");
    assert_eq!(opened["result"]["order"]["state"], json!("disputed"));
    assert_eq!(opened["result"]["order"]["dispute"]["state"], json!("open"));
    assert_eq!(
        opened["result"]["order"]["dispute"]["opened_by"],
        json!(buyer.pubky)
    );

    // Participants are not moderators: self-resolution is refused for the
    // seller and the buyer alike.
    for ordinary in [&seller, &buyer] {
        let (status, body) = execute(
            &app,
            &ordinary.token,
            &order_command(
                "dispute.resolve",
                order_id,
                3,
                json!({ "resolution": "seller_favor", "rationale": "Self-resolution attempt" }),
                1_231,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    }

    let (status, resolved) = execute(
        &app,
        &moderator_token,
        &order_command(
            "dispute.resolve",
            order_id,
            3,
            json!({ "resolution": "buyer_refund", "rationale": "Evidence supports the buyer." }),
            1_232,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolution failed: {resolved}");
    // A buyer remedy leaves the order disputed, awaiting the externally
    // evidenced refund.
    assert_eq!(resolved["result"]["order"]["state"], json!("disputed"));
    assert_eq!(
        resolved["result"]["order"]["dispute"]["state"],
        json!("resolved")
    );
    assert_eq!(
        resolved["result"]["order"]["dispute"]["resolution"],
        json!("buyer_refund")
    );

    // The refund evidence record completes the buyer-remedy path.
    let (status, refunded) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            4,
            json!({
                "amount_minor": order.total_minor,
                "transaction_id": "bitcoin-tx-evidence-456",
            }),
            1_233,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund failed: {refunded}");
    assert_eq!(
        refunded["result"]["order"]["state"],
        json!("refunded_external")
    );

    // Both participants were notified of the resolution through the outbox.
    drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    for participant in [&buyer, &seller] {
        let (status, body) = get(&app, "/v1/notifications", &participant.token).await;
        assert_eq!(status, StatusCode::OK);
        let types: Vec<&str> = body["notifications"]
            .as_array()
            .expect("notifications array")
            .iter()
            .filter_map(|n| n["type"].as_str())
            .collect();
        assert!(types.contains(&"dispute_updated"), "got: {types:?}");
    }
}

// New in the Rust service: a non-buyer remedy completes the order.
#[sqlx::test]
async fn a_seller_favor_resolution_completes_the_disputed_order(pool: PgPool) {
    let (moderator_keypair, moderator_pubky) = common::random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky]).await;
    let moderator_token = common::authenticate(&app, &moderator_keypair).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    execute(
        &app,
        &seller.token,
        &order_command(
            "dispute.open",
            &order.order_id,
            2,
            json!({ "reason": "Buyer refuses contact", "requested_remedy": "other" }),
            1_234,
        ),
    )
    .await;
    let (status, resolved) = execute(
        &app,
        &moderator_token,
        &order_command(
            "dispute.resolve",
            &order.order_id,
            3,
            json!({ "resolution": "seller_favor", "rationale": "The order was fulfilled as described." }),
            1_235,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolution failed: {resolved}");
    assert_eq!(resolved["result"]["order"]["state"], json!("completed"));
    assert_eq!(
        resolved["result"]["order"]["dispute"]["state"],
        json!("resolved")
    );
}

// New in the Rust service (dispute.evidence has no prototype counterpart):
// evidence is recorded append-only and its body never appears in ordinary
// projections or command results (ADR-0019 §8) — it is served only by the
// scoped case-file read, covered separately below.
#[sqlx::test]
async fn dispute_evidence_is_recorded_append_only_and_never_exposed(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();

    // Evidence requires an open dispute.
    let evidence_body = "Carrier photo reference SECRET-EVIDENCE-42";
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.evidence",
            order_id,
            2,
            json!({ "body": evidence_body }),
            1_240,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.open",
            order_id,
            2,
            json!({ "reason": "Item arrived damaged", "requested_remedy": "refund" }),
            1_241,
        ),
    )
    .await;
    let (status, added) = execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.evidence",
            order_id,
            3,
            json!({ "body": evidence_body }),
            1_242,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "evidence failed: {added}");
    assert_eq!(added["result"]["order"]["revision"], json!(4));
    assert_eq!(
        added["result"]["order"]["dispute"]["evidence_count"],
        json!(1)
    );
    // The body is withheld from the command result the submitter authored,
    // exactly as from every read projection.
    assert!(!added.to_string().contains("SECRET-EVIDENCE-42"));

    let (status, projected) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(projected["dispute"]["evidence_count"], json!(1));
    assert!(!projected.to_string().contains("SECRET-EVIDENCE-42"));

    // Nor from the order list projection.
    let (status, listed) = get(&app, "/v1/orders", &buyer.token).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!listed.to_string().contains("SECRET-EVIDENCE-42"));

    // The row itself is durable and append-only.
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM dispute_evidence").await,
        1
    );
    let tampered = sqlx::query("UPDATE dispute_evidence SET body = 'rewritten'")
        .execute(&app.pool)
        .await;
    assert!(tampered.is_err(), "evidence must reject UPDATE");
    let deleted = sqlx::query("DELETE FROM dispute_evidence")
        .execute(&app.pool)
        .await;
    assert!(deleted.is_err(), "evidence must reject DELETE");
}

// New in the Rust service: the dispute case file (ADR-0019 §5, §8 —
// "Operator queries return role-scoped, deliberately redacted views"). The
// two dispute participants and the configured moderator role read the
// evidence itself; each side sees what the other alleged; anyone else is
// refused with the existence-hiding 404. Moderator reads are privileged
// cross-user access and leave an append-only audit row; participant reads
// are ordinary object participation and leave none.
#[sqlx::test]
async fn dispute_evidence_is_readable_by_participants_and_moderators_only(pool: PgPool) {
    let (moderator_keypair, moderator_pubky) = common::random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky.clone()]).await;
    let moderator_token = common::authenticate(&app, &moderator_keypair).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    let evidence_uri = format!("/v1/orders/{order_id}/evidence");

    // Before any dispute: a participant reads their (empty) case file, but
    // the moderator role does not reach an undisputed order at all.
    let (status, body) = get(&app, &evidence_uri, &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "unexpected: {body}");
    assert_eq!(body["evidence"], json!([]));
    let (status, body) = get(&app, &evidence_uri, &moderator_token).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");

    execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.open",
            order_id,
            2,
            json!({ "reason": "Item arrived damaged", "requested_remedy": "refund" }),
            1_260,
        ),
    )
    .await;
    let buyer_evidence = "Unboxing photo BUYER-EVIDENCE-77";
    let seller_evidence = "Carrier handover scan SELLER-EVIDENCE-88";
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.evidence",
            order_id,
            3,
            json!({ "body": buyer_evidence }),
            1_261,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "buyer evidence failed: {body}");
    // Distinct timestamps make the newest-first ordering observable.
    app.clock.advance_seconds(60);
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "dispute.evidence",
            order_id,
            4,
            json!({ "body": seller_evidence }),
            1_262,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seller evidence failed: {body}");

    // The moderator reads the full case file, newest first: submitter,
    // body, byte size, and timestamp per item.
    let (status, body) = get(&app, &evidence_uri, &moderator_token).await;
    assert_eq!(status, StatusCode::OK, "moderator read failed: {body}");
    assert_eq!(body["order_id"], json!(order_id));
    let items = body["evidence"].as_array().expect("evidence array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["submitter_pubky"], json!(seller.pubky));
    assert_eq!(items[0]["body"], json!(seller_evidence));
    assert_eq!(items[0]["body_bytes"], json!(seller_evidence.len()));
    assert_eq!(items[1]["submitter_pubky"], json!(buyer.pubky));
    assert_eq!(items[1]["body"], json!(buyer_evidence));

    // Each participant sees the whole file, including the counterparty's
    // submission: a party who cannot see the allegation cannot answer it.
    for participant in [&buyer, &seller] {
        let (status, body) = get(&app, &evidence_uri, &participant.token).await;
        assert_eq!(status, StatusCode::OK, "participant read failed: {body}");
        let rendered = body.to_string();
        assert!(rendered.contains("BUYER-EVIDENCE-77"), "got: {rendered}");
        assert!(rendered.contains("SELLER-EVIDENCE-88"), "got: {rendered}");
    }

    // A non-participant non-moderator is refused with the same 404 an
    // absent order returns — never an empty list.
    let (status, body) = get(&app, &evidence_uri, &stranger.token).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));

    // Exactly the one moderator read was audited (the pre-dispute refusal
    // and the participant reads leave no rows), and the audit trail is
    // append-only like report_decisions.
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM dispute_evidence_reads").await,
        1
    );
    let (reader, items_served): (String, i64) =
        sqlx::query_as("SELECT reader_pubky, evidence_items FROM dispute_evidence_reads")
            .fetch_one(&app.pool)
            .await
            .expect("audit row present");
    assert_eq!(reader, moderator_pubky);
    assert_eq!(items_served, 2);
    let tampered = sqlx::query("UPDATE dispute_evidence_reads SET reader_pubky = 'rewritten'")
        .execute(&app.pool)
        .await;
    assert!(tampered.is_err(), "audit rows must reject UPDATE");
    let deleted = sqlx::query("DELETE FROM dispute_evidence_reads")
        .execute(&app.pool)
        .await;
    assert!(deleted.is_err(), "audit rows must reject DELETE");
}

// New in the Rust service: the moderator adjudication surface. The dispute
// queue and the disputed-order projection give a configured moderator the
// dispute reason and the order revision that dispute.resolve requires;
// non-moderators are refused the queue outright; page sizes follow the
// bounded-limit convention; resolved cases stay readable for review.
#[sqlx::test]
async fn the_dispute_queue_is_moderator_only_and_bounded(pool: PgPool) {
    let (moderator_keypair, moderator_pubky) = common::random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky]).await;
    let moderator_token = common::authenticate(&app, &moderator_keypair).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = order.order_id.as_str();
    // A second, undisputed order stays outside the moderator's reach.
    let bystander_seller = new_actor(&app).await;
    let bystander_buyer = new_actor(&app).await;
    let undisputed = create_paid_order(&app, &bystander_seller, &bystander_buyer).await;

    execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.open",
            order_id,
            2,
            json!({ "reason": "Wrong item shipped", "requested_remedy": "refund" }),
            1_270,
        ),
    )
    .await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "dispute.evidence",
            order_id,
            3,
            json!({ "body": "Label photo QUEUE-EVIDENCE-13" }),
            1_271,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "evidence failed: {body}");

    // The queue serves moderators the disputed order's projection: dispute
    // reason, state, and the revision dispute.resolve needs — but never the
    // delivery address or any evidence content.
    let (status, body) = get(&app, "/v1/disputes", &moderator_token).await;
    assert_eq!(status, StatusCode::OK, "queue read failed: {body}");
    let disputes = body["disputes"].as_array().expect("disputes array");
    assert_eq!(disputes.len(), 1);
    assert_eq!(disputes[0]["id"], json!(order_id));
    assert_eq!(disputes[0]["revision"], json!(4));
    assert_eq!(
        disputes[0]["dispute"]["reason"],
        json!("Wrong item shipped")
    );
    assert_eq!(disputes[0]["dispute"]["evidence_count"], json!(1));
    assert!(disputes[0].get("delivery_address").is_none());
    assert!(!body.to_string().contains("QUEUE-EVIDENCE-13"));

    // The single-order projection opens to the moderator for the disputed
    // order only; an undisputed order stays 404 like any foreign order.
    let (status, body) = get(&app, &format!("/v1/orders/{order_id}"), &moderator_token).await;
    assert_eq!(status, StatusCode::OK, "unexpected: {body}");
    assert!(body.get("delivery_address").is_none());
    let (status, body) = get(
        &app,
        &format!("/v1/orders/{}", undisputed.order_id),
        &moderator_token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");

    // Non-moderators are refused the queue — participants included.
    for ordinary in [&buyer, &seller, &bystander_buyer] {
        let (status, body) = get(&app, "/v1/disputes", &ordinary.token).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    }

    // Bounded pagination, per the projection convention.
    let (status, body) = get(&app, "/v1/disputes?limit=0", &moderator_token).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    let (status, body) = get(
        &app,
        &format!("/v1/orders/{order_id}/evidence?limit=201"),
        &moderator_token,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    app.clock.advance_seconds(60);
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "dispute.evidence",
            order_id,
            4,
            json!({ "body": "Second item" }),
            1_272,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second evidence failed: {body}");
    let (status, body) = get(
        &app,
        &format!("/v1/orders/{order_id}/evidence?limit=1"),
        &moderator_token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected: {body}");
    let items = body["evidence"].as_array().expect("evidence array");
    assert_eq!(items.len(), 1, "the page is capped at the requested limit");
    assert_eq!(items[0]["body"], json!("Second item"));

    // Resolution does not close the case file: the moderator can still
    // review the queue and the evidence afterwards, and post-resolution
    // reads are audited exactly like pre-resolution ones.
    let (status, body) = execute(
        &app,
        &moderator_token,
        &order_command(
            "dispute.resolve",
            order_id,
            5,
            json!({
                "resolution": "seller_favor",
                "rationale": "The evidence supports the seller.",
            }),
            1_273,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolution failed: {body}");
    let (status, body) = get(&app, "/v1/disputes", &moderator_token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["disputes"][0]["dispute"]["state"], json!("resolved"));
    let (status, body) = get(
        &app,
        &format!("/v1/orders/{order_id}/evidence"),
        &moderator_token,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-resolution read failed: {body}"
    );
    assert_eq!(
        body["evidence"].as_array().expect("evidence array").len(),
        2
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM dispute_evidence_reads").await,
        2
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
            "dispute.open",
            json!({ "reason": "Unrelated party filing", "requested_remedy": "refund" }),
        ),
        (
            "dispute.evidence",
            json!({ "body": "Unrelated party evidence" }),
        ),
        (
            "dispute.resolve",
            json!({ "resolution": "seller_favor", "rationale": "Unrelated party ruling" }),
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
// return the stored result without re-executing (ADR-0019 §3). Two orders
// are needed because the dispute path and the completed return path exclude
// each other on one order (a received return can no longer enter dispute).
#[sqlx::test]
async fn replays_each_post_purchase_command_idempotently(pool: PgPool) {
    let (moderator_keypair, moderator_pubky) = common::random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky]).await;
    let moderator_token = common::authenticate(&app, &moderator_keypair).await;
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
    // Checkout moves no inventory, so the second checkout still sees the
    // listing at revision 1.
    let second_checkout =
        common::checkout_command_with_id(&seller.pubky, &indexed_command_id(0x8000, 1_279));
    let (status, body) = execute(&app, &buyer.token, &second_checkout).await;
    assert_eq!(status, StatusCode::OK, "second checkout failed: {body}");
    let dispute_order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let dispute_payment_id = body["result"]["payments"][0]["id"]
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
        (
            "buyer",
            payment_command(&dispute_payment_id, 1, "confirmed", 1, 1_289),
        ),
        (
            "buyer",
            order_command(
                "dispute.open",
                &dispute_order_id,
                2,
                json!({ "reason": "Seller went unresponsive", "requested_remedy": "refund" }),
                1_290,
            ),
        ),
        (
            "buyer",
            order_command(
                "dispute.evidence",
                &dispute_order_id,
                3,
                json!({ "body": "Message log reference 77." }),
                1_291,
            ),
        ),
        (
            "moderator",
            order_command(
                "dispute.resolve",
                &dispute_order_id,
                4,
                json!({ "resolution": "buyer_refund", "rationale": "The seller never responded." }),
                1_292,
            ),
        ),
    ];
    for (role, command) in steps {
        let token = match role {
            "buyer" => &buyer.token,
            "seller" => &seller.token,
            _ => &moderator_token,
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
        assert_eq!(projected["dispute"], Value::Null);
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
