//! Offer lifecycle tests ported one-for-one from the TypeScript prototype
//! suite (`services/marketplace/src/transaction-service.test.ts`), plus the
//! Rust-service guarantees for idempotent replay and asset validation.

mod common;

use axum::http::StatusCode;
use serde_json::json;
use sqlx::PgPool;

use common::{
    count, counter_offer_command, create_offer_command, execute, new_actor, offer_action,
    register_command, reserve_command, test_app, OFFER_COMMAND_ID,
};

// TS case: "supports private offer, counteroffer, and atomic acceptance history"
#[sqlx::test]
async fn supports_private_offer_counteroffer_and_atomic_acceptance_history(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    let (status, created) =
        execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {created}");
    assert_eq!(created["revision"], json!(1));
    assert_eq!(created["result"]["kind"], json!("offer"));
    let offer = &created["result"]["offer"];
    assert_eq!(offer["buyer_pubky"], json!(buyer.pubky));
    assert_eq!(offer["seller_pubky"], json!(seller.pubky));
    assert_eq!(offer["state"], json!("pending"));
    assert_eq!(offer["offered_by"], json!(buyer.pubky));
    assert_eq!(offer["expires_at"], json!("2026-08-19T23:00:00.000Z"));

    let (status, countered) = execute(&app, &seller.token, &counter_offer_command(1)).await;
    assert_eq!(status, StatusCode::OK, "counter failed: {countered}");
    assert_eq!(countered["revision"], json!(2));
    let offer = &countered["result"]["offer"];
    assert_eq!(offer["state"], json!("countered"));
    assert_eq!(offer["offered_by"], json!(seller.pubky));
    assert_eq!(offer["amount"]["amount_minor"], json!(11_000));

    let (status, accepted) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.accept", 2, "00000000-0000-4000-8000-000000000502"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "accept failed: {accepted}");
    assert_eq!(accepted["revision"], json!(3));
    assert_eq!(
        accepted["event_ids"].as_array().map(Vec::len),
        Some(2),
        "acceptance emits the offer event and the inventory event"
    );
    assert_eq!(accepted["result"]["kind"], json!("accepted_offer"));
    assert_eq!(accepted["result"]["offer"]["state"], json!("accepted"));
    assert_eq!(accepted["result"]["offer"]["revision"], json!(3));
    let listing = &accepted["result"]["listing"];
    assert_eq!(listing["available_quantity"], json!(1));
    assert_eq!(listing["reserved_quantity"], json!(1));
    assert_eq!(listing["server_revision"], json!(2));
    let reservation = &accepted["result"]["reservation"];
    assert_eq!(reservation["buyer_pubky"], json!(buyer.pubky));
    assert_eq!(reservation["quantity"], json!(1));

    let (history,): (serde_json::Value,) =
        sqlx::query_as("SELECT history FROM offers WHERE id = $1")
            .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("fixture id parses"))
            .fetch_one(&app.pool)
            .await
            .expect("offer row exists");
    let actions: Vec<&str> = history
        .as_array()
        .expect("history array")
        .iter()
        .map(|entry| entry["action"].as_str().expect("action string"))
        .collect();
    assert_eq!(actions, vec!["created", "countered", "accepted"]);
}

// TS case: "enforces participant roles for counter, reject, and withdraw"
#[sqlx::test]
async fn enforces_participant_roles_for_counter_reject_and_withdraw(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;

    let (status, body) = execute(&app, &buyer.token, &counter_offer_command(1)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("The current offer author cannot counter their own terms.")
    );

    let (status, body) = execute(&app, &other_buyer.token, &counter_offer_command(1)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("Only offer participants may act on it.")
    );

    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.withdraw", 1, "00000000-0000-4000-8000-000000000503"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("Only the current offer author may withdraw it.")
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.reject", 1, "00000000-0000-4000-8000-000000000504"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("The current offer author cannot reject their own terms.")
    );
}

// TS case: "supports rejection by the recipient and withdrawal by the current author"
#[sqlx::test]
async fn supports_rejection_by_recipient_and_withdrawal_by_current_author(pool: PgPool) {
    let app = test_app(pool).await;

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.reject", 1, "00000000-0000-4000-8000-000000000505"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reject failed: {body}");
    assert_eq!(body["result"]["offer"]["state"], json!("rejected"));

    // A separate seller/listing exercises withdrawal by the current author.
    let second_seller = new_actor(&app).await;
    let second_buyer = new_actor(&app).await;
    execute(
        &app,
        &second_seller.token,
        &register_command(&second_seller.pubky, 1),
    )
    .await;
    let mut offer = create_offer_command(&second_seller.pubky, 1);
    offer["command_id"] = json!("00000000-0000-4000-8000-000000000510");
    let (status, body) = execute(&app, &second_buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK, "second offer failed: {body}");
    let withdraw = serde_json::json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000000506",
        "aggregate_id": "offer:00000000-0000-4000-8000-000000000510",
        "expected_revision": 1,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.withdraw",
        "payload": { "offer_id": "00000000-0000-4000-8000-000000000510" },
    });
    let (status, body) = execute(&app, &second_buyer.token, &withdraw).await;
    assert_eq!(status, StatusCode::OK, "withdraw failed: {body}");
    assert_eq!(body["result"]["offer"]["state"], json!("withdrawn"));
}

// TS case: "does not accept an offer after another buyer reserves the inventory"
#[sqlx::test]
async fn does_not_accept_offer_after_another_buyer_reserves_inventory(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    let (status, _) = execute(
        &app,
        &other_buyer.token,
        &reserve_command(&seller.pubky, 20, 1, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000000507"),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));
    assert_eq!(
        body["error"]["message"],
        json!("The offered quantity is no longer available.")
    );
}

// TS case: "rejects actions after server-time offer expiry"
#[sqlx::test]
async fn rejects_actions_after_server_time_offer_expiry(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    app.clock.advance_seconds(3_601);

    let (status, body) = execute(&app, &seller.token, &counter_offer_command(1)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("OFFER_EXPIRED"));
    assert_eq!(body["error"]["message"], json!("The offer has expired."));
}

// Prototype createOffer semantics: the offer must use the listing asset.
#[sqlx::test]
async fn rejects_offers_in_a_different_asset_and_stale_listing_revisions(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let mut wrong_asset = create_offer_command(&seller.pubky, 1);
    wrong_asset["payload"]["amount"]["currency"] = json!("EUR");
    let (status, body) = execute(&app, &buyer.token, &wrong_asset).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        body["error"]["message"],
        json!("Offer amount must use the listing asset and exponent.")
    );

    let mut stale = create_offer_command(&seller.pubky, 1);
    stale["expected_revision"] = json!(0);
    let (status, body) = execute(&app, &buyer.token, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));

    let mut oversized = create_offer_command(&seller.pubky, 2);
    oversized["command_id"] = json!("00000000-0000-4000-8000-000000000511");
    let (status, body) = execute(&app, &buyer.token, &oversized).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));

    let (status, body) =
        execute(&app, &seller.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body["error"]["message"],
        json!("A seller cannot make an offer on their own listing.")
    );
}

// ADR-0019 §3 idempotency applies to every ported command.
#[sqlx::test]
async fn replays_offer_commands_idempotently(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let command = create_offer_command(&seller.pubky, 1);

    let (first_status, first) = execute(&app, &buyer.token, &command).await;
    let (replay_status, replay) = execute(&app, &buyer.token, &command).await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay, first);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'offer.created'"
        )
        .await,
        1
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 1);

    let mut changed = create_offer_command(&seller.pubky, 1);
    changed["payload"]["amount"]["amount_minor"] = json!(9_999);
    let (status, body) = execute(&app, &buyer.token, &changed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("IDEMPOTENCY_CONFLICT"));
}

// Offer notifications carry the offer amount (ADR-0019 §8: both parties
// already read it on the offer projection). The counter carries the
// countered amount; the acceptance carries the amount that was accepted.
#[sqlx::test]
async fn offer_notifications_carry_the_offer_amount(pool: PgPool) {
    use marketplace_service::clock::Clock;
    use marketplace_service::workers::drain_outbox;

    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    execute(&app, &seller.token, &counter_offer_command(1)).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.accept", 2, "00000000-0000-4000-8000-000000000509"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "accept failed: {body}");

    drain_outbox(&app.pool, app.clock.now(), 50)
        .await
        .expect("outbox drains");
    let delivered: Vec<(String, String, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT type, recipient_pubky, amount FROM notifications ORDER BY type")
            .fetch_all(&app.pool)
            .await
            .expect("notifications listed");
    assert_eq!(
        delivered,
        vec![
            (
                "offer_accepted".to_string(),
                seller.pubky.clone(),
                Some(json!({ "amount_minor": 11_000, "currency": "USD", "exponent": 2 })),
            ),
            (
                "offer_countered".to_string(),
                buyer.pubky.clone(),
                Some(json!({ "amount_minor": 11_000, "currency": "USD", "exponent": 2 })),
            ),
            (
                "offer_received".to_string(),
                seller.pubky.clone(),
                Some(json!({ "amount_minor": 10_000, "currency": "USD", "exponent": 2 })),
            ),
        ]
    );
}
