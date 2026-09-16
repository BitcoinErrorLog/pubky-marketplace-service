//! Offer lifecycle tests ported one-for-one from the TypeScript prototype
//! suite (`services/marketplace/src/transaction-service.test.ts`), plus the
//! Rust-service guarantees for idempotent replay and asset validation.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HomeserverRawFetchOutcome,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};

use common::{
    config_durable, count, counter_offer_command, create_offer_command, execute, listing_aggregate,
    new_actor, offer_action, register_command, register_listing_command, reserve_command, send,
    test_app, test_app_with_config, test_app_with_homeserver, test_app_with_homeserver_client,
    OFFER_COMMAND_ID,
};

struct MutatingListingSnapshotSource {
    pool: PgPool,
    raw: Vec<u8>,
    raw_fetches: AtomicUsize,
}

impl MutatingListingSnapshotSource {
    fn new(pool: PgPool, record: serde_json::Value) -> Self {
        Self {
            pool,
            raw: serde_json::to_vec(&record).expect("snapshot serializes"),
            raw_fetches: AtomicUsize::new(0),
        }
    }
}

impl HomeserverListingClient for MutatingListingSnapshotSource {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            HomeserverFetchOutcome::Found(
                serde_json::from_slice(&self.raw).expect("snapshot is JSON"),
            )
        })
    }

    fn fetch_listing_raw<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverRawFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            let raw = self.raw.clone();
            if self.raw_fetches.fetch_add(1, Ordering::SeqCst) == 1 {
                sqlx::query("UPDATE offers SET terms_listing_record_sha256 = $2 WHERE id = $1")
                    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
                    .bind("b".repeat(64))
                    .execute(&self.pool)
                    .await
                    .expect("mutate negotiated snapshot before offer lock");
            }
            HomeserverRawFetchOutcome::Found(raw)
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::Unavailable })
    }
}

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
    assert_eq!(
        accepted["result"]["offer"]["award"]["listing"]["seller_pubky"],
        json!(seller.pubky)
    );
    assert_eq!(
        accepted["result"]["offer"]["award"]["listing"]["listing_id"],
        json!("boots_01")
    );
    assert_eq!(
        accepted["result"]["offer"]["award"]["listing"]["title"],
        json!("Marketplace item")
    );
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
    app.clock.advance_seconds(1_800);
    let (status, offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&buyer.token),
        &serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer list failed: {offers}");
    assert_eq!(offers["offers"][0]["state"], json!("expired"));
    assert_eq!(offers["offers"][0]["award"]["state"], json!("expired"));
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

#[allow(clippy::too_many_arguments)]
fn offer_checkout_command(
    offer_id: &str,
    award_id: &str,
    listing_aggregate_id: &str,
    listing_revision: i64,
    listing_record_sha256: &str,
    variant_id: &str,
    quantity: i64,
    command_id: &str,
) -> serde_json::Value {
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": format!("offer:{offer_id}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": offer_id,
            "award_id": award_id,
            "listing_aggregate_id": listing_aggregate_id,
            "listing_revision": listing_revision,
            "listing_record_sha256": listing_record_sha256,
            "variant_id": variant_id,
            "quantity": quantity,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    })
}

async fn accepted_offer_fixture(
    app: &common::TestApp,
    seller: &common::TestActor,
    buyer: &common::TestActor,
) -> (String, String, String, i64, String, i64) {
    app.clock.set(chrono::Utc::now());
    execute(app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    sqlx::query(
        "UPDATE offers SET accepted_listing_record_sha256 = $2, accepted_variant_id = $3 \
         WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .bind("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    .bind("boots_01")
    .execute(&app.pool)
    .await
    .expect("normalize accepted snapshot fixture");
    let row: (uuid::Uuid, uuid::Uuid, String, i64, String, i64) = sqlx::query_as(
        "SELECT award_id, id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_quantity FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    (
        row.1.to_string(),
        row.0.to_string(),
        row.2,
        row.3,
        row.4,
        row.5,
    )
}

#[sqlx::test]
async fn offer_checkout_transfers_the_accepted_hold_and_preserves_merchandise_terms(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001102",
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    assert_eq!(body["event_ids"].as_array().map(Vec::len), Some(2));
    let event_kinds: Vec<String> =
        sqlx::query_scalar("SELECT kind FROM events WHERE command_id = $1 ORDER BY kind")
            .bind(
                uuid::Uuid::parse_str("00000000-0000-4000-8000-000000001102").expect("command id"),
            )
            .fetch_all(&app.pool)
            .await
            .expect("conversion events");
    assert_eq!(
        event_kinds,
        vec!["offer.converted".to_string(), "order.created".to_string()]
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_created'"
        )
        .await,
        1
    );
    assert_eq!(body["result"]["order"]["priced_from"], json!("offer"));
    assert_eq!(
        body["result"]["order"]["total"]["amount_minor"],
        json!(10_000)
    );
    assert_eq!(
        body["result"]["order"]["lines"][0]["unit_price"]["amount_minor"],
        json!(10_000)
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE offer_award_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 1);
    let state: (String, String, i64, i64) = sqlx::query_as(
        "SELECT o.state, r.status, l.available_quantity, l.reserved_quantity \
         FROM offers o JOIN reservations r ON r.id = o.reservation_id \
         JOIN listings l ON l.aggregate_id = o.listing_aggregate_id WHERE o.id = $1",
    )
    .bind(uuid::Uuid::parse_str(&offer_id).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("converted state");
    assert_eq!(
        state,
        ("converted".to_string(), "converted".to_string(), 1, 1)
    );
}

#[sqlx::test]
async fn accepted_award_projection_totals_match_converted_order_for_participants(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;

    let (status, buyer_offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&buyer.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "buyer offer projection failed: {buyer_offers}"
    );
    let buyer_award = &buyer_offers["offers"][0]["award"];
    assert_eq!(
        buyer_award["subtotal"],
        json!({
            "amount_minor": 10_000,
            "currency": "USD",
            "exponent": 2
        })
    );
    assert_eq!(
        buyer_award["shipping"],
        json!({
            "amount_minor": 0,
            "currency": "USD",
            "exponent": 2
        })
    );
    assert_eq!(
        buyer_award["merchandise_total"],
        json!({
            "amount_minor": 10_000,
            "currency": "USD",
            "exponent": 2
        })
    );

    let (status, seller_offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "seller offer projection failed: {seller_offers}"
    );
    assert_eq!(&seller_offers["offers"][0]["award"], buyer_award);

    let (status, other_offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&other.token),
        &json!(null),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "non-participant offer projection failed: {other_offers}"
    );
    assert_eq!(other_offers["offers"], json!([]));

    let command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001103",
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let order = &body["result"]["order"];
    assert_eq!(buyer_award["subtotal"], order["subtotal"]);
    assert_eq!(buyer_award["shipping"], order["shipping"]);
    assert_eq!(buyer_award["merchandise_total"], order["total"]);
    let accepted_total_minor: i64 =
        sqlx::query_scalar("SELECT accepted_total_minor FROM offers WHERE id = $1")
            .bind(uuid::Uuid::parse_str(&offer_id).expect("offer id"))
            .fetch_one(&app.pool)
            .await
            .expect("accepted total");
    assert_eq!(
        buyer_award["merchandise_total"]["amount_minor"],
        json!(accepted_total_minor)
    );
}

#[sqlx::test]
async fn offer_checkout_uses_injected_clock_and_longest_configured_hold_window(pool: PgPool) {
    let mut config = config_durable();
    config.locks_payment_window_seconds = 4_200;
    config.fiat_payment_window_seconds = 5_100;
    config.sandbox_payment_window_seconds = 6_000;
    let app = test_app_with_config(pool, config).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let lock_now = app.clock.now();
    let command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001119",
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let hold_expires_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT hold_expires_at FROM orders WHERE offer_award_id = $1")
            .bind(uuid::Uuid::parse_str(&award_id).expect("award id"))
            .fetch_one(&app.pool)
            .await
            .expect("hold deadline");
    assert_eq!(hold_expires_at, lock_now + chrono::Duration::seconds(6_000));
}

#[sqlx::test]
async fn offer_checkout_refuses_at_the_injected_award_deadline(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    app.clock.advance_seconds(1_800);
    let command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001120",
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_EXPIRED"));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test]
async fn offer_checkout_refusal_contract_is_exact_and_refusals_do_not_write(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let mut command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001103",
    );
    let (status, body) = execute(&app, &other.token, &command).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("Only the accepted offer's buyer may place this order.")
    );
    command["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(
        body["error"]["message"],
        json!("The offer revision is stale.")
    );
    command["expected_revision"] = json!(2);
    command["payload"]["quantity"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_QUANTITY_MISMATCH"));
    assert_eq!(
        body["error"]["message"],
        json!("The checkout quantity does not match the accepted offer.")
    );
    command["payload"]["quantity"] = json!(1);
    command["payload"]["variant_id"] = json!("wrong-variant");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_VARIANT_MISMATCH"));
    assert_eq!(
        body["error"]["message"],
        json!("The checkout variant does not match the accepted offer.")
    );
    command["payload"]["variant_id"] = json!("boots_01");
    command["payload"]["listing_record_sha256"] = json!("f".repeat(64));
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_LISTING_CHANGED"));
    command["payload"]["listing_record_sha256"] = json!(hash);
    execute(
        &app,
        &seller.token,
        &register_listing_command(&seller.pubky, "boots_02", 1, 20),
    )
    .await;
    sqlx::query("UPDATE reservations SET listing_aggregate_id = $2 WHERE offer_award_id = $1")
        .bind(uuid::Uuid::parse_str(&award_id).expect("award id"))
        .bind(format!("listing:{}_boots_02", seller.pubky))
        .execute(&app.pool)
        .await
        .expect("break award hold");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_HOLD_MISSING"));
    assert_eq!(
        body["error"]["message"],
        json!("The inventory reserved for this accepted offer is no longer held.")
    );
    sqlx::query("UPDATE reservations SET listing_aggregate_id = $2 WHERE offer_award_id = $1")
        .bind(uuid::Uuid::parse_str(&award_id).expect("award id"))
        .bind(&listing)
        .execute(&app.pool)
        .await
        .expect("restore award hold fixture");
    sqlx::query("UPDATE reservations SET status = 'expired' WHERE offer_award_id = $1")
        .bind(uuid::Uuid::parse_str(&award_id).expect("award id"))
        .execute(&app.pool)
        .await
        .expect("expire reservation");
    command["payload"]["listing_record_sha256"] = json!(hash);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_EXPIRED"));
    assert_eq!(
        body["error"]["message"],
        json!("This accepted offer's checkout window has expired. Nothing was ordered.")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test]
async fn offer_checkout_success_replay_is_idempotent(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let mut command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001104",
    );
    command["payload"]["quantity"] = json!(2);
    let (refusal_status, refusal) = execute(&app, &buyer.token, &command).await;
    assert_eq!(refusal_status, StatusCode::CONFLICT);
    assert_eq!(refusal["error"]["code"], json!("AWARD_QUANTITY_MISMATCH"));
    command["payload"]["quantity"] = json!(quantity);
    let (first_status, first) = execute(&app, &buyer.token, &command).await;
    let (replay_status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay, first);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 1);
    command["command_id"] = json!("00000000-0000-4000-8000-000000001105");
    command["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_ALREADY_CONVERTED"));
    command["expected_revision"] = json!(3);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_ALREADY_CONVERTED"));
}

#[sqlx::test]
async fn offer_checkout_rejects_non_accepted_state_with_exact_contract(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    let mut command = offer_checkout_command(
        OFFER_COMMAND_ID,
        "00000000-0000-4000-8000-000000000001",
        &listing_aggregate(&seller.pubky),
        1,
        &"a".repeat(64),
        "boots_01",
        1,
        "00000000-0000-4000-8000-000000001106",
    );
    command["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("Only an accepted offer can enter offer checkout.")
    );
}

#[sqlx::test]
async fn expired_award_releases_inventory(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    app.clock.advance_seconds(1_801);
    let expired = marketplace_service::workers::expire_due_offers(&app.pool, app.clock.now())
        .await
        .expect("award expiry");
    assert_eq!(expired, 1);
    let row: (String, String, i64, i64) = sqlx::query_as(
        "SELECT o.state, r.status, l.available_quantity, l.reserved_quantity \
         FROM offers o JOIN reservations r ON r.id = o.reservation_id \
         JOIN listings l ON l.aggregate_id = o.listing_aggregate_id WHERE o.id = $1",
    )
    .bind(uuid::Uuid::parse_str(&offer_id).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("expired award state");
    assert_eq!(row, ("expired".to_string(), "expired".to_string(), 2, 0));
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = 'offer.expired'",
    )
    .bind(format!("offer:{offer_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("expiry event count");
    assert_eq!(event_count, 1);
    let mut command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001118",
    );
    command["expected_revision"] = json!(3);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_EXPIRED"));
}

#[sqlx::test]
async fn poisoned_award_does_not_roll_back_other_award_expiries(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let first_buyer = new_actor(&app).await;
    let second_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    execute(
        &app,
        &first_buyer.token,
        &create_offer_command(&seller.pubky, 1),
    )
    .await;
    execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001124"),
    )
    .await;
    let second_offer_id = "00000000-0000-4000-8000-000000000600";
    let mut second_offer = create_offer_command(&seller.pubky, 1);
    second_offer["command_id"] = json!(second_offer_id);
    second_offer["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &second_buyer.token, &second_offer).await;
    assert_eq!(status, StatusCode::OK, "second offer failed: {body}");
    let second_accept = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000001125",
        "aggregate_id": format!("offer:{second_offer_id}"),
        "expected_revision": 1,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.accept",
        "payload": {"offer_id": second_offer_id}
    });
    let (status, body) = execute(&app, &seller.token, &second_accept).await;
    assert_eq!(status, StatusCode::OK, "second accept failed: {body}");
    sqlx::query(
        "UPDATE reservations SET quantity = 2 \
         WHERE offer_award_id = (SELECT award_id FROM offers WHERE id = $1)",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("first offer id"))
    .execute(&app.pool)
    .await
    .expect("poison first award");
    app.clock.advance_seconds(1_801);
    let expired = marketplace_service::workers::expire_due_offers(&app.pool, app.clock.now())
        .await
        .expect("award sweep");
    assert_eq!(expired, 1);
    let states: Vec<(uuid::Uuid, String)> =
        sqlx::query_as("SELECT id, state FROM offers ORDER BY id")
            .fetch_all(&app.pool)
            .await
            .expect("offer states");
    assert_eq!(
        states,
        vec![
            (
                uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("first offer id"),
                "accepted".to_string(),
            ),
            (
                uuid::Uuid::parse_str(second_offer_id).expect("second offer id"),
                "expired".to_string(),
            ),
        ]
    );
}

#[sqlx::test]
async fn offer_checkout_rejects_variant_mismatch_with_exact_contract(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let command = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "different_variant",
        quantity,
        "00000000-0000-4000-8000-000000001107",
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_VARIANT_MISMATCH"));
    assert_eq!(
        body["error"]["message"],
        json!("The checkout variant does not match the accepted offer.")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test]
async fn reservation_expiry_sweeps_only_generic_reservations(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001108"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    let award_id: uuid::Uuid = sqlx::query_scalar("SELECT award_id FROM offers WHERE id = $1")
        .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .fetch_one(&app.pool)
        .await
        .expect("award id");
    let generic_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO reservations (id, listing_aggregate_id, buyer_pubky, quantity, status, \
         expires_at, created_at, updated_at) VALUES ($1, $2, $3, 1, 'active', $4, $4, $4)",
    )
    .bind(generic_id)
    .bind(listing_aggregate(&seller.pubky))
    .bind(&buyer.pubky)
    .bind(app.clock.now() - chrono::Duration::seconds(1))
    .execute(&app.pool)
    .await
    .expect("generic reservation");
    sqlx::query(
        "UPDATE listings SET available_quantity = available_quantity - 1, \
         reserved_quantity = reserved_quantity + 1 WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&app.pool)
    .await
    .expect("account generic reservation");
    let released = marketplace_service::expiry::expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("generic expiry");
    assert_eq!(released, 1);
    let generic_status: String =
        sqlx::query_scalar("SELECT status FROM reservations WHERE id = $1")
            .bind(generic_id)
            .fetch_one(&app.pool)
            .await
            .expect("generic status");
    let award_status: String =
        sqlx::query_scalar("SELECT status FROM reservations WHERE offer_award_id = $1")
            .bind(award_id)
            .fetch_one(&app.pool)
            .await
            .expect("award status");
    assert_eq!(generic_status, "expired");
    assert_eq!(award_status, "active");
    let quantities: (i64, i64) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing quantities");
    assert_eq!(quantities, (1, 1));
    app.clock.advance_seconds(1_801);
    let awards = marketplace_service::workers::expire_due_offers(&app.pool, app.clock.now())
        .await
        .expect("award expiry");
    assert_eq!(awards, 1);
    let final_quantities: (i64, i64) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("final listing quantities");
    assert_eq!(final_quantities, (2, 0));
}

#[sqlx::test]
async fn legacy_accepted_offer_is_refused_as_unconvertible(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    sqlx::query(
        "UPDATE offers SET state = 'expired', expiry_reason = 'legacy_unconvertible' WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .execute(&app.pool)
    .await
    .expect("legacy offer row");
    let command = offer_checkout_command(
        OFFER_COMMAND_ID,
        "00000000-0000-4000-8000-000000000001",
        &listing_aggregate(&seller.pubky),
        1,
        &"a".repeat(64),
        "boots_01",
        1,
        "00000000-0000-4000-8000-000000001109",
    );
    let mut command = command;
    command["expected_revision"] = json!(1);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("Only an accepted offer can enter offer checkout.")
    );
}

#[sqlx::test]
async fn concurrent_checkout_and_expiry_paths_complete_without_deadlock(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, award_id, listing, revision, hash, quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    let initial_total: i64 = sqlx::query_scalar(
        "SELECT available_quantity + reserved_quantity + sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(&listing)
    .fetch_one(&app.pool)
    .await
    .expect("initial inventory identity");
    let command_a = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001110",
    );
    let command_b = offer_checkout_command(
        &offer_id,
        &award_id,
        &listing,
        revision,
        &hash,
        "boots_01",
        quantity,
        "00000000-0000-4000-8000-000000001111",
    );
    let router_a = app.router.clone();
    let router_b = app.router.clone();
    let token_a = buyer.token.clone();
    let token_b = buyer.token.clone();
    let pool_a = app.pool.clone();
    let pool_b = app.pool.clone();
    let now = app.clock.now();
    let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async move {
        tokio::join!(
            send(router_a, "POST", "/v1/commands", Some(&token_a), &command_a),
            send(router_b, "POST", "/v1/commands", Some(&token_b), &command_b),
            marketplace_service::workers::expire_due_offers(&pool_a, now),
            marketplace_service::expiry::expire_due_reservations(&pool_b, now),
        )
    })
    .await
    .expect("concurrent paths complete");
    let (first, second, award_expiry, generic_expiry) = joined;
    assert!(first.0 == StatusCode::OK || second.0 == StatusCode::OK);
    assert!(award_expiry.is_ok());
    assert!(generic_expiry.is_ok());
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
    let states: (String, i64, i64, i64) = sqlx::query_as(
        "SELECT o.state, l.available_quantity, l.reserved_quantity, l.sold_quantity \
         FROM offers o JOIN listings l ON l.aggregate_id = o.listing_aggregate_id WHERE o.id = $1",
    )
    .bind(uuid::Uuid::parse_str(&offer_id).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("final concurrent state");
    assert_eq!(states.0, "converted");
    assert_eq!(states.1 + states.2 + states.3, initial_total);
}

#[sqlx::test]
async fn award_expiry_retries_after_a_real_postgres_deadlock(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, _award_id, _listing, _revision, _hash, _quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    app.clock.advance_seconds(1_801);
    let now = app.clock.now();
    let offer_uuid = uuid::Uuid::parse_str(&offer_id).expect("offer id");
    let reservation_id: uuid::Uuid =
        sqlx::query_scalar("SELECT reservation_id FROM offers WHERE id = $1")
            .bind(offer_uuid)
            .fetch_one(&app.pool)
            .await
            .expect("reservation id");

    let options = app.pool.connect_options().clone();
    let mut offer_holder = PgConnection::connect_with(&options)
        .await
        .expect("offer connection");
    let mut reservation_holder = PgConnection::connect_with(&options)
        .await
        .expect("reservation connection");
    sqlx::query("BEGIN")
        .execute(&mut offer_holder)
        .await
        .expect("offer transaction");
    sqlx::query("BEGIN")
        .execute(&mut reservation_holder)
        .await
        .expect("reservation transaction");
    sqlx::query("SELECT id FROM offers WHERE id = $1 FOR UPDATE")
        .bind(offer_uuid)
        .execute(&mut offer_holder)
        .await
        .expect("hold offer lock");
    sqlx::query("SELECT id FROM reservations WHERE id = $1 FOR UPDATE")
        .bind(reservation_id)
        .execute(&mut reservation_holder)
        .await
        .expect("hold reservation lock");

    let (offer_wait, reservation_wait) = tokio::join!(
        sqlx::query("SELECT id FROM reservations WHERE id = $1 FOR UPDATE")
            .bind(reservation_id)
            .execute(&mut offer_holder),
        sqlx::query("SELECT id FROM offers WHERE id = $1 FOR UPDATE")
            .bind(offer_uuid)
            .execute(&mut reservation_holder),
    );
    let deadlock = match (offer_wait, reservation_wait) {
        (Err(error), _) | (_, Err(error)) => error,
        (Ok(_), Ok(_)) => panic!("opposite lock order must produce a deadlock"),
    };
    assert_eq!(
        deadlock
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("40P01")
    );
    let _ = sqlx::query("ROLLBACK").execute(&mut offer_holder).await;
    let _ = sqlx::query("ROLLBACK")
        .execute(&mut reservation_holder)
        .await;

    let expired = marketplace_service::workers::expire_due_offers(&app.pool, now)
        .await
        .expect("fresh expiry transaction succeeds after deadlock");
    assert_eq!(expired, 1, "fresh expiry transaction completes after retry");
    let state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(offer_uuid)
        .fetch_one(&app.pool)
        .await
        .expect("offer state");
    assert_eq!(state, "expired");
}

#[sqlx::test]
async fn award_expiry_worker_retries_when_it_is_the_deadlock_victim(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (offer_id, _award_id, listing_aggregate_id, _revision, _hash, _quantity) =
        accepted_offer_fixture(&app, &seller, &buyer).await;
    app.clock.advance_seconds(1_801);
    let now = app.clock.now();
    let offer_uuid = uuid::Uuid::parse_str(&offer_id).expect("offer id");
    let retries_before = marketplace_service::workers::award_expiry_retry_count();

    let options = app.pool.connect_options().clone();
    let mut reverse_holder = PgConnection::connect_with(&options)
        .await
        .expect("reverse-order connection");
    sqlx::query("BEGIN")
        .execute(&mut reverse_holder)
        .await
        .expect("reverse-order transaction");
    sqlx::query("SELECT aggregate_id FROM listings WHERE aggregate_id = $1 FOR UPDATE")
        .bind(&listing_aggregate_id)
        .execute(&mut reverse_holder)
        .await
        .expect("hold listing lock");

    let worker = tokio::spawn({
        let pool = app.pool.clone();
        async move { marketplace_service::workers::expire_due_offers(&pool, now).await }
    });
    let wait_started = tokio::time::Instant::now();
    loop {
        let worker_waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS ( \
                 SELECT 1 FROM pg_stat_activity \
                 WHERE wait_event_type = 'Lock' \
                   AND query LIKE '%UPDATE listings SET server_revision%' \
             )",
        )
        .fetch_one(&app.pool)
        .await
        .expect("lock-wait probe");
        if worker_waiting {
            break;
        }
        assert!(
            wait_started.elapsed() < std::time::Duration::from_secs(10),
            "award-expiry worker never reached the listing lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let _reverse_offer_lock = sqlx::query("SELECT id FROM offers WHERE id = $1 FOR UPDATE")
        .bind(offer_uuid)
        .execute(&mut reverse_holder)
        .await;
    let _ = sqlx::query("ROLLBACK").execute(&mut reverse_holder).await;

    let expired = worker
        .await
        .expect("worker task joins")
        .expect("worker expiry completes after retry");
    assert_eq!(expired, 1);
    assert!(
        marketplace_service::workers::award_expiry_retry_count() > retries_before,
        "worker must observe at least one 40P01 retry"
    );

    let (offer_state, reservation_status, available, reserved, sold, total): (
        String,
        String,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT o.state, r.status, l.available_quantity, l.reserved_quantity, \
         l.sold_quantity, l.total_quantity \
         FROM offers o \
         JOIN reservations r ON r.id = o.reservation_id \
         JOIN listings l ON l.aggregate_id = o.listing_aggregate_id \
         WHERE o.id = $1",
    )
    .bind(offer_uuid)
    .fetch_one(&app.pool)
    .await
    .expect("expired award state");
    assert_eq!(offer_state, "expired");
    assert_eq!(reservation_status, "expired");
    assert_eq!(reserved, 0);
    assert_eq!(available + reserved + sold, total);
    let expiry_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = 'offer.expired'",
    )
    .bind(format!("offer:{offer_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("expiry event count");
    assert_eq!(
        expiry_events, 1,
        "reservation release is emitted exactly once"
    );
}

#[sqlx::test]
async fn changed_listing_snapshot_between_fetch_and_offer_lock_is_refused(pool: PgPool) {
    let source = Arc::new(MutatingListingSnapshotSource::new(
        pool.clone(),
        json!({
            "revision": 1,
            "title": "Boots",
            "sale": {
                "format": "fixed_price",
                "unitPrice": {"amountMinor": 12500, "currency": "USD", "exponent": 2}
            },
            "variants": [{"id": "boots_01", "enabled": true, "quantity": 2}],
            "shippingOptions": [{"pricing": "free"}]
        }),
    ));
    let app = test_app_with_homeserver_client(pool, source).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001112"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_LISTING_CHANGED"));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM reservations").await,
        0
    );
    let state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .fetch_one(&app.pool)
        .await
        .expect("offer remains");
    assert_eq!(state, "pending");
}

#[sqlx::test]
async fn raw_listing_snapshot_bounds_and_money_shape_are_refused(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let valid = json!({
        "revision": 1,
        "title": "Valid",
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
        },
        "variants": [
            {"id": "boots_01", "enabled": true, "quantity": 1},
            {"enabled": false, "quantity": "not-a-number"}
        ],
        "shippingOptions": [{"pricing": "free"}]
    });
    homeserver.put_record(&seller.pubky, "boots_01", valid.clone());
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["command_id"] = json!("00000000-0000-4000-8000-000000001113");
    let (status, _) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK);
    let oversized = json!({
        "revision": 1,
        "title": "Oversized",
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
        },
        "variants": [{"id": "boots_01", "enabled": true, "quantity": 1}],
        "shippingOptions": [{"pricing": "free"}],
        "padding": "x".repeat(1_048_577)
    });
    homeserver.put_record(&seller.pubky, "boots_01", oversized);
    let (status, body) = execute(
        &app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000001114",
            "aggregate_id": "offer:00000000-0000-4000-8000-000000001113",
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.accept",
            "payload": {"offer_id": "00000000-0000-4000-8000-000000001113"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_LISTING_CHANGED"));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM offers WHERE accepted_listing_record_sha256 IS NOT NULL"
        )
        .await,
        0
    );
    let malformed = json!({
        "revision": 2,
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2, "extra": true}
        },
        "variants": [{"id": "boots_01", "enabled": true, "quantity": 1}],
        "shippingOptions": [{"pricing": "free"}]
    });
    homeserver.put_record(&seller.pubky, "boots_01", malformed.clone());
    homeserver.put_record(&seller.pubky, "boots_01", valid.clone());
    let mut second_offer = create_offer_command(&seller.pubky, 1);
    second_offer["command_id"] = json!("00000000-0000-4000-8000-000000001115");
    let (status, _) = execute(&app, &buyer.token, &second_offer).await;
    assert_eq!(status, StatusCode::OK);
    homeserver.put_record(&seller.pubky, "boots_01", malformed.clone());
    let (status, body) = execute(
        &app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000001116",
            "aggregate_id": "offer:00000000-0000-4000-8000-000000001115",
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.accept",
            "payload": {"offer_id": "00000000-0000-4000-8000-000000001115"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("AWARD_LISTING_CHANGED"));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM offers WHERE accepted_listing_record_sha256 IS NOT NULL"
        )
        .await,
        0
    );

    let strict_failures = [
        (
            "00000000-0000-4000-8000-000000001121",
            json!({
                "revision": 3,
                "sale": {
                    "format": "fixed_price",
                    "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
                },
                "variants": [{"id": "boots_01", "enabled": false, "quantity": 1}],
                "shippingOptions": [{"pricing": "free"}]
            }),
        ),
        (
            "00000000-0000-4000-8000-000000001122",
            json!({
                "revision": 4,
                "sale": {
                    "format": "fixed_price",
                    "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
                },
                "variants": [{
                    "id": "boots_01",
                    "enabled": true,
                    "quantity": 1,
                    "priceOverride": {"amountMinor": "1000", "currency": "USD", "exponent": 2}
                }],
                "shippingOptions": [{"pricing": "free"}]
            }),
        ),
        (
            "00000000-0000-4000-8000-000000001123",
            json!({
                "revision": 5,
                "sale": {
                    "format": "fixed_price",
                    "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
                },
                "variants": [{"id": "boots_01", "enabled": true, "quantity": 1}],
                "shippingOptions": [{"pricing": "flat"}]
            }),
        ),
        (
            "00000000-0000-4000-8000-000000001126",
            json!({
                "revision": 6,
                "sale": {
                    "format": "fixed_price",
                    "unitPrice": {"amountMinor": 1000, "currency": "USD", "exponent": 2}
                },
                "variants": [{
                    "id": "boots_01",
                    "enabled": true,
                    "quantity": 1,
                    "options": [{"name": "size", "value": 42}]
                }],
                "shippingOptions": [{"pricing": "free"}]
            }),
        ),
    ];
    for (command_id, record) in strict_failures {
        homeserver.put_record(&seller.pubky, "boots_01", record);
        let mut command = create_offer_command(&seller.pubky, 1);
        command["command_id"] = json!(command_id);
        let (status, body) = execute(&app, &buyer.token, &command).await;
        assert_eq!(status, StatusCode::CONFLICT, "{command_id}: {body}");
        assert_eq!(
            body["error"]["code"],
            json!("AWARD_LISTING_CHANGED"),
            "{command_id}: {body}"
        );
    }
    homeserver.put_record(&seller.pubky, "boots_01", valid);
    let (status, body) = execute(
        &app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-000000001127",
            "aggregate_id": "offer:00000000-0000-4000-8000-000000001115",
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "offer.accept",
            "payload": {"offer_id": "00000000-0000-4000-8000-000000001115"}
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "disabled sibling must be ignored: {body}"
    );
}

#[sqlx::test]
async fn counter_offer_refreshes_negotiated_terms_from_the_homeserver(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let initial = json!({
        "revision": 1,
        "title": "Boots",
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 12500, "currency": "USD", "exponent": 2}
        },
        "variants": [{"id": "boots_01", "enabled": true, "quantity": 2}],
        "shippingOptions": [{"pricing": "free"}]
    });
    homeserver.put_record(&seller.pubky, "boots_01", initial);
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let original_hash: String =
        sqlx::query_scalar("SELECT terms_listing_record_sha256 FROM offers WHERE id = $1")
            .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
            .fetch_one(&app.pool)
            .await
            .expect("initial hash");
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        json!({
            "revision": 2,
            "title": "Boots refreshed",
            "sale": {
                "format": "fixed_price",
                "unitPrice": {"amountMinor": 12500, "currency": "USD", "exponent": 2}
            },
            "variants": [{"id": "boots_01", "enabled": true, "quantity": 2}],
            "shippingOptions": [{"pricing": "free"}]
        }),
    );
    let (status, body) = execute(&app, &seller.token, &counter_offer_command(1)).await;
    assert_eq!(status, StatusCode::OK, "counter failed: {body}");
    let (revision, refreshed_hash): (i64, String) = sqlx::query_as(
        "SELECT terms_listing_revision, terms_listing_record_sha256 FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("refreshed terms");
    assert_eq!(revision, 2);
    assert_ne!(refreshed_hash, original_hash);
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

    drain_outbox(&app.pool, None, app.clock.now(), 50)
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
