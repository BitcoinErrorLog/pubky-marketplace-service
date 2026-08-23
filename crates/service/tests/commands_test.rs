//! Vertical-slice command tests ported one-for-one from the TypeScript
//! prototype suite (`services/marketplace/src/transaction-service.test.ts`).
//! Actor pubkys are real ed25519 keypairs authenticated through the full
//! AuthToken flow; assertions are adapted to the snake_case wire
//! format defined by ADR-0019 §3.

mod common;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use serde_json::json;
use sqlx::PgPool;

use common::{
    checkout_command, count, execute, listing_aggregate, new_actor, random_keypair,
    register_command, reserve_command, test_app,
};

// TS case: "registers seller-owned inventory at revision one"
#[sqlx::test]
async fn registers_seller_owned_inventory_at_revision_one(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(
        body["aggregate_id"],
        json!(listing_aggregate(&seller.pubky))
    );
    assert_eq!(body["revision"], json!(1));
    let listing = &body["result"]["listing"];
    assert_eq!(body["result"]["kind"], json!("listing"));
    assert_eq!(listing["available_quantity"], json!(1));
    assert_eq!(listing["reserved_quantity"], json!(0));
    assert_eq!(listing["server_revision"], json!(1));
    assert_eq!(listing["state"], json!("available"));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 1);
}

// TS case: "rejects registration by anyone other than the public listing seller"
#[sqlx::test]
async fn rejects_registration_by_non_seller(pool: PgPool) {
    let app = test_app(pool).await;
    let buyer = new_actor(&app).await;
    let (_, seller_pubky) = random_keypair();

    let (status, body) = execute(&app, &buyer.token, &register_command(&seller_pubky, 1)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body,
        json!({
            "ok": false,
            "error": {
                "code": "UNAUTHORIZED",
                "message": "Only the listing seller may register inventory.",
            },
        })
    );
}

// TS case: "returns the exact stored result for an idempotent replay"
#[sqlx::test]
async fn returns_exact_stored_result_for_idempotent_replay(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let command = register_command(&seller.pubky, 1);

    let (first_status, first) = execute(&app, &seller.token, &command).await;
    let (replay_status, replay) = execute(&app, &seller.token, &command).await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay, first);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 1);
}

// TS case: "rejects changed input under an already accepted command id"
#[sqlx::test]
async fn rejects_changed_input_under_accepted_command_id(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({
            "ok": false,
            "error": {
                "code": "IDEMPOTENCY_CONFLICT",
                "message": "The command id was already used with different input.",
            },
        })
    );
}

// TS case: "allows exactly one of 100 concurrent buyers to reserve one unit"
#[sqlx::test]
async fn allows_exactly_one_of_100_concurrent_buyers_to_reserve_one_unit(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let mut handles = Vec::with_capacity(100);
    for index in 1..=100u64 {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = reserve_command(&seller.pubky, index, 1, 1);
        handles.push(tokio::spawn(async move {
            common::send(router, "POST", "/v1/commands", Some(&token), &command).await
        }));
    }
    let mut accepted = 0;
    let mut rejected = 0;
    for handle in handles {
        let (_, body) = handle.await.expect("request task completes");
        if body["ok"] == json!(true) {
            accepted += 1;
        } else {
            rejected += 1;
            assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(rejected, 99);

    let (available, reserved, revision, state): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, server_revision, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (available, reserved, revision, state.as_str()),
        (0, 1, 2, "reserved")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 2);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM reservations").await,
        1
    );
}

// TS case: "uses server time for reservation expiry"
#[sqlx::test]
async fn uses_server_time_for_reservation_expiry(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["kind"], json!("reservation"));
    assert_eq!(
        body["result"]["reservation"]["created_at"],
        json!("2026-08-19T22:00:00.000Z")
    );
    assert_eq!(
        body["result"]["reservation"]["expires_at"],
        json!("2026-08-19T22:10:00.000Z")
    );
}

// TS case: "rejects seller self-reservation and stale buyer revisions"
#[sqlx::test]
async fn rejects_seller_self_reservation_and_stale_revisions(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &reserve_command(&seller.pubky, 1, 1, 1),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));

    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 2, 1, 0)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));
}

// TS case: "prevents a seller update from reducing total quantity below reservations"
#[sqlx::test]
async fn prevents_seller_update_reducing_quantity_below_reservations(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, _) = execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 2, 1)).await;
    assert_eq!(status, StatusCode::OK);

    let mut update = register_command(&seller.pubky, 1);
    update["command_id"] = json!("018f47d2-6a27-7c23-a49d-6b21bb770121");
    update["expected_revision"] = json!(2);
    update["payload"]["listing_revision"] = json!(2);
    let (status, body) = execute(&app, &seller.token, &update).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
    assert_eq!(body["error"]["current_revision"], json!(2));
}

// TS case: "returns redacted validation issues for malformed commands"
#[sqlx::test]
async fn returns_redacted_validation_issues_for_malformed_commands(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let mut command = register_command(&seller.pubky, 1);
    command["private_address"] = json!("secret-address");

    let (status, body) = execute(&app, &seller.token, &command).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert!(body["error"]["issues"].is_array());
    assert!(!body.to_string().contains("secret-address"));
}

// New in the Rust service: unsupported versions and unported command kinds
// are rejected by the envelope contract (ADR-0019 §3).
#[sqlx::test]
async fn rejects_unsupported_versions_and_unported_kinds(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let mut versioned = register_command(&seller.pubky, 1);
    versioned["version"] = json!(2);
    let (status, body) = execute(&app, &seller.token, &versioned).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["issues"][0]["path"], json!("version"));

    let mut unported = register_command(&seller.pubky, 1);
    unported["kind"] = json!("message.send");
    let (status, body) = execute(&app, &seller.token, &unported).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["issues"][0]["path"], json!("kind"));
}

// TS case: "creates an immutable checkout snapshot, reservation, order, and
// sandbox payment" (sandbox payment advancement is covered in
// post_purchase_test.rs).
#[sqlx::test]
async fn creates_immutable_checkout_snapshot_order_and_sandbox_payment(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], json!(1));
    assert_eq!(body["result"]["kind"], json!("checkout"));
    let order = &body["result"]["orders"][0];
    assert_eq!(order["buyer_pubky"], json!(buyer.pubky));
    assert_eq!(order["seller_pubky"], json!(seller.pubky));
    assert_eq!(order["state"], json!("pending_payment"));
    assert_eq!(order["subtotal"]["amount_minor"], json!(12_500));
    assert_eq!(order["shipping"]["amount_minor"], json!(1_200));
    assert_eq!(order["tax"]["amount_minor"], json!(1_096));
    assert_eq!(order["total"]["amount_minor"], json!(14_796));
    assert_eq!(order["guarantee_policy_version"], json!(1));
    let line = &order["lines"][0];
    assert_eq!(line["listing_revision"], json!(1));
    assert_eq!(line["content_hash"], json!("a".repeat(64)));
    assert_eq!(line["quantity"], json!(1));
    let payment = &body["result"]["payments"][0];
    assert_eq!(payment["state"], json!("awaiting_entitlement"));
    assert_eq!(payment["adapter"], json!("sandbox"));
    assert_eq!(payment["amount"]["amount_minor"], json!(14_796));

    let (available, reserved, revision, state): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, server_revision, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (available, reserved, revision, state.as_str()),
        (0, 1, 2, "reserved")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.order_created'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.created'"
        )
        .await,
        1
    );
}

// TS case: "rejects duplicate checkout lines, stale stock, self-purchase, and
// invalid payment transitions" (the payment-transition half lives in
// post_purchase_test.rs).
#[sqlx::test]
async fn rejects_duplicate_lines_self_purchase_stale_stock_and_oversell(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    let mut duplicate = checkout_command(&seller.pubky);
    let line = duplicate["payload"]["lines"][0].clone();
    duplicate["payload"]["lines"]
        .as_array_mut()
        .expect("lines")
        .push(line);
    let (status, body) = execute(&app, &buyer.token, &duplicate).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    let (status, body) = execute(&app, &seller.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("A buyer cannot purchase their own listing.")
    );

    let mut oversell =
        common::checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001");
    oversell["payload"]["lines"][0]["quantity"] = json!(3);
    let (status, body) = execute(&app, &buyer.token, &oversell).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));

    // A partial reservation advances the listing revision; a checkout built
    // against the old revision is stale.
    execute(&app, &buyer.token, &reserve_command(&seller.pubky, 5, 1, 1)).await;
    let stale =
        common::checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001002");
    let (status, body) = execute(&app, &buyer.token, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(2));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

// TS case: checkout of a fully reserved listing is rejected as INVALID_STATE
// ("Only available fixed-price listings can enter checkout.").
#[sqlx::test]
async fn rejects_checkout_of_a_reserved_listing(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (status, _) = execute(
        &app,
        &other_buyer.token,
        &reserve_command(&seller.pubky, 20, 1, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut checkout = checkout_command(&seller.pubky);
    checkout["payload"]["lines"][0]["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &checkout).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("Only available fixed-price listings can enter checkout.")
    );
}

// Server-time reservation expiry (slice requirement; the prototype engine
// stores `expires_at` but never sweeps it — see README divergences).
#[sqlx::test]
async fn expires_due_reservations_and_releases_inventory(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (status, _) = execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::OK);

    // Not due yet: the reservation TTL is 600 seconds of server time.
    let expired = marketplace_service::expiry::expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(expired, 0);

    app.clock.advance_seconds(601);
    let expired = marketplace_service::expiry::expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(expired, 1);

    let (available, reserved, revision, state): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, server_revision, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (available, reserved, revision, state.as_str()),
        (1, 0, 3, "available")
    );
    let (reservation_status,): (String,) =
        sqlx::query_as("SELECT status FROM reservations LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("reservation row exists");
    assert_eq!(reservation_status, "expired");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'inventory.reservation_expired'"
        )
        .await,
        1
    );

    // The sweep is idempotent.
    let expired = marketplace_service::expiry::expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(expired, 0);
}

// The released unit is purchasable again after expiry.
#[sqlx::test]
async fn released_inventory_is_purchasable_after_expiry(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    app.clock.advance_seconds(601);
    marketplace_service::expiry::expire_due_reservations(&app.pool, app.clock.now())
        .await
        .expect("sweep runs");

    let mut checkout = checkout_command(&seller.pubky);
    checkout["payload"]["lines"][0]["expected_revision"] = json!(3);
    let (status, body) = execute(&app, &other_buyer.token, &checkout).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["kind"], json!("checkout"));
}

// Additive contract change: a checkout line may snapshot the buyer's chosen
// variant (id + at most three option dimensions) for fulfillment display.
// The order line echoes it verbatim; variant-less checkouts stay
// byte-identical to before the field existed.
#[sqlx::test]
async fn checkout_snapshots_the_variant_onto_the_order_line(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    let mut command = checkout_command(&seller.pubky);
    command["payload"]["lines"][0]["variant_id"] = json!("variant_forest_m");
    command["payload"]["lines"][0]["variant_options"] = json!([
        { "name": "Size", "value": "M" },
        { "name": "Color", "value": "Forest green" },
    ]);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "variant checkout failed: {body}");
    let line = &body["result"]["orders"][0]["lines"][0];
    assert_eq!(line["variant_id"], json!("variant_forest_m"));
    assert_eq!(
        line["variant_options"],
        json!([
            { "name": "Size", "value": "M" },
            { "name": "Color", "value": "Forest green" },
        ])
    );

    // The stored order row serves the same snapshot on later reads.
    let (lines,): (serde_json::Value,) =
        sqlx::query_as("SELECT lines FROM orders ORDER BY created_at DESC LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    assert_eq!(lines[0]["variant_id"], json!("variant_forest_m"));

    // A variant-less checkout line carries no variant keys at all.
    let plain =
        common::checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001099");
    let mut plain = plain;
    plain["expected_revision"] = json!(0);
    plain["payload"]["lines"][0]["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &plain).await;
    assert_eq!(status, StatusCode::OK, "plain checkout failed: {body}");
    let plain_line = &body["result"]["orders"][0]["lines"][0];
    assert!(plain_line.get("variant_id").is_none());
    assert!(plain_line.get("variant_options").is_none());
}

// Deployment boundary: `payment.sandbox_advance` must be rejected outright
// when SANDBOX_PAYMENTS_ENABLED is off (the production default). The
// client-side transport allowlist is not a security boundary; this is.
#[sqlx::test]
async fn rejects_sandbox_advance_when_sandbox_payments_disabled(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.sandbox_payments_enabled = false;
    let app = common::test_app_with_config(pool, config).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        body["error"]["message"],
        json!("Sandbox payment commands are disabled on this deployment.")
    );

    // Nothing advanced: the payment is untouched and no receipt exists.
    let (state,): (String,) = sqlx::query_as("SELECT state FROM payments WHERE id = $1::uuid")
        .bind(&payment_id)
        .fetch_one(&app.pool)
        .await
        .expect("payment row exists");
    assert_eq!(state, "awaiting_entitlement");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 0);
}
