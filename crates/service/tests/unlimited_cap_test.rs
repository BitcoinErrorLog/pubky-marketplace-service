//! The unlimited digital stock cap (digital delivery design §2 "Stock"):
//! Shop stores Unlimited as a quantity of 1,000,000, so a listing that ships
//! or offers pickup cannot register it, on any path.

mod common;

use axum::http::StatusCode;
use common::*;
use marketplace_domain::commands::{UNLIMITED_STOCK_ON_PHYSICAL_LISTING, UNLIMITED_STOCK_QUANTITY};
use marketplace_service::config::Config;
use serde_json::{json, Value};
use sqlx::PgPool;

fn register(seller: &str, listing_id: &str, methods: Value, quantity: i64, index: u64) -> Value {
    let mut command = register_command(seller, quantity);
    command["command_id"] = json!(indexed_command_id(0xca90, index));
    command["aggregate_id"] = json!(format!("listing:{seller}_{listing_id}"));
    command["payload"]["listing_id"] = json!(listing_id);
    command["payload"]["fulfillment_methods"] = methods;
    command
}

fn assert_cap_refused(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_refuses_the_unlimited_cap_on_a_listing_that_ships_or_offers_pickup(pool: PgPool) {
    let (app, _server) =
        test_app_with_digital(pool, Some(test_digital_keys()), Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let physical = [
        json!(["shipping"]),
        json!(["pickup"]),
        json!(["shipping", "pickup"]),
        json!(["shipping", "digital"]),
        json!(["pickup", "digital"]),
    ];
    for (index, methods) in physical.into_iter().enumerate() {
        let listing_id = format!("cap_{index}");
        let command = register(
            &seller.pubky,
            &listing_id,
            methods.clone(),
            UNLIMITED_STOCK_QUANTITY,
            index as u64,
        );
        let (status, body) = execute(&app, &seller.token, &command).await;
        assert_cap_refused(status, &body);
        let finite = register(
            &seller.pubky,
            &format!("finite_{index}"),
            methods,
            UNLIMITED_STOCK_QUANTITY - 1,
            100 + index as u64,
        );
        let (status, body) = execute(&app, &seller.token, &finite).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    // Digital-only is how Unlimited is stored.
    let digital = register(
        &seller.pubky,
        "guide_01",
        json!(["digital"]),
        UNLIMITED_STOCK_QUANTITY,
        200,
    );
    let (status, body) = execute(&app, &seller.token, &digital).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_and_sync_many_refuse_a_physical_record_at_the_unlimited_cap(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    // The harness mirrors each register command into the seller's record, so
    // the refused register still leaves a physical record at the cap on the
    // homeserver, as Inventory Studio or another client could publish it.
    let command = register(
        &seller.pubky,
        "boots_09",
        json!(["shipping"]),
        UNLIMITED_STOCK_QUANTITY,
        300,
    );
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_cap_refused(status, &body);

    let sync = json!({
        "version": 1,
        "command_id": indexed_command_id(0xca91, 1),
        "aggregate_id": format!("listing:{}_boots_09", seller.pubky),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "listing.sync",
        "payload": { "seller_pubky": seller.pubky, "listing_id": "boots_09" }
    });
    let (status, body) = execute(&app, &seller.token, &sync).await;
    assert_cap_refused(status, &body);

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "boots_09"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    let item = &body["results"][0];
    assert_eq!(item["status"], json!(409), "{body}");
    assert_eq!(
        item["result"]["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/listing:{}_boots_09", seller.pubky),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "nothing was registered: {body}"
    );
}

/// A Locks listing record at the cap, publishing `published`.
fn locks_record(seller: &str, listing_id: &str, published: Value, lock: &Value) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "ownerPubky": seller,
        "listingId": listing_id,
        "title": "Premium archive",
        "revision": 1,
        "media": [{"contentHash": "a".repeat(64)}],
        "variants": [{
            "id": "variant_1",
            "enabled": true,
            "quantity": UNLIMITED_STOCK_QUANTITY,
            "sku": null,
            "options": []
        }],
        "shippingOptions": [{"pricing": "free"}],
        "fulfillmentMethods": published,
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 12_500, "currency": "USD", "exponent": 2}
        },
        "digitalLock": lock
    })
}

fn locks_lock(seller: &str) -> Value {
    json!({"policyUri": lock_resource_for(seller), "criterionId": "paykit"})
}

// Sol round 3 service P1: a Locks lock exempts the cap only when the seller's
// record publishes digital alone. The Locks derivation registers both as
// shipping, so the record decides.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_decides_a_locks_listing_at_the_cap_from_the_seller_record(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);
    let cases = [
        (
            "reveal_digital",
            json!(["digital"]),
            json!(["shipping"]),
            true,
        ),
        (
            "reveal_ship",
            json!(["physical", "shipping", "digital"]),
            json!(["shipping"]),
            false,
        ),
        (
            "reveal_pickup",
            json!(["pickup", "digital"]),
            json!(["pickup"]),
            false,
        ),
    ];
    for (index, (listing_id, published, registered, accepted)) in cases.into_iter().enumerate() {
        put_command_mirror_record(
            &seller.pubky,
            listing_id,
            locks_record(&seller.pubky, listing_id, published, &lock),
        );
        let mut command = register(
            &seller.pubky,
            listing_id,
            registered,
            UNLIMITED_STOCK_QUANTITY,
            400 + index as u64,
        );
        command["payload"]["digital_lock"] = lock.clone();
        // `send`, not `execute`: the record above is the seller's, not a mirror of the command.
        let (status, body) = send(
            app.router.clone(),
            "POST",
            "/v1/commands",
            Some(&seller.token),
            &command,
        )
        .await;
        if accepted {
            assert_eq!(status, StatusCode::OK, "{listing_id}: {body}");
        } else {
            assert_cap_refused(status, &body);
        }
    }

    // A Locks register at the cap whose record cannot be read is refused, not trusted.
    let mut missing = register(
        &seller.pubky,
        "reveal_missing",
        json!(["shipping"]),
        UNLIMITED_STOCK_QUANTITY,
        410,
    );
    missing["payload"]["digital_lock"] = lock.clone();
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some(&seller.token),
        &missing,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_decides_a_locks_listing_at_the_cap_from_the_seller_record(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);
    let sync = |listing_id: &str, index: u64| {
        json!({
            "version": 1,
            "command_id": indexed_command_id(0xca92, index),
            "aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "listing.sync",
            "payload": { "seller_pubky": seller.pubky, "listing_id": listing_id }
        })
    };

    put_command_mirror_record(
        &seller.pubky,
        "reveal_digital",
        locks_record(&seller.pubky, "reveal_digital", json!(["digital"]), &lock),
    );
    let (status, body) = execute(&app, &seller.token, &sync("reveal_digital", 1)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "digital-only Locks keeps the cap: {body}"
    );

    for (index, (listing_id, published)) in [
        ("reveal_ship", json!(["physical", "shipping", "digital"])),
        ("reveal_pickup", json!(["pickup", "digital"])),
    ]
    .into_iter()
    .enumerate()
    {
        put_command_mirror_record(
            &seller.pubky,
            listing_id,
            locks_record(&seller.pubky, listing_id, published, &lock),
        );
        let (status, body) =
            execute(&app, &seller.token, &sync(listing_id, 2 + index as u64)).await;
        assert_cap_refused(status, &body);
    }

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "reveal_ship"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(409), "{body}");
    assert_eq!(
        body["results"][0]["result"]["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );
}
