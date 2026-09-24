//! Digital delivery, service Wave 1 slice 1 (digital-delivery-design.md
//! §2, §4.1, §6 rows A1–A5, B1, B2, B5–B8, C1–C3, D12, §7 inputs). These
//! tests drive the HTTP command surface, the real homeserver client against
//! a local deliverable server, and the sealing module against a real
//! Postgres database.

mod common;

use axum::http::StatusCode;
use marketplace_domain::commands::{DigitalDeliveryKind, FulfillmentMethod};
use marketplace_service::config::Config;
use marketplace_service::digital::{
    self, assert_digital_sealing_coherent, email_aad, pin_aad, reseal_previous_key_batch,
    version_aad, DigitalKeys,
};
use marketplace_service::homeserver::registration_payload_from_record;
use marketplace_service::locks::{LocksKeys, LocksRuntime};
use marketplace_service::pickup::PickupKeys;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use common::{
    create_offer_command, execute, indexed_command_id, listing_aggregate, new_actor, offer_action,
    register_command, send, test_app_with_digital, test_digital_keys, DeliverableServer, TestActor,
    TestApp, OFFER_COMMAND_ID, TEST_DIGITAL_ENCRYPTION_KEY, TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY,
    TEST_LOCKS_ENCRYPTION_KEY, TEST_LOCKS_HMAC_KEY, TEST_PICKUP_ENCRYPTION_KEY,
    TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY,
};

const DELIVERABLE_ID: &str = "4f1c0e7a2b6d4c85a9e3f1027b5d6c38";
const FILE_KEY: &str = "b7c1d2e3f4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0f";
const FILE_IV: &str = "0a1b2c3d4e5f60718293a4b5";
const PLAINTEXT_BLAKE3: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const LINK: &str = "https://drive.example.com/file/d/SECRET-LINK-SENTINEL/view";
const TEXT: &str = "LICENCE-TEXT-SENTINEL-8K2Q-77ZX";

/// A record shaped like the live Shop listing (camelCase, strict specs
/// fields), with the fulfillment array under test.
fn shop_record(seller: &str, listing_id: &str, methods: Value, digital_lock: bool) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "listing",
        "ownerPubky": seller,
        "listingId": listing_id,
        "revision": 1,
        "createdAt": "2026-09-18T08:51:57.956Z",
        "updatedAt": "2026-09-18T08:51:57.956Z",
        "state": "active",
        "title": "Field guide PDF",
        "description": "A digital field guide.",
        "taxonomyVersion": 1,
        "categoryId": "other",
        "condition": "new",
        "tags": ["guide"],
        "location": { "countryCode": "US" },
        "media": [{
            "id": "cover",
            "type": "image",
            "url": format!("pubky://{seller}/pub/pubky.app/marketplace/v1/media/cover"),
            "contentHash": "a".repeat(64),
            "mimeType": "image/png",
            "byteSize": 1,
            "width": 1,
            "height": 1,
            "altText": "Cover"
        }],
        "variants": [{ "id": "v1", "options": {}, "quantity": 5, "mediaIds": ["cover"], "enabled": true }],
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 900, "currency": "USD", "exponent": 2 },
            "acceptsOffers": false
        },
        "fulfillmentMethods": methods,
        "shippingOptions": [],
        "returnPolicy": { "acceptsReturns": false, "buyerPaysReturnShipping": false },
        "adultOnly": false
    });
    if digital_lock {
        record["digitalLock"] = json!({
            "policyUri": format!("pubky://{seller}/pub/locks.app/policies/standard.json"),
            "criterionId": "paykit",
            "resourceHash": "b".repeat(64),
            "minimumConfirmations": 0
        });
    }
    record
}

fn derived(record: &Value, seller: &str, listing_id: &str) -> Vec<FulfillmentMethod> {
    registration_payload_from_record(seller, listing_id, record)
        .expect("record parses")
        .expect("record registers")
        .fulfillment_methods
}

const SELLER_Z32: &str = "n3pfudgxncn8i1e6icuq7umoczemjuyi6xdfrfczk3o8ej3e55my";

#[test]
fn digital_without_lock_derives_digital_never_shipping() {
    let record = shop_record(SELLER_Z32, "guide_01", json!(["digital"]), false);
    assert_eq!(
        derived(&record, SELLER_Z32, "guide_01"),
        vec![FulfillmentMethod::Digital]
    );
}

#[test]
fn locks_listing_derivation_unchanged() {
    for methods in [
        json!(["digital"]),
        json!(["physical", "shipping", "digital"]),
    ] {
        let record = shop_record(SELLER_Z32, "guide_01", methods.clone(), true);
        let registration = registration_payload_from_record(SELLER_Z32, "guide_01", &record);
        // A lock whose policy is not a canonical Locks resource is the
        // existing malformed-lock refusal; the derivation question only
        // arises for a well-formed lock.
        if let Ok(Some(payload)) = registration {
            assert_eq!(
                payload.fulfillment_methods,
                vec![FulfillmentMethod::Shipping],
                "{methods}: digital is skipped for a Locks listing"
            );
        }
    }
}

#[test]
fn mixed_method_derivations() {
    for (methods, expected) in [
        (
            json!(["physical", "shipping", "digital"]),
            vec![FulfillmentMethod::Shipping, FulfillmentMethod::Digital],
        ),
        (
            json!(["pickup", "digital"]),
            vec![FulfillmentMethod::Pickup, FulfillmentMethod::Digital],
        ),
        (
            json!(["physical", "shipping", "pickup", "digital"]),
            vec![
                FulfillmentMethod::Shipping,
                FulfillmentMethod::Pickup,
                FulfillmentMethod::Digital,
            ],
        ),
        (json!(["physical"]), vec![FulfillmentMethod::Shipping]),
        (
            json!(["physical", "shipping", "pickup"]),
            vec![FulfillmentMethod::Shipping, FulfillmentMethod::Pickup],
        ),
    ] {
        let record = shop_record(SELLER_Z32, "guide_01", methods.clone(), false);
        assert_eq!(
            derived(&record, SELLER_Z32, "guide_01"),
            expected,
            "{methods}"
        );
    }
}

fn digital_register(seller: &str, quantity: i64, methods: Value) -> Value {
    let mut command = register_command(seller, quantity);
    command["payload"]["fulfillment_methods"] = methods;
    command
}

fn set_command(seller: &str, expected_version: i64, delivery: Value, n: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0xd100, n),
        "aggregate_id": listing_aggregate(seller),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "digital_delivery.set",
        "payload": { "expected_version": expected_version, "delivery": delivery },
    })
}

fn clear_command(seller: &str, expected_version: i64, n: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0xd200, n),
        "aggregate_id": listing_aggregate(seller),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "digital_delivery.clear",
        "payload": { "expected_version": expected_version },
    })
}

/// A ciphertext of `plaintext_len + 16` bytes carrying a sentinel.
fn ciphertext(plaintext_len: usize) -> Vec<u8> {
    let sentinel = b"CIPHERTEXT-BYTES-SENTINEL-";
    (0..plaintext_len + 16)
        .map(|i| sentinel[i % sentinel.len()])
        .collect()
}

fn file_delivery(version: i64, bytes: &[u8]) -> Value {
    json!({
        "kind": "file",
        "deliverable_id": DELIVERABLE_ID,
        "version": version,
        "key": FILE_KEY,
        "iv": FILE_IV,
        "ciphertext_blake3": blake3::hash(bytes).to_hex().to_string(),
        "plaintext_blake3": PLAINTEXT_BLAKE3,
        "size_bytes": bytes.len() as i64 - 16,
        "content_type": "application/pdf",
        "file_name": "field-guide.pdf",
    })
}

async fn digital_app(pool: PgPool) -> (TestApp, DeliverableServer) {
    test_app_with_digital(pool, Some(test_digital_keys()), Config::for_tests()).await
}

async fn register_digital_listing(app: &TestApp, seller: &TestActor, methods: Value) {
    let (status, body) = execute(
        app,
        &seller.token,
        &digital_register(&seller.pubky, 5, methods),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
}

async fn get(app: &TestApp, token: Option<&str>, uri: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, token, &Value::Null).await
}

/// Every public table column that could hold text or bytes, searched for
/// `needle` (as text in text/json columns, as bytes in bytea columns).
async fn columns_containing(pool: &PgPool, needle: &str) -> Vec<String> {
    let columns: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.table_name, c.column_name, c.data_type FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
         WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' \
           AND c.data_type IN ('text', 'character varying', 'jsonb', 'json', 'bytea', 'ARRAY')",
    )
    .fetch_all(pool)
    .await
    .expect("column catalog");
    let mut hits = Vec::new();
    for (table, column, data_type) in columns {
        let predicate = if data_type == "bytea" {
            format!("position($1::bytea IN \"{column}\") > 0")
        } else {
            format!("strpos(\"{column}\"::text, $2) > 0")
        };
        let (found,): (bool,) = sqlx::query_as(&format!(
            "SELECT EXISTS (SELECT 1 FROM \"{table}\" WHERE {predicate})"
        ))
        .bind(needle.as_bytes())
        .bind(needle)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("scan {table}.{column}: {error}"));
        if found {
            hits.push(format!("{table}.{column}"));
        }
    }
    hits
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn deployed_register_payload_replay(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    // The deployed Shop payload: no `fulfillment_methods` key at all.
    let command = register_command(&seller.pubky, 3);
    assert!(command["payload"].get("fulfillment_methods").is_none());
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (methods,): (Vec<String>,) =
        sqlx::query_as("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    assert_eq!(methods, vec!["shipping".to_string()]);
    assert_eq!(
        body["result"]["listing"].get("digital_delivery"),
        Some(&Value::Null),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_accepts_digital_method(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    for (n, methods) in [
        json!(["digital"]),
        json!(["shipping", "digital"]),
        json!(["shipping", "pickup", "digital"]),
    ]
    .into_iter()
    .enumerate()
    {
        let seller = new_actor(&app).await;
        let mut command = digital_register(&seller.pubky, 2, methods.clone());
        command["command_id"] = json!(indexed_command_id(0xd000, n as u64));
        let (status, body) = execute(&app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "{methods}: {body}");
        let (stored,): (Vec<String>,) =
            sqlx::query_as("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
                .bind(listing_aggregate(&seller.pubky))
                .fetch_one(&app.pool)
                .await
                .expect("listing row");
        assert_eq!(json!(stored), methods);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn auction_with_digital_refused(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let mut command = common::register_auction_command(&seller.pubky);
    command["payload"]["fulfillment_methods"] = json!(["shipping", "digital"]);
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let issues = body["error"]["issues"].to_string();
    assert!(
        issues.contains("Auction listings are shipping-only"),
        "{body}"
    );
}

fn checkout_command(lines: Vec<Value>, with_address: bool, n: u64) -> Value {
    let command_id = indexed_command_id(0xd300, n);
    let mut payload = json!({ "lines": lines, "guarantee_policy_version": 1 });
    if with_address {
        payload["delivery_address"] = json!({
            "name": "Alice Buyer",
            "line1": "1 Market Street",
            "line2": "",
            "city": "New York",
            "region": "NY",
            "postal_code": "10001",
            "country_code": "US",
        });
    }
    json!({
        "version": 1,
        "command_id": command_id,
        "aggregate_id": format!("checkout:{command_id}"),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "checkout.create",
        "payload": payload,
    })
}

fn checkout_line(seller: &str, fulfillment: Option<&str>) -> Value {
    let mut line = json!({
        "listing_aggregate_id": listing_aggregate(seller),
        "expected_revision": 1,
        "quantity": 1,
    });
    if let Some(fulfillment) = fulfillment {
        line["fulfillment"] = json!(fulfillment);
    }
    line
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn deployed_payload_replay_physical(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["shipping", "digital"])).await;
    // The deployed Shop checkout: no `fulfillment`, an address, no email.
    let command = checkout_command(vec![checkout_line(&seller.pubky, None)], true, 1);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (fulfillment,): (String,) = sqlx::query_as("SELECT fulfillment FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("one order");
    assert_eq!(fulfillment, "shipping");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn absent_fulfillment_on_digital_only_refused(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let command = checkout_command(vec![checkout_line(&seller.pubky, None)], true, 2);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
    let (orders,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("order count");
    assert_eq!(orders, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn digital_only_checkout_rejects_address(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let command = checkout_command(vec![checkout_line(&seller.pubky, Some("digital"))], true, 3);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert!(
        body["error"]["issues"]
            .to_string()
            .contains("no shipped line must not carry a delivery address"),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unkeyed_service_reports_unavailable(pool: PgPool) {
    let (keyed, _keyed_server) =
        test_app_with_digital(pool.clone(), Some(test_digital_keys()), Config::for_tests()).await;
    let (_, health) = get(&keyed, None, "/health").await;
    assert_eq!(health["digital_delivery_available"], json!(true));
    assert_eq!(health["digital_delivery_max_bytes"], json!(52_428_800));

    let (app, _server) = test_app_with_digital(pool, None, Config::for_tests()).await;
    let (status, health) = get(&app, None, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["digital_delivery_available"], json!(false));

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    // A1 holds without the key: the listing is digital, never shipping.
    let (methods,): (Vec<String>,) =
        sqlx::query_as("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    assert_eq!(methods, vec!["digital".to_string()]);

    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, json!({ "kind": "text", "text": TEXT }), 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("digital_delivery_unavailable")
    );

    let command = checkout_command(
        vec![checkout_line(&seller.pubky, Some("digital"))],
        false,
        4,
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("digital_delivery_unavailable")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_on_shipping_listing_unchanged(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["error"].get("reason").is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_refused_on_digital_only_listing(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("offers_unavailable_for_digital")
    );

    // A pickup-only listing keeps the existing refusal without a reason.
    let pickup_seller = new_actor(&app).await;
    register_digital_listing(&app, &pickup_seller, json!(["pickup"])).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &create_offer_command(&pickup_seller.pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"].get("reason").is_none(), "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offer_checkout_on_mixed_listing_ships(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    register_digital_listing(&app, &seller, json!(["shipping", "digital"])).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let offer_id = Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id");
    sqlx::query(
        "UPDATE offers SET accepted_listing_record_sha256 = $2, accepted_variant_id = $3 \
         WHERE id = $1",
    )
    .bind(offer_id)
    .bind("a".repeat(64))
    .bind("boots_01")
    .execute(&app.pool)
    .await
    .expect("normalize accepted snapshot fixture");
    let (award_id, listing, revision, hash, quantity): (Uuid, String, i64, String, i64) =
        sqlx::query_as(
            "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
             accepted_listing_record_sha256, accepted_quantity FROM offers WHERE id = $1",
        )
        .bind(offer_id)
        .fetch_one(&app.pool)
        .await
        .expect("accepted offer row");
    let command = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000001102",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": award_id,
            "listing_aggregate_id": listing,
            "listing_revision": revision,
            "listing_record_sha256": hash,
            "variant_id": "boots_01",
            "quantity": quantity,
            "delivery_address": {
                "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                "city": "New York", "region": "NY", "postal_code": "10001", "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (fulfillment,): (String,) = sqlx::query_as("SELECT fulfillment FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("one order");
    assert_eq!(fulfillment, "shipping");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn digital_delivery_set_requires_seller(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let (status, body) = execute(
        &app,
        &other.token,
        &set_command(&seller.pubky, 0, json!({ "kind": "text", "text": TEXT }), 2),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    let (status, _) = execute(&app, &other.token, &clear_command(&seller.pubky, 0, 1)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, json!({ "kind": "text", "text": TEXT }), 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The owner read answers the seller only.
    let uri = format!(
        "/v1/listings/{}/digital-delivery",
        listing_aggregate(&seller.pubky)
    );
    let (status, _) = get(&app, Some(&other.token), &uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, owner) = get(&app, Some(&seller.token), &uri).await;
    assert_eq!(status, StatusCode::OK, "{owner}");
    assert_eq!(owner["current"]["text"], json!(TEXT));
    assert_eq!(owner["last_version"], json!(1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn set_verifies_ciphertext_hash_and_size(pool: PgPool) {
    let mut config = Config::for_tests();
    config.digital_delivery_max_bytes = 4_096;
    let (app, server) = test_app_with_digital(pool, Some(test_digital_keys()), config).await;
    let seller = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;

    let bytes = ciphertext(2_048);
    // Missing on the homeserver.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("deliverable_unverifiable"));

    // Present but not the declared bytes.
    let mut tampered = bytes.clone();
    tampered[0] ^= 1;
    server.put(&seller.pubky, DELIVERABLE_ID, 1, tampered);
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("deliverable_unverifiable"));

    // Longer than declared: the read stops at the declared length.
    let mut longer = bytes.clone();
    longer.extend_from_slice(b"extra");
    server.put(&seller.pubky, DELIVERABLE_ID, 1, longer);
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("deliverable_unverifiable"));

    // Over the deployment cap: refused before any read.
    let requests_before = server.requests();
    let big = ciphertext(4_097);
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &big), 4),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["reason"], json!("deliverable_too_large"));
    assert_eq!(server.requests(), requests_before);

    // The declared bytes verify.
    server.put(&seller.pubky, DELIVERABLE_ID, 1, bytes.clone());
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 5),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["deliverable_id"], json!(DELIVERABLE_ID));
    assert_eq!(body["result"]["version"], json!(1));

    // The public listing projection describes the file, nothing more.
    let (_, listing) = get(
        &app,
        Some(&seller.token),
        &format!("/v1/listings/{}", listing_aggregate(&seller.pubky)),
    )
    .await;
    assert_eq!(
        listing["digital_delivery"],
        json!({ "kind": "file", "content_type": "application/pdf", "size_bytes": 2_048 }),
        "{listing}"
    );
    assert!(!listing.to_string().contains(DELIVERABLE_ID));

    // A stale expected version is a revision conflict carrying the counter.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 6),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["current_revision"], json!(1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn set_result_and_audit_never_carry_secret(pool: PgPool) {
    common::install_log_capture();
    let (app, server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let bytes = ciphertext(512);
    server.put(&seller.pubky, DELIVERABLE_ID, 1, bytes.clone());

    let mut results = Vec::new();
    for (n, (expected, delivery)) in [
        (0, file_delivery(1, &bytes)),
        (1, json!({ "kind": "link", "url": LINK })),
        (2, json!({ "kind": "text", "text": TEXT })),
    ]
    .into_iter()
    .enumerate()
    {
        let (status, body) = execute(
            &app,
            &seller.token,
            &set_command(&seller.pubky, expected, delivery, 10 + n as u64),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let replay = execute(
            &app,
            &seller.token,
            &set_command(
                &seller.pubky,
                expected,
                match n {
                    0 => file_delivery(1, &bytes),
                    1 => json!({ "kind": "link", "url": LINK }),
                    _ => json!({ "kind": "text", "text": TEXT }),
                },
                10 + n as u64,
            ),
        )
        .await;
        assert_eq!(
            replay.0,
            StatusCode::OK,
            "exact replay returns the stored result"
        );
        results.push(body);
        results.push(replay.1);
    }
    // A refusal carrying a secret payload.
    let (status, _) = execute(
        &app,
        &seller.token,
        &set_command(
            &seller.pubky,
            0,
            json!({ "kind": "text", "text": TEXT }),
            20,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    for secret in [FILE_KEY, FILE_IV, LINK, TEXT, "field-guide.pdf"] {
        for body in &results {
            assert!(!body.to_string().contains(secret), "{secret} in {body}");
        }
        let hits = columns_containing(&app.pool, secret).await;
        assert!(hits.is_empty(), "{secret} stored in plaintext at {hits:?}");
        assert!(
            !common::captured_logs().contains(secret),
            "{secret} reached the logs"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn setup_fetch_bytes_discarded(pool: PgPool) {
    let (app, server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let bytes = ciphertext(8_192);
    server.put(&seller.pubky, DELIVERABLE_ID, 1, bytes.clone());
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 0, file_delivery(1, &bytes), 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(server.requests(), 1, "the ciphertext is read exactly once");
    let hits = columns_containing(&app.pool, "CIPHERTEXT-BYTES-SENTINEL-").await;
    assert!(hits.is_empty(), "ciphertext bytes stored at {hits:?}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn new_version_becomes_current_and_clear_keeps_the_counter(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let seller = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let uri = format!(
        "/v1/listings/{}/digital-delivery",
        listing_aggregate(&seller.pubky)
    );
    for (expected, text) in [(0, "first"), (1, "second")] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &set_command(
                &seller.pubky,
                expected,
                json!({ "kind": "text", "text": text }),
                30 + expected as u64,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (_, owner) = get(&app, Some(&seller.token), &uri).await;
    assert_eq!(owner["current"]["text"], json!("second"));
    assert_eq!(owner["current"]["version"], json!(2));
    let (versions,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM listing_digital_versions WHERE listing_aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("version count");
    assert_eq!(versions, 1, "an unpinned superseded version is deleted");

    // A kind change with no buyer is allowed.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 2, json!({ "kind": "email" }), 40),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(&app, &seller.token, &clear_command(&seller.pubky, 3, 2)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, owner) = get(&app, Some(&seller.token), &uri).await;
    assert_eq!(owner["current"], Value::Null);
    assert_eq!(owner["last_version"], json!(3));
    let (kind,): (Option<String>,) =
        sqlx::query_as("SELECT digital_delivery_kind FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    assert_eq!(kind, None);
    // Versions never restart after a clear.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_command(&seller.pubky, 3, json!({ "kind": "message" }), 41),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["version"], json!(4));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seal_rows_cannot_be_transplanted(pool: PgPool) {
    let (app, _server) = digital_app(pool).await;
    let keys = test_digital_keys();
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    for seller in [&first, &second] {
        register_digital_listing(&app, seller, json!(["digital"])).await;
        let (status, body) = execute(
            &app,
            &seller.token,
            &set_command(
                &seller.pubky,
                0,
                json!({ "kind": "text", "text": TEXT }),
                50,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    // Move the first listing's sealed payload onto the second listing.
    sqlx::query(
        "UPDATE listing_digital_versions SET payload_ciphertext = \
           (SELECT payload_ciphertext FROM listing_digital_versions WHERE listing_aggregate_id = $1) \
         WHERE listing_aggregate_id = $2",
    )
    .bind(listing_aggregate(&first.pubky))
    .bind(listing_aggregate(&second.pubky))
    .execute(&app.pool)
    .await
    .expect("transplant");
    let uri = format!(
        "/v1/listings/{}/digital-delivery",
        listing_aggregate(&second.pubky)
    );
    let (status, body) = get(&app, Some(&second.token), &uri).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(!body.to_string().contains(TEXT));

    // Every field of every family's associated data binds the row.
    let order = Uuid::new_v4();
    let listing = "listing:seller_guide";
    let id = DELIVERABLE_ID;
    let other_id = "0123456789abcdef0123456789abcdef";
    let sealed_version = keys.seal(
        &version_aad(listing, id, 1, DigitalDeliveryKind::File),
        b"v",
    );
    for aad in [
        version_aad("listing:seller_other", id, 1, DigitalDeliveryKind::File),
        version_aad(listing, other_id, 1, DigitalDeliveryKind::File),
        version_aad(listing, id, 2, DigitalDeliveryKind::File),
        version_aad(listing, id, 1, DigitalDeliveryKind::Link),
    ] {
        keys.open(&aad, &sealed_version)
            .expect_err("moved version row opens");
    }
    let sealed_pin = keys.seal(&pin_aad(order, 0, listing, id, 1), b"p");
    for aad in [
        pin_aad(Uuid::new_v4(), 0, listing, id, 1),
        pin_aad(order, 1, listing, id, 1),
        pin_aad(order, 0, "listing:seller_other", id, 1),
        pin_aad(order, 0, listing, other_id, 1),
        pin_aad(order, 0, listing, id, 2),
        version_aad(listing, id, 1, DigitalDeliveryKind::File),
    ] {
        keys.open(&aad, &sealed_pin)
            .expect_err("moved pin row opens");
    }
    let sealed_email = keys.seal(&email_aad(order, "buyer_one"), b"e");
    for aad in [
        email_aad(Uuid::new_v4(), "buyer_one"),
        email_aad(order, "buyer_two"),
    ] {
        keys.open(&aad, &sealed_email)
            .expect_err("moved email row opens");
    }
}

fn locks_runtime() -> LocksRuntime {
    LocksRuntime {
        keys: LocksKeys::from_hex(TEST_LOCKS_ENCRYPTION_KEY, TEST_LOCKS_HMAC_KEY)
            .expect("locks keys"),
        client: Arc::new(common::FakeLocksClient::default()),
    }
}

#[test]
fn digital_key_must_differ_from_every_other_key() {
    let locks = locks_runtime();
    let pickup = PickupKeys::from_hex(
        TEST_PICKUP_ENCRYPTION_KEY,
        Some(TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("pickup keys");
    let distinct = DigitalKeys::from_hex(
        TEST_DIGITAL_ENCRYPTION_KEY,
        Some(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("digital keys");
    digital::ensure_distinct_keys(&distinct, Some(&locks), Some(&pickup))
        .expect("distinct keys pass");
    for (current, previous) in [
        (TEST_LOCKS_ENCRYPTION_KEY, None),
        (TEST_LOCKS_HMAC_KEY, None),
        (TEST_PICKUP_ENCRYPTION_KEY, None),
        (TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY, None),
        (TEST_DIGITAL_ENCRYPTION_KEY, Some(TEST_LOCKS_ENCRYPTION_KEY)),
        (
            TEST_DIGITAL_ENCRYPTION_KEY,
            Some(TEST_PICKUP_ENCRYPTION_KEY),
        ),
    ] {
        let keys = DigitalKeys::from_hex(current, previous).expect("keys parse");
        digital::ensure_distinct_keys(&keys, Some(&locks), Some(&pickup))
            .expect_err("an aliased key is refused");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn boot_probe_and_rotation_cover_all_three_families(pool: PgPool) {
    let previous_only =
        DigitalKeys::from_hex(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY, None).expect("previous key");
    let rotated = DigitalKeys::from_hex(
        TEST_DIGITAL_ENCRYPTION_KEY,
        Some(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("rotated keys");
    let current_only = DigitalKeys::from_hex(TEST_DIGITAL_ENCRYPTION_KEY, None).expect("current");

    // An empty store boots with or without a key.
    assert_digital_sealing_coherent(&pool, None)
        .await
        .expect("empty store boots unkeyed");

    // One row per family, sealed under the previous key.
    let (app, _server) = digital_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_digital_listing(&app, &seller, json!(["digital"])).await;
    let listing = listing_aggregate(&seller.pubky);
    // Any real order row satisfies the pin and email foreign keys.
    let shipper = new_actor(&app).await;
    execute(&app, &shipper.token, &register_command(&shipper.pubky, 2)).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command(vec![checkout_line(&shipper.pubky, None)], true, 60),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_id,): (Uuid,) = sqlx::query_as("SELECT id FROM orders")
        .fetch_one(&pool)
        .await
        .expect("order id");
    sqlx::query(
        "INSERT INTO listing_digital_versions (listing_aggregate_id, seller_pubky, deliverable_id, \
         version, kind, payload_ciphertext, created_at) VALUES ($1, $2, $3, 1, 'text', $4, now())",
    )
    .bind(&listing)
    .bind(&seller.pubky)
    .bind(DELIVERABLE_ID)
    .bind(previous_only.seal(
        &version_aad(&listing, DELIVERABLE_ID, 1, DigitalDeliveryKind::Text),
        br#"{"text":"t"}"#,
    ))
    .execute(&pool)
    .await
    .expect("seed version");
    sqlx::query(
        "INSERT INTO order_digital_pins (order_id, line_index, listing_aggregate_id, \
         deliverable_id, version, kind, payload_ciphertext, confirming_adapter, created_at) \
         VALUES ($1, 0, $2, $3, 1, 'text', $4, 'paykit', now())",
    )
    .bind(order_id)
    .bind(&listing)
    .bind(DELIVERABLE_ID)
    .bind(previous_only.seal(&pin_aad(order_id, 0, &listing, DELIVERABLE_ID, 1), b"{}"))
    .execute(&pool)
    .await
    .expect("seed pin");
    sqlx::query(
        "INSERT INTO order_delivery_emails (order_id, buyer_pubky, email_ciphertext, created_at, \
         updated_at) VALUES ($1, $2, $3, now(), now())",
    )
    .bind(order_id)
    .bind(&buyer.pubky)
    .bind(previous_only.seal(&email_aad(order_id, &buyer.pubky), b"buyer@example.com"))
    .execute(&pool)
    .await
    .expect("seed email");

    assert_digital_sealing_coherent(&pool, None)
        .await
        .expect_err("sealed rows without a key fail the boot");
    assert_digital_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect_err("a wrong key fails the boot");
    let scans = assert_digital_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect("the dual-key window boots");
    assert_eq!(scans.len(), 3);
    assert!(scans.iter().all(|(_, scan)| scan.straggler_class));

    let progress = reseal_previous_key_batch(&pool, &rotated)
        .await
        .expect("re-seal pass");
    assert_eq!(
        (
            progress.versions_resealed,
            progress.pins_resealed,
            progress.emails_resealed
        ),
        (1, 1, 1)
    );
    assert_eq!(progress.remaining_under_previous, 0);
    assert_digital_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect("after rotation the current key alone boots");

    // A row under neither key fails the pass without stalling the others.
    sqlx::query("UPDATE order_delivery_emails SET email_ciphertext = $1")
        .bind(
            DigitalKeys::from_hex(&"a".repeat(64), None)
                .expect("stray")
                .seal(b"x", b"y"),
        )
        .execute(&pool)
        .await
        .expect("corrupt email");
    reseal_previous_key_batch(&pool, &rotated)
        .await
        .expect_err("an unopenable row fails the pass");
}

async fn seed_version(pool: &PgPool, listing: &str, keys: &DigitalKeys) {
    sqlx::query(
        "INSERT INTO listing_digital_versions (listing_aggregate_id, seller_pubky, deliverable_id, \
         version, kind, payload_ciphertext, created_at) VALUES ($1, 's', $2, 1, 'text', $3, now())",
    )
    .bind(listing)
    .bind(DELIVERABLE_ID)
    .bind(keys.seal(
        &version_aad(listing, DELIVERABLE_ID, 1, DigitalDeliveryKind::Text),
        br#"{"text":"t"}"#,
    ))
    .execute(pool)
    .await
    .expect("seed version");
}

// Review P1 (slice 1): during a rotation, a row sealed under neither key
// that sits after a valid previous-key row in the sampled range must still
// fail the boot.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn boot_probe_authenticates_every_sampled_row(pool: PgPool) {
    let current = DigitalKeys::from_hex(TEST_DIGITAL_ENCRYPTION_KEY, None).expect("current");
    let previous =
        DigitalKeys::from_hex(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY, None).expect("previous");
    let unrelated = DigitalKeys::from_hex(&"a".repeat(64), None).expect("unrelated");
    let rotated = DigitalKeys::from_hex(
        TEST_DIGITAL_ENCRYPTION_KEY,
        Some(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("rotated");
    seed_version(&pool, "listing:probe_1", &current).await;
    seed_version(&pool, "listing:probe_2", &previous).await;
    assert_digital_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect("current and previous rows boot under the rotation window");
    seed_version(&pool, "listing:probe_3", &unrelated).await;
    assert_digital_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect_err("a row under neither key fails the boot");
}

struct ChangeEmailBeforeWrite {
    pool: PgPool,
    order_id: Uuid,
    buyer: String,
}

impl digital::ResealHook for ChangeEmailBeforeWrite {
    fn before_write<'a>(
        &'a self,
        family: digital::SealedFamily,
        _row_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if family != digital::SealedFamily::Email {
                return;
            }
            let current =
                DigitalKeys::from_hex(TEST_DIGITAL_ENCRYPTION_KEY, None).expect("current");
            sqlx::query(
                "UPDATE order_delivery_emails SET email_ciphertext = $2 WHERE order_id = $1",
            )
            .bind(self.order_id)
            .bind(current.seal(&email_aad(self.order_id, &self.buyer), b"new@example.com"))
            .execute(&self.pool)
            .await
            .expect("concurrent email change");
        })
    }
}

// Review P2 (slice 1): a buyer's address change that commits while the
// re-seal pass holds the old ciphertext is not overwritten.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn reseal_does_not_overwrite_a_concurrent_email_change(pool: PgPool) {
    let (app, _server) = digital_app(pool.clone()).await;
    let shipper = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &shipper.token, &register_command(&shipper.pubky, 2)).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command(vec![checkout_line(&shipper.pubky, None)], true, 70),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_id,): (Uuid,) = sqlx::query_as("SELECT id FROM orders")
        .fetch_one(&pool)
        .await
        .expect("order id");
    let previous =
        DigitalKeys::from_hex(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY, None).expect("previous");
    sqlx::query(
        "INSERT INTO order_delivery_emails (order_id, buyer_pubky, email_ciphertext, created_at, \
         updated_at) VALUES ($1, $2, $3, now(), now())",
    )
    .bind(order_id)
    .bind(&buyer.pubky)
    .bind(previous.seal(&email_aad(order_id, &buyer.pubky), b"old@example.com"))
    .execute(&pool)
    .await
    .expect("seed email");
    let rotated = DigitalKeys::from_hex(
        TEST_DIGITAL_ENCRYPTION_KEY,
        Some(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("rotated");
    let hook = ChangeEmailBeforeWrite {
        pool: pool.clone(),
        order_id,
        buyer: buyer.pubky.clone(),
    };
    let progress = digital::reseal_previous_key_batch_with_hook(&pool, &rotated, &hook)
        .await
        .expect("re-seal pass");
    assert_eq!((progress.emails_resealed, progress.skipped_changed), (0, 1));
    assert_eq!(progress.remaining_under_previous, 0);
    let (sealed,): (Vec<u8>,) =
        sqlx::query_as("SELECT email_ciphertext FROM order_delivery_emails WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("email row");
    let current = DigitalKeys::from_hex(TEST_DIGITAL_ENCRYPTION_KEY, None).expect("current");
    assert_eq!(
        current
            .open(&email_aad(order_id, &buyer.pubky), &sealed)
            .expect("opens under current"),
        b"new@example.com",
        "the buyer's newer address survives the rotation"
    );
}

// Kimi P1: the pins and the access log are the entitlement and delivery
// evidence. Neither can be deleted; access rows never change; a pin changes
// only its ciphertext (the key-rotation re-seal).
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn digital_evidence_is_append_only(pool: PgPool) {
    let (app, _server) = digital_app(pool.clone()).await;
    let shipper = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &shipper.token, &register_command(&shipper.pubky, 2)).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command(vec![checkout_line(&shipper.pubky, None)], true, 80),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (order_id,): (Uuid,) = sqlx::query_as("SELECT id FROM orders")
        .fetch_one(&pool)
        .await
        .expect("order id");
    sqlx::query(
        "INSERT INTO order_digital_pins (order_id, line_index, listing_aggregate_id, \
         deliverable_id, version, kind, payload_ciphertext, confirming_adapter, created_at) \
         VALUES ($1, 0, 'listing:x', $2, 1, 'text', '\\x01'::bytea, 'paykit', now())",
    )
    .bind(order_id)
    .bind(DELIVERABLE_ID)
    .execute(&pool)
    .await
    .expect("pin insert");
    sqlx::query(
        "INSERT INTO order_digital_access (order_id, line_index, accessed_at) VALUES ($1, 0, now())",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("access insert");
    for (statement, what) in [
        (
            "UPDATE order_digital_access SET accessed_at = now() - interval '1 day'",
            "access update",
        ),
        ("DELETE FROM order_digital_access", "access delete"),
        ("DELETE FROM order_digital_pins", "pin delete"),
        (
            "UPDATE order_digital_pins SET version = 2",
            "pin version change",
        ),
        (
            "UPDATE order_digital_pins SET confirming_adapter = 'sandbox'",
            "pin adapter change",
        ),
        (
            "UPDATE order_digital_pins SET listing_aggregate_id = 'listing:y'",
            "pin transplant",
        ),
    ] {
        sqlx::query(statement).execute(&pool).await.expect_err(what);
    }
    sqlx::query("UPDATE order_digital_pins SET payload_ciphertext = '\\x02'::bytea")
        .execute(&pool)
        .await
        .expect("the re-seal may rewrite the ciphertext");
}
