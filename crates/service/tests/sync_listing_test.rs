//! `listing.sync` tests: service-side registration from the canonical
//! homeserver record, issuable by ANY authenticated actor. The homeserver is
//! a real local HTTP listener serving camelCase records at the production
//! path, reached through the production `HttpHomeserverClient` — header
//! handling, 404 mapping, and lenient record parsing are all exercised for
//! real. Covered: a buyer's happy sync creating a fresh aggregate, the
//! convergent no-op replays (same record, and an aggregate already ahead of
//! the record), the definitive 404, the retriable unreachable-homeserver
//! failure, and the committed-inventory invariant that survives both
//! registration paths.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    execute, new_actor, register_command, send, sync_command, test_app, test_app_with_homeserver,
    test_app_with_homeserver_client, TestApp,
};
use marketplace_service::homeserver::HttpHomeserverClient;
use std::sync::Arc;

const LISTING_ID: &str = "boots_01";

/// A canonical camelCase homeserver listing record, shaped like the live
/// records the reference client publishes (nulls for optional fields,
/// unknown fields present).
fn homeserver_record(revision: i64, quantities: &[i64]) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "title": "Winter boots",
        "revision": revision,
        "location": { "countryCode": "US", "region": null },
        "media": [{
            "id": "media_01",
            "type": "image",
            "mimeType": "image/jpeg",
            "contentHash": "a".repeat(64),
            "byteSize": 999_533,
            "unknownFutureField": true,
        }],
        "variants": quantities.iter().enumerate().map(|(index, quantity)| json!({
            "id": format!("variant_{index}"),
            "enabled": index % 2 == 0,
            "quantity": quantity,
            "sku": null,
            "priceOverride": null,
        })).collect::<Vec<_>>(),
        "shippingOptions": [
            {
                "id": "ship_flat",
                "pricing": "flat",
                "label": "Seller shipping",
                "price": { "amountMinor": 500, "currency": "USD", "exponent": 2 },
                "estimatedMinDays": 2,
                "estimatedMaxDays": 7,
            },
            {
                "id": "ship_calc",
                "pricing": "calculated",
                "label": "Carrier calculated",
                "provider": "ups",
                "serviceCode": "ground",
                "estimatedMinDays": 2,
                "estimatedMaxDays": 7,
            },
        ],
        "sale": {
            "acceptsOffers": true,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

async fn read_listing(app: &TestApp, token: &str, aggregate_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{aggregate_id}"),
        Some(token),
        &json!(null),
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn a_buyer_sync_registers_an_unregistered_listing(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // The client sums ALL variants' quantities, enabled or not: 2 + 3 = 5.
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(1, &[2, 3]));

    let aggregate_id = format!("listing:{}_{LISTING_ID}", seller.pubky);
    let (status, body) = read_listing(&app, &buyer.token, &aggregate_id).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unregistered before sync: {body}"
    );

    // The buyer — NOT the seller — heals the listing.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync failed: {body}");
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["revision"], json!(1));
    let listing = &body["result"]["listing"];
    assert_eq!(listing["seller_pubky"], json!(seller.pubky));
    assert_eq!(listing["title"], json!("Winter boots"));
    assert_eq!(listing["listing_revision"], json!(1));
    assert_eq!(listing["total_quantity"], json!(5));
    assert_eq!(listing["available_quantity"], json!(5));
    assert_eq!(listing["state"], json!("available"));
    assert_eq!(listing["unit_price"]["amount_minor"], json!(12_500));
    // The cheapest PRICEABLE seller-signed option: the $5 flat rate (the
    // calculated option cannot be priced here and is skipped).
    assert_eq!(listing["shipping"]["amount_minor"], json!(500));
    assert_eq!(listing["sale_format"], json!("fixed_price"));

    let (status, body) = read_listing(&app, &buyer.token, &aggregate_id).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "projection readable after sync: {body}"
    );
    assert_eq!(body["server_revision"], json!(1));

    // The audit trail names the sync, not a seller registration.
    let (kind, actor_pubky): (String, String) =
        sqlx::query_as("SELECT kind, actor_pubky FROM events WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("one event recorded");
    assert_eq!(kind, "listing.synced");
    assert_eq!(actor_pubky, buyer.pubky);
}

#[sqlx::test(migrations = "./migrations")]
async fn re_syncing_the_same_record_is_a_no_op_success(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(1, &[1]));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first sync failed: {body}");
    assert_eq!(body["revision"], json!(1));

    // A fresh command id (not an idempotent replay) converges to the same
    // state: success, current revision, nothing rewritten, no new event.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-sync failed: {body}");
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["revision"], json!(1));
    assert_eq!(body["event_ids"], json!([]));

    let events = common::count(&app.pool, "SELECT COUNT(*) FROM events").await;
    assert_eq!(events, 1, "the no-op re-sync must not append events");
}

#[sqlx::test(migrations = "./migrations")]
async fn a_stale_record_never_regresses_a_newer_aggregate(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // The seller registered normally (listing_revision 1, quantity 1)...
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    // ...and the homeserver still serves that same record revision.
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(1, &[7]));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "stale-record sync must not regress: {body}"
    );
    assert_eq!(body["ok"], json!(true));
    // The aggregate keeps the registered quantity, not the record's 7 —
    // inventory never moves on an equal-revision sync.
    assert_eq!(body["result"]["listing"]["total_quantity"], json!(1));
    // But the seller-signed record's shipping ($5 flat) HEALS the
    // registration-supplied $12: shipping is a derived, inventory-neutral
    // term and the record is the canonical source for it.
    assert_eq!(
        body["result"]["listing"]["shipping"]["amount_minor"],
        json!(500)
    );
    assert_eq!(body["revision"], json!(2));

    // A second sync of the same record converges: nothing left to heal.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-sync failed: {body}");
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["event_ids"], json!([]));
}

#[sqlx::test(migrations = "./migrations")]
async fn a_newer_record_refreshes_the_aggregate(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(2, &[4]));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "advancing sync failed: {body}");
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["result"]["listing"]["listing_revision"], json!(2));
    assert_eq!(body["result"]["listing"]["total_quantity"], json!(4));
}

#[sqlx::test(migrations = "./migrations")]
async fn a_missing_homeserver_record_is_a_definitive_not_found(pool: PgPool) {
    let (app, _homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(
        body["error"]["message"],
        json!("The seller's homeserver has no such listing record.")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn an_unreachable_homeserver_is_a_distinct_retriable_failure(pool: PgPool) {
    // Nothing listens on this client's target port.
    let unreachable =
        Arc::new(HttpHomeserverClient::new("http://127.0.0.1:9").expect("client builds"));
    let app = test_app_with_homeserver_client(pool, unreachable).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("UPSTREAM_UNAVAILABLE"));
}

#[sqlx::test(migrations = "./migrations")]
async fn sync_is_refused_where_no_homeserver_is_configured(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(
        body["error"]["message"],
        json!("Listing sync is not enabled on this deployment.")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn sync_still_enforces_the_committed_inventory_invariant(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // Two units registered and both committed through a real reservation.
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::reserve_command(&seller.pubky, 0, 2, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reserve fixture failed: {body}");

    // A newer record shrinking the quantity below committed inventory is
    // refused, exactly as `listing.register` would refuse it.
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(2, &[1]));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
    assert_eq!(
        body["error"]["message"],
        json!("Listing quantity cannot fall below committed inventory.")
    );

    // A record that zeroes the quantity entirely fails the registration
    // payload floor (quantity must be 1..=1,000,000) before any write.
    homeserver.put_record(&seller.pubky, LISTING_ID, homeserver_record(2, &[0]));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, LISTING_ID, 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The seller's listing record does not satisfy registration invariants.")
    );
}
