//! Listing tombstones. The homeserver is a real local listener serving the
//! record path and a batch-mode `/events-stream` in the captured staging
//! format, reached through the production `HttpHomeserverClient`.
//!
//! Covered: `listing.sync` (any actor, and `sync-many`) tombstoning a
//! listing whose record the seller deleted; a bare 404 without a homeserver
//! `DEL` leaving the listing alone; a tombstoned listing leaving public
//! reads, seller lists, inventory, checkout, payment holds, offers,
//! reserves, and bids while a paid order still resolves; a deleted auction
//! closing unsold; re-creation at the same id reviving with the new
//! record's stock only; and the worker following deletes with no sync.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    checkout_command, close_auction_command, create_offer_command, create_paid_order, execute,
    listing_aggregate, new_actor, offer_action, order_command, payment_command, place_bid_command,
    register_auction_command, register_command, reserve_command, send, sync_command, test_app,
    test_app_with_homeserver, FakeHomeserver, TestActor, TestApp, OFFER_COMMAND_ID,
};
use marketplace_service::clock::Clock;

const LISTING_ID: &str = "boots_01";

/// A seller-signed fixed-price record at the production shape the offer
/// snapshot and sync derivation both read.
fn record(revision: i64, quantity: i64) -> Value {
    json!({
        "title": "Winter boots",
        "revision": revision,
        "location": { "countryCode": "US", "region": null },
        "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
        "variants": [{
            "id": "variant_1",
            "enabled": true,
            "quantity": quantity,
            "sku": null,
            "options": [],
        }],
        "shippingOptions": [{ "pricing": "free" }],
        "fulfillmentMethods": ["shipping"],
        "sale": {
            "acceptsOffers": true,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

async fn get(app: &TestApp, token: &str, uri: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

async fn tombstone_row(pool: &PgPool, aggregate_id: &str) -> (Option<String>, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT deleted_event_cursor, total_quantity, available_quantity, reserved_quantity, \
         sold_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("listing row is retained")
}

async fn deleted_events(pool: &PgPool, aggregate_id: &str) -> Vec<(i64, String)> {
    sqlx::query_as(
        "SELECT revision, actor_pubky FROM events \
         WHERE aggregate_id = $1 AND kind = 'listing.deleted' ORDER BY revision",
    )
    .bind(aggregate_id)
    .fetch_all(pool)
    .await
    .expect("deletion events")
}

/// Registers `boots_01` from a homeserver record the seller published.
async fn published_listing(
    app: &TestApp,
    homeserver: &FakeHomeserver,
    seller: &TestActor,
    quantity: i64,
) {
    homeserver.put_record(&seller.pubky, LISTING_ID, record(1, quantity));
    let mut register = register_command(&seller.pubky, quantity);
    register["command_id"] = json!(Uuid::new_v4());
    let (status, body) = execute(app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");
}

async fn sync_as(
    app: &TestApp,
    actor: &TestActor,
    seller: &str,
    number: u64,
) -> (StatusCode, Value) {
    execute(app, &actor.token, &sync_command(seller, LISTING_ID, number)).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_tombstones_a_listing_its_seller_deleted_on_the_homeserver(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    let (status, _) = get(&app, &buyer.token, &format!("/v1/listings/{aggregate_id}")).await;
    assert_eq!(status, StatusCode::OK);

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    // Any authenticated actor may converge the service on the record.
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "sync after delete: {body}");
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));
    let deleted = &body["result"]["listing"];
    assert_eq!(deleted["aggregate_id"], json!(aggregate_id));
    assert_eq!(deleted["server_revision"], json!(2));
    assert!(deleted["deleted_at"].is_string(), "{deleted}");
    assert!(deleted.get("available_quantity").is_none(), "{deleted}");

    // The row stays with its ledger; the homeserver DEL is the evidence.
    assert_eq!(
        tombstone_row(&app.pool, &aggregate_id).await,
        (Some("2".to_string()), 3, 3, 0, 0)
    );
    assert_eq!(
        deleted_events(&app.pool, &aggregate_id).await,
        vec![(2, buyer.pubky.clone())]
    );

    // Gone from public reads and the seller's lists.
    let (status, body) = get(&app, &buyer.token, &format!("/v1/listings/{aggregate_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "public read: {body}");
    let (status, body) = get(
        &app,
        &seller.token,
        &format!("/v1/sellers/{}/listings", seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seller export: {body}");
    assert_eq!(body["listings"], json!([]));
    let (status, _) = get(
        &app,
        &seller.token,
        &format!("/v1/listings/{}/{LISTING_ID}", seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get(
        &app,
        &seller.token,
        &format!("/v1/inventory/listings/{aggregate_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The seller's event feed carries the deletion for automation clients.
    let (status, body) = get(
        &app,
        &seller.token,
        &format!("/v1/sellers/{}/events", seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|event| event["type"] == json!("listing.deleted")),
        "{body}"
    );

    // Converged: another sync, single or batched, is a no-op.
    let (status, body) = sync_as(&app, &seller, &seller.pubky, 2).await;
    assert_eq!(status, StatusCode::OK, "replayed sync: {body}");
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({ "listings": [{ "seller_pubky": seller.pubky, "listing_id": LISTING_ID }] }),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(200));
    assert_eq!(
        body["results"][0]["result"]["result"]["kind"],
        json!("listing_deleted")
    );
    assert_eq!(deleted_events(&app.pool, &aggregate_id).await.len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bare_404_without_a_homeserver_delete_keeps_the_listing(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let buyer = new_actor(&app).await;

    // The seller is not hosted on this homeserver at all: its record fetch
    // 404s exactly like a deleted record would.
    let unhosted = new_actor(&app).await;
    let mut register = register_command(&unhosted.pubky, 2);
    register["command_id"] = json!(Uuid::new_v4());
    let (status, body) = execute(&app, &unhosted.token, &register).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A hosted seller whose record vanished without a DEL event.
    let hosted = new_actor(&app).await;
    published_listing(&app, &homeserver, &hosted, 2).await;
    homeserver.put_other_file(&hosted.pubky, "/pub/pubky.app/profile.json");
    homeserver.drop_record_silently(&hosted.pubky, LISTING_ID);

    for (number, seller) in [(10, &unhosted), (11, &hosted)] {
        let (status, body) = sync_as(&app, &buyer, &seller.pubky, number).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
        let aggregate_id = listing_aggregate(&seller.pubky);
        assert_eq!(
            tombstone_row(&app.pool, &aggregate_id).await,
            (None, 2, 2, 0, 0)
        );
        let (status, _) = get(&app, &buyer.token, &format!("/v1/listings/{aggregate_id}")).await;
        assert_eq!(status, StatusCode::OK);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_deleted_listing_refuses_new_commitments(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;

    // Placed before the delete: a pending offer and an unpaid order.
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer: {body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 20).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let revision = body["revision"].as_i64().expect("revision");

    let mut checkout = checkout_command(&seller.pubky);
    checkout["command_id"] = json!("00000000-0000-4000-8000-000000001001");
    checkout["aggregate_id"] = json!("checkout:00000000-0000-4000-8000-000000001001");
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["command_id"] = json!(Uuid::new_v4());
    offer["expected_revision"] = json!(revision);
    let refusals = [
        ("checkout", &buyer, checkout),
        ("offer", &buyer, offer),
        (
            "reserve",
            &buyer,
            reserve_command(&seller.pubky, 21, 1, revision),
        ),
        // The unpaid order's first lock point would take the stock.
        (
            "payment hold",
            &buyer,
            payment_command(&payment_id, 1, "detected", 0, 22),
        ),
    ];
    for (label, actor, command) in refusals {
        let (status, body) = execute(&app, &actor.token, &command).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{label}: {body}");
        assert_eq!(body["error"]["code"], json!("NOT_FOUND"), "{label}");
    }
    // Accepting the pending offer is refused too; its record is gone.
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000000502"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "accept: {body}");

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(&seller.token),
        &json!({
            "schema_version": 1,
            "kind": "inventory.adjust",
            "aggregate_id": listing_aggregate(&seller.pubky),
            "listing_id": LISTING_ID,
            "expected_revision": revision,
            "delta": 5,
            "idempotency_key": Uuid::new_v4(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "adjust: {body}");
    assert_eq!(
        tombstone_row(&app.pool, &listing_aggregate(&seller.pubky)).await,
        (Some("2".to_string()), 3, 3, 0, 0)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paid_order_on_a_deleted_listing_still_resolves(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(&seller.pubky, LISTING_ID, record(1, 1));
    let order = create_paid_order(&app, &seller, &buyer).await;
    let aggregate_id = listing_aggregate(&seller.pubky);

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 30).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));

    let (status, body) = get(
        &app,
        &buyer.token,
        &format!("/v1/orders/{}", order.order_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order.order_id,
            2,
            json!({ "reason": "Seller withdrew it" }),
            31,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel request: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("order.cancel_approve", &order.order_id, 3, json!({}), 32),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel approve: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));

    // The sale returned to the retained ledger; the listing stays deleted.
    let (cursor, total, available, reserved, sold) = tombstone_row(&app.pool, &aggregate_id).await;
    assert!(cursor.is_some());
    assert_eq!((total, available, reserved, sold), (1, 1, 0, 0));
    let (status, _) = get(&app, &buyer.token, &format!("/v1/listings/{aggregate_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_deleted_auction_takes_no_bids_and_closes_unsold(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let late_bidder = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &place_bid_command(&seller.pubky, 40, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "leading bid: {body}");
    // A competing bid lifts the visible price past the 60.00 reserve, so a
    // live listing would close sold.
    let rival = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &rival.token,
        &place_bid_command(&seller.pubky, 43, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "rival bid: {body}");
    assert_eq!(
        body["result"]["listing"]["auction"]["current_price"]["amount_minor"],
        json!(8_500)
    );

    let aggregate_id = listing_aggregate(&seller.pubky);
    let mut tx = app.pool.begin().await.expect("tx");
    let (deleted, _) = marketplace_service::listing_deletion::tombstone(
        &mut tx,
        &aggregate_id,
        "7",
        "system",
        Uuid::new_v4(),
        app.clock.now(),
    )
    .await
    .expect("tombstone")
    .expect("live auction tombstoned");
    tx.commit().await.expect("commit");

    let (status, body) = execute(
        &app,
        &late_bidder.token,
        &place_bid_command(&seller.pubky, 41, 12_000, deleted.server_revision),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "bid after delete: {body}");

    app.clock.advance_seconds(11 * 60);
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, deleted.server_revision, 42),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close: {body}");
    assert_eq!(body["result"]["outcome"], json!("unsold"));
    assert!(body["result"]["order"].is_null(), "{body}");
    let orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("orders");
    assert_eq!(orders, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_recreated_record_revives_with_its_own_stock_only(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(3, 2));
    let (status, body) = sync_as(&app, &seller, &seller.pubky, 50).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Stock the deleted listing must not carry over: a seller adjustment
    // and a pending offer.
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(&seller.token),
        &json!({
            "schema_version": 1,
            "kind": "inventory.adjust",
            "aggregate_id": aggregate_id,
            "listing_id": LISTING_ID,
            "expected_revision": 1,
            "delta": 5,
            "idempotency_key": Uuid::new_v4(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "adjust: {body}");
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::OK, "offer: {body}");
    assert_eq!(
        tombstone_row(&app.pool, &aggregate_id).await,
        (None, 7, 7, 0, 0)
    );

    app.clock.advance_seconds(60);
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 51).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));

    // The seller publishes the same record at the id again.
    app.clock.advance_seconds(60);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(3, 2));
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 52).await;
    assert_eq!(status, StatusCode::OK, "revival: {body}");
    let listing = &body["result"]["listing"];
    assert_eq!(body["result"]["kind"], json!("listing"));
    assert_eq!(listing["listing_revision"], json!(3));
    assert_eq!(listing["total_quantity"], json!(2));
    assert_eq!(listing["available_quantity"], json!(2));
    assert_eq!(listing["state"], json!("available"));
    let (deleted_at, recreated_at): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as("SELECT deleted_at, recreated_at FROM listings WHERE aggregate_id = $1")
        .bind(&aggregate_id)
        .fetch_one(&app.pool)
        .await
        .expect("revived row");
    assert_eq!(deleted_at, None);
    assert_eq!(recreated_at, Some(app.clock.now()));
    let (status, body) = get(&app, &buyer.token, &format!("/v1/listings/{aggregate_id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The offer's terms still match the identical record, but it belongs
    // to the deleted listing.
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000000503"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "stale offer accept: {body}");
    let offer_state: String = sqlx::query_scalar("SELECT state FROM offers WHERE id = $1")
        .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .fetch_one(&app.pool)
        .await
        .expect("offer");
    assert_eq!(offer_state, "pending");

    // New commitments work against the revived stock.
    let mut checkout = checkout_command(&seller.pubky);
    checkout["payload"]["lines"][0]["expected_revision"] = body_revision(&app, &aggregate_id).await;
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout after revival: {body}");
}

async fn body_revision(app: &TestApp, aggregate_id: &str) -> Value {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("revision");
    json!(revision)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn revival_keeps_what_the_deleted_listing_still_owes(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(1, 1));
    create_paid_order(&app, &seller, &buyer).await;

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 60).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // `listing.register` re-creates it as a new listing (revision 0).
    app.clock.advance_seconds(60);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(1, 3));
    let mut register = register_command(&seller.pubky, 3);
    register["command_id"] = json!(Uuid::new_v4());
    let (status, body) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register revival: {body}");
    let listing = &body["result"]["listing"];
    assert_eq!(listing["total_quantity"], json!(3));
    assert_eq!(listing["sold_quantity"], json!(1));
    assert_eq!(listing["available_quantity"], json!(2));
    let (cursor, ..) = tombstone_row(&app.pool, &aggregate_id).await;
    assert_eq!(cursor, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_deleted_auction_id_is_not_revived(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 70).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let mut auction = record(2, 1);
    auction["shippingOptions"] = json!([{
        "pricing": "flat",
        "price": { "amountMinor": 1_200, "currency": "USD", "exponent": 2 },
    }]);
    auction["sale"] = json!({
        "format": "auction",
        "startingPrice": { "amountMinor": 4_500, "currency": "USD", "exponent": 2 },
        "startsAt": "2026-08-19T22:00:00.000Z",
        "endsAt": "2026-08-19T22:10:00.000Z",
        "minimumIncrement": { "amountMinor": 500, "currency": "USD", "exponent": 2 },
        "antiSnipingWindowSeconds": 60,
        "antiSnipingExtensionSeconds": 120,
    });
    homeserver.put_record(&seller.pubky, LISTING_ID, auction);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 71).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    let (cursor, ..) = tombstone_row(&app.pool, &listing_aggregate(&seller.pubky)).await;
    assert!(cursor.is_some());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_follows_homeserver_deletes_without_a_sync(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 2).await;
    homeserver.put_record(&seller.pubky, "boots_02", record(1, 1));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_02", 80),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let deleted_id = listing_aggregate(&seller.pubky);
    let kept_id = format!("listing:{}_boots_02", seller.pubky);

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    // Deleted and re-created before the worker looks: the latest event is
    // the PUT, so it stays.
    homeserver.delete_record(&seller.pubky, "boots_02");
    homeserver.put_record(&seller.pubky, "boots_02", record(1, 1));

    let holder = Uuid::new_v4();
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass");
    assert_eq!(summary.listings_tombstoned, 1);
    assert_eq!(
        deleted_events(&app.pool, &deleted_id).await,
        vec![(2, "system".to_string())]
    );
    assert_eq!(
        tombstone_row(&app.pool, &deleted_id).await.0,
        Some("3".to_string())
    );
    assert_eq!(tombstone_row(&app.pool, &kept_id).await.0, None);
    let (cursor,): (Option<String>,) =
        sqlx::query_as("SELECT event_cursor FROM listing_deletion_cursors WHERE seller_pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("seller cursor");
    assert_eq!(cursor, Some("5".to_string()));

    // Later passes resume from the cursor and never tombstone twice.
    app.clock.advance_seconds(61);
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("second worker pass");
    assert_eq!(summary.listings_tombstoned, 0);
    assert_eq!(deleted_events(&app.pool, &deleted_id).await.len(), 1);

    homeserver.delete_record(&seller.pubky, "boots_02");
    app.clock.advance_seconds(61);
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("third worker pass");
    assert_eq!(summary.listings_tombstoned, 1);
    assert_eq!(
        tombstone_row(&app.pool, &kept_id).await.0,
        Some("6".to_string())
    );
}

async fn register_as(app: &TestApp, seller: &TestActor, listing_id: &str, quantity: i64) {
    let mut register = register_command(&seller.pubky, quantity);
    register["command_id"] = json!(Uuid::new_v4());
    register["aggregate_id"] = json!(format!("listing:{}_{listing_id}", seller.pubky));
    register["payload"]["listing_id"] = json!(listing_id);
    let (status, body) = execute(app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register {listing_id}: {body}");
}

async fn seller_cursor(pool: &PgPool, seller: &str) -> Option<String> {
    sqlx::query_scalar("SELECT event_cursor FROM listing_deletion_cursors WHERE seller_pubky = $1")
        .bind(seller)
        .fetch_optional(pool)
        .await
        .expect("cursor row")
        .flatten()
}

async fn tombstoned_ids(pool: &PgPool, seller: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT listing_id FROM listings WHERE seller_pubky = $1 AND deleted_at IS NOT NULL \
         ORDER BY listing_id",
    )
    .bind(seller)
    .fetch_all(pool)
    .await
    .expect("tombstoned ids")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_slow_homeserver_cannot_hold_the_follower_past_its_deadline(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.listing_deletion_pass_budget_ms = 1_500;
    let (app, homeserver) = common::test_app_with_homeserver_config(pool, config).await;
    let seller = new_actor(&app).await;
    let ids: Vec<String> = (0..100).map(|index| format!("bulk_{index:03}")).collect();
    for id in &ids {
        homeserver.put_record(&seller.pubky, id, record(1, 1));
        register_as(&app, &seller, id, 1).await;
    }
    for id in &ids {
        homeserver.delete_record(&seller.pubky, id);
    }
    let holder = Uuid::new_v4();

    // The first page is the 100 PUTs: nothing to settle, due again at once.
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("first pass");
    assert_eq!(summary.listings_tombstoned, 0);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("100")
    );

    // A full page of 100 DELs behind 300 ms per homeserver request: each
    // confirmation costs two requests, so the 1.5 s budget ends the pass
    // after one or two of them.
    homeserver.set_delay(std::time::Duration::from_millis(300));
    let started = std::time::Instant::now();
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("slow pass");
    let elapsed = started.elapsed();
    // The whole worker pass, including the tasks before the follower; an
    // unbounded follower would spend 600 ms on each of the page's DELs.
    assert!(
        elapsed < std::time::Duration::from_millis(4_000),
        "the pass outlived its budget: {elapsed:?}"
    );
    let settled = summary.listings_tombstoned;
    assert!((1..20).contains(&settled), "settled {settled}");
    // The cursor stops at the last settled DEL; nothing after it is skipped.
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await,
        Some((100 + settled).to_string())
    );
    assert_eq!(
        tombstoned_ids(&app.pool, &seller.pubky).await,
        ids[..settled as usize].to_vec()
    );

    homeserver.set_delay(std::time::Duration::ZERO);
    for _ in 0..10 {
        app.clock.advance_seconds(61);
        marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
            .await
            .expect("catch-up pass");
    }
    assert_eq!(tombstoned_ids(&app.pool, &seller.pubky).await, ids);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("200")
    );
    let deletions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE kind = 'listing.deleted' AND actor_pubky = 'system'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("deletion events");
    assert_eq!(deletions, 100);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_expired_holder_cannot_rewind_the_follower_cursor(pool: PgPool) {
    use marketplace_service::listing_deletion::{follow_homeserver_deletions, FollowerPass};
    use marketplace_service::workers::{release_lease, try_acquire_lease, TASK_LISTING_DELETIONS};

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let ids = ["race_0", "race_1", "race_2"];
    for id in ids {
        homeserver.put_record(&seller.pubky, id, record(1, 1));
        register_as(&app, &seller, id, 1).await;
    }

    // Holder A reads the page of three PUTs; the answer is slow.
    homeserver.set_delay(std::time::Duration::from_millis(1_500));
    let holder_a = Uuid::new_v4();
    assert!(try_acquire_lease(
        &app.pool,
        TASK_LISTING_DELETIONS,
        holder_a,
        app.clock.now(),
        30
    )
    .await
    .expect("lease A"));
    let state = app.state.clone();
    let pass_a = tokio::spawn(async move {
        let pass = FollowerPass {
            pool: &state.pool,
            homeserver: state.homeserver.as_deref().expect("homeserver"),
            clock: state.clock.as_ref(),
            holder: holder_a,
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(10),
        };
        follow_homeserver_deletions(&pass).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Meanwhile the seller deletes all three, A's lease lapses, and holder
    // B follows them through to cursor 6.
    for id in ids {
        homeserver.delete_record(&seller.pubky, id);
    }
    homeserver.set_delay(std::time::Duration::ZERO);
    app.clock.advance_seconds(31);
    let holder_b = Uuid::new_v4();
    assert!(try_acquire_lease(
        &app.pool,
        TASK_LISTING_DELETIONS,
        holder_b,
        app.clock.now(),
        30
    )
    .await
    .expect("lease B"));
    let pass = FollowerPass {
        pool: &app.pool,
        homeserver: app.state.homeserver.as_deref().expect("homeserver"),
        clock: app.clock.as_ref(),
        holder: holder_b,
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(10),
    };
    assert_eq!(follow_homeserver_deletions(&pass).await.expect("pass B"), 3);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("6")
    );
    release_lease(&app.pool, TASK_LISTING_DELETIONS, holder_b, app.clock.now())
        .await
        .expect("release B");

    // A's stale page (through cursor 3) arrives after B finished: its write
    // is refused, and the cursor stays where B left it.
    assert_eq!(pass_a.await.expect("pass A joins").expect("pass A"), 0);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("6")
    );
    assert_eq!(tombstoned_ids(&app.pool, &seller.pubky).await, ids);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_revived_listing_is_not_gated_by_the_deleted_listings_drop(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    homeserver.put_drop_record(
        &seller.pubky,
        "winter_drop",
        common::drop_record_json(
            &seller.pubky,
            "winter_drop",
            1,
            &[LISTING_ID],
            &common::ts_after(-60),
            None,
            1,
            1,
        ),
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::sync_drop_command(&seller.pubky, "winter_drop", 90),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync: {body}");
    let drop_id = common::drop_aggregate(&seller.pubky, "winter_drop");

    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 91).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.advance_seconds(60);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(2, 5));
    let (status, body) = sync_as(&app, &buyer, &seller.pubky, 92).await;
    assert_eq!(status, StatusCode::OK, "revival: {body}");
    assert_eq!(body["result"]["listing"]["available_quantity"], json!(5));

    // Open sale again: two units in one line, which a drop-bound listing
    // refuses, and the old drop's single unit is not drawn on.
    let revision = body["revision"].clone();
    let mut checkout = checkout_command(&seller.pubky);
    checkout["payload"]["lines"][0]["expected_revision"] = revision.clone();
    checkout["payload"]["lines"][0]["quantity"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let current = body_revision(&app, &listing_aggregate(&seller.pubky)).await;
    let reserve = reserve_command(&seller.pubky, 93, 2, current.as_i64().expect("revision"));
    let (status, body) = execute(&app, &buyer.token, &reserve).await;
    assert_eq!(status, StatusCode::OK, "reserve: {body}");
    let (remaining, bindings): (i64, i64) = sqlx::query_as(
        "SELECT d.remaining_quantity, \
         (SELECT COUNT(*) FROM drop_listings WHERE drop_aggregate_id = d.aggregate_id AND NOT released) \
         FROM drops d WHERE d.aggregate_id = $1",
    )
    .bind(&drop_id)
    .fetch_one(&app.pool)
    .await
    .expect("drop row");
    assert_eq!((remaining, bindings), (1, 0));
}

/// Takes a shared-manual bitcoin order through its payment window so its
/// hold lapses, applies `between`, then lets the late settlement arrive.
/// Returns the order and whether the late money took stock.
async fn late_settlement(
    app: &TestApp,
    paykit: &common::FakePaykit,
    between: &str,
) -> (String, String, Option<String>, i64) {
    use common::paykit_review::{bound_shared_manual_order, poll_now, status_confirmed};
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (order_id, _payment_id, reference) =
        bound_shared_manual_order(app, paykit, &seller, &buyer).await;
    let after_window = app.clock.now() + chrono::Duration::seconds(7300);
    assert!(
        marketplace_service::workers::expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("payment-window reaper")
            >= 1
    );
    let aggregate_id = listing_aggregate(&seller.pubky);
    match between {
        "deleted" => {
            let mut tx = app.pool.begin().await.expect("tx");
            marketplace_service::listing_deletion::tombstone(
                &mut tx,
                &aggregate_id,
                "9",
                "system",
                Uuid::new_v4(),
                after_window,
            )
            .await
            .expect("tombstone")
            .expect("live listing");
            tx.commit().await.expect("commit");
        }
        "revived" => {
            sqlx::query("UPDATE listings SET recreated_at = $2 WHERE aggregate_id = $1")
                .bind(&aggregate_id)
                .bind(after_window)
                .execute(&app.pool)
                .await
                .expect("revival marker");
        }
        _ => {}
    }
    let mut late = status_confirmed("shared_manual", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(app, after_window + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, review_reason, reserved): (String, Option<String>, i64) = sqlx::query_as(
        "SELECT o.state, p.review_reason, l.reserved_quantity + l.sold_quantity \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         JOIN listings l ON l.aggregate_id = $2 WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .bind(&aggregate_id)
    .fetch_one(&app.pool)
    .await
    .expect("late facts");
    (order_id, order_state, review_reason, reserved)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn late_bitcoin_money_takes_no_stock_from_a_deleted_or_revived_listing(pool: PgPool) {
    let (app, _stripe, paykit) = common::test_app_with_payments(pool).await;

    // Control: with the listing live, the late money takes its unit back.
    let (_, _, reason, committed) = late_settlement(&app, &paykit, "live").await;
    assert_ne!(reason.as_deref(), Some("refund_required"));
    assert_eq!(committed, 1, "the live listing re-holds the unit");

    for between in ["deleted", "revived"] {
        let (_, order_state, reason, committed) = late_settlement(&app, &paykit, between).await;
        assert_eq!(order_state, "cancelled", "{between}");
        assert_eq!(reason.as_deref(), Some("refund_required"), "{between}");
        assert_eq!(committed, 0, "{between}: no unit was taken");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_held_manual_review_on_a_deleted_listing_still_resolves_paid(pool: PgPool) {
    use common::paykit_review::{into_manual_review_held, resolve_call};
    let (app, _stripe, paykit) = common::test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _reference) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let mut tx = app.pool.begin().await.expect("tx");
    marketplace_service::listing_deletion::tombstone(
        &mut tx,
        &listing_aggregate(&seller.pubky),
        "9",
        "system",
        Uuid::new_v4(),
        app.clock.now(),
    )
    .await
    .expect("tombstone")
    .expect("live listing");
    tx.commit().await.expect("commit");

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "reason": "checked my wallet" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).expect("order uuid"))
        .fetch_one(&app.pool)
        .await
        .expect("order");
    assert_eq!(state, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_deleted_auctions_bid_history_is_kept_for_its_parties_only(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 60, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let aggregate_id = listing_aggregate(&seller.pubky);
    let path = format!("/v1/listings/{aggregate_id}/bids");
    let (status, _) = get(&app, &stranger.token, &path).await;
    assert_eq!(status, StatusCode::OK, "live history is public");

    let mut tx = app.pool.begin().await.expect("tx");
    marketplace_service::listing_deletion::tombstone(
        &mut tx,
        &aggregate_id,
        "9",
        "system",
        Uuid::new_v4(),
        app.clock.now(),
    )
    .await
    .expect("tombstone")
    .expect("live auction");
    tx.commit().await.expect("commit");

    for (label, actor, expected) in [
        ("seller", &seller, StatusCode::OK),
        ("bidder", &bidder, StatusCode::OK),
        ("stranger", &stranger, StatusCode::NOT_FOUND),
    ] {
        let (status, body) = get(&app, &actor.token, &path).await;
        assert_eq!(status, expected, "{label}: {body}");
    }
}
