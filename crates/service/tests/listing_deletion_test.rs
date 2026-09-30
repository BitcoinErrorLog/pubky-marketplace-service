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
        marketplace_service::listing_deletion::DeletionAuthority::Command,
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

    // Without the delay, one pass confirms at most 20 deletions.
    homeserver.set_delay(std::time::Duration::ZERO);
    app.clock.advance_seconds(61);
    let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
        .await
        .expect("capped pass");
    assert_eq!(summary.listings_tombstoned, 20);
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
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

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
    let fence_a = take_follower_lease(&app, holder_a).await;
    let state = app.state.clone();
    let pass_a = tokio::spawn(async move {
        let pass = follower_pass(&state, holder_a, fence_a, 10_000);
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
    let fence_b = take_follower_lease(&app, holder_b).await;
    let pass = follower_pass(&app.state, holder_b, fence_b, 10_000);
    assert_eq!(follow_homeserver_deletions(&pass).await.expect("pass B"), 3);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("6")
    );
    pass.release_lease().await.expect("release B");

    // A's stale page (through cursor 3) arrives after B finished: its write
    // is refused, and the cursor stays where B left it.
    assert_eq!(pass_a.await.expect("pass A joins").expect("pass A"), 0);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("6")
    );
    assert_eq!(tombstoned_ids(&app.pool, &seller.pubky).await, ids);
}

/// Takes the follower lease for `holder` at the test clock and returns its
/// fence.
async fn take_follower_lease(app: &TestApp, holder: Uuid) -> i64 {
    marketplace_service::workers::try_acquire_fenced_lease(
        &app.pool,
        marketplace_service::workers::TASK_LISTING_DELETIONS,
        holder,
        app.clock.now(),
        30,
    )
    .await
    .expect("lease query")
    .expect("the follower lease is free")
}

/// A follower pass for a lease already taken, with both deadlines
/// `millis` from now.
fn follower_pass(
    state: &marketplace_service::AppState,
    holder: Uuid,
    fence: i64,
    millis: u64,
) -> marketplace_service::listing_deletion::FollowerPass<'_> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(millis);
    marketplace_service::listing_deletion::FollowerPass {
        pool: &state.pool,
        homeserver: state.homeserver.as_deref().expect("homeserver"),
        clock: state.clock.as_ref(),
        holder,
        fence,
        deadline,
        lease_deadline: deadline,
    }
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
                marketplace_service::listing_deletion::DeletionAuthority::Command,
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
        marketplace_service::listing_deletion::DeletionAuthority::Command,
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
        marketplace_service::listing_deletion::DeletionAuthority::Command,
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_follower_lease_is_taken_at_the_current_time(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    homeserver.delete_record(&seller.pubky, LISTING_ID);

    // The pass started 40 s ago (the tasks before the follower ran long);
    // a lease dated then would already have lapsed.
    let stale = app.clock.now() - chrono::Duration::seconds(40);
    let summary = marketplace_service::workers::run_once(&app.state, Uuid::new_v4(), stale)
        .await
        .expect("worker pass");
    assert_eq!(summary.listings_tombstoned, 1);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("2")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_tombstone_takes_drop_and_listing_locks_in_the_sell_out_confirm_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    homeserver.put_drop_record(
        &seller.pubky,
        "lock_drop",
        common::drop_record_json(
            &seller.pubky,
            "lock_drop",
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
        &common::sync_drop_command(&seller.pubky, "lock_drop", 95),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync: {body}");
    let drop_id = common::drop_aggregate(&seller.pubky, "lock_drop");
    let aggregate_id = listing_aggregate(&seller.pubky);

    // Open the tombstone's transaction first so it already holds a pool
    // connection, then hold the drop's bindings the way `record_paid_unit`
    // does before `payment.rs` locks the listing.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
    let pool = app.pool.clone();
    let tombstone_id = aggregate_id.clone();
    let now = app.clock.now();
    let deletion = tokio::spawn(async move {
        let mut tx = pool.begin().await?;
        sqlx::query("SET deadlock_timeout = '200ms'")
            .execute(&mut *tx)
            .await?;
        let _ = ready_tx.send(());
        let _ = go_rx.await;
        let deleted = marketplace_service::listing_deletion::tombstone(
            &mut tx,
            marketplace_service::listing_deletion::DeletionAuthority::Command,
            &tombstone_id,
            "9",
            "system",
            Uuid::new_v4(),
            now,
        )
        .await?;
        tx.commit().await?;
        Ok::<bool, sqlx::Error>(deleted.is_some())
    });
    ready_rx.await.expect("tombstone transaction");

    let mut confirm = app.pool.begin().await.expect("confirm tx");
    sqlx::query("SET deadlock_timeout = '200ms'")
        .execute(&mut *confirm)
        .await
        .expect("confirm deadlock timeout");
    sqlx::query("UPDATE drop_listings SET active = FALSE WHERE drop_aggregate_id = $1")
        .bind(&drop_id)
        .execute(&mut *confirm)
        .await
        .expect("bindings locked");
    let _ = go_tx.send(());

    // Wait until the tombstone is blocked on the binding row. In the old
    // order it already holds the listing row by then, and the confirmation's
    // listing lock deadlocks. Bindings-first leaves the listing free.
    let poll = app.pool.clone();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if deletion.is_finished() {
            let deleted = deletion.await.expect("tombstone task");
            panic!("tombstone finished before waiting on the bindings: {deleted:?}");
        }
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() \
             AND wait_event_type = 'Lock' AND query LIKE '%drop_listings%'",
        )
        .fetch_one(&poll)
        .await
        .expect("lock wait poll");
        if waiting >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let activity: Vec<(Option<String>, Option<String>, String)> = sqlx::query_as(
                "SELECT wait_event_type, wait_event, left(query, 160) \
                 FROM pg_stat_activity WHERE datname = current_database() \
                 AND pid <> pg_backend_pid()",
            )
            .fetch_all(&poll)
            .await
            .expect("activity");
            panic!("tombstone never waited on drop_listings: {activity:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let locked: Result<(i64,), sqlx::Error> =
        sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1 FOR UPDATE")
            .bind(&aggregate_id)
            .fetch_one(&mut *confirm)
            .await;
    assert!(locked.is_ok(), "confirmation lock: {locked:?}");
    confirm.commit().await.expect("confirmation commits");
    let deleted = deletion.await.expect("tombstone task");
    assert!(
        matches!(deleted, Ok(true)),
        "tombstone after the confirmation: {deleted:?}"
    );
    let released: bool = sqlx::query_scalar(
        "SELECT bool_and(released) FROM drop_listings WHERE drop_aggregate_id = $1",
    )
    .bind(&drop_id)
    .fetch_one(&app.pool)
    .await
    .expect("binding released");
    assert!(released, "tombstone releases the binding it locked");
}

/// Waits until another backend is blocked on a lock while running a
/// statement that contains `fragment`.
async fn wait_for_lock_wait(pool: &PgPool, fragment: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() \
             AND wait_event_type = 'Lock' AND strpos(query, $1) > 0",
        )
        .bind(fragment)
        .fetch_one(pool)
        .await
        .expect("lock wait poll");
        if waiting >= 1 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let activity: Vec<(Option<String>, Option<String>, String)> = sqlx::query_as(
                "SELECT wait_event_type, wait_event, left(query, 160) \
                 FROM pg_stat_activity WHERE datname = current_database() \
                 AND pid <> pg_backend_pid()",
            )
            .fetch_all(pool)
            .await
            .expect("activity");
            panic!("nothing waited on {fragment:?}: {activity:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn spawn_command(
    app: &TestApp,
    actor: &TestActor,
    body: Value,
) -> tokio::task::JoinHandle<(StatusCode, Value)> {
    let router = app.router.clone();
    let token = actor.token.clone();
    tokio::spawn(async move { send(router, "POST", "/v1/commands", Some(&token), &body).await })
}

async fn tombstone_now(pool: &PgPool, aggregate_id: &str, now: chrono::DateTime<chrono::Utc>) {
    let mut tx = pool.begin().await.expect("tombstone tx");
    marketplace_service::listing_deletion::tombstone(
        &mut tx,
        marketplace_service::listing_deletion::DeletionAuthority::Command,
        aggregate_id,
        "9",
        "system",
        Uuid::new_v4(),
        now,
    )
    .await
    .expect("tombstone")
    .expect("live listing");
    tx.commit().await.expect("tombstone commits");
}

/// Re-creates `boots_01` from a new record with five units.
async fn revive(app: &TestApp, homeserver: &FakeHomeserver, seller: &TestActor, number: u64) {
    app.clock.advance_seconds(60);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(2, 5));
    let (status, body) = sync_as(app, seller, &seller.pubky, number).await;
    assert_eq!(status, StatusCode::OK, "revival: {body}");
    assert_eq!(body["result"]["listing"]["available_quantity"], json!(5));
}

/// A two-unit checkout line, which a drop-bound listing refuses.
async fn two_unit_checkout(
    app: &TestApp,
    buyer: &TestActor,
    seller: &TestActor,
) -> (StatusCode, Value) {
    let revision = body_revision(app, &listing_aggregate(&seller.pubky)).await;
    let mut checkout = common::checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = revision;
    checkout["payload"]["lines"][0]["quantity"] = json!(2);
    execute(app, &buyer.token, &checkout).await
}

async fn unreleased_bindings(pool: &PgPool, drop_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM drop_listings \
         WHERE drop_aggregate_id = $1 AND NOT released",
    )
    .bind(drop_id)
    .fetch_one(pool)
    .await
    .expect("bindings")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_sync_that_read_its_listing_live_cannot_bind_it_after_the_tombstone(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    let drop_record = |revision: i64, starts_in: i64| {
        common::drop_record_json(
            &seller.pubky,
            "race_drop",
            revision,
            &[LISTING_ID],
            &common::ts_after(starts_in),
            None,
            1,
            1,
        )
    };
    homeserver.put_drop_record(&seller.pubky, "race_drop", drop_record(1, 3600));
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::sync_drop_command(&seller.pubky, "race_drop", 100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "announced drop: {body}");
    let drop_id = common::drop_aggregate(&seller.pubky, "race_drop");

    // The re-sync reads the listing live, then waits on the drop row.
    homeserver.put_drop_record(&seller.pubky, "race_drop", drop_record(2, -60));
    let mut hold = app.pool.begin().await.expect("hold tx");
    sqlx::query("SELECT 1 FROM drops WHERE aggregate_id = $1 FOR UPDATE")
        .bind(&drop_id)
        .execute(&mut *hold)
        .await
        .expect("drop row held");
    let resync = spawn_command(
        &app,
        &seller,
        common::sync_drop_command(&seller.pubky, "race_drop", 101),
    );
    wait_for_lock_wait(&app.pool, "FROM drops WHERE aggregate_id").await;

    tombstone_now(&app.pool, &aggregate_id, app.clock.now()).await;
    hold.rollback().await.expect("release the drop row");
    let (status, body) = resync.await.expect("resync task");
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "resync: {body}");
    assert_eq!(
        body["error"]["message"],
        json!("The drop references unregistered listings.")
    );
    assert_eq!(unreleased_bindings(&app.pool, &drop_id).await, 0);
    let (record_revision,): (i64,) =
        sqlx::query_as("SELECT record_revision FROM drops WHERE aggregate_id = $1")
            .bind(&drop_id)
            .fetch_one(&app.pool)
            .await
            .expect("drop row");
    assert_eq!(record_revision, 1, "the refused re-sync rolled back");

    revive(&app, &homeserver, &seller, 102).await;
    let (status, body) = two_unit_checkout(&app, &buyer, &seller).await;
    assert_eq!(status, StatusCode::OK, "checkout after revival: {body}");
    let current = body_revision(&app, &aggregate_id).await;
    let reserve = reserve_command(&seller.pubky, 103, 2, current.as_i64().expect("revision"));
    let (status, body) = execute(&app, &buyer.token, &reserve).await;
    assert_eq!(status, StatusCode::OK, "reserve after revival: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_binding_committed_while_the_tombstone_waits_does_not_gate_the_revived_listing(
    pool: PgPool,
) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    let live_drop = |drop_id: &str| {
        common::drop_record_json(
            &seller.pubky,
            drop_id,
            1,
            &[LISTING_ID],
            &common::ts_after(-60),
            None,
            1,
            1,
        )
    };
    homeserver.put_drop_record(&seller.pubky, "early_drop", live_drop("early_drop"));
    let early = common::drop_aggregate(&seller.pubky, "early_drop");

    // The sync passes its locked live check and then waits on its event
    // insert, still holding the listing share lock.
    let mut hold = app.pool.begin().await.expect("hold tx");
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $1, $2, 1, 'test', 'test.hold', now())",
    )
    .bind(Uuid::new_v4())
    .bind(&early)
    .execute(&mut *hold)
    .await
    .expect("event revision held");
    let sync = spawn_command(
        &app,
        &seller,
        common::sync_drop_command(&seller.pubky, "early_drop", 110),
    );
    wait_for_lock_wait(&app.pool, "INSERT INTO events").await;

    // The tombstone releases the bindings it can see, then waits on the
    // listing row the sync holds.
    let pool = app.pool.clone();
    let tombstone_id = aggregate_id.clone();
    let now = app.clock.now();
    let deletion = tokio::spawn(async move { tombstone_now(&pool, &tombstone_id, now).await });
    wait_for_lock_wait(&app.pool, "UPDATE listings SET deleted_at").await;
    hold.rollback().await.expect("release the event revision");
    let (status, body) = sync.await.expect("sync task");
    assert_eq!(
        status,
        StatusCode::OK,
        "sync ahead of the tombstone: {body}"
    );
    deletion.await.expect("tombstone task");
    assert_eq!(
        unreleased_bindings(&app.pool, &early).await,
        1,
        "the binding committed after the tombstone's release"
    );

    revive(&app, &homeserver, &seller, 111).await;
    let (status, body) = two_unit_checkout(&app, &buyer, &seller).await;
    assert_eq!(status, StatusCode::OK, "checkout after revival: {body}");
    let current = body_revision(&app, &aggregate_id).await;
    let reserve = reserve_command(&seller.pubky, 112, 2, current.as_i64().expect("revision"));
    let (status, body) = execute(&app, &buyer.token, &reserve).await;
    assert_eq!(status, StatusCode::OK, "reserve after revival: {body}");
    let (remaining,): (i64,) =
        sqlx::query_as("SELECT remaining_quantity FROM drops WHERE aggregate_id = $1")
            .bind(&early)
            .fetch_one(&app.pool)
            .await
            .expect("early drop");
    assert_eq!(
        remaining, 1,
        "the deleted generation's drop is not drawn on"
    );

    // The revived generation binds to a new drop, and that binding gates.
    homeserver.put_drop_record(&seller.pubky, "late_drop", live_drop("late_drop"));
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::sync_drop_command(&seller.pubky, "late_drop", 113),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "new drop on the revived listing: {body}"
    );
    let (status, body) = two_unit_checkout(&app, &buyer, &seller).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!(marketplace_service::handlers::drops::DROP_SINGLE_LINE)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_sync_refuses_a_listing_re_created_while_it_was_binding(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    register_as(&app, &seller, "boots_02", 1).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    homeserver.put_drop_record(
        &seller.pubky,
        "pair_drop",
        common::drop_record_json(
            &seller.pubky,
            "pair_drop",
            1,
            &[LISTING_ID, "boots_02"],
            &common::ts_after(-60),
            None,
            1,
            1,
        ),
    );
    let pair = common::drop_aggregate(&seller.pubky, "pair_drop");

    // An uncommitted active binding on `boots_02` stops the sync between
    // its `boots_01` binding and its locked re-read.
    let mut hold = app.pool.begin().await.expect("hold tx");
    sqlx::query(
        "INSERT INTO drops (aggregate_id, seller_pubky, drop_id, record_revision, revision, \
         state, format, starts_at, ends_at, total_quantity, per_buyer_limit, \
         remaining_quantity, paid_quantity, stock_display, listing_ids, created_at, updated_at) \
         VALUES ($1, $2, 'hold_drop', 1, 1, 'live', 'fcfs', now(), NULL, 1, 1, 1, 0, 'exact', \
         '[\"boots_02\"]', now(), now())",
    )
    .bind(common::drop_aggregate(&seller.pubky, "hold_drop"))
    .bind(&seller.pubky)
    .execute(&mut *hold)
    .await
    .expect("holding drop");
    sqlx::query(
        "INSERT INTO drop_listings (drop_aggregate_id, seller_pubky, listing_id, active) \
         VALUES ($1, $2, 'boots_02', TRUE)",
    )
    .bind(common::drop_aggregate(&seller.pubky, "hold_drop"))
    .bind(&seller.pubky)
    .execute(&mut *hold)
    .await
    .expect("holding binding");
    let sync = spawn_command(
        &app,
        &seller,
        common::sync_drop_command(&seller.pubky, "pair_drop", 120),
    );
    wait_for_lock_wait(&app.pool, "INSERT INTO drop_listings").await;

    // `boots_01` is deleted and re-created before the sync re-reads it.
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let (status, body) = sync_as(&app, &seller, &seller.pubky, 121).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["kind"], json!("listing_deleted"));
    revive(&app, &homeserver, &seller, 122).await;
    hold.rollback().await.expect("release boots_02");

    let (status, body) = sync.await.expect("sync task");
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "sync: {body}");
    assert_eq!(
        body["error"]["issues"],
        json!([{
            "path": "payload.listing_ids",
            "message": format!("Unregistered listing: {LISTING_ID}"),
        }])
    );
    let drops: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM drops WHERE aggregate_id = $1")
            .bind(&pair)
            .fetch_one(&app.pool)
            .await
            .expect("drop count");
    assert_eq!(drops, 0, "the refused registration rolled back");
    let (generation,): (i64,) =
        sqlx::query_as("SELECT generation FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("listing");
    assert_eq!(generation, 1);
}

/// Waits until `task` has finished or another backend is blocked on a lock
/// while running a statement that contains `fragment`.
async fn wait_for_finish_or_lock_wait<T>(
    pool: &PgPool,
    task: &tokio::task::JoinHandle<T>,
    fragment: &str,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if task.is_finished() {
            return;
        }
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() \
             AND wait_event_type = 'Lock' AND strpos(query, $1) > 0",
        )
        .bind(fragment)
        .fetch_one(pool)
        .await
        .expect("lock wait poll");
        if waiting >= 1 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "neither finished nor waited on {fragment:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Statements of this test's database blocked on a lock right now.
async fn lock_waiters(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT left(query, 120) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
         AND wait_event_type = 'Lock'",
    )
    .fetch_all(pool)
    .await
    .expect("lock waiters")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_follower_statement_ends_at_the_lease_deadline(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;
    use marketplace_service::workers::TASK_LISTING_DELETIONS;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    let holder = Uuid::new_v4();
    let fence = take_follower_lease(&app, holder).await;
    let pass = follower_pass(&app.state, holder, fence, 10_000);
    assert_eq!(
        follow_homeserver_deletions(&pass)
            .await
            .expect("first pass"),
        0
    );
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("1")
    );

    // Each blocker holds a lock one follower statement needs. The pass
    // gives the statement one second, the time left in its lease.
    let scenarios: [(&str, &str, Option<&str>, bool); 4] = [
        (
            "cursor write",
            "SELECT 1 FROM listing_deletion_cursors WHERE seller_pubky = $1 FOR UPDATE",
            Some(seller.pubky.as_str()),
            false,
        ),
        (
            "due sellers",
            "LOCK TABLE listing_deletion_cursors IN ACCESS EXCLUSIVE MODE",
            None,
            false,
        ),
        (
            "lease check",
            "SELECT 1 FROM worker_leases WHERE task = $1 FOR UPDATE",
            Some(TASK_LISTING_DELETIONS),
            true,
        ),
        (
            "tombstone",
            "SELECT 1 FROM listings WHERE aggregate_id = $1 FOR UPDATE",
            Some(aggregate_id.as_str()),
            true,
        ),
    ];
    let mut deleted_on_homeserver = false;
    for (label, blocker_sql, bind, deleted) in scenarios {
        if deleted && !deleted_on_homeserver {
            homeserver.delete_record(&seller.pubky, LISTING_ID);
            deleted_on_homeserver = true;
        }
        app.clock.advance_seconds(61);
        let fence = take_follower_lease(&app, holder).await;
        let mut blocker = app.pool.begin().await.expect("blocker tx");
        let mut query = sqlx::query(blocker_sql);
        if let Some(value) = bind {
            query = query.bind(value);
        }
        query.execute(&mut *blocker).await.expect(label);

        let started = std::time::Instant::now();
        let pass = follower_pass(&app.state, holder, fence, 1_000);
        let tombstoned = follow_homeserver_deletions(&pass).await.expect(label);
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(2_500),
            "{label}: the pass outlived its lease: {elapsed:?}"
        );
        assert_eq!(tombstoned, 0, "{label}");

        // The server cancelled the blocked statement: nothing still waits
        // on the blocker's lock.
        let settle = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let waiting = lock_waiters(&app.pool).await;
            if waiting.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < settle,
                "{label}: a follower statement outlived the lease: {waiting:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        blocker.rollback().await.expect("release the blocker");
        assert_eq!(
            tombstone_row(&app.pool, &aggregate_id).await.0,
            None,
            "{label}"
        );
        assert_eq!(
            seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
            Some("1"),
            "{label}"
        );
    }

    // Nothing was left behind: the next pass settles the deletion.
    app.clock.advance_seconds(61);
    let fence = take_follower_lease(&app, holder).await;
    let pass = follower_pass(&app.state, holder, fence, 10_000);
    assert_eq!(
        follow_homeserver_deletions(&pass).await.expect("last pass"),
        1
    );
    assert_eq!(
        tombstone_row(&app.pool, &aggregate_id).await.0,
        Some("2".to_string())
    );
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("2")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_follower_stops_waiting_for_a_connection_at_the_lease_deadline(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let holder = Uuid::new_v4();
    let fence = take_follower_lease(&app, holder).await;

    let size = app.pool.options().get_max_connections();
    let mut held = Vec::new();
    for _ in 0..size {
        held.push(app.pool.acquire().await.expect("pool connection"));
    }
    let started = std::time::Instant::now();
    let pass = follower_pass(&app.state, holder, fence, 1_000);
    let tombstoned = follow_homeserver_deletions(&pass)
        .await
        .expect("starved pass");
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_millis(2_500),
        "the pass waited for a connection past its lease: {elapsed:?}"
    );
    assert_eq!(tombstoned, 0);
    drop(held);

    let pass = follower_pass(&app.state, holder, fence, 10_000);
    assert_eq!(
        follow_homeserver_deletions(&pass).await.expect("fed pass"),
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pass_whose_lease_was_taken_over_tombstones_nothing(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let aggregate_id = listing_aggregate(&seller.pubky);

    // A confirms the deletion slowly; its lease lapses and B takes it.
    homeserver.set_delay(std::time::Duration::from_millis(600));
    let holder_a = Uuid::new_v4();
    let fence_a = take_follower_lease(&app, holder_a).await;
    let state = app.state.clone();
    let pass_a = tokio::spawn(async move {
        let pass = follower_pass(&state, holder_a, fence_a, 10_000);
        follow_homeserver_deletions(&pass).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    app.clock.advance_seconds(31);
    take_follower_lease(&app, Uuid::new_v4()).await;

    assert_eq!(pass_a.await.expect("pass A joins").expect("pass A"), 0);
    assert_eq!(tombstone_row(&app.pool, &aggregate_id).await.0, None);
    assert!(deleted_events(&app.pool, &aggregate_id).await.is_empty());
    assert_eq!(seller_cursor(&app.pool, &seller.pubky).await, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_renewed_or_expired_lease_refuses_the_stale_pass(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    for scenario in ["renewed by the same holder", "expired"] {
        let seller = new_actor(&app).await;
        published_listing(&app, &homeserver, &seller, 1).await;
        homeserver.delete_record(&seller.pubky, LISTING_ID);
        let aggregate_id = listing_aggregate(&seller.pubky);

        homeserver.set_delay(std::time::Duration::from_millis(600));
        let holder = Uuid::new_v4();
        let fence = take_follower_lease(&app, holder).await;
        let state = app.state.clone();
        let stale = tokio::spawn(async move {
            let pass = follower_pass(&state, holder, fence, 10_000);
            follow_homeserver_deletions(&pass).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        app.clock.advance_seconds(31);
        if scenario == "expired" {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT fence FROM worker_leases WHERE task = 'listing_deletions'"
                )
                .fetch_one(&app.pool)
                .await
                .expect("lease row"),
                fence,
                "nobody took the expired lease"
            );
        } else {
            assert!(take_follower_lease(&app, holder).await > fence);
        }

        assert_eq!(
            stale.await.expect("stale pass joins").expect("stale pass"),
            0,
            "{scenario}"
        );
        assert_eq!(
            tombstone_row(&app.pool, &aggregate_id).await.0,
            None,
            "{scenario}"
        );
        assert_eq!(
            seller_cursor(&app.pool, &seller.pubky).await,
            None,
            "{scenario}"
        );
        homeserver.set_delay(std::time::Duration::ZERO);
        app.clock.advance_seconds(31);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_takeover_waits_for_the_fenced_write_in_flight(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;
    use marketplace_service::workers::{try_acquire_fenced_lease, TASK_LISTING_DELETIONS};

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    let aggregate_id = listing_aggregate(&seller.pubky);

    // A's tombstone passes its lease check, then waits on the listing row.
    let mut blocker = app.pool.begin().await.expect("blocker tx");
    sqlx::query("SELECT 1 FROM listings WHERE aggregate_id = $1 FOR UPDATE")
        .bind(&aggregate_id)
        .execute(&mut *blocker)
        .await
        .expect("listing row held");
    let holder_a = Uuid::new_v4();
    let fence_a = take_follower_lease(&app, holder_a).await;
    let state = app.state.clone();
    let pass_a = tokio::spawn(async move {
        let pass = follower_pass(&state, holder_a, fence_a, 10_000);
        follow_homeserver_deletions(&pass).await
    });
    wait_for_lock_wait(&app.pool, "UPDATE listings SET deleted_at").await;

    // A's lease lapses. B's takeover waits until A's write ends.
    app.clock.advance_seconds(31);
    let pool = app.pool.clone();
    let now = app.clock.now();
    let takeover = tokio::spawn(async move {
        try_acquire_fenced_lease(&pool, TASK_LISTING_DELETIONS, Uuid::new_v4(), now, 30).await
    });
    wait_for_finish_or_lock_wait(&app.pool, &takeover, "INSERT INTO worker_leases").await;
    assert!(
        !takeover.is_finished(),
        "the takeover did not wait for the write in flight"
    );

    blocker.rollback().await.expect("release the listing row");
    assert_eq!(pass_a.await.expect("pass A joins").expect("pass A"), 1);
    let fence_b = takeover
        .await
        .expect("takeover joins")
        .expect("takeover query")
        .expect("B takes the expired lease");
    assert!(fence_b > fence_a);
    assert_eq!(
        tombstone_row(&app.pool, &aggregate_id).await.0,
        Some("2".to_string())
    );
    // A's cursor write came after the takeover and was refused.
    assert_eq!(seller_cursor(&app.pool, &seller.pubky).await, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_follower_cursor_never_moves_backwards(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    for id in ["mono_0", "mono_1", "mono_2"] {
        homeserver.put_record(&seller.pubky, id, record(1, 1));
        register_as(&app, &seller, id, 1).await;
    }

    // The pass reads the page of three PUTs slowly; meanwhile the stored
    // cursor moves past it.
    homeserver.set_delay(std::time::Duration::from_millis(600));
    let holder = Uuid::new_v4();
    let fence = take_follower_lease(&app, holder).await;
    let state = app.state.clone();
    let pass = tokio::spawn(async move {
        let pass = follower_pass(&state, holder, fence, 10_000);
        follow_homeserver_deletions(&pass).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut ahead = app.pool.begin().await.expect("cursor tx");
    declare_authority(&mut ahead, &format!("follower:{holder}:{fence}")).await;
    sqlx::query(
        "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
         VALUES ($1, '50', $2)",
    )
    .bind(&seller.pubky)
    .bind(app.clock.now())
    .execute(&mut *ahead)
    .await
    .expect("cursor moved ahead");
    ahead.commit().await.expect("cursor commits");

    assert_eq!(pass.await.expect("pass joins").expect("pass"), 0);
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("50")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_follower_counts_its_deadlines_from_before_it_waits_for_the_lease_row(pool: PgPool) {
    use marketplace_service::workers::{release_lease, TASK_LISTING_DELETIONS};

    let mut config = marketplace_service::config::Config::for_tests();
    config.worker_lease_seconds = 2;
    config.listing_deletion_pass_budget_ms = 1_000;
    let pass_budget = std::time::Duration::from_millis(config.listing_deletion_pass_budget_ms);
    let (app, homeserver) = common::test_app_with_homeserver_config(pool, config).await;
    let seller = new_actor(&app).await;
    let ids: Vec<String> = (0..5).map(|index| format!("wait_{index}")).collect();
    for id in &ids {
        homeserver.put_record(&seller.pubky, id, record(1, 1));
        register_as(&app, &seller, id, 1).await;
        homeserver.delete_record(&seller.pubky, id);
    }
    let previous = Uuid::new_v4();
    take_follower_lease(&app, previous).await;
    release_lease(&app.pool, TASK_LISTING_DELETIONS, previous, app.clock.now())
        .await
        .expect("previous pass released");

    // Another pass's write holds the free lease row. The follower blocks
    // in its acquisition until this transaction ends.
    let mut blocker = app.pool.begin().await.expect("blocker tx");
    sqlx::query("SELECT 1 FROM worker_leases WHERE task = $1 FOR UPDATE")
        .bind(TASK_LISTING_DELETIONS)
        .execute(&mut *blocker)
        .await
        .expect("lease row held");
    let state = app.state.clone();
    let now = app.clock.now();
    let mut pass = tokio::spawn(async move {
        marketplace_service::workers::run_once(&state, Uuid::new_v4(), now).await
    });
    wait_for_lock_wait(&app.pool, "INSERT INTO worker_leases").await;

    // The pass budget is a tokio Instant captured before that acquisition.
    // Freezing the clock and sleeping the budget advances that Instant
    // through the whole budget while the row is still locked. The
    // acquisition times out on that same Instant, so the pass finishes
    // still waiting on the row and confirms nothing. A budget captured
    // after the acquisition would leave the pass blocked here: its
    // acquire deadline is the lease (2s), which this sleep does not reach.
    let finished_while_held = {
        struct Resume;
        impl Drop for Resume {
            fn drop(&mut self) {
                tokio::time::resume();
            }
        }
        tokio::time::pause();
        let _resume = Resume;
        tokio::select! {
            biased;
            result = &mut pass => Some(result),
            _ = tokio::time::sleep(pass_budget) => None,
        }
    };
    blocker.rollback().await.expect("blocker released");
    let passed = finished_while_held.is_some();
    let summary = match finished_while_held {
        Some(result) => result.expect("worker pass joins").expect("worker pass"),
        None => pass.await.expect("worker pass joins").expect("worker pass"),
    };
    assert!(
        passed,
        "the follower was still waiting on the lease row after its pass budget; \
         tombstoned once the row was released: {}",
        summary.listings_tombstoned
    );
    assert_eq!(
        summary.listings_tombstoned, 0,
        "the pass confirmed deletions after its budget was spent waiting"
    );
}

/// Declares a deletion authority for the rest of `tx`, as the service does.
async fn declare_authority(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>, authority: &str) {
    sqlx::query("SELECT set_config('marketplace.listing_deletion_authority', $1, true)")
        .bind(authority)
        .execute(&mut **tx)
        .await
        .expect("deletion authority");
}

// The follower's SQL as the image before migration 0050 runs it
// (`e1a56c2` listing_deletion.rs and workers.rs), verbatim.
const OLD_IMAGE_ACQUIRE: &str = "INSERT INTO worker_leases (task, holder, lease_until) \
     VALUES ($1, $2, $3) ON CONFLICT (task) DO UPDATE SET holder = EXCLUDED.holder, \
     lease_until = EXCLUDED.lease_until \
     WHERE worker_leases.lease_until <= $4 OR worker_leases.holder = EXCLUDED.holder";
const OLD_IMAGE_LIVE_READ: &str = "SELECT seller_pubky, listing_id FROM listings \
     WHERE aggregate_id = $1 AND deleted_at IS NULL";
const OLD_IMAGE_RELEASE_BINDINGS: &str =
    "UPDATE drop_listings SET active = FALSE, released = TRUE \
     WHERE seller_pubky = $1 AND listing_id = $2 AND NOT released";
const OLD_IMAGE_TOMBSTONE: &str =
    "UPDATE listings SET deleted_at = $2, deleted_event_cursor = $3, \
     server_revision = server_revision + 1, updated_at = $2 \
     WHERE aggregate_id = $1 AND deleted_at IS NULL RETURNING server_revision";
const OLD_IMAGE_RECORD_POLL: &str = "INSERT INTO listing_deletion_cursors \
     (seller_pubky, event_cursor, polled_at) \
     SELECT $1, $2, $3 WHERE EXISTS ( \
         SELECT 1 FROM worker_leases \
         WHERE task = $4 AND holder = $5 AND lease_until > $6) \
     ON CONFLICT (seller_pubky) DO UPDATE SET \
         event_cursor = COALESCE(EXCLUDED.event_cursor, listing_deletion_cursors.event_cursor), \
         polled_at = EXCLUDED.polled_at";

async fn old_image_acquire(app: &TestApp, holder: Uuid) {
    let now = app.clock.now();
    let taken = sqlx::query(OLD_IMAGE_ACQUIRE)
        .bind(marketplace_service::workers::TASK_LISTING_DELETIONS)
        .bind(holder)
        .bind(now + chrono::Duration::seconds(30))
        .bind(now)
        .execute(&app.pool)
        .await
        .expect("old-image acquire");
    assert_eq!(taken.rows_affected(), 1, "the old image holds the lease");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_old_image_tombstone_held_across_a_new_image_takeover_never_commits(pool: PgPool) {
    use marketplace_service::listing_deletion::follow_homeserver_deletions;

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 3).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    homeserver.put_drop_record(
        &seller.pubky,
        "overlap_drop",
        common::drop_record_json(
            &seller.pubky,
            "overlap_drop",
            1,
            &[LISTING_ID],
            &common::ts_after(3600),
            None,
            1,
            1,
        ),
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::sync_drop_command(&seller.pubky, "overlap_drop", 130),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "announced drop: {body}");
    let drop_id = common::drop_aggregate(&seller.pubky, "overlap_drop");

    // The old image confirmed the seller's DEL (cursor 2); the seller then
    // re-created the record (cursor 3).
    homeserver.delete_record(&seller.pubky, LISTING_ID);
    homeserver.put_record(&seller.pubky, LISTING_ID, record(2, 3));
    let old_holder = Uuid::new_v4();
    old_image_acquire(&app, old_holder).await;
    let mut old = app.pool.begin().await.expect("old-image tombstone tx");
    let live: Option<(String, String)> = sqlx::query_as(OLD_IMAGE_LIVE_READ)
        .bind(&aggregate_id)
        .fetch_optional(&mut *old)
        .await
        .expect("old-image live read");
    assert!(live.is_some());

    // Its write stalls past the lease. The new image takes over and reads
    // the DEL and the later PUT.
    app.clock.advance_seconds(31);
    let new_holder = Uuid::new_v4();
    let fence = take_follower_lease(&app, new_holder).await;
    let pass = follower_pass(&app.state, new_holder, fence, 10_000);
    assert_eq!(
        follow_homeserver_deletions(&pass).await.expect("new pass"),
        0
    );
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("3")
    );

    // The old write resumes.
    sqlx::query(OLD_IMAGE_RELEASE_BINDINGS)
        .bind(&seller.pubky)
        .bind(LISTING_ID)
        .execute(&mut *old)
        .await
        .expect("old-image binding release runs");
    let refused = sqlx::query(OLD_IMAGE_TOMBSTONE)
        .bind(&aggregate_id)
        .bind(app.clock.now())
        .bind("2")
        .execute(&mut *old)
        .await
        .expect_err("the old-image tombstone must be refused");
    assert!(
        refused
            .to_string()
            .contains("no current deletion authority"),
        "{refused}"
    );
    old.rollback().await.expect("old tx rolls back");

    // A follower authority whose fence is no longer the lease row's is
    // refused the same way.
    let mut stale = app.pool.begin().await.expect("stale authority tx");
    declare_authority(&mut stale, &format!("follower:{new_holder}:{}", fence - 1)).await;
    let refused = sqlx::query(OLD_IMAGE_TOMBSTONE)
        .bind(&aggregate_id)
        .bind(app.clock.now())
        .bind("2")
        .execute(&mut *stale)
        .await
        .expect_err("a stale fence must be refused");
    assert!(
        refused
            .to_string()
            .contains("no current deletion authority"),
        "{refused}"
    );
    stale.rollback().await.expect("stale tx rolls back");

    assert_eq!(tombstone_row(&app.pool, &aggregate_id).await.0, None);
    assert!(deleted_events(&app.pool, &aggregate_id).await.is_empty());
    assert_eq!(
        unreleased_bindings(&app.pool, &drop_id).await,
        1,
        "the refused tombstone released no binding"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_old_image_cursor_write_cannot_move_the_follower_cursor(pool: PgPool) {
    use marketplace_service::workers::{try_acquire_fenced_lease, TASK_LISTING_DELETIONS};

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    let old_holder = Uuid::new_v4();
    old_image_acquire(&app, old_holder).await;
    let old_now = app.clock.now();
    let old_poll = |pool: PgPool, seller: String, cursor: &'static str| async move {
        sqlx::query(OLD_IMAGE_RECORD_POLL)
            .bind(seller)
            .bind(cursor)
            .bind(old_now)
            .bind(TASK_LISTING_DELETIONS)
            .bind(old_holder)
            .bind(old_now)
            .execute(&pool)
            .await
    };

    // While it still holds the lease, the old image cannot write the cursor.
    let refused = old_poll(app.pool.clone(), seller.pubky.clone(), "1")
        .await
        .expect_err("an old-image cursor write must be refused");
    assert!(
        refused.to_string().contains("no current follower lease"),
        "{refused}"
    );
    assert_eq!(seller_cursor(&app.pool, &seller.pubky).await, None);

    // The reviewer's overlap: the old statement checks its lease before the
    // new image's takeover commits and writes after it.
    app.clock.advance_seconds(31);
    let new_holder = Uuid::new_v4();
    let mut takeover = app.pool.begin().await.expect("takeover tx");
    let fence = try_acquire_fenced_lease(
        &mut *takeover,
        TASK_LISTING_DELETIONS,
        new_holder,
        app.clock.now(),
        30,
    )
    .await
    .expect("takeover")
    .expect("the old lease lapsed");
    declare_authority(&mut takeover, &format!("follower:{new_holder}:{fence}")).await;
    sqlx::query(
        "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
         VALUES ($1, '3', $2)",
    )
    .bind(&seller.pubky)
    .bind(app.clock.now())
    .execute(&mut *takeover)
    .await
    .expect("new-image cursor");
    // The guard refuses the statement before it would wait on the new
    // row; either way it cannot land once the takeover commits.
    let stale = tokio::spawn(old_poll(app.pool.clone(), seller.pubky.clone(), "1"));
    wait_for_finish_or_lock_wait(&app.pool, &stale, "INSERT INTO listing_deletion_cursors").await;
    takeover.commit().await.expect("takeover commits");
    let refused = stale
        .await
        .expect("old statement joins")
        .expect_err("the stale old-image cursor must be refused");
    assert!(
        refused.to_string().contains("no current follower lease"),
        "{refused}"
    );
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("3")
    );

    // Even the current follower cannot move the cursor backwards.
    let mut current = app.pool.begin().await.expect("current tx");
    declare_authority(&mut current, &format!("follower:{new_holder}:{fence}")).await;
    let refused = sqlx::query(
        "UPDATE listing_deletion_cursors SET event_cursor = '2' WHERE seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .execute(&mut *current)
    .await
    .expect_err("a backwards cursor must be refused");
    assert!(refused.to_string().contains("move backwards"), "{refused}");
    current.rollback().await.expect("current tx rolls back");
    assert_eq!(
        seller_cursor(&app.pool, &seller.pubky).await.as_deref(),
        Some("3")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_delete_confirmed_before_a_revival_cannot_hide_the_revived_listing(pool: PgPool) {
    use marketplace_service::listing_deletion::{tombstone, DeletionAuthority};

    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    published_listing(&app, &homeserver, &seller, 1).await;
    let aggregate_id = listing_aggregate(&seller.pubky);
    let tombstone_at = |cursor: &'static str| {
        let pool = app.pool.clone();
        let aggregate_id = aggregate_id.clone();
        let now = app.clock.now();
        async move {
            let mut tx = pool.begin().await.expect("tombstone tx");
            let deleted = tombstone(
                &mut tx,
                DeletionAuthority::Command,
                &aggregate_id,
                cursor,
                "system",
                Uuid::new_v4(),
                now,
            )
            .await;
            if matches!(deleted, Ok(Some(_))) {
                tx.commit().await.expect("tombstone commits");
            }
            deleted.map(|deleted| deleted.is_some())
        }
    };
    assert!(tombstone_at("2").await.expect("first delete"));
    revive(&app, &homeserver, &seller, 131).await;
    let marker: Option<String> =
        sqlx::query_scalar("SELECT revived_from_cursor FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("revival marker");
    assert_eq!(marker.as_deref(), Some("2"));

    // The service skips a delete no newer than the one the revival
    // superseded.
    assert!(!tombstone_at("2").await.expect("stale delete"));
    assert_eq!(tombstone_row(&app.pool, &aggregate_id).await.0, None);

    // The database refuses it from any writer, even with an authority.
    let mut raw = app.pool.begin().await.expect("raw tx");
    declare_authority(&mut raw, "command").await;
    let refused = sqlx::query(OLD_IMAGE_TOMBSTONE)
        .bind(&aggregate_id)
        .bind(app.clock.now())
        .bind("2")
        .execute(&mut *raw)
        .await
        .expect_err("a delete older than the revival must be refused");
    assert!(
        refused.to_string().contains("predates the revival"),
        "{refused}"
    );
    raw.rollback().await.expect("raw tx rolls back");

    // The marker never moves backwards.
    let mut raw = app.pool.begin().await.expect("marker tx");
    let refused =
        sqlx::query("UPDATE listings SET revived_from_cursor = '1' WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .execute(&mut *raw)
            .await
            .expect_err("a backwards marker must be refused");
    assert!(refused.to_string().contains("move backwards"), "{refused}");
    raw.rollback().await.expect("marker tx rolls back");

    // A newer delete of the revived record tombstones it.
    assert!(tombstone_at("4").await.expect("newer delete"));
    assert_eq!(
        tombstone_row(&app.pool, &aggregate_id).await.0,
        Some("4".to_string())
    );
}

const LOWER_LISTING: &str = "order_a";
const HIGHER_LISTING: &str = "order_b";

/// Registers `order_a` and `order_b` with three units each. Their aggregate
/// ids sort in that order.
async fn two_listings(
    app: &TestApp,
    homeserver: Option<&FakeHomeserver>,
    seller: &TestActor,
    register: fn(&str, i64) -> Value,
) -> (String, String) {
    for listing_id in [LOWER_LISTING, HIGHER_LISTING] {
        if let Some(homeserver) = homeserver {
            homeserver.put_record(&seller.pubky, listing_id, record(1, 3));
        }
        let mut command = register(&seller.pubky, 3);
        command["command_id"] = json!(Uuid::new_v4());
        command["aggregate_id"] = json!(format!("listing:{}_{listing_id}", seller.pubky));
        command["payload"]["listing_id"] = json!(listing_id);
        let (status, body) = execute(app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "register {listing_id}: {body}");
    }
    (
        format!("listing:{}_{LOWER_LISTING}", seller.pubky),
        format!("listing:{}_{HIGHER_LISTING}", seller.pubky),
    )
}

/// A checkout of one unit of each listing, the higher aggregate id first.
async fn higher_first_checkout(
    app: &TestApp,
    seller: &TestActor,
    lower: &str,
    higher: &str,
) -> Value {
    let revision = |aggregate_id: &str| {
        let pool = app.pool.clone();
        let aggregate_id = aggregate_id.to_string();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT server_revision FROM listings WHERE aggregate_id = $1",
            )
            .bind(aggregate_id)
            .fetch_one(&pool)
            .await
            .expect("listing revision")
        }
    };
    let mut checkout = common::checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"] = json!([
        { "listing_aggregate_id": higher, "expected_revision": revision(higher).await, "quantity": 1 },
        { "listing_aggregate_id": lower, "expected_revision": revision(lower).await, "quantity": 1 },
    ]);
    checkout
}

/// Checks out both listings higher first and returns the order and payment
/// ids.
async fn higher_first_order(
    app: &TestApp,
    buyer: &TestActor,
    seller: &TestActor,
    lower: &str,
    higher: &str,
) -> (String, String) {
    let checkout = higher_first_checkout(app, seller, lower, higher).await;
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "two-line checkout: {body}");
    assert_eq!(body["result"]["orders"].as_array().map(Vec::len), Some(1));
    (
        body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    )
}

/// Runs the command `spawn` starts, which locks the listings of a two-line
/// order whose lines name the higher aggregate id first, beside a
/// `drop.sync` that share-locks both listings. A share lock on the lower
/// listing, held by a third transaction, pauses the command on it; the
/// sync then takes what it can. Once the share lock goes, a command that
/// locked in line order holds the higher listing while it waits on the
/// sync's lock on the lower one, and the sync waits on the higher one:
/// Postgres aborts one of them.
async fn beside_a_drop_sync<T: Send + 'static>(
    app: &TestApp,
    sync_app: &TestApp,
    homeserver: &FakeHomeserver,
    seller: &TestActor,
    lower: &str,
    spawn: impl FnOnce() -> tokio::task::JoinHandle<T>,
) -> T {
    let mut blocker = app.pool.begin().await.expect("blocker tx");
    sqlx::query("SELECT 1 FROM listings WHERE aggregate_id = $1 FOR SHARE")
        .bind(lower)
        .execute(&mut *blocker)
        .await
        .expect("lower listing share-locked");
    let command = spawn();
    wait_for_finish_or_lock_wait(&app.pool, &command, "listings").await;
    assert!(
        !command.is_finished(),
        "the command did not wait on the lower listing"
    );

    homeserver.put_drop_record(
        &seller.pubky,
        "order_drop",
        common::drop_record_json(
            &seller.pubky,
            "order_drop",
            1,
            &[LOWER_LISTING, HIGHER_LISTING],
            &common::ts_after(3600),
            None,
            1,
            1,
        ),
    );
    let sync = spawn_command(
        sync_app,
        seller,
        common::sync_drop_command(&seller.pubky, "order_drop", 900),
    );
    wait_for_finish_or_lock_wait(&app.pool, &sync, "FOR SHARE OF l").await;
    blocker.rollback().await.expect("release the lower listing");

    let (status, body) = sync.await.expect("sync task");
    assert_eq!(
        status,
        StatusCode::OK,
        "drop.sync beside the command: {body}"
    );
    command.await.expect("command task")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_multi_line_checkout_and_a_drop_sync_lock_listings_in_one_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (lower, higher) = two_listings(&app, Some(&homeserver), &seller, register_command).await;
    let checkout = higher_first_checkout(&app, &seller, &lower, &higher).await;
    let (status, body) = beside_a_drop_sync(&app, &app, &homeserver, &seller, &lower, || {
        spawn_command(&app, &buyer, checkout)
    })
    .await;
    assert_eq!(status, StatusCode::OK, "checkout beside the sync: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_multi_line_payment_hold_and_a_drop_sync_lock_listings_in_one_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (lower, higher) = two_listings(&app, Some(&homeserver), &seller, register_command).await;
    let (_, payment_id) = higher_first_order(&app, &buyer, &seller, &lower, &higher).await;
    let (status, body) = beside_a_drop_sync(&app, &app, &homeserver, &seller, &lower, || {
        spawn_command(
            &app,
            &buyer,
            payment_command(&payment_id, 1, "detected", 0, 901),
        )
    })
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "payment start beside the sync: {body}"
    );
    let (reserved,): (i64,) = sqlx::query_as(
        "SELECT SUM(reserved_quantity)::bigint FROM listings WHERE aggregate_id IN ($1, $2)",
    )
    .bind(&lower)
    .bind(&higher)
    .fetch_one(&app.pool)
    .await
    .expect("reserved units");
    assert_eq!(reserved, 2, "both lines are held");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_multi_line_payment_confirmation_and_a_drop_sync_lock_listings_in_one_order(
    pool: PgPool,
) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (lower, higher) = two_listings(&app, Some(&homeserver), &seller, register_command).await;
    let (_, payment_id) = higher_first_order(&app, &buyer, &seller, &lower, &higher).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "detected", 0, 902),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment start: {body}");
    let (status, body) = beside_a_drop_sync(&app, &app, &homeserver, &seller, &lower, || {
        spawn_command(
            &app,
            &buyer,
            payment_command(&payment_id, 2, "confirmed", 1, 903),
        )
    })
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "confirmation beside the sync: {body}"
    );
    let (sold,): (i64,) = sqlx::query_as(
        "SELECT SUM(sold_quantity)::bigint FROM listings WHERE aggregate_id IN ($1, $2)",
    )
    .bind(&lower)
    .bind(&higher)
    .fetch_one(&app.pool)
    .await
    .expect("sold units");
    assert_eq!(sold, 2, "both lines are sold");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_multi_line_hold_release_and_a_drop_sync_lock_listings_in_one_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (lower, higher) = two_listings(&app, Some(&homeserver), &seller, register_command).await;
    let (_, payment_id) = higher_first_order(&app, &buyer, &seller, &lower, &higher).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "detected", 0, 904),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment start: {body}");
    let after_window = app.clock.now() + chrono::Duration::days(1);
    let expired = beside_a_drop_sync(&app, &app, &homeserver, &seller, &lower, || {
        let state = app.state.clone();
        tokio::spawn(async move {
            marketplace_service::workers::expire_due_payment_windows(&state, after_window)
                .await
                .map_err(|error| error.to_string())
        })
    })
    .await;
    assert_eq!(expired, Ok(1), "the hold released beside the sync");
    let (reserved,): (i64,) = sqlx::query_as(
        "SELECT SUM(reserved_quantity)::bigint FROM listings WHERE aggregate_id IN ($1, $2)",
    )
    .bind(&lower)
    .bind(&higher)
    .fetch_one(&app.pool)
    .await
    .expect("reserved units");
    assert_eq!(reserved, 0, "both lines are released");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_bitcoin_reacquire_and_a_drop_sync_lock_listings_in_one_order(pool: PgPool) {
    use common::paykit_review::{enable_bitcoin, status_confirmed};

    let (app, _stripe, paykit) = common::test_app_with_payments(pool).await;
    // The payments app mirrors listing records but serves no drops; the
    // sync runs through a second app on the same database.
    let homeserver = common::spawn_fake_homeserver().await;
    let sync_app =
        common::test_app_with_homeserver_client(app.pool.clone(), homeserver.client()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("shared_manual");
    enable_bitcoin(&app, &paykit, &seller).await;
    let (lower, higher) = two_listings(&app, None, &seller, common::register_sat_command).await;
    let (order_id, _) = higher_first_order(&app, &buyer, &seller, &lower, &higher).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    marketplace_service::workers::drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let order_uuid = Uuid::parse_str(&order_id).expect("order uuid");
    let reference = marketplace_service::payments::attempt_reference(order_uuid, 1);

    // The hold lapses; the money arrives late and reacquires both lines.
    let after_window = app.clock.now() + chrono::Duration::seconds(7300);
    assert!(
        marketplace_service::workers::expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("payment-window reaper")
            >= 1
    );
    let mut late = status_confirmed("shared_manual", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    let poll_at = after_window + chrono::Duration::seconds(60);
    let applied = beside_a_drop_sync(&app, &sync_app, &homeserver, &seller, &lower, || {
        let state = app.state.clone();
        tokio::spawn(async move {
            let client = state
                .payments
                .as_ref()
                .and_then(|payments| payments.paykit.as_ref())
                .expect("paykit client");
            marketplace_service::workers::verify_due_paykit_payments(&state, client, poll_at)
                .await
                .map_err(|error| error.to_string())
        })
    })
    .await;
    assert_eq!(
        applied,
        Ok(1),
        "the late settlement applied beside the sync"
    );
    let (state, committed): (String, i64) = sqlx::query_as(
        "SELECT o.state, (SELECT SUM(reserved_quantity + sold_quantity)::bigint FROM listings          WHERE aggregate_id IN ($2, $3)) FROM orders o WHERE o.id = $1",
    )
    .bind(order_uuid)
    .bind(&lower)
    .bind(&higher)
    .fetch_one(&app.pool)
    .await
    .expect("late facts");
    assert_eq!(committed, 2, "both lines were reacquired (order {state})");
}
