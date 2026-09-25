//! Offers on pickup listings (issue BitcoinErrorLog/pubky-marketplace#57):
//! an accepted offer settles through the existing pickup order path — the
//! award checkout without an address, the sealed pickup details, the
//! handover, and completion — while shipped awards keep their terms.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use marketplace_service::http::build_router;
use marketplace_service::workers;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    config_durable, create_offer_command, execute, new_actor, offer_action, order_command,
    register_command, send, send_bytes, test_app_with_payments_config, test_pickup_keys,
    FakePaypalIpn, TestActor, TestApp, OFFER_COMMAND_ID,
};

const SPOT: &str = "Central Station, north entrance";
const OFFER_MINOR: i64 = 10_000;
const SHIPPING_MINOR: i64 = 1_200;

/// Serves the seller-signed listing record, as the seller's homeserver does.
struct ListingRecordHomeserver {
    fulfillment: &'static [&'static str],
    shipping: bool,
}

impl HomeserverListingClient for ListingRecordHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            HomeserverFetchOutcome::Found(listing_record(
                seller_pubky,
                self.fulfillment,
                self.shipping,
            ))
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }
}

/// The listing record the Shop publishes. A pickup-only record carries
/// `fulfillmentMethods: ["pickup"]` and no shipping options at all.
fn listing_record(seller: &str, fulfillment: &[&str], shipping: bool) -> Value {
    json!({
        "schemaVersion": 1,
        "recordType": "listing",
        "ownerPubky": seller,
        "revision": 1,
        "listingId": "boots_01",
        "state": "active",
        "title": "Cast iron pan",
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
            "acceptsOffers": true,
        },
        "variants": [{ "id": "boots_01", "options": [], "quantity": 1, "enabled": true }],
        "fulfillmentMethods": fulfillment,
        "shippingOptions": if shipping {
            json!([{
                "id": "seller_flat_rate",
                "pricing": "flat",
                "label": "Standard shipping",
                "price": { "amountMinor": SHIPPING_MINOR, "currency": "USD", "exponent": 2 },
                "estimatedMinDays": 3,
                "estimatedMaxDays": 7,
            }])
        } else {
            json!([])
        },
    })
}

/// A durable (sandbox-off) payments app with the pickup seal, whose
/// homeserver serves a record publishing `fulfillment`.
async fn pickup_offer_app(
    pool: PgPool,
    fulfillment: &'static [&'static str],
    shipping: bool,
) -> (TestApp, FakePaypalIpn) {
    let (app, _stripe, _paykit, ipn, _shippo) =
        test_app_with_payments_config(pool, config_durable()).await;
    let state = app
        .state
        .clone()
        .with_pickup(Some(test_pickup_keys()))
        .with_homeserver(Some(Arc::new(ListingRecordHomeserver {
            fulfillment,
            shipping,
        })));
    (
        TestApp {
            router: build_router(state.clone()),
            pool: app.pool,
            clock: app.clock,
            state,
        },
        ipn,
    )
}

struct Award {
    id: Uuid,
    listing_revision: i64,
    record_sha256: String,
    variant_id: String,
    shipping_minor: i64,
    total_minor: i64,
}

/// Registers the listing with `methods`, then the buyer offers and the
/// seller accepts, against the real homeserver snapshot of the record.
async fn accepted_award(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    methods: &[&str],
    shipping_minor: i64,
) -> Award {
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    let mut register = register_command(&seller.pubky, 1);
    register["payload"]["fulfillment_methods"] = json!(methods);
    register["payload"]["shipping_minor"] = json!(shipping_minor);
    let (status, body) = execute(app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");

    let (status, body) = execute(app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000c101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");

    let row: (Uuid, i64, String, String, i64, i64) = sqlx::query_as(
        "SELECT award_id, accepted_listing_revision, accepted_listing_record_sha256, \
         accepted_variant_id, accepted_shipping_minor, accepted_total_minor \
         FROM offers WHERE id = $1",
    )
    .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    Award {
        id: row.0,
        listing_revision: row.1,
        record_sha256: row.2,
        variant_id: row.3,
        shipping_minor: row.4,
        total_minor: row.5,
    }
}

fn offer_checkout(
    seller: &str,
    award: &Award,
    fulfillment: Option<&str>,
    with_address: bool,
) -> Value {
    let mut payload = json!({
        "offer_id": OFFER_COMMAND_ID,
        "award_id": award.id,
        "listing_aggregate_id": format!("listing:{seller}_boots_01"),
        "listing_revision": award.listing_revision,
        "listing_record_sha256": award.record_sha256,
        "variant_id": award.variant_id,
        "quantity": 1,
        "guarantee_policy_version": 1,
    });
    if let Some(fulfillment) = fulfillment {
        payload["fulfillment"] = json!(fulfillment);
    }
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
        "command_id": "00000000-0000-4000-8000-00000000c102",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": payload,
    })
}

async fn order_revision(app: &TestApp, order_id: &str) -> (String, i64) {
    sqlx::query_as("SELECT state, revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row")
}

async fn order_step(app: &TestApp, token: &str, kind: &str, order_id: &str, n: u64) -> Value {
    let (_, revision) = order_revision(app, order_id).await;
    let (status, body) = execute(
        app,
        token,
        &order_command(kind, order_id, revision, json!({}), n),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{kind} failed: {body}");
    body
}

fn completed_ipn(order_id: &str, gross: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/completed.ipn",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in url::form_urlencoded::parse(text.trim_end().as_bytes()) {
        let value = match name.as_ref() {
            "custom" => order_id.to_string(),
            "mc_gross" | "payment_gross" => gross.to_string(),
            _ => value.into_owned(),
        };
        serializer.append_pair(&name, &value);
    }
    serializer.finish().into_bytes()
}

// Offer → accept → award checkout (pickup, no address) → sealed pickup
// details → PayPal payment → buyer reveal → ready → handover → completed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn pickup_only_offer_settles_through_the_pickup_order_path(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["pickup"], false).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");

    let award = accepted_award(&app, &seller, &buyer, &["pickup"], 0).await;
    assert_eq!(award.shipping_minor, 0);
    assert_eq!(award.total_minor, OFFER_MINOR);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let order = &body["result"]["order"];
    assert_eq!(order["lines"][0]["fulfillment"], json!("pickup"));
    assert_eq!(order["shipping"]["amount_minor"], json!(0));
    assert_eq!(order["total"]["amount_minor"], json!(OFFER_MINOR));
    let order_id = order["id"].as_str().expect("order id").to_string();
    let (fulfillment, address_is_null, total): (String, bool, i64) = sqlx::query_as(
        "SELECT fulfillment, delivery_address IS NULL, total_minor FROM orders WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order row");
    assert_eq!(
        (fulfillment.as_str(), address_is_null, total),
        ("pickup", true, OFFER_MINOR)
    );

    let (status, body) = execute(
        &app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": "00000000-0000-4000-8000-00000000c103",
            "aggregate_id": format!("listing:{}_boots_01", seller.pubky),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "pickup_details.set",
            "payload": {
                "expected_version": 0,
                "details": {
                    "kind": "spot",
                    "spot": SPOT,
                    "instructions": "Ask for the blue backpack.",
                    "availability": {
                        "windows": [{ "day": "sat", "start": "10:00", "end": "14:00" }],
                        "zone": "Europe/Berlin",
                    },
                },
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set details failed: {body}");

    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        completed_ipn(&order_id, "100.00"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(order_revision(&app, &order_id).await.0, "paid");

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}/pickup-details"),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "reveal failed: {body}");
    assert!(body.to_string().contains(SPOT), "{body}");

    let ready = order_step(
        &app,
        &seller.token,
        "fulfillment.mark_ready",
        &order_id,
        0xc104,
    )
    .await;
    assert_eq!(ready["result"]["order"]["state"], json!("ready_for_pickup"));
    let handed = order_step(
        &app,
        &buyer.token,
        "fulfillment.confirm_pickup",
        &order_id,
        0xc105,
    )
    .await;
    assert_eq!(handed["result"]["order"]["state"], json!("delivered"));

    let due = app.clock.now() + chrono::Duration::days(14);
    let completed = workers::complete_due_delivered_orders(&app.pool, due, 14, 100, 2)
        .await
        .expect("completion sweep runs");
    assert_eq!(completed, 1);
    assert_eq!(order_revision(&app, &order_id).await.0, "completed");
}

// Shipping-plus-pickup: the buyer may collect instead, and then pays the
// accepted merchandise only; choosing shipping keeps the shipped terms.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shipping_and_pickup_award_prices_the_chosen_fulfillment(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["physical", "shipping", "pickup"], true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(
        &app,
        &seller,
        &buyer,
        &["shipping", "pickup"],
        SHIPPING_MINOR,
    )
    .await;
    assert_eq!(award.shipping_minor, SHIPPING_MINOR);
    assert_eq!(award.total_minor, OFFER_MINOR + SHIPPING_MINOR);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    assert_eq!(
        body["result"]["order"]["shipping"]["amount_minor"],
        json!(0)
    );
    assert_eq!(
        body["result"]["order"]["total"]["amount_minor"],
        json!(OFFER_MINOR)
    );
    let payment_minor: i64 =
        sqlx::query_scalar("SELECT amount_minor FROM payments WHERE order_id = $1::uuid")
            .bind(body["result"]["order"]["id"].as_str().expect("order id"))
            .fetch_one(&app.pool)
            .await
            .expect("payment row");
    assert_eq!(payment_minor, OFFER_MINOR);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shipping_award_on_a_shipping_and_pickup_listing_keeps_shipped_terms(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["physical", "shipping", "pickup"], true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(
        &app,
        &seller,
        &buyer,
        &["shipping", "pickup"],
        SHIPPING_MINOR,
    )
    .await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, None, true),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let order = &body["result"]["order"];
    assert_eq!(order["lines"][0]["fulfillment"], json!("shipping"));
    assert_eq!(order["shipping"]["amount_minor"], json!(SHIPPING_MINOR));
    assert_eq!(
        order["total"]["amount_minor"],
        json!(OFFER_MINOR + SHIPPING_MINOR)
    );
}

// The fulfillment is the buyer's explicit choice, checked against what the
// listing publishes; the address rule follows the choice.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn award_checkout_refuses_a_fulfillment_the_listing_does_not_publish(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["pickup"], false).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(&app, &seller, &buyer, &["pickup"], 0).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, None, true),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), true),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("shipping"), false),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("order count");
    assert_eq!(orders, 0, "refused award checkouts write nothing");
}

async fn award_checkout_count(app: &TestApp) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("order count")
}

async fn accepted_methods(app: &TestApp) -> Option<Vec<String>> {
    sqlx::query_scalar("SELECT accepted_fulfillment_methods FROM offers WHERE id = $1")
        .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .fetch_one(&app.pool)
        .await
        .expect("accepted methods")
}

// Split authority, one way: the durable row still publishes shipping, but the
// seller-signed record the award was priced from is pickup-only (shipping 0).
// Omitting `fulfillment` (= shipping) must not create a free-shipping order.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn durable_shipping_row_with_a_pickup_snapshot_never_ships_free(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["pickup"], false).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(&app, &seller, &buyer, &["shipping"], SHIPPING_MINOR).await;
    assert_eq!(award.shipping_minor, 0);
    assert_eq!(
        accepted_methods(&app).await,
        Some(vec!["pickup".to_string()])
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, None, true),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
    assert_eq!(award_checkout_count(&app).await, 0);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["lines"][0]["fulfillment"],
        json!("pickup")
    );
    assert_eq!(
        body["result"]["order"]["shipping"]["amount_minor"],
        json!(0)
    );
}

// Split authority, the other way: the durable row is pickup-only, but the
// signed record the award was priced from ships (shipping 12.00). A pickup
// order must not be created from a shipped award.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn durable_pickup_row_with_a_shipping_snapshot_never_makes_a_pickup_order(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["physical", "shipping"], true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(&app, &seller, &buyer, &["pickup"], 0).await;
    assert_eq!(award.shipping_minor, SHIPPING_MINOR);
    assert_eq!(
        accepted_methods(&app).await,
        Some(vec!["shipping".to_string()])
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
    assert_eq!(award_checkout_count(&app).await, 0);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, None, true),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["shipping"]["amount_minor"],
        json!(SHIPPING_MINOR)
    );
}

// The award keeps the fulfillment authority it was accepted with: a later
// change to the listing row's methods neither removes an accepted method nor
// adds one. The projection shows the accepted methods.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn award_keeps_its_accepted_fulfillment_after_the_listing_changes(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["physical", "shipping", "pickup"], true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(
        &app,
        &seller,
        &buyer,
        &["shipping", "pickup"],
        SHIPPING_MINOR,
    )
    .await;

    let (status, offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{offers}");
    assert_eq!(
        offers["offers"][0]["award"]["fulfillment_methods"],
        json!(["shipping", "pickup"])
    );

    sqlx::query("UPDATE listings SET fulfillment_methods = '{shipping}' WHERE aggregate_id = $1")
        .bind(format!("listing:{}_boots_01", seller.pubky))
        .execute(&app.pool)
        .await
        .expect("listing methods change after acceptance");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["lines"][0]["fulfillment"],
        json!("pickup")
    );
    assert_eq!(
        body["result"]["order"]["total"]["amount_minor"],
        json!(OFFER_MINOR)
    );
}

// An award accepted before the snapshot recorded its methods (NULL) is
// shipping-only, as it was priced.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn award_accepted_before_0044_stays_shipping_only(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["physical", "shipping", "pickup"], true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let award = accepted_award(
        &app,
        &seller,
        &buyer,
        &["shipping", "pickup"],
        SHIPPING_MINOR,
    )
    .await;
    sqlx::query("UPDATE offers SET accepted_fulfillment_methods = NULL WHERE id = $1")
        .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
        .execute(&app.pool)
        .await
        .expect("legacy award");

    let (status, offers) = send(
        app.router.clone(),
        "GET",
        "/v1/offers",
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{offers}");
    assert_eq!(
        offers["offers"][0]["award"]["fulfillment_methods"],
        json!(["shipping"])
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_checkout(&seller.pubky, &award, Some("pickup"), false),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
}

// 0044's constraints: only physical methods, and an award whose snapshot does
// not ship carries zero shipping.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0044_constrains_the_accepted_methods(pool: PgPool) {
    let (app, _ipn) = pickup_offer_app(pool, &["pickup"], false).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    accepted_award(&app, &seller, &buyer, &["pickup"], 0).await;
    let offer_id = Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id");
    for (methods, shipping) in [
        (vec!["digital"], 0_i64),
        (vec!["pickup"], SHIPPING_MINOR),
        (vec![], 0),
    ] {
        let result = sqlx::query(
            "UPDATE offers SET accepted_fulfillment_methods = $2, accepted_shipping_minor = $3 \
             WHERE id = $1",
        )
        .bind(offer_id)
        .bind(&methods)
        .bind(shipping)
        .execute(&app.pool)
        .await;
        assert!(
            result.is_err(),
            "methods {methods:?} with shipping {shipping} must be refused"
        );
    }
    sqlx::query(
        "UPDATE offers SET accepted_fulfillment_methods = '{shipping,pickup}', \
         accepted_shipping_minor = $2 WHERE id = $1",
    )
    .bind(offer_id)
    .bind(SHIPPING_MINOR)
    .execute(&app.pool)
    .await
    .expect("shipping-and-pickup award may carry shipping");
    sqlx::raw_sql(include_str!(
        "../migrations/0044_offer_accepted_fulfillment_methods.sql"
    ))
    .execute(&app.pool)
    .await
    .expect("0044 must be directly rerunnable");
}
