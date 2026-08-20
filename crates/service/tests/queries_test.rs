//! Role-scoped read projection tests: every projection carries the
//! aggregate revision the client needs for `expected_revision`, every
//! endpoint refuses non-participants (object fetches 404, lists scope in
//! the WHERE clause like `GET /v1/reports`), and no projection exposes
//! delivery details or Locks bundle ids (ADR-0019 §8).

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command_with_id, create_offer_command, execute, listing_aggregate, new_actor,
    place_bid_command, register_auction_command, register_command, reserve_command, send, test_app,
    TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::drain_outbox;

async fn get(app: &TestApp, uri: &str, token: Option<&str>) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, token, &json!(null)).await
}

fn assert_not_found(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
}

/// A checkout whose ids do not collide with another actor's fixtures.
fn checkout(seller_pubky: &str, command_index: u64) -> Value {
    checkout_command_with_id(
        seller_pubky,
        &common::indexed_command_id(0x9000, command_index),
    )
}

/// An offer command whose aggregate does not collide with another offer.
fn offer_with_id(seller_pubky: &str, command_id: &str) -> Value {
    let mut command = create_offer_command(seller_pubky, 1);
    command["command_id"] = json!(command_id);
    command
}

#[sqlx::test]
async fn listing_projection_is_public_catalog_data_for_authenticated_users(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    let (status, body) =
        execute(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 2, 1)).await;
    assert_eq!(status, StatusCode::OK, "reserve failed: {body}");

    let aggregate = listing_aggregate(&seller.pubky);

    // The projection requires a session: no trust-me anonymous reads.
    let (status, _) = get(&app, &format!("/v1/listings/{aggregate}"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Any authenticated user reads the catalog projection, including the
    // server revision the next command needs as expected_revision.
    let (status, listing) = get(
        &app,
        &format!("/v1/listings/{aggregate}"),
        Some(&stranger.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "listing read failed: {listing}");
    assert_eq!(listing["aggregate_id"], json!(aggregate));
    assert_eq!(listing["server_revision"], json!(2));
    assert_eq!(listing["state"], json!("available"));
    assert_eq!(listing["available_quantity"], json!(3));
    assert_eq!(listing["reserved_quantity"], json!(2));
    assert_eq!(listing["sale_format"], json!("fixed_price"));
    assert_eq!(listing["auction"], Value::Null);
    assert_eq!(listing["unit_price"]["amount_minor"], json!(12_500));

    // The reservation holder's identity is not part of the catalog.
    let serialized = listing.to_string();
    assert!(
        !serialized.contains(&buyer.pubky),
        "listing projection must not leak reservation buyer identities"
    );

    let (status, body) = get(
        &app,
        "/v1/listings/listing:unknown_aggregate",
        Some(&stranger.token),
    )
    .await;
    assert_not_found(status, &body);
}

#[sqlx::test]
async fn auction_projection_exposes_the_leader_but_no_other_bidder_data(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let first_bidder = new_actor(&app).await;
    let second_bidder = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    let (status, body) = execute(
        &app,
        &first_bidder.token,
        &place_bid_command(&seller.pubky, 1, 7_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first bid failed: {body}");
    let (status, body) = execute(
        &app,
        &second_bidder.token,
        &place_bid_command(&seller.pubky, 2, 9_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second bid failed: {body}");

    let aggregate = listing_aggregate(&seller.pubky);
    let (status, listing) = get(
        &app,
        &format!("/v1/listings/{aggregate}"),
        Some(&stranger.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "listing read failed: {listing}");
    assert_eq!(listing["server_revision"], json!(3));
    assert_eq!(
        listing["auction"]["leader_pubky"],
        json!(second_bidder.pubky),
        "the current leader is part of the auction state"
    );
    assert_eq!(listing["auction"]["bid_count"], json!(2));

    // Outbid bidders and maximum (proxy) bids stay private.
    let serialized = listing.to_string();
    assert!(
        !serialized.contains(&first_bidder.pubky),
        "auction projection must not expose outbid bidder identities"
    );
    assert!(
        !serialized.contains("maximum_amount"),
        "auction projection must not expose proxy bid maximums"
    );
}

#[sqlx::test]
async fn offers_are_readable_only_by_their_participants(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    execute(
        &app,
        &other_seller.token,
        &register_command(&other_seller.pubky, 5),
    )
    .await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_with_id(&seller.pubky, "00000000-0000-4000-9000-000000000001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer failed: {body}");
    let offer_id = body["result"]["offer"]["id"].clone();
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &offer_with_id(&other_seller.pubky, "00000000-0000-4000-9000-000000000002"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "other offer failed: {body}");
    let other_offer_id = body["result"]["offer"]["id"].clone();

    // Buyer and seller each read the offer, with the revision the next
    // offer command needs and the negotiation message both already hold.
    for participant in [&buyer, &seller] {
        let (status, body) = get(&app, "/v1/offers", Some(&participant.token)).await;
        assert_eq!(status, StatusCode::OK, "offer list failed: {body}");
        let offers = body["offers"].as_array().expect("offers is an array");
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0]["id"], offer_id);
        assert_eq!(offers[0]["revision"], json!(1));
        assert_eq!(offers[0]["state"], json!("pending"));
        assert_eq!(offers[0]["message"], json!("Would you take this?"));
    }

    // A user who participates in a different offer gets exactly their own:
    // the scope filters rather than merely returning nothing.
    let (status, body) = get(&app, "/v1/offers", Some(&other_buyer.token)).await;
    assert_eq!(status, StatusCode::OK, "other list failed: {body}");
    let offers = body["offers"].as_array().expect("offers is an array");
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0]["id"], other_offer_id);
    assert_ne!(offers[0]["id"], offer_id);
}

#[sqlx::test]
async fn orders_and_payments_are_participant_scoped_and_redacted(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    execute(
        &app,
        &other_seller.token,
        &register_command(&other_seller.pubky, 5),
    )
    .await;

    let (status, body) = execute(&app, &buyer.token, &checkout(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let locks_bundle_id = body["result"]["payments"][0]["locks_bundle_id"]
        .as_str()
        .expect("command result carries the bundle id")
        .to_string();
    let (status, body) = execute(&app, &other_buyer.token, &checkout(&other_seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "other checkout failed: {body}");
    let other_order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();

    // Sessions are required on every projection endpoint.
    let (status, _) = get(&app, "/v1/orders", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Buyer and seller both read the order with its embedded payment; the
    // delivery address and the Locks bundle id are redacted (ADR-0019 §8).
    for participant in [&buyer, &seller] {
        let (status, body) = get(&app, "/v1/orders", Some(&participant.token)).await;
        assert_eq!(status, StatusCode::OK, "order list failed: {body}");
        let orders = body["orders"].as_array().expect("orders is an array");
        assert_eq!(orders.len(), 1);
        let order = &orders[0];
        assert_eq!(order["id"], json!(order_id));
        assert_eq!(order["revision"], json!(1));
        assert_eq!(order["state"], json!("pending_payment"));
        assert_eq!(order["receipt_id"], Value::Null);
        assert!(
            order.get("delivery_address").is_none(),
            "order projection must not expose the delivery address"
        );
        assert_eq!(order["payment"]["id"], json!(payment_id));
        assert_eq!(order["payment"]["revision"], json!(1));
        assert!(
            order["payment"].get("locks_bundle_id").is_none(),
            "payment projection must not expose the Locks bundle id"
        );
        let serialized = body.to_string();
        assert!(
            !serialized.contains("1 Market Street"),
            "no projection may carry delivery details"
        );
        assert!(
            !serialized.contains(&locks_bundle_id),
            "no projection may carry the Locks bundle id"
        );
    }

    // A non-participant with orders of their own reads exactly those.
    let (status, body) = get(&app, "/v1/orders", Some(&other_buyer.token)).await;
    assert_eq!(status, StatusCode::OK, "other order list failed: {body}");
    let orders = body["orders"].as_array().expect("orders is an array");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["id"], json!(other_order_id));

    // Object fetches refuse non-participants without revealing existence.
    let (status, body) = get(
        &app,
        &format!("/v1/orders/{order_id}"),
        Some(&other_buyer.token),
    )
    .await;
    assert_not_found(status, &body);
    let (status, body) = get(
        &app,
        &format!("/v1/payments/{payment_id}"),
        Some(&other_buyer.token),
    )
    .await;
    assert_not_found(status, &body);

    // Participants fetch the same objects successfully, redacted.
    let (status, order) = get(&app, &format!("/v1/orders/{order_id}"), Some(&seller.token)).await;
    assert_eq!(status, StatusCode::OK, "order fetch failed: {order}");
    assert_eq!(order["id"], json!(order_id));
    assert!(order.get("delivery_address").is_none());
    assert_eq!(order["payment"]["id"], json!(payment_id));
    let (status, payment) = get(
        &app,
        &format!("/v1/payments/{payment_id}"),
        Some(&buyer.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment fetch failed: {payment}");
    assert_eq!(payment["id"], json!(payment_id));
    assert_eq!(payment["revision"], json!(1));
    assert_eq!(payment["state"], json!("awaiting_entitlement"));
    assert!(payment.get("locks_bundle_id").is_none());
}

#[sqlx::test]
async fn notifications_are_readable_only_by_their_recipient(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    execute(
        &app,
        &other_seller.token,
        &register_command(&other_seller.pubky, 5),
    )
    .await;
    execute(&app, &buyer.token, &checkout(&seller.pubky, 1)).await;
    execute(&app, &other_buyer.token, &checkout(&other_seller.pubky, 2)).await;
    let delivered = drain_outbox(&app.pool, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    assert_eq!(delivered, 2);

    // Each seller reads exactly the notification addressed to them; the
    // scope filters rather than merely returning nothing.
    let (status, body) = get(&app, "/v1/notifications", Some(&seller.token)).await;
    assert_eq!(status, StatusCode::OK, "notification list failed: {body}");
    let notifications = body["notifications"]
        .as_array()
        .expect("notifications is an array");
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0]["type"], json!("order_created"));
    assert_eq!(notifications[0]["recipient_pubky"], json!(seller.pubky));
    assert_eq!(notifications[0]["actor_pubky"], json!(buyer.pubky));
    assert_eq!(notifications[0]["read_at"], Value::Null);

    let (status, body) = get(&app, "/v1/notifications", Some(&other_seller.token)).await;
    assert_eq!(status, StatusCode::OK, "notification list failed: {body}");
    let notifications = body["notifications"]
        .as_array()
        .expect("notifications is an array");
    assert_eq!(notifications.len(), 1);
    assert_eq!(
        notifications[0]["recipient_pubky"],
        json!(other_seller.pubky)
    );
    assert_eq!(notifications[0]["actor_pubky"], json!(other_buyer.pubky));
}

#[sqlx::test]
async fn list_limits_are_bounded_and_ordering_is_newest_first(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;

    let (status, body) = execute(&app, &buyer.token, &checkout(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    // The second order is created one second later so the newest-first
    // ordering is observable; the first checkout bumped the listing to
    // revision 2.
    app.clock
        .set(app.clock.now() + chrono::Duration::seconds(1));
    let mut second_checkout = checkout(&seller.pubky, 2);
    second_checkout["payload"]["lines"][0]["expected_revision"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &second_checkout).await;
    assert_eq!(status, StatusCode::OK, "second checkout failed: {body}");
    let newest_order_id = body["result"]["orders"][0]["id"].clone();

    for uri in ["/v1/orders?limit=0", "/v1/orders?limit=201"] {
        let (status, body) = get(&app, uri, Some(&buyer.token)).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "unexpected: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    }

    let (status, body) = get(&app, "/v1/orders?limit=1", Some(&buyer.token)).await;
    assert_eq!(status, StatusCode::OK, "bounded list failed: {body}");
    let orders = body["orders"].as_array().expect("orders is an array");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["id"], newest_order_id);

    let (status, body) = get(&app, "/v1/orders", Some(&buyer.token)).await;
    assert_eq!(status, StatusCode::OK, "default list failed: {body}");
    assert_eq!(body["orders"].as_array().map(Vec::len), Some(2));
}
