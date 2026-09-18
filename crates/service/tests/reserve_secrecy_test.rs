//! Focused end-to-end SQLx/router proofs for secret auction reserve authority.

mod common;

use axum::http::StatusCode;
use common::{
    close_auction_command, count, execute, indexed_command_id, listing_aggregate, new_actor,
    place_bid_command, register_auction_command, send, sync_command, test_app_with_homeserver,
    TestApp,
};
use marketplace_service::reserve_secrecy::ensure_reserve_free;
use serde_json::{json, Value};
use sqlx::PgPool;

const CAPTURED_SELLER: &str = "n3pfudgxncn8i1e6icuq7umoczemjuyi6xdfrfczk3o8ej3e55my";
const CAPTURED_LISTING: &str = "7dd7e4279c2745df8b174656b9ee0670";

#[test]
fn captured_public_auction_shape_is_the_positive_and_negative_parser_boundary() {
    let captured: Value = serde_json::from_str(include_str!(
        "fixtures/reserve-secrecy/public-auction-wire.json"
    ))
    .expect("Wave 1 public capture fixture parses");
    ensure_reserve_free(&captured).expect("captured public auction is reserve-free");
    marketplace_service::homeserver::registration_payload_from_record(
        CAPTURED_SELLER,
        CAPTURED_LISTING,
        &captured,
    )
    .expect("captured record has no malformed lock")
    .expect("captured record is a valid public auction");

    for injected in [
        json!({"reserve_price": null}),
        json!({"nested": {"reservePrice": false}}),
        json!({"rows": [{"reserve_met": null}]}),
        json!({"rows": [{"nested": [{"reserveMet": false}]}]}),
    ] {
        let mut bad = captured.clone();
        bad["captureRegression"] = injected;
        assert!(
            marketplace_service::homeserver::registration_payload_from_record(
                CAPTURED_SELLER,
                CAPTURED_LISTING,
                &bad,
            )
            .expect("guard returns a contract refusal")
            .is_none(),
            "captured public shape with injected reserve key must be rejected"
        );
    }
}

fn public_record_from_command(command: &Value) -> Value {
    let payload = &command["payload"];
    let terms = &payload["auction_terms"];
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "ownerPubky": payload["seller_pubky"],
        "listingId": payload["listing_id"],
        "title": payload["title"].as_str().unwrap_or("Marketplace item"),
        "revision": payload["listing_revision"],
        "media": [{
            "id": "media_01",
            "type": "image",
            "mimeType": "image/jpeg",
            "contentHash": payload["content_hash"],
            "byteSize": 1
        }],
        "variants": [{
            "id": "variant_01",
            "enabled": true,
            "quantity": payload["quantity"],
            "priceOverride": null,
            "sku": null
        }],
        "shippingOptions": [{
            "id": "shipping",
            "pricing": "flat",
            "label": "Shipping",
            "price": {
                "amountMinor": payload["shipping_minor"],
                "currency": payload["unit_price"]["currency"],
                "exponent": payload["unit_price"]["exponent"]
            }
        }],
        "fulfillmentMethods": ["shipping"],
        "sale": {
            "format": "auction",
            "startingPrice": {
                "amountMinor": payload["unit_price"]["amount_minor"],
                "currency": payload["unit_price"]["currency"],
                "exponent": payload["unit_price"]["exponent"]
            },
            "startsAt": terms["starts_at"],
            "endsAt": terms["ends_at"],
            "minimumIncrement": {
                "amountMinor": terms["minimum_increment"]["amount_minor"],
                "currency": terms["minimum_increment"]["currency"],
                "exponent": terms["minimum_increment"]["exponent"]
            },
            "antiSnipingWindowSeconds": terms["anti_sniping_window_seconds"],
            "antiSnipingExtensionSeconds": terms["anti_sniping_extension_seconds"]
        },
        "capturedShapeBoundary": {
            "adultOnly": false,
            "attributes": null,
            "location": {"countryCode": "US", "region": "NY"}
        }
    })
}

fn assert_non_seller(value: &Value) {
    ensure_reserve_free(value).expect("non-seller boundary is reserve-free");
}

async fn listing_get(
    app: &TestApp,
    token: Option<&str>,
    aggregate_id: &str,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{aggregate_id}"),
        token,
        &Value::Null,
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn seller_only_projection_and_all_non_seller_boundaries_remain_secret_after_close(
    pool: PgPool,
) {
    common::install_log_capture();
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let other_bidder = new_actor(&app).await;
    let observer = new_actor(&app).await;
    let mut register = register_auction_command(&seller.pubky);
    register["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(6_789);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&register),
    );

    let (status, registered) = execute(&app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let seller_listing = &registered["result"]["listing"];
    assert_eq!(seller_listing["reserve_record_revision"], json!(1));
    assert!(seller_listing["reserve_price"].is_object());
    assert_eq!(seller_listing["reserve_met"], json!(false));
    assert_non_seller(&seller_listing["auction"]);

    let aggregate_id = listing_aggregate(&seller.pubky);
    let (status, anonymous) = listing_get(&app, None, &aggregate_id).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{anonymous}");
    assert_non_seller(&anonymous);

    let (status, observer_view) = listing_get(&app, Some(&observer.token), &aggregate_id).await;
    assert_eq!(status, StatusCode::OK, "{observer_view}");
    assert_non_seller(&observer_view);
    assert!(observer_view.get("viewer_bid").is_none());

    let (status, first_bid) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first_bid}");
    assert_non_seller(&first_bid);
    assert!(first_bid["result"]["listing"]["viewer_bid"].is_object());

    let (status, second_bid) = execute(
        &app,
        &other_bidder.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second_bid}");
    assert_non_seller(&second_bid);

    let (status, history) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{aggregate_id}/bids"),
        Some(&observer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_non_seller(&history);

    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 50),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{closed}");
    assert_eq!(closed["result"]["outcome"], json!("sold"));
    assert_eq!(closed["result"]["listing"]["reserve_met"], json!(true));
    assert!(closed["result"]["listing"]["reserve_price"].is_object());
    assert_non_seller(&closed["result"]["listing"]["auction"]);

    let (status, bidder_after_close) = listing_get(&app, Some(&bidder.token), &aggregate_id).await;
    assert_eq!(status, StatusCode::OK, "{bidder_after_close}");
    assert_eq!(bidder_after_close["auction"]["status"], json!("sold"));
    assert_non_seller(&bidder_after_close);

    let stored_auction: Value =
        sqlx::query_scalar("SELECT auction FROM listings WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("stored auction");
    assert_non_seller(&stored_auction);
    let result_rows: Vec<(String, Value)> =
        sqlx::query_as("SELECT actor_pubky, result FROM command_results ORDER BY created_at")
            .fetch_all(&app.pool)
            .await
            .expect("command results");
    for (actor, result) in result_rows {
        if actor != seller.pubky {
            assert_non_seller(&result);
        }
    }
    let outbox: Vec<Value> = sqlx::query_scalar("SELECT payload FROM outbox")
        .fetch_all(&app.pool)
        .await
        .expect("outbox rows");
    for payload in outbox {
        assert_non_seller(&payload);
    }
    let logs = common::captured_logs();
    for forbidden in [
        "reserve_price",
        "reservePrice",
        "reserve_met",
        "reserveMet",
        "\"amount_minor\":6789",
    ] {
        assert!(
            !logs.contains(forbidden),
            "logs contain forbidden reserve marker {forbidden}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn register_edit_uses_dual_cas_and_command_body_idempotency(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let mut create = register_auction_command(&seller.pubky);
    create["payload"]["auction_terms"]["starts_at"] = json!("2026-08-19T22:10:00.000Z");
    create["payload"]["auction_terms"]["ends_at"] = json!("2026-08-19T22:20:00.000Z");
    create["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(8_000);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&create),
    );
    let (status, created) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let mut edit = create.clone();
    edit["command_id"] = json!(indexed_command_id(0x9001, 1));
    edit["expected_revision"] = json!(1);
    edit["payload"]["listing_revision"] = json!(2);
    edit["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
    edit["payload"]["auction_reserve"]["record_revision"] = json!(2);
    edit["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(7_000);
    homeserver.put_record(&seller.pubky, "boots_01", public_record_from_command(&edit));
    let (status, edited) = execute(&app, &seller.token, &edit).await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["revision"], json!(2));
    assert_eq!(
        edited["result"]["listing"]["reserve_record_revision"],
        json!(2)
    );

    let (status, replay) = execute(&app, &seller.token, &edit).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, edited);

    let mut changed_body = edit.clone();
    changed_body["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(6_999);
    let (status, conflict) = execute(&app, &seller.token, &changed_body).await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["error"]["code"], json!("IDEMPOTENCY_CONFLICT"));

    let before: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'listing_revision', l.listing_revision,
            'server_revision', l.server_revision,
            'record_revision', r.record_revision,
            'reserve_amount_minor', r.reserve_amount_minor,
            'last_command_id', r.last_command_id
         )
         FROM listings l
         JOIN listing_auction_reserves r ON r.listing_aggregate_id = l.aggregate_id
         WHERE l.aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("dual authority snapshot");

    let mut stale_reserve = edit.clone();
    stale_reserve["command_id"] = json!(indexed_command_id(0x9001, 2));
    stale_reserve["expected_revision"] = json!(2);
    stale_reserve["payload"]["listing_revision"] = json!(3);
    stale_reserve["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
    stale_reserve["payload"]["auction_reserve"]["record_revision"] = json!(2);
    stale_reserve["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(6_500);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&stale_reserve),
    );
    let (status, stale) = execute(&app, &seller.token, &stale_reserve).await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["error"]["code"], json!("REVISION_CONFLICT"));

    let after: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'listing_revision', l.listing_revision,
            'server_revision', l.server_revision,
            'record_revision', r.record_revision,
            'reserve_amount_minor', r.reserve_amount_minor,
            'last_command_id', r.last_command_id
         )
         FROM listings l
         JOIN listing_auction_reserves r ON r.listing_aggregate_id = l.aggregate_id
         WHERE l.aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("dual authority snapshot");
    assert_eq!(after, before, "stale reserve CAS writes nothing");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn auction_sync_is_reserve_blind_mutation_free_and_rejects_public_reserve_keys(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let create = register_auction_command(&seller.pubky);
    let public = public_record_from_command(&create);
    homeserver.put_record(&seller.pubky, "boots_01", public.clone());
    let (status, missing_listing) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 900),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{missing_listing}");
    assert_eq!(
        missing_listing["error"]["code"],
        json!("SELLER_REGISTRATION_REQUIRED")
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM listings").await, 0);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 0);

    let (status, body) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let aggregate_id = listing_aggregate(&seller.pubky);
    let before: Value = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM listing_auction_reserves r WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&app.pool)
    .await
    .expect("secret row");

    let (status, synced) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{synced}");
    assert_eq!(synced["event_ids"], json!([]));
    assert_non_seller(&synced);
    let after: Value = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM listing_auction_reserves r WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&app.pool)
    .await
    .expect("secret row");
    assert_eq!(after, before);

    let listing_before_flip: Value =
        sqlx::query_scalar("SELECT to_jsonb(l) FROM listings l WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("auction before public format flip");
    let mut fixed_price_flip = public.clone();
    fixed_price_flip["revision"] = json!(2);
    fixed_price_flip["sale"] = json!({
        "format": "fixed_price",
        "unitPrice": {
            "amountMinor": create["payload"]["unit_price"]["amount_minor"],
            "currency": create["payload"]["unit_price"]["currency"],
            "exponent": create["payload"]["unit_price"]["exponent"]
        }
    });
    homeserver.put_record(&seller.pubky, "boots_01", fixed_price_flip);
    for (token, command_number) in [(&buyer.token, 901), (&seller.token, 902)] {
        let (status, refused) = execute(
            &app,
            token,
            &sync_command(&seller.pubky, "boots_01", command_number),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{refused}");
        assert_eq!(
            refused["error"]["code"],
            json!("SELLER_REGISTRATION_REQUIRED")
        );
    }
    let listing_after_flip: Value =
        sqlx::query_scalar("SELECT to_jsonb(l) FROM listings l WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("auction after refused public format flip");
    let reserve_after_flip: Value = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM listing_auction_reserves r WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&app.pool)
    .await
    .expect("reserve after refused public format flip");
    assert_eq!(listing_after_flip, listing_before_flip);
    assert_eq!(reserve_after_flip, before);

    let mut newer = public.clone();
    newer["revision"] = json!(2);
    homeserver.put_record(&seller.pubky, "boots_01", newer);
    let (status, refused) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(
        refused["error"]["code"],
        json!("SELLER_REGISTRATION_REQUIRED")
    );

    let mut forbidden = public.clone();
    forbidden["extension"] = json!({"nested": [{"reservePrice": null}]});
    homeserver.put_record(&seller.pubky, "boots_01", forbidden);
    let (status, refused) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM events").await,
        1,
        "auction sync never appends an event"
    );

    homeserver.put_record(&seller.pubky, "boots_01", public);
    let listing_before: Value =
        sqlx::query_scalar("SELECT to_jsonb(l) FROM listings l WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("listing before missing-secret sync");
    sqlx::query("DELETE FROM listing_auction_reserves WHERE listing_aggregate_id = $1")
        .bind(&aggregate_id)
        .execute(&app.pool)
        .await
        .expect("test removes reserve authority");
    let (status, missing_secret) = execute(
        &app,
        &buyer.token,
        &sync_command(&seller.pubky, "boots_01", 4),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{missing_secret}");
    assert_eq!(
        missing_secret["error"]["code"],
        json!("SELLER_REGISTRATION_REQUIRED")
    );
    let listing_after: Value =
        sqlx::query_scalar("SELECT to_jsonb(l) FROM listings l WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("listing after missing-secret sync");
    assert_eq!(listing_after, listing_before);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn two_same_base_reserve_edits_have_one_dual_cas_winner(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let mut create = register_auction_command(&seller.pubky);
    create["payload"]["auction_terms"]["starts_at"] = json!("2026-08-19T22:10:00.000Z");
    create["payload"]["auction_terms"]["ends_at"] = json!("2026-08-19T22:20:00.000Z");
    create["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(8_000);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&create),
    );
    let (status, body) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let mut first = create.clone();
    first["command_id"] = json!(indexed_command_id(0x9002, 1));
    first["expected_revision"] = json!(1);
    first["payload"]["listing_revision"] = json!(2);
    first["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
    first["payload"]["auction_reserve"]["record_revision"] = json!(2);
    first["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(7_000);
    let mut second = first.clone();
    second["command_id"] = json!(indexed_command_id(0x9002, 2));
    second["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(6_500);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&first),
    );

    let first_future = execute(&app, &seller.token, &first);
    let second_future = execute(&app, &seller.token, &second);
    let (first_result, second_result) = tokio::join!(first_future, second_future);
    let results = [first_result, second_result];
    assert_eq!(
        results
            .iter()
            .filter(|(status, _)| *status == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(status, body)| {
                *status == StatusCode::CONFLICT
                    && body["error"]["code"] == json!("REVISION_CONFLICT")
            })
            .count(),
        1
    );
    let revisions: (i64, i64) = sqlx::query_as(
        "SELECT l.server_revision, r.record_revision
         FROM listings l JOIN listing_auction_reserves r
           ON r.listing_aggregate_id = l.aggregate_id
         WHERE l.aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("dual revisions");
    assert_eq!(revisions, (2, 2));
}

#[sqlx::test(migrations = "./migrations")]
async fn forbidden_reserve_edits_and_stale_aggregate_write_nothing(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let mut create = register_auction_command(&seller.pubky);
    create["payload"]["auction_terms"]["starts_at"] = json!("2026-08-19T22:10:00.000Z");
    create["payload"]["auction_terms"]["ends_at"] = json!("2026-08-19T22:20:00.000Z");
    create["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(8_000);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&create),
    );
    let (status, body) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for (index, reserve_price) in [
        (
            1,
            json!({"amount_minor": 9_000, "currency": "USD", "exponent": 2}),
        ),
        (2, Value::Null),
    ] {
        let mut edit = create.clone();
        edit["command_id"] = json!(indexed_command_id(0x9003, index));
        edit["expected_revision"] = json!(1);
        edit["payload"]["listing_revision"] = json!(2);
        edit["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
        edit["payload"]["auction_reserve"]["record_revision"] = json!(2);
        edit["payload"]["auction_reserve"]["reserve_price"] = reserve_price;
        homeserver.put_record(&seller.pubky, "boots_01", public_record_from_command(&edit));
        let (status, refused) = execute(&app, &seller.token, &edit).await;
        assert_eq!(status, StatusCode::CONFLICT, "{refused}");
        assert_eq!(refused["error"]["code"], json!("INVALID_STATE"));
    }

    let mut stale_aggregate = create.clone();
    stale_aggregate["command_id"] = json!(indexed_command_id(0x9003, 3));
    stale_aggregate["expected_revision"] = json!(0);
    stale_aggregate["payload"]["listing_revision"] = json!(2);
    stale_aggregate["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
    stale_aggregate["payload"]["auction_reserve"]["record_revision"] = json!(2);
    stale_aggregate["payload"]["auction_reserve"]["reserve_price"]["amount_minor"] = json!(7_000);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&stale_aggregate),
    );
    let (status, refused) = execute(&app, &seller.token, &stale_aggregate).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("REVISION_CONFLICT"));

    app.clock.advance_seconds(10 * 60);
    let mut post_start = stale_aggregate.clone();
    post_start["command_id"] = json!(indexed_command_id(0x9003, 4));
    post_start["expected_revision"] = json!(1);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&post_start),
    );
    let (status, refused) = execute(&app, &seller.token, &post_start).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("INVALID_STATE"));

    let state: (i64, i64, i64) = sqlx::query_as(
        "SELECT l.server_revision, r.record_revision, r.reserve_amount_minor
         FROM listings l JOIN listing_auction_reserves r
           ON r.listing_aggregate_id = l.aggregate_id
         WHERE l.aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("authority rows");
    assert_eq!(state, (1, 1, 8_000));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM events").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn close_checks_secret_money_asset_and_missing_row_before_settlement(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let other = new_actor(&app).await;
    let create = register_auction_command(&seller.pubky);
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&create),
    );
    let (status, body) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    execute(
        &app,
        &other.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    app.clock.advance_seconds(11 * 60);
    let aggregate_id = listing_aggregate(&seller.pubky);

    sqlx::query(
        "UPDATE listing_auction_reserves SET listing_revision = 2
         WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .execute(&app.pool)
    .await
    .expect("test introduces reserve/listing revision mismatch");
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 79),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    sqlx::query(
        "UPDATE listing_auction_reserves SET listing_revision = 1, reserve_exponent = 3
         WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .execute(&app.pool)
    .await
    .expect("test introduces exponent mismatch");
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 80),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    sqlx::query(
        "UPDATE listing_auction_reserves SET reserve_exponent = 2, reserve_currency = 'EUR'
         WHERE listing_aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .execute(&app.pool)
    .await
    .expect("test introduces currency mismatch");
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 81),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    sqlx::query("DELETE FROM listing_auction_reserves WHERE listing_aggregate_id = $1")
        .bind(&aggregate_id)
        .execute(&app.pool)
        .await
        .expect("test removes secret authority");
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 82),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    let state: (i64, String, i64) = sqlx::query_as(
        "SELECT server_revision, auction->>'status', reserved_quantity
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .fetch_one(&app.pool)
    .await
    .expect("listing remains");
    assert_eq!(state, (3, "active".to_string(), 0));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM reservations").await,
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn reserve_cannot_be_added_after_null_or_changed_after_a_bid(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let mut create = register_auction_command(&seller.pubky);
    create["payload"]["auction_reserve"]["reserve_price"] = Value::Null;
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&create),
    );
    let (status, body) = execute(&app, &seller.token, &create).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, bid) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 7_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bid}");

    let mut add_after_null = create.clone();
    add_after_null["command_id"] = json!(indexed_command_id(0x9004, 1));
    add_after_null["expected_revision"] = json!(2);
    add_after_null["payload"]["listing_revision"] = json!(2);
    add_after_null["payload"]["auction_reserve"]["expected_record_revision"] = json!(1);
    add_after_null["payload"]["auction_reserve"]["record_revision"] = json!(2);
    add_after_null["payload"]["auction_reserve"]["reserve_price"] =
        json!({"amount_minor": 6_000, "currency": "USD", "exponent": 2});
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        public_record_from_command(&add_after_null),
    );
    let (status, refused) = execute(&app, &seller.token, &add_after_null).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("INVALID_STATE"));
    let authority: (i64, i64, Option<i64>) = sqlx::query_as(
        "SELECT l.server_revision, r.record_revision, r.reserve_amount_minor
         FROM listings l JOIN listing_auction_reserves r
           ON r.listing_aggregate_id = l.aggregate_id
         WHERE l.aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("authority remains");
    assert_eq!(authority, (2, 1, None));

    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 2, 90),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{closed}");
    assert_eq!(closed["result"]["outcome"], json!("sold"));
    assert_eq!(
        closed["result"]["listing"]["auction"]["status"],
        json!("sold")
    );
}
