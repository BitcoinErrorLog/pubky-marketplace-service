//! Local pickup, PART A (Wave 7 safe subset): sealed details, pinned
//! reveal, handover flow, unilateral exits, reputation rules, rotation and
//! retention. These tests drive the real application seams — the HTTP
//! command surface, the Locks verification worker, the delivery sweep, and
//! the stat-attestation worker — against a real Postgres database.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use marketplace_service::clock::Clock;
use marketplace_service::locks::{LocksLookupOutcome, LocksRuntime, LocksTaskStatus};
use marketplace_service::pickup::{self, PickupKeys};
use marketplace_service::workers::{self, run_once};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tower::util::ServiceExt;
use uuid::Uuid;

use common::{
    checkout_command_with_id, count, execute, indexed_command_id, lock_resource_for, new_actor,
    order_command, register_command, register_listing_command, register_locks_command, send,
    spawn_fake_homeserver, test_app, test_app_with_pickup, test_app_with_pickup_and_locks,
    test_attestor, test_locks_keys, FakeLocksClient, TestActor, TestApp, TEST_BUNDLE_ID,
};
use marketplace_service::clock::AdjustableClock;
use marketplace_service::http::build_router;

const BUNDLE_2: &str = "222G40R40M30E209185GR38E1W";
const BUNDLE_3: &str = "333G40R40M30E209185GR38E1W";
const BUNDLE_4: &str = "444G40R40M30E209185GR38E1W";
const BUNDLE_5: &str = "555G40R40M30E209185GR38E1W";
const BUNDLE_6: &str = "666G40R40M30E209185GR38E1W";
const SPOT: &str = "Central Station, north entrance";
const SPOT_EDITED: &str = "Harbor Pier 9, west gate";

fn listing_agg(seller: &str, listing_id: &str) -> String {
    format!("listing:{seller}_{listing_id}")
}

/// A `listing.register` envelope publishing BOTH fulfillment methods.
fn register_pickup_listing(seller: &str, listing_id: &str, quantity: i64, n: u64) -> Value {
    let mut command = register_listing_command(seller, listing_id, quantity, n);
    command["payload"]["fulfillment_methods"] = json!(["shipping", "pickup"]);
    command
}

fn set_details(seller: &str, listing_id: &str, expected_version: i64, spot: &str, n: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x9000, n),
        "aggregate_id": listing_agg(seller, listing_id),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "pickup_details.set",
        "payload": {
            "expected_version": expected_version,
            "details": {
                "kind": "spot",
                "spot": spot,
                "instructions": "Ask for the blue backpack.",
                "availability": {
                    "windows": [{ "day": "sat", "start": "10:00", "end": "14:00" }],
                    "zone": "Europe/Berlin",
                },
            },
        },
    })
}

fn clear_details(seller: &str, listing_id: &str, expected_version: i64, n: u64) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x9100, n),
        "aggregate_id": listing_agg(seller, listing_id),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "pickup_details.clear",
        "payload": { "expected_version": expected_version },
    })
}

/// A checkout whose lines each declare a fulfillment method; the address is
/// included only when `with_address` (and then only legal with shipping).
fn checkout_lines(lines: Vec<Value>, with_address: bool, command_id: &str) -> Value {
    let mut payload = json!({
        "lines": lines,
        "guarantee_policy_version": 1,
    });
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

fn line(seller: &str, listing_id: &str, revision: i64, fulfillment: &str) -> Value {
    json!({
        "listing_aggregate_id": listing_agg(seller, listing_id),
        "expected_revision": revision,
        "quantity": 1,
        "fulfillment": fulfillment,
    })
}

struct PickupOrder {
    order_id: String,
    payment_id: String,
}

/// Registers a pickup-enabled listing and checks it out with the pickup
/// choice (no address), leaving the order `pending_payment`.
async fn create_pickup_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listing_id: &str,
    register_n: u64,
    checkout_id: &str,
) -> PickupOrder {
    let (status, body) = execute(
        app,
        &seller.token,
        &register_pickup_listing(&seller.pubky, listing_id, 5, register_n),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout_lines(
            vec![line(&seller.pubky, listing_id, 1, "pickup")],
            false,
            checkout_id,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    PickupOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    }
}

/// Confirms the order's payment through the real Locks verification worker
/// seam: registration, a completed lifecycle outcome, and one worker pass.
/// The pinned confirming adapter is then the worker's rail (`locks`).
async fn confirm_via_locks(
    app: &TestApp,
    fake: &FakeLocksClient,
    buyer: &TestActor,
    order: &PickupOrder,
    seller: &TestActor,
    bundle_id: &str,
    n: u64,
) {
    let (status, body) = execute(
        app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            1,
            bundle_id,
            &lock_resource_for(&seller.pubky),
            n,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "locks registration failed: {body}");
    fake.set_outcome(
        bundle_id,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1, "payment confirmed");
}

async fn order_row(pool: &PgPool, order_id: &str) -> (String, i64) {
    let (state, revision): (String, i64) =
        sqlx::query_as("SELECT state, revision FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(pool)
            .await
            .expect("order row exists");
    (state, revision)
}

async fn reveal(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}/pickup-details"),
        Some(token),
        &Value::Null,
    )
    .await
}

async fn owner_read(app: &TestApp, token: &str, aggregate_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{aggregate_id}/pickup-details"),
        Some(token),
        &Value::Null,
    )
    .await
}

/// The full fixture: pickup listing with details set (v1), a pickup order
/// paid through the Locks worker.
async fn paid_pickup_order(
    app: &TestApp,
    fake: &FakeLocksClient,
    seller: &TestActor,
    buyer: &TestActor,
    listing_id: &str,
    seed: u64,
) -> PickupOrder {
    let order = create_pickup_order(
        app,
        seller,
        buyer,
        listing_id,
        0x100 + seed,
        &format!("00000000-0000-4000-9000-{seed:012}"),
    )
    .await;
    let (status, body) = execute(
        app,
        &seller.token,
        &set_details(&seller.pubky, listing_id, 0, SPOT, 0x200 + seed),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set details failed: {body}");
    confirm_via_locks(
        app,
        fake,
        buyer,
        &order,
        seller,
        TEST_BUNDLE_ID,
        0x300 + seed,
    )
    .await;
    order
}

// The entitlement is the durable payment fact, re-evaluated on every read:
// an unpaid buyer is refused; the paying buyer gets the pinned snapshot per
// line — and LATER EDITS never change what the reveal serves.
#[sqlx::test]
async fn reveal_requires_payment_and_serves_the_pinned_snapshot(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x101,
        "00000000-0000-4000-9000-000000000101",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x201),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Unpaid: no durable payment fact.
    let (status, body) = reveal(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    confirm_via_locks(&app, &fake, &buyer, &order, &seller, TEST_BUNDLE_ID, 0x301).await;

    // Paid: the pinned snapshot per line, with read-only windows and zone.
    let (status, body) = reveal(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"].as_array().expect("lines").len(), 1);
    let line = &body["lines"][0];
    assert_eq!(line["version"], json!(1));
    assert_eq!(line["details"]["kind"], json!("spot"));
    assert_eq!(line["details"]["spot"], json!(SPOT));
    assert_eq!(
        line["details"]["availability"]["zone"],
        json!("Europe/Berlin")
    );
    assert_eq!(
        line["details"]["availability"]["windows"][0]["day"],
        json!("sat")
    );
    assert_eq!(line["withdrawn_by_seller"], json!(false));
    assert!(body["first_revealed_at"].is_string());

    // The pin is immutable: later edits bump the version but the reveal
    // keeps serving what the buyer paid against.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x202),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = reveal(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"][0]["details"]["spot"], json!(SPOT));
    assert_eq!(body["lines"][0]["version"], json!(1));
    assert_eq!(body["lines"][0]["current_version"], json!(2));
    assert_eq!(body["lines"][0]["updated_since_payment"], json!(true));

    // The snapshot row exists with the worker's rail as confirming adapter,
    // sealed: the spot text appears nowhere in the stored ciphertext.
    let (ciphertext, adapter, pinned_version): (Vec<u8>, String, i64) = sqlx::query_as(
        "SELECT snapshot_ciphertext, confirming_adapter, version FROM pickup_line_snapshots \
         WHERE order_id = $1::uuid AND line_index = 0",
    )
    .bind(&order.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("snapshot row exists");
    assert_eq!(adapter, "locks");
    assert_eq!(pinned_version, 1);
    assert!(!ciphertext
        .windows(SPOT.len())
        .any(|window| window == SPOT.as_bytes()));
    // The order line carries version_at_payment.
    let (lines,): (Value,) = sqlx::query_as("SELECT lines FROM orders WHERE id = $1::uuid")
        .bind(&order.order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row");
    assert_eq!(lines[0]["version_at_payment"], json!(1));
}

// Cache-Control: no-store on both entitled reads.
#[sqlx::test]
async fn entitled_reads_are_no_store(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x11).await;

    for uri in [
        format!("/v1/orders/{}/pickup-details", order.order_id),
        format!(
            "/v1/listings/{}/pickup-details",
            listing_agg(&seller.pubky, "boots_01")
        ),
    ] {
        let request = Request::builder()
            .method("GET")
            .uri(&uri)
            .header(
                "authorization",
                format!(
                    "Bearer {}",
                    if uri.contains(&order.order_id) {
                        &buyer.token
                    } else {
                        &seller.token
                    }
                ),
            )
            .body(Body::empty())
            .expect("request builds");
        let response = app.router.clone().oneshot(request).await.expect("runs");
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(
            response.headers().get("cache-control").expect("header"),
            "no-store",
            "{uri} must be no-store"
        );
    }
}

// Wrong-role rejection across the whole surface: the seller never calls the
// buyer reveal, a non-owner never reads or writes the details, and the
// handover commands reject outsiders and wrong roles.
#[sqlx::test]
async fn wrong_roles_are_rejected_everywhere(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let other_seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x12).await;

    // The seller is refused on the buyer-only reveal.
    let (status, body) = reveal(&app, &seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    // A stranger gets the indistinguishable 404.
    let (status, _) = reveal(&app, &other_seller.token, &order.order_id).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The owner read answers the seller their own details; other sellers
    // and the buyer get 404.
    let (status, body) =
        owner_read(&app, &seller.token, &listing_agg(&seller.pubky, "boots_01")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["current"]["details"]["spot"], json!(SPOT));
    assert_eq!(body["current"]["version"], json!(1));
    assert_eq!(body["last_version"], json!(1));
    let (status, _) = owner_read(
        &app,
        &other_seller.token,
        &listing_agg(&seller.pubky, "boots_01"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = owner_read(&app, &buyer.token, &listing_agg(&seller.pubky, "boots_01")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // pickup_details.set/clear are seller-only.
    let (status, body) = execute(
        &app,
        &other_seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x210),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &clear_details(&seller.pubky, "boots_01", 1, 0x211),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // mark_ready is seller-only; a third party may not confirm the handover.
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.mark_ready",
            &order.order_id,
            revision,
            json!({}),
            0x212,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = execute(
        &app,
        &other_seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order.order_id,
            revision,
            json!({}),
            0x213,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

// The reveal entitlement ENDS at the terminal transition: a cancel from
// pending_payment never established it (no receipt); a paid order cancelled
// on ANY path refuses from the cancel event on; any other terminal state
// refuses too.
#[sqlx::test]
async fn reveal_entitlement_ends_at_terminal_states(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // (a) Cancelled from pending_payment: never paid, never revealed.
    let unpaid = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x131,
        "00000000-0000-4000-9000-000000000131",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x231),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &unpaid.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &unpaid.order_id,
            revision,
            json!({ "reason": "changed mind" }),
            0x232,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = reveal(&app, &buyer.token, &unpaid.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // (b) Paid, then the ORDINARY approved cancel: refused from the cancel
    // event on, while the pinned snapshot is retained as dispute evidence.
    let ordinary = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02",
        0x132,
        "00000000-0000-4000-9000-000000000132",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02", 0, SPOT, 0x233),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &ordinary, &seller, BUNDLE_2, 0x234).await;
    // No reveal here: a first reveal would open the bounded withdrawal
    // window and turn the cancel into the unilateral exit. With neither
    // unilateral condition present this is the ordinary two-step cancel.
    let (_, revision) = order_row(&app.pool, &ordinary.order_id).await;
    let (status, _) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &ordinary.order_id,
            revision,
            json!({ "reason": "no longer needed" }),
            0x235,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, revision) = order_row(&app.pool, &ordinary.order_id).await;
    let (status, _) = execute(
        &app,
        &seller.token,
        &order_command(
            "order.cancel_approve",
            &ordinary.order_id,
            revision,
            json!({}),
            0x236,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = reveal(&app, &buyer.token, &ordinary.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    // The snapshot is retained (dispute evidence) even though the read refuses.
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM pickup_line_snapshots WHERE order_id = '{}'::uuid",
                ordinary.order_id
            )
        )
        .await,
        1
    );

    // (c) Paid, then a UNILATERAL terms-change cancel: refused from the
    // cancel event on too.
    let unilateral = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_03",
        0x133,
        "00000000-0000-4000-9000-000000000133",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_03", 0, SPOT, 0x237),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &unilateral, &seller, BUNDLE_3, 0x238).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_03", 1, SPOT_EDITED, 0x239),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &unilateral.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &unilateral.order_id,
            revision,
            json!({ "reason": "the meeting point moved" }),
            0x240,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (state, _) = order_row(&app.pool, &unilateral.order_id).await;
    assert_eq!(state, "cancelled");
    let (status, body) = reveal(&app, &buyer.token, &unilateral.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // (d) Any other terminal state (here: delivered): the read refuses.
    let delivered = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_04", 0x13).await;
    let (_, revision) = order_row(&app.pool, &delivered.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &delivered.order_id,
            revision,
            json!({}),
            0x241,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.clock.set(app.clock.now() + chrono::Duration::days(15));
    let completed = workers::complete_due_delivered_orders(&app.pool, app.clock.now(), 14, 100, 2)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 1);
    let (status, body) = reveal(&app, &buyer.token, &delivered.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

// The standard projection (cached, shape-logged, list-rendered) NEVER
// carries details; command results, notifications, and Debug impls are
// redacted; the meeting point appears only in the two entitled reads.
#[sqlx::test]
async fn no_serialization_surface_leaks_the_meeting_point(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x14).await;

    // Order projections (single + list) carry no details.
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order.order_id),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let serialized = serde_json::to_string(&body).expect("serialize");
    assert!(
        !serialized.contains(SPOT),
        "order projection leaked: {serialized}"
    );
    assert!(!serialized.contains("blue backpack"));
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/orders",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let serialized = serde_json::to_string(&body).expect("serialize");
    assert!(
        !serialized.contains(SPOT),
        "orders list leaked: {serialized}"
    );

    // Notifications delivered through the outbox carry no address material.
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/notifications",
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let serialized = serde_json::to_string(&body).expect("serialize");
    assert!(
        !serialized.contains(SPOT),
        "notifications leaked: {serialized}"
    );

    // Command results carry no details either (set returns metadata only).
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x242),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let serialized = serde_json::to_string(&body).expect("serialize");
    assert!(
        !serialized.contains(SPOT_EDITED),
        "command result leaked: {serialized}"
    );

    // The sealed rows never contain plaintext, in either family.
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
        "SELECT details_ciphertext FROM listing_pickup_details \
         UNION ALL SELECT snapshot_ciphertext FROM pickup_line_snapshots",
    )
    .fetch_all(&app.pool)
    .await
    .expect("sealed rows");
    assert!(!rows.is_empty());
    for (ciphertext,) in rows {
        for needle in [SPOT, SPOT_EDITED, "blue backpack"] {
            assert!(
                !ciphertext
                    .windows(needle.len())
                    .any(|window| window == needle.as_bytes()),
                "ciphertext contains plaintext"
            );
        }
    }

    // The domain types' Debug impls are redacted.
    let command = marketplace_domain::commands::parse_command(&set_details(
        &seller.pubky,
        "boots_01",
        2,
        SPOT,
        0x243,
    ))
    .expect("valid command");
    let debug = format!("{:?}", command.payload);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(SPOT));
    assert!(!debug.contains("blue backpack"));
    assert!(!debug.contains("Europe/Berlin"));
}

// Mixed carts split one order per (seller, fulfillment): several pickup
// lines from one seller share one pickup order; pickup orders charge no
// shipping and store no address; shipped orders keep both.
#[sqlx::test]
async fn mixed_cart_splits_per_seller_fulfillment(pool: PgPool) {
    let app = test_app(pool).await;
    let seller_a = new_actor(&app).await;
    let seller_b = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    for (seller, listing_id, n) in [
        (&seller_a, "boots_01", 0x151),
        (&seller_a, "boots_02", 0x152),
        (&seller_a, "boots_03", 0x153),
    ] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &register_pickup_listing(&seller.pubky, listing_id, 5, n),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, body) =
        execute(&app, &seller_b.token, &register_command(&seller_b.pubky, 5)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines(
            vec![
                line(&seller_a.pubky, "boots_01", 1, "pickup"),
                line(&seller_a.pubky, "boots_02", 1, "pickup"),
                line(&seller_a.pubky, "boots_03", 1, "shipping"),
                line(&seller_b.pubky, "boots_01", 1, "shipping"),
            ],
            true,
            "00000000-0000-4000-9000-000000000154",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let orders = body["result"]["orders"].as_array().expect("orders");
    // THREE orders: the two pickup lines share one; each shipped group is
    // its own order.
    assert_eq!(orders.len(), 3, "{body}");
    let pickup_order = orders
        .iter()
        .find(|order| order["fulfillment"] == json!("pickup"))
        .expect("one pickup order");
    assert_eq!(pickup_order["lines"].as_array().expect("lines").len(), 2);
    assert_eq!(pickup_order["shipping"]["amount_minor"], json!(0));
    assert_eq!(pickup_order["delivery_address"], Value::Null);
    let shipped: Vec<&Value> = orders
        .iter()
        .filter(|order| order["fulfillment"] == json!("shipping"))
        .collect();
    assert_eq!(shipped.len(), 2);
    for order in &shipped {
        assert_eq!(order["shipping"]["amount_minor"], json!(1200));
    }
    // The pickup order stores NO address in the database.
    let (stored,): (Option<Value>,) =
        sqlx::query_as("SELECT delivery_address FROM orders WHERE id = $1::uuid")
            .bind(pickup_order["id"].as_str().expect("id"))
            .fetch_one(&app.pool)
            .await
            .expect("row");
    assert!(stored.is_none());

    // A pickup choice a listing does not publish is refused with a typed
    // error — never rewritten to shipping.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines(
            vec![line(&seller_b.pubky, "boots_01", 1, "pickup")],
            false,
            "00000000-0000-4000-9000-000000000155",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // Offers on a non-shipping listing are refused with a typed error.
    let mut pickup_only = register_pickup_listing(&seller_a.pubky, "boots_04", 1, 0x156);
    pickup_only["payload"]["fulfillment_methods"] = json!(["pickup"]);
    let (status, body) = execute(&app, &seller_a.token, &pickup_only).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut offer = common::create_offer_command(&seller_a.pubky, 1);
    offer["aggregate_id"] = json!(listing_agg(&seller_a.pubky, "boots_04"));
    let (status, body) = execute(&app, &buyer.token, &offer).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// The address rule pair: a pickup-only checkout PRESENTING a
// delivery_address is rejected INVALID_COMMAND; a shipped checkout MISSING
// one is rejected too.
#[sqlx::test]
async fn pickup_only_checkout_address_rules(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_pickup_listing(&seller.pubky, "boots_01", 5, 0x161),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines(
            vec![line(&seller.pubky, "boots_01", 1, "pickup")],
            true,
            "00000000-0000-4000-9000-000000000162",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    let without_address = checkout_lines(
        vec![line(&seller.pubky, "boots_01", 1, "pickup")],
        false,
        "00000000-0000-4000-9000-000000000163",
    );
    let (status, body) = execute(&app, &buyer.token, &without_address).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines(
            vec![line(&seller.pubky, "boots_01", 1, "shipping")],
            false,
            "00000000-0000-4000-9000-000000000164",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
}

// Sandbox-pinned snapshots are refused by the reveal EVEN AFTER the
// deployment sandbox flag flips back off — the pinned adapter, not the
// current flag, decides (the flag-toggle window off->on->off).
#[sqlx::test]
async fn sandbox_pinned_snapshot_stays_refused_across_flag_flips(pool: PgPool) {
    // Details are set while the flag is OFF.
    let app_off = test_app_with_pickup(pool.clone(), false).await;
    let seller = new_actor(&app_off).await;
    let buyer = new_actor(&app_off).await;
    let order = create_pickup_order(
        &app_off,
        &seller,
        &buyer,
        "boots_01",
        0x171,
        "00000000-0000-4000-9000-000000000171",
    )
    .await;
    let (status, body) = execute(
        &app_off,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x271),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The flag flips ON: sandbox_advance confirms the payment (pinning the
    // sandbox adapter), and the reveal is refused outright on the flag.
    let app_on = test_app_with_pickup(pool.clone(), true).await;
    let (status, body) = execute(
        &app_on,
        &buyer.token,
        &common::payment_command(&order.payment_id, 1, "confirmed", 1, 0x272),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = reveal(&app_on, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (adapter,): (String,) = sqlx::query_as(
        "SELECT confirming_adapter FROM pickup_line_snapshots WHERE order_id = $1::uuid",
    )
    .bind(&order.order_id)
    .fetch_one(&pool)
    .await
    .expect("snapshot row");
    assert_eq!(adapter, "sandbox");

    // The flag flips back OFF: the pinned adapter still refuses the reveal.
    let app_off_again = test_app_with_pickup(pool.clone(), false).await;
    let (status, body) = reveal(&app_off_again, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// Sandbox payments enabled: pickup_details.set is refused, the buyer reveal
// is refused (the deployment boundary, independent of the pinned adapter),
// and pickup_available reports off.
#[sqlx::test]
async fn sandbox_deployments_refuse_pickup_writes_and_reveals(pool: PgPool) {
    let app = test_app_with_pickup(pool.clone(), true).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_pickup_listing(&seller.pubky, "boots_01", 5, 0x181),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x281),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // The reveal is refused on the flag even for a REAL-rail payment: the
    // same pool gets a locks-confirmed order from a durable deployment view.
    let fake = Arc::new(FakeLocksClient::default());
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let locks = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: fake.clone(),
    });
    let mut config = common::config_durable();
    config.sandbox_payments_enabled = true;
    let state = AppState::new(pool.clone(), clock.clone(), config)
        .with_locks(Some(locks))
        .with_pickup(Some(common::test_pickup_keys()));
    let app_locks = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };
    // Re-authenticate the same keypairs against the new router.
    let seller2 = TestActor {
        token: common::authenticate(&app_locks, &seller.keypair).await,
        keypair: seller.keypair,
        pubky: seller.pubky.clone(),
    };
    let buyer = new_actor(&app_locks).await;
    let order = create_pickup_order(
        &app_locks,
        &seller2,
        &buyer,
        "boots_02",
        0x182,
        "00000000-0000-4000-9000-000000000182",
    )
    .await;
    let (status, body) = execute(
        &app_locks,
        &seller2.token,
        &set_details(&seller2.pubky, "boots_02", 0, SPOT, 0x282),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "set is refused while the flag is on: {body}"
    );
    confirm_via_locks(&app_locks, &fake, &buyer, &order, &seller2, BUNDLE_2, 0x283).await;
    let (status, body) = reveal(&app_locks, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // The capability flag: on only when the key is configured AND sandbox
    // payments are disabled.
    let (status, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pickup_available"], json!(false));
    let durable = test_app_with_pickup(pool.clone(), false).await;
    let (_, body) = send(durable.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(body["pickup_available"], json!(true));
    let no_keys = test_app(pool.clone()).await;
    let (_, body) = send(no_keys.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(body["pickup_available"], json!(false));
}

// Versions are monotonic per listing across clear: the counter row survives
// `pickup_details.clear`; the post-clear set CASes against it and continues
// the sequence, and terms-change detection still fires against a pre-clear
// version_at_payment. A stale expected_version conflicts.
#[sqlx::test]
async fn versions_are_monotonic_across_clear(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x19).await;

    // v2 (edit), then clear (CAS v2), then the post-clear set continues at
    // v3 — never a restart at v1.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x291),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["version"], json!(2));
    // A stale expected_version conflicts with the current version.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT, 0x292),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));

    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_01", 2, 0x293),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The owner read after a clear returns no details ALONGSIDE the
    // surviving counter.
    let (status, body) =
        owner_read(&app, &seller.token, &listing_agg(&seller.pubky, "boots_01")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["current"], Value::Null);
    assert_eq!(body["last_version"], json!(2));

    // The reveal keeps serving the pinned snapshot, flagged
    // withdrawn-by-seller, in place of the (absent) current details.
    let (status, body) = reveal(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"][0]["details"]["spot"], json!(SPOT));
    assert_eq!(body["lines"][0]["withdrawn_by_seller"], json!(true));

    // The next set CASes against the surviving counter: v3.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 2, SPOT, 0x294),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["version"], json!(3));
    // Terms-change detection still fires against the pre-clear
    // version_at_payment (1): the seller cannot self-confirm the handover.
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order.order_id,
            revision,
            json!({}),
            0x295,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    // But the buyer may accept the terms by confirming.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order.order_id,
            revision,
            json!({}),
            0x296,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
}

// clear hard-deletes unreferenced versions and retains the pinned ones;
// retained versions and snapshots purge once referencing orders go
// terminal — with the cancelled-order exception living until refund
// evidence is recorded, and the ordinary terminal purge taking it when no
// evidence lands.
#[sqlx::test]
async fn clear_retention_and_terminal_purge(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x1a).await;
    // A second, unreferenced version (v2) exists alongside the pinned v1.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x2a1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_01", 2, 0x2a2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let versions: Vec<(i64,)> = sqlx::query_as(
        "SELECT version FROM listing_pickup_details WHERE aggregate_id = $1 ORDER BY version",
    )
    .bind(listing_agg(&seller.pubky, "boots_01"))
    .fetch_all(&app.pool)
    .await
    .expect("versions");
    assert_eq!(
        versions.iter().map(|(v,)| *v).collect::<Vec<_>>(),
        vec![1],
        "the version pinned by the paid, non-terminal order is retained; \
         the unreferenced one is hard-deleted"
    );

    // The buyer cancels unilaterally (the clear is a terms change) and the
    // reveal ends — but the snapshot and retained version survive as the
    // dispute exhibit until refund evidence is recorded.
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order.order_id,
            revision,
            json!({ "reason": "details withdrawn" }),
            0x2a3,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (state, _) = order_row(&app.pool, &order.order_id).await;
    assert_eq!(state, "cancelled");
    pickup::purge_terminal_pickup_retention(&app.pool, app.clock.now(), 30)
        .await
        .expect("purge runs");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM pickup_line_snapshots WHERE order_id = '{}'::uuid",
                order.order_id
            )
        )
        .await,
        1,
        "a cancelled-after-payment snapshot outlives the cancel until refund evidence"
    );

    // The seller records the external refund evidence; the next purge takes
    // the snapshot AND the retained version.
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            &order.order_id,
            revision,
            json!({ "amount_minor": 12500, "transaction_id": "bitcoin-tx-evidence-123" }),
            0x2a4,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (snapshots_purged, versions_purged) =
        pickup::purge_terminal_pickup_retention(&app.pool, app.clock.now(), 30)
            .await
            .expect("purge runs");
    assert_eq!((snapshots_purged, versions_purged), (1, 1));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM listing_pickup_details").await,
        0
    );

    // When no evidence ever lands, the ordinary terminal purge takes the
    // snapshot once the dispute-retention window has elapsed.
    let order2 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02",
        0x1b1,
        "00000000-0000-4000-9000-0000000001b1",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02", 0, SPOT, 0x2a5),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order2, &seller, BUNDLE_2, 0x2a6).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_02", 1, 0x2a7),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order2.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order2.order_id,
            revision,
            json!({ "reason": "withdrawn spot" }),
            0x2a8,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // 29 days on: still retained. 31 days on: the ordinary purge takes it.
    let later = app.clock.now() + chrono::Duration::days(29);
    pickup::purge_terminal_pickup_retention(&app.pool, later, 30)
        .await
        .expect("purge runs");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM pickup_line_snapshots WHERE order_id = '{}'::uuid",
                order2.order_id
            )
        )
        .await,
        1
    );
    let later = app.clock.now() + chrono::Duration::days(31);
    pickup::purge_terminal_pickup_retention(&app.pool, later, 30)
        .await
        .expect("purge runs");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM pickup_line_snapshots WHERE order_id = '{}'::uuid",
                order2.order_id
            )
        )
        .await,
        0
    );
}

// Editing details after payment bumps the version and notifies exactly the
// paid buyers of orders on that listing.
#[sqlx::test]
async fn edit_after_payment_notifies_exactly_the_paid_buyers(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer1 = new_actor(&app).await;
    let buyer2 = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer1, "boots_01", 0x1c).await;
    // buyer2 has an UNPAID order on the same listing.
    let (listing_revision,): (i64,) =
        sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(listing_agg(&seller.pubky, "boots_01"))
            .fetch_one(&app.pool)
            .await
            .expect("listing");
    let (status, body) = execute(
        &app,
        &buyer2.token,
        &checkout_lines(
            vec![line(&seller.pubky, "boots_01", listing_revision, "pickup")],
            false,
            "00000000-0000-4000-9000-0000000001c2",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let _ = order;

    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x2b1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let notified: Vec<(String,)> = sqlx::query_as(
        "SELECT recipient_pubky FROM notifications WHERE type = 'pickup_details_updated'",
    )
    .fetch_all(&app.pool)
    .await
    .expect("notifications");
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, buyer1.pubky);

    // Clearing notifies the same paid buyer with the cleared type.
    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_01", 2, 0x2b2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let notified: Vec<(String,)> = sqlx::query_as(
        "SELECT recipient_pubky FROM notifications WHERE type = 'pickup_details_cleared'",
    )
    .fetch_all(&app.pool)
    .await
    .expect("notifications");
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, buyer1.pubky);
}

// The unilateral terms-change exit and the bounded post-reveal withdrawal:
// both move the order straight to cancelled with the distinct event kind,
// release inventory through approve's path, and notify the seller; outside
// the conditions the same command is the ordinary cancel_requested.
#[sqlx::test]
async fn unilateral_exits_and_their_window_edges(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // (1) Terms change (version bump after payment): cancel_request from
    // `paid` cancels immediately, emitting order.cancelled_terms_change,
    // releasing sold inventory back to available, and notifying the seller.
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x1d).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x2c1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order.order_id,
            revision,
            json!({ "reason": "spot moved after I paid" }),
            0x2c2,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' \
                 AND kind = 'order.cancelled_terms_change'",
                order.order_id
            )
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' AND kind = 'order.cancelled'",
                order.order_id
            )
        )
        .await,
        0,
        "the distinct kind replaces order.cancelled"
    );
    let (available, state): (i64, String) =
        sqlx::query_as("SELECT available_quantity, state FROM listings WHERE aggregate_id = $1")
            .bind(listing_agg(&seller.pubky, "boots_01"))
            .fetch_one(&app.pool)
            .await
            .expect("listing");
    assert_eq!((available, state.as_str()), (5, "available"));
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM notifications WHERE type = 'order_cancelled' \
                 AND recipient_pubky = '{}'",
                seller.pubky
            )
        )
        .await,
        1
    );

    // (2) Bounded withdrawal: first reveal, then mark_ready, then cancel
    // from `ready_for_pickup` — mark_ready does NOT close the window.
    let order2 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02",
        0x1d2,
        "00000000-0000-4000-9000-0000000001d2",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02", 0, SPOT, 0x2c3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order2, &seller, BUNDLE_2, 0x2c4).await;
    let (status, body) = reveal(&app, &buyer.token, &order2.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order2.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.mark_ready",
            &order2.order_id,
            revision,
            json!({}),
            0x2c5,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("ready_for_pickup"));
    let (_, revision) = order_row(&app.pool, &order2.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order2.order_id,
            revision,
            json!({ "reason": "the spot is unusable as revealed" }),
            0x2c6,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));
    // The bounded-withdrawal cancel also ends the reveal entitlement from
    // the cancel event on.
    let (status, body) = reveal(&app, &buyer.token, &order2.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // (2b) The terms-change exit fires from `ready_for_pickup` too: mark
    // ready first, THEN edit the details, then cancel unilaterally.
    let order2b = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02b",
        0x1db,
        "00000000-0000-4000-9000-0000000001db",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02b", 0, SPOT, 0x2c6 + 0x10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(
        &app,
        &fake,
        &buyer,
        &order2b,
        &seller,
        BUNDLE_6,
        0x2c6 + 0x11,
    )
    .await;
    let (_, revision) = order_row(&app.pool, &order2b.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.mark_ready",
            &order2b.order_id,
            revision,
            json!({}),
            0x2c6 + 0x12,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02b", 1, SPOT_EDITED, 0x2c6 + 0x13),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order2b.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order2b.order_id,
            revision,
            json!({ "reason": "spot moved while I was on my way" }),
            0x2c6 + 0x14,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));

    // (3) The window closes on the handover confirm: afterwards the same
    // command is InvalidState.
    let order3 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_03",
        0x1d3,
        "00000000-0000-4000-9000-0000000001d3",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_03", 0, SPOT, 0x2c7),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order3, &seller, BUNDLE_3, 0x2c8).await;
    let (status, _) = reveal(&app, &buyer.token, &order3.order_id).await;
    assert_eq!(status, StatusCode::OK);
    let (_, revision) = order_row(&app.pool, &order3.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order3.order_id,
            revision,
            json!({}),
            0x2c9,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order3.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order3.order_id,
            revision,
            json!({ "reason": "too late" }),
            0x2ca,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // (4) The lost race: cancel_request racing mark_ready BEFORE any first
    // reveal resolves to the ordinary cancel_requested (no unilateral exit
    // without a stamped first_revealed_at), from `paid` and from
    // `ready_for_pickup` alike.
    let order4 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_04",
        0x1d4,
        "00000000-0000-4000-9000-0000000001d4",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_04", 0, SPOT, 0x2cb),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order4, &seller, BUNDLE_4, 0x2cc).await;
    let (_, revision) = order_row(&app.pool, &order4.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order4.order_id,
            revision,
            json!({ "reason": "before any reveal" }),
            0x2cd,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancel_requested"));

    // (5) Command replay is idempotent: the same cancel_request envelope
    // returns the stored result without a second event.
    let order5 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_05",
        0x1d5,
        "00000000-0000-4000-9000-0000000001d5",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_05", 0, SPOT, 0x2ce),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order5, &seller, BUNDLE_5, 0x2cf).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_05", 1, SPOT_EDITED, 0x2d0),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &order5.order_id).await;
    let cancel = order_command(
        "order.cancel_request",
        &order5.order_id,
        revision,
        json!({ "reason": "terms moved" }),
        0x2d1,
    );
    let (status, first) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let (status, replay) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first, replay);
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' \
                 AND kind = 'order.cancelled_terms_change'",
                order5.order_id
            )
        )
        .await,
        1
    );
}

// The handover matrix: buyer and seller confirms from paid and
// ready_for_pickup; the handover record carries the actor and a server
// instant; a duplicate confirm cannot write a second row; the
// fulfillment-method guards reject cross-method commands; next_actor maps
// the pickup states.
#[sqlx::test]
async fn confirm_pickup_matrix_and_method_guards(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // Buyer confirms from `paid`; the handover row records buyer + instant.
    let order1 = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x1e).await;
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order1.order_id),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["next_actor"], json!("seller"));
    let (_, revision) = order_row(&app.pool, &order1.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order1.order_id,
            revision,
            json!({}),
            0x2e1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
    let (confirmed_by, confirmed_at): (String, DateTime<Utc>) = sqlx::query_as(
        "SELECT confirmed_by, confirmed_at FROM pickup_handovers WHERE order_id = $1::uuid",
    )
    .bind(&order1.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("handover row");
    assert_eq!(confirmed_by, "buyer");
    assert_eq!(confirmed_at, app.clock.now());
    // A duplicate confirm is InvalidState and writes no second row.
    let (_, revision) = order_row(&app.pool, &order1.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order1.order_id,
            revision,
            json!({}),
            0x2e2,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM pickup_handovers WHERE order_id = '{}'::uuid",
                order1.order_id
            )
        )
        .await,
        1
    );
    // fulfillment.delivered emitted once, the same kind a shipped order's
    // confirmation emits.
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' \
                 AND kind = 'fulfillment.delivered'",
                order1.order_id
            )
        )
        .await,
        1
    );

    // Seller confirms from `ready_for_pickup`; next_actor maps the state.
    let order2 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02",
        0x1e2,
        "00000000-0000-4000-9000-0000000001e2",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_02", 0, SPOT, 0x2e3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(&app, &fake, &buyer, &order2, &seller, BUNDLE_2, 0x2e4).await;
    let (_, revision) = order_row(&app.pool, &order2.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.mark_ready",
            &order2.order_id,
            revision,
            json!({}),
            0x2e5,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["next_actor"], json!("buyer"));
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM notifications WHERE type = 'pickup_ready' \
                 AND recipient_pubky = '{}'",
                buyer.pubky
            )
        )
        .await,
        1
    );
    let (_, revision) = order_row(&app.pool, &order2.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order2.order_id,
            revision,
            json!({}),
            0x2e6,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (confirmed_by,): (String,) =
        sqlx::query_as("SELECT confirmed_by FROM pickup_handovers WHERE order_id = $1::uuid")
            .bind(&order2.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("handover row");
    assert_eq!(
        confirmed_by, "seller",
        "a seller-only confirm is seller-attested"
    );

    // The method guards: pickup orders refuse ship/confirm_delivery;
    // shipped orders refuse mark_ready/confirm_pickup.
    let shipped = {
        let (status, body) = execute(
            &app,
            &seller.token,
            &register_listing_command(&seller.pubky, "boots_ship", 1, 0x1e7),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout_lines(
                vec![line(&seller.pubky, "boots_ship", 1, "shipping")],
                true,
                "00000000-0000-4000-9000-0000000001e7",
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string()
    };
    let (_, revision) = order_row(&app.pool, &shipped).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.mark_ready",
            &shipped,
            revision,
            json!({}),
            0x2e7,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &shipped,
            revision,
            json!({}),
            0x2e8,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let order3 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_03",
        0x1e3,
        "00000000-0000-4000-9000-0000000001e3",
    )
    .await;
    confirm_via_locks(&app, &fake, &buyer, &order3, &seller, BUNDLE_3, 0x2e9).await;
    let (_, revision) = order_row(&app.pool, &order3.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order3.order_id,
            revision,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
            0x2ea,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order3.order_id,
            revision,
            json!({}),
            0x2eb,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// The auto-complete sweep coalesces the handover instant for pickup orders
// on the same deadline as shipped orders' shipment delivered_at.
#[sqlx::test]
async fn auto_complete_coalesces_the_handover_instant(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_pickup_order(&app, &fake, &seller, &buyer, "boots_01", 0x1f).await;
    let (_, revision) = order_row(&app.pool, &order.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &order.order_id,
            revision,
            json!({}),
            0x2f1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // 13 days on: not yet due. 14 days on: completed from the handover
    // instant, with the system-attributed order.completed event.
    let at_13 = app.clock.now() + chrono::Duration::days(13);
    let completed = workers::complete_due_delivered_orders(&app.pool, at_13, 14, 100, 2)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 0);
    let at_14 = app.clock.now() + chrono::Duration::days(14);
    let completed = workers::complete_due_delivered_orders(&app.pool, at_14, 14, 100, 2)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 1);
    let (state, _) = order_row(&app.pool, &order.order_id).await;
    assert_eq!(state, "completed");
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' \
                 AND kind = 'order.completed' AND actor_pubky = 'system'",
                order.order_id
            )
        )
        .await,
        1
    );
}

// Reputation: a terms-change cancellation excludes the WHOLE order from
// terminated_badly (including a refund.recorded_external leg on it), while
// an ordinary approved cancel still counts; pickup completions count under
// the confirming-actor rule.
#[sqlx::test]
async fn reputation_rules_for_pickup(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let fake = Arc::new(FakeLocksClient::default());
    let locks = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: fake.clone(),
    });
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), common::config_durable())
        .with_locks(Some(locks))
        .with_pickup(Some(common::test_pickup_keys()))
        .with_attestor(Some(test_attestor()));
    let app = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };

    // --- Seller A: one shipped delivered order + one pickup order cancelled
    // via the terms-change exit WITH a recorded external refund. The whole
    // cancelled order is excluded: completionRatePermille stays 1000.
    let seller_a = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // A shipped order delivered + completed (the completion baseline).
    let (status, body) =
        execute(&app, &seller_a.token, &register_command(&seller_a.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller_a.pubky, "00000000-0000-4000-9000-000000000201"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let shipped_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let shipped_payment = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    // Confirm through the sandbox rail? No — sandbox is off; use locks.
    let registration = register_locks_command(
        &shipped_payment,
        1,
        TEST_BUNDLE_ID,
        &lock_resource_for(&seller_a.pubky),
        0x301,
    );
    let (status, body) = execute(&app, &buyer.token, &registration).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let (_, revision) = order_row(&app.pool, &shipped_id).await;
    let (status, body) = execute(
        &app,
        &seller_a.token,
        &order_command(
            "fulfillment.ship",
            &shipped_id,
            revision,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
            0x302,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &shipped_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &shipped_id,
            revision,
            json!({}),
            0x303,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The pickup order cancelled via terms-change + refund evidence.
    let pickup_order = create_pickup_order(
        &app,
        &seller_a,
        &buyer,
        "boots_p1",
        0x204,
        "00000000-0000-4000-9000-000000000204",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller_a.token,
        &set_details(&seller_a.pubky, "boots_p1", 0, SPOT, 0x304),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    confirm_via_locks(
        &app,
        &fake,
        &buyer,
        &pickup_order,
        &seller_a,
        BUNDLE_2,
        0x305,
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller_a.token,
        &set_details(&seller_a.pubky, "boots_p1", 1, SPOT_EDITED, 0x306),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &pickup_order.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &pickup_order.order_id,
            revision,
            json!({ "reason": "spot moved" }),
            0x307,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancelled"));
    let (_, revision) = order_row(&app.pool, &pickup_order.order_id).await;
    let (status, body) = execute(
        &app,
        &seller_a.token,
        &order_command(
            "refund.record_external",
            &pickup_order.order_id,
            revision,
            json!({ "amount_minor": 12500, "transaction_id": "bitcoin-tx-evidence-123" }),
            0x308,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // --- Seller B: identical shape, but an ORDINARY approved cancel with
    // the same refund leg — which still counts as terminated_badly.
    let seller_b = new_actor(&app).await;
    let (status, body) =
        execute(&app, &seller_b.token, &register_command(&seller_b.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller_b.pubky, "00000000-0000-4000-9000-000000000205"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let b_shipped = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let b_payment = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &b_payment,
            1,
            BUNDLE_3,
            &lock_resource_for(&seller_b.pubky),
            0x309,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fake.set_outcome(
        BUNDLE_3,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let (_, revision) = order_row(&app.pool, &b_shipped).await;
    let (status, body) = execute(
        &app,
        &seller_b.token,
        &order_command(
            "fulfillment.ship",
            &b_shipped,
            revision,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-2" }),
            0x30a,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &b_shipped).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &b_shipped,
            revision,
            json!({}),
            0x30b,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A second shipped order for seller B, cancelled the ordinary way with
    // the refund leg.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller_b.pubky, "00000000-0000-4000-9000-000000000206"),
    )
    .await;
    // Sold out — quantity was 1. Register more stock instead.
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = execute(
        &app,
        &seller_b.token,
        &register_listing_command(&seller_b.pubky, "boots_02", 1, 0x207),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_lines(
            vec![line(&seller_b.pubky, "boots_02", 1, "shipping")],
            true,
            "00000000-0000-4000-9000-000000000208",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let b_cancelled = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let b_payment2 = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &b_payment2,
            1,
            BUNDLE_4,
            &lock_resource_for(&seller_b.pubky),
            0x30c,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fake.set_outcome(
        BUNDLE_4,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let (_, revision) = order_row(&app.pool, &b_cancelled).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &b_cancelled,
            revision,
            json!({ "reason": "changed mind" }),
            0x30d,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &b_cancelled).await;
    let (status, body) = execute(
        &app,
        &seller_b.token,
        &order_command(
            "order.cancel_approve",
            &b_cancelled,
            revision,
            json!({}),
            0x30e,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &b_cancelled).await;
    let (status, body) = execute(
        &app,
        &seller_b.token,
        &order_command(
            "refund.record_external",
            &b_cancelled,
            revision,
            json!({ "amount_minor": 13700, "transaction_id": "bitcoin-tx-evidence-456" }),
            0x30f,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // run_once signs attestations as a side effect (the stat task rides the
    // same worker pass); clear them so the manual run re-computes from the
    // full event history.
    sqlx::query("DELETE FROM seller_stat_attestations")
        .execute(&app.pool)
        .await
        .expect("clear attestations");
    let signed =
        workers::generate_due_stat_attestations(&app.pool, &test_attestor(), app.clock.now())
            .await
            .expect("stat job runs");
    assert_eq!(signed, 2);
    let rows: Vec<(String, Value)> =
        sqlx::query_as("SELECT seller_pubky, body FROM seller_stat_attestations")
            .fetch_all(&app.pool)
            .await
            .expect("attestations");
    let rate_for = |seller: &str| {
        rows.iter()
            .find(|(pubky, _)| pubky == seller)
            .map(|(_, body)| body["completionRatePermille"].as_i64().expect("rate"))
            .expect("attestation exists")
    };
    assert_eq!(
        rate_for(&seller_a.pubky),
        1000,
        "the terms-change cancel is excluded whole, refund leg included"
    );
    assert_eq!(
        rate_for(&seller_b.pubky),
        500,
        "the ordinary approved cancel with a refund leg still counts"
    );
}

// The confirming-actor rule: a buyer-confirmed handover counts at once; a
// seller-unilateral confirm counts only after a dispute-free auto-complete
// and never before it.
#[sqlx::test]
async fn reputation_counts_pickup_completions_by_confirming_actor(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let fake = Arc::new(FakeLocksClient::default());
    let locks = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: fake.clone(),
    });
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), common::config_durable())
        .with_locks(Some(locks))
        .with_pickup(Some(common::test_pickup_keys()))
        .with_attestor(Some(test_attestor()));
    let app = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // o1: buyer-confirmed handover (counts at once).
    let o1 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x211,
        "00000000-0000-4000-9000-000000000211",
    )
    .await;
    // o2: seller-confirmed handover, later auto-completed dispute-free
    // (counts only then).
    let o2 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_02",
        0x212,
        "00000000-0000-4000-9000-000000000212",
    )
    .await;
    // o3: seller-confirmed handover, still delivered (never counts on its
    // own).
    let o3 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_03",
        0x213,
        "00000000-0000-4000-9000-000000000213",
    )
    .await;
    // o4: cancelled from pending_payment (terminated_badly = 1, so the rate
    // distinguishes 2-of-3 from 3-of-4 completions).
    let o4 = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_04",
        0x214,
        "00000000-0000-4000-9000-000000000214",
    )
    .await;

    for (listing_id, n) in [
        ("boots_01", 0x311),
        ("boots_02", 0x312),
        ("boots_03", 0x313),
    ] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &set_details(&seller.pubky, listing_id, 0, SPOT, n),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    for (order, bundle, n) in [
        (&o1, TEST_BUNDLE_ID, 0x321),
        (&o2, BUNDLE_2, 0x322),
        (&o3, BUNDLE_3, 0x323),
    ] {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &register_locks_command(
                &order.payment_id,
                1,
                bundle,
                &lock_resource_for(&seller.pubky),
                n,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        fake.set_outcome(
            bundle,
            LocksLookupOutcome::Status(LocksTaskStatus::Completed),
        );
        run_once(&app.state, Uuid::new_v4(), app.clock.now())
            .await
            .expect("worker pass");
    }
    // o1 buyer-confirmed; o2 seller-confirmed. o3's seller-only confirm
    // happens AFTER the auto-complete sweep below, so it is delivered but
    // never auto-completed when the second attestation is computed.
    let (_, revision) = order_row(&app.pool, &o1.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &o1.order_id,
            revision,
            json!({}),
            0x331,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, revision) = order_row(&app.pool, &o2.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &o2.order_id,
            revision,
            json!({}),
            0x332,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // o4 cancelled from pending_payment (terminated_badly = 1).
    let (_, revision) = order_row(&app.pool, &o4.order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &o4.order_id,
            revision,
            json!({ "reason": "never mind" }),
            0x334,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Before o2 auto-completes: only o1 counts -> 1 of (1+1) = 500. Clear
    // any attestation the run_once side effects signed mid-flight.
    sqlx::query("DELETE FROM seller_stat_attestations")
        .execute(&app.pool)
        .await
        .expect("clear attestations");
    let signed =
        workers::generate_due_stat_attestations(&app.pool, &test_attestor(), app.clock.now())
            .await
            .expect("stat job runs");
    assert_eq!(signed, 1);
    let (body,): (Value,) =
        sqlx::query_as("SELECT body FROM seller_stat_attestations WHERE seller_pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("attestation");
    assert_eq!(
        body["completionRatePermille"],
        json!(500),
        "only the buyer-confirmed handover counts at once"
    );

    // Advance past the auto-complete deadline: o1 and o2 complete
    // dispute-free and o2 now counts. o3's seller-unilateral confirm lands
    // AFTER the sweep: delivered, seller-attested, never auto-completed —
    // it must not count.
    app.clock.set(app.clock.now() + chrono::Duration::days(15));
    let completed = workers::complete_due_delivered_orders(&app.pool, app.clock.now(), 14, 100, 2)
        .await
        .expect("sweep runs");
    assert_eq!(completed, 2, "o1 and o2 auto-complete");
    let (_, revision) = order_row(&app.pool, &o3.order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &o3.order_id,
            revision,
            json!({}),
            0x333,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    sqlx::query("DELETE FROM seller_stat_attestations")
        .execute(&app.pool)
        .await
        .expect("clear attestations");
    let signed =
        workers::generate_due_stat_attestations(&app.pool, &test_attestor(), app.clock.now())
            .await
            .expect("stat job runs");
    assert_eq!(signed, 1);
    let (body,): (Value,) =
        sqlx::query_as("SELECT body FROM seller_stat_attestations WHERE seller_pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("attestation");
    assert_eq!(
        body["completionRatePermille"],
        json!(666),
        "o1 and the dispute-free auto-completed o2 count; the still-open \
         seller-attested o3 does not"
    );
}

// listing.sync carries no details and can never null them; the public
// fulfillmentMethods converge through both register and sync.
#[sqlx::test]
async fn sync_converges_methods_but_never_touches_details(pool: PgPool) {
    let homeserver = spawn_fake_homeserver().await;
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), common::config_durable())
        .with_homeserver(Some(homeserver.client()))
        .with_pickup(Some(common::test_pickup_keys()));
    let app = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // The seller-signed record publishes both methods; sync registers it.
    let record = json!({
        "title": "Boots",
        "revision": 1,
        "media": [{ "id": "m1", "contentHash": "a".repeat(64) }],
        "variants": [{ "id": "v1", "quantity": 3, "enabled": true }],
        "sale": { "format": "fixed_price", "unitPrice": { "amountMinor": 12500, "currency": "USD", "exponent": 2 } },
        "fulfillmentMethods": ["shipping", "pickup"],
    });
    homeserver.put_record(&seller.pubky, "boots_01", record.clone());
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 0x401),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["listing"]["fulfillment_methods"],
        json!(["shipping", "pickup"])
    );

    // Details are set service-side; a sync from a device WITHOUT the
    // details (the record never carries them) cannot null them.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x402),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut updated = record.clone();
    updated["revision"] = json!(2);
    homeserver.put_record(&seller.pubky, "boots_01", updated);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 0x403),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) =
        owner_read(&app, &seller.token, &listing_agg(&seller.pubky, "boots_01")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["current"]["details"]["spot"], json!(SPOT));
    assert_eq!(body["last_version"], json!(1));
}

// Key absent (all-or-none gating): pickup_details.set and the buyer reveal
// are refused even on a non-sandbox deployment; nothing is ever stored
// plaintext.
#[sqlx::test]
async fn key_absent_refuses_writes_and_reveals(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let fake = Arc::new(FakeLocksClient::default());
    let locks = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: fake.clone(),
    });
    let clock = Arc::new(AdjustableClock::new(now));
    // Sandbox OFF, locks on, pickup keys ABSENT.
    let state = AppState::new(pool.clone(), clock.clone(), common::config_durable())
        .with_locks(Some(locks));
    let app = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x501,
        "00000000-0000-4000-9000-000000000501",
    )
    .await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x502),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    confirm_via_locks(&app, &fake, &buyer, &order, &seller, TEST_BUNDLE_ID, 0x503).await;
    let (status, body) = reveal(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM listing_pickup_details").await,
        0,
        "nothing is stored without the key"
    );
    let (_, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(body["pickup_available"], json!(false));
}

// The all-or-none boot probe: sealed rows without a key refuse startup; a
// key that opens NEITHER family refuses startup; the dual-key window and
// the re-seal job rotate both families with a completion criterion over
// both.
#[sqlx::test]
async fn boot_probe_and_two_family_rotation(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let current = common::test_pickup_keys();
    let previous_only = PickupKeys::from_hex(common::TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY, None)
        .expect("previous key parses");
    let rotated = common::test_pickup_keys_with_previous();
    let aggregate = listing_agg(&"s".repeat(52), "boots_01");

    // An empty database is coherent with and without keys.
    pickup::assert_pickup_sealing_coherent(&pool, None)
        .await
        .expect("empty without key");
    pickup::assert_pickup_sealing_coherent(&pool, Some(&current))
        .await
        .expect("empty with key");

    // Seed one row per family, sealed under the PREVIOUS key.
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, fulfillment, created_at, updated_at) \
         VALUES ($1, 'b', 's', 1, 'paid', '[]', 100, 0, 100, 'USD', 2, 1, $2, 'pickup', $3, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed order");
    let (order_id,): (Uuid,) = sqlx::query_as("SELECT id FROM orders LIMIT 1")
        .fetch_one(&pool)
        .await
        .expect("order id");
    let details_ct = previous_only.seal(&pickup::details_aad(&aggregate, 1), b"spot-alpha");
    sqlx::query(
        "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
         details_ciphertext, created_at, updated_at) VALUES ($1, $2, 1, $3, $4, $4)",
    )
    .bind(&aggregate)
    .bind("s".repeat(52))
    .bind(&details_ct)
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed details");
    let snapshot_ct = previous_only.seal(&pickup::snapshot_aad(order_id, 0, 1), b"spot-alpha");
    sqlx::query(
        "INSERT INTO pickup_line_snapshots (order_id, line_index, listing_aggregate_id, \
         version, snapshot_ciphertext, confirming_adapter, created_at) \
         VALUES ($1, 0, $2, 1, $3, 'locks', $4)",
    )
    .bind(order_id)
    .bind(&aggregate)
    .bind(&snapshot_ct)
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed snapshot");

    // No key configured: the probe refuses startup.
    pickup::assert_pickup_sealing_coherent(&pool, None)
        .await
        .expect_err("sealed rows without a key must fail the boot");
    // A key that opens neither family (current-only, rows under previous):
    // refused.
    pickup::assert_pickup_sealing_coherent(&pool, Some(&current))
        .await
        .expect_err("rows that do not open must fail the boot");
    // The dual-key window opens both families.
    pickup::assert_pickup_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect("dual-key window opens both families");

    // The re-seal job rotates BOTH families under the current key and
    // reports completion only when zero rows remain under the previous key.
    let progress = pickup::reseal_previous_key_batch(&pool, &rotated, now)
        .await
        .expect("re-seal runs");
    assert_eq!(progress.details_resealed, 1);
    assert_eq!(progress.snapshots_resealed, 1);
    assert_eq!(
        progress.remaining_under_previous, 0,
        "rotation complete over both families"
    );
    // Both rows now open under the current key alone.
    pickup::assert_pickup_sealing_coherent(&pool, Some(&current))
        .await
        .expect("rotated rows open under the current key");
    let (stored,): (Vec<u8>,) =
        sqlx::query_as("SELECT details_ciphertext FROM listing_pickup_details")
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(
        current
            .open(&pickup::details_aad(&aggregate, 1), &stored)
            .unwrap(),
        b"spot-alpha"
    );
    // A second pass is a no-op.
    let progress = pickup::reseal_previous_key_batch(&pool, &rotated, now)
        .await
        .expect("re-seal runs");
    assert_eq!(progress.details_resealed + progress.snapshots_resealed, 0);
    assert_eq!(progress.remaining_under_previous, 0);
}

// Rotation past the first batch: 130 details rows sealed under the previous
// key must ALL rotate in one job run — a first-100-only batch would re-read
// the same rotated rows on every pass and never report completion.
#[sqlx::test]
async fn rotation_walks_past_the_first_batch_and_reports_completion(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let current = common::test_pickup_keys();
    let previous_only = PickupKeys::from_hex(common::TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY, None)
        .expect("previous key parses");
    let rotated = common::test_pickup_keys_with_previous();
    let aggregate = listing_agg(&"s".repeat(52), "boots_01");

    // 130 rows in ONE family, all sealed under the previous key.
    for version in 1..=130i64 {
        let ciphertext = previous_only.seal(
            &pickup::details_aad(&aggregate, version),
            format!("spot-{version}").as_bytes(),
        );
        sqlx::query(
            "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
             details_ciphertext, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(&aggregate)
        .bind("s".repeat(52))
        .bind(version)
        .bind(&ciphertext)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed details");
    }

    // One run of the re-seal job rotates ALL of them and reports completion.
    let progress = pickup::reseal_previous_key_batch(&pool, &rotated, now)
        .await
        .expect("re-seal runs");
    assert_eq!(progress.details_resealed, 130, "every row rotates");
    assert_eq!(
        progress.remaining_under_previous, 0,
        "rotation completes past row 100"
    );
    // Every row opens under the current key alone, with its own plaintext.
    let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, details_ciphertext FROM listing_pickup_details ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .expect("rows");
    assert_eq!(rows.len(), 130);
    for (version, ciphertext) in rows {
        let plaintext = current
            .open(&pickup::details_aad(&aggregate, version), &ciphertext)
            .expect("rotated row opens under the current key");
        assert_eq!(plaintext, format!("spot-{version}").into_bytes());
    }
    // A second pass is a no-op.
    let progress = pickup::reseal_previous_key_batch(&pool, &rotated, now)
        .await
        .expect("re-seal runs");
    assert_eq!(progress.details_resealed, 0);
    assert_eq!(progress.remaining_under_previous, 0);
}

// The boot probe must catch a HALF-rotated table whose previous key was
// dropped: probing only the first row per family would sample the rotated
// row and pass, leaving the straggler to fail the first buyer's reveal.
#[sqlx::test]
async fn boot_probe_catches_a_half_rotated_table_with_the_previous_key_dropped(pool: PgPool) {
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let current = common::test_pickup_keys();
    let previous_only = PickupKeys::from_hex(common::TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY, None)
        .expect("previous key parses");
    let rotated = common::test_pickup_keys_with_previous();
    let seller = "s".repeat(52);

    // The row that sorts FIRST opens under the current key; the later row
    // is still sealed under the previous one.
    for (listing_id, keys) in [("aaa", current.clone()), ("zzz", Arc::new(previous_only))] {
        let aggregate = listing_agg(&seller, listing_id);
        let ciphertext = keys.seal(&pickup::details_aad(&aggregate, 1), b"spot");
        sqlx::query(
            "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
             details_ciphertext, created_at, updated_at) VALUES ($1, $2, 1, $3, $4, $4)",
        )
        .bind(&aggregate)
        .bind(&seller)
        .bind(&ciphertext)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed details");
    }

    // The previous key dropped from the configuration: the boot probe must
    // FAIL even though the family's first row opens under the current key.
    pickup::assert_pickup_sealing_coherent(&pool, Some(&current))
        .await
        .expect_err("a straggler under the dropped previous key must fail the boot");
    // The dual-key window still boots: both key classes open.
    pickup::assert_pickup_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect("the dual-key window opens both key classes");
}

// An order that pinned NOTHING (the listing's details were cleared before
// checkout) has no meeting point: the reveal is a typed refusal and never
// stamps first_revealed_at, so the buyer's cancel stays on the ordinary
// path while the seller can still complete the handover.
#[sqlx::test]
async fn reveal_refuses_an_order_that_pinned_nothing(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // One pickup listing whose details are set and CLEARED before checkout.
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_pickup_listing(&seller.pubky, "boots_01", 5, 0x601),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x602),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_01", 1, 0x603),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Two pickup orders on the cleared listing, both paid via Locks.
    let mut orders = Vec::new();
    for (index, checkout_id) in [
        "00000000-0000-4000-9000-000000000604",
        "00000000-0000-4000-9000-000000000605",
    ]
    .iter()
    .enumerate()
    {
        // Each confirmed order bumps the listing's server revision.
        let (listing_revision,): (i64,) =
            sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(listing_agg(&seller.pubky, "boots_01"))
                .fetch_one(&app.pool)
                .await
                .expect("listing");
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout_lines(
                vec![line(&seller.pubky, "boots_01", listing_revision, "pickup")],
                false,
                checkout_id,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
        let order = PickupOrder {
            order_id: body["result"]["orders"][0]["id"]
                .as_str()
                .expect("order id")
                .to_string(),
            payment_id: body["result"]["payments"][0]["id"]
                .as_str()
                .expect("payment id")
                .to_string(),
        };
        let bundle = [TEST_BUNDLE_ID, BUNDLE_2][index];
        confirm_via_locks(
            &app,
            &fake,
            &buyer,
            &order,
            &seller,
            bundle,
            0x606 + index as u64,
        )
        .await;
        orders.push(order);
    }

    // The reveal is refused with the typed error — no empty 200 — and the
    // withdrawal stamp is never written.
    let (status, body) = reveal(&app, &buyer.token, &orders[0].order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("This order carries no pinned pickup details.")
    );
    let (stamped,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT first_revealed_at FROM orders WHERE id = $1::uuid")
            .bind(&orders[0].order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert!(
        stamped.is_none(),
        "no first_revealed_at without a pinned snapshot"
    );

    // The buyer's cancel_request follows the NORMAL path (cancel_requested,
    // awaiting the seller) — never the unilateral terms-change exit.
    let (_, revision) = order_row(&app.pool, &orders[0].order_id).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &orders[0].order_id,
            revision,
            json!({ "reason": "changed my mind" }),
            0x608,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancel_requested"));
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' \
                 AND kind = 'order.cancelled_terms_change'",
                orders[0].order_id
            )
        )
        .await,
        0,
        "no terms-change cancel on an order that never had terms"
    );

    // The seller can still complete the handover on the second order: no
    // pinned terms means no unresolved terms change blocks the confirm.
    let (_, revision) = order_row(&app.pool, &orders[1].order_id).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "fulfillment.confirm_pickup",
            &orders[1].order_id,
            revision,
            json!({}),
            0x609,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
}

// A details row that fails to open (sealed under an unknown key) must FAIL
// the confirmation cleanly — receipt transaction rolled back, no panic.
#[sqlx::test]
async fn confirm_order_fails_cleanly_on_an_unopenable_details_row(pool: PgPool) {
    // Sandbox on (so payment.sandbox_advance confirms) with pickup keys
    // configured; the corrupted row is seeded directly, as an operator
    // mishap would leave it.
    let app = test_app_with_pickup(pool, true).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x701,
        "00000000-0000-4000-9000-000000000701",
    )
    .await;

    // Seed details v1 sealed under an UNKNOWN key, with its counter row.
    let aggregate = listing_agg(&seller.pubky, "boots_01");
    let unknown = PickupKeys::from_hex(
        "9999999999999999999999999999999999999999999999999999999999999999",
        None,
    )
    .expect("unknown key parses");
    let ciphertext = unknown.seal(&pickup::details_aad(&aggregate, 1), SPOT.as_bytes());
    sqlx::query(
        "INSERT INTO listing_pickup_version_counters (aggregate_id, seller_pubky, last_version, \
         updated_at) VALUES ($1, $2, 1, $3)",
    )
    .bind(&aggregate)
    .bind(&seller.pubky)
    .bind(app.clock.now())
    .execute(&app.pool)
    .await
    .expect("counter row");
    sqlx::query(
        "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
         details_ciphertext, created_at, updated_at) VALUES ($1, $2, 1, $3, $4, $4)",
    )
    .bind(&aggregate)
    .bind(&seller.pubky)
    .bind(&ciphertext)
    .bind(app.clock.now())
    .execute(&app.pool)
    .await
    .expect("details row");

    // The confirmation returns an error — no panic — and rolls back.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::payment_command(&order.payment_id, 1, "confirmed", 1, 0x702),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"]["code"], json!("INTERNAL"));
    let (state, receipt): (String, Option<Uuid>) =
        sqlx::query_as("SELECT state, receipt_id FROM orders WHERE id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(state, "pending_payment", "the confirmation rolled back");
    assert!(receipt.is_none(), "no receipt was issued");
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM pickup_line_snapshots").await,
        0,
        "no snapshot pinned"
    );
}

// The worker path: one unopenable details row fails the pass LOUDLY (the
// worker loop logs and continues) instead of panicking the loop dead; once
// the row is repaired the next pass confirms the payment.
#[sqlx::test]
async fn worker_survives_an_unopenable_details_row(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pickup_order(
        &app,
        &seller,
        &buyer,
        "boots_01",
        0x801,
        "00000000-0000-4000-9000-000000000801",
    )
    .await;

    // The same operator-mishap seed: details v1 sealed under an unknown key.
    let aggregate = listing_agg(&seller.pubky, "boots_01");
    let unknown = PickupKeys::from_hex(
        "9999999999999999999999999999999999999999999999999999999999999999",
        None,
    )
    .expect("unknown key parses");
    let ciphertext = unknown.seal(&pickup::details_aad(&aggregate, 1), SPOT.as_bytes());
    sqlx::query(
        "INSERT INTO listing_pickup_version_counters (aggregate_id, seller_pubky, last_version, \
         updated_at) VALUES ($1, $2, 1, $3)",
    )
    .bind(&aggregate)
    .bind(&seller.pubky)
    .bind(app.clock.now())
    .execute(&app.pool)
    .await
    .expect("counter row");
    sqlx::query(
        "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
         details_ciphertext, created_at, updated_at) VALUES ($1, $2, 1, $3, $4, $4)",
    )
    .bind(&aggregate)
    .bind(&seller.pubky)
    .bind(&ciphertext)
    .bind(app.clock.now())
    .execute(&app.pool)
    .await
    .expect("details row");

    // A completed Locks outcome arrives; the pass fails on the unopenable
    // row — an Err the worker loop logs, NOT a panic that kills the loop.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::register_locks_command(
            &order.payment_id,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            0x802,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    let result = run_once(&app.state, Uuid::new_v4(), app.clock.now()).await;
    assert!(
        result.is_err(),
        "the pass reports the unopenable row instead of panicking"
    );
    let (state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order.order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row");
    assert_eq!(state, "pending_payment", "the confirmation rolled back");

    // Repair the row (re-seal under the configured key) and let the next
    // pass — after the claim and lease deferrals elapse — confirm it.
    let repaired =
        common::test_pickup_keys().seal(&pickup::details_aad(&aggregate, 1), SPOT.as_bytes());
    sqlx::query(
        "UPDATE listing_pickup_details SET details_ciphertext = $2 WHERE aggregate_id = $1",
    )
    .bind(&aggregate)
    .bind(&repaired)
    .execute(&app.pool)
    .await
    .expect("repair details row");
    app.clock
        .set(app.clock.now() + chrono::Duration::seconds(61));
    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("the worker loop continues after the failed pass");
    assert_eq!(summary.locks_completions_applied, 1, "payment confirmed");
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM pickup_line_snapshots").await,
        1,
        "the repaired row pins on the next pass"
    );
}

// One buyer with TWO paid orders on the same listing gets one notification
// per order on a details update — the notifications dedup on (event id,
// recipient) must never collapse them.
#[sqlx::test]
async fn details_update_notifies_each_paid_order_of_the_same_buyer(pool: PgPool) {
    let (app, fake) = test_app_with_pickup_and_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &register_pickup_listing(&seller.pubky, "boots_01", 5, 0x901),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0x902),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut orders = Vec::new();
    for (index, checkout_id) in [
        "00000000-0000-4000-9000-000000000903",
        "00000000-0000-4000-9000-000000000904",
    ]
    .iter()
    .enumerate()
    {
        // Each confirmed order bumps the listing's server revision.
        let (listing_revision,): (i64,) =
            sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1")
                .bind(listing_agg(&seller.pubky, "boots_01"))
                .fetch_one(&app.pool)
                .await
                .expect("listing");
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout_lines(
                vec![line(&seller.pubky, "boots_01", listing_revision, "pickup")],
                false,
                checkout_id,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
        let order = PickupOrder {
            order_id: body["result"]["orders"][0]["id"]
                .as_str()
                .expect("order id")
                .to_string(),
            payment_id: body["result"]["payments"][0]["id"]
                .as_str()
                .expect("payment id")
                .to_string(),
        };
        let bundle = [TEST_BUNDLE_ID, BUNDLE_2][index];
        confirm_via_locks(
            &app,
            &fake,
            &buyer,
            &order,
            &seller,
            bundle,
            0x905 + index as u64,
        )
        .await;
        orders.push(order);
    }

    // One details edit; the worker delivers the outbox intents.
    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 1, SPOT_EDITED, 0x907),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    let mut notified: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM notifications \
         WHERE type = 'pickup_details_updated' AND recipient_pubky = $1 ORDER BY aggregate_id",
    )
    .bind(&buyer.pubky)
    .fetch_all(&app.pool)
    .await
    .expect("notifications");
    notified.sort();
    let mut expected: Vec<String> = orders
        .iter()
        .map(|order| format!("order:{}", order.order_id))
        .collect();
    expected.sort();
    assert_eq!(
        notified.iter().map(|row| &row.0).collect::<Vec<_>>(),
        expected.iter().collect::<Vec<_>>(),
        "one notification per paid order"
    );
}

// A sync record repeating a fulfillment method non-adjacently
// (["shipping", "pickup", "shipping"]) must converge to the unique
// first-seen methods — adjacent-only dedup would fail registration
// validation and block the sync forever.
#[sqlx::test]
async fn sync_dedups_non_adjacent_fulfillment_methods(pool: PgPool) {
    let homeserver = spawn_fake_homeserver().await;
    let now: DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let state = AppState::new(pool.clone(), clock.clone(), common::config_durable())
        .with_homeserver(Some(homeserver.client()))
        .with_pickup(Some(common::test_pickup_keys()));
    let app = TestApp {
        router: build_router(state.clone()),
        pool: pool.clone(),
        clock,
        state,
    };
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    let record = json!({
        "title": "Boots",
        "revision": 1,
        "media": [{ "id": "m1", "contentHash": "a".repeat(64) }],
        "variants": [{ "id": "v1", "quantity": 3, "enabled": true }],
        "sale": { "format": "fixed_price", "unitPrice": { "amountMinor": 12500, "currency": "USD", "exponent": 2 } },
        "fulfillmentMethods": ["shipping", "pickup", "shipping"],
    });
    homeserver.put_record(&seller.pubky, "boots_01", record);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 0xa01),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["listing"]["fulfillment_methods"],
        json!(["shipping", "pickup"]),
        "non-adjacent duplicates collapse, first-seen order preserved"
    );
}

// `pickup_details.set` on a listing that does not publish pickup is refused
// (the details would be unreachable); `clear` stays allowed so stale
// details remain removable.
#[sqlx::test]
async fn set_requires_the_listing_to_publish_pickup(pool: PgPool) {
    let app = test_app_with_pickup(pool, false).await;
    let seller = new_actor(&app).await;

    // The default registration publishes shipping only.
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &seller.token,
        &set_details(&seller.pubky, "boots_01", 0, SPOT, 0xb01),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The listing does not publish pickup.")
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM listing_pickup_details").await,
        0,
        "nothing sealed for a non-pickup listing"
    );

    // Clearing stale data stays possible on the same listing.
    let (status, body) = execute(
        &app,
        &seller.token,
        &clear_details(&seller.pubky, "boots_01", 0, 0xb02),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
