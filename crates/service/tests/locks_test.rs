//! Server-side Locks verification (plan task 4.5, ADR-0019 §7/§8).
//!
//! The lifecycle lookup is driven by a programmable fake so no live Lock
//! Server is required; the fake exercises completed, pending, in-progress,
//! failed, expired, not-found, unavailable, late, and duplicate outcomes.
//! Nothing here fakes verification semantics: the worker under test is the
//! production code path, and no client claim ever advances a payment.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use marketplace_service::clock::{AdjustableClock, Clock};
use marketplace_service::config::Config;
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use marketplace_service::http::build_router;
use marketplace_service::locks::{LocksLookupOutcome, LocksRuntime, LocksTaskStatus};
use marketplace_service::workers::{self, run_once, try_acquire_lease};
use marketplace_service::AppState;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    checkout_command_with_id, count, create_pending_order, execute, lock_document_for,
    lock_resource_for, lock_resource_for_payment, new_actor, payment_command, register_command,
    register_listing_command, register_locks_command, send, test_app, test_app_with_locks,
    test_locks_keys, FakeLocksClient, PendingOrder, TestActor, TestApp, TEST_BUNDLE_ID,
    TEST_LOCK_ID,
};

/// A content-lock homeserver double serving exactly the documents the test
/// scripts, keyed by `(creator, content path)`; every other fetch is a clean
/// not-found. It exists so identity-validation negatives can serve tampered
/// or foreign documents at the seller-authoritative resource path.
struct ScriptedLocksHomeserver {
    documents: HashMap<(String, String), Value>,
}

impl HomeserverListingClient for ScriptedLocksHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_content_lock<'a>(
        &'a self,
        creator_pubky: &'a str,
        content_path: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            match self
                .documents
                .get(&(creator_pubky.to_string(), content_path.to_string()))
            {
                Some(document) => HomeserverFetchOutcome::Found(document.clone()),
                None => HomeserverFetchOutcome::NotFound,
            }
        })
    }
}

/// A Locks-enabled test app whose homeserver serves the scripted
/// content-lock documents and nothing else.
async fn test_app_with_lock_documents(
    pool: PgPool,
    documents: HashMap<(String, String), Value>,
) -> TestApp {
    test_app_with_homeserver(pool, Arc::new(ScriptedLocksHomeserver { documents })).await
}

/// A homeserver double that serves seller listing records from an
/// interior-mutable map — so `listing.sync` can heal a post-checkout lock
/// change through the real sync path — AND the fixture content-lock
/// documents, so prepare can fetch them.
#[derive(Default)]
struct SyncableLocksHomeserver {
    records: Mutex<HashMap<(String, String), Value>>,
}

impl SyncableLocksHomeserver {
    fn put_record(&self, seller_pubky: &str, listing_id: &str, record: Value) {
        self.records
            .lock()
            .expect("records lock")
            .insert((seller_pubky.to_string(), listing_id.to_string()), record);
    }
}

impl HomeserverListingClient for SyncableLocksHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            match self
                .records
                .lock()
                .expect("records lock")
                .get(&(seller_pubky.to_string(), listing_id.to_string()))
            {
                Some(record) => HomeserverFetchOutcome::Found(record.clone()),
                None => HomeserverFetchOutcome::NotFound,
            }
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_content_lock<'a>(
        &'a self,
        creator_pubky: &'a str,
        content_path: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            for (amount, asset) in [(13_700, "USD"), (12_500, "USD"), (25_000, "USD")] {
                let resource = lock_resource_for_payment(creator_pubky, amount, asset);
                if resource.strip_prefix(creator_pubky) == Some(content_path) {
                    return HomeserverFetchOutcome::Found(lock_document_for(
                        creator_pubky,
                        amount,
                        asset,
                    ));
                }
            }
            HomeserverFetchOutcome::NotFound
        })
    }
}

/// A canonical camelCase listing record carrying a `digitalLock`, mirroring
/// the registered fixture listing (same revision, price, shipping, and
/// quantity) so an equal-revision sync heals exactly the lock fields.
fn listing_record_with_lock(revision: i64, policy_uri: &str) -> Value {
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
        }],
        "variants": [{
            "id": "variant_0",
            "enabled": true,
            "quantity": 1,
            "sku": null,
            "priceOverride": null,
        }],
        "shippingOptions": [{
            "id": "ship_flat",
            "pricing": "flat",
            "label": "Seller shipping",
            "price": { "amountMinor": 1_200, "currency": "USD", "exponent": 2 },
            "estimatedMinDays": 2,
            "estimatedMaxDays": 7,
        }],
        "sale": {
            "acceptsOffers": true,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
        "digitalLock": { "policyUri": policy_uri, "criterionId": "paykit" },
    })
}

/// A homeserver double whose content-lock fetches are always a transient
/// failure (transport/5xx class), never a definitive not-found.
struct UnavailableLocksHomeserver;

impl HomeserverListingClient for UnavailableLocksHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::Unavailable })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::Unavailable })
    }

    fn fetch_content_lock<'a>(
        &'a self,
        _creator_pubky: &'a str,
        _content_path: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::Unavailable })
    }
}

async fn test_app_with_homeserver(
    pool: PgPool,
    homeserver: Arc<dyn HomeserverListingClient>,
) -> TestApp {
    let now: chrono::DateTime<chrono::Utc> = common::NOW.parse().expect("timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let locks = Arc::new(LocksRuntime {
        keys: test_locks_keys(),
        client: Arc::new(FakeLocksClient::default()),
    });
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
        .with_locks(Some(locks))
        .with_homeserver(Some(homeserver));
    TestApp {
        router: build_router(state.clone()),
        pool,
        clock,
        state,
    }
}

/// A checkout envelope for the shared fixture listing at a known server
/// revision (each payment hold bumps it).
fn checkout_at_listing_revision(seller_pubky: &str, command_id: &str, revision: i64) -> Value {
    let mut checkout = checkout_command_with_id(seller_pubky, command_id);
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    checkout
}

/// The recorded binding outcomes for one payment, as a sorted multiset.
async fn binding_outcomes(pool: &PgPool, payment_id: &str) -> Vec<String> {
    let mut outcomes: Vec<(String,)> = sqlx::query_as(
        "SELECT outcome FROM payment_locks_binding_outcomes WHERE payment_id = $1::uuid",
    )
    .bind(payment_id)
    .fetch_all(pool)
    .await
    .expect("binding outcomes listed");
    outcomes.sort();
    outcomes.into_iter().map(|(outcome,)| outcome).collect()
}

/// A second canonical bundle id, distinct from [`TEST_BUNDLE_ID`].
const OTHER_BUNDLE_ID: &str = "111G40R40M30E209185GR38E1W";

async fn register_locks(
    app: &TestApp,
    buyer_token: &str,
    order: &PendingOrder,
    seller_pubky: &str,
) -> Value {
    let prepare = payment_command(&order.payment_id, 1, "prepare_locks", 0, 90);
    let mut prepare = prepare;
    prepare["kind"] = json!("payment.prepare_locks");
    prepare["payload"] = json!({ "payment_id": order.payment_id });
    let (status, body) = execute(app, buyer_token, &prepare).await;
    assert_eq!(status, StatusCode::OK, "preparation failed: {body}");
    let (status, body) = execute(
        app,
        buyer_token,
        &register_locks_command(
            &order.payment_id,
            2,
            TEST_BUNDLE_ID,
            &lock_resource_for(seller_pubky),
            1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");
    body
}

async fn payment_state(pool: &PgPool, payment_id: &str) -> (String, String, i64) {
    let (state, adapter, revision): (String, String, i64) =
        sqlx::query_as("SELECT state, adapter, revision FROM payments WHERE id = $1::uuid")
            .bind(payment_id)
            .fetch_one(pool)
            .await
            .expect("payment row exists");
    (state, adapter, revision)
}

async fn order_state(pool: &PgPool, order_id: &str) -> String {
    let (state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row exists");
    state
}

// A prepared-but-never-registered row is invisible to the lifecycle claim:
// a worker pass between prepare and register completes without error,
// performs no lookup and no claim stamp for the prepared row, and still
// processes other registered rows; attaching afterwards resumes ordinary
// polling for the prepared payment.
#[sqlx::test]
async fn a_prepared_row_survives_a_worker_pass_untouched(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // Two units so the buyer holds two payments: one stays prepared-only,
    // the other registers and completes during the interleaved pass.
    let mut listing = register_command(&seller.pubky, 2);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut payments = Vec::new();
    for command_id in [
        "00000000-0000-4000-8000-000000000100",
        "00000000-0000-4000-8000-000000000101",
    ] {
        let (status, checkout) = execute(
            &app,
            &buyer.token,
            &checkout_command_with_id(&seller.pubky, command_id),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{checkout}");
        payments.push(
            checkout["result"]["payments"][0]["id"]
                .as_str()
                .expect("payment id")
                .to_string(),
        );
    }
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&payments[0], 80),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first preparation: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&payments[1], 81),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second preparation: {body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &payments[1],
            2,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            82,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "registration: {body}");
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );

    // The interleaved pass: the prepared row must not be claimed, and the
    // batch must not fail because of it.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass completes with a prepared row present");
    assert_eq!(summary.locks_completions_applied, 1);
    assert_eq!(
        fake.lookups(),
        vec![(seller.pubky.clone(), TEST_BUNDLE_ID.to_string())],
        "only the registered correlation is looked up"
    );
    let (preparation_state, last_checked_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as(
            "SELECT preparation_state, last_checked_at FROM payment_locks_correlations \
             WHERE payment_id = $1::uuid",
        )
        .bind(&payments[0])
        .fetch_one(&app.pool)
        .await
        .expect("prepared correlation exists");
    assert_eq!(preparation_state, "prepared");
    assert!(
        last_checked_at.is_none(),
        "a prepared row is never claim-stamped"
    );
    let (state, _, _) = payment_state(&app.pool, &payments[1]).await;
    assert_eq!(state, "confirmed", "the registered row was processed");

    // Attaching afterwards resumes ordinary polling for the first payment.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &payments[0],
            2,
            OTHER_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            83,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "late registration: {body}");
    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 0);
    assert_eq!(
        fake.lookups(),
        vec![
            (seller.pubky.clone(), TEST_BUNDLE_ID.to_string()),
            (seller.pubky.clone(), OTHER_BUNDLE_ID.to_string()),
        ],
        "the newly registered correlation polls normally"
    );
}

// Receiver-clock eligibility at attachment: once the prepared window has
// elapsed on the service clock, registration is refused with a static
// INVALID_STATE even when the expiry sweep has not run yet; the sweep then
// terminalises the preparation (payment expired, order cancelled, hold
// restocked). A cancelled order likewise refuses attachment.
#[sqlx::test]
async fn registration_after_window_expiry_before_the_sweep_is_refused(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 90),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");

    // The receiver clock passes the preparation/hold deadline without a
    // sweep: attachment must refuse, never confirm later.
    app.clock.advance_seconds(3_601);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            2,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            91,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The Locks preparation has expired.")
    );
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        state, "awaiting_entitlement",
        "a refused registration advances nothing"
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.locks_registered'"
        )
        .await,
        0
    );
    let (preparation_state, bundle): (String, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT preparation_state, bundle_id_ciphertext FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert_eq!(preparation_state, "prepared");
    assert!(bundle.is_none());

    // The sweep then terminalises the preparation on marketplace time.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");

    // Registration stays refused after the sweep (the payment is terminal).
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            3,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            92,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// A buyer-cancelled order no longer holds the prepared window: attachment
// is refused even though the payment itself is still awaiting entitlement.
#[sqlx::test]
async fn registration_after_order_cancellation_is_refused(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 95),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");

    let cancel = common::order_command(
        "order.cancel_request",
        &order.order_id,
        1,
        json!({ "reason": "Changed my mind" }),
        96,
    );
    let (status, body) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            2,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            97,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The order no longer holds the prepared Locks payment window.")
    );
    let (preparation_state, bundle): (String, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT preparation_state, bundle_id_ciphertext FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert_eq!(preparation_state, "prepared");
    assert!(bundle.is_none());
}

// The minted client reference is plaintext only in the authenticated
// response: the durable command_results row stores it sealed to the payment
// and buyer, and an exact replay unseals it back to the same buyer.
#[sqlx::test]
async fn the_prepare_result_is_sealed_at_rest_and_unsealed_on_replay(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let command = common::prepare_locks_command(&order.payment_id, 100);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");
    let client_reference = body["result"]["client_reference"]
        .as_str()
        .expect("the response carries the minted reference")
        .to_string();

    let (stored,): (Value,) =
        sqlx::query_as("SELECT result FROM command_results WHERE command_id = $1::uuid")
            .bind(command["command_id"].as_str().expect("command id"))
            .fetch_one(&app.pool)
            .await
            .expect("stored result row exists");
    let serialized = stored.to_string();
    assert!(
        !serialized.contains(&client_reference),
        "the stored result must not contain the plaintext reference"
    );
    assert!(
        stored["result"]["client_reference_sealed"].is_string(),
        "the stored result carries the sealed reference"
    );
    assert!(stored["result"]["client_reference"].is_null());

    // An exact replay returns the same reference, unsealed for the buyer.
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, body, "replay restores the exact original result");

    // Another actor cannot replay the buyer's sealed result: the same
    // command id under a different actor is a NEW command, refused before
    // any correlation detail is exposed.
    let outsider = new_actor(&app).await;
    let (status, replay) = execute(&app, &outsider.token, &command).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{replay}");
    assert_eq!(replay["error"]["code"], json!("UNAUTHORIZED"));
    assert!(!replay.to_string().contains(&client_reference));
}

// The adapter is pinned atomically with the preparation (DESIGN §3.2):
// immediately after prepare — before any bundle attaches — the payment
// adapter is `locks`, the revision has advanced, and the sandbox command
// can no longer drive the payment.
#[sqlx::test]
async fn prepare_pins_the_locks_adapter_atomically(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let command = common::prepare_locks_command(&order.payment_id, 110);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");
    assert_eq!(body["revision"], json!(2));
    let (state, adapter, revision) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        (state.as_str(), adapter.as_str(), revision),
        ("awaiting_entitlement", "locks", 2),
        "the prepared row, the hold, and the adapter switch commit together"
    );
    let (preparation_state,): (String,) =
        sqlx::query_as("SELECT preparation_state FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(preparation_state, "prepared");

    // The sandbox path is already closed, ahead of attachment.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 2, "confirmed", 1, 111),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    let (state, adapter, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        (state.as_str(), adapter.as_str()),
        ("awaiting_entitlement", "locks")
    );
}

// Every designed binding outcome is recorded in the static vocabulary:
// `prepared`/`registered` stamp the correlation row transactionally and
// append to the audit table; each refusal appends its own value even though
// the refused command rolls back.
#[sqlx::test]
async fn every_designed_binding_outcome_is_recorded(pool: PgPool) {
    // prepared + registered, with the column stamped transactionally.
    let (app, _fake) = test_app_with_locks(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // Three units: this test holds three orders on one listing.
    let mut listing = register_command(&seller.pubky, 3);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000004002"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = common::PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    };
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    let (column,): (String,) =
        sqlx::query_as("SELECT binding_outcome FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(column, "registered");
    assert_eq!(
        binding_outcomes(&app.pool, &order.payment_id).await,
        vec!["prepared".to_string(), "registered".to_string()]
    );

    // refused_no_prepare: registration without any preparation.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_at_listing_revision(&seller.pubky, "00000000-0000-4000-8000-000000004000", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let no_prepare_payment = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &no_prepare_payment,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            120,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        binding_outcomes(&app.pool, &no_prepare_payment).await,
        vec!["refused_no_prepare".to_string()]
    );

    // refused_expired: the prepared window lapses before attachment.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_at_listing_revision(&seller.pubky, "00000000-0000-4000-8000-000000004001", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let expired_payment = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&expired_payment, 121),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");
    app.clock.advance_seconds(3_601);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &expired_payment,
            2,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            122,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        binding_outcomes(&app.pool, &expired_payment).await,
        vec!["prepared".to_string(), "refused_expired".to_string()]
    );

    // refused_identity: a tampered document at the seller-authoritative
    // path (its bytes no longer derive that path).
    let seller_key = common::random_keypair();
    let seller_pubky = seller_key.1.clone();
    let mut tampered = lock_document_for(&seller_pubky, 13_700, "USD");
    tampered["criteria"][0]["params"]["amount"] = json!("13701");
    let resource = lock_resource_for(&seller_pubky);
    let path = resource
        .strip_prefix(&seller_pubky)
        .expect("creator prefixes resource")
        .to_string();
    let identity_app = test_app_with_lock_documents(
        pool.clone(),
        HashMap::from([((seller_pubky.clone(), path), tampered)]),
    )
    .await;
    let identity_seller = TestActor {
        token: common::authenticate(&identity_app, &seller_key.0).await,
        keypair: seller_key.0,
        pubky: seller_pubky,
    };
    let identity_buyer = new_actor(&identity_app).await;
    let identity_order =
        create_pending_order(&identity_app, &identity_seller, &identity_buyer).await;
    let (status, body) = execute(
        &identity_app,
        &identity_buyer.token,
        &common::prepare_locks_command(&identity_order.payment_id, 123),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        binding_outcomes(&identity_app.pool, &identity_order.payment_id).await,
        vec!["refused_identity".to_string()]
    );

    // refused_criterion: an identity-valid document whose sole criterion
    // amount does not equal the payment amount, served at its own typed
    // path.
    let seller_key = common::random_keypair();
    let seller_pubky = seller_key.1.clone();
    let document = lock_document_for(&seller_pubky, 12_500, "USD");
    let resource = lock_resource_for_payment(&seller_pubky, 12_500, "USD");
    let path = resource
        .strip_prefix(&seller_pubky)
        .expect("creator prefixes resource")
        .to_string();
    let criterion_app = test_app_with_lock_documents(
        pool.clone(),
        HashMap::from([((seller_pubky.clone(), path), document)]),
    )
    .await;
    let criterion_seller = TestActor {
        token: common::authenticate(&criterion_app, &seller_key.0).await,
        keypair: seller_key.0,
        pubky: seller_pubky,
    };
    let criterion_buyer = new_actor(&criterion_app).await;
    let mut listing = register_command(&criterion_seller.pubky, 1);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": resource,
        "criterionId": "paykit",
    });
    let (status, body) = execute(&criterion_app, &criterion_seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &criterion_app,
        &criterion_buyer.token,
        &common::checkout_command(&criterion_seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let criterion_payment = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        &criterion_app,
        &criterion_buyer.token,
        &common::prepare_locks_command(&criterion_payment, 124),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        binding_outcomes(&criterion_app.pool, &criterion_payment).await,
        vec!["refused_criterion".to_string()]
    );

    // refused_unavailable: the seller's homeserver cannot be reached.
    let unavailable_app =
        test_app_with_homeserver(pool, Arc::new(UnavailableLocksHomeserver)).await;
    let unavailable_seller = new_actor(&unavailable_app).await;
    let unavailable_buyer = new_actor(&unavailable_app).await;
    let unavailable_order =
        create_pending_order(&unavailable_app, &unavailable_seller, &unavailable_buyer).await;
    let (status, body) = execute(
        &unavailable_app,
        &unavailable_buyer.token,
        &common::prepare_locks_command(&unavailable_order.payment_id, 125),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], json!("UPSTREAM_UNAVAILABLE"));
    assert_eq!(
        binding_outcomes(&unavailable_app.pool, &unavailable_order.payment_id).await,
        vec!["refused_unavailable".to_string()]
    );
}

// Prepare state-machine proofs (Sol Wave 1A review, P3-1): same-reference
// replay by the same buyer; a different actor refused before any
// correlation detail; a concurrent double prepare producing exactly one row
// with both callers observing the same reference; and an elapsed
// preparation never replayed. The worker-pass-between-prepare-and-register
// interleaving is pinned by `a_prepared_row_survives_a_worker_pass_untouched`,
// and prepare-after-registration by `prepare_after_registration_is_a_stable_invalid_state`.
#[sqlx::test]
async fn prepare_replay_returns_the_same_reference_only_to_the_buyer(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let outsider = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let first = common::prepare_locks_command(&order.payment_id, 130);
    let (status, first_body) = execute(&app, &buyer.token, &first).await;
    assert_eq!(status, StatusCode::OK, "preparation: {first_body}");

    // A fresh command id from the same buyer recovers the SAME reference
    // and window — no new hold, no new row, no revision change.
    let second = common::prepare_locks_command(&order.payment_id, 131);
    let (status, second_body) = execute(&app, &buyer.token, &second).await;
    assert_eq!(status, StatusCode::OK, "replay: {second_body}");
    assert_eq!(
        second_body["result"]["client_reference"], first_body["result"]["client_reference"],
        "the same buyer recovers the same server-minted reference"
    );
    assert_eq!(
        second_body["result"]["window_expires_at"],
        first_body["result"]["window_expires_at"]
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        1
    );
    let (_, _, revision) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(revision, 2, "a replay mints nothing and bumps nothing");

    // Neither the seller nor an outsider can prepare (or recover the
    // reference): refused before any correlation detail is exposed.
    for actor in [&seller, &outsider] {
        let command = common::prepare_locks_command(&order.payment_id, 132);
        let (status, body) = execute(&app, &actor.token, &command).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
        assert!(!body.to_string().contains(
            first_body["result"]["client_reference"]
                .as_str()
                .expect("reference")
        ));
    }
}

#[sqlx::test]
async fn concurrent_double_prepare_mints_exactly_one_reference(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    // Two distinct command ids race the same payment: the payment row lock
    // serializes them, the winner mints, and the loser observes the
    // winner's prepared row and recovers the same reference.
    let first = common::prepare_locks_command(&order.payment_id, 140);
    let second = common::prepare_locks_command(&order.payment_id, 141);
    let (first_result, second_result) = tokio::join!(
        send(
            app.router.clone(),
            "POST",
            "/v1/commands",
            Some(&buyer.token),
            &first
        ),
        send(
            app.router.clone(),
            "POST",
            "/v1/commands",
            Some(&buyer.token),
            &second
        ),
    );
    let (first_status, first_body) = first_result;
    let (second_status, second_body) = second_result;
    assert_eq!(first_status, StatusCode::OK, "first: {first_body}");
    assert_eq!(second_status, StatusCode::OK, "second: {second_body}");
    assert_eq!(
        first_body["result"]["client_reference"], second_body["result"]["client_reference"],
        "both callers observe the same server-minted reference"
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        1,
        "exactly one prepared row wins"
    );
    let (state, adapter, revision) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        (state.as_str(), adapter.as_str(), revision),
        ("awaiting_entitlement", "locks", 2),
        "one hold, one adapter switch, one revision bump"
    );
}

#[sqlx::test]
async fn an_elapsed_preparation_is_never_replayed(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 150),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation: {body}");
    let reference = body["result"]["client_reference"]
        .as_str()
        .expect("reference")
        .to_string();

    // The window lapses with no sweep: a replay must NOT hand back the
    // elapsed reference.
    app.clock.advance_seconds(3_601);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 151),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The Locks preparation has expired.")
    );
    assert!(!body.to_string().contains(&reference));
    assert_eq!(
        binding_outcomes(&app.pool, &order.payment_id).await,
        vec!["prepared".to_string(), "refused_expired".to_string()]
    );

    // The sweep terminalises the elapsed preparation.
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
}

// Prepare after registration is a stable INVALID_STATE — it must never
// fall through to a second insert and surface a uniqueness conflict
// (Sol Wave 1A review, P3-2).
#[sqlx::test]
async fn prepare_after_registration_is_a_stable_invalid_state(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 160),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The payment already has a Locks preparation.")
    );
    // Idempotent under repetition: the same refusal again, never an
    // INVARIANT_VIOLATION uniqueness fall-through.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 161),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        1
    );
}

// Registration stores only an encrypted correlation bound to the order's
// participants, amount, asset, policy version, and lock resource hash; the
// payment flips to the 'locks' adapter and the bundle id appears nowhere in
// plaintext.
#[sqlx::test]
async fn registration_stores_an_encrypted_bound_correlation(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let body = register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    assert_eq!(body["revision"], json!(3));
    assert_eq!(body["result"]["payment"]["adapter"], json!("locks"));
    assert_eq!(
        body["result"]["payment"]["state"],
        json!("awaiting_entitlement"),
        "registration must not advance the payment"
    );
    assert_eq!(body["result"]["verification"]["state"], json!("pending"));
    assert!(body["result"]["verification"]["window_expires_at"].is_string());

    let (payment_id, buyer_pubky, creator_pubky, amount, asset, policy, ciphertext, token): (
        Uuid,
        String,
        String,
        i64,
        String,
        i32,
        Vec<u8>,
        Vec<u8>,
    ) = sqlx::query_as(
        "SELECT payment_id, buyer_pubky, creator_pubky, amount_minor, asset, policy_version, \
         bundle_id_ciphertext, bundle_lookup_token FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert_eq!(payment_id.to_string(), order.payment_id);
    assert_eq!(buyer_pubky, buyer.pubky);
    assert_eq!(creator_pubky, seller.pubky);
    assert_eq!(amount, 13_700); // 12_500 + 1_200 seller-signed shipping
    assert_eq!(asset, "USD");
    assert_eq!(policy, 1);
    assert!(
        !ciphertext
            .windows(TEST_BUNDLE_ID.len())
            .any(|window| window == TEST_BUNDLE_ID.as_bytes()),
        "the stored ciphertext must not contain the plaintext bundle id"
    );
    assert_ne!(token, TEST_BUNDLE_ID.as_bytes().to_vec());
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.locks_registered'"
        )
        .await,
        1
    );
}

// The registration guards: only the buyer, only the right aggregate and
// revision, only a payment awaiting entitlement, and only a lock resource
// created by the order's seller.
#[sqlx::test]
async fn registration_enforces_participant_state_and_creator_guards(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let outsider = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let resource = lock_resource_for(&seller.pubky);

    let command = register_locks_command(&order.payment_id, 1, TEST_BUNDLE_ID, &resource, 10);
    let (status, body) = execute(&app, &outsider.token, &command).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "only the buyer registers: {body}"
    );

    let missing = register_locks_command(
        &Uuid::new_v4().to_string(),
        1,
        TEST_BUNDLE_ID,
        &resource,
        11,
    );
    let (status, _) = execute(&app, &buyer.token, &missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let stale = register_locks_command(&order.payment_id, 7, TEST_BUNDLE_ID, &resource, 12);
    let (status, body) = execute(&app, &buyer.token, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));

    // A lock resource created by someone other than the order's seller is
    // refused: the correlation must bind the lifecycle to the seller.
    let foreign = register_locks_command(
        &order.payment_id,
        1,
        TEST_BUNDLE_ID,
        &lock_resource_for(&outsider.pubky),
        13,
    );
    let (status, body) = execute(&app, &buyer.token, &foreign).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // A payment no longer awaiting entitlement refuses registration.
    let (status, _) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "confirmed", 1, 14),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let confirmed = register_locks_command(&order.payment_id, 2, TEST_BUNDLE_ID, &resource, 15);
    let (status, body) = execute(&app, &buyer.token, &confirmed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// Replay discipline: exact replay returns the stored result; a changed
// payload under the same command id conflicts; a second registration for
// the same payment is refused; the same lifecycle identity can never
// correlate a second order (unique HMAC lookup token).
#[sqlx::test]
async fn registration_rejects_changed_replays_and_identity_reuse(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let resource = lock_resource_for(&seller.pubky);

    // Two units so the same buyer/seller pair can hold two orders.
    let mut listing = register_command(&seller.pubky, 2);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    let (status, _) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK);
    let (status, first_checkout) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Checkout moves no inventory, so the second checkout sees the listing
    // still at revision 1.
    let second = checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001");
    let (status, second_checkout) = execute(&app, &buyer.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{second_checkout}");
    let first_payment = first_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");
    let second_payment = second_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");

    for (payment_id, command_id) in [(first_payment, 18), (second_payment, 19)] {
        let prepare = json!({
            "version": 1,
            "command_id": common::indexed_command_id(0x8002, command_id),
            "aggregate_id": format!("payment:{payment_id}"),
            "expected_revision": 1,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "payment.prepare_locks",
            "payload": { "payment_id": payment_id },
        });
        let (status, body) = execute(&app, &buyer.token, &prepare).await;
        assert_eq!(status, StatusCode::OK, "preparation failed: {body}");
    }
    let command = register_locks_command(first_payment, 2, TEST_BUNDLE_ID, &resource, 20);
    let (status, original) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{original}");

    // Exact replay: the stored result, no re-execution.
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, original);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        2
    );

    // Changed replay under the same command id: idempotency conflict.
    let mut changed = command.clone();
    changed["payload"]["bundle_id"] = json!(OTHER_BUNDLE_ID);
    let (status, body) = execute(&app, &buyer.token, &changed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("IDEMPOTENCY_CONFLICT"));

    // A different registration for the already-correlated payment: refused.
    let second_registration =
        register_locks_command(first_payment, 3, OTHER_BUNDLE_ID, &resource, 21);
    let (status, body) = execute(&app, &buyer.token, &second_registration).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // The same {creator, bundle_id} identity on another order: rejected by
    // the unique lookup token, not application logic.
    let reused_identity = register_locks_command(second_payment, 2, TEST_BUNDLE_ID, &resource, 22);
    let (status, body) = execute(&app, &buyer.token, &reused_identity).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
}

// Once a payment is correlated to a real Locks lifecycle, the sandbox
// command — a client claim — can no longer advance it.
#[sqlx::test]
async fn sandbox_advance_is_refused_for_a_locks_correlated_payment(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    for target in ["detected", "confirmed", "expired", "manual_review"] {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &payment_command(&order.payment_id, 3, target, 1, 30),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{target}: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    }
    let (state, adapter, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        (state.as_str(), adapter.as_str()),
        ("awaiting_entitlement", "locks")
    );
}

// Without the Locks runtime (sandbox-only deployment) the registration
// command is refused outright: fail closed, no correlation is stored.
#[sqlx::test]
async fn registration_is_refused_when_locks_is_not_configured(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            40,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        0
    );
}

// The worker independently verifies a completed lifecycle and applies the
// full confirmation exactly once: payment confirmed, order paid, receipt
// issued, inventory converted, seller notified. Repeats and simulated
// redeliveries are harmless — the unique payment-confirmed event index and
// the payment-state compare-and-swap decide, not application logic.
#[sqlx::test]
async fn worker_confirms_a_verified_completion_exactly_once(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );

    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    assert_eq!(
        fake.lookups(),
        vec![(seller.pubky.clone(), TEST_BUNDLE_ID.to_string())],
        "the service queried the lifecycle for exactly the registered identity"
    );

    let (state, adapter, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!((state.as_str(), adapter.as_str()), ("confirmed", "locks"));
    assert_eq!(order_state(&app.pool, &order.order_id).await, "paid");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.payment_confirmed'"
        )
        .await,
        1
    );
    let (sold, reserved): (i64, i64) =
        sqlx::query_as("SELECT sold_quantity, reserved_quantity FROM listings")
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!((sold, reserved), (1, 0), "the held unit converted to sold");
    let (verification_state, completed_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT verification_state, completed_at FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(verification_state, "completed");
    assert!(completed_at.is_some());

    // A later pass has nothing to verify: the correlation is terminal.
    app.clock.advance_seconds(60);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 0);
    assert_eq!(
        fake.lookup_count(),
        1,
        "terminal correlations are not re-polled"
    );

    // A duplicate/reordered completion delivery (simulated by resetting the
    // correlation claim, as a crash between lookup and effect would leave
    // it) cannot repeat any effect.
    sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = 'pending', \
         last_checked_at = NULL, completed_at = NULL",
    )
    .execute(&app.pool)
    .await
    .expect("reset runs");
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 0,
        "duplicate completion is harmless"
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1,
        "still exactly one payment-confirmed event"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "confirmed");
}

// Pending, in-progress, not-found, and unavailable lookups leave the
// payment untouched and the correlation pending; polling is rate-limited by
// the poll interval and history rows are appended only on status change.
#[sqlx::test]
async fn non_terminal_lifecycles_stay_pending_and_poll_boundedly(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    // Not yet submitted upstream: not_found, still pending.
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    // Within the poll interval the correlation is not re-claimed.
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(fake.lookup_count(), 1, "polling respects the poll interval");

    for (advance, outcome, expected_status) in [
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::Pending),
            "pending",
        ),
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::Pending),
            "pending",
        ),
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::InProgress),
            "in_progress",
        ),
        (31, LocksLookupOutcome::Unavailable, "in_progress"),
    ] {
        app.clock.advance_seconds(advance);
        fake.set_outcome(TEST_BUNDLE_ID, outcome);
        let summary = run_once(&app.state, holder, app.clock.now())
            .await
            .expect("worker pass runs");
        assert_eq!(summary.locks_completions_applied, 0);
        let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
        assert_eq!(state, "awaiting_entitlement");
        let (verification_state, last_observed): (String, Option<String>) = sqlx::query_as(
            "SELECT verification_state, last_observed_status FROM payment_locks_correlations",
        )
        .fetch_one(&app.pool)
        .await
        .expect("correlation row exists");
        assert_eq!(verification_state, "pending");
        assert_eq!(last_observed.as_deref(), Some(expected_status));
    }
    // History: not_found, pending, in_progress — one row per change, none
    // for the repeat or the transport failure.
    let history: Vec<(String, String)> = sqlx::query_as(
        "SELECT observed_status, outcome FROM payment_locks_observations ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("observations listed");
    assert_eq!(
        history,
        vec![
            ("not_found".to_string(), "none".to_string()),
            ("pending".to_string(), "none".to_string()),
            ("in_progress".to_string(), "none".to_string()),
        ]
    );
}

// An upstream terminal failure is recorded and stops polling, but is NOT a
// marketplace expiry: the payment stays awaiting entitlement until the
// marketplace's own payment window elapses (ADR-0019 §7).
#[sqlx::test]
async fn upstream_failure_is_separate_from_marketplace_expiry(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Failed),
    );

    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 0);
    assert_eq!(summary.payment_windows_expired, 0);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        state, "awaiting_entitlement",
        "an upstream failure must not expire the payment"
    );
    let (verification_state,): (String,) =
        sqlx::query_as("SELECT verification_state FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(verification_state, "upstream_failed");

    // Terminal upstream state stops polling.
    app.clock.advance_seconds(60);
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(fake.lookup_count(), 1);

    // The marketplace window is what expires the payment, on its own clock.
    app.clock.advance_seconds(3_600);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.expired'"
        )
        .await,
        1
    );
}

// The marketplace payment window expires a still-pending payment; a
// completion verified after that expiry moves the payment to manual review
// with its history retained — never a confirmation, never dropped.
#[sqlx::test]
async fn late_completion_after_window_expiry_goes_to_manual_review(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Pending),
    );
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");

    // The window (3600 s) elapses while the lifecycle is still pending: the
    // hold releases back to the listing, the payment expires, and the ORDER
    // is cancelled with the stored server reason — the buyer simply checks
    // out again.
    app.clock.advance_seconds(3_601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, revision) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
    assert_eq!(revision, 4);
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");
    let (reason, stock_held): (Option<String>, bool) =
        sqlx::query_as("SELECT cancellation_reason, stock_held FROM orders WHERE id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    assert_eq!(reason.as_deref(), Some("payment window elapsed"));
    assert!(!stock_held);
    let (available, reserved): (i64, i64) =
        sqlx::query_as("SELECT available_quantity, reserved_quantity FROM listings")
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!((available, reserved), (1, 0), "the lapsed hold restocked");
    let (verification_state,): (String,) =
        sqlx::query_as("SELECT verification_state FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(
        verification_state, "pending",
        "an expired window keeps polling so a late completion still surfaces"
    );

    // The completion arrives late: manual review, retained, no receipt.
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "manual_review");
    assert_eq!(
        order_state(&app.pool, &order.order_id).await,
        "cancelled",
        "the window sweep already cancelled the order"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 0);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.manual_review'"
        )
        .await,
        1
    );
    let history: Vec<(String, String)> = sqlx::query_as(
        "SELECT observed_status, outcome FROM payment_locks_observations ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("observations listed");
    assert_eq!(
        history,
        vec![
            ("pending".to_string(), "none".to_string()),
            ("window_elapsed".to_string(), "payment_expired".to_string()),
            ("completed".to_string(), "manual_review".to_string()),
        ],
        "the late completion and its reconciliation history are retained"
    );
    // The history is append-only by trigger.
    sqlx::query("UPDATE payment_locks_observations SET outcome = 'forged'")
        .execute(&app.pool)
        .await
        .expect_err("observations are append-only");
}

// A verified completion whose order can no longer be confirmed (the buyer
// cancelled while the lifecycle was pending) is retained under manual
// review instead of confirming a dead order — and instead of vanishing.
#[sqlx::test]
async fn completion_that_cannot_confirm_the_order_goes_to_manual_review(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    let cancel = common::order_command(
        "order.cancel_request",
        &order.order_id,
        1,
        json!({ "reason": "Changed my mind" }),
        50,
    );
    let (status, body) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");

    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "manual_review");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 0);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        0,
        "a dead order is never confirmed"
    );
}

// The verification task participates in the lease discipline like every
// other worker task: an instance that does not hold the lease skips it, and
// the task is recovered after the lease lapses.
#[sqlx::test]
async fn locks_verification_respects_worker_leases(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );

    let now = app.clock.now();
    assert!(try_acquire_lease(
        &app.pool,
        workers::TASK_LOCKS_VERIFICATION,
        instance_a,
        now,
        30
    )
    .await
    .expect("lease query runs"));

    let summary = run_once(&app.state, instance_b, now)
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 0,
        "excluded instance skips the task"
    );
    assert_eq!(fake.lookup_count(), 0);

    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, instance_b, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 1,
        "the lapsed lease is recovered"
    );
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "confirmed");
}

// Redaction (ADR-0019 §8): the bundle id and the expected lock resource
// appear in no command result, read projection, event, outbox intent, or
// notification — across the whole checkout-prepare-register-confirm flow.
// The resource sentinel is the DYNAMICALLY derived seller-authored lock
// resource planted at checkout, not a fixed constant.
#[sqlx::test]
async fn bundle_and_lock_resource_never_leave_the_correlation_store(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let expected_resource = lock_resource_for(&seller.pubky);
    let registration = register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");

    let assert_redacted = |surface: &str, serialized: &str| {
        assert!(
            !serialized.contains(TEST_BUNDLE_ID),
            "{surface} leaks the bundle id"
        );
        assert!(
            !serialized.contains(&expected_resource),
            "{surface} leaks the expected lock resource planted at checkout"
        );
        assert!(
            !serialized.contains(TEST_LOCK_ID),
            "{surface} leaks the lock resource"
        );
        assert!(
            !serialized.contains("/pub/locks.app/"),
            "{surface} leaks a content-lock path"
        );
        assert!(
            !serialized.contains("locks_bundle_id"),
            "{surface} reintroduces the removed bundle field"
        );
    };

    // The registration command result the buyer received.
    assert_redacted("registration result", &registration.to_string());

    // Every stored command result (idempotent replays serve these bytes).
    let stored_results: Vec<(Value,)> = sqlx::query_as("SELECT result FROM command_results")
        .fetch_all(&app.pool)
        .await
        .expect("command results listed");
    for (result,) in &stored_results {
        assert_redacted("stored command result", &result.to_string());
    }

    // Read projections, as each participant sees them.
    for actor in [&buyer, &seller] {
        let (status, payment) = send(
            app.router.clone(),
            "GET",
            &format!("/v1/payments/{}", order.payment_id),
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("payment projection", &payment.to_string());
        let (status, order_view) = send(
            app.router.clone(),
            "GET",
            &format!("/v1/orders/{}", order.order_id),
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("order projection", &order_view.to_string());
        let (status, notifications) = send(
            app.router.clone(),
            "GET",
            "/v1/notifications",
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("notifications projection", &notifications.to_string());
    }

    // Durable side-channel surfaces: the stored order lines, events, outbox
    // intents, notifications, and the observation history hold statuses and
    // ids only.
    for (surface, sql) in [
        (
            "order lines at rest",
            "SELECT COALESCE(string_agg(lines::text, ','), '') FROM orders",
        ),
        (
            "events",
            "SELECT COALESCE(string_agg(kind, ','), '') FROM events",
        ),
        (
            "outbox",
            "SELECT COALESCE(string_agg(payload::text, ','), '') FROM outbox",
        ),
        (
            "notifications",
            "SELECT COALESCE(string_agg(type || aggregate_id, ','), '') FROM notifications",
        ),
        (
            "observations",
            "SELECT COALESCE(string_agg(observed_status || outcome, ','), '') \
             FROM payment_locks_observations",
        ),
    ] {
        let (serialized,): (String,) = sqlx::query_as(sql)
            .fetch_one(&app.pool)
            .await
            .expect("surface query runs");
        assert_redacted(surface, &serialized);
    }

    // At rest, the correlation row holds the bundle id only as ciphertext.
    let (ciphertext, token, hash): (Vec<u8>, Vec<u8>, String) = sqlx::query_as(
        "SELECT bundle_id_ciphertext, bundle_lookup_token, lock_resource_hash \
         FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert!(!ciphertext
        .windows(TEST_BUNDLE_ID.len())
        .any(|window| window == TEST_BUNDLE_ID.as_bytes()));
    assert_ne!(token, TEST_BUNDLE_ID.as_bytes().to_vec());
    assert_redacted("lock resource hash", &hash);

    // At rest, the immutable checkout-time snapshot — planted dynamically
    // with the SAME seller-authored resource the sentinel scans for — holds
    // that resource only as ciphertext plus its hash.
    let (snapshot_ciphertext, snapshot_hash): (Vec<u8>, String) = sqlx::query_as(
        "SELECT expected_resource_ciphertext, expected_resource_hash \
         FROM payment_locks_checkout_snapshots",
    )
    .fetch_one(&app.pool)
    .await
    .expect("checkout snapshot row exists");
    assert!(
        !snapshot_ciphertext
            .windows(expected_resource.len())
            .any(|window| window == expected_resource.as_bytes()),
        "the checkout snapshot never stores the resource in plaintext"
    );
    assert_redacted("checkout snapshot hash", &snapshot_hash);
}

// Exactly one seller-authored payment lock per order: a cart with zero
// Locks policies is refused at prepare with the static copy, and so is a
// cart whose lines carry two DISTINCT locks (multi-lock aggregation is a
// future design, never an implicit pick).
#[sqlx::test]
async fn prepare_refuses_orders_with_zero_or_multiple_distinct_locks(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // Zero locks: a plain listing checkout carries no payment lock.
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000002000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(payment_id, 60),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The order has no seller-authored Locks payment lock snapshot.")
    );

    // Multiple distinct locks: two lines from the same seller carrying two
    // different policies collapse to one order, which prepare refuses.
    let mut first = register_listing_command(&seller.pubky, "boots_02", 1, 61);
    first["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for_payment(&seller.pubky, 13_700, "USD"),
        "criterionId": "paykit",
    });
    let (status, body) = execute(&app, &seller.token, &first).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut second = register_listing_command(&seller.pubky, "boots_03", 1, 63);
    second["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for_payment(&seller.pubky, 12_500, "USD"),
        "criterionId": "paykit",
    });
    let (status, body) = execute(&app, &seller.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let checkout = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000002001",
        "aggregate_id": "checkout:00000000-0000-4000-8000-000000002001",
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "checkout.create",
        "payload": {
            "lines": [
                {
                    "listing_aggregate_id": format!("listing:{}_boots_02", seller.pubky),
                    "expected_revision": 1,
                    "quantity": 1,
                },
                {
                    "listing_aggregate_id": format!("listing:{}_boots_03", seller.pubky),
                    "expected_revision": 1,
                    "quantity": 1,
                },
            ],
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US",
            },
            "guarantee_policy_version": 1,
        },
    });
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["orders"].as_array().expect("orders").len(),
        1,
        "same seller and fulfillment split into one order"
    );
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(payment_id, 62),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The order has no seller-authored Locks payment lock snapshot.")
    );
}

// Identity validation through the handler (not only the pure function): a
// document served at the seller-authoritative path whose bytes no longer
// hash to that path is refused at prepare, and no correlation is created.
#[sqlx::test]
async fn prepare_refuses_a_lock_document_with_changed_bytes(pool: PgPool) {
    let seller_key = common::random_keypair();
    let seller_pubky = seller_key.1.clone();
    let mut tampered = lock_document_for(&seller_pubky, 13_700, "USD");
    // One flipped byte: the amount changes, so the canonical bytes no longer
    // derive the advertised lock id (a path mismatch, fail closed).
    tampered["criteria"][0]["params"]["amount"] = json!("13701");
    let resource = lock_resource_for(&seller_pubky);
    let path = resource
        .strip_prefix(&seller_pubky)
        .expect("creator prefixes resource")
        .to_string();
    let app = test_app_with_lock_documents(
        pool,
        HashMap::from([((seller_pubky.clone(), path), tampered)]),
    )
    .await;
    let seller = TestActor {
        token: common::authenticate(&app, &seller_key.0).await,
        keypair: seller_key.0,
        pubky: seller_pubky,
    };
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 70),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The seller's Locks document does not match the payment.")
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        0,
        "a path mismatch never creates a correlation"
    );
}

// Paykit v1 policy refusals through the handler: a document carrying an
// unknown criterion param (`exponent`) or a lock logic that does not name
// exactly the sole criterion is refused at prepare even though it is served
// at its OWN content-addressed path — upstream rejects both
// (`PaykitPaymentPolicyValidationError::InvalidParams(UnknownField)` /
// `InvalidLockLogic`), and so does the mirror.
#[sqlx::test]
async fn prepare_refuses_policy_invalid_documents_at_their_own_path(pool: PgPool) {
    for (case, mutate) in [
        (
            "unknown paykit param",
            Box::new(|document: &mut Value| {
                document["criteria"][0]["params"]["exponent"] = json!("2");
            }) as Box<dyn Fn(&mut Value)>,
        ),
        (
            "criterion not the sole lock-logic member",
            Box::new(|document: &mut Value| {
                document["lock_logic"] = json!({"type": "all", "criteria": ["paykit", "paykit"]});
            }) as Box<dyn Fn(&mut Value)>,
        ),
    ] {
        let seller_key = common::random_keypair();
        let seller_pubky = seller_key.1.clone();
        let mut document = lock_document_for(&seller_pubky, 13_700, "USD");
        mutate(&mut document);
        let typed: marketplace_service::content_lock::ContentLock =
            serde_json::from_value(document.clone()).expect("document decodes");
        let lock_id = typed.lock_id().expect("typed lock id");
        let resource = format!("{seller_pubky}/pub/locks.app/{lock_id}.json");
        let path = format!("/pub/locks.app/{lock_id}.json");
        let app = test_app_with_lock_documents(
            pool.clone(),
            HashMap::from([((seller_pubky.clone(), path), document)]),
        )
        .await;
        let seller = TestActor {
            token: common::authenticate(&app, &seller_key.0).await,
            keypair: seller_key.0,
            pubky: seller_pubky,
        };
        let buyer = new_actor(&app).await;
        let mut listing = register_command(&seller.pubky, 1);
        listing["payload"]["digital_lock"] = json!({
            "policyUri": resource,
            "criterionId": "paykit",
        });
        let (status, body) = execute(&app, &seller.token, &listing).await;
        assert_eq!(status, StatusCode::OK, "{case} listing: {body}");
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000003000"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{case} checkout: {body}");
        let payment_id = body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id");
        let (status, body) = execute(
            &app,
            &buyer.token,
            &common::prepare_locks_command(payment_id, 72),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{case}: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{case}");
        assert_eq!(
            body["error"]["message"],
            json!("The seller's Locks document does not match the payment."),
            "{case}"
        );
        assert_eq!(
            count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
            0,
            "{case}: a policy-invalid document never creates a correlation"
        );
    }
}

// The immutable checkout-time lock snapshot (Sol Wave 1A review round 2,
// P1-4): a post-checkout mutation of the mutable listing lock columns —
// here an equal-revision `listing.sync` healing lock A to lock B, which is
// expressly permitted — must NOT move an existing order's authority.
// Prepare seals the checkout-time snapshot (A), never the healed row (B).
#[sqlx::test]
async fn prepare_seals_the_checkout_snapshot_not_a_post_checkout_lock_mutation(pool: PgPool) {
    let homeserver = Arc::new(SyncableLocksHomeserver::default());
    let app = test_app_with_homeserver(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let lock_a = lock_resource_for(&seller.pubky);
    let lock_b = lock_resource_for_payment(&seller.pubky, 12_500, "USD");
    assert_ne!(lock_a, lock_b);
    let mut listing = register_command(&seller.pubky, 1);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_a,
        "criterionId": "paykit",
    });
    let (status, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000005000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();

    // The checkout transaction planted the immutable snapshot of lock A.
    let (snapshot_hash,): (String,) = sqlx::query_as(
        "SELECT expected_resource_hash FROM payment_locks_checkout_snapshots \
         WHERE payment_id = $1::uuid",
    )
    .bind(&payment_id)
    .fetch_one(&app.pool)
    .await
    .expect("checkout snapshot row exists");
    assert_eq!(
        snapshot_hash,
        blake3::hash(lock_a.as_bytes()).to_hex().to_string()
    );

    // The interleaving from the review: an equal-revision sync heals the
    // mutable listing rows from lock A to lock B AFTER checkout.
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        listing_record_with_lock(1, &lock_b),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 510),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "healing sync: {body}");
    let (current,): (Option<String>,) =
        sqlx::query_as("SELECT digital_lock_policy_uri FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!(
        current.as_deref(),
        Some(lock_b.as_str()),
        "the mutable listing row really moved to lock B"
    );

    // Prepare seals the checkout snapshot (A), never the healed row (B).
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&payment_id, 511),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prepare: {body}");
    let (hash, ciphertext): (String, Vec<u8>) = sqlx::query_as(
        "SELECT expected_resource_hash, expected_resource_ciphertext \
         FROM payment_locks_correlations WHERE payment_id = $1::uuid",
    )
    .bind(&payment_id)
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert_eq!(
        hash,
        blake3::hash(lock_a.as_bytes()).to_hex().to_string(),
        "the correlation seals the checkout-time lock A, not the healed B"
    );
    let opened = common::test_locks_keys().open_prepared_value(
        Uuid::parse_str(&payment_id).expect("payment id is a uuid"),
        b"locks-resource:",
        &ciphertext,
    );
    assert_eq!(opened.as_deref(), Some(lock_a.as_str()));
}

// Legacy orders — and carts whose checkout saw zero or multiple distinct
// locks — carry no snapshot row: prepare refuses them statically rather
// than falling back to the mutable listing rows.
#[sqlx::test]
async fn prepare_refuses_a_payment_without_a_checkout_snapshot(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    // Simulate a legacy order: its checkout predates the snapshot row.
    sqlx::query("DELETE FROM payment_locks_checkout_snapshots WHERE payment_id = $1::uuid")
        .bind(&order.payment_id)
        .execute(&app.pool)
        .await
        .expect("snapshot row deleted");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 520),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The order has no seller-authored Locks payment lock snapshot.")
    );
    assert_eq!(
        binding_outcomes(&app.pool, &order.payment_id).await,
        vec!["refused_identity".to_string()]
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        0,
        "a snapshot-less payment never creates a correlation"
    );
}

// The creator swap: a document naming a different creator served at the
// seller's path is an identity mismatch and is refused at prepare.
#[sqlx::test]
async fn prepare_refuses_a_lock_document_with_a_swapped_creator(pool: PgPool) {
    let seller_key = common::random_keypair();
    let seller_pubky = seller_key.1.clone();
    let (_, other_pubky) = common::random_keypair();
    let mut swapped = lock_document_for(&seller_pubky, 13_700, "USD");
    swapped["creator"] = json!(other_pubky);
    let resource = lock_resource_for(&seller_pubky);
    let path = resource
        .strip_prefix(&seller_pubky)
        .expect("creator prefixes resource")
        .to_string();
    let app = test_app_with_lock_documents(
        pool,
        HashMap::from([((seller_pubky.clone(), path), swapped)]),
    )
    .await;
    let seller = TestActor {
        token: common::authenticate(&app, &seller_key.0).await,
        keypair: seller_key.0,
        pubky: seller_pubky,
    };
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 71),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The seller's Locks document does not match the payment.")
    );
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        0,
        "a creator mismatch never creates a correlation"
    );
}
