//! The unlimited digital stock cap (digital delivery design §2 "Stock"):
//! Shop stores Unlimited as a quantity of 1,000,000, so a listing that ships
//! or offers pickup cannot register it, on any path.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use common::*;
use marketplace_domain::commands::{UNLIMITED_STOCK_ON_PHYSICAL_LISTING, UNLIMITED_STOCK_QUANTITY};
use marketplace_service::config::Config;
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
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

fn locks_register(
    seller: &str,
    listing_id: &str,
    methods: Value,
    lock: &Value,
    index: u64,
) -> Value {
    let mut command = register(seller, listing_id, methods, UNLIMITED_STOCK_QUANTITY, index);
    command["payload"]["digital_lock"] = lock.clone();
    command
}

async fn send_command(app: &TestApp, token: &str, command: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some(token),
        command,
    )
    .await
}

// Sol round 4 P1: a digital-only Locks record exempts only the exact
// registration its derivation produces (shipping, its lock, its quantity and
// version). Any other command at the cap is refused.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_binds_a_locks_exemption_to_the_registration_its_record_derives(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);
    let other_lock = json!({"policyUri": lock_resource_for(&seller.pubky), "criterionId": "other"});
    let digital_only =
        |listing_id: &str| locks_record(&seller.pubky, listing_id, json!(["digital"]), &lock);

    let mut refused: Vec<(&str, Value, Value)> = Vec::new();
    // Service methods the derivation never produces for a digital-only Locks record.
    refused.push((
        "m_pickup",
        digital_only("m_pickup"),
        locks_register(&seller.pubky, "m_pickup", json!(["pickup"]), &lock, 500),
    ));
    refused.push((
        "m_ship_pickup",
        digital_only("m_ship_pickup"),
        locks_register(
            &seller.pubky,
            "m_ship_pickup",
            json!(["shipping", "pickup"]),
            &lock,
            501,
        ),
    ));
    // A lock the record does not carry.
    refused.push((
        "m_lock",
        digital_only("m_lock"),
        locks_register(
            &seller.pubky,
            "m_lock",
            json!(["shipping"]),
            &other_lock,
            502,
        ),
    ));
    // A quantity the record's variants do not sum to.
    let mut small = digital_only("m_quantity");
    small["variants"][0]["quantity"] = json!(5);
    refused.push((
        "m_quantity",
        small,
        locks_register(&seller.pubky, "m_quantity", json!(["shipping"]), &lock, 503),
    ));
    // A record version the command does not register.
    let mut newer = locks_register(&seller.pubky, "m_revision", json!(["shipping"]), &lock, 504);
    newer["payload"]["listing_revision"] = json!(2);
    refused.push(("m_revision", digital_only("m_revision"), newer));
    // A digital-only record with no lock at all.
    let mut unlocked = digital_only("m_unlocked");
    unlocked
        .as_object_mut()
        .expect("record object")
        .remove("digitalLock");
    refused.push((
        "m_unlocked",
        unlocked,
        locks_register(&seller.pubky, "m_unlocked", json!(["shipping"]), &lock, 505),
    ));
    // A malformed record that merely names digital.
    refused.push((
        "m_malformed",
        json!({"fulfillmentMethods": ["digital"]}),
        locks_register(
            &seller.pubky,
            "m_malformed",
            json!(["shipping"]),
            &lock,
            506,
        ),
    ));

    for (listing_id, record, command) in refused {
        put_command_mirror_record(&seller.pubky, listing_id, record);
        let (status, body) = send_command(&app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::CONFLICT, "{listing_id}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
            "{listing_id}: {body}"
        );
    }

    // The exact derivation registers.
    put_command_mirror_record(&seller.pubky, "m_exact", digital_only("m_exact"));
    let exact = locks_register(&seller.pubky, "m_exact", json!(["shipping"]), &lock, 507);
    let (status, body) = send_command(&app, &seller.token, &exact).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A homeserver double that counts listing fetches and, during each one,
/// tries the executor's advisory lock for the command under test from a
/// separate connection.
struct ProbeHomeserver {
    pool: PgPool,
    record: Mutex<Option<Value>>,
    lock_key: Mutex<Option<String>>,
    fetches: AtomicUsize,
    lock_free_during_fetch: Mutex<Vec<bool>>,
}

impl HomeserverListingClient for ProbeHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let key = self.lock_key.lock().expect("probe key").clone();
            if let Some(key) = key {
                let mut connection = self.pool.acquire().await.expect("probe connection");
                let free: bool =
                    sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 42))")
                        .bind(&key)
                        .fetch_one(&mut *connection)
                        .await
                        .expect("probe try lock");
                if free {
                    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 42))")
                        .bind(&key)
                        .execute(&mut *connection)
                        .await
                        .expect("probe unlock");
                }
                self.lock_free_during_fetch
                    .lock()
                    .expect("probe results")
                    .push(free);
            }
            match self.record.lock().expect("probe record").clone() {
                Some(record) => HomeserverFetchOutcome::Found(record),
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
}

// Sol round 4 P2: the record is read once per command, after the cheap
// checks, and outside the command's transaction and advisory lock.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_reads_the_record_once_after_cheap_checks_and_outside_the_command_lock(
    pool: PgPool,
) {
    let probe = Arc::new(ProbeHomeserver {
        pool: pool.clone(),
        record: Mutex::new(None),
        lock_key: Mutex::new(None),
        fetches: AtomicUsize::new(0),
        lock_free_during_fetch: Mutex::new(Vec::new()),
    });
    let app = test_app_with_homeserver_client(pool, probe.clone()).await;
    let seller = new_actor(&app).await;
    let outsider = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);
    *probe.record.lock().expect("probe record") = Some(locks_record(
        &seller.pubky,
        "probe_01",
        json!(["digital"]),
        &lock,
    ));

    let command = locks_register(&seller.pubky, "probe_01", json!(["shipping"]), &lock, 600);
    *probe.lock_key.lock().expect("probe key") = Some(format!(
        "{}:{}",
        seller.pubky,
        command["command_id"].as_str().expect("command id")
    ));
    let (status, body) = send_command(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        probe.fetches.load(Ordering::SeqCst),
        1,
        "one fetch per command"
    );
    assert_eq!(
        *probe.lock_free_during_fetch.lock().expect("probe results"),
        vec![true],
        "the command's advisory lock is not held during the fetch"
    );

    // Refused on cheap checks without reading the record.
    let mut wrong_aggregate =
        locks_register(&seller.pubky, "probe_02", json!(["shipping"]), &lock, 601);
    wrong_aggregate["aggregate_id"] = json!(format!("listing:{}_probe_other", seller.pubky));
    let (status, body) = send_command(&app, &seller.token, &wrong_aggregate).await;
    assert_eq!(
        body["error"]["code"],
        json!("INVALID_COMMAND"),
        "{status}: {body}"
    );
    let not_seller = locks_register(&seller.pubky, "probe_03", json!(["shipping"]), &lock, 602);
    let (status, body) = send_command(&app, &outsider.token, &not_seller).await;
    assert_eq!(
        body["error"]["code"],
        json!("UNAUTHORIZED"),
        "{status}: {body}"
    );
    assert_eq!(
        probe.fetches.load(Ordering::SeqCst),
        1,
        "no fetch before cheap checks"
    );

    // A Locks auction at the cap reads the record once for both its cap
    // decision and its auction authority.
    let mut auction = register_auction_command(&seller.pubky);
    auction["command_id"] = json!(indexed_command_id(0xca93, 1));
    auction["aggregate_id"] = json!(format!("listing:{}_probe_04", seller.pubky));
    auction["payload"]["listing_id"] = json!("probe_04");
    auction["payload"]["quantity"] = json!(UNLIMITED_STOCK_QUANTITY);
    auction["payload"]["digital_lock"] = lock.clone();
    *probe.record.lock().expect("probe record") = Some(locks_record(
        &seller.pubky,
        "probe_04",
        json!(["digital"]),
        &lock,
    ));
    *probe.lock_key.lock().expect("probe key") = None;
    let _ = send_command(&app, &seller.token, &auction).await;
    assert_eq!(
        probe.fetches.load(Ordering::SeqCst),
        2,
        "one fetch for the auction command"
    );
}
