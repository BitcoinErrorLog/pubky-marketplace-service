//! Phase 6 Wave 1 inventory contract proofs. Every request drives the real
//! Axum router, genuine `pubky-common` AuthToken verification, the persisted
//! bearer session, middleware, handler boundary, and PostgreSQL transaction.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;
use chrono::Utc;
use common::{
    execute, indexed_command_id, listing_aggregate, new_actor, random_keypair, register_command,
    send, send_bytes, send_with_headers, test_app, test_app_with_homeserver,
    test_app_with_homeserver_client, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::homeserver::HttpHomeserverClient;
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::Capability;
use pubky_common::crypto::Keypair;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const INVENTORY_SCOPE: &str = "/pub/pubky.app/marketplace-service/v1/";

fn adjust_request(
    seller_pubky: &str,
    expected_revision: i64,
    delta: i64,
    idempotency_key: Uuid,
) -> Value {
    json!({
        "schema_version": 1,
        "kind": "inventory.adjust",
        "aggregate_id": listing_aggregate(seller_pubky),
        "listing_id": "boots_01",
        "expected_revision": expected_revision,
        "delta": delta,
        "idempotency_key": idempotency_key,
    })
}

async fn adjust(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(token),
        body,
    )
    .await
}

async fn projection(app: &TestApp, token: Option<&str>, aggregate_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/inventory/listings/{aggregate_id}"),
        token,
        &Value::Null,
    )
    .await
}

async fn register(app: &TestApp, seller: &TestActor, quantity: i64) {
    let (status, body) = execute(
        app,
        &seller.token,
        &register_command(&seller.pubky, quantity),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "listing registration: {body}");
}

async fn post_capability_token(
    app: &TestApp,
    keypair: &Keypair,
    capabilities: Vec<Capability>,
) -> (StatusCode, Value) {
    let fixture_now = app.clock.now();
    app.clock.set(Utc::now());
    let result = send_bytes(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        AuthToken::sign(keypair, capabilities).serialize(),
    )
    .await;
    app.clock.set(fixture_now);
    result
}

async fn actor_with_capabilities(app: &TestApp, capabilities: Vec<Capability>) -> TestActor {
    let (keypair, pubky) = random_keypair();
    let (status, body) = post_capability_token(app, &keypair, capabilities).await;
    assert_eq!(status, StatusCode::CREATED, "session creation: {body}");
    TestActor {
        keypair,
        pubky,
        token: body["token"].as_str().expect("bearer token").to_string(),
    }
}

async fn listing_facts(pool: &PgPool, aggregate_id: &str) -> (i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT server_revision, total_quantity, available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("listing facts")
}

async fn inventory_event_count(pool: &PgPool, aggregate_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = 'inventory.adjusted'",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("event count")
}

#[sqlx::test(migrations = "./migrations")]
async fn genuine_auth_path_enforces_semantic_inventory_capability(pool: PgPool) {
    let app = test_app(pool).await;

    let exact = actor_with_capabilities(
        &app,
        vec![Capability::read_write(INVENTORY_SCOPE).expect("exact scope")],
    )
    .await;
    let broader = actor_with_capabilities(&app, vec![Capability::root()]).await;
    let readonly = actor_with_capabilities(
        &app,
        vec![Capability::read(INVENTORY_SCOPE).expect("read scope")],
    )
    .await;
    let writeonly = actor_with_capabilities(
        &app,
        vec![Capability::write(INVENTORY_SCOPE).expect("write scope")],
    )
    .await;
    let unrelated = actor_with_capabilities(
        &app,
        vec![Capability::read_write("/pub/pubky.app/marketplace/").expect("unrelated scope")],
    )
    .await;

    register(&app, &exact, 3).await;
    let aggregate = listing_aggregate(&exact.pubky);

    for actor in [&exact, &broader] {
        let (status, body) = projection(&app, Some(&actor.token), &aggregate).await;
        assert_eq!(status, StatusCode::OK, "covered grant rejected: {body}");
    }
    for actor in [&readonly, &writeonly, &unrelated] {
        let (status, body) = projection(&app, Some(&actor.token), &aggregate).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "insufficient grant accepted: {body}"
        );
        assert_eq!(body["error"]["code"], json!("capability_required"));
    }
    let (status, _) = projection(&app, None, &aggregate).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let stored: String =
        sqlx::query_scalar("SELECT capabilities FROM auth_sessions WHERE pubky = $1")
            .bind(&exact.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("stored canonical grant");
    assert_eq!(stored, "/pub/pubky.app/marketplace-service/v1/:rw");

    let key = Uuid::new_v4();
    let (status, body) = adjust(&app, &exact.token, &adjust_request(&exact.pubky, 1, 1, key)).await;
    assert_eq!(status, StatusCode::OK, "exact scoped adjust: {body}");

    let (status, body) = adjust(
        &app,
        &broader.token,
        &adjust_request(&exact.pubky, 2, 1, Uuid::new_v4()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "foreign broad actor must not own the listing: {body}"
    );
    assert_eq!(body["error"]["code"], json!("seller_ownership_required"));
}

#[sqlx::test(migrations = "./migrations")]
async fn exact_replay_returns_original_result_before_rate_accounting(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 5).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let request = adjust_request(&seller.pubky, 1, 2, Uuid::new_v4());

    let (status, first) = adjust(&app, &seller.token, &request).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["result"]["server_revision"], json!(2));
    assert_eq!(first["result"]["stock"]["total"], json!(7));
    assert_eq!(first["result"]["stock"]["available"], json!(7));
    let before_tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_rate_limits WHERE endpoint_class = 'inventory.adjust'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("rate row");

    let (status, replay) = adjust(&app, &seller.token, &request).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(
        replay, first,
        "replay returns the immutable original result"
    );
    let after_tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_rate_limits WHERE endpoint_class = 'inventory.adjust'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("rate row");

    assert_eq!(after_tokens, before_tokens, "replay consumes no rate token");
    assert_eq!(listing_facts(&app.pool, &aggregate).await, (2, 7, 7, 0, 0));
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
        .fetch_one(&app.pool)
        .await
        .expect("result count");
    assert_eq!(results, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn changed_body_replay_is_quarantined_without_mutation(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 5).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let key = Uuid::new_v4();
    let first = adjust_request(&seller.pubky, 1, 1, key);
    let changed = adjust_request(&seller.pubky, 1, 2, key);

    let (status, body) = adjust(&app, &seller.token, &first).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let facts = listing_facts(&app.pool, &aggregate).await;
    let (status, body) = adjust(&app, &seller.token, &changed).await;

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("idempotency_conflict"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, facts);
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);
    let conflicts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_conflicts")
        .fetch_one(&app.pool)
        .await
        .expect("quarantine count");
    assert_eq!(conflicts, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn exhausted_rate_bucket_cannot_delay_or_suppress_changed_replay_quarantine(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 5).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let key = Uuid::new_v4();
    let original = adjust_request(&seller.pubky, 1, 1, key);

    let (status, body) = adjust(&app, &seller.token, &original).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The default burst has 240 tokens. The accepted request consumed one;
    // these distinct stale requests consume the remaining 239 without
    // changing listing, event, or immutable-result facts.
    for index in 0..239 {
        let stale = adjust_request(
            &seller.pubky,
            1,
            1,
            Uuid::parse_str(&indexed_command_id(0x9020, index)).expect("fixture uuid"),
        );
        let (status, body) = adjust(&app, &seller.token, &stale).await;
        assert_eq!(status, StatusCode::CONFLICT, "index {index}: {body}");
        assert_eq!(body["error"]["code"], json!("revision_conflict"));
    }
    let tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_rate_limits WHERE endpoint_class = 'inventory.adjust'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("exhausted rate row");
    assert!(tokens < 1.0, "rate bucket must be exhausted, got {tokens}");

    let facts = listing_facts(&app.pool, &aggregate).await;
    let events = inventory_event_count(&app.pool, &aggregate).await;
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
        .fetch_one(&app.pool)
        .await
        .expect("result count");

    // Holding the exhausted rate row makes any attempted rate accounting
    // wait. A changed replay must classify and persist under its idempotency
    // lock without depending on that operational bucket.
    let mut rate_lock = app.pool.begin().await.expect("rate lock transaction");
    sqlx::query(
        "SELECT tokens FROM inventory_rate_limits \
         WHERE endpoint_class = 'inventory.adjust' FOR UPDATE",
    )
    .fetch_one(&mut *rate_lock)
    .await
    .expect("rate row locks");

    let mut changed = adjust_request(&seller.pubky, 1, 2, key);
    changed["external_ref"] = json!({"channel": "shopify", "external_id": "changed-replay"});
    let router = app.router.clone();
    let token = seller.token.clone();
    let changed_for_task = changed.clone();
    let mut changed_task = tokio::spawn(async move {
        send(
            router,
            "POST",
            "/v1/inventory/adjust",
            Some(&token),
            &changed_for_task,
        )
        .await
    });
    let completed = tokio::time::timeout(Duration::from_secs(2), &mut changed_task).await;
    rate_lock.rollback().await.expect("release rate row");
    let (status, body) = match completed {
        Ok(joined) => joined.expect("changed replay task"),
        Err(_) => {
            let _ = tokio::time::timeout(Duration::from_secs(5), changed_task).await;
            panic!("changed replay waited on exhausted rate accounting before quarantine");
        }
    };

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("idempotency_conflict"));
    let (status, body) = adjust(&app, &seller.token, &changed).await;
    assert_eq!(status, StatusCode::CONFLICT, "deduplicated replay: {body}");
    assert_eq!(body["error"]["code"], json!("idempotency_conflict"));

    let conflicts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_adjustment_conflicts \
         WHERE seller_pubky = $1 AND idempotency_key = $2",
    )
    .bind(&seller.pubky)
    .bind(key)
    .fetch_one(&app.pool)
    .await
    .expect("quarantine count");
    assert_eq!(conflicts, 1, "same changed hash is deduplicated");
    assert_eq!(listing_facts(&app.pool, &aggregate).await, facts);
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, events);
    let after_results: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
            .fetch_one(&app.pool)
            .await
            .expect("result count");
    assert_eq!(after_results, results);
}

#[sqlx::test(migrations = "./migrations")]
async fn revision_negative_stock_and_spoof_refusals_mutate_nothing(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    register(&app, &seller, 2).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let initial = listing_facts(&app.pool, &aggregate).await;

    let (status, body) = adjust(
        &app,
        &seller.token,
        &adjust_request(&seller.pubky, 2, 1, Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("revision_conflict"));
    assert_eq!(body["error"]["current_revision"], json!(1));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, initial);

    let (status, body) = adjust(
        &app,
        &seller.token,
        &adjust_request(&seller.pubky, 1, -3, Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("negative_stock"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, initial);

    let (status, body) = adjust(
        &app,
        &other.token,
        &adjust_request(&seller.pubky, 1, 1, Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], json!("seller_ownership_required"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, initial);

    let mut spoofed = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    spoofed["seller_pubky"] = json!(other.pubky);
    let (status, body) = adjust(&app, &seller.token, &spoofed).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("invalid_request"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, initial);

    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 0);
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
        .fetch_one(&app.pool)
        .await
        .expect("result count");
    assert_eq!(results, 0);
}

fn homeserver_record(seller: &str, variants: Value) -> Value {
    json!({
        "schemaVersion": 1,
        "recordType": "listing",
        "ownerPubky": seller,
        "listingId": "boots_01",
        "revision": 1,
        "title": "Boots",
        "media": [{"id": "m1", "contentHash": "a".repeat(64)}],
        "variants": variants,
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 12500, "currency": "USD", "exponent": 2}
        },
        "shippingOptions": [{"pricing": "free"}]
    })
}

#[derive(Clone)]
struct DelayedListingServer {
    record: Arc<Mutex<Value>>,
    lookup_entered: Arc<tokio::sync::Barrier>,
    release_lookup: Arc<tokio::sync::Notify>,
    delay_next: Arc<AtomicBool>,
}

async fn serve_delayed_listing(
    axum::extract::State(server): axum::extract::State<DelayedListingServer>,
    axum::extract::Path(listing_id): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let seller = headers
        .get("pubky-host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let record = server.record.lock().expect("listing record lock").clone();
    if record["ownerPubky"] != json!(seller) || record["listingId"] != json!(listing_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if server.delay_next.swap(false, Ordering::SeqCst) {
        server.lookup_entered.wait().await;
        server.release_lookup.notified().await;
    }
    (StatusCode::OK, axum::Json(record)).into_response()
}

async fn spawn_delayed_listing_server(record: Value) -> (String, DelayedListingServer) {
    let server = DelayedListingServer {
        record: Arc::new(Mutex::new(record)),
        lookup_entered: Arc::new(tokio::sync::Barrier::new(2)),
        release_lookup: Arc::new(tokio::sync::Notify::new()),
        delay_next: Arc::new(AtomicBool::new(true)),
    };
    let router = Router::new()
        .route(
            "/pub/pubky.app/marketplace/v1/listings/{listing_id}",
            axum::routing::get(serve_delayed_listing),
        )
        .with_state(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("delayed homeserver binds");
    let address = listener.local_addr().expect("delayed homeserver address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("delayed homeserver serves");
    });
    (format!("http://{address}"), server)
}

#[sqlx::test(migrations = "./migrations")]
async fn sole_enabled_variant_is_an_assertion_and_multi_variant_is_refused(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);

    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        homeserver_record(
            &seller.pubky,
            json!([
                {"id": "v1", "sku": "SKU-1", "quantity": 3, "enabled": true},
                {"id": "disabled", "sku": "SKU-X", "quantity": 0, "enabled": false}
            ]),
        ),
    );
    let mut sole = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    sole["variant"] = json!({"id": "v1", "sku": "SKU-1"});
    let (status, body) = adjust(&app, &seller.token, &sole).await;
    assert_eq!(status, StatusCode::OK, "sole enabled variant: {body}");
    assert_eq!(body["result"]["stock"]["available"], json!(4));
    assert!(
        !serde_json::to_string(&body)
            .expect("response serializes")
            .contains("variant_available"),
        "service must not claim variant availability"
    );

    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        homeserver_record(
            &seller.pubky,
            json!([
                {"id": "v1", "sku": "SKU-1", "quantity": 2, "enabled": true},
                {"id": "v2", "sku": "SKU-2", "quantity": 2, "enabled": true}
            ]),
        ),
    );
    let mut multiple = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    multiple["variant"] = json!({"id": "v1"});
    let before = listing_facts(&app.pool, &aggregate).await;
    let (status, body) = adjust(&app, &seller.token, &multiple).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("variant_authority_unsupported")
    );
    assert_eq!(listing_facts(&app.pool, &aggregate).await, before);
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn delayed_variant_lookup_holds_no_transaction_and_revision_binding_refuses_mismatch(
    pool: PgPool,
) {
    let (keypair, seller_pubky) = random_keypair();
    let record = homeserver_record(
        &seller_pubky,
        json!([{"id": "v1", "sku": "SKU-1", "quantity": 3, "enabled": true}]),
    );
    let (base_url, homeserver) = spawn_delayed_listing_server(record).await;
    let client = Arc::new(HttpHomeserverClient::new(&base_url).expect("HTTP client builds"));
    let app = test_app_with_homeserver_client(pool, client).await;
    let (status, session) = post_capability_token(&app, &keypair, vec![Capability::root()]).await;
    assert_eq!(status, StatusCode::CREATED, "session creation: {session}");
    let token = session["token"].as_str().expect("bearer token").to_string();
    let (status, second_session) =
        post_capability_token(&app, &keypair, vec![Capability::root()]).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "second session creation: {second_session}"
    );
    let ordinary_token = second_session["token"]
        .as_str()
        .expect("second bearer token")
        .to_string();
    let seller = TestActor {
        keypair,
        pubky: seller_pubky,
        token,
    };
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);

    let mut delayed = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    delayed["variant"] = json!({"id": "v1", "sku": "SKU-1"});
    let router = app.router.clone();
    let token = seller.token.clone();
    let delayed_task = tokio::spawn(async move {
        send(
            router,
            "POST",
            "/v1/inventory/adjust",
            Some(&token),
            &delayed,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), homeserver.lookup_entered.wait())
        .await
        .expect("real HTTP lookup reaches delayed response barrier");

    // This ordinary adjustment uses a second genuine session for the same
    // seller, so it has an independent rate row but needs the same listing
    // row. It must finish while the real HttpHomeserverClient response is
    // still delayed, proving the lookup holds no listing lock or transaction.
    let ordinary = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    let ordinary_result = tokio::time::timeout(
        Duration::from_secs(2),
        adjust(&app, &ordinary_token, &ordinary),
    )
    .await;
    homeserver.release_lookup.notify_one();
    let (delayed_status, delayed_body) = tokio::time::timeout(Duration::from_secs(5), delayed_task)
        .await
        .expect("delayed variant request finishes")
        .expect("delayed variant task");
    let (status, body) =
        ordinary_result.expect("ordinary adjustment blocked by delayed variant lookup");
    assert_eq!(status, StatusCode::OK, "ordinary adjustment: {body}");
    assert_eq!(delayed_status, StatusCode::CONFLICT, "{delayed_body}");
    assert_eq!(
        delayed_body["error"]["code"],
        json!("revision_conflict"),
        "lookup assertion matched listing revision, but ordinary mutation advanced server revision"
    );
    assert_eq!(listing_facts(&app.pool, &aggregate).await, (2, 4, 4, 0, 0));
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);

    // The fetched listing-record revision is an assertion bound to the
    // locked service listing_revision. A newer remote record is not CAS
    // authority and must be refused without any mutation.
    homeserver.record.lock().expect("listing record lock")["revision"] = json!(2);
    let before = listing_facts(&app.pool, &aggregate).await;
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
        .fetch_one(&app.pool)
        .await
        .expect("result count");
    let mut mismatched = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    mismatched["variant"] = json!({"id": "v1"});
    let (status, body) = adjust(&app, &seller.token, &mismatched).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("variant_record_conflict"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, before);
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);
    let after_results: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
            .fetch_one(&app.pool)
            .await
            .expect("result count");
    assert_eq!(after_results, results);
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_same_revision_adjustments_have_one_winner(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 5).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let first = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    let second = adjust_request(&seller.pubky, 1, 2, Uuid::new_v4());

    let left = adjust(&app, &seller.token, &first);
    let right = adjust(&app, &seller.token, &second);
    let ((left_status, left_body), (right_status, right_body)) = tokio::join!(left, right);

    let statuses = [left_status, right_status];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::OK)
            .count(),
        1,
        "left={left_body}, right={right_body}"
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1,
        "left={left_body}, right={right_body}"
    );
    let loser = if left_status == StatusCode::CONFLICT {
        &left_body
    } else {
        &right_body
    };
    assert_eq!(loser["error"]["code"], json!("revision_conflict"));
    let facts = listing_facts(&app.pool, &aggregate).await;
    assert_eq!(facts.0, 2);
    assert!(matches!(facts.1, 6 | 7));
    assert_eq!(facts.1, facts.2 + facts.3 + facts.4);
    assert_eq!(inventory_event_count(&app.pool, &aggregate).await, 1);
    let results: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_adjustment_results")
        .fetch_one(&app.pool)
        .await
        .expect("result count");
    assert_eq!(results, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn external_references_are_bounded_private_and_seller_scoped(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let second_seller = new_actor(&app).await;
    register(&app, &seller, 2).await;
    register(&app, &second_seller, 2).await;
    let aggregate = listing_aggregate(&seller.pubky);

    let mut first = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    first["external_ref"] = json!({"channel": "shopify", "external_id": "evt-001"});
    let (status, body) = adjust(&app, &seller.token, &first).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let mut duplicate = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    duplicate["external_ref"] = json!({"channel": "shopify", "external_id": "evt-001"});
    let before = listing_facts(&app.pool, &aggregate).await;
    let (status, body) = adjust(&app, &seller.token, &duplicate).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("external_reference_conflict"));
    assert_eq!(listing_facts(&app.pool, &aggregate).await, before);

    let mut scoped = adjust_request(&second_seller.pubky, 1, 1, Uuid::new_v4());
    scoped["external_ref"] = json!({"channel": "shopify", "external_id": "evt-001"});
    let (status, body) = adjust(&app, &second_seller.token, &scoped).await;
    assert_eq!(status, StatusCode::OK, "seller-scoped reference: {body}");

    let mut digit_prefixed = adjust_request(&second_seller.pubky, 2, 1, Uuid::new_v4());
    digit_prefixed["external_ref"] = json!({"channel": "1shop", "external_id": "evt-002"});
    let (status, body) = adjust(&app, &second_seller.token, &digit_prefixed).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schema-valid digit-prefixed channel: {body}"
    );

    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_external_refs \
         WHERE channel = 'shopify' AND external_id = 'evt-001'",
    )
    .fetch_one(&app.pool)
    .await
    .expect("private refs");
    assert_eq!(rows, 2);

    let (_, projection) = projection(&app, Some(&second_seller.token), &aggregate).await;
    let wire = serde_json::to_string(&projection).expect("projection serializes");
    assert!(!wire.contains("external_ref"));
    assert!(!wire.contains("evt-001"));

    let mut oversized = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    oversized["external_ref"] = json!({"channel": "x".repeat(33), "external_id": "id"});
    let (status, body) = adjust(&app, &seller.token, &oversized).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("invalid_request"));

    let mut oversized_body = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    oversized_body["padding"] = json!("x".repeat(5_000));
    let (status, body) = adjust(&app, &seller.token, &oversized_body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("invalid_request"));
}

#[sqlx::test(migrations = "./migrations")]
async fn named_rate_limit_defaults_and_replay_bypass_are_enforced(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 2).await;
    let request = adjust_request(&seller.pubky, 1, 1, Uuid::new_v4());
    let (status, original) = adjust(&app, &seller.token, &request).await;
    assert_eq!(status, StatusCode::OK, "{original}");

    // Default sustained 120/minute and burst 2x gives an initial capacity
    // of 240. The accepted adjustment used one token; 239 stale, uniquely
    // keyed requests consume the remainder without mutating stock.
    for index in 0..239 {
        let stale = adjust_request(
            &seller.pubky,
            1,
            1,
            Uuid::parse_str(&indexed_command_id(0x9010, index)).expect("fixture uuid"),
        );
        let (status, body) = adjust(&app, &seller.token, &stale).await;
        assert_eq!(status, StatusCode::CONFLICT, "index {index}: {body}");
        assert_eq!(body["error"]["code"], json!("revision_conflict"));
    }

    let (status, replay) = adjust(&app, &seller.token, &request).await;
    assert_eq!(status, StatusCode::OK, "replay bypasses exhausted bucket");
    assert_eq!(replay, original);

    let next = adjust_request(&seller.pubky, 2, 1, Uuid::new_v4());
    let (status, headers, body) = send_with_headers(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(&seller.token),
        &next,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], json!("rate_limited"));
    assert_eq!(body["error"]["limit_class"], json!("inventory.adjust"));
    assert!(headers.get("retry-after").is_some());
}
