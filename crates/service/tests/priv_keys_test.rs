//! Per-user `/priv` data keys: release authorization, custody at rest,
//! idempotent creation, the boot probe, rotation and the distinctness
//! assertion (priv-encryption-plan.md, Phase 1).

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Duration;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::digital::DigitalKeys;
use marketplace_service::http::build_router;
use marketplace_service::locks::{LocksKeys, LocksRuntime};
use marketplace_service::pickup::PickupKeys;
use marketplace_service::priv_keys::{
    self, assert_priv_key_sealing_coherent, data_key_aad, reseal_previous_key_batch,
    reseal_previous_key_batch_with_hook, PrivKeys, ResealHook,
};
use marketplace_service::workers::run_once;
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::Capability;
use pubky_common::crypto::Keypair;
use rand::RngCore;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const PRIV_KEY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PRIV_PREVIOUS_KEY: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const STRAY_KEY: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const ROUTE: &str = "/v1/me/priv-keys";

fn keys(current: &str, previous: Option<&str>) -> PrivKeys {
    PrivKeys::from_hex(current, previous).expect("priv keys parse")
}

async fn priv_app(pool: PgPool, priv_keys: Option<PrivKeys>) -> TestApp {
    let mut app = test_app(pool).await;
    app.state = app.state.clone().with_priv_keys(priv_keys.map(Arc::new));
    app.router = build_router(app.state.clone());
    app
}

async fn session_with(app: &TestApp, keypair: &Keypair, capabilities: Vec<Capability>) -> String {
    let fixture_now = app.clock.now();
    app.clock.set(chrono::Utc::now());
    let (status, body) = send_bytes(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        AuthToken::sign(keypair, capabilities).serialize(),
    )
    .await;
    app.clock.set(fixture_now);
    assert_eq!(status, StatusCode::CREATED, "session: {body}");
    body["token"].as_str().expect("token").to_string()
}

/// A session row carrying a stored grant string directly, for grant forms
/// the AuthToken path cannot produce (the empty grant of a bridged session).
async fn session_with_stored_grant(app: &TestApp, pubky: &str, capabilities: &str) -> String {
    let mut token = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token);
    let now = app.clock.now();
    sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(marketplace_service::auth::hash_token(&token))
    .bind(pubky)
    .bind(capabilities)
    .bind(now)
    .bind(now + Duration::days(1))
    .execute(&app.pool)
    .await
    .expect("stored session");
    URL_SAFE_NO_PAD.encode(token)
}

async fn get_keys(app: &TestApp, token: Option<&str>) -> (StatusCode, Value) {
    let (status, headers, body) =
        send_with_headers(app.router.clone(), "GET", ROUTE, token, &Value::Null).await;
    if token.is_some() {
        assert_eq!(
            headers.get("cache-control").and_then(|v| v.to_str().ok()),
            Some("no-store"),
            "every authenticated answer is no-store: {status} {body}"
        );
    }
    (status, body)
}

fn decoded_key(entry: &Value) -> Vec<u8> {
    URL_SAFE_NO_PAD
        .decode(entry["key"].as_str().expect("key string"))
        .expect("key is base64url")
}

async fn row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM user_priv_keys")
        .fetch_one(pool)
        .await
        .expect("row count")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn root_session_receives_one_stable_key_stored_only_sealed(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let (keypair, pubky) = random_keypair();
    let token = session_with(&app, &keypair, vec![Capability::root()]).await;

    let (status, first) = get_keys(&app, Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["schema_version"], json!(1));
    assert_eq!(first["owner"], json!(pubky));
    let entries = first["keys"].as_array().expect("keys");
    assert_eq!(entries.len(), 1);
    let key_id = entries[0]["key_id"].as_str().expect("key id").to_string();
    assert_eq!(first["current_key_id"], json!(key_id));
    assert_eq!(key_id.len(), 32);
    assert!(key_id
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    let key = decoded_key(&entries[0]);
    assert_eq!(key.len(), 32);
    assert_ne!(key, vec![0u8; 32]);

    let (status, again) = get_keys(&app, Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, first, "a second request returns the same key");

    // Another device: a fresh session for the same pubky gets the same key.
    let other_device = session_with(&app, &keypair, vec![Capability::root()]).await;
    let (_, from_other_device) = get_keys(&app, Some(&other_device)).await;
    assert_eq!(from_other_device["keys"], first["keys"]);

    let (owner, generation, stored_id, sealed): (String, i32, String, Vec<u8>) =
        sqlx::query_as("SELECT owner_pubky, generation, key_id, sealed_key FROM user_priv_keys")
            .fetch_one(&pool)
            .await
            .expect("one stored row");
    assert_eq!(
        (owner.as_str(), generation, stored_id.as_str()),
        (pubky.as_str(), 1, key_id.as_str())
    );
    assert_eq!(sealed.len(), 72);
    assert!(
        !sealed
            .windows(key.len())
            .any(|window| window == key.as_slice()),
        "the stored row does not contain the plaintext key"
    );
    let opened = keys(PRIV_KEY, None)
        .open(&data_key_aad(&pubky, &key_id), &sealed)
        .expect("row opens under the configured key with the owner/key-id AAD");
    assert_eq!(opened, key);
    keys(STRAY_KEY, None)
        .open(&data_key_aad(&pubky, &key_id), &sealed)
        .expect_err("another key does not open the row");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn each_owner_gets_an_independent_key(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let alice = new_actor(&app).await;
    let bob = new_actor(&app).await;
    let (_, a) = get_keys(&app, Some(&alice.token)).await;
    let (_, b) = get_keys(&app, Some(&bob.token)).await;
    assert_eq!(a["owner"], json!(alice.pubky));
    assert_eq!(b["owner"], json!(bob.pubky));
    assert_ne!(decoded_key(&a["keys"][0]), decoded_key(&b["keys"][0]));
    assert_ne!(a["current_key_id"], b["current_key_id"]);
    assert_eq!(row_count(&pool).await, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_priv_app_read_write_sessions_receive_a_key(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;

    let (status, _) = get_keys(&app, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let refused: Vec<Vec<Capability>> = vec![
        // Legacy Shop grant from before the `/priv` scope.
        vec![
            Capability::read_write("/pub/pubky.app/").expect("scope"),
            Capability::read_write("/pub/paykit/").expect("scope"),
        ],
        vec![Capability::read("/priv/pubky.app/").expect("scope")],
        vec![Capability::write("/priv/pubky.app/").expect("scope")],
        vec![Capability::read_write("/priv/pubky.app/marketplace/").expect("scope")],
        vec![Capability::read_write("/priv/other.app/").expect("scope")],
        // The grant flow's marketplace-only scope.
        vec![Capability::read_write("/pub/pubky.app/marketplace-service/v1/").expect("scope")],
    ];
    for capabilities in refused {
        let label = format!("{capabilities:?}");
        let (keypair, _) = random_keypair();
        let token = session_with(&app, &keypair, capabilities).await;
        let (status, body) = get_keys(&app, Some(&token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["code"], json!("needs_reauth"), "{label}");
        assert!(body.get("keys").is_none());
    }
    // A bridged session stores an empty grant until it settles.
    let (_, pubky) = random_keypair();
    let bridged = session_with_stored_grant(&app, &pubky, "").await;
    let (status, body) = get_keys(&app, Some(&bridged)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("needs_reauth"));
    assert_eq!(
        row_count(&pool).await,
        0,
        "a refused session creates no key"
    );

    let granted: Vec<Vec<Capability>> = vec![
        vec![
            Capability::read_write("/pub/pubky.app/").expect("scope"),
            Capability::read_write("/pub/paykit/").expect("scope"),
            Capability::read_write("/priv/pubky.app/").expect("scope"),
        ],
        vec![Capability::read_write("/priv/").expect("scope")],
        vec![Capability::root()],
    ];
    for capabilities in granted {
        let label = format!("{capabilities:?}");
        let (keypair, _) = random_keypair();
        let token = session_with(&app, &keypair, capabilities).await;
        let (status, body) = get_keys(&app, Some(&token)).await;
        assert_eq!(status, StatusCode::OK, "{label}: {body}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_is_refused_and_advertised_off_without_the_sealing_key(pool: PgPool) {
    let off = priv_app(pool.clone(), None).await;
    let actor = new_actor(&off).await;
    let (status, body) = get_keys(&off, Some(&actor.token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], json!("priv_keys_unavailable"));
    let (_, health) = send(off.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(health["priv_keys_available"], json!(false));
    assert_eq!(row_count(&pool).await, 0);

    let on = priv_app(pool, Some(keys(PRIV_KEY, None))).await;
    let (_, health) = send(on.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(health["priv_keys_available"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_first_requests_create_exactly_one_key(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let router = app.router.clone();
        let token = actor.token.clone();
        tasks.push(tokio::spawn(async move {
            send(router, "GET", ROUTE, Some(&token), &Value::Null).await
        }));
    }
    let mut released = Vec::new();
    for task in tasks {
        let (status, body) = task.await.expect("task");
        assert_eq!(status, StatusCode::OK, "{body}");
        released.push(body["keys"].clone());
    }
    assert!(released.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(row_count(&pool).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_row_that_does_not_open_fails_instead_of_minting_a_new_key(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let alice = new_actor(&app).await;
    let bob = new_actor(&app).await;
    let (status, _) = get_keys(&app, Some(&alice.token)).await;
    assert_eq!(status, StatusCode::OK);

    // Alice's sealed key moved to Bob does not open under Bob's AAD.
    sqlx::query("UPDATE user_priv_keys SET owner_pubky = $1")
        .bind(&bob.pubky)
        .execute(&pool)
        .await
        .expect("transplant");
    let (status, body) = get_keys(&app, Some(&bob.token)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(body.get("keys").is_none());

    // A changed key id breaks the AAD the same way.
    sqlx::query("UPDATE user_priv_keys SET owner_pubky = $1, key_id = $2")
        .bind(&alice.pubky)
        .bind("0".repeat(32))
        .execute(&pool)
        .await
        .expect("rename");
    let (status, _) = get_keys(&app, Some(&alice.token)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(row_count(&pool).await, 1, "no replacement key was minted");

    // A service holding the wrong sealing key fails the same way.
    sqlx::query("DELETE FROM user_priv_keys")
        .execute(&pool)
        .await
        .expect("reset");
    get_keys(&app, Some(&alice.token)).await;
    let wrong = priv_app(pool.clone(), Some(keys(STRAY_KEY, None))).await;
    let (status, _) = get_keys(&wrong, Some(&alice.token)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(row_count(&pool).await, 1);
}

async fn seed_keys(pool: &PgPool, sealing: &PrivKeys, owners: usize) -> Vec<(String, Vec<u8>)> {
    let now = chrono::Utc::now();
    let mut seeded = Vec::new();
    for _ in 0..owners {
        let (_, pubky) = random_keypair();
        let released = priv_keys::release_owner_keys(pool, sealing, &pubky, now)
            .await
            .expect("release");
        seeded.push((pubky, released[0].key.to_vec()));
    }
    seeded
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn boot_probe_and_rotation_preserve_every_key(pool: PgPool) {
    let previous_only = keys(PRIV_PREVIOUS_KEY, None);
    let rotated = keys(PRIV_KEY, Some(PRIV_PREVIOUS_KEY));
    let current_only = keys(PRIV_KEY, None);

    assert_eq!(
        assert_priv_key_sealing_coherent(&pool, None)
            .await
            .expect("an empty table boots unkeyed"),
        None
    );

    let seeded = seed_keys(&pool, &previous_only, 3).await;
    assert_priv_key_sealing_coherent(&pool, None)
        .await
        .expect_err("sealed rows without a key fail the boot");
    assert_priv_key_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect_err("a wrong key fails the boot");
    let scan = assert_priv_key_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect("the dual-key window boots")
        .expect("rows were probed");
    assert!(scan.straggler_class && !scan.current_class);
    assert_eq!(scan.probed, 3);

    // Mid-rotation, an owner's request still opens their key under the
    // previous sealing key.
    let (owner, key) = &seeded[0];
    let mid_rotation = priv_keys::release_owner_keys(&pool, &rotated, owner, chrono::Utc::now())
        .await
        .expect("release during the dual-key window");
    assert_eq!(&mid_rotation[0].key.to_vec(), key);

    let unrotated = reseal_previous_key_batch(&pool, &current_only, chrono::Utc::now())
        .await
        .expect("no previous key is a no-op");
    assert_eq!(unrotated.resealed, 0);

    let progress = reseal_previous_key_batch(&pool, &rotated, chrono::Utc::now())
        .await
        .expect("re-seal pass");
    assert_eq!(progress.resealed, 3);
    assert_eq!(progress.remaining_under_previous, 0);
    let scan = assert_priv_key_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect("after rotation the current key alone boots")
        .expect("rows were probed");
    assert!(scan.current_class && !scan.straggler_class);

    for (owner, key) in &seeded {
        let released =
            priv_keys::release_owner_keys(&pool, &current_only, owner, chrono::Utc::now())
                .await
                .expect("release after rotation");
        assert_eq!(released.len(), 1);
        assert_eq!(
            &released[0].key.to_vec(),
            key,
            "rotation keeps the data key"
        );
    }

    // A row under neither key fails the pass without stalling the others.
    let more = seed_keys(&pool, &previous_only, 2).await;
    let (first_id,): (i64,) = sqlx::query_as("SELECT MIN(id) FROM user_priv_keys")
        .fetch_one(&pool)
        .await
        .expect("first id");
    sqlx::query("UPDATE user_priv_keys SET sealed_key = $2 WHERE id = $1")
        .bind(first_id)
        .bind(keys(STRAY_KEY, None).seal(b"x", &[7u8; 32]))
        .execute(&pool)
        .await
        .expect("corrupt the first row");
    assert_priv_key_sealing_coherent(&pool, Some(&rotated))
        .await
        .expect_err("an unopenable row fails the boot");
    reseal_previous_key_batch(&pool, &rotated, chrono::Utc::now())
        .await
        .expect_err("an unopenable row fails the pass");
    for (owner, key) in &more {
        let released =
            priv_keys::release_owner_keys(&pool, &current_only, owner, chrono::Utc::now())
                .await
                .expect("rows after the corrupt one were still re-sealed");
        assert_eq!(&released[0].key.to_vec(), key);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn boot_probe_samples_the_last_rows_past_its_batch_cap(pool: PgPool) {
    let current_only = keys(PRIV_KEY, None);
    let rows = priv_keys::PROBE_BATCH_SIZE as usize * priv_keys::PROBE_MAX_BATCHES as usize + 5;
    let now = chrono::Utc::now();
    for index in 0..rows {
        let owner = format!("{index:0>52}");
        let key_id = format!("{index:0>32x}");
        sqlx::query(
            "INSERT INTO user_priv_keys (owner_pubky, generation, key_id, sealed_key, \
             created_at, updated_at) VALUES ($1, 1, $2, $3, $4, $4)",
        )
        .bind(&owner)
        .bind(&key_id)
        .bind(current_only.seal(&data_key_aad(&owner, &key_id), &[1u8; 32]))
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed row");
    }
    let scan = assert_priv_key_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect("boots")
        .expect("probed");
    assert!(scan.probed < rows as u64);
    assert_eq!(scan.table_total, Some(rows as u64));

    // The newest row is sampled even though the ascending scan stopped.
    sqlx::query(
        "UPDATE user_priv_keys SET sealed_key = $1 WHERE id = (SELECT MAX(id) FROM user_priv_keys)",
    )
    .bind(keys(STRAY_KEY, None).seal(b"x", &[1u8; 32]))
    .execute(&pool)
    .await
    .expect("corrupt newest");
    assert_priv_key_sealing_coherent(&pool, Some(&current_only))
        .await
        .expect_err("a corrupt newest row fails the boot");
}

struct ConcurrentReseal {
    pool: PgPool,
    sealing: PrivKeys,
}

impl ResealHook for ConcurrentReseal {
    fn before_write<'a>(
        &'a self,
        row_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let (owner, key_id): (String, String) =
                sqlx::query_as("SELECT owner_pubky, key_id FROM user_priv_keys WHERE id = $1")
                    .bind(row_id)
                    .fetch_one(&self.pool)
                    .await
                    .expect("row");
            sqlx::query("UPDATE user_priv_keys SET sealed_key = $2 WHERE id = $1")
                .bind(row_id)
                .bind(
                    self.sealing
                        .seal(&data_key_aad(&owner, &key_id), &[9u8; 32]),
                )
                .execute(&self.pool)
                .await
                .expect("concurrent write");
        })
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn reseal_does_not_overwrite_a_row_changed_after_it_was_read(pool: PgPool) {
    let rotated = keys(PRIV_KEY, Some(PRIV_PREVIOUS_KEY));
    let seeded = seed_keys(&pool, &keys(PRIV_PREVIOUS_KEY, None), 1).await;
    let hook = ConcurrentReseal {
        pool: pool.clone(),
        sealing: keys(PRIV_KEY, None),
    };
    let progress = reseal_previous_key_batch_with_hook(&pool, &rotated, chrono::Utc::now(), &hook)
        .await
        .expect("pass");
    assert_eq!((progress.resealed, progress.skipped_changed), (0, 1));
    let released = priv_keys::release_owner_keys(
        &pool,
        &keys(PRIV_KEY, None),
        &seeded[0].0,
        chrono::Utc::now(),
    )
    .await
    .expect("release");
    assert_eq!(released[0].key, [9u8; 32], "the concurrent write survives");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_tick_reseals_while_a_previous_key_is_configured(pool: PgPool) {
    let seeded = seed_keys(&pool, &keys(PRIV_PREVIOUS_KEY, None), 2).await;

    let current_only = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let summary = run_once(
        &current_only.state,
        Uuid::new_v4(),
        current_only.clock.now(),
    )
    .await
    .expect("tick");
    assert_eq!(summary.priv_keys_resealed, 0, "no previous key, no pass");

    let rotating = priv_app(pool.clone(), Some(keys(PRIV_KEY, Some(PRIV_PREVIOUS_KEY)))).await;
    let summary = run_once(&rotating.state, Uuid::new_v4(), rotating.clock.now())
        .await
        .expect("tick");
    assert_eq!(summary.priv_keys_resealed, 2);
    assert_priv_key_sealing_coherent(&pool, Some(&keys(PRIV_KEY, None)))
        .await
        .expect("the current key alone boots after the tick");
    for (owner, key) in &seeded {
        let released =
            priv_keys::release_owner_keys(&pool, &keys(PRIV_KEY, None), owner, chrono::Utc::now())
                .await
                .expect("release");
        assert_eq!(&released[0].key.to_vec(), key);
    }
}

fn locks_runtime() -> LocksRuntime {
    LocksRuntime {
        keys: LocksKeys::from_hex(TEST_LOCKS_ENCRYPTION_KEY, TEST_LOCKS_HMAC_KEY)
            .expect("locks keys"),
        client: Arc::new(common::FakeLocksClient::default()),
    }
}

#[test]
fn priv_keys_must_differ_from_every_other_sealing_key() {
    let locks = locks_runtime();
    let pickup = PickupKeys::from_hex(
        TEST_PICKUP_ENCRYPTION_KEY,
        Some(TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("pickup keys");
    let digital = DigitalKeys::from_hex(
        TEST_DIGITAL_ENCRYPTION_KEY,
        Some(TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY),
    )
    .expect("digital keys");
    priv_keys::ensure_distinct_keys(
        &keys(PRIV_KEY, Some(PRIV_PREVIOUS_KEY)),
        Some(&locks),
        Some(&pickup),
        Some(&digital),
    )
    .expect("distinct keys pass");
    for aliased in [
        TEST_LOCKS_ENCRYPTION_KEY,
        TEST_LOCKS_HMAC_KEY,
        TEST_PICKUP_ENCRYPTION_KEY,
        TEST_PICKUP_PREVIOUS_ENCRYPTION_KEY,
        TEST_DIGITAL_ENCRYPTION_KEY,
        TEST_DIGITAL_PREVIOUS_ENCRYPTION_KEY,
    ] {
        for candidate in [keys(aliased, None), keys(PRIV_KEY, Some(aliased))] {
            priv_keys::ensure_distinct_keys(
                &candidate,
                Some(&locks),
                Some(&pickup),
                Some(&digital),
            )
            .expect_err("an aliased key is refused");
        }
    }
}

#[test]
fn priv_keys_from_env_is_all_or_none_and_checks_distinctness() {
    let digital = DigitalKeys::from_hex(TEST_DIGITAL_ENCRYPTION_KEY, None).expect("digital keys");
    let current = priv_keys::ENV_PRIV_DATA_KEY_ENCRYPTION_KEY;
    let previous = priv_keys::ENV_PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS;
    // Env mutation is process-wide; this test binary's other tests never
    // read these two variables.
    std::env::remove_var(current);
    std::env::remove_var(previous);
    assert!(priv_keys::priv_keys_from_env(None, None, Some(&digital))
        .expect("unset is off")
        .is_none());
    std::env::set_var(previous, PRIV_PREVIOUS_KEY);
    priv_keys::priv_keys_from_env(None, None, Some(&digital))
        .expect_err("a previous key alone is refused");
    std::env::set_var(current, TEST_DIGITAL_ENCRYPTION_KEY);
    priv_keys::priv_keys_from_env(None, None, Some(&digital))
        .expect_err("a key shared with digital delivery is refused");
    std::env::set_var(current, PRIV_KEY);
    let loaded = priv_keys::priv_keys_from_env(None, None, Some(&digital))
        .expect("valid keys load")
        .expect("on");
    assert!(loaded.has_previous());
    std::env::remove_var(current);
    std::env::remove_var(previous);
}

// ---------------------------------------------------------------------------
// Phase 4: owner-held keys. The owner wraps every data key on their own
// homeserver, then asks the service to drop its copies.
// ---------------------------------------------------------------------------

const RELEASE_ROUTE: &str = "/v1/me/priv-keys/release";

async fn post_release(
    app: &TestApp,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    send_with_headers(app.router.clone(), "POST", RELEASE_ROUTE, token, body).await
}

async fn post_release_raw(app: &TestApp, token: &str, body: Vec<u8>) -> (StatusCode, Value) {
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(RELEASE_ROUTE)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::from(body))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn released_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM user_priv_key_custody_releases")
        .fetch_one(pool)
        .await
        .expect("released count")
}

async fn key_ids_of(app: &TestApp, token: &str) -> Vec<String> {
    let (status, body) = get_keys(app, Some(token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["keys"]
        .as_array()
        .expect("keys")
        .iter()
        .map(|entry| entry["key_id"].as_str().expect("key id").to_string())
        .collect()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn releasing_custody_drops_the_sealed_key_and_the_service_never_makes_another(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let key_ids = key_ids_of(&app, &actor.token).await;
    assert_eq!(row_count(&pool).await, 1);

    let (status, headers, body) =
        post_release(&app, Some(&actor.token), &json!({ "key_ids": key_ids })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({ "schema_version": 1, "owner": actor.pubky, "released": true })
    );
    assert_eq!(
        headers.get("cache-control").and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    assert_eq!(row_count(&pool).await, 0, "no sealed key remains");
    assert_eq!(released_count(&pool).await, 1);

    for _ in 0..2 {
        let (status, body) = get_keys(&app, Some(&actor.token)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], json!("custody_released"));
        assert!(body.get("keys").is_none());
    }
    assert_eq!(
        row_count(&pool).await,
        0,
        "a read never mints a replacement"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_second_session_of_the_same_owner_also_finds_custody_released(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let (keypair, _) = random_keypair();
    let first = session_with(&app, &keypair, vec![Capability::root()]).await;
    let key_ids = key_ids_of(&app, &first).await;
    let (status, _, _) = post_release(&app, Some(&first), &json!({ "key_ids": key_ids })).await;
    assert_eq!(status, StatusCode::OK);

    let other_device = session_with(&app, &keypair, vec![Capability::root()]).await;
    let (status, body) = get_keys(&app, Some(&other_device)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(row_count(&pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_repeated_release_succeeds_and_changes_nothing(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let key_ids = key_ids_of(&app, &actor.token).await;
    let request = json!({ "key_ids": key_ids });
    let (first, _, _) = post_release(&app, Some(&actor.token), &request).await;
    let released_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT released_at FROM user_priv_key_custody_releases")
            .fetch_one(&pool)
            .await
            .expect("released at");

    // The answer to a lost reply: the same request, or any other well-formed
    // one, finds the work done.
    let (again, _, body) = post_release(&app, Some(&actor.token), &request).await;
    let other = json!({ "key_ids": ["f".repeat(32)] });
    let (other_status, _, other_body) = post_release(&app, Some(&actor.token), &other).await;

    assert_eq!(first, StatusCode::OK);
    assert_eq!((again, other_status), (StatusCode::OK, StatusCode::OK));
    assert_eq!(body["released"], json!(true));
    assert_eq!(other_body["released"], json!(true));
    assert_eq!(released_count(&pool).await, 1);
    let kept: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT released_at FROM user_priv_key_custody_releases")
            .fetch_one(&pool)
            .await
            .expect("released at");
    assert_eq!(kept, released_at, "the first release time is kept");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_release_must_name_exactly_the_keys_the_service_holds(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let held = key_ids_of(&app, &actor.token).await;
    // A second key issued since the owner read the first (an operator-side
    // rotation), so the owner's list is stale.
    let second = "fedcba9876543210fedcba9876543210";
    let sealed = keys(PRIV_KEY, None).seal(&data_key_aad(&actor.pubky, second), &[9u8; 32]);
    sqlx::query(
        "INSERT INTO user_priv_keys (owner_pubky, generation, key_id, sealed_key, created_at, \
         updated_at) VALUES ($1, 2, $2, $3, now(), now())",
    )
    .bind(&actor.pubky)
    .bind(second)
    .bind(&sealed)
    .execute(&pool)
    .await
    .expect("second key");

    for (label, key_ids) in [
        ("only the first key", vec![held[0].clone()]),
        ("an unknown key", vec!["a".repeat(32)]),
        (
            "the held keys plus an unknown one",
            vec![held[0].clone(), second.to_string(), "a".repeat(32)],
        ),
        ("only the second key", vec![second.to_string()]),
    ] {
        let (status, _, body) =
            post_release(&app, Some(&actor.token), &json!({ "key_ids": key_ids })).await;
        assert_eq!(status, StatusCode::CONFLICT, "{label}: {body}");
        assert_eq!(body["error"]["code"], json!("key_set_changed"), "{label}");
        assert_eq!(row_count(&pool).await, 2, "{label}: nothing was dropped");
        assert_eq!(released_count(&pool).await, 0, "{label}");
    }

    // The right set, in any order, releases both.
    let (status, _, body) = post_release(
        &app,
        Some(&actor.token),
        &json!({ "key_ids": [second, held[0]] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_count(&pool).await, 0);
    let (count,): (i32,) = sqlx::query_as("SELECT key_count FROM user_priv_key_custody_releases")
        .fetch_one(&pool)
        .await
        .expect("key count");
    assert_eq!(count, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn releasing_before_any_key_exists_drops_nothing_and_records_nothing(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;

    let (status, _, body) = post_release(
        &app,
        Some(&actor.token),
        &json!({ "key_ids": ["a".repeat(32)] }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("key_set_changed"));
    assert_eq!(released_count(&pool).await, 0);
    let (status, _) = get_keys(&app, Some(&actor.token)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the owner can still get a first key"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_release_only_touches_the_session_owner(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let alice = new_actor(&app).await;
    let bob = new_actor(&app).await;
    let alice_ids = key_ids_of(&app, &alice.token).await;
    let bob_ids = key_ids_of(&app, &bob.token).await;
    assert_ne!(alice_ids, bob_ids);

    // Naming Bob's key with Alice's session drops nothing of either.
    let (status, _, _) =
        post_release(&app, Some(&alice.token), &json!({ "key_ids": bob_ids })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(row_count(&pool).await, 2);

    let (status, _, _) =
        post_release(&app, Some(&alice.token), &json!({ "key_ids": alice_ids })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row_count(&pool).await, 1);
    let (status, body) = get_keys(&app, Some(&bob.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["owner"], json!(bob.pubky));
    let (status, _) = get_keys(&app, Some(&alice.token)).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_needs_the_same_authority_as_the_key_read(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let owner = new_actor(&app).await;
    let key_ids = key_ids_of(&app, &owner.token).await;
    let request = json!({ "key_ids": key_ids });

    let (status, _, _) = post_release(&app, None, &request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let refused: Vec<Vec<Capability>> = vec![
        vec![Capability::read("/priv/pubky.app/").expect("scope")],
        vec![Capability::read_write("/priv/pubky.app/marketplace/").expect("scope")],
        vec![Capability::read_write("/pub/pubky.app/marketplace-service/v1/").expect("scope")],
    ];
    for capabilities in refused {
        let label = format!("{capabilities:?}");
        let (keypair, _) = random_keypair();
        let token = session_with(&app, &keypair, capabilities).await;
        let (status, _, body) = post_release(&app, Some(&token), &request).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["code"], json!("needs_reauth"), "{label}");
    }
    let (_, pubky) = random_keypair();
    let bridged = session_with_stored_grant(&app, &pubky, "").await;
    let (status, _, body) = post_release(&app, Some(&bridged), &request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(row_count(&pool).await, 1, "a refused release drops nothing");
    assert_eq!(released_count(&pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn release_is_unavailable_without_the_sealing_key(pool: PgPool) {
    let off = priv_app(pool.clone(), None).await;
    let actor = new_actor(&off).await;
    let (status, _, body) = post_release(
        &off,
        Some(&actor.token),
        &json!({ "key_ids": ["a".repeat(32)] }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], json!("priv_keys_unavailable"));
    assert_eq!(released_count(&pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_malformed_release_request_is_refused_before_anything_is_dropped(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let key_ids = key_ids_of(&app, &actor.token).await;
    let good = key_ids[0].clone();

    let too_many: Vec<String> = (0..65).map(|index| format!("{index:032x}")).collect();
    let bodies: Vec<(&str, Vec<u8>)> = vec![
        ("not json", b"nope".to_vec()),
        ("empty body", Vec::new()),
        ("an array", b"[]".to_vec()),
        ("no key ids", br#"{"key_ids":[]}"#.to_vec()),
        ("missing field", br#"{}"#.to_vec()),
        (
            "extra field",
            serde_json::to_vec(&json!({ "key_ids": [good], "all": true })).unwrap(),
        ),
        (
            "a repeated id",
            serde_json::to_vec(&json!({ "key_ids": [good, good] })).unwrap(),
        ),
        (
            "an uppercase id",
            serde_json::to_vec(&json!({ "key_ids": [good.to_uppercase()] })).unwrap(),
        ),
        (
            "a short id",
            serde_json::to_vec(&json!({ "key_ids": ["abcd"] })).unwrap(),
        ),
        (
            "a non-string id",
            serde_json::to_vec(&json!({ "key_ids": [1] })).unwrap(),
        ),
        (
            "too many ids",
            serde_json::to_vec(&json!({ "key_ids": too_many })).unwrap(),
        ),
    ];
    for (label, body) in bodies {
        let (status, response) = post_release_raw(&app, &actor.token, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {response}");
        assert_eq!(
            response["error"]["code"],
            json!("invalid_request"),
            "{label}"
        );
    }
    assert_eq!(row_count(&pool).await, 1);
    assert_eq!(released_count(&pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn reads_racing_a_release_never_yield_a_second_key(pool: PgPool) {
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let actor = new_actor(&app).await;
    let key_ids = key_ids_of(&app, &actor.token).await;

    let mut tasks = Vec::new();
    for index in 0..9 {
        let router = app.router.clone();
        let token = actor.token.clone();
        let request = json!({ "key_ids": key_ids });
        tasks.push(tokio::spawn(async move {
            if index == 4 {
                send(router, "POST", RELEASE_ROUTE, Some(&token), &request).await
            } else {
                send(router, "GET", ROUTE, Some(&token), &Value::Null).await
            }
        }));
    }
    for task in tasks {
        let (status, body) = task.await.expect("task");
        match status {
            StatusCode::OK => {
                if let Some(entries) = body["keys"].as_array() {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0]["key_id"], json!(key_ids[0]));
                } else {
                    assert_eq!(body["released"], json!(true));
                }
            }
            StatusCode::CONFLICT => assert_eq!(body["error"]["code"], json!("custody_released")),
            other => panic!("unexpected {other}: {body}"),
        }
    }
    assert_eq!(row_count(&pool).await, 0);
    assert_eq!(released_count(&pool).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_release_leaves_the_boot_probe_and_other_owners_untouched(pool: PgPool) {
    let sealing = keys(PRIV_KEY, None);
    let app = priv_app(pool.clone(), Some(keys(PRIV_KEY, None))).await;
    let alice = new_actor(&app).await;
    let bob = new_actor(&app).await;
    let alice_ids = key_ids_of(&app, &alice.token).await;
    key_ids_of(&app, &bob.token).await;

    let (status, _, _) =
        post_release(&app, Some(&alice.token), &json!({ "key_ids": alice_ids })).await;
    assert_eq!(status, StatusCode::OK);
    let scan = assert_priv_key_sealing_coherent(&pool, Some(&sealing))
        .await
        .expect("probe passes")
        .expect("Bob's row is probed");
    assert_eq!(scan.probed, 1);

    // The last owner released: nothing sealed remains, so the service may boot
    // without a sealing key.
    let (status, _, _) = post_release(
        &app,
        Some(&bob.token),
        &json!({ "key_ids": key_ids_of(&app, &bob.token).await }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(assert_priv_key_sealing_coherent(&pool, None)
        .await
        .expect("no rows, no key needed")
        .is_none());
}
