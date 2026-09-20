//! Durable grant-worker relay lease: restore must keep the inbox listener
//! alive for the lease so a stored approval is observed exactly once.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use marketplace_domain::pubky::encode_pubky;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::grant;
use marketplace_service::grant::test_support::GrantTestAuthority;
use pubky::{Keypair, PubkySigner};
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::Notify;
use url::Url;
use uuid::Uuid;

use common::{send, test_app_with_grant_authority};

const VERIFY_LEASE_SECONDS: i64 = 2;
const LONG_POLL: Duration = Duration::from_secs(25);

struct RelayInner {
    messages: std::sync::Mutex<HashMap<String, Bytes>>,
    acked: std::sync::Mutex<HashMap<String, bool>>,
    stall: std::sync::Mutex<Option<Arc<Notify>>>,
    ack_stall: std::sync::Mutex<Option<Arc<Notify>>>,
    posted: Notify,
    get_started: Notify,
    idle: Notify,
    gets: AtomicU64,
    posts: AtomicU64,
    deletes: AtomicU64,
    inflight: AtomicU64,
}

#[derive(Clone)]
struct RelayInbox {
    inner: Arc<RelayInner>,
    base: Url,
}

struct InflightGuard(Arc<RelayInner>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let remaining = self.0.inflight.fetch_sub(1, Ordering::SeqCst) - 1;
        if remaining == 0 {
            self.0.idle.notify_waiters();
        }
    }
}

impl RelayInbox {
    async fn spawn() -> Self {
        let inner = Arc::new(RelayInner {
            messages: std::sync::Mutex::new(HashMap::new()),
            acked: std::sync::Mutex::new(HashMap::new()),
            stall: std::sync::Mutex::new(None),
            ack_stall: std::sync::Mutex::new(None),
            posted: Notify::new(),
            get_started: Notify::new(),
            idle: Notify::new(),
            gets: AtomicU64::new(0),
            posts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
        });
        let router = Router::new()
            .route("/inbox/{channel}/ack", get(relay_ack_status))
            .route("/inbox/{channel}/await", get(relay_await_ack))
            .route(
                "/inbox/{channel}",
                get(relay_get).post(relay_post).delete(relay_delete),
            )
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock inbox binds");
        let addr = listener.local_addr().expect("mock inbox address");
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("mock inbox serves");
        });
        Self {
            inner,
            base: Url::parse(&format!("http://{addr}/inbox")).expect("inbox base"),
        }
    }

    fn base(&self) -> Url {
        self.base.clone()
    }

    fn gets(&self) -> u64 {
        self.inner.gets.load(Ordering::SeqCst)
    }

    fn deletes(&self) -> u64 {
        self.inner.deletes.load(Ordering::SeqCst)
    }

    fn has_message(&self) -> bool {
        !self.inner.messages.lock().expect("messages").is_empty()
    }

    #[allow(dead_code)]
    fn stall(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.inner.stall.lock().expect("stall") = Some(notify.clone());
        notify
    }

    fn stall_delete(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.inner.ack_stall.lock().expect("ack stall") = Some(notify.clone());
        notify
    }

    fn clear_stall(&self) {
        *self.inner.stall.lock().expect("stall") = None;
        *self.inner.ack_stall.lock().expect("ack stall") = None;
    }

    async fn wait_deletes(&self, count: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.deletes() >= count {
                return;
            }
            if Instant::now() >= deadline {
                panic!("inbox DELETE count {} < {count}", self.deletes());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_get_started(&self) {
        if self.gets() > 0 {
            return;
        }
        tokio::time::timeout(Duration::from_secs(5), self.inner.get_started.notified())
            .await
            .expect("relay GET started");
    }

    async fn wait_idle(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.inner.inflight.load(Ordering::SeqCst) == 0 {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "inbox GETs still in flight: {}",
                    self.inner.inflight.load(Ordering::SeqCst)
                );
            }
            tokio::time::timeout(Duration::from_millis(50), self.inner.idle.notified())
                .await
                .ok();
        }
    }

    async fn drain_create_listener(&self) {
        tokio::select! {
            () = self.wait_get_started() => {}
            () = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
        self.wait_idle().await;
    }
}

async fn maybe_stall(state: &Arc<RelayInner>) {
    let stall = state.stall.lock().expect("stall").clone();
    if let Some(stall) = stall {
        stall.notified().await;
    }
}

async fn maybe_stall_delete(state: &Arc<RelayInner>) {
    let stall = state.ack_stall.lock().expect("ack stall").clone();
    if let Some(stall) = stall {
        stall.notified().await;
    }
}

async fn relay_get(
    State(state): State<Arc<RelayInner>>,
    Path(channel): Path<String>,
) -> (StatusCode, Bytes) {
    state.gets.fetch_add(1, Ordering::SeqCst);
    state.inflight.fetch_add(1, Ordering::SeqCst);
    state.get_started.notify_waiters();
    let _guard = InflightGuard(state.clone());
    maybe_stall(&state).await;
    let posted = state.posted.notified();
    if let Some(body) = state
        .messages
        .lock()
        .expect("messages")
        .get(&channel)
        .cloned()
    {
        return (StatusCode::OK, body);
    }
    tokio::select! {
        () = posted => {}
        () = tokio::time::sleep(LONG_POLL) => {
            return (StatusCode::REQUEST_TIMEOUT, Bytes::new());
        }
    }
    match state
        .messages
        .lock()
        .expect("messages")
        .get(&channel)
        .cloned()
    {
        Some(body) => (StatusCode::OK, body),
        None => (StatusCode::REQUEST_TIMEOUT, Bytes::new()),
    }
}

async fn relay_post(
    State(state): State<Arc<RelayInner>>,
    Path(channel): Path<String>,
    body: Bytes,
) -> StatusCode {
    state.posts.fetch_add(1, Ordering::SeqCst);
    state
        .messages
        .lock()
        .expect("messages")
        .insert(channel, body);
    state.posted.notify_waiters();
    StatusCode::OK
}

async fn relay_delete(
    State(state): State<Arc<RelayInner>>,
    Path(channel): Path<String>,
) -> StatusCode {
    state.deletes.fetch_add(1, Ordering::SeqCst);
    maybe_stall_delete(&state).await;
    let removed = state
        .messages
        .lock()
        .expect("messages")
        .remove(&channel)
        .is_some();
    if removed {
        state.acked.lock().expect("acked").insert(channel, true);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn relay_ack_status(
    State(state): State<Arc<RelayInner>>,
    Path(channel): Path<String>,
) -> (StatusCode, &'static str) {
    match state.acked.lock().expect("acked").get(&channel).copied() {
        Some(true) => (StatusCode::OK, "true"),
        Some(false) => (StatusCode::OK, "false"),
        None => (StatusCode::NOT_FOUND, ""),
    }
}

async fn relay_await_ack(
    State(state): State<Arc<RelayInner>>,
    Path(channel): Path<String>,
) -> (StatusCode, &'static str) {
    let deadline = Instant::now() + LONG_POLL;
    loop {
        if state.acked.lock().expect("acked").get(&channel).copied() == Some(true) {
            return (StatusCode::OK, "true");
        }
        if Instant::now() >= deadline {
            return (StatusCode::REQUEST_TIMEOUT, "false");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct FlowHarness {
    app: common::TestApp,
    pool: PgPool,
    relay: RelayInbox,
    signer: Keypair,
    flow_id: Uuid,
    authorization_url: String,
}

async fn create_awaiting_flow(pool: PgPool, relay: RelayInbox) -> FlowHarness {
    let authority = GrantTestAuthority::generate_with_relay(
        Config::for_tests().session_ttl_seconds,
        relay.base(),
        VERIFY_LEASE_SECONDS,
    );
    let (app, authority) = test_app_with_grant_authority(pool.clone(), authority).await;
    let signer = Keypair::random();
    let expected_pubky = signer.public_key().z32();
    let result_key = SigningKey::from_bytes(&[19u8; 32]);
    let result_cpk = encode_pubky(&result_key.verifying_key().to_bytes());
    let assertion = authority.sign_bootstrap_assertion(
        &expected_pubky,
        &[23u8; 32],
        &result_cpk,
        app.clock.now(),
        Uuid::new_v4(),
    );
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion": assertion}),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    relay.drain_create_listener().await;
    let flow_id = Uuid::parse_str(body["flow_id"].as_str().expect("flow_id")).expect("uuid");
    FlowHarness {
        app,
        pool,
        relay,
        signer,
        flow_id,
        authorization_url: body["authorization_url"]
            .as_str()
            .expect("authorization_url")
            .to_string(),
    }
}

async fn approve(harness: &FlowHarness) {
    let signer = PubkySigner::new(harness.signer.clone()).expect("signer");
    signer
        .approve_auth(&harness.authorization_url)
        .await
        .expect("grant posted to inbox");
}

async fn flow_row(pool: &PgPool, flow_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, terminal_code FROM grant_flows WHERE flow_id = $1")
        .bind(flow_id)
        .fetch_one(pool)
        .await
        .expect("flow row")
}

async fn approved_pubky(pool: &PgPool, flow_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT approved_pubky FROM grant_flows WHERE flow_id = $1")
        .bind(flow_id)
        .fetch_one(pool)
        .await
        .expect("approved_pubky")
}

async fn wait_exchanging(pool: &PgPool, flow_id: Uuid) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if approved_pubky(pool, flow_id).await.is_some() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("flow did not enter exchanging (approved_pubky unset)");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_live_lease(pool: &PgPool, flow_id: Uuid, now: DateTime<Utc>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let live: Option<bool> =
            sqlx::query_scalar("SELECT lease_until > $2 FROM grant_flows WHERE flow_id = $1")
                .bind(flow_id)
                .bind(now)
                .fetch_optional(pool)
                .await
                .expect("lease_until");
        if live == Some(true) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("heartbeat did not renew lease_until past {now}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_not_awaiting(pool: &PgPool, flow_id: Uuid) -> (String, Option<String>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = flow_row(pool, flow_id).await;
        if row.0 != "awaiting" {
            return row;
        }
        if Instant::now() >= deadline {
            panic!("flow returned to awaiting after inbox consume");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn assert_processed(status: &str, terminal_code: Option<&str>, relay: &RelayInbox) {
    assert_ne!(
        status,
        "awaiting",
        "lease must leave awaiting after observing the inbox (gets={} deletes={} has_message={} code={terminal_code:?})",
        relay.gets(),
        relay.deletes(),
        relay.has_message()
    );
    assert_ne!(
        status, "verifying",
        "lease must not remain verifying after observing the inbox"
    );
    assert_eq!(
        relay.deletes(),
        1,
        "exactly one durable lease must ACK the inbox message"
    );
    assert_eq!(
        (status, terminal_code),
        ("failed", Some("grant_exchange")),
        "local mock has no PKARR homeserver; observation still completes the lease"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn restore_observes_preexisting_inbox_message_on_one_durable_lease(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    approve(&harness).await;
    assert!(
        harness.relay.has_message(),
        "approval must sit in the inbox before restore"
    );

    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("one durable lease");
    assert_eq!(scanned, 1);

    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);
}

#[sqlx::test(migrations = "./migrations")]
async fn lease_expiry_with_live_listener_returns_awaiting_without_consuming(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    let started = Instant::now();
    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("empty-inbox lease");
    assert_eq!(scanned, 1);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "listener must live for the remaining lease, not return from a nonblocking probe"
    );
    harness.relay.wait_idle().await;
    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_eq!(status, "awaiting");
    assert_eq!(terminal_code, None);
    assert_eq!(harness.relay.deletes(), 0);

    approve(&harness).await;
    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("lease after expiry still consumes");
    assert_eq!(scanned, 1);
    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);
}

#[sqlx::test(migrations = "./migrations")]
async fn two_workers_race_and_only_one_consumes(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    approve(&harness).await;

    let (left, right) = tokio::join!(
        grant::poll_once(&harness.app.state),
        grant::poll_once(&harness.app.state)
    );
    let scanned = left.expect("left worker") + right.expect("right worker");
    assert_eq!(
        scanned, 1,
        "SKIP LOCKED must hand the flow to exactly one lease"
    );

    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_crash_after_inbox_consume_does_not_return_awaiting(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    let stall = harness.relay.stall_delete();
    approve(&harness).await;
    let state = harness.app.state.clone();
    let worker = tokio::spawn(async move { grant::poll_once(&state).await });
    harness.relay.wait_deletes(1).await;
    worker.abort();
    let join = worker.await;
    assert!(join.is_err(), "post-consume crash aborts the worker task");
    stall.notify_waiters();
    harness.relay.clear_stall();
    harness.relay.wait_idle().await;

    let (status, _) = wait_not_awaiting(&harness.pool, harness.flow_id).await;
    assert_ne!(
        status, "awaiting",
        "abort after inbox consume must not revive awaiting"
    );
    assert_eq!(harness.relay.deletes(), 1, "consume already ACKed");

    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("no successor lease after consume");
    assert_eq!(scanned, 0, "retry must not require a second produce");
}

#[sqlx::test(migrations = "./migrations")]
async fn replica_reaper_does_not_lease_lost_after_inbox_consume(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    let stall = harness.relay.stall_delete();
    approve(&harness).await;

    let state = harness.app.state.clone();
    let worker = tokio::spawn(async move { grant::poll_once(&state).await });
    wait_exchanging(&harness.pool, harness.flow_id).await;
    harness.app.clock.advance_seconds(60);
    wait_live_lease(&harness.pool, harness.flow_id, harness.app.clock.now()).await;
    let _ = grant::reap_once(&harness.app.state)
        .await
        .expect("replica reaper");
    stall.notify_waiters();
    harness.relay.clear_stall();
    worker.await.expect("worker join").expect("worker poll");

    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_ne!(
        terminal_code.as_deref(),
        Some("lease_lost"),
        "consumed approval must not be reap-lost, status={status}"
    );
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);
}

#[sqlx::test(migrations = "./migrations")]
async fn post_ack_abort_does_not_return_awaiting(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    let stall = harness.relay.stall_delete();
    approve(&harness).await;

    let state = harness.app.state.clone();
    let worker = tokio::spawn(async move { grant::poll_once(&state).await });
    harness.relay.wait_deletes(1).await;
    worker.abort();
    let _ = worker.await;
    stall.notify_waiters();
    harness.relay.clear_stall();
    harness.relay.wait_idle().await;

    let (status, _) = wait_not_awaiting(&harness.pool, harness.flow_id).await;
    assert_ne!(status, "awaiting");
    assert_eq!(harness.relay.deletes(), 1, "consume already ACKed");

    harness.app.clock.advance_seconds(60);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = grant::reap_once(&harness.app.state)
        .await
        .expect("reap dead exchanging");
    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_ne!(status, "awaiting");
    assert_eq!(status, "failed");
    assert_eq!(terminal_code.as_deref(), Some("grant_exchange"));

    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("no successor lease");
    assert_eq!(scanned, 0, "retry must not require a second produce");
}

#[sqlx::test(migrations = "./migrations")]
async fn duplicate_inbox_message_after_commit_is_ignored(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    approve(&harness).await;
    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("first consume");
    assert_eq!(scanned, 1);
    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);

    approve(&harness).await;
    let scanned = grant::poll_once(&harness.app.state)
        .await
        .expect("duplicate must not acquire a terminal flow");
    assert_eq!(scanned, 0);
    let (again, again_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_eq!(again, status);
    assert_eq!(again_code, terminal_code);
    assert_eq!(
        harness.relay.deletes(),
        1,
        "duplicate leftover must not be consumed"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn late_inbox_message_within_lease_is_observed(pool: PgPool) {
    let relay = RelayInbox::spawn().await;
    let harness = create_awaiting_flow(pool.clone(), relay.clone()).await;
    let state = harness.app.state.clone();
    let worker = tokio::spawn(async move { grant::poll_once(&state).await });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    approve(&harness).await;
    worker.await.expect("worker join").expect("late consume");
    let (status, terminal_code) = flow_row(&harness.pool, harness.flow_id).await;
    assert_processed(&status, terminal_code.as_deref(), &harness.relay);
}
