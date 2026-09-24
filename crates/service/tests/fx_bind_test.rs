//! FX-at-bind integration tests (USD/2 convert-at-bind): feed-failure
//! fail-closed behavior with zero side effects and no rate/feed leakage in
//! tracing, the 3-sample bootstrap, the persisted median across a process
//! restart with outlier rejection at insert, bind idempotency against the
//! feed fetch counter, the prepare/activate/void lifecycle carrying the
//! quoted sats, exact/under/over settlement, refunds derived from the
//! frozen observation, and the USD end-to-end projection — all against real
//! Postgres with the real marketplace FX wrapper over a scripted local
//! Blocktank double.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use common::fx_feed::{fx_body, FakeFxFeed};
use common::paykit_review::{enable_bitcoin, poll_now, resolve_call};
use common::*;
use marketplace_service::bitcoin_review::{
    route_due_seller_confirmation_windows, SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows, sample_fx_rate};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

/// The fixture order total: 12_500 + 1_200 shipping at USD/2.
const ORDER_TOTAL_MINOR: u128 = 13_700;
const RATE: &str = "77197";

fn expected_quoted_sats(total_minor: u128, rate: u128) -> u128 {
    let numerator = total_minor * 100_000_000;
    let denominator = 100 * rate;
    numerator.div_ceil(denominator)
}

#[derive(Debug, sqlx::FromRow)]
struct FxOrderFacts {
    state: String,
    payment_method: Option<String>,
    stock_held: bool,
    paykit_request_state: Option<String>,
    bitcoin_quoted_sats: Option<i64>,
    bitcoin_quote_source: Option<String>,
    bitcoin_quote_expires_at: Option<DateTime<Utc>>,
    paykit_total_sats: Option<i64>,
    paykit_expires_at: Option<DateTime<Utc>>,
    paykit_observed_sats: Option<i64>,
    paykit_observation: Option<Value>,
    hold_expires_at: Option<DateTime<Utc>>,
    hold_source: Option<String>,
}

async fn order_facts(pool: &PgPool, order_id: &str) -> FxOrderFacts {
    sqlx::query_as(
        "SELECT state, payment_method, stock_held, paykit_request_state, bitcoin_quoted_sats, \
         bitcoin_quote_source, bitcoin_quote_expires_at, paykit_total_sats, paykit_expires_at, \
         paykit_observed_sats, paykit_observation, hold_expires_at, hold_source FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn payment_state(pool: &PgPool, order_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(pool)
        .await
        .expect("payment row")
}

/// Seeds `count` accepted reference samples at one-minute buckets ending at
/// `now`, exactly what the sampler would have persisted.
async fn seed_fx_samples(pool: &PgPool, rate: &str, now: DateTime<Utc>, count: i64) {
    for back in 0..count {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank')",
        )
        .bind(rate)
        .bind(accepted)
        .bind(bucket)
        .execute(pool)
        .await
        .expect("seed fx sample");
    }
}

async fn sample_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM fx_rate_samples")
        .fetch_one(pool)
        .await
        .expect("sample count")
}

struct FxFixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
    seller: TestActor,
    buyer: TestActor,
}

async fn fx_fixture(pool: PgPool) -> FxFixture {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 20)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    fx.set_body(fx_body(RATE, now.timestamp_millis()));
    FxFixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn checkout_usd(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    PendingOrder {
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

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await
}

/// Delivers the pending `paykit.activate` outbox row so the order's invoice
/// is observing (and therefore claimable by the status poller).
async fn activate_bound(fixture: &FxFixture) {
    let client = fixture
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&fixture.app.pool, Some(client), fixture.app.clock.now(), 30)
        .await
        .expect("activation delivers");
}

fn quoted_sats() -> u128 {
    expected_quoted_sats(ORDER_TOTAL_MINOR, u128::from_str(RATE).unwrap())
}

fn assert_no_side_effects(facts: &FxOrderFacts, paykit: &FakePaykit, context: &str) {
    assert_eq!(facts.payment_method, None, "{context}: no method bound");
    assert!(
        !facts.stock_held,
        "{context}: bind refusal must not acquire a hold"
    );
    assert_eq!(
        facts.hold_source.as_deref(),
        None,
        "{context}: bind refusal must not write hold_source"
    );
    assert_eq!(facts.bitcoin_quoted_sats, None, "{context}: no quote row");
    assert!(
        paykit.requests().is_empty(),
        "{context}: no Paykit request left the process"
    );
}

async fn restock_listing(pool: &PgPool, seller_pubky: &str) {
    sqlx::query(
        "UPDATE listings SET available_quantity = total_quantity, reserved_quantity = 0, \
         sold_quantity = 0, state = 'available', server_revision = server_revision + 1 \
         WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller_pubky))
    .execute(pool)
    .await
    .expect("restock listing");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn feed_failures_are_typed_409_with_no_side_effects_and_clean_tracing(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    #[derive(Clone)]
    struct Buffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Buffer;
        fn make_writer(&self) -> Buffer {
            self.clone()
        }
    }
    let subscriber = tracing_subscriber::fmt()
        .with_writer(Buffer(buffer.clone()))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let cases: Vec<(String, &str)> = vec![
        (r#"{"tickers":[]}"#.to_string(), "fx_reference_unavailable"),
        (
            format!(
                r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"77197","base":"BTC","quote":"USD","lastUpdatedAt":{now_ms}}},{{"symbol":"BTCUSD","lastPrice":"77198","base":"BTC","quote":"USD","lastUpdatedAt":{now_ms}}}]}}"#,
                now_ms = now.timestamp_millis()
            ),
            "fx_reference_unavailable",
        ),
        (
            fx_body(RATE, (now - Duration::seconds(301)).timestamp_millis()),
            "fx_rate_stale",
        ),
        (
            fx_body(RATE, (now + Duration::seconds(61)).timestamp_millis()),
            "fx_rate_stale",
        ),
        (
            fx_body("83000", now.timestamp_millis()),
            "fx_deviation_exceeded",
        ),
    ];
    for (body, reason) in cases {
        fixture.fx.set_body(body);
        let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
        let (status, body) =
            bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{reason}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason), "{body}");
        let facts = order_facts(&pool, &order.order_id).await;
        assert_no_side_effects(&facts, &fixture.paykit, reason);
    }
    // Transport failure: the feed is down entirely.
    fixture.fx.set_status(500);
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "transport failure");

    let captured = String::from_utf8(buffer.lock().expect("buffer lock").clone()).unwrap();
    for line in captured.lines() {
        let lower = line.to_lowercase();
        assert!(
            !lower.contains("lastprice") && !lower.contains("tickers") && !lower.contains("btcusd"),
            "tracing must never carry feed body excerpts:\n{line}"
        );
        // A numeric rate may only ever surface inside an FX/quote log line;
        // none may (bare digit runs in ids/pubkeys are not rate leaks).
        if lower.contains("fx") || lower.contains("rate") || lower.contains("quote") {
            assert!(
                !line.contains(RATE) && !line.contains("83000"),
                "tracing must never carry numeric FX rates:\n{line}"
            );
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bootstrap_below_three_samples_is_refused_without_a_fetch(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();

    // Zero samples: the reference is unavailable and the feed is never dialed.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(
        fixture.fx.fetch_count(),
        0,
        "no feed fetch without a median"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "zero samples");

    // Two samples: still below the bootstrap floor.
    seed_fx_samples(&pool, RATE, now, 2).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(fixture.fx.fetch_count(), 0, "no feed fetch below 3 samples");
    let facts = order_facts(&pool, &order.order_id).await;
    assert_no_side_effects(&facts, &fixture.paykit, "two samples");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sampler_persists_samples_rejects_outliers_and_quotes_across_restart(pool: PgPool) {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool.clone()).await;
    let now = app.clock.now();
    // Three accepted samples straight through the production sampler.
    for back in [120_i64, 60, 0] {
        let t = now - Duration::seconds(back);
        fx.set_body(fx_body(RATE, t.timestamp_millis()));
        let accepted = sample_fx_rate(&pool, &fx.base_url, t)
            .await
            .expect("sampler runs");
        assert_eq!(accepted, 1, "sample at -{back}s accepted");
    }
    assert_eq!(sample_count(&pool).await, 3);

    // An outlier (> 500 bps off the persisted median) never reaches the table.
    let t = now + Duration::seconds(60);
    fx.set_body(fx_body("83000", t.timestamp_millis()));
    let accepted = sample_fx_rate(&pool, &fx.base_url, t)
        .await
        .expect("sampler runs");
    assert_eq!(accepted, 0, "outlier rejected at insert");
    assert_eq!(sample_count(&pool).await, 3, "outlier was not persisted");

    // A fresh process (new AppState, new feed double) on the SAME database
    // quotes from the persisted median: nothing is cached in memory.
    let (restarted, paykit2, fx2) = test_app_with_payments_and_fx(pool.clone()).await;
    let seller = new_actor(&restarted).await;
    let buyer = new_actor(&restarted).await;
    enable_bitcoin(&restarted, &paykit2, &seller).await;
    let (status, body) = execute(
        &restarted,
        &seller.token,
        &register_command(&seller.pubky, 5),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let now2 = restarted.clock.now();
    fx2.set_body(fx_body(RATE, now2.timestamp_millis()));
    let order = checkout_usd(&restarted, &seller, &buyer).await;
    let (status, body) = bind_bitcoin(&restarted, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind after restart failed: {body}");
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(i64::try_from(expected_quoted_sats(ORDER_TOTAL_MINOR, 77197)).unwrap()),
        "the restarted process quotes from the persisted reference"
    );
    assert_eq!(paykit.requests().len(), 0, "the first process is untouched");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bind_replay_conflict_and_race_yield_exactly_one_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats();

    // First bind: exactly one feed fetch, one Paykit prepare.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let quote = body["order"]["bitcoin_quote"].clone();
    assert_eq!(quote["quoted_sats"], json!(expected as i64));
    assert_eq!(quote["rate"], json!(RATE));
    assert_eq!(quote["source"], json!("blocktank"));
    assert_eq!(fixture.fx.fetch_count(), 1);
    assert_eq!(fixture.paykit.requests().len(), 1);

    // Same order + same method: the stored quote, byte-identical, zero fetch.
    let (status, replay) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "replay failed: {replay}");
    assert_eq!(
        replay["order"]["bitcoin_quote"], quote,
        "stored quote replayed"
    );
    assert_eq!(
        fixture.fx.fetch_count(),
        1,
        "replay must not refetch the feed"
    );
    assert_eq!(
        fixture.paykit.requests().len(),
        1,
        "replay must not re-prepare"
    );

    // A different method on the bound order is a conflict.
    let (status, body) = send(
        fixture.app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&fixture.buyer.token),
        &json!({ "method": "stripe" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("payment_method_already_bound")
    );

    // Concurrent bind race: exactly one quote, one hold, one fetch, one prepare.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (first, second) = tokio::join!(
        bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id),
        bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id),
    );
    assert_eq!(first.0, StatusCode::OK, "first: {}", first.1);
    assert_eq!(second.0, StatusCode::OK, "second: {}", second.1);
    assert_eq!(
        first.1["order"]["bitcoin_quote"], second.1["order"]["bitcoin_quote"],
        "both racers observe the same stored quote"
    );
    assert_eq!(
        fixture.fx.fetch_count(),
        2,
        "exactly one race winner fetched"
    );
    assert_eq!(
        fixture.paykit.requests().len(),
        2,
        "exactly one prepare won"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.bitcoin_quoted_sats, Some(expected as i64));
    assert!(facts.stock_held, "the winner holds the stock");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn lifecycle_prepare_activate_void_and_expiry_keep_the_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    // Prepare carries the quoted sats; quote_expires_at IS the Paykit expiry.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(
        fixture.paykit.requests()[0].amount_sats,
        expected,
        "the Paykit prepare body charges exactly the quoted sats"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.bitcoin_quoted_sats, Some(expected as i64));
    assert_eq!(
        facts.bitcoin_quote_expires_at, facts.paykit_expires_at,
        "quote_expires_at == the Paykit invoice expires_at"
    );
    assert_eq!(facts.paykit_expires_at, facts.hold_expires_at);
    let quote_expires_at = facts.bitcoin_quote_expires_at;

    // Activate carries the same prepared invoice through the outbox.
    let client = fixture
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&pool, Some(client), now, 30)
        .await
        .expect("activation delivers");
    let calls = fixture.paykit.calls();
    assert!(
        calls.iter().any(|call| call.path.ends_with("/activate")),
        "activation reached the Paykit seam: {calls:?}"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.bitcoin_quote_expires_at, quote_expires_at,
        "the quote expiry is immutable through activation"
    );

    // The payment window lapses on the activated order: the order cancels
    // and the quote evidence is retained.
    let later = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, later)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "cancelled");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "expiry keeps the quote"
    );
    assert_eq!(
        facts.bitcoin_quote_expires_at, quote_expires_at,
        "expiry keeps the quote expiry"
    );
    assert_eq!(facts.bitcoin_quote_source.as_deref(), Some("blocktank"));

    // A still-preparing bind is voided at the Paykit seam on expiry, and its
    // quote is retained identically.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    fixture.app.clock.set(now);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "second bind failed: {body}");
    assert!(
        expire_due_payment_windows(&fixture.app.state, later)
            .await
            .expect("window sweep runs")
            >= 1
    );
    drain_outbox(&pool, Some(client), later, 30)
        .await
        .expect("void delivers");
    let calls = fixture.paykit.calls();
    assert!(
        calls.iter().any(|call| call.path.ends_with("/void")),
        "the lapsed preparing bind is voided at the Paykit seam: {calls:?}"
    );
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "cancelled");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "void keeps the quote"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shared_manual_extension_and_late_settlement_retain_the_quote(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    // The 24-hour shared-manual hold extension retains the quote.
    fixture.paykit.set_allocation_mode("shared_manual");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    let deadline = now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS);
    assert_eq!(
        facts.hold_expires_at,
        Some(deadline),
        "the hold extends across the 24h seller-confirmation window"
    );
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "extension keeps the quote"
    );
    assert!(
        facts.bitcoin_quote_expires_at.is_some(),
        "extension keeps the quote expiry"
    );

    // Late settlement on an exclusive order whose window lapsed: cancelled,
    // manual review, every quote field retained as evidence.
    fixture.paykit.set_allocation_mode("exclusive");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2(
        "confirmed",
        true,
        "exclusive",
        Some("fx-late-txid"),
        Some(total_sats as u64),
        Some(2),
    );
    late["late_settlement"] = json!(true);
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.state, "paid");
    assert_eq!(payment_state(&pool, &order.order_id).await, "confirmed");
    assert_eq!(
        facts.bitcoin_quoted_sats,
        Some(expected as i64),
        "late settlement keeps the quote"
    );
    assert!(
        facts.bitcoin_quote_expires_at.is_some(),
        "late settlement keeps the quote expiry"
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "the late observation is frozen as evidence"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn quote_divergent_observation_clears_an_active_seller_window(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    fixture.paykit.set_allocation_mode("shared_manual");
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-window-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert_eq!(poll_now(&fixture.app, now).await, 1);
    fixture.paykit.set_allocation_mode("exclusive");
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "exclusive",
            Some("fx-window-txid"),
            Some(total_sats as u64 + 1),
            Some(3),
        ),
    );
    assert_eq!(poll_now(&fixture.app, now + Duration::seconds(60)).await, 1);

    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(facts.paykit_request_state.as_deref(), Some("confirmed"));
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    let (entered, deadline): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT paykit_seller_confirmation_entered_at, \
         paykit_seller_confirmation_deadline FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("seller window columns");
    assert_eq!(entered, None);
    assert_eq!(deadline, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn settlement_only_exact_non_late_exclusive_auto_pays(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    let settle = |observed_delta: i64, amount_matched: bool| {
        let fixture = &fixture;
        let pool = &pool;
        async move {
            let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
            let (status, body) =
                bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
            assert_eq!(status, StatusCode::OK, "bind failed: {body}");
            activate_bound(fixture).await;
            let total_sats = order_facts(pool, &order.order_id)
                .await
                .paykit_total_sats
                .expect("invoice total");
            let observed = total_sats + observed_delta;
            let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
            fixture.paykit.set_status(
                &reference,
                bitcoin_status_v2(
                    "confirmed",
                    amount_matched,
                    "exclusive",
                    Some("fx-settle-txid"),
                    u64::try_from(observed).ok(),
                    Some(2),
                ),
            );
            assert_eq!(poll_now(&fixture.app, now).await, 1);
            order.order_id
        }
    };

    // Exact: the invoice total lands on-chain; the order auto-pays.
    let order_id = settle(0, true).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(facts.state, "paid", "exact non-late exclusive auto-pays");
    assert_eq!(
        facts.paykit_observed_sats, facts.paykit_total_sats,
        "the exact observation is frozen"
    );

    // Over: paykit-server itself calls the amount matched, but the observed
    // sats exceed the frozen quote — a human must review.
    let order_id = settle(1_000, true).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(
        facts.state, "pending_payment",
        "over-payment never auto-pays"
    );
    assert_eq!(payment_state(&pool, &order_id).await, "manual_review");
    assert!(
        facts.bitcoin_quoted_sats.is_some(),
        "review keeps the quote"
    );

    // Under: paykit-server reports the shortfall; the same review class.
    let order_id = settle(-1_000, false).await;
    let facts = order_facts(&pool, &order_id).await;
    assert_eq!(
        facts.state, "pending_payment",
        "under-payment never auto-pays"
    );
    assert_eq!(payment_state(&pool, &order_id).await, "manual_review");
    assert!(
        facts.bitcoin_quoted_sats.is_some(),
        "review keeps the quote"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refunds_derive_from_the_frozen_observation_only(pool: PgPool) {
    install_log_capture();
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;

    // A late exclusive settlement observed exactly at the invoice total
    // lands in manual review; the refund derives from the FROZEN observed
    // sats (quote + nonce), never the fiat total and never the bare quote.
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let bound = order_facts(&pool, &order.order_id).await;
    let total_sats = bound.paykit_total_sats.expect("invoice total");
    let quoted_sats = bound.bitcoin_quoted_sats.expect("frozen quote");
    assert_ne!(
        total_sats, ORDER_TOTAL_MINOR as i64,
        "sats are not fiat minor"
    );
    assert_ne!(
        total_sats, quoted_sats,
        "the invoice total carries the nonce"
    );
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2(
        "confirmed",
        true,
        "exclusive",
        Some("fx-refund-txid"),
        Some(total_sats as u64),
        Some(2),
    );
    late["late_settlement"] = json!(true);
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&pool)
    .await
    .expect("sold-out so late money cannot complete");
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "frozen at first sight"
    );
    let (status, body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-1" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "refund resolve failed: {body}\n{}",
        captured_logs()
    );
    let external_refund: Value =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("external refund row");
    assert_eq!(
        external_refund["amount_minor"],
        json!(total_sats),
        "the refund derives from the frozen observation (quote + nonce sats)"
    );

    // A NULL frozen observation blocks the automatic refund entirely.
    restock_listing(&pool, &fixture.seller.pubky).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let after_window = now + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&fixture.app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    let mut late = bitcoin_status_v2("confirmed", true, "exclusive", None, None, None);
    late["late_settlement"] = json!(true);
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&pool)
    .await
    .expect("sold-out so late money cannot complete");
    fixture.paykit.set_status(&reference, late);
    assert!(poll_now(&fixture.app, after_window + Duration::seconds(60)).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        facts.paykit_observed_sats, None,
        "nothing observed, nothing frozen"
    );
    let (status, _body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-2" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a NULL frozen observation blocks the automatic refund"
    );
    assert_eq!(
        payment_state(&pool, &order.order_id).await,
        "manual_review",
        "the blocked refund leaves the review open"
    );

    // A later observation refreshes the JSON facts only; the frozen column
    // is assigned at ENTRY (A1) and is single-assignment (the shared-manual
    // status-only refresh path).
    fixture.paykit.set_allocation_mode("shared_manual");
    restock_listing(&pool, &fixture.seller.pubky).await;
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    fixture.app.clock.set(now);
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let total_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-refresh-txid"),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "A1: the first authority-establishing observation is frozen at entry"
    );
    // The second observation carries different facts (more confirmations).
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-refresh-txid"),
            Some(total_sats as u64),
            Some(9),
        ),
    );
    let _ = poll_now(&fixture.app, now + Duration::seconds(30)).await;
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "the refresh touches the JSON only, never the frozen column"
    );
    let observation = facts.paykit_observation.expect("observation facts");
    assert_eq!(
        observation["confirmations"],
        json!(9),
        "the later observation refreshed the JSON facts"
    );
    // Route the 24h window: the reaper's COALESCE freeze is a fallback for
    // pre-existing rows and must not win against the entry freeze.
    let routed = route_due_seller_confirmation_windows(
        &fixture.app.state,
        now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("seller-window reaper runs");
    assert!(routed >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(total_sats),
        "frozen at entry (A1); the reaper's fallback freeze never wins"
    );
    let observation_after = facts
        .paykit_observation
        .expect("observation facts retained");
    assert_eq!(observation_after["confirmations"], json!(9));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shared_manual_freeze_pins_the_first_observation_for_refunds(pool: PgPool) {
    install_log_capture();
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    fixture.paykit.set_allocation_mode("shared_manual");

    // N sats: the FIRST matching shared-manual observation establishes
    // authority, enters awaiting_seller_confirmation, and is frozen onto
    // the order in the same transaction (A1).
    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    activate_bound(&fixture).await;
    let n_sats = order_facts(&pool, &order.order_id)
        .await
        .paykit_total_sats
        .expect("invoice total");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).unwrap(), 1);
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "shared_manual",
            Some("fx-freeze-txid"),
            Some(n_sats as u64),
            Some(1),
        ),
    );
    assert!(poll_now(&fixture.app, now).await >= 1);
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        facts.paykit_observed_sats,
        Some(n_sats),
        "A1: frozen at entry, in the same transaction as the state change"
    );
    assert_eq!(
        facts
            .paykit_observation
            .as_ref()
            .expect("observation facts")["observed_sats"],
        json!(n_sats)
    );

    // A later status refresh reports M != N sats: the JSON observation
    // moves to M, the frozen column stays N.
    let m_sats = n_sats + 1_000;
    fixture.paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            false,
            "shared_manual",
            Some("fx-freeze-txid"),
            Some(m_sats as u64),
            Some(3),
        ),
    );
    let _ = poll_now(&fixture.app, now + Duration::seconds(30)).await;
    let facts = order_facts(&pool, &order.order_id).await;
    assert_eq!(
        facts.paykit_observed_sats,
        Some(n_sats),
        "the freeze pins the first observation"
    );
    assert_eq!(
        facts
            .paykit_observation
            .as_ref()
            .expect("observation facts")["observed_sats"],
        json!(m_sats),
        "the JSON carries the refreshed facts"
    );

    // Route the 24h window: the reaper's fallback freeze never wins.
    let routed = route_due_seller_confirmation_windows(
        &fixture.app.state,
        now + Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("seller-window reaper runs");
    assert!(routed >= 1);
    assert_eq!(payment_state(&pool, &order.order_id).await, "manual_review");
    assert_eq!(
        order_facts(&pool, &order.order_id)
            .await
            .paykit_observed_sats,
        Some(n_sats),
        "still the first observation after the reaper"
    );

    // The refund derives from the frozen first observation N, never M.
    let (status, body) = resolve_call(
        &fixture.app,
        &fixture.seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-freeze" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "refund resolve failed: {body}\n{}",
        captured_logs()
    );
    let external_refund: Value =
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).unwrap())
            .fetch_one(&pool)
            .await
            .expect("external refund row");
    assert_eq!(
        external_refund["amount_minor"],
        json!(n_sats),
        "the refund derives from the frozen first observation"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usd_bind_end_to_end_amount_and_role_scoped_projection(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    let now = fixture.app.clock.now();
    seed_fx_samples(&pool, RATE, now, 3).await;
    let expected = quoted_sats() as u64;

    let order = checkout_usd(&fixture.app, &fixture.seller, &fixture.buyer).await;
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    // The Paykit prepare body charges exactly the quoted sats.
    assert_eq!(fixture.paykit.requests().len(), 1);
    assert_eq!(fixture.paykit.requests()[0].amount_sats, expected);

    // Buyer and seller projections expose the quote, scoped like every
    // other order field; a stranger gets the same 404 as for any order.
    for (token, role) in [
        (&fixture.buyer.token, "buyer"),
        (&fixture.seller.token, "seller"),
    ] {
        let (status, body) = send(
            fixture.app.router.clone(),
            "GET",
            &format!("/v1/orders/{}", order.order_id),
            Some(token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{role}: {body}");
        let quote = &body["bitcoin_quote"];
        assert_eq!(quote["quoted_sats"], json!(expected as i64), "{role}");
        assert_eq!(quote["rate"], json!(RATE), "{role}");
        assert_eq!(quote["source"], json!("blocktank"), "{role}");
        assert!(
            quote["fetched_at"].is_string(),
            "{role}: fx_rate_fetched_at"
        );
        assert!(quote["expires_at"].is_string(), "{role}: quote_expires_at");
        // Fiat monetary fields are untouched by the conversion.
        assert_eq!(
            body["total"]["amount_minor"],
            json!(ORDER_TOTAL_MINOR as i64),
            "{role}"
        );
        assert_eq!(body["total"]["currency"], json!("USD"), "{role}");
    }
    let stranger = new_actor(&fixture.app).await;
    let (status, _) = send(
        fixture.app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order.order_id),
        Some(&stranger.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "strangers see no quote");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usd_offer_checkout_bitcoin_bind_keeps_settlement_and_merchandise_typed(pool: PgPool) {
    let fixture = fx_fixture(pool.clone()).await;
    seed_fx_samples(&pool, RATE, fixture.app.clock.now(), 3).await;
    let (status, body) = execute(
        &fixture.app,
        &fixture.buyer.token,
        &create_offer_command(&fixture.seller.pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        &fixture.app,
        &fixture.seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-00000000f101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    sqlx::query(
        "UPDATE offers SET accepted_listing_record_sha256 = $2, accepted_variant_id = $3 \
         WHERE id = $1",
    )
    .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer uuid"))
    .bind("a".repeat(64))
    .bind("boots_01")
    .execute(&fixture.app.pool)
    .await
    .expect("accepted snapshot normalized");
    let (award_id, listing_revision): (Uuid, i64) =
        sqlx::query_as("SELECT award_id, accepted_listing_revision FROM offers WHERE id = $1")
            .bind(Uuid::parse_str(OFFER_COMMAND_ID).expect("offer uuid"))
            .fetch_one(&fixture.app.pool)
            .await
            .expect("accepted award");
    let checkout = json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-00000000f102",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": award_id,
            "listing_aggregate_id": listing_aggregate(&fixture.seller.pubky),
            "listing_revision": listing_revision,
            "listing_record_sha256": "a".repeat(64),
            "variant_id": "boots_01",
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = execute(&fixture.app, &fixture.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "offer checkout failed: {body}");
    let order_id = body["result"]["order"]["id"].as_str().expect("order id");
    let (status, body) = bind_bitcoin(&fixture.app, &fixture.buyer.token, order_id).await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    let (status, projection) = send(
        fixture.app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&fixture.buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "projection failed: {projection}");
    assert_eq!(
        projection["payment"]["amount"],
        json!({
            "amount_minor": projection["bitcoin_payable"]["amount_minor"],
            "currency": "SAT",
            "exponent": 0
        })
    );
    assert_eq!(
        projection["payment"]["merchandise_amount"],
        json!({"amount_minor": 10_000, "currency": "USD", "exponent": 2})
    );
    assert_eq!(
        projection["merchandise_total"],
        json!({"amount_minor": 10_000, "currency": "USD", "exponent": 2})
    );
    assert_eq!(projection["priced_from"], json!("offer"));
}
