//! Released Paykit attempts stay observed (migration 0042): a re-bind never
//! discards the earlier attempt's reference, money confirmed on it takes the
//! late-money path, and attempts released before 0042 are backfilled.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{
    create_sat_order, enable_bitcoin, poll_now, status_confirmed, status_detected,
};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::paykit_attempts::{
    list_needs_review, resolve_needs_review, ReviewOutcome,
};
use marketplace_service::payments::{attempt_reference, legacy_order_reference};
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const MIGRATION: &str = include_str!("../migrations/0042_paykit_superseded_attempts.sql");

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) {
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
}

async fn drain(app: &TestApp) {
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("drain runs");
}

async fn current_invoice(pool: &PgPool, order_id: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT paykit_invoice_id FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn released(pool: &PgPool, order_id: Uuid) -> Vec<(Uuid, Option<String>, String)> {
    sqlx::query_as(
        "SELECT invoice_id, reference, state FROM paykit_superseded_attempts \
         WHERE order_id = $1 ORDER BY released_at, invoice_id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .expect("released attempts")
}

async fn payment_facts(pool: &PgPool, order_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT state, review_reason FROM payments WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("payment row")
}

/// Binds bitcoin; attempt 1 is activated at paykit, but its activation
/// response is lost and the retry meets `invoice_finalized` (paykit already
/// moved the invoice into its expiry tail). The marketplace releases the
/// bind. Returns the order id and attempt 1's invoice.
async fn released_first_attempt(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (Uuid, Uuid) {
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(app, paykit, seller).await;
    let order = create_sat_order(app, seller, buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    bind_bitcoin(app, &buyer.token, &order.order_id).await;
    let first = current_invoice(&app.pool, order_id).await;
    paykit.set_invoice_state(first, "expired_tail");
    paykit.script_activate(
        first,
        vec![FakePaykitReply::Error(409, "invoice_finalized".to_string())],
    );
    drain(app).await;
    let (method, activation): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT payment_method, paykit_activation_state FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(method, None, "the bind is released");
    assert_eq!(activation.as_deref(), Some("voided"));
    (order_id, first)
}

fn late(mut status: Value) -> Value {
    status["late_settlement"] = json!(true);
    status
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_a_released_attempt_after_a_rebind_reaches_late_money(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    let first_reference = attempt_reference(order_id, 1);
    assert_eq!(
        released(&pool, order_id).await,
        vec![(first, Some(first_reference.clone()), "watching".to_string())]
    );

    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    drain(&app).await;
    let second = current_invoice(&pool, order_id).await;
    let second_reference = attempt_reference(order_id, 2);
    assert_ne!(second, first);
    assert_eq!(
        released(&pool, order_id).await,
        vec![(first, Some(first_reference.clone()), "watching".to_string())],
        "the re-bind keeps attempt 1 recorded and watched"
    );

    paykit.set_status(
        &first_reference,
        late(status_confirmed("exclusive", true, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);

    // Attempt 2 still holds the stock, so the late money waits for a human.
    assert_eq!(
        payment_facts(&pool, order_id).await,
        (
            "manual_review".to_string(),
            Some("late_settlement".to_string())
        )
    );
    let (invoice, reference, request_state, method): (
        Uuid,
        String,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT paykit_invoice_id, paykit_request_reference, paykit_request_state, \
         payment_method FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("order row");
    assert_eq!(invoice, first, "the paid attempt is the attempt of record");
    assert_eq!(reference, first_reference);
    assert_eq!(request_state.as_deref(), Some("confirmed"));
    assert_eq!(method.as_deref(), Some("bitcoin"));
    let mut rows = released(&pool, order_id).await;
    rows.sort_by_key(|(invoice, ..)| *invoice == second);
    assert_eq!(
        rows,
        vec![
            (first, Some(first_reference), "late_money".to_string()),
            (
                second,
                Some(second_reference.clone()),
                "watching".to_string()
            ),
        ],
        "attempt 2 is released in its place and still watched"
    );

    // Attempt 2 settling as well cannot complete the order on its own.
    paykit.set_status(&second_reference, status_confirmed("exclusive", true, 2));
    poll_now(&app, app.clock.now() + chrono::Duration::seconds(600)).await;
    assert_eq!(
        payment_facts(&pool, order_id).await,
        (
            "manual_review".to_string(),
            Some("late_settlement".to_string())
        )
    );
    let second_state: String = sqlx::query_scalar(
        "SELECT state FROM paykit_superseded_attempts WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(order_id)
    .bind(second)
    .fetch_one(&pool)
    .await
    .expect("attempt 2 row");
    assert_eq!(second_state, "needs_review");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_released_attempt_without_money_closes_after_its_tail(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    let expires_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT expires_at FROM paykit_superseded_attempts WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("expiry recorded");

    poll_now(&app, expires_at + chrono::Duration::hours(23)).await;
    assert_eq!(released(&pool, order_id).await[0].2, "watching");
    poll_now(&app, expires_at + chrono::Duration::hours(24)).await;
    assert_eq!(
        released(&pool, order_id).await,
        vec![(
            first,
            Some(attempt_reference(order_id, 1)),
            "closed_unpaid".to_string()
        )]
    );
    assert_eq!(
        payment_facts(&pool, order_id).await,
        ("awaiting_entitlement".to_string(), None)
    );
}

// Production rows released before 0042 carry the order-id reference, or
// none at all once a fiat bind replaced it.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0042_backfills_attempts_released_before_it(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let unbound_buyer = new_actor(&app).await;
    let (unbound, unbound_invoice) =
        released_first_attempt(&app, &paykit, &seller, &unbound_buyer).await;
    let fiat_seller = new_actor(&app).await;
    let fiat_buyer = new_actor(&app).await;
    let (fiat, fiat_invoice) =
        released_first_attempt(&app, &paykit, &fiat_seller, &fiat_buyer).await;
    sqlx::query("DELETE FROM paykit_superseded_attempts")
        .execute(&pool)
        .await
        .expect("pre-0042 shape");
    sqlx::query("UPDATE orders SET paykit_request_reference = $2 WHERE id = $1")
        .bind(unbound)
        .bind(legacy_order_reference(unbound))
        .execute(&pool)
        .await
        .expect("the pre-0042 order-id reference");
    sqlx::query(
        "UPDATE orders SET paykit_request_reference = NULL, payment_method = 'stripe' \
         WHERE id = $1",
    )
    .bind(fiat)
    .execute(&pool)
    .await
    .expect("a fiat bind replaced the reference");

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0042 reruns");
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0042 reruns idempotently");
    assert_eq!(
        released(&pool, unbound).await,
        vec![(
            unbound_invoice,
            Some(legacy_order_reference(unbound)),
            "watching".to_string()
        )]
    );
    assert_eq!(
        released(&pool, fiat).await,
        vec![(fiat_invoice, None, "watching".to_string())]
    );

    for order_id in [unbound, fiat] {
        paykit.set_status(
            &legacy_order_reference(order_id),
            late(status_confirmed("exclusive", true, 2)),
        );
    }
    assert!(poll_now(&app, app.clock.now()).await >= 2);
    assert_eq!(released(&pool, unbound).await[0].2, "late_money");
    let (state, _reason) = payment_facts(&pool, unbound).await;
    assert_ne!(state, "awaiting_entitlement", "the late money was routed");
    assert_eq!(
        released(&pool, fiat).await[0].2,
        "needs_review",
        "bitcoin money on an order now bound to a fiat method is held for a human"
    );
    let method: Option<String> =
        sqlx::query_scalar("SELECT payment_method FROM orders WHERE id = $1")
            .bind(fiat)
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert_eq!(
        method.as_deref(),
        Some("stripe"),
        "the fiat bind is untouched"
    );
}

/// Bind, then the buyer cancels while the request is still preparing: the
/// production shape the 0042 backfill must cover (order cancelled, payment
/// expired, activation voided, request state cleared, method still bitcoin).
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancelled_preparing_attempt_is_recorded_backfilled_and_routed(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    bind_bitcoin(&app, &buyer.token, &order.order_id).await;
    let invoice = current_invoice(&pool, order_id).await;
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("order revision");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order.order_id,
            revision,
            json!({ "reason": "Changed mind" }),
            (Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    let shape: (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
    ) = sqlx::query_as(
        "SELECT o.state, p.state, o.paykit_activation_state, o.paykit_request_state, \
             o.payment_method, o.stock_held FROM orders o JOIN payments p ON p.order_id = o.id \
             WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("order shape");
    assert_eq!(
        shape,
        (
            "cancelled".to_string(),
            "expired".to_string(),
            Some("voided".to_string()),
            None,
            Some("bitcoin".to_string()),
            false
        ),
        "the production shape"
    );
    assert_eq!(
        released(&pool, order_id).await,
        vec![(
            invoice,
            Some(attempt_reference(order_id, 1)),
            "watching".to_string()
        )],
        "the cancel records the released attempt"
    );

    // As in production: released before 0042, under the order-id reference.
    sqlx::query("DELETE FROM paykit_superseded_attempts")
        .execute(&pool)
        .await
        .expect("pre-0042 shape");
    sqlx::query("UPDATE orders SET paykit_request_reference = $2 WHERE id = $1")
        .bind(order_id)
        .bind(legacy_order_reference(order_id))
        .execute(&pool)
        .await
        .expect("the pre-0042 order-id reference");
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0042 reruns");
    assert_eq!(
        released(&pool, order_id).await,
        vec![(
            invoice,
            Some(legacy_order_reference(order_id)),
            "watching".to_string()
        )],
        "0042 backfills the bitcoin-labelled release"
    );

    paykit.set_status(
        &legacy_order_reference(order_id),
        late(status_confirmed("exclusive", true, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(released(&pool, order_id).await[0].2, "late_money");
    let (state, _reason) = payment_facts(&pool, order_id).await;
    assert!(
        matches!(state.as_str(), "confirmed" | "manual_review"),
        "the late money was routed, not left {state}"
    );
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Quote {
    paykit_invoice_id: Uuid,
    paykit_total_sats: i64,
    paykit_prepare_expires_at: Option<DateTime<Utc>>,
    bitcoin_quote_rate: Option<String>,
    bitcoin_quoted_sats: Option<i64>,
    bitcoin_quote_fetched_at: Option<DateTime<Utc>>,
    bitcoin_quote_expires_at: Option<DateTime<Utc>>,
    bitcoin_quote_currency: Option<String>,
    bitcoin_quote_exponent: Option<i16>,
    bitcoin_quote_spread_bps: Option<i32>,
    bitcoin_quote_source: Option<String>,
}

async fn quote(pool: &PgPool, order_id: Uuid) -> Quote {
    sqlx::query_as(
        "SELECT paykit_invoice_id, paykit_total_sats, paykit_prepare_expires_at, \
         bitcoin_quote_rate::text AS bitcoin_quote_rate, bitcoin_quoted_sats, \
         bitcoin_quote_fetched_at, bitcoin_quote_expires_at, \
         bitcoin_quote_currency::text AS bitcoin_quote_currency, bitcoin_quote_exponent, \
         bitcoin_quote_spread_bps, bitcoin_quote_source FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order quote")
}

async fn seed_usd_samples(pool: &PgPool, rate: &str, now: DateTime<Utc>) {
    for back in 0..3i64 {
        let accepted = now - chrono::Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank')",
        )
        .bind(rate)
        .bind(accepted)
        .bind(DateTime::from_timestamp(bucket_ts, 0).expect("bucket"))
        .execute(pool)
        .await
        .expect("fx sample");
    }
}

// A USD order re-bound at a different rate: money on the first attempt
// restores the first attempt's quote with its invoice.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_restored_attempt_brings_back_its_own_quote(pool: PgPool) {
    let (app, paykit, fx) = test_app_with_payments_and_fx(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 20)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    seed_usd_samples(&pool, "77197", now).await;
    fx.set_body(common::fx_feed::fx_body("77197", now.timestamp_millis()));
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing row");
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order_id = Uuid::parse_str(body["result"]["orders"][0]["id"].as_str().expect("id"))
        .expect("order uuid");

    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    let first = quote(&pool, order_id).await;
    assert_eq!(first.bitcoin_quote_rate.as_deref(), Some("77197"));
    paykit.set_invoice_state(first.paykit_invoice_id, "expired_tail");
    paykit.script_activate(
        first.paykit_invoice_id,
        vec![FakePaykitReply::Error(409, "invoice_finalized".to_string())],
    );
    drain(&app).await;

    app.clock.set(now + chrono::Duration::seconds(30));
    fx.set_body(common::fx_feed::fx_body(
        "78500",
        app.clock.now().timestamp_millis(),
    ));
    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    drain(&app).await;
    let second = quote(&pool, order_id).await;
    assert_eq!(second.bitcoin_quote_rate.as_deref(), Some("78500"));
    assert_ne!(second.bitcoin_quoted_sats, first.bitcoin_quoted_sats);
    assert_ne!(
        second.bitcoin_quote_fetched_at,
        first.bitcoin_quote_fetched_at
    );

    paykit.set_status(
        &attempt_reference(order_id, 1),
        late(status_confirmed("exclusive", true, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(
        quote(&pool, order_id).await,
        first,
        "the paid attempt's invoice, total, prepare expiry, and quote are restored together"
    );
}

async fn status_calls(paykit: &FakePaykit, reference: &str) -> usize {
    paykit
        .calls()
        .into_iter()
        .filter(|call| call.path == "/transactions/status" && call.body["bundle_id"] == reference)
        .count()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn released_attempt_checks_back_off_and_a_stale_detection_escalates(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    let reference = attempt_reference(order_id, 1);
    let every = chrono::Duration::seconds(app.state.config.paykit_poll_seconds);
    let t0 = app.clock.now();

    poll_now(&app, t0).await;
    assert_eq!(status_calls(&paykit, &reference).await, 1);
    poll_now(&app, t0 + every).await;
    assert_eq!(status_calls(&paykit, &reference).await, 2);
    // The next check waits twice as long.
    poll_now(&app, t0 + every * 2).await;
    assert_eq!(status_calls(&paykit, &reference).await, 2, "not yet due");
    poll_now(&app, t0 + every * 3).await;
    assert_eq!(status_calls(&paykit, &reference).await, 3);

    // The gap never exceeds an hour.
    sqlx::query(
        "UPDATE paykit_superseded_attempts SET check_count = 40, next_check_at = NULL \
         WHERE order_id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("many checks");
    let t1 = t0 + chrono::Duration::hours(1);
    paykit.set_status(&reference, status_detected("exclusive", 0));
    poll_now(&app, t1).await;
    let (next, detected_at): (DateTime<Utc>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT next_check_at, detected_at FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("attempt row");
    assert_eq!(next, t1 + chrono::Duration::seconds(3600));
    assert_eq!(detected_at, Some(t1));

    // A detection that never confirms goes to an operator after seven days.
    poll_now(&app, t1 + chrono::Duration::days(6)).await;
    assert_eq!(released(&pool, order_id).await[0].2, "watching");
    poll_now(&app, t1 + chrono::Duration::days(7)).await;
    let (state, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("attempt row");
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("needs_review", Some("detected_unconfirmed"))
    );
    let calls = status_calls(&paykit, &reference).await;
    poll_now(&app, t1 + chrono::Duration::days(8)).await;
    assert_eq!(
        status_calls(&paykit, &reference).await,
        calls,
        "a held attempt is no longer polled"
    );
}

// Money on a released attempt after the order was already paid by its
// current attempt: held for an operator, who records the refund.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_operator_resolves_a_released_attempt_held_for_review(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    drain(&app).await;
    paykit.set_status(
        &attempt_reference(order_id, 2),
        status_confirmed("exclusive", true, 2),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(payment_facts(&pool, order_id).await.0, "confirmed");

    paykit.set_status(
        &attempt_reference(order_id, 1),
        late(status_confirmed("exclusive", true, 2)),
    );
    poll_now(&app, app.clock.now() + chrono::Duration::seconds(60)).await;
    let held = list_needs_review(&pool).await.expect("list");
    assert_eq!(held.len(), 1);
    assert_eq!(
        (
            held[0].order_id,
            held[0].invoice_id,
            held[0].review_reason.as_str()
        ),
        (order_id, first, "payment_settled")
    );
    assert_eq!(held[0].buyer_pubky, buyer.pubky);

    let now = app.clock.now();
    assert!(resolve_needs_review(
        &pool,
        order_id,
        first,
        ReviewOutcome::Refunded,
        "  ",
        "ops",
        now
    )
    .await
    .is_err());
    assert!(resolve_needs_review(
        &pool,
        order_id,
        first,
        ReviewOutcome::Refunded,
        "refund-txid-9f2c",
        "ops@synonym",
        now
    )
    .await
    .expect("resolve"));
    let resolved: (String, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state, resolution_outcome, resolution_note, resolved_by \
         FROM paykit_superseded_attempts WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(order_id)
    .bind(first)
    .fetch_one(&pool)
    .await
    .expect("attempt row");
    assert_eq!(
        resolved,
        (
            "resolved".to_string(),
            Some("refunded".to_string()),
            Some("refund-txid-9f2c".to_string()),
            Some("ops@synonym".to_string())
        )
    );
    assert!(!resolve_needs_review(
        &pool,
        order_id,
        first,
        ReviewOutcome::Dismissed,
        "again",
        "ops",
        now
    )
    .await
    .expect("second resolve"));
    assert!(list_needs_review(&pool).await.expect("list").is_empty());
    assert_eq!(
        payment_facts(&pool, order_id).await.0,
        "confirmed",
        "the order's settlement is untouched"
    );
}

async fn attempt_state(pool: &PgPool, order_id: Uuid, invoice: Uuid) -> (String, Option<String>) {
    sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts \
         WHERE order_id = $1 AND invoice_id = $2",
    )
    .bind(order_id)
    .bind(invoice)
    .fetch_one(pool)
    .await
    .expect("attempt row")
}

// A released attempt's order later taken over by Locks, which pins the
// payment's adapter without a payment method: money on the released
// attempt must not move the payment off Locks.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_a_released_attempt_of_a_locks_payment_is_held_for_review(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    sqlx::query(
        "UPDATE payments SET revision = revision + 1, adapter = 'locks' WHERE order_id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("Locks attach pins the adapter");

    paykit.set_status(
        &attempt_reference(order_id, 1),
        late(status_confirmed("exclusive", true, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(
        attempt_state(&pool, order_id, first).await,
        ("needs_review".to_string(), Some("other_rail".to_string()))
    );
    let (adapter, state, method, activation): (String, String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT p.adapter, p.state, o.payment_method, o.paykit_activation_state \
         FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.id = $1",
        )
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("order and payment");
    assert_eq!(
        (
            adapter.as_str(),
            state.as_str(),
            method,
            activation.as_deref()
        ),
        ("locks", "awaiting_entitlement", None, Some("voided")),
        "the Locks payment and the order are untouched"
    );
}

// Money of the wrong amount on a released attempt restores that attempt and
// goes to manual review, never to completion.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_mismatched_amount_on_a_released_attempt_goes_to_manual_review(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    drain(&app).await;

    paykit.set_status(
        &attempt_reference(order_id, 1),
        late(status_confirmed("exclusive", false, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(
        payment_facts(&pool, order_id).await,
        (
            "manual_review".to_string(),
            Some("amount_mismatch".to_string())
        )
    );
    assert_eq!(current_invoice(&pool, order_id).await, first);
    assert_eq!(
        attempt_state(&pool, order_id, first).await,
        ("late_money".to_string(), None)
    );
}

// The order's current attempt is still preparing when money confirms on
// the released one: the preparing attempt is released in its place and is
// never activated.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_preparing_current_attempt_is_released_and_never_activated(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, first) = released_first_attempt(&app, &paykit, &seller, &buyer).await;
    bind_bitcoin(&app, &buyer.token, &order_id.to_string()).await;
    let second = current_invoice(&pool, order_id).await;

    paykit.set_status(
        &attempt_reference(order_id, 1),
        late(status_confirmed("exclusive", true, 2)),
    );
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(current_invoice(&pool, order_id).await, first);
    assert_eq!(
        attempt_state(&pool, order_id, second).await,
        ("watching".to_string(), None),
        "the preparing attempt is released and watched"
    );
    let activation: Option<String> =
        sqlx::query_scalar("SELECT paykit_activation_state FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert_eq!(activation.as_deref(), Some("active"));

    drain(&app).await;
    let activations_of_second = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path == format!("/v0/payment-requests/{second}/activate"))
        .count();
    assert_eq!(
        activations_of_second, 0,
        "the preparing invoice is never published"
    );
    assert_eq!(
        payment_facts(&pool, order_id).await,
        (
            "manual_review".to_string(),
            Some("late_settlement".to_string())
        )
    );
}
