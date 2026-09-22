//! Exclusive checkout hold (#50): 900 s park, rail re-arm, late_completion /
//! refund_required fork, operator SQL, and same-ms per-rail pipelines.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{
    bound_order, enable_bitcoin, poll_now, resolve_call, status_confirmed,
};
use common::*;
use common::{FakePaykit, FakeStripeSession, TestActor};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::payments::order_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const HOLDING_COPY: &str =
    "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks.";
const RESTRICTED_KEY: &str = "rk_test_51NxyzMarketplace";
const REFUND_REQUIRED_PAID: &str =
    "This payment cannot complete the order. Return the funds, then record the refund.";

async fn put_config(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        body,
    )
    .await
}

async fn bind_method(
    app: &TestApp,
    token: &str,
    order_id: &str,
    method: &str,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": method }),
    )
    .await
}

fn full_config_body() -> Value {
    json!({
        "bitcoin_enabled": true,
        "stripe_payment_link": "https://buy.stripe.com/test_abc123",
        "stripe_restricted_key": RESTRICTED_KEY,
        "paypal_merchant_email": "merchant@example.com",
    })
}

async fn hold_row(pool: &PgPool, order_id: &str) -> (bool, Option<String>, Option<String>) {
    let (stock_held, expires, source): (bool, Option<DateTime<Utc>>, Option<String>) =
        sqlx::query_as(
            "SELECT stock_held, hold_expires_at, hold_source FROM orders WHERE id = $1::uuid",
        )
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row");
    (
        stock_held,
        expires.map(marketplace_service::clock::format_timestamp),
        source,
    )
}

fn ipn_body(order_id: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in [
        ("payment_status", "Completed"),
        ("receiver_email", "merchant@example.com"),
        ("business", "merchant@example.com"),
        ("mc_gross", "137.00"),
        ("mc_currency", "USD"),
        ("custom", order_id),
        ("txn_id", "7XP31449AB123456C"),
    ] {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

/// Two buyers race `checkout.create` on a qty-1 listing. Exactly one holds;
/// the loser is 409 with the holding copy and never receives an order.
async fn race_qty_one_checkouts(
    app: &TestApp,
    seller_pubky: &str,
    first: &TestActor,
    second: &TestActor,
    prefix: u16,
) -> (String, String, String) {
    let token_a = first.token.clone();
    let token_b = second.token.clone();
    let router_a = app.router.clone();
    let router_b = app.router.clone();
    let cmd_a = checkout_command_with_id(seller_pubky, &indexed_command_id(prefix, 1));
    let cmd_b = checkout_command_with_id(seller_pubky, &indexed_command_id(prefix, 2));
    let (res_a, res_b) = tokio::join!(
        common::send(router_a, "POST", "/v1/commands", Some(&token_a), &cmd_a),
        common::send(router_b, "POST", "/v1/commands", Some(&token_b), &cmd_b),
    );
    let mut wins = 0;
    let mut winner: Option<(String, String, String)> = None;
    for (actor, (status, body)) in [(first, res_a), (second, res_b)] {
        if body["ok"] == json!(true) {
            assert_eq!(status, StatusCode::OK);
            wins += 1;
            winner = Some((
                actor.token.clone(),
                body["result"]["orders"][0]["id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
                body["result"]["payments"][0]["id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            ));
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["message"], json!(HOLDING_COPY));
        }
    }
    assert_eq!(wins, 1);
    winner.expect("one winner")
}

async fn listing_revision(pool: &PgPool, seller_pubky: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller_pubky))
        .fetch_one(pool)
        .await
        .expect("revision")
}

async fn bound_sat_qty_one(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
    mode: &str,
) -> (String, String) {
    paykit.set_allocation_mode(mode);
    enable_bitcoin(app, paykit, seller).await;
    let (status, body) = execute(app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] =
        json!(listing_revision(&app.pool, &seller.pubky).await);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = bind_method(app, &buyer.token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    (
        order_id.clone(),
        order_reference(Uuid::parse_str(&order_id).unwrap()),
    )
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn qty_two_second_checkout_succeeds(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    let (status, first_body) = execute(
        &app,
        &first.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xb000, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first_body}");
    assert_eq!(first_body["result"]["orders"][0]["stock_held"], json!(true));

    let mut second_cmd = checkout_command_with_id(&seller.pubky, &indexed_command_id(0xb000, 2));
    second_cmd["payload"]["lines"][0]["expected_revision"] = json!(2);
    let (status, second_body) = execute(&app, &second.token, &second_cmd).await;
    assert_eq!(status, StatusCode::OK, "{second_body}");
    assert_eq!(
        second_body["result"]["orders"][0]["stock_held"],
        json!(true)
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bind_rearms_fiat_and_bitcoin_windows(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    assert_eq!(
        hold_row(&app.pool, &order.order_id).await,
        (true, Some(ts_after(900)), Some("checkout".into()))
    );

    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        hold_row(&app.pool, &order.order_id).await,
        (true, Some(ts_after(3_600)), Some("bind".into()))
    );

    let seller_btc = new_actor(&app).await;
    let buyer_btc = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller_btc).await;
    let sat = common::paykit_review::create_sat_order(&app, &seller_btc, &buyer_btc).await;
    assert_eq!(
        hold_row(&app.pool, &sat.order_id).await,
        (true, Some(ts_after(900)), Some("checkout".into()))
    );
    let (status, body) = bind_method(&app, &buyer_btc.token, &sat.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        hold_row(&app.pool, &sat.order_id).await,
        (true, Some(ts_after(7_200)), Some("bind".into()))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_hold_expires_at_901_and_restocks(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();

    let still =
        expire_due_payment_windows(&app.state, app.clock.now() + chrono::Duration::seconds(899))
            .await
            .expect("sweep");
    assert_eq!(still, 0);
    let expired =
        expire_due_payment_windows(&app.state, app.clock.now() + chrono::Duration::seconds(901))
            .await
            .expect("sweep");
    assert_eq!(expired, 1);
    let (state, stock_held, reason): (String, bool, Option<String>) = sqlx::query_as(
        "SELECT state, stock_held, cancellation_reason FROM orders WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order");
    assert_eq!(state, "cancelled");
    assert!(!stock_held);
    assert_eq!(reason.as_deref(), Some("payment window elapsed"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn operator_sql_selects_only_unbound_pending(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let now = app.clock.now();
    let unbound = Uuid::new_v4();
    let bound = Uuid::new_v4();
    let auction = Uuid::new_v4();
    for (id, method, auction_id) in [
        (unbound, None, None),
        (bound, Some("stripe"), None),
        (auction, None, Some("listing:seller_boots")),
    ] {
        sqlx::query(
            "INSERT INTO orders (id, auction_aggregate_id, buyer_pubky, seller_pubky, \
             revision, state, lines, subtotal_minor, shipping_minor, total_minor, \
             currency, exponent, guarantee_policy_version, payment_id, payment_method, \
             stock_held, created_at, updated_at) \
             VALUES ($1, $2, 'buyer', 'seller', 1, 'pending_payment', '[]'::jsonb, 0, 0, 0, \
             'USD', 2, 1, $3, $4, false, $5, $5)",
        )
        .bind(id)
        .bind(auction_id)
        .bind(Uuid::new_v4())
        .bind(method)
        .bind(now)
        .execute(&pool)
        .await
        .expect("legacy-shaped insert");
    }

    let unbound_pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM orders WHERE state = 'pending_payment' \
         AND stock_held = false AND auction_aggregate_id IS NULL \
         AND drop_aggregate_id IS NULL AND payment_method IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("dry-run unbound");
    assert_eq!(unbound_pending, 1);
    let bound_unheld: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM orders WHERE state = 'pending_payment' \
         AND stock_held = false AND payment_method IS NOT NULL \
         AND auction_aggregate_id IS NULL AND drop_aggregate_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("dry-run bound");
    assert_eq!(bound_unheld, 1);

    let source = include_str!("../../../scripts/expire-unbound-pending.sql");
    assert!(source.contains("UPDATE orders"));
    assert!(
        source
            .lines()
            .filter(|line| line.trim_start().starts_with("UPDATE "))
            .all(|line| line.trim_start().starts_with("--")),
        "operator UPDATE must stay commented"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn same_ms_stripe_pipelines_one_winner(pool: PgPool) {
    let (app, stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    stripe.accept_key(RESTRICTED_KEY);
    let seller = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (token, order_id, _payment_id) =
        race_qty_one_checkouts(&app, &seller.pubky, &first, &second, 0xb100).await;

    let (status, body) = bind_method(&app, &token, &order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let total = body["order"]["total"]["amount_minor"]
        .as_i64()
        .expect("total");
    stripe.add_session(FakeStripeSession {
        id: "cs_paid_match".into(),
        client_reference_id: order_id.clone(),
        payment_status: "paid".into(),
        amount_total: total,
        currency: "usd".into(),
    });
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/verify"),
        Some(&token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let paid: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order");
    assert_eq!(paid, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn same_ms_paypal_pipelines_one_winner(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let seller = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (token, order_id, _payment_id) =
        race_qty_one_checkouts(&app, &seller.pubky, &first, &second, 0xb101).await;
    let (status, body) = bind_method(&app, &token, &order_id, "paypal").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        ipn_body(&order_id).into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let paid: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order");
    assert_eq!(paid, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn same_ms_bitcoin_exclusive_pipelines_one_winner(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    execute(&app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
    paykit.set_allocation_mode("exclusive");
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (token, order_id, _payment_id) =
        race_qty_one_checkouts(&app, &seller.pubky, &first, &second, 0xb102).await;
    let (status, body) = bind_method(&app, &token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation");
    let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let paid: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order");
    assert_eq!(paid, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn same_ms_bitcoin_ln_pipelines_one_winner(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    execute(&app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
    paykit.set_allocation_mode("exclusive");
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (token, order_id, _payment_id) =
        race_qty_one_checkouts(&app, &seller.pubky, &first, &second, 0xb103).await;
    let (status, body) = bind_method(&app, &token, &order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation");
    let reference = order_reference(Uuid::parse_str(&order_id).unwrap());
    paykit.set_status(&reference, status_confirmed("exclusive", true, 1));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let paid: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order");
    assert_eq!(paid, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn same_ms_sandbox_pipelines_one_winner(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (token, _order_id, payment_id) =
        race_qty_one_checkouts(&app, &seller.pubky, &first, &second, 0xb104).await;
    let (status, body) = execute(
        &app,
        &token,
        &payment_command(&payment_id, 1, "confirmed", 1, 601),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_late_completion_when_stock_is_free(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, payment_state, reason): (String, String, Option<String>) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("facts");
    assert_eq!(order_state, "paid");
    assert_eq!(payment_state, "confirmed");
    assert!(reason.is_none());
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.payment_confirmed'",
    )
    .fetch_one(&pool)
    .await
    .expect("notifications");
    assert!(n >= 2, "late_completion notifies buyer and seller: {n}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_refund_required_when_second_buyer_holds(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let first = new_actor(&app).await;
    let (order_id, reference) =
        bound_sat_qty_one(&app, &paykit, &seller, &first, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    let second = new_actor(&app).await;
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] =
        json!(listing_revision(&pool, &seller.pubky).await);
    let (status, body) = execute(&app, &second.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["orders"][0]["stock_held"], json!(true));

    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, payment_state, reason): (String, String, Option<String>) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).unwrap())
    .fetch_one(&pool)
    .await
    .expect("facts");
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(reason.as_deref(), Some("refund_required"));

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("refund_required"));
    assert_eq!(body["error"]["message"], json!(REFUND_REQUIRED_PAID));
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.payment_refund_required'",
    )
    .fetch_one(&pool)
    .await
    .expect("refund notifications");
    assert!(n >= 2, "refund_required notifies both parties: {n}");
}

/// Unheld `pending_payment` (the bound-zombie class the sweep skips) with
/// stock gone: late-cancel must bump `orders.revision` so
/// `refund.record_external` can write the next event instead of colliding
/// on `events_one_per_aggregate_revision`.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn late_cancel_then_record_external_keeps_revision_monotonic(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, reference) =
        bound_sat_qty_one(&app, &paykit, &seller, &buyer, "exclusive").await;
    let order_uuid = Uuid::parse_str(&order_id).unwrap();

    sqlx::query("UPDATE orders SET stock_held = false, hold_expires_at = NULL WHERE id = $1")
        .bind(order_uuid)
        .execute(&pool)
        .await
        .expect("force unheld zombie");
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&pool)
    .await
    .expect("deplete stock");

    let after = app.clock.now() + chrono::Duration::seconds(7300);
    assert_eq!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire skips unheld"),
        0,
        "unheld pending_payment is not expire_held_order"
    );

    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);

    let (order_state, payment_state, reason, order_revision, total_minor): (
        String,
        String,
        Option<String>,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason, o.revision, o.total_minor \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid)
    .fetch_one(&pool)
    .await
    .expect("facts");
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(reason.as_deref(), Some("refund_required"));
    assert_order_event_revisions_match(&pool, &order_id, order_revision).await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            &order_id,
            order_revision,
            json!({
                "amount_minor": total_minor,
                "transaction_id": "bitcoin-tx-evidence-123",
            }),
            1_501,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("refunded_external"));

    let (final_revision,): (i64,) = sqlx::query_as("SELECT revision FROM orders WHERE id = $1")
        .bind(order_uuid)
        .fetch_one(&pool)
        .await
        .expect("final revision");
    assert!(
        final_revision > order_revision,
        "record_external must bump past the late-cancel revision"
    );
    assert_order_event_revisions_match(&pool, &order_id, final_revision).await;
}

async fn assert_order_event_revisions_match(pool: &PgPool, order_id: &str, order_revision: i64) {
    let aggregate = format!("order:{order_id}");
    let (max_event, event_count): (Option<i64>, i64) =
        sqlx::query_as("SELECT MAX(revision), COUNT(*) FROM events WHERE aggregate_id = $1")
            .bind(&aggregate)
            .fetch_one(pool)
            .await
            .expect("event revisions");
    assert_eq!(
        max_event,
        Some(order_revision),
        "orders.revision must equal max(events.revision) for {aggregate}"
    );
    assert_eq!(
        event_count, order_revision,
        "order event revisions must be contiguous 1..={order_revision}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paypal_ipn_after_expire_with_stock_gone_is_refund_required(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "paypal").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let after = app.clock.now() + chrono::Duration::seconds(3700);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&pool)
    .await
    .expect("sold out");
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        ipn_body(&order.order_id).into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reason: Option<String> =
        sqlx::query_scalar("SELECT review_reason FROM payments WHERE order_id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&pool)
            .await
            .expect("reason");
    assert_eq!(reason.as_deref(), Some("refund_required"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn stripe_verify_after_expire_with_stock_gone_is_refund_required(pool: PgPool) {
    let (app, stripe, _paykit) = test_app_with_payments(pool.clone()).await;
    stripe.accept_key(RESTRICTED_KEY);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_config(&app, &seller.token, &full_config_body()).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = bind_method(&app, &buyer.token, &order.order_id, "stripe").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let total = body["order"]["total"]["amount_minor"]
        .as_i64()
        .expect("total");
    let after = app.clock.now() + chrono::Duration::seconds(3700);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&pool)
    .await
    .expect("sold out");
    stripe.add_session(FakeStripeSession {
        id: "cs_late_gone".into(),
        client_reference_id: order.order_id.clone(),
        payment_status: "paid".into(),
        amount_total: total,
        currency: "usd".into(),
    });
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/fiat/verify", order.order_id),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reason: Option<String> =
        sqlx::query_scalar("SELECT review_reason FROM payments WHERE order_id = $1::uuid")
            .bind(&order.order_id)
            .fetch_one(&pool)
            .await
            .expect("reason");
    assert_eq!(reason.as_deref(), Some("refund_required"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sandbox_confirm_inside_window_pays_held_checkout(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "confirmed", 1, 501),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_exclusive_confirm_inside_window_pays(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    paykit.set_status(&reference, status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).unwrap())
        .fetch_one(&pool)
        .await
        .expect("state");
    assert_eq!(state, "paid");
    let _ = order_reference(Uuid::parse_str(&order_id).unwrap());
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 5)
        .await
        .ok();
}
