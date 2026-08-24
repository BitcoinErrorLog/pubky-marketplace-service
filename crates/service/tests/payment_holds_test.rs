//! Payment-time inventory holds ("only a payment should lock an item"):
//! checkout stops holding stock, each payment lock point acquires the hold
//! atomically with a bounded server-time window, and the payment-window
//! worker releases lapsed holds, expires the payment, and cancels the order.
//!
//! Covered here: the two-buyer last-unit race decided at the lock points,
//! the abandoned checkout that blocks nobody, the three lock points and
//! their windows (Locks registration, fiat/bitcoin bind, sandbox first
//! advance), idempotent lock points that never double-decrement, window
//! expiry with restock and re-checkout, the sweep never touching confirmed
//! orders, and the migration backfill's WHERE-clause behavior over
//! representative legacy-shaped rows.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use marketplace_service::clock::Clock;
use marketplace_service::workers::run_once;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    checkout_command_with_id, count, create_paid_order, create_pending_order, execute,
    indexed_command_id, listing_aggregate, lock_resource_for, new_actor, payment_command,
    register_command, register_locks_command, send, test_app, test_app_with_locks,
    test_app_with_payments, ts_after, TestApp, TEST_BUNDLE_ID,
};

const SOLD_OUT_COPY: &str = "The listing sold out before this payment started.";

async fn listing_quantities(app: &TestApp, seller_pubky: &str) -> (i64, i64, i64, String) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller_pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists")
}

async fn order_hold(app: &TestApp, order_id: &str) -> (bool, Option<String>) {
    let (stock_held, hold_expires_at): (bool, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT stock_held, hold_expires_at FROM orders WHERE id = $1::uuid")
            .bind(order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    (
        stock_held,
        hold_expires_at.map(marketplace_service::clock::format_timestamp),
    )
}

// Two buyers check out the LAST unit: both orders are created (checkout no
// longer contends); the first payment lock point wins the hold; the second
// fails with the pinned sold-out copy.
#[sqlx::test]
async fn both_buyers_check_out_the_last_unit_and_the_first_lock_point_wins(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, first_checkout) = execute(
        &app,
        &first.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa000, 1)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "first checkout failed: {first_checkout}"
    );
    let (status, second_checkout) = execute(
        &app,
        &second.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa000, 2)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "second checkout must also succeed: {second_checkout}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 2);

    let first_payment = first_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present");
    let second_payment = second_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present");

    // First payment start wins the hold.
    let (status, body) = execute(
        &app,
        &first.token,
        &payment_command(first_payment, 1, "detected", 0, 100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "winning lock point failed: {body}");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    // The second gets the new pinned copy.
    let (status, body) = execute(
        &app,
        &second.token,
        &payment_command(second_payment, 1, "detected", 0, 101),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));
    assert_eq!(body["error"]["message"], json!(SOLD_OUT_COPY));
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string()),
        "the losing lock point moved nothing"
    );
}

// An abandoned checkout — an order with no payment activity — holds
// nothing: the listing stays buyable and another buyer pays it through
// immediately.
#[sqlx::test]
async fn an_abandoned_checkout_holds_nothing_and_blocks_nobody(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let abandoner = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(
        &app,
        &abandoner.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa001, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string()),
        "the abandoned checkout holds nothing"
    );

    // A second buyer checks out and pays the unit end to end.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa001, 2)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second checkout failed: {body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 102),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirmation failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
}

// `payment.register_locks` is a lock point: it acquires the hold and arms
// the Locks payment window (the correlation window IS the hold window). An
// exact replay of the registration returns the stored result without a
// second decrement.
#[sqlx::test]
async fn register_locks_acquires_the_hold_and_arms_the_locks_window(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    assert_eq!(order_hold(&app, &order.order_id).await, (false, None));

    let registration = register_locks_command(
        &order.payment_id,
        1,
        TEST_BUNDLE_ID,
        &lock_resource_for(&seller.pubky),
        200,
    );
    let (status, body) = execute(&app, &buyer.token, &registration).await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");

    // LOCKS_PAYMENT_WINDOW_SECONDS is 3600 in the test config.
    assert_eq!(
        order_hold(&app, &order.order_id).await,
        (true, Some(ts_after(3_600)))
    );
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );
    // One window concept: the correlation window equals the hold window.
    let (window_expires_at,): (DateTime<Utc>,) =
        sqlx::query_as("SELECT window_expires_at FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(
        marketplace_service::clock::format_timestamp(window_expires_at),
        ts_after(3_600)
    );

    // A later lock point on the same order does not double-decrement: the
    // exact replay serves the stored result without re-executing.
    let (status, replay) = execute(&app, &buyer.token, &registration).await;
    assert_eq!(status, StatusCode::OK, "replay failed: {replay}");
    assert_eq!(replay, body);
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string()),
        "the replayed lock point moved nothing"
    );
}

// The payment-method bind is a lock point for all three rails: it acquires
// the hold and arms the fiat payment window.
#[sqlx::test(migrations = "./migrations")]
async fn the_payment_method_bind_acquires_the_hold_and_arms_the_fiat_window(pool: PgPool) {
    let (app, _stripe, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({
            "bitcoin_enabled": false,
            "stripe_payment_link": "https://buy.stripe.com/test_abc123",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(&app, &seller, &buyer).await;
    assert_eq!(order_hold(&app, &order.order_id).await, (false, None));

    // A second buyer checks out the same last unit BEFORE any payment
    // starts: both orders exist; the stock will decide at the lock points.
    let second_buyer = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &second_buyer.token,
        &checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa002, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second checkout failed: {body}");
    let second_order = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();

    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "stripe" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "stripe bind failed: {body}");
    assert_eq!(body["order"]["stock_held"], json!(true));
    // FIAT_PAYMENT_WINDOW_SECONDS is 3600 in the test config.
    assert_eq!(body["order"]["hold_expires_at"], json!(ts_after(3_600)));
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    // The idempotent re-bind of the same method does not double-decrement.
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "stripe" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-bind failed: {body}");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    // The second buyer's bind against the now sold-out listing fails with
    // the pinned copy (both orders were created; only one payment can
    // start).
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{second_order}/payment-method"),
        Some(&second_buyer.token),
        &json!({ "method": "stripe" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));
    assert_eq!(body["error"]["reason"], json!("sold_out"));
    assert_eq!(body["error"]["message"], json!(SOLD_OUT_COPY));
}

// The sandbox lock point arms the sandbox window on the FIRST transition
// out of awaiting_entitlement; the later confirm converts the same held
// unit without a second decrement.
#[sqlx::test]
async fn the_sandbox_first_advance_arms_the_sandbox_window_and_never_double_decrements(
    pool: PgPool,
) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "detected", 0, 300),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first advance failed: {body}");
    // SANDBOX_PAYMENT_WINDOW_SECONDS is 900 in the test config.
    assert_eq!(
        order_hold(&app, &order.order_id).await,
        (true, Some(ts_after(900)))
    );
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 1, 0, "reserved".to_string())
    );

    // The second transition is not a lock point: exactly the one held unit
    // converts to sold.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 2, "confirmed", 1, 301),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirmation failed: {body}");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
    assert_eq!(order_hold(&app, &order.order_id).await, (false, None));
}

// Window expiry end to end on the sandbox rail: the held order lapses, the
// stock restocks, the order cancels with the stored reason, and the buyer
// simply checks out again. The sandbox 'detected' payment has no
// detected → expired edge, so — exactly like buyer cancellation — the sweep
// leaves the payment record untouched.
#[sqlx::test]
async fn a_lapsed_sandbox_hold_restocks_and_the_buyer_can_recheckout(pool: PgPool) {
    let app = test_app(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "detected", 0, 400),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "lock point failed: {body}");

    // Within the window the sweep does nothing.
    app.clock.advance_seconds(899);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 0);

    app.clock.advance_seconds(2);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);

    let (order_state, reason, stock_held): (String, Option<String>, bool) = sqlx::query_as(
        "SELECT state, cancellation_reason, stock_held FROM orders WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order row exists");
    assert_eq!(order_state, "cancelled");
    assert_eq!(reason.as_deref(), Some("payment window elapsed"));
    assert!(!stock_held);
    let (payment_state,): (String,) =
        sqlx::query_as("SELECT state FROM payments WHERE id = $1::uuid")
            .bind(&order.payment_id)
            .fetch_one(&app.pool)
            .await
            .expect("payment row exists");
    assert_eq!(
        payment_state, "detected",
        "no detected → expired edge exists; the record stays untouched"
    );
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (1, 0, 0, "available".to_string())
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancelled'"
        )
        .await,
        1
    );

    // The buyer simply checks out again (listing revision advanced: hold 2,
    // release 3) and pays through.
    let mut retry = checkout_command_with_id(&seller.pubky, &indexed_command_id(0xa003, 1));
    retry["payload"]["lines"][0]["expected_revision"] = json!(3);
    let (status, body) = execute(&app, &buyer.token, &retry).await;
    assert_eq!(status, StatusCode::OK, "re-checkout failed: {body}");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 401),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-payment failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));

    // A repeated sweep finds nothing: the sweep is idempotent and never
    // touches confirmed orders.
    app.clock.advance_seconds(10_000);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 0);
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
}

// The sweep never touches confirmed orders, no matter how much server time
// passes after payment.
#[sqlx::test]
async fn the_sweep_never_touches_confirmed_orders(pool: PgPool) {
    let app = test_app(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    app.clock.advance_seconds(100_000);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 0);
    let (order_state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order.order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row exists");
    assert_eq!(order_state, "paid");
    assert_eq!(
        listing_quantities(&app, &seller.pubky).await,
        (0, 0, 1, "sold".to_string())
    );
}

// The migration backfill: pending checkout orders (which decremented at
// checkout under the old semantics) become held with the right window
// source; auction orders and settled orders are untouched. The rows are
// inserted with the new columns' defaults (the legacy shape) and the
// migration's own backfill UPDATE is executed against them verbatim.
#[sqlx::test]
async fn the_backfill_marks_exactly_the_legacy_pending_checkout_orders(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let now: DateTime<Utc> = app.clock.now();

    let insert_order =
        |id: Uuid, state: &'static str, auction: Option<String>, method: Option<&'static str>| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "INSERT INTO orders (id, auction_aggregate_id, buyer_pubky, seller_pubky, \
                 revision, state, lines, subtotal_minor, shipping_minor, tax_minor, \
                 total_minor, currency, exponent, guarantee_policy_version, payment_id, \
                 payment_method, created_at, updated_at) \
                 VALUES ($1, $2, 'buyer', 'seller', 1, $3, '[]'::jsonb, 0, 0, 0, 0, 'USD', 2, \
                 1, $4, $5, $6, $6)",
                )
                .bind(id)
                .bind(auction)
                .bind(state)
                .bind(Uuid::new_v4())
                .bind(method)
                .bind(now)
                .execute(&pool)
                .await
                .expect("legacy-shaped order inserts");
            }
        };

    let plain_pending = Uuid::new_v4();
    let fiat_pending = Uuid::new_v4();
    let correlated_pending = Uuid::new_v4();
    let auction_pending = Uuid::new_v4();
    let already_paid = Uuid::new_v4();
    insert_order(plain_pending, "pending_payment", None, None).await;
    insert_order(fiat_pending, "pending_payment", None, Some("stripe")).await;
    insert_order(correlated_pending, "pending_payment", None, None).await;
    insert_order(
        auction_pending,
        "pending_payment",
        Some("listing:seller_boots".to_string()),
        None,
    )
    .await;
    insert_order(already_paid, "paid", None, None).await;

    // The correlated order carries a Locks correlation whose window must
    // become the hold window.
    let correlated_payment = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
         state, confirmations, amount_minor, currency, exponent, created_at, updated_at) \
         VALUES ($1, $2, 'buyer', 'seller', 1, 'locks', 'awaiting_entitlement', 0, 0, 'USD', \
         2, $3, $3)",
    )
    .bind(correlated_payment)
    .bind(correlated_pending)
    .bind(now)
    .execute(&pool)
    .await
    .expect("payment row inserts");
    let correlation_window = now + chrono::Duration::seconds(1_234);
    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, \
         creator_pubky, lock_resource_hash, amount_minor, asset, exponent, policy_version, \
         bundle_id_ciphertext, bundle_lookup_token, verification_state, window_expires_at, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, 'buyer', 'seller', 'hash', 0, 'USD', 2, 1, '\\x00'::bytea, \
         '\\x01'::bytea, 'pending', $4, $5, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(correlated_payment)
    .bind(correlated_pending)
    .bind(correlation_window)
    .bind(now)
    .execute(&pool)
    .await
    .expect("correlation row inserts");

    // Execute the migration's backfill statement verbatim.
    let migration = include_str!("../migrations/0012_payment_holds.sql");
    let backfill_start = migration
        .find("UPDATE orders o SET")
        .expect("the migration contains the backfill UPDATE");
    let backfill = &migration[backfill_start..];
    let backfill = &backfill[..backfill.find(';').expect("statement terminated") + 1];
    sqlx::query(backfill)
        .execute(&pool)
        .await
        .expect("backfill statement runs");

    let hold = |id: Uuid| {
        let pool = pool.clone();
        async move {
            let row: (bool, Option<DateTime<Utc>>) =
                sqlx::query_as("SELECT stock_held, hold_expires_at FROM orders WHERE id = $1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .expect("order row exists");
            row
        }
    };

    // Legacy abandoned cart: held, self-cleans within the hour.
    let (held, window) = hold(plain_pending).await;
    assert!(held);
    let window = window.expect("a backfilled hold has a window");
    let hour = chrono::Duration::hours(1);
    let leeway = chrono::Duration::minutes(5);
    let wall = Utc::now();
    assert!(
        window > wall + hour - leeway && window < wall + hour + leeway,
        "legacy pending orders get roughly one hour: {window}"
    );

    // Bound fiat method: now() + the fiat window (one hour at the default).
    let (held, window) = hold(fiat_pending).await;
    assert!(held);
    let window = window.expect("a backfilled hold has a window");
    assert!(window > wall + hour - leeway && window < wall + hour + leeway);

    // Locks correlation: the correlation window IS the hold window.
    let (held, window) = hold(correlated_pending).await;
    assert!(held);
    assert_eq!(window, Some(correlation_window));

    // Auction orders hold through their reservation, never these columns.
    assert_eq!(hold(auction_pending).await, (false, None));
    // Settled orders are untouched.
    assert_eq!(hold(already_paid).await, (false, None));
}
