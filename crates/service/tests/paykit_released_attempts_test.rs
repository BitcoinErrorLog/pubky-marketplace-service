//! Released Paykit attempts stay observed (migration 0042): a re-bind never
//! discards the earlier attempt's reference, money confirmed on it takes the
//! late-money path, and attempts released before 0042 are backfilled.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{create_sat_order, enable_bitcoin, poll_now, status_confirmed};
use common::*;
use marketplace_service::clock::Clock;
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
