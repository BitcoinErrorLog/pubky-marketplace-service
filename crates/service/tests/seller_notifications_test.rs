//! The seller is told about every Paykit Bitcoin state that waits for
//! them: a payment seen on a `shared_manual` order (only their attestation
//! can pay it) and every entry to `manual_review`, with the reason.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::paykit_review::{
    bound_order, delivered_notifications, into_awaiting_confirmation, into_manual_review_held,
    into_manual_review_mismatch, poll_now, status_confirmed,
};
use common::*;
use marketplace_service::bitcoin_review::watch_manual_reviews;
use marketplace_service::clock::Clock;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// Verbatim live producer captures (W1.14 e2e, paykit-server-e2e
/// `tests/expiry.rs`), as in `shared_manual_test.rs`.
const LIVE_EXCLUSIVE_LATE_STATUS: &str = r#"{"allocation_mode":"exclusive","amount_matched":true,"confirmations":6,"contract_version":"paykit.bitcoin_status/v2","late_settlement":true,"status":"confirmed"}"#;
const LIVE_SHARED_MANUAL_LATE_STATUS: &str = r#"{"allocation_mode":"shared_manual","amount_matched":true,"confirmations":6,"contract_version":"paykit.bitcoin_status/v2","late_settlement":true,"status":"confirmed"}"#;

fn body(payload: &str) -> Value {
    serde_json::from_str(payload).expect("captured status body")
}

fn order_uuid(order_id: &str) -> Uuid {
    Uuid::parse_str(order_id).expect("order uuid")
}

async fn waiting_facts(pool: &PgPool, order_id: &str) -> (Option<String>, String, i32, Value) {
    sqlx::query_as(
        "SELECT o.paykit_request_state, p.state, p.confirmations, \
         COALESCE(o.paykit_observation, 'null'::jsonb) \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order and payment")
}

async fn payment_events(pool: &PgPool, order_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p \
         ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = $2",
    )
    .bind(order_uuid(order_id))
    .bind(kind)
    .fetch_one(pool)
    .await
    .expect("event count")
}

/// Exactly one `bitcoin_manual_review` notification about this order, sent
/// by the system with `reason`; the buyer has none.
async fn assert_review_notice(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    order_id: &str,
    reason: &str,
) {
    let seller_rows = delivered_notifications(app, &seller.token, "bitcoin_manual_review").await;
    assert_eq!(seller_rows.len(), 1, "one review notice: {seller_rows:?}");
    assert_eq!(
        seller_rows[0]["aggregate_id"],
        json!(format!("order:{order_id}"))
    );
    assert_eq!(seller_rows[0]["actor_pubky"], json!("system"));
    assert_eq!(seller_rows[0]["review_reason"], json!(reason));
    assert!(
        delivered_notifications(app, &buyer.token, "bitcoin_manual_review")
            .await
            .is_empty(),
        "the buyer cannot resolve the review"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_redelivered_payment_seen_intent_notifies_the_seller_once(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id) = into_awaiting_confirmation(&app, &paykit, &seller, &buyer).await;
    assert_eq!(
        delivered_notifications(&app, &seller.token, "bitcoin_payment_seen")
            .await
            .len(),
        1
    );

    // A lapsed lease redelivers the intent; `(event_id, recipient_pubky)`
    // keeps one notification.
    let reopened = sqlx::query(
        "UPDATE outbox SET delivered_at = NULL, lease_until = NULL \
         WHERE kind = 'notification.bitcoin_payment_seen'",
    )
    .execute(&pool)
    .await
    .expect("reopen the intent");
    assert_eq!(reopened.rows_affected(), 1, "one intent per entry");
    assert_eq!(
        delivered_notifications(&app, &seller.token, "bitcoin_payment_seen")
            .await
            .len(),
        1
    );

    // Status refreshes inside the window never re-announce the payment.
    let reference = marketplace_service::payments::attempt_reference(order_uuid(&order_id), 1);
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 3));
    assert_eq!(
        poll_now(&app, app.clock.now() + Duration::seconds(60)).await,
        0
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_payment_seen'"
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_settlement_review_tells_the_seller_why(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    for (mode, status) in [
        ("shared_manual", LIVE_SHARED_MANUAL_LATE_STATUS),
        ("exclusive", LIVE_EXCLUSIVE_LATE_STATUS),
    ] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, _payment_id, reference) =
            bound_order(&app, &paykit, &seller, &buyer, mode).await;
        paykit.set_status(&reference, body(status));
        assert_eq!(poll_now(&app, app.clock.now()).await, 1, "{mode}");
        let (request_state, payment_state, _, _) = waiting_facts(&pool, &order_id).await;
        assert_eq!(request_state.as_deref(), Some("confirmed"), "{mode}");
        assert_eq!(payment_state, "manual_review", "{mode}");
        assert_review_notice(&app, &seller, &buyer, &order_id, "late_settlement").await;
        assert!(
            delivered_notifications(&app, &seller.token, "bitcoin_payment_seen")
                .await
                .is_empty(),
            "{mode}: a late first sighting never announces a confirmable payment"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_amount_mismatch_review_tells_the_seller_why(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id) = into_manual_review_mismatch(&app, &paykit, &seller, &buyer).await;
    assert_review_notice(&app, &seller, &buyer, &order_id, "amount_mismatch").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_confirmation_the_order_cannot_take_tells_the_seller_why(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    sqlx::query(
        "UPDATE orders SET state = 'cancelled', stock_held = FALSE, \
         cancellation_reason = 'cancelled before the payment confirmed' WHERE id = $1",
    )
    .bind(order_uuid(&order_id))
    .execute(&pool)
    .await
    .expect("cancel the order under the poller");
    paykit.set_status(&reference, status_confirmed("exclusive", true, 6));
    assert_eq!(poll_now(&app, app.clock.now()).await, 1);
    let (_, payment_state, _, _) = waiting_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "manual_review");
    assert_review_notice(&app, &seller, &buyer, &order_id, "confirmation_failed").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unpinned_legacy_review_sends_no_resolution_prompt(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    sqlx::query(
        "UPDATE orders SET paykit_stack_id = NULL, paykit_stack_endpoint = NULL WHERE id = $1",
    )
    .bind(order_uuid(&order_id))
    .execute(&pool)
    .await
    .expect("simulate a pre-0022 row without resolution pins");
    paykit.set_status(&reference, status_confirmed("exclusive", false, 6));
    assert_eq!(poll_now(&app, app.clock.now()).await, 1);
    let (_, payment_state, _, _) = waiting_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "manual_review");
    assert!(
        delivered_notifications(&app, &seller.token, "bitcoin_manual_review")
            .await
            .is_empty(),
        "the resolve endpoint refuses an unpinned order"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_window_elapsing_tells_the_seller_why(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    assert_review_notice(
        &app,
        &seller,
        &buyer,
        &order_id,
        "seller_confirmation_window_elapsed",
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_overdue_review_reminds_the_seller_once(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let entered_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE order_id = $1")
            .bind(order_uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("entry stamp");

    // Four calendar days always spans two business days, and stays inside
    // the seven-day inactivity bound.
    let overdue = entered_at + Duration::days(4);
    let (alerts, abandoned) = watch_manual_reviews(&app.state, overdue)
        .await
        .expect("watch runs");
    assert_eq!((alerts, abandoned), (1, 0));
    let (alerts, _) = watch_manual_reviews(&app.state, overdue + Duration::hours(1))
        .await
        .expect("watch runs");
    assert_eq!(alerts, 0, "the reminder fires once per entry");
    assert_eq!(
        payment_events(&pool, &order_id, "payment.manual_review_overdue").await,
        1
    );
    let (_, payment_state, _, _) = waiting_facts(&pool, &order_id).await;
    assert_eq!(payment_state, "manual_review", "a reminder moves nothing");

    let mut reasons: Vec<String> =
        delivered_notifications(&app, &seller.token, "bitcoin_manual_review")
            .await
            .iter()
            .map(|notification| {
                notification["review_reason"]
                    .as_str()
                    .expect("a review notice carries its reason")
                    .to_string()
            })
            .collect();
    reasons.sort();
    assert_eq!(
        reasons,
        [
            "seller_confirmation_window_elapsed",
            "seller_response_overdue"
        ],
        "the entry and one reminder"
    );
    assert!(
        delivered_notifications(&app, &buyer.token, "bitcoin_manual_review")
            .await
            .is_empty()
    );
}
