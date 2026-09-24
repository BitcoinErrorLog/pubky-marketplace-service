//! 0039: `paypal_txn_id` backfill and the refund ledger.

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;
use sqlx::PgPool;

const MIGRATION: &str = include_str!("../migrations/0039_paypal_gateway_refunds.sql");

fn completed_ipn(order_id: &str, txn_id: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in [
        ("payment_status", "Completed"),
        ("receiver_email", "merchant@example.com"),
        ("mc_gross", "137.00"),
        ("mc_currency", "USD"),
        ("custom", order_id),
        ("txn_id", txn_id),
    ] {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

/// A PayPal order paid by a verified IPN, as production holds it before
/// 0039: `fiat_transaction_ref` set by the gateway, `paypal_txn_id` unset.
async fn gateway_paid_order(app: &TestApp, txn_id: &str) -> String {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = create_pending_order(app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        completed_ipn(&order.order_id, txn_id).into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    sqlx::query("UPDATE orders SET paypal_txn_id = NULL WHERE id = $1::uuid")
        .bind(&order.order_id)
        .execute(&app.pool)
        .await
        .expect("pre-0039 shape");
    order.order_id
}

async fn stored(pool: &PgPool, order_id: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT fiat_transaction_ref, paypal_txn_id FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0039_backfills_only_gateway_verified_payment_ids(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let plain = gateway_paid_order(&app, "1HH00000HH0000001").await;

    // A buyer report before the receipt is what the gateway overwrote.
    let reported_before = gateway_paid_order(&app, "1HH00000HH0000002").await;
    sqlx::query(
        "UPDATE orders o SET payment_reported_at = r.issued_at - interval '1 minute' \
         FROM receipts r WHERE r.id = o.receipt_id AND o.id = $1::uuid",
    )
    .bind(&reported_before)
    .execute(&pool)
    .await
    .unwrap();

    // A buyer report after the receipt overwrote the gateway's value.
    let reported_after = gateway_paid_order(&app, "1HH00000HH0000003").await;
    sqlx::query(
        "UPDATE orders o SET payment_reported_at = r.issued_at + interval '1 minute', \
         fiat_transaction_ref = 'BUYER-TYPED-REF' \
         FROM receipts r WHERE r.id = o.receipt_id AND o.id = $1::uuid",
    )
    .bind(&reported_after)
    .execute(&pool)
    .await
    .unwrap();

    // A seller-attested payment.
    let seller_attested = gateway_paid_order(&app, "1HH00000HH0000004").await;
    sqlx::query("UPDATE orders SET fiat_verified_by = 'seller' WHERE id = $1::uuid")
        .bind(&seller_attested)
        .execute(&pool)
        .await
        .unwrap();

    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION)
            .execute(&pool)
            .await
            .expect("0039 is directly rerunnable");
    }

    assert_eq!(
        stored(&pool, &plain).await.1.as_deref(),
        Some("1HH00000HH0000001")
    );
    assert_eq!(
        stored(&pool, &reported_before).await.1.as_deref(),
        Some("1HH00000HH0000002")
    );
    assert_eq!(stored(&pool, &reported_after).await.1, None);
    assert_eq!(stored(&pool, &seller_attested).await.1, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0039_ledger_constraints_hold(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let order_id = gateway_paid_order(&app, "1HH00000HH0000005").await;
    let insert = |txn: &'static str, status: &'static str, amount: i64| {
        let pool = pool.clone();
        let order_id = order_id.clone();
        async move {
            sqlx::query(
                "INSERT INTO order_gateway_refunds \
                 (refund_txn_id, order_id, parent_txn_id, payment_status, amount_minor, recorded_at) \
                 VALUES ($1, $2::uuid, '1HH00000HH0000005', $3, $4, now())",
            )
            .bind(txn)
            .bind(&order_id)
            .bind(status)
            .bind(amount)
            .execute(&pool)
            .await
        }
    };
    insert("R1", "Refunded", 100).await.expect("valid row");
    assert!(
        insert("R1", "Refunded", 100).await.is_err(),
        "one row per refund txn"
    );
    assert!(insert("R2", "Canceled_Reversal", 100).await.is_err());
    assert!(insert("R3", "Reversed", 0).await.is_err());
    assert!(insert("", "Reversed", 1).await.is_err());
    assert!(
        sqlx::query("UPDATE orders SET paypal_txn_id = '' WHERE id = $1::uuid")
            .bind(&order_id)
            .execute(&pool)
            .await
            .is_err()
    );
}
