//! 0039: `paypal_txn_id` and receiver snapshot backfill, and the refund
//! ledger and inbox constraints.

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

struct Paid {
    order_id: String,
}

/// A PayPal order paid by a verified IPN, optionally after a buyer report,
/// reset to the pre-0039 shape: `fiat_transaction_ref` as the service left
/// it, no `paypal_txn_id` or receiver snapshot.
async fn gateway_paid_order(app: &TestApp, buyer_ref: Option<&str>, txn_id: &str) -> Paid {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "Merchant@Example.com" }),
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
    if let Some(reference) = buyer_ref {
        let (status, body) = send(
            app.router.clone(),
            "POST",
            &format!("/v0/orders/{}/fiat/mark-paid", order.order_id),
            Some(&buyer.token),
            &json!({ "transaction_ref": reference }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        completed_ipn(&order.order_id, txn_id).into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    sqlx::query(
        "UPDATE orders SET paypal_txn_id = NULL, paypal_receiver_email = NULL, \
         paypal_receiver_id = NULL WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .execute(&app.pool)
    .await
    .expect("pre-0039 shape");
    Paid {
        order_id: order.order_id,
    }
}

async fn stored(pool: &PgPool, order_id: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT paypal_txn_id, paypal_receiver_email FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn rerun(pool: &PgPool) {
    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION)
            .execute(pool)
            .await
            .expect("0039 is directly rerunnable");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0039_backfills_only_gateway_verified_payment_ids(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let plain = gateway_paid_order(&app, None, "1HH00000HH0000001").await;

    // A buyer report appended before the gateway's payment is what the
    // gateway overwrote.
    let reported_before =
        gateway_paid_order(&app, Some("BUYER-TYPED-1"), "1HH00000HH0000002").await;

    // A buyer report before a verified payment whose IPN carried no usable
    // `txn_id`: the buyer's value survives and is not PayPal-shaped.
    let buyer_value_survives = gateway_paid_order(&app, Some("BUYER-TYPED-2"), "").await;
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT fiat_transaction_ref FROM orders WHERE id = $1::uuid"
        )
        .bind(&buyer_value_survives.order_id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .as_deref(),
        Some("BUYER-TYPED-2")
    );

    // A buyer report appended after the receipt, at the same instant, with a
    // PayPal-shaped value: only the event sequence shows the report
    // overwrote the gateway's value.
    let reported_after = gateway_paid_order(&app, None, "1HH00000HH0000003").await;
    sqlx::query(
        "UPDATE orders o SET payment_reported_at = r.issued_at, \
         fiat_transaction_ref = '9ZZ00000ZZ0000003' \
         FROM receipts r WHERE r.id = o.receipt_id AND o.id = $1::uuid",
    )
    .bind(&reported_after.order_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
         SELECT gen_random_uuid(), gen_random_uuid(), 'order:' || o.id::text, o.revision + 100, \
                o.buyer_pubky, 'order.fiat_payment_reported', r.issued_at \
         FROM orders o JOIN receipts r ON r.id = o.receipt_id WHERE o.id = $1::uuid",
    )
    .bind(&reported_after.order_id)
    .execute(&pool)
    .await
    .unwrap();

    // A seller-attested payment.
    let seller_attested = gateway_paid_order(&app, None, "1HH00000HH0000004").await;
    sqlx::query("UPDATE orders SET fiat_verified_by = 'seller' WHERE id = $1::uuid")
        .bind(&seller_attested.order_id)
        .execute(&pool)
        .await
        .unwrap();

    rerun(&pool).await;

    assert_eq!(
        stored(&pool, &plain.order_id).await,
        (
            Some("1HH00000HH0000001".to_string()),
            Some("merchant@example.com".to_string())
        )
    );
    assert_eq!(
        stored(&pool, &reported_before.order_id).await.0.as_deref(),
        Some("1HH00000HH0000002")
    );
    assert_eq!(
        stored(&pool, &buyer_value_survives.order_id).await,
        (None, None)
    );
    assert_eq!(stored(&pool, &reported_after.order_id).await, (None, None));
    assert_eq!(stored(&pool, &seller_attested.order_id).await, (None, None));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0039_skips_a_reference_shared_by_two_orders(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let first = gateway_paid_order(&app, None, "1HH00000HH0000005").await;
    let second = gateway_paid_order(&app, None, "1HH00000HH0000006").await;
    sqlx::query("UPDATE orders SET fiat_transaction_ref = '1HH00000HH0000005' WHERE id = $1::uuid")
        .bind(&second.order_id)
        .execute(&pool)
        .await
        .unwrap();
    rerun(&pool).await;
    assert_eq!(stored(&pool, &first.order_id).await, (None, None));
    assert_eq!(stored(&pool, &second.order_id).await, (None, None));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0039_constraints_hold(pool: PgPool) {
    let (app, _stripe, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let paid = gateway_paid_order(&app, None, "1HH00000HH0000007").await;
    let other = gateway_paid_order(&app, None, "1HH00000HH0000008").await;
    rerun(&pool).await;
    let order_id = paid.order_id.clone();
    let insert = |txn: &'static str, status: &'static str, amount: i64| {
        let pool = pool.clone();
        let order_id = order_id.clone();
        async move {
            sqlx::query(
                "INSERT INTO order_gateway_refunds \
                 (refund_txn_id, order_id, parent_txn_id, payment_status, amount_minor, recorded_at) \
                 VALUES ($1, $2::uuid, '1HH00000HH0000007', $3, $4, now())",
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
    insert("R2", "Canceled_Reversal", 100)
        .await
        .expect("canceled reversal row");
    assert!(
        insert("R1", "Refunded", 100).await.is_err(),
        "one row per refund txn"
    );
    assert!(insert("R3", "Pending", 100).await.is_err());
    assert!(insert("R4", "Reversed", 0).await.is_err());
    assert!(insert("", "Reversed", 1).await.is_err());

    // One verified payment id per order, and never without its receiver.
    assert!(sqlx::query(
        "UPDATE orders SET paypal_txn_id = '1HH00000HH0000007', \
         paypal_receiver_email = 'merchant@example.com' WHERE id = $1::uuid"
    )
    .bind(&other.order_id)
    .execute(&pool)
    .await
    .is_err());
    assert!(
        sqlx::query("UPDATE orders SET paypal_receiver_email = NULL WHERE id = $1::uuid")
            .bind(&paid.order_id)
            .execute(&pool)
            .await
            .is_err()
    );

    let inbox = |reason: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO gateway_refund_inbox \
                 (txn_id, payment_status, reason, fields, received_at) \
                 VALUES (gen_random_uuid()::text, 'Refunded', $1, '{}'::jsonb, now())",
            )
            .bind(reason)
            .execute(&pool)
            .await
        }
    };
    inbox("unknown_parent").await.expect("valid reason");
    assert!(inbox("anything_else").await.is_err());
}
