//! Migration 0042 against a production-schema clone
//! (`scripts/release/rehearse-0042-production-clone.sh`): the clone holds
//! production's roles, schema, and migration ledger (0001..=0039) and no
//! production rows. The test seeds synthetic orders in the released shape
//! production holds (buyer-cancelled while preparing: order cancelled,
//! payment expired, activation voided, request state cleared, method still
//! bitcoin, the order-id reference), applies the pending migrations with the
//! service's real migrator (which verifies the applied checksums against the
//! clone's ledger), and routes confirmed money on one of them.

mod common;

use common::paykit_review::{poll_now, status_confirmed};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payments::legacy_order_reference;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

async fn seed_released_shape(pool: &PgPool, stack_endpoint: &str) -> (Uuid, Uuid) {
    let order_id = Uuid::new_v4();
    let payment_id = Uuid::new_v4();
    let invoice_id = Uuid::new_v4();
    let (_, seller) = random_keypair();
    let (_, buyer) = random_keypair();
    let mut tx = pool.begin().await.expect("seed transaction");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *tx)
        .await
        .expect("defer constraints");
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, created_at, updated_at, payment_method, \
         stock_held, cancellation_reason, paykit_request_reference, paykit_request_state, \
         paykit_invoice_id, paykit_stack_id, paykit_stack_endpoint, paykit_total_sats, \
         paykit_expires_at, paykit_prepare_expires_at, paykit_allocation_mode, \
         paykit_address_fingerprint, paykit_bind_attempt, paykit_activation_state) \
         VALUES ($1, $2, $3, 3, 'cancelled', $4, 50000, 0, 50000, 'SAT', 0, 1, $5, \
         now() - interval '3 days', now() - interval '3 days', 'bitcoin', false, \
         'Changed mind', $6, NULL, $7, 'production:clone-stack', $8, 50437, \
         now() - interval '3 days' + interval '2 hours', now() - interval '3 days' + interval '15 minutes', \
         'exclusive', '3f7a1c9e5b204d86', 1, 'voided')",
    )
    .bind(order_id)
    .bind(&buyer)
    .bind(&seller)
    .bind(json!([{
        "listing_aggregate_id": format!("listing:{seller}_released_shape"),
        "quantity": 1,
        "unit_price_minor": 50000
    }]))
    .bind(payment_id)
    .bind(legacy_order_reference(order_id))
    .bind(invoice_id)
    .bind(stack_endpoint)
    .execute(&mut *tx)
    .await
    .expect("released-shape order");
    sqlx::query(
        "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
         state, confirmations, amount_minor, currency, exponent, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 3, 'paykit', 'expired', 0, 50437, 'SAT', 0, \
         now() - interval '3 days', now() - interval '3 days')",
    )
    .bind(payment_id)
    .bind(order_id)
    .bind(&buyer)
    .bind(&seller)
    .execute(&mut *tx)
    .await
    .expect("released-shape payment");
    tx.commit().await.expect("seed commit");
    (order_id, invoice_id)
}

#[tokio::test]
#[ignore = "runs against a production-schema clone (MARKETPLACE_CLONE_DATABASE_URL)"]
async fn migration_0042_on_a_production_schema_clone_recovers_released_attempts() {
    let url = std::env::var("MARKETPLACE_CLONE_DATABASE_URL")
        .expect("MARKETPLACE_CLONE_DATABASE_URL names the production-schema clone");
    let pool = pool_with_limit(&url, 5).await;
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("clone ledger");
    assert_eq!(
        applied,
        (1..=39).collect::<Vec<_>>(),
        "the production ledger"
    );

    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seeded = [
        seed_released_shape(&pool, &paykit.base_url).await,
        seed_released_shape(&pool, &paykit.base_url).await,
    ];

    MIGRATOR
        .run(&pool)
        .await
        .expect("pending migrations apply over the production ledger");
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("clone ledger");
    assert_eq!(applied.last(), Some(&42));

    for (order_id, invoice_id) in seeded {
        let row: (Uuid, Option<String>, String) = sqlx::query_as(
            "SELECT invoice_id, reference, state FROM paykit_superseded_attempts \
             WHERE order_id = $1",
        )
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("backfilled attempt");
        assert_eq!(
            row,
            (
                invoice_id,
                Some(legacy_order_reference(order_id)),
                "watching".to_string()
            )
        );
    }

    let (paid, _) = seeded[0];
    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&legacy_order_reference(paid), late);
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    let state: String =
        sqlx::query_scalar("SELECT state FROM paykit_superseded_attempts WHERE order_id = $1")
            .bind(paid)
            .fetch_one(&pool)
            .await
            .expect("routed attempt");
    assert_eq!(state, "late_money");
    let (payment_state, review_reason): (String, Option<String>) =
        sqlx::query_as("SELECT state, review_reason FROM payments WHERE order_id = $1")
            .bind(paid)
            .fetch_one(&pool)
            .await
            .expect("payment row");
    assert_eq!(
        (payment_state.as_str(), review_reason.as_deref()),
        ("manual_review", Some("refund_required")),
        "the listing is gone, so the late money is a refund for the seller to record"
    );
    let untouched: String =
        sqlx::query_scalar("SELECT state FROM paykit_superseded_attempts WHERE order_id = $1")
            .bind(seeded[1].0)
            .fetch_one(&pool)
            .await
            .expect("second attempt");
    assert_eq!(untouched, "watching");
}
