//! Schema proof for the additive FX-at-bind migration.

mod common;

use common::{create_pending_order, new_actor, test_app};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn fx_schema_has_constraints_and_single_assignment_trigger(pool: PgPool) {
    let columns: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_name = 'orders' AND column_name IN \
         ('bitcoin_quote_rate', 'bitcoin_quote_source', 'bitcoin_quote_fetched_at', \
          'bitcoin_quoted_sats', 'bitcoin_quote_expires_at', 'bitcoin_quote_currency', \
          'bitcoin_quote_exponent', 'bitcoin_quote_spread_bps', 'paykit_observed_sats')",
    )
    .fetch_one(&pool)
    .await
    .expect("FX columns exist");
    assert_eq!(columns.0, 9);

    let trigger: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.triggers \
         WHERE trigger_name = 'orders_paykit_observed_sats_immutable'",
    )
    .fetch_one(&pool)
    .await
    .expect("trigger catalog is readable");
    assert_eq!(trigger.0, 1);

    let unique_index: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_constraint \
         WHERE conname = 'fx_rate_samples_one_per_bucket')",
    )
    .fetch_one(&pool)
    .await
    .expect("sample uniqueness is readable");
    assert!(unique_index.0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paykit_observed_sats_is_single_assignment_at_the_database_boundary(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order id is a UUID");

    sqlx::query(
        "UPDATE orders SET paykit_total_sats = 1500, paykit_observed_sats = 1200 \
         WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("first observation freezes");

    sqlx::query("UPDATE orders SET paykit_observation = $2::jsonb WHERE id = $1")
        .bind(order_id)
        .bind(serde_json::json!({ "observed_sats": 1300 }))
        .execute(&pool)
        .await
        .expect("later status refresh updates the JSON fact");

    let error = sqlx::query("UPDATE orders SET paykit_observed_sats = 1300 WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect_err("a second non-null observation must be rejected");
    assert!(
        error
            .to_string()
            .contains("paykit_observed_sats is single-assignment"),
        "{error}"
    );

    let (observed, observation): (i64, serde_json::Value) =
        sqlx::query_as("SELECT paykit_observed_sats, paykit_observation FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("frozen observation remains readable");
    assert_eq!(observed, 1200);
    assert_eq!(observation["observed_sats"], 1300);
}
