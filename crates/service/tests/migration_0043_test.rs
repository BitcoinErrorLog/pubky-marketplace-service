use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::*;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

async fn column_exists(pool: &PgPool, column: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'orders' \
           AND column_name = $1 AND data_type = 'timestamp with time zone')",
    )
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("column existence")
}

async fn trigger_exists(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger \
         WHERE tgname = 'orders_stamp_ended_at' AND NOT tgisinternal)",
    )
    .fetch_one(pool)
    .await
    .expect("trigger existence")
}

async fn ended_at(pool: &PgPool, id: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT ended_at FROM orders WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("ended_at")
}

async fn set_state(pool: &PgPool, id: Uuid, state: &str, at: DateTime<Utc>) {
    sqlx::query("UPDATE orders SET state = $2, updated_at = $3 WHERE id = $1")
        .bind(id)
        .bind(state)
        .bind(at)
        .execute(pool)
        .await
        .expect("state");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0043_adds_manual_delivery_and_is_rerunnable(pool: PgPool) {
    for column in ["digital_message_delivered_at", "ended_at"] {
        assert!(column_exists(&pool, column).await, "{column}");
    }
    assert!(trigger_exists(&pool).await);
    sqlx::raw_sql(include_str!(
        "../migrations/0043_digital_manual_delivery.sql"
    ))
    .execute(&pool)
    .await
    .expect("0043 must be directly rerunnable");
    for column in ["digital_message_delivered_at", "ended_at"] {
        assert!(column_exists(&pool, column).await, "{column}");
    }
    assert!(trigger_exists(&pool).await);
    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id IN (37, 38) ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("catalog");
    assert_eq!(
        kinds,
        vec![
            (37, "set_delivery_email".to_string()),
            (38, "deliver_digital".to_string())
        ]
    );
}

// `ended_at` moves only when the state enters a terminal state, clears when
// the order leaves one, and is backfilled for orders already ended.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0043_stamps_and_backfills_ended_at(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let id = Uuid::parse_str(
        body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id"),
    )
    .expect("order uuid");
    assert_eq!(
        ended_at(&pool, id).await,
        None,
        "a live order has not ended"
    );

    let t0: DateTime<Utc> = "2026-09-01T00:00:00Z".parse().expect("t0");
    let day = chrono::Duration::days(1);
    set_state(&pool, id, "cancelled", t0).await;
    assert_eq!(ended_at(&pool, id).await, Some(t0));
    // A write that keeps the state leaves the clock alone.
    set_state(&pool, id, "cancelled", t0 + day * 20).await;
    assert_eq!(ended_at(&pool, id).await, Some(t0));
    // Leaving the terminal state clears it; the next end restamps it.
    set_state(&pool, id, "pending_payment", t0 + day * 21).await;
    assert_eq!(ended_at(&pool, id).await, None);
    set_state(&pool, id, "completed", t0 + day * 22).await;
    assert_eq!(ended_at(&pool, id).await, Some(t0 + day * 22));
    set_state(&pool, id, "refunded_external", t0 + day * 23).await;
    assert_eq!(ended_at(&pool, id).await, Some(t0 + day * 23));

    // Backfill: an ended order without a stamp gets its `updated_at`.
    sqlx::raw_sql(
        "ALTER TABLE orders DISABLE TRIGGER orders_stamp_ended_at; \
         UPDATE orders SET ended_at = NULL; \
         ALTER TABLE orders ENABLE TRIGGER orders_stamp_ended_at;",
    )
    .execute(&pool)
    .await
    .expect("clear stamps");
    assert_eq!(ended_at(&pool, id).await, None);
    sqlx::raw_sql(include_str!(
        "../migrations/0043_digital_manual_delivery.sql"
    ))
    .execute(&pool)
    .await
    .expect("rerun");
    assert_eq!(ended_at(&pool, id).await, Some(t0 + day * 23));
}

async fn total_balance_validated(pool: &PgPool) -> Option<bool> {
    sqlx::query_scalar(
        "SELECT convalidated FROM pg_constraint \
         WHERE conname = 'orders_total_balance' AND conrelid = 'orders'::regclass",
    )
    .fetch_optional(pool)
    .await
    .expect("orders_total_balance")
}

// Staging holds historical cancelled orders whose `total_minor` includes a
// tax that 0018 dropped, so they break `orders_total_balance`, which 0018
// left NOT VALID. The `ended_at` backfill rewrites every ended order, and an
// UPDATE re-checks NOT VALID constraints on the rows it touches, so 0043
// must drop the constraint around the backfill and re-add it NOT VALID.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0043_backfills_ended_orders_that_break_the_total_balance(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let id = Uuid::parse_str(
        body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id"),
    )
    .expect("order uuid");
    let ended: DateTime<Utc> = "2026-08-01T00:00:00Z".parse().expect("ended");
    set_state(&pool, id, "cancelled", ended).await;

    // The staging shape: a cancelled order whose total carries 100 minor
    // units no longer in subtotal + shipping, written before 0043 existed.
    sqlx::raw_sql(
        "ALTER TABLE orders DROP CONSTRAINT orders_total_balance; \
         ALTER TABLE orders DISABLE TRIGGER orders_stamp_ended_at; \
         UPDATE orders SET total_minor = subtotal_minor + shipping_minor + 100, ended_at = NULL; \
         ALTER TABLE orders ENABLE TRIGGER orders_stamp_ended_at; \
         ALTER TABLE orders ADD CONSTRAINT orders_total_balance \
           CHECK (total_minor = subtotal_minor + shipping_minor) NOT VALID;",
    )
    .execute(&pool)
    .await
    .expect("stage a historical unbalanced order");
    let (total, subtotal, shipping): (i64, i64, i64) = sqlx::query_as(
        "SELECT total_minor, subtotal_minor, shipping_minor FROM orders WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .expect("totals");
    assert_eq!(total, subtotal + shipping + 100);
    assert_eq!(ended_at(&pool, id).await, None);

    sqlx::raw_sql(include_str!(
        "../migrations/0043_digital_manual_delivery.sql"
    ))
    .execute(&pool)
    .await
    .expect("0043 applies over an ended order that breaks orders_total_balance");

    assert_eq!(ended_at(&pool, id).await, Some(ended));
    let after: i64 = sqlx::query_scalar("SELECT total_minor FROM orders WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("total after");
    assert_eq!(
        after, total,
        "the paid total is history and stays as it was"
    );
    assert_eq!(
        total_balance_validated(&pool).await,
        Some(false),
        "orders_total_balance is back, NOT VALID"
    );
    let unbalanced_write =
        sqlx::query("UPDATE orders SET total_minor = total_minor + 1 WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .expect_err("new writes are still balance-checked");
    assert!(
        unbalanced_write
            .to_string()
            .contains("orders_total_balance"),
        "{unbalanced_write}"
    );
}
