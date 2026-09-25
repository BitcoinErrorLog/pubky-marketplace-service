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
