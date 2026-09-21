use std::borrow::Cow;

use sqlx::migrate::Migrator;
use sqlx::PgPool;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

fn migrator_through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}

async fn column_exists(pool: &PgPool, table: &str, column: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
         )",
    )
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("column existence")
}

async fn constraint_exists(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $1)")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("constraint existence")
}

#[sqlx::test(migrations = false)]
async fn migration_0038_adds_hold_source_and_review_reason_without_dml(pool: PgPool) {
    migrator_through(37)
        .run(&pool)
        .await
        .expect("0001..0037 apply");
    assert!(!column_exists(&pool, "orders", "hold_source").await);
    assert!(!column_exists(&pool, "payments", "review_reason").await);

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0038 applies to the 0037 catalog");

    assert!(column_exists(&pool, "orders", "hold_source").await);
    assert!(column_exists(&pool, "payments", "review_reason").await);
    assert!(constraint_exists(&pool, "orders_hold_source_check").await);
    assert!(constraint_exists(&pool, "payments_review_reason_check").await);

    sqlx::raw_sql(include_str!("../migrations/0038_checkout_holds.sql"))
        .execute(&pool)
        .await
        .expect("0038 must be directly rerunnable");
    assert!(constraint_exists(&pool, "orders_hold_source_check").await);

    let cancelled: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM orders WHERE state = 'cancelled'")
            .fetch_one(&pool)
            .await
            .expect("no DML");
    assert_eq!(cancelled, 0, "0038 must not cancel live orders");
}

#[test]
fn migration_0038_source_is_schema_only() {
    let source = include_str!("../migrations/0038_checkout_holds.sql");
    let upper = source.to_ascii_uppercase();
    for forbidden in ["INSERT ", "UPDATE ", "DELETE ", "TRUNCATE "] {
        assert!(
            !upper.contains(forbidden),
            "0038 must not contain {forbidden}"
        );
    }
}
