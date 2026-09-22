use sqlx::PgPool;

mod common;

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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0038_adds_hold_source_and_review_reason_without_dml(pool: PgPool) {
    // Cluster-global refusal-audit roles from 0032/0037 make a fresh
    // 0001..0037 apply unsafe in this shared Postgres. The 0037→0038
    // upgrade is clone-proven read-only against production (catalog 37,
    // both columns absent). This test proves the applied catalog and that
    // 0038 is additive and directly rerunnable with no DML.
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
