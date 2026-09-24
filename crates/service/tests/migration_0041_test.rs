use sqlx::PgPool;

mod common;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0041_adds_digital_delivered_at_and_is_rerunnable(pool: PgPool) {
    let column = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'orders' \
               AND column_name = 'digital_delivered_at' AND data_type = 'timestamp with time zone')",
        )
        .fetch_one(&pool)
        .await
        .expect("column existence")
    };
    assert!(column().await);
    sqlx::raw_sql(include_str!("../migrations/0041_digital_orders.sql"))
        .execute(&pool)
        .await
        .expect("0041 must be directly rerunnable");
    assert!(column().await);
}

#[test]
fn migration_0041_source_is_schema_only() {
    let upper = include_str!("../migrations/0041_digital_orders.sql").to_ascii_uppercase();
    for forbidden in ["INSERT ", "UPDATE ", "DELETE ", "TRUNCATE "] {
        assert!(
            !upper.contains(forbidden),
            "0041 must not contain {forbidden}"
        );
    }
}
