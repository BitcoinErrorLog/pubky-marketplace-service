use sqlx::PgPool;

mod common;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0043_adds_manual_delivery_and_is_rerunnable(pool: PgPool) {
    let column = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'orders' \
               AND column_name = 'digital_message_delivered_at' \
               AND data_type = 'timestamp with time zone')",
        )
        .fetch_one(&pool)
        .await
        .expect("column existence")
    };
    assert!(column().await);
    sqlx::raw_sql(include_str!(
        "../migrations/0043_digital_manual_delivery.sql"
    ))
    .execute(&pool)
    .await
    .expect("0043 must be directly rerunnable");
    assert!(column().await);
    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 37 ORDER BY id",
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
