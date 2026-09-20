use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn automation_schema_is_additive_and_constrained(pool: PgPool) {
    for table in [
        "webhook_endpoints",
        "webhook_deliveries",
        "webhook_dead_letters",
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("catalog query");
        assert!(exists, "{table} must exist");
    }

    let session_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'auth_sessions' \
           AND column_name = ANY($1)",
    )
    .bind(
        &[
            "session_id",
            "label",
            "client_metadata",
            "last_used_at",
            "revoked_at",
        ][..],
    )
    .fetch_one(&pool)
    .await
    .expect("session column catalog");
    assert_eq!(session_columns, 5);

    let earlier_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 1 AND 34")
            .fetch_one(&pool)
            .await
            .expect("migration catalog");
    assert_eq!(earlier_count, 34, "no earlier migration may be omitted");
}
