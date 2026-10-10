//! Migration 0052 support-identity catalog proof.

use sqlx::PgPool;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0052_adds_closed_upstream_support_identity(pool: PgPool) {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'orders' \
         AND column_name IN ('paykit_payment_reference', 'paykit_operation_id', \
             'paykit_payment_window_seconds', 'paykit_asset') ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("0052 support columns");
    assert_eq!(
        columns,
        vec![
            ("paykit_asset".into(), "text".into()),
            ("paykit_operation_id".into(), "text".into()),
            ("paykit_payment_reference".into(), "uuid".into()),
            ("paykit_payment_window_seconds".into(), "integer".into()),
        ]
    );

    let definition: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid = 'orders'::regclass \
         AND conname = 'orders_paykit_prepared_attempt_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("0052 all-or-nothing constraint");
    for column in [
        "paykit_payment_reference",
        "paykit_operation_id",
        "paykit_payment_window_seconds",
        "paykit_asset",
    ] {
        assert!(
            definition.contains(column),
            "constraint omits {column}: {definition}"
        );
    }

    assert!(
        definition.contains("paykit_api"),
        "constraint omits API shape: {definition}"
    );
    assert!(
        definition.contains("upstream"),
        "constraint omits upstream shape: {definition}"
    );

    let released_columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'paykit_superseded_attempts' \
         AND column_name IN ('payment_reference', 'operation_id', \
             'payment_window_seconds', 'asset') ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("0052 released-attempt support columns");
    assert_eq!(
        released_columns,
        vec![
            ("asset".into(), "text".into()),
            ("operation_id".into(), "text".into()),
            ("payment_reference".into(), "uuid".into()),
            ("payment_window_seconds".into(), "integer".into()),
        ]
    );

    let released_shape: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid = 'paykit_superseded_attempts'::regclass \
         AND conname = 'paykit_superseded_attempts_api_shape'",
    )
    .fetch_one(&pool)
    .await
    .expect("0052 released-attempt API shape constraint");
    for term in [
        "paykit_api",
        "fork",
        "upstream",
        "stack_id",
        "stack_endpoint",
        "payment_reference",
        "operation_id",
        "payment_window_seconds",
        "asset",
    ] {
        assert!(
            released_shape.contains(term),
            "constraint omits {term}: {released_shape}"
        );
    }
}
