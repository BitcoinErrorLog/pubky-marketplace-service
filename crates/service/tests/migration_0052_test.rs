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
}
