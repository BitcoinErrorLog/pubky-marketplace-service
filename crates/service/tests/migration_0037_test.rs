use std::borrow::Cow;

use marketplace_service::refusal_audit::{
    probe_retention_authority, RETENTION_CONN_LIMIT_MIN, RETENTION_POOL_MAX_CONNECTIONS,
};
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

async fn retention_conn_limit(pool: &PgPool) -> i32 {
    sqlx::query_scalar(
        "SELECT rolconnlimit FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_retention'",
    )
    .fetch_one(pool)
    .await
    .expect("retention role limit")
}

#[sqlx::test(migrations = false)]
async fn migration_0037_raises_retention_limit_from_0036_posture(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    migrator_through(36)
        .run(&pool)
        .await
        .expect("production 0001..0036 catalog applies");
    sqlx::query("ALTER ROLE marketplace_refusal_audit_retention CONNECTION LIMIT 1")
        .execute(&pool)
        .await
        .expect("0036 production retention limit");
    assert_eq!(retention_conn_limit(&pool).await, 1);

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0037 applies to the exact 0036 catalog");

    assert_eq!(retention_conn_limit(&pool).await, RETENTION_CONN_LIMIT_MIN);
    let writer_limit: i32 = sqlx::query_scalar(
        "SELECT rolconnlimit FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_writer_login'",
    )
    .fetch_one(&pool)
    .await
    .expect("writer role limit");
    assert_eq!(writer_limit, 2);

    let login = common::claim_refusal_audit_retention_login(&pool).await;
    let pool_one = common::pool_with_limit(&login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let pool_two = common::pool_with_limit(&login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let mut first = pool_one.acquire().await.expect("first retention backend");
    probe_retention_authority(&mut first)
        .await
        .expect("first replica retention probe after 0037");
    let mut second = pool_two.acquire().await.expect("second retention backend");
    probe_retention_authority(&mut second)
        .await
        .expect("second replica retention probe after 0037");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0037_direct_rerun_keeps_overlap_limits(pool: PgPool) {
    sqlx::raw_sql(include_str!(
        "../migrations/0037_refusal_audit_overlap_limits.sql"
    ))
    .execute(&pool)
    .await
    .expect("0037 must be directly rerunnable");
    assert_eq!(retention_conn_limit(&pool).await, RETENTION_CONN_LIMIT_MIN);
}
