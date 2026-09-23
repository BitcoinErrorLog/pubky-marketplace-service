mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use marketplace_service::http::build_router;
use marketplace_service::refusal_audit::{
    probe_retention_authority, probe_writer_authority, AuditKeys, RefusalAuditRuntime,
    RETENTION_CONN_LIMIT_MIN, RETENTION_POOL_MAX_CONNECTIONS, WRITER_LOGIN_CONN_LIMIT,
    WRITER_POOL_MAX_CONNECTIONS,
};
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

fn audit_keys() -> AuditKeys {
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
    AuditKeys::parse(&root, "1", None, None).expect("keys")
}

async fn wait_until_ready(runtime: &RefusalAuditRuntime) {
    for _ in 0..50 {
        if runtime.is_ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "writer replica did not become ready: {:?}",
        runtime.metrics()
    );
}

async fn http_get_status(addr: SocketAddr, path: &str) -> u16 {
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    let body = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("replica accepts");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        stream.flush().await.expect("flush request");
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.expect("read response");
        body
    })
    .await
    .expect("replica /ready responds");
    let text = String::from_utf8_lossy(&body);
    let status = text.split_whitespace().nth(1).unwrap_or("");
    status
        .parse()
        .unwrap_or_else(|_| panic!("replica /ready HTTP status, got {text:?}"))
}

fn overflow_pool(url: &str, max_connections: u32) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy(url)
        .expect("lazy overflow pool")
}

async fn spawn_replica(
    state: marketplace_service::AppState,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("replica binds");
    let addr = listener.local_addr().expect("replica address");
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("replica serves");
    });
    (addr, handle)
}

async fn forget_test_only_restore_migration(pool: &PgPool) {
    // TEST_MIGRATOR wraps production migrations with version 0 (restore the
    // 0032-era connection limit) and 9000000001 (release the session lock).
    // The production migrator does not know those versions.
    let deleted = sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (0, 9000000001)")
        .execute(pool)
        .await
        .expect("drop test-only migration rows")
        .rows_affected();
    assert_eq!(
        deleted, 2,
        "TEST_MIGRATOR records version 0 and 9000000001 around the production catalog"
    );
}

fn third_writer_must_fail(error: sqlx::Error) {
    match error {
        sqlx::Error::PoolTimedOut => {}
        sqlx::Error::Database(database)
            if database.code().as_deref() == Some("53300")
                || database.message().contains("too many connections for role") => {}
        other => panic!("third writer acquire must fail closed, got {other:?}"),
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_writer_pools_of_one_pass_probe_and_a_third_cannot_acquire(pool: PgPool) {
    let login = common::claim_refusal_audit_writer_login(&pool).await;
    let pool_one = common::pool_with_limit(&login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let pool_two = common::pool_with_limit(&login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let pool_three = overflow_pool(&login.url, WRITER_POOL_MAX_CONNECTIONS);

    let mut first = pool_one.acquire().await.expect("first writer backend");
    probe_writer_authority(&mut first)
        .await
        .expect("first replica authority probe");
    let mut second = pool_two.acquire().await.expect("second writer backend");
    probe_writer_authority(&mut second)
        .await
        .expect("second replica authority probe");

    let limit: i32 = sqlx::query_scalar(
        "SELECT rolconnlimit FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_writer_login'",
    )
    .fetch_one(&pool)
    .await
    .expect("writer role limit");
    assert_eq!(limit, WRITER_LOGIN_CONN_LIMIT);

    third_writer_must_fail(
        pool_three
            .acquire()
            .await
            .expect_err("third overlapping writer backend exceeds role limit 2"),
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_service_replicas_both_serve_ready_200_on_one_database(pool: PgPool) {
    let writer_login = common::claim_refusal_audit_writer_login(&pool).await;
    let retention_login = common::claim_refusal_audit_retention_login(&pool).await;
    let writer_one = common::pool_with_limit(&writer_login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let writer_two = common::pool_with_limit(&writer_login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let retention_one =
        common::pool_with_limit(&retention_login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let retention_two =
        common::pool_with_limit(&retention_login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let mut retention_backend_one = retention_one
        .acquire()
        .await
        .expect("first retention backend");
    probe_retention_authority(&mut retention_backend_one)
        .await
        .expect("first replica retention authority probe");
    let mut retention_backend_two = retention_two
        .acquire()
        .await
        .expect("second retention backend");
    probe_retention_authority(&mut retention_backend_two)
        .await
        .expect("second replica retention authority probe");

    let runtime_one = Arc::new(RefusalAuditRuntime::spawn(writer_one, audit_keys()));
    let runtime_two = Arc::new(RefusalAuditRuntime::spawn(writer_two, audit_keys()));
    wait_until_ready(&runtime_one).await;
    wait_until_ready(&runtime_two).await;

    let app_one = common::test_app(pool.clone()).await;
    let app_two = common::test_app(pool.clone()).await;
    let (addr_one, handle_one) = spawn_replica(app_one.state.with_refusal_audit(runtime_one)).await;
    let (addr_two, handle_two) = spawn_replica(app_two.state.with_refusal_audit(runtime_two)).await;

    let status_one = http_get_status(addr_one, "/ready").await;
    let status_two = http_get_status(addr_two, "/ready").await;
    assert_eq!(status_one, 200, "first replica /ready");
    assert_eq!(status_two, 200, "second replica /ready");
    assert_eq!(
        RETENTION_CONN_LIMIT_MIN, 2,
        "rolling image requires retention rolconnlimit >= 2"
    );

    handle_one.abort();
    handle_two.abort();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_retention_pools_of_one_pass_probe_after_0037(pool: PgPool) {
    let login = common::claim_refusal_audit_retention_login(&pool).await;
    let pool_one = common::pool_with_limit(&login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let pool_two = common::pool_with_limit(&login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let pool_three = overflow_pool(&login.url, RETENTION_POOL_MAX_CONNECTIONS);

    let mut first = pool_one.acquire().await.expect("first retention backend");
    probe_retention_authority(&mut first)
        .await
        .expect("first replica retention authority probe");
    let mut second = pool_two.acquire().await.expect("second retention backend");
    probe_retention_authority(&mut second)
        .await
        .expect("second replica retention authority probe");

    let limit: i32 = sqlx::query_scalar(
        "SELECT rolconnlimit FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_retention'",
    )
    .fetch_one(&pool)
    .await
    .expect("retention role limit");
    assert_eq!(limit, RETENTION_CONN_LIMIT_MIN);

    third_writer_must_fail(
        pool_three
            .acquire()
            .await
            .expect_err("third overlapping retention backend exceeds role limit 2"),
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_migrate_on_an_already_applied_catalog_is_a_noop(pool: PgPool) {
    forget_test_only_restore_migration(&pool).await;
    let before: Vec<(i64, bool)> =
        sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("catalog before");

    let pool_one = pool.clone();
    let pool_two = pool.clone();
    let (first, second) =
        tokio::join!(ALL_MIGRATIONS.run(&pool_one), ALL_MIGRATIONS.run(&pool_two));
    first.expect("first migrator");
    second.expect("second migrator");

    let after: Vec<(i64, bool)> =
        sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("catalog after");
    assert_eq!(
        before, after,
        "already-applied migrate must not change the catalog"
    );
    assert!(
        after.iter().all(|(_, success)| *success),
        "every recorded migration remains successful"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_replicas_hold_writer_and_retention_during_concurrent_migrate(pool: PgPool) {
    let writer_login = common::claim_refusal_audit_writer_login(&pool).await;
    let retention_login = common::claim_refusal_audit_retention_login(&pool).await;
    let writer_one = common::pool_with_limit(&writer_login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let writer_two = common::pool_with_limit(&writer_login.url, WRITER_POOL_MAX_CONNECTIONS).await;
    let retention_one =
        common::pool_with_limit(&retention_login.url, RETENTION_POOL_MAX_CONNECTIONS).await;
    let retention_two =
        common::pool_with_limit(&retention_login.url, RETENTION_POOL_MAX_CONNECTIONS).await;

    let mut writer_backend_one = writer_one.acquire().await.expect("first writer backend");
    probe_writer_authority(&mut writer_backend_one)
        .await
        .expect("first writer probe");
    let mut writer_backend_two = writer_two.acquire().await.expect("second writer backend");
    probe_writer_authority(&mut writer_backend_two)
        .await
        .expect("second writer probe");
    let mut retention_backend_one = retention_one
        .acquire()
        .await
        .expect("first retention backend");
    probe_retention_authority(&mut retention_backend_one)
        .await
        .expect("first retention probe");
    let mut retention_backend_two = retention_two
        .acquire()
        .await
        .expect("second retention backend");
    probe_retention_authority(&mut retention_backend_two)
        .await
        .expect("second retention probe");

    forget_test_only_restore_migration(&pool).await;
    let domain_one = pool.clone();
    let domain_two = pool.clone();
    let (first, second) = tokio::join!(
        ALL_MIGRATIONS.run(&domain_one),
        ALL_MIGRATIONS.run(&domain_two)
    );
    first.expect("first overlapping migrator");
    second.expect("second overlapping migrator");
}
