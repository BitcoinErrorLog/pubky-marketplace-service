mod common;

use chrono::{Duration, Timelike, Utc};
use marketplace_service::refusal_audit::{
    AuditKeys, CommandKind, RefusalAuditRuntime, RefusalKind, SurfaceKind,
};
use marketplace_service::{
    clock::AdjustableClock,
    config::Config,
    homeserver::{HomeserverFetchOutcome, HomeserverListingClient},
    http::build_router,
    locks::LocksRuntime,
    AppState,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

static WRITER_LOGIN_FIXTURE: Mutex<()> = Mutex::const_new(());

type AttackBucketRow = (i16, i64, Vec<u8>, bool, Option<Vec<u8>>);

struct ActualWriterFixture {
    runtime: Arc<RefusalAuditRuntime>,
    pool: PgPool,
    keys: AuditKeys,
    login_guard: Option<MutexGuard<'static, ()>>,
}

impl Drop for ActualWriterFixture {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let login_guard = self
            .login_guard
            .take()
            .expect("writer fixture guard is present");
        tokio::spawn(async move {
            pool.close().await;
            drop(login_guard);
        });
    }
}

struct RefusalAuditLocksHomeserver {
    documents: HashMap<(String, String), serde_json::Value>,
}

impl HomeserverListingClient for RefusalAuditLocksHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_content_lock<'a>(
        &'a self,
        creator_pubky: &'a str,
        content_path: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            match self
                .documents
                .get(&(creator_pubky.to_string(), content_path.to_string()))
            {
                Some(document) => HomeserverFetchOutcome::Found(document.clone()),
                None => HomeserverFetchOutcome::NotFound,
            }
        })
    }
}

async fn refusal_audit_locks_app(pool: PgPool, seller_pubky: &str) -> common::TestApp {
    let now: chrono::DateTime<Utc> = common::NOW.parse().expect("timestamp");
    let clock = Arc::new(AdjustableClock::new(now));
    let resource = common::lock_resource_for_payment(seller_pubky, 27_400, "USD");
    let path = resource
        .strip_prefix(seller_pubky)
        .expect("creator-prefixed resource")
        .to_string();
    let homeserver = Arc::new(RefusalAuditLocksHomeserver {
        documents: HashMap::from([(
            (seller_pubky.to_string(), path),
            common::lock_document_for(seller_pubky, 27_400, "USD"),
        )]),
    });
    let locks = Arc::new(LocksRuntime {
        keys: common::test_locks_keys(),
        client: Arc::new(common::FakeLocksClient::default()),
    });
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
        .with_locks(Some(locks))
        .with_homeserver(Some(homeserver));
    common::TestApp {
        router: build_router(state.clone()),
        pool,
        clock,
        state,
    }
}

fn hour(value: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    value
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("valid hour")
}

async fn insert_bucket(
    pool: &PgPool,
    bucket_start: chrono::DateTime<Utc>,
    actor_byte: u8,
    count: i64,
) {
    sqlx::query(
        "INSERT INTO command_refusal_audit_buckets \
         (bucket_start, surface_kind, command_kind, refusal_kind, actor_key_epoch, actor_tag, \
          occurrence_count, first_occurred_at, last_occurred_at, command_id_present) \
         VALUES ($1, 1, 14, 12, 1, $2, $3, $1, $1, false)",
    )
    .bind(bucket_start)
    .bind(vec![actor_byte; 16])
    .bind(count)
    .execute(pool)
    .await
    .expect("insert audit fixture");
}

async fn actual_writer_fixture(pool: &PgPool) -> ActualWriterFixture {
    let login_guard = WRITER_LOGIN_FIXTURE.lock().await;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .expect("database name");
    let password = format!("test-only-{}", Uuid::new_v4());
    sqlx::query(&format!(
        "ALTER ROLE marketplace_refusal_audit_writer_login PASSWORD '{}'",
        password.replace('\'', "''")
    ))
    .execute(pool)
    .await
    .expect("set isolated test login password");
    let quoted_database = format!("\"{}\"", database.replace('"', "\"\""));
    sqlx::query(&format!(
        "GRANT CONNECT ON DATABASE {quoted_database} TO marketplace_refusal_audit_writer_login"
    ))
    .execute(pool)
    .await
    .expect("grant test database connect");
    let url = format!(
        "postgres://marketplace_refusal_audit_writer_login:{}@localhost:5432/{}",
        password, database
    );
    let writer_pool = PgPoolOptions::new()
        .max_connections(2)
        .min_connections(0)
        .acquire_timeout(StdDuration::from_millis(100))
        .idle_timeout(Some(StdDuration::from_secs(60)))
        .connect(&url)
        .await
        .expect("actual writer login connects");
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
    let keys = AuditKeys::parse(&root, "1", None, None).expect("keys");
    let runtime = Arc::new(RefusalAuditRuntime::spawn(
        writer_pool.clone(),
        keys.clone(),
    ));
    for _ in 0..50 {
        if runtime.is_ready() {
            return ActualWriterFixture {
                runtime,
                pool: writer_pool,
                keys,
                login_guard: Some(login_guard),
            };
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("online least-authority probe did not become ready");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_summary_is_actor_aggregated_and_access_audited(pool: PgPool) {
    let now = hour(Utc::now());
    insert_bucket(&pool, now - Duration::hours(2), 1, 2).await;
    insert_bucket(&pool, now - Duration::hours(2), 2, 3).await;

    let rows = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1, $2, NULL, NULL, NULL, 100, NULL)",
    )
    .bind(now - Duration::hours(3))
    .bind(now)
    .fetch_all(&pool)
    .await
    .expect("summary function succeeds");

    assert_eq!(rows.len(), 1, "summary must not expose actor multiplicity");
    assert_eq!(rows[0].get::<i64, _>("occurrence_count"), 5);
    let access_count: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(occurrence_count), 0)::bigint \
         FROM command_refusal_audit_access_buckets",
    )
    .fetch_one(&pool)
    .await
    .expect("access audit reads");
    assert_eq!(access_count, 1, "operator read must be audited first");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_operator_quota_is_bounded_and_fail_closed(pool: PgPool) {
    let day = Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    sqlx::query(
        "INSERT INTO command_refusal_audit_access_limits(bucket_start, admitted_rows) \
         VALUES ($1, 1000)",
    )
    .bind(day)
    .execute(&pool)
    .await
    .expect("quota limit fixture");
    sqlx::query(
        "INSERT INTO command_refusal_audit_access_buckets(\
           bucket_start,session_role,function_kind,filter_class,result_size_band,occurrence_count) \
         SELECT $1, ('quota_fixture_' || g)::name, 1, 0, 0, 1 \
         FROM generate_series(1,1000) g",
    )
    .bind(day)
    .execute(&pool)
    .await
    .expect("quota fixture");
    let now = hour(Utc::now());
    let rows = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&pool)
    .await
    .expect("quota denial is a bounded empty result");
    assert!(
        rows.is_empty(),
        "quota exhaustion must return no audit data"
    );
    let overflow: i64 = sqlx::query_scalar(
        "SELECT overflow_count FROM command_refusal_audit_access_limits WHERE bucket_start = $1",
    )
    .bind(day)
    .fetch_one(&pool)
    .await
    .expect("quota overflow");
    assert_eq!(overflow, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_operator_cursor_is_validated_and_keyset(pool: PgPool) {
    let now = hour(Utc::now());
    insert_bucket(&pool, now - Duration::hours(3), 1, 1).await;
    insert_bucket(&pool, now - Duration::hours(2), 2, 1).await;

    let summary_first = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,1,NULL)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .fetch_one(&pool)
    .await
    .expect("first summary page");
    let summary_cursor = serde_json::json!({
        "bucket_start": summary_first.get::<chrono::DateTime<Utc>, _>("bucket_start"),
        "surface_kind": summary_first.get::<i16, _>("surface_kind"),
        "command_kind": summary_first.get::<i16, _>("command_kind"),
        "refusal_kind": summary_first.get::<i16, _>("refusal_kind"),
    });
    let summary_second = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,1,$3)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .bind(summary_cursor)
    .fetch_one(&pool)
    .await
    .expect("second summary page");
    assert_ne!(
        summary_first.get::<chrono::DateTime<Utc>, _>("bucket_start"),
        summary_second.get::<chrono::DateTime<Utc>, _>("bucket_start")
    );

    let first = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_buckets($1, $2, NULL, NULL, NULL, 1, NULL)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .fetch_one(&pool)
    .await
    .expect("first keyset page");
    let cursor = serde_json::json!({
        "bucket_start": first.get::<chrono::DateTime<Utc>, _>("bucket_start"),
        "surface_kind": first.get::<i16, _>("surface_kind"),
        "command_kind": first.get::<i16, _>("command_kind"),
        "refusal_kind": first.get::<i16, _>("refusal_kind"),
        "actor_key_epoch": first.get::<i16, _>("actor_key_epoch"),
        "actor_tag": hex::encode(first.get::<Vec<u8>, _>("actor_tag")),
    });
    let second = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_buckets($1, $2, NULL, NULL, NULL, 1, $3)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .bind(cursor)
    .fetch_one(&pool)
    .await
    .expect("second keyset page");
    assert_ne!(
        first.get::<chrono::DateTime<Utc>, _>("bucket_start"),
        second.get::<chrono::DateTime<Utc>, _>("bucket_start"),
        "cursor must advance rather than repeat page one"
    );

    let malformed = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_buckets($1, $2, NULL, NULL, NULL, 1, $3)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .bind(serde_json::json!({"bucket_start": "attacker"}))
    .fetch_all(&pool)
    .await;
    assert!(malformed.is_err(), "malformed cursors fail closed");
    let invalid_filter = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1, $2, 999, NULL, NULL, 1, NULL)",
    )
    .bind(now - Duration::hours(4))
    .bind(now)
    .fetch_all(&pool)
    .await;
    assert!(
        invalid_filter.is_err(),
        "unknown static filters fail closed"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_objects_have_hardened_owners_and_acls(pool: PgPool) {
    let catalog: Vec<(i16, String)> =
        sqlx::query_as("SELECT id, name FROM command_refusal_kinds ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("refusal catalog");
    let source = RefusalKind::ALL
        .iter()
        .map(|kind| (*kind as i16, kind.name().to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        catalog, source,
        "source and migration catalogs have exact parity"
    );
    let command_catalog: Vec<(i16, String)> =
        sqlx::query_as("SELECT id, name FROM command_refusal_command_kinds ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("command catalog");
    let command_source = CommandKind::ALL
        .iter()
        .map(|kind| (*kind as i16, kind.name().to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        command_catalog, command_source,
        "closed command-kind enum and migration catalog have exact parity"
    );
    let immutable = sqlx::query("UPDATE command_refusal_kinds SET name=name WHERE id=1")
        .execute(&pool)
        .await
        .expect_err("catalog mutation trigger is active");
    assert_eq!(
        immutable.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("P0001"))
    );

    let unsafe_roles: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname IN (\
           'marketplace_refusal_audit_owner', 'marketplace_refusal_audit_writer', \
           'marketplace_refusal_audit_aggregate', 'marketplace_refusal_audit_raw', \
           'marketplace_refusal_audit_retention', 'marketplace_refusal_audit_writer_login') \
         AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls \
              OR rolinherit OR rolconnlimit <> CASE \
                WHEN rolname = 'marketplace_refusal_audit_writer_login' THEN 2 \
                WHEN rolname = 'marketplace_refusal_audit_retention' THEN 1 ELSE -1 END \
              OR rolcanlogin <> (rolname IN (\
                'marketplace_refusal_audit_writer_login', 'marketplace_refusal_audit_retention')))",
    )
    .fetch_one(&pool)
    .await
    .expect("role inventory");
    assert_eq!(
        unsafe_roles, 0,
        "every named role has exact safe attributes"
    );
    let migration_memberships: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_catalog.pg_auth_members m \
         JOIN pg_catalog.pg_roles member ON member.oid = m.member \
         JOIN pg_catalog.pg_roles parent ON parent.oid = m.roleid \
         WHERE member.rolname = current_user \
           AND parent.rolname LIKE 'marketplace_refusal_audit_%'",
    )
    .fetch_one(&pool)
    .await
    .expect("migration membership inventory");
    assert_eq!(
        migration_memberships, 0,
        "migration/domain principal retains no audit-role membership"
    );

    let wrongly_owned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_catalog.pg_roles r ON r.oid = c.relowner \
         WHERE n.nspname = 'public' AND c.relname LIKE 'command_refusal_%' \
           AND r.rolname <> 'marketplace_refusal_audit_owner'",
    )
    .fetch_one(&pool)
    .await
    .expect("object owner inventory");
    assert_eq!(wrongly_owned, 0, "domain migrator must retain no ownership");

    let unsafe_definers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_catalog.pg_proc p \
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
         JOIN pg_catalog.pg_roles r ON r.oid = p.proowner \
         WHERE n.nspname = 'public' AND p.proname IN (\
           'operator_refusal_audit_summary', 'operator_refusal_audit_buckets', \
           'purge_refusal_audit', 'record_refusal_audit_access') AND (NOT p.prosecdef \
             OR r.rolname <> 'marketplace_refusal_audit_owner' \
             OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[] \
             OR has_function_privilege(0, p.oid, 'EXECUTE'))",
    )
    .fetch_one(&pool)
    .await
    .expect("security definer inventory");
    assert_eq!(unsafe_definers, 0, "SECURITY DEFINER inventory is exact");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_default_public_execute_is_red_then_green_denied(pool: PgPool) {
    let role = format!("refusal_unprivileged_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE ROLE \"{role}\" NOLOGIN NOINHERIT"))
        .execute(&pool)
        .await
        .expect("unprivileged role");
    sqlx::query(&format!("GRANT \"{role}\" TO CURRENT_USER"))
        .execute(&pool)
        .await
        .expect("test may set role");
    sqlx::query(&format!("GRANT USAGE ON SCHEMA public TO \"{role}\""))
        .execute(&pool)
        .await
        .expect("schema usage");
    let now = hour(Utc::now());
    let mut connection = pool.acquire().await.expect("test connection");
    sqlx::query(&format!("SET ROLE \"{role}\""))
        .execute(&mut *connection)
        .await
        .expect("set unprivileged role");
    let denied = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect_err("PUBLIC execute must be denied");
    assert_eq!(
        denied.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    let raw_denied = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_buckets($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect_err("PUBLIC raw execute must be denied");
    assert_eq!(
        raw_denied
            .as_database_error()
            .and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    let public_execute: bool = sqlx::query_scalar(
        "SELECT has_function_privilege(0, \
           'public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', \
           'EXECUTE') OR has_function_privilege(0, \
           'public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', \
           'EXECUTE')",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("PUBLIC function ACLs");
    assert!(!public_execute);
    let direct = sqlx::query("SELECT 1 FROM command_refusal_audit_buckets LIMIT 1")
        .fetch_all(&mut *connection)
        .await
        .expect_err("base-table reads are denied");
    assert_eq!(
        direct.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    sqlx::query("RESET ROLE")
        .execute(&mut *connection)
        .await
        .expect("reset role");
    sqlx::query(
        "GRANT EXECUTE ON FUNCTION public.operator_refusal_audit_summary(\
         timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) TO PUBLIC",
    )
    .execute(&mut *connection)
    .await
    .expect("calibrate PostgreSQL default-PUBLIC red posture");
    sqlx::query(&format!("SET ROLE \"{role}\""))
        .execute(&mut *connection)
        .await
        .expect("set red-posture role");
    sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect("red posture demonstrates PUBLIC execution");
    sqlx::query("RESET ROLE")
        .execute(&mut *connection)
        .await
        .expect("reset red posture");
    sqlx::query(
        "REVOKE EXECUTE ON FUNCTION public.operator_refusal_audit_summary(\
         timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) FROM PUBLIC",
    )
    .execute(&mut *connection)
    .await
    .expect("restore hardened ACL");
    sqlx::query(&format!("SET ROLE \"{role}\""))
        .execute(&mut *connection)
        .await
        .expect("set green-posture role");
    let denied_again = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect_err("green posture denies PUBLIC again");
    assert_eq!(
        denied_again
            .as_database_error()
            .and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    sqlx::query("RESET ROLE")
        .execute(&mut *connection)
        .await
        .expect("reset final posture");
    let access_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM command_refusal_audit_access_buckets")
            .fetch_one(&mut *connection)
            .await
            .expect("access count");
    assert_eq!(
        access_count, 1,
        "only the deliberately red successful call is access-audited"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_operator_roles_enforce_aggregate_and_raw_capabilities(pool: PgPool) {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("database name");
    let login = format!("refusal_operator_{}", Uuid::new_v4().simple());
    let password = format!("test-only-{}", Uuid::new_v4());
    sqlx::raw_sql(&format!(
        "CREATE ROLE \"{login}\" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE \
         NOREPLICATION NOBYPASSRLS PASSWORD '{password}'; \
         GRANT CONNECT ON DATABASE \"{database}\" TO \"{login}\"; \
         GRANT USAGE ON SCHEMA public TO \"{login}\"; \
         GRANT marketplace_refusal_audit_aggregate TO \"{login}\";"
    ))
    .execute(&pool)
    .await
    .expect("individual aggregate operator");
    let operator_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://{login}:{password}@localhost:5432/{database}"
        ))
        .await
        .expect("operator login");
    let now = hour(Utc::now());
    let direct = sqlx::query("SELECT 1 FROM command_refusal_audit_buckets")
        .fetch_all(&operator_pool)
        .await
        .expect_err("operator has no direct table read");
    assert_eq!(
        direct.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    let mut connection = operator_pool.acquire().await.expect("operator connection");
    sqlx::query("SET ROLE marketplace_refusal_audit_aggregate")
        .execute(&mut *connection)
        .await
        .expect("time-limited aggregate elevation");
    sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect("aggregate summary allowed");
    let raw = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_buckets($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(1))
    .bind(now)
    .fetch_all(&mut *connection)
    .await
    .expect_err("aggregate capability cannot read tag-bearing buckets");
    assert_eq!(
        raw.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    let access_actor: String = sqlx::query_scalar(
        "SELECT session_role::text FROM command_refusal_audit_access_buckets LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("access actor");
    assert_eq!(access_actor, login);
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_access_log_failure_prevents_operator_read(pool: PgPool) {
    let now = hour(Utc::now());
    insert_bucket(&pool, now - Duration::hours(1), 1, 5).await;
    sqlx::query(
        "REVOKE INSERT, UPDATE ON command_refusal_audit_access_buckets \
         FROM marketplace_refusal_audit_owner",
    )
    .execute(&pool)
    .await
    .expect("inject access-audit write denial");
    let result = sqlx::query(
        "SELECT * FROM public.operator_refusal_audit_summary($1,$2,NULL,NULL,NULL,10,NULL)",
    )
    .bind(now - Duration::hours(2))
    .bind(now)
    .fetch_all(&pool)
    .await;
    let error = result.expect_err("operator read fails when access record cannot be written");
    assert_eq!(
        error.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_migration_0032_direct_rerun_is_idempotent(pool: PgPool) {
    sqlx::raw_sql(include_str!("../migrations/0032_refusal_audit.sql"))
        .execute(&pool)
        .await
        .expect("0032 must be directly rerunnable");
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM command_refusal_surface_kinds), \
                (SELECT count(*) FROM command_refusal_command_kinds), \
                (SELECT count(*) FROM command_refusal_kinds)",
    )
    .fetch_one(&pool)
    .await
    .expect("catalog counts");
    assert_eq!(counts, (2, 35, 40));
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_migration_least_authority_rerun_is_blocked_without_admin_option(
    pool: PgPool,
) {
    let role = format!("refusal_migrator_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" LOGIN INHERIT NOSUPERUSER NOCREATEDB CREATEROLE \
         NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1"
    ))
    .execute(&pool)
    .await
    .expect("least-authority migration role");
    sqlx::query(&format!("GRANT \"{role}\" TO CURRENT_USER"))
        .execute(&pool)
        .await
        .expect("test role switch");
    sqlx::query(&format!("ALTER SCHEMA public OWNER TO \"{role}\""))
        .execute(&pool)
        .await
        .expect("fresh-database schema-owner posture");
    let mut connection = pool.acquire().await.expect("migration connection");
    sqlx::query(&format!("SET ROLE \"{role}\""))
        .execute(&mut *connection)
        .await
        .expect("least-authority posture");
    let blocked = sqlx::raw_sql(include_str!("../migrations/0032_refusal_audit.sql"))
        .execute(&mut *connection)
        .await
        .expect_err("PostgreSQL requires owner-role ADMIN OPTION for this direct rerun");
    assert_eq!(
        blocked.as_database_error().and_then(|error| error.code()),
        Some(std::borrow::Cow::Borrowed("42501"))
    );
    sqlx::query("RESET ROLE")
        .execute(&mut *connection)
        .await
        .expect("reset migration role");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_retention_is_oldest_first_bounded_and_resumable(pool: PgPool) {
    let now = hour(Utc::now());
    let holder_a = Uuid::new_v4();
    let holder_b = Uuid::new_v4();
    assert!(marketplace_service::workers::try_acquire_lease(
        &pool,
        marketplace_service::workers::TASK_REFUSAL_AUDIT_RETENTION,
        holder_a,
        now,
        30,
    )
    .await
    .expect("first retention holder"));
    assert!(!marketplace_service::workers::try_acquire_lease(
        &pool,
        marketplace_service::workers::TASK_REFUSAL_AUDIT_RETENTION,
        holder_b,
        now,
        30,
    )
    .await
    .expect("second retention holder"));
    sqlx::query(
        "INSERT INTO command_refusal_audit_bucket_limits(bucket_start, admitted_rows) \
         SELECT $1 - interval '31 days' - make_interval(hours => g), 0 \
         FROM generate_series(0, 600) g",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("limit backlog");
    sqlx::query(
        "INSERT INTO command_refusal_audit_buckets(\
           bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,\
           occurrence_count,first_occurred_at,last_occurred_at,command_id_present) \
         SELECT $1 - interval '31 days' - make_interval(hours => g), 1, 14, 12, 1, \
                decode(lpad(to_hex(g), 32, '0'), 'hex'), 1, \
                $1 - interval '31 days' - make_interval(hours => g), \
                $1 - interval '31 days' - make_interval(hours => g), false \
         FROM generate_series(0, 600) g",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("detail backlog");
    insert_bucket(&pool, now - Duration::days(30), 99, 1).await;

    let first: i32 = sqlx::query_scalar("SELECT public.purge_refusal_audit($1, 500)")
        .bind(now)
        .fetch_one(&pool)
        .await
        .expect("first bounded purge");
    assert_eq!(first, 1000, "at most two 500-row batches per tick");
    let remaining: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM command_refusal_audit_buckets \
                  WHERE bucket_start < $1 - interval '30 days') \
              + (SELECT count(*) FROM command_refusal_audit_bucket_limits \
                  WHERE bucket_start < $1 - interval '30 days')",
    )
    .bind(now)
    .fetch_one(&pool)
    .await
    .expect("remaining backlog");
    assert_eq!(remaining, 202);
    let second: i32 = sqlx::query_scalar("SELECT public.purge_refusal_audit($1, 500)")
        .bind(now)
        .fetch_one(&pool)
        .await
        .expect("resumed purge");
    assert_eq!(second, 202);
    let boundary: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM command_refusal_audit_buckets WHERE bucket_start = $1",
    )
    .bind(now - Duration::days(30))
    .fetch_one(&pool)
    .await
    .expect("boundary survives");
    assert_eq!(boundary, 1, "cutoff equality is retained");
    marketplace_service::workers::release_lease(
        &pool,
        marketplace_service::workers::TASK_REFUSAL_AUDIT_RETENTION,
        holder_a,
        now,
    )
    .await
    .expect("release retention lease");
    assert!(marketplace_service::workers::try_acquire_lease(
        &pool,
        marketplace_service::workers::TASK_REFUSAL_AUDIT_RETENTION,
        holder_b,
        now,
        30,
    )
    .await
    .expect("retention lease recovers"));
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_retention_registry_bounds_access_tables(pool: PgPool) {
    let now = hour(Utc::now());
    let day = now - Duration::days(31);
    sqlx::query(
        "INSERT INTO command_refusal_audit_access_limits(bucket_start, admitted_rows) \
         SELECT date_trunc('day', $1 - make_interval(days => g)), 0 \
         FROM generate_series(0,500) g",
    )
    .bind(day)
    .execute(&pool)
    .await
    .expect("access-limit backlog");
    sqlx::query(
        "INSERT INTO command_refusal_audit_access_buckets(\
           bucket_start,session_role,function_kind,filter_class,result_size_band,occurrence_count) \
         SELECT date_trunc('day', $1 - make_interval(days => g)), \
                ('retention_actor_' || g)::name, 1, 0, 0, 1 \
         FROM generate_series(0,500) g",
    )
    .bind(day)
    .execute(&pool)
    .await
    .expect("access-bucket backlog");
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT public.purge_refusal_audit($1,500)")
            .bind(now)
            .fetch_one(&pool)
            .await
            .expect("bounded access purge"),
        1000
    );
    let remaining: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM command_refusal_audit_access_buckets) + \
                (SELECT count(*) FROM command_refusal_audit_access_limits)",
    )
    .fetch_one(&pool)
    .await
    .expect("remaining access backlog");
    assert_eq!(remaining, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT public.purge_refusal_audit($1,500)")
            .bind(now)
            .fetch_one(&pool)
            .await
            .expect("resumed access purge"),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_retention_actual_login_is_probed_before_purge(pool: PgPool) {
    let now = hour(Utc::now());
    insert_bucket(&pool, now - Duration::days(31), 1, 1).await;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("database name");
    let password = format!("test-only-{}", Uuid::new_v4());
    sqlx::query(&format!(
        "ALTER ROLE marketplace_refusal_audit_retention PASSWORD '{}'",
        password.replace('\'', "''")
    ))
    .execute(&pool)
    .await
    .expect("set retention test password");
    let quoted_database = format!("\"{}\"", database.replace('"', "\"\""));
    sqlx::query(&format!(
        "GRANT CONNECT ON DATABASE {quoted_database} TO marketplace_refusal_audit_retention"
    ))
    .execute(&pool)
    .await
    .expect("retention connect");
    let url = format!(
        "postgres://marketplace_refusal_audit_retention:{}@localhost:5432/{}",
        password, database
    );
    let retention_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(StdDuration::from_millis(100))
        .connect(&url)
        .await
        .expect("actual retention login connects");
    assert_eq!(
        marketplace_service::refusal_audit::purge_once(&retention_pool, now)
            .await
            .expect("probed purge"),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_rotation_verifies_old_epoch_until_destroyed(pool: PgPool) {
    let previous_root =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
    let active_root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [8u8; 32]);
    let mut keys = AuditKeys::parse(&active_root, "2", Some(&previous_root), Some("1"))
        .expect("two retained epochs");
    let actor = pubky_common::crypto::Keypair::random().public_key().z32();
    let unrelated = pubky_common::crypto::Keypair::random().public_key().z32();
    let bucket = hour(Utc::now()) - Duration::hours(1);
    for (epoch, tag) in keys.erasure_candidates(&actor).expect("actor tags") {
        sqlx::query(
            "INSERT INTO command_refusal_audit_buckets(\
             bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,\
             occurrence_count,first_occurred_at,last_occurred_at,command_id_present) \
             VALUES ($1,1,14,12,$2,$3,1,$1,$1,false)",
        )
        .bind(bucket)
        .bind(epoch)
        .bind(tag.as_slice())
        .execute(&pool)
        .await
        .expect("erasable row");
    }
    let unrelated_tag = keys.actor_tag(&unrelated).expect("unrelated tag");
    sqlx::query(
        "INSERT INTO command_refusal_audit_buckets(\
         bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,\
         occurrence_count,first_occurred_at,last_occurred_at,command_id_present) \
         VALUES ($1,1,14,12,2,$2,1,$1,$1,false)",
    )
    .bind(bucket)
    .bind(unrelated_tag.as_slice())
    .execute(&pool)
    .await
    .expect("unrelated row");
    assert_eq!(keys.erase_actor(&pool, &actor).await.expect("erase"), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM command_refusal_audit_buckets")
            .fetch_one(&pool)
            .await
            .expect("remaining"),
        1
    );
    let old_tag = keys
        .actor_tag_for_epoch(1, &actor)
        .expect("old epoch derivation")
        .expect("previous key loaded");
    assert!(keys
        .verify_actor_tag(1, &actor, &old_tag)
        .expect("constant-time old verification"));
    assert!(keys.assert_second_rotation_safe(1, true, true).is_err());
    let expired = hour(Utc::now()) - Duration::days(31);
    sqlx::query(
        "INSERT INTO command_refusal_audit_buckets(\
         bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,\
         occurrence_count,first_occurred_at,last_occurred_at,command_id_present) \
         VALUES ($1,1,14,12,1,$2,1,$1,$1,false)",
    )
    .bind(expired)
    .bind(old_tag.as_slice())
    .execute(&pool)
    .await
    .expect("expired previous-epoch row");
    sqlx::query_scalar::<_, i32>("SELECT public.purge_refusal_audit($1, 500)")
        .bind(hour(Utc::now()))
        .fetch_one(&pool)
        .await
        .expect("age purge removes previous epoch");
    let retained_previous: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM command_refusal_audit_buckets WHERE actor_key_epoch = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("previous rows");
    assert_eq!(retained_previous, 0);
    keys.destroy_previous(retained_previous as u64, true, true)
        .expect("replica/backup-attested destruction");
    assert!(!keys
        .verify_actor_tag(1, &actor, &old_tag)
        .expect("destroyed epoch cannot verify"));
    sqlx::query("UPDATE command_refusal_audit_buckets SET actor_key_epoch = 3")
        .execute(&pool)
        .await
        .expect("unsupported retained epoch fixture");
    assert!(keys.assert_database_epochs_supported(&pool).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_uses_separate_least_privilege_pool(pool: PgPool) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let writer_pool = &writer.pool;
    assert_eq!(writer_pool.options().get_max_connections(), 2);
    assert_eq!(writer_pool.options().get_min_connections(), 0);
    assert_eq!(
        writer_pool.options().get_acquire_timeout(),
        StdDuration::from_millis(100)
    );
    assert_eq!(
        writer_pool.options().get_idle_timeout(),
        Some(StdDuration::from_secs(60))
    );
    let writer_identity: (String, String) =
        sqlx::query_as("SELECT session_user::text, current_user::text")
            .fetch_one(writer_pool)
            .await
            .expect("writer identity");
    assert_eq!(writer_identity.0, "marketplace_refusal_audit_writer_login");
    assert_eq!(writer_identity.1, "marketplace_refusal_audit_writer_login");
    assert!(runtime.is_ready(), "online least-authority probe passes");
    let actor = pubky_common::crypto::Keypair::random().public_key().z32();
    let envelope = runtime
        .envelope(
            Utc::now(),
            SurfaceKind::V1Command,
            CommandKind::PlaceBid,
            RefusalKind::BidTooLow,
            &actor,
            Some(Uuid::new_v4()),
        )
        .expect("fixed descriptor");
    runtime.try_send(envelope);
    for _ in 0..50 {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM command_refusal_audit_buckets")
            .fetch_one(&pool)
            .await
            .expect("bucket count");
        if count == 1 {
            assert_eq!(runtime.metrics().delivered, 1);
            let catalog_mutation =
                sqlx::query("UPDATE command_refusal_kinds SET name=name WHERE id=1")
                    .execute(writer_pool)
                    .await
                    .expect_err("writer cannot mutate catalogs");
            assert_eq!(
                catalog_mutation
                    .as_database_error()
                    .and_then(|error| error.code()),
                Some(std::borrow::Cow::Borrowed("42501"))
            );
            let bucket_delete = sqlx::query("DELETE FROM command_refusal_audit_buckets")
                .execute(writer_pool)
                .await
                .expect_err("writer cannot delete buckets");
            assert_eq!(
                bucket_delete
                    .as_database_error()
                    .and_then(|error| error.code()),
                Some(std::borrow::Cow::Borrowed("42501"))
            );
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!(
        "actual-login delivery did not insert: {:?}",
        runtime.metrics()
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_writer_identity_mismatch_blocks_readiness_and_drops_delivery(pool: PgPool) {
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
    let keys = AuditKeys::parse(&root, "1", None, None).expect("keys");
    let runtime = Arc::new(RefusalAuditRuntime::spawn(pool.clone(), keys));
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    assert!(
        !runtime.is_ready(),
        "domain principal can never satisfy writer readiness"
    );
    let actor = pubky_common::crypto::Keypair::random().public_key().z32();
    runtime.try_send(
        runtime
            .envelope(
                Utc::now(),
                SurfaceKind::V1Command,
                CommandKind::RegisterListing,
                RefusalKind::InvalidState,
                &actor,
                None,
            )
            .expect("descriptor"),
    );
    assert_eq!(runtime.metrics().dropped_writer_unverified, 1);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM command_refusal_audit_buckets")
        .fetch_one(&pool)
        .await
        .expect("bucket count");
    assert_eq!(rows, 0);
    let app = common::test_app(pool).await;
    let router =
        marketplace_service::http::build_router(app.state.clone().with_refusal_audit(runtime));
    let (status, _) = common::send(router, "GET", "/ready", None, &serde_json::Value::Null).await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_writer_failure_never_changes_command_outcome(pool: PgPool) {
    let app = common::test_app(pool.clone()).await;
    let actor = common::new_actor(&app).await;
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
    let keys = AuditKeys::parse(&root, "1", None, None).expect("keys");
    let runtime = Arc::new(RefusalAuditRuntime::spawn(pool.clone(), keys));
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let router = marketplace_service::http::build_router(
        app.state.clone().with_refusal_audit(runtime.clone()),
    );
    let (status, body) = common::send(
        router,
        "POST",
        "/v1/commands",
        Some(&actor.token),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "INVALID_COMMAND");
    assert_eq!(runtime.metrics().dropped_writer_unverified, 1);
    let outcomes: i64 = sqlx::query_scalar("SELECT count(*) FROM command_results")
        .fetch_one(&pool)
        .await
        .expect("domain outcomes");
    assert_eq!(outcomes, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_exhausted_writer_pool_never_starves_domain_pool(pool: PgPool) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let writer_pool = &writer.pool;
    let held_one = writer_pool
        .acquire()
        .await
        .expect("first writer connection");
    let held_two = writer_pool
        .acquire()
        .await
        .expect("second writer connection");
    let app = common::test_app(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let bidder = common::new_actor(&app).await;
    common::execute(
        &app,
        &seller.token,
        &common::register_auction_command(&seller.pubky),
    )
    .await;
    let router = marketplace_service::http::build_router(
        app.state.clone().with_refusal_audit(Arc::clone(runtime)),
    );
    let mut requests = tokio::task::JoinSet::new();
    for index in 0..25 {
        let router = router.clone();
        let token = bidder.token.clone();
        let mut command = common::place_bid_command(&seller.pubky, 0, 7_000, 0);
        command["command_id"] = serde_json::json!(Uuid::from_u128(0x9000 + index));
        requests.spawn(async move {
            tokio::time::timeout(
                StdDuration::from_millis(250),
                common::send(router, "POST", "/v1/commands", Some(&token), &command),
            )
            .await
            .expect("command must not await exhausted writer capacity")
        });
    }
    while let Some(result) = requests.join_next().await {
        let (status, body) = result.expect("request task");
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "REVISION_CONFLICT");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM bids")
            .fetch_one(&pool)
            .await
            .expect("domain state"),
        0
    );

    tokio::time::sleep(StdDuration::from_millis(500)).await;
    let metrics = runtime.metrics();
    assert!(metrics.retries_acquire_timeout >= 3);
    assert!(metrics.dropped_after_retries >= 1);
    drop((held_one, held_two));
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_ambiguous_commit_is_not_retried_and_discards_connection(pool: PgPool) {
    sqlx::raw_sql(
        "CREATE FUNCTION terminate_refusal_writer_on_commit() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NEW; END $$; \
         CREATE CONSTRAINT TRIGGER refusal_writer_ambiguous_commit \
         AFTER INSERT ON command_refusal_audit_buckets DEFERRABLE INITIALLY DEFERRED \
         FOR EACH ROW EXECUTE FUNCTION terminate_refusal_writer_on_commit();",
    )
    .execute(&pool)
    .await
    .expect("deferred commit fault");
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let actor = common::new_actor(&common::test_app(pool.clone()).await).await;
    runtime.try_send(
        runtime
            .envelope(
                Utc::now(),
                SurfaceKind::V1Command,
                CommandKind::PlaceBid,
                RefusalKind::BidTooLow,
                &actor.pubky,
                Some(Uuid::new_v4()),
            )
            .expect("descriptor"),
    );
    for _ in 0..100 {
        if runtime.metrics().dropped_ambiguous_commit == 1 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    let metrics = runtime.metrics();
    assert_eq!(metrics.dropped_ambiguous_commit, 1);
    assert_eq!(
        metrics.retries_db_error, 0,
        "ambiguous commit is never retried"
    );
    assert_eq!(metrics.delivered, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM command_refusal_audit_buckets")
            .fetch_one(&pool)
            .await
            .expect("ambiguous transaction result"),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_writer_panic_restarts_and_flushes_gap(pool: PgPool) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let actor = common::new_actor(&common::test_app(pool.clone()).await).await;
    let descriptor = || {
        runtime
            .envelope(
                Utc::now(),
                SurfaceKind::V1Command,
                CommandKind::PlaceBid,
                RefusalKind::BidTooLow,
                &actor.pubky,
                Some(Uuid::new_v4()),
            )
            .expect("descriptor")
    };
    runtime.inject_writer_panic_once();
    runtime.try_send(descriptor());
    for _ in 0..100 {
        if runtime.metrics().writer_panics == 1 && runtime.is_ready() {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    assert_eq!(runtime.metrics().writer_panics, 1);
    assert!(
        runtime.is_ready(),
        "supervised writer restarts and reprobes"
    );

    runtime.try_send(descriptor());
    for _ in 0..100 {
        let gap_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM command_refusal_audit_loss_gap WHERE id)",
        )
        .fetch_one(&pool)
        .await
        .expect("gap state");
        if runtime.metrics().delivered == 1 && gap_exists {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    assert_eq!(runtime.metrics().delivered, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT pending_loss_count FROM command_refusal_audit_loss_gap WHERE id",
        )
        .fetch_one(&pool)
        .await
        .expect("coalesced panic gap"),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_identity_spoof_and_command_id_attacks_do_not_change_bucket_keys(
    pool: PgPool,
) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let keys = &writer.keys;
    let mut app = common::test_app(pool.clone()).await;
    app.state = app.state.clone().with_refusal_audit(runtime.clone());
    app.router = marketplace_service::http::build_router(app.state.clone());
    let seller = common::new_actor(&app).await;
    let bidder = common::new_actor(&app).await;
    common::execute(
        &app,
        &seller.token,
        &common::register_auction_command(&seller.pubky),
    )
    .await;

    let repeated_id = Uuid::new_v4();
    for index in 0..12 {
        let mut command = common::place_bid_command(&seller.pubky, 1, 7_000 + index, 1);
        command["command_id"] = serde_json::json!(if index < 2 {
            repeated_id
        } else {
            Uuid::new_v4()
        });
        command["payload"]["maximum_amount"]["currency"] = serde_json::json!("EUR");
        let (_, body) = common::execute(&app, &bidder.token, &command).await;
        assert_eq!(body["error"]["code"], "INVALID_COMMAND");
    }
    let mut malformed = common::place_bid_command(&seller.pubky, 1, 7_000, 1);
    malformed["command_id"] = serde_json::json!("not-a-uuid");
    let (_, body) = common::execute(&app, &bidder.token, &malformed).await;
    assert_eq!(body["error"]["code"], "INVALID_COMMAND");

    tokio::time::timeout(StdDuration::from_secs(2), async {
        while runtime.metrics().delivered != 13 {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("all real writer deliveries committed");
    assert_eq!(runtime.metrics().delivered, 13);
    let expected_actor_tag = keys.actor_tag(&bidder.pubky).expect("session actor tag");
    let rows: Vec<AttackBucketRow> = sqlx::query_as(
        "SELECT refusal_kind, occurrence_count, actor_tag, command_id_present, sample_command_tag \
         FROM command_refusal_audit_buckets ORDER BY refusal_kind",
    )
    .fetch_all(&pool)
    .await
    .expect("attack buckets");
    assert_eq!(rows.len(), 2, "UUID changes never create per-command rows");
    assert_eq!(rows[0].0, RefusalKind::InvalidEnvelope as i16);
    assert_eq!(rows[0].1, 1);
    assert!(!rows[0].3);
    assert!(rows[0].4.is_none());
    assert_eq!(rows[1].0, RefusalKind::BidWrongAsset as i16);
    assert_eq!(rows[1].1, 12);
    for (_, _, actor_tag, _, _) in rows {
        assert_eq!(actor_tag, expected_actor_tag);
        assert_ne!(
            actor_tag,
            keys.actor_tag(&seller.pubky).expect("spoofed seller tag")
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_concurrent_delivery_obeys_bounds(pool: PgPool) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let app = common::test_app(pool.clone()).await;
    let mut actors = Vec::new();
    for _ in 0..20 {
        actors.push(common::new_actor(&app).await);
    }
    let router = marketplace_service::http::build_router(
        app.state.clone().with_refusal_audit(Arc::clone(runtime)),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for actor in actors {
        for _ in 0..10 {
            let router = router.clone();
            let token = actor.token.clone();
            tasks.spawn(async move {
                common::send(
                    router,
                    "POST",
                    "/v1/commands",
                    Some(&token),
                    &serde_json::json!({}),
                )
                .await
            });
        }
    }
    while let Some(result) = tasks.join_next().await {
        let (status, body) = result.expect("request task");
        assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "INVALID_COMMAND");
    }
    for _ in 0..200 {
        if runtime.metrics().delivered == 200 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    assert_eq!(runtime.metrics().delivered, 200);
    let (rows, admitted, occurrences): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), COALESCE(max(l.admitted_rows),0)::bigint, \
                COALESCE(sum(b.occurrence_count),0)::bigint \
         FROM command_refusal_audit_buckets b \
         LEFT JOIN command_refusal_audit_bucket_limits l USING (bucket_start)",
    )
    .fetch_one(&pool)
    .await
    .expect("concurrent delivery totals");
    assert_eq!(rows, 20);
    assert_eq!(admitted, 20);
    assert_eq!(occurrences, 200);
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_forbidden_plaintext_inverse_scan(pool: PgPool) {
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    let actor = pubky_common::crypto::Keypair::random().public_key().z32();
    let command_id = Uuid::new_v4();
    let app = common::test_app(pool.clone()).await;
    let authenticated = common::new_actor(&app).await;
    let router = marketplace_service::http::build_router(
        app.state.clone().with_refusal_audit(Arc::clone(runtime)),
    );
    let (status, _) = common::send(
        router,
        "POST",
        "/v1/commands",
        Some(&authenticated.token),
        &serde_json::json!({
            "pin": "unique-pin-739184",
            "maximum": "private-maximum-918273",
            "refund_reference": "refund-reference-564738",
            "bundle": "bundle-secret-192837",
            "unicode": "\u{202e}audit-injection\ncontrol"
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    runtime.try_send(
        runtime
            .envelope(
                Utc::now(),
                SurfaceKind::V1Command,
                CommandKind::PlaceBid,
                RefusalKind::BidTooLow,
                &actor,
                Some(command_id),
            )
            .expect("bounded envelope"),
    );
    for _ in 0..50 {
        if runtime.metrics().delivered == 2 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    assert_eq!(runtime.metrics().delivered, 2);
    let stored: String = sqlx::query_scalar(
        "SELECT concat_ws('|', \
           COALESCE((SELECT string_agg(row_to_json(x)::text, '|') FROM command_refusal_audit_buckets x), ''), \
           COALESCE((SELECT string_agg(row_to_json(x)::text, '|') FROM command_refusal_audit_bucket_limits x), ''), \
           COALESCE((SELECT string_agg(row_to_json(x)::text, '|') FROM command_refusal_audit_access_buckets x), ''), \
           COALESCE((SELECT string_agg(row_to_json(x)::text, '|') FROM command_refusal_audit_access_limits x), ''))",
    )
    .fetch_one(&pool)
    .await
    .expect("inverse scan");
    for forbidden in [
        actor.as_str(),
        authenticated.pubky.as_str(),
        &command_id.to_string(),
        "unique-pin-739184",
        "private-maximum-918273",
        "refund-reference-564738",
        "bundle-secret-192837",
        "\u{202e}audit-injection",
    ] {
        assert!(!stored.contains(forbidden), "forbidden plaintext persisted");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_manual_resolve_missing_pin_is_recorded_after_domain_release(pool: PgPool) {
    use common::paykit_review::{into_manual_review_held, resolve_call};

    let (mut app, _stripe, paykit) = common::test_app_with_payments(pool.clone()).await;
    let seller = common::new_actor(&app).await;
    let buyer = common::new_actor(&app).await;
    let (order_id, _) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    sqlx::query("UPDATE orders SET paykit_stack_id = NULL WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).expect("order id"))
        .execute(&pool)
        .await
        .expect("remove pin fixture");
    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    app.state = app.state.clone().with_refusal_audit(Arc::clone(runtime));
    app.router = marketplace_service::http::build_router(app.state.clone());

    let (status, body) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &serde_json::json!({"outcome": "paid"}),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], "missing_pin");
    for _ in 0..50 {
        let count: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(occurrence_count), 0)::bigint \
             FROM command_refusal_audit_buckets \
             WHERE surface_kind = 2 AND command_kind = 34 AND refusal_kind = 30",
        )
        .fetch_one(&pool)
        .await
        .expect("manual refusal count");
        if count == 1 {
            assert_eq!(runtime.metrics().delivered, 1);
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("manual-resolution refusal was not recorded");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_enqueue_happens_after_domain_rollback_release(pool: PgPool) {
    let seller_key = common::random_keypair();
    let seller_pubky = seller_key.1.clone();
    let mut app = refusal_audit_locks_app(pool.clone(), &seller_pubky).await;
    let seller = common::TestActor {
        token: common::authenticate(&app, &seller_key.0).await,
        keypair: seller_key.0,
        pubky: seller_pubky,
    };
    let buyer = common::new_actor(&app).await;
    let resource = common::lock_resource_for_payment(&seller.pubky, 27_400, "USD");
    for (listing_id, command_number) in [("boots_01", 1_u64), ("cap_01", 2)] {
        let mut listing =
            common::register_listing_command(&seller.pubky, listing_id, 1, command_number);
        listing["payload"]["digital_lock"] = serde_json::json!({
            "policyUri": resource,
            "criterionId": "paykit",
        });
        let (status, body) = common::execute(&app, &seller.token, &listing).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    }
    let listing_a = format!("listing:{}_boots_01", seller.pubky);
    let listing_b = format!("listing:{}_cap_01", seller.pubky);
    let checkout_id = Uuid::new_v4();
    let checkout = serde_json::json!({
        "version": 1,
        "command_id": checkout_id,
        "aggregate_id": format!("checkout:{checkout_id}"),
        "expected_revision": 0,
        "issued_at": common::NOW,
        "kind": "checkout.create",
        "payload": {
            "lines": [
                { "listing_aggregate_id": listing_a.clone(), "expected_revision": 1, "quantity": 1 },
                { "listing_aggregate_id": listing_b.clone(), "expected_revision": 1, "quantity": 1 }
            ],
            "delivery_address": {
                "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                "city": "New York", "region": "NY", "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    });
    let (status, body) = common::execute(&app, &buyer.token, &checkout).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id");
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");
    sqlx::query(
        "UPDATE listings SET state='reserved', available_quantity=0, reserved_quantity=1, \
         server_revision=server_revision+1, updated_at=clock_timestamp() WHERE aggregate_id=$1",
    )
    .bind(&listing_b)
    .execute(&pool)
    .await
    .expect("second line sold out");

    let writer = actual_writer_fixture(&pool).await;
    let runtime = &writer.runtime;
    app.state = app.state.clone().with_refusal_audit(Arc::clone(runtime));
    app.router = build_router(app.state.clone());
    let before: serde_json::Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(\
          'listings',(SELECT jsonb_agg(row_to_json(x) ORDER BY aggregate_id) FROM listings x),\
          'orders',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM orders x),\
          'payments',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM payments x),\
          'correlations',(SELECT jsonb_agg(row_to_json(x) ORDER BY payment_id) FROM payment_locks_correlations x),\
          'events',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM events x),\
          'results',(SELECT jsonb_agg(row_to_json(x) ORDER BY command_id) FROM command_results x))",
    )
    .fetch_one(&pool)
    .await
    .expect("domain facts before refusal");
    let (status, body) = common::execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(payment_id, 640),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "INSUFFICIENT_INVENTORY");
    let after: serde_json::Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(\
          'listings',(SELECT jsonb_agg(row_to_json(x) ORDER BY aggregate_id) FROM listings x),\
          'orders',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM orders x),\
          'payments',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM payments x),\
          'correlations',(SELECT jsonb_agg(row_to_json(x) ORDER BY payment_id) FROM payment_locks_correlations x),\
          'events',(SELECT jsonb_agg(row_to_json(x) ORDER BY id) FROM events x),\
          'results',(SELECT jsonb_agg(row_to_json(x) ORDER BY command_id) FROM command_results x))",
    )
    .fetch_one(&pool)
    .await
    .expect("released domain connection is immediately reusable");
    assert_eq!(after, before, "the complete domain transaction rolled back");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM orders WHERE id=$1::uuid")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("order remains readable"),
        "pending_payment"
    );
    for _ in 0..100 {
        let count: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(occurrence_count),0)::bigint \
             FROM command_refusal_audit_buckets \
             WHERE surface_kind=1 AND command_kind=17 AND refusal_kind=7",
        )
        .fetch_one(&pool)
        .await
        .expect("later audit aggregate");
        if count == 1 {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("rollback-first refusal was not delivered after connection release");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_d26_2_precedence_and_personal_minimum_categories_are_exact(pool: PgPool) {
    let mut app = common::test_app(pool.clone()).await;
    let writer = actual_writer_fixture(&pool).await;
    app.state = app
        .state
        .clone()
        .with_refusal_audit(Arc::clone(&writer.runtime));
    app.router = marketplace_service::http::build_router(app.state.clone());
    let seller = common::new_actor(&app).await;
    let bidder = common::new_actor(&app).await;
    common::execute(
        &app,
        &seller.token,
        &common::register_auction_command(&seller.pubky),
    )
    .await;

    let (_, stale) = common::execute(
        &app,
        &bidder.token,
        &common::place_bid_command(&seller.pubky, 0, 1, 0),
    )
    .await;
    assert_eq!(stale["error"]["code"], "REVISION_CONFLICT");
    let mut wrong_asset_command = common::place_bid_command(&seller.pubky, 1, 7_000, 1);
    wrong_asset_command["payload"]["maximum_amount"]["currency"] = serde_json::json!("EUR");
    let (_, wrong_asset) = common::execute(&app, &bidder.token, &wrong_asset_command).await;
    assert_eq!(wrong_asset["error"]["code"], "INVALID_COMMAND");
    let (_, low) = common::execute(
        &app,
        &bidder.token,
        &common::place_bid_command(&seller.pubky, 2, 1, 1),
    )
    .await;
    assert_eq!(low["error"]["code"], "BID_TOO_LOW");
    for _ in 0..50 {
        let categories: Vec<i16> = sqlx::query_scalar(
            "SELECT refusal_kind FROM command_refusal_audit_buckets \
             WHERE command_kind = 14 ORDER BY refusal_kind",
        )
        .fetch_all(&pool)
        .await
        .expect("bid refusal categories");
        if categories.len() == 3 {
            assert_eq!(categories, vec![5, 12, 35]);
            let bids: i64 = sqlx::query_scalar("SELECT count(*) FROM bids")
                .fetch_one(&pool)
                .await
                .expect("bid count");
            assert_eq!(bids, 0, "refused private maxima are never stored");
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("D26.2 refusal categories were not delivered");
}

#[sqlx::test(migrations = "./migrations")]
async fn refusal_audit_retention_outage_does_not_abort_domain_worker_pass(pool: PgPool) {
    use sqlx::postgres::PgConnectOptions;

    let options: PgConnectOptions =
        "postgres://marketplace_refusal_audit_retention@127.0.0.1:1/unavailable"
            .parse()
            .expect("unavailable URL parses");
    let retention_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(StdDuration::from_millis(5))
        .connect_lazy_with(options);
    let app = common::test_app(pool).await;
    let state = app
        .state
        .clone()
        .with_refusal_audit_retention_pool(retention_pool);
    let summary = marketplace_service::workers::run_once(
        &state,
        Uuid::new_v4(),
        common::NOW.parse().expect("fixed worker time"),
    )
    .await
    .expect("audit retention outage must not abort the domain pass");
    assert_eq!(summary.refusal_audit_rows_purged, 0);
}
