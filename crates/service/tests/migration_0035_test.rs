use std::borrow::Cow;

use chrono::{DateTime, Utc};
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

#[test]
fn migration_0035_never_changes_roles_grants_or_ownership() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/migrations/0035_automation_apis.sql"
    ))
    .expect("migration source");
    let uppercase = source.to_ascii_uppercase();
    for forbidden in [
        "CREATE ROLE",
        "ALTER ROLE",
        "GRANT ",
        "REVOKE ",
        "ALTER OWNER",
        " OWNER TO ",
        "SECURITY DEFINER",
    ] {
        assert!(
            !uppercase.contains(forbidden),
            "migration 0035 must not contain {forbidden}"
        );
    }
}

#[sqlx::test(migrations = false)]
async fn migration_0035_upgrades_the_exact_0034_catalog_and_preserves_sessions(pool: PgPool) {
    let through_0034 = Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= 34)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    through_0034
        .run(&pool)
        .await
        .expect("production 0001..0034 catalog applies");
    let now: DateTime<Utc> = "2026-09-20T08:00:00Z".parse().expect("fixture time");
    let pubky = "y".repeat(52);
    sqlx::query(
        "INSERT INTO auth_sessions \
         (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, $2, '/:rw', $3, $4)",
    )
    .bind(vec![7_u8; 32])
    .bind(&pubky)
    .bind(now)
    .bind(now + chrono::Duration::days(1))
    .execute(&pool)
    .await
    .expect("0034 session fixture");

    ALL_MIGRATIONS
        .run(&pool)
        .await
        .expect("0035 applies to the exact production catalog");

    let row: (Uuid, String, serde_json::Value, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT session_id, pubky, client_metadata, revoked_at \
         FROM auth_sessions WHERE token_hash = $1",
    )
    .bind(vec![7_u8; 32])
    .fetch_one(&pool)
    .await
    .expect("upgraded session");
    assert_eq!(row.1, pubky);
    assert_eq!(row.2, serde_json::json!({}));
    assert_eq!(row.3, None);
}

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

    let endpoint_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'webhook_endpoints' \
           AND column_name = ANY($1)",
    )
    .bind(&["enqueue_sequence", "enqueue_checked_at"][..])
    .fetch_one(&pool)
    .await
    .expect("endpoint cursor catalog");
    assert_eq!(endpoint_columns, 2);

    let principal: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&pool)
        .await
        .expect("current principal");
    for table in [
        "webhook_endpoints",
        "webhook_deliveries",
        "webhook_dead_letters",
        "automation_rate_limits",
    ] {
        let (owner, dml): (String, bool) = sqlx::query_as(
            "SELECT tableowner, has_table_privilege(current_user, $1, \
             'SELECT,INSERT,UPDATE,DELETE') FROM pg_tables \
             WHERE schemaname = 'public' AND tablename = $1",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("owner and DML privileges");
        assert_eq!(
            owner, principal,
            "{table} must be owned by the migration principal"
        );
        assert!(dml, "{table} must be usable by the runtime principal");
    }

    let earlier_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 1 AND 34")
            .fetch_one(&pool)
            .await
            .expect("migration catalog");
    assert_eq!(earlier_count, 34, "no earlier migration may be omitted");
}
