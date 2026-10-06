//! Startup migrations.
//!
//! 0032 transfers the refusal-audit catalog to the NOLOGIN
//! `marketplace_refusal_audit_owner` and revokes it from the migration login.
//! 0040 and 0043 append to that catalog without taking the role back, so a
//! least-authority (non-superuser) migration login is refused. Both files are
//! applied in production and immutable, so the login borrows the owner role
//! for the run instead, the same way 0032's own rerun bootstrap does. A
//! superuser needs no membership and is left untouched.

use std::borrow::Cow;

use sqlx::migrate::Migrator;
use sqlx::{PgConnection, PgPool};

const OWNER_CREATED_BY: i64 = 32;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn run(pool: &PgPool) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    let superuser: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")
            .fetch_one(&mut *connection)
            .await?;
    if superuser || !pending(&mut connection).await? {
        return Ok(MIGRATOR.run_direct(&mut *connection).await?);
    }
    if !owner_exists(&mut connection).await? {
        through(OWNER_CREATED_BY)
            .run_direct(&mut *connection)
            .await?;
    }
    sqlx::query("GRANT marketplace_refusal_audit_owner TO CURRENT_USER")
        .execute(&mut *connection)
        .await?;
    let migrated = MIGRATOR.run_direct(&mut *connection).await;
    let revoked = sqlx::query("REVOKE marketplace_refusal_audit_owner FROM CURRENT_USER")
        .execute(&mut *connection)
        .await;
    migrated?;
    revoked?;
    Ok(())
}

async fn pending(connection: &mut PgConnection) -> sqlx::Result<bool> {
    let ledger: Option<String> = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::text")
        .fetch_one(&mut *connection)
        .await?;
    if ledger.is_none() {
        return Ok(true);
    }
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success")
            .fetch_all(&mut *connection)
            .await?;
    Ok(MIGRATOR
        .iter()
        .any(|migration| !applied.contains(&migration.version)))
}

async fn owner_exists(connection: &mut PgConnection) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles \
         WHERE rolname = 'marketplace_refusal_audit_owner')",
    )
    .fetch_one(connection)
    .await
}

fn through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            MIGRATOR
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: MIGRATOR.ignore_missing,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
    }
}
