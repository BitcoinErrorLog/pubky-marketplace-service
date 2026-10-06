//! Startup migrations.
//!
//! 0032 transfers the refusal-audit catalog to the NOLOGIN
//! `marketplace_refusal_audit_owner` and revokes it from the migration login.
//! 0040 and 0043 append to that catalog without taking the role back, so a
//! least-authority (non-superuser) migration login is refused. Both files are
//! applied in production and immutable, so the login borrows the owner role
//! for the run instead, the same way 0032's own rerun bootstrap does. A
//! superuser needs no membership and is left untouched.

use sqlx::PgPool;

pub async fn run(pool: &PgPool) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    let borrow_owner: bool = sqlx::query_scalar(
        "SELECT NOT login.rolsuper AND EXISTS ( \
           SELECT 1 FROM pg_catalog.pg_roles \
           WHERE rolname = 'marketplace_refusal_audit_owner') \
         FROM pg_catalog.pg_roles login WHERE login.rolname = current_user",
    )
    .fetch_one(&mut *connection)
    .await?;
    if borrow_owner {
        sqlx::query("GRANT marketplace_refusal_audit_owner TO CURRENT_USER")
            .execute(&mut *connection)
            .await?;
    }
    let migrated = sqlx::migrate!("./migrations")
        .run_direct(&mut *connection)
        .await;
    if borrow_owner {
        let revoked = sqlx::query("REVOKE marketplace_refusal_audit_owner FROM CURRENT_USER")
            .execute(&mut *connection)
            .await;
        migrated?;
        revoked?;
        return Ok(());
    }
    Ok(migrated?)
}
