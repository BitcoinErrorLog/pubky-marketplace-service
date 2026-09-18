//! Non-HTTP operator entry point for refusal-audit erasure and key retirement.
//!
//! The binary using this module requires a dedicated PostgreSQL login. Actor
//! identities are read from standard input so they do not appear in argv.

use std::io::BufRead;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use url::Url;

use crate::refusal_audit::AuditKeys;

pub const ADMIN_LOGIN: &str = "marketplace_refusal_audit_admin_login";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminOperation {
    EraseActor,
    DestroyPrevious,
}

impl AdminOperation {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "erase-actor" => Ok(Self::EraseActor),
            "destroy-previous" => Ok(Self::DestroyPrevious),
            _ => anyhow::bail!("operation must be erase-actor or destroy-previous"),
        }
    }
}

pub struct AdminConfig {
    database_url: String,
    keys: AuditKeys,
}

impl AdminConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("REFUSAL_AUDIT_ADMIN_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_ADMIN_DATABASE_URL must be set"))?;
        let active_root = std::env::var("REFUSAL_AUDIT_HMAC_ROOT_B64")
            .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_ROOT_B64 must be set"))?;
        let active_epoch = std::env::var("REFUSAL_AUDIT_HMAC_KEY_EPOCH")
            .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_KEY_EPOCH must be set"))?;
        let previous_root = std::env::var("REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64").ok();
        let previous_epoch = std::env::var("REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH").ok();
        let keys = AuditKeys::parse(
            &active_root,
            &active_epoch,
            previous_root.as_deref(),
            previous_epoch.as_deref(),
        )?;
        Self::new(database_url, keys)
    }

    pub fn new(database_url: String, keys: AuditKeys) -> anyhow::Result<Self> {
        validate_admin_url(&database_url)?;
        Ok(Self { database_url, keys })
    }
}

fn validate_admin_url(value: &str) -> anyhow::Result<()> {
    let url = Url::parse(value)
        .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_ADMIN_DATABASE_URL must be PostgreSQL"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.username() != ADMIN_LOGIN
        || url.password().is_none_or(str::is_empty)
        || url.host_str().is_none_or(str::is_empty)
        || url.path().trim_matches('/').is_empty()
        || url.fragment().is_some()
    {
        anyhow::bail!(
            "REFUSAL_AUDIT_ADMIN_DATABASE_URL must name the authenticated refusal-audit admin login"
        );
    }
    Ok(())
}

pub async fn run(
    config: AdminConfig,
    operation: AdminOperation,
    mut input: impl BufRead,
) -> anyhow::Result<()> {
    let options: PgConnectOptions = config.database_url.parse()?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(Duration::from_millis(500))
        .connect_with(options)
        .await?;
    admin_authority_probe(&pool).await?;
    config.keys.assert_database_epochs_supported(&pool).await?;

    match operation {
        AdminOperation::EraseActor => {
            let mut actor = String::new();
            input.read_line(&mut actor)?;
            if actor.ends_with('\n') {
                actor.pop();
                if actor.ends_with('\r') {
                    actor.pop();
                }
            }
            let mut extra = String::new();
            input.read_to_string(&mut extra)?;
            if actor.is_empty() || !extra.is_empty() {
                anyhow::bail!("erase-actor requires exactly one actor identity line on stdin");
            }
            config.keys.erase_actor(&pool, &actor).await?;
            println!("account erasure completed");
        }
        AdminOperation::DestroyPrevious => {
            let previous_epoch = config
                .keys
                .previous_epoch()
                .ok_or_else(|| anyhow::anyhow!("a previous key epoch must be configured"))?;
            let retained_previous_rows: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM command_refusal_audit_buckets \
                 WHERE actor_key_epoch = $1",
            )
            .bind(previous_epoch)
            .fetch_one(&pool)
            .await?;
            let storage_safe: bool =
                sqlx::query_scalar("SELECT public.refusal_audit_previous_epoch_storage_safe($1)")
                    .bind(previous_epoch)
                    .fetch_one(&pool)
                    .await?;
            let mut keys = config.keys;
            keys.assert_second_rotation_safe(
                retained_previous_rows.try_into()?,
                storage_safe,
                storage_safe,
            )?;
            keys.destroy_previous(
                retained_previous_rows.try_into()?,
                storage_safe,
                storage_safe,
            )?;
            println!("previous refusal-audit key destroyed");
        }
    }
    pool.close().await;
    Ok(())
}

async fn admin_authority_probe(pool: &PgPool) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT session_user::text AS session_user, current_user::text AS current_user, \
           r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper \
           AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication \
           AND NOT r.rolbypassrls AND r.rolconnlimit = 1 AS safe_attributes, \
           NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid) \
             AS no_memberships, \
           has_table_privilege(session_user, \
             'public.command_refusal_audit_buckets', 'SELECT,DELETE') \
           AND NOT has_table_privilege(session_user, \
             'public.command_refusal_audit_buckets', 'INSERT') \
           AND NOT has_table_privilege(session_user, \
             'public.command_refusal_audit_buckets', 'UPDATE') AS bucket_authority, \
           has_table_privilege(session_user, \
             'public.command_refusal_audit_epoch_inventory', 'SELECT') \
           AND NOT has_table_privilege(session_user, \
             'public.command_refusal_audit_epoch_inventory', 'INSERT') \
           AND NOT has_table_privilege(session_user, \
             'public.command_refusal_audit_epoch_inventory', 'UPDATE') \
           AND NOT has_table_privilege(session_user, \
             'public.command_refusal_audit_epoch_inventory', 'DELETE') \
           AND has_function_privilege(session_user, \
             'public.refusal_audit_previous_epoch_storage_safe(smallint)', 'EXECUTE') \
             AS lifecycle_authority, \
           NOT has_function_privilege(session_user, \
             'public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', \
             'EXECUTE') \
           AND NOT has_function_privilege(session_user, \
             'public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', \
             'EXECUTE') AS no_read_operator_authority, \
           NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind IN ('r','p') \
               AND c.relname NOT IN ('command_refusal_audit_buckets', \
                 'command_refusal_audit_epoch_inventory') \
               AND (has_table_privilege(session_user, c.oid, 'INSERT') \
                 OR has_table_privilege(session_user, c.oid, 'UPDATE') \
                 OR has_table_privilege(session_user, c.oid, 'DELETE') \
                 OR has_table_privilege(session_user, c.oid, 'TRUNCATE') \
                 OR has_table_privilege(session_user, c.oid, 'REFERENCES') \
                 OR has_table_privilege(session_user, c.oid, 'TRIGGER'))) \
             AS no_other_mutation \
         FROM pg_catalog.pg_roles r WHERE r.rolname = session_user",
    )
    .fetch_one(pool)
    .await?;
    let verified = row.get::<String, _>("session_user") == ADMIN_LOGIN
        && row.get::<String, _>("current_user") == ADMIN_LOGIN
        && row.get::<bool, _>("safe_attributes")
        && row.get::<bool, _>("no_memberships")
        && row.get::<bool, _>("bucket_authority")
        && row.get::<bool, _>("lifecycle_authority")
        && row.get::<bool, _>("no_read_operator_authority")
        && row.get::<bool, _>("no_other_mutation");
    if !verified {
        anyhow::bail!("refusal-audit admin authority verification failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_url_is_strict() {
        assert!(validate_admin_url(
            "postgres://marketplace_refusal_audit_admin_login:password@db.test/audit"
        )
        .is_ok());
        for value in [
            "not-a-url",
            "https://marketplace_refusal_audit_admin_login:password@db.test/audit",
            "postgres://postgres:password@db.test/audit",
            "postgres://marketplace_refusal_audit_admin_login@db.test/audit",
            "postgres://marketplace_refusal_audit_admin_login:password@db.test/",
        ] {
            assert!(validate_admin_url(value).is_err(), "{value}");
        }
    }
}
