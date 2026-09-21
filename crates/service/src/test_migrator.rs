//! Test-only migrator. 0032 is byte-immutable and fail-closed on retention
//! `CONNECTION LIMIT 1`. 0037 raises that cluster-global limit to 2, so the
//! next `#[sqlx::test]` database replays 0032 against LIMIT 2 and aborts.
//! Restore the 0032-era limit first; 0037 then raises it again.

use std::borrow::Cow;
use std::sync::LazyLock;

use sqlx::migrate::{Migration, MigrationType, Migrator};

const RESTORE_0032_RETENTION_LIMIT: &str = "\
DO $restore$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'marketplace_refusal_audit_retention'
  ) THEN
    ALTER ROLE marketplace_refusal_audit_retention CONNECTION LIMIT 1;
  END IF;
END $restore$;";

pub static TEST_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let all = sqlx::migrate!("./migrations");
    let prelude = Migration::new(
        0,
        Cow::Borrowed("restore_0032_retention_connlimit"),
        MigrationType::Simple,
        Cow::Borrowed(RESTORE_0032_RETENTION_LIMIT),
        false,
    );
    let mut migrations = Vec::with_capacity(all.migrations.len() + 1);
    migrations.push(prelude);
    migrations.extend(all.migrations.iter().cloned());
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: all.ignore_missing,
        locking: all.locking,
        no_tx: all.no_tx,
    }
});
