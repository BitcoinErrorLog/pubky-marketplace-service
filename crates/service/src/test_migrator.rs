//! Test-only migrator. 0032 is byte-immutable and fail-closed on retention
//! `CONNECTION LIMIT 1`. 0037 raises that cluster-global limit to 2, so the
//! next `#[sqlx::test]` database replays 0032 against LIMIT 2 and aborts.
//! Restore the 0032-era limit first; 0037 then raises it again.
//!
//! Roles are cluster-global. PostgreSQL advisory locks are per-database, so
//! this lock does not serialize two `#[sqlx::test]` databases. The pre-push
//! gate runs those tests on one thread. The prelude still restores the
//! 0032-era limit on the same connection before 0032 runs, and the trailing
//! migration releases the lock. A failed migrate drops the connection, which
//! releases the session lock.

use std::borrow::Cow;
use std::sync::LazyLock;

use sqlx::migrate::{Migration, MigrationType, Migrator};

/// Session advisory lock held for the whole test migration. Not a
/// `hashtextextended` seed (those are 32, 42, 6341, 6342, 6353–6355, 6361).
const MIGRATION_LOCK_ID: i64 = 871_946_354_001;

pub static TEST_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let all = sqlx::migrate!("./migrations");
    let prelude_sql = format!(
        "DO $restore$\n\
         BEGIN\n\
           PERFORM pg_advisory_lock({MIGRATION_LOCK_ID});\n\
           IF EXISTS (\n\
             SELECT 1 FROM pg_catalog.pg_roles\n\
             WHERE rolname = 'marketplace_refusal_audit_retention'\n\
           ) THEN\n\
             ALTER ROLE marketplace_refusal_audit_retention CONNECTION LIMIT 1;\n\
           END IF;\n\
         END $restore$;"
    );
    let release_sql = format!("SELECT pg_advisory_unlock({MIGRATION_LOCK_ID});");
    // no_tx: the session advisory lock must outlive the statement. A
    // transactional migration commits that statement on its own connection
    // transaction, which is the wrong scope for a lock held across 0032.
    let prelude = Migration::new(
        0,
        Cow::Borrowed("restore_0032_retention_connlimit"),
        MigrationType::Simple,
        Cow::Owned(prelude_sql),
        true,
    );
    let release = Migration::new(
        9_000_000_001,
        Cow::Borrowed("release_test_migration_lock"),
        MigrationType::Simple,
        Cow::Owned(release_sql),
        true,
    );
    let mut migrations = Vec::with_capacity(all.migrations.len() + 2);
    migrations.push(prelude);
    migrations.extend(all.migrations.iter().cloned());
    migrations.push(release);
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: all.ignore_missing,
        locking: all.locking,
        no_tx: all.no_tx,
    }
});
