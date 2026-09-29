//! Index builds on existing tables do not block writes (`docs/migrations.md`).
//!
//! Every migration after [`LAST_BLOCKING_INDEX_MIGRATION`] that builds or
//! drops an index on a table it did not create does so with `CONCURRENTLY`,
//! as the only statement of a `-- no-transaction` file, and a concurrent
//! build never uses `IF NOT EXISTS`. The fixtures under
//! `tests/fixtures/migration_rules/` run through the sqlx migrator against
//! Postgres to show why each part of that shape is needed.

use std::path::Path;

use sqlx::migrate::Migrator;
use sqlx::PgPool;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
const FIXTURES_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/migration_rules"
);

/// 0001–0049 predate the rule and are applied, so they are immutable. 0047
/// built `listings_live_seller_idx` and rebuilt
/// `drop_listings_one_active_per_listing` with a plain `CREATE INDEX`.
const LAST_BLOCKING_INDEX_MIGRATION: u32 = 49;

/// The statements of a migration, comments removed, whitespace collapsed,
/// upper-cased.
fn statements(sql: &str) -> Vec<String> {
    let without_comments: String = sql
        .lines()
        .map(|line| line.split("--").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    without_comments
        .split(';')
        .map(|statement| {
            statement
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase()
        })
        .filter(|statement| !statement.is_empty())
        .collect()
}

fn word_after<'a>(statement: &'a str, marker: &str) -> Option<&'a str> {
    let rest = &statement[statement.find(marker)? + marker.len()..];
    let rest = rest.strip_prefix("IF NOT EXISTS ").unwrap_or(rest);
    let rest = rest.strip_prefix("ONLY ").unwrap_or(rest);
    rest.split([' ', '(']).find(|word| !word.is_empty())
}

/// Every way `sql` breaks the rule.
fn violations(sql: &str) -> Vec<String> {
    let no_transaction = sql.starts_with("-- no-transaction");
    let statements = statements(sql);
    let created: Vec<&str> = statements
        .iter()
        .filter(|statement| statement.starts_with("CREATE TABLE "))
        .filter_map(|statement| word_after(statement, "CREATE TABLE "))
        .collect();
    let mut found = Vec::new();
    for statement in &statements {
        let builds =
            statement.starts_with("CREATE INDEX ") || statement.starts_with("CREATE UNIQUE INDEX ");
        let drops = statement.starts_with("DROP INDEX ") || statement.starts_with("REINDEX ");
        let words: Vec<&str> = statement
            .split([' ', '(', ')', ','])
            .filter(|word| !word.is_empty())
            .collect();
        let constraint = statement.starts_with("ALTER TABLE ")
            && (words.contains(&"UNIQUE")
                || words.contains(&"EXCLUDE")
                || words.windows(2).any(|pair| pair == ["PRIMARY", "KEY"]))
            && !statement.contains(" USING INDEX ");
        if constraint {
            let table = word_after(statement, "ALTER TABLE ").unwrap_or_default();
            if !created.contains(&table) {
                found.push(format!(
                    "builds a constraint index on existing table {table}: {statement}"
                ));
            }
            continue;
        }
        if !builds && !drops {
            continue;
        }
        if statement.contains(" CONCURRENTLY ") {
            if !no_transaction {
                found.push(format!(
                    "runs CONCURRENTLY inside a transaction: {statement}"
                ));
            }
            if statements.len() != 1 {
                found.push(format!(
                    "runs CONCURRENTLY beside other statements: {statement}"
                ));
            }
            if builds && statement.contains(" IF NOT EXISTS ") {
                found.push(format!(
                    "a concurrent build with IF NOT EXISTS would skip an invalid leftover: {statement}"
                ));
            }
            continue;
        }
        let table = word_after(statement, " ON ").unwrap_or_default();
        if drops || !created.contains(&table) {
            found.push(format!("blocks writes on an existing table: {statement}"));
        }
    }
    found
}

fn migration_files() -> Vec<(u32, String, String)> {
    let mut files: Vec<(u32, String, String)> = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations dir")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?.to_string();
            if name.starts_with("._") || !name.ends_with(".sql") {
                return None;
            }
            let version = name.split('_').next()?.parse().ok()?;
            Some((
                version,
                name,
                std::fs::read_to_string(&path).expect("migration readable"),
            ))
        })
        .collect();
    files.sort();
    files
}

#[test]
fn migrations_after_0049_build_indexes_without_blocking_writes() {
    let files = migration_files();
    assert!(
        files
            .iter()
            .any(|(version, _, _)| *version > LAST_BLOCKING_INDEX_MIGRATION),
        "the rule covers the current migrations"
    );
    let broken: Vec<String> = files
        .iter()
        .filter(|(version, _, _)| *version > LAST_BLOCKING_INDEX_MIGRATION)
        .flat_map(|(_, name, sql)| {
            violations(sql)
                .into_iter()
                .map(move |violation| format!("{name}: {violation}"))
        })
        .collect();
    assert!(broken.is_empty(), "{broken:#?}");
}

#[test]
fn the_rule_refuses_every_blocking_or_unsafe_shape() {
    for (label, sql) in [
        (
            "a plain build on an existing table",
            "CREATE INDEX listings_probe_idx ON listings (updated_at);",
        ),
        (
            "a plain unique build",
            "CREATE UNIQUE INDEX IF NOT EXISTS orders_probe_idx ON orders (id);",
        ),
        (
            "a concurrent build inside a transaction",
            "CREATE INDEX CONCURRENTLY listings_probe_idx ON listings (updated_at);",
        ),
        (
            "a concurrent build beside another statement",
            "-- no-transaction\nCREATE INDEX CONCURRENTLY a_idx ON listings (updated_at);\n\
             ALTER TABLE listings ADD COLUMN IF NOT EXISTS probe TEXT;",
        ),
        (
            "a concurrent build that skips an existing name",
            "-- no-transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS a_idx ON listings (updated_at)",
        ),
        (
            "a plain drop",
            "DROP INDEX IF EXISTS listings_live_seller_idx;",
        ),
        (
            "a unique constraint built in place",
            "ALTER TABLE listings ADD CONSTRAINT listings_probe_key UNIQUE (title);",
        ),
        (
            "an inline unique column",
            "ALTER TABLE listings ADD COLUMN probe INT UNIQUE;",
        ),
        (
            "an inline primary key column",
            "ALTER TABLE worker_leases ADD COLUMN probe BIGINT PRIMARY KEY;",
        ),
        (
            "an exclusion constraint",
            "ALTER TABLE listings ADD CONSTRAINT listings_probe_excl EXCLUDE USING gist (title WITH =);",
        ),
    ] {
        assert!(!violations(sql).is_empty(), "{label} passed the rule");
    }
    for (label, sql) in [
        (
            "a concurrent build alone",
            "-- no-transaction\n-- why\nCREATE INDEX CONCURRENTLY a_idx ON listings (updated_at)\n",
        ),
        (
            "a concurrent drop alone",
            "-- no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS a_idx;\n",
        ),
        (
            "an index on a table the same file creates",
            "CREATE TABLE IF NOT EXISTS probe (id BIGINT PRIMARY KEY);\n\
             CREATE UNIQUE INDEX IF NOT EXISTS probe_idx ON probe (id);",
        ),
        (
            "a constraint attached to a concurrently built index",
            "ALTER TABLE listings ADD CONSTRAINT listings_probe_key UNIQUE USING INDEX a_idx;",
        ),
        (
            "a column whose name mentions unique",
            "ALTER TABLE worker_leases ADD COLUMN IF NOT EXISTS unique_runs BIGINT NOT NULL DEFAULT 0;",
        ),
        (
            "dropping a unique constraint",
            "ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_probe_unique;",
        ),
    ] {
        assert_eq!(violations(sql), Vec::<String>::new(), "{label}");
    }
    let fixture = std::fs::read_to_string(format!(
        "{FIXTURES_DIR}/concurrent_index/9100000001_listings_rule_probe_idx.sql"
    ))
    .expect("fixture");
    assert_eq!(violations(&fixture), Vec::<String>::new());
}

async fn fixture_migrator(case: &str) -> Migrator {
    let mut migrator = Migrator::new(Path::new(&format!("{FIXTURES_DIR}/{case}")))
        .await
        .expect("fixture migrations resolve");
    migrator.set_ignore_missing(true);
    migrator
}

async fn insert_listing(
    executor: impl sqlx::PgExecutor<'_>,
    listing_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, sale_format, updated_at) \
         VALUES ($1, 'rule_seller', $2, 'Boots', 1, $3, 1, 'available', 1, 1, 0, 0, 100, 'USD', 2, \
         'fixed_price', now())",
    )
    .bind(format!("listing:rule_seller_{listing_id}"))
    .bind(listing_id)
    .bind("a".repeat(64))
    .execute(executor)
    .await
    .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_concurrent_index_migration_lets_writes_through_while_it_builds(pool: PgPool) {
    // An open write holds the table the way a live checkout does; the
    // build waits for it.
    let mut open_write = pool.begin().await.expect("open write");
    insert_listing(&mut *open_write, "before_build")
        .await
        .expect("open write inserts");
    let migrator = fixture_migrator("concurrent_index").await;
    let build_pool = pool.clone();
    let build = tokio::spawn(async move { migrator.run(&build_pool).await });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND strpos(query, 'listings_rule_probe_idx') > 0",
        )
        .fetch_one(&pool)
        .await
        .expect("build wait poll");
        if waiting >= 1 {
            break;
        }
        assert!(
            !build.is_finished(),
            "the build did not wait for the open write"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the build never waited"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // A new write while the build waits goes straight through.
    let mut write = pool.begin().await.expect("write during the build");
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *write)
        .await
        .expect("lock timeout");
    insert_listing(&mut *write, "during_build")
        .await
        .expect("a write during the build is not blocked");
    write.commit().await.expect("write commits");

    open_write.commit().await.expect("open write commits");
    build
        .await
        .expect("build joins")
        .expect("the concurrent build applies");
    let valid: bool = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid = 'listings_rule_probe_idx'::regclass",
    )
    .fetch_one(&pool)
    .await
    .expect("probe index");
    assert!(valid, "the concurrent build left a valid index");
    let recorded: bool =
        sqlx::query_scalar("SELECT success FROM _sqlx_migrations WHERE version = 9100000001")
            .fetch_one(&pool)
            .await
            .expect("migration recorded");
    assert!(recorded);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_concurrent_build_fails_inside_a_transaction_or_beside_another_statement(pool: PgPool) {
    for case in ["in_transaction", "with_a_second_statement"] {
        let error = fixture_migrator(case)
            .await
            .run(&pool)
            .await
            .expect_err(case)
            .to_string();
        assert!(
            error.contains("cannot run inside a transaction block"),
            "{case}: {error}"
        );
        let built: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM pg_class WHERE relname LIKE 'listings_rule_probe%'",
        )
        .fetch_one(&pool)
        .await
        .expect("probe indexes");
        assert_eq!(built, 0, "{case}");
        let recorded: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM _sqlx_migrations WHERE version = 9100000001",
        )
        .fetch_one(&pool)
        .await
        .expect("migration rows");
        assert_eq!(recorded, 0, "{case}");
    }
}
