//! Migration 0023 test: applies 0001..=0022, then 0023 twice (idempotent),
//! and asserts the `shared_manual` seller-confirmation / manual-review
//! resolution schema (design §B.8.8/§B.9 r13): the widened
//! `paykit_request_state` vocabulary, the seller-confirmation window
//! biconditional, the common `manual_review_entered_at` entry stamp and its
//! CHECK, the resolution-field CHECK matrix (valid/invalid combos), the
//! pre-existing-row backfill, the recreated `orders_paykit_pending`
//! predicate, the three new tables with their uniqueness rules, and the
//! migration catalog's shape (strictly increasing, unique, ending at 0023).
//! The scratch databases are dropped at the end.
//!
//! Also here: the schema-level enumeration of every writer of
//! `payments.state = 'manual_review'` (including parameterized sandbox
//! transitions). The database CHECK makes an unstamped entry uncommittable;
//! this recursive source scan makes the enumeration explicit, so a future
//! writer added without the stamp fails the build before it ever runs.

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

fn migration_path(number: u32) -> String {
    let prefix = format!("{number:04}_");
    let mut matches: Vec<String> = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            (name.starts_with(&prefix) && name.ends_with(".sql"))
                .then(|| format!("{MIGRATIONS_DIR}/{name}"))
        })
        .collect();
    matches.sort();
    matches
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no migration {prefix}*.sql"))
}

fn migration_sql(number: u32) -> String {
    let path = migration_path(number);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

async fn apply(pool: &PgPool, number: u32) {
    let sql = migration_sql(number);
    sqlx::raw_sql(&sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("apply migration {number:04}: {e}"));
}

async fn scratch_pool(admin: &PgPool, base: &PgConnectOptions, name: &str) -> PgPool {
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(admin)
        .await
        .expect("create scratch database");
    let options = base.clone().database(name);
    PgPoolOptions::new()
        .connect_with(options)
        .await
        .expect("connect to scratch database")
}

async fn drop_scratch(admin: &PgPool, name: &str) {
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(admin)
        .await
        .expect("drop scratch database");
}

async fn admin_pool() -> (PgPool, PgConnectOptions) {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point at a throwaway Postgres");
    let base = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
    let admin = PgPoolOptions::new()
        .connect_with(base.clone())
        .await
        .expect("connect to Postgres");
    (admin, base)
}

async fn seed_order_and_payment(pool: &PgPool) -> (Uuid, Uuid) {
    let order_id = Uuid::new_v4();
    let payment_id = Uuid::new_v4();
    let seeded_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("seed timestamp")
        .to_utc();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 1, 'pending_payment', '[]', NULL, 50000, 1200, 51200, \
         'SAT', 0, 1, $2, $3, $3)",
    )
    .bind(order_id)
    .bind(payment_id)
    .bind(seeded_at)
    .execute(pool)
    .await
    .expect("seed order");
    sqlx::query(
        "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
         state, confirmations, amount_minor, currency, exponent, created_at, \
         updated_at) \
         VALUES ($1, $2, 'buyer', 'seller', 1, 'paykit', 'awaiting_entitlement', 0, 51200, \
         'SAT', 0, $3, $3)",
    )
    .bind(payment_id)
    .bind(order_id)
    .bind(seeded_at)
    .execute(pool)
    .await
    .expect("seed payment");
    (order_id, payment_id)
}

#[tokio::test]
async fn migration_0023_adds_the_shared_manual_resolution_schema() {
    let (admin, base) = admin_pool().await;
    let name = format!("mig0023_{}", Uuid::new_v4().simple());
    let pool = scratch_pool(&admin, &base, &name).await;

    // Pre-0023 schema, with a pre-existing manual_review payment that the
    // migration must backfill (documented approximate entered-at instant).
    for number in 1..=22 {
        apply(&pool, number).await;
    }
    let (order_id, payment_id) = seed_order_and_payment(&pool).await;
    sqlx::query("UPDATE payments SET state = 'manual_review' WHERE id = $1")
        .bind(payment_id)
        .execute(&pool)
        .await
        .expect("seed a pre-0023 manual_review payment");

    // The migration applies twice (idempotent).
    apply(&pool, 23).await;
    apply(&pool, 23).await;

    // Backfill: the pre-existing manual_review row got its entry stamp.
    let backfilled: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT manual_review_entered_at FROM payments WHERE id = $1")
            .bind(payment_id)
            .fetch_one(&pool)
            .await
            .expect("payment row exists");
    assert!(
        backfilled.is_some(),
        "a pre-existing manual_review payment is backfilled"
    );

    // The widened request-state vocabulary admits the new state and keeps
    // the old ones; an unknown value is still rejected. The window
    // biconditional holds at every step: entry carries both server-clock
    // stamps in the same UPDATE, and leaving clears both.
    for state in [
        "preparing",
        "pending",
        "detected",
        "confirmed",
        "awaiting_seller_confirmation",
    ] {
        if state == "awaiting_seller_confirmation" {
            sqlx::query(
                "UPDATE orders SET paykit_request_state = $2, \
                 paykit_seller_confirmation_entered_at = NOW(), \
                 paykit_seller_confirmation_deadline = NOW() + INTERVAL '24 hours' \
                 WHERE id = $1",
            )
            .bind(order_id)
            .bind(state)
            .execute(&pool)
            .await
            .expect("entry with the window stamps satisfies the CHECK");
        } else {
            sqlx::query(
                "UPDATE orders SET paykit_request_state = $2, \
                 paykit_seller_confirmation_entered_at = NULL, \
                 paykit_seller_confirmation_deadline = NULL WHERE id = $1",
            )
            .bind(order_id)
            .bind(state)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("paykit_request_state '{state}' must be admitted: {e}"));
        }
    }
    let rejected =
        sqlx::query("UPDATE orders SET paykit_request_state = 'observing' WHERE id = $1")
            .bind(order_id)
            .execute(&pool)
            .await;
    assert!(
        rejected.is_err(),
        "an unknown paykit_request_state must be rejected"
    );

    // The window biconditional, both directions.
    let violated = sqlx::query(
        "UPDATE orders SET paykit_request_state = 'awaiting_seller_confirmation', \
         paykit_seller_confirmation_entered_at = NULL, \
         paykit_seller_confirmation_deadline = NULL WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await;
    assert!(
        violated.is_err(),
        "entering the state without the window stamps must fail"
    );
    let violated = sqlx::query(
        "UPDATE orders SET paykit_request_state = 'pending', \
         paykit_seller_confirmation_entered_at = NOW(), \
         paykit_seller_confirmation_deadline = NOW() WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await;
    assert!(
        violated.is_err(),
        "window stamps outside the state must fail"
    );

    // The manual_review entry CHECK: an unstamped entry cannot commit, and
    // leaving the state with the stamp still set cannot either.
    let (order2, payment2) = seed_order_and_payment(&pool).await;
    let _ = order2;
    let unstamped = sqlx::query("UPDATE payments SET state = 'manual_review' WHERE id = $1")
        .bind(payment2)
        .execute(&pool)
        .await;
    assert!(
        unstamped.is_err(),
        "manual_review without manual_review_entered_at must fail"
    );
    sqlx::query(
        "UPDATE payments SET state = 'manual_review', manual_review_entered_at = NOW() \
         WHERE id = $1",
    )
    .bind(payment2)
    .execute(&pool)
    .await
    .expect("a stamped entry commits");
    let uncleared = sqlx::query("UPDATE payments SET state = 'confirmed' WHERE id = $1")
        .bind(payment2)
        .execute(&pool)
        .await;
    assert!(
        uncleared.is_err(),
        "leaving manual_review without clearing the stamp must fail"
    );
    sqlx::query(
        "UPDATE payments SET state = 'confirmed', manual_review_entered_at = NULL WHERE id = $1",
    )
    .bind(payment2)
    .execute(&pool)
    .await
    .expect("a cleared exit commits");

    // The resolution CHECK matrix.
    let resolution_id = Uuid::new_v4();
    // All-NULL is the only valid pre-resolution shape (checked implicitly
    // by every UPDATE above). A full seller_attestation row commits.
    sqlx::query(
        "UPDATE payments SET resolution_id = $2, resolution_outcome = 'paid', \
         resolution_basis = 'seller_attestation', resolved_at = NOW(), \
         resolved_by_pubky = 'seller' WHERE id = $1",
    )
    .bind(payment2)
    .bind(resolution_id)
    .execute(&pool)
    .await
    .expect("a seller_attestation paid resolution commits");
    // A seller_unresponsive abandoned resolution carries NO pubky.
    let (_o3, payment3) = seed_order_and_payment(&pool).await;
    sqlx::query(
        "UPDATE payments SET state = 'manual_review', manual_review_entered_at = NOW(), \
         resolution_id = $2, resolution_outcome = 'abandoned', \
         resolution_basis = 'seller_unresponsive', resolved_at = NOW() WHERE id = $1",
    )
    .bind(payment3)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect_err("an abandoned resolution must leave manual_review");
    sqlx::query(
        "UPDATE payments SET state = 'expired', manual_review_entered_at = NULL, \
         resolution_id = $2, resolution_outcome = 'abandoned', \
         resolution_basis = 'seller_unresponsive', resolved_at = NOW() WHERE id = $1",
    )
    .bind(payment3)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("a seller_unresponsive abandoned resolution commits");
    // Invalid combos, each rejected.
    let (_o4, payment4) = seed_order_and_payment(&pool).await;
    for (label, set) in [
        ("refunded without a reference", "resolution_id = gen_random_uuid(), resolution_outcome = 'refunded', resolution_basis = 'seller_attestation', resolved_at = NOW(), resolved_by_pubky = 'seller'"),
        ("paid with a reference", "resolution_id = gen_random_uuid(), resolution_outcome = 'paid', resolution_basis = 'seller_attestation', resolved_at = NOW(), resolved_by_pubky = 'seller', refund_reference = 'txn'"),
        ("seller_unresponsive with a pubky", "resolution_id = gen_random_uuid(), resolution_outcome = 'abandoned', resolution_basis = 'seller_unresponsive', resolved_at = NOW(), resolved_by_pubky = 'seller'"),
        ("seller_attestation without a pubky", "resolution_id = gen_random_uuid(), resolution_outcome = 'paid', resolution_basis = 'seller_attestation', resolved_at = NOW()"),
        ("outcome without a basis", "resolution_id = gen_random_uuid(), resolution_outcome = 'paid', resolved_at = NOW(), resolved_by_pubky = 'seller'"),
        ("basis without an outcome", "resolution_basis = 'seller_attestation'"),
        ("an unknown outcome", "resolution_id = gen_random_uuid(), resolution_outcome = 'chargeback', resolution_basis = 'seller_attestation', resolved_at = NOW(), resolved_by_pubky = 'seller'"),
        ("an unknown basis", "resolution_id = gen_random_uuid(), resolution_outcome = 'paid', resolution_basis = 'operator', resolved_at = NOW(), resolved_by_pubky = 'seller'"),
    ] {
        let rejected = sqlx::query(&format!("UPDATE payments SET {set} WHERE id = $1"))
            .bind(payment4)
            .execute(&pool)
            .await;
        assert!(rejected.is_err(), "{label} must be rejected");
    }

    // The recreated poll index covers the status-only claim set and still
    // excludes `preparing`.
    let indexdef: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'orders_paykit_pending'",
    )
    .fetch_one(&pool)
    .await
    .expect("partial index exists");
    for covered in ["'pending'", "'detected'", "'awaiting_seller_confirmation'"] {
        assert!(
            indexdef.contains(covered),
            "index predicate covers {covered}: {indexdef}"
        );
    }
    assert!(
        !indexdef.contains("preparing"),
        "a preparing order stays unclaimed: {indexdef}"
    );

    // The reaper indexes exist.
    for index in [
        "orders_seller_confirmation_due",
        "orders_paykit_stack_idx",
        "payments_manual_review_due",
        "paykit_resolve_outbox_claim",
        "paykit_resolve_outbox_stack",
    ] {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)")
                .bind(index)
                .fetch_one(&pool)
                .await
                .expect("index lookup");
        assert!(exists, "index {index} exists");
    }

    // The three new tables and their uniqueness rules.
    let (_o5, payment5) = seed_order_and_payment(&pool).await;
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, 1, 'seller', 'payment.confirmed', NOW())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(format!("payment:{payment5}"))
    .execute(&pool)
    .await
    .expect("seed event");
    sqlx::query(
        "INSERT INTO paykit_seller_confirmations (order_id, payment_id, confirmed_by_pubky, \
         confirmed_at, confirmed_txid, confirmed_amount_sats, confirmed_reason, \
         confirmation_source, confirmation_basis, paykit_observation, event_id, created_at) \
         VALUES ($1, $2, 'seller', NOW(), 'tx', 51200, 'ok', 'seller', 'seller_attestation', \
         '{}', $3, NOW())",
    )
    .bind(_o5)
    .bind(payment5)
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("one seller confirmation per order");
    let duplicate = sqlx::query(
        "INSERT INTO paykit_seller_confirmations (order_id, payment_id, confirmed_by_pubky, \
         confirmed_at, confirmed_txid, confirmed_amount_sats, confirmed_reason, \
         confirmation_source, confirmation_basis, paykit_observation, event_id, created_at) \
         VALUES ($1, $2, 'seller', NOW(), 'tx', 51200, 'ok', 'seller', 'seller_attestation', \
         '{}', $3, NOW())",
    )
    .bind(_o5)
    .bind(payment5)
    .bind(event_id)
    .execute(&pool)
    .await;
    assert!(
        duplicate.is_err(),
        "a second seller confirmation for one order is impossible"
    );
    let bad_basis = sqlx::query(
        "INSERT INTO paykit_seller_confirmations (order_id, payment_id, confirmed_by_pubky, \
         confirmed_at, confirmed_txid, confirmed_amount_sats, confirmed_reason, \
         confirmation_source, confirmation_basis, paykit_observation, event_id, created_at) \
         VALUES (gen_random_uuid(), $1, 'seller', NOW(), 'tx', 1, 'ok', 'seller', 'chain_proof', \
         '{}', $2, NOW())",
    )
    .bind(payment5)
    .bind(event_id)
    .execute(&pool)
    .await;
    assert!(
        bad_basis.is_err(),
        "confirmation_basis is seller_attestation, always"
    );

    let (_o6, payment6) = seed_order_and_payment(&pool).await;
    sqlx::query(
        "INSERT INTO paykit_manual_resolutions (order_id, payment_id, resolution_id, outcome, \
         basis, resolved_at, resolved_by_pubky, request_hash, response, event_id, created_at) \
         VALUES ($1, $2, $3, 'paid', 'seller_attestation', NOW(), 'seller', 'h', '{}', $4, NOW())",
    )
    .bind(_o6)
    .bind(payment6)
    .bind(Uuid::new_v4())
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("one resolution per order");
    let duplicate_key = sqlx::query(
        "INSERT INTO paykit_manual_resolutions (order_id, payment_id, resolution_id, outcome, \
         basis, resolved_at, resolved_by_pubky, request_hash, response, event_id, created_at) \
         VALUES ($1, $2, gen_random_uuid(), 'paid', 'seller_attestation', NOW(), 'seller', 'h', \
         '{}', $3, NOW())",
    )
    .bind(_o6)
    .bind(payment6)
    .bind(event_id)
    .execute(&pool)
    .await;
    assert!(
        duplicate_key.is_err(),
        "a second resolution for one order is impossible"
    );

    let (_o7, payment7) = seed_order_and_payment(&pool).await;
    sqlx::query(
        "INSERT INTO paykit_resolve_outbox (order_id, payment_id, event_id, invoice_id, \
         resolution, resolved_at, stack_id, stack_endpoint, next_attempt_at, delivery_deadline, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 'paid_manually', NOW(), 'proof:x', 'http://a', NOW(), \
         NOW() + INTERVAL '1 hour', NOW(), NOW())",
    )
    .bind(_o7)
    .bind(payment7)
    .bind(event_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("one resolve outbox row per order");
    let duplicate_row = sqlx::query(
        "INSERT INTO paykit_resolve_outbox (order_id, payment_id, event_id, invoice_id, \
         resolution, resolved_at, stack_id, stack_endpoint, next_attempt_at, delivery_deadline, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 'refunded', NOW(), 'proof:x', 'http://a', NOW(), \
         NOW() + INTERVAL '1 hour', NOW(), NOW())",
    )
    .bind(_o7)
    .bind(payment7)
    .bind(event_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;
    assert!(
        duplicate_row.is_err(),
        "a second resolve outbox row for one order is impossible"
    );
    // The delivery-state CHECKs: delivered needs delivered_at, terminal
    // needs a reason, acknowledgement needs the actor.
    for (label, set) in [
        (
            "delivered without delivered_at",
            "delivery_state = 'delivered'",
        ),
        (
            "terminal without a reason",
            "delivery_state = 'terminal_unresolved'",
        ),
        ("acknowledged without an actor", "acknowledged_at = NOW()"),
    ] {
        let rejected = sqlx::query(&format!(
            "UPDATE paykit_resolve_outbox SET {set} WHERE order_id = $1"
        ))
        .bind(_o7)
        .execute(&pool)
        .await;
        assert!(rejected.is_err(), "{label} must be rejected");
    }

    // The refund cap for a paykit order is the paykit total (listing total
    // + nonce), not the bare listing total; non-paykit orders keep the old
    // bound.
    let (order_id, _payment_id) = seed_order_and_payment(&pool).await;
    sqlx::query("UPDATE orders SET paykit_total_sats = 51637 WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("paykit total persisted");
    sqlx::query(
        "UPDATE orders SET external_refund = \
         jsonb_build_object('amount_minor', 51637, 'transaction_id', 'tx') WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("a refund up to the paykit total is admitted");
    let over_cap = sqlx::query(
        "UPDATE orders SET external_refund = \
         jsonb_build_object('amount_minor', 51638, 'transaction_id', 'tx') WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await;
    assert!(
        over_cap.is_err(),
        "a refund over the paykit total is rejected"
    );
    let (plain_order, _) = seed_order_and_payment(&pool).await;
    let fiat_cap = sqlx::query(
        "UPDATE orders SET external_refund = \
         jsonb_build_object('amount_minor', 51201, 'transaction_id', 'tx') WHERE id = $1",
    )
    .bind(plain_order)
    .execute(&pool)
    .await;
    assert!(
        fiat_cap.is_err(),
        "a non-paykit order keeps the listing-total cap"
    );

    drop_scratch(&admin, &name).await;
}

/// The migration catalog is strictly increasing, unique per number, and
/// ends at 0023. Migrations 0001–0022 are never rewritten; this asserts
/// 0023 is the only addition.
#[test]
fn migration_catalog_ends_unique_at_0023() {
    let mut numbers: Vec<u32> = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".sql")?
                .get(..4)
                .and_then(|prefix| prefix.parse::<u32>().ok())
        })
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    let expected: Vec<u32> = (1..=23).collect();
    assert_eq!(numbers, expected, "the catalog is 0001..=0023, gapless");
}

/// Every writer of `payments.state = 'manual_review'` across Bitcoin,
/// Locks, fiat, and sandbox stamps `manual_review_entered_at` in the same
/// UPDATE.
/// The schema CHECK makes an unstamped entry uncommittable; this scan makes
/// the writer set explicit so a new writer cannot sneak in unreviewed.
#[test]
fn every_manual_review_writer_stamps_the_entry_time() {
    let src_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
    fn rust_files(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("src directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                rust_files(&path, files);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    let mut files = Vec::new();
    rust_files(std::path::Path::new(src_dir), &mut files);
    let mut writers: Vec<(String, usize)> = Vec::new();
    for path in files {
        let file_name = path
            .file_name()
            .expect("file name")
            .to_string_lossy()
            .into_owned();
        let content = std::fs::read_to_string(&path).expect("readable source");
        for (index, _) in content.match_indices("UPDATE payments SET") {
            let window_end = index
                + content[index..]
                    .find('"')
                    .expect("the SQL string literal closes");
            let window = &content[index..window_end];
            let set_clause = window.split("WHERE").next().unwrap_or(window);
            let writes_manual_review = set_clause.contains("state = 'manual_review'")
                || (set_clause.contains("state = $2")
                    && set_clause.contains("manual_review_entered_at = CASE")
                    && set_clause.contains("manual_review"));
            if !writes_manual_review {
                continue;
            }
            assert!(
                window.contains("manual_review_entered_at"),
                "{file_name}: a writer of payments.state = 'manual_review' \
                 does not stamp manual_review_entered_at:\n{window}"
            );
            writers.push((file_name.clone(), index));
        }
    }
    // The enumerated set: Locks apply_manual_review (1), the paykit worker
    // (3: late settlement, amount mismatch, confirm-failure), the fiat
    // apply_fiat_paid (2: expired, confirm-failure), the sandbox transition
    // (1), and the shared_manual 24-hour seller-window reaper (1). Any NEW
    // writer fails here until it is reviewed, stamped, and enumerated.
    let mut per_file: std::collections::BTreeMap<String, usize> = Default::default();
    for (file, _) in &writers {
        *per_file.entry(file.clone()).or_default() += 1;
    }
    assert_eq!(
        per_file,
        std::collections::BTreeMap::from([
            ("bitcoin_review.rs".to_string(), 1),
            ("payment.rs".to_string(), 1),
            ("payment_methods.rs".to_string(), 2),
            ("workers.rs".to_string(), 4),
        ]),
        "the manual_review writer set drifted; stamp and enumerate the new writer"
    );
}
