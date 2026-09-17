//! Schema proof for the additive immutable checkout-time Locks snapshot
//! migration (0029) and the retention-registry enumeration of its
//! sensitive columns: the snapshot's resource columns are ciphertext or a
//! hash — never plaintext — so the table adds no plaintext retention
//! surface, and a new sensitive column fails the registry assertion until
//! it is reviewed and enumerated here.

mod common;

use common::count;
use sqlx::PgPool;

/// The 0029 snapshot columns, enumerated once so a schema drift fails
/// loudly.
const SNAPSHOT_COLUMNS: [&str; 11] = [
    "payment_id",
    "order_id",
    "expected_resource_ciphertext",
    "expected_resource_hash",
    "criterion_id",
    "amount_minor",
    "asset",
    "exponent",
    "expected_reader_pubky",
    "expected_recipient_pubky",
    "created_at",
];

/// The retention registry: every snapshot column allowed to carry lock
/// resource material, with the storage class that keeps it out of the
/// plaintext retention surface.
const SENSITIVE_SNAPSHOT_COLUMNS: [(&str, &str); 2] = [
    ("expected_resource_ciphertext", "bytea"),
    ("expected_resource_hash", "text"),
];

#[sqlx::test(migrations = "./migrations")]
async fn migration_0029_adds_the_checkout_lock_snapshot(pool: PgPool) {
    let mut columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'payment_locks_checkout_snapshots' ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("column catalog is readable");
    columns.sort();
    let mut expected: Vec<(String,)> = SNAPSHOT_COLUMNS
        .iter()
        .map(|column| (column.to_string(),))
        .collect();
    expected.sort();
    assert_eq!(
        columns, expected,
        "the checkout snapshot column set drifted"
    );

    // One snapshot per payment, keyed by the payment id.
    let (primary_key,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.table_constraints
             WHERE table_name = 'payment_locks_checkout_snapshots'
             AND constraint_type = 'PRIMARY KEY'
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("constraint catalog is readable");
    assert!(primary_key, "the snapshot is keyed one-per-payment");

    // Every sensitive column is sealed or hashed, never plaintext.
    let sensitive: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'payment_locks_checkout_snapshots' \
         AND (column_name LIKE '%resource%') ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("column catalog is readable");
    let mut expected: Vec<(String, String)> = SENSITIVE_SNAPSHOT_COLUMNS
        .iter()
        .map(|(name, data_type)| (name.to_string(), data_type.to_string()))
        .collect();
    expected.sort();
    assert_eq!(
        sensitive, expected,
        "the sensitive-column registry drifted: every resource column must \
         stay ciphertext or hash (no plaintext retention surface)"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE table_name = 'payment_locks_checkout_snapshots' \
             AND column_name IN ('lock_resource', 'pubky_lock_resource', 'resource', 'policy_uri')"
        )
        .await,
        0,
        "no plaintext resource column may exist"
    );
}
