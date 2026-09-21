//! Schema proof for the additive Locks order-binding migration (0026) and
//! the retention-registry enumeration of its new sensitive columns: every
//! column capable of holding Locks correlation material (bundle id, lock
//! resource, client reference) is ciphertext, an HMAC token, or a hash, so
//! the migration adds no plaintext retention surface a cleanup task would
//! have to erase. A new sensitive column fails the registry assertion until
//! it is reviewed and enumerated here.

mod common;

use common::count;
use sqlx::PgPool;

/// The 0026 listing columns, enumerated once so a schema drift fails loudly.
const LISTING_LOCK_COLUMNS: [&str; 2] = ["digital_lock_policy_uri", "digital_lock_criterion_id"];

/// The 0026 correlation columns, enumerated once so a schema drift fails
/// loudly.
const CORRELATION_BINDING_COLUMNS: [&str; 8] = [
    "expected_resource_ciphertext",
    "expected_resource_hash",
    "criterion_id",
    "expected_reader_pubky",
    "expected_recipient_pubky",
    "client_reference_ciphertext",
    "preparation_state",
    "binding_outcome",
];

/// The retention registry: every `payment_locks_correlations` column allowed
/// to carry Locks correlation material, with the storage class that keeps it
/// out of the plaintext retention surface. Anything matching the sensitive
/// name pattern outside this set is a plaintext leak the cleanup story does
/// not cover.
const SENSITIVE_CORRELATION_COLUMNS: [(&str, &str); 6] = [
    ("bundle_id_ciphertext", "bytea"),
    ("bundle_lookup_token", "bytea"),
    ("expected_resource_ciphertext", "bytea"),
    ("expected_resource_hash", "text"),
    ("client_reference_ciphertext", "bytea"),
    ("lock_resource_hash", "text"),
];

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0026_adds_the_locks_binding_schema(pool: PgPool) {
    for (table, columns) in [
        ("listings", LISTING_LOCK_COLUMNS.as_slice()),
        (
            "payment_locks_correlations",
            CORRELATION_BINDING_COLUMNS.as_slice(),
        ),
    ] {
        for column in columns {
            let (present,): (bool,) = sqlx::query_as(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_name = $1 AND column_name = $2)",
            )
            .bind(table)
            .bind(column)
            .fetch_one(&pool)
            .await
            .expect("column catalog is readable");
            assert!(present, "{table}.{column} is missing");
        }
    }

    // The pair and prepared-fields CHECK constraints exist by name.
    for constraint in [
        "listings_digital_lock_pair",
        "payment_locks_prepared_fields",
    ] {
        let (present,): (bool,) =
            sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $1)")
                .bind(constraint)
                .fetch_one(&pool)
                .await
                .expect("constraint catalog is readable");
        assert!(present, "{constraint} is missing");
    }

    // The legacy bundle columns are nullable so a prepared row can exist
    // before any bundle is attached.
    for column in ["bundle_id_ciphertext", "bundle_lookup_token"] {
        let (nullable,): (String,) = sqlx::query_as(
            "SELECT is_nullable FROM information_schema.columns \
             WHERE table_name = 'payment_locks_correlations' AND column_name = $1",
        )
        .bind(column)
        .fetch_one(&pool)
        .await
        .expect("nullability is readable");
        assert_eq!(nullable, "YES", "{column} must accept a prepared row");
    }

    // The listing pair CHECK: one half of the pair without the other is
    // uncommittable.
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
         listing_revision, content_hash, server_revision, state, total_quantity, \
         available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
         unit_price_currency, unit_price_exponent, shipping_minor, sale_format, \
         fulfillment_methods, digital_lock_policy_uri, updated_at) \
         VALUES ('listing:s_x', 's', 'x', 't', 1, 'h', 1, 'active', 1, 1, 0, 0, \
         100, 'USD', 2, 0, 'fixed', '{shipping}', \
         'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json', \
         now())",
    )
    .execute(&pool)
    .await
    .expect_err("a policy uri without a criterion id violates the pair CHECK");

    // The prepared-fields CHECK: a 'prepared' row without the full
    // expectation set is uncommittable.
    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, \
         creator_pubky, lock_resource_hash, amount_minor, asset, exponent, policy_version, \
         verification_state, window_expires_at, preparation_state, created_at, updated_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'b', 'c', 'h', \
         100, 'USD', 2, 1, 'pending', now(), 'prepared', now(), now())",
    )
    .execute(&pool)
    .await
    .expect_err("a prepared row without the expectation fields violates the CHECK");

    // The preparation_state CHECK refuses values outside the designed set.
    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, \
         creator_pubky, lock_resource_hash, amount_minor, asset, exponent, policy_version, \
         verification_state, window_expires_at, preparation_state, created_at, updated_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'b', 'c', 'h', \
         100, 'USD', 2, 1, 'pending', now(), 'minted', now(), now())",
    )
    .execute(&pool)
    .await
    .expect_err("an unknown preparation state violates the CHECK");
}

// The retention registry: enumerate every correlation column whose name
// carries bundle, resource, or reference material and prove each one is
// sealed or hashed — never plaintext — so the table's retention story (an
// audit row with no purge) never exposes correlation secrets, and a future
// column cannot join the table unreviewed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn locks_correlation_sensitive_columns_stay_sealed_or_hashed(pool: PgPool) {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'payment_locks_correlations' \
         AND (column_name LIKE '%bundle%' OR column_name LIKE '%resource%' \
              OR column_name LIKE '%reference%') \
         ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("column catalog is readable");
    let actual: Vec<(String, String)> = columns.into_iter().collect();
    let mut expected: Vec<(String, String)> = SENSITIVE_CORRELATION_COLUMNS
        .iter()
        .map(|(name, data_type)| (name.to_string(), data_type.to_string()))
        .collect();
    expected.sort();
    assert_eq!(
        actual, expected,
        "the sensitive-column registry drifted: every bundle/resource/reference \
         column must stay ciphertext, token, or hash (no plaintext retention surface)"
    );

    // And the registry's inverse: no plaintext column may carry the sealed
    // values, so the redaction suite's only durable surfaces are the
    // enumerated ones.
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE table_name = 'payment_locks_correlations' \
             AND column_name IN ('bundle_id', 'lock_resource', 'pubky_lock_resource', \
                                 'client_reference', 'locks_bundle_id')"
        )
        .await,
        0,
        "no plaintext correlation column may exist"
    );
}
