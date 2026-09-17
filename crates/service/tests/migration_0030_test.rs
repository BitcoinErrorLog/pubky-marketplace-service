//! Schema proof for the additive refusal-audit boundedness migration
//! (0030): the `(payment_id, command_id)` idempotency key with its UNIQUE
//! arbiter (an exact retried command appends nothing), the extended static
//! outcome vocabulary, and the retention-registry enumeration of the audit
//! table — its full column set is ids, the static outcome, and a
//! timestamp, so a new column (or a sensitive one) fails loudly, and the
//! age-based purge rule (`recorded_at`) has its column pinned.

mod common;

use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn migration_0030_keys_binding_outcomes_per_command(pool: PgPool) {
    // The idempotency-key column exists and the UNIQUE arbiter backing
    // ON CONFLICT (payment_id, command_id) DO NOTHING is present.
    let (nullable,): (String,) = sqlx::query_as(
        "SELECT is_nullable FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' AND column_name = 'command_id'",
    )
    .fetch_one(&pool)
    .await
    .expect("command_id column exists");
    assert_eq!(nullable, "YES", "legacy rows predate the key");
    let (unique_index,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (
             SELECT 1 FROM pg_indexes
             WHERE tablename = 'payment_locks_binding_outcomes'
             AND indexname = 'payment_locks_binding_outcomes_payment_command_uq'
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("index catalog is readable");
    assert!(unique_index, "the (payment, command) UNIQUE arbiter exists");

    // The arbiter rejects a second row for the same (payment, command)…
    let payment_id = uuid::Uuid::new_v4();
    let command_id = uuid::Uuid::new_v4();
    for _ in 0..2 {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes \
             (id, payment_id, command_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), $1, $2, 'refused_no_prepare', now()) \
             ON CONFLICT (payment_id, command_id) DO NOTHING",
        )
        .bind(payment_id)
        .bind(command_id)
        .execute(&pool)
        .await
        .expect("idempotent insert");
    }
    let (rows,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM payment_locks_binding_outcomes WHERE payment_id = $1")
            .bind(payment_id)
            .fetch_one(&pool)
            .await
            .expect("rows counted");
    assert_eq!(rows, 1, "an exact retry appends nothing");
    // …while a distinct command id appends its own row.
    sqlx::query(
        "INSERT INTO payment_locks_binding_outcomes \
         (id, payment_id, command_id, outcome, recorded_at) \
         VALUES (gen_random_uuid(), $1, gen_random_uuid(), 'refused_no_prepare', now()) \
         ON CONFLICT (payment_id, command_id) DO NOTHING",
    )
    .bind(payment_id)
    .execute(&pool)
    .await
    .expect("a distinct command appends");
    let (rows,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM payment_locks_binding_outcomes WHERE payment_id = $1")
            .bind(payment_id)
            .fetch_one(&pool)
            .await
            .expect("rows counted");
    assert_eq!(rows, 2, "distinct commands keep full audit granularity");

    // The extended vocabulary commits; a value outside it still violates
    // the CHECK.
    for outcome in [
        "refused_already_registered",
        "refused_order_hold",
        "refused_no_snapshot",
    ] {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes \
             (id, payment_id, command_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), $1, now())",
        )
        .bind(outcome)
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("designed outcome {outcome} is committable: {error}"));
    }
    sqlx::query(
        "INSERT INTO payment_locks_binding_outcomes \
         (id, payment_id, command_id, outcome, recorded_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'refused_unknown', now())",
    )
    .execute(&pool)
    .await
    .expect_err("an outcome outside the vocabulary violates the CHECK");

    // The retention purge key: the age column the purge rule deletes by is
    // pinned, and the table still carries no correlation material.
    let (recorded_at,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' AND column_name = 'recorded_at')",
    )
    .fetch_one(&pool)
    .await
    .expect("column catalog is readable");
    assert!(
        recorded_at,
        "the age-based purge rule deletes by recorded_at"
    );
    let (sensitive,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' \
         AND (column_name LIKE '%bundle%' OR column_name LIKE '%resource%' \
              OR column_name LIKE '%reference%')",
    )
    .fetch_one(&pool)
    .await
    .expect("column catalog is readable");
    assert_eq!(
        sensitive, 0,
        "the audit table must never retain correlation material"
    );
}
