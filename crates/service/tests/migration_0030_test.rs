//! Schema proof for the historical outcome-table refusal-audit migration
//! (0030) AS AMENDED by the round-cap cut (0031): the `command_id` column
//! 0030 added remains (migrations are additive-only, so it is never
//! dropped), but 0031 removed refusal rows from this outcome table — the
//! `(payment_id, command_id)` UNIQUE arbiter is gone, the outcome CHECK
//! admits only the two success outcomes, and every `refused_*` value is
//! uncommittable. This is distinct from the later bounded refusal-audit
//! buckets. The retention-registry enumeration of the outcome table
//! stays: its full column set is ids, the static outcome, and a
//! timestamp, so a new column (or a sensitive one) fails loudly.

mod common;

use sqlx::PgPool;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0030_keys_binding_outcomes_per_command(pool: PgPool) {
    // The 0030 key column exists and stays nullable (legacy rows predate
    // it); additive-only history never drops it.
    let (nullable,): (String,) = sqlx::query_as(
        "SELECT is_nullable FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' AND column_name = 'command_id'",
    )
    .fetch_one(&pool)
    .await
    .expect("command_id column exists");
    assert_eq!(nullable, "YES", "legacy rows predate the key");

    // The round-cap cut (0031) dropped the (payment, command) UNIQUE
    // arbiter with the outcome-table refusal auditing it served: success outcomes are
    // recorded inside the committing transaction, which the executor's
    // command_results dedup already serializes, so no audit arbiter
    // remains.
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
    assert!(
        !unique_index,
        "0031 dropped the (payment, command) UNIQUE arbiter"
    );

    // Two success-outcome rows for one payment commit side by side —
    // nothing suppresses a legitimate `registered` after a `prepared`.
    let payment_id = uuid::Uuid::new_v4();
    for outcome in ["prepared", "registered"] {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes \
             (id, payment_id, command_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), $1, gen_random_uuid(), $2, now())",
        )
        .bind(payment_id)
        .bind(outcome)
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("success outcome {outcome} is committable: {error}"));
    }
    let (rows,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM payment_locks_binding_outcomes WHERE payment_id = $1")
            .bind(payment_id)
            .fetch_one(&pool)
            .await
            .expect("rows counted");
    assert_eq!(rows, 2, "both success outcomes commit");

    // Every refusal value the round cap removed violates the CHECK.
    for outcome in [
        "refused_identity",
        "refused_criterion",
        "refused_unavailable",
        "refused_expired",
        "refused_no_prepare",
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
        .expect_err("a removed refusal outcome must violate the CHECK");
    }

    // The historical outcome table still carries no correlation material.
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
        "the outcome table must never retain correlation material"
    );
}
