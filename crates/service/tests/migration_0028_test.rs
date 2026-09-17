//! Schema proof for the additive Locks binding-outcome audit migration
//! (0028): the audit table exists with the static outcome vocabulary
//! enforced by CHECK, so a refusal outcome outside the designed set is
//! uncommittable.

mod common;

use sqlx::PgPool;

/// The 0028 outcome vocabulary, enumerated once so a schema drift fails
/// loudly.
const BINDING_OUTCOMES: [&str; 7] = [
    "prepared",
    "registered",
    "refused_identity",
    "refused_criterion",
    "refused_unavailable",
    "refused_expired",
    "refused_no_prepare",
];

#[sqlx::test(migrations = "./migrations")]
async fn migration_0028_adds_the_binding_outcome_audit(pool: PgPool) {
    for column in ["id", "payment_id", "outcome", "recorded_at"] {
        let (present,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_name = 'payment_locks_binding_outcomes' AND column_name = $1)",
        )
        .bind(column)
        .fetch_one(&pool)
        .await
        .expect("column catalog is readable");
        assert!(
            present,
            "payment_locks_binding_outcomes.{column} is missing"
        );
    }

    // Every designed outcome is accepted.
    for outcome in BINDING_OUTCOMES {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes (id, payment_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), $1, now())",
        )
        .bind(outcome)
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("designed outcome {outcome} is committable: {error}"));
    }

    // Anything outside the vocabulary violates the CHECK.
    sqlx::query(
        "INSERT INTO payment_locks_binding_outcomes (id, payment_id, outcome, recorded_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), 'refused_unknown', now())",
    )
    .execute(&pool)
    .await
    .expect_err("an outcome outside the vocabulary violates the CHECK");
}
