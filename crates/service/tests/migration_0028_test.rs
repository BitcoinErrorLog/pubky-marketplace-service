//! Schema proof for the additive Locks binding-outcome audit migration
//! (0028): the audit table exists with the static outcome vocabulary
//! enforced by CHECK, so an outcome outside the designed set is
//! uncommittable. Migration 0031 narrowed the vocabulary to the two
//! success outcomes: refusal rows are never written, so every `refused_*`
//! value is uncommittable.

mod common;

use sqlx::PgPool;

/// The outcome vocabulary after 0031's round-cap cut (refusal auditing
/// removed): only the two success outcomes, enumerated once so a schema
/// drift fails loudly.
const BINDING_OUTCOMES: [&str; 2] = ["prepared", "registered"];

/// The refusal values the round cap removed from the vocabulary: each
/// must now violate the CHECK.
const REMOVED_REFUSAL_OUTCOMES: [&str; 8] = [
    "refused_identity",
    "refused_criterion",
    "refused_unavailable",
    "refused_expired",
    "refused_no_prepare",
    "refused_already_registered",
    "refused_order_hold",
    "refused_no_snapshot",
];

#[sqlx::test(migrations = "./migrations")]
async fn migration_0028_adds_the_binding_outcome_audit(pool: PgPool) {
    // The exact column set, enumerated once so a drift (or a new sensitive
    // column) fails loudly: the audit table carries ids, the static outcome
    // vocabulary, and a timestamp — never bundle, resource, or reference
    // material.
    let mut columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("column catalog is readable");
    columns.sort();
    assert_eq!(
        columns,
        vec![
            ("command_id".to_string(),),
            ("id".to_string(),),
            ("outcome".to_string(),),
            ("payment_id".to_string(),),
            ("recorded_at".to_string(),),
        ],
        "the binding-outcome audit column set drifted"
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

    // Every refusal value the round cap removed violates the CHECK.
    for outcome in REMOVED_REFUSAL_OUTCOMES {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes (id, payment_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), $1, now())",
        )
        .bind(outcome)
        .execute(&pool)
        .await
        .expect_err("a removed refusal outcome must violate the CHECK");
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
