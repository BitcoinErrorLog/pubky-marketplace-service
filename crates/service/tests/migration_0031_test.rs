//! Schema proof for the round-cap cut migration (0031): historical
//! outcome-table refusal auditing is removed from the binding-outcome
//! mechanism. The outcome CHECK
//! admits ONLY the two success outcomes (`prepared`, `registered`) and
//! rejects every `refused_*` value; the `(payment_id, command_id)` UNIQUE
//! arbiter 0030 added is dropped; and the table COMMENT documents that
//! refusal rows are never written. The 0028 table and the 0030
//! `command_id` column remain (migrations are additive-only).

mod common;

use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn migration_0031_removes_refusal_auditing(pool: PgPool) {
    // The narrowed CHECK: the exact vocabulary is the two success
    // outcomes, enforced by the constraint definition itself.
    let (check,): (String,) = sqlx::query_as(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conname = 'payment_locks_binding_outcomes_outcome_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("the outcome CHECK exists");
    assert!(
        check.contains("'prepared'"),
        "prepared is admitted: {check}"
    );
    assert!(
        check.contains("'registered'"),
        "registered is admitted: {check}"
    );
    assert!(
        !check.contains("refused"),
        "no refusal value remains in the vocabulary: {check}"
    );

    // The two success outcomes commit; every removed refusal value is
    // uncommittable.
    for outcome in ["prepared", "registered"] {
        sqlx::query(
            "INSERT INTO payment_locks_binding_outcomes (id, payment_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), $1, now())",
        )
        .bind(outcome)
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("success outcome {outcome} is committable: {error}"));
    }
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
            "INSERT INTO payment_locks_binding_outcomes (id, payment_id, outcome, recorded_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), $1, now())",
        )
        .bind(outcome)
        .execute(&pool)
        .await
        .expect_err("a removed refusal outcome must violate the CHECK");
    }

    // The (payment_id, command_id) UNIQUE arbiter is gone.
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
        "the round cap dropped the refusal-audit UNIQUE arbiter"
    );

    // The additive-only history remains: the 0030 command_id column was
    // never dropped, and the table COMMENT documents that refusal rows
    // are not written.
    let (command_id,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'payment_locks_binding_outcomes' AND column_name = 'command_id')",
    )
    .fetch_one(&pool)
    .await
    .expect("column catalog is readable");
    assert!(command_id, "additive-only history keeps command_id");
    let (comment,): (Option<String>,) = sqlx::query_as(
        "SELECT obj_description('payment_locks_binding_outcomes'::regclass, 'pg_class')",
    )
    .fetch_one(&pool)
    .await
    .expect("table comment is readable");
    let comment = comment.expect("the table carries a COMMENT");
    assert!(
        comment.contains("Refusal rows are never written"),
        "the COMMENT documents the removed outcome-table refusal auditing: {comment}"
    );
}
