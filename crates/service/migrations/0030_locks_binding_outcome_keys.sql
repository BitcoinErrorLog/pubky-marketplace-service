-- Historical Locks outcome-table refusal auditing (Sol Wave 1A review
-- round 2, P2-3 + the pool-exhaustion P1). Migration 0031 subsequently
-- removes these refusal outcomes. The separate refusal-audit bucket system
-- introduced later is unrelated to this table and preserves these historical
-- migration semantics.
--
-- Idempotency key: (payment_id, command_id). A refused command is never
-- stored in command_results, so an exact retry re-executes; the UNIQUE
-- key plus ON CONFLICT DO NOTHING makes that retry append nothing. The
-- command-scoped key is chosen over an outcome/actor coalesce BECAUSE it
-- preserves one audit row per distinct refusing command (full granularity
-- of distinct refusals) while bounding each command id to exactly one row;
-- an outcome-keyed coalesce would collapse distinct commands and destroy
-- audit evidence.
--
-- Retention: rows are purged by age (`recorded_at` older than
-- LOCKS_OUTCOME_RETENTION_DAYS) by the worker's locks pass. The
-- vocabulary is static and the rows carry no correlation material.
--
-- Vocabulary: the three previously unrecorded designed refusals join the
-- CHECK — the already-registered refusal, the cancellation/order-hold
-- refusal, and the missing-checkout-snapshot refusal.

ALTER TABLE payment_locks_binding_outcomes
    ADD COLUMN command_id UUID;

CREATE UNIQUE INDEX payment_locks_binding_outcomes_payment_command_uq
    ON payment_locks_binding_outcomes (payment_id, command_id);

ALTER TABLE payment_locks_binding_outcomes
    DROP CONSTRAINT payment_locks_binding_outcomes_outcome_check;

ALTER TABLE payment_locks_binding_outcomes
    ADD CONSTRAINT payment_locks_binding_outcomes_outcome_check CHECK (outcome IN (
        'prepared',
        'registered',
        'refused_identity',
        'refused_criterion',
        'refused_unavailable',
        'refused_expired',
        'refused_no_prepare',
        'refused_already_registered',
        'refused_order_hold',
        'refused_no_snapshot'
    ));
