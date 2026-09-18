-- Round-cap cut (Wave 1A review round 3, decision 2026-09-17): refusal
-- auditing is REMOVED from the binding-outcome mechanism. Committing a
-- refusing command's transaction could persist partial inventory
-- mutations (hold acquisition updates earlier listing rows before a later
-- order line fails), and the client-chosen command id made the audit key
-- suppressible. Refusing commands therefore roll back exactly as they did
-- before the audit existed, and only the two success outcomes remain in
-- the vocabulary. Migrations are additive-only: 0028/0030 objects stay;
-- this migration narrows the CHECK and drops the (payment_id, command_id)
-- UNIQUE arbiter so no refusal row can ever be written again. Refusal
-- auditing returns as a designed item (server-derived identity, bounded
-- per payment, savepoint semantics) in a later wave.

ALTER TABLE payment_locks_binding_outcomes
    DROP CONSTRAINT payment_locks_binding_outcomes_outcome_check;

ALTER TABLE payment_locks_binding_outcomes
    ADD CONSTRAINT payment_locks_binding_outcomes_outcome_check CHECK (outcome IN (
        'prepared',
        'registered'
    ));

DROP INDEX IF EXISTS payment_locks_binding_outcomes_payment_command_uq;

COMMENT ON TABLE payment_locks_binding_outcomes IS
    'Locks binding outcomes: prepared/registered only, written in the transaction that performs the state change. Refusal rows are never written — a refusing command rolls back whole (refusal auditing was removed at the round cap; see migration 0031).';
