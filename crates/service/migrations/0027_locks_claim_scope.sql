-- The lifecycle claim must never select a prepared (not yet registered)
-- row: such rows carry no bundle ciphertext, and one undecodable null row
-- would fail the whole claim batch (Sol Wave 1A review, P1-1). The claim
-- query, its supporting partial index, and the schema now agree: only
-- `registered` correlations with a non-null bundle are polled.

DROP INDEX payment_locks_correlations_pending_idx;
CREATE INDEX payment_locks_correlations_pending_idx
    ON payment_locks_correlations (last_checked_at NULLS FIRST)
    WHERE verification_state = 'pending'
      AND preparation_state = 'registered'
      AND bundle_id_ciphertext IS NOT NULL;

-- The reciprocal of 0026's payment_locks_prepared_fields CHECK: a
-- registered row always carries both bundle fields (legacy rows already
-- do — the columns were NOT NULL until 0026), so together the two checks
-- make bundle presence biconditional with the registered state.
ALTER TABLE payment_locks_correlations
    ADD CONSTRAINT payment_locks_registered_bundle CHECK (
        preparation_state <> 'registered'
        OR (
            bundle_id_ciphertext IS NOT NULL
            AND bundle_lookup_token IS NOT NULL
        )
    );
