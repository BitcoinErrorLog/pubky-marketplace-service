-- The Locks binding outcome audit (Sol Wave 1A review, P2-3). Every
-- designed prepare/register outcome is recorded with the static vocabulary
-- below: row outcomes (`prepared`, `registered`) are also stamped on the
-- correlation's binding_outcome column in the same transaction, while
-- refusals are recorded here only — a refused command rolls back, so the
-- audit row is written independently and never blocks a retry.

CREATE TABLE payment_locks_binding_outcomes (
    id UUID PRIMARY KEY,
    payment_id UUID NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'prepared',
        'registered',
        'refused_identity',
        'refused_criterion',
        'refused_unavailable',
        'refused_expired',
        'refused_no_prepare'
    )),
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX payment_locks_binding_outcomes_payment_idx
    ON payment_locks_binding_outcomes (payment_id);
