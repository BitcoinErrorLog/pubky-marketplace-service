-- An undeliverable notification outbox row is set aside on its own instead
-- of failing every drain pass and stalling the rows behind it. A
-- quarantined row is never claimed again and stays for an operator. NULL on
-- every other row. Additive and rerunnable.
--   unroutable_kind          the kind is neither a notification nor a paykit row
--   missing_recipient_pubky  the payload has no string recipient_pubky
--   missing_actor_pubky      the payload has no string actor_pubky
--   missing_aggregate_id     the payload has no string aggregate_id
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_quarantine_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_quarantine_check
    CHECK (
        (quarantined_at IS NULL) = (quarantine_reason IS NULL)
        AND (
            quarantine_reason IS NULL
            OR quarantine_reason IN (
                'unroutable_kind',
                'missing_recipient_pubky',
                'missing_actor_pubky',
                'missing_aggregate_id'
            )
        )
    );
