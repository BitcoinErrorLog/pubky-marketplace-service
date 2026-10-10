-- Distinguish durable fork work from upstream Marketplace lifecycle work.
-- Existing rows are fork rows; upstream rows use creator + invoice_id and
-- deliberately carry no stack identity or per-order endpoint.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS paykit_api TEXT
        CHECK (paykit_api IN ('fork', 'upstream')),
    ADD COLUMN IF NOT EXISTS paykit_payment_reference UUID,
    ADD COLUMN IF NOT EXISTS paykit_operation_id TEXT,
    ADD COLUMN IF NOT EXISTS paykit_payment_window_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS paykit_asset TEXT;

-- Support can name exact upstream attempt without re-deriving it. Fork rows
-- keep all four columns NULL. Keep asset open for later rails.
ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_paykit_prepared_attempt_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_paykit_prepared_attempt_check CHECK (
        (
            paykit_api = 'upstream'
            AND paykit_payment_reference IS NOT NULL
            AND paykit_operation_id IS NOT NULL
            AND char_length(paykit_operation_id) > 0
            AND paykit_payment_window_seconds > 0
            AND paykit_asset IS NOT NULL
            AND char_length(paykit_asset) > 0
        ) OR (
            paykit_api IS DISTINCT FROM 'upstream'
            AND paykit_payment_reference IS NULL
            AND paykit_operation_id IS NULL
            AND paykit_payment_window_seconds IS NULL
            AND paykit_asset IS NULL
        )
    ) NOT VALID;

ALTER TABLE payments
    DROP CONSTRAINT IF EXISTS payments_review_reason_check;
ALTER TABLE payments
    ADD CONSTRAINT payments_review_reason_check CHECK (
        review_reason IS NULL OR review_reason IN (
            'late_settlement', 'refund_required', 'amount_mismatch',
            'unpinned_legacy', 'upstream_inconsistent'
        )
    ) NOT VALID;

UPDATE orders SET paykit_api = 'fork'
WHERE paykit_invoice_id IS NOT NULL AND paykit_api IS NULL;

ALTER TABLE paykit_superseded_attempts
    ADD COLUMN IF NOT EXISTS paykit_api TEXT NOT NULL DEFAULT 'fork',
    ADD COLUMN IF NOT EXISTS payment_reference UUID,
    ADD COLUMN IF NOT EXISTS operation_id TEXT,
    ADD COLUMN IF NOT EXISTS payment_window_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS asset TEXT;

ALTER TABLE paykit_superseded_attempts
    ALTER COLUMN stack_id DROP NOT NULL,
    ALTER COLUMN stack_endpoint DROP NOT NULL;

ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_reference_check;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_reference_check CHECK (
        reference IS NULL
        OR char_length(reference) = 26
        OR reference ~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    );
ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_api_shape;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_api_shape CHECK (
        (
            paykit_api = 'fork'
            AND stack_id IS NOT NULL
            AND stack_endpoint IS NOT NULL
            AND payment_reference IS NULL
            AND operation_id IS NULL
            AND payment_window_seconds IS NULL
            AND asset IS NULL
        ) OR (
            paykit_api = 'upstream'
            AND stack_id IS NULL
            AND stack_endpoint IS NULL
            AND payment_reference IS NOT NULL
            AND operation_id IS NOT NULL
            AND char_length(operation_id) > 0
            AND payment_window_seconds > 0
            AND asset IS NOT NULL
            AND char_length(asset) > 0
        )
    );

ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_review_reason_check;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_review_reason_check CHECK (
        review_reason IN (
            'payment_settled', 'other_rail', 'detected_unconfirmed', 'upstream_inconsistent'
        )
    );

DO $$
DECLARE
    old_dismissal_check TEXT;
BEGIN
    SELECT conname INTO old_dismissal_check
    FROM pg_constraint
    WHERE conrelid = 'paykit_superseded_attempts'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%resolution_outcome IS DISTINCT FROM%'
      AND pg_get_constraintdef(oid) LIKE '%review_reason = ''detected_unconfirmed''%'
    LIMIT 1;
    IF old_dismissal_check IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE paykit_superseded_attempts DROP CONSTRAINT %I',
            old_dismissal_check
        );
    END IF;
END $$;
ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_dismissal_check;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_dismissal_check CHECK (
        resolution_outcome IS DISTINCT FROM 'dismissed'
        OR review_reason IN ('detected_unconfirmed', 'upstream_inconsistent')
    );

ALTER TABLE paykit_resolve_outbox
    ADD COLUMN IF NOT EXISTS paykit_api TEXT NOT NULL DEFAULT 'fork',
    ADD COLUMN IF NOT EXISTS creator_pubky TEXT;

ALTER TABLE paykit_resolve_outbox
    ALTER COLUMN stack_id DROP NOT NULL,
    ALTER COLUMN stack_endpoint DROP NOT NULL;

ALTER TABLE paykit_resolve_outbox
    DROP CONSTRAINT IF EXISTS paykit_resolve_outbox_api_shape;
ALTER TABLE paykit_resolve_outbox
    ADD CONSTRAINT paykit_resolve_outbox_api_shape CHECK (
        (paykit_api = 'fork' AND stack_id IS NOT NULL AND stack_endpoint IS NOT NULL
            AND creator_pubky IS NULL)
        OR (paykit_api = 'upstream' AND stack_id IS NULL AND stack_endpoint IS NULL
            AND creator_pubky IS NOT NULL)
    );
