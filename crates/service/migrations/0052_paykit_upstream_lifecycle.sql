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
        (paykit_payment_reference IS NULL) = (paykit_operation_id IS NULL)
        AND (paykit_payment_reference IS NULL) = (paykit_payment_window_seconds IS NULL)
        AND (paykit_payment_reference IS NULL) = (paykit_asset IS NULL)
        AND (paykit_payment_window_seconds IS NULL OR paykit_payment_window_seconds > 0)
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
    ADD COLUMN IF NOT EXISTS paykit_api TEXT NOT NULL DEFAULT 'fork';

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
        (paykit_api = 'fork' AND stack_id IS NOT NULL AND stack_endpoint IS NOT NULL)
        OR (paykit_api = 'upstream' AND stack_id IS NULL AND stack_endpoint IS NULL)
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
