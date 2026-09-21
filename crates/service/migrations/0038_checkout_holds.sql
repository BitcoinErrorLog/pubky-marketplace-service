-- Exclusive checkout hold (D30.1 / #50): schema-only, additive, idempotent.
-- sqlx migrate-at-boot must not cancel live orders. Zombie cleanup is a
-- separate operator step in scripts/expire-unbound-pending.sql.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS hold_source TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS review_reason TEXT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_hold_source_check;
ALTER TABLE orders ADD CONSTRAINT orders_hold_source_check
    CHECK (
        hold_source IS NULL
        OR hold_source IN ('checkout', 'locks', 'bind', 'sandbox', 'drop_claim')
    );

ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_review_reason_check;
ALTER TABLE payments ADD CONSTRAINT payments_review_reason_check
    CHECK (
        review_reason IS NULL
        OR review_reason IN (
            'late_settlement',
            'refund_required',
            'amount_mismatch',
            'unpinned_legacy'
        )
    );
