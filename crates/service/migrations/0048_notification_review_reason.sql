-- Why a Bitcoin payment waits for the seller's decision, carried on the
-- `bitcoin_manual_review` notification. Frozen when the notification is
-- written: a later change to the payment never rewrites it.
-- NULL on every other notification type and on rows delivered before it
-- existed. Additive and rerunnable.
--   late_settlement                     Paykit reported the settlement late
--   amount_mismatch                     the observed amount differs from the invoice
--   confirmation_failed                 confirmed money the order could no longer take
--   seller_confirmation_window_elapsed  the 24-hour seller window lapsed unconfirmed
--   seller_response_overdue             the review has waited two business days
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS review_reason TEXT;
ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_review_reason_check;
ALTER TABLE notifications ADD CONSTRAINT notifications_review_reason_check
    CHECK (
        review_reason IS NULL
        OR review_reason IN (
            'late_settlement',
            'amount_mismatch',
            'confirmation_failed',
            'seller_confirmation_window_elapsed',
            'seller_response_overdue'
        )
    );
