-- The Paykit attempt the upstream API prepares (`PAYKIT_SERVER_API=upstream`,
-- pubky/paykit-server #66): additive, idempotent, directly rerunnable.
--
-- The upstream prepare names an attempt by a caller operation id and a
-- UUIDv4 payment reference, binds a payment window in seconds that starts at
-- activation, and answers no stack, nonce or address fingerprint. These
-- columns hold what the fork's pins do not:
--
--   paykit_payment_reference        the deterministic UUIDv4 sent as `reference`
--                                   (derived from the order id and the bind
--                                   attempt); NULL for every fork attempt
--   paykit_operation_id             `marketplace-payment:{attempt_reference}:{attempt}`
--   paykit_payment_window_seconds   the payment window bound at preparation
--   paykit_asset                    what the attempt is denominated in; no
--                                   CHECK, so a later asset needs no migration
--
-- The activation deadline is `paykit_prepare_expires_at` (paykit-server's
-- database clock, 15 minutes after preparation by default), the same column
-- the fork records. An upstream attempt has `paykit_stack_id` and
-- `paykit_stack_endpoint` NULL.
--
-- Nullable columns without defaults: no table rewrite, and a binary built
-- before this file lists its `orders` columns, so it ignores them. There is
-- NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS paykit_payment_reference UUID,
    ADD COLUMN IF NOT EXISTS paykit_operation_id TEXT,
    ADD COLUMN IF NOT EXISTS paykit_payment_window_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS paykit_asset TEXT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paykit_prepared_attempt_check;
ALTER TABLE orders ADD CONSTRAINT orders_paykit_prepared_attempt_check
    CHECK (
        (paykit_payment_reference IS NULL) = (paykit_operation_id IS NULL)
        AND (paykit_payment_reference IS NULL) = (paykit_asset IS NULL)
        AND (paykit_payment_reference IS NULL) = (paykit_payment_window_seconds IS NULL)
        AND (paykit_payment_window_seconds IS NULL OR paykit_payment_window_seconds > 0)
    ) NOT VALID;
