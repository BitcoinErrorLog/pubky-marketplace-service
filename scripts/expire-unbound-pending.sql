-- Operator step for leftover unbound pending_payment carts (Pav 10 Sep class).
-- NOT a sqlx migration. Do not run at migrate-at-boot. Do not run in this
-- implementation wave. Dry-run the SELECTs first (read-only, exact-ID
-- `railway connect --ssh`, counts/ids only, no PII). Apply the UPDATE only
-- after stop-start cutover, and only when bound_unheld is 0 or escalated.

-- Dry-run: ordinary unbound pending (intended blast + in-flight method-screen carts)
SELECT count(*) AS unbound_pending
FROM orders
WHERE state = 'pending_payment'
  AND stock_held = false
  AND auction_aggregate_id IS NULL
  AND drop_aggregate_id IS NULL
  AND payment_method IS NULL;

SELECT id AS unbound_pending_id
FROM orders
WHERE state = 'pending_payment'
  AND stock_held = false
  AND auction_aggregate_id IS NULL
  AND drop_aggregate_id IS NULL
  AND payment_method IS NULL;

-- Bound-but-unheld: payment destination exists. Do not cancel these.
-- Late money follows the late_completion / refund_required fork.
SELECT count(*) AS bound_unheld
FROM orders
WHERE state = 'pending_payment'
  AND stock_held = false
  AND payment_method IS NOT NULL
  AND auction_aggregate_id IS NULL
  AND drop_aggregate_id IS NULL;

-- Apply (unbound_pending only). No hold to release.
-- UPDATE orders
-- SET state = 'cancelled',
--     cancellation_reason = 'payment window elapsed',
--     updated_at = now()
-- WHERE state = 'pending_payment'
--   AND stock_held = false
--   AND auction_aggregate_id IS NULL
--   AND drop_aggregate_id IS NULL
--   AND payment_method IS NULL;
