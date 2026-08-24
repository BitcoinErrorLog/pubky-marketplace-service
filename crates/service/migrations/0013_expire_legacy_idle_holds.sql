-- 0012's backfill gave legacy abandoned carts a one-hour drain window out of
-- caution. That caution is worthless for orders with ZERO payment activity:
-- nobody is mid-flow on them, and under the new semantics a genuinely active
-- buyer just checks out again in seconds. Expire those holds NOW so the
-- window sweep restocks them on the next pass instead of wasting an hour of
-- everyone's time.
--
-- Scope: pending, stock-holding, non-auction orders with no Locks
-- correlation, no bound payment method, and a sandbox payment still sitting
-- in `awaiting_entitlement` — i.e. exactly the orders on which no payment
-- was ever started.
UPDATE orders o SET
    hold_expires_at = now()
WHERE o.state = 'pending_payment'
  AND o.stock_held
  AND o.auction_aggregate_id IS NULL
  AND o.payment_method IS NULL
  AND NOT EXISTS (SELECT 1 FROM payment_locks_correlations c WHERE c.order_id = o.id)
  AND EXISTS (
      SELECT 1 FROM payments p
      WHERE p.id = o.payment_id
        AND p.adapter = 'sandbox'
        AND p.state = 'awaiting_entitlement'
  );
