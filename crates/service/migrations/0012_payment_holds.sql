-- Payment-time inventory holds: "only a payment should lock an item".
--
-- Checkout no longer moves inventory for ordinary listings. A payment lock
-- point (payment.register_locks, the payment-method bind, or the sandbox
-- adapter's first transition out of awaiting_entitlement) atomically moves
-- available -> reserved for the order's quantity and arms a bounded
-- server-time hold window on the order. Drop-bound checkouts keep
-- lock-at-claim (the FCFS race is the product) and arm the claim window at
-- checkout. The payment-window worker releases lapsed holds, expires the
-- payment, and cancels the order.

ALTER TABLE orders
    ADD COLUMN hold_expires_at TIMESTAMPTZ,
    -- Whether this order currently holds `reserved` listing stock of its
    -- own. Auction orders are deliberately excluded: their hold lives in
    -- the winning `reservations` row and is governed by reservation expiry.
    ADD COLUMN stock_held BOOLEAN NOT NULL DEFAULT false;

-- The payment-window sweep scans due holds by this partial predicate.
CREATE INDEX orders_hold_window_due ON orders (hold_expires_at)
    WHERE state = 'pending_payment' AND stock_held;

-- `orders.cancellation_reason` already exists (0001_init.sql); the window
-- sweep stores "payment window elapsed" there.

-- Backfill: under the old semantics every pending_payment checkout order
-- DID decrement its listings at checkout, so each one currently holds
-- reserved stock. Auction orders are excluded — their hold is the winning
-- reservation row, swept by reservation expiry exactly as before.
--
--   * Orders with a Locks correlation keep the correlation's window: the
--     correlation window IS the hold window from now on.
--   * Orders with a bound fiat/bitcoin payment method get now() plus the
--     fiat payment window (FIAT_PAYMENT_WINDOW_SECONDS default, 1 hour).
--   * Every other pending order — legacy abandoned carts included — gets
--     now() + 1 hour, so currently-stuck listings self-clean through the
--     new worker within the hour.
UPDATE orders o SET
    stock_held = true,
    hold_expires_at = CASE
        WHEN EXISTS (
            SELECT 1 FROM payment_locks_correlations c WHERE c.order_id = o.id
        ) THEN (
            SELECT c.window_expires_at FROM payment_locks_correlations c
            WHERE c.order_id = o.id
        )
        WHEN o.payment_method IS NOT NULL THEN now() + interval '1 hour'
        ELSE now() + interval '1 hour'
    END
WHERE o.state = 'pending_payment' AND o.auction_aggregate_id IS NULL;
