-- Automatic PayPal refund detection (docs/paypal-refund-ipn.md): additive,
-- idempotent, directly rerunnable.
--
-- `paypal_txn_id` is the original payment's PayPal `txn_id`, written only
-- from a postback-verified `Completed` IPN that passed the receiver,
-- currency, amount, and order checks. Refund IPNs match their
-- `parent_txn_id` against it, never against the buyer-writable
-- `fiat_transaction_ref`.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paypal_txn_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_reversed_at TIMESTAMPTZ;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paypal_txn_id_check;
ALTER TABLE orders ADD CONSTRAINT orders_paypal_txn_id_check
    CHECK (paypal_txn_id IS NULL OR char_length(paypal_txn_id) BETWEEN 1 AND 64);

CREATE INDEX IF NOT EXISTS orders_paypal_txn_id_idx
    ON orders (paypal_txn_id) WHERE paypal_txn_id IS NOT NULL;

-- Gateway-verified payments already carry the IPN `txn_id` in
-- `fiat_transaction_ref`, unless the buyer reported a reference after the
-- receipt was issued (a report overwrites the stored value).
UPDATE orders o
   SET paypal_txn_id = o.fiat_transaction_ref
  FROM receipts r
 WHERE r.id = o.receipt_id
   AND o.paypal_txn_id IS NULL
   AND o.payment_method = 'paypal'
   AND o.fiat_verified_by = 'gateway'
   AND o.fiat_transaction_ref IS NOT NULL
   AND char_length(o.fiat_transaction_ref) BETWEEN 1 AND 64
   AND (o.payment_reported_at IS NULL OR o.payment_reported_at <= r.issued_at);

-- One row per PayPal refund or reversal transaction: the primary key makes
-- a repeated IPN a no-op.
CREATE TABLE IF NOT EXISTS order_gateway_refunds (
    refund_txn_id TEXT PRIMARY KEY
        CHECK (char_length(refund_txn_id) BETWEEN 1 AND 64),
    order_id UUID NOT NULL REFERENCES orders (id),
    parent_txn_id TEXT NOT NULL
        CHECK (char_length(parent_txn_id) BETWEEN 1 AND 64),
    payment_status TEXT NOT NULL
        CHECK (payment_status IN ('Refunded', 'Reversed')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS order_gateway_refunds_order_idx
    ON order_gateway_refunds (order_id);
