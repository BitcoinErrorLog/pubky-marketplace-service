-- Automatic PayPal refund detection (docs/paypal-refund-ipn.md): additive,
-- idempotent, directly rerunnable. Production preflight (2026-09-24):
-- `orders` holds 69 rows (256 kB), so the index builds below hold their
-- table lock for milliseconds.
--
-- `paypal_txn_id` is the original payment's PayPal `txn_id`, written only
-- from a postback-verified `Completed` IPN that passed the receiver,
-- currency, amount, and order checks, together with a snapshot of the
-- receiver that payment was verified against. Refund IPNs match their
-- `parent_txn_id` against it, never against the buyer-writable
-- `fiat_transaction_ref`, and their receiver against the snapshot, never
-- against the seller's current configuration. A backfilled payment id has
-- no observed receiver (both snapshot columns NULL); its refunds are held
-- for review as `receiver_unverified`.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paypal_txn_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paypal_receiver_email TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paypal_receiver_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_reversed_at TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_reversal_cancelled_at TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS gateway_refund_review_at TIMESTAMPTZ;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paypal_txn_id_check;
ALTER TABLE orders ADD CONSTRAINT orders_paypal_txn_id_check
    CHECK (
        (paypal_txn_id IS NULL AND paypal_receiver_email IS NULL AND paypal_receiver_id IS NULL)
        OR (
            paypal_txn_id IS NOT NULL
            AND char_length(paypal_txn_id) BETWEEN 1 AND 64
            AND (paypal_receiver_email IS NULL
                 OR char_length(paypal_receiver_email) BETWEEN 1 AND 254)
            AND (paypal_receiver_id IS NULL OR char_length(paypal_receiver_id) BETWEEN 1 AND 64)
            AND (paypal_receiver_id IS NULL OR paypal_receiver_email IS NOT NULL)
        )
    );

-- One verified PayPal payment pays one order.
CREATE UNIQUE INDEX IF NOT EXISTS orders_paypal_txn_id_key
    ON orders (paypal_txn_id) WHERE paypal_txn_id IS NOT NULL;

-- Gateway-verified payments already carry the IPN `txn_id` in
-- `fiat_transaction_ref`, unless a buyer report was appended to the order's
-- event log after the receipt (a report overwrites the stored value). The
-- event sequence decides, not timestamps. The receiver that payment matched
-- was never stored, and the seller's current configuration is not evidence
-- of it, so the receiver stays unresolved.
-- A reference shared by two gateway-verified orders is not backfilled.
UPDATE orders o
   SET paypal_txn_id = o.fiat_transaction_ref
 WHERE o.paypal_txn_id IS NULL
   AND o.payment_method = 'paypal'
   AND o.fiat_verified_by = 'gateway'
   AND o.fiat_transaction_ref IS NOT NULL
   AND char_length(o.fiat_transaction_ref) BETWEEN 1 AND 64
   -- With a buyer report on the order, only PayPal's 17-character
   -- transaction id shape is trusted: a buyer-typed value survives when the
   -- verifying IPN carried no usable `txn_id`.
   AND (o.payment_reported_at IS NULL OR o.fiat_transaction_ref ~ '^[A-Z0-9]{17}$')
   AND EXISTS (
       SELECT 1 FROM events r
        WHERE r.aggregate_id = 'order:' || o.id::text
          AND r.kind = 'receipt.issued'
          AND NOT EXISTS (
              SELECT 1 FROM events p
               WHERE p.aggregate_id = r.aggregate_id
                 AND p.kind = 'order.fiat_payment_reported'
                 AND p.sequence > r.sequence))
   AND NOT EXISTS (
       SELECT 1 FROM orders d
        WHERE d.id <> o.id
          AND d.fiat_transaction_ref = o.fiat_transaction_ref
          AND d.payment_method = 'paypal'
          AND d.fiat_verified_by = 'gateway')
   AND NOT EXISTS (
       SELECT 1 FROM orders d WHERE d.paypal_txn_id = o.fiat_transaction_ref);

-- One row per PayPal refund, reversal, or canceled reversal applied to an
-- order: the primary key makes a repeated IPN a no-op. `from_state` and
-- `from_return_state` record what a row's transition to `refunded_external`
-- replaced, so a canceled reversal can restore it.
CREATE TABLE IF NOT EXISTS order_gateway_refunds (
    refund_txn_id TEXT PRIMARY KEY
        CHECK (char_length(refund_txn_id) BETWEEN 1 AND 64),
    order_id UUID NOT NULL REFERENCES orders (id),
    parent_txn_id TEXT NOT NULL
        CHECK (char_length(parent_txn_id) BETWEEN 1 AND 64),
    payment_status TEXT NOT NULL
        CHECK (payment_status IN ('Refunded', 'Reversed', 'Canceled_Reversal')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    from_state TEXT,
    from_return_state TEXT,
    recorded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS order_gateway_refunds_order_idx
    ON order_gateway_refunds (order_id);

-- Verified refund-class IPNs that could not be applied to an order: no
-- order owns the parent payment yet, or the receiver, currency, or amount
-- does not validate. Nothing verified is discarded. An `unknown_parent` row
-- is applied when the parent payment's `Completed` IPN is recorded.
CREATE TABLE IF NOT EXISTS gateway_refund_inbox (
    txn_id TEXT PRIMARY KEY CHECK (char_length(txn_id) BETWEEN 1 AND 64),
    parent_txn_id TEXT,
    payment_status TEXT NOT NULL
        CHECK (payment_status IN ('Refunded', 'Reversed', 'Canceled_Reversal')),
    reason TEXT NOT NULL CHECK (reason IN (
        'missing_parent', 'unknown_parent', 'custom_mismatch',
        'receiver_mismatch', 'receiver_unverified', 'currency_mismatch',
        'amount_invalid')),
    order_id UUID REFERENCES orders (id),
    fields JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ
);

-- A canceled PayPal reversal supersedes the `refunded` annotation its
-- reversal wrote (annotations are append-only).
ALTER TABLE attestation_annotations
    DROP CONSTRAINT IF EXISTS attestation_annotations_outcome_check;
ALTER TABLE attestation_annotations
    ADD CONSTRAINT attestation_annotations_outcome_check
    CHECK (outcome IN ('refunded', 'refund_reversal_cancelled'));

CREATE INDEX IF NOT EXISTS gateway_refund_inbox_parent_idx
    ON gateway_refund_inbox (parent_txn_id) WHERE resolved_at IS NULL;
