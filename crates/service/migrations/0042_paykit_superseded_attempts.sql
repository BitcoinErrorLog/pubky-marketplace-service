-- Paykit attempts an order has released: additive, idempotent, directly
-- rerunnable. Numbered after 0040/0041 (digital delivery), which it does
-- not depend on.
--
-- A bitcoin bind that `void_prepare_effects` releases leaves the order
-- unbound, and a re-bind overwrites the order's single set of Paykit pins
-- with the next attempt. paykit-server keeps reporting status for the
-- released invoice by `(creator, reference)`, and a released invoice can
-- still be payable (an activation that committed at paykit whose response
-- was lost). Each released attempt keeps its pins here and is polled
-- through its observation tail: `watching` until the tail closes with no
-- money (`closed_unpaid`), confirmed money routes to the late-money path
-- (`late_money`), and money the order can no longer take is held for a
-- human (`needs_review`).
--
-- `reference` is NULL only for attempts released before per-attempt
-- references: those used the order-id reference, which the poller derives.
CREATE TABLE IF NOT EXISTS paykit_superseded_attempts (
    order_id UUID NOT NULL REFERENCES orders (id),
    invoice_id UUID NOT NULL,
    reference TEXT CHECK (reference IS NULL OR char_length(reference) = 26),
    stack_id TEXT NOT NULL,
    stack_endpoint TEXT NOT NULL,
    total_sats BIGINT NOT NULL CHECK (total_sats > 0),
    expires_at TIMESTAMPTZ,
    allocation_mode TEXT,
    address_fingerprint TEXT,
    released_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'watching'
        CHECK (state IN ('watching', 'closed_unpaid', 'late_money', 'needs_review')),
    detected_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    observation JSONB,
    PRIMARY KEY (order_id, invoice_id),
    CHECK ((state = 'watching') = (closed_at IS NULL))
);

CREATE INDEX IF NOT EXISTS paykit_superseded_attempts_watching
    ON paykit_superseded_attempts (last_checked_at NULLS FIRST)
    WHERE state = 'watching';

-- Attempts already released in production: the order is no longer bound
-- to bitcoin (the method was cleared, or a fiat method replaced it) and
-- still carries the released attempt's pins.
INSERT INTO paykit_superseded_attempts (
    order_id, invoice_id, reference, stack_id, stack_endpoint, total_sats,
    expires_at, allocation_mode, address_fingerprint, released_at
)
SELECT id, paykit_invoice_id, paykit_request_reference, paykit_stack_id,
       paykit_stack_endpoint, paykit_total_sats, paykit_expires_at,
       paykit_allocation_mode, paykit_address_fingerprint, updated_at
FROM orders
WHERE paykit_activation_state = 'voided'
  AND payment_method IS DISTINCT FROM 'bitcoin'
  AND paykit_invoice_id IS NOT NULL
  AND paykit_stack_id IS NOT NULL
  AND paykit_stack_endpoint IS NOT NULL
  AND paykit_total_sats > 0
  AND (paykit_request_reference IS NULL OR char_length(paykit_request_reference) = 26)
ON CONFLICT (order_id, invoice_id) DO NOTHING;
