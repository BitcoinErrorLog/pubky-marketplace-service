-- Paykit attempts an order has released: additive, idempotent, directly
-- rerunnable. Numbered after 0040/0041 (digital delivery), which it does
-- not depend on.
--
-- An order carries one set of Paykit pins. When an attempt is released —
-- the activation worker voids the bind, a buyer cancels a preparing
-- request, or a preparing order's hold expires — the order stops polling
-- it, and a re-bind (or a fiat bind) later overwrites those pins.
-- paykit-server keeps reporting status for the released invoice by
-- `(creator, reference)`, and a released invoice can still be payable (an
-- activation that committed at paykit whose response was lost). Each
-- released attempt keeps its pins and its bind-time quote here and is
-- polled with backoff:
--
--   watching       no terminal answer yet
--   closed_unpaid  no money by the end of the observation tail
--   late_money     confirmed money routed to the late-money path
--   needs_review   money the order can no longer take, or a detection that
--                  never confirmed; an operator records the outcome
--   resolved       an operator recorded a refund, or dismissed a detection
--                  that never confirmed
--
-- `reference` is NULL only for attempts released before per-attempt
-- references whose column a fiat bind later cleared: those used the
-- order-id reference, which the poller derives.
CREATE TABLE IF NOT EXISTS paykit_superseded_attempts (
    order_id UUID NOT NULL REFERENCES orders (id),
    invoice_id UUID NOT NULL,
    reference TEXT CHECK (reference IS NULL OR char_length(reference) = 26),
    stack_id TEXT NOT NULL,
    stack_endpoint TEXT NOT NULL,
    total_sats BIGINT NOT NULL CHECK (total_sats > 0),
    expires_at TIMESTAMPTZ,
    prepare_expires_at TIMESTAMPTZ,
    allocation_mode TEXT,
    address_fingerprint TEXT,
    bitcoin_quote_rate NUMERIC,
    bitcoin_quote_source TEXT,
    bitcoin_quote_fetched_at TIMESTAMPTZ,
    bitcoin_quoted_sats BIGINT,
    bitcoin_quote_expires_at TIMESTAMPTZ,
    bitcoin_quote_currency CHAR(3),
    bitcoin_quote_exponent SMALLINT,
    bitcoin_quote_spread_bps INTEGER,
    released_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'watching'
        CHECK (state IN ('watching', 'closed_unpaid', 'late_money', 'needs_review', 'resolved')),
    detected_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ,
    check_count INTEGER NOT NULL DEFAULT 0 CHECK (check_count >= 0),
    closed_at TIMESTAMPTZ,
    observation JSONB,
    review_reason TEXT
        CHECK (review_reason IN ('payment_settled', 'other_rail', 'detected_unconfirmed')),
    resolution_outcome TEXT CHECK (resolution_outcome IN ('refunded', 'dismissed')),
    resolution_note TEXT CHECK (char_length(resolution_note) BETWEEN 1 AND 500),
    resolved_by TEXT CHECK (char_length(resolved_by) BETWEEN 1 AND 128),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (order_id, invoice_id),
    CHECK ((state = 'watching') = (closed_at IS NULL)),
    CHECK ((state = 'needs_review' OR state = 'resolved') = (review_reason IS NOT NULL)),
    CHECK (
        (state = 'resolved')
        = (resolution_outcome IS NOT NULL AND resolution_note IS NOT NULL
           AND resolved_by IS NOT NULL AND resolved_at IS NOT NULL)
    ),
    -- Confirmed money closes only as a refund; a dismissal is for a
    -- detection that never confirmed.
    CHECK (resolution_outcome IS DISTINCT FROM 'dismissed'
           OR review_reason = 'detected_unconfirmed')
);

CREATE INDEX IF NOT EXISTS paykit_superseded_attempts_due
    ON paykit_superseded_attempts (next_check_at NULLS FIRST)
    WHERE state = 'watching';
CREATE INDEX IF NOT EXISTS paykit_superseded_attempts_needs_review
    ON paykit_superseded_attempts (closed_at)
    WHERE state = 'needs_review';

-- Attempts already released in production: activation voided, no live
-- request state, and complete pins. The payment method label plays no
-- part: a buyer cancel of a preparing request keeps `bitcoin`.
INSERT INTO paykit_superseded_attempts (
    order_id, invoice_id, reference, stack_id, stack_endpoint, total_sats,
    expires_at, prepare_expires_at, allocation_mode, address_fingerprint,
    bitcoin_quote_rate, bitcoin_quote_source, bitcoin_quote_fetched_at,
    bitcoin_quoted_sats, bitcoin_quote_expires_at, bitcoin_quote_currency,
    bitcoin_quote_exponent, bitcoin_quote_spread_bps, released_at
)
SELECT id, paykit_invoice_id, paykit_request_reference, paykit_stack_id,
       paykit_stack_endpoint, paykit_total_sats, paykit_expires_at,
       paykit_prepare_expires_at, paykit_allocation_mode,
       paykit_address_fingerprint, bitcoin_quote_rate, bitcoin_quote_source,
       bitcoin_quote_fetched_at, bitcoin_quoted_sats, bitcoin_quote_expires_at,
       bitcoin_quote_currency, bitcoin_quote_exponent, bitcoin_quote_spread_bps,
       updated_at
FROM orders
WHERE paykit_activation_state = 'voided'
  AND paykit_request_state IS NULL
  AND paykit_invoice_id IS NOT NULL
  AND paykit_stack_id IS NOT NULL
  AND paykit_stack_endpoint IS NOT NULL
  AND paykit_total_sats > 0
  AND (paykit_request_reference IS NULL OR char_length(paykit_request_reference) = 26)
ON CONFLICT (order_id, invoice_id) DO NOTHING;
