-- `shared_manual` checkout, seller confirmation, and Paykit manual-review
-- resolution (design §B.8.8/§B.9 r13).
--
-- A non-late matching Paykit observation on an order whose persisted
-- `paykit_allocation_mode = 'shared_manual'` enters
-- `orders.paykit_request_state = 'awaiting_seller_confirmation'`: the hold
-- is extended to a bounded 24-hour seller-confirmation window with the
-- server-clock entry/deadline recorded, and status polling keeps refreshing
-- facts without ever auto-paying. At the window the reaper CASes the order
-- to `confirmed` and the payment to `manual_review` (hold PRESERVED),
-- starting the two-business-day seller-response SLA and the seven-day
-- inactivity clock. The seller resolves `manual_review` as paid / refunded
-- / abandoned through one CAS shared with the seven-day inactivity reaper;
-- every outcome writes one audit row, one event, and one stack-AND-endpoint
-- pinned `paykit.resolve` outbox row. Late settlement never enters
-- `awaiting_seller_confirmation`; it takes the existing late path straight
-- to `manual_review`.

-- === Orders: the seller-confirmation window and the live observation ======

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paykit_request_state_check;
ALTER TABLE orders ADD CONSTRAINT orders_paykit_request_state_check
    CHECK (paykit_request_state IN
        ('preparing', 'pending', 'detected', 'confirmed', 'awaiting_seller_confirmation'));

ALTER TABLE orders
    -- The latest Paykit observation as the status-only path refreshed it:
    -- {state, observed_sats, confirmations, amount_matched, txid,
    --  observed_at, disappeared}. Facts only; never a transition input.
    ADD COLUMN IF NOT EXISTS paykit_observation JSONB,
    -- Server-clock entry into `awaiting_seller_confirmation` and the
    -- 24-hour seller-confirmation deadline armed at entry. Both clear when
    -- the order leaves the state, so the biconditional CHECK below holds.
    ADD COLUMN IF NOT EXISTS paykit_seller_confirmation_entered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paykit_seller_confirmation_deadline TIMESTAMPTZ;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_seller_confirmation_window_check;
ALTER TABLE orders ADD CONSTRAINT orders_seller_confirmation_window_check
    CHECK (
        (paykit_request_state = 'awaiting_seller_confirmation')
        = (paykit_seller_confirmation_entered_at IS NOT NULL
           AND paykit_seller_confirmation_deadline IS NOT NULL)
    );

-- The seller-window reaper scans due confirmations by this predicate.
CREATE INDEX IF NOT EXISTS orders_seller_confirmation_due
    ON orders (paykit_seller_confirmation_deadline)
    WHERE paykit_request_state = 'awaiting_seller_confirmation';

-- §C.16 condition 7 (drain retention) scans orders pinned to a stack.
CREATE INDEX IF NOT EXISTS orders_paykit_stack_idx
    ON orders (paykit_stack_id)
    WHERE paykit_stack_id IS NOT NULL;

-- The status-only poll also claims orders awaiting seller confirmation
-- (facts refresh; the state never advances from a poll). `preparing`
-- stays excluded.
DROP INDEX IF EXISTS orders_paykit_pending;
CREATE INDEX orders_paykit_pending ON orders (paykit_last_checked_at)
    WHERE paykit_request_state IN ('pending', 'detected', 'awaiting_seller_confirmation');

-- === Payments: common manual-review entry and the resolution record =======

ALTER TABLE payments
    -- Stamped by EVERY transition into `manual_review` (Bitcoin, Locks and
    -- fiat writers alike); the entry/SLA/inactivity clocks all read it.
    -- Cleared when the payment leaves `manual_review` so the CHECK holds.
    ADD COLUMN IF NOT EXISTS manual_review_entered_at TIMESTAMPTZ,
    -- The two-business-day seller-response SLA alert fires once per entry.
    ADD COLUMN IF NOT EXISTS manual_review_sla_alerted_at TIMESTAMPTZ,
    -- The resolution record (all NULL until resolved): the Idempotency-Key
    -- (seller) or the minted key (inactivity reaper), the outcome, who and
    -- on what basis, and the validated external refund reference.
    ADD COLUMN IF NOT EXISTS resolution_id UUID,
    ADD COLUMN IF NOT EXISTS resolution_outcome TEXT
        CHECK (resolution_outcome IN ('paid', 'refunded', 'abandoned')),
    ADD COLUMN IF NOT EXISTS resolution_basis TEXT
        CHECK (resolution_basis IN ('seller_attestation', 'seller_unresponsive')),
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolved_by_pubky TEXT,
    ADD COLUMN IF NOT EXISTS refund_reference TEXT;

-- Pre-existing `manual_review` rows are backfilled with NOW(), documented
-- as an approximate entered-at instant (design §B.9 r12).
UPDATE payments SET manual_review_entered_at = NOW()
    WHERE state = 'manual_review' AND manual_review_entered_at IS NULL;

ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_manual_review_entry_check;
ALTER TABLE payments ADD CONSTRAINT payments_manual_review_entry_check
    CHECK ((state = 'manual_review') = (manual_review_entered_at IS NOT NULL));

ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_resolution_check;
ALTER TABLE payments ADD CONSTRAINT payments_resolution_check
    CHECK (
        (
            resolution_outcome IS NULL AND resolution_basis IS NULL
            AND resolved_at IS NULL AND resolution_id IS NULL
            AND resolved_by_pubky IS NULL AND refund_reference IS NULL
        ) OR (
            -- A resolved payment has left `manual_review` by definition:
            -- the resolution CAS sets the exit state and these fields in
            -- one UPDATE.
            state <> 'manual_review'
            AND resolution_outcome IS NOT NULL AND resolution_basis IS NOT NULL
            AND resolved_at IS NOT NULL AND resolution_id IS NOT NULL
            AND ((resolution_basis = 'seller_attestation')
                 = (resolved_by_pubky IS NOT NULL))
            AND ((resolution_outcome = 'refunded') = (refund_reference IS NOT NULL))
        )
    );

-- The inactivity reaper and SLA scan read unresolved reviews by entry time.
CREATE INDEX IF NOT EXISTS payments_manual_review_due
    ON payments (manual_review_entered_at)
    WHERE state = 'manual_review';

-- === Seller confirmations (audit + idempotency-on-order-id) ===============

-- The refunded-resolution branch records what the buyer ACTUALLY paid,
-- which for a bitcoin-bound order is the paykit total (listing total +
-- paykit-minted nonce, §B.11.3). The 0004 CHECK caps a refund at
-- `total_minor`; for paykit orders the honest cap is the paykit total.
-- Non-paykit orders are unaffected (COALESCE keeps the old bound).
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_external_refund_within_total;
ALTER TABLE orders ADD CONSTRAINT orders_external_refund_within_total CHECK (
    external_refund IS NULL
    OR (external_refund ->> 'amount_minor')::bigint
        BETWEEN 1 AND GREATEST(total_minor, COALESCE(paykit_total_sats, total_minor))
);

-- One row per order, ever: the confirm endpoint's idempotency lookup reads
-- it AFTER authorisation and BEFORE the state check, and it is written in
-- the same transaction as the conditional UPDATE it records.
CREATE TABLE IF NOT EXISTS paykit_seller_confirmations (
    order_id UUID PRIMARY KEY REFERENCES orders (id),
    payment_id UUID NOT NULL REFERENCES payments (id),
    -- The authenticated session pubky resolved order -> listing -> seller;
    -- never body-supplied.
    confirmed_by_pubky TEXT NOT NULL,
    confirmed_at TIMESTAMPTZ NOT NULL,
    -- Derived from the stored Paykit observation, never the request body.
    confirmed_txid TEXT,
    confirmed_amount_sats BIGINT,
    confirmed_reason TEXT,
    confirmation_source TEXT NOT NULL
        CHECK (confirmation_source = 'seller'),
    -- A constant, not a computation: the seller asserted this (residual R5
    -- stays attributable).
    confirmation_basis TEXT NOT NULL
        CHECK (confirmation_basis = 'seller_attestation'),
    -- The exact observation the seller acted on, frozen at confirmation.
    paykit_observation JSONB NOT NULL,
    event_id UUID NOT NULL REFERENCES events (id),
    created_at TIMESTAMPTZ NOT NULL
);

-- === Manual-review resolutions (audit + seller/reaper idempotency) ========

-- One row per order, ever. `resolution_id` is the caller's Idempotency-Key
-- (a UUID); the inactivity reaper mints one per abandoned order. The
-- (order_id, resolution_id) uniqueness replays a same-key duplicate; the
-- order-id primary key makes a different key after resolution a conflict.
CREATE TABLE IF NOT EXISTS paykit_manual_resolutions (
    order_id UUID PRIMARY KEY REFERENCES orders (id),
    payment_id UUID NOT NULL REFERENCES payments (id),
    resolution_id UUID NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('paid', 'refunded', 'abandoned')),
    basis TEXT NOT NULL
        CHECK (basis IN ('seller_attestation', 'seller_unresponsive')),
    resolved_at TIMESTAMPTZ NOT NULL,
    -- NULL exactly when the basis is `seller_unresponsive`.
    resolved_by_pubky TEXT,
    reason TEXT,
    -- Validated external refund reference; non-NULL iff outcome='refunded'.
    refund_reference TEXT,
    -- The immutable observed-payment snapshot the refund amount derives
    -- from, never the request body.
    observed_payment_snapshot JSONB,
    -- The canonical request hash: the same key with a different body is a
    -- 409 conflict; the stored response replays the winner's result.
    request_hash TEXT NOT NULL,
    response JSONB NOT NULL,
    event_id UUID NOT NULL REFERENCES events (id),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT paykit_manual_resolutions_resolution_key
        UNIQUE (order_id, resolution_id),
    CONSTRAINT paykit_manual_resolutions_shape CHECK (
        ((basis = 'seller_unresponsive') = (resolved_by_pubky IS NULL))
        AND ((outcome = 'refunded') = (refund_reference IS NOT NULL))
    )
);

-- === The stack-and-endpoint pinned paykit.resolve outbox ==================

-- Written in the same transaction as the confirmation/resolution it
-- records; exactly one per order. The delivery arm dials the ROW'S pinned
-- endpoint (never the current default), compares the row's `stack_id`
-- against the pinned endpoint's `/health/ready` before sending, and maps
-- every response class to exactly one outcome: permanent refusals
-- terminate visibly, 401/403 retry with an immediate first alert,
-- 408/425/429/5xx and transport failures retry under the hard one-hour
-- `delivery_deadline` (429/503 honour Retry-After as a floor, never an
-- undercut of the normal backoff), and anything unmapped terminates
-- `unmapped_resolve_error` with the status/code recorded.
CREATE TABLE IF NOT EXISTS paykit_resolve_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders (id),
    payment_id UUID NOT NULL REFERENCES payments (id),
    event_id UUID NOT NULL REFERENCES events (id),
    invoice_id UUID NOT NULL,
    resolution TEXT NOT NULL
        CHECK (resolution IN ('paid_manually', 'refunded', 'abandoned')),
    resolved_at TIMESTAMPTZ NOT NULL,
    -- Both pins are frozen at bind (identity AND address): the delivery arm
    -- refuses any other stack and any other endpoint.
    stack_id TEXT NOT NULL,
    stack_endpoint TEXT NOT NULL,
    delivery_state TEXT NOT NULL DEFAULT 'queued'
        CHECK (delivery_state IN ('queued', 'delivered', 'terminal_unresolved')),
    terminal_reason TEXT,
    -- The status and application code recorded for `unmapped_resolve_error`.
    terminal_detail TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    -- The backoff/Retry-After floor: the row is claimable only at or after
    -- this instant (and never past the deadline).
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    -- 401/403 alerts fire on the FIRST occurrence only.
    auth_alerted BOOLEAN NOT NULL DEFAULT FALSE,
    -- Hard one-hour delivery bound from row creation.
    delivery_deadline TIMESTAMPTZ NOT NULL,
    -- The infrastructure acknowledgement: deliberate, recorded, and the
    -- only way a terminal row stops blocking the §C.16 condition-6 drain.
    -- Terminates DELIVERY only; it never touches the order outcome.
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    acknowledgement_note TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT paykit_resolve_outbox_delivery_check CHECK (
        (delivery_state = 'delivered') = (delivered_at IS NOT NULL)
    ),
    CONSTRAINT paykit_resolve_outbox_terminal_check CHECK (
        (delivery_state = 'terminal_unresolved') = (terminal_reason IS NOT NULL)
    ),
    CONSTRAINT paykit_resolve_outbox_ack_check CHECK (
        (acknowledged_at IS NULL) = (acknowledged_by IS NULL)
    )
);

-- The delivery arm claims due queued rows by their scheduled attempt.
CREATE INDEX IF NOT EXISTS paykit_resolve_outbox_claim
    ON paykit_resolve_outbox (next_attempt_at)
    WHERE delivery_state = 'queued';

-- §C.16 condition 6 scans a stack's unfinished rows.
CREATE INDEX IF NOT EXISTS paykit_resolve_outbox_stack
    ON paykit_resolve_outbox (stack_id)
    WHERE delivery_state != 'delivered';
