-- Two-phase paykit payment requests (design §B.11.2/§B.11.3): phase 1
-- (`POST /v0/payment-requests`) prepares an invoice and returns
-- `{invoice_id, stack_id, total_sats, ...}`; the activation outbox row
-- (same transaction as the bind) drives phase 2. Until activation the
-- order is `preparing`: not polled, not payable.
--
-- `paykit_stack_id` / `paykit_stack_endpoint` pin the issuing stack per
-- order at bind time (the endpoint is the base URL the phase-1 call used,
-- never configuration read later), so activate/void/resolve route back to
-- the stack that issued the invoice even after a repoint.
-- `paykit_total_sats` (amount + paykit-minted nonce) is the figure the
-- marketplace charges, displays and records. No Bitcoin address is ever
-- persisted (§B.0): only the derived-address fingerprint.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS paykit_invoice_id UUID,
    ADD COLUMN IF NOT EXISTS paykit_stack_id TEXT,
    ADD COLUMN IF NOT EXISTS paykit_stack_endpoint TEXT,
    ADD COLUMN IF NOT EXISTS paykit_total_sats BIGINT,
    ADD COLUMN IF NOT EXISTS paykit_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paykit_prepare_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paykit_allocation_mode TEXT,
    ADD COLUMN IF NOT EXISTS paykit_address_fingerprint TEXT,
    -- Per-order phase-1 attempt counter: the idempotency key is
    -- `{order_reference}:{bind_attempt}`, so a re-bind after a void gets a
    -- fresh key while a transport retry replays.
    ADD COLUMN IF NOT EXISTS paykit_bind_attempt INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS paykit_activation_state TEXT
        CHECK (paykit_activation_state IN ('preparing', 'active', 'voided'));

-- `preparing` joins the request-state vocabulary as the initial value.
-- The `orders_paykit_pending` partial index predicate and the poll query
-- are deliberately UNCHANGED: a `preparing` order is not claimed by
-- `claim_due_paykit_orders` until activation flips it to `pending`.
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paykit_request_state_check;
ALTER TABLE orders ADD CONSTRAINT orders_paykit_request_state_check
    CHECK (paykit_request_state IN ('preparing', 'pending', 'detected', 'confirmed'));
