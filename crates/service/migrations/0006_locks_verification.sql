-- Server-side Locks verification (plan task 4.5, ADR-0019 §7/§8).
--
-- The service stores an encrypted correlation between an order/payment and a
-- Locks verification lifecycle, then advances the payment only after
-- independently verifying a completed Locks result. The bundle id is a
-- bearer secret: it is encrypted at rest (XChaCha20-Poly1305, bound to the
-- payment id) and queried only through an HMAC lookup token, never stored or
-- indexed in plaintext.

-- === Payments =================================================================

-- The plaintext sandbox placeholder is removed: real bundle ids live only in
-- the encrypted correlation record, and nothing consumed the sandbox value.
ALTER TABLE payments DROP COLUMN locks_bundle_id;

-- Payments correlated to a Locks lifecycle use the 'locks' adapter; those
-- payments are advanced exclusively by server-side verification, never by
-- the sandbox command.
ALTER TABLE payments DROP CONSTRAINT payments_adapter_check;
ALTER TABLE payments ADD CONSTRAINT payments_adapter_check
    CHECK (adapter IN ('sandbox', 'locks'));

-- === Locks correlations =======================================================
-- One correlation per payment and per order (a changed re-registration is
-- rejected), binding order, buyer, creator (the seller), lock resource hash,
-- amount, asset, and policy version to the encrypted lifecycle identity
-- (upstream-integration "Transaction-service correlation").

CREATE TABLE payment_locks_correlations (
    id UUID PRIMARY KEY,
    payment_id UUID NOT NULL UNIQUE REFERENCES payments (id),
    order_id UUID NOT NULL UNIQUE REFERENCES orders (id),
    buyer_pubky TEXT NOT NULL,
    -- The Locks creator (the content-lock owner), required to equal the
    -- order's seller at registration; half of the lifecycle identity
    -- { creator, bundle_id }.
    creator_pubky TEXT NOT NULL,
    -- BLAKE3 hex of the registered pubky lock resource string; the raw
    -- resource is never stored or exposed.
    lock_resource_hash TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    asset TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    policy_version INTEGER NOT NULL,
    -- 24-byte XChaCha20-Poly1305 nonce followed by the ciphertext of the
    -- bundle id, with the payment id as associated data.
    bundle_id_ciphertext BYTEA NOT NULL,
    -- HMAC-SHA256(lookup key, creator || bundle id). Unique: one bundle id
    -- can never correlate two orders under the same creator.
    bundle_lookup_token BYTEA NOT NULL UNIQUE,
    -- The upstream fact as last verified against the Lock Server. 'pending'
    -- rows are polled; terminal upstream states stop polling, so the loop
    -- is bounded by the Lock Server's own task ageing.
    verification_state TEXT NOT NULL CHECK (
        verification_state IN ('pending', 'completed', 'upstream_failed', 'upstream_expired')
    ),
    -- Marketplace payment window (server time). Elapsing moves the payment
    -- to 'expired' without treating it as an upstream failure; a completion
    -- verified afterwards goes to 'manual_review' and is retained.
    window_expires_at TIMESTAMPTZ NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_observed_status TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX payment_locks_correlations_pending_idx
    ON payment_locks_correlations (last_checked_at NULLS FIRST)
    WHERE verification_state = 'pending';

-- === Reconciliation history ===================================================
-- Append-only record of every observed upstream status change and every
-- marketplace action taken on it (payment_confirmed, payment_expired,
-- manual_review), so late completions and reconciliation are never silently
-- discarded. Statuses only — never the bundle id or lock resource.

CREATE TABLE payment_locks_observations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    correlation_id UUID NOT NULL REFERENCES payment_locks_correlations (id),
    observed_status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER payment_locks_observations_append_only
    BEFORE UPDATE OR DELETE ON payment_locks_observations
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();
