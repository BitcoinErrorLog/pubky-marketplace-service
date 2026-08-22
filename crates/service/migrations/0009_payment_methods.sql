-- Seller-configurable payment methods and per-order method binding.
--
-- Sellers publish which rails they accept (Bitcoin via their claimed Paykit
-- watch-only account, Stripe Payment Links, PayPal merchant email). Buyers
-- bind exactly one method to a pending order; fiat verification is either
-- processor-verified (Stripe, via the seller's stored restricted key) or
-- seller-attested (PayPal two-step: buyer reports, seller confirms).

CREATE TABLE seller_payment_configs (
    seller_pubky TEXT PRIMARY KEY,
    bitcoin_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    stripe_payment_link TEXT,
    -- The Stripe restricted key is a bearer credential: sealed with
    -- XChaCha20-Poly1305 under STRIPE_KEY_ENCRYPTION_KEY with the seller
    -- pubky as associated data. It is write-only at the API surface.
    stripe_restricted_key_ciphertext BYTEA,
    paypal_merchant_email TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Bound methods close the sandbox path with their own adapters.
ALTER TABLE payments DROP CONSTRAINT payments_adapter_check;
ALTER TABLE payments ADD CONSTRAINT payments_adapter_check
    CHECK (adapter IN ('sandbox', 'locks', 'paykit', 'stripe', 'paypal'));

ALTER TABLE orders
    ADD COLUMN payment_method TEXT
        CHECK (payment_method IN ('bitcoin', 'stripe', 'paypal')),
    -- Snapshot of the checkout URL at binding time, so the buyer's link for
    -- an in-flight order is stable even if the seller edits their config.
    ADD COLUMN fiat_checkout_url TEXT,
    ADD COLUMN payment_reported_at TIMESTAMPTZ,
    ADD COLUMN fiat_transaction_ref TEXT,
    -- Paykit payment-request correlation for physical bitcoin orders: the
    -- creator-scoped reference (Crockford base32 of the order UUID) doubles
    -- as the paykit-server status-lookup bundle identifier.
    ADD COLUMN paykit_request_reference TEXT,
    ADD COLUMN paykit_request_state TEXT
        CHECK (paykit_request_state IN ('pending', 'detected', 'confirmed')),
    ADD COLUMN paykit_last_checked_at TIMESTAMPTZ;

-- The paykit verification worker claims due bitcoin orders by this partial
-- predicate; the stamp update itself is the claim.
CREATE INDEX orders_paykit_pending ON orders (paykit_last_checked_at)
    WHERE paykit_request_state IN ('pending', 'detected');
