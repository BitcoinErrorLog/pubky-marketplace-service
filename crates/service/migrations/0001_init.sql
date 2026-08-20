-- Marketplace Transaction Service schema (ADR-0019 §4).
-- Every accepted command atomically persists aggregate state, one immutable
-- domain event, the idempotency result, and complete outbox intents.

-- === Listings (inventory authority for one seller listing) ==================

CREATE TABLE listings (
    aggregate_id TEXT PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    title TEXT NOT NULL,
    listing_revision BIGINT NOT NULL CHECK (listing_revision > 0),
    content_hash TEXT NOT NULL,
    server_revision BIGINT NOT NULL CHECK (server_revision > 0),
    state TEXT NOT NULL CHECK (state IN ('available', 'reserved', 'sold')),
    total_quantity BIGINT NOT NULL CHECK (total_quantity >= 0),
    available_quantity BIGINT NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity BIGINT NOT NULL CHECK (reserved_quantity >= 0),
    sold_quantity BIGINT NOT NULL CHECK (sold_quantity >= 0),
    unit_price_amount_minor BIGINT NOT NULL CHECK (unit_price_amount_minor > 0),
    unit_price_currency TEXT NOT NULL,
    unit_price_exponent INTEGER NOT NULL CHECK (unit_price_exponent BETWEEN 0 AND 18),
    sale_format TEXT NOT NULL CHECK (sale_format IN ('fixed_price', 'auction')),
    auction JSONB,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT listings_quantity_balance
        CHECK (available_quantity + reserved_quantity + sold_quantity = total_quantity),
    CONSTRAINT listings_seller_listing_unique UNIQUE (seller_pubky, listing_id)
);

-- === Reservations ============================================================

CREATE TABLE reservations (
    id UUID PRIMARY KEY,
    listing_aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    buyer_pubky TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'converted', 'released', 'expired')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX reservations_active_expiry_idx ON reservations (expires_at) WHERE status = 'active';

-- === Offers ==================================================================

CREATE TABLE offers (
    id UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL UNIQUE,
    listing_aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    buyer_pubky TEXT NOT NULL,
    seller_pubky TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'countered', 'accepted', 'rejected', 'withdrawn', 'expired')),
    offered_by TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    message TEXT NOT NULL,
    history JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX offers_listing_idx ON offers (listing_aggregate_id);

-- === Auction bids ============================================================

CREATE TABLE bids (
    id UUID PRIMARY KEY,
    listing_aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    bidder_pubky TEXT NOT NULL,
    maximum_amount_minor BIGINT NOT NULL CHECK (maximum_amount_minor > 0),
    currency TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT bids_listing_sequence_unique UNIQUE (listing_aggregate_id, sequence)
);

-- === Orders ==================================================================

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    checkout_command_id UUID,
    auction_aggregate_id TEXT,
    buyer_pubky TEXT NOT NULL,
    seller_pubky TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    state TEXT NOT NULL CHECK (
        state IN (
            'pending_payment', 'paid', 'processing', 'shipped', 'delivered', 'completed',
            'cancel_requested', 'cancelled', 'return_requested', 'return_approved',
            'return_received', 'disputed', 'refunded_external', 'closed'
        )
    ),
    lines JSONB NOT NULL,
    delivery_address JSONB NOT NULL,
    subtotal_minor BIGINT NOT NULL CHECK (subtotal_minor >= 0),
    shipping_minor BIGINT NOT NULL CHECK (shipping_minor >= 0),
    tax_minor BIGINT NOT NULL CHECK (tax_minor >= 0),
    total_minor BIGINT NOT NULL CHECK (total_minor >= 0),
    currency TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    guarantee_policy_version INTEGER NOT NULL,
    payment_id UUID NOT NULL,
    receipt_id UUID,
    cancellation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT orders_total_balance
        CHECK (total_minor = subtotal_minor + shipping_minor + tax_minor)
);

-- One winning order per auction.
CREATE UNIQUE INDEX orders_one_winner_per_auction
    ON orders (auction_aggregate_id)
    WHERE auction_aggregate_id IS NOT NULL;

-- A checkout command creates at most one order per seller group, ever.
CREATE UNIQUE INDEX orders_one_per_checkout_seller
    ON orders (checkout_command_id, seller_pubky)
    WHERE checkout_command_id IS NOT NULL;

-- === Payments ================================================================

CREATE TABLE payments (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders (id),
    buyer_pubky TEXT NOT NULL,
    seller_pubky TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    adapter TEXT NOT NULL CHECK (adapter = 'sandbox'),
    state TEXT NOT NULL
        CHECK (state IN ('awaiting_entitlement', 'detected', 'confirmed', 'expired', 'manual_review')),
    confirmations INTEGER NOT NULL CHECK (confirmations >= 0),
    locks_bundle_id UUID NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- === Append-only event log ===================================================

CREATE TABLE events (
    id UUID PRIMARY KEY,
    sequence BIGINT GENERATED ALWAYS AS IDENTITY,
    command_id UUID NOT NULL,
    aggregate_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    actor_pubky TEXT NOT NULL,
    kind TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    -- One current revision per aggregate: at most one event may claim a
    -- given aggregate revision.
    CONSTRAINT events_one_per_aggregate_revision UNIQUE (aggregate_id, revision)
);

-- One payment-confirmed event per payment aggregate.
CREATE UNIQUE INDEX events_one_payment_confirmed
    ON events (aggregate_id)
    WHERE kind = 'payment.confirmed';

CREATE FUNCTION forbid_event_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

-- === Idempotency results =====================================================

-- One accepted result per actor + command id (ADR-0019 §3).
CREATE TABLE command_results (
    actor_pubky TEXT NOT NULL,
    command_id UUID NOT NULL,
    request_hash TEXT NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (actor_pubky, command_id)
);

-- === Outbox ==================================================================

CREATE TABLE outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events (id),
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ
);

CREATE INDEX outbox_undelivered_idx ON outbox (created_at) WHERE delivered_at IS NULL;

-- === Pubky authentication ====================================================

CREATE TABLE auth_challenges (
    id UUID PRIMARY KEY,
    pubky TEXT NOT NULL,
    nonce BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ
);

CREATE INDEX auth_challenges_expiry_idx ON auth_challenges (expires_at);

CREATE TABLE auth_sessions (
    token_hash BYTEA PRIMARY KEY,
    pubky TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX auth_sessions_expiry_idx ON auth_sessions (expires_at);
