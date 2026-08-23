-- Drops (ADR-0026): timed, limited releases. One drop aggregate per
-- seller-signed drop record, synced from the seller's homeserver. The
-- database enforces the caps: remaining/paid quantities can never leave
-- 0..=total, one buyer can never exceed the per-buyer limit, and one listing
-- can never be bound to two announced/live drops at once.

-- === Drops (release authority for one seller drop) ===========================

CREATE TABLE drops (
    aggregate_id TEXT PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    drop_id TEXT NOT NULL,
    -- The seller-signed record revision last applied (drop.sync convergence
    -- key, like listings.listing_revision).
    record_revision BIGINT NOT NULL CHECK (record_revision > 0),
    -- The server-side aggregate revision (command CAS key, like
    -- listings.server_revision).
    revision BIGINT NOT NULL CHECK (revision > 0),
    state TEXT NOT NULL CHECK (
        state IN ('announced', 'live', 'ended_sold_out', 'ended_closed', 'ended_cancelled')
    ),
    format TEXT NOT NULL CHECK (format = 'fcfs'),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ,
    total_quantity BIGINT NOT NULL CHECK (total_quantity BETWEEN 1 AND 1000000),
    per_buyer_limit BIGINT NOT NULL
        CHECK (per_buyer_limit BETWEEN 1 AND 100 AND per_buyer_limit <= total_quantity),
    remaining_quantity BIGINT NOT NULL
        CHECK (remaining_quantity >= 0 AND remaining_quantity <= total_quantity),
    paid_quantity BIGINT NOT NULL CHECK (paid_quantity >= 0 AND paid_quantity <= total_quantity),
    stock_display TEXT NOT NULL CHECK (stock_display IN ('exact', 'bands', 'hidden')),
    listing_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT drops_seller_drop_unique UNIQUE (seller_pubky, drop_id),
    CONSTRAINT drops_ends_after_starts CHECK (ends_at IS NULL OR ends_at > starts_at)
);

-- Sweep-worker scans: announced drops due to start, live drops due to end.
CREATE INDEX drops_announced_due_idx ON drops (starts_at) WHERE state = 'announced';
CREATE INDEX drops_live_due_idx ON drops (ends_at) WHERE state = 'live';

-- === Drop listing bindings ===================================================

-- `active` denormalizes "the drop is announced or live" onto the binding row
-- (maintained in the same transactions that change drop state) so the
-- one-active-drop-per-listing rule below can be a DATABASE constraint, not
-- handler discipline. Ended bindings are kept: gating keeps refusing a
-- listing whose drop ended until the seller binds it to a new drop.
CREATE TABLE drop_listings (
    drop_aggregate_id TEXT NOT NULL REFERENCES drops (aggregate_id),
    seller_pubky TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    PRIMARY KEY (drop_aggregate_id, seller_pubky, listing_id)
);

CREATE UNIQUE INDEX drop_listings_one_active_per_listing
    ON drop_listings (seller_pubky, listing_id)
    WHERE active;

CREATE INDEX drop_listings_listing_idx ON drop_listings (seller_pubky, listing_id);

-- === Per-buyer drop purchases ================================================

-- One counter row per (drop, buyer). The per-buyer limit is denormalized at
-- first hold so the CHECK below makes exceeding it impossible even if a
-- handler guard were bypassed; terms are locked once the drop is live, so
-- the denormalized limit can never go stale under an existing row.
CREATE TABLE drop_purchases (
    drop_aggregate_id TEXT NOT NULL REFERENCES drops (aggregate_id),
    buyer_pubky TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    per_buyer_limit INTEGER NOT NULL,
    PRIMARY KEY (drop_aggregate_id, buyer_pubky),
    CONSTRAINT drop_purchases_within_limit CHECK (quantity >= 0 AND quantity <= per_buyer_limit)
);

-- === Drop stamps on holds ====================================================

-- Which drop an order's units were debited from (NULL for non-drop orders).
-- Every release path credits exactly the stamped drop, so holds taken
-- outside drop gating can never over-credit a drop's counters.
ALTER TABLE orders ADD COLUMN drop_aggregate_id TEXT;

-- Same stamp for standalone reservations (a reserve without checkout holds
-- a drop unit identically): reservation expiry credits the stamped drop.
ALTER TABLE reservations ADD COLUMN drop_aggregate_id TEXT;
