-- Local pickup, Wave 7 safe subset (local-pickup-design.md PART A).
--
-- Adds: the listing's published `fulfillment_methods`; the required
-- per-order `fulfillment` column and the `first_revealed_at` withdrawal
-- stamp; the sealed pickup-details store with its per-listing monotonic
-- version counter; the sealed per-line pinned payment snapshots; and the
-- one-row-per-order pickup handover record. The order state vocabulary
-- gains `ready_for_pickup`.
--
-- Forward-only and additive: every statement is idempotent (IF NOT EXISTS /
-- IF EXISTS / NOT VALID constraint re-add), so a partially applied deploy
-- can simply re-run. Pre-existing orders backfill to `fulfillment =
-- 'shipping'` — every order the service has ever created is a shipped
-- physical order (digital listings carry no fulfillment choice and are not
-- registered with this service; §A2). The `version_at_payment` pin lives as
-- a JSON key on the order line: pre-migration rows simply have no such key,
-- which reads as "no terms version pinned" (§A3). Sealed pickup rows are
-- XChaCha20-Poly1305 ciphertext with a fresh nonce per seal; the plaintext
-- never has a storage path.
--
-- There is NO DOWN migration.

-- === Listings: published fulfillment methods ==============================
-- `shipping` | `pickup` | both, echoed from the owner-signed listing record
-- at `listing.register`/`listing.sync` (§A1). Public catalog data; existing
-- listings default to shipping only.

ALTER TABLE listings
    ADD COLUMN IF NOT EXISTS fulfillment_methods TEXT[] NOT NULL DEFAULT '{shipping}';

-- Uniqueness within the array is enforced by command validation (the
-- service writes only through validated payloads); Postgres check
-- constraints cannot express it (no subqueries allowed).
ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_fulfillment_methods_check;
ALTER TABLE listings
    ADD CONSTRAINT listings_fulfillment_methods_check CHECK (
        cardinality(fulfillment_methods) BETWEEN 1 AND 2
        AND fulfillment_methods <@ ARRAY['shipping', 'pickup']::TEXT[]
    ) NOT VALID;

-- === Orders: required fulfillment + the withdrawal stamp ==================

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS fulfillment TEXT NOT NULL DEFAULT 'shipping';

-- Backfill every pre-existing row to `shipping`. All orders created before
-- this migration are shipped physical orders; digital listings are excluded
-- from the backfill because they are never registered here (§A2).
UPDATE orders SET fulfillment = 'shipping' WHERE fulfillment IS DISTINCT FROM 'shipping';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_fulfillment_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_fulfillment_check
    CHECK (fulfillment IN ('shipping', 'pickup'));

-- The first successful buyer reveal stamps the bounded withdrawal window
-- (§A3); NULL until the first reveal.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS first_revealed_at TIMESTAMPTZ;

-- === Order state vocabulary: ready_for_pickup =============================
-- Drop-then-create keeps this idempotent on a partially applied deploy;
-- existing rows are unaffected (none can hold the new state yet).

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_state_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_state_check CHECK (
        state IN (
            'pending_payment', 'paid', 'ready_for_pickup', 'processing', 'shipped',
            'delivered', 'completed', 'cancel_requested', 'cancelled',
            'return_requested', 'return_approved', 'return_received',
            'refunded_external', 'closed'
        )
    );

-- === Sealed pickup details (family 1) =====================================
-- Append-only per-listing versions of the seller's pickup details, sealed
-- under PICKUP_DETAILS_ENCRYPTION_KEY with AAD = aggregate id ‖ version
-- (§A1/§A3). Retention: on `pickup_details.clear`, versions not referenced
-- as `version_at_payment` by a paid, non-terminal order are hard-deleted;
-- retained versions purge once their referencing orders go terminal.

CREATE TABLE IF NOT EXISTS listing_pickup_details (
    aggregate_id TEXT NOT NULL,
    seller_pubky TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    details_ciphertext BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (aggregate_id, version)
);

-- The per-listing monotonic version counter in its OWN row (§A3): it
-- survives `pickup_details.clear`, so versions never restart and a
-- delete-and-recreate cannot fool terms-change detection. `last_version`
-- is 0 when no details were ever set.
CREATE TABLE IF NOT EXISTS listing_pickup_version_counters (
    aggregate_id TEXT PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0 CHECK (last_version >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

-- === Sealed pinned payment snapshots (family 2) ===========================
-- Written per pickup order line inside `confirm_order`'s receipt
-- transaction (§A3): the details as shown at payment, sealed with AAD =
-- order id ‖ line index ‖ version, plus the adapter that confirmed the
-- payment (`sandbox` confirmations pin exactly like worker-confirmed
-- payments, and the reveal read refuses a sandbox-pinned snapshot
-- regardless of the deployment's current sandbox flag).

CREATE TABLE IF NOT EXISTS pickup_line_snapshots (
    order_id UUID NOT NULL REFERENCES orders (id),
    line_index INTEGER NOT NULL CHECK (line_index >= 0),
    listing_aggregate_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    snapshot_ciphertext BYTEA NOT NULL,
    confirming_adapter TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, line_index)
);

-- === Pickup handovers =====================================================
-- Written by `fulfillment.confirm_pickup` (§A6): one handover per order
-- (PRIMARY KEY on order_id, so a duplicate or replayed confirm cannot
-- write a second row), carrying who confirmed and the server instant.
-- `confirmed_by` is the confirming ROLE (`buyer` | `seller`): a seller-only
-- confirm is a seller-attested handover, which reputation counts only on a
-- buyer confirm or a dispute-free auto-complete.

CREATE TABLE IF NOT EXISTS pickup_handovers (
    order_id UUID PRIMARY KEY REFERENCES orders (id),
    confirmed_by TEXT NOT NULL CHECK (confirmed_by IN ('buyer', 'seller')),
    confirmed_at TIMESTAMPTZ NOT NULL
);
