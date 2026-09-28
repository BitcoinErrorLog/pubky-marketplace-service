-- Listing tombstones: the service follows a seller's homeserver delete.
--
-- A listing is deleted when its canonical record is gone from the seller's
-- homeserver AND that homeserver's per-user event stream records a `DEL` as
-- the latest event for the exact record path. A bare 404 is not enough: the
-- homeserver answers 404 for users it does not host. `deleted_event_cursor`
-- is that `DEL` event's cursor, kept as the evidence.
--
-- The row is retained, never hard-deleted, so past orders, digital delivery
-- evidence, and disputes still resolve against it and the quantity ledger
-- keeps balancing. Public reads, seller lists, and every new commitment
-- (checkout, payment holds, offers, reserves, bids, drop bindings) skip a
-- tombstoned row.
--
-- A record re-created at the same id revives the aggregate with stock
-- derived from the new record only. `recreated_at` marks that revival:
-- offers, awards, and unpaid orders created before it cannot commit stock
-- against the new listing.
--
-- `generation` counts revivals. Each drop binding carries the generation it
-- was made against (`drop_listings.listing_generation`), and gating reads
-- only bindings of the listing's current generation. A binding committed
-- by a `drop.sync` that overlapped the tombstone therefore never gates, or
-- draws on, the re-created listing, and the one-active-drop rule is per
-- generation. Existing rows are generation 0 on both sides, so every
-- binding that gates today keeps gating.
--
-- `listing_deletion_cursors` is the per-seller position in the homeserver
-- event stream for the deletion follower.
--
-- Additive and rerunnable. There is NO DOWN migration.

ALTER TABLE listings ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS deleted_event_cursor TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS recreated_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE drop_listings
    ADD COLUMN IF NOT EXISTS listing_generation BIGINT NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS drop_listings_one_active_per_listing;
CREATE UNIQUE INDEX drop_listings_one_active_per_listing
    ON drop_listings (seller_pubky, listing_id, listing_generation)
    WHERE active;

ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_deletion_evidence_check;
ALTER TABLE listings
    ADD CONSTRAINT listings_deletion_evidence_check CHECK (
        (deleted_at IS NULL AND deleted_event_cursor IS NULL)
        OR (
            deleted_at IS NOT NULL
            AND deleted_event_cursor IS NOT NULL
            AND deleted_event_cursor ~ '^[0-9]{1,20}$'
        )
    ) NOT VALID;
ALTER TABLE listings VALIDATE CONSTRAINT listings_deletion_evidence_check;

CREATE INDEX IF NOT EXISTS listings_live_seller_idx
    ON listings (seller_pubky, server_revision, aggregate_id)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS listing_deletion_cursors (
    seller_pubky TEXT PRIMARY KEY,
    event_cursor TEXT CHECK (event_cursor IS NULL OR event_cursor ~ '^[0-9]{1,20}$'),
    polled_at TIMESTAMPTZ NOT NULL
);
