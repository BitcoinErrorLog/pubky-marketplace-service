-- A revival that no homeserver record backs can be retired, and a record
-- the service accepted after a delete was confirmed never is.
--
-- 0050 records the delete a revival superseded in `revived_from_cursor`
-- and refuses a later tombstone whose `DEL` cursor is no newer. A revival
-- with no record behind it (a `listing.register` for a deleted id the
-- seller never published again) left that same `DEL` the latest event for
-- the record path, and nothing could tombstone the row.
--
-- * `listings.record_epoch` counts the record-derived writes the service
--   accepted for the row: a revival, a change to any field the seller's
--   record supplies, or a sync that found the record and changed nothing
--   (it advances the epoch by one explicitly). The trigger below maintains
--   it for every writer: a statement can advance it by exactly one, never
--   set it otherwise.
-- * A tombstone commits only when its transaction declares, in
--   `marketplace.listing_deletion_observed_epoch`, the epoch the row had
--   before the delete was confirmed against the homeserver, and that is
--   still the row's epoch. A register or sync that landed between the
--   confirmation and the tombstone, and a revival, advance the epoch, so a
--   confirmation read before them cannot retire the row. With the epoch
--   unchanged, the `DEL` that a revival superseded may retire it: the
--   delete was still the record's latest event after the revival. An older
--   `DEL` is refused as before. A binary that predates this migration
--   declares no epoch, so none of its tombstones commit.
-- * `listing_revival_checks` records, per revived listing, the generation
--   the follower confirmed is not deleted and when it last tried. Its
--   absence, or an older generation, keeps a revival due; the attempt time
--   rotates the ones the homeserver could not answer for to the back.
--
-- Additive and rerunnable. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE listings ADD COLUMN IF NOT EXISTS record_epoch BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS listing_revival_checks (
    aggregate_id TEXT PRIMARY KEY,
    checked_generation BIGINT,
    attempted_at TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE FUNCTION listings_record_epoch() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL)
        OR NEW.listing_revision IS DISTINCT FROM OLD.listing_revision
        OR NEW.title IS DISTINCT FROM OLD.title
        OR NEW.content_hash IS DISTINCT FROM OLD.content_hash
        OR NEW.unit_price_amount_minor IS DISTINCT FROM OLD.unit_price_amount_minor
        OR NEW.shipping_minor IS DISTINCT FROM OLD.shipping_minor
        OR NEW.fulfillment_methods IS DISTINCT FROM OLD.fulfillment_methods
        OR NEW.digital_lock_policy_uri IS DISTINCT FROM OLD.digital_lock_policy_uri
        OR NEW.digital_lock_criterion_id IS DISTINCT FROM OLD.digital_lock_criterion_id THEN
        NEW.record_epoch := OLD.record_epoch + 1;
    ELSIF NEW.record_epoch IS DISTINCT FROM OLD.record_epoch + 1 THEN
        NEW.record_epoch := OLD.record_epoch;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS listings_record_epoch ON listings;
CREATE TRIGGER listings_record_epoch
    BEFORE UPDATE ON listings
    FOR EACH ROW
    EXECUTE FUNCTION listings_record_epoch();

CREATE OR REPLACE FUNCTION listing_deletion_observed_epoch() RETURNS TEXT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(current_setting('marketplace.listing_deletion_observed_epoch', true), '')
$$;

CREATE OR REPLACE FUNCTION listings_deletion_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.revived_from_cursor IS NOT NULL
        AND (NEW.revived_from_cursor IS NULL
             OR NEW.revived_from_cursor::numeric < OLD.revived_from_cursor::numeric) THEN
        RAISE EXCEPTION 'listing revival marker refused: it would move backwards';
    END IF;
    IF OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL THEN
        IF OLD.deleted_event_cursor IS NOT NULL
            AND (NEW.revived_from_cursor IS NULL
                 OR OLD.deleted_event_cursor::numeric > NEW.revived_from_cursor::numeric) THEN
            NEW.revived_from_cursor := OLD.deleted_event_cursor;
        END IF;
    ELSIF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        IF listing_deletion_authority() <> 'command'
            AND NOT listing_deletion_authority_holds_lease(listing_deletion_authority()) THEN
            RAISE EXCEPTION 'listing tombstone refused: no current deletion authority';
        END IF;
        IF listing_deletion_observed_epoch() <> OLD.record_epoch::text THEN
            RAISE EXCEPTION 'listing tombstone refused: the record changed after the delete was confirmed';
        END IF;
        IF NEW.revived_from_cursor IS NOT NULL
            AND NEW.deleted_event_cursor ~ '^[0-9]{1,20}$'
            AND NEW.deleted_event_cursor::numeric < NEW.revived_from_cursor::numeric THEN
            RAISE EXCEPTION 'listing tombstone refused: its delete predates the revival';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
