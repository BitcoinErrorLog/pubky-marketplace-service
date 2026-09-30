-- A revival that no homeserver record backs can be retired.
--
-- 0050 records the delete a revival superseded in `revived_from_cursor`
-- and refuses a later tombstone whose `DEL` cursor is no newer, so a
-- delete confirmed before a re-creation cannot hide the re-created
-- listing. A revival with no record behind it (a `listing.register` for a
-- deleted id the seller never published again) left that same `DEL` the
-- latest event for the record path, and nothing could tombstone the row.
--
-- * A tombstone whose `DEL` cursor equals `revived_from_cursor` commits
--   when its transaction declares, in
--   `marketplace.listing_deletion_observed_generation`, the generation the
--   row had before the delete was confirmed, and that is still the row's
--   generation. Every revival advances the generation, so a confirmation
--   read before a revival still cannot tombstone the revived row; one read
--   after it proves no record was published since that `DEL`. A binary
--   that predates this migration never declares one, so 0050's refusal
--   holds for it. An older `DEL` is refused as before.
-- * `listing_revival_checks` records the generation of a revived listing
--   the follower has confirmed is not deleted, so it confirms each revival
--   once. Its absence is what keeps an unconfirmed revival due.
--
-- Additive and rerunnable. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS listing_revival_checks (
    aggregate_id TEXT PRIMARY KEY,
    generation BIGINT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE FUNCTION listing_deletion_observed_generation() RETURNS TEXT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(current_setting('marketplace.listing_deletion_observed_generation', true), '')
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
        IF NEW.revived_from_cursor IS NOT NULL
            AND NEW.deleted_event_cursor ~ '^[0-9]{1,20}$'
            AND (NEW.deleted_event_cursor::numeric < NEW.revived_from_cursor::numeric
                 OR (NEW.deleted_event_cursor::numeric = NEW.revived_from_cursor::numeric
                     AND listing_deletion_observed_generation() <> OLD.generation::text)) THEN
            RAISE EXCEPTION 'listing tombstone refused: its delete predates the revival';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
