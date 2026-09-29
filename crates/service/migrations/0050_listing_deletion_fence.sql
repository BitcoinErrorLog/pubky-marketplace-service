-- Fenced listing deletion: guards in the database, whichever binary writes.
--
-- `worker_leases.fence` grows by one on every successful acquisition of a
-- task lease, renewals included. The listing deletion follower keeps the
-- fence it acquired and share-locks its lease row before each write; a
-- takeover updates that row, so it waits for a write in flight.
--
-- The binary checks that, but a binary built before this migration does
-- not. The triggers below refuse the writes that do harm when they are
-- stale, for every writer:
--
-- * A tombstone (`listings.deleted_at` NULL -> set) commits only when its
--   transaction declares `marketplace.listing_deletion_authority` (set
--   with set_config(..., true), so it ends with the transaction): either
--   `command` (a `listing.sync` that confirmed the delete itself) or
--   `follower:<holder>:<fence>` naming the current follower lease row,
--   which the trigger share-locks. A binary that predates this migration
--   never declares one, so none of its tombstones commit.
-- * A revival (`deleted_at` set -> NULL) records the delete it superseded
--   in `revived_from_cursor`. A later tombstone must carry a strictly newer
--   `DEL` cursor, so a delete confirmed before the revival cannot hide the
--   re-created listing. `revived_from_cursor` never moves backwards.
-- * A `listing_deletion_cursors` write commits only under a current
--   follower authority, and the cursor never moves backwards (an unchanged
--   cursor is a poll that found nothing new). Cursor rows are never
--   deleted.
--
-- A statement from an older binary refused here raises, so its whole
-- transaction rolls back: a refused tombstone also releases no drop
-- bindings and records no `listing.deleted` event.
--
-- Additive and rerunnable. There is NO DOWN migration.

-- The column adds and trigger creation lock `listings` briefly. Waiting
-- longer than this behind a long transaction would queue every listing
-- read and write; failing instead stops the start, and the next start
-- retries.
SET LOCAL lock_timeout = '10s';

ALTER TABLE worker_leases ADD COLUMN IF NOT EXISTS fence BIGINT NOT NULL DEFAULT 0;

ALTER TABLE listings ADD COLUMN IF NOT EXISTS revived_from_cursor TEXT
    CONSTRAINT listings_revived_from_cursor_check
    CHECK (revived_from_cursor IS NULL OR revived_from_cursor ~ '^[0-9]{1,20}$');

CREATE OR REPLACE FUNCTION listing_deletion_authority_holds_lease(authority TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    holder_text TEXT := split_part(authority, ':', 2);
    fence_text TEXT := split_part(authority, ':', 3);
BEGIN
    IF split_part(authority, ':', 1) <> 'follower'
        OR holder_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR fence_text !~ '^[0-9]{1,18}$'
        OR split_part(authority, ':', 4) <> '' THEN
        RETURN FALSE;
    END IF;
    PERFORM 1 FROM worker_leases
        WHERE task = 'listing_deletions'
          AND holder = holder_text::uuid
          AND fence = fence_text::bigint
        FOR SHARE;
    RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION listing_deletion_authority() RETURNS TEXT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(current_setting('marketplace.listing_deletion_authority', true), '')
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
            AND NEW.deleted_event_cursor::numeric <= NEW.revived_from_cursor::numeric THEN
            RAISE EXCEPTION 'listing tombstone refused: its delete predates the revival';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS listings_deletion_guard ON listings;
CREATE TRIGGER listings_deletion_guard
    BEFORE UPDATE OF deleted_at, revived_from_cursor ON listings
    FOR EACH ROW
    WHEN (OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
          OR OLD.revived_from_cursor IS DISTINCT FROM NEW.revived_from_cursor)
    EXECUTE FUNCTION listings_deletion_guard();

CREATE OR REPLACE FUNCTION listing_deletion_cursors_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'listing deletion cursor rows are never deleted';
    END IF;
    IF NOT listing_deletion_authority_holds_lease(listing_deletion_authority()) THEN
        RAISE EXCEPTION 'listing deletion cursor write refused: no current follower lease';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.event_cursor IS NOT NULL
        AND (NEW.event_cursor IS NULL
             OR NEW.event_cursor::numeric < OLD.event_cursor::numeric) THEN
        RAISE EXCEPTION 'listing deletion cursor refused: it would move backwards';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS listing_deletion_cursors_guard ON listing_deletion_cursors;
CREATE TRIGGER listing_deletion_cursors_guard
    BEFORE INSERT OR UPDATE OR DELETE ON listing_deletion_cursors
    FOR EACH ROW
    EXECUTE FUNCTION listing_deletion_cursors_guard();
