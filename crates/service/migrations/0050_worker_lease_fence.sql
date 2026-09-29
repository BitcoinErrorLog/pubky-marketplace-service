-- Fenced worker leases.
--
-- `fence` grows by one on every successful acquisition of a task lease,
-- renewals included. A pass that needs fencing (the listing deletion
-- follower) keeps the fence it acquired, and each of its writes first
-- share-locks the lease row and proceeds only while that row still names
-- the same holder and fence and has not expired. A takeover updates the
-- row, so it waits for an in-flight fenced write to commit or roll back,
-- and a pass whose lease expired, was taken over, or was renewed by a
-- later pass writes nothing.
--
-- `worker_leases` has one row per background task. A NOT NULL column with
-- a constant default is a catalog-only change; existing rows read 0 until
-- their next acquisition.
--
-- Additive and rerunnable. There is NO DOWN migration.

ALTER TABLE worker_leases ADD COLUMN IF NOT EXISTS fence BIGINT NOT NULL DEFAULT 0;
