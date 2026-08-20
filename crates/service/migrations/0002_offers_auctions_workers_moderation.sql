-- Vertical slice 2: offers, auctions, background workers, moderation.

-- === Notifications (outbox consumer) =========================================
-- Delivered from the outbox at least once; the unique key on
-- (event_id, recipient_pubky) makes redelivery a no-op (consumer-side dedup).

CREATE TABLE notifications (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events (id),
    recipient_pubky TEXT NOT NULL,
    actor_pubky TEXT NOT NULL,
    type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    read_at TIMESTAMPTZ,
    CONSTRAINT notifications_dedup_by_event UNIQUE (event_id, recipient_pubky)
);

CREATE INDEX notifications_recipient_idx ON notifications (recipient_pubky, created_at DESC);

-- === Trust reports and append-only moderator decisions =======================

CREATE TABLE reports (
    id UUID PRIMARY KEY,
    reporter_pubky TEXT NOT NULL,
    target_type TEXT NOT NULL CHECK (target_type IN ('listing', 'user', 'message', 'review')),
    target_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (
        reason IN ('prohibited_item', 'counterfeit', 'scam', 'harassment', 'unsafe', 'other')
    ),
    details TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'dismissed', 'actioned')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX reports_reporter_idx ON reports (reporter_pubky, created_at DESC);

CREATE TABLE report_decisions (
    id UUID PRIMARY KEY,
    report_id UUID NOT NULL REFERENCES reports (id),
    moderator_pubky TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('dismissed', 'actioned')),
    rationale TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Decisions are append-only, like the event log.
CREATE TRIGGER report_decisions_append_only
    BEFORE UPDATE OR DELETE ON report_decisions
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

-- === Worker leases ============================================================
-- One row per background task. A worker instance holds the lease until
-- lease_until; another instance may take over only after it expires, so two
-- instances never drain the same task concurrently. A crashed holder is
-- recovered when its lease lapses.

CREATE TABLE worker_leases (
    task TEXT PRIMARY KEY,
    holder UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL
);

-- === Auction winner orders ====================================================
-- Auction close creates the winning order before the buyer has supplied a
-- delivery address; checkout orders continue to set it at creation.

ALTER TABLE orders ALTER COLUMN delivery_address DROP NOT NULL;
