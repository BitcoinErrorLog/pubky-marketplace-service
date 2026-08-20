-- Scoped dispute-evidence reads (ADR-0019 §5, §8). Evidence bodies stay out
-- of every general projection, command result, log, and metric, but §8's
-- operator-query clause ("role-scoped, deliberately redacted views") means
-- an adjudicator must still be able to read the case file: a moderator who
-- cannot see the evidence cannot resolve the dispute. The read surface is
-- GET /v1/orders/{id}/evidence, scoped to the two dispute participants and
-- the configured moderator role.
--
-- Reading evidence through the moderator role is a privileged, cross-user
-- action. Every such read is recorded here append-only — mirroring how
-- report_decisions makes moderator decisions durable — so privileged access
-- is never invisible. The row is written in the same transaction as the
-- read itself: if the audit insert fails, the read is refused.

CREATE TABLE dispute_evidence_reads (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders (id),
    reader_pubky TEXT NOT NULL,
    evidence_items BIGINT NOT NULL CHECK (evidence_items >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER dispute_evidence_reads_append_only
    BEFORE UPDATE OR DELETE ON dispute_evidence_reads
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();
