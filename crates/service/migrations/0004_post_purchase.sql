-- Post-purchase lifecycle: receipts, fulfillment, returns, disputes,
-- external refunds, and reviews (ADR-0019 §4 invariants).

-- === Order sub-documents ======================================================
-- The shipment, return request, dispute, and external refund records live on
-- the order aggregate as JSONB sub-documents, exactly as the auction state
-- lives on listings. Their state vocabularies are canonical in
-- contracts/state-machines.json (return and dispute machines).

ALTER TABLE orders
    ADD COLUMN shipment JSONB,
    ADD COLUMN return_request JSONB,
    ADD COLUMN dispute JSONB,
    ADD COLUMN external_refund JSONB;

-- An external refund never exceeds the order value (ADR-0019 §4: cumulative
-- external refunds not exceeding confirmed value; the service records at
-- most one refund evidence record per order).
ALTER TABLE orders
    ADD CONSTRAINT orders_external_refund_within_total CHECK (
        external_refund IS NULL
        OR (external_refund ->> 'amount_minor')::bigint BETWEEN 1 AND total_minor
    );

-- === Receipts =================================================================
-- Issued exactly once per order/payment when the sandbox payment confirms;
-- immutable once written, like the event log.

CREATE TABLE receipts (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders (id),
    payment_id UUID NOT NULL UNIQUE REFERENCES payments (id),
    issuer_pubky TEXT NOT NULL,
    recipient_pubky TEXT NOT NULL,
    total_minor BIGINT NOT NULL CHECK (total_minor >= 0),
    currency TEXT NOT NULL,
    exponent INTEGER NOT NULL CHECK (exponent BETWEEN 0 AND 18),
    content_hash TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER receipts_append_only
    BEFORE UPDATE OR DELETE ON receipts
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

-- === Reviews ==================================================================
-- One review per participant per order per role, enforced by the database
-- constraint rather than application logic (ADR-0019 §4). Rows are updated
-- in place only through the bounded-window review.update command.

CREATE TABLE reviews (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders (id),
    reviewer_pubky TEXT NOT NULL,
    reviewer_role TEXT NOT NULL CHECK (reviewer_role IN ('buyer', 'seller')),
    subject_pubky TEXT NOT NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT reviews_one_per_order_role UNIQUE (order_id, reviewer_role)
);

-- === Dispute evidence =========================================================
-- Append-only evidence attached to an open dispute. The body is private
-- order evidence (ADR-0019 §8): it is never served by any read projection or
-- command result. Only the count is reflected in the dispute sub-document.

CREATE TABLE dispute_evidence (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders (id),
    submitter_pubky TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER dispute_evidence_append_only
    BEFORE UPDATE OR DELETE ON dispute_evidence
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();
