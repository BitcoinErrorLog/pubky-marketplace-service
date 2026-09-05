-- Remove operator authority (owner decision, 2026-09-05: "there is no
-- master of the marketplace"). Peer-to-peer trades have no operator
-- adjudication and no service-computed tax, so the dispute/evidence/report
-- tables, the order dispute sub-document, and the per-order tax column are
-- dropped outright (the production database is empty; staging holds test
-- data only — removal approved).
--
-- Row remediation runs BEFORE the narrowed check constraints are re-added,
-- so the migration applies cleanly on a database that still holds disputed
-- orders or operator annotation outcomes (staging test data):
--   * `disputed` orders are completed (seller-favour default, owner-approved
--     for the staging test data; production has no rows);
--   * dispute/disavow attestation annotations are deleted (operator
--     artefacts with no meaning in a peer-to-peer marketplace).
-- Idempotent: every destructive statement is guarded with IF EXISTS, each
-- re-added check constraint is dropped first, and the remediation UPDATE /
-- DELETE are no-ops once applied.
--
-- There is NO DOWN migration: dropped tables/columns cannot be restored.
-- Production must be empty or backed up before applying (it is empty as of
-- 2026-09-05).

-- === Disputes and dispute evidence ===========================================

DROP TABLE IF EXISTS dispute_evidence_reads;
DROP TABLE IF EXISTS dispute_evidence;

ALTER TABLE orders DROP COLUMN IF EXISTS dispute;

-- === Trust reports ===========================================================

DROP TABLE IF EXISTS report_decisions;
DROP TABLE IF EXISTS reports;

-- === Tax =====================================================================
-- Prices are the seller's listed price plus seller-signed shipping; the
-- service never computes a tax line.

ALTER TABLE orders DROP COLUMN IF EXISTS tax_minor;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_total_balance;
ALTER TABLE orders
    ADD CONSTRAINT orders_total_balance
    CHECK (total_minor = subtotal_minor + shipping_minor);

-- === State vocabularies ======================================================
-- `disputed` leaves the order state enum; refund annotations are the only
-- attestor outcome left. Remediate pre-existing rows first so the narrowed
-- check constraints can be added on a non-empty database.

-- Complete every disputed order. There is no arbiter any more, so an order
-- that was awaiting adjudication completes as if undisputed (seller-favour
-- default; owner-approved for the staging test data; production has no
-- rows). revision and updated_at move exactly as a handler state transition
-- would move them, so optimistic-concurrency checks keep working. No event
-- row is inserted: handlers record the *causing* action (review.created,
-- order.cancelled, ...) and there is no order.completed event kind;
-- projections read orders.state directly, and the seller-stats worker keys
-- off fulfillment.delivered events: a disputed order that had reached
-- delivered already has one, and a pre-delivery disputed order correctly
-- records no delivery stat. The one-row gap in per-aggregate event
-- revisions is harmless — events_one_per_aggregate_revision only enforces
-- uniqueness, and the next event claims revision + 1.
UPDATE orders
SET state = 'completed', revision = revision + 1, updated_at = now()
WHERE state = 'disputed';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_state_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_state_check CHECK (
        state IN (
            'pending_payment', 'paid', 'processing', 'shipped', 'delivered', 'completed',
            'cancel_requested', 'cancelled', 'return_requested', 'return_approved',
            'return_received', 'refunded_external', 'closed'
        )
    );

-- Delete dispute/disavow annotations: operator artefacts with no meaning in
-- a peer-to-peer marketplace. The append-only trigger must be lifted for
-- the DELETE and is re-created immediately after (drop-then-create keeps
-- this idempotent); the remaining 'refunded' rows stay immutable.
DROP TRIGGER IF EXISTS attestation_annotations_append_only ON attestation_annotations;

DELETE FROM attestation_annotations
WHERE outcome IN (
    'dispute_resolved_for_buyer',
    'dispute_resolved_for_seller',
    'attestation_disavowed'
);

CREATE TRIGGER attestation_annotations_append_only
    BEFORE UPDATE OR DELETE ON attestation_annotations
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

ALTER TABLE attestation_annotations
    DROP CONSTRAINT IF EXISTS attestation_annotations_outcome_check;
ALTER TABLE attestation_annotations
    ADD CONSTRAINT attestation_annotations_outcome_check
    CHECK (outcome IN ('refunded'));
