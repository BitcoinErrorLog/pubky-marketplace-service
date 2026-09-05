-- Remove operator authority (owner decision, 2026-09-05: "there is no
-- master of the marketplace"). Peer-to-peer trades have no operator
-- adjudication and no service-computed tax, so the dispute/evidence/report
-- tables, the order dispute sub-document, and the per-order tax column are
-- dropped outright (the production database is empty; staging holds test
-- data only — removal approved). Idempotent: every destructive statement
-- is guarded with IF EXISTS, and each re-added check constraint is dropped
-- first.

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
-- attestor outcome left.

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_state_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_state_check CHECK (
        state IN (
            'pending_payment', 'paid', 'processing', 'shipped', 'delivered', 'completed',
            'cancel_requested', 'cancelled', 'return_requested', 'return_approved',
            'return_received', 'refunded_external', 'closed'
        )
    );

ALTER TABLE attestation_annotations
    DROP CONSTRAINT IF EXISTS attestation_annotations_outcome_check;
ALTER TABLE attestation_annotations
    ADD CONSTRAINT attestation_annotations_outcome_check
    CHECK (outcome IN ('refunded'));
