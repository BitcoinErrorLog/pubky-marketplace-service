-- Trust & reputation Phase 1 (ADR 0024): purchase attestations, the D2
-- seller band-consent preference, attestor outcome annotations, and weekly
-- signed seller stat attestations. Everything here is additive.

-- === Purchase attestations ====================================================
-- One durable attestation per (order, reviewer), issued inside the
-- review.create transaction and re-fetchable idempotently. Immutable once
-- written: the attestation attests the purchase, not the review text, so
-- review edits never touch it.

CREATE TABLE review_attestations (
    order_id UUID NOT NULL REFERENCES orders (id),
    reviewer_pubky TEXT NOT NULL,
    order_ref TEXT NOT NULL,
    jws TEXT NOT NULL,
    claims JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, reviewer_pubky)
);

CREATE INDEX review_attestations_order_ref_idx ON review_attestations (order_ref);

CREATE TRIGGER review_attestations_append_only
    BEFORE UPDATE OR DELETE ON review_attestations
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

-- === Seller band consent (ratified D2) =======================================
-- The seller's standing amount-band preference. A purchase attestation
-- carries an amount band only when this row allows it AND the reviewer
-- opted in at review time. Absent row means "not consented".

CREATE TABLE attestation_band_consents (
    seller_pubky TEXT PRIMARY KEY,
    allows_amount_band BOOLEAN NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL
);

-- === Attestor outcome annotations (design §5.6) ==============================
-- Append-only, keyed by the salted order_ref. Outcomes referencing dispute
-- resolutions are stored with the winning side (buyer/seller); Phase 3
-- publication maps them to the reviewer-relative record vocabulary
-- (dispute_resolved_for_reviewer / against_reviewer) per review role.
-- The disavowal reason stays internal and is never published.

CREATE TABLE attestation_annotations (
    id UUID PRIMARY KEY,
    order_ref TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'refunded',
            'dispute_resolved_for_buyer',
            'dispute_resolved_for_seller',
            'attestation_disavowed'
        )
    ),
    reason TEXT,
    annotated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX attestation_annotations_order_ref_idx
    ON attestation_annotations (order_ref, annotated_at DESC);

CREATE TRIGGER attestation_annotations_append_only
    BEFORE UPDATE OR DELETE ON attestation_annotations
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

-- === Seller stat attestations (ratified D3) ==================================
-- Weekly per-seller fulfillment stats computed from the private order book,
-- signed by the attestor. Stored here for the Phase 3 attestor-homeserver
-- publisher; one row per (seller, period end).

CREATE TABLE seller_stat_attestations (
    id UUID PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    period_from DATE NOT NULL,
    period_to DATE NOT NULL,
    body JSONB NOT NULL,
    jws TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT seller_stat_attestations_one_per_period
        UNIQUE (seller_pubky, period_to)
);

CREATE INDEX seller_stat_attestations_latest_idx
    ON seller_stat_attestations (seller_pubky, created_at DESC);
