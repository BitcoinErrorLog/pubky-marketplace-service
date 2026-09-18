-- Phase 6 Wave 1: listing-total inventory adjustments, immutable
-- idempotency/conflict evidence, seller-private external references, and
-- service-clock rate buckets. Migrations 0032 and 0033 are reserved by
-- adjacent unmerged streams; 0034 deliberately avoids that collision.

CREATE TABLE inventory_adjustment_results (
    seller_pubky TEXT NOT NULL CHECK (char_length(seller_pubky) = 52),
    idempotency_key UUID NOT NULL,
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    event_id UUID NOT NULL UNIQUE REFERENCES events (id),
    result_json TEXT NOT NULL CHECK (
        octet_length(result_json) BETWEEN 2 AND 8192
        AND result_json::jsonb IS NOT NULL
    ),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (seller_pubky, idempotency_key)
);

CREATE TABLE inventory_adjustment_conflicts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_pubky TEXT NOT NULL CHECK (char_length(seller_pubky) = 52),
    idempotency_key UUID NOT NULL,
    aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    original_request_hash TEXT NOT NULL CHECK (original_request_hash ~ '^[0-9a-f]{64}$'),
    conflicting_request_hash TEXT NOT NULL CHECK (conflicting_request_hash ~ '^[0-9a-f]{64}$'),
    observed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT inventory_adjustment_conflict_changed CHECK (
        original_request_hash <> conflicting_request_hash
    ),
    CONSTRAINT inventory_adjustment_conflict_unique
        UNIQUE (seller_pubky, idempotency_key, conflicting_request_hash)
);

CREATE TABLE inventory_external_refs (
    seller_pubky TEXT NOT NULL CHECK (char_length(seller_pubky) = 52),
    channel TEXT NOT NULL CHECK (
        octet_length(channel) BETWEEN 1 AND 32
        AND channel ~ '^[a-z0-9][a-z0-9._-]{0,31}$'
    ),
    external_id TEXT NOT NULL CHECK (
        octet_length(external_id) BETWEEN 1 AND 128
        AND external_id !~ '[[:cntrl:]]'
    ),
    aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id),
    event_id UUID NOT NULL UNIQUE REFERENCES events (id),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (seller_pubky, channel, external_id)
);

CREATE TABLE inventory_rate_limits (
    session_hash BYTEA NOT NULL REFERENCES auth_sessions (token_hash) ON DELETE CASCADE,
    endpoint_class TEXT NOT NULL CHECK (
        endpoint_class IN ('inventory.adjust', 'inventory.read')
    ),
    tokens DOUBLE PRECISION NOT NULL CHECK (tokens >= 0 AND tokens <= 20000),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_hash, endpoint_class)
);

CREATE FUNCTION forbid_inventory_evidence_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'inventory evidence is immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER inventory_adjustment_results_immutable
    BEFORE UPDATE OR DELETE ON inventory_adjustment_results
    FOR EACH ROW EXECUTE FUNCTION forbid_inventory_evidence_mutation();

CREATE TRIGGER inventory_adjustment_conflicts_immutable
    BEFORE UPDATE OR DELETE ON inventory_adjustment_conflicts
    FOR EACH ROW EXECUTE FUNCTION forbid_inventory_evidence_mutation();

CREATE TRIGGER inventory_external_refs_immutable
    BEFORE UPDATE OR DELETE ON inventory_external_refs
    FOR EACH ROW EXECUTE FUNCTION forbid_inventory_evidence_mutation();
