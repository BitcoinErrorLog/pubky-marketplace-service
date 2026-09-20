-- Phase 6 Wave 3a: session administration, cursor/export support, and
-- durable signed-webhook delivery. Migration 0034 is the production base;
-- all changes here are additive.

ALTER TABLE auth_sessions
    ADD COLUMN session_id UUID,
    ADD COLUMN label TEXT,
    ADD COLUMN client_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN last_used_at TIMESTAMPTZ,
    ADD COLUMN revoked_at TIMESTAMPTZ;

UPDATE auth_sessions
SET session_id = (
    substr(md5(encode(token_hash, 'hex')), 1, 8) || '-' ||
    substr(md5(encode(token_hash, 'hex')), 9, 4) || '-' ||
    substr(md5(encode(token_hash, 'hex')), 13, 4) || '-' ||
    substr(md5(encode(token_hash, 'hex')), 17, 4) || '-' ||
    substr(md5(encode(token_hash, 'hex')), 21, 12)
)::uuid
WHERE session_id IS NULL;

ALTER TABLE auth_sessions
    ALTER COLUMN session_id SET NOT NULL,
    ADD CONSTRAINT auth_sessions_session_id_unique UNIQUE (session_id),
    ADD CONSTRAINT auth_sessions_label_bounded CHECK (
        label IS NULL OR (
            octet_length(label) BETWEEN 1 AND 80
            AND label !~ '[[:cntrl:]]'
        )
    ),
    ADD CONSTRAINT auth_sessions_metadata_object_bounded CHECK (
        jsonb_typeof(client_metadata) = 'object'
        AND octet_length(client_metadata::text) <= 2048
    );

CREATE INDEX auth_sessions_owner_created_idx
    ON auth_sessions (pubky, created_at DESC, session_id DESC);

CREATE TABLE webhook_endpoints (
    id UUID PRIMARY KEY,
    seller_pubky TEXT NOT NULL CHECK (char_length(seller_pubky) = 52),
    endpoint_url TEXT NOT NULL CHECK (octet_length(endpoint_url) BETWEEN 12 AND 2048),
    contract_version INTEGER NOT NULL DEFAULT 1 CHECK (contract_version = 1),
    key_id UUID NOT NULL,
    signing_key BYTEA NOT NULL CHECK (octet_length(signing_key) = 32),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX webhook_endpoints_active_owner_idx
    ON webhook_endpoints (seller_pubky, id)
    WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX webhook_endpoints_active_owner_url_unique
    ON webhook_endpoints (seller_pubky, endpoint_url)
    WHERE deleted_at IS NULL;

CREATE TABLE webhook_deliveries (
    endpoint_id UUID NOT NULL REFERENCES webhook_endpoints (id),
    event_id UUID NOT NULL REFERENCES events (id),
    key_id UUID NOT NULL,
    signing_key BYTEA NOT NULL CHECK (octet_length(signing_key) = 32),
    body JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    first_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    last_status INTEGER,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (endpoint_id, event_id),
    CONSTRAINT webhook_delivery_terminal_exclusive CHECK (
        delivered_at IS NULL OR dead_lettered_at IS NULL
    )
);

CREATE INDEX webhook_deliveries_due_idx
    ON webhook_deliveries (next_attempt_at, endpoint_id, event_id)
    WHERE delivered_at IS NULL AND dead_lettered_at IS NULL;

CREATE TABLE automation_rate_limits (
    seller_pubky TEXT NOT NULL CHECK (char_length(seller_pubky) = 52),
    endpoint_class TEXT NOT NULL CHECK (
        endpoint_class IN (
            'session.admin', 'export.listings', 'export.orders',
            'events.read', 'listing.sync_many', 'webhook.admin'
        )
    ),
    tokens DOUBLE PRECISION NOT NULL CHECK (tokens >= 0 AND tokens <= 20000),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (seller_pubky, endpoint_class)
);

CREATE TABLE webhook_dead_letters (
    endpoint_id UUID NOT NULL,
    event_id UUID NOT NULL,
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    final_status INTEGER,
    dead_lettered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (endpoint_id, event_id)
);
