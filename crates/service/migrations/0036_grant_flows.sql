-- Durable Pubky grant-flow authority and one-use result handoff.
--
-- `flow_id` is correlation only. Secret grant state and the temporary result
-- payload are XChaCha20-Poly1305 ciphertexts; raw delivery IDs, result tokens,
-- and result nonces are never stored.

ALTER TABLE auth_sessions
    ADD COLUMN session_id BIGINT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE auth_sessions
    ADD CONSTRAINT auth_sessions_session_id_key UNIQUE (session_id);

CREATE TABLE grant_flows (
    flow_id UUID PRIMARY KEY,
    expected_pubky TEXT NOT NULL,
    assertion_jti UUID UNIQUE,
    client_id TEXT NOT NULL,
    cpk TEXT NOT NULL,
    capabilities TEXT NOT NULL,
    relay_url TEXT NOT NULL,
    grant_state_sealed BYTEA,
    key_epoch SMALLINT NOT NULL,
    result_hash_epoch SMALLINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'awaiting', 'verifying', 'complete', 'mismatch', 'expired',
        'cancelled', 'invalid', 'failed'
    )),
    version BIGINT NOT NULL DEFAULT 0,
    lease_owner UUID,
    lease_until TIMESTAMPTZ,
    approved_pubky TEXT,
    terminal_code TEXT,
    result_delivery_id_hash BYTEA NOT NULL
        CHECK (octet_length(result_delivery_id_hash) = 32),
    result_cpk TEXT NOT NULL,
    result_token_hash BYTEA
        CHECK (
            result_token_hash IS NULL
            OR octet_length(result_token_hash) = 32
        ),
    result_token_expires_at TIMESTAMPTZ,
    result_token_delivered_at TIMESTAMPTZ,
    result_payload_sealed BYTEA,
    result_auth_session_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    terminal_at TIMESTAMPTZ,
    result_claimed_at TIMESTAMPTZ,
    CHECK (key_epoch BETWEEN 1 AND 32767),
    CHECK (result_hash_epoch BETWEEN 1 AND 32767),
    CHECK (expires_at > created_at),
    UNIQUE (client_id, cpk)
);

ALTER TABLE grant_flows
    ADD CONSTRAINT grant_flows_result_auth_session_id_fkey
    FOREIGN KEY (result_auth_session_id)
    REFERENCES auth_sessions(session_id)
    ON DELETE SET NULL;

CREATE TABLE grant_result_nonces (
    nonce_id UUID PRIMARY KEY,
    flow_id UUID NOT NULL
        REFERENCES grant_flows(flow_id)
        ON DELETE CASCADE,
    purpose TEXT NOT NULL CHECK (purpose IN ('ticket', 'claim')),
    bff_principal TEXT NOT NULL,
    nonce_hash BYTEA NOT NULL CHECK (octet_length(nonce_hash) = 32),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    CHECK (expires_at > created_at)
);

CREATE TABLE grant_rate_limits (
    bucket_hash BYTEA NOT NULL CHECK (octet_length(bucket_hash) = 32),
    endpoint_class TEXT NOT NULL CHECK (endpoint_class IN (
        'create_ip', 'create_pubky', 'status_flow',
        'result_principal', 'result_flow'
    )),
    window_started_at TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL CHECK (request_count > 0),
    PRIMARY KEY (bucket_hash, endpoint_class)
);

CREATE INDEX grant_flows_worker_scan_idx
    ON grant_flows (expires_at, created_at)
    WHERE status IN ('awaiting', 'verifying');

CREATE INDEX grant_flows_result_expiry_idx
    ON grant_flows (result_token_expires_at)
    WHERE status = 'complete' AND result_claimed_at IS NULL;

CREATE INDEX grant_result_nonces_expiry_idx
    ON grant_result_nonces (expires_at)
    WHERE used_at IS NULL;

CREATE INDEX grant_rate_limits_window_idx
    ON grant_rate_limits (window_started_at);
