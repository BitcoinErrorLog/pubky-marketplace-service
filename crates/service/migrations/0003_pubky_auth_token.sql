-- Replace challenge–response authentication with Pubky AuthToken
-- verification. The challenge handshake required the client to sign a nonce,
-- which no real browser client can do (the secret key lives in the user's
-- signer device, e.g. Pubky Ring). Session establishment now verifies a
-- postcard-serialized AuthToken produced by the Pubky auth flow.

-- The challenge endpoint is gone, so its storage goes with it.
DROP TABLE IF EXISTS auth_challenges;

-- Service-enforced single use of accepted AuthTokens. A token's identity per
-- the Pubky Auth spec is its (public key, timestamp) pair; the primary key
-- rejects any second presentation of the same token. Rows are prunable once
-- the token is outside the acceptance window, because such tokens are
-- rejected by the window check before this table is consulted.
CREATE TABLE auth_token_uses (
    pubky TEXT NOT NULL,
    token_timestamp_micros BIGINT NOT NULL,
    used_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pubky, token_timestamp_micros)
);

CREATE INDEX auth_token_uses_used_at_idx ON auth_token_uses (used_at);

-- The capabilities granted by the AuthToken become the session's recorded
-- scope. Existing sessions (pre-AuthToken) carry no capability grant.
ALTER TABLE auth_sessions ADD COLUMN capabilities TEXT NOT NULL DEFAULT '';
