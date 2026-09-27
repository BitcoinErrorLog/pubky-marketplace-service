-- Per-user `/priv` data keys (priv-encryption-plan.md, Phase 1).
--
-- One row per data key an owner holds. The key is 32 random bytes sealed
-- under PRIV_DATA_KEY_ENCRYPTION_KEY with the `seal.rs` construction:
-- a 24-byte nonce, then the XChaCha20-Poly1305 ciphertext of the 32-byte key
-- and its 16-byte tag, so every sealed value is exactly 72 bytes. The
-- associated data is `priv-dek|v1|{owner_pubky}|{key_id}`, so a row moved to
-- another owner or key id does not open. No plaintext column exists.
--
-- `generation` numbers an owner's keys from 1. The unique (owner,
-- generation) pair makes first-key creation idempotent under concurrent
-- requests. The BIGSERIAL id lets the boot probe and the re-seal job page
-- the table by key.
--
-- Additive and rerunnable. There is NO DOWN migration; rolling back the
-- service leaves these rows in place, still sealed.

CREATE TABLE IF NOT EXISTS user_priv_keys (
    id BIGSERIAL PRIMARY KEY,
    owner_pubky TEXT NOT NULL CHECK (char_length(owner_pubky) = 52),
    generation INTEGER NOT NULL CHECK (generation >= 1),
    key_id TEXT NOT NULL CHECK (key_id ~ '^[0-9a-f]{32}$'),
    sealed_key BYTEA NOT NULL CHECK (octet_length(sealed_key) = 72),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS user_priv_keys_owner_generation_key
    ON user_priv_keys (owner_pubky, generation);
CREATE UNIQUE INDEX IF NOT EXISTS user_priv_keys_owner_key_id_key
    ON user_priv_keys (owner_pubky, key_id);
