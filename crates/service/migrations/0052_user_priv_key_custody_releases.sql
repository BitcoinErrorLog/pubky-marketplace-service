-- Owner-held `/priv` data keys (priv-encryption-plan.md, Phase 4).
--
-- One row per owner whose data keys the service dropped after the owner wrapped
-- them on their own homeserver (`POST /v1/me/priv-keys/release`). The row is
-- the reason `GET /v1/me/priv-keys` answers `custody_released` for the owner
-- and never creates a replacement key: a new key would orphan the records the
-- wrapped keys protect. It holds no key material and no ciphertext. The key
-- ids are not kept: the deleted `user_priv_keys` rows were the only copy that
-- tied them to the owner, and the owner's wrapped files name them.
--
-- `key_count` is how many keys the release dropped, for the operator's log
-- and for counting released owners. Additive and rerunnable. There is NO DOWN
-- migration; rolling back the service leaves these rows in place, and a
-- service that predates 0052 would create a new key for a released owner, so
-- roll forward rather than back once any release has happened.

CREATE TABLE IF NOT EXISTS user_priv_key_custody_releases (
    owner_pubky TEXT PRIMARY KEY CHECK (char_length(owner_pubky) = 52),
    released_at TIMESTAMPTZ NOT NULL,
    key_count INTEGER NOT NULL CHECK (key_count >= 1)
);
