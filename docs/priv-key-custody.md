# Private data key custody

The Shop encrypts a user's watchlist, receipts, badge checkpoints and mute
changes under `/priv/pubky.app/marketplace/v2/` on the user's homeserver with
a random per-user data key. The service seals that key at rest
(`PRIV_DATA_KEY_ENCRYPTION_KEY`, `user_priv_keys`) and releases it with
`GET /v1/me/priv-keys` to a session that can read and write
`/priv/pubky.app/`. While it holds the key, the service can decrypt the data.

A user whose signer delivers scoped encryption keys (the `e` action on
`/priv/pubky.app/marketplace/`) can take custody back. The Shop wraps each data
key under a key only that signer can derive, stores the wrapped copy on the
user's own homeserver, reads it back, and then asks the service to drop its
copy. The data keys do not change, so no record is re-encrypted and no record
path moves. Users whose signer cannot do this keep the service-held key and
nothing here applies to them.

## Endpoints

| Request | Answer |
| --- | --- |
| `GET /v1/me/priv-keys` | `200` the owner's keys, as before. `409 custody_released` when the owner released custody. `403 needs_reauth`, `503 priv_keys_unavailable` as before. |
| `POST /v1/me/priv-keys/release` with `{"key_ids": ["<32 lowercase hex>", ...]}` | `200 {"schema_version": 1, "owner": ..., "released": true}`. `409 key_set_changed` when the ids are not exactly the keys the service holds. `400 invalid_request` for a malformed body. `403 needs_reauth` and `503 priv_keys_unavailable` as for the read. Every answer is `no-store`. |

The owner is always the session's actor. Authorization is the same as for the
key read: the session's grant must cover `/priv/pubky.app/` with read and write.

## What a release does

In one transaction under a per-owner advisory lock, the service checks that the
request names exactly the key ids it holds for the owner, deletes every
`user_priv_keys` row of the owner, and inserts a row into
`user_priv_key_custody_releases` (migration 0055). The row holds no key
material. It is why a later `GET` answers `custody_released` and why the
service never creates a replacement key for that owner: a new key would orphan
the records the wrapped keys protect.

- The service cannot see the wrapped files (`/priv` is readable only by its
  owner), so the request is the owner's own statement that they exist. The
  Shop sends it only after writing every wrapped file and opening it again.
- Naming exactly the held ids means a key issued after the owner read the keys
  is never dropped unseen. The owner reads again and retries.
- Repeating a completed release succeeds and changes nothing, so a lost reply
  is safe to retry.
- Creating an owner's first key takes the same lock, so no key can appear after
  a release.

## Operating it

- No new environment variable. Releases need `PRIV_DATA_KEY_ENCRYPTION_KEY`
  like the read, so a deployment without it answers `priv_keys_unavailable`.
- The boot probe, the re-seal job and the distinctness assertion work on the
  rows that remain. Once every owner released, the service no longer needs the
  sealing key to boot.
- Count released owners with
  `SELECT COUNT(*) FROM user_priv_key_custody_releases`. The log line
  `released priv data key custody to the owner` carries the actor prefix and
  the number of keys, never a key.
- **Roll forward, never back, once a release has happened.** A binary that
  predates 0055 does not know the tombstone and would create a new key for a
  released owner, a key none of that user's records use.
- Deploying the service first is safe: the Shop does not call the release
  endpoint until its own scoped-keys switch is on.
