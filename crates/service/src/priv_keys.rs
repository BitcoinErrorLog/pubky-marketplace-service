//! Per-user `/priv` data keys (priv-encryption-plan.md, Phase 1).
//!
//! Each owner pubky gets a random 32-byte data key, generated from the OS
//! RNG on the owner's first request and stored in `user_priv_keys` sealed
//! with the [`crate::seal`] construction under
//! `PRIV_DATA_KEY_ENCRYPTION_KEY`. Associated data:
//!
//! `priv-dek|v1|{owner_pubky}|{key_id}`
//!
//! Neither field can contain `|`: a pubky is z-base-32 and a key id is 32
//! lowercase hex characters.
//!
//! `GET /v1/me/priv-keys` releases the owner's keys only to a session whose
//! verified grant covers `/priv/pubky.app/` with read and write, the scope
//! that can already read and write the plaintext the key protects. The
//! grant flow requests that scope (`grant::GRANT_REQUEST_CAPABILITIES`), so a
//! Bitkit or Ring grant session qualifies once approved. Narrower sessions —
//! empty-grant bridged sessions, AuthToken sessions without the scope, and
//! grant sessions settled before the scope was requested — get
//! `needs_reauth`.
//!
//! Phase 4 (owner-held keys). A Shop session whose signer delivered scoped
//! encryption keys wraps each data key under a key only that signer can derive
//! and stores the wrapped copy on the owner's homeserver. It then asks
//! `POST /v1/me/priv-keys/release` to drop the service's copies. The service
//! cannot see the wrapped files (`/priv` is readable only by the owner), so the
//! request is the owner's own statement that the wrapped copies exist. The
//! release deletes every sealed key of the owner in one transaction, and only
//! when the request names exactly the key ids the service holds, so a key issued
//! meanwhile is never dropped unseen. A tombstone row then records the release:
//! `GET /v1/me/priv-keys` answers `custody_released` for that owner and never
//! creates a replacement key, which would silently orphan the data the wrapped
//! keys protect. Creating the first key and releasing take the same
//! per-owner advisory lock, so neither can interleave with the other.
//!
//! Rotation and the boot probe follow digital delivery: opens try the
//! current key, then `PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS`; the re-seal
//! pass moves rows to the current key and reports completion from a
//! measured count; the boot probe samples both ends of the table so a wrong
//! or half-rotated key fails startup rather than the first owner's request.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use pubky_common::capabilities::{Action, Capabilities};
use pubky_common::StoragePath;
use rand::RngCore;
use serde_json::json;
use sqlx::PgPool;

use crate::auth::AuthSession;
use crate::clock::format_timestamp;
use crate::digital::{DigitalKeys, ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY};
use crate::locks::{LocksRuntime, ENV_BUNDLE_ENCRYPTION_KEY, ENV_LOOKUP_HMAC_KEY};
use crate::pickup::{PickupKeys, ENV_PICKUP_DETAILS_ENCRYPTION_KEY};
use crate::rotating_keys::{KeyFamily, RotatingKeys};
use crate::{logging, AppState};

pub const ENV_PRIV_DATA_KEY_ENCRYPTION_KEY: &str = "PRIV_DATA_KEY_ENCRYPTION_KEY";
pub const ENV_PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS: &str = "PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS";

/// The homeserver scope whose read and write authority a session must hold
/// before the service releases the key protecting data under it.
pub const PRIV_APP_SCOPE: &str = "/priv/pubky.app/";

/// Length of one data key.
pub const DATA_KEY_LEN: usize = 32;

pub struct PrivDataFamily;

impl KeyFamily for PrivDataFamily {
    const CURRENT_ENV: &'static str = ENV_PRIV_DATA_KEY_ENCRYPTION_KEY;
    const PREVIOUS_ENV: &'static str = ENV_PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS;
    const DESCRIPTION: &'static str = "priv data key";
    const DEBUG_NAME: &'static str = "PrivKeys";
}

/// The configured sealing keys for per-user data keys.
pub type PrivKeys = RotatingKeys<PrivDataFamily>;

pub fn data_key_aad(owner_pubky: &str, key_id: &str) -> Vec<u8> {
    format!("priv-dek|v1|{owner_pubky}|{key_id}").into_bytes()
}

/// Builds the keys from the environment. `None` means key release is off:
/// `GET /v1/me/priv-keys` answers `priv_keys_unavailable` and
/// `/health.priv_keys_available` is false.
pub fn priv_keys_from_env(
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
    digital: Option<&DigitalKeys>,
) -> anyhow::Result<Option<Arc<PrivKeys>>> {
    let Some(keys) = PrivKeys::from_env()? else {
        return Ok(None);
    };
    ensure_distinct_keys(&keys, locks, pickup, digital)?;
    Ok(Some(Arc::new(keys)))
}

/// A shared key would let a dump of one family's ciphertext be opened as
/// another's, so both priv keys must differ from the Locks bundle key, the
/// Locks lookup HMAC key, both pickup keys and both digital delivery keys.
pub fn ensure_distinct_keys(
    keys: &PrivKeys,
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
    digital: Option<&DigitalKeys>,
) -> anyhow::Result<()> {
    for key in keys.key_material() {
        if let Some(locks) = locks {
            if key == locks.keys.encryption_bytes() {
                anyhow::bail!(
                    "{ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} and {ENV_BUNDLE_ENCRYPTION_KEY} must be \
                     distinct keys"
                );
            }
            if key == locks.keys.lookup_hmac_bytes() {
                anyhow::bail!(
                    "{ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} and {ENV_LOOKUP_HMAC_KEY} must be \
                     distinct keys"
                );
            }
        }
        if let Some(pickup) = pickup {
            if pickup.key_material().any(|other| other == key) {
                anyhow::bail!(
                    "{ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} and {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} \
                     must be distinct keys"
                );
            }
        }
        if let Some(digital) = digital {
            if digital.key_material().any(|other| other == key) {
                anyhow::bail!(
                    "{ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} and {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} \
                     must be distinct keys"
                );
            }
        }
    }
    Ok(())
}

/// Whether a verified grant covers `/priv/pubky.app/` with both read and
/// write. A broader directory grant (`/:rw`) covers it; read-only,
/// write-only, narrower, malformed, empty and unrelated grants do not.
pub fn capability_covers_priv_app(raw: &str) -> bool {
    let Ok(capabilities) = raw.parse::<Capabilities>() else {
        return false;
    };
    let required = StoragePath::new(PRIV_APP_SCOPE).expect("priv app scope is canonical");
    capabilities.iter().any(|capability| {
        capability.scope_covers_path(&required)
            && capability.actions().contains(&Action::Read)
            && capability.actions().contains(&Action::Write)
    })
}

/// One opened data key. Held only long enough to serialize the response.
pub struct ReleasedKey {
    pub key_id: String,
    pub key: [u8; DATA_KEY_LEN],
    pub created_at: DateTime<Utc>,
}

type KeyRow = (i64, String, Vec<u8>, DateTime<Utc>);

async fn owner_rows(pool: &PgPool, owner_pubky: &str) -> Result<Vec<KeyRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT generation::BIGINT, key_id, sealed_key, created_at FROM user_priv_keys \
         WHERE owner_pubky = $1 ORDER BY generation ASC",
    )
    .bind(owner_pubky)
    .fetch_all(pool)
    .await
}

/// Raised by [`release_owner_keys`] for an owner whose keys were released to
/// them (Phase 4). The service holds none and must not make a new one.
#[derive(Debug)]
pub struct CustodyReleased;

impl std::fmt::Display for CustodyReleased {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the owner's priv data keys were released to the owner")
    }
}

impl std::error::Error for CustodyReleased {}

async fn lock_owner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    owner_pubky: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('priv-keys|' || $1, 0))")
        .bind(owner_pubky)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn custody_was_released(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    owner_pubky: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM user_priv_key_custody_releases WHERE owner_pubky = $1)",
    )
    .bind(owner_pubky)
    .fetch_one(&mut **tx)
    .await
}

/// Creates the owner's first data key if it has none and never released
/// them. Concurrent callers race on the unique (owner, generation) pair; the
/// loser's insert is a no-op and both then read the one stored key. A release
/// takes the same per-owner lock, so a key cannot appear after it. Returns
/// whether the owner's custody was released instead.
async fn create_first_key(
    pool: &PgPool,
    keys: &PrivKeys,
    owner_pubky: &str,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_owner(&mut tx, owner_pubky).await?;
    if custody_was_released(&mut tx, owner_pubky).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let mut data_key = [0u8; DATA_KEY_LEN];
    rand::rngs::OsRng.fill_bytes(&mut data_key);
    let mut key_id_bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut key_id_bytes);
    let key_id = hex::encode(key_id_bytes);
    let sealed = keys.seal(&data_key_aad(owner_pubky, &key_id), &data_key);
    data_key.fill(0);
    sqlx::query(
        "INSERT INTO user_priv_keys \
             (owner_pubky, generation, key_id, sealed_key, created_at, updated_at) \
         VALUES ($1, 1, $2, $3, $4, $4) \
         ON CONFLICT (owner_pubky, generation) DO NOTHING",
    )
    .bind(owner_pubky)
    .bind(&key_id)
    .bind(&sealed)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(false)
}

/// Returns every data key the owner holds, oldest first, creating the first
/// one on the owner's first request. A stored key that does not open, or
/// opens to the wrong length, is an error: the caller must never receive a
/// fresh key in place of one that already protects data. An owner whose keys
/// were released to them gets [`CustodyReleased`], never a new key.
pub async fn release_owner_keys(
    pool: &PgPool,
    keys: &PrivKeys,
    owner_pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<ReleasedKey>> {
    let mut rows = owner_rows(pool, owner_pubky).await?;
    if rows.is_empty() {
        if create_first_key(pool, keys, owner_pubky, now).await? {
            return Err(CustodyReleased.into());
        }
        rows = owner_rows(pool, owner_pubky).await?;
    }
    rows.into_iter()
        .map(|(_, key_id, sealed, created_at)| {
            let mut opened = keys.open(&data_key_aad(owner_pubky, &key_id), &sealed)?;
            let key = <[u8; DATA_KEY_LEN]>::try_from(opened.as_slice())
                .map_err(|_| anyhow::anyhow!("a stored priv data key has the wrong length"));
            opened.fill(0);
            Ok(ReleasedKey {
                key_id,
                key: key?,
                created_at,
            })
        })
        .collect()
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn refusal(status: StatusCode, code: &str, message: &str) -> Response {
    no_store(
        (
            status,
            Json(json!({
                "schema_version": 1,
                "ok": false,
                "error": { "code": code, "message": message },
            })),
        )
            .into_response(),
    )
}

/// `GET /v1/me/priv-keys`: the session owner's data keys. The owner is
/// always the session's actor; there is no path or query parameter naming
/// another pubky.
pub async fn get_own_priv_keys(
    State(state): State<AppState>,
    Extension(session): Extension<AuthSession>,
) -> Response {
    if !capability_covers_priv_app(&session.capabilities) {
        return refusal(
            StatusCode::FORBIDDEN,
            "needs_reauth",
            "The session grant does not include read and write access to /priv/pubky.app/.",
        );
    }
    let Some(keys) = state.priv_keys.as_deref() else {
        return refusal(
            StatusCode::SERVICE_UNAVAILABLE,
            "priv_keys_unavailable",
            "Private data keys are not available on this deployment.",
        );
    };
    let owner = &session.actor.0;
    let released = match release_owner_keys(&state.pool, keys, owner, state.clock.now()).await {
        Ok(released) => released,
        Err(error) if error.downcast_ref::<CustodyReleased>().is_some() => {
            return refusal(
                StatusCode::CONFLICT,
                "custody_released",
                "The private data keys are held by their owner; the service no longer has them.",
            );
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                actor_prefix = logging::actor_prefix(owner),
                "priv data key release failed"
            );
            return refusal(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "The private data keys could not be read.",
            );
        }
    };
    let current_key_id = released.last().map(|key| key.key_id.clone());
    let entries = released
        .iter()
        .map(|key| {
            json!({
                "key_id": key.key_id,
                "key": URL_SAFE_NO_PAD.encode(key.key),
                "created_at": format_timestamp(key.created_at),
            })
        })
        .collect::<Vec<_>>();
    tracing::info!(
        actor_prefix = logging::actor_prefix(owner),
        keys = entries.len(),
        "released priv data keys"
    );
    no_store(
        (
            StatusCode::OK,
            Json(json!({
                "schema_version": 1,
                "owner": owner,
                "current_key_id": current_key_id,
                "keys": entries,
            })),
        )
            .into_response(),
    )
}

/// What a release request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustodyRelease {
    /// The service holds no key for the owner: the release just happened, or
    /// an earlier one did.
    Released,
    /// The request does not name exactly the keys the service holds. Nothing
    /// was dropped.
    KeySetChanged,
}

/// Drops every sealed key of the owner and records that custody was released,
/// when `key_ids` is exactly the set the service holds. A repeat of a
/// completed release succeeds without looking at `key_ids`.
pub async fn release_owner_custody(
    pool: &PgPool,
    owner_pubky: &str,
    key_ids: &[String],
    now: DateTime<Utc>,
) -> Result<CustodyRelease, sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_owner(&mut tx, owner_pubky).await?;
    if custody_was_released(&mut tx, owner_pubky).await? {
        tx.rollback().await?;
        return Ok(CustodyRelease::Released);
    }
    let held: Vec<String> = sqlx::query_scalar(
        "SELECT key_id FROM user_priv_keys WHERE owner_pubky = $1 \
         ORDER BY generation ASC FOR UPDATE",
    )
    .bind(owner_pubky)
    .fetch_all(&mut *tx)
    .await?;
    let mut held_sorted = held.clone();
    held_sorted.sort();
    let mut named = key_ids.to_vec();
    named.sort();
    if held.is_empty() || held_sorted != named {
        tx.rollback().await?;
        return Ok(CustodyRelease::KeySetChanged);
    }
    sqlx::query("DELETE FROM user_priv_keys WHERE owner_pubky = $1")
        .bind(owner_pubky)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO user_priv_key_custody_releases (owner_pubky, released_at, key_count) \
         VALUES ($1, $2, $3)",
    )
    .bind(owner_pubky)
    .bind(now)
    .bind(held.len() as i32)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(CustodyRelease::Released)
}

/// The most key ids one release request may name. An owner holds one key
/// today; the cap only bounds a hostile body.
const MAX_RELEASE_KEY_IDS: usize = 64;

fn is_key_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Parses `{"key_ids": ["<32 lowercase hex>", ...]}`: non-empty, no repeats,
/// every id well formed, nothing else in the body.
fn parse_release_request(body: &[u8]) -> Option<Vec<String>> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ReleaseRequest {
        key_ids: Vec<String>,
    }
    let request: ReleaseRequest = serde_json::from_slice(body).ok()?;
    let ids = request.key_ids;
    let distinct: std::collections::BTreeSet<&String> = ids.iter().collect();
    if ids.is_empty()
        || ids.len() > MAX_RELEASE_KEY_IDS
        || distinct.len() != ids.len()
        || !ids.iter().all(|id| is_key_id(id))
    {
        return None;
    }
    Some(ids)
}

/// `POST /v1/me/priv-keys/release`: the session owner's statement that every
/// data key the service holds for them is now wrapped on their homeserver. The
/// owner is always the session's actor. Same authorization as the key read.
pub async fn release_own_priv_keys(
    State(state): State<AppState>,
    Extension(session): Extension<AuthSession>,
    body: Bytes,
) -> Response {
    if !capability_covers_priv_app(&session.capabilities) {
        return refusal(
            StatusCode::FORBIDDEN,
            "needs_reauth",
            "The session grant does not include read and write access to /priv/pubky.app/.",
        );
    }
    if state.priv_keys.is_none() {
        return refusal(
            StatusCode::SERVICE_UNAVAILABLE,
            "priv_keys_unavailable",
            "Private data keys are not available on this deployment.",
        );
    }
    let Some(key_ids) = parse_release_request(&body) else {
        return refusal(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Expected a JSON body {\"key_ids\": [...]} naming each key by its 32-character lowercase hex id.",
        );
    };
    let owner = &session.actor.0;
    match release_owner_custody(&state.pool, owner, &key_ids, state.clock.now()).await {
        Ok(CustodyRelease::Released) => {
            tracing::info!(
                actor_prefix = logging::actor_prefix(owner),
                keys = key_ids.len(),
                "released priv data key custody to the owner"
            );
            no_store(
                (
                    StatusCode::OK,
                    Json(json!({
                        "schema_version": 1,
                        "owner": owner,
                        "released": true,
                    })),
                )
                    .into_response(),
            )
        }
        Ok(CustodyRelease::KeySetChanged) => refusal(
            StatusCode::CONFLICT,
            "key_set_changed",
            "The keys held for this owner are not the keys named in the request.",
        ),
        Err(error) => {
            tracing::error!(
                error = %error,
                actor_prefix = logging::actor_prefix(owner),
                "priv data key custody release failed"
            );
            refusal(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "The private data keys could not be released.",
            )
        }
    }
}

struct SealedRow {
    id: i64,
    aad: Vec<u8>,
    ciphertext: Vec<u8>,
}

enum Direction {
    Ascending { after: i64 },
    Descending,
}

async fn sealed_rows(
    pool: &PgPool,
    direction: Direction,
    limit: i64,
) -> Result<Vec<SealedRow>, sqlx::Error> {
    let (filter, order, after) = match direction {
        Direction::Ascending { after } => ("id > $2", "id ASC", after),
        Direction::Descending => ("$2 = $2", "id DESC", 0),
    };
    let rows: Vec<(i64, String, String, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT id, owner_pubky, key_id, sealed_key FROM user_priv_keys WHERE {filter} \
         ORDER BY {order} LIMIT $1"
    ))
    .bind(limit)
    .bind(after)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, owner, key_id, ciphertext)| SealedRow {
            id,
            aad: data_key_aad(&owner, &key_id),
            ciphertext,
        })
        .collect())
}

pub const PROBE_BATCH_SIZE: i64 = 100;
/// The probe is a boot check, not a table scan: at most this many ascending
/// batches, plus the table's last batch.
pub const PROBE_MAX_BATCHES: u32 = 3;

/// What the boot probe saw.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ProbeScan {
    /// Distinct rows opened across both sampled ends.
    pub probed: u64,
    /// The table's row count when it holds more rows than were probed.
    pub table_total: Option<u64>,
    /// Whether a probed row opened under the current key.
    pub current_class: bool,
    /// Whether a probed row opened only under the previous key.
    pub straggler_class: bool,
}

/// The boot check, run after migrations: refuses to start when sealed rows
/// exist without the key, or when any sampled row, at either end of the
/// table, opens under neither the current nor the previous key.
pub async fn assert_priv_key_sealing_coherent(
    pool: &PgPool,
    keys: Option<&PrivKeys>,
) -> anyhow::Result<Option<ProbeScan>> {
    let (exists,): (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM user_priv_keys)")
        .fetch_one(pool)
        .await?;
    if !exists {
        return Ok(None);
    }
    let Some(keys) = keys else {
        anyhow::bail!(
            "sealed priv data keys exist but {ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} is not configured"
        );
    };
    let mut scan = ProbeScan::default();
    let classify = |row: &SealedRow, scan: &mut ProbeScan| -> anyhow::Result<()> {
        if keys.opens_under_current(&row.aad, &row.ciphertext) {
            scan.current_class = true;
        } else if keys.open_under_previous(&row.aad, &row.ciphertext).is_ok() {
            scan.straggler_class = true;
        } else {
            anyhow::bail!(
                "sealed priv data keys do not open under the configured \
                 {ENV_PRIV_DATA_KEY_ENCRYPTION_KEY} (current or previous)"
            );
        }
        scan.probed += 1;
        Ok(())
    };
    let mut after = 0i64;
    let mut batches = 0u32;
    let mut stopped_early = false;
    loop {
        let rows = sealed_rows(pool, Direction::Ascending { after }, PROBE_BATCH_SIZE).await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = last.id;
        batches += 1;
        for row in &rows {
            classify(row, &mut scan)?;
        }
        // Without a previous key there is no legitimate straggler class to
        // hunt for once a current-key row is confirmed.
        if (scan.current_class && (scan.straggler_class || !keys.has_previous()))
            || batches >= PROBE_MAX_BATCHES
        {
            stopped_early = true;
            break;
        }
    }
    for row in sealed_rows(pool, Direction::Descending, PROBE_BATCH_SIZE).await? {
        if row.id > after {
            classify(&row, &mut scan)?;
        }
    }
    if stopped_early {
        let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM user_priv_keys")
            .fetch_one(pool)
            .await?;
        let total = total as u64;
        scan.table_total = (total > scan.probed).then_some(total);
        if let Some(total) = scan.table_total {
            tracing::warn!(
                total,
                probed = scan.probed,
                "priv data key boot probe was partial: only both ends of the table were \
                 sampled; rows in between are the re-seal job's responsibility"
            );
        }
    }
    Ok(Some(scan))
}

/// Progress of one re-seal pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResealProgress {
    pub resealed: u64,
    /// Rows whose ciphertext changed between the read and the write.
    pub skipped_changed: u64,
    pub remaining_under_previous: u64,
}

const RESEAL_BATCH_SIZE: i64 = 100;

/// Runs between a re-seal pass reading a row and writing it back. The pass
/// takes no row lock, so tests use this seam to commit a concurrent write
/// at exactly that point.
pub trait ResealHook: Send + Sync {
    fn before_write<'a>(
        &'a self,
        row_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

struct NoResealHook;

impl ResealHook for NoResealHook {
    fn before_write<'a>(
        &'a self,
        _row_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

/// One re-seal pass: pages every row by id, re-seals rows still under the
/// previous key, and reports completion from a measured count. A row that
/// opens under neither key is logged by id and counted; the pass continues
/// and then fails, so one corrupt row cannot stall the rotation of every row
/// after it. A write lands only if the row still holds the ciphertext the
/// pass read.
pub async fn reseal_previous_key_batch(
    pool: &PgPool,
    keys: &PrivKeys,
    now: DateTime<Utc>,
) -> anyhow::Result<ResealProgress> {
    reseal_previous_key_batch_with_hook(pool, keys, now, &NoResealHook).await
}

/// [`reseal_previous_key_batch`] with a [`ResealHook`] between each read and
/// its write.
pub async fn reseal_previous_key_batch_with_hook(
    pool: &PgPool,
    keys: &PrivKeys,
    now: DateTime<Utc>,
    hook: &dyn ResealHook,
) -> anyhow::Result<ResealProgress> {
    if !keys.has_previous() {
        return Ok(ResealProgress::default());
    }
    let mut progress = ResealProgress::default();
    let mut unopenable = 0u64;
    let mut after = 0i64;
    loop {
        let rows = sealed_rows(pool, Direction::Ascending { after }, RESEAL_BATCH_SIZE).await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = last.id;
        for row in rows {
            if keys.opens_under_current(&row.aad, &row.ciphertext) {
                continue;
            }
            let Ok(mut plaintext) = keys.open_under_previous(&row.aad, &row.ciphertext) else {
                unopenable += 1;
                tracing::error!(
                    row_id = row.id,
                    "sealed priv data key opens under neither key; continuing"
                );
                continue;
            };
            hook.before_write(row.id).await;
            let resealed = keys.seal(&row.aad, &plaintext);
            plaintext.fill(0);
            let written = sqlx::query(
                "UPDATE user_priv_keys SET sealed_key = $3, updated_at = $4 \
                 WHERE id = $1 AND sealed_key = $2",
            )
            .bind(row.id)
            .bind(&row.ciphertext)
            .bind(&resealed)
            .bind(now)
            .execute(pool)
            .await?
            .rows_affected();
            if written == 1 {
                progress.resealed += 1;
            } else {
                progress.skipped_changed += 1;
            }
        }
    }
    if unopenable > 0 {
        anyhow::bail!(
            "{unopenable} sealed priv data key(s) open under neither the current nor the previous \
             key (their ids were logged); the rest of the pass completed"
        );
    }
    let mut remaining = 0u64;
    let mut after = 0i64;
    loop {
        let rows = sealed_rows(pool, Direction::Ascending { after }, RESEAL_BATCH_SIZE).await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = last.id;
        remaining += rows
            .iter()
            .filter(|row| !keys.opens_under_current(&row.aad, &row.ciphertext))
            .count() as u64;
    }
    progress.remaining_under_previous = remaining;
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aad_binds_owner_and_key_id_with_a_distinct_prefix() {
        let owner = "o".repeat(52);
        let aad = String::from_utf8(data_key_aad(&owner, &"a".repeat(32))).unwrap();
        assert_eq!(aad, format!("priv-dek|v1|{owner}|{}", "a".repeat(32)));
        for other in [
            "digital-version/v1|",
            "digital-pin/v1|",
            "buyer-email/v1|",
            "pubky-priv-aead/v1",
        ] {
            assert!(!aad.starts_with(other));
        }
    }

    #[test]
    fn priv_app_coverage_requires_read_and_write_over_the_whole_scope() {
        for covered in [
            "/:rw",
            "/priv/:rw",
            "/priv/pubky.app/:rw",
            "/pub/pubky.app/:rw,/priv/pubky.app/:rw",
        ] {
            assert!(capability_covers_priv_app(covered), "{covered}");
        }
        for refused in [
            "",
            "garbage",
            "/priv/pubky.app/:r",
            "/priv/pubky.app/:w",
            "/priv/pubky.app/marketplace/:rw",
            "/pub/pubky.app/:rw",
            "/pub/pubky.app/marketplace-service/v1/:rw",
            "/priv/other.app/:rw",
        ] {
            assert!(!capability_covers_priv_app(refused), "{refused}");
        }
    }
}
