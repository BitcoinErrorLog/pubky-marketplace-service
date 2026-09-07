//! Local pickup sealing, key rotation, boot probe, and retention (local
//! pickup design PART A, §A1/§A3).
//!
//! Two sealed families reuse the [`crate::seal`] construction (XChaCha20-
//! Poly1305, fresh nonce per seal, no plaintext serialization path):
//!
//! 1. **Details versions** (`listing_pickup_details`): the seller-authored
//!    meeting point, sealed with AAD = listing aggregate id ‖ details
//!    version.
//! 2. **Pinned payment snapshots** (`pickup_line_snapshots`): the details
//!    as shown to the buyer at payment, sealed per order line with AAD =
//!    order id ‖ line index ‖ version.
//!
//! Key rotation is a dual-key read window: opens try the current key, then
//! `PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS`; the re-seal job walks rows
//! still sealed under the previous key in BOTH families and re-seals them
//! under the current one, and rotation is complete only when zero rows in
//! either family remain under the previous key (§A1). A wrong or
//! half-rotated key fails the boot probe ([`assert_pickup_sealing_coherent`])
//! at startup, not the first buyer's reveal.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::locks::{LocksRuntime, ENV_BUNDLE_ENCRYPTION_KEY, ENV_LOOKUP_HMAC_KEY};
use crate::seal::{self, KEY_LEN};

/// Environment variable holding the 32-byte hex pickup-details encryption
/// key. Pickup is OFF unless it is configured (all-or-none gating, §A8).
pub const ENV_PICKUP_DETAILS_ENCRYPTION_KEY: &str = "PICKUP_DETAILS_ENCRYPTION_KEY";
/// Optional previous key for the dual-key read window during rotation.
pub const ENV_PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS: &str =
    "PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS";

/// The configured pickup sealing keys. Seals always use the current key;
/// opens try current-then-previous (the dual-key read window, §A1).
pub struct PickupKeys {
    current: [u8; KEY_LEN],
    previous: Option<[u8; KEY_LEN]>,
}

/// Key material never appears in logs, not even truncated.
impl std::fmt::Debug for PickupKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PickupKeys(<redacted>)")
    }
}

impl PickupKeys {
    pub fn from_hex(current_hex: &str, previous_hex: Option<&str>) -> anyhow::Result<Self> {
        let current = seal::parse_key(ENV_PICKUP_DETAILS_ENCRYPTION_KEY, current_hex)?;
        let previous = previous_hex
            .map(|hex| seal::parse_key(ENV_PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS, hex))
            .transpose()?;
        if previous.as_ref() == Some(&current) {
            anyhow::bail!(
                "{ENV_PICKUP_DETAILS_ENCRYPTION_KEY} and \
                 {ENV_PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS} must be distinct keys"
            );
        }
        Ok(Self { current, previous })
    }

    /// Whether a previous key is configured (the dual-key read window is
    /// open and the re-seal job has work to do).
    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    /// Seals under the CURRENT key. Only ever called with fresh plaintext;
    /// re-sealing during rotation opens first and seals again under current.
    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        seal::seal(&self.current, aad, plaintext)
    }

    /// Opens under the current key, then the previous one (the dual-key
    /// read window). Fails when neither key authenticates the ciphertext.
    pub fn open(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        if let Ok(plaintext) = seal::open(&self.current, aad, sealed) {
            return Ok(plaintext);
        }
        if let Some(previous) = &self.previous {
            return seal::open(previous, aad, sealed);
        }
        Err(anyhow::anyhow!(
            "ciphertext did not authenticate under the configured pickup key"
        ))
    }

    /// True when the ciphertext authenticates under the CURRENT key (used by
    /// the re-seal job to find rows still sealed under the previous key).
    fn opens_under_current(&self, aad: &[u8], sealed: &[u8]) -> bool {
        seal::open(&self.current, aad, sealed).is_ok()
    }

    /// Opens strictly under the previous key (re-seal job only).
    fn open_under_previous(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Some(previous) = &self.previous else {
            anyhow::bail!("no previous pickup key configured");
        };
        seal::open(previous, aad, sealed)
    }
}

/// The AAD binding a details-version ciphertext to its listing aggregate
/// and version (§A1).
pub fn details_aad(aggregate_id: &str, version: i64) -> Vec<u8> {
    format!("{aggregate_id}\n{version}").into_bytes()
}

/// The AAD binding a pinned snapshot to its order, line, and version —
/// order id ‖ line index ‖ version (§A3) — so a snapshot cannot be
/// transplanted across orders, lines, or versions.
pub fn snapshot_aad(order_id: Uuid, line_index: i32, version: i64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(32);
    aad.extend_from_slice(order_id.as_bytes());
    aad.extend_from_slice(format!("\n{line_index}\n{version}").as_bytes());
    aad
}

/// Builds the pickup keys from the environment (all-or-none), enforcing
/// distinctness from the Locks key material (§A1: the existing distinctness
/// check is the template). `None` means pickup is OFF: the service refuses
/// `pickup_details.set` and reports `pickup_available` false.
pub fn pickup_keys_from_env(
    locks: Option<&LocksRuntime>,
) -> anyhow::Result<Option<Arc<PickupKeys>>> {
    let current = std::env::var(ENV_PICKUP_DETAILS_ENCRYPTION_KEY).ok();
    let previous = std::env::var(ENV_PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS).ok();
    let Some(current) = current else {
        if previous.is_some() {
            anyhow::bail!(
                "{ENV_PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS} requires \
                 {ENV_PICKUP_DETAILS_ENCRYPTION_KEY}"
            );
        }
        return Ok(None);
    };
    let keys = PickupKeys::from_hex(&current, previous.as_deref())?;
    ensure_distinct_from_locks(&keys, locks)?;
    Ok(Some(Arc::new(keys)))
}

/// The pickup key must be distinct from the Locks key material (§A1: the
/// existing distinctness check is the template) — a shared key would let a
/// dump of one family's ciphertext be opened for the other.
pub(crate) fn ensure_distinct_from_locks(
    keys: &PickupKeys,
    locks: Option<&LocksRuntime>,
) -> anyhow::Result<()> {
    if let Some(locks) = locks {
        if keys.current == *locks.keys.encryption_bytes() {
            anyhow::bail!(
                "{ENV_PICKUP_DETAILS_ENCRYPTION_KEY} and {ENV_BUNDLE_ENCRYPTION_KEY} must be \
                 distinct keys"
            );
        }
        // The Locks runtime holds both Locks keys; the HMAC key is equally
        // Locks key material and must not alias the pickup key either.
        if keys.current == *locks.keys.lookup_hmac_bytes() {
            anyhow::bail!(
                "{ENV_PICKUP_DETAILS_ENCRYPTION_KEY} and {ENV_LOOKUP_HMAC_KEY} must be \
                 distinct keys"
            );
        }
    }
    Ok(())
}

/// One boot-probe row: the AAD inputs and the ciphertext.
struct ProbeRow {
    aad: Vec<u8>,
    ciphertext: Vec<u8>,
}

/// Whether any sealed pickup rows exist at all (the probe is vacuous on an
/// empty store, and an unkeyed deployment may start only then).
async fn sealed_rows_exist(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let (exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM listing_pickup_details) \
            OR EXISTS(SELECT 1 FROM pickup_line_snapshots)",
    )
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

const PROBE_BATCH_SIZE: i64 = 100;

/// The probe is a BOOT check, not a table scan: at most this many batches
/// per family. In the steady state every row opens under the current key
/// and the "still under previous" class never appears, so an unbounded
/// probe would page the whole table — one AEAD open per row — on every
/// start. When a previous key IS configured, stragglers past the bound are
/// the re-seal job's responsibility, not the probe's.
const PROBE_MAX_BATCHES: u32 = 3;

/// The outcome of probing one sealed family: one row per key class found
/// within the bound, plus the number of rows scanned (the observability
/// counter proving the probe stays bounded).
struct ProbeScan {
    rows: Vec<ProbeRow>,
    scanned: u64,
}

/// Whether the probe may stop paging: both key classes were found, or no
/// previous key is configured and the current-key class is already
/// confirmed — without a rotation window there is no legitimate straggler
/// class to hunt for (a same-batch straggler still surfaces, because the
/// batch is always classified in full before this check runs).
fn probe_scan_complete(probes: &[Option<ProbeRow>; 2], keys: &PickupKeys, batches: u32) -> bool {
    probes.iter().all(Option::is_some)
        || (!keys.has_previous() && probes[0].is_some())
        || batches >= PROBE_MAX_BATCHES
}

/// One probe row per KEY CLASS of the details family: the first row that
/// opens under the current key and the first that does not (still sealed
/// under the previous key). Probing only the first row of the family could
/// miss a half-rotated table whose previous key was dropped — the straggler
/// class must be probed too, or it would fail the first buyer's reveal
/// instead of the boot (§A8). Bounded to [`PROBE_MAX_BATCHES`] batches.
async fn details_probe_rows(pool: &PgPool, keys: &PickupKeys) -> Result<ProbeScan, sqlx::Error> {
    // [opens under the current key, does not] — one probe row each.
    let mut probes: [Option<ProbeRow>; 2] = [None, None];
    let mut scanned = 0u64;
    let mut batches = 0u32;
    let mut cursor: Option<(String, i64)> = None;
    loop {
        let rows: Vec<(String, i64, Vec<u8>)> =
            match &cursor {
                None => sqlx::query_as(
                    "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
                     ORDER BY aggregate_id, version LIMIT $1",
                )
                .bind(PROBE_BATCH_SIZE)
                .fetch_all(pool)
                .await?,
                Some((after_id, after_version)) => sqlx::query_as(
                    "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
                     WHERE (aggregate_id, version) > ($1, $2) \
                     ORDER BY aggregate_id, version LIMIT $3",
                )
                .bind(after_id)
                .bind(after_version)
                .bind(PROBE_BATCH_SIZE)
                .fetch_all(pool)
                .await?,
            };
        if rows.is_empty() {
            break;
        }
        batches += 1;
        cursor = rows
            .last()
            .map(|(aggregate_id, version, _)| (aggregate_id.clone(), *version));
        for (aggregate_id, version, ciphertext) in rows {
            scanned += 1;
            let aad = details_aad(&aggregate_id, version);
            let class = usize::from(!keys.opens_under_current(&aad, &ciphertext));
            if probes[class].is_none() {
                probes[class] = Some(ProbeRow { aad, ciphertext });
            }
        }
        if probe_scan_complete(&probes, keys, batches) {
            break;
        }
    }
    Ok(ProbeScan {
        rows: probes.into_iter().flatten().collect(),
        scanned,
    })
}

/// The snapshot family's [`details_probe_rows`]: one probe row per key
/// class, paged on (order_id, line_index), under the same batch bound.
async fn snapshot_probe_rows(pool: &PgPool, keys: &PickupKeys) -> Result<ProbeScan, sqlx::Error> {
    let mut probes: [Option<ProbeRow>; 2] = [None, None];
    let mut scanned = 0u64;
    let mut batches = 0u32;
    let mut cursor: Option<(Uuid, i32)> = None;
    loop {
        let rows: Vec<(Uuid, i32, i64, Vec<u8>)> = match &cursor {
            None => {
                sqlx::query_as(
                    "SELECT order_id, line_index, version, snapshot_ciphertext \
                     FROM pickup_line_snapshots ORDER BY order_id, line_index LIMIT $1",
                )
                .bind(PROBE_BATCH_SIZE)
                .fetch_all(pool)
                .await?
            }
            Some((after_order, after_line)) => {
                sqlx::query_as(
                    "SELECT order_id, line_index, version, snapshot_ciphertext \
                     FROM pickup_line_snapshots \
                     WHERE (order_id, line_index) > ($1, $2) \
                     ORDER BY order_id, line_index LIMIT $3",
                )
                .bind(after_order)
                .bind(after_line)
                .bind(PROBE_BATCH_SIZE)
                .fetch_all(pool)
                .await?
            }
        };
        if rows.is_empty() {
            break;
        }
        batches += 1;
        cursor = rows
            .last()
            .map(|(order_id, line_index, _, _)| (*order_id, *line_index));
        for (order_id, line_index, version, ciphertext) in rows {
            scanned += 1;
            let aad = snapshot_aad(order_id, line_index, version);
            let class = usize::from(!keys.opens_under_current(&aad, &ciphertext));
            if probes[class].is_none() {
                probes[class] = Some(ProbeRow { aad, ciphertext });
            }
        }
        if probe_scan_complete(&probes, keys, batches) {
            break;
        }
    }
    Ok(ProbeScan {
        rows: probes.into_iter().flatten().collect(),
        scanned,
    })
}

/// The all-or-none boot check, run at startup AFTER migrations (the schema
/// must exist first): refuses to start when sealed pickup rows exist
/// without `PICKUP_DETAILS_ENCRYPTION_KEY` configured, and attempts real
/// opens — current key, then previous — across BOTH sealed families and
/// BOTH key classes of each (a row already under the current key AND a row
/// still under the previous one, when such rows exist), so a wrong key or a
/// half-rotated table whose previous key was dropped fails the boot rather
/// than the first buyer's reveal (§A8).
pub async fn assert_pickup_sealing_coherent(
    pool: &PgPool,
    keys: Option<&PickupKeys>,
) -> anyhow::Result<()> {
    if !sealed_rows_exist(pool).await? {
        return Ok(());
    }
    let Some(keys) = keys else {
        anyhow::bail!(
            "sealed pickup rows exist but {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} is not configured"
        );
    };
    for (family, scan) in [
        ("details", details_probe_rows(pool, keys).await?),
        ("snapshot", snapshot_probe_rows(pool, keys).await?),
    ] {
        tracing::debug!(
            family,
            scanned = scan.scanned,
            "pickup boot probe scanned the sealed family (bounded)"
        );
        for row in scan.rows {
            keys.open(&row.aad, &row.ciphertext).map_err(|_| {
                anyhow::anyhow!(
                    "sealed pickup {family} rows do not open under the configured \
                     {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} (current or previous)"
                )
            })?;
        }
    }
    Ok(())
}

/// Progress of one re-seal pass over BOTH sealed families. Rotation is
/// complete only when `remaining_under_previous` is zero across both
/// families (§A1).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResealProgress {
    pub details_resealed: u64,
    pub snapshots_resealed: u64,
    pub remaining_under_previous: u64,
}

const RESEAL_BATCH_SIZE: i64 = 100;

/// One re-seal pass: keyset-paginates EVERY row of each sealed family in
/// primary-key order (a bare `LIMIT` batch would re-read the same already
/// rotated rows on every pass and never reach the rows past it, so the job
/// could never report completion), opens each under the current key (rows
/// already rotated are skipped), opens stragglers under the PREVIOUS key
/// and re-seals them under the current one. Runs on server time through
/// the worker runtime.
///
/// A row that authenticates under NEITHER key no longer aborts the pass:
/// its identity (aggregate id/version or order id/line index — never
/// plaintext) is recorded, the pass continues over the remaining rows, and
/// the pass returns an error at the end naming how many rows were
/// unopenable — a wrong key must never be silently skipped, but one
/// corrupt row must not stall the rotation of every row after it.
///
/// The completion criterion is folded into the same streamed pass rather
/// than a second fetch-all sweep: the pass already classifies every row of
/// both families exactly once, so `remaining_under_previous` is the rows
/// that failed the current-key open minus the rows this pass re-sealed —
/// unopenable rows are NOT counted as "under previous" (they open under
/// neither key). Rotation is complete only when it reaches zero (§A1).
pub async fn reseal_previous_key_batch(
    pool: &PgPool,
    keys: &PickupKeys,
    _now: DateTime<Utc>,
) -> anyhow::Result<ResealProgress> {
    if !keys.has_previous() {
        return Ok(ResealProgress::default());
    }
    let mut progress = ResealProgress::default();
    // Rows that failed the current-key open, per family, so the completion
    // count can be derived without re-opening a single ciphertext.
    let mut failed_current = 0u64;
    let mut unopenable = 0u64;

    // Family 1: details versions, paged on (aggregate_id, version).
    let mut cursor: Option<(String, i64)> = None;
    loop {
        let details: Vec<(String, i64, Vec<u8>)> =
            match &cursor {
                None => sqlx::query_as(
                    "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
                     ORDER BY aggregate_id, version LIMIT $1",
                )
                .bind(RESEAL_BATCH_SIZE)
                .fetch_all(pool)
                .await?,
                Some((after_id, after_version)) => sqlx::query_as(
                    "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
                     WHERE (aggregate_id, version) > ($1, $2) \
                     ORDER BY aggregate_id, version LIMIT $3",
                )
                .bind(after_id)
                .bind(after_version)
                .bind(RESEAL_BATCH_SIZE)
                .fetch_all(pool)
                .await?,
            };
        if details.is_empty() {
            break;
        }
        cursor = details
            .last()
            .map(|(aggregate_id, version, _)| (aggregate_id.clone(), *version));
        for (aggregate_id, version, ciphertext) in &details {
            let aad = details_aad(aggregate_id, *version);
            if keys.opens_under_current(&aad, ciphertext) {
                continue;
            }
            failed_current += 1;
            let plaintext = match keys.open_under_previous(&aad, ciphertext) {
                Ok(plaintext) => plaintext,
                Err(_) => {
                    unopenable += 1;
                    tracing::error!(
                        aggregate_id = %aggregate_id,
                        version = version,
                        "listing_pickup_details row opens under neither the current nor the \
                         previous pickup key; skipping it and continuing the re-seal pass"
                    );
                    continue;
                }
            };
            let resealed = keys.seal(&aad, &plaintext);
            // The re-seal deliberately does NOT touch `updated_at`: that
            // timestamp is the owner-read "details last edited" fact, and a
            // key rotation is not an edit.
            sqlx::query(
                "UPDATE listing_pickup_details SET details_ciphertext = $3 \
                 WHERE aggregate_id = $1 AND version = $2",
            )
            .bind(aggregate_id)
            .bind(version)
            .bind(&resealed)
            .execute(pool)
            .await?;
            progress.details_resealed += 1;
        }
    }

    // Family 2: pinned payment snapshots, paged on (order_id, line_index).
    let mut cursor: Option<(Uuid, i32)> = None;
    loop {
        let snapshots: Vec<(Uuid, i32, i64, Vec<u8>)> = match &cursor {
            None => {
                sqlx::query_as(
                    "SELECT order_id, line_index, version, snapshot_ciphertext \
                     FROM pickup_line_snapshots ORDER BY order_id, line_index LIMIT $1",
                )
                .bind(RESEAL_BATCH_SIZE)
                .fetch_all(pool)
                .await?
            }
            Some((after_order, after_line)) => {
                sqlx::query_as(
                    "SELECT order_id, line_index, version, snapshot_ciphertext \
                     FROM pickup_line_snapshots \
                     WHERE (order_id, line_index) > ($1, $2) \
                     ORDER BY order_id, line_index LIMIT $3",
                )
                .bind(after_order)
                .bind(after_line)
                .bind(RESEAL_BATCH_SIZE)
                .fetch_all(pool)
                .await?
            }
        };
        if snapshots.is_empty() {
            break;
        }
        cursor = snapshots
            .last()
            .map(|(order_id, line_index, _, _)| (*order_id, *line_index));
        for (order_id, line_index, version, ciphertext) in &snapshots {
            let aad = snapshot_aad(*order_id, *line_index, *version);
            if keys.opens_under_current(&aad, ciphertext) {
                continue;
            }
            failed_current += 1;
            let plaintext = match keys.open_under_previous(&aad, ciphertext) {
                Ok(plaintext) => plaintext,
                Err(_) => {
                    unopenable += 1;
                    tracing::error!(
                        order_id = %order_id,
                        line_index = line_index,
                        version = version,
                        "pickup_line_snapshots row opens under neither the current nor the \
                         previous pickup key; skipping it and continuing the re-seal pass"
                    );
                    continue;
                }
            };
            let resealed = keys.seal(&aad, &plaintext);
            sqlx::query(
                "UPDATE pickup_line_snapshots SET snapshot_ciphertext = $4 \
                 WHERE order_id = $1 AND line_index = $2 AND version = $3",
            )
            .bind(order_id)
            .bind(line_index)
            .bind(version)
            .bind(&resealed)
            .execute(pool)
            .await?;
            progress.snapshots_resealed += 1;
        }
    }

    // The completion criterion spans BOTH families: rotation is complete
    // only when zero rows in either family remain sealed under the
    // previous key. Every row that failed the current-key open was either
    // re-sealed above (no longer under previous) or recorded unopenable
    // (under NEITHER key — not counted here); seals only ever write the
    // current key, so nothing new enters the previous class mid-pass.
    progress.remaining_under_previous = failed_current
        .saturating_sub(progress.details_resealed + progress.snapshots_resealed)
        .saturating_sub(unopenable);
    if unopenable > 0 {
        anyhow::bail!(
            "{unopenable} sealed pickup row(s) open under neither the current nor the previous \
             pickup key (their identities were logged); the rest of the pass completed"
        );
    }
    Ok(progress)
}

/// The retention purge (§A3): hard-deletes pinned snapshots whose
/// referencing orders are terminal, and detail versions no snapshot
/// references any more. A cancelled-after-payment order's snapshot is the
/// dispute exhibit: it outlives the cancel until the seller's refund
/// evidence is recorded (`refund.record_external` sets
/// `orders.external_refund`), after which it purges with the rest; when no
/// evidence ever lands, the ordinary terminal-order purge takes it once
/// `dispute_retention_days` have elapsed since the order closed. The
/// current (latest) details version of each listing is never purged here —
//  only `pickup_details.clear` removes live details.
pub async fn purge_terminal_pickup_retention(
    pool: &PgPool,
    now: DateTime<Utc>,
    dispute_retention_days: i64,
) -> anyhow::Result<(u64, u64)> {
    let dispute_cutoff = now - chrono::Duration::days(dispute_retention_days);
    let snapshots_purged = sqlx::query(
        "DELETE FROM pickup_line_snapshots s USING orders o \
         WHERE o.id = s.order_id \
           AND o.state IN ('completed', 'cancelled', 'refunded_external', 'closed') \
           AND (o.state <> 'cancelled' \
                OR o.external_refund IS NOT NULL \
                OR o.updated_at <= $1)",
    )
    .bind(dispute_cutoff)
    .execute(pool)
    .await?
    .rows_affected();

    // Detail versions no remaining snapshot references, except the live
    // ones: the current (max) version of a non-cleared listing is never
    // purged here; after a clear, EVERY retained version is a dispute
    // exhibit and purges once nothing references it.
    let versions_purged = sqlx::query(
        "DELETE FROM listing_pickup_details d \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM pickup_line_snapshots s \
             WHERE s.listing_aggregate_id = d.aggregate_id AND s.version = d.version) \
           AND (EXISTS ( \
                    SELECT 1 FROM listing_pickup_version_counters c \
                    WHERE c.aggregate_id = d.aggregate_id AND c.cleared_at IS NOT NULL) \
                OR d.version < (SELECT MAX(latest.version) FROM listing_pickup_details latest \
                                WHERE latest.aggregate_id = d.aggregate_id))",
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok((snapshots_purged, versions_purged))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CURRENT_KEY: &str = "3333333333333333333333333333333333333333333333333333333333333333";
    const PREVIOUS_KEY: &str = "4444444444444444444444444444444444444444444444444444444444444444";
    const OTHER_KEY: &str = "5555555555555555555555555555555555555555555555555555555555555555";
    const SPOT: &str = "Central Station, north entrance";

    fn keys() -> PickupKeys {
        PickupKeys::from_hex(CURRENT_KEY, None).expect("keys parse")
    }

    #[test]
    fn details_round_trip_with_fresh_nonces_and_bound_aad() {
        let keys = keys();
        let aad = details_aad("listing:seller_boots_01", 1);
        let first = keys.seal(&aad, SPOT.as_bytes());
        let second = keys.seal(&aad, SPOT.as_bytes());
        assert_ne!(first, second, "each seal uses a fresh random nonce");
        assert_eq!(keys.open(&aad, &first).unwrap(), SPOT.as_bytes());
        // Wrong AAD (another listing, another version) fails the open.
        keys.open(&details_aad("listing:seller_boots_02", 1), &first)
            .expect_err("a transplanted details ciphertext must not open");
        keys.open(&details_aad("listing:seller_boots_01", 2), &first)
            .expect_err("a different version must not open");
        // Wrong key fails the open.
        PickupKeys::from_hex(OTHER_KEY, None)
            .expect("other key parses")
            .open(&aad, &first)
            .expect_err("a different key must not open");
        // The ciphertext never contains the plaintext.
        assert!(!first
            .windows(SPOT.len())
            .any(|window| window == SPOT.as_bytes()));
    }

    #[test]
    fn snapshot_aad_binds_order_line_and_version() {
        let keys = keys();
        let order_id = Uuid::new_v4();
        let aad = snapshot_aad(order_id, 0, 1);
        let sealed = keys.seal(&aad, SPOT.as_bytes());
        keys.open(&snapshot_aad(Uuid::new_v4(), 0, 1), &sealed)
            .expect_err("a wrong order id must not open");
        keys.open(&snapshot_aad(order_id, 1, 1), &sealed)
            .expect_err("a wrong line index must not open");
        keys.open(&snapshot_aad(order_id, 0, 2), &sealed)
            .expect_err("a wrong version must not open");
        assert_eq!(keys.open(&aad, &sealed).unwrap(), SPOT.as_bytes());
    }

    #[test]
    fn dual_key_window_opens_previous_key_rows_and_debug_is_redacted() {
        let rotated =
            PickupKeys::from_hex(CURRENT_KEY, Some(PREVIOUS_KEY)).expect("rotated keys parse");
        let previous_only = PickupKeys::from_hex(PREVIOUS_KEY, None).expect("previous key parses");
        let aad = details_aad("listing:seller_boots_01", 3);
        let sealed_under_previous = previous_only.seal(&aad, SPOT.as_bytes());
        // The dual-key read window opens it; a current-only key cannot.
        assert_eq!(
            rotated.open(&aad, &sealed_under_previous).unwrap(),
            SPOT.as_bytes()
        );
        keys()
            .open(&aad, &sealed_under_previous)
            .expect_err("the previous key's rows need the read window");
        // Current-key rows open identically under the window.
        let sealed_under_current = keys().seal(&aad, SPOT.as_bytes());
        assert_eq!(
            rotated.open(&aad, &sealed_under_current).unwrap(),
            SPOT.as_bytes()
        );
        let debug = format!("{:?}", rotated);
        assert!(!debug.contains(CURRENT_KEY) && !debug.contains(PREVIOUS_KEY));
    }

    #[test]
    fn key_parsing_fails_closed() {
        PickupKeys::from_hex("not-hex", None).expect_err("non-hex current key rejected");
        PickupKeys::from_hex(CURRENT_KEY, Some("abcd")).expect_err("short previous key rejected");
        PickupKeys::from_hex(CURRENT_KEY, Some(CURRENT_KEY))
            .expect_err("identical current/previous keys rejected");
    }

    // The boot probe is a bounded check, not a table scan: on a large
    // all-current table it must not page every row (one AEAD open per row)
    // on every start. With no previous key configured the first all-current
    // batch settles the family; with a previous key configured the hunt for
    // the straggler class stops at PROBE_MAX_BATCHES (stragglers beyond the
    // bound are the re-seal job's responsibility).
    #[sqlx::test]
    async fn boot_probe_stays_bounded_on_a_large_all_current_table(pool: PgPool) {
        let keys = keys();
        let rotated =
            PickupKeys::from_hex(CURRENT_KEY, Some(PREVIOUS_KEY)).expect("rotated keys parse");
        let aggregate = "listing:probe_bound";
        let now = Utc::now();
        let row_count = PROBE_BATCH_SIZE * i64::from(PROBE_MAX_BATCHES) + 1;
        for version in 1..=row_count {
            let ciphertext = keys.seal(&details_aad(aggregate, version), b"spot");
            sqlx::query(
                "INSERT INTO listing_pickup_details (aggregate_id, seller_pubky, version, \
                 details_ciphertext, created_at, updated_at) VALUES ($1, 's', $2, $3, $4, $4)",
            )
            .bind(aggregate)
            .bind(version)
            .bind(&ciphertext)
            .bind(now)
            .execute(&pool)
            .await
            .expect("seed details");
        }

        // No previous key: one batch (the first all-current one) settles
        // the family — the remaining rows are never paged, let alone opened.
        let scan = details_probe_rows(&pool, &keys).await.expect("probe runs");
        assert_eq!(scan.scanned, PROBE_BATCH_SIZE as u64);
        assert_eq!(scan.rows.len(), 1, "only the current key class exists");
        assert_pickup_sealing_coherent(&pool, Some(&keys))
            .await
            .expect("all-current table without a previous key boots");

        // A configured previous key keeps hunting the straggler class, but
        // the hunt is capped at PROBE_MAX_BATCHES batches.
        let scan = details_probe_rows(&pool, &rotated)
            .await
            .expect("probe runs");
        assert_eq!(
            scan.scanned,
            PROBE_BATCH_SIZE as u64 * u64::from(PROBE_MAX_BATCHES)
        );
        assert_pickup_sealing_coherent(&pool, Some(&rotated))
            .await
            .expect("all-current table with a previous key boots");
    }

    struct NoLocksClient;
    impl crate::locks::LocksLifecycleClient for NoLocksClient {
        fn lookup<'a>(
            &'a self,
            _creator: &'a str,
            _bundle_id: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::locks::LocksLookupOutcome> + Send + 'a>,
        > {
            Box::pin(async { crate::locks::LocksLookupOutcome::Unavailable })
        }
    }

    fn locks_runtime(encryption: &str, hmac: &str) -> LocksRuntime {
        LocksRuntime {
            keys: crate::locks::LocksKeys::from_hex(encryption, hmac).expect("locks keys parse"),
            client: Arc::new(NoLocksClient),
        }
    }

    #[test]
    fn pickup_key_must_differ_from_locks_key_material() {
        let locks_enc = "1111111111111111111111111111111111111111111111111111111111111111";
        let locks_mac = "2222222222222222222222222222222222222222222222222222222222222222";
        let locks = locks_runtime(locks_enc, locks_mac);
        // A distinct pickup key passes.
        ensure_distinct_from_locks(&keys(), Some(&locks)).expect("distinct keys pass");
        // Aliasing either Locks key fails closed.
        let alias_enc = PickupKeys::from_hex(locks_enc, None).expect("parses");
        ensure_distinct_from_locks(&alias_enc, Some(&locks))
            .expect_err("pickup key aliasing the Locks encryption key rejected");
        let alias_mac = PickupKeys::from_hex(locks_mac, None).expect("parses");
        ensure_distinct_from_locks(&alias_mac, Some(&locks))
            .expect_err("pickup key aliasing the Locks HMAC key rejected");
        // No Locks configured: nothing to alias.
        ensure_distinct_from_locks(&keys(), None).expect("no locks runtime passes");
    }
}
