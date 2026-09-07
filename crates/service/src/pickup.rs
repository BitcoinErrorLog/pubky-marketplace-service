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

/// One boot-probe row per sealed family: the AAD inputs and the ciphertext.
struct ProbeRow {
    aad: Vec<u8>,
    ciphertext: Vec<u8>,
}

async fn first_details_probe_row(pool: &PgPool) -> Result<Option<ProbeRow>, sqlx::Error> {
    let row: Option<(String, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
         ORDER BY aggregate_id, version LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(aggregate_id, version, ciphertext)| ProbeRow {
        aad: details_aad(&aggregate_id, version),
        ciphertext,
    }))
}

async fn first_snapshot_probe_row(pool: &PgPool) -> Result<Option<ProbeRow>, sqlx::Error> {
    let row: Option<(Uuid, i32, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT order_id, line_index, version, snapshot_ciphertext FROM pickup_line_snapshots \
         ORDER BY order_id, line_index LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(order_id, line_index, version, ciphertext)| ProbeRow {
            aad: snapshot_aad(order_id, line_index, version),
            ciphertext,
        }),
    )
}

/// The all-or-none boot check, run at startup AFTER migrations (the schema
/// must exist first): refuses to start when sealed pickup rows exist
/// without `PICKUP_DETAILS_ENCRYPTION_KEY` configured, and attempts ONE
/// real open — current key, then previous — across BOTH sealed families (a
/// details version and a pinned snapshot), so a wrong or half-rotated key
/// fails the boot rather than the first buyer's reveal (§A8).
pub async fn assert_pickup_sealing_coherent(
    pool: &PgPool,
    keys: Option<&PickupKeys>,
) -> anyhow::Result<()> {
    let details_row = first_details_probe_row(pool).await?;
    let snapshot_row = first_snapshot_probe_row(pool).await?;
    if details_row.is_none() && snapshot_row.is_none() {
        return Ok(());
    }
    let Some(keys) = keys else {
        anyhow::bail!(
            "sealed pickup rows exist but {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} is not configured"
        );
    };
    for (family, row) in [("details", details_row), ("snapshot", snapshot_row)] {
        if let Some(row) = row {
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

/// One re-seal pass: walks a batch of rows in each sealed family, opens
/// each under the current key (rows already rotated are skipped), opens
/// stragglers under the PREVIOUS key and re-seals them under the current
/// one, then counts what remains under the previous key across both
/// families. Runs on server time through the worker runtime; rows that
/// authenticate under NEITHER key abort the pass loudly — a wrong key must
/// never be silently skipped.
pub async fn reseal_previous_key_batch(
    pool: &PgPool,
    keys: &PickupKeys,
    now: DateTime<Utc>,
) -> anyhow::Result<ResealProgress> {
    if !keys.has_previous() {
        return Ok(ResealProgress::default());
    }
    let mut progress = ResealProgress::default();

    // Family 1: details versions.
    let details: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details \
         ORDER BY aggregate_id, version LIMIT $1",
    )
    .bind(RESEAL_BATCH_SIZE)
    .fetch_all(pool)
    .await?;
    for (aggregate_id, version, ciphertext) in &details {
        let aad = details_aad(aggregate_id, *version);
        if keys.opens_under_current(&aad, ciphertext) {
            continue;
        }
        let plaintext = keys.open_under_previous(&aad, ciphertext).map_err(|_| {
            anyhow::anyhow!(
                "listing_pickup_details ({aggregate_id}, v{version}) opens under neither the \
                 current nor the previous pickup key"
            )
        })?;
        let resealed = keys.seal(&aad, &plaintext);
        sqlx::query(
            "UPDATE listing_pickup_details SET details_ciphertext = $3, updated_at = $4 \
             WHERE aggregate_id = $1 AND version = $2",
        )
        .bind(aggregate_id)
        .bind(version)
        .bind(&resealed)
        .bind(now)
        .execute(pool)
        .await?;
        progress.details_resealed += 1;
    }

    // Family 2: pinned payment snapshots.
    let snapshots: Vec<(Uuid, i32, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT order_id, line_index, version, snapshot_ciphertext FROM pickup_line_snapshots \
         ORDER BY order_id, line_index LIMIT $1",
    )
    .bind(RESEAL_BATCH_SIZE)
    .fetch_all(pool)
    .await?;
    for (order_id, line_index, version, ciphertext) in &snapshots {
        let aad = snapshot_aad(*order_id, *line_index, *version);
        if keys.opens_under_current(&aad, ciphertext) {
            continue;
        }
        let plaintext = keys.open_under_previous(&aad, ciphertext).map_err(|_| {
            anyhow::anyhow!(
                "pickup_line_snapshots ({order_id}, line {line_index}, v{version}) opens under \
                 neither the current nor the previous pickup key"
            )
        })?;
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

    // The completion criterion spans BOTH families: rotation is complete
    // only when zero rows in either family remain sealed under the
    // previous key. The probe opens are cheap (AEAD over small payloads)
    // and the job only runs while a previous key is configured.
    let mut remaining = 0u64;
    for (aggregate_id, version, ciphertext) in sqlx::query_as::<_, (String, i64, Vec<u8>)>(
        "SELECT aggregate_id, version, details_ciphertext FROM listing_pickup_details",
    )
    .fetch_all(pool)
    .await?
    {
        if !keys.opens_under_current(&details_aad(&aggregate_id, version), &ciphertext) {
            remaining += 1;
        }
    }
    for (order_id, line_index, version, ciphertext) in
        sqlx::query_as::<_, (Uuid, i32, i64, Vec<u8>)>(
            "SELECT order_id, line_index, version, snapshot_ciphertext FROM pickup_line_snapshots",
        )
        .fetch_all(pool)
        .await?
    {
        if !keys.opens_under_current(&snapshot_aad(order_id, line_index, version), &ciphertext) {
            remaining += 1;
        }
    }
    progress.remaining_under_previous = remaining;
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
