//! Digital delivery sealing, key rotation and boot probe
//! (digital-delivery-design.md §4.1, §7).
//!
//! One key, `DIGITAL_DELIVERY_ENCRYPTION_KEY`, seals three families with the
//! [`crate::seal`] construction (XChaCha20-Poly1305, fresh 24-byte nonce per
//! seal). Associated data is UTF-8 joined with `|`; every field is an id,
//! integer or fixed token that cannot contain `|`, and each family has its
//! own prefix, distinct from the browser's `pubky-marketplace-deliverable/v1`:
//!
//! | Family | Table | Associated data |
//! |---|---|---|
//! | Deliverable version | `listing_digital_versions` | `digital-version/v1\|{listing}\|{deliverable_id}\|{version}\|{kind}` |
//! | Order pin | `order_digital_pins` | `digital-pin/v1\|{order_id}\|{line_index}\|{listing}\|{deliverable_id}\|{version}` |
//! | Buyer email | `order_delivery_emails` | `buyer-email/v1\|{order_id}\|{buyer_pubky}` |
//!
//! A row moved to another listing, deliverable, version, kind, order, line
//! or buyer fails to open. The service opens a version or pin only to hand
//! its payload to the entitled buyer; it never decrypts a file.
//!
//! Rotation is pickup's dual-key read window: opens try the current key,
//! then `DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS`; the re-seal pass walks
//! all three families and rotation is complete when a measured sweep finds
//! zero rows under the previous key. The boot probe is bounded: it samples
//! both ends of each family, so a wrong or half-rotated key fails startup
//! rather than the first buyer's download.

use std::sync::Arc;

use marketplace_domain::commands::DigitalDeliveryKind;
use sqlx::PgPool;
use uuid::Uuid;

use crate::locks::{LocksRuntime, ENV_BUNDLE_ENCRYPTION_KEY, ENV_LOOKUP_HMAC_KEY};
use crate::pickup::{PickupKeys, ENV_PICKUP_DETAILS_ENCRYPTION_KEY};
use crate::seal::{self, KEY_LEN};

pub const ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY: &str = "DIGITAL_DELIVERY_ENCRYPTION_KEY";
pub const ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS: &str =
    "DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS";

/// The configured sealing keys. Seals always use the current key.
pub struct DigitalKeys {
    current: [u8; KEY_LEN],
    previous: Option<[u8; KEY_LEN]>,
}

impl std::fmt::Debug for DigitalKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DigitalKeys(<redacted>)")
    }
}

impl DigitalKeys {
    pub fn from_hex(current_hex: &str, previous_hex: Option<&str>) -> anyhow::Result<Self> {
        let current = seal::parse_key(ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY, current_hex)?;
        let previous = previous_hex
            .map(|hex| seal::parse_key(ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS, hex))
            .transpose()?;
        if previous.as_ref() == Some(&current) {
            anyhow::bail!(
                "{ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} and \
                 {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS} must be distinct keys"
            );
        }
        Ok(Self { current, previous })
    }

    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        seal::seal(&self.current, aad, plaintext)
    }

    pub fn open(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        if let Ok(plaintext) = seal::open(&self.current, aad, sealed) {
            return Ok(plaintext);
        }
        if let Some(previous) = &self.previous {
            return seal::open(previous, aad, sealed);
        }
        Err(anyhow::anyhow!(
            "ciphertext did not authenticate under the configured digital delivery key"
        ))
    }

    fn opens_under_current(&self, aad: &[u8], sealed: &[u8]) -> bool {
        seal::open(&self.current, aad, sealed).is_ok()
    }

    fn open_under_previous(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Some(previous) = &self.previous else {
            anyhow::bail!("no previous digital delivery key configured");
        };
        seal::open(previous, aad, sealed)
    }

    fn key_material(&self) -> impl Iterator<Item = &[u8; KEY_LEN]> {
        std::iter::once(&self.current).chain(self.previous.iter())
    }
}

pub fn version_aad(
    listing_aggregate_id: &str,
    deliverable_id: &str,
    version: i64,
    kind: DigitalDeliveryKind,
) -> Vec<u8> {
    format!(
        "digital-version/v1|{listing_aggregate_id}|{deliverable_id}|{version}|{}",
        kind.as_str()
    )
    .into_bytes()
}

pub fn pin_aad(
    order_id: Uuid,
    line_index: i32,
    listing_aggregate_id: &str,
    deliverable_id: &str,
    version: i64,
) -> Vec<u8> {
    format!(
        "digital-pin/v1|{order_id}|{line_index}|{listing_aggregate_id}|{deliverable_id}|{version}"
    )
    .into_bytes()
}

pub fn email_aad(order_id: Uuid, buyer_pubky: &str) -> Vec<u8> {
    format!("buyer-email/v1|{order_id}|{buyer_pubky}").into_bytes()
}

/// Builds the keys from the environment. `None` means digital delivery is
/// off: `digital_delivery.set` and digital checkout lines are refused and
/// `/health.digital_delivery_available` is false. The key must differ from
/// every other sealing and HMAC key the service holds.
pub fn digital_keys_from_env(
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
) -> anyhow::Result<Option<Arc<DigitalKeys>>> {
    let current = std::env::var(ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY).ok();
    let previous = std::env::var(ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS).ok();
    let Some(current) = current else {
        if previous.is_some() {
            anyhow::bail!(
                "{ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS} requires \
                 {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY}"
            );
        }
        return Ok(None);
    };
    let keys = DigitalKeys::from_hex(&current, previous.as_deref())?;
    ensure_distinct_keys(&keys, locks, pickup)?;
    Ok(Some(Arc::new(keys)))
}

/// A shared key would let a dump of one family's ciphertext be opened as
/// another's, so both digital keys must differ from the Locks bundle key,
/// the Locks lookup HMAC key and both pickup keys.
pub fn ensure_distinct_keys(
    keys: &DigitalKeys,
    locks: Option<&LocksRuntime>,
    pickup: Option<&PickupKeys>,
) -> anyhow::Result<()> {
    for key in keys.key_material() {
        if let Some(locks) = locks {
            if key == locks.keys.encryption_bytes() {
                anyhow::bail!(
                    "{ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} and {ENV_BUNDLE_ENCRYPTION_KEY} must \
                     be distinct keys"
                );
            }
            if key == locks.keys.lookup_hmac_bytes() {
                anyhow::bail!(
                    "{ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} and {ENV_LOOKUP_HMAC_KEY} must be \
                     distinct keys"
                );
            }
        }
        if let Some(pickup) = pickup {
            if pickup.key_material().any(|pickup_key| pickup_key == key) {
                anyhow::bail!(
                    "{ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} and {ENV_PICKUP_DETAILS_ENCRYPTION_KEY} \
                     must be distinct keys"
                );
            }
        }
    }
    Ok(())
}

/// The three sealed families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedFamily {
    Version,
    Pin,
    Email,
}

impl SealedFamily {
    pub const ALL: [SealedFamily; 3] = [
        SealedFamily::Version,
        SealedFamily::Pin,
        SealedFamily::Email,
    ];

    pub fn name(self) -> &'static str {
        match self {
            SealedFamily::Version => "version",
            SealedFamily::Pin => "pin",
            SealedFamily::Email => "email",
        }
    }
}

/// One sealed row: its key, its associated data and its ciphertext.
struct SealedRow {
    id: i64,
    aad: Vec<u8>,
    ciphertext: Vec<u8>,
}

type VersionRowTuple = (i64, String, String, i64, String, Vec<u8>);
type PinRowTuple = (i64, Uuid, i32, String, String, i64, Vec<u8>);

enum Direction {
    Ascending { after: i64 },
    Descending,
}

async fn family_rows(
    pool: &PgPool,
    family: SealedFamily,
    direction: Direction,
    limit: i64,
) -> Result<Vec<SealedRow>, sqlx::Error> {
    let (filter, order, after) = match direction {
        Direction::Ascending { after } => ("id > $2", "id ASC", after),
        Direction::Descending => ("$2 = $2", "id DESC", 0),
    };
    Ok(match family {
        SealedFamily::Version => {
            let rows: Vec<VersionRowTuple> = sqlx::query_as(&format!(
                "SELECT id, listing_aggregate_id, deliverable_id, version, kind, \
                     payload_ciphertext FROM listing_digital_versions WHERE {filter} \
                     ORDER BY {order} LIMIT $1"
            ))
            .bind(limit)
            .bind(after)
            .fetch_all(pool)
            .await?;
            // `kind` is CHECK-constrained to the five tokens `parse` accepts.
            rows.into_iter()
                .filter_map(|(id, listing, deliverable, version, kind, ciphertext)| {
                    let kind = DigitalDeliveryKind::parse(&kind)?;
                    Some(SealedRow {
                        id,
                        aad: version_aad(&listing, &deliverable, version, kind),
                        ciphertext,
                    })
                })
                .collect()
        }
        SealedFamily::Pin => {
            let rows: Vec<PinRowTuple> = sqlx::query_as(&format!(
                "SELECT id, order_id, line_index, listing_aggregate_id, deliverable_id, \
                     version, payload_ciphertext FROM order_digital_pins WHERE {filter} \
                     ORDER BY {order} LIMIT $1"
            ))
            .bind(limit)
            .bind(after)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(
                    |(id, order_id, line_index, listing, deliverable, version, ciphertext)| {
                        SealedRow {
                            id,
                            aad: pin_aad(order_id, line_index, &listing, &deliverable, version),
                            ciphertext,
                        }
                    },
                )
                .collect()
        }
        SealedFamily::Email => {
            let rows: Vec<(i64, Uuid, String, Vec<u8>)> = sqlx::query_as(&format!(
                "SELECT id, order_id, buyer_pubky, email_ciphertext FROM order_delivery_emails \
                 WHERE email_ciphertext IS NOT NULL AND {filter} ORDER BY {order} LIMIT $1"
            ))
            .bind(limit)
            .bind(after)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(|(id, order_id, buyer, ciphertext)| SealedRow {
                    id,
                    aad: email_aad(order_id, &buyer),
                    ciphertext,
                })
                .collect()
        }
    })
}

async fn family_count(pool: &PgPool, family: SealedFamily) -> Result<i64, sqlx::Error> {
    let sql = match family {
        SealedFamily::Version => "SELECT COUNT(*) FROM listing_digital_versions",
        SealedFamily::Pin => "SELECT COUNT(*) FROM order_digital_pins",
        SealedFamily::Email => {
            "SELECT COUNT(*) FROM order_delivery_emails WHERE email_ciphertext IS NOT NULL"
        }
    };
    let (count,): (i64,) = sqlx::query_as(sql).fetch_one(pool).await?;
    Ok(count)
}

pub const PROBE_BATCH_SIZE: i64 = 100;
/// The probe is a boot check, not a table scan: at most this many ascending
/// batches per family, plus the family's last batch.
pub const PROBE_MAX_BATCHES: u32 = 3;

/// What the probe saw in one family.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ProbeScan {
    /// Rows the ascending scan paged.
    pub scanned: u64,
    /// Distinct rows opened across both sampled ends.
    pub probed: u64,
    /// The family's row count when it holds more rows than were probed.
    pub family_total: Option<u64>,
    /// Rows found under the current key / under neither-current (straggler
    /// class), within the probed set.
    pub current_class: bool,
    pub straggler_class: bool,
}

async fn probe_family(
    pool: &PgPool,
    keys: &DigitalKeys,
    family: SealedFamily,
) -> anyhow::Result<ProbeScan> {
    let mut scan = ProbeScan::default();
    let mut samples: Vec<SealedRow> = Vec::new();
    let mut after = 0i64;
    let mut batches = 0u32;
    let mut stopped_early = false;
    let classify = |row: SealedRow, scan: &mut ProbeScan, samples: &mut Vec<SealedRow>| {
        let current = keys.opens_under_current(&row.aad, &row.ciphertext);
        if current && !scan.current_class {
            scan.current_class = true;
            samples.push(row);
        } else if !current && !scan.straggler_class {
            scan.straggler_class = true;
            samples.push(row);
        }
    };
    loop {
        let rows = family_rows(
            pool,
            family,
            Direction::Ascending { after },
            PROBE_BATCH_SIZE,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        batches += 1;
        after = rows.last().map(|row| row.id).unwrap_or(after);
        for row in rows {
            scan.scanned += 1;
            classify(row, &mut scan, &mut samples);
        }
        // Without a previous key there is no legitimate straggler class to
        // hunt for once a current-key row is confirmed.
        let settled = (scan.current_class && (scan.straggler_class || !keys.has_previous()))
            || batches >= PROBE_MAX_BATCHES;
        if settled {
            stopped_early = true;
            break;
        }
    }
    scan.probed = scan.scanned;
    for row in family_rows(pool, family, Direction::Descending, PROBE_BATCH_SIZE).await? {
        if row.id <= after {
            continue;
        }
        scan.probed += 1;
        classify(row, &mut scan, &mut samples);
    }
    if stopped_early {
        let total = family_count(pool, family).await? as u64;
        scan.family_total = (total > scan.probed).then_some(total);
    }
    for row in samples {
        keys.open(&row.aad, &row.ciphertext).map_err(|_| {
            anyhow::anyhow!(
                "sealed digital delivery {} rows do not open under the configured \
                 {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} (current or previous)",
                family.name()
            )
        })?;
    }
    Ok(scan)
}

async fn sealed_rows_exist(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let (exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM listing_digital_versions) \
            OR EXISTS(SELECT 1 FROM order_digital_pins) \
            OR EXISTS(SELECT 1 FROM order_delivery_emails WHERE email_ciphertext IS NOT NULL)",
    )
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// The boot check, run after migrations: refuses to start when sealed rows
/// exist without the key, or when a sampled row of either key class, at
/// either end of any family, opens under neither the current nor the
/// previous key.
pub async fn assert_digital_sealing_coherent(
    pool: &PgPool,
    keys: Option<&DigitalKeys>,
) -> anyhow::Result<Vec<(SealedFamily, ProbeScan)>> {
    if !sealed_rows_exist(pool).await? {
        return Ok(Vec::new());
    }
    let Some(keys) = keys else {
        anyhow::bail!(
            "sealed digital delivery rows exist but {ENV_DIGITAL_DELIVERY_ENCRYPTION_KEY} is not \
             configured"
        );
    };
    let mut scans = Vec::with_capacity(SealedFamily::ALL.len());
    for family in SealedFamily::ALL {
        let scan = probe_family(pool, keys, family).await?;
        if let Some(total) = scan.family_total {
            tracing::warn!(
                family = family.name(),
                total,
                probed = scan.probed,
                "digital delivery boot probe was partial: only both ends of the family were \
                 sampled; rows in between are the re-seal job's responsibility"
            );
        }
        scans.push((family, scan));
    }
    Ok(scans)
}

/// Progress of one re-seal pass over all three families.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResealProgress {
    pub versions_resealed: u64,
    pub pins_resealed: u64,
    pub emails_resealed: u64,
    pub remaining_under_previous: u64,
}

const RESEAL_BATCH_SIZE: i64 = 100;

async fn write_resealed(
    pool: &PgPool,
    family: SealedFamily,
    id: i64,
    ciphertext: &[u8],
) -> Result<(), sqlx::Error> {
    let sql = match family {
        SealedFamily::Version => {
            "UPDATE listing_digital_versions SET payload_ciphertext = $2 WHERE id = $1"
        }
        SealedFamily::Pin => "UPDATE order_digital_pins SET payload_ciphertext = $2 WHERE id = $1",
        SealedFamily::Email => {
            "UPDATE order_delivery_emails SET email_ciphertext = $2 \
             WHERE id = $1 AND email_ciphertext IS NOT NULL"
        }
    };
    sqlx::query(sql)
        .bind(id)
        .bind(ciphertext)
        .execute(pool)
        .await?;
    Ok(())
}

/// One re-seal pass: pages every row of each family by id, re-seals rows
/// still under the previous key, and reports completion from a measured
/// count. A row that opens under neither key is logged by id and counted;
/// the pass continues and then fails, so one corrupt row cannot stall the
/// rotation of every row after it.
pub async fn reseal_previous_key_batch(
    pool: &PgPool,
    keys: &DigitalKeys,
) -> anyhow::Result<ResealProgress> {
    if !keys.has_previous() {
        return Ok(ResealProgress::default());
    }
    let mut progress = ResealProgress::default();
    let mut unopenable = 0u64;
    for family in SealedFamily::ALL {
        let mut after = 0i64;
        loop {
            let rows = family_rows(
                pool,
                family,
                Direction::Ascending { after },
                RESEAL_BATCH_SIZE,
            )
            .await?;
            let Some(last) = rows.last() else {
                break;
            };
            after = last.id;
            for row in rows {
                if keys.opens_under_current(&row.aad, &row.ciphertext) {
                    continue;
                }
                let Ok(plaintext) = keys.open_under_previous(&row.aad, &row.ciphertext) else {
                    unopenable += 1;
                    tracing::error!(
                        family = family.name(),
                        row_id = row.id,
                        "sealed digital delivery row opens under neither key; continuing"
                    );
                    continue;
                };
                write_resealed(pool, family, row.id, &keys.seal(&row.aad, &plaintext)).await?;
                match family {
                    SealedFamily::Version => progress.versions_resealed += 1,
                    SealedFamily::Pin => progress.pins_resealed += 1,
                    SealedFamily::Email => progress.emails_resealed += 1,
                }
            }
        }
    }
    if unopenable > 0 {
        anyhow::bail!(
            "{unopenable} sealed digital delivery row(s) open under neither the current nor the \
             previous key (their ids were logged); the rest of the pass completed"
        );
    }
    progress.remaining_under_previous = count_rows_under_previous(pool, keys).await?;
    Ok(progress)
}

async fn count_rows_under_previous(pool: &PgPool, keys: &DigitalKeys) -> anyhow::Result<u64> {
    let mut remaining = 0u64;
    for family in SealedFamily::ALL {
        let mut after = 0i64;
        loop {
            let rows = family_rows(
                pool,
                family,
                Direction::Ascending { after },
                RESEAL_BATCH_SIZE,
            )
            .await?;
            let Some(last) = rows.last() else {
                break;
            };
            after = last.id;
            remaining += rows
                .iter()
                .filter(|row| keys.open_under_previous(&row.aad, &row.ciphertext).is_ok())
                .count() as u64;
        }
    }
    Ok(remaining)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CURRENT: &str = "6666666666666666666666666666666666666666666666666666666666666666";
    const PREVIOUS: &str = "7777777777777777777777777777777777777777777777777777777777777777";

    #[test]
    fn family_prefixes_are_distinct_and_fields_cannot_contain_the_separator() {
        let order = Uuid::nil();
        let version = String::from_utf8(version_aad(
            "listing:s_l",
            "0123456789abcdef0123456789abcdef",
            1,
            DigitalDeliveryKind::File,
        ))
        .unwrap();
        let pin = String::from_utf8(pin_aad(
            order,
            0,
            "listing:s_l",
            "0123456789abcdef0123456789abcdef",
            1,
        ))
        .unwrap();
        let email = String::from_utf8(email_aad(order, "buyer")).unwrap();
        assert!(version.starts_with("digital-version/v1|"));
        assert!(pin.starts_with("digital-pin/v1|"));
        assert!(email.starts_with("buyer-email/v1|"));
        for aad in [&version, &pin, &email] {
            assert!(!aad.starts_with("pubky-marketplace-deliverable/v1"));
        }
        assert_eq!(version.matches('|').count(), 4);
        assert_eq!(pin.matches('|').count(), 5);
        assert_eq!(email.matches('|').count(), 2);
    }

    #[test]
    fn keys_parse_fail_closed_and_debug_is_redacted() {
        DigitalKeys::from_hex("zz", None).expect_err("non-hex key rejected");
        DigitalKeys::from_hex(CURRENT, Some(CURRENT)).expect_err("equal keys rejected");
        let keys = DigitalKeys::from_hex(CURRENT, Some(PREVIOUS)).expect("keys parse");
        let debug = format!("{keys:?}");
        assert!(!debug.contains(CURRENT) && !debug.contains(PREVIOUS));
    }
}
