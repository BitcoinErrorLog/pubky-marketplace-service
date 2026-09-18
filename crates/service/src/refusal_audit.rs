//! Privacy-bounded, asynchronous recording of designed command refusals.
//!
//! The request path constructs fixed-size envelopes only. Database delivery is
//! owned by a separate task and must never be awaited by a command handler.

use std::fmt;
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Timelike, Utc};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

const HKDF_SALT: &[u8] = b"marketplace/refusal-audit/hkdf-salt/v1";
const ACTOR_INFO: &[u8] = b"marketplace/refusal-audit/actor-key/v1";
const SAMPLE_INFO: &[u8] = b"marketplace/refusal-audit/sample-key/v1";
const ACTOR_DOMAIN: &[u8] = b"marketplace/refusal-audit/actor-tag/v1";
const SAMPLE_DOMAIN: &[u8] = b"marketplace/refusal-audit/sample-command-tag/v1";

pub const RETENTION_DAYS: i64 = 30;
pub const QUEUE_CAPACITY: usize = 4_096;
pub const MAX_ADMITTED_ROWS_PER_HOUR: i32 = 10_000;
pub const WRITER_LOGIN: &str = "marketplace_refusal_audit_writer_login";
pub const RETENTION_LOGIN: &str = "marketplace_refusal_audit_retention";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum SurfaceKind {
    V1Command = 1,
    BitcoinManualResolve = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum RefusalKind {
    InvalidEnvelope = 1,
    InvalidCommand = 2,
    Unauthorized = 3,
    NotFound = 4,
    RevisionConflict = 5,
    IdempotencyConflict = 6,
    InsufficientInventory = 7,
    InvariantViolation = 8,
    OfferExpired = 9,
    InvalidState = 10,
    AuctionClosed = 11,
    BidTooLow = 12,
    UpstreamUnavailable = 13,
    AwardExpired = 14,
    AwardAlreadyConverted = 15,
    AwardQuantityMismatch = 16,
    AwardVariantMismatch = 17,
    AwardListingChanged = 18,
    AwardHoldMissing = 19,
    ManualResolveConfirmationObservationMismatch = 20,
    ManualResolveConfirmationEffectsFailed = 21,
    ManualResolveInvalidReason = 22,
    ManualResolveInvalidIdempotencyKey = 23,
    ManualResolveInvalidOutcome = 24,
    ManualResolveInvalidRefundReference = 25,
    ManualResolveNotOrderSeller = 26,
    ManualResolveOrderNotFound = 27,
    ManualResolveOrderNotAwaitingConfirmation = 28,
    ManualResolveNotApplicable = 29,
    ManualResolveMissingPin = 30,
    ManualResolveConflict = 31,
    ManualResolveAlreadyResolved = 32,
    ManualResolveNotInReview = 33,
    ManualResolveStockUnavailable = 34,
}

#[derive(Debug, Clone)]
pub struct RefusalEnvelope {
    pub occurred_at: DateTime<Utc>,
    pub surface: SurfaceKind,
    pub command_kind: i16,
    pub refusal_kind: RefusalKind,
    pub actor_epoch: i16,
    pub actor_tag: [u8; 16],
    pub sample_command_tag: Option<[u8; 16]>,
    pub command_id_present: bool,
}

#[derive(Clone)]
pub struct RefusalAuditRuntime {
    sender: mpsc::Sender<RefusalEnvelope>,
    keys: AuditKeys,
}

impl fmt::Debug for RefusalAuditRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RefusalAuditRuntime")
            .field("active_epoch", &self.keys.active_epoch)
            .finish_non_exhaustive()
    }
}

impl RefusalAuditRuntime {
    pub fn spawn(pool: PgPool, keys: AuditKeys) -> Self {
        let (sender, mut receiver) = mpsc::channel(QUEUE_CAPACITY);
        tokio::spawn(async move {
            while let Some(envelope) = receiver.recv().await {
                // The consumer alone owns the writer connection work. A
                // failure is deliberately logged and discarded: delivery is
                // observability, never part of a marketplace command result.
                if let Err(error) = deliver(&pool, &envelope).await {
                    tracing::warn!(error = %error, "refusal audit delivery failed");
                }
            }
        });
        Self { sender, keys }
    }

    pub fn envelope(
        &self,
        occurred_at: DateTime<Utc>,
        surface: SurfaceKind,
        command_kind: i16,
        refusal_kind: RefusalKind,
        actor: &str,
        command_id: Option<Uuid>,
    ) -> anyhow::Result<RefusalEnvelope> {
        Ok(RefusalEnvelope {
            occurred_at,
            surface,
            command_kind,
            refusal_kind,
            actor_epoch: self.keys.active_epoch,
            actor_tag: self.keys.actor_tag(actor)?,
            sample_command_tag: command_id
                .map(|id| self.keys.sample_tag(surface, actor, id))
                .transpose()?,
            command_id_present: command_id.is_some(),
        })
    }

    /// This never awaits queue capacity or does database work.
    pub fn try_send(&self, envelope: RefusalEnvelope) {
        if self.sender.try_send(envelope).is_err() {
            tracing::warn!("refusal audit envelope dropped");
        }
    }
}

async fn deliver(pool: &PgPool, envelope: &RefusalEnvelope) -> Result<(), sqlx::Error> {
    let bucket = envelope
        .occurred_at
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("valid timestamp hour");
    tokio::time::timeout(Duration::from_millis(250), async {
        let mut transaction = pool.begin().await?;
        sqlx::query("SET LOCAL statement_timeout = '200ms'; SET LOCAL lock_timeout = '50ms'")
            .execute(&mut *transaction)
            .await?;
        let updated = sqlx::query(
            "UPDATE command_refusal_audit_buckets SET \
             occurrence_count = CASE WHEN occurrence_count = 9223372036854775807 THEN occurrence_count ELSE occurrence_count + 1 END, \
             count_saturated = count_saturated OR occurrence_count = 9223372036854775807, \
             last_occurred_at = GREATEST(last_occurred_at, $1) \
             WHERE bucket_start = $2 AND surface_kind = $3 AND command_kind = $4 \
             AND refusal_kind = $5 AND actor_key_epoch = $6 AND actor_tag = $7",
        )
        .bind(envelope.occurred_at)
        .bind(bucket)
        .bind(envelope.surface as i16)
        .bind(envelope.command_kind)
        .bind(envelope.refusal_kind as i16)
        .bind(envelope.actor_epoch)
        .bind(envelope.actor_tag.as_slice())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 32))")
                .bind(bucket.to_rfc3339())
                .execute(&mut *transaction)
                .await?;
            let second_update = sqlx::query(
                "UPDATE command_refusal_audit_buckets SET occurrence_count = occurrence_count + 1, \
                 last_occurred_at = GREATEST(last_occurred_at, $1) WHERE bucket_start = $2 \
                 AND surface_kind = $3 AND command_kind = $4 AND refusal_kind = $5 \
                 AND actor_key_epoch = $6 AND actor_tag = $7",
            )
            .bind(envelope.occurred_at).bind(bucket).bind(envelope.surface as i16)
            .bind(envelope.command_kind).bind(envelope.refusal_kind as i16)
            .bind(envelope.actor_epoch).bind(envelope.actor_tag.as_slice())
            .execute(&mut *transaction).await?;
            if second_update.rows_affected() == 0 {
                sqlx::query(
                    "INSERT INTO command_refusal_audit_bucket_limits (bucket_start, admitted_rows) VALUES ($1, 0) \
                     ON CONFLICT (bucket_start) DO NOTHING",
                ).bind(bucket).execute(&mut *transaction).await?;
                let admitted: i32 = sqlx::query_scalar(
                    "SELECT admitted_rows FROM command_refusal_audit_bucket_limits WHERE bucket_start = $1 FOR UPDATE",
                ).bind(bucket).fetch_one(&mut *transaction).await?;
                if admitted < MAX_ADMITTED_ROWS_PER_HOUR {
                    sqlx::query(
                        "INSERT INTO command_refusal_audit_buckets \
                         (bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,occurrence_count,first_occurred_at,last_occurred_at,sample_command_tag,command_id_present) \
                         VALUES ($1,$2,$3,$4,$5,$6,1,$7,$7,$8,$9)",
                    ).bind(bucket).bind(envelope.surface as i16).bind(envelope.command_kind)
                     .bind(envelope.refusal_kind as i16).bind(envelope.actor_epoch)
                     .bind(envelope.actor_tag.as_slice()).bind(envelope.occurred_at)
                     .bind(envelope.sample_command_tag.as_ref().map(|tag| tag.as_slice()))
                     .bind(envelope.command_id_present).execute(&mut *transaction).await?;
                    sqlx::query("UPDATE command_refusal_audit_bucket_limits SET admitted_rows = admitted_rows + 1 WHERE bucket_start = $1")
                        .bind(bucket).execute(&mut *transaction).await?;
                } else {
                    sqlx::query(
                        "UPDATE command_refusal_audit_bucket_limits SET \
                         overflow_count = CASE WHEN overflow_count = 9223372036854775807 THEN overflow_count ELSE overflow_count + 1 END, \
                         overflow_saturated = overflow_saturated OR overflow_count = 9223372036854775807 \
                         WHERE bucket_start = $1",
                    ).bind(bucket).execute(&mut *transaction).await?;
                }
            }
        }
        transaction.commit().await
    })
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
}

impl RefusalKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::InvalidEnvelope => "invalid_envelope",
            Self::InvalidCommand => "invalid_command",
            Self::Unauthorized => "unauthorized",
            Self::NotFound => "not_found",
            Self::RevisionConflict => "revision_conflict",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::InsufficientInventory => "insufficient_inventory",
            Self::InvariantViolation => "invariant_violation",
            Self::OfferExpired => "offer_expired",
            Self::InvalidState => "invalid_state",
            Self::AuctionClosed => "auction_closed",
            Self::BidTooLow => "bid_too_low",
            Self::UpstreamUnavailable => "upstream_unavailable",
            Self::AwardExpired => "award_expired",
            Self::AwardAlreadyConverted => "award_already_converted",
            Self::AwardQuantityMismatch => "award_quantity_mismatch",
            Self::AwardVariantMismatch => "award_variant_mismatch",
            Self::AwardListingChanged => "award_listing_changed",
            Self::AwardHoldMissing => "award_hold_missing",
            Self::ManualResolveConfirmationObservationMismatch => {
                "manual_resolve_confirmation_observation_mismatch"
            }
            Self::ManualResolveConfirmationEffectsFailed => {
                "manual_resolve_confirmation_effects_failed"
            }
            Self::ManualResolveInvalidReason => "manual_resolve_invalid_reason",
            Self::ManualResolveInvalidIdempotencyKey => "manual_resolve_invalid_idempotency_key",
            Self::ManualResolveInvalidOutcome => "manual_resolve_invalid_outcome",
            Self::ManualResolveInvalidRefundReference => "manual_resolve_invalid_refund_reference",
            Self::ManualResolveNotOrderSeller => "manual_resolve_not_order_seller",
            Self::ManualResolveOrderNotFound => "manual_resolve_order_not_found",
            Self::ManualResolveOrderNotAwaitingConfirmation => {
                "manual_resolve_order_not_awaiting_confirmation"
            }
            Self::ManualResolveNotApplicable => "manual_resolve_not_applicable",
            Self::ManualResolveMissingPin => "manual_resolve_missing_pin",
            Self::ManualResolveConflict => "manual_resolve_conflict",
            Self::ManualResolveAlreadyResolved => "manual_resolve_already_resolved",
            Self::ManualResolveNotInReview => "manual_resolve_not_in_review",
            Self::ManualResolveStockUnavailable => "manual_resolve_stock_unavailable",
        }
    }
}

pub fn refusal_kind_for_error(code: marketplace_domain::ErrorCode) -> RefusalKind {
    use marketplace_domain::ErrorCode;
    match code {
        ErrorCode::InvalidCommand => RefusalKind::InvalidCommand,
        ErrorCode::Unauthorized => RefusalKind::Unauthorized,
        ErrorCode::NotFound => RefusalKind::NotFound,
        ErrorCode::RevisionConflict => RefusalKind::RevisionConflict,
        ErrorCode::IdempotencyConflict => RefusalKind::IdempotencyConflict,
        ErrorCode::InsufficientInventory => RefusalKind::InsufficientInventory,
        ErrorCode::InvariantViolation => RefusalKind::InvariantViolation,
        ErrorCode::OfferExpired => RefusalKind::OfferExpired,
        ErrorCode::InvalidState => RefusalKind::InvalidState,
        ErrorCode::AuctionClosed => RefusalKind::AuctionClosed,
        ErrorCode::BidTooLow => RefusalKind::BidTooLow,
        ErrorCode::UpstreamUnavailable => RefusalKind::UpstreamUnavailable,
        ErrorCode::AwardExpired => RefusalKind::AwardExpired,
        ErrorCode::AwardAlreadyConverted => RefusalKind::AwardAlreadyConverted,
        ErrorCode::AwardQuantityMismatch => RefusalKind::AwardQuantityMismatch,
        ErrorCode::AwardVariantMismatch => RefusalKind::AwardVariantMismatch,
        ErrorCode::AwardListingChanged => RefusalKind::AwardListingChanged,
        ErrorCode::AwardHoldMissing => RefusalKind::AwardHoldMissing,
    }
}

pub const fn refusal_kind_for_review_reason(reason: crate::contracts::ReviewReason) -> RefusalKind {
    use crate::contracts::ReviewReason;
    match reason {
        ReviewReason::ConfirmationObservationMismatch => {
            RefusalKind::ManualResolveConfirmationObservationMismatch
        }
        ReviewReason::ConfirmationEffectsFailed => {
            RefusalKind::ManualResolveConfirmationEffectsFailed
        }
        ReviewReason::InvalidReason => RefusalKind::ManualResolveInvalidReason,
        ReviewReason::InvalidIdempotencyKey => RefusalKind::ManualResolveInvalidIdempotencyKey,
        ReviewReason::InvalidOutcome => RefusalKind::ManualResolveInvalidOutcome,
        ReviewReason::InvalidRefundReference => RefusalKind::ManualResolveInvalidRefundReference,
        ReviewReason::NotOrderSeller => RefusalKind::ManualResolveNotOrderSeller,
        ReviewReason::OrderNotFound => RefusalKind::ManualResolveOrderNotFound,
        ReviewReason::OrderNotAwaitingConfirmation => {
            RefusalKind::ManualResolveOrderNotAwaitingConfirmation
        }
        ReviewReason::ResolutionNotApplicable => RefusalKind::ManualResolveNotApplicable,
        ReviewReason::MissingPin => RefusalKind::ManualResolveMissingPin,
        ReviewReason::Conflict => RefusalKind::ManualResolveConflict,
        ReviewReason::AlreadyResolved => RefusalKind::ManualResolveAlreadyResolved,
        ReviewReason::NotInManualReview => RefusalKind::ManualResolveNotInReview,
        ReviewReason::StockUnavailable => RefusalKind::ManualResolveStockUnavailable,
    }
}

#[derive(Clone)]
pub struct AuditKeys {
    pub active_epoch: i16,
    active_actor: [u8; 32],
    active_sample: [u8; 32],
    previous: Option<(i16, [u8; 32], [u8; 32])>,
}

impl fmt::Debug for AuditKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditKeys")
            .field("active_epoch", &self.active_epoch)
            .field(
                "previous_epoch",
                &self.previous.as_ref().map(|value| value.0),
            )
            .finish_non_exhaustive()
    }
}

impl AuditKeys {
    pub fn parse(
        active_root_b64: &str,
        active_epoch: &str,
        previous_root_b64: Option<&str>,
        previous_epoch: Option<&str>,
    ) -> anyhow::Result<Self> {
        let active_epoch = parse_epoch(active_epoch)?;
        let active_root = parse_root(active_root_b64)?;
        let (active_actor, active_sample) = derive_keys(&active_root, active_epoch)?;
        let previous = match (previous_root_b64, previous_epoch) {
            (None, None) => None,
            (Some(root), Some(epoch)) => {
                let epoch = parse_epoch(epoch)?;
                let root = parse_root(root)?;
                if epoch != active_epoch - 1 || root == active_root {
                    anyhow::bail!("REFUSAL_AUDIT_HMAC_PREVIOUS_* is inconsistent");
                }
                let (actor, sample) = derive_keys(&root, epoch)?;
                Some((epoch, actor, sample))
            }
            _ => anyhow::bail!("REFUSAL_AUDIT_HMAC_PREVIOUS_* must be supplied together"),
        };
        Ok(Self {
            active_epoch,
            active_actor,
            active_sample,
            previous,
        })
    }

    pub fn actor_tag(&self, actor: &str) -> anyhow::Result<[u8; 16]> {
        tag(&self.active_actor, actor_input(actor)?)
    }

    pub fn sample_tag(
        &self,
        surface: SurfaceKind,
        actor: &str,
        command_id: Uuid,
    ) -> anyhow::Result<[u8; 16]> {
        tag(
            &self.active_sample,
            sample_input(surface, actor, command_id)?,
        )
    }

    pub fn previous_epoch(&self) -> Option<i16> {
        self.previous.as_ref().map(|value| value.0)
    }
}

fn parse_root(value: &str) -> anyhow::Result<[u8; 32]> {
    if value.trim() != value {
        anyhow::bail!("REFUSAL_AUDIT_HMAC_ROOT_B64 must be canonical base64");
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_ROOT_B64 must be canonical base64"))?;
    if decoded.len() != 32 || base64::engine::general_purpose::STANDARD.encode(&decoded) != value {
        anyhow::bail!("REFUSAL_AUDIT_HMAC_ROOT_B64 must encode 32 bytes");
    }
    decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_ROOT_B64 must encode 32 bytes"))
}

fn parse_epoch(value: &str) -> anyhow::Result<i16> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        anyhow::bail!("REFUSAL_AUDIT_HMAC_KEY_EPOCH must be canonical");
    }
    let epoch: i16 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_KEY_EPOCH must be a positive smallint"))?;
    if epoch < 1 {
        anyhow::bail!("REFUSAL_AUDIT_HMAC_KEY_EPOCH must be a positive smallint");
    }
    Ok(epoch)
}

fn derive_keys(root: &[u8; 32], epoch: i16) -> anyhow::Result<([u8; 32], [u8; 32])> {
    let hkdf = Hkdf::<Sha256>::new(Some(HKDF_SALT), root);
    let epoch = epoch.to_be_bytes();
    let mut actor_info = ACTOR_INFO.to_vec();
    actor_info.extend_from_slice(&epoch);
    let mut sample_info = SAMPLE_INFO.to_vec();
    sample_info.extend_from_slice(&epoch);
    let mut actor = [0; 32];
    let mut sample = [0; 32];
    hkdf.expand(&actor_info, &mut actor)
        .map_err(|_| anyhow::anyhow!("audit HKDF expansion failed"))?;
    hkdf.expand(&sample_info, &mut sample)
        .map_err(|_| anyhow::anyhow!("audit HKDF expansion failed"))?;
    Ok((actor, sample))
}

fn actor_input(actor: &str) -> anyhow::Result<Vec<u8>> {
    marketplace_domain::commands::validate_actor(actor)
        .map_err(|_| anyhow::anyhow!("authenticated actor is not canonical"))?;
    let mut input = ACTOR_DOMAIN.to_vec();
    let length: u32 = actor
        .len()
        .try_into()
        .map_err(|_| anyhow::anyhow!("authenticated actor is too long"))?;
    input.extend_from_slice(&length.to_be_bytes());
    input.extend_from_slice(actor.as_bytes());
    Ok(input)
}

fn sample_input(surface: SurfaceKind, actor: &str, command_id: Uuid) -> anyhow::Result<Vec<u8>> {
    marketplace_domain::commands::validate_actor(actor)
        .map_err(|_| anyhow::anyhow!("authenticated actor is not canonical"))?;
    let mut input = SAMPLE_DOMAIN.to_vec();
    input.extend_from_slice(&(surface as i16).to_be_bytes());
    let length: u32 = actor
        .len()
        .try_into()
        .map_err(|_| anyhow::anyhow!("authenticated actor is too long"))?;
    input.extend_from_slice(&length.to_be_bytes());
    input.extend_from_slice(actor.as_bytes());
    input.extend_from_slice(command_id.as_bytes());
    Ok(input)
}

fn tag(key: &[u8; 32], input: Vec<u8>) -> anyhow::Result<[u8; 16]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| anyhow::anyhow!("audit HMAC initialization failed"))?;
    mac.update(&input);
    mac.finalize().into_bytes()[..16]
        .try_into()
        .map_err(|_| anyhow::anyhow!("audit tag truncation failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hkdf_hmac_tags_are_separated() {
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let first = AuditKeys::parse(&root, "1", None, None).unwrap();
        let second = AuditKeys::parse(&root, "2", None, None).unwrap();
        let actor = pubky_common::crypto::Keypair::random().public_key().z32();
        let command = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        assert_ne!(
            first.actor_tag(&actor).unwrap(),
            second.actor_tag(&actor).unwrap()
        );
        assert_ne!(
            first.actor_tag(&actor).unwrap(),
            first
                .sample_tag(SurfaceKind::V1Command, &actor, command)
                .unwrap()
        );
    }

    #[test]
    fn audit_key_input_is_strict() {
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        assert!(AuditKeys::parse(&root, "1", None, None).is_ok());
        assert!(AuditKeys::parse(&root, "01", None, None).is_err());
        assert!(AuditKeys::parse(&root, "0", None, None).is_err());
        assert!(AuditKeys::parse(&format!(" {root}"), "1", None, None).is_err());
        assert!(AuditKeys::parse(&root, "1", Some(&root), Some("0")).is_err());
    }
}
