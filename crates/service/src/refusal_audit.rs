//! Privacy-bounded, asynchronous recording of designed command refusals.
//!
//! The request path constructs fixed-size envelopes only. Database delivery is
//! owned by a separate task and must never be awaited by a command handler.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Timelike, Utc};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;
use sqlx::pool::PoolConnection;
use sqlx::{Acquire, PgPool, Postgres};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, Mutex};
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
/// Per-replica writer pool cap. Role `CONNECTION LIMIT` stays 2 so two
/// overlapping replicas (1+1) fit without a third leftover backend.
pub const WRITER_POOL_MAX_CONNECTIONS: u32 = 1;
pub const WRITER_LOGIN_CONN_LIMIT: i32 = 2;
pub const RETENTION_POOL_MAX_CONNECTIONS: u32 = 1;
/// Stop-start image: survive today's LIMIT 1 and post-0037 LIMIT 2.
pub const RETENTION_CONN_LIMIT_MIN: i32 = 1;
const DELIVERY_ATTEMPTS: usize = 3;
const DELIVERY_DEADLINE: Duration = Duration::from_millis(250);
const CONNECTION_CLEANUP_DEADLINE: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefusalAuditDeploymentMetadata {
    pub retention_days: i64,
    pub queue_capacity: usize,
    pub hourly_row_limit: i32,
    pub writer_pool_limit: u32,
    pub delivery_attempts: usize,
    pub transaction_deadline_ms: u64,
    pub statement_deadline_ms: u64,
    pub lock_deadline_ms: u64,
    pub unhealthy_intervals_before_alert: u8,
    pub purge_backlog_days_before_alert: u8,
    pub oldest_row_grace_days: u8,
}

pub const fn deployment_metadata() -> RefusalAuditDeploymentMetadata {
    RefusalAuditDeploymentMetadata {
        retention_days: RETENTION_DAYS,
        queue_capacity: QUEUE_CAPACITY,
        hourly_row_limit: MAX_ADMITTED_ROWS_PER_HOUR,
        writer_pool_limit: WRITER_POOL_MAX_CONNECTIONS,
        delivery_attempts: DELIVERY_ATTEMPTS,
        transaction_deadline_ms: 250,
        statement_deadline_ms: 200,
        lock_deadline_ms: 50,
        unhealthy_intervals_before_alert: 2,
        purge_backlog_days_before_alert: 1,
        oldest_row_grace_days: 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalAuditAlert {
    Loss,
    HourlyOverflow,
    WriterUnhealthy,
    PurgeBacklog,
    OldestData,
}

pub fn alert_conditions(
    metrics: RefusalAuditMetricsSnapshot,
    hourly_overflow: u64,
    consecutive_unhealthy_intervals: u8,
    purge_backlog_age_seconds: u64,
    oldest_data_age_seconds: u64,
) -> Vec<RefusalAuditAlert> {
    let mut alerts = Vec::new();
    if metrics.pending_loss_gap > 0 {
        alerts.push(RefusalAuditAlert::Loss);
    }
    if hourly_overflow > 0 {
        alerts.push(RefusalAuditAlert::HourlyOverflow);
    }
    if consecutive_unhealthy_intervals >= 2 {
        alerts.push(RefusalAuditAlert::WriterUnhealthy);
    }
    if purge_backlog_age_seconds > 86_400 {
        alerts.push(RefusalAuditAlert::PurgeBacklog);
    }
    if oldest_data_age_seconds > ((RETENTION_DAYS + 1) as u64 * 86_400) {
        alerts.push(RefusalAuditAlert::OldestData);
    }
    alerts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropCause {
    QueueFull,
    QueueClosed,
    WriterUnverified,
    RetriesExhausted,
    AmbiguousCommit,
    WriterPanic,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefusalAuditMetricsSnapshot {
    pub delivered: u64,
    pub dropped_queue_full: u64,
    pub dropped_queue_closed: u64,
    pub dropped_writer_unverified: u64,
    pub dropped_after_retries: u64,
    pub dropped_ambiguous_commit: u64,
    pub retries_acquire_timeout: u64,
    pub retries_authority_verification: u64,
    pub retries_statement_timeout: u64,
    pub retries_db_error: u64,
    pub purge_failures: u64,
    pub purge_rows: u64,
    pub writer_panics: u64,
    pub queue_depth: u64,
    pub oldest_queued_seconds: u64,
    pub hourly_overflow: u64,
    pub purge_backlog_seconds: u64,
    pub oldest_retained_seconds: u64,
    pub pending_loss_gap: u64,
    pub writer_alive: bool,
    pub writer_authority_verified: bool,
    pub consecutive_unhealthy_intervals: u64,
}

#[derive(Default)]
struct RefusalAuditMetrics {
    delivered: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_queue_closed: AtomicU64,
    dropped_writer_unverified: AtomicU64,
    dropped_after_retries: AtomicU64,
    dropped_ambiguous_commit: AtomicU64,
    retries_acquire_timeout: AtomicU64,
    retries_authority_verification: AtomicU64,
    retries_statement_timeout: AtomicU64,
    retries_db_error: AtomicU64,
    purge_failures: AtomicU64,
    purge_rows: AtomicU64,
    writer_panics: AtomicU64,
    queue_depth: AtomicU64,
    oldest_queued_at: AtomicI64,
    hourly_overflow: AtomicU64,
    purge_backlog_seconds: AtomicU64,
    oldest_retained_seconds: AtomicU64,
    pending_loss_gap: AtomicU64,
    writer_alive: AtomicBool,
    writer_authority_verified: AtomicBool,
    consecutive_unhealthy_intervals: AtomicU64,
}

impl RefusalAuditMetrics {
    fn snapshot(&self) -> RefusalAuditMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        RefusalAuditMetricsSnapshot {
            delivered: load(&self.delivered),
            dropped_queue_full: load(&self.dropped_queue_full),
            dropped_queue_closed: load(&self.dropped_queue_closed),
            dropped_writer_unverified: load(&self.dropped_writer_unverified),
            dropped_after_retries: load(&self.dropped_after_retries),
            dropped_ambiguous_commit: load(&self.dropped_ambiguous_commit),
            retries_acquire_timeout: load(&self.retries_acquire_timeout),
            retries_authority_verification: load(&self.retries_authority_verification),
            retries_statement_timeout: load(&self.retries_statement_timeout),
            retries_db_error: load(&self.retries_db_error),
            purge_failures: load(&self.purge_failures),
            purge_rows: load(&self.purge_rows),
            writer_panics: load(&self.writer_panics),
            queue_depth: load(&self.queue_depth),
            oldest_queued_seconds: match self.oldest_queued_at.load(Ordering::Relaxed) {
                timestamp if timestamp > 0 => {
                    Utc::now().timestamp().saturating_sub(timestamp) as u64
                }
                _ => 0,
            },
            hourly_overflow: load(&self.hourly_overflow),
            purge_backlog_seconds: load(&self.purge_backlog_seconds),
            oldest_retained_seconds: load(&self.oldest_retained_seconds),
            pending_loss_gap: load(&self.pending_loss_gap),
            writer_alive: self.writer_alive.load(Ordering::Relaxed),
            writer_authority_verified: self.writer_authority_verified.load(Ordering::Relaxed),
            consecutive_unhealthy_intervals: load(&self.consecutive_unhealthy_intervals),
        }
    }

    fn record_loss(&self, cause: DropCause) {
        let cause_name = match cause {
            DropCause::QueueFull => "queue_full",
            DropCause::QueueClosed => "queue_closed",
            DropCause::WriterUnverified => "writer_unverified",
            DropCause::RetriesExhausted => "retries_exhausted",
            DropCause::AmbiguousCommit => "ambiguous_commit",
            DropCause::WriterPanic => "writer_panic",
        };
        match cause {
            DropCause::QueueFull => &self.dropped_queue_full,
            DropCause::QueueClosed => &self.dropped_queue_closed,
            DropCause::WriterUnverified => &self.dropped_writer_unverified,
            DropCause::RetriesExhausted => &self.dropped_after_retries,
            DropCause::AmbiguousCommit => &self.dropped_ambiguous_commit,
            DropCause::WriterPanic => &self.writer_panics,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.pending_loss_gap.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            metric = "refusal_audit_envelopes_total",
            result = cause_name,
            value = 1_u64
        );
        tracing::info!(
            metric = "refusal_audit_loss_total",
            cause = cause_name,
            value = 1_u64
        );
        tracing::warn!(cause = cause_name, "refusal audit loss");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum SurfaceKind {
    V1Command = 1,
    BitcoinManualResolve = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum CommandKind {
    InvalidEnvelope = 0,
    RegisterListing = 1,
    SyncListing = 2,
    SyncDrop = 3,
    CancelDrop = 4,
    ReleaseDropListings = 5,
    ReserveInventory = 6,
    CreateCheckout = 7,
    CreateOffer = 8,
    CounterOffer = 9,
    AcceptOffer = 10,
    OfferCheckout = 11,
    RejectOffer = 12,
    WithdrawOffer = 13,
    PlaceBid = 14,
    CloseAuction = 15,
    AdvanceSandboxPayment = 16,
    PrepareLocks = 17,
    RegisterLocks = 18,
    RequestCancellation = 19,
    ApproveCancellation = 20,
    ShipOrder = 21,
    ConfirmDelivery = 22,
    SetPickupDetails = 23,
    ClearPickupDetails = 24,
    MarkReadyForPickup = 25,
    ConfirmPickup = 26,
    RequestReturn = 27,
    ApproveReturn = 28,
    ReceiveReturn = 29,
    RecordExternalRefund = 30,
    CreateReview = 31,
    UpdateReview = 32,
    SetBandConsent = 33,
    ManualResolve = 34,
}

impl CommandKind {
    pub const ALL: [Self; 35] = [
        Self::InvalidEnvelope,
        Self::RegisterListing,
        Self::SyncListing,
        Self::SyncDrop,
        Self::CancelDrop,
        Self::ReleaseDropListings,
        Self::ReserveInventory,
        Self::CreateCheckout,
        Self::CreateOffer,
        Self::CounterOffer,
        Self::AcceptOffer,
        Self::OfferCheckout,
        Self::RejectOffer,
        Self::WithdrawOffer,
        Self::PlaceBid,
        Self::CloseAuction,
        Self::AdvanceSandboxPayment,
        Self::PrepareLocks,
        Self::RegisterLocks,
        Self::RequestCancellation,
        Self::ApproveCancellation,
        Self::ShipOrder,
        Self::ConfirmDelivery,
        Self::SetPickupDetails,
        Self::ClearPickupDetails,
        Self::MarkReadyForPickup,
        Self::ConfirmPickup,
        Self::RequestReturn,
        Self::ApproveReturn,
        Self::ReceiveReturn,
        Self::RecordExternalRefund,
        Self::CreateReview,
        Self::UpdateReview,
        Self::SetBandConsent,
        Self::ManualResolve,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::InvalidEnvelope => "invalid_envelope",
            Self::RegisterListing => "register_listing",
            Self::SyncListing => "sync_listing",
            Self::SyncDrop => "sync_drop",
            Self::CancelDrop => "cancel_drop",
            Self::ReleaseDropListings => "release_drop_listings",
            Self::ReserveInventory => "reserve_inventory",
            Self::CreateCheckout => "create_checkout",
            Self::CreateOffer => "create_offer",
            Self::CounterOffer => "counter_offer",
            Self::AcceptOffer => "accept_offer",
            Self::OfferCheckout => "offer_checkout",
            Self::RejectOffer => "reject_offer",
            Self::WithdrawOffer => "withdraw_offer",
            Self::PlaceBid => "place_bid",
            Self::CloseAuction => "close_auction",
            Self::AdvanceSandboxPayment => "advance_sandbox_payment",
            Self::PrepareLocks => "prepare_locks",
            Self::RegisterLocks => "register_locks",
            Self::RequestCancellation => "request_cancellation",
            Self::ApproveCancellation => "approve_cancellation",
            Self::ShipOrder => "ship_order",
            Self::ConfirmDelivery => "confirm_delivery",
            Self::SetPickupDetails => "set_pickup_details",
            Self::ClearPickupDetails => "clear_pickup_details",
            Self::MarkReadyForPickup => "mark_ready_for_pickup",
            Self::ConfirmPickup => "confirm_pickup",
            Self::RequestReturn => "request_return",
            Self::ApproveReturn => "approve_return",
            Self::ReceiveReturn => "receive_return",
            Self::RecordExternalRefund => "record_external_refund",
            Self::CreateReview => "create_review",
            Self::UpdateReview => "update_review",
            Self::SetBandConsent => "set_band_consent",
            Self::ManualResolve => "manual_resolve",
        }
    }
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
    ManualResolveInvalidReason = 22,
    ManualResolveInvalidIdempotencyKey = 23,
    ManualResolveInvalidOutcome = 24,
    ManualResolveInvalidRefundReference = 25,
    ManualResolveNotOrderSeller = 26,
    ManualResolveOrderNotFound = 27,
    ManualResolveNotApplicable = 29,
    ManualResolveMissingPin = 30,
    ManualResolveConflict = 31,
    ManualResolveAlreadyResolved = 32,
    ManualResolveNotInReview = 33,
    ManualResolveStockUnavailable = 34,
    BidWrongAsset = 35,
    BidSellerForbidden = 36,
    BidNotAuction = 37,
    BidListingNotFound = 38,
    LocksIdentityMismatch = 39,
    LocksUpstreamUnavailable = 40,
}

#[derive(Debug, Clone)]
pub struct RefusalEnvelope {
    pub occurred_at: DateTime<Utc>,
    pub surface: SurfaceKind,
    pub command_kind: CommandKind,
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
    metrics: Arc<RefusalAuditMetrics>,
    panic_next_writer: Arc<AtomicBool>,
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
        let metadata = deployment_metadata();
        tracing::info!(
            retention_days = metadata.retention_days,
            queue_capacity = metadata.queue_capacity,
            hourly_row_limit = metadata.hourly_row_limit,
            writer_pool_limit = metadata.writer_pool_limit,
            delivery_attempts = metadata.delivery_attempts,
            transaction_deadline_ms = metadata.transaction_deadline_ms,
            statement_deadline_ms = metadata.statement_deadline_ms,
            lock_deadline_ms = metadata.lock_deadline_ms,
            "refusal audit deployment metadata"
        );
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        let metrics = Arc::new(RefusalAuditMetrics::default());
        let panic_next_writer = Arc::new(AtomicBool::new(false));
        let supervisor_metrics = metrics.clone();
        let supervisor_keys = keys.clone();
        let supervisor_panic = panic_next_writer.clone();
        tokio::spawn(async move {
            loop {
                supervisor_metrics
                    .writer_alive
                    .store(true, Ordering::Relaxed);
                // Probe proactively so producer enablement can become ready
                // before the first refusal. Every later acquired connection
                // is independently probed again by deliver_with_retries.
                if let Ok(mut connection) = pool.acquire().await {
                    let verified = bounded_authority_probe(&mut connection).await.is_ok()
                        && supervisor_keys
                            .assert_connection_epochs_supported(&mut connection)
                            .await
                            .is_ok();
                    supervisor_metrics
                        .writer_authority_verified
                        .store(verified, Ordering::Relaxed);
                }
                let worker_pool = pool.clone();
                let worker_receiver = receiver.clone();
                let worker_metrics = supervisor_metrics.clone();
                let worker_keys = supervisor_keys.clone();
                let worker_panic = supervisor_panic.clone();
                let worker = tokio::spawn(async move {
                    writer_loop(
                        worker_pool,
                        worker_receiver,
                        worker_metrics,
                        worker_keys,
                        worker_panic,
                    )
                    .await
                });
                match worker.await {
                    Ok(()) => {
                        supervisor_metrics
                            .writer_alive
                            .store(false, Ordering::Relaxed);
                        break;
                    }
                    Err(join_error) if join_error.is_panic() => {
                        supervisor_metrics
                            .writer_alive
                            .store(false, Ordering::Relaxed);
                        supervisor_metrics.record_loss(DropCause::WriterPanic);
                        tracing::error!("refusal audit writer panicked; restarting");
                    }
                    Err(_) => {
                        supervisor_metrics
                            .writer_alive
                            .store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }
        });
        Self {
            sender,
            keys,
            metrics,
            panic_next_writer,
        }
    }

    pub fn envelope(
        &self,
        occurred_at: DateTime<Utc>,
        surface: SurfaceKind,
        command_kind: CommandKind,
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
        // Producer enablement is fail closed until the online authority
        // probe succeeds. Marketplace outcomes remain fail open.
        if !self
            .metrics
            .writer_authority_verified
            .load(Ordering::Relaxed)
        {
            self.metrics.record_loss(DropCause::WriterUnverified);
            return;
        }
        match self.sender.try_send(envelope) {
            Ok(()) => {
                let previous_depth = self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                if previous_depth == 0 {
                    self.metrics
                        .oldest_queued_at
                        .store(Utc::now().timestamp(), Ordering::Relaxed);
                }
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.record_loss(DropCause::QueueFull);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.record_loss(DropCause::QueueClosed);
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        self.metrics.writer_alive.load(Ordering::Relaxed)
            && self
                .metrics
                .writer_authority_verified
                .load(Ordering::Relaxed)
    }

    pub fn metrics(&self) -> RefusalAuditMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn record_purge_failure(&self) {
        self.metrics.purge_failures.fetch_add(1, Ordering::Relaxed);
        tracing::info!(metric = "refusal_audit_purge_errors_total", value = 1_u64);
    }

    pub fn record_purge_success(&self, rows: u64) {
        self.metrics.purge_rows.fetch_add(rows, Ordering::Relaxed);
        tracing::info!(metric = "refusal_audit_rows_purged_total", value = rows);
    }

    #[cfg(any(test, feature = "test-faults"))]
    pub fn inject_writer_panic_once(&self) {
        self.panic_next_writer.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug)]
enum AttemptFailure {
    Acquire,
    Authority,
    PreCommit,
    PreCommitTimeout,
    AmbiguousCommit,
}

async fn writer_loop(
    pool: PgPool,
    receiver: Arc<Mutex<mpsc::Receiver<RefusalEnvelope>>>,
    metrics: Arc<RefusalAuditMetrics>,
    keys: AuditKeys,
    _panic_next_writer: Arc<AtomicBool>,
) {
    let mut probe_interval = tokio::time::interval(Duration::from_secs(5));
    probe_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = probe_interval.tick() => {
                let verified = match pool.acquire().await {
                    Ok(mut connection) => {
                        let verified = bounded_authority_probe(&mut connection).await.is_ok()
                            && keys
                                .assert_connection_epochs_supported(&mut connection)
                                .await
                                .is_ok();
                        if verified {
                            refresh_storage_metrics(&mut connection, &metrics).await;
                        }
                        verified
                    }
                    Err(_) => false,
                };
                metrics
                    .writer_authority_verified
                    .store(verified, Ordering::Relaxed);
                if verified {
                    metrics
                        .consecutive_unhealthy_intervals
                        .store(0, Ordering::Relaxed);
                } else {
                    metrics
                        .consecutive_unhealthy_intervals
                        .fetch_add(1, Ordering::Relaxed);
                }
                emit_operational_metrics(&metrics);
            }
            next = async { receiver.lock().await.recv().await } => {
                let Some(envelope) = next else {
                    return;
                };
                if metrics.queue_depth.fetch_sub(1, Ordering::Relaxed) == 1 {
                    metrics.oldest_queued_at.store(0, Ordering::Relaxed);
                }
                #[cfg(any(test, feature = "test-faults"))]
                if _panic_next_writer.swap(false, Ordering::Relaxed) {
                    panic!("injected refusal-audit writer panic");
                }
                deliver_with_retries(&pool, &envelope, &metrics, &keys).await;
            }
        }
    }
}

async fn deliver_with_retries(
    pool: &PgPool,
    envelope: &RefusalEnvelope,
    metrics: &RefusalAuditMetrics,
    keys: &AuditKeys,
) {
    for attempt in 0..DELIVERY_ATTEMPTS {
        if attempt > 0 {
            let jitter = match attempt {
                1 => rand::thread_rng().gen_range(10..=20),
                _ => rand::thread_rng().gen_range(20..=40),
            };
            tokio::time::sleep(Duration::from_millis(jitter)).await;
        }
        match deliver_attempt(pool, envelope, keys).await {
            Ok(()) => {
                metrics.delivered.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    metric = "refusal_audit_envelopes_total",
                    result = "delivered",
                    value = 1_u64
                );
                metrics
                    .writer_authority_verified
                    .store(true, Ordering::Relaxed);
                flush_loss_gap(pool, metrics).await;
                return;
            }
            Err(AttemptFailure::AmbiguousCommit) => {
                metrics.record_loss(DropCause::AmbiguousCommit);
                return;
            }
            Err(AttemptFailure::Authority) => {
                metrics
                    .writer_authority_verified
                    .store(false, Ordering::Relaxed);
                metrics
                    .retries_authority_verification
                    .fetch_add(1, Ordering::Relaxed);
                emit_retry_metric("authority_verification");
                if attempt + 1 == DELIVERY_ATTEMPTS {
                    metrics.record_loss(DropCause::WriterUnverified);
                    return;
                }
            }
            Err(AttemptFailure::Acquire) => {
                metrics
                    .retries_acquire_timeout
                    .fetch_add(1, Ordering::Relaxed);
                emit_retry_metric("acquire_timeout");
            }
            Err(AttemptFailure::PreCommitTimeout) => {
                metrics
                    .retries_statement_timeout
                    .fetch_add(1, Ordering::Relaxed);
                emit_retry_metric("statement_timeout");
            }
            Err(AttemptFailure::PreCommit) => {
                metrics.retries_db_error.fetch_add(1, Ordering::Relaxed);
                emit_retry_metric("db_error");
            }
        }
    }
    metrics.record_loss(DropCause::RetriesExhausted);
}

fn emit_retry_metric(cause: &'static str) {
    tracing::info!(metric = "refusal_audit_retries_total", cause, value = 1_u64);
}

fn emit_operational_metrics(metrics: &RefusalAuditMetrics) {
    let snapshot = metrics.snapshot();
    tracing::info!(
        metric = "refusal_audit_operational_gauges",
        queue_depth = snapshot.queue_depth,
        oldest_queued_seconds = snapshot.oldest_queued_seconds,
        writer_alive = snapshot.writer_alive,
        writer_authority_verified = snapshot.writer_authority_verified,
        hourly_overflow = snapshot.hourly_overflow,
        purge_backlog_seconds = snapshot.purge_backlog_seconds,
        oldest_retained_seconds = snapshot.oldest_retained_seconds,
    );
    for alert in alert_conditions(
        snapshot,
        snapshot.hourly_overflow,
        snapshot
            .consecutive_unhealthy_intervals
            .try_into()
            .unwrap_or(u8::MAX),
        snapshot.purge_backlog_seconds,
        snapshot.oldest_retained_seconds,
    ) {
        tracing::warn!(?alert, "refusal audit operational alert");
    }
}

async fn refresh_storage_metrics(
    connection: &mut PoolConnection<Postgres>,
    metrics: &RefusalAuditMetrics,
) {
    let query = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT \
           COALESCE((SELECT sum(overflow_count)::bigint \
             FROM command_refusal_audit_bucket_limits \
             WHERE bucket_start = date_trunc('hour', clock_timestamp())), 0), \
           COALESCE((SELECT GREATEST(0, extract(epoch FROM \
             (clock_timestamp() - min(bucket_start))))::bigint \
             FROM command_refusal_audit_buckets), 0), \
           COALESCE((SELECT GREATEST(0, extract(epoch FROM \
             (clock_timestamp() - interval '30 days' - min(bucket_start))))::bigint \
             FROM command_refusal_audit_buckets \
             WHERE bucket_start < date_trunc('hour', clock_timestamp()) - interval '30 days'), 0)",
    )
    .fetch_one(&mut **connection);
    match tokio::time::timeout(DELIVERY_DEADLINE, query).await {
        Ok(Ok((overflow, oldest, backlog))) => {
            metrics
                .hourly_overflow
                .store(overflow.max(0) as u64, Ordering::Relaxed);
            metrics
                .oldest_retained_seconds
                .store(oldest.max(0) as u64, Ordering::Relaxed);
            metrics
                .purge_backlog_seconds
                .store(backlog.max(0) as u64, Ordering::Relaxed);
        }
        Ok(Err(_)) => {}
        Err(_) => connection.close_on_drop(),
    }
}

async fn deliver_attempt(
    pool: &PgPool,
    envelope: &RefusalEnvelope,
    keys: &AuditKeys,
) -> Result<(), AttemptFailure> {
    let mut connection = pool.acquire().await.map_err(|_| AttemptFailure::Acquire)?;
    bounded_authority_probe(&mut connection)
        .await
        .map_err(|_| AttemptFailure::Authority)?;
    keys.assert_connection_epochs_supported(&mut connection)
        .await
        .map_err(|_| AttemptFailure::Authority)?;
    let deadline = tokio::time::Instant::now() + DELIVERY_DEADLINE;
    let mut transaction = connection
        .begin()
        .await
        .map_err(|_| AttemptFailure::PreCommit)?;
    let prepared =
        tokio::time::timeout_at(deadline, prepare_delivery(&mut transaction, envelope)).await;
    match prepared {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            if !matches!(
                tokio::time::timeout(CONNECTION_CLEANUP_DEADLINE, transaction.rollback()).await,
                Ok(Ok(()))
            ) {
                let _ = connection.close().await;
            }
            return Err(AttemptFailure::PreCommit);
        }
        Err(_) => {
            // A timed-out connection is never returned to the pool: closing
            // it forces PostgreSQL to roll the unfinished transaction back.
            let _ = tokio::time::timeout(CONNECTION_CLEANUP_DEADLINE, transaction.rollback()).await;
            let _ = connection.close().await;
            return Err(AttemptFailure::PreCommitTimeout);
        }
    }
    let committed = tokio::time::timeout_at(deadline, transaction.commit()).await;
    match committed {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => {
            let _ = connection.close().await;
            Err(AttemptFailure::AmbiguousCommit)
        }
    }
}

async fn authority_probe(connection: &mut PoolConnection<Postgres>) -> Result<(), sqlx::Error> {
    let verified: bool = sqlx::query_scalar(
        "SELECT session_user = $1 \
         AND current_user = $1 \
         AND r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper \
         AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication \
         AND NOT r.rolbypassrls AND r.rolconnlimit = 2 \
         AND pg_has_role(session_user, 'marketplace_refusal_audit_writer', 'MEMBER') \
         AND (SELECT array_agg(parent.rolname ORDER BY parent.rolname) \
              FROM pg_catalog.pg_auth_members m \
              JOIN pg_catalog.pg_roles member ON member.oid = m.member \
              JOIN pg_catalog.pg_roles parent ON parent.oid = m.roleid \
              WHERE member.rolname = session_user) \
             = ARRAY['marketplace_refusal_audit_writer']::name[] \
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m \
              JOIN pg_catalog.pg_roles member ON member.oid = m.member \
              WHERE member.rolname = 'marketplace_refusal_audit_writer') \
         AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles capability \
              WHERE capability.rolname = 'marketplace_refusal_audit_writer' \
                AND NOT capability.rolcanlogin AND NOT capability.rolinherit \
                AND NOT capability.rolsuper AND NOT capability.rolcreatedb \
                AND NOT capability.rolcreaterole AND NOT capability.rolreplication \
                AND NOT capability.rolbypassrls AND capability.rolconnlimit = -1) \
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c \
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'public' AND c.relkind IN ('r','p') \
                AND c.relname NOT IN ('command_refusal_surface_kinds', \
                  'command_refusal_command_kinds', 'command_refusal_kinds', \
                  'command_refusal_audit_buckets', 'command_refusal_audit_bucket_limits', \
                  'command_refusal_audit_loss_gap') \
                AND (has_table_privilege(session_user, c.oid, 'INSERT') \
                  OR has_table_privilege(session_user, c.oid, 'UPDATE') \
                  OR has_table_privilege(session_user, c.oid, 'DELETE') \
                  OR has_table_privilege(session_user, c.oid, 'TRUNCATE') \
                  OR has_table_privilege(session_user, c.oid, 'REFERENCES') \
                  OR has_table_privilege(session_user, c.oid, 'TRIGGER'))) \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_buckets', 'DELETE') \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_bucket_limits', 'DELETE') \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_loss_gap', 'DELETE') \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_access_buckets', 'SELECT') \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_access_buckets', 'INSERT') \
         AND NOT has_table_privilege(session_user, 'public.command_refusal_audit_access_limits', 'SELECT') \
         AND NOT has_function_privilege(session_user, \
             'public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', 'EXECUTE') \
         AND NOT has_function_privilege(session_user, \
             'public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', 'EXECUTE') \
         FROM pg_catalog.pg_roles r WHERE r.rolname = session_user",
    )
    .bind(WRITER_LOGIN)
    .fetch_one(&mut **connection)
    .await?;
    if verified {
        Ok(())
    } else {
        Err(sqlx::Error::Protocol(
            "refusal audit writer authority verification failed".into(),
        ))
    }
}

async fn bounded_authority_probe(
    connection: &mut PoolConnection<Postgres>,
) -> Result<(), sqlx::Error> {
    match tokio::time::timeout(DELIVERY_DEADLINE, authority_probe(connection)).await {
        Ok(result) => result,
        Err(_) => {
            connection.close_on_drop();
            Err(sqlx::Error::PoolTimedOut)
        }
    }
}

async fn retention_authority_probe(
    connection: &mut PoolConnection<Postgres>,
) -> Result<(), sqlx::Error> {
    let verified: bool = sqlx::query_scalar(
        "SELECT session_user = $1 AND current_user = $1 \
         AND r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper \
         AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication \
         AND NOT r.rolbypassrls AND r.rolconnlimit >= $2 \
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid) \
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c \
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'public' AND c.relkind IN ('r','p') \
                AND (has_table_privilege(session_user, c.oid, 'SELECT') \
                  OR has_table_privilege(session_user, c.oid, 'INSERT') \
                  OR has_table_privilege(session_user, c.oid, 'UPDATE') \
                  OR has_table_privilege(session_user, c.oid, 'DELETE') \
                  OR has_table_privilege(session_user, c.oid, 'TRUNCATE') \
                  OR has_table_privilege(session_user, c.oid, 'REFERENCES') \
                  OR has_table_privilege(session_user, c.oid, 'TRIGGER'))) \
         AND NOT has_function_privilege(session_user, \
             'public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)', 'EXECUTE') \
         AND has_function_privilege(session_user, \
             'public.purge_refusal_audit(timestamptz,integer)', 'EXECUTE') \
         FROM pg_catalog.pg_roles r WHERE r.rolname = session_user",
    )
    .bind(RETENTION_LOGIN)
    .bind(RETENTION_CONN_LIMIT_MIN)
    .fetch_one(&mut **connection)
    .await?;
    if verified {
        Ok(())
    } else {
        Err(sqlx::Error::Protocol(
            "refusal audit retention authority verification failed".into(),
        ))
    }
}

#[cfg(any(test, feature = "test-faults"))]
pub async fn probe_writer_authority(
    connection: &mut PoolConnection<Postgres>,
) -> Result<(), sqlx::Error> {
    bounded_authority_probe(connection).await
}

#[cfg(any(test, feature = "test-faults"))]
pub async fn probe_retention_authority(
    connection: &mut PoolConnection<Postgres>,
) -> Result<(), sqlx::Error> {
    retention_authority_probe(connection).await
}

pub async fn purge_once(pool: &PgPool, reference_time: DateTime<Utc>) -> Result<i32, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    retention_authority_probe(&mut connection).await?;
    sqlx::query_scalar("SELECT public.purge_refusal_audit(date_trunc('hour', $1), 500)")
        .bind(reference_time)
        .fetch_one(&mut *connection)
        .await
}

async fn prepare_delivery<'a>(
    transaction: &mut sqlx::Transaction<'a, Postgres>,
    envelope: &RefusalEnvelope,
) -> Result<(), sqlx::Error> {
    let bucket = envelope
        .occurred_at
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("valid timestamp hour");
    sqlx::query("SET LOCAL statement_timeout = '200ms'")
        .execute(&mut **transaction)
        .await?;
    sqlx::query("SET LOCAL lock_timeout = '50ms'")
        .execute(&mut **transaction)
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
        .bind(envelope.command_kind as i16)
        .bind(envelope.refusal_kind as i16)
        .bind(envelope.actor_epoch)
        .bind(envelope.actor_tag.as_slice())
        .execute(&mut **transaction)
        .await?;
    if updated.rows_affected() == 0 {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 32))")
            .bind(bucket.to_rfc3339())
            .execute(&mut **transaction)
            .await?;
        let second_update = sqlx::query(
                "UPDATE command_refusal_audit_buckets SET \
                 occurrence_count = CASE WHEN occurrence_count = 9223372036854775807 THEN occurrence_count ELSE occurrence_count + 1 END, \
                 count_saturated = count_saturated OR occurrence_count = 9223372036854775807, \
                 last_occurred_at = GREATEST(last_occurred_at, $1) WHERE bucket_start = $2 \
                 AND surface_kind = $3 AND command_kind = $4 AND refusal_kind = $5 \
                 AND actor_key_epoch = $6 AND actor_tag = $7",
            )
            .bind(envelope.occurred_at).bind(bucket).bind(envelope.surface as i16)
            .bind(envelope.command_kind as i16).bind(envelope.refusal_kind as i16)
            .bind(envelope.actor_epoch).bind(envelope.actor_tag.as_slice())
            .execute(&mut **transaction).await?;
        if second_update.rows_affected() == 0 {
            sqlx::query(
                    "INSERT INTO command_refusal_audit_bucket_limits (bucket_start, admitted_rows) VALUES ($1, 0) \
                     ON CONFLICT (bucket_start) DO NOTHING",
                ).bind(bucket).execute(&mut **transaction).await?;
            let admitted: i32 = sqlx::query_scalar(
                    "SELECT admitted_rows FROM command_refusal_audit_bucket_limits WHERE bucket_start = $1 FOR UPDATE",
                ).bind(bucket).fetch_one(&mut **transaction).await?;
            if admitted < MAX_ADMITTED_ROWS_PER_HOUR {
                sqlx::query(
                        "INSERT INTO command_refusal_audit_buckets \
                         (bucket_start,surface_kind,command_kind,refusal_kind,actor_key_epoch,actor_tag,occurrence_count,first_occurred_at,last_occurred_at,sample_command_tag,command_id_present) \
                         VALUES ($1,$2,$3,$4,$5,$6,1,$7,$7,$8,$9)",
                    ).bind(bucket).bind(envelope.surface as i16).bind(envelope.command_kind as i16)
                     .bind(envelope.refusal_kind as i16).bind(envelope.actor_epoch)
                     .bind(envelope.actor_tag.as_slice()).bind(envelope.occurred_at)
                     .bind(envelope.sample_command_tag.as_ref().map(|tag| tag.as_slice()))
                     .bind(envelope.command_id_present).execute(&mut **transaction).await?;
                sqlx::query("UPDATE command_refusal_audit_bucket_limits SET admitted_rows = admitted_rows + 1 WHERE bucket_start = $1")
                        .bind(bucket).execute(&mut **transaction).await?;
            } else {
                sqlx::query(
                        "UPDATE command_refusal_audit_bucket_limits SET \
                         overflow_count = CASE WHEN overflow_count = 9223372036854775807 THEN overflow_count ELSE overflow_count + 1 END, \
                         overflow_saturated = overflow_saturated OR overflow_count = 9223372036854775807 \
                         WHERE bucket_start = $1",
                    ).bind(bucket).execute(&mut **transaction).await?;
            }
        }
    }
    Ok(())
}

async fn flush_loss_gap(pool: &PgPool, metrics: &RefusalAuditMetrics) {
    let pending = metrics.pending_loss_gap.swap(0, Ordering::Relaxed);
    if pending == 0 {
        return;
    }
    let Ok(mut connection) = pool.acquire().await else {
        metrics
            .pending_loss_gap
            .fetch_add(pending, Ordering::Relaxed);
        return;
    };
    if bounded_authority_probe(&mut connection).await.is_err() {
        metrics
            .pending_loss_gap
            .fetch_add(pending, Ordering::Relaxed);
        return;
    }
    let result = tokio::time::timeout(
        DELIVERY_DEADLINE,
        sqlx::query(
        "INSERT INTO command_refusal_audit_loss_gap(id, pending_loss_count, updated_at) \
         VALUES (true, $1, clock_timestamp()) \
         ON CONFLICT (id) DO UPDATE SET \
           pending_loss_count = CASE \
             WHEN command_refusal_audit_loss_gap.pending_loss_count > 9223372036854775807 - EXCLUDED.pending_loss_count \
             THEN 9223372036854775807 \
             ELSE command_refusal_audit_loss_gap.pending_loss_count + EXCLUDED.pending_loss_count END, \
           updated_at = EXCLUDED.updated_at",
    )
    .bind(i64::try_from(pending).unwrap_or(i64::MAX))
    .execute(&mut *connection),
    )
    .await;
    if !matches!(result, Ok(Ok(_))) {
        let _ = connection.close().await;
        metrics
            .pending_loss_gap
            .fetch_add(pending, Ordering::Relaxed);
    }
}

impl RefusalKind {
    pub const ALL: [Self; 37] = [
        Self::InvalidEnvelope,
        Self::InvalidCommand,
        Self::Unauthorized,
        Self::NotFound,
        Self::RevisionConflict,
        Self::IdempotencyConflict,
        Self::InsufficientInventory,
        Self::InvariantViolation,
        Self::OfferExpired,
        Self::InvalidState,
        Self::AuctionClosed,
        Self::BidTooLow,
        Self::UpstreamUnavailable,
        Self::AwardExpired,
        Self::AwardAlreadyConverted,
        Self::AwardQuantityMismatch,
        Self::AwardVariantMismatch,
        Self::AwardListingChanged,
        Self::AwardHoldMissing,
        Self::ManualResolveInvalidReason,
        Self::ManualResolveInvalidIdempotencyKey,
        Self::ManualResolveInvalidOutcome,
        Self::ManualResolveInvalidRefundReference,
        Self::ManualResolveNotOrderSeller,
        Self::ManualResolveOrderNotFound,
        Self::ManualResolveNotApplicable,
        Self::ManualResolveMissingPin,
        Self::ManualResolveConflict,
        Self::ManualResolveAlreadyResolved,
        Self::ManualResolveNotInReview,
        Self::ManualResolveStockUnavailable,
        Self::BidWrongAsset,
        Self::BidSellerForbidden,
        Self::BidNotAuction,
        Self::BidListingNotFound,
        Self::LocksIdentityMismatch,
        Self::LocksUpstreamUnavailable,
    ];

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
            Self::ManualResolveInvalidReason => "manual_resolve_invalid_reason",
            Self::ManualResolveInvalidIdempotencyKey => "manual_resolve_invalid_idempotency_key",
            Self::ManualResolveInvalidOutcome => "manual_resolve_invalid_outcome",
            Self::ManualResolveInvalidRefundReference => "manual_resolve_invalid_refund_reference",
            Self::ManualResolveNotOrderSeller => "manual_resolve_not_order_seller",
            Self::ManualResolveOrderNotFound => "manual_resolve_order_not_found",
            Self::ManualResolveNotApplicable => "manual_resolve_not_applicable",
            Self::ManualResolveMissingPin => "manual_resolve_missing_pin",
            Self::ManualResolveConflict => "manual_resolve_conflict",
            Self::ManualResolveAlreadyResolved => "manual_resolve_already_resolved",
            Self::ManualResolveNotInReview => "manual_resolve_not_in_review",
            Self::ManualResolveStockUnavailable => "manual_resolve_stock_unavailable",
            Self::BidWrongAsset => "bid_wrong_asset",
            Self::BidSellerForbidden => "bid_seller_forbidden",
            Self::BidNotAuction => "bid_not_auction",
            Self::BidListingNotFound => "bid_listing_not_found",
            Self::LocksIdentityMismatch => "locks_identity_mismatch",
            Self::LocksUpstreamUnavailable => "locks_upstream_unavailable",
        }
    }
}

pub const fn refusal_kind_for_review_reason(
    reason: crate::contracts::ReviewReason,
) -> Option<RefusalKind> {
    use crate::contracts::ReviewReason;
    match reason {
        ReviewReason::ConfirmationObservationMismatch
        | ReviewReason::ConfirmationEffectsFailed
        | ReviewReason::OrderNotAwaitingConfirmation => None,
        ReviewReason::InvalidReason => Some(RefusalKind::ManualResolveInvalidReason),
        ReviewReason::InvalidIdempotencyKey => {
            Some(RefusalKind::ManualResolveInvalidIdempotencyKey)
        }
        ReviewReason::InvalidOutcome => Some(RefusalKind::ManualResolveInvalidOutcome),
        ReviewReason::InvalidRefundReference => {
            Some(RefusalKind::ManualResolveInvalidRefundReference)
        }
        ReviewReason::NotOrderSeller => Some(RefusalKind::ManualResolveNotOrderSeller),
        ReviewReason::OrderNotFound => Some(RefusalKind::ManualResolveOrderNotFound),
        ReviewReason::ResolutionNotApplicable => Some(RefusalKind::ManualResolveNotApplicable),
        ReviewReason::MissingPin => Some(RefusalKind::ManualResolveMissingPin),
        ReviewReason::Conflict => Some(RefusalKind::ManualResolveConflict),
        ReviewReason::AlreadyResolved => Some(RefusalKind::ManualResolveAlreadyResolved),
        ReviewReason::NotInManualReview => Some(RefusalKind::ManualResolveNotInReview),
        ReviewReason::StockUnavailable => Some(RefusalKind::ManualResolveStockUnavailable),
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

    pub fn actor_tag_for_epoch(&self, epoch: i16, actor: &str) -> anyhow::Result<Option<[u8; 16]>> {
        let key = if epoch == self.active_epoch {
            Some(&self.active_actor)
        } else {
            self.previous
                .as_ref()
                .filter(|value| value.0 == epoch)
                .map(|value| &value.1)
        };
        key.map(|key| tag(key, actor_input(actor)?)).transpose()
    }

    pub fn verify_actor_tag(
        &self,
        epoch: i16,
        actor: &str,
        candidate: &[u8; 16],
    ) -> anyhow::Result<bool> {
        Ok(self
            .actor_tag_for_epoch(epoch, actor)?
            .is_some_and(|expected| bool::from(expected.ct_eq(candidate))))
    }

    pub fn erasure_candidates(&self, actor: &str) -> anyhow::Result<Vec<(i16, [u8; 16])>> {
        let mut candidates = vec![(self.active_epoch, self.actor_tag(actor)?)];
        if let Some((epoch, key, _)) = &self.previous {
            candidates.push((*epoch, tag(key, actor_input(actor)?)?));
        }
        Ok(candidates)
    }

    /// Runs only through an explicitly supplied administrative connection;
    /// the ordinary service and writer roles are denied DELETE.
    pub async fn erase_actor(&self, pool: &PgPool, actor: &str) -> anyhow::Result<u64> {
        let candidates = self.erasure_candidates(actor)?;
        let active = candidates[0];
        let previous = candidates.get(1).copied();
        let deleted = sqlx::query(
            "DELETE FROM command_refusal_audit_buckets \
             WHERE (actor_key_epoch = $1 AND actor_tag = $2) \
                OR ($3::smallint IS NOT NULL AND actor_key_epoch = $3 AND actor_tag = $4)",
        )
        .bind(active.0)
        .bind(active.1.as_slice())
        .bind(previous.map(|value| value.0))
        .bind(previous.map(|value| value.1.to_vec()))
        .execute(pool)
        .await?;
        Ok(deleted.rows_affected())
    }

    pub async fn assert_database_epochs_supported(&self, pool: &PgPool) -> anyhow::Result<()> {
        let unsupported: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM command_refusal_audit_buckets \
             WHERE actor_key_epoch <> $1 \
               AND ($2::smallint IS NULL OR actor_key_epoch <> $2)",
        )
        .bind(self.active_epoch)
        .bind(self.previous_epoch())
        .fetch_one(pool)
        .await?;
        if unsupported != 0 {
            anyhow::bail!("refusal-audit rows require an unloaded key epoch");
        }
        Ok(())
    }

    async fn assert_connection_epochs_supported(
        &self,
        connection: &mut PoolConnection<Postgres>,
    ) -> anyhow::Result<()> {
        let unsupported = match tokio::time::timeout(
            DELIVERY_DEADLINE,
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM command_refusal_audit_buckets \
                 WHERE actor_key_epoch <> $1 \
                   AND ($2::smallint IS NULL OR actor_key_epoch <> $2)",
            )
            .bind(self.active_epoch)
            .bind(self.previous_epoch())
            .fetch_one(&mut **connection),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                connection.close_on_drop();
                anyhow::bail!("refusal-audit epoch probe timed out");
            }
        };
        if unsupported != 0 {
            anyhow::bail!("refusal-audit rows require an unloaded key epoch");
        }
        Ok(())
    }

    /// A second rotation is forbidden while the previous epoch can remain
    /// in a row, replica, or unexpired backup.
    pub fn assert_second_rotation_safe(
        &self,
        retained_previous_rows: u64,
        replicas_expired: bool,
        backups_expired: bool,
    ) -> anyhow::Result<()> {
        if self.previous.is_some()
            && (retained_previous_rows != 0 || !replicas_expired || !backups_expired)
        {
            anyhow::bail!("previous refusal-audit epoch is still retained");
        }
        Ok(())
    }

    pub fn destroy_previous(
        &mut self,
        retained_previous_rows: u64,
        replicas_expired: bool,
        backups_expired: bool,
    ) -> anyhow::Result<()> {
        self.assert_second_rotation_safe(
            retained_previous_rows,
            replicas_expired,
            backups_expired,
        )?;
        if let Some((_, mut actor, mut sample)) = self.previous.take() {
            actor.fill(0);
            sample.fill(0);
        }
        Ok(())
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
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
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
    use sha2::Digest;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    #[test]
    fn refusal_audit_migration_0032_bytes_are_immutable() {
        let bytes = include_bytes!("../migrations/0032_refusal_audit.sql");
        assert_eq!(
            hex::encode(Sha256::digest(bytes)),
            "e771c5ef7bbbb04b571701710e0a6ed05eaf601165c473e82cabce552509eacf"
        );
    }

    #[test]
    fn refusal_audit_hkdf_hmac_vectors_are_stable_and_separated() {
        // Independently generated with Python 3 stdlib hashlib/hmac RFC 5869,
        // not by this implementation.
        let root = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        let actor = "7jfgaa9nutjyixzikb7tgmsf9gkwq7iqz498zr1nd5ig1fng4esy";
        let command = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let first = AuditKeys::parse(root, "1", None, None).unwrap();
        let decoded: [u8; 32] = base64::engine::general_purpose::STANDARD
            .decode(root)
            .unwrap()
            .try_into()
            .unwrap();
        let mut extract = Hmac::<Sha256>::new_from_slice(HKDF_SALT).unwrap();
        extract.update(&decoded);
        assert_eq!(
            hex::encode(extract.finalize().into_bytes()),
            "f056b98905a3df0d2d41c51327c57f0c31002ebfc0f0924e7bdd4f23a16498db"
        );
        let (actor_key, sample_key) = derive_keys(&decoded, 1).unwrap();
        assert_eq!(
            hex::encode(actor_key),
            "da90bc5f84e7029c539689451a9d4bb7ad88c59ad86d8b1a5f09758b8adccd56"
        );
        assert_eq!(
            hex::encode(sample_key),
            "0b2ddb213307235b976987c2183ba3e26b1c2007ddcc73240cf0e2ca14aa8181"
        );
        let mut actor_mac = Hmac::<Sha256>::new_from_slice(&actor_key).unwrap();
        actor_mac.update(&actor_input(actor).unwrap());
        assert_eq!(
            hex::encode(actor_mac.finalize().into_bytes()),
            "61fd4a7237f48973955357748f069e1e3e669affcddca8bee0373b0ccf3bc7b4"
        );
        let mut sample_mac = Hmac::<Sha256>::new_from_slice(&sample_key).unwrap();
        sample_mac.update(&sample_input(SurfaceKind::V1Command, actor, command).unwrap());
        assert_eq!(
            hex::encode(sample_mac.finalize().into_bytes()),
            "14dd1a32c318509c4dc6903a7de6e7fabb17bc3ec13aee2c3ee06b37d3dbbd2c"
        );
        assert_eq!(
            hex::encode(first.actor_tag(actor).unwrap()),
            "61fd4a7237f48973955357748f069e1e"
        );
        assert_eq!(
            hex::encode(
                first
                    .sample_tag(SurfaceKind::V1Command, actor, command)
                    .unwrap()
            ),
            "14dd1a32c318509c4dc6903a7de6e7fa"
        );
        assert_eq!(
            hex::encode(
                first
                    .sample_tag(SurfaceKind::BitcoinManualResolve, actor, command)
                    .unwrap()
            ),
            "1d284da5375a14470905f2f15ac4285a"
        );
        let second = AuditKeys::parse(root, "2", None, None).unwrap();
        let (actor_key_2, sample_key_2) = derive_keys(&decoded, 2).unwrap();
        assert_eq!(
            hex::encode(actor_key_2),
            "176dc8d745d8ff5cd5fa4f497dff21dd5cfa79865f269e3daf7cc71c2a38d907"
        );
        assert_eq!(
            hex::encode(sample_key_2),
            "42f5f3f395231b2e45982c0956bac763770280a46994ab3feadd4188df15171e"
        );
        assert_eq!(
            hex::encode(second.actor_tag(actor).unwrap()),
            "ef036cd6bcac86d54053f91b909a89e6"
        );
        assert_eq!(
            hex::encode(
                second
                    .sample_tag(SurfaceKind::V1Command, actor, command)
                    .unwrap()
            ),
            "2b1eed4cf034b4dee4541e5b9905da59"
        );
        assert_ne!(
            first.actor_tag(actor).unwrap(),
            second.actor_tag(actor).unwrap()
        );
        assert_ne!(
            first
                .sample_tag(SurfaceKind::V1Command, actor, command)
                .unwrap(),
            first
                .sample_tag(SurfaceKind::V1Command, actor, Uuid::nil())
                .unwrap()
        );
    }

    #[test]
    fn refusal_audit_hmac_config_is_mandatory_and_strict() {
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let other = base64::engine::general_purpose::STANDARD.encode([8u8; 32]);
        assert!(AuditKeys::parse(&root, "1", None, None).is_ok());
        assert!(AuditKeys::parse(&other, "2", Some(&root), Some("1")).is_ok());
        for epoch in ["", "0", "01", "-1", "+1", "32768"] {
            assert!(AuditKeys::parse(&root, epoch, None, None).is_err());
        }
        for malformed in [
            format!(" {root}"),
            root.trim_end_matches('=').to_string(),
            base64::engine::general_purpose::STANDARD.encode([7u8; 31]),
            base64::engine::general_purpose::STANDARD.encode([7u8; 33]),
            format!("_{}", &root[1..]),
        ] {
            assert!(AuditKeys::parse(&malformed, "1", None, None).is_err());
        }
        assert!(AuditKeys::parse(&other, "2", Some(&root), None).is_err());
        assert!(AuditKeys::parse(&other, "2", None, Some("1")).is_err());
        assert!(AuditKeys::parse(&root, "2", Some(&root), Some("1")).is_err());
        assert!(AuditKeys::parse(&other, "3", Some(&root), Some("1")).is_err());
    }

    #[test]
    fn refusal_audit_rotation_verifies_old_epoch_until_destroyed() {
        let previous = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let active = base64::engine::general_purpose::STANDARD.encode([8u8; 32]);
        let actor = pubky_common::crypto::Keypair::random().public_key().z32();
        let mut keys = AuditKeys::parse(&active, "2", Some(&previous), Some("1")).unwrap();
        let old_tag = keys.actor_tag_for_epoch(1, &actor).unwrap().unwrap();
        assert!(keys.verify_actor_tag(1, &actor, &old_tag).unwrap());
        assert!(!keys.verify_actor_tag(2, &actor, &old_tag).unwrap());
        assert!(keys.assert_second_rotation_safe(1, true, true).is_err());
        assert!(keys.assert_second_rotation_safe(0, false, true).is_err());
        keys.destroy_previous(0, true, true).unwrap();
        assert!(!keys.verify_actor_tag(1, &actor, &old_tag).unwrap());
        assert_eq!(keys.erasure_candidates(&actor).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn refusal_audit_loss_is_bounded_and_accounted() {
        let options: PgConnectOptions =
            "postgres://marketplace_refusal_audit_writer_login@127.0.0.1:1/unavailable"
                .parse()
                .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(WRITER_POOL_MAX_CONNECTIONS)
            .acquire_timeout(Duration::from_millis(5))
            .connect_lazy_with(options);
        let metrics = RefusalAuditMetrics::default();
        let envelope = RefusalEnvelope {
            occurred_at: Utc::now(),
            surface: SurfaceKind::V1Command,
            command_kind: CommandKind::RegisterListing,
            refusal_kind: RefusalKind::InvalidState,
            actor_epoch: 1,
            actor_tag: [1; 16],
            sample_command_tag: None,
            command_id_present: false,
        };
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let keys = AuditKeys::parse(&root, "1", None, None).unwrap();
        deliver_with_retries(&pool, &envelope, &metrics, &keys).await;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retries_acquire_timeout, 3);
        assert_eq!(snapshot.dropped_after_retries, 1);
        assert_eq!(snapshot.pending_loss_gap, 1);
    }

    #[tokio::test]
    async fn refusal_audit_lazy_pool_starts_during_audit_db_outage() {
        let options: PgConnectOptions =
            "postgres://marketplace_refusal_audit_writer_login@127.0.0.1:1/unavailable"
                .parse()
                .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(WRITER_POOL_MAX_CONNECTIONS)
            .acquire_timeout(Duration::from_millis(5))
            .connect_lazy_with(options);
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let keys = AuditKeys::parse(&root, "1", None, None).unwrap();
        let runtime = RefusalAuditRuntime::spawn(pool, keys);
        assert!(!runtime.is_ready());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!runtime.is_ready());
    }

    #[test]
    fn refusal_audit_queue_saturation_accounts_full_and_closed_paths() {
        let root = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let keys = AuditKeys::parse(&root, "1", None, None).unwrap();
        let metrics = Arc::new(RefusalAuditMetrics::default());
        metrics
            .writer_authority_verified
            .store(true, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let runtime = RefusalAuditRuntime {
            sender,
            keys,
            metrics: metrics.clone(),
            panic_next_writer: Arc::new(AtomicBool::new(false)),
        };
        let envelope = RefusalEnvelope {
            occurred_at: Utc::now(),
            surface: SurfaceKind::V1Command,
            command_kind: CommandKind::RegisterListing,
            refusal_kind: RefusalKind::InvalidState,
            actor_epoch: 1,
            actor_tag: [1; 16],
            sample_command_tag: None,
            command_id_present: false,
        };
        for _ in 0..QUEUE_CAPACITY {
            runtime.try_send(envelope.clone());
        }
        runtime.try_send(envelope.clone());
        assert_eq!(runtime.metrics().dropped_queue_full, 1);
        assert_eq!(runtime.metrics().queue_depth, QUEUE_CAPACITY as u64);
        drop(receiver);
        runtime.try_send(envelope);
        assert_eq!(runtime.metrics().dropped_queue_closed, 1);
    }

    #[test]
    fn refusal_audit_deployment_metadata_and_alert_thresholds_are_pinned() {
        assert_eq!(
            deployment_metadata(),
            RefusalAuditDeploymentMetadata {
                retention_days: 30,
                queue_capacity: 4_096,
                hourly_row_limit: 10_000,
                writer_pool_limit: WRITER_POOL_MAX_CONNECTIONS,
                delivery_attempts: 3,
                transaction_deadline_ms: 250,
                statement_deadline_ms: 200,
                lock_deadline_ms: 50,
                unhealthy_intervals_before_alert: 2,
                purge_backlog_days_before_alert: 1,
                oldest_row_grace_days: 1,
            }
        );
        assert_eq!(
            alert_conditions(
                RefusalAuditMetricsSnapshot {
                    pending_loss_gap: 1,
                    ..Default::default()
                },
                1,
                2,
                86_401,
                31 * 86_400 + 1,
            ),
            vec![
                RefusalAuditAlert::Loss,
                RefusalAuditAlert::HourlyOverflow,
                RefusalAuditAlert::WriterUnhealthy,
                RefusalAuditAlert::PurgeBacklog,
                RefusalAuditAlert::OldestData,
            ]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn refusal_audit_10000_row_concurrent_admission_bound(pool: PgPool) {
        let occurred_at = Utc::now();
        let bucket = occurred_at
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("valid timestamp hour");
        // The hourly cap is MAX_ADMITTED_ROWS_PER_HOUR (10_000), enforced by
        // admitted_rows and the table CHECK. Driving 10_001 unique tags through
        // prepare_delivery serializes on the hour advisory lock under the
        // production SET LOCAL lock_timeout = '50ms'. On a 2-core GitHub
        // runner that wait exceeds 50ms (55P03); production retries that
        // PreCommit up to DELIVERY_ATTEMPTS. This test keeps the 50ms timeout
        // and the 10_000 cap, and races only the last three slots.
        sqlx::query(
            "INSERT INTO command_refusal_audit_buckets (
                bucket_start, surface_kind, command_kind, refusal_kind,
                actor_key_epoch, actor_tag, occurrence_count,
                first_occurred_at, last_occurred_at, command_id_present
             ) VALUES ($1, $2, $3, $4, 1, $5, 1, $6, $6, false)",
        )
        .bind(bucket)
        .bind(SurfaceKind::V1Command as i16)
        .bind(CommandKind::RegisterListing as i16)
        .bind(RefusalKind::InvalidState as i16)
        .bind([0_u8; 16].as_slice())
        .bind(occurred_at)
        .execute(&pool)
        .await
        .expect("saturation fixture row");
        sqlx::query(
            "INSERT INTO command_refusal_audit_bucket_limits (bucket_start, admitted_rows) \
             VALUES ($1, $2)",
        )
        .bind(bucket)
        .bind(MAX_ADMITTED_ROWS_PER_HOUR - 2)
        .execute(&pool)
        .await
        .expect("seed admitted_rows at cap-2");

        let mut tasks = tokio::task::JoinSet::new();
        let concurrency = Arc::new(tokio::sync::Semaphore::new(2));
        for index in 1u32..=3 {
            let pool = pool.clone();
            let concurrency = concurrency.clone();
            tasks.spawn(async move {
                let _permit = concurrency.acquire_owned().await.expect("stress permit");
                let mut tag = [0u8; 16];
                tag[12..].copy_from_slice(&index.to_be_bytes());
                let envelope = RefusalEnvelope {
                    occurred_at,
                    surface: SurfaceKind::V1Command,
                    command_kind: CommandKind::RegisterListing,
                    refusal_kind: RefusalKind::InvalidState,
                    actor_epoch: 1,
                    actor_tag: tag,
                    sample_command_tag: None,
                    command_id_present: false,
                };
                let mut transaction = pool.begin().await.expect("stress transaction");
                prepare_delivery(&mut transaction, &envelope)
                    .await
                    .expect("bounded admission");
                transaction.commit().await.expect("stress commit");
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.expect("delivery task");
        }
        let (rows, admitted, overflow): (i64, i32, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM command_refusal_audit_buckets), \
                    (SELECT admitted_rows FROM command_refusal_audit_bucket_limits), \
                    (SELECT sum(overflow_count)::bigint FROM command_refusal_audit_bucket_limits)",
        )
        .fetch_one(&pool)
        .await
        .expect("admission result");
        assert_eq!(admitted, MAX_ADMITTED_ROWS_PER_HOUR);
        assert_eq!(overflow, 1);
        assert_eq!(rows, 3);

        sqlx::query(
            "UPDATE command_refusal_audit_buckets SET occurrence_count = 9223372036854775807 \
             WHERE actor_tag = $1",
        )
        .bind([0_u8; 16].as_slice())
        .execute(&pool)
        .await
        .expect("existing-row saturation fixture");
        let saturated_existing = RefusalEnvelope {
            occurred_at,
            surface: SurfaceKind::V1Command,
            command_kind: CommandKind::RegisterListing,
            refusal_kind: RefusalKind::InvalidState,
            actor_epoch: 1,
            actor_tag: [0; 16],
            sample_command_tag: None,
            command_id_present: false,
        };
        let mut transaction = pool.begin().await.expect("saturation transaction");
        prepare_delivery(&mut transaction, &saturated_existing)
            .await
            .expect("existing row saturates");
        transaction.commit().await.expect("saturation commit");
        let existing: (i64, bool) = sqlx::query_as(
            "SELECT occurrence_count, count_saturated \
             FROM command_refusal_audit_buckets WHERE actor_tag = $1",
        )
        .bind([0_u8; 16].as_slice())
        .fetch_one(&pool)
        .await
        .expect("saturated existing row");
        assert_eq!(existing, (i64::MAX, true));

        sqlx::query(
            "UPDATE command_refusal_audit_bucket_limits \
             SET overflow_count = 9223372036854775807",
        )
        .execute(&pool)
        .await
        .expect("overflow saturation fixture");
        let mut new_tag = [0_u8; 16];
        new_tag[12..].copy_from_slice(&10_002_u32.to_be_bytes());
        let overflow_envelope = RefusalEnvelope {
            actor_tag: new_tag,
            ..saturated_existing
        };
        let mut transaction = pool.begin().await.expect("overflow transaction");
        prepare_delivery(&mut transaction, &overflow_envelope)
            .await
            .expect("overflow saturates");
        transaction.commit().await.expect("overflow commit");
        let overflow_state: (i64, bool) = sqlx::query_as(
            "SELECT overflow_count, overflow_saturated \
             FROM command_refusal_audit_bucket_limits",
        )
        .fetch_one(&pool)
        .await
        .expect("saturated overflow");
        assert_eq!(overflow_state, (i64::MAX, true));
    }
}
