use std::net::SocketAddr;

use axum::http::HeaderValue;
use url::Url;

use crate::refusal_audit::AuditKeys;

/// Default `DELIVERY_ASSUME_DAYS` when the env var is unset.
pub const DEFAULT_DELIVERY_ASSUME_DAYS: i64 = 14;
/// Default `AUTO_COMPLETE_DAYS` when the env var is unset.
pub const DEFAULT_AUTO_COMPLETE_DAYS: i64 = 14;
/// Default `DELIVERY_SWEEP_BATCH_SIZE`: rows claimed per inner pass
/// (same shape as the paykit/outbox worker batches).
pub const DEFAULT_DELIVERY_SWEEP_BATCH_SIZE: i64 = 100;

#[derive(Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    /// Separate least-privilege writer login; this must not be the domain URL.
    pub refusal_audit_database_url: String,
    /// Separate retention login; it can execute only the bounded purge function.
    pub refusal_audit_retention_database_url: String,
    pub refusal_audit_keys: AuditKeys,
    /// Exact origins allowed by CORS. Empty means no browser origin is
    /// allowed (non-browser clients are unaffected).
    pub allowed_origins: Vec<HeaderValue>,
    /// Acceptance window (seconds) around server time for an AuthToken's
    /// signing timestamp. Tokens outside `now ± window` are rejected. The
    /// verifying library additionally rejects tokens more than 3 minutes
    /// from system time, so values above 180 only widen the future bound in
    /// deployments where the injected clock diverges from system time.
    pub auth_token_window_seconds: i64,
    pub session_ttl_seconds: i64,
    /// Per-seller sustained request limit for Wave 3a endpoint classes.
    pub automation_rate_limit_per_minute: i64,
    /// Token-bucket capacity multiplier for Wave 3a endpoints.
    pub automation_rate_limit_burst_multiplier: i64,
    /// Seller event cursor retention (`EVENT_RETENTION_DAYS`, default 30).
    pub event_retention_days: i64,
    /// Signed-webhook worker cadence (`WEBHOOK_WORKER_INTERVAL_SECONDS`,
    /// default 10).
    pub webhook_worker_interval_seconds: u64,
    /// Delivery lease duration (`WEBHOOK_LEASE_SECONDS`, default 30).
    pub webhook_lease_seconds: i64,
    /// Maximum delivery attempts before dead-lettering
    /// (`WEBHOOK_MAX_ATTEMPTS`, default 12).
    pub webhook_max_attempts: i32,
    /// Maximum delivery age in hours (`WEBHOOK_MAX_AGE_HOURS`, default 24).
    pub webhook_max_age_hours: i64,
    /// Initial and maximum exponential retry delays.
    pub webhook_retry_base_seconds: i64,
    pub webhook_retry_max_seconds: i64,
    pub worker_interval_seconds: u64,
    pub worker_lease_seconds: i64,
    /// Marketplace payment window armed by `payment.register_locks`: the
    /// lock point acquires the order's inventory hold and this window bounds
    /// it — the correlation window IS the hold window. A payment still
    /// awaiting entitlement when it elapses moves to `expired`, the order is
    /// cancelled, and the hold restocks. Deliberately separate from upstream
    /// failure — Locks v1 leaves transport/status failures pending
    /// (ADR-0019 §7).
    pub locks_payment_window_seconds: i64,
    /// Hold window armed by the payment-method bind
    /// (`POST /v0/orders/{id}/payment-method`), which is the lock point for
    /// the fiat and bitcoin rails (`FIAT_PAYMENT_WINDOW_SECONDS`, default
    /// 3600, minimum 60).
    pub fiat_payment_window_seconds: i64,
    /// Hold window armed by `payment.sandbox_advance`'s first transition
    /// out of `awaiting_entitlement`, the sandbox lock point
    /// (`SANDBOX_PAYMENT_WINDOW_SECONDS`, default 900, minimum 60).
    pub sandbox_payment_window_seconds: i64,
    /// Hold window armed at checkout for drop-bound orders, which keep
    /// lock-at-claim (`DROP_CLAIM_WINDOW_SECONDS`, default 600, minimum
    /// 60). A payment lock point re-arms the window to its own span.
    pub drop_claim_window_seconds: i64,
    /// Minimum seconds between lifecycle lookups for one pending
    /// correlation.
    pub locks_poll_seconds: i64,
    /// Minimum seconds between paykit-server status polls for one pending
    /// bitcoin order.
    pub paykit_poll_seconds: i64,
    /// Seconds a failed Paykit availability refresh may serve its last
    /// successful value (`PAYKIT_RAIL_STALE_SECONDS`, default 60).
    pub paykit_rail_stale_seconds: i64,
    /// Days after shipment when a `shipped` order is marked `delivered` on
    /// server time (`DELIVERY_ASSUME_DAYS`, default 14, minimum 1). There is
    /// no carrier tracking feed (ADR-0019); the assumption flags
    /// `delivery_assumed = true` on the order projection so the UI can say
    /// "marked delivered automatically after N days; tell us if it hasn't
    /// arrived".
    pub delivery_assume_days: i64,
    /// Days after delivery when a `delivered` order completes on server time
    /// (`AUTO_COMPLETE_DAYS`, default 14, minimum 1), unless a return or
    /// cancel request is open (those are their own order states, so an open
    /// request takes the order out of the sweep by construction).
    pub auto_complete_days: i64,
    /// Rows claimed per inner delivery/auto-complete sweep pass
    /// (`DELIVERY_SWEEP_BATCH_SIZE`, default 100, minimum 1). The worker
    /// iterates until a pass claims fewer than this many rows or the
    /// per-lease batch cap is hit.
    pub delivery_sweep_batch_size: i64,
    /// The deployment's public web-app origin (`PUBLIC_APP_ORIGIN`, e.g.
    /// `https://shop.pubky.app`), used as the buyer return destination on
    /// hosted checkouts that support one (PayPal `_xclick` `return`/
    /// `cancel_return`). Optional: unset omits the return parameters.
    pub public_app_origin: Option<String>,
    /// This service's own public origin (`PUBLIC_SERVICE_ORIGIN`, e.g.
    /// `https://marketplace-service-production.up.railway.app`), used to
    /// build gateway callback URLs (PayPal `notify_url` → `/v0/paypal/ipn`).
    /// Optional: unset omits the callback and PayPal payments stay
    /// participant-attested.
    pub public_service_origin: Option<String>,
    /// Whether `payment.sandbox_advance` is accepted at all
    /// (`SANDBOX_PAYMENTS_ENABLED`, default false). The sandbox adapter lets
    /// the buyer drive payment transitions by explicit command; on a
    /// deployment handling real orders that is a self-serve path to `paid`
    /// without money moving, so it must be opt-in per deployment. The
    /// client-side transport allowlist is a UX courtesy, not a boundary —
    /// this flag is the boundary. When on, `pickup_details.set` and the
    /// buyer pickup reveal are refused outright (local pickup design §A8):
    /// no real meeting point can be stored against — or revealed under —
    /// fake money.
    pub sandbox_payments_enabled: bool,
    /// Days a cancelled-after-payment order's pinned pickup snapshot is
    /// retained as the dispute exhibit when no refund evidence ever lands,
    /// before the ordinary terminal-order purge takes it
    /// (`PICKUP_DISPUTE_RETENTION_DAYS`, default 30, minimum 1; §A3).
    pub pickup_dispute_retention_days: i64,
    /// Days a Locks checkout snapshot is retained after its payment went
    /// terminal before the worker's locks pass hard-deletes it
    /// (`LOCKS_SNAPSHOT_RETENTION_DAYS`, default 90, minimum 1). The purge
    /// is bounded (oldest first, 500 rows per pass) and runs under the
    /// locks-verification lease.
    pub locks_snapshot_retention_days: i64,
    /// The bounded sole FX source URL. Permanently the pinned Blocktank
    /// BTCUSD ticker endpoint ([`crate::fx::FX_URL`]): a release binary
    /// CANNOT be repointed by environment — `from_env` never consults
    /// `FX_FEED_URL`. Integration tests override the field in-process (the
    /// test harness constructs `Config` directly and points it at a local
    /// scripted double before building `AppState`); that seam is not
    /// reachable from any environment variable.
    pub fx_feed_url: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL must be set"))?;
        let refusal_audit_database_url = required_postgres_url(
            "REFUSAL_AUDIT_DATABASE_URL",
            crate::refusal_audit::WRITER_LOGIN,
        )?;
        let refusal_audit_retention_database_url = required_postgres_url(
            "REFUSAL_AUDIT_RETENTION_DATABASE_URL",
            crate::refusal_audit::RETENTION_LOGIN,
        )?;
        reject_audit_url_reuse(&database_url, &refusal_audit_database_url)?;
        reject_audit_url_reuse(&database_url, &refusal_audit_retention_database_url)?;
        reject_audit_url_reuse(
            &refusal_audit_database_url,
            &refusal_audit_retention_database_url,
        )?;
        let refusal_audit_keys = refusal_audit_keys_from_env()?;
        for name in [
            "REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED",
            "REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED",
            "REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED",
        ] {
            if !env_bool(name, false)? {
                anyhow::bail!("{name} must be true before refusal-audit production readiness");
            }
        }
        let bind_addr = std::env::var("BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()?;
        let allowed_origins = std::env::var("ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(|origin| {
                HeaderValue::from_str(origin)
                    .map_err(|_| anyhow::anyhow!("invalid origin in ALLOWED_ORIGINS"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let auth_token_window_seconds = env_i64("AUTH_TOKEN_WINDOW_SECONDS", 120)?;
        let session_ttl_seconds = env_i64("AUTH_SESSION_TTL_SECONDS", 86_400)?;
        let automation_rate_limit_per_minute =
            positive_i64("AUTOMATION_RATE_LIMIT_PER_MINUTE", 120)?;
        let automation_rate_limit_burst_multiplier =
            positive_i64("AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER", 2)?;
        if automation_rate_limit_burst_multiplier > 10 {
            anyhow::bail!("AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER must be at most 10");
        }
        let event_retention_days = env_days("EVENT_RETENTION_DAYS", 30)?;
        let webhook_worker_interval_seconds =
            positive_i64("WEBHOOK_WORKER_INTERVAL_SECONDS", 10)?.try_into()?;
        let webhook_lease_seconds = positive_i64("WEBHOOK_LEASE_SECONDS", 30)?;
        let webhook_max_attempts: i32 = positive_i64("WEBHOOK_MAX_ATTEMPTS", 12)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("WEBHOOK_MAX_ATTEMPTS is too large"))?;
        let webhook_max_age_hours = positive_i64("WEBHOOK_MAX_AGE_HOURS", 24)?;
        let webhook_retry_base_seconds = positive_i64("WEBHOOK_RETRY_BASE_SECONDS", 5)?;
        let webhook_retry_max_seconds = positive_i64("WEBHOOK_RETRY_MAX_SECONDS", 3_600)?;
        if webhook_retry_max_seconds < webhook_retry_base_seconds {
            anyhow::bail!("WEBHOOK_RETRY_MAX_SECONDS must be at least WEBHOOK_RETRY_BASE_SECONDS");
        }
        let worker_interval_seconds = env_i64("WORKER_INTERVAL_SECONDS", 10)?.try_into()?;
        let worker_lease_seconds = env_i64("WORKER_LEASE_SECONDS", 30)?;
        let locks_payment_window_seconds = env_i64("LOCKS_PAYMENT_WINDOW_SECONDS", 3_600)?;
        if locks_payment_window_seconds < 60 {
            anyhow::bail!("LOCKS_PAYMENT_WINDOW_SECONDS must be at least 60");
        }
        let fiat_payment_window_seconds = env_i64("FIAT_PAYMENT_WINDOW_SECONDS", 3_600)?;
        if fiat_payment_window_seconds < 60 {
            anyhow::bail!("FIAT_PAYMENT_WINDOW_SECONDS must be at least 60");
        }
        let sandbox_payment_window_seconds = env_i64("SANDBOX_PAYMENT_WINDOW_SECONDS", 900)?;
        if sandbox_payment_window_seconds < 60 {
            anyhow::bail!("SANDBOX_PAYMENT_WINDOW_SECONDS must be at least 60");
        }
        let drop_claim_window_seconds = env_i64("DROP_CLAIM_WINDOW_SECONDS", 600)?;
        if drop_claim_window_seconds < 60 {
            anyhow::bail!("DROP_CLAIM_WINDOW_SECONDS must be at least 60");
        }
        let locks_poll_seconds = env_i64("LOCKS_POLL_SECONDS", 30)?;
        if locks_poll_seconds < 1 {
            anyhow::bail!("LOCKS_POLL_SECONDS must be at least 1");
        }
        let paykit_poll_seconds = env_i64("PAYKIT_POLL_SECONDS", 15)?;
        if paykit_poll_seconds < 1 {
            anyhow::bail!("PAYKIT_POLL_SECONDS must be at least 1");
        }
        let paykit_rail_stale_seconds = env_i64("PAYKIT_RAIL_STALE_SECONDS", 60)?;
        if paykit_rail_stale_seconds < paykit_poll_seconds {
            anyhow::bail!("PAYKIT_RAIL_STALE_SECONDS must be at least PAYKIT_POLL_SECONDS");
        }
        let delivery_assume_days = env_days("DELIVERY_ASSUME_DAYS", DEFAULT_DELIVERY_ASSUME_DAYS)?;
        let auto_complete_days = env_days("AUTO_COMPLETE_DAYS", DEFAULT_AUTO_COMPLETE_DAYS)?;
        let delivery_sweep_batch_size = env_days(
            "DELIVERY_SWEEP_BATCH_SIZE",
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        )?;
        let sandbox_payments_enabled = env_bool("SANDBOX_PAYMENTS_ENABLED", false)?;
        let pickup_dispute_retention_days = env_days("PICKUP_DISPUTE_RETENTION_DAYS", 30)?;
        let locks_snapshot_retention_days = env_days("LOCKS_SNAPSHOT_RETENTION_DAYS", 90)?;
        // The FX feed is the pinned Blocktank endpoint, always: the source
        // is a permanent bounded single source, so no environment variable
        // is consulted here (a release binary cannot be repointed).
        let fx_feed_url = crate::fx::FX_URL.to_string();
        let public_app_origin = env_origin("PUBLIC_APP_ORIGIN")?;
        let public_service_origin = env_origin("PUBLIC_SERVICE_ORIGIN")?;
        Ok(Self {
            bind_addr,
            database_url,
            refusal_audit_database_url,
            refusal_audit_retention_database_url,
            refusal_audit_keys,
            allowed_origins,
            auth_token_window_seconds,
            session_ttl_seconds,
            automation_rate_limit_per_minute,
            automation_rate_limit_burst_multiplier,
            event_retention_days,
            webhook_worker_interval_seconds,
            webhook_lease_seconds,
            webhook_max_attempts,
            webhook_max_age_hours,
            webhook_retry_base_seconds,
            webhook_retry_max_seconds,
            worker_interval_seconds,
            worker_lease_seconds,
            locks_payment_window_seconds,
            fiat_payment_window_seconds,
            sandbox_payment_window_seconds,
            drop_claim_window_seconds,
            locks_poll_seconds,
            paykit_poll_seconds,
            paykit_rail_stale_seconds,
            delivery_assume_days,
            auto_complete_days,
            delivery_sweep_batch_size,
            public_app_origin,
            public_service_origin,
            sandbox_payments_enabled,
            pickup_dispute_retention_days,
            locks_snapshot_retention_days,
            fx_feed_url,
        })
    }

    /// Configuration used by the integration test harness.
    pub fn for_tests() -> Self {
        let test_root =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid test bind address"),
            database_url: String::new(),
            refusal_audit_database_url: "postgres://writer@audit.test/refusal_audit".to_string(),
            refusal_audit_retention_database_url: "postgres://retention@audit.test/refusal_audit"
                .to_string(),
            refusal_audit_keys: AuditKeys::parse(&test_root, "1", None, None)
                .expect("test audit key parses"),
            allowed_origins: vec![HeaderValue::from_static("http://localhost:3000")],
            auth_token_window_seconds: 120,
            session_ttl_seconds: 86_400,
            automation_rate_limit_per_minute: 120,
            automation_rate_limit_burst_multiplier: 2,
            event_retention_days: 30,
            webhook_worker_interval_seconds: 3_600,
            webhook_lease_seconds: 30,
            webhook_max_attempts: 12,
            webhook_max_age_hours: 24,
            webhook_retry_base_seconds: 5,
            webhook_retry_max_seconds: 3_600,
            worker_interval_seconds: 3_600,
            worker_lease_seconds: 30,
            locks_payment_window_seconds: 3_600,
            fiat_payment_window_seconds: 3_600,
            sandbox_payment_window_seconds: 900,
            drop_claim_window_seconds: 600,
            locks_poll_seconds: 30,
            paykit_poll_seconds: 15,
            paykit_rail_stale_seconds: 60,
            delivery_assume_days: DEFAULT_DELIVERY_ASSUME_DAYS,
            auto_complete_days: DEFAULT_AUTO_COMPLETE_DAYS,
            delivery_sweep_batch_size: DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            public_app_origin: Some("https://app.test".to_string()),
            public_service_origin: Some("https://svc.test".to_string()),
            sandbox_payments_enabled: true,
            pickup_dispute_retention_days: 30,
            locks_snapshot_retention_days: 90,
            fx_feed_url: crate::fx::FX_URL.to_string(),
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("bind_addr", &self.bind_addr)
            .field("database_url", &"<redacted>")
            .field("refusal_audit_database_url", &"<redacted>")
            .field("refusal_audit_retention_database_url", &"<redacted>")
            .field("refusal_audit_keys", &self.refusal_audit_keys)
            .finish_non_exhaustive()
    }
}

fn required_postgres_url(name: &str, expected_login: &str) -> anyhow::Result<String> {
    let value = std::env::var(name).map_err(|_| anyhow::anyhow!("{name} must be set"))?;
    validate_postgres_url(name, &value, expected_login)?;
    Ok(value)
}

fn validate_postgres_url(name: &str, value: &str, expected_login: &str) -> anyhow::Result<()> {
    let url = Url::parse(value).map_err(|_| anyhow::anyhow!("{name} must be a PostgreSQL URL"))?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        anyhow::bail!("{name} must be a PostgreSQL URL");
    }
    if url.username() != expected_login {
        anyhow::bail!("{name} must name its designated refusal-audit login");
    }
    Ok(())
}

fn reject_audit_url_reuse(first: &str, second: &str) -> anyhow::Result<()> {
    let first = Url::parse(first).map_err(|_| anyhow::anyhow!("database URL must be valid"))?;
    let second = Url::parse(second).map_err(|_| anyhow::anyhow!("database URL must be valid"))?;
    let normalized = |url: &Url| {
        (
            if url.scheme() == "postgresql" {
                "postgres"
            } else {
                url.scheme()
            }
            .to_string(),
            url.username().to_string(),
            url.password().map(str::to_string),
            url.host_str().unwrap_or("localhost").to_string(),
            url.port_or_known_default().unwrap_or(5432),
            url.path().trim_start_matches('/').to_string(),
            url.query().map(str::to_string),
        )
    };
    if normalized(&first) == normalized(&second)
        || (!first.username().is_empty()
            && !second.username().is_empty()
            && first.username() == second.username())
    {
        anyhow::bail!("refusal-audit database URLs must use distinct principals");
    }
    Ok(())
}

fn refusal_audit_keys_from_env() -> anyhow::Result<AuditKeys> {
    AuditKeys::parse(
        &std::env::var("REFUSAL_AUDIT_HMAC_ROOT_B64")
            .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_ROOT_B64 must be set"))?,
        &std::env::var("REFUSAL_AUDIT_HMAC_KEY_EPOCH")
            .map_err(|_| anyhow::anyhow!("REFUSAL_AUDIT_HMAC_KEY_EPOCH must be set"))?,
        std::env::var("REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64")
            .ok()
            .as_deref(),
        std::env::var("REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH")
            .ok()
            .as_deref(),
    )
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer")),
        Err(_) => Ok(default),
    }
}

fn positive_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    let value = env_i64(name, default)?;
    if value < 1 {
        anyhow::bail!("{name} must be at least 1");
    }
    Ok(value)
}

/// A positive whole-day count from the environment (minimum 1, so a
/// deployment cannot disable or zero out a server-time post-purchase
/// transition).
fn env_days(name: &str, default: i64) -> anyhow::Result<i64> {
    parse_days(name, std::env::var(name).ok().as_deref(), default)
}

fn parse_days(name: &str, raw: Option<&str>, default: i64) -> anyhow::Result<i64> {
    match raw {
        None => Ok(default),
        Some(value) => {
            let days: i64 = value
                .parse()
                .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?;
            if days < 1 {
                anyhow::bail!("{name} must be at least 1");
            }
            Ok(days)
        }
    }
}

/// An optional http(s) origin from the environment, normalized without a
/// trailing slash.
fn env_origin(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(raw) => {
            let trimmed = raw.trim().trim_end_matches('/').to_string();
            let parsed = url::Url::parse(&trimmed)
                .map_err(|_| anyhow::anyhow!("{name} must be a valid URL"))?;
            if parsed.scheme() != "https" && parsed.scheme() != "http" {
                anyhow::bail!("{name} must be an http(s) origin");
            }
            Ok(Some(trimmed))
        }
        Err(_) => Ok(None),
    }
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Ok(value) => match value.trim() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(anyhow::anyhow!("{name} must be true, false, 1, or 0")),
        },
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_days, refusal_audit_keys_from_env, reject_audit_url_reuse, required_postgres_url,
        validate_postgres_url, Config, DEFAULT_AUTO_COMPLETE_DAYS, DEFAULT_DELIVERY_ASSUME_DAYS,
    };

    /// Serializes the environment-mutating test (env is process-global).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn from_env_never_consults_fx_feed_url() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous_database_url = std::env::var("DATABASE_URL").ok();
        let previous_fx_feed_url = std::env::var("FX_FEED_URL").ok();
        let previous_writer_url = std::env::var("REFUSAL_AUDIT_DATABASE_URL").ok();
        let previous_retention_url = std::env::var("REFUSAL_AUDIT_RETENTION_DATABASE_URL").ok();
        let previous_root = std::env::var("REFUSAL_AUDIT_HMAC_ROOT_B64").ok();
        let previous_epoch = std::env::var("REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH").ok();
        let previous_backup = std::env::var("REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED").ok();
        let previous_replica = std::env::var("REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED").ok();
        let previous_risk = std::env::var("REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED").ok();
        std::env::set_var("DATABASE_URL", "postgres://example.invalid/test");
        std::env::set_var(
            "REFUSAL_AUDIT_DATABASE_URL",
            "postgres://marketplace_refusal_audit_writer_login@audit.example/refusal",
        );
        std::env::set_var(
            "REFUSAL_AUDIT_RETENTION_DATABASE_URL",
            "postgres://marketplace_refusal_audit_retention@audit.example/refusal",
        );
        std::env::set_var(
            "REFUSAL_AUDIT_HMAC_ROOT_B64",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]),
        );
        std::env::set_var("REFUSAL_AUDIT_HMAC_KEY_EPOCH", "1");
        std::env::set_var("REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED", "true");
        std::env::set_var("REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED", "true");
        std::env::set_var("REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED", "true");
        std::env::set_var("FX_FEED_URL", "https://attacker.example/fx");
        let result = Config::from_env();
        match previous_database_url {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
        }
        match previous_fx_feed_url {
            Some(value) => std::env::set_var("FX_FEED_URL", value),
            None => std::env::remove_var("FX_FEED_URL"),
        }
        for (name, previous) in [
            ("REFUSAL_AUDIT_DATABASE_URL", previous_writer_url),
            (
                "REFUSAL_AUDIT_RETENTION_DATABASE_URL",
                previous_retention_url,
            ),
            ("REFUSAL_AUDIT_HMAC_ROOT_B64", previous_root),
            ("REFUSAL_AUDIT_HMAC_KEY_EPOCH", previous_epoch),
            ("REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED", previous_backup),
            ("REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED", previous_replica),
            ("REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED", previous_risk),
        ] {
            match previous {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        let config = result.expect("from_env succeeds with DATABASE_URL set");
        assert_eq!(
            config.fx_feed_url,
            crate::fx::FX_URL,
            "a release binary cannot be repointed by FX_FEED_URL"
        );
    }

    #[test]
    fn delivery_day_counts_parse_with_defaults_and_bounds() {
        // Unset falls back to the default; a positive integer parses.
        assert_eq!(
            parse_days("DELIVERY_ASSUME_DAYS", None, DEFAULT_DELIVERY_ASSUME_DAYS).unwrap(),
            DEFAULT_DELIVERY_ASSUME_DAYS
        );
        assert_eq!(
            parse_days("AUTO_COMPLETE_DAYS", Some("30"), DEFAULT_AUTO_COMPLETE_DAYS).unwrap(),
            30
        );

        // Non-integer input is rejected.
        let error = parse_days(
            "DELIVERY_ASSUME_DAYS",
            Some("two"),
            DEFAULT_DELIVERY_ASSUME_DAYS,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("must be an integer"),
            "unexpected: {error}"
        );

        // Zero and negative counts are rejected: a deployment cannot
        // disable the server-time transitions by configuration.
        for value in ["0", "-3"] {
            let error = parse_days(
                "AUTO_COMPLETE_DAYS",
                Some(value),
                DEFAULT_AUTO_COMPLETE_DAYS,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("must be at least 1"),
                "unexpected: {error}"
            );
        }
    }

    #[test]
    fn refusal_audit_writer_url_is_mandatory_distinct_and_valid() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let expected = crate::refusal_audit::WRITER_LOGIN;
        let test_name = "REFUSAL_AUDIT_TEST_DATABASE_URL";
        std::env::remove_var(test_name);
        assert!(required_postgres_url(test_name, expected).is_err());
        std::env::set_var(
            test_name,
            "postgres://marketplace_refusal_audit_writer_login@audit.test/refusal",
        );
        assert!(required_postgres_url(test_name, expected).is_ok());
        std::env::remove_var(test_name);
        assert!(validate_postgres_url(
            "REFUSAL_AUDIT_DATABASE_URL",
            "postgres://marketplace_refusal_audit_writer_login@audit.test/refusal",
            expected,
        )
        .is_ok());
        for value in [
            "not a url",
            "https://marketplace_refusal_audit_writer_login@audit.test/refusal",
            "postgres://postgres@audit.test/refusal",
            "postgres://writer@audit.test/refusal",
        ] {
            assert!(
                validate_postgres_url("REFUSAL_AUDIT_DATABASE_URL", value, expected).is_err(),
                "{value} must be rejected"
            );
        }
        for (first, second) in [
            (
                "postgres://domain@db.test/service",
                "postgres://domain@db.test/service",
            ),
            (
                "postgresql://domain@db.test/service",
                "postgres://domain@db.test:5432/service",
            ),
            (
                "postgres://domain@db.test/service",
                "postgres://domain@alias.test/audit",
            ),
        ] {
            assert!(
                reject_audit_url_reuse(first, second).is_err(),
                "{first} and {second} must be rejected offline"
            );
        }
        assert!(reject_audit_url_reuse(
            "postgres://domain@db.test/service",
            "postgres://marketplace_refusal_audit_writer_login@db.test/service"
        )
        .is_ok());
    }

    #[test]
    fn refusal_audit_hmac_config_is_mandatory_and_strict() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        const NAMES: [&str; 4] = [
            "REFUSAL_AUDIT_HMAC_ROOT_B64",
            "REFUSAL_AUDIT_HMAC_KEY_EPOCH",
            "REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64",
            "REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH",
        ];
        let previous = NAMES.map(|name| std::env::var(name).ok());
        let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7_u8; 32]);
        let other = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [8_u8; 32]);
        for name in NAMES {
            std::env::remove_var(name);
        }
        assert!(refusal_audit_keys_from_env().is_err());
        std::env::set_var(NAMES[0], &root);
        assert!(refusal_audit_keys_from_env().is_err());
        std::env::set_var(NAMES[1], "01");
        assert!(refusal_audit_keys_from_env().is_err());
        std::env::set_var(NAMES[1], "2");
        std::env::set_var(NAMES[2], &other);
        assert!(refusal_audit_keys_from_env().is_err());
        std::env::set_var(NAMES[3], "1");
        let keys = refusal_audit_keys_from_env().expect("valid two-epoch configuration");
        assert_eq!(keys.active_epoch, 2);
        assert_eq!(keys.previous_epoch(), Some(1));
        for (name, value) in NAMES.into_iter().zip(previous) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}
