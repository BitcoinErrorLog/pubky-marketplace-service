use std::collections::HashSet;
use std::net::SocketAddr;

use axum::http::HeaderValue;
use url::Url;

use crate::refusal_audit::AuditKeys;

/// Default PayPal Website Payments Standard checkout (`_xclick`).
pub const DEFAULT_PAYPAL_CHECKOUT_URL: &str = "https://www.paypal.com/cgi-bin/webscr";

/// Live-test seller allow-list (`LIVE_TEST_SELLER_ALLOWLIST`).
///
/// - **Unset** (env absent): current production — every seller may bind.
/// - **Set but empty** (blank, whitespace, or comma-only): deny every seller.
/// - **Set with pubkys**: only those z32 identities may bind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveTestSellerAllowlist {
    Unrestricted,
    Restricted(HashSet<String>),
}

impl LiveTestSellerAllowlist {
    pub fn permits(&self, seller_pubky: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Restricted(set) => set.contains(seller_pubky),
        }
    }
}

/// Default `DELIVERY_ASSUME_DAYS` when the env var is unset.
pub const DEFAULT_DELIVERY_ASSUME_DAYS: i64 = 14;
/// Default `AUTO_COMPLETE_DAYS` when the env var is unset.
pub const DEFAULT_AUTO_COMPLETE_DAYS: i64 = 14;
/// Default `DELIVERY_SWEEP_BATCH_SIZE`: rows claimed per inner pass
/// (same shape as the paykit/outbox worker batches).
pub const DEFAULT_DELIVERY_SWEEP_BATCH_SIZE: i64 = 100;
/// Upper bound of `automation_rate_limits.tokens`. Env `rate × burst`
/// capacity must fit this CHECK or process start fails closed.
pub const AUTOMATION_RATE_LIMIT_MAX_TOKENS: i64 = 20_000;

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
    /// Maximum retained events enqueued for one endpoint in one worker pass.
    pub webhook_enqueue_batch_size: i64,
    /// Maximum endpoints whose enqueue cursor advances in one worker pass.
    pub webhook_enqueue_endpoints_per_pass: i64,
    /// Maximum non-terminal deliveries retained for one seller.
    pub webhook_max_pending_per_seller: i64,
    /// Maximum active endpoints one seller may fan out to.
    pub webhook_max_endpoints_per_seller: i64,
    /// Terminal delivery/dead-letter retention and bounded purge size.
    pub webhook_terminal_retention_days: i64,
    pub webhook_purge_batch_size: i64,
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
    /// Hold window armed at ordinary `checkout.create`
    /// (`CHECKOUT_HOLD_WINDOW_SECONDS`, default 900, minimum 60). Bind /
    /// Locks / sandbox re-arm this to the rail window.
    pub checkout_hold_window_seconds: i64,
    /// Hold window re-armed by a fiat payment-method bind
    /// (`FIAT_PAYMENT_WINDOW_SECONDS`, default 3600, minimum 60).
    pub fiat_payment_window_seconds: i64,
    /// Hold window re-armed by a bitcoin payment-method bind
    /// (`BITCOIN_PAYMENT_WINDOW_SECONDS`, default 7200, minimum 60). Do not
    /// reuse the fiat window: a 1-conf can exceed 3600 s.
    pub bitcoin_payment_window_seconds: i64,
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
    /// Live-test seller allow-list. See [`LiveTestSellerAllowlist`].
    pub live_test_seller_allowlist: LiveTestSellerAllowlist,
    /// Optional fiat bind cap in the listing's own minor units
    /// (`LIVE_TEST_MAX_USD_MINOR`). Name is historical: the integer is
    /// compared to `order.total_minor` for every fiat currency (not SAT/BTC),
    /// with no FX conversion. Unset means no extra fiat cap.
    pub live_test_max_usd_minor: Option<i64>,
    /// Optional satoshi bind cap (`LIVE_TEST_MAX_SATS`). Unset means no
    /// extra sats cap. Applies to SAT/BTC orders and to the quoted sats of
    /// a USD bitcoin bind.
    pub live_test_max_sats: Option<i64>,
    /// Payment methods refused at bind (`PAYMENT_RAILS_DISABLED`, comma
    /// list of `bitcoin`/`stripe`/`paypal`). Empty means all rails stay
    /// available.
    pub payment_rails_disabled: HashSet<String>,
    /// PayPal `_xclick` checkout base (`PAYPAL_CHECKOUT_URL`). Default is
    /// live `www.paypal.com`; `www.sandbox.paypal.com` is the only other
    /// allowed host.
    pub paypal_checkout_url: String,
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
        automation_rate_capacity(
            automation_rate_limit_per_minute,
            automation_rate_limit_burst_multiplier,
        )?;
        let event_retention_days = env_days("EVENT_RETENTION_DAYS", 30)?;
        let webhook_worker_interval_seconds =
            positive_i64("WEBHOOK_WORKER_INTERVAL_SECONDS", 10)?.try_into()?;
        let webhook_lease_seconds = positive_i64("WEBHOOK_LEASE_SECONDS", 30)?;
        let webhook_max_attempts: i32 = positive_i64("WEBHOOK_MAX_ATTEMPTS", 12)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("WEBHOOK_MAX_ATTEMPTS is too large"))?;
        let webhook_max_age_hours = positive_i64("WEBHOOK_MAX_AGE_HOURS", 24)?;
        let webhook_enqueue_batch_size = positive_i64("WEBHOOK_ENQUEUE_BATCH_SIZE", 100)?;
        let webhook_enqueue_endpoints_per_pass =
            positive_i64("WEBHOOK_ENQUEUE_ENDPOINTS_PER_PASS", 100)?;
        let webhook_max_pending_per_seller =
            positive_i64("WEBHOOK_MAX_PENDING_PER_SELLER", 10_000)?;
        let webhook_max_endpoints_per_seller =
            positive_i64("WEBHOOK_MAX_ENDPOINTS_PER_SELLER", 100)?;
        let webhook_terminal_retention_days = positive_i64("WEBHOOK_TERMINAL_RETENTION_DAYS", 30)?;
        let webhook_purge_batch_size = positive_i64("WEBHOOK_PURGE_BATCH_SIZE", 500)?;
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
        let checkout_hold_window_seconds = env_i64("CHECKOUT_HOLD_WINDOW_SECONDS", 900)?;
        if checkout_hold_window_seconds < 60 {
            anyhow::bail!("CHECKOUT_HOLD_WINDOW_SECONDS must be at least 60");
        }
        let fiat_payment_window_seconds = env_i64("FIAT_PAYMENT_WINDOW_SECONDS", 3_600)?;
        if fiat_payment_window_seconds < 60 {
            anyhow::bail!("FIAT_PAYMENT_WINDOW_SECONDS must be at least 60");
        }
        let bitcoin_payment_window_seconds = env_i64("BITCOIN_PAYMENT_WINDOW_SECONDS", 7_200)?;
        if bitcoin_payment_window_seconds < 60 {
            anyhow::bail!("BITCOIN_PAYMENT_WINDOW_SECONDS must be at least 60");
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
        let live_test_seller_allowlist = match std::env::var("LIVE_TEST_SELLER_ALLOWLIST") {
            Err(std::env::VarError::NotPresent) => LiveTestSellerAllowlist::Unrestricted,
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("LIVE_TEST_SELLER_ALLOWLIST must be valid UTF-8")
            }
            Ok(raw) => {
                let parsed = parse_live_test_seller_allowlist(Some(&raw))?;
                if matches!(&parsed, LiveTestSellerAllowlist::Restricted(set) if set.is_empty()) {
                    tracing::warn!(
                        "LIVE_TEST_SELLER_ALLOWLIST is set but empty; denying all sellers at bind"
                    );
                }
                parsed
            }
        };
        let live_test_max_usd_minor = env_optional_positive_i64("LIVE_TEST_MAX_USD_MINOR")?;
        let live_test_max_sats = env_optional_positive_i64("LIVE_TEST_MAX_SATS")?;
        let payment_rails_disabled =
            parse_payment_rails_disabled(std::env::var("PAYMENT_RAILS_DISABLED").ok().as_deref())?;
        let paypal_checkout_url =
            parse_paypal_checkout_url(std::env::var("PAYPAL_CHECKOUT_URL").ok().as_deref())?;
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
            webhook_enqueue_batch_size,
            webhook_enqueue_endpoints_per_pass,
            webhook_max_pending_per_seller,
            webhook_max_endpoints_per_seller,
            webhook_terminal_retention_days,
            webhook_purge_batch_size,
            webhook_retry_base_seconds,
            webhook_retry_max_seconds,
            worker_interval_seconds,
            worker_lease_seconds,
            locks_payment_window_seconds,
            checkout_hold_window_seconds,
            fiat_payment_window_seconds,
            bitcoin_payment_window_seconds,
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
            live_test_seller_allowlist,
            live_test_max_usd_minor,
            live_test_max_sats,
            payment_rails_disabled,
            paypal_checkout_url,
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
            webhook_enqueue_batch_size: 100,
            webhook_enqueue_endpoints_per_pass: 100,
            webhook_max_pending_per_seller: 10_000,
            webhook_max_endpoints_per_seller: 100,
            webhook_terminal_retention_days: 30,
            webhook_purge_batch_size: 500,
            webhook_retry_base_seconds: 5,
            webhook_retry_max_seconds: 3_600,
            worker_interval_seconds: 3_600,
            worker_lease_seconds: 30,
            locks_payment_window_seconds: 3_600,
            checkout_hold_window_seconds: 900,
            fiat_payment_window_seconds: 3_600,
            bitcoin_payment_window_seconds: 7_200,
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
            live_test_seller_allowlist: LiveTestSellerAllowlist::Unrestricted,
            live_test_max_usd_minor: None,
            live_test_max_sats: None,
            payment_rails_disabled: HashSet::new(),
            paypal_checkout_url: DEFAULT_PAYPAL_CHECKOUT_URL.to_string(),
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

fn automation_rate_capacity(rate: i64, burst: i64) -> anyhow::Result<i64> {
    let Some(capacity) = rate.checked_mul(burst) else {
        anyhow::bail!(
            "AUTOMATION_RATE_LIMIT_PER_MINUTE × AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER must be at most {AUTOMATION_RATE_LIMIT_MAX_TOKENS}"
        );
    };
    if capacity > AUTOMATION_RATE_LIMIT_MAX_TOKENS {
        anyhow::bail!(
            "AUTOMATION_RATE_LIMIT_PER_MINUTE × AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER must be at most {AUTOMATION_RATE_LIMIT_MAX_TOKENS}"
        );
    }
    Ok(capacity)
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

fn env_optional_positive_i64(name: &str) -> anyhow::Result<Option<i64>> {
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => {
            let parsed: i64 = value
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("{name} must be an integer"))?;
            if parsed < 1 {
                anyhow::bail!("{name} must be at least 1");
            }
            Ok(Some(parsed))
        }
    }
}

fn strip_pubky_scheme(value: &str) -> &str {
    value
        .strip_prefix("pubky://")
        .or_else(|| value.strip_prefix("pubky:"))
        .unwrap_or(value)
}

fn parse_live_test_seller_allowlist(raw: Option<&str>) -> anyhow::Result<LiveTestSellerAllowlist> {
    let Some(raw) = raw else {
        return Ok(LiveTestSellerAllowlist::Unrestricted);
    };
    let mut allowlist = HashSet::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let z32 = strip_pubky_scheme(trimmed);
        let key = pubky_common::crypto::PublicKey::try_from_z32(z32).map_err(|_| {
            anyhow::anyhow!(
                "LIVE_TEST_SELLER_ALLOWLIST entries must be 52-character z-base-32 pubkys"
            )
        })?;
        allowlist.insert(key.z32());
    }
    Ok(LiveTestSellerAllowlist::Restricted(allowlist))
}

fn parse_payment_rails_disabled(raw: Option<&str>) -> anyhow::Result<HashSet<String>> {
    let mut disabled = HashSet::new();
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(disabled);
    };
    for token in raw.split(',') {
        let method = token.trim().to_ascii_lowercase();
        if method.is_empty() {
            continue;
        }
        if !matches!(method.as_str(), "bitcoin" | "stripe" | "paypal") {
            anyhow::bail!("PAYMENT_RAILS_DISABLED entries must be bitcoin, stripe, or paypal");
        }
        disabled.insert(method);
    }
    Ok(disabled)
}

fn parse_paypal_checkout_url(raw: Option<&str>) -> anyhow::Result<String> {
    let value = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_PAYPAL_CHECKOUT_URL);
    let parsed = Url::parse(value)
        .map_err(|_| anyhow::anyhow!("PAYPAL_CHECKOUT_URL must be a valid URL"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("PAYPAL_CHECKOUT_URL must be https");
    }
    let host = parsed.host_str().unwrap_or_default();
    if !matches!(host, "www.paypal.com" | "www.sandbox.paypal.com") {
        anyhow::bail!("PAYPAL_CHECKOUT_URL host must be www.paypal.com or www.sandbox.paypal.com");
    }
    if parsed.path() != "/cgi-bin/webscr" {
        anyhow::bail!("PAYPAL_CHECKOUT_URL path must be /cgi-bin/webscr");
    }
    Ok(format!("https://{host}/cgi-bin/webscr"))
}

#[cfg(test)]
mod tests {
    use super::{
        automation_rate_capacity, parse_days, parse_live_test_seller_allowlist,
        parse_payment_rails_disabled, parse_paypal_checkout_url, refusal_audit_keys_from_env,
        reject_audit_url_reuse, required_postgres_url, validate_postgres_url, Config,
        LiveTestSellerAllowlist, AUTOMATION_RATE_LIMIT_MAX_TOKENS, DEFAULT_AUTO_COMPLETE_DAYS,
        DEFAULT_DELIVERY_ASSUME_DAYS, DEFAULT_PAYPAL_CHECKOUT_URL,
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
    fn automation_rate_capacity_matches_tokens_check() {
        assert_eq!(automation_rate_capacity(120, 2).expect("default"), 240);
        assert_eq!(
            automation_rate_capacity(2_000, 10).expect("exact CHECK bound"),
            AUTOMATION_RATE_LIMIT_MAX_TOKENS
        );
        let overflow = automation_rate_capacity(5_000, 10).expect_err("exceeds CHECK");
        assert!(
            overflow
                .to_string()
                .contains(&AUTOMATION_RATE_LIMIT_MAX_TOKENS.to_string()),
            "unexpected: {overflow}"
        );
    }

    #[test]
    fn from_env_rejects_rate_burst_that_exceeds_tokens_check() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous_database_url = std::env::var("DATABASE_URL").ok();
        let previous_writer_url = std::env::var("REFUSAL_AUDIT_DATABASE_URL").ok();
        let previous_retention_url = std::env::var("REFUSAL_AUDIT_RETENTION_DATABASE_URL").ok();
        let previous_root = std::env::var("REFUSAL_AUDIT_HMAC_ROOT_B64").ok();
        let previous_epoch = std::env::var("REFUSAL_AUDIT_HMAC_KEY_EPOCH").ok();
        let previous_backup = std::env::var("REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED").ok();
        let previous_replica = std::env::var("REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED").ok();
        let previous_risk = std::env::var("REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED").ok();
        let previous_rate = std::env::var("AUTOMATION_RATE_LIMIT_PER_MINUTE").ok();
        let previous_burst = std::env::var("AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER").ok();
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
        std::env::set_var("AUTOMATION_RATE_LIMIT_PER_MINUTE", "5000");
        std::env::set_var("AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER", "10");
        let result = Config::from_env();
        match previous_database_url {
            Some(value) => std::env::set_var("DATABASE_URL", value),
            None => std::env::remove_var("DATABASE_URL"),
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
            ("AUTOMATION_RATE_LIMIT_PER_MINUTE", previous_rate),
            ("AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER", previous_burst),
        ] {
            match previous {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        let error = result.expect_err("rate × burst above CHECK must fail closed");
        assert!(
            error
                .to_string()
                .contains(&AUTOMATION_RATE_LIMIT_MAX_TOKENS.to_string()),
            "unexpected: {error}"
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

    #[test]
    fn live_test_seller_allowlist_unset_is_unrestricted_set_empty_denies_all() {
        assert_eq!(
            parse_live_test_seller_allowlist(None).expect("unset"),
            LiveTestSellerAllowlist::Unrestricted
        );
        for blank in ["", "  ", ",", " , , ", "\t,\n"] {
            match parse_live_test_seller_allowlist(Some(blank)).expect("set-but-empty") {
                LiveTestSellerAllowlist::Restricted(set) => {
                    assert!(set.is_empty(), "{blank:?} must deny every seller");
                    assert!(
                        !LiveTestSellerAllowlist::Restricted(set).permits("any-seller"),
                        "{blank:?} must not fail open"
                    );
                }
                other => panic!("{blank:?} must be Restricted(empty), got {other:?}"),
            }
        }
    }

    #[test]
    fn live_test_seller_allowlist_strips_only_scheme_prefixes() {
        let pubky = pubky_common::crypto::Keypair::random().public_key().z32();
        let parsed = parse_live_test_seller_allowlist(Some(&format!(
            " {pubky}, pubky:{pubky}, pubky://{pubky} "
        )))
        .expect("canonical plus scheme prefixes");
        match parsed {
            LiveTestSellerAllowlist::Restricted(set) => {
                assert_eq!(set.len(), 1);
                assert!(set.contains(&pubky));
            }
            other => panic!("expected Restricted, got {other:?}"),
        }
        // Bare `pubky` + z32 (no colon) is not a scheme. Stripping it would
        // also mangle a z32 that itself starts with "pubky".
        let no_colon = format!("pubky{pubky}");
        let error = parse_live_test_seller_allowlist(Some(&no_colon)).expect_err("no-colon prefix");
        assert!(
            error.to_string().contains("52-character"),
            "unexpected: {error}"
        );
        let error = parse_live_test_seller_allowlist(Some("not-a-pubky")).expect_err("malformed");
        assert!(
            error.to_string().contains("52-character"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn payment_rails_disabled_accepts_known_methods_and_rejects_unknown() {
        assert!(parse_payment_rails_disabled(None)
            .expect("unset")
            .is_empty());
        let disabled = parse_payment_rails_disabled(Some(" stripe,BITCOIN ")).expect("list");
        assert!(disabled.contains("stripe"));
        assert!(disabled.contains("bitcoin"));
        assert!(!disabled.contains("paypal"));
        let error = parse_payment_rails_disabled(Some("wire")).expect_err("unknown");
        assert!(
            error.to_string().contains("bitcoin, stripe, or paypal"),
            "unexpected: {error}"
        );
    }

    #[test]
    fn paypal_checkout_url_defaults_to_live_and_allows_sandbox_host() {
        assert_eq!(
            parse_paypal_checkout_url(None).expect("default"),
            DEFAULT_PAYPAL_CHECKOUT_URL
        );
        assert_eq!(
            parse_paypal_checkout_url(Some("https://www.sandbox.paypal.com/cgi-bin/webscr"))
                .expect("sandbox"),
            "https://www.sandbox.paypal.com/cgi-bin/webscr"
        );
        for value in [
            "http://www.paypal.com/cgi-bin/webscr",
            "https://paypal.com/cgi-bin/webscr",
            "https://www.paypal.com/other",
            "https://evil.example/cgi-bin/webscr",
        ] {
            assert!(
                parse_paypal_checkout_url(Some(value)).is_err(),
                "{value} must be rejected"
            );
        }
    }

    #[test]
    fn for_tests_leaves_live_test_gates_unset() {
        let config = Config::for_tests();
        assert_eq!(
            config.live_test_seller_allowlist,
            LiveTestSellerAllowlist::Unrestricted
        );
        assert_eq!(config.live_test_max_usd_minor, None);
        assert_eq!(config.live_test_max_sats, None);
        assert!(config.payment_rails_disabled.is_empty());
        assert_eq!(config.paypal_checkout_url, DEFAULT_PAYPAL_CHECKOUT_URL);
    }
}
