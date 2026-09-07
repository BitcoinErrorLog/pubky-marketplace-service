use std::net::SocketAddr;

use axum::http::HeaderValue;

/// Default `DELIVERY_ASSUME_DAYS` when the env var is unset.
pub const DEFAULT_DELIVERY_ASSUME_DAYS: i64 = 14;
/// Default `AUTO_COMPLETE_DAYS` when the env var is unset.
pub const DEFAULT_AUTO_COMPLETE_DAYS: i64 = 14;
/// Default `DELIVERY_SWEEP_BATCH_SIZE`: rows claimed per inner pass
/// (same shape as the paykit/outbox worker batches).
pub const DEFAULT_DELIVERY_SWEEP_BATCH_SIZE: i64 = 100;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL must be set"))?;
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
        let delivery_assume_days = env_days("DELIVERY_ASSUME_DAYS", DEFAULT_DELIVERY_ASSUME_DAYS)?;
        let auto_complete_days = env_days("AUTO_COMPLETE_DAYS", DEFAULT_AUTO_COMPLETE_DAYS)?;
        let delivery_sweep_batch_size = env_days(
            "DELIVERY_SWEEP_BATCH_SIZE",
            DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        )?;
        let sandbox_payments_enabled = env_bool("SANDBOX_PAYMENTS_ENABLED", false)?;
        let pickup_dispute_retention_days = env_days("PICKUP_DISPUTE_RETENTION_DAYS", 30)?;
        let public_app_origin = env_origin("PUBLIC_APP_ORIGIN")?;
        let public_service_origin = env_origin("PUBLIC_SERVICE_ORIGIN")?;
        Ok(Self {
            bind_addr,
            database_url,
            allowed_origins,
            auth_token_window_seconds,
            session_ttl_seconds,
            worker_interval_seconds,
            worker_lease_seconds,
            locks_payment_window_seconds,
            fiat_payment_window_seconds,
            sandbox_payment_window_seconds,
            drop_claim_window_seconds,
            locks_poll_seconds,
            paykit_poll_seconds,
            delivery_assume_days,
            auto_complete_days,
            delivery_sweep_batch_size,
            public_app_origin,
            public_service_origin,
            sandbox_payments_enabled,
            pickup_dispute_retention_days,
        })
    }

    /// Configuration used by the integration test harness.
    pub fn for_tests() -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid test bind address"),
            database_url: String::new(),
            allowed_origins: vec![HeaderValue::from_static("http://localhost:3000")],
            auth_token_window_seconds: 120,
            session_ttl_seconds: 86_400,
            worker_interval_seconds: 3_600,
            worker_lease_seconds: 30,
            locks_payment_window_seconds: 3_600,
            fiat_payment_window_seconds: 3_600,
            sandbox_payment_window_seconds: 900,
            drop_claim_window_seconds: 600,
            locks_poll_seconds: 30,
            paykit_poll_seconds: 15,
            delivery_assume_days: DEFAULT_DELIVERY_ASSUME_DAYS,
            auto_complete_days: DEFAULT_AUTO_COMPLETE_DAYS,
            delivery_sweep_batch_size: DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
            public_app_origin: Some("https://app.test".to_string()),
            public_service_origin: Some("https://svc.test".to_string()),
            sandbox_payments_enabled: true,
            pickup_dispute_retention_days: 30,
        }
    }
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer")),
        Err(_) => Ok(default),
    }
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
    use super::{parse_days, DEFAULT_AUTO_COMPLETE_DAYS, DEFAULT_DELIVERY_ASSUME_DAYS};

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
}
