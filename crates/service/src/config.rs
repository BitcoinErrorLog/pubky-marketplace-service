use std::net::SocketAddr;

use axum::http::HeaderValue;
use marketplace_domain::pubky::is_valid_pubky;

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
    /// Pubkys holding the moderator role (`MODERATOR_PUBKYS`, comma
    /// separated). Validated as z-base-32 at startup. The role is scoped to
    /// moderation (reading all reports, deciding reports) — it grants no
    /// other authority.
    pub moderator_pubkys: Vec<String>,
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
        let moderator_pubkys =
            parse_moderator_pubkys(&std::env::var("MODERATOR_PUBKYS").unwrap_or_default())?;
        Ok(Self {
            bind_addr,
            database_url,
            allowed_origins,
            auth_token_window_seconds,
            session_ttl_seconds,
            worker_interval_seconds,
            worker_lease_seconds,
            moderator_pubkys,
        })
    }

    pub fn is_moderator(&self, pubky: &str) -> bool {
        self.moderator_pubkys.iter().any(|entry| entry == pubky)
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
            moderator_pubkys: Vec::new(),
        }
    }
}

/// Parses the comma-separated moderator list, rejecting anything that is not
/// a 52-character z-base-32 Pubky so a misconfigured role fails at startup.
pub fn parse_moderator_pubkys(raw: &str) -> anyhow::Result<Vec<String>> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            if is_valid_pubky(entry) {
                Ok(entry.to_string())
            } else {
                Err(anyhow::anyhow!(
                    "MODERATOR_PUBKYS contains an invalid pubky (expected 52 z-base-32 characters)"
                ))
            }
        })
        .collect()
}

fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be an integer")),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_moderator_pubkys;

    #[test]
    fn parses_a_valid_moderator_list() {
        let raw = format!(" {} , {} ", "y".repeat(52), "o".repeat(52));
        let parsed = parse_moderator_pubkys(&raw).expect("valid list parses");
        assert_eq!(parsed, vec!["y".repeat(52), "o".repeat(52)]);
        assert_eq!(
            parse_moderator_pubkys("").expect("empty list parses"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn rejects_invalid_moderator_pubkys_at_startup() {
        parse_moderator_pubkys("not-a-pubky").expect_err("invalid pubky rejected");
        parse_moderator_pubkys(&"y".repeat(51)).expect_err("wrong length rejected");
        let mixed = format!("{},{}", "y".repeat(52), "L".repeat(52));
        parse_moderator_pubkys(&mixed).expect_err("mixed list rejected");
    }
}
