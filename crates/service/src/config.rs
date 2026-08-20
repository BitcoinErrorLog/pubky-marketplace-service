use std::net::SocketAddr;

use axum::http::HeaderValue;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    /// Exact origins allowed by CORS. Empty means no browser origin is
    /// allowed (non-browser clients are unaffected).
    pub allowed_origins: Vec<HeaderValue>,
    pub challenge_ttl_seconds: i64,
    pub session_ttl_seconds: i64,
    pub reservation_sweep_interval_seconds: u64,
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
        let challenge_ttl_seconds = env_i64("AUTH_CHALLENGE_TTL_SECONDS", 120)?;
        let session_ttl_seconds = env_i64("AUTH_SESSION_TTL_SECONDS", 86_400)?;
        let reservation_sweep_interval_seconds =
            env_i64("RESERVATION_SWEEP_INTERVAL_SECONDS", 10)?.try_into()?;
        Ok(Self {
            bind_addr,
            database_url,
            allowed_origins,
            challenge_ttl_seconds,
            session_ttl_seconds,
            reservation_sweep_interval_seconds,
        })
    }

    /// Configuration used by the integration test harness.
    pub fn for_tests() -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid test bind address"),
            database_url: String::new(),
            allowed_origins: vec![HeaderValue::from_static("http://localhost:3000")],
            challenge_ttl_seconds: 120,
            session_ttl_seconds: 86_400,
            reservation_sweep_interval_seconds: 3_600,
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
