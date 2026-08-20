use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use marketplace_service::clock::SystemClock;
use marketplace_service::config::Config;
use marketplace_service::{http, workers, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    // Fail closed before serving: a partial Locks configuration (URL without
    // keys, or keys without URL) refuses to start rather than running with
    // verification silently disabled or bearer material unprotected.
    let locks = marketplace_service::locks::runtime_from_env()?;
    tracing::info!(
        locks_verification = if locks.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "locks verification mode resolved"
    );
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("database migrations applied");

    let bind_addr = config.bind_addr;
    let state = AppState::new(pool, Arc::new(SystemClock), config).with_locks(locks);
    workers::spawn(state.clone());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = %bind_addr, "marketplace transaction service listening");
    axum::serve(listener, http::build_router(state)).await?;
    Ok(())
}
