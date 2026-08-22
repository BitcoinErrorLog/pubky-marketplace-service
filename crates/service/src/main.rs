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
    // Fail closed likewise for the attestor: a partial configuration (key
    // without salt, or salt without key) refuses to start rather than
    // issuing attestations with unlinkable order refs.
    let attestor = marketplace_service::attestor::Attestor::from_env()?;
    // Fail closed for payment methods: STRIPE_KEY_ENCRYPTION_KEY enables the
    // surface, and the Paykit pair (server URL + signing key) is
    // all-or-nothing within it.
    let payments = marketplace_service::payments::payments_runtime_from_env()?;
    match &payments {
        Some(payments) => tracing::info!(
            paykit = if payments.paykit.is_some() {
                "enabled"
            } else {
                "disabled"
            },
            "payment methods enabled"
        ),
        None => tracing::info!("payment methods disabled (no Stripe key encryption key)"),
    }
    // HOMESERVER_URL is required: `listing.sync` fetches canonical
    // seller-signed records from it, and running without the sync path would
    // silently re-open the unregistered-listing dead-end it exists to fix.
    let homeserver = Arc::new(marketplace_service::homeserver::client_from_env()?);
    tracing::info!("homeserver listing sync enabled");
    match &attestor {
        Some(attestor) => tracing::info!(
            attestor_pubky = attestor.pubky(),
            "attestation issuance enabled"
        ),
        None => tracing::info!("attestation issuance disabled (no attestor key configured)"),
    }
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("database migrations applied");

    let bind_addr = config.bind_addr;
    let state = AppState::new(pool, Arc::new(SystemClock), config)
        .with_locks(locks)
        .with_attestor(attestor)
        .with_homeserver(Some(homeserver))
        .with_payments(payments);
    workers::spawn(state.clone());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = %bind_addr, "marketplace transaction service listening");
    axum::serve(listener, http::build_router(state)).await?;
    Ok(())
}
