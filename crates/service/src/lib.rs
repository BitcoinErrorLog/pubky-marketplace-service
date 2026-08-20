//! Pubky Marketplace Transaction Service.
//!
//! Server-authoritative service for marketplace inventory, reservations,
//! checkout/orders, and payments per ADR-0019, implemented in Rust per
//! ADR-0022 with PostgreSQL as the persistence boundary.

pub mod auth;
pub mod clock;
pub mod config;
pub mod executor;
pub mod expiry;
pub mod handlers;
pub mod http;
pub mod model;
pub mod queries;
pub mod result;
pub mod workers;

use std::sync::Arc;

use sqlx::PgPool;

use crate::clock::Clock;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub clock: Arc<dyn Clock>,
    pub config: Arc<Config>,
}

impl AppState {
    pub fn new(pool: PgPool, clock: Arc<dyn Clock>, config: Config) -> Self {
        Self {
            pool,
            clock,
            config: Arc::new(config),
        }
    }
}
