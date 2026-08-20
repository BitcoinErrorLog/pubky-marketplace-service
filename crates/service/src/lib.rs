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
pub mod locks;
pub mod model;
pub mod queries;
pub mod result;
pub mod workers;

use std::sync::Arc;

use sqlx::PgPool;

use crate::clock::Clock;
use crate::config::Config;
use crate::locks::LocksRuntime;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub clock: Arc<dyn Clock>,
    pub config: Arc<Config>,
    /// Locks verification keys and lifecycle client. `None` on sandbox-only
    /// deployments: `payment.register_locks` is refused and the lifecycle
    /// poller is not scheduled (fail closed; see [`locks::runtime_from_env`]).
    pub locks: Option<Arc<LocksRuntime>>,
}

impl AppState {
    pub fn new(pool: PgPool, clock: Arc<dyn Clock>, config: Config) -> Self {
        Self {
            pool,
            clock,
            config: Arc::new(config),
            locks: None,
        }
    }

    pub fn with_locks(mut self, locks: Option<Arc<LocksRuntime>>) -> Self {
        self.locks = locks;
        self
    }
}
