//! Pubky Marketplace Transaction Service.
//!
//! Server-authoritative service for marketplace inventory, reservations,
//! checkout/orders, and payments per ADR-0019, implemented in Rust per
//! ADR-0022 with PostgreSQL as the persistence boundary.

pub mod attestor;
pub mod auth;
pub mod clock;
pub mod config;
pub mod executor;
pub mod expiry;
pub mod handlers;
pub mod homeserver;
pub mod http;
pub mod locks;
pub mod model;
pub mod payment_methods;
pub mod payments;
pub mod queries;
pub mod result;
pub mod shipping;
pub mod workers;

use std::sync::Arc;

use sqlx::PgPool;

use crate::attestor::Attestor;
use crate::clock::Clock;
use crate::config::Config;
use crate::homeserver::HomeserverListingClient;
use crate::locks::LocksRuntime;
use crate::payments::PaymentsRuntime;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub clock: Arc<dyn Clock>,
    pub config: Arc<Config>,
    /// Locks verification keys and lifecycle client. `None` on sandbox-only
    /// deployments: `payment.register_locks` is refused and the lifecycle
    /// poller is not scheduled (fail closed; see [`locks::runtime_from_env`]).
    pub locks: Option<Arc<LocksRuntime>>,
    /// The attestor signing identity (ADR 0024). `None` when the deployment
    /// carries no attestor key: reviews still work but no purchase
    /// attestations are issued, no annotations are recorded, the weekly stat
    /// job does not run, and `attestation.disavow` is refused (fail closed;
    /// see [`attestor::Attestor::from_env`]).
    pub attestor: Option<Arc<Attestor>>,
    /// The homeserver listing-record fetch backing `listing.sync`. Required
    /// in production (`HOMESERVER_URL`); `None` only in tests that do not
    /// exercise sync, where the command is refused (fail closed).
    pub homeserver: Option<Arc<dyn HomeserverListingClient>>,
    /// Seller payment-method rails: Stripe key sealing/verification and the
    /// signed Paykit client. `None` when `STRIPE_KEY_ENCRYPTION_KEY` is
    /// unset: the whole `/v0` payment-methods surface is refused (fail
    /// closed; see [`payments::payments_runtime_from_env`]).
    pub payments: Option<Arc<PaymentsRuntime>>,
}

impl AppState {
    pub fn new(pool: PgPool, clock: Arc<dyn Clock>, config: Config) -> Self {
        Self {
            pool,
            clock,
            config: Arc::new(config),
            locks: None,
            attestor: None,
            homeserver: None,
            payments: None,
        }
    }

    pub fn with_locks(mut self, locks: Option<Arc<LocksRuntime>>) -> Self {
        self.locks = locks;
        self
    }

    pub fn with_attestor(mut self, attestor: Option<Arc<Attestor>>) -> Self {
        self.attestor = attestor;
        self
    }

    pub fn with_homeserver(mut self, homeserver: Option<Arc<dyn HomeserverListingClient>>) -> Self {
        self.homeserver = homeserver;
        self
    }

    pub fn with_payments(mut self, payments: Option<Arc<PaymentsRuntime>>) -> Self {
        self.payments = payments;
        self
    }
}
