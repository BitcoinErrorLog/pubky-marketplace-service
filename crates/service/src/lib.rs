//! Pubky Marketplace Transaction Service.
//!
//! Server-authoritative service for marketplace inventory, reservations,
//! checkout/orders, and payments per ADR-0019, implemented in Rust per
//! ADR-0022 with PostgreSQL as the persistence boundary.

pub mod attestor;
pub mod auth;
pub mod bitcoin_review;
pub mod clock;
pub mod config;
pub mod executor;
pub mod expiry;
pub mod handlers;
pub mod homeserver;
pub mod http;
pub mod locks;
pub mod model;
pub mod payment_availability;
pub mod payment_methods;
pub mod payments;
pub mod pickup;
pub mod queries;
pub mod resolve_delivery;
pub mod result;
pub mod seal;
pub mod shipping;
pub mod workers;

use std::sync::Arc;

use sqlx::PgPool;

use crate::attestor::Attestor;
use crate::clock::Clock;
use crate::config::Config;
use crate::homeserver::HomeserverListingClient;
use crate::locks::LocksRuntime;
use crate::payment_availability::PaymentAvailabilityCache;
use crate::payments::PaymentsRuntime;
use crate::pickup::PickupKeys;

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
    /// attestations are issued, no annotations are recorded, and the weekly
    /// stat job does not run (fail closed;
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
    /// Cached Paykit rail and seller claim availability.
    pub payment_availability: PaymentAvailabilityCache,
    /// The pickup-details sealing keys (local pickup design §A1). `None`
    /// when `PICKUP_DETAILS_ENCRYPTION_KEY` is unset: pickup is OFF —
    /// `pickup_details.set` is refused, no details are ever stored
    /// plaintext, and `pickup_available` reports false (all-or-none gating;
    /// see [`pickup::pickup_keys_from_env`]).
    pub pickup: Option<Arc<PickupKeys>>,
    /// Per-endpoint cache of pinned-stack readiness identities for the
    /// resolve delivery arm (§B.8.8: 15 s TTL, per endpoint).
    pub resolve_pin_cache: resolve_delivery::ResolvePinCache,
}

impl AppState {
    /// The public capability flag (§A7): pickup is available only when the
    /// sealing key is configured AND sandbox payments are disabled on this
    /// deployment.
    pub fn pickup_available(&self) -> bool {
        self.pickup.is_some() && !self.config.sandbox_payments_enabled
    }
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
            payment_availability: PaymentAvailabilityCache::default(),
            pickup: None,
            resolve_pin_cache: resolve_delivery::ResolvePinCache::default(),
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

    pub fn with_pickup(mut self, pickup: Option<Arc<PickupKeys>>) -> Self {
        self.pickup = pickup;
        self
    }
}
