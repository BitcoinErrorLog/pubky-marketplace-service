use axum::extract::State;
use axum::http::{header, Method, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::auth::{self, Actor};
use crate::{executor, queries, AppState};

pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(state.config.allowed_origins.clone())
        .allow_methods([Method::GET, Method::POST, Method::PUT])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    let protected = Router::new()
        .route("/v1/commands", post(execute_command))
        .route("/v1/listings/{aggregate_id}", get(queries::get_listing))
        .route(
            "/v1/listings/{aggregate_id}/bids",
            get(queries::list_listing_bids),
        )
        .route("/v1/drops/{aggregate_id}", get(queries::get_drop))
        .route(
            "/v1/drops/{aggregate_id}/me",
            get(queries::get_drop_ready_check),
        )
        .route("/v1/offers", get(queries::list_offers))
        .route("/v1/orders", get(queries::list_orders))
        .route("/v1/orders/{id}", get(queries::get_order))
        // The two entitled pickup-details reads (§A3/§A4): the ONLY paths
        // that ever open the seal. Both answer `Cache-Control: no-store`;
        // the TraceLayer logs no bodies, so the plaintext cannot reach logs.
        .route(
            "/v1/orders/{id}/pickup-details",
            get(crate::handlers::pickup::get_order_pickup_details),
        )
        .route(
            "/v1/listings/{aggregate_id}/pickup-details",
            get(crate::handlers::pickup::get_listing_pickup_details),
        )
        .route(
            "/v1/orders/{id}/review-attestation",
            get(queries::get_review_attestation),
        )
        .route(
            "/v1/sellers/{pubky}/band-consent",
            get(queries::get_band_consent),
        )
        .route("/v1/payments/{id}", get(queries::get_payment))
        .route("/v1/receipts/{id}", get(queries::get_receipt))
        .route(
            "/v1/receipts/{id}/attestation",
            get(queries::get_receipt_attestation),
        )
        .route(
            "/v1/receipts/{id}/edition-attestation",
            get(queries::get_edition_attestation),
        )
        .route("/v1/notifications", get(queries::list_notifications))
        .route(
            "/v0/sellers/me/payment-config",
            put(crate::payment_methods::put_payment_config)
                .get(crate::payment_methods::get_own_payment_config),
        )
        .route(
            "/v0/orders/{id}/payment-method",
            post(crate::payment_methods::bind_payment_method),
        )
        .route(
            "/v0/orders/{id}/fiat/verify",
            post(crate::payment_methods::verify_fiat_payment),
        )
        .route(
            "/v0/orders/{id}/fiat/mark-paid",
            post(crate::payment_methods::mark_fiat_paid),
        )
        .route(
            "/v0/orders/{id}/fiat/confirm-received",
            post(crate::payment_methods::confirm_fiat_received),
        )
        .route(
            "/v0/sellers/me/shipping-config",
            put(crate::shipping::put_shipping_config).get(crate::shipping::get_shipping_config),
        )
        .route(
            "/v0/orders/{id}/shipping/rates",
            post(crate::shipping::quote_shipping_rates),
        )
        .route(
            "/v0/orders/{id}/shipping/label",
            post(crate::shipping::purchase_shipping_label).get(crate::shipping::get_shipping_label),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_session,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/auth/sessions", post(auth::create_session))
        // Public: buyers read a seller's available rails before checkout.
        .route(
            "/v0/sellers/{pubky}/payment-config",
            get(crate::payment_methods::get_payment_config),
        )
        // Public: anyone reads a drop's schedule and redacted stock; clients
        // correct countdowns from the projection's server_time.
        .route(
            "/v0/drops/{seller_pubky}/{drop_id}",
            get(queries::get_public_drop),
        )
        // Public: PayPal's IPN callback. Unauthenticated by nature;
        // authenticity is the postback, authorization is the match against
        // the seller's configured email and the exact order total.
        .route("/v0/paypal/ipn", post(crate::payment_methods::paypal_ipn))
        .merge(protected)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Public health/capability surface. `pickup_available` tells clients
/// whether local pickup can be used on this deployment (the sealing key is
/// configured AND sandbox payments are disabled, §A7); clients hide the
/// pickup option everywhere when it is off.
async fn health(State(state): State<AppState>) -> Json<Value> {
    let (bitcoin_offer_available, age_seconds) = state
        .payment_availability
        .rail_snapshot(state.clock.now())
        .await;
    Json(json!({
        "status": "ok",
        "pickup_available": state.pickup_available(),
        "paykit_rail": {
            "bitcoin_offer_available": bitcoin_offer_available,
            "age_seconds": age_seconds,
        },
    }))
}

async fn ready(State(state): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ready" }))).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "readiness probe failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable" })),
            )
                .into_response()
        }
    }
}

async fn execute_command(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(raw): Json<Value>,
) -> Response {
    match executor::execute(&state, &actor.0, &raw).await {
        Ok((status, body)) => (status, Json(body)).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "command execution failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "ok": false,
                    "error": { "code": "INTERNAL", "message": "The command could not be processed." },
                })),
            )
                .into_response()
        }
    }
}
