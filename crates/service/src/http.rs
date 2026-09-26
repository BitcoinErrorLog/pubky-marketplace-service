use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::{header, Method, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Extension, Json, Router};
use serde_json::{json, Value};
use std::time::Instant;
use tower_http::cors::CorsLayer;

use crate::auth::{self, Actor};
use crate::{executor, logging, queries, AppState};

pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(state.config.allowed_origins.clone())
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    let inventory = Router::new()
        .route(
            "/v1/inventory/adjust",
            post(crate::inventory::adjust_inventory),
        )
        .route(
            "/v1/inventory/listings/{aggregate_id}",
            get(crate::inventory::get_inventory_projection),
        )
        .route_layer(middleware::from_fn(auth::require_inventory_capability))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_session,
        ))
        .layer(DefaultBodyLimit::max(
            crate::inventory::MAX_INVENTORY_BODY_BYTES,
        ));

    let automation = Router::new()
        .route(
            "/v1/auth/sessions/{id}",
            patch(auth::update_session_metadata).delete(auth::revoke_session),
        )
        .route(
            "/v1/sellers/{pubky}/listings",
            get(crate::automation::list_seller_listings),
        )
        .route(
            "/v1/listings/{seller}/{listing_id}",
            get(crate::automation::get_seller_listing),
        )
        .route(
            "/v1/sellers/{pubky}/orders",
            get(crate::automation::list_seller_orders),
        )
        .route(
            "/v1/sellers/{pubky}/events",
            get(crate::automation::list_seller_events),
        )
        .route("/v1/listings/sync-many", post(crate::automation::sync_many))
        .route("/v1/webhooks", post(crate::automation::add_webhook))
        .route(
            "/v1/webhooks/{id}/rotate",
            post(crate::automation::rotate_webhook),
        )
        .route(
            "/v1/webhooks/{id}",
            delete(crate::automation::delete_webhook),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            crate::automation::require_automation_rate,
        ))
        .route_layer(middleware::from_fn(auth::require_inventory_capability))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_session,
        ))
        .layer(DefaultBodyLimit::max(64 * 1024));

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
            "/v1/orders/{id}/delivery-email",
            get(crate::handlers::digital_manual::get_order_delivery_email),
        )
        .route(
            "/v1/orders/{id}/digital-evidence",
            get(crate::handlers::digital_orders::get_order_digital_evidence),
        )
        .route(
            "/v1/orders/{id}/digital-delivery",
            get(crate::handlers::digital_orders::get_order_digital_delivery),
        )
        .route(
            "/v1/listings/{aggregate_id}/digital-delivery",
            get(crate::handlers::digital::get_listing_digital_delivery),
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
            "/v0/orders/{id}/confirm-bitcoin-payment",
            post(crate::bitcoin_review::confirm_bitcoin_payment),
        )
        .route(
            "/v0/orders/{id}/bitcoin/resolve",
            post(crate::bitcoin_review::resolve_bitcoin_payment),
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
        .route(
            "/v1/auth/sessions",
            post(auth::create_session).get(auth::list_sessions),
        )
        .route("/v1/auth/grant-flows", post(crate::grant::create_flow))
        .route(
            "/v1/auth/grant-flows/{flow_id}",
            get(crate::grant::get_status),
        )
        .route(
            "/v1/auth/grant-flows/{flow_id}/cancel",
            post(crate::grant::cancel_flow),
        )
        .route(
            "/v1/auth/grant-flows/{flow_id}/result-nonces",
            post(crate::grant::issue_result_nonce),
        )
        .route(
            "/v1/auth/grant-flows/{flow_id}/result-ticket",
            post(crate::grant::result_ticket),
        )
        .route(
            "/v1/auth/grant-flows/{flow_id}/claim",
            post(crate::grant::claim_result),
        )
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
        .merge(inventory)
        .merge(automation)
        .layer(cors)
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

async fn log_request(request: axum::extract::Request, next: middleware::Next) -> Response {
    let started = Instant::now();
    let method = request.method().to_string();
    let route = logging::route_template(&request).to_string();
    let response = next.run(request).await;
    let latency_ms = started.elapsed().as_millis() as u64;
    logging::log_http_request(&method, &route, &response, latency_ms);
    response
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
        "offer_checkout": true,
        "pickup_available": state.pickup_available(),
        "digital_delivery_available": state.digital_delivery_available(),
        "digital_delivery_max_bytes": state.config.digital_delivery_max_bytes,
        "paykit_rail": {
            "bitcoin_offer_available": bitcoin_offer_available,
            "age_seconds": age_seconds,
        },
    }))
}

async fn ready(State(state): State<AppState>) -> Response {
    if let Some(audit) = &state.refusal_audit {
        if !audit.is_ready() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable" })),
            )
                .into_response();
        }
    }
    // A purge that stopped running leaves buyer addresses past their
    // retention; readiness reports it rather than keeping them silently.
    match crate::handlers::digital_manual::overdue_delivery_emails(
        &state.pool,
        state.clock.now(),
        state.config.buyer_email_retention_days,
        state.config.buyer_email_unpaid_retention_days,
    )
    .await
    {
        Ok(0) => {}
        Ok(_) => {
            tracing::error!(
                "buyer delivery emails are past their retention; the purge is not running"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable", "reason": "delivery_email_purge_overdue" })),
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(error = %error, "readiness probe failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable" })),
            )
                .into_response();
        }
    }
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
        Ok((status, body)) => match crate::reserve_secrecy::guard_command_result(&actor.0, body) {
            Ok(body) => (status, Json(body)).into_response(),
            Err(body) => (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response(),
        },
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
