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
        .route("/v1/reports", get(list_reports))
        .route("/v1/listings/{aggregate_id}", get(queries::get_listing))
        .route("/v1/offers", get(queries::list_offers))
        .route("/v1/orders", get(queries::list_orders))
        .route("/v1/orders/{id}", get(queries::get_order))
        .route("/v1/orders/{id}/evidence", get(queries::list_evidence))
        .route(
            "/v1/orders/{id}/review-attestation",
            get(queries::get_review_attestation),
        )
        .route(
            "/v1/sellers/{pubky}/band-consent",
            get(queries::get_band_consent),
        )
        .route("/v1/disputes", get(queries::list_disputes))
        .route("/v1/payments/{id}", get(queries::get_payment))
        .route("/v1/receipts/{id}", get(queries::get_receipt))
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
        .merge(protected)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
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

/// Role-scoped report queries (task 3.5): a configured moderator reads every
/// report; any other authenticated user reads only the reports they
/// submitted, never another user's.
async fn list_reports(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> Response {
    let columns = crate::handlers::report::REPORT_COLUMNS;
    let result = if state.config.is_moderator(&actor.0) {
        let sql = format!("SELECT {columns} FROM reports ORDER BY created_at DESC");
        sqlx::query_as::<_, crate::model::ReportRow>(&sql)
            .fetch_all(&state.pool)
            .await
    } else {
        let sql = format!(
            "SELECT {columns} FROM reports WHERE reporter_pubky = $1 ORDER BY created_at DESC"
        );
        sqlx::query_as::<_, crate::model::ReportRow>(&sql)
            .bind(&actor.0)
            .fetch_all(&state.pool)
            .await
    };
    match result {
        Ok(reports) => {
            let views: Vec<Value> = reports.iter().map(crate::model::ReportRow::view).collect();
            (StatusCode::OK, Json(json!({ "reports": views }))).into_response()
        }
        Err(error) => {
            tracing::error!(error = %error, "report query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "ok": false,
                    "error": { "code": "INTERNAL", "message": "Reports could not be listed." },
                })),
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
