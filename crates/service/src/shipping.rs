//! Seller shipping configuration and Shippo label purchase
//! (`/v0/sellers/me/shipping-config`, `/v0/orders/{id}/shipping/*`).
//!
//! Trust shape mirrors the payment rails: the SELLER supplies their own
//! Shippo API token (sealed at rest with the runtime cipher, never returned
//! by any read, used only as the Authorization header of server-side Shippo
//! calls), so the marketplace holds no platform shipping credentials and
//! never moves the seller's money — a purchased label is charged by Shippo
//! to the seller's own account.
//!
//! Privacy (ADR-0019 §8): the label PDF embeds the buyer's delivery
//! address, so everything label-related is SELLER-scoped. The stored label
//! never appears in the shared order projection; the buyer sees tracking
//! only when the seller ships, exactly as with a manually entered tracking
//! number.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use marketplace_domain::ErrorCode;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::model::OrderRow;
use crate::payments::{PaymentsRuntime, ShippoError};
use crate::queries::ORDER_COLUMNS;
use crate::AppState;

const CONFIG_COLUMNS: &str =
    "seller_pubky, shippo_api_key_ciphertext, ship_from, created_at, updated_at";

/// Associated-data domain for sealing the Shippo token: distinct from the
/// Stripe key's plain-pubky AAD so ciphertexts are never interchangeable
/// across purposes.
fn seal_domain(seller_pubky: &str) -> String {
    format!("{seller_pubky}#shippo")
}

#[derive(Debug, sqlx::FromRow)]
struct SellerShippingConfigRow {
    #[allow(dead_code)]
    seller_pubky: String,
    shippo_api_key_ciphertext: Option<Vec<u8>>,
    ship_from: Option<Value>,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

fn shipping_error(code: ErrorCode, reason: &str, message: &str) -> Response {
    (
        StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
        Json(json!({
            "ok": false,
            "error": { "code": code, "message": message, "reason": reason },
        })),
    )
        .into_response()
}

fn internal(context: &str, error: &dyn std::fmt::Display) -> Response {
    tracing::error!(error = %error, "{context} failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "ok": false,
            "error": { "code": "INTERNAL", "message": "The request could not be processed." },
        })),
    )
        .into_response()
}

/// The shipping surface reuses the payments runtime (its cipher seals the
/// token; its HTTP conventions reach Shippo); fail closed without it.
fn runtime(state: &AppState) -> Result<std::sync::Arc<PaymentsRuntime>, Box<Response>> {
    state.payments.clone().ok_or_else(|| {
        Box::new(shipping_error(
            ErrorCode::UpstreamUnavailable,
            "shipping_disabled",
            "Shipping integration is not enabled on this deployment.",
        ))
    })
}

async fn load_config(
    pool: &sqlx::PgPool,
    seller_pubky: &str,
) -> Result<Option<SellerShippingConfigRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {CONFIG_COLUMNS} FROM shipping_configs WHERE seller_pubky = $1"
    ))
    .bind(seller_pubky)
    .fetch_optional(pool)
    .await
}

// ---------------------------------------------------------------------------
// Seller shipping configuration
// ---------------------------------------------------------------------------

/// The ship-from address, in the same field vocabulary as the checkout
/// delivery address.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShipFromAddress {
    name: String,
    line1: String,
    #[serde(default)]
    line2: String,
    city: String,
    #[serde(default)]
    region: String,
    postal_code: String,
    country_code: String,
    #[serde(default)]
    phone: String,
    #[serde(default)]
    email: String,
}

impl ShipFromAddress {
    fn validate(&self) -> Result<(), &'static str> {
        let bounded = |value: &str, max: usize| !value.trim().is_empty() && value.len() <= max;
        if !bounded(&self.name, 100) {
            return Err("The ship-from name is required (at most 100 characters).");
        }
        if !bounded(&self.line1, 200) || self.line2.len() > 200 {
            return Err("The ship-from street address is required (at most 200 characters).");
        }
        if !bounded(&self.city, 100) {
            return Err("The ship-from city is required (at most 100 characters).");
        }
        if self.region.len() > 100 {
            return Err("The ship-from region is too long (at most 100 characters).");
        }
        if !bounded(&self.postal_code, 20) {
            return Err("The ship-from postal code is required (at most 20 characters).");
        }
        if self.country_code.len() != 2
            || !self.country_code.chars().all(|c| c.is_ascii_uppercase())
        {
            return Err("The ship-from country must be a 2-letter ISO code (e.g. US).");
        }
        if self.phone.len() > 30 || self.email.len() > 100 {
            return Err("The ship-from phone or email is too long.");
        }
        Ok(())
    }

    fn stored(&self) -> Value {
        json!({
            "name": self.name.trim(),
            "line1": self.line1.trim(),
            "line2": self.line2.trim(),
            "city": self.city.trim(),
            "region": self.region.trim(),
            "postal_code": self.postal_code.trim(),
            "country_code": self.country_code,
            "phone": self.phone.trim(),
            "email": self.email.trim(),
        })
    }
}

/// A stored address (ship-from or the order's delivery address) as Shippo's
/// address object.
fn shippo_address(stored: &Value) -> Value {
    let field = |name: &str| stored.get(name).and_then(Value::as_str).unwrap_or_default();
    json!({
        "name": field("name"),
        "street1": field("line1"),
        "street2": field("line2"),
        "city": field("city"),
        "state": field("region"),
        "zip": field("postal_code"),
        "country": field("country_code"),
        "phone": field("phone"),
        "email": field("email"),
    })
}

fn validate_shippo_api_key(value: &str) -> Result<(), &'static str> {
    if !value.starts_with("shippo_") {
        return Err("A Shippo API token starts with 'shippo_'.");
    }
    if value.len() < 12 || value.len() > 200 || !value.chars().all(|c| c.is_ascii_graphic()) {
        return Err("The Shippo API token looks malformed.");
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutShippingConfigBody {
    /// Write-only. Absent or null PRESERVES the stored token (the client can
    /// never read it back to re-send it); an empty string clears it.
    #[serde(default)]
    shippo_api_key: Option<String>,
    /// Absent or null clears the stored address.
    #[serde(default)]
    ship_from: Option<ShipFromAddress>,
}

fn config_view(ship_from: &Option<Value>, key_set: bool, updated_at: DateTime<Utc>) -> Value {
    json!({
        "ship_from": ship_from,
        "shippo_api_key_set": key_set,
        "updated_at": format_timestamp(updated_at),
    })
}

/// `PUT /v0/sellers/me/shipping-config` (seller session): full replace of
/// the ship-from address; the write-only Shippo token changes only when an
/// explicit value is sent and is never returned.
pub async fn put_shipping_config(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Json(body): Json<PutShippingConfigBody>,
) -> Response {
    let payments = match runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    if let Some(address) = &body.ship_from {
        if let Err(message) = address.validate() {
            return shipping_error(ErrorCode::InvalidCommand, "invalid_ship_from", message);
        }
    }
    let key_update: Option<Option<Vec<u8>>> = match body.shippo_api_key.as_deref() {
        None => None,
        Some("") => Some(None),
        Some(key) => {
            if let Err(message) = validate_shippo_api_key(key) {
                return shipping_error(ErrorCode::InvalidCommand, "invalid_shippo_key", message);
            }
            Some(Some(
                payments
                    .stripe_key_cipher
                    .encrypt(&seal_domain(&actor.0), key),
            ))
        }
    };
    let ship_from = body.ship_from.as_ref().map(ShipFromAddress::stored);
    let now = state.clock.now();
    let row: Result<SellerShippingConfigRow, sqlx::Error> = match key_update {
        Some(ciphertext) => {
            sqlx::query_as(&format!(
                "INSERT INTO shipping_configs \
                 (seller_pubky, shippo_api_key_ciphertext, ship_from, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $4) \
                 ON CONFLICT (seller_pubky) DO UPDATE SET \
                 shippo_api_key_ciphertext = EXCLUDED.shippo_api_key_ciphertext, \
                 ship_from = EXCLUDED.ship_from, updated_at = EXCLUDED.updated_at \
                 RETURNING {CONFIG_COLUMNS}"
            ))
            .bind(&actor.0)
            .bind(&ciphertext)
            .bind(&ship_from)
            .bind(now)
            .fetch_one(&state.pool)
            .await
        }
        None => {
            sqlx::query_as(&format!(
                "INSERT INTO shipping_configs \
                 (seller_pubky, shippo_api_key_ciphertext, ship_from, created_at, updated_at) \
                 VALUES ($1, NULL, $2, $3, $3) \
                 ON CONFLICT (seller_pubky) DO UPDATE SET \
                 ship_from = EXCLUDED.ship_from, updated_at = EXCLUDED.updated_at \
                 RETURNING {CONFIG_COLUMNS}"
            ))
            .bind(&actor.0)
            .bind(&ship_from)
            .bind(now)
            .fetch_one(&state.pool)
            .await
        }
    };
    match row {
        Ok(row) => (
            StatusCode::OK,
            Json(json!({
                "shipping_config": config_view(
                    &row.ship_from,
                    row.shippo_api_key_ciphertext.is_some(),
                    row.updated_at,
                ),
            })),
        )
            .into_response(),
        Err(error) => internal("shipping config write", &error),
    }
}

/// `GET /v0/sellers/me/shipping-config` (seller session).
pub async fn get_shipping_config(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
) -> Response {
    if runtime(&state).is_err() {
        return shipping_error(
            ErrorCode::UpstreamUnavailable,
            "shipping_disabled",
            "Shipping integration is not enabled on this deployment.",
        );
    }
    match load_config(&state.pool, &actor.0).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(json!({
                "shipping_config": config_view(
                    &row.ship_from,
                    row.shippo_api_key_ciphertext.is_some(),
                    row.updated_at,
                ),
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(json!({ "shipping_config": Value::Null })),
        )
            .into_response(),
        Err(error) => internal("shipping config read", &error),
    }
}

// ---------------------------------------------------------------------------
// Rates and label purchase
// ---------------------------------------------------------------------------

/// The parcel the seller is shipping, metric. The client prefills from the
/// listing record's package data; the seller confirms or edits before
/// quoting, because only they know what the box actually is.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParcelBody {
    weight_grams: i64,
    length_mm: i64,
    width_mm: i64,
    height_mm: i64,
}

impl ParcelBody {
    fn validate(&self) -> Result<(), &'static str> {
        if !(1..=1_000_000).contains(&self.weight_grams) {
            return Err("The parcel weight must be between 1 g and 1,000 kg.");
        }
        for dimension in [self.length_mm, self.width_mm, self.height_mm] {
            if !(1..=10_000).contains(&dimension) {
                return Err("Each parcel dimension must be between 1 mm and 10 m.");
            }
        }
        Ok(())
    }

    fn shippo_parcel(&self) -> Value {
        let cm = |mm: i64| format!("{:.1}", mm as f64 / 10.0);
        json!({
            "length": cm(self.length_mm),
            "width": cm(self.width_mm),
            "height": cm(self.height_mm),
            "distance_unit": "cm",
            "weight": self.weight_grams.to_string(),
            "mass_unit": "g",
        })
    }
}

/// Loads the order and enforces every precondition shared by the shipping
/// endpoints: seller session, a label-eligible state, and a delivery
/// address to ship to.
async fn seller_order_for_shipping(
    state: &AppState,
    actor: &str,
    order_id: Uuid,
) -> Result<OrderRow, Box<Response>> {
    let order: Option<OrderRow> =
        sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1"))
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|error| Box::new(internal("order lookup", &error)))?;
    let Some(order) = order else {
        return Err(Box::new(shipping_error(
            ErrorCode::NotFound,
            "order_not_found",
            "The order was not found.",
        )));
    };
    if order.seller_pubky != actor {
        return Err(Box::new(shipping_error(
            ErrorCode::Unauthorized,
            "not_seller",
            "Only the seller may manage shipping for this order.",
        )));
    }
    if !matches!(order.state.as_str(), "paid" | "processing") {
        return Err(Box::new(shipping_error(
            ErrorCode::InvalidState,
            "not_shippable",
            "Shipping labels apply to paid orders that have not shipped yet.",
        )));
    }
    if order.delivery_address.is_none() {
        return Err(Box::new(shipping_error(
            ErrorCode::InvalidState,
            "no_delivery_address",
            "This order carries no delivery address to ship to.",
        )));
    }
    Ok(order)
}

/// The seller's decrypted Shippo token and stored ship-from address, or the
/// exact configuration error to surface.
async fn shipping_credentials(
    state: &AppState,
    payments: &PaymentsRuntime,
    seller_pubky: &str,
) -> Result<(String, Value), Box<Response>> {
    let config = load_config(&state.pool, seller_pubky)
        .await
        .map_err(|error| Box::new(internal("shipping config read", &error)))?;
    let Some(config) = config else {
        return Err(Box::new(shipping_error(
            ErrorCode::InvalidState,
            "shipping_not_configured",
            "Configure your Shippo API token and ship-from address first.",
        )));
    };
    let Some(sealed) = config.shippo_api_key_ciphertext else {
        return Err(Box::new(shipping_error(
            ErrorCode::InvalidState,
            "shippo_key_missing",
            "No Shippo API token is configured.",
        )));
    };
    let Some(ship_from) = config.ship_from else {
        return Err(Box::new(shipping_error(
            ErrorCode::InvalidState,
            "ship_from_missing",
            "No ship-from address is configured.",
        )));
    };
    let api_key = payments
        .stripe_key_cipher
        .decrypt(&seal_domain(seller_pubky), &sealed)
        .map_err(|error| Box::new(internal("shippo token decryption", &error)))?;
    Ok((api_key, ship_from))
}

fn shippo_failure(error: ShippoError) -> Response {
    match error {
        ShippoError::KeyInvalid => shipping_error(
            ErrorCode::InvalidState,
            "shippo_key_invalid",
            "Shippo rejected the configured API token; update your shipping settings.",
        ),
        ShippoError::Rejected(message) => {
            shipping_error(ErrorCode::InvalidState, "shippo_rejected", &message)
        }
        ShippoError::Unavailable => shipping_error(
            ErrorCode::UpstreamUnavailable,
            "shippo_unavailable",
            "Shippo could not be reached; try again.",
        ),
    }
}

/// `POST /v0/orders/{id}/shipping/rates` (seller session): quotes real
/// Shippo rates for this order's delivery address with the seller's own
/// token. Nothing is purchased.
pub async fn quote_shipping_rates(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    Json(parcel): Json<ParcelBody>,
) -> Response {
    let payments = match runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    if let Err(message) = parcel.validate() {
        return shipping_error(ErrorCode::InvalidCommand, "invalid_parcel", message);
    }
    let order = match seller_order_for_shipping(&state, &actor.0, order_id).await {
        Ok(order) => order,
        Err(response) => return *response,
    };
    let (api_key, ship_from) = match shipping_credentials(&state, &payments, &actor.0).await {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let address_to = shippo_address(order.delivery_address.as_ref().expect("checked above"));
    match payments
        .shippo
        .shipment_rates(
            &api_key,
            &shippo_address(&ship_from),
            &address_to,
            &parcel.shippo_parcel(),
        )
        .await
    {
        Ok(rates) => (StatusCode::OK, Json(json!({ "ok": true, "rates": rates }))).into_response(),
        Err(error) => shippo_failure(error),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PurchaseLabelBody {
    rate_id: String,
}

/// `POST /v0/orders/{id}/shipping/label` (seller session): purchases the
/// selected rate through the seller's own Shippo account — REAL money, on
/// the seller's Shippo balance — and stores the label on the order. One
/// label per order: a repeat call returns the stored label unchanged.
pub async fn purchase_shipping_label(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    Json(body): Json<PurchaseLabelBody>,
) -> Response {
    let payments = match runtime(&state) {
        Ok(payments) => payments,
        Err(response) => return *response,
    };
    if body.rate_id.is_empty() || body.rate_id.len() > 100 {
        return shipping_error(
            ErrorCode::InvalidCommand,
            "invalid_rate",
            "A rate id from a previous quote is required.",
        );
    }
    let order = match seller_order_for_shipping(&state, &actor.0, order_id).await {
        Ok(order) => order,
        Err(response) => return *response,
    };
    if let Some(label) = order.shipping_label {
        return (StatusCode::OK, Json(json!({ "ok": true, "label": label }))).into_response();
    }
    let (api_key, _ship_from) = match shipping_credentials(&state, &payments, &actor.0).await {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let label = match payments
        .shippo
        .purchase_label(&api_key, &body.rate_id)
        .await
    {
        Ok(label) => label,
        Err(error) => return shippo_failure(error),
    };
    let now = state.clock.now();
    let stored = json!({
        "transaction_id": label.transaction_id,
        "carrier": label.carrier,
        "servicelevel": label.servicelevel,
        "amount": label.amount,
        "currency": label.currency,
        "tracking_number": label.tracking_number,
        "tracking_url": label.tracking_url,
        "label_url": label.label_url,
        "purchased_at": format_timestamp(now),
    });
    // First writer wins: a concurrent purchase attempt that lost the race
    // reads back the stored label instead of overwriting it. The label
    // itself was still bought on Shippo's side; the seller sees exactly one
    // on the order, and duplicates are theirs to void in Shippo.
    let written = sqlx::query(
        "UPDATE orders SET shipping_label = $2, updated_at = $3 \
         WHERE id = $1 AND shipping_label IS NULL",
    )
    .bind(order.id)
    .bind(&stored)
    .bind(now)
    .execute(&state.pool)
    .await;
    match written {
        Ok(result) if result.rows_affected() == 1 => {
            (StatusCode::OK, Json(json!({ "ok": true, "label": stored }))).into_response()
        }
        Ok(_) => {
            let current: Result<Option<(Value,)>, sqlx::Error> =
                sqlx::query_as("SELECT shipping_label FROM orders WHERE id = $1")
                    .bind(order.id)
                    .fetch_optional(&state.pool)
                    .await;
            match current {
                Ok(Some((label,))) => {
                    (StatusCode::OK, Json(json!({ "ok": true, "label": label }))).into_response()
                }
                Ok(None) => internal("label readback", &"order disappeared"),
                Err(error) => internal("label readback", &error),
            }
        }
        Err(error) => internal("label write", &error),
    }
}

/// `GET /v0/orders/{id}/shipping/label` (seller session): the stored label,
/// or 404 when none was purchased.
pub async fn get_shipping_label(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
) -> Response {
    let order: Option<OrderRow> =
        match sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = $1"))
            .bind(order_id)
            .fetch_optional(&state.pool)
            .await
        {
            Ok(order) => order,
            Err(error) => return internal("order lookup", &error),
        };
    let Some(order) = order else {
        return shipping_error(
            ErrorCode::NotFound,
            "order_not_found",
            "The order was not found.",
        );
    };
    if order.seller_pubky != actor.0 {
        return shipping_error(
            ErrorCode::Unauthorized,
            "not_seller",
            "Only the seller may read this order's shipping label.",
        );
    }
    match order.shipping_label {
        Some(label) => {
            (StatusCode::OK, Json(json!({ "ok": true, "label": label }))).into_response()
        }
        None => shipping_error(
            ErrorCode::NotFound,
            "label_not_found",
            "No shipping label has been purchased for this order.",
        ),
    }
}
