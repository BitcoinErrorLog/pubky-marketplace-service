//! Role-scoped read projections.
//!
//! Every endpoint requires the same Bearer session as `/v1/commands` and
//! enforces object-level participation in the SQL `WHERE` clause, exactly
//! like the existing `GET /v1/reports` scope: the authenticated actor is
//! bound as a query parameter, so a non-participant's query can never match
//! another user's rows. Single-object endpoints return 404 for absent *and*
//! non-participant rows, so they do not reveal whether an aggregate exists.
//!
//! Redaction (ADR-0019 §8): projections never carry private delivery
//! details (`orders.delivery_address`) or Locks bundle correlation ids
//! (`payments.locks_bundle_id`); see [`crate::model::OrderRow::projection`]
//! and [`crate::model::PaymentRow::projection`]. Offer messages are
//! negotiation content between exactly the two offer participants, and the
//! projection is readable by exactly those two participants — the same
//! audience the command results already return them to.
//!
//! Dispute case files (ADR-0019 §5, §8): evidence bodies stay out of every
//! general projection and command result, but §8's operator-query clause
//! ("role-scoped, deliberately redacted views") requires that adjudication
//! remain possible. [`list_evidence`] serves the case file to exactly the
//! two dispute participants plus the configured moderator role, and
//! [`list_disputes`] gives moderators the adjudication queue. Moderator
//! evidence reads are recorded append-only in `dispute_evidence_reads`
//! within the same transaction as the read.
//!
//! Pagination: list endpoints accept `?limit=` between 1 and
//! [`MAX_LIMIT`] (default [`DEFAULT_LIMIT`]); anything else is rejected
//! with 422 `INVALID_COMMAND`. Ordering is newest-first and stable:
//! `created_at DESC, id DESC`.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use marketplace_domain::ErrorCode;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Actor;
use crate::handlers::{offers::OFFER_COLUMNS, LISTING_COLUMNS, REVIEW_COLUMNS};
use crate::model::{
    DisputeEvidenceRow, ListingRow, NotificationRow, OfferRow, OrderRow, PaymentRow, ReceiptRow,
    ReviewRow,
};
use crate::AppState;

pub const DEFAULT_LIMIT: i64 = 50;
pub const MAX_LIMIT: i64 = 200;

pub const ORDER_COLUMNS: &str = "id, auction_aggregate_id, buyer_pubky, seller_pubky, revision, \
     state, lines, delivery_address, subtotal_minor, shipping_minor, tax_minor, total_minor, \
     currency, exponent, guarantee_policy_version, payment_id, receipt_id, cancellation_reason, \
     shipment, return_request, dispute, external_refund, created_at, updated_at";

pub const PAYMENT_COLUMNS: &str = "id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
     state, confirmations, locks_bundle_id, amount_minor, currency, exponent, created_at, \
     updated_at";

pub const NOTIFICATION_COLUMNS: &str =
    "id, recipient_pubky, actor_pubky, type, aggregate_id, created_at, read_at";

pub const RECEIPT_COLUMNS: &str = "id, order_id, payment_id, issuer_pubky, recipient_pubky, \
     total_minor, currency, exponent, content_hash, issued_at";

pub const EVIDENCE_COLUMNS: &str =
    "id, order_id, submitter_pubky, body, octet_length(body) AS body_bytes, created_at";

/// The SQL participation scope for one order: participants reach their own
/// orders; the configured moderator role additionally reaches any order
/// under (or previously under) dispute — and nothing else. `$1` is the
/// order id, `$2` the authenticated actor.
fn order_access_scope(is_moderator: bool) -> &'static str {
    if is_moderator {
        "id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2 OR dispute IS NOT NULL)"
    } else {
        "id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2)"
    }
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    limit: Option<i64>,
}

impl ListQuery {
    /// The validated page size: 1..=[`MAX_LIMIT`], default [`DEFAULT_LIMIT`];
    /// `None` for an out-of-range value.
    fn limit(&self) -> Option<i64> {
        match self.limit {
            None => Some(DEFAULT_LIMIT),
            Some(limit) if (1..=MAX_LIMIT).contains(&limit) => Some(limit),
            Some(_) => None,
        }
    }
}

fn invalid_limit() -> Response {
    query_error(
        ErrorCode::InvalidCommand,
        &format!("The limit must be between 1 and {MAX_LIMIT}."),
    )
}

fn query_error(code: ErrorCode, message: &str) -> Response {
    (
        StatusCode::from_u16(code.http_status()).expect("error codes map to valid statuses"),
        Json(json!({ "ok": false, "error": { "code": code, "message": message } })),
    )
        .into_response()
}

fn internal_error(context: &str, error: &sqlx::Error) -> Response {
    tracing::error!(error = %error, "{context} query failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "ok": false,
            "error": { "code": "INTERNAL", "message": "The projection could not be read." },
        })),
    )
        .into_response()
}

/// `GET /v1/listings/{aggregate_id}`: the listing/inventory projection,
/// readable by any authenticated user (public catalog data). Exposes no
/// buyer identity beyond the auction's current leader, which the auction
/// state already makes visible to every bidder.
pub async fn get_listing(
    State(state): State<AppState>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let listing: Result<Option<ListingRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1"
    ))
    .bind(&aggregate_id)
    .fetch_optional(&state.pool)
    .await;
    match listing {
        Ok(Some(listing)) => (StatusCode::OK, Json(listing.view())).into_response(),
        Ok(None) => query_error(ErrorCode::NotFound, "The listing was not found."),
        Err(error) => internal_error("listing", &error),
    }
}

/// `GET /v1/offers`: offers where the caller is buyer or seller, never
/// another user's.
pub async fn list_offers(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(limit) = query.limit() else {
        return invalid_limit();
    };
    let offers: Result<Vec<OfferRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {OFFER_COLUMNS} FROM offers WHERE buyer_pubky = $1 OR seller_pubky = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2"
    ))
    .bind(&actor.0)
    .bind(limit)
    .fetch_all(&state.pool)
    .await;
    match offers {
        Ok(offers) => {
            let views: Vec<Value> = offers.iter().map(OfferRow::view).collect();
            (StatusCode::OK, Json(json!({ "offers": views }))).into_response()
        }
        Err(error) => internal_error("offers", &error),
    }
}

/// Attaches the order's payment projection when it exists ("each with its
/// payment if present") and its reviews. `receipt_id` points at the durable
/// receipt served by `GET /v1/receipts/{id}`.
fn order_with_payment(
    order: &OrderRow,
    payment: Option<&PaymentRow>,
    reviews: &[&ReviewRow],
) -> Value {
    let mut view = order.projection();
    if let Some(payment) = payment {
        view["payment"] = payment.projection();
    }
    view["reviews"] = Value::Array(reviews.iter().map(|review| review.view()).collect());
    view
}

/// `GET /v1/orders`: orders where the caller is buyer or seller, each with
/// its payment projection.
pub async fn list_orders(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(limit) = query.limit() else {
        return invalid_limit();
    };
    let orders: Vec<OrderRow> = match sqlx::query_as(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders WHERE buyer_pubky = $1 OR seller_pubky = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2"
    ))
    .bind(&actor.0)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    {
        Ok(orders) => orders,
        Err(error) => return internal_error("orders", &error),
    };
    let order_ids: Vec<Uuid> = orders.iter().map(|order| order.id).collect();
    let payments: Vec<PaymentRow> = match sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = ANY($1)"
    ))
    .bind(&order_ids)
    .fetch_all(&state.pool)
    .await
    {
        Ok(payments) => payments,
        Err(error) => return internal_error("payments", &error),
    };
    let reviews: Vec<ReviewRow> = match sqlx::query_as(&format!(
        "SELECT {REVIEW_COLUMNS} FROM reviews WHERE order_id = ANY($1) ORDER BY created_at, id"
    ))
    .bind(&order_ids)
    .fetch_all(&state.pool)
    .await
    {
        Ok(reviews) => reviews,
        Err(error) => return internal_error("reviews", &error),
    };
    let views: Vec<Value> = orders
        .iter()
        .map(|order| {
            let payment = payments.iter().find(|payment| payment.order_id == order.id);
            let order_reviews: Vec<&ReviewRow> = reviews
                .iter()
                .filter(|review| review.order_id == order.id)
                .collect();
            order_with_payment(order, payment, &order_reviews)
        })
        .collect();
    (StatusCode::OK, Json(json!({ "orders": views }))).into_response()
}

/// `GET /v1/orders/{id}`: a single order with its payment projection.
/// Participants only, except that a configured moderator may read an order
/// under (or previously under) dispute — the redacted projection a deciding
/// moderator needs (`dispute`, state, revision, totals; never the delivery
/// address). Absent and foreign orders are both 404.
pub async fn get_order(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let scope = order_access_scope(state.config.is_moderator(&actor.0));
    let order: Result<Option<OrderRow>, sqlx::Error> =
        sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE {scope}"))
            .bind(id)
            .bind(&actor.0)
            .fetch_optional(&state.pool)
            .await;
    let order = match order {
        Ok(Some(order)) => order,
        Ok(None) => return query_error(ErrorCode::NotFound, "The order was not found."),
        Err(error) => return internal_error("order", &error),
    };
    let payment: Result<Option<PaymentRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE order_id = $1"
    ))
    .bind(order.id)
    .fetch_optional(&state.pool)
    .await;
    let payment = match payment {
        Ok(payment) => payment,
        Err(error) => return internal_error("payment", &error),
    };
    let reviews: Result<Vec<ReviewRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {REVIEW_COLUMNS} FROM reviews WHERE order_id = $1 ORDER BY created_at, id"
    ))
    .bind(order.id)
    .fetch_all(&state.pool)
    .await;
    match reviews {
        Ok(reviews) => {
            let order_reviews: Vec<&ReviewRow> = reviews.iter().collect();
            (
                StatusCode::OK,
                Json(order_with_payment(&order, payment.as_ref(), &order_reviews)),
            )
                .into_response()
        }
        Err(error) => internal_error("reviews", &error),
    }
}

/// `GET /v1/receipts/{id}`: a single receipt, readable only by its issuer
/// (the seller) and recipient (the buyer); absent and foreign receipts are
/// both 404.
pub async fn get_receipt(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let receipt: Result<Option<ReceiptRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {RECEIPT_COLUMNS} FROM receipts \
         WHERE id = $1 AND (issuer_pubky = $2 OR recipient_pubky = $2)"
    ))
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    match receipt {
        Ok(Some(receipt)) => (StatusCode::OK, Json(receipt.view())).into_response(),
        Ok(None) => query_error(ErrorCode::NotFound, "The receipt was not found."),
        Err(error) => internal_error("receipt", &error),
    }
}

/// `GET /v1/payments/{id}`: a single payment projection, participants only;
/// absent and foreign payments are both 404.
pub async fn get_payment(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let payment: Result<Option<PaymentRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments \
         WHERE id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2)"
    ))
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    match payment {
        Ok(Some(payment)) => (StatusCode::OK, Json(payment.projection())).into_response(),
        Ok(None) => query_error(ErrorCode::NotFound, "The payment was not found."),
        Err(error) => internal_error("payment", &error),
    }
}

/// `GET /v1/notifications`: notifications delivered to the caller, never
/// another recipient's.
pub async fn list_notifications(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(limit) = query.limit() else {
        return invalid_limit();
    };
    let notifications: Result<Vec<NotificationRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {NOTIFICATION_COLUMNS} FROM notifications WHERE recipient_pubky = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2"
    ))
    .bind(&actor.0)
    .bind(limit)
    .fetch_all(&state.pool)
    .await;
    match notifications {
        Ok(notifications) => {
            let views: Vec<Value> = notifications.iter().map(NotificationRow::view).collect();
            (StatusCode::OK, Json(json!({ "notifications": views }))).into_response()
        }
        Err(error) => internal_error("notifications", &error),
    }
}

/// `GET /v1/disputes`: the moderator adjudication queue — every order under
/// (or previously under) dispute, as the same redacted order projection
/// participants receive (no delivery address; the dispute sub-document
/// carries only a content-free `evidence_count`). Without it a moderator
/// cannot learn a dispute's reason or the order revision that
/// `dispute.resolve` requires. Non-moderators are refused outright, not
/// handed an empty list.
pub async fn list_disputes(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(query): Query<ListQuery>,
) -> Response {
    if !state.config.is_moderator(&actor.0) {
        return query_error(
            ErrorCode::Unauthorized,
            "Only a configured moderator may read the dispute queue.",
        );
    }
    let Some(limit) = query.limit() else {
        return invalid_limit();
    };
    let orders: Result<Vec<OrderRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders WHERE dispute IS NOT NULL \
         ORDER BY created_at DESC, id DESC LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(&state.pool)
    .await;
    match orders {
        Ok(orders) => {
            let views: Vec<Value> = orders.iter().map(OrderRow::projection).collect();
            (StatusCode::OK, Json(json!({ "disputes": views }))).into_response()
        }
        Err(error) => internal_error("disputes", &error),
    }
}

/// `GET /v1/orders/{id}/evidence`: the dispute case file — every evidence
/// item (submitter, body, byte size, timestamp) for one order, readable by
/// exactly the two dispute participants and the configured moderator role
/// (moderators only for orders under, or previously under, dispute).
///
/// Both parties see the full file: a dispute where one side cannot see what
/// the other alleged cannot be answered, and a resolution based on evidence
/// hidden from a party could not be contested. Anyone else is refused with
/// the same 404 an absent order returns, exactly like the other
/// single-object endpoints.
///
/// A moderator-role read is privileged cross-user access: it is recorded
/// append-only in `dispute_evidence_reads` in the same transaction as the
/// read, so a failed audit write refuses the read instead of serving
/// unaudited data. Participant reads are object-scoped participation like
/// every other projection and are not audited.
pub async fn list_evidence(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(order_id): Path<Uuid>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(limit) = query.limit() else {
        return invalid_limit();
    };
    let is_moderator = state.config.is_moderator(&actor.0);
    let scope = order_access_scope(is_moderator);
    let order: Option<OrderRow> =
        match sqlx::query_as(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE {scope}"))
            .bind(order_id)
            .bind(&actor.0)
            .fetch_optional(&state.pool)
            .await
        {
            Ok(order) => order,
            Err(error) => return internal_error("evidence order", &error),
        };
    let Some(order) = order else {
        return query_error(ErrorCode::NotFound, "The order was not found.");
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal_error("evidence", &error),
    };
    let evidence: Vec<DisputeEvidenceRow> = match sqlx::query_as(&format!(
        "SELECT {EVIDENCE_COLUMNS} FROM dispute_evidence WHERE order_id = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2"
    ))
    .bind(order.id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => return internal_error("evidence", &error),
    };
    if is_moderator {
        let audited = sqlx::query(
            "INSERT INTO dispute_evidence_reads (id, order_id, reader_pubky, evidence_items, \
             created_at) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(Uuid::new_v4())
        .bind(order.id)
        .bind(&actor.0)
        .bind(evidence.len() as i64)
        .bind(state.clock.now())
        .execute(&mut *tx)
        .await;
        if let Err(error) = audited {
            return internal_error("evidence audit", &error);
        }
    }
    if let Err(error) = tx.commit().await {
        return internal_error("evidence", &error);
    }
    let views: Vec<Value> = evidence.iter().map(DisputeEvidenceRow::view).collect();
    (
        StatusCode::OK,
        Json(json!({ "order_id": order.id, "evidence": views })),
    )
        .into_response()
}
