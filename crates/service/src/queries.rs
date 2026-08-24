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
//! details (`orders.delivery_address`) or the Locks bundle correlation,
//! which exists only encrypted in `payment_locks_correlations` and has no
//! serialization path at all; see [`crate::model::OrderRow::projection`]
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
use chrono::{DateTime, Utc};
use marketplace_domain::ErrorCode;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Actor;
use crate::clock::format_timestamp;
use crate::handlers::{
    drops::DROP_COLUMNS, offers::OFFER_COLUMNS, LISTING_COLUMNS, REVIEW_COLUMNS,
};
use crate::model::{
    DisputeEvidenceRow, DropRow, ListingRow, NotificationRow, OfferRow, OrderRow, PaymentRow,
    ReceiptRow, ReviewRow,
};
use crate::AppState;

pub const DEFAULT_LIMIT: i64 = 50;
pub const MAX_LIMIT: i64 = 200;

pub const ORDER_COLUMNS: &str =
    "id, auction_aggregate_id, drop_aggregate_id, buyer_pubky, seller_pubky, revision, \
     state, lines, delivery_address, subtotal_minor, shipping_minor, tax_minor, total_minor, \
     currency, exponent, guarantee_policy_version, payment_id, receipt_id, edition, \
     cancellation_reason, stock_held, hold_expires_at, \
     shipment, return_request, dispute, external_refund, payment_method, fiat_checkout_url, \
     payment_reported_at, fiat_transaction_ref, fiat_verified_by, shipping_label, paykit_request_reference, paykit_request_state, \
     paykit_last_checked_at, created_at, updated_at";

pub const PAYMENT_COLUMNS: &str = "id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
     state, confirmations, amount_minor, currency, exponent, created_at, updated_at";

pub const NOTIFICATION_COLUMNS: &str =
    "id, recipient_pubky, actor_pubky, type, aggregate_id, amount, created_at, read_at";

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

/// `GET /v1/orders/{id}/review-attestation`: the actor's own stored purchase
/// attestation for the order (ADR 0024 §4). Issuance is deterministic per
/// (order, reviewer), so this re-fetch is idempotent — no consumption
/// semantics. 404 covers absent orders, foreign orders, and orders the
/// actor has not reviewed, indistinguishably.
pub async fn get_review_attestation(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let row: Result<Option<(String, Value)>, sqlx::Error> = sqlx::query_as(
        "SELECT a.jws, a.claims FROM review_attestations a \
         JOIN orders o ON o.id = a.order_id \
         WHERE a.order_id = $1 AND a.reviewer_pubky = $2 \
           AND (o.buyer_pubky = $2 OR o.seller_pubky = $2)",
    )
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    match row {
        Ok(Some((jws, claims))) => (
            StatusCode::OK,
            Json(json!({ "attestation": { "jws": jws, "claims": claims } })),
        )
            .into_response(),
        Ok(None) => query_error(ErrorCode::NotFound, "The review attestation was not found."),
        Err(error) => internal_error("review attestation", &error),
    }
}

/// `GET /v1/sellers/{pubky}/band-consent`: the seller's standing amount-band
/// preference (ratified D2). Readable by any authenticated user — it is a
/// disclosure preference, not private order data — so the buyer's client can
/// honestly decide whether to surface the per-review band opt-in. Absent row
/// means "not consented".
pub async fn get_band_consent(
    State(state): State<AppState>,
    Extension(_actor): Extension<Actor>,
    Path(seller_pubky): Path<String>,
) -> Response {
    if !marketplace_domain::pubky::is_valid_pubky(&seller_pubky) {
        return query_error(ErrorCode::InvalidCommand, "The seller pubky is invalid.");
    }
    let row: Result<Option<(bool,)>, sqlx::Error> = sqlx::query_as(
        "SELECT allows_amount_band FROM attestation_band_consents WHERE seller_pubky = $1",
    )
    .bind(&seller_pubky)
    .fetch_optional(&state.pool)
    .await;
    match row {
        Ok(row) => (
            StatusCode::OK,
            Json(json!({
                "seller_pubky": seller_pubky,
                "allows_amount_band": row.map(|(allows,)| allows).unwrap_or(false),
            })),
        )
            .into_response(),
        Err(error) => internal_error("band consent", &error),
    }
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

/// `GET /v1/listings/{aggregate_id}/bids`: the auction's bid history as the
/// visible price progression — sequence, bidder, the visible price right
/// after each bid, and when. Any authenticated user may read it (the same
/// audience that can bid), which is what makes the one-winner algorithm
/// auditable from outside. Deliberately ABSENT: every bidder's proxy
/// maximum, which stays secret forever; bids recorded before the visible
/// price existed show `visible_amount: null` rather than an invented figure.
pub async fn list_listing_bids(
    State(state): State<AppState>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let listing: Result<Option<ListingRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1"
    ))
    .bind(&aggregate_id)
    .fetch_optional(&state.pool)
    .await;
    let listing = match listing {
        Ok(Some(listing)) => listing,
        Ok(None) => return query_error(ErrorCode::NotFound, "The listing was not found."),
        Err(error) => return internal_error("listing", &error),
    };
    type BidHistoryRow = (i64, String, Option<i64>, String, i32, DateTime<Utc>);
    let rows: Result<Vec<BidHistoryRow>, sqlx::Error> = sqlx::query_as(
        "SELECT sequence, bidder_pubky, visible_amount_minor, currency, exponent, created_at \
         FROM bids WHERE listing_aggregate_id = $1 ORDER BY sequence",
    )
    .bind(&aggregate_id)
    .fetch_all(&state.pool)
    .await;
    match rows {
        Ok(rows) => {
            let bids: Vec<Value> = rows
                .into_iter()
                .map(
                    |(sequence, bidder_pubky, visible_amount_minor, currency, exponent, at)| {
                        json!({
                            "sequence": sequence,
                            "bidder_pubky": bidder_pubky,
                            "visible_amount": visible_amount_minor
                                .map(|amount| crate::model::money_json(amount, &currency, exponent)),
                            "created_at": crate::clock::format_timestamp(at),
                        })
                    },
                )
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "bids": bids,
                    "auction": listing.auction.clone().unwrap_or(Value::Null),
                    // Clients correct their countdown against the service
                    // clock — the only clock auctions run on.
                    "server_time": crate::clock::format_timestamp(state.clock.now()),
                })),
            )
                .into_response()
        }
        Err(error) => internal_error("bid history", &error),
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

/// One row loaded for receipt attestation issuance: the receipt's identity
/// and stored creation instant plus its order's participants and totals —
/// every input the claims derive from.
type ReceiptAttestationRow = (Uuid, Uuid, DateTime<Utc>, String, String, i64, String, i32);

/// `GET /v1/receipts/{id}/attestation`: a compact JWS signed by the
/// attestor, attesting the receipt's facts (participants, order and receipt
/// ids, totals, `paid_at`) so the portable receipt document a buyer or
/// seller publishes on their own homeserver stays verifiable after this
/// operator disappears ("credible exit for orders"). Readable by exactly
/// the order's buyer and seller; absent and foreign receipts are both 404,
/// exactly like `GET /v1/receipts/{id}`.
///
/// Every claim derives from stored rows (the receipt's creation instant,
/// the order's totals) — never from the current time — so the JWS is
/// deterministic: repeated calls return the byte-identical token and
/// nothing is stored. Without a configured attestor the fetch is 404, the
/// same observable outcome the review-attestation re-fetch has on an
/// attestor-less deployment.
pub async fn get_receipt_attestation(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let row: Result<Option<ReceiptAttestationRow>, sqlx::Error> = sqlx::query_as(
        "SELECT r.id, r.order_id, r.issued_at, o.buyer_pubky, o.seller_pubky, \
         o.total_minor, o.currency, o.exponent \
         FROM receipts r JOIN orders o ON o.id = r.order_id \
         WHERE r.id = $1 AND (o.buyer_pubky = $2 OR o.seller_pubky = $2)",
    )
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    let (receipt_id, order_id, issued_at, buyer, seller, total_minor, currency, exponent) =
        match row {
            Ok(Some(row)) => row,
            Ok(None) => return query_error(ErrorCode::NotFound, "The receipt was not found."),
            Err(error) => return internal_error("receipt attestation", &error),
        };
    let Some(attestor) = state.attestor.as_ref() else {
        return query_error(
            ErrorCode::NotFound,
            "The receipt attestation was not found.",
        );
    };
    let issued = attestor.issue_receipt_attestation(
        order_id,
        receipt_id,
        &buyer,
        &seller,
        total_minor,
        &currency,
        i64::from(exponent),
        issued_at,
    );
    (
        StatusCode::OK,
        Json(json!({
            "receipt_attestation": { "jws": issued.jws, "claims": issued.claims },
        })),
    )
        .into_response()
}

/// One row loaded for drop edition attestation issuance: the receipt's
/// identity and stored creation instant, the order's participants and
/// edition, and the drop's aggregate id and total — every input the claims
/// derive from. The `drops` join means only drop-stamped orders match.
type EditionAttestationRow = (Uuid, DateTime<Utc>, String, String, i32, String, i64);

/// `GET /v1/receipts/{id}/edition-attestation`: a compact JWS signed by the
/// attestor, attesting which numbered edition of a drop this paid order
/// received (`edition` of `of`). Mirrors `GET /v1/receipts/{id}/attestation`
/// exactly: readable by the order's buyer and seller only, and every claim
/// derives from stored rows (the receipt's creation instant, the order's
/// edition, the drop's total) — never from the current time — so the JWS is
/// deterministic and nothing is stored.
///
/// Absent receipts, foreign receipts, non-drop orders (no
/// `drop_aggregate_id`/`edition`), and attestor-less deployments are all
/// 404 with the same body as `GET /v1/receipts/{id}`, indistinguishably.
pub async fn get_edition_attestation(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(id): Path<Uuid>,
) -> Response {
    let row: Result<Option<EditionAttestationRow>, sqlx::Error> = sqlx::query_as(
        "SELECT r.id, r.issued_at, o.buyer_pubky, o.seller_pubky, o.edition, \
         o.drop_aggregate_id, d.total_quantity \
         FROM receipts r \
         JOIN orders o ON o.id = r.order_id \
         JOIN drops d ON d.aggregate_id = o.drop_aggregate_id \
         WHERE r.id = $1 AND (o.buyer_pubky = $2 OR o.seller_pubky = $2) \
           AND o.edition IS NOT NULL",
    )
    .bind(id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    let (receipt_id, issued_at, buyer, seller, edition, drop_aggregate_id, total_quantity) =
        match row {
            Ok(Some(row)) => row,
            Ok(None) => return query_error(ErrorCode::NotFound, "The receipt was not found."),
            Err(error) => return internal_error("edition attestation", &error),
        };
    let Some(attestor) = state.attestor.as_ref() else {
        return query_error(ErrorCode::NotFound, "The receipt was not found.");
    };
    // The claim carries the DROP ID (the seller's record identifier), parsed
    // from the aggregate id's `drop:{seller}_{drop_id}` shape; the seller in
    // that shape is the drop owner, which is the order's seller.
    let drop_id = drop_aggregate_id
        .strip_prefix(&format!("drop:{seller}_"))
        .expect("drop aggregate ids have the drop:{seller}_{drop_id} shape");
    let issued = attestor.issue_drop_edition_attestation(
        receipt_id,
        &buyer,
        &seller,
        drop_id,
        i64::from(edition),
        total_quantity,
        issued_at,
    );
    (
        StatusCode::OK,
        Json(json!({
            "edition_attestation": { "jws": issued.jws, "claims": issued.claims },
        })),
    )
        .into_response()
}

/// The banded remaining-stock disclosure for `stock_display = 'bands'`:
/// `last_few` at or below 5% of the total (with a minimum threshold of one
/// unit, so small drops still reach the band), `low` at or below 25%, and
/// `plenty` above that. Exact integer comparisons — no floating point.
fn remaining_band(remaining: i64, total: i64) -> &'static str {
    if remaining <= (total / 20).max(1) {
        "last_few"
    } else if remaining * 4 <= total {
        "low"
    } else {
        "plenty"
    }
}

/// `GET /v0/drops/{seller_pubky}/{drop_id}`: the public drop projection —
/// no session, like the public seller payment-config route. Stock-display
/// redaction is applied SERVER-side: `exact` exposes `remaining`, `bands`
/// exposes only `remaining_band` ([`remaining_band`]), `hidden` exposes
/// neither — an exact count never leaves the service under bands/hidden.
///
/// The read applies the same lazy server-time transitions gating uses,
/// inside a transaction with the drop row locked, so a public read never
/// shows `announced` after `starts_at`. `server_time` is the service clock
/// now, in the canonical wire timestamp format — clients correct their
/// countdowns from it instead of trusting local clocks.
pub async fn get_public_drop(
    State(state): State<AppState>,
    Path((seller_pubky, drop_id)): Path<(String, String)>,
) -> Response {
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal_error("public drop", &error),
    };
    let drop: Option<DropRow> = match sqlx::query_as(&format!(
        "SELECT {DROP_COLUMNS} FROM drops \
         WHERE seller_pubky = $1 AND drop_id = $2 FOR UPDATE"
    ))
    .bind(&seller_pubky)
    .bind(&drop_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(drop) => drop,
        Err(error) => return internal_error("public drop", &error),
    };
    let Some(drop) = drop else {
        return query_error(ErrorCode::NotFound, "The drop was not found.");
    };
    let now = state.clock.now();
    let drop =
        match crate::handlers::drops::apply_time_transitions(&mut tx, drop, Uuid::new_v4(), now)
            .await
        {
            Ok(drop) => drop,
            Err(error) => return internal_error("public drop", &error),
        };
    if let Err(error) = tx.commit().await {
        return internal_error("public drop", &error);
    }

    let (remaining, band) = match drop.stock_display.as_str() {
        "exact" => (json!(drop.remaining_quantity), Value::Null),
        "bands" => (
            Value::Null,
            json!(remaining_band(drop.remaining_quantity, drop.total_quantity)),
        ),
        _ => (Value::Null, Value::Null),
    };
    (
        StatusCode::OK,
        Json(json!({
            "drop": {
                "seller_pubky": drop.seller_pubky,
                "drop_id": drop.drop_id,
                "aggregate_id": drop.aggregate_id,
                "state": drop.state,
                "format": drop.format,
                "starts_at": format_timestamp(drop.starts_at),
                "ends_at": drop.ends_at.map(format_timestamp),
                "stock_display": drop.stock_display,
                "total_quantity": drop.total_quantity,
                "per_buyer_limit": drop.per_buyer_limit,
                "remaining": remaining,
                "remaining_band": band,
                "revision": drop.revision,
                "server_time": format_timestamp(now),
            },
        })),
    )
        .into_response()
}

/// `GET /v1/drops/{aggregate_id}`: the seller's own drop projection — the
/// full facts (exact remaining, paid count, distinct buyer count, schedule,
/// caps, listing ids, revision) plus `server_time`. Scoped to exactly the
/// seller: absent and foreign drops are both 404, like the other
/// single-object endpoints, so the read does not reveal whether an
/// aggregate exists.
pub async fn get_drop(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let drop: Result<Option<DropRow>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {DROP_COLUMNS} FROM drops WHERE aggregate_id = $1 AND seller_pubky = $2"
    ))
    .bind(&aggregate_id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    let drop = match drop {
        Ok(Some(drop)) => drop,
        Ok(None) => return query_error(ErrorCode::NotFound, "The drop was not found."),
        Err(error) => return internal_error("drop", &error),
    };
    // Buyers currently accounted at least one held or paid unit; a buyer
    // whose holds all lapsed no longer counts.
    let buyer_count: Result<(i64,), sqlx::Error> = sqlx::query_as(
        "SELECT COUNT(*) FROM drop_purchases WHERE drop_aggregate_id = $1 AND quantity > 0",
    )
    .bind(&aggregate_id)
    .fetch_one(&state.pool)
    .await;
    let (buyer_count,) = match buyer_count {
        Ok(count) => count,
        Err(error) => return internal_error("drop buyers", &error),
    };
    let mut view = drop.view();
    view["buyer_count"] = json!(buyer_count);
    view["server_time"] = json!(format_timestamp(state.clock.now()));
    (StatusCode::OK, Json(json!({ "drop": view }))).into_response()
}

/// `GET /v1/drops/{aggregate_id}/me`: the buyer ready-check — any
/// authenticated user reads their own per-drop counters: units currently
/// held or paid (`purchased`), the drop's per-buyer limit, and the
/// allowance left. Zeros purchased when the buyer has no counter row.
pub async fn get_drop_ready_check(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(aggregate_id): Path<String>,
) -> Response {
    let row: Result<Option<(i64, i64)>, sqlx::Error> = sqlx::query_as(
        "SELECT d.per_buyer_limit, COALESCE(p.quantity, 0)::BIGINT FROM drops d \
         LEFT JOIN drop_purchases p \
           ON p.drop_aggregate_id = d.aggregate_id AND p.buyer_pubky = $2 \
         WHERE d.aggregate_id = $1",
    )
    .bind(&aggregate_id)
    .bind(&actor.0)
    .fetch_optional(&state.pool)
    .await;
    match row {
        Ok(Some((per_buyer_limit, purchased))) => (
            StatusCode::OK,
            Json(json!({
                "purchased": purchased,
                "per_buyer_limit": per_buyer_limit,
                "remaining_allowance": per_buyer_limit - purchased,
            })),
        )
            .into_response(),
        Ok(None) => query_error(ErrorCode::NotFound, "The drop was not found."),
        Err(error) => internal_error("drop ready-check", &error),
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
