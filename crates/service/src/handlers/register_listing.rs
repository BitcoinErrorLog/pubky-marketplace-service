use chrono::{DateTime, Utc};
use marketplace_domain::commands::{Command, RegisterListingPayload, SaleFormat};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{current_listing_revision, fetch_listing, fetch_listing_for_update};
use crate::model::{money_json, ListingRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RegisterListingPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if actor != payload.seller_pubky {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the listing seller may register inventory.",
        )));
    }
    let expected_aggregate_id =
        ids::listing_aggregate_id(&payload.seller_pubky, &payload.listing_id);
    if command.aggregate_id != expected_aggregate_id {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The listing aggregate id does not match its seller and listing.",
        )));
    }

    let current = fetch_listing_for_update(tx, &command.aggregate_id).await?;
    let current_revision = current.as_ref().map(|c| c.server_revision).unwrap_or(0);
    if command.expected_revision != current_revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            current_revision,
        )));
    }
    if let Some(current) = &current {
        if payload.listing_revision <= current.listing_revision {
            return Ok(Err(CommandFailure::with_revision(
                ErrorCode::RevisionConflict,
                "The public listing revision must advance.",
                current_revision,
            )));
        }
    }

    apply_registration(
        tx,
        actor,
        command.command_id,
        &command.aggregate_id,
        payload,
        current.as_ref(),
        "listing.registered",
        now,
    )
    .await
}

/// Writes (or refreshes) the listing aggregate from a validated registration
/// payload: the shared tail of `listing.register` and `listing.sync`. The
/// caller has already taken the row lock (`current` comes from
/// `fetch_listing_for_update`) and made its own authority and revision
/// decisions; this enforces only the inventory invariant that survives both
/// paths — quantity can never fall below committed (reserved + sold) stock.
// Eight positional facts of one write; both callers must supply all of them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_registration(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command_id: Uuid,
    aggregate_id: &str,
    payload: &RegisterListingPayload,
    current: Option<&ListingRow>,
    event_kind: &str,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let current_revision = current.as_ref().map(|c| c.server_revision).unwrap_or(0);
    let reserved = current.as_ref().map(|c| c.reserved_quantity).unwrap_or(0);
    let sold = current.as_ref().map(|c| c.sold_quantity).unwrap_or(0);
    let committed = reserved + sold;
    if payload.quantity < committed {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InvariantViolation,
            "Listing quantity cannot fall below committed inventory.",
            current_revision,
        )));
    }

    let state = if payload.quantity == committed {
        if committed > 0 {
            "reserved"
        } else {
            "sold"
        }
    } else {
        "available"
    };
    let sale_format = match payload.sale_format {
        SaleFormat::FixedPrice => "fixed_price",
        SaleFormat::Auction => "auction",
    };
    let auction = payload.auction_terms.as_ref().map(|terms| {
        auction_json(
            terms,
            payload,
            current.as_ref().and_then(|c| c.auction.as_ref()),
            now,
        )
    });
    let new_revision = current_revision + 1;

    let written = if current.is_none() {
        let inserted = sqlx::query(
            "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
             listing_revision, content_hash, server_revision, state, total_quantity, \
             available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
             unit_price_currency, unit_price_exponent, sale_format, auction, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
             ON CONFLICT (aggregate_id) DO NOTHING",
        )
        .bind(aggregate_id)
        .bind(&payload.seller_pubky)
        .bind(&payload.listing_id)
        .bind(&payload.title)
        .bind(payload.listing_revision)
        .bind(&payload.content_hash)
        .bind(new_revision)
        .bind(state)
        .bind(payload.quantity)
        .bind(payload.quantity - committed)
        .bind(reserved)
        .bind(sold)
        .bind(payload.unit_price.amount_minor)
        .bind(&payload.unit_price.currency)
        .bind(payload.unit_price.exponent)
        .bind(sale_format)
        .bind(&auction)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        inserted.rows_affected() == 1
    } else {
        let updated = sqlx::query(
            "UPDATE listings SET title = $3, listing_revision = $4, content_hash = $5, \
             server_revision = $6, state = $7, total_quantity = $8, available_quantity = $9, \
             unit_price_amount_minor = $10, unit_price_currency = $11, unit_price_exponent = $12, \
             sale_format = $13, auction = $14, updated_at = $15 \
             WHERE aggregate_id = $1 AND server_revision = $2",
        )
        .bind(aggregate_id)
        .bind(current_revision)
        .bind(&payload.title)
        .bind(payload.listing_revision)
        .bind(&payload.content_hash)
        .bind(new_revision)
        .bind(state)
        .bind(payload.quantity)
        .bind(payload.quantity - committed)
        .bind(payload.unit_price.amount_minor)
        .bind(&payload.unit_price.currency)
        .bind(payload.unit_price.exponent)
        .bind(sale_format)
        .bind(&auction)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        updated.rows_affected() == 1
    };
    if !written {
        let latest = current_listing_revision(tx, aggregate_id).await?;
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            latest,
        )));
    }

    let event_id = insert_event(
        tx,
        command_id,
        aggregate_id,
        new_revision,
        actor,
        event_kind,
        now,
    )
    .await?;

    let listing = fetch_listing(tx, aggregate_id)
        .await?
        .expect("listing was just written in this transaction");
    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "listing", "listing": listing.view() }),
    }))
}

fn auction_json(
    terms: &marketplace_domain::commands::AuctionTerms,
    payload: &RegisterListingPayload,
    current: Option<&Value>,
    now: DateTime<Utc>,
) -> Value {
    let existing = |field: &str| current.and_then(|value| value.get(field)).cloned();
    let status = existing("status").unwrap_or_else(|| {
        if terms.starts_at > now {
            json!("scheduled")
        } else {
            json!("active")
        }
    });
    let unit_price = money_json(
        payload.unit_price.amount_minor,
        &payload.unit_price.currency,
        payload.unit_price.exponent,
    );
    let reserve_met = existing("reserve_met").unwrap_or_else(|| {
        json!(terms
            .reserve_price
            .as_ref()
            .is_none_or(|reserve| payload.unit_price.amount_minor >= reserve.amount_minor))
    });
    json!({
        "starts_at": format_timestamp(terms.starts_at),
        "ends_at": format_timestamp(terms.ends_at),
        "minimum_increment": money_json(
            terms.minimum_increment.amount_minor,
            &terms.minimum_increment.currency,
            terms.minimum_increment.exponent,
        ),
        "reserve_price": terms.reserve_price.as_ref().map(|reserve| money_json(
            reserve.amount_minor,
            &reserve.currency,
            reserve.exponent,
        )),
        "anti_sniping_window_seconds": terms.anti_sniping_window_seconds,
        "anti_sniping_extension_seconds": terms.anti_sniping_extension_seconds,
        "status": status,
        "current_price": existing("current_price").unwrap_or(unit_price),
        "leader_pubky": existing("leader_pubky").unwrap_or(Value::Null),
        "bid_count": existing("bid_count").unwrap_or(json!(0)),
        "reserve_met": reserve_met,
    })
}
