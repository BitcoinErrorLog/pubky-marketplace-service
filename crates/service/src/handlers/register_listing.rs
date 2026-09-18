use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    validate_public_listing_payload, AuctionReserve, Command, RegisterListingPayload, SaleFormat,
};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    current_listing_revision, fetch_auction_reserve, fetch_auction_reserve_for_update,
    fetch_listing, fetch_listing_for_update,
};
use crate::homeserver::{
    registration_payload_from_record, HomeserverFetchOutcome, HomeserverListingClient,
};
use crate::model::{money_json, AuctionReserveRow, AuctionState, ListingRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RegisterListingPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
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
    let current_reserve = fetch_auction_reserve_for_update(tx, &command.aggregate_id).await?;
    match (
        current.as_ref(),
        current_reserve.as_ref(),
        payload.auction_reserve.as_ref(),
    ) {
        (None, None, Some(reserve)) => {
            if payload.listing_revision != 1
                || reserve.expected_record_revision != 0
                || reserve.record_revision != 1
            {
                return Ok(Err(CommandFailure::with_revision(
                    ErrorCode::RevisionConflict,
                    "The reserve record revision is stale.",
                    0,
                )));
            }
        }
        (None, None, None) => {}
        (Some(listing), Some(stored), Some(reserve)) if listing.sale_format == "auction" => {
            if reserve.expected_record_revision != stored.record_revision
                || reserve.record_revision != stored.record_revision + 1
            {
                return Ok(Err(CommandFailure::with_revision(
                    ErrorCode::RevisionConflict,
                    "The reserve record revision is stale.",
                    listing.server_revision,
                )));
            }
        }
        (Some(listing), None, None) if listing.sale_format == "fixed_price" => {}
        _ => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvariantViolation,
                "The listing reserve authority is inconsistent.",
            )))
        }
    }

    if payload.sale_format == SaleFormat::Auction {
        let Some(homeserver) = homeserver else {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidCommand,
                "Listing registration is not enabled on this deployment.",
            )));
        };
        let public_record = match homeserver
            .fetch_listing(&payload.seller_pubky, &payload.listing_id)
            .await
        {
            HomeserverFetchOutcome::Found(record) => record,
            HomeserverFetchOutcome::NotFound => {
                return Ok(Err(CommandFailure::new(
                    ErrorCode::NotFound,
                    "The seller's homeserver has no such listing record.",
                )))
            }
            HomeserverFetchOutcome::Unavailable => {
                return Ok(Err(CommandFailure::new(
                    ErrorCode::UpstreamUnavailable,
                    "The seller's homeserver could not be reached. Try again shortly.",
                )))
            }
        };
        let public_candidate =
            match registration_payload_from_record(
                &payload.seller_pubky,
                &payload.listing_id,
                &public_record,
            ) {
                Ok(Some(candidate)) => match validate_public_listing_payload(candidate) {
                    Ok(candidate) => candidate,
                    Err(issues) => return Ok(Err(CommandFailure {
                        issues: Some(issues),
                        ..CommandFailure::new(
                            ErrorCode::InvalidState,
                            "The seller's listing record does not satisfy registration invariants.",
                        )
                    })),
                },
                _ => {
                    return Ok(Err(CommandFailure::new(
                        ErrorCode::InvalidState,
                        "The seller's listing record could not be interpreted for registration.",
                    )))
                }
            };
        let mut public_command = payload.clone();
        public_command.auction_reserve = None;
        if public_candidate != public_command {
            return Ok(Err(CommandFailure::new(
                ErrorCode::RevisionConflict,
                "The public listing candidate does not match the registration command.",
            )));
        }
    }

    if let Some(listing) = current.as_ref() {
        if let Some(failure) = validate_edit(listing, current_reserve.as_ref(), payload, now)? {
            return Ok(Err(failure));
        }
    }

    apply_registration(
        tx,
        actor,
        command.command_id,
        &command.aggregate_id,
        payload,
        current.as_ref(),
        current_reserve.as_ref(),
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
    current_reserve: Option<&AuctionReserveRow>,
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
    let fulfillment_methods: Vec<String> = payload
        .fulfillment_methods
        .iter()
        .map(|method| method.as_str().to_string())
        .collect();

    let written = if current.is_none() {
        let inserted = sqlx::query(
            "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
             listing_revision, content_hash, server_revision, state, total_quantity, \
             available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
             unit_price_currency, unit_price_exponent, shipping_minor, sale_format, auction, \
             fulfillment_methods, digital_lock_policy_uri, digital_lock_criterion_id, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22) \
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
        .bind(payload.shipping_minor)
        .bind(sale_format)
        .bind(&auction)
        .bind(&fulfillment_methods)
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.policy_uri))
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.criterion_id))
        .bind(now)
        .execute(&mut **tx)
        .await?;
        inserted.rows_affected() == 1
    } else {
        let updated = sqlx::query(
            "UPDATE listings SET title = $3, listing_revision = $4, content_hash = $5, \
             server_revision = $6, state = $7, total_quantity = $8, available_quantity = $9, \
             unit_price_amount_minor = $10, unit_price_currency = $11, unit_price_exponent = $12, \
             shipping_minor = $13, sale_format = $14, auction = $15, fulfillment_methods = $16, \
             digital_lock_policy_uri = $17, digital_lock_criterion_id = $18, updated_at = $19 \
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
        .bind(payload.shipping_minor)
        .bind(sale_format)
        .bind(&auction)
        .bind(&fulfillment_methods)
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.policy_uri))
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.criterion_id))
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

    if let Some(reserve) = payload.auction_reserve.as_ref() {
        persist_reserve(
            tx,
            aggregate_id,
            payload.listing_revision,
            command_id,
            reserve,
            current_reserve,
            now,
        )
        .await?;
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
    let reserve = fetch_auction_reserve(tx, aggregate_id).await?;
    let projection = listing
        .projection_for_actor_with_auction(actor, reserve.as_ref(), None)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "listing", "listing": projection }),
    }))
}

async fn persist_reserve(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    listing_revision: i64,
    command_id: Uuid,
    reserve: &AuctionReserve,
    current: Option<&AuctionReserveRow>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let reserve_price = reserve.reserve_price.as_ref();
    if current.is_none() {
        sqlx::query(
            "INSERT INTO listing_auction_reserves \
             (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor, \
              reserve_currency, reserve_exponent, last_command_id, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(aggregate_id)
        .bind(listing_revision)
        .bind(reserve.record_revision)
        .bind(reserve_price.map(|money| money.amount_minor))
        .bind(reserve_price.map(|money| &money.currency))
        .bind(reserve_price.map(|money| money.exponent))
        .bind(command_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    } else {
        let updated = sqlx::query(
            "UPDATE listing_auction_reserves SET listing_revision = $2, record_revision = $3, \
             reserve_amount_minor = $4, reserve_currency = $5, reserve_exponent = $6, \
             last_command_id = $7, updated_at = $8 \
             WHERE listing_aggregate_id = $1 AND record_revision = $9",
        )
        .bind(aggregate_id)
        .bind(listing_revision)
        .bind(reserve.record_revision)
        .bind(reserve_price.map(|money| money.amount_minor))
        .bind(reserve_price.map(|money| &money.currency))
        .bind(reserve_price.map(|money| money.exponent))
        .bind(command_id)
        .bind(now)
        .bind(reserve.expected_record_revision)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "reserve compare-and-swap failed".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_edit(
    current: &ListingRow,
    current_reserve: Option<&AuctionReserveRow>,
    payload: &RegisterListingPayload,
    now: DateTime<Utc>,
) -> Result<Option<CommandFailure>, sqlx::Error> {
    if current.sale_format
        != match payload.sale_format {
            SaleFormat::FixedPrice => "fixed_price",
            SaleFormat::Auction => "auction",
        }
    {
        return Ok(Some(CommandFailure::new(
            ErrorCode::InvalidState,
            "The listing sale format cannot change after registration.",
        )));
    }
    if current.sale_format != "auction" {
        return Ok(None);
    }
    let Some(stored_reserve) = current_reserve else {
        return Ok(Some(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "The auction reserve authority is missing.",
        )));
    };
    let Some(candidate_reserve) = payload.auction_reserve.as_ref() else {
        return Ok(Some(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "The auction reserve authority is missing.",
        )));
    };
    let stored_auction = current
        .auction
        .as_ref()
        .ok_or_else(|| sqlx::Error::Protocol("auction state is missing".to_string()))
        .and_then(|value| {
            AuctionState::from_value(value)
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        })?;
    let candidate_terms = payload
        .auction_terms
        .as_ref()
        .expect("validated auction payload has terms");
    let terms_changed = current.unit_price_amount_minor != payload.unit_price.amount_minor
        || current.unit_price_currency != payload.unit_price.currency
        || current.unit_price_exponent != payload.unit_price.exponent
        || stored_auction.starts_at != candidate_terms.starts_at
        || stored_auction.ends_at != candidate_terms.ends_at
        || stored_auction.minimum_increment != candidate_terms.minimum_increment
        || stored_auction.anti_sniping_window_seconds
            != candidate_terms.anti_sniping_window_seconds
        || stored_auction.anti_sniping_extension_seconds
            != candidate_terms.anti_sniping_extension_seconds;
    if terms_changed {
        return Ok(Some(CommandFailure::new(
            ErrorCode::InvalidState,
            "Auction terms cannot change after registration.",
        )));
    }
    let stored_price = stored_reserve.reserve_price();
    let candidate_price = candidate_reserve.reserve_price.as_ref();
    if stored_price.as_ref() == candidate_price {
        return Ok(None);
    }
    let allowed_decrease = match (stored_price.as_ref(), candidate_price) {
        (Some(stored), Some(candidate)) => {
            candidate.same_asset(stored)
                && candidate.amount_minor < stored.amount_minor
                && stored_auction.status == "scheduled"
                && now < stored_auction.starts_at
                && stored_auction.bid_count == 0
        }
        _ => false,
    };
    if !allowed_decrease {
        return Ok(Some(CommandFailure::new(
            ErrorCode::InvalidState,
            "The auction reserve change is not permitted.",
        )));
    }
    Ok(None)
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
    json!({
        "starts_at": format_timestamp(terms.starts_at),
        "ends_at": format_timestamp(terms.ends_at),
        "minimum_increment": money_json(
            terms.minimum_increment.amount_minor,
            &terms.minimum_increment.currency,
            terms.minimum_increment.exponent,
        ),
        "anti_sniping_window_seconds": terms.anti_sniping_window_seconds,
        "anti_sniping_extension_seconds": terms.anti_sniping_extension_seconds,
        "status": status,
        "current_price": existing("current_price").unwrap_or(unit_price),
        "leader_pubky": existing("leader_pubky").unwrap_or(Value::Null),
        "bid_count": existing("bid_count").unwrap_or(json!(0)),
    })
}
