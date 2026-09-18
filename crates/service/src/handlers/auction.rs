//! Auction commands (`auction.place_bid`, `auction.close`), ported from the
//! TypeScript prototype engine: proxy/maximum bidding with a deterministic
//! first-accepted-sequence tie breaker, minimum-increment visible pricing,
//! anti-sniping extension, reserve price, and authoritative close.
//!
//! Close is shared between the seller command and the server-time worker
//! (`workers::close_due_auctions`). A sold close creates the winning
//! reservation plus exactly one winning order and sandbox payment; the
//! partial unique index `orders_one_winner_per_auction` makes a second
//! winning order impossible even across racing closers.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{CloseAuctionPayload, PlaceBidPayload};
use marketplace_domain::state_machines::{auction_machine, can_transition};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::handlers::{
    fetch_auction_reserve_for_update, fetch_listing_for_update, insert_notification_intent,
    LISTING_COLUMNS,
};
use crate::model::{
    money_json, AuctionReserveRow, AuctionState, BidRow, ListingRow, OrderRow, PaymentRow,
    ProjectionContext, ReservationRow,
};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// The auction winner's inventory hold, as in the prototype engine.
const AUCTION_HOLD_SECONDS: i64 = 30 * 60;

/// Returns the lowest proxy maximum accepted for the next bid by a bidder.
///
/// The visible price advances by the auction increment, while a bidder
/// raising their own proxy ceiling only needs to exceed their previous
/// maximum by one minor unit. This is shared with the authenticated listing
/// projection so the UI cannot show a stale threshold.
pub fn personal_minimum_next_bid(
    current_price_minor: i64,
    minimum_increment_minor: i64,
    bidder_previous_maximum: i64,
) -> i64 {
    let visible_price_minimum = current_price_minor.saturating_add(minimum_increment_minor);
    if bidder_previous_maximum == 0 {
        visible_price_minimum
    } else {
        visible_price_minimum.max(bidder_previous_maximum.saturating_add(1))
    }
}

pub async fn place_bid(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &PlaceBidPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(listing) = fetch_listing_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::BidListingNotFound,
            ErrorCode::NotFound,
            "The auction listing is not registered.",
        )));
    };
    if listing.seller_pubky == actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::BidSellerForbidden,
            ErrorCode::Unauthorized,
            "A seller cannot bid on their own auction.",
        )));
    }
    let auction = match parse_auction(&listing) {
        Some(auction) => auction,
        None => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::BidNotAuction,
                ErrorCode::InvalidState,
                "This listing is not an auction.",
            )));
        }
    };
    let Some(reserve) = fetch_auction_reserve_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "The auction reserve authority is missing.",
        )));
    };
    if auction.status != "active" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AuctionClosed,
            ErrorCode::AuctionClosed,
            "The auction is not open for bidding.",
        )));
    }
    if command.expected_revision != listing.server_revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The auction revision is stale.",
            listing.server_revision,
        )));
    }
    if now < auction.starts_at || now >= auction.ends_at {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AuctionClosed,
            ErrorCode::AuctionClosed,
            "The auction is not open for bidding.",
        )));
    }
    if payload.maximum_amount.currency != listing.unit_price_currency
        || payload.maximum_amount.exponent != listing.unit_price_exponent
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::BidWrongAsset,
            ErrorCode::InvalidCommand,
            "Bid maximum must use the auction asset and exponent.",
        )));
    }
    let previous_bids = fetch_bids(tx, &listing.aggregate_id).await?;
    let bidder_previous_maximum = previous_bids
        .iter()
        .filter(|bid| bid.bidder_pubky == actor)
        .map(|bid| bid.maximum_amount_minor)
        .max()
        .unwrap_or(0);
    let minimum_next_bid = personal_minimum_next_bid(
        auction.current_price.amount_minor,
        auction.minimum_increment.amount_minor,
        bidder_previous_maximum,
    );
    if payload.maximum_amount.amount_minor < minimum_next_bid {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::BidTooLow,
            ErrorCode::BidTooLow,
            "Bid maximum must meet the current visible price increment and exceed the prior maximum.",
            listing.server_revision,
        )));
    }

    let bid = BidRow {
        id: command.command_id,
        listing_aggregate_id: listing.aggregate_id.clone(),
        bidder_pubky: actor.to_string(),
        maximum_amount_minor: payload.maximum_amount.amount_minor,
        currency: payload.maximum_amount.currency.clone(),
        exponent: payload.maximum_amount.exponent,
        sequence: auction.bid_count + 1,
        created_at: now,
    };
    sqlx::query(
        "INSERT INTO bids (id, listing_aggregate_id, bidder_pubky, maximum_amount_minor, \
         currency, exponent, sequence, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(bid.id)
    .bind(&bid.listing_aggregate_id)
    .bind(&bid.bidder_pubky)
    .bind(bid.maximum_amount_minor)
    .bind(&bid.currency)
    .bind(bid.exponent)
    .bind(bid.sequence)
    .bind(bid.created_at)
    .execute(&mut **tx)
    .await?;

    let mut all_bids = previous_bids;
    all_bids.push(bid.clone());
    let ranked = ranked_bidder_maximums(&all_bids);
    let leader = ranked.first().expect("at least the new bid exists");
    let runner_up = ranked.get(1);
    let visible_amount = match runner_up {
        Some(runner_up) => leader
            .maximum_amount_minor
            .min(runner_up.maximum_amount_minor + auction.minimum_increment.amount_minor),
        None => listing.unit_price_amount_minor,
    };
    // The post-bid visible price is recorded on the bid row itself so bid
    // history can show the price progression without ever exposing any
    // bidder's secret proxy maximum.
    sqlx::query("UPDATE bids SET visible_amount_minor = $2 WHERE id = $1")
        .bind(bid.id)
        .bind(visible_amount)
        .execute(&mut **tx)
        .await?;
    let should_extend = auction.anti_sniping_window_seconds > 0
        && (auction.ends_at - now)
            <= chrono::Duration::seconds(auction.anti_sniping_window_seconds);
    let previous_leader = auction.leader_pubky.clone();

    let mut updated_auction = auction;
    if should_extend {
        updated_auction.ends_at =
            now + chrono::Duration::seconds(updated_auction.anti_sniping_extension_seconds);
    }
    updated_auction.current_price = updated_auction.current_price.with_amount(visible_amount);
    updated_auction.leader_pubky = Some(leader.bidder_pubky.clone());
    updated_auction.bid_count += 1;

    let updated_listing: ListingRow = sqlx::query_as(&format!(
        "UPDATE listings SET server_revision = server_revision + 1, auction = $2, \
         updated_at = $3 WHERE aggregate_id = $1 RETURNING {LISTING_COLUMNS}"
    ))
    .bind(&listing.aggregate_id)
    .bind(updated_auction.to_value())
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &listing.aggregate_id,
        updated_listing.server_revision,
        actor,
        "auction.bid_placed",
        now,
    )
    .await?;
    if let Some(previous_leader) = previous_leader {
        if previous_leader != leader.bidder_pubky && previous_leader != actor {
            // The amount is the new visible price — the figure the displaced
            // leader must beat, already on the auction projection they read.
            insert_notification_intent(
                tx,
                event_id,
                "outbid",
                &previous_leader,
                actor,
                &listing.aggregate_id,
                Some(&money_json(
                    visible_amount,
                    &listing.unit_price_currency,
                    listing.unit_price_exponent,
                )),
                now,
            )
            .await?;
        }
    }

    let viewer_bid = viewer_bid_json(
        &updated_auction,
        bid.maximum_amount_minor,
        &bid.currency,
        bid.exponent,
    );
    let listing_projection = updated_listing
        .projection_for_actor_with_auction(actor, Some(&reserve), Some(viewer_bid))
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    Ok(Ok(HandlerSuccess {
        revision: updated_listing.server_revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "bid",
            "listing": listing_projection,
            "bid": bid.view(),
        }),
    }))
}

pub async fn close(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    _payload: &CloseAuctionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(listing) = fetch_listing_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The auction listing is not registered.",
        )));
    };
    if listing.seller_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the seller may close this sandbox auction.",
        )));
    }
    let auction = parse_auction(&listing);
    let Some(auction) = auction.filter(|auction| auction.status == "active") else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The auction is not active.",
        )));
    };
    if command.expected_revision != listing.server_revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The auction revision is stale.",
            listing.server_revision,
        )));
    }
    if now < auction.ends_at {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AuctionClosed,
            ErrorCode::AuctionClosed,
            "The auction has not ended yet.",
        )));
    }

    let outcome =
        close_locked_auction(tx, &listing, auction, actor, command.command_id, now).await?;
    Ok(Ok(HandlerSuccess {
        revision: outcome.listing.server_revision,
        event_ids: outcome.event_ids.clone(),
        result: outcome.result_json(actor)?,
    }))
}

pub struct AuctionCloseOutcome {
    pub sold: bool,
    pub winner_pubky: Option<String>,
    pub listing: ListingRow,
    pub reserve: AuctionReserveRow,
    pub reservation: Option<ReservationRow>,
    pub order: Option<OrderRow>,
    pub payment: Option<PaymentRow>,
    pub event_ids: Vec<Uuid>,
}

impl AuctionCloseOutcome {
    pub fn result_json(&self, actor: &str) -> Result<Value, sqlx::Error> {
        // The order and payment use the redacted projections: no delivery
        // address (always null here — the winner has not supplied one) and
        // no Locks bundle id, which stays out of command results exactly as
        // out of read projections (ADR-0019 §8).
        let listing = self
            .listing
            .projection_for_actor_with_auction(actor, Some(&self.reserve), None)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        Ok(json!({
            "kind": "auction_result",
            "outcome": if self.sold { "sold" } else { "unsold" },
            "winner_pubky": self.winner_pubky,
            "listing": listing,
            "reservation": self.reservation.as_ref().map(ReservationRow::view),
            "order": self.order.as_ref().map(|order| {
                order.project(ProjectionContext::CommandResult {
                    authenticated_actor: "",
                })
            }),
            "payment": self.payment.as_ref().map(PaymentRow::projection),
        }))
    }
}

/// Closes an active auction whose listing row is already locked. Callers
/// (the seller command and the server-time worker) must have verified the
/// auction is active and its end time has passed. Sold auctions get exactly
/// one winning reservation, order, and sandbox payment.
pub async fn close_locked_auction(
    tx: &mut Transaction<'_, Postgres>,
    listing: &ListingRow,
    auction: AuctionState,
    actor: &str,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<AuctionCloseOutcome, sqlx::Error> {
    let reserve = fetch_auction_reserve_for_update(tx, &listing.aggregate_id)
        .await?
        .ok_or_else(|| sqlx::Error::Protocol("auction reserve authority is missing".to_string()))?;
    let reserve_satisfied = reserve_satisfied(listing, &auction, &reserve)?;
    let winner = auction.leader_pubky.clone().filter(|_| reserve_satisfied);
    let sold = winner.is_some();
    let auction_status = if sold { "sold" } else { "unsold" };
    debug_assert!(can_transition(&auction_machine(), "active", auction_status));

    let mut closed_auction = auction;
    closed_auction.status = auction_status.to_string();
    let (new_state, quantity_delta) = if sold {
        ("reserved", 1i64)
    } else {
        ("available", 0i64)
    };
    let updated_listing: ListingRow = sqlx::query_as(&format!(
        "UPDATE listings SET server_revision = server_revision + 1, state = $2, \
         available_quantity = available_quantity - $3, \
         reserved_quantity = reserved_quantity + $3, auction = $4, updated_at = $5 \
         WHERE aggregate_id = $1 AND auction->>'status' = 'active' \
         RETURNING {LISTING_COLUMNS}"
    ))
    .bind(&listing.aggregate_id)
    .bind(new_state)
    .bind(quantity_delta)
    .bind(closed_auction.to_value())
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let close_event_id = insert_event(
        tx,
        command_id,
        &listing.aggregate_id,
        updated_listing.server_revision,
        actor,
        if sold {
            "auction.closed_sold"
        } else {
            "auction.closed_unsold"
        },
        now,
    )
    .await?;
    let mut event_ids = vec![close_event_id];

    // The closing visible price, carried by the close notifications
    // (auction_won to the winner, auction_ended to every other bidder).
    let final_price_json = money_json(
        closed_auction.current_price.amount_minor,
        &closed_auction.current_price.currency,
        closed_auction.current_price.exponent,
    );

    let (reservation, order, payment) = if let Some(winner_pubky) = &winner {
        let expires_at = now + chrono::Duration::seconds(AUCTION_HOLD_SECONDS);
        sqlx::query(
            "INSERT INTO reservations (id, listing_aggregate_id, buyer_pubky, quantity, status, \
             expires_at, created_at, updated_at) VALUES ($1, $2, $3, 1, 'active', $4, $5, $5)",
        )
        .bind(command_id)
        .bind(&listing.aggregate_id)
        .bind(winner_pubky)
        .bind(expires_at)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        let reservation = ReservationRow {
            id: command_id,
            listing_aggregate_id: listing.aggregate_id.clone(),
            buyer_pubky: winner_pubky.clone(),
            quantity: 1,
            status: "active".to_string(),
            expires_at,
            created_at: now,
        };

        // The winning order snapshots the final auction price and applies
        // the same pricing policy as checkout: the seller-signed flat
        // shipping, nothing else added.
        let final_price = closed_auction.current_price.clone();
        let subtotal_minor = final_price.amount_minor;
        let shipping_minor = listing.shipping_minor;
        let total_minor = subtotal_minor + shipping_minor;
        let order_id = Uuid::new_v4();
        let payment_id = Uuid::new_v4();
        let lines = json!([{
            "listing_aggregate_id": listing.aggregate_id,
            "listing_revision": listing.listing_revision,
            "content_hash": listing.content_hash,
            "title": listing.title,
            "quantity": 1,
            "unit_price": {
                "amount_minor": final_price.amount_minor,
                "currency": final_price.currency,
                "exponent": final_price.exponent,
            },
            "subtotal": {
                "amount_minor": final_price.amount_minor,
                "currency": final_price.currency,
                "exponent": final_price.exponent,
            },
        }]);
        let order = OrderRow::new_for_insert(
            order_id,
            Some(listing.aggregate_id.clone()),
            None,
            winner_pubky.clone(),
            listing.seller_pubky.clone(),
            crate::queries::seller_has_rail(tx, &listing.seller_pubky).await?,
            lines,
            None,
            subtotal_minor,
            shipping_minor,
            total_minor,
            final_price.currency.clone(),
            final_price.exponent,
            1,
            payment_id,
            false,
            None,
            "shipping".to_string(),
            now,
            "listing".to_string(),
        );
        sqlx::query(
            "INSERT INTO orders (id, auction_aggregate_id, buyer_pubky, seller_pubky, revision, \
             state, lines, delivery_address, subtotal_minor, shipping_minor, \
             total_minor, currency, exponent, guarantee_policy_version, payment_id, \
             fulfillment, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10, $11, $12, $13, $14, 'shipping', \
             $15, $15)",
        )
        .bind(order.id)
        .bind(&listing.aggregate_id)
        .bind(&order.buyer_pubky)
        .bind(&order.seller_pubky)
        .bind(order.revision)
        .bind(&order.state)
        .bind(&order.lines)
        .bind(order.subtotal_minor)
        .bind(order.shipping_minor)
        .bind(order.total_minor)
        .bind(&order.currency)
        .bind(order.exponent)
        .bind(order.guarantee_policy_version)
        .bind(order.payment_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;

        let payment = PaymentRow {
            id: payment_id,
            order_id,
            buyer_pubky: winner_pubky.clone(),
            seller_pubky: listing.seller_pubky.clone(),
            revision: 1,
            adapter: "sandbox".to_string(),
            state: "awaiting_entitlement".to_string(),
            confirmations: 0,
            amount_minor: total_minor,
            currency: final_price.currency.clone(),
            exponent: final_price.exponent,
            merchandise_amount_minor: Some(total_minor),
            merchandise_currency: Some(final_price.currency.clone()),
            merchandise_exponent: Some(final_price.exponent),
            manual_review_entered_at: None,
            manual_review_sla_alerted_at: None,
            resolution_id: None,
            resolution_outcome: None,
            resolution_basis: None,
            resolved_at: None,
            resolved_by_pubky: None,
            refund_reference: None,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
             state, confirmations, amount_minor, currency, exponent, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $12)",
        )
        .bind(payment.id)
        .bind(payment.order_id)
        .bind(&payment.buyer_pubky)
        .bind(&payment.seller_pubky)
        .bind(payment.revision)
        .bind(&payment.adapter)
        .bind(&payment.state)
        .bind(payment.confirmations)
        .bind(payment.amount_minor)
        .bind(&payment.currency)
        .bind(payment.exponent)
        .bind(now)
        .execute(&mut **tx)
        .await?;

        let order_event_id = insert_event(
            tx,
            command_id,
            &ids::order_aggregate_id(order_id),
            1,
            actor,
            "order.created",
            now,
        )
        .await?;
        event_ids.push(order_event_id);
        insert_notification_intent(
            tx,
            close_event_id,
            "auction_won",
            winner_pubky,
            actor,
            &listing.aggregate_id,
            Some(&final_price_json),
            now,
        )
        .await?;

        (Some(reservation), Some(order), Some(payment))
    } else {
        (None, None, None)
    };

    // Every distinct bidder except the winner learns the auction closed
    // (`auction_ended`), sold or unsold — previously only the winner
    // (`auction_won`) and the displaced leader at bid time (`outbid`) heard
    // anything. The payload carries the aggregate ref and the closing
    // visible price, both already on the listing projection every bidder
    // reads (ADR-0019 §8). All intents share the one close event, so the
    // (event id, recipient) dedup makes redelivery harmless.
    let bidders: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT bidder_pubky FROM bids WHERE listing_aggregate_id = $1 \
         ORDER BY bidder_pubky",
    )
    .bind(&listing.aggregate_id)
    .fetch_all(&mut **tx)
    .await?;
    for (bidder_pubky,) in &bidders {
        if winner.as_deref() == Some(bidder_pubky.as_str()) {
            continue;
        }
        insert_notification_intent(
            tx,
            close_event_id,
            "auction_ended",
            bidder_pubky,
            actor,
            &listing.aggregate_id,
            Some(&final_price_json),
            now,
        )
        .await?;
    }

    Ok(AuctionCloseOutcome {
        sold,
        winner_pubky: winner,
        listing: updated_listing,
        reserve,
        reservation,
        order,
        payment,
        event_ids,
    })
}

fn reserve_satisfied(
    listing: &ListingRow,
    auction: &AuctionState,
    reserve: &AuctionReserveRow,
) -> Result<bool, sqlx::Error> {
    if reserve.listing_aggregate_id != listing.aggregate_id
        || reserve.listing_revision != listing.listing_revision
    {
        return Err(sqlx::Error::Protocol(
            "auction reserve revision is inconsistent".to_string(),
        ));
    }
    if auction.current_price.currency != listing.unit_price_currency
        || auction.current_price.exponent != listing.unit_price_exponent
        || auction.minimum_increment.currency != listing.unit_price_currency
        || auction.minimum_increment.exponent != listing.unit_price_exponent
    {
        return Err(sqlx::Error::Protocol(
            "auction current-price asset is inconsistent".to_string(),
        ));
    }
    let Some(reserve_price) = reserve.reserve_price() else {
        return Ok(true);
    };
    if reserve_price.currency != listing.unit_price_currency
        || reserve_price.exponent != listing.unit_price_exponent
    {
        return Err(sqlx::Error::Protocol(
            "auction reserve asset is inconsistent".to_string(),
        ));
    }
    Ok(auction.current_price.amount_minor >= reserve_price.amount_minor)
}

fn viewer_bid_json(
    auction: &AuctionState,
    maximum_minor: i64,
    currency: &str,
    exponent: i32,
) -> Value {
    let minimum_minor = personal_minimum_next_bid(
        auction.current_price.amount_minor,
        auction.minimum_increment.amount_minor,
        maximum_minor,
    );
    json!({
        "maximum_amount": money_json(maximum_minor, currency, exponent),
        "minimum_next_bid": money_json(minimum_minor, currency, exponent),
    })
}

pub async fn viewer_bid_projection(
    tx: &mut Transaction<'_, Postgres>,
    listing: &ListingRow,
    actor: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let own_bid: Option<(i64, String, i32)> = sqlx::query_as(
        "SELECT maximum_amount_minor, currency, exponent FROM bids \
         WHERE listing_aggregate_id = $1 AND bidder_pubky = $2 \
         ORDER BY maximum_amount_minor DESC LIMIT 1",
    )
    .bind(&listing.aggregate_id)
    .bind(actor)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((maximum_minor, currency, exponent)) = own_bid else {
        return Ok(None);
    };
    let auction = parse_auction(listing)
        .ok_or_else(|| sqlx::Error::Protocol("auction state is missing".to_string()))?;
    Ok(Some(viewer_bid_json(
        &auction,
        maximum_minor,
        &currency,
        exponent,
    )))
}

pub fn parse_auction(listing: &ListingRow) -> Option<AuctionState> {
    if listing.sale_format != "auction" {
        return None;
    }
    let value = listing.auction.as_ref()?;
    AuctionState::from_value(value).ok()
}

async fn fetch_bids(
    tx: &mut Transaction<'_, Postgres>,
    listing_aggregate_id: &str,
) -> Result<Vec<BidRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, listing_aggregate_id, bidder_pubky, maximum_amount_minor, currency, \
         exponent, sequence, created_at FROM bids WHERE listing_aggregate_id = $1 \
         ORDER BY sequence",
    )
    .bind(listing_aggregate_id)
    .fetch_all(&mut **tx)
    .await
}

/// One representative bid per bidder — their highest maximum, breaking ties
/// by the earliest accepted sequence — ranked by maximum descending, then
/// sequence ascending (the prototype's deterministic proxy ranking).
fn ranked_bidder_maximums(bids: &[BidRow]) -> Vec<&BidRow> {
    let mut latest: Vec<&BidRow> = Vec::new();
    for bid in bids {
        match latest
            .iter_mut()
            .find(|current| current.bidder_pubky == bid.bidder_pubky)
        {
            Some(current) => {
                if bid.maximum_amount_minor > current.maximum_amount_minor
                    || (bid.maximum_amount_minor == current.maximum_amount_minor
                        && bid.sequence < current.sequence)
                {
                    *current = bid;
                }
            }
            None => latest.push(bid),
        }
    }
    latest.sort_by(|left, right| {
        right
            .maximum_amount_minor
            .cmp(&left.maximum_amount_minor)
            .then(left.sequence.cmp(&right.sequence))
    });
    latest
}

#[cfg(test)]
mod tests {
    use super::ranked_bidder_maximums;
    use crate::model::BidRow;
    use chrono::Utc;
    use uuid::Uuid;

    fn bid(bidder: &str, maximum: i64, sequence: i64) -> BidRow {
        BidRow {
            id: Uuid::new_v4(),
            listing_aggregate_id: "listing:test".to_string(),
            bidder_pubky: bidder.to_string(),
            maximum_amount_minor: maximum,
            currency: "USD".to_string(),
            exponent: 2,
            sequence,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn ranks_by_maximum_then_first_accepted_sequence() {
        let bids = vec![bid("alice", 10_000, 1), bid("bob", 10_000, 2)];
        let ranked = ranked_bidder_maximums(&bids);
        assert_eq!(ranked[0].bidder_pubky, "alice");
        assert_eq!(ranked[1].bidder_pubky, "bob");
    }

    #[test]
    fn keeps_each_bidders_highest_maximum() {
        let bids = vec![
            bid("alice", 5_000, 1),
            bid("bob", 8_000, 2),
            bid("alice", 12_000, 3),
        ];
        let ranked = ranked_bidder_maximums(&bids);
        assert_eq!(ranked[0].bidder_pubky, "alice");
        assert_eq!(ranked[0].maximum_amount_minor, 12_000);
        assert_eq!(ranked[1].bidder_pubky, "bob");
    }
}
