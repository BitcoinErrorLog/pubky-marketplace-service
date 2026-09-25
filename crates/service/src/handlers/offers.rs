//! Offer lifecycle commands (`offer.create`, `offer.counter`, `offer.accept`,
//! `offer.reject`, `offer.withdraw`), ported from the TypeScript prototype
//! engine with identical semantics: offer expiry on server time, listing
//! asset/quantity validation, inventory reservation on acceptance, revision
//! compare-and-swap, and append-only offer history.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{CounterOfferPayload, OfferActionPayload, OfferTermsPayload};
use marketplace_domain::state_machines::{can_transition, listing_machine, offer_machine};
use marketplace_domain::{ids, Command, ErrorCode, Money};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    fetch_auction_reserve, fetch_listing, fetch_listing_for_update, insert_notification_intent,
    LISTING_COLUMNS,
};
use crate::homeserver::{
    award_terms_from_bytes, award_terms_from_bytes_for_variant, AwardListingSnapshot,
    AwardVariantSnapshot, HomeserverListingClient, HomeserverRawFetchOutcome,
};
use crate::model::{ListingRow, OfferRow, ReservationRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub const OFFER_COLUMNS: &str = "id, aggregate_id, listing_aggregate_id, buyer_pubky, \
     seller_pubky, revision, state, offered_by, amount_minor, currency, exponent, quantity, \
     message, history, expires_at, created_at, updated_at, variant_id, \
     terms_listing_revision, terms_listing_record_sha256, terms_snapshot, award_id, \
     accepted_at, award_expires_at, reservation_id, accepted_unit_price_minor, \
     accepted_currency, accepted_exponent, accepted_quantity, accepted_listing_aggregate_id, \
     accepted_listing_title, accepted_listing_revision, accepted_listing_record_sha256, \
     accepted_variant_id, accepted_variant_sku, accepted_variant_options, accepted_shipping_minor, \
     accepted_subtotal_minor, accepted_total_minor, accepted_fulfillment, converted_order_id, \
     converted_at, expiry_reason";

/// Acceptance holds the inventory for 30 minutes, as in the prototype engine.
const ACCEPTED_OFFER_HOLD_SECONDS: i64 = 30 * 60;

fn history_entry(
    revision: i64,
    actor: &str,
    action: &str,
    amount: &Value,
    quantity: i64,
    message: &str,
    occurred_at: DateTime<Utc>,
) -> Value {
    json!({
        "revision": revision,
        "actor_pubky": actor,
        "action": action,
        "amount": amount,
        "quantity": quantity,
        "message": message,
        "occurred_at": format_timestamp(occurred_at),
    })
}

pub async fn create(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OfferTermsPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(listing) = fetch_listing(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The listing is not registered.",
        )));
    };
    if listing.seller_pubky == actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "A seller cannot make an offer on their own listing.",
        )));
    }
    if command.expected_revision != listing.server_revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            listing.server_revision,
        )));
    }
    if listing.available_quantity < payload.quantity {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::InsufficientInventory,
            ErrorCode::InsufficientInventory,
            "The requested offer quantity is unavailable.",
            listing.server_revision,
        )));
    }
    if !same_asset(&listing, &payload.amount) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "Offer amount must use the listing asset and exponent.",
        )));
    }
    // Offers settle through the listing's physical fulfillment: shipping or
    // local pickup, chosen by the buyer at offer checkout (§A2). Digital
    // items, the only other published method, take no offers.
    if !listing
        .fulfillment_methods
        .iter()
        .any(|m| m == "shipping" || m == "pickup")
    {
        return Ok(Err(CommandFailure::refused_with_reason(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Offers are not available on digital items.",
            "offers_unavailable_for_digital",
        )));
    }

    let terms = if let Some(homeserver) = homeserver {
        match homeserver
            .fetch_listing_raw(&listing.seller_pubky, &listing.listing_id)
            .await
        {
            HomeserverRawFetchOutcome::Found(raw) => match award_terms_from_bytes(&raw) {
                Ok(snapshot) => Some(snapshot),
                Err(_) => {
                    return Ok(Err(CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::AwardListingChanged,
                        ErrorCode::AwardListingChanged,
                        "The listing snapshot does not match the offer terms.",
                    )));
                }
            },
            HomeserverRawFetchOutcome::TooLarge => {
                // Design §7 designates an oversized seller record as a
                // changed canonical body, never malformed command input.
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::AwardListingChanged,
                    ErrorCode::AwardListingChanged,
                    "The listing snapshot does not match the offer terms.",
                )));
            }
            HomeserverRawFetchOutcome::NotFound | HomeserverRawFetchOutcome::Unavailable => None,
        }
    } else {
        None
    };
    let selected_variant = terms.as_ref().and_then(|snapshot| {
        (snapshot.variants.len() == 1).then(|| snapshot.variants[0].id.clone())
    });
    let terms_snapshot = terms.as_ref().map(|snapshot| {
        json!({
            "revision": snapshot.revision,
            "title": snapshot.title,
            "currency": snapshot.currency,
            "exponent": snapshot.exponent,
            "shipping_minor": snapshot.shipping_minor,
            "variant": selected_variant.clone(),
        })
    });

    let aggregate_id = ids::offer_aggregate_id(command.command_id);
    let expires_at = now + chrono::Duration::seconds(payload.expires_in_seconds);
    let amount_json = json!({
        "amount_minor": payload.amount.amount_minor,
        "currency": payload.amount.currency,
        "exponent": payload.amount.exponent,
    });
    let history = json!([history_entry(
        1,
        actor,
        "created",
        &amount_json,
        payload.quantity,
        &payload.message,
        now,
    )]);
    let offer: OfferRow = sqlx::query_as(&format!(
        "INSERT INTO offers (id, aggregate_id, listing_aggregate_id, buyer_pubky, seller_pubky, \
         revision, state, offered_by, amount_minor, currency, exponent, quantity, message, \
         history, expires_at, created_at, updated_at, variant_id, terms_listing_revision, \
         terms_listing_record_sha256, terms_snapshot) \
         VALUES ($1, $2, $3, $4, $5, 1, 'pending', $4, $6, $7, $8, $9, $10, $11, $12, $13, $13, \
                 $14, $15, $16, $17) \
         RETURNING {OFFER_COLUMNS}"
    ))
    .bind(command.command_id)
    .bind(&aggregate_id)
    .bind(&listing.aggregate_id)
    .bind(actor)
    .bind(&listing.seller_pubky)
    .bind(payload.amount.amount_minor)
    .bind(&payload.amount.currency)
    .bind(payload.amount.exponent)
    .bind(payload.quantity)
    .bind(&payload.message)
    .bind(&history)
    .bind(expires_at)
    .bind(now)
    .bind(&selected_variant)
    .bind(terms.as_ref().map(|snapshot| snapshot.revision))
    .bind(terms.as_ref().map(|snapshot| snapshot.raw_sha256.clone()))
    .bind(&terms_snapshot)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &aggregate_id,
        1,
        actor,
        "offer.created",
        now,
    )
    .await?;
    // Offer notifications carry the offer amount: both participants read the
    // full offer projection, so the figure is already theirs (ADR-0019 §8).
    insert_notification_intent(
        tx,
        event_id,
        "offer_received",
        &offer.seller_pubky,
        actor,
        &aggregate_id,
        Some(&amount_json),
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: 1,
        event_ids: vec![event_id],
        result: json!({ "kind": "offer", "offer": offer.view(now) }),
    }))
}

pub async fn counter(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &CounterOfferPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let locator: Option<OfferRow> = sqlx::query_as(&format!(
        "SELECT {OFFER_COLUMNS} FROM offers \
         WHERE id = $1 AND (buyer_pubky = $2 OR seller_pubky = $2)"
    ))
    .bind(payload.offer_id)
    .bind(actor)
    .fetch_optional(&mut **tx)
    .await?;
    let refreshed_terms = if let (Some(homeserver), Some(locator)) = (homeserver, locator.as_ref())
    {
        let listing_id: Option<String> =
            sqlx::query_scalar("SELECT listing_id FROM listings WHERE aggregate_id = $1")
                .bind(&locator.listing_aggregate_id)
                .fetch_optional(&mut **tx)
                .await?;
        let Some(listing_id) = listing_id else {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::NotFound,
                ErrorCode::NotFound,
                "The offer listing is unavailable.",
            )));
        };
        match homeserver
            .fetch_listing_raw(&locator.seller_pubky, &listing_id)
            .await
        {
            HomeserverRawFetchOutcome::Found(raw) => {
                match award_terms_from_bytes_for_variant(&raw, locator.variant_id.as_deref()) {
                    Ok(snapshot) => Some(snapshot),
                    Err(_) => {
                        return Ok(Err(CommandFailure::refused(
                            crate::refusal_audit::RefusalKind::AwardListingChanged,
                            ErrorCode::AwardListingChanged,
                            "The listing snapshot does not match the offer terms.",
                        )));
                    }
                }
            }
            HomeserverRawFetchOutcome::TooLarge => {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::AwardListingChanged,
                    ErrorCode::AwardListingChanged,
                    "The listing snapshot does not match the offer terms.",
                )));
            }
            HomeserverRawFetchOutcome::NotFound | HomeserverRawFetchOutcome::Unavailable => {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The seller listing record is unavailable.",
                )));
            }
        }
    } else {
        None
    };
    let offer =
        match actionable_offer(tx, actor, payload.offer_id, &command.aggregate_id, now).await? {
            Ok(offer) => offer,
            Err(failure) => return Ok(Err(failure)),
        };
    if command.expected_revision != offer.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The offer revision is stale.",
            offer.revision,
        )));
    }
    if actor == offer.offered_by {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "The current offer author cannot counter their own terms.",
        )));
    }
    if offer.currency != payload.amount.currency || offer.exponent != payload.amount.exponent {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "Counteroffer amount must use the original asset and exponent.",
        )));
    }
    let refreshed_variant = if let Some(snapshot) = refreshed_terms.as_ref() {
        if snapshot.currency != payload.amount.currency
            || snapshot.exponent != payload.amount.exponent
        {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::AwardListingChanged,
                ErrorCode::AwardListingChanged,
                "The listing snapshot does not match the offer terms.",
            )));
        }
        let variant = offer
            .variant_id
            .as_deref()
            .and_then(|id| snapshot.variants.iter().find(|variant| variant.id == id))
            .or_else(|| {
                (offer.variant_id.is_none() && snapshot.variants.len() == 1)
                    .then(|| &snapshot.variants[0])
            });
        let Some(variant) = variant else {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::AwardVariantMismatch,
                ErrorCode::AwardVariantMismatch,
                "The checkout variant does not match the accepted offer.",
            )));
        };
        if variant.quantity < payload.quantity {
            return Ok(Err(CommandFailure::refused_with_revision(
                crate::refusal_audit::RefusalKind::InsufficientInventory,
                ErrorCode::InsufficientInventory,
                "The counteroffer quantity is unavailable.",
                offer.revision,
            )));
        }
        Some(variant.id.clone())
    } else {
        None
    };
    let Some(listing) = fetch_listing(tx, &offer.listing_aggregate_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The offer listing is unavailable.",
        )));
    };
    if listing.available_quantity < payload.quantity {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::InsufficientInventory,
            ErrorCode::InsufficientInventory,
            "The counteroffer quantity is unavailable.",
            offer.revision,
        )));
    }

    let new_revision = offer.revision + 1;
    let expires_at = now + chrono::Duration::seconds(payload.expires_in_seconds);
    let amount_json = json!({
        "amount_minor": payload.amount.amount_minor,
        "currency": payload.amount.currency,
        "exponent": payload.amount.exponent,
    });
    let entry = history_entry(
        new_revision,
        actor,
        "countered",
        &amount_json,
        payload.quantity,
        &payload.message,
        now,
    );
    let updated: OfferRow = sqlx::query_as(&format!(
        "UPDATE offers SET revision = $2, state = 'countered', offered_by = $3, \
         amount_minor = $4, quantity = $5, message = $6, expires_at = $7, updated_at = $8, \
         history = history || $9::jsonb, \
         variant_id = COALESCE($10, variant_id), \
         terms_listing_revision = COALESCE($11, terms_listing_revision), \
         terms_listing_record_sha256 = COALESCE($12, terms_listing_record_sha256), \
         terms_snapshot = COALESCE($13, terms_snapshot) \
         WHERE id = $1 RETURNING {OFFER_COLUMNS}"
    ))
    .bind(offer.id)
    .bind(new_revision)
    .bind(actor)
    .bind(payload.amount.amount_minor)
    .bind(payload.quantity)
    .bind(&payload.message)
    .bind(expires_at)
    .bind(now)
    .bind(json!([entry]))
    .bind(refreshed_variant.as_deref())
    .bind(refreshed_terms.as_ref().map(|snapshot| snapshot.revision))
    .bind(
        refreshed_terms
            .as_ref()
            .map(|snapshot| snapshot.raw_sha256.as_str()),
    )
    .bind(refreshed_terms.as_ref().map(|snapshot| {
        json!({
            "revision": snapshot.revision,
            "title": snapshot.title,
            "currency": snapshot.currency,
            "exponent": snapshot.exponent,
            "shipping_minor": snapshot.shipping_minor,
            "variant": refreshed_variant.as_ref().or(offer.variant_id.as_ref()),
        })
    }))
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &offer.aggregate_id,
        new_revision,
        actor,
        "offer.countered",
        now,
    )
    .await?;
    insert_notification_intent(
        tx,
        event_id,
        "offer_countered",
        other_participant(&updated, actor),
        actor,
        &offer.aggregate_id,
        Some(&amount_json),
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "offer", "offer": updated.view(now) }),
    }))
}

pub async fn accept(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OfferActionPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let locator: Option<OfferRow> =
        sqlx::query_as(&format!("SELECT {OFFER_COLUMNS} FROM offers WHERE id = $1"))
            .bind(payload.offer_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(locator) = locator else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The offer was not found.",
        )));
    };
    let (snapshot, fetched_identity) = if let Some(homeserver) = homeserver {
        let listing_id: Option<(String,)> =
            sqlx::query_as("SELECT listing_id FROM listings WHERE aggregate_id = $1")
                .bind(&locator.listing_aggregate_id)
                .fetch_optional(&mut **tx)
                .await?;
        let Some((listing_id,)) = listing_id else {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::NotFound,
                ErrorCode::NotFound,
                "The offer listing is unavailable.",
            )));
        };
        let raw = match homeserver
            .fetch_listing_raw(&locator.seller_pubky, &listing_id)
            .await
        {
            HomeserverRawFetchOutcome::Found(raw) => raw,
            HomeserverRawFetchOutcome::TooLarge => {
                // Design §7 designates an oversized seller record as a
                // changed canonical body, never malformed command input.
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::AwardListingChanged,
                    ErrorCode::AwardListingChanged,
                    "The listing snapshot does not match the offer terms.",
                )));
            }
            HomeserverRawFetchOutcome::NotFound | HomeserverRawFetchOutcome::Unavailable => {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The seller listing record is unavailable.",
                )));
            }
        };
        let snapshot = match award_terms_from_bytes_for_variant(&raw, locator.variant_id.as_deref())
        {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::AwardListingChanged,
                    ErrorCode::AwardListingChanged,
                    "The listing snapshot does not match the offer terms.",
                )));
            }
        };
        (
            snapshot,
            Some((
                locator.seller_pubky.clone(),
                locator.listing_aggregate_id.clone(),
                listing_id,
            )),
        )
    } else {
        (
            AwardListingSnapshot {
                raw_sha256: locator
                    .terms_listing_record_sha256
                    .clone()
                    .unwrap_or_else(|| locator.listing_aggregate_id.clone()),
                revision: locator.terms_listing_revision.unwrap_or(1),
                title: "Marketplace item".to_string(),
                currency: locator.currency.clone(),
                exponent: locator.exponent,
                shipping_minor: 0,
                variants: vec![AwardVariantSnapshot {
                    id: locator
                        .variant_id
                        .clone()
                        .unwrap_or_else(|| locator.listing_aggregate_id.clone()),
                    sku: None,
                    options: Value::Array(vec![]),
                    quantity: locator.quantity,
                    price: None,
                }],
            },
            None,
        )
    };
    let offer =
        match actionable_offer(tx, actor, payload.offer_id, &command.aggregate_id, now).await? {
            Ok(offer) => offer,
            Err(failure) => return Ok(Err(failure)),
        };
    if let Some((fetched_seller, fetched_aggregate, fetched_listing_id)) = fetched_identity {
        let current_listing = fetch_listing(tx, &offer.listing_aggregate_id).await?;
        if offer.seller_pubky != fetched_seller
            || offer.listing_aggregate_id != fetched_aggregate
            || current_listing.as_ref().is_none_or(|listing| {
                listing.seller_pubky != fetched_seller
                    || listing.listing_id != fetched_listing_id
                    || listing.aggregate_id != fetched_aggregate
            })
        {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::AwardListingChanged,
                ErrorCode::AwardListingChanged,
                "The listing snapshot does not match the offer terms.",
            )));
        }
    }
    if command.expected_revision != offer.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The offer revision is stale.",
            offer.revision,
        )));
    }
    if actor == offer.offered_by {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "The current offer author cannot accept their own terms.",
        )));
    }
    if offer
        .terms_listing_record_sha256
        .as_deref()
        .is_some_and(|hash| hash != snapshot.raw_sha256)
        || offer
            .terms_listing_revision
            .is_some_and(|revision| revision != snapshot.revision)
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardListingChanged,
            ErrorCode::AwardListingChanged,
            "The listing snapshot does not match the offer terms.",
        )));
    }
    let variant = if let Some(variant_id) = offer.variant_id.as_deref() {
        snapshot
            .variants
            .iter()
            .find(|variant| variant.id == variant_id)
    } else if snapshot.variants.len() == 1 {
        snapshot.variants.first()
    } else {
        None
    };
    let Some(variant) = variant else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardVariantMismatch,
            ErrorCode::AwardVariantMismatch,
            "The checkout variant does not match the accepted offer.",
        )));
    };
    if variant.quantity < offer.quantity
        || snapshot.currency != offer.currency
        || snapshot.exponent != offer.exponent
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "Offer amount must use the listing asset and exponent.",
        )));
    }
    let Some(listing) = fetch_listing_for_update(tx, &offer.listing_aggregate_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The offer listing is unavailable.",
        )));
    };
    if listing.available_quantity < offer.quantity {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::InsufficientInventory,
            ErrorCode::InsufficientInventory,
            "The offered quantity is no longer available.",
            offer.revision,
        )));
    }

    let new_listing_state = if listing.available_quantity == offer.quantity {
        "reserved"
    } else {
        "available"
    };
    if !can_transition(&listing_machine(), &listing.state, new_listing_state) {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "The listing cannot enter the reserved state.",
            listing.server_revision,
        )));
    }
    let updated_listing: ListingRow = sqlx::query_as(&format!(
        "UPDATE listings SET server_revision = server_revision + 1, state = $3, \
         available_quantity = available_quantity - $4, \
         reserved_quantity = reserved_quantity + $4, updated_at = $5 \
         WHERE aggregate_id = $1 AND server_revision = $2 AND available_quantity >= $4 \
         RETURNING {LISTING_COLUMNS}"
    ))
    .bind(&listing.aggregate_id)
    .bind(listing.server_revision)
    .bind(new_listing_state)
    .bind(offer.quantity)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let expires_at = now + chrono::Duration::seconds(ACCEPTED_OFFER_HOLD_SECONDS);
    let award_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO reservations (id, listing_aggregate_id, buyer_pubky, quantity, status, \
         expires_at, created_at, updated_at, offer_award_id) \
         VALUES ($1, $2, $3, $4, 'active', $5, $6, $6, $7)",
    )
    .bind(command.command_id)
    .bind(&listing.aggregate_id)
    .bind(&offer.buyer_pubky)
    .bind(offer.quantity)
    .bind(expires_at)
    .bind(now)
    .bind(award_id)
    .execute(&mut **tx)
    .await?;
    let reservation = ReservationRow {
        id: command.command_id,
        listing_aggregate_id: listing.aggregate_id.clone(),
        buyer_pubky: offer.buyer_pubky.clone(),
        quantity: offer.quantity,
        status: "active".to_string(),
        expires_at,
        created_at: now,
    };

    let accepted: OfferRow = sqlx::query_as(&format!(
        "UPDATE offers SET revision = $2, state = 'accepted', updated_at = $3,
         award_id = $4, accepted_at = $3, award_expires_at = $5, reservation_id = $6,
         accepted_unit_price_minor = amount_minor, accepted_currency = currency,
         accepted_exponent = exponent, accepted_quantity = quantity,
         accepted_listing_aggregate_id = listing_aggregate_id,
         accepted_listing_title = $7, accepted_listing_revision = $8,
         accepted_listing_record_sha256 = $9, accepted_variant_id = $10,
         accepted_variant_sku = $11, accepted_variant_options = $12, accepted_shipping_minor = $13,
         accepted_subtotal_minor = $14, accepted_total_minor = $15,
         accepted_fulfillment = 'shipping',
         history = history || $16::jsonb
         WHERE id = $1 RETURNING {OFFER_COLUMNS}"
    ))
    .bind(offer.id)
    .bind(offer.revision + 1)
    .bind(now)
    .bind(award_id)
    .bind(expires_at)
    .bind(command.command_id)
    .bind(&snapshot.title)
    .bind(snapshot.revision)
    .bind(&snapshot.raw_sha256)
    .bind(&variant.id)
    .bind(variant.sku.as_deref())
    .bind(&variant.options)
    .bind(snapshot.shipping_minor)
    .bind(
        offer
            .amount_minor
            .checked_mul(offer.quantity)
            .ok_or_else(|| sqlx::Error::Protocol("offer amount overflow".to_string()))?,
    )
    .bind(
        offer
            .amount_minor
            .checked_mul(offer.quantity)
            .and_then(|subtotal| subtotal.checked_add(snapshot.shipping_minor))
            .ok_or_else(|| sqlx::Error::Protocol("offer total overflow".to_string()))?,
    )
    .bind(json!([history_entry(
        offer.revision + 1,
        actor,
        "accepted",
        &offer.amount_json(),
        offer.quantity,
        "",
        now,
    )]))
    .fetch_one(&mut **tx)
    .await?;

    let offer_event_id = insert_event(
        tx,
        command.command_id,
        &offer.aggregate_id,
        accepted.revision,
        actor,
        "offer.accepted",
        now,
    )
    .await?;
    let inventory_event_id = insert_event(
        tx,
        command.command_id,
        &listing.aggregate_id,
        updated_listing.server_revision,
        actor,
        "inventory.reserved",
        now,
    )
    .await?;
    insert_notification_intent(
        tx,
        offer_event_id,
        "offer_accepted",
        other_participant(&accepted, actor),
        actor,
        &offer.aggregate_id,
        Some(&accepted.amount_json()),
        now,
    )
    .await?;

    let reserve = fetch_auction_reserve(tx, &updated_listing.aggregate_id).await?;
    let listing_projection = updated_listing
        .projection_for_actor_with_auction(actor, reserve.as_ref(), None)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    Ok(Ok(HandlerSuccess {
        revision: accepted.revision,
        event_ids: vec![offer_event_id, inventory_event_id],
        result: json!({
            "kind": "accepted_offer",
            "offer": accepted.view(now),
            "listing": listing_projection,
            "reservation": reservation.view(),
        }),
    }))
}

pub async fn reject(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OfferActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let offer =
        match actionable_offer(tx, actor, payload.offer_id, &command.aggregate_id, now).await? {
            Ok(offer) => offer,
            Err(failure) => return Ok(Err(failure)),
        };
    if command.expected_revision != offer.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The offer revision is stale.",
            offer.revision,
        )));
    }
    if actor == offer.offered_by {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "The current offer author cannot reject their own terms.",
        )));
    }

    let updated = finish_offer(tx, &offer, actor, "rejected", now).await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &offer.aggregate_id,
        updated.revision,
        actor,
        "offer.rejected",
        now,
    )
    .await?;
    insert_notification_intent(
        tx,
        event_id,
        "offer_rejected",
        other_participant(&updated, actor),
        actor,
        &offer.aggregate_id,
        Some(&updated.amount_json()),
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "offer", "offer": updated.view(now) }),
    }))
}

pub async fn withdraw(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OfferActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let offer =
        match actionable_offer(tx, actor, payload.offer_id, &command.aggregate_id, now).await? {
            Ok(offer) => offer,
            Err(failure) => return Ok(Err(failure)),
        };
    // The prototype checks authorship before the revision (unlike reject).
    if actor != offer.offered_by {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the current offer author may withdraw it.",
        )));
    }
    if command.expected_revision != offer.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The offer revision is stale.",
            offer.revision,
        )));
    }

    let updated = finish_offer(tx, &offer, actor, "withdrawn", now).await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &offer.aggregate_id,
        updated.revision,
        actor,
        "offer.withdrawn",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: updated.revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "offer", "offer": updated.view(now) }),
    }))
}

/// Loads and locks an offer that the actor may act on: participant-only,
/// still `pending`/`countered`, and not expired on server time.
async fn actionable_offer(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    offer_id: Uuid,
    aggregate_id: &str,
    now: DateTime<Utc>,
) -> Result<Result<OfferRow, CommandFailure>, sqlx::Error> {
    let offer: Option<OfferRow> = sqlx::query_as(&format!(
        "SELECT {OFFER_COLUMNS} FROM offers WHERE id = $1 FOR UPDATE"
    ))
    .bind(offer_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(offer) = offer else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The offer was not found.",
        )));
    };
    if aggregate_id != offer.aggregate_id {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The offer aggregate id is invalid.",
        )));
    }
    if actor != offer.buyer_pubky && actor != offer.seller_pubky {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only offer participants may act on it.",
        )));
    }
    if offer.state != "pending" && offer.state != "countered" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The offer is no longer actionable.",
        )));
    }
    if offer.expires_at <= now {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::OfferExpired,
            ErrorCode::OfferExpired,
            "The offer has expired.",
        )));
    }
    Ok(Ok(offer))
}

/// Moves an offer into a terminal state and appends the history entry
/// (empty message, current amount and quantity, as in the prototype).
async fn finish_offer(
    tx: &mut Transaction<'_, Postgres>,
    offer: &OfferRow,
    actor: &str,
    state: &str,
    now: DateTime<Utc>,
) -> Result<OfferRow, sqlx::Error> {
    debug_assert!(can_transition(&offer_machine(), &offer.state, state));
    let new_revision = offer.revision + 1;
    let entry = history_entry(
        new_revision,
        actor,
        state,
        &offer.amount_json(),
        offer.quantity,
        "",
        now,
    );
    sqlx::query_as(&format!(
        "UPDATE offers SET revision = $2, state = $3, updated_at = $4, \
         history = history || $5::jsonb \
         WHERE id = $1 RETURNING {OFFER_COLUMNS}"
    ))
    .bind(offer.id)
    .bind(new_revision)
    .bind(state)
    .bind(now)
    .bind(json!([entry]))
    .fetch_one(&mut **tx)
    .await
}

fn other_participant<'a>(offer: &'a OfferRow, actor: &str) -> &'a str {
    if actor == offer.seller_pubky {
        &offer.buyer_pubky
    } else {
        &offer.seller_pubky
    }
}

fn same_asset(listing: &ListingRow, amount: &Money) -> bool {
    listing.unit_price_currency == amount.currency && listing.unit_price_exponent == amount.exponent
}
