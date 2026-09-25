use chrono::{DateTime, Utc};
use marketplace_domain::commands::{Command, FulfillmentMethod, OfferCheckoutPayload};
use marketplace_domain::{ids, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::Clock;
use crate::executor::insert_event;
use crate::handlers::offers::OFFER_COLUMNS;
use crate::handlers::{fetch_listing_for_update, insert_notification_intent};
use crate::model::OfferRow;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

#[derive(sqlx::FromRow)]
struct AwardReservation {
    listing_aggregate_id: String,
    buyer_pubky: String,
    quantity: i64,
    status: String,
    expires_at: DateTime<Utc>,
    offer_award_id: Option<Uuid>,
}

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OfferCheckoutPayload,
    clock: &dyn Clock,
    hold_window_seconds: i64,
) -> Result<HandlerResult, sqlx::Error> {
    let offer: Option<OfferRow> = sqlx::query_as(&format!(
        "SELECT {OFFER_COLUMNS} FROM offers WHERE id = $1 FOR UPDATE"
    ))
    .bind(payload.offer_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(offer) = offer else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the accepted offer's buyer may place this order.",
        )));
    };
    if command.aggregate_id != ids::offer_aggregate_id(offer.id) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The offer checkout input is invalid.",
        )));
    }
    if actor != offer.buyer_pubky {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the accepted offer's buyer may place this order.",
        )));
    }
    if offer.state == "converted" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardAlreadyConverted,
            ErrorCode::AwardAlreadyConverted,
            "This accepted offer has already been converted to an order.",
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
    let lock_now = clock.now();
    if offer.expiry_reason.as_deref() == Some("award_window")
        || (offer.state == "accepted"
            && offer
                .award_expires_at
                .is_none_or(|deadline| lock_now >= deadline))
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardExpired,
            ErrorCode::AwardExpired,
            "This accepted offer's checkout window has expired. Nothing was ordered.",
        )));
    }
    if offer.state != "accepted" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only an accepted offer can enter offer checkout.",
        )));
    }
    if offer.accepted_variant_id.as_deref() != Some(&payload.variant_id) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardVariantMismatch,
            ErrorCode::AwardVariantMismatch,
            "The checkout variant does not match the accepted offer.",
        )));
    }
    if offer.award_id != Some(payload.award_id)
        || offer.accepted_listing_aggregate_id.as_deref() != Some(&payload.listing_aggregate_id)
        || offer.accepted_listing_revision != Some(payload.listing_revision)
        || offer.accepted_listing_record_sha256.as_deref() != Some(&payload.listing_record_sha256)
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardListingChanged,
            ErrorCode::AwardListingChanged,
            "The listing snapshot does not match the offer terms.",
        )));
    }
    if offer.accepted_quantity != Some(payload.quantity) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardQuantityMismatch,
            ErrorCode::AwardQuantityMismatch,
            "The checkout quantity does not match the accepted offer.",
        )));
    }

    let reservation_id = offer
        .reservation_id
        .ok_or_else(|| sqlx::Error::Protocol("accepted offer has no reservation".to_string()))?;
    let reservation: Option<AwardReservation> = sqlx::query_as(
        "SELECT listing_aggregate_id, buyer_pubky, quantity, status, expires_at, offer_award_id \
             FROM reservations WHERE id = $1 FOR UPDATE",
    )
    .bind(reservation_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(reservation) = reservation else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardHoldMissing,
            ErrorCode::AwardHoldMissing,
            "The inventory reserved for this accepted offer is no longer held.",
        )));
    };
    if reservation.status != "active" || reservation.expires_at <= lock_now {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardExpired,
            ErrorCode::AwardExpired,
            "This accepted offer's checkout window has expired. Nothing was ordered.",
        )));
    }
    let Some(listing) = fetch_listing_for_update(tx, &reservation.listing_aggregate_id).await?
    else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardHoldMissing,
            ErrorCode::AwardHoldMissing,
            "The inventory reserved for this accepted offer is no longer held.",
        )));
    };
    if reservation.buyer_pubky != offer.buyer_pubky
        || reservation.listing_aggregate_id != payload.listing_aggregate_id
        || reservation.quantity != payload.quantity
        || reservation.offer_award_id != Some(payload.award_id)
        || listing.reserved_quantity < reservation.quantity
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardHoldMissing,
            ErrorCode::AwardHoldMissing,
            "The inventory reserved for this accepted offer is no longer held.",
        )));
    }
    // The award settles through the fulfillment the buyer chose, validated
    // against the methods the ACCEPTED snapshot published — the same snapshot
    // that priced its shipping — never the mutable listing row, which can
    // diverge from it or change after acceptance. A disallowed choice is a
    // typed refusal, never a silent fall back.
    let fulfillment = payload.fulfillment.unwrap_or(FulfillmentMethod::Shipping);
    if !offer
        .award_fulfillment_methods()
        .iter()
        .any(|accepted| accepted == fulfillment.as_str())
    {
        return Ok(Err(CommandFailure::refused_with_reason(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The accepted offer does not include the chosen fulfillment method.",
            "fulfillment_not_published",
        )));
    }
    let pickup = fulfillment == FulfillmentMethod::Pickup;
    let delivery_address = match (&payload.delivery_address, pickup) {
        (Some(address), false) => Some(serde_json::to_value(address).expect("address serializes")),
        (None, true) => None,
        (Some(_), true) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A checkout with no shipped group must not carry a delivery address.",
            )));
        }
        (None, false) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A delivery address is required when any checkout group ships.",
            )));
        }
    };

    let order_id = Uuid::new_v4();
    let payment_id = Uuid::new_v4();
    let subtotal = offer
        .accepted_subtotal_minor
        .ok_or_else(|| sqlx::Error::Protocol("accepted offer has no subtotal".to_string()))?;
    // A pickup order's shipping is zero, never charged and refunded later
    // (§A2): the buyer pays the accepted merchandise subtotal only.
    let (shipping, total) = if pickup {
        (0, subtotal)
    } else {
        (
            offer.accepted_shipping_minor.unwrap_or(0),
            offer
                .accepted_total_minor
                .ok_or_else(|| sqlx::Error::Protocol("accepted offer has no total".to_string()))?,
        )
    };
    let currency = offer
        .accepted_currency
        .as_deref()
        .unwrap_or(&offer.currency);
    let exponent = offer.accepted_exponent.unwrap_or(offer.exponent);
    let lines = json!([{
        "listing_aggregate_id": offer.accepted_listing_aggregate_id,
        "listing_revision": offer.accepted_listing_revision,
        "title": offer.accepted_listing_title,
        "quantity": reservation.quantity,
        "unit_price": {"amount_minor": offer.accepted_unit_price_minor, "currency": currency, "exponent": exponent},
        "subtotal": {"amount_minor": subtotal, "currency": currency, "exponent": exponent},
        "shipping": {"amount_minor": shipping, "currency": currency, "exponent": exponent},
        "priced_from": "offer",
        "offer_id": offer.id,
        "award_id": payload.award_id,
        "variant_id": payload.variant_id,
        "fulfillment": fulfillment.as_str()
    }]);
    let hold_expires_at = lock_now + chrono::Duration::seconds(hold_window_seconds);
    let converted_reservation: Option<Uuid> = sqlx::query_scalar(
        "UPDATE reservations SET status = 'converted', updated_at = $2 \
         WHERE id = $1 AND status = 'active' RETURNING id",
    )
    .bind(reservation_id)
    .bind(lock_now)
    .fetch_optional(&mut **tx)
    .await?;
    if converted_reservation.is_none() {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::AwardHoldMissing,
            ErrorCode::AwardHoldMissing,
            "The inventory reserved for this accepted offer is no longer held.",
        )));
    }
    sqlx::query(
        "INSERT INTO orders (id, checkout_command_id, offer_award_id, priced_from, \
         buyer_pubky, seller_pubky, revision, state, lines, delivery_address, subtotal_minor, \
         shipping_minor, total_minor, currency, exponent, guarantee_policy_version, payment_id, \
         stock_held, hold_expires_at, fulfillment, created_at, updated_at) \
         VALUES ($1, $2, $3, 'offer', $4, $5, 1, 'pending_payment', $6, $7, $8, $9, $10, \
         $11, $12, $13, $14, TRUE, $15, $17, $16, $16)",
    )
    .bind(order_id)
    .bind(command.command_id)
    .bind(payload.award_id)
    .bind(actor)
    .bind(&offer.seller_pubky)
    .bind(&lines)
    .bind(&delivery_address)
    .bind(subtotal)
    .bind(shipping)
    .bind(total)
    .bind(currency)
    .bind(exponent)
    .bind(payload.guarantee_policy_version as i32)
    .bind(payment_id)
    .bind(hold_expires_at)
    .bind(lock_now)
    .bind(fulfillment.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
         state, confirmations, amount_minor, currency, exponent, \
         merchandise_amount_minor, merchandise_currency, merchandise_exponent, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 1, 'sandbox', 'awaiting_entitlement', 0, $5, $6, $7, \
                 $5, $6, $7, $8, $8)",
    )
    .bind(payment_id)
    .bind(order_id)
    .bind(actor)
    .bind(&offer.seller_pubky)
    .bind(total)
    .bind(currency)
    .bind(exponent)
    .bind(lock_now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE offers SET state = 'converted', revision = revision + 1, \
         converted_order_id = $2, converted_at = $3, updated_at = $3 WHERE id = $1",
    )
    .bind(offer.id)
    .bind(order_id)
    .bind(lock_now)
    .execute(&mut **tx)
    .await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &offer.aggregate_id,
        offer.revision + 1,
        actor,
        "offer.converted",
        lock_now,
    )
    .await?;
    let order_event_id = insert_event(
        tx,
        command.command_id,
        &ids::order_aggregate_id(order_id),
        1,
        actor,
        "order.created",
        lock_now,
    )
    .await?;
    insert_notification_intent(
        tx,
        order_event_id,
        "order_created",
        &offer.seller_pubky,
        actor,
        &ids::order_aggregate_id(order_id),
        None,
        lock_now,
    )
    .await?;
    Ok(Ok(HandlerSuccess {
        revision: offer.revision + 1,
        event_ids: vec![event_id, order_event_id],
        result: json!({
            "kind": "order",
            "order": {
                "id": order_id,
                "priced_from": "offer",
                "offer_award_id": payload.award_id,
                "lines": lines,
                "subtotal": {"amount_minor": subtotal, "currency": currency, "exponent": exponent},
                "shipping": {"amount_minor": shipping, "currency": currency, "exponent": exponent},
                "total": {"amount_minor": total, "currency": currency, "exponent": exponent},
                "payment_id": payment_id,
                "stock_held": true,
                "hold_expires_at": hold_expires_at,
            }
        }),
    }))
}
