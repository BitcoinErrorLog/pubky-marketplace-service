use chrono::{DateTime, Utc};
use marketplace_domain::commands::{CheckoutLine, Command, CreateCheckoutPayload};
use marketplace_domain::state_machines::{can_transition, listing_machine};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::handlers::{current_listing_revision, fetch_listing, sandbox_tax_minor, SHIPPING_MINOR};
use crate::model::{money_json, ListingRow, OrderRow, PaymentRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &CreateCheckoutPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if command.aggregate_id != ids::checkout_aggregate_id(command.command_id)
        || command.expected_revision != 0
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Checkout aggregate identity or revision is invalid.",
        )));
    }

    let mut resolved: Vec<(&CheckoutLine, ListingRow)> = Vec::with_capacity(payload.lines.len());
    for line in &payload.lines {
        let Some(listing) = fetch_listing(tx, &line.listing_aggregate_id).await? else {
            return Ok(Err(CommandFailure::new(
                ErrorCode::NotFound,
                "A checkout listing is unavailable.",
            )));
        };
        resolved.push((line, listing));
    }
    for (line, listing) in &resolved {
        if listing.seller_pubky == actor {
            return Ok(Err(CommandFailure::new(
                ErrorCode::Unauthorized,
                "A buyer cannot purchase their own listing.",
            )));
        }
        if listing.sale_format != "fixed_price" || listing.state != "available" {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "Only available fixed-price listings can enter checkout.",
            )));
        }
        if line.expected_revision != listing.server_revision {
            return Ok(Err(CommandFailure::with_revision(
                ErrorCode::RevisionConflict,
                "A checkout listing revision is stale.",
                listing.server_revision,
            )));
        }
        if line.quantity > listing.available_quantity {
            return Ok(Err(CommandFailure::with_revision(
                ErrorCode::InsufficientInventory,
                "Checkout quantity is unavailable.",
                listing.server_revision,
            )));
        }
    }
    let first = &resolved[0].1;
    let same_asset = resolved.iter().all(|(_, listing)| {
        listing.unit_price_currency == first.unit_price_currency
            && listing.unit_price_exponent == first.unit_price_exponent
    });
    if !same_asset {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "One checkout may contain only one asset and exponent.",
        )));
    }
    let currency = first.unit_price_currency.clone();
    let exponent = first.unit_price_exponent;

    // Group checkout lines by seller, preserving line order.
    let mut seller_groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (index, (_, listing)) in resolved.iter().enumerate() {
        match seller_groups
            .iter_mut()
            .find(|(seller, _)| *seller == listing.seller_pubky)
        {
            Some((_, indices)) => indices.push(index),
            None => seller_groups.push((listing.seller_pubky.clone(), vec![index])),
        }
    }

    let delivery_address = serde_json::to_value(&payload.delivery_address)
        .expect("delivery address serializes infallibly");
    let mut orders: Vec<Value> = Vec::with_capacity(seller_groups.len());
    let mut payments: Vec<Value> = Vec::with_capacity(seller_groups.len());
    let mut event_ids: Vec<Uuid> = Vec::with_capacity(seller_groups.len());
    for (seller_pubky, indices) in &seller_groups {
        let lines: Vec<Value> = indices
            .iter()
            .map(|&index| {
                let (line, listing) = &resolved[index];
                json!({
                    "listing_aggregate_id": listing.aggregate_id,
                    "listing_revision": listing.listing_revision,
                    "content_hash": listing.content_hash,
                    "title": listing.title,
                    "quantity": line.quantity,
                    "unit_price": listing.unit_price_json(),
                    "subtotal": money_json(
                        listing.unit_price_amount_minor * line.quantity,
                        &listing.unit_price_currency,
                        listing.unit_price_exponent,
                    ),
                })
            })
            .collect();
        let subtotal_minor: i64 = indices
            .iter()
            .map(|&index| {
                let (line, listing) = &resolved[index];
                listing.unit_price_amount_minor * line.quantity
            })
            .sum();
        let tax_minor = sandbox_tax_minor(subtotal_minor, SHIPPING_MINOR);
        let total_minor = subtotal_minor + SHIPPING_MINOR + tax_minor;
        let order_id = Uuid::new_v4();
        let payment_id = Uuid::new_v4();

        let order = OrderRow {
            id: order_id,
            auction_aggregate_id: None,
            buyer_pubky: actor.to_string(),
            seller_pubky: seller_pubky.clone(),
            revision: 1,
            state: "pending_payment".to_string(),
            lines: Value::Array(lines),
            delivery_address: Some(delivery_address.clone()),
            subtotal_minor,
            shipping_minor: SHIPPING_MINOR,
            tax_minor,
            total_minor,
            currency: currency.clone(),
            exponent,
            guarantee_policy_version: payload.guarantee_policy_version as i32,
            payment_id,
            receipt_id: None,
            cancellation_reason: None,
            shipment: None,
            return_request: None,
            dispute: None,
            external_refund: None,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO orders (id, checkout_command_id, buyer_pubky, seller_pubky, revision, \
             state, lines, delivery_address, subtotal_minor, shipping_minor, tax_minor, \
             total_minor, currency, exponent, guarantee_policy_version, payment_id, \
             created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $17)",
        )
        .bind(order.id)
        .bind(command.command_id)
        .bind(&order.buyer_pubky)
        .bind(&order.seller_pubky)
        .bind(order.revision)
        .bind(&order.state)
        .bind(&order.lines)
        .bind(&order.delivery_address)
        .bind(order.subtotal_minor)
        .bind(order.shipping_minor)
        .bind(order.tax_minor)
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
            buyer_pubky: actor.to_string(),
            seller_pubky: seller_pubky.clone(),
            revision: 1,
            adapter: "sandbox".to_string(),
            state: "awaiting_entitlement".to_string(),
            confirmations: 0,
            locks_bundle_id: Uuid::new_v4(),
            amount_minor: total_minor,
            currency: currency.clone(),
            exponent,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
             state, confirmations, locks_bundle_id, amount_minor, currency, exponent, \
             created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)",
        )
        .bind(payment.id)
        .bind(payment.order_id)
        .bind(&payment.buyer_pubky)
        .bind(&payment.seller_pubky)
        .bind(payment.revision)
        .bind(&payment.adapter)
        .bind(&payment.state)
        .bind(payment.confirmations)
        .bind(payment.locks_bundle_id)
        .bind(payment.amount_minor)
        .bind(&payment.currency)
        .bind(payment.exponent)
        .bind(now)
        .execute(&mut **tx)
        .await?;

        let order_aggregate_id = ids::order_aggregate_id(order_id);
        let event_id = insert_event(
            tx,
            command.command_id,
            &order_aggregate_id,
            1,
            actor,
            "order.created",
            now,
        )
        .await?;
        event_ids.push(event_id);

        // Complete notification intent for the seller, delivered by workers
        // at least once (ADR-0019 §4).
        crate::handlers::insert_notification_intent(
            tx,
            event_id,
            "order_created",
            seller_pubky,
            actor,
            &order_aggregate_id,
            now,
        )
        .await?;

        orders.push(order.view());
        // Same redaction as the read projections: the bundle id is bearer
        // material, so it never crosses the wire, not even to the buyer who
        // triggered the checkout. Nothing client-side consumes it.
        payments.push(payment.projection());
    }

    // Move the purchased quantity to reserved with a compare-and-swap on the
    // validated line revision; any concurrent movement aborts the checkout.
    for (line, listing) in &resolved {
        let new_state = if listing.available_quantity == line.quantity {
            "reserved"
        } else {
            "available"
        };
        if !can_transition(&listing_machine(), &listing.state, new_state) {
            return Ok(Err(CommandFailure::with_revision(
                ErrorCode::InvariantViolation,
                "The listing cannot enter the reserved state.",
                listing.server_revision,
            )));
        }
        let updated = sqlx::query(
            "UPDATE listings SET server_revision = server_revision + 1, state = $3, \
             available_quantity = available_quantity - $4, \
             reserved_quantity = reserved_quantity + $4, updated_at = $5 \
             WHERE aggregate_id = $1 AND server_revision = $2 AND available_quantity >= $4",
        )
        .bind(&listing.aggregate_id)
        .bind(line.expected_revision)
        .bind(new_state)
        .bind(line.quantity)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            let latest = current_listing_revision(tx, &listing.aggregate_id).await?;
            return Ok(Err(CommandFailure::with_revision(
                ErrorCode::RevisionConflict,
                "A checkout listing revision is stale.",
                latest,
            )));
        }
    }

    Ok(Ok(HandlerSuccess {
        revision: 1,
        event_ids,
        result: json!({
            "kind": "checkout",
            "orders": orders,
            "payments": payments,
        }),
    }))
}

#[cfg(test)]
mod tests {
    use super::sandbox_tax_minor;

    #[test]
    fn sandbox_tax_matches_the_prototype_rounding() {
        // Prototype fixture: subtotal 12_500 + shipping 1_200 => tax 1_096.
        assert_eq!(sandbox_tax_minor(12_500, 1_200), 1_096);
        assert_eq!(sandbox_tax_minor(0, 1_200), 96);
        // 8% of 13_707 = 1_096.56 rounds to 1_097.
        assert_eq!(sandbox_tax_minor(12_507, 1_200), 1_097);
        // 8% of 13_705 = 1_096.4 rounds to 1_096.
        assert_eq!(sandbox_tax_minor(12_505, 1_200), 1_096);
    }
}
