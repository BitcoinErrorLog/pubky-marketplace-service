use chrono::{DateTime, Utc};
use marketplace_domain::commands::{CheckoutLine, Command, CreateCheckoutPayload};
use marketplace_domain::state_machines::{can_transition, listing_machine};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::handlers::{current_listing_revision, fetch_listing};
use crate::model::{money_json, ListingRow, OrderRow, PaymentRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &CreateCheckoutPayload,
    drop_claim_window_seconds: i64,
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
        if listing.sale_format != "fixed_price" {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "Only fixed-price listings can enter checkout.",
            )));
        }
        // State-specific refusals: a `reserved` listing is held by another
        // buyer's in-flight payment and may restock when its window lapses —
        // the buyer deserves that truth, not a generic unavailability.
        if listing.state == "reserved" {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks.",
            )));
        }
        if listing.state == "sold" {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "This listing has sold out.",
            )));
        }
        if listing.state != "available" {
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
        // Advisory pre-check only ("only a payment locks an item"): the
        // checkout refuses when the stock is not available at this instant
        // but moves nothing — the hold is acquired at a payment lock point.
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

    // Drop gating (ADR-0026). Bindings are looked up — and the bound drop
    // row-locked — BEFORE any listing row lock or order insert, the lock
    // order every gating and release path shares. Cart shape: editions map
    // one order to one unit, so a checkout containing a drop-bound line
    // must be exactly that one line, holding one unit.
    let mut bound_drop: Option<crate::model::DropRow> = None;
    for (_, listing) in &resolved {
        if let Some(drop) =
            crate::handlers::drops::lock_bound_drop(tx, &listing.seller_pubky, &listing.listing_id)
                .await?
        {
            bound_drop = Some(drop);
            break;
        }
    }
    let mut drop_aggregate_id: Option<String> = None;
    if let Some(drop) = bound_drop {
        if resolved.len() != 1 || resolved[0].0.quantity != 1 {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidCommand,
                crate::handlers::drops::DROP_SINGLE_LINE,
            )));
        }
        match crate::handlers::drops::enforce_drop_gate(tx, drop, actor, 1, command.command_id, now)
            .await?
        {
            Ok(drop) => drop_aggregate_id = Some(drop.aggregate_id),
            Err(failure) => return Ok(Err(failure)),
        }
    }

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
                let mut line_json = json!({
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
                });
                // The buyer's variant snapshot rides the order line so
                // packing slips and order rows can show which variant was
                // bought. It is display data validated for shape only:
                // registration carries no variant inventory, so the service
                // cannot check it against the owner-signed listing content.
                if let Some(variant_id) = &line.variant_id {
                    line_json["variant_id"] = json!(variant_id);
                }
                if let Some(options) = &line.variant_options {
                    line_json["variant_options"] = serde_json::to_value(options)
                        .expect("variant options serialize infallibly");
                }
                line_json
            })
            .collect();
        let subtotal_minor: i64 = indices
            .iter()
            .map(|&index| {
                let (line, listing) = &resolved[index];
                listing.unit_price_amount_minor * line.quantity
            })
            .sum();
        // Shipping is the seller-signed flat rate, charged once per order
        // line (quantity-independent). Tax is NOT computed: this service
        // has no basis to determine anyone's tax, so it never invents one —
        // sellers price tax into their listings if they need to.
        let shipping_minor: i64 = indices
            .iter()
            .map(|&index| resolved[index].1.shipping_minor)
            .sum();
        let tax_minor = 0;
        let total_minor = subtotal_minor + shipping_minor + tax_minor;
        let order_id = Uuid::new_v4();
        let payment_id = Uuid::new_v4();

        // Ordinary orders start with NO hold; a drop-bound checkout keeps
        // lock-at-claim (the gate above debited the drop; the listing moves
        // below), so its claim window arms immediately.
        let stock_held = drop_aggregate_id.is_some();
        let hold_expires_at = drop_aggregate_id
            .is_some()
            .then(|| now + chrono::Duration::seconds(drop_claim_window_seconds));

        let order = OrderRow {
            id: order_id,
            auction_aggregate_id: None,
            drop_aggregate_id: drop_aggregate_id.clone(),
            buyer_pubky: actor.to_string(),
            seller_pubky: seller_pubky.clone(),
            revision: 1,
            state: "pending_payment".to_string(),
            lines: Value::Array(lines),
            delivery_address: Some(delivery_address.clone()),
            subtotal_minor,
            shipping_minor,
            tax_minor,
            total_minor,
            currency: currency.clone(),
            exponent,
            guarantee_policy_version: payload.guarantee_policy_version as i32,
            payment_id,
            receipt_id: None,
            edition: None,
            cancellation_reason: None,
            stock_held,
            hold_expires_at,
            shipment: None,
            return_request: None,
            dispute: None,
            external_refund: None,
            payment_method: None,
            fiat_checkout_url: None,
            payment_reported_at: None,
            fiat_transaction_ref: None,
            fiat_verified_by: None,
            paykit_request_reference: None,
            paykit_request_state: None,
            paykit_last_checked_at: None,
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO orders (id, checkout_command_id, drop_aggregate_id, buyer_pubky, \
             seller_pubky, revision, state, lines, delivery_address, subtotal_minor, \
             shipping_minor, tax_minor, total_minor, currency, exponent, \
             guarantee_policy_version, payment_id, stock_held, hold_expires_at, created_at, \
             updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, \
             $18, $19, $20, $20)",
        )
        .bind(order.id)
        .bind(command.command_id)
        .bind(&order.drop_aggregate_id)
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
        .bind(order.stock_held)
        .bind(order.hold_expires_at)
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
            amount_minor: total_minor,
            currency: currency.clone(),
            exponent,
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
            None,
            now,
        )
        .await?;

        orders.push(order.view());
        // Same redaction as the read projections: the bundle id is bearer
        // material, so it never crosses the wire, not even to the buyer who
        // triggered the checkout. Nothing client-side consumes it.
        payments.push(payment.projection());
    }

    // Drop-bound checkout keeps lock-at-claim (ADR-0026: the FCFS race IS
    // the product): the purchased unit moves to reserved with a
    // compare-and-swap on the validated line revision, in the same
    // transaction as the drop debit above. Ordinary checkouts move NOTHING —
    // the hold is acquired at a payment lock point
    // (`handlers::holds::acquire_payment_hold`).
    if drop_aggregate_id.is_some() {
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
