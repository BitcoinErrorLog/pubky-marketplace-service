//! `payment.register_locks` (plan task 4.5): the buyer registers the Locks
//! lifecycle correlation for their payment.
//!
//! Registration binds the encrypted lifecycle identity to the order, buyer,
//! creator (the seller), lock resource hash, amount, asset, and policy
//! version (upstream-integration "Transaction-service correlation"). It
//! never advances the payment: only the background worker's independent
//! verification of a completed Locks result does that (ADR-0019 §7 — the
//! service must not forge completion from client input).
//!
//! Replay discipline:
//! - an exact replay of the command returns the stored result (executor);
//! - a changed replay under the same command id is an idempotency conflict
//!   (executor);
//! - a second registration for the same payment or order is refused
//!   (`INVALID_STATE`, backstopped by the table's UNIQUE constraints);
//! - the same `{creator, bundle_id}` identity can never correlate a second
//!   order — the HMAC lookup token is UNIQUE.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    parse_lock_resource, PrepareLocksPayload, RegisterLocksPayload,
};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_order_for_update, holds};
use crate::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use crate::locks::{LocksKeys, LocksRuntime};
use crate::model::PaymentRow;
use crate::queries::PAYMENT_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn register(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RegisterLocksPayload,
    locks: Option<&LocksRuntime>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Fail closed: without configured keys the bundle id cannot be stored
    // encrypted, so the deployment refuses the command outright.
    let Some(locks) = locks else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Locks verification is not enabled on this deployment.",
        )));
    };

    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(payload.payment_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(payment) = payment else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The payment was not found.",
        )));
    };
    if payment.buyer_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the buyer may register the Locks correlation.",
        )));
    }
    if command.aggregate_id != ids::payment_aggregate_id(payment.id) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The payment aggregate id is invalid.",
        )));
    }
    if command.expected_revision != payment.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The payment revision is stale.",
            payment.revision,
        )));
    }
    if payment.state != "awaiting_entitlement" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Only a payment awaiting entitlement can register a Locks correlation.",
        )));
    }
    let prepared: Option<(Uuid, String, String, DateTime<Utc>)> =
        sqlx::query_as("SELECT id, creator_pubky, preparation_state, window_expires_at FROM payment_locks_correlations WHERE payment_id = $1 FOR UPDATE")
            .bind(payment.id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some((correlation_id, creator, preparation_state, window_expires_at)) = prepared else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Prepare the seller-authorized Locks payment before registering a bundle.",
        )));
    };
    if preparation_state != "prepared" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The payment already has a registered Locks bundle.",
        )));
    }
    // Receiver-clock eligibility, under the payment/order locks: attachment
    // is allowed only while the prepared window is still open and the order
    // still holds exactly that window. Without this an elapsed preparation
    // could attach (and later confirm) before the expiry sweep runs.
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };
    if order.state != "pending_payment"
        || !order.stock_held
        || order.hold_expires_at != Some(window_expires_at)
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order no longer holds the prepared Locks payment window.",
        )));
    }
    if now >= window_expires_at {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The Locks preparation has expired.",
        )));
    }
    let bundle_id_ciphertext = locks.keys.encrypt_bundle_id(payment.id, &payload.bundle_id);
    let bundle_lookup_token = locks.keys.lookup_token(&creator, &payload.bundle_id);

    sqlx::query(
        "UPDATE payment_locks_correlations SET bundle_id_ciphertext = $2, \
         bundle_lookup_token = $3, preparation_state = 'registered', verification_state = 'pending', \
         updated_at = $4 WHERE id = $1",
    )
    .bind(correlation_id)
    .bind(&bundle_id_ciphertext)
    .bind(&bundle_lookup_token)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    // The 'locks' adapter permanently closes the sandbox path for this
    // payment: from here only server-side verification advances it.
    let updated_payment: PaymentRow = sqlx::query_as(&format!(
        "UPDATE payments SET revision = revision + 1, adapter = 'locks', updated_at = $2 \
         WHERE id = $1 RETURNING {PAYMENT_COLUMNS}"
    ))
    .bind(payment.id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        updated_payment.revision,
        actor,
        "payment.locks_registered",
        now,
    )
    .await?;

    tracing::info!(
        payment_id = %payment.id,
        correlation_id = %correlation_id,
        "registered locks correlation"
    );
    Ok(Ok(HandlerSuccess {
        revision: updated_payment.revision,
        event_ids: vec![event_id],
        result: json!({
            "kind": "payment",
            "payment": updated_payment.projection(),
            // Correlation metadata only — never the bundle id or the lock
            // resource (ADR-0019 §8).
            "verification": {
                "state": "pending",
                "window_expires_at": format_timestamp(window_expires_at),
            },
        }),
    }))
}

/// Creates the sole authoritative Locks preparation for a payment. The
/// listing's seller-authored lock, rather than registration input, defines
/// every expected fact retained on the correlation.
#[allow(clippy::too_many_arguments)]
pub async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &PrepareLocksPayload,
    locks: Option<&LocksRuntime>,
    homeserver: Option<&dyn HomeserverListingClient>,
    payment_window_seconds: i64,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let (Some(locks), Some(homeserver)) = (locks, homeserver) else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Locks preparation is not enabled on this deployment.",
        )));
    };
    let payment: Option<PaymentRow> = sqlx::query_as(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = $1 FOR UPDATE"
    ))
    .bind(payload.payment_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(payment) = payment else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The payment was not found.",
        )));
    };
    if payment.buyer_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the buyer may prepare the Locks payment.",
        )));
    }
    if command.aggregate_id != ids::payment_aggregate_id(payment.id) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The payment aggregate id is invalid.",
        )));
    }
    if command.expected_revision != payment.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The payment revision is stale.",
            payment.revision,
        )));
    }
    let existing: Option<(String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT preparation_state, client_reference_ciphertext, window_expires_at \
         FROM payment_locks_correlations WHERE payment_id = $1 FOR UPDATE",
    )
    .bind(payment.id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((state, Some(sealed_reference), expires_at)) = existing.as_ref() {
        if state == "prepared" {
            let reference = crate::seal::open(
                locks.keys.encryption_bytes(),
                &[b"locks-client-reference:".as_slice(), payment.id.as_bytes()].concat(),
                sealed_reference,
            )
            .ok()
            .and_then(|value| String::from_utf8(value).ok());
            if let Some(client_reference) = reference {
                return Ok(Ok(HandlerSuccess {
                    revision: payment.revision,
                    event_ids: vec![],
                    result: json!({"kind": "payment", "client_reference": client_reference, "window_expires_at": format_timestamp(*expires_at)}),
                }));
            }
        }
    } else if existing.is_some() {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The payment already has a Locks preparation.",
        )));
    }
    if payment.state != "awaiting_entitlement" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Only a payment awaiting entitlement can prepare Locks.",
        )));
    }
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };
    if order.state != "pending_payment" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "Only a pending order can prepare Locks.",
        )));
    }
    // The seller-authoritative lock snapshot lives on the listing rows, not
    // on the projected order lines (ADR-0019 §8): collect the distinct
    // seller-authored Locks payment locks across the order's listings and
    // require exactly one (multi-lock aggregation is a future design).
    let mut lock_pairs: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();
    for line in order.lines.as_array().into_iter().flatten() {
        let Some(aggregate_id) = line
            .get("listing_aggregate_id")
            .and_then(serde_json::Value::as_str)
        else {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvariantViolation,
                "An order line is missing its listing.",
            )));
        };
        let Some(listing) = crate::handlers::fetch_listing_for_update(tx, aggregate_id).await?
        else {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvariantViolation,
                "An order line's listing is missing.",
            )));
        };
        if let (Some(policy_uri), Some(criterion_id)) = (
            listing.digital_lock_policy_uri,
            listing.digital_lock_criterion_id,
        ) {
            lock_pairs.insert((policy_uri, criterion_id));
        }
    }
    if lock_pairs.len() != 1 {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The order must contain exactly one seller-authored Locks payment lock.",
        )));
    }
    let (resource, criterion_id) = lock_pairs.into_iter().next().expect("non-empty lock pair");
    let Some((creator, _)) = parse_lock_resource(&resource) else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The seller's Locks resource is invalid.",
        )));
    };
    if creator != payment.seller_pubky {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The seller's Locks resource creator does not match the order.",
        )));
    }
    let content = match homeserver
        .fetch_content_lock(
            creator,
            resource
                .strip_prefix(creator)
                .expect("creator prefixes canonical resource"),
        )
        .await
    {
        HomeserverFetchOutcome::Found(value) => value,
        HomeserverFetchOutcome::NotFound => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "The seller's Locks document is unavailable.",
            )))
        }
        HomeserverFetchOutcome::Unavailable => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::UpstreamUnavailable,
                "The seller's Locks document could not be reached.",
            )))
        }
    };
    let content_lock = match crate::content_lock::validate_content_lock_value(&content, &resource) {
        Ok(content_lock) => content_lock,
        Err(_) => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                "The seller's Locks document does not match the payment.",
            )))
        }
    };
    if content_lock.validate_paykit_payment_v1_policy().is_err()
        || !criterion_matches_payment(
            &content_lock,
            &criterion_id,
            payment.amount_minor,
            &payment.currency,
        )
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The seller's Locks document does not match the payment.",
        )));
    }
    if let Err(failure) =
        holds::acquire_payment_hold(tx, order, payment_window_seconds, now).await?
    {
        return Ok(Err(failure));
    }
    let client_reference = LocksKeys::mint_client_reference();
    let correlation_id = Uuid::new_v4();
    let window_expires_at = now + chrono::Duration::seconds(payment_window_seconds);
    let resource_hash = blake3::hash(resource.as_bytes()).to_hex().to_string();
    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, creator_pubky, \
         lock_resource_hash, amount_minor, asset, exponent, policy_version, verification_state, window_expires_at, \
         expected_resource_ciphertext, expected_resource_hash, criterion_id, expected_reader_pubky, expected_recipient_pubky, \
         client_reference_ciphertext, preparation_state, created_at, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,(SELECT guarantee_policy_version FROM orders WHERE id=$3),'pending',$10,$11,$6,$12,$4,$5,$13,'prepared',$14,$14)",
    )
    .bind(correlation_id).bind(payment.id).bind(payment.order_id).bind(&payment.buyer_pubky)
    .bind(&payment.seller_pubky).bind(&resource_hash).bind(payment.amount_minor).bind(&payment.currency)
    .bind(payment.exponent).bind(window_expires_at)
    .bind(locks.keys.encrypt_prepared_value(payment.id, b"locks-resource:", &resource))
    .bind(&criterion_id)
    .bind(locks.keys.encrypt_prepared_value(payment.id, b"locks-client-reference:", &client_reference))
    .bind(now).execute(&mut **tx).await?;
    Ok(Ok(HandlerSuccess {
        revision: payment.revision,
        event_ids: vec![],
        result: json!({"kind": "payment", "client_reference": client_reference, "window_expires_at": format_timestamp(window_expires_at)}),
    }))
}

/// The marketplace economics gate on top of the upstream policy invariants:
/// the sole `paykit-payment` criterion must be the seller-authored criterion
/// from the checkout snapshot, and its amount/asset must equal the immutable
/// payment facts exactly (no conversion, no unit guessing).
fn criterion_matches_payment(
    content_lock: &crate::content_lock::ContentLock,
    criterion_id: &str,
    amount_minor: i64,
    asset: &str,
) -> bool {
    let [criterion] = &content_lock.criteria[..] else {
        return false;
    };
    criterion.criterion_id == criterion_id
        && criterion.verifier_type == crate::content_lock::VerifierType::PaykitPayment
        && criterion
            .params
            .get("recipient_pubky")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::content_lock::PubkyIdentity::parse)
            .as_ref()
            == Some(&content_lock.creator)
        && criterion
            .params
            .get("asset")
            .and_then(serde_json::Value::as_str)
            == Some(asset)
        && criterion
            .params
            .get("amount")
            .and_then(serde_json::Value::as_str)
            == Some(amount_minor.to_string().as_str())
}
