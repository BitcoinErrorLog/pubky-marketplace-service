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
use marketplace_domain::commands::{parse_lock_resource, RegisterLocksPayload};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{fetch_order_for_update, holds};
use crate::locks::LocksRuntime;
use crate::model::PaymentRow;
use crate::queries::PAYMENT_COLUMNS;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub async fn register(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RegisterLocksPayload,
    locks: Option<&LocksRuntime>,
    payment_window_seconds: i64,
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
    let already_correlated: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM payment_locks_correlations WHERE payment_id = $1")
            .bind(payment.id)
            .fetch_optional(&mut **tx)
            .await?;
    if already_correlated.is_some() {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The payment already has a Locks correlation; a changed registration is refused.",
        )));
    }

    // Bind the lifecycle to the order's seller: the lock creator (and the
    // payment recipient, which Locks v1 requires to equal the creator) must
    // be the seller, so a buyer cannot point the order at an unrelated lock.
    let (creator, _lock_id) = parse_lock_resource(&payload.pubky_lock_resource)
        .expect("lock resource format validated by the command contract");
    if creator != payment.seller_pubky {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The lock resource creator must be the order's seller.",
        )));
    }

    // The payment lock point: registering the correlation is the payment
    // start, so it acquires the order's inventory hold and arms the payment
    // window — the correlation window IS the hold window (one window
    // concept, not two).
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };
    if let Err(failure) =
        holds::acquire_payment_hold(tx, order, payment_window_seconds, now).await?
    {
        return Ok(Err(failure));
    }

    let correlation_id = Uuid::new_v4();
    let bundle_id_ciphertext = locks.keys.encrypt_bundle_id(payment.id, &payload.bundle_id);
    let bundle_lookup_token = locks.keys.lookup_token(creator, &payload.bundle_id);
    let lock_resource_hash = blake3::hash(payload.pubky_lock_resource.as_bytes())
        .to_hex()
        .to_string();
    let window_expires_at = now + chrono::Duration::seconds(payment_window_seconds);

    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, \
         creator_pubky, lock_resource_hash, amount_minor, asset, exponent, policy_version, \
         bundle_id_ciphertext, bundle_lookup_token, verification_state, window_expires_at, \
         created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
         (SELECT guarantee_policy_version FROM orders WHERE id = $3), \
         $10, $11, 'pending', $12, $13, $13)",
    )
    .bind(correlation_id)
    .bind(payment.id)
    .bind(payment.order_id)
    .bind(&payment.buyer_pubky)
    .bind(&payment.seller_pubky)
    .bind(&lock_resource_hash)
    .bind(payment.amount_minor)
    .bind(&payment.currency)
    .bind(payment.exponent)
    .bind(&bundle_id_ciphertext)
    .bind(&bundle_lookup_token)
    .bind(window_expires_at)
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
