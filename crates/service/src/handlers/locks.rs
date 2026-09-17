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
    .bind(payment.currency)
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
    let lock_pairs: std::collections::BTreeSet<(String, String)> = order
        .lines
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|line| {
            Some((
                line.get("digital_lock_policy_uri")?.as_str()?.to_owned(),
                line.get("digital_lock_criterion_id")?.as_str()?.to_owned(),
            ))
        })
        .collect();
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
    if !crate::locks::validate_content_lock_identity(&content, &resource)
        || !content_lock_matches(
            &content,
            creator,
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

fn content_lock_matches(
    content: &serde_json::Value,
    creator: &str,
    criterion_id: &str,
    amount_minor: i64,
    asset: &str,
) -> bool {
    let Some(object) = content.as_object() else {
        return false;
    };
    let Some(criteria) = object.get("criteria").and_then(serde_json::Value::as_array) else {
        return false;
    };
    let Some(criterion) = (criteria.len() == 1)
        .then_some(criteria[0].as_object())
        .flatten()
    else {
        return false;
    };
    criterion
        .get("criterion_id")
        .and_then(serde_json::Value::as_str)
        == Some(criterion_id)
        && criterion
            .get("verifier_type")
            .and_then(serde_json::Value::as_str)
            == Some("paykit-payment")
        && criterion
            .get("params")
            .and_then(|params| params.get("recipient_pubky"))
            .and_then(serde_json::Value::as_str)
            == Some(creator)
        && criterion
            .get("params")
            .and_then(|params| params.get("asset"))
            .and_then(serde_json::Value::as_str)
            == Some(asset)
        && criterion
            .get("params")
            .and_then(|params| params.get("amount"))
            .and_then(serde_json::Value::as_str)
            == Some(amount_minor.to_string().as_str())
}
