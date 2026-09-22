//! `payment.prepare_locks` / `payment.register_locks`: the two-step Locks
//! lifecycle binding for a payment.
//!
//! PREPARATION is the authority-creation step (DESIGN §3.2): under the
//! payment and order locks it loads the immutable checkout-time lock
//! snapshot (never the mutable listing rows) — the SOLE AUTHORITY and the
//! ONLY SOURCE — compares EVERY snapshotted fact (resource hash,
//! criterion, amount, asset, exponent, reader, recipient, order) to the
//! locked payment/order rows and refuses statically on any mismatch
//! (including a payment already bound to a non-Locks rail), validates the
//! fetched content lock through the strict
//! typed upstream mirror and the snapshotted payment economics, mints and seals the
//! server-originated `client_reference`, inserts the sole `prepared`
//! correlation populated FROM THE VERIFIED SNAPSHOT (sealed expected
//! resource, hash, criterion, reader,
//! recipient, amount, asset, exponent, policy version), acquires the
//! inventory hold, and pins the payment adapter to `locks` — atomically.
//! The minted reference exists in plaintext only in the authenticated
//! buyer's response; its durable command-result copy is sealed to the
//! payment and buyer.
//!
//! REGISTRATION only attaches the buyer-generated bundle id to that
//! prepared row: it accepts no resource, criterion, economics, or reference
//! from the client, requires receiver-clock eligibility (the prepared
//! window open, the order still holding it), and seals the bundle with the
//! payment-bound key. Neither step advances the payment: only the
//! background worker's independent verification of a completed Locks
//! result does that (ADR-0019 §7 — the service must not forge completion
//! from client input).
//!
//! Replay discipline:
//! - an exact replay of either command returns the stored result
//!   (executor); a prepare replay unseals the buyer-scoped reference;
//! - a changed replay under the same command id is an idempotency conflict
//!   (executor);
//! - a repeated prepare recovers the same live reference for the same
//!   buyer, is `UNAUTHORIZED` for any other actor, is a stable
//!   `INVALID_STATE` after registration or expiry, and a concurrent double
//!   prepare produces exactly one row, one hold, and one reference;
//! - a second registration for the same payment or order is refused
//!   (`INVALID_STATE`, backstopped by the table's UNIQUE constraints);
//! - the same `{creator, bundle_id}` identity can never correlate a second
//!   order — the HMAC lookup token is UNIQUE.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    canonical_lock_resource, PrepareLocksPayload, RegisterLocksPayload, LOCKS_CONTENT_LOCK_PREFIX,
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

/// The designed binding outcomes (migrations 0028/0031 enforce this
/// vocabulary): only the two success outcomes exist. Each is stamped on
/// the correlation's `binding_outcome` column and appended to the historical
/// outcome table in the transaction that performs the state change. Refusals
/// are never written to that table: a refusing command rolls back whole,
/// exactly like any other rejected command (migration 0031 removed its
/// refusal vocabulary and commit-on-refusal path). The separate refusal-audit
/// buckets record only a bounded, server-derived descriptor after rollback.
const OUTCOME_PREPARED: &str = "prepared";
const OUTCOME_REGISTERED: &str = "registered";

/// Appends one success outcome row inside the command transaction, so it
/// commits with the state change. Refusals are never written to this table.
async fn record_binding_outcome<'e, E>(
    executor: E,
    command_id: Uuid,
    payment_id: Uuid,
    outcome: &'static str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    sqlx::query(
        "INSERT INTO payment_locks_binding_outcomes (id, payment_id, command_id, outcome, recorded_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(payment_id)
    .bind(command_id)
    .bind(outcome)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

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
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
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
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The payment was not found.",
        )));
    };
    if payment.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::LocksIdentityMismatch,
            ErrorCode::Unauthorized,
            "Only the buyer may register the Locks correlation.",
        )));
    }
    if command.aggregate_id != ids::payment_aggregate_id(payment.id) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The payment aggregate id is invalid.",
        )));
    }
    if command.expected_revision != payment.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The payment revision is stale.",
            payment.revision,
        )));
    }
    if payment.state != "awaiting_entitlement" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
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
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Prepare the seller-authorized Locks payment before registering a bundle.",
        )));
    };
    if preparation_state != "prepared" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The payment already has a registered Locks bundle.",
        )));
    }
    // Receiver-clock eligibility, under the payment/order locks: attachment
    // is allowed only while the prepared window is still open and the order
    // still holds exactly that window. Without this an elapsed preparation
    // could attach (and later confirm) before the expiry sweep runs.
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };
    if order.state != "pending_payment"
        || !order.stock_held
        || order.hold_expires_at != Some(window_expires_at)
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The order no longer holds the prepared Locks payment window.",
        )));
    }
    if now >= window_expires_at {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The Locks preparation has expired.",
        )));
    }
    let bundle_id_ciphertext = locks.keys.encrypt_bundle_id(payment.id, &payload.bundle_id);
    let bundle_lookup_token = locks.keys.lookup_token(&creator, &payload.bundle_id);

    sqlx::query(
        "UPDATE payment_locks_correlations SET bundle_id_ciphertext = $2, \
         bundle_lookup_token = $3, preparation_state = 'registered', verification_state = 'pending', \
         binding_outcome = 'registered', updated_at = $4 WHERE id = $1",
    )
    .bind(correlation_id)
    .bind(&bundle_id_ciphertext)
    .bind(&bundle_lookup_token)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    record_binding_outcome(
        &mut **tx,
        command.command_id,
        payment.id,
        OUTCOME_REGISTERED,
        now,
    )
    .await?;

    // The adapter was pinned to 'locks' atomically with the preparation;
    // restating it here keeps attachment self-contained while the revision
    // bump serializes this transition against any concurrent command.
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

/// The immutable checkout-time Locks authority snapshot (migration 0029),
/// loaded whole: prepare compares every field to the locked payment/order
/// rows before any authority row is created, then populates the
/// correlation from these verified values only.
#[derive(sqlx::FromRow)]
struct CheckoutSnapshot {
    expected_resource_ciphertext: Vec<u8>,
    expected_resource_hash: String,
    criterion_id: String,
    amount_minor: i64,
    asset: String,
    exponent: i32,
    expected_reader_pubky: String,
    expected_recipient_pubky: String,
    order_id: Uuid,
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
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
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
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The payment was not found.",
        )));
    };
    if payment.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::LocksIdentityMismatch,
            ErrorCode::Unauthorized,
            "Only the buyer may prepare the Locks payment.",
        )));
    }
    if command.aggregate_id != ids::payment_aggregate_id(payment.id) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The payment aggregate id is invalid.",
        )));
    }
    // The prepared-row replay is checked BEFORE the revision gate: a second
    // prepare — including the loser of a concurrent double prepare, which
    // observes the winner's revision bump — recovers the same live
    // reference rather than conflicting (DESIGN §3.2). An elapsed
    // preparation is never replayed.
    let existing: Option<(String, Option<Vec<u8>>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT preparation_state, client_reference_ciphertext, window_expires_at \
         FROM payment_locks_correlations WHERE payment_id = $1 FOR UPDATE",
    )
    .bind(payment.id)
    .fetch_optional(&mut **tx)
    .await?;
    match existing.as_ref() {
        Some((state, Some(sealed_reference), expires_at)) if state == "prepared" => {
            if now >= *expires_at {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidState,
                    ErrorCode::InvalidState,
                    "The Locks preparation has expired.",
                )));
            }
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
            // A prepared row whose sealed reference cannot be opened fails
            // closed (tampering or a key rotation) rather than minting a
            // second preparation.
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The payment already has a Locks preparation.",
            )));
        }
        // Any other existing row (notably `registered`) is a stable
        // INVALID_STATE — never a fall-through to a uniqueness conflict.
        Some(_) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The payment already has a Locks preparation.",
            )));
        }
        None => {}
    }
    if command.expected_revision != payment.revision {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The payment revision is stale.",
            payment.revision,
        )));
    }
    if payment.state != "awaiting_entitlement" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only a payment awaiting entitlement can prepare Locks.",
        )));
    }
    let Some(order) = fetch_order_for_update(tx, payment.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "Payment order is missing.",
        )));
    };
    if order.state != "pending_payment" {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Only a pending order can prepare Locks.",
        )));
    }
    // The seller-authoritative lock is the IMMUTABLE checkout-time snapshot
    // (DESIGN §§3.1–3.2): prepare reads only this private per-payment row,
    // never the mutable listing rows — a post-checkout lock change
    // (including equal-revision sync healing) cannot move an existing
    // order's authority. A payment without a snapshot (legacy orders, or
    // zero/multiple distinct locks at checkout — multi-lock aggregation is
    // a future design) is refused statically.
    //
    // The snapshot is the SOLE AUTHORITY and the ONLY SOURCE: every field
    // is loaded and compared to the locked payment/order rows BEFORE any
    // authority row is created, and the correlation is populated from the
    // verified snapshot values, never from the mutable current payment —
    // whose economics a non-Locks rail bind rewrites (SAT/0).
    let snapshot: Option<CheckoutSnapshot> = sqlx::query_as(
        "SELECT expected_resource_ciphertext, expected_resource_hash, criterion_id, \
         amount_minor, asset, exponent, expected_reader_pubky, expected_recipient_pubky, \
         order_id FROM payment_locks_checkout_snapshots WHERE payment_id = $1",
    )
    .bind(payment.id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(snapshot) = snapshot else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The order has no seller-authored Locks payment lock snapshot.",
        )));
    };
    // A payment whose method is already bound to a non-Locks rail can
    // never prepare Locks: the bind (payment_methods.rs — bitcoin
    // settlement rewrites the payment to SAT/0) priced another rail, not
    // the snapshotted merchandise terms.
    if order.payment_method.is_some() {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The payment is already bound to a non-Locks payment rail.",
        )));
    }
    // Every snapshotted fact must equal the locked payment/order rows
    // exactly — order, reader (buyer), recipient (seller), and the full
    // economics — before any authority row is created.
    if snapshot.order_id != payment.order_id
        || snapshot.expected_reader_pubky != payment.buyer_pubky
        || snapshot.expected_recipient_pubky != payment.seller_pubky
        || snapshot.amount_minor != payment.amount_minor
        || snapshot.asset != payment.currency
        || snapshot.exponent != payment.exponent
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The checkout Locks snapshot does not match the payment.",
        )));
    }
    let Some(resource) = locks.keys.open_prepared_value(
        payment.id,
        b"locks-checkout-resource:",
        &snapshot.expected_resource_ciphertext,
    ) else {
        // A snapshot that does not authenticate under the configured key
        // fails closed (tampering or a key rotation) rather than falling
        // back to the mutable listing rows.
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "The checkout Locks snapshot could not be opened.",
        )));
    };
    // The opened resource must be exactly the resource the snapshot
    // sealed at checkout.
    let resource_hash = blake3::hash(resource.as_bytes()).to_hex().to_string();
    if resource_hash != snapshot.expected_resource_hash {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The checkout Locks snapshot does not match the payment.",
        )));
    }
    let Some(canonical_resource) = canonical_lock_resource(&resource) else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's Locks resource is invalid.",
        )));
    };
    let Some((creator, content_path)) = canonical_resource.split_once(LOCKS_CONTENT_LOCK_PREFIX)
    else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's Locks resource is invalid.",
        )));
    };
    if creator != payment.seller_pubky {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's Locks resource creator does not match the order.",
        )));
    }
    let content = match homeserver
        .fetch_content_lock(
            creator,
            &format!("{LOCKS_CONTENT_LOCK_PREFIX}{content_path}"),
        )
        .await
    {
        HomeserverFetchOutcome::Found(value) => value,
        HomeserverFetchOutcome::NotFound => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's Locks document is unavailable.",
            )));
        }
        HomeserverFetchOutcome::Unavailable => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::LocksUpstreamUnavailable,
                ErrorCode::UpstreamUnavailable,
                "The seller's Locks document could not be reached.",
            )));
        }
    };
    let content_lock = match crate::content_lock::validate_content_lock_value(&content, &resource) {
        Ok(content_lock) => content_lock,
        Err(_) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's Locks document does not match the payment.",
            )));
        }
    };
    if content_lock.validate_paykit_payment_v1_policy().is_err() {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's Locks document does not match the payment.",
        )));
    }
    if !criterion_matches_payment(
        &content_lock,
        &snapshot.criterion_id,
        snapshot.amount_minor,
        &snapshot.asset,
    ) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's Locks document does not match the payment.",
        )));
    }
    if let Err(failure) = holds::acquire_payment_hold(
        tx,
        order,
        payment_window_seconds,
        holds::HOLD_SOURCE_LOCKS,
        now,
    )
    .await?
    {
        return Ok(Err(failure));
    }
    let client_reference = LocksKeys::mint_client_reference();
    let correlation_id = Uuid::new_v4();
    let window_expires_at = now + chrono::Duration::seconds(payment_window_seconds);
    // The authority row is populated from the VERIFIED SNAPSHOT values
    // only — order, parties, economics, and the resource hash — never
    // from the mutable current payment (each was compared equal above;
    // the snapshot is the sole source).
    sqlx::query(
        "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, creator_pubky, \
         lock_resource_hash, amount_minor, asset, exponent, policy_version, verification_state, window_expires_at, \
         expected_resource_ciphertext, expected_resource_hash, criterion_id, expected_reader_pubky, expected_recipient_pubky, \
         client_reference_ciphertext, preparation_state, binding_outcome, created_at, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,(SELECT guarantee_policy_version FROM orders WHERE id=$3),'pending',$10,$11,$6,$12,$4,$5,$13,'prepared','prepared',$14,$14)",
    )
    .bind(correlation_id).bind(payment.id).bind(snapshot.order_id).bind(&snapshot.expected_reader_pubky)
    .bind(&snapshot.expected_recipient_pubky).bind(&snapshot.expected_resource_hash).bind(snapshot.amount_minor).bind(&snapshot.asset)
    .bind(snapshot.exponent).bind(window_expires_at)
    .bind(locks.keys.encrypt_prepared_value(payment.id, b"locks-resource:", &resource))
    .bind(&snapshot.criterion_id)
    .bind(locks.keys.encrypt_prepared_value(payment.id, b"locks-client-reference:", &client_reference))
    .bind(now).execute(&mut **tx).await?;
    record_binding_outcome(
        &mut **tx,
        command.command_id,
        payment.id,
        OUTCOME_PREPARED,
        now,
    )
    .await?;
    // The hold, the prepared row, and the adapter switch commit atomically
    // (DESIGN §3.2): from here only server-side verification can advance
    // this payment — the sandbox path is already closed, not only once a
    // bundle attaches.
    let updated_payment: PaymentRow = sqlx::query_as(&format!(
        "UPDATE payments SET revision = revision + 1, adapter = 'locks', updated_at = $2 \
         WHERE id = $1 RETURNING {PAYMENT_COLUMNS}"
    ))
    .bind(payment.id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Ok(HandlerSuccess {
        revision: updated_payment.revision,
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
