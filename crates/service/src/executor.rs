//! Command envelope execution per ADR-0019 §3.
//!
//! Exactly one accepted result exists per actor + command id. An exact
//! replay returns the stored result without re-executing; a replay with a
//! different canonical payload is a conflict. Failures are never stored, so
//! a retried command that previously failed re-executes (matching the
//! TypeScript prototype engine).

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use marketplace_domain::commands::{parse_command, validate_actor, Command, CommandPayload};
use marketplace_domain::ErrorCode;
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::time::Instant;
use uuid::Uuid;

use crate::logging::{log_command, log_invalid_command};
use crate::model::redact_command_result;
use crate::refusal_audit::{CommandKind, RefusalKind, SurfaceKind};
use crate::result::{success_body, CommandFailure, HandlerResult};
use crate::AppState;

pub async fn execute(
    state: &AppState,
    actor: &str,
    raw: &Value,
) -> Result<(StatusCode, Value), sqlx::Error> {
    let started = Instant::now();
    if let Err(issues) = validate_actor(actor) {
        log_invalid_command(None, started.elapsed().as_millis() as u64);
        return Ok(failure_response(&CommandFailure::invalid_envelope(issues)));
    }
    let command = match parse_command(raw) {
        Ok(command) => command,
        Err(issues) => {
            log_invalid_command(Some(actor), started.elapsed().as_millis() as u64);
            let failure = CommandFailure::invalid_envelope(issues);
            let response = failure_response(&failure);
            enqueue_refusal(
                state,
                actor,
                state.clock.now(),
                SurfaceKind::V1Command,
                CommandKind::InvalidEnvelope,
                RefusalKind::InvalidEnvelope,
                None,
            );
            return Ok(response);
        }
    };
    let request_hash = command.request_hash();

    let mut tx = state.pool.begin().await?;

    // Serialize concurrent submissions of the same actor + command id so a
    // duplicate cannot execute twice before either result is stored.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 42))")
        .bind(format!("{actor}:{}", command.command_id))
        .execute(&mut *tx)
        .await?;

    let stored: Option<(String, Value)> = sqlx::query_as(
        "SELECT request_hash, result FROM command_results \
         WHERE actor_pubky = $1 AND command_id = $2",
    )
    .bind(actor)
    .bind(command.command_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((stored_hash, stored_result)) = stored {
        tx.commit().await?;
        if stored_hash == request_hash {
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "idempotent_replay",
                None,
                None,
                stored_result["revision"].as_i64().unwrap_or_default(),
                started.elapsed().as_millis() as u64,
            );
            if command.kind() == "checkout.create" {
                let Some(stored_result) = redact_command_result(stored_result) else {
                    let failure = CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::InvariantViolation,
                        ErrorCode::InvariantViolation,
                        "The stored command result could not be processed.",
                    );
                    let response = failure_response(&failure);
                    enqueue_refusal(
                        state,
                        actor,
                        state.clock.now(),
                        SurfaceKind::V1Command,
                        command_kind(&command.payload),
                        failure.refusal_kind(),
                        Some(command.command_id),
                    );
                    return Ok(response);
                };
                return Ok((StatusCode::OK, stored_result));
            }
            if command.kind() == "payment.prepare_locks" {
                // The stored result is sealed to this payment and buyer; it
                // is opened only for the same authenticated actor (the
                // command_results lookup above is actor-scoped).
                let Some(locks) = state.locks.as_deref() else {
                    let failure = CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::InvariantViolation,
                        ErrorCode::InvariantViolation,
                        "The stored command result could not be processed.",
                    );
                    let response = failure_response(&failure);
                    enqueue_refusal(
                        state,
                        actor,
                        state.clock.now(),
                        SurfaceKind::V1Command,
                        command_kind(&command.payload),
                        failure.refusal_kind(),
                        Some(command.command_id),
                    );
                    return Ok(response);
                };
                let Some(stored_result) =
                    unseal_prepare_locks_result(locks, &command, actor, &stored_result)
                else {
                    let failure = CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::InvariantViolation,
                        ErrorCode::InvariantViolation,
                        "The stored command result could not be processed.",
                    );
                    let response = failure_response(&failure);
                    enqueue_refusal(
                        state,
                        actor,
                        state.clock.now(),
                        SurfaceKind::V1Command,
                        command_kind(&command.payload),
                        failure.refusal_kind(),
                        Some(command.command_id),
                    );
                    return Ok(response);
                };
                return Ok((StatusCode::OK, stored_result));
            }
            return Ok((StatusCode::OK, stored_result));
        }
        let failure = CommandFailure::refused(
            crate::refusal_audit::RefusalKind::IdempotencyConflict,
            ErrorCode::IdempotencyConflict,
            "The command id was already used with different input.",
        );
        log_command(
            actor,
            command.kind(),
            &command.command_id.to_string(),
            &command.aggregate_id,
            "conflict",
            Some(failure.code()),
            Some(failure.message()),
            command.expected_revision,
            started.elapsed().as_millis() as u64,
        );
        let response = failure_response(&failure);
        enqueue_refusal(
            state,
            actor,
            state.clock.now(),
            SurfaceKind::V1Command,
            command_kind(&command.payload),
            failure.refusal_kind(),
            Some(command.command_id),
        );
        return Ok(response);
    }

    let now = state.clock.now();
    let outcome = dispatch(state, &mut tx, actor, &command, now).await;
    match outcome {
        Ok(Ok(success)) => {
            let body = success_body(&command, &success);
            // The minted Locks client reference exists in plaintext only in
            // the authenticated response: the durable copy of a prepare
            // result is sealed to the payment and buyer (ADR-0019 §8 —
            // persist only ciphertext).
            let stored_body = if command.kind() == "payment.prepare_locks" {
                let protected = state
                    .locks
                    .as_deref()
                    .and_then(|locks| seal_prepare_locks_result(locks, &command, actor, &body));
                let Some(stored_body) = protected else {
                    let failure = CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::InvariantViolation,
                        ErrorCode::InvariantViolation,
                        "The command result could not be protected.",
                    );
                    let response = failure_response(&failure);
                    let descriptor = state.refusal_audit.as_ref().and_then(|audit| {
                        audit
                            .envelope(
                                now,
                                SurfaceKind::V1Command,
                                command_kind(&command.payload),
                                failure.refusal_kind(),
                                actor,
                                Some(command.command_id),
                            )
                            .ok()
                    });
                    tx.rollback().await?;
                    if let (Some(audit), Some(descriptor)) = (&state.refusal_audit, descriptor) {
                        audit.try_send(descriptor);
                    }
                    return Ok(response);
                };
                stored_body
            } else {
                body.clone()
            };
            sqlx::query(
                "INSERT INTO command_results (actor_pubky, command_id, request_hash, result, created_at) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(actor)
            .bind(command.command_id)
            .bind(&request_hash)
            .bind(&stored_body)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "accepted",
                None,
                None,
                success.revision,
                started.elapsed().as_millis() as u64,
            );
            Ok((StatusCode::OK, body))
        }
        Ok(Err(failure)) => {
            let response = failure_response(&failure);
            let descriptor = state.refusal_audit.as_ref().and_then(|audit| {
                audit
                    .envelope(
                        now,
                        SurfaceKind::V1Command,
                        command_kind(&command.payload),
                        failure.refusal_kind(),
                        actor,
                        Some(command.command_id),
                    )
                    .ok()
            });
            tx.rollback().await?;
            if let (Some(audit), Some(descriptor)) = (&state.refusal_audit, descriptor) {
                audit.try_send(descriptor);
            }
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                failure
                    .current_revision()
                    .unwrap_or(command.expected_revision),
                started.elapsed().as_millis() as u64,
            );
            Ok(response)
        }
        Err(error) => {
            tx.rollback().await?;
            if is_unique_violation(&error) {
                let failure = CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "A uniqueness constraint rejected the command.",
                );
                log_command(
                    actor,
                    command.kind(),
                    &command.command_id.to_string(),
                    &command.aggregate_id,
                    "conflict",
                    Some(failure.code()),
                    Some(failure.message()),
                    command.expected_revision,
                    started.elapsed().as_millis() as u64,
                );
                let response = failure_response(&failure);
                enqueue_refusal(
                    state,
                    actor,
                    now,
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return Ok(response);
            }
            Err(error)
        }
    }
}

fn enqueue_refusal(
    state: &AppState,
    actor: &str,
    now: DateTime<Utc>,
    surface: SurfaceKind,
    command_kind: CommandKind,
    refusal_kind: RefusalKind,
    command_id: Option<Uuid>,
) {
    if let Some(audit) = &state.refusal_audit {
        if let Ok(descriptor) =
            audit.envelope(now, surface, command_kind, refusal_kind, actor, command_id)
        {
            audit.try_send(descriptor);
        }
    }
}

fn command_kind(payload: &CommandPayload) -> CommandKind {
    match payload {
        CommandPayload::RegisterListing(_) => CommandKind::RegisterListing,
        CommandPayload::SyncListing(_) => CommandKind::SyncListing,
        CommandPayload::SyncDrop(_) => CommandKind::SyncDrop,
        CommandPayload::CancelDrop(_) => CommandKind::CancelDrop,
        CommandPayload::ReleaseDropListings(_) => CommandKind::ReleaseDropListings,
        CommandPayload::ReserveInventory(_) => CommandKind::ReserveInventory,
        CommandPayload::CreateCheckout(_) => CommandKind::CreateCheckout,
        CommandPayload::CreateOffer(_) => CommandKind::CreateOffer,
        CommandPayload::CounterOffer(_) => CommandKind::CounterOffer,
        CommandPayload::AcceptOffer(_) => CommandKind::AcceptOffer,
        CommandPayload::OfferCheckout(_) => CommandKind::OfferCheckout,
        CommandPayload::RejectOffer(_) => CommandKind::RejectOffer,
        CommandPayload::WithdrawOffer(_) => CommandKind::WithdrawOffer,
        CommandPayload::PlaceBid(_) => CommandKind::PlaceBid,
        CommandPayload::CloseAuction(_) => CommandKind::CloseAuction,
        CommandPayload::AdvanceSandboxPayment(_) => CommandKind::AdvanceSandboxPayment,
        CommandPayload::PrepareLocks(_) => CommandKind::PrepareLocks,
        CommandPayload::RegisterLocks(_) => CommandKind::RegisterLocks,
        CommandPayload::RequestCancellation(_) => CommandKind::RequestCancellation,
        CommandPayload::ApproveCancellation(_) => CommandKind::ApproveCancellation,
        CommandPayload::ShipOrder(_) => CommandKind::ShipOrder,
        CommandPayload::ConfirmDelivery(_) => CommandKind::ConfirmDelivery,
        CommandPayload::SetPickupDetails(_) => CommandKind::SetPickupDetails,
        CommandPayload::ClearPickupDetails(_) => CommandKind::ClearPickupDetails,
        CommandPayload::MarkReadyForPickup(_) => CommandKind::MarkReadyForPickup,
        CommandPayload::ConfirmPickup(_) => CommandKind::ConfirmPickup,
        CommandPayload::RequestReturn(_) => CommandKind::RequestReturn,
        CommandPayload::ApproveReturn(_) => CommandKind::ApproveReturn,
        CommandPayload::ReceiveReturn(_) => CommandKind::ReceiveReturn,
        CommandPayload::RecordExternalRefund(_) => CommandKind::RecordExternalRefund,
        CommandPayload::CreateReview(_) => CommandKind::CreateReview,
        CommandPayload::UpdateReview(_) => CommandKind::UpdateReview,
        CommandPayload::SetBandConsent(_) => CommandKind::SetBandConsent,
    }
}

async fn dispatch(
    state: &AppState,
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    match &command.payload {
        CommandPayload::RegisterListing(payload) => {
            crate::handlers::register_listing::handle(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::SyncListing(payload) => {
            crate::handlers::sync_listing::handle(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::SyncDrop(payload) => {
            crate::handlers::drops::sync(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::CancelDrop(payload) => {
            crate::handlers::drops::cancel(tx, actor, command, payload, now).await
        }
        CommandPayload::ReleaseDropListings(payload) => {
            crate::handlers::drops::release_listings(tx, actor, command, payload, now).await
        }
        CommandPayload::ReserveInventory(payload) => {
            crate::handlers::reserve_inventory::handle(tx, actor, command, payload, now).await
        }
        CommandPayload::CreateCheckout(payload) => {
            crate::handlers::checkout::handle(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref().map(|runtime| &runtime.keys),
                state.config.drop_claim_window_seconds,
                now,
            )
            .await
        }
        CommandPayload::CreateOffer(payload) => {
            crate::handlers::offers::create(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::CounterOffer(payload) => {
            crate::handlers::offers::counter(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::AcceptOffer(payload) => {
            crate::handlers::offers::accept(
                tx,
                actor,
                command,
                payload,
                state.homeserver.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::OfferCheckout(payload) => {
            let hold_window_seconds = state
                .config
                .locks_payment_window_seconds
                .max(state.config.fiat_payment_window_seconds)
                .max(state.config.sandbox_payment_window_seconds);
            crate::handlers::offer_checkout::handle(
                tx,
                actor,
                command,
                payload,
                state.clock.as_ref(),
                hold_window_seconds,
            )
            .await
        }
        CommandPayload::RejectOffer(payload) => {
            crate::handlers::offers::reject(tx, actor, command, payload, now).await
        }
        CommandPayload::WithdrawOffer(payload) => {
            crate::handlers::offers::withdraw(tx, actor, command, payload, now).await
        }
        CommandPayload::PlaceBid(payload) => {
            crate::handlers::auction::place_bid(tx, actor, command, payload, now).await
        }
        CommandPayload::CloseAuction(payload) => {
            crate::handlers::auction::close(tx, actor, command, payload, now).await
        }
        CommandPayload::AdvanceSandboxPayment(payload) => {
            // Deployment boundary, not client courtesy: on a durable
            // deployment the buyer must never be able to drive a payment to
            // `paid` by command (ADR-0019 §7).
            if !state.config.sandbox_payments_enabled {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidCommand,
                    ErrorCode::InvalidCommand,
                    "Sandbox payment commands are disabled on this deployment.",
                )));
            }
            crate::handlers::payment::advance(
                tx,
                actor,
                command,
                payload,
                state.config.sandbox_payment_window_seconds,
                state.pickup.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::RegisterLocks(payload) => {
            crate::handlers::locks::register(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::PrepareLocks(payload) => {
            crate::handlers::locks::prepare(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref(),
                state.homeserver.as_deref(),
                state.config.locks_payment_window_seconds,
                now,
            )
            .await
        }
        CommandPayload::RequestCancellation(payload) => {
            crate::handlers::cancellation::request(tx, actor, command, payload, now).await
        }
        CommandPayload::ApproveCancellation(payload) => {
            crate::handlers::cancellation::approve(tx, actor, command, payload, now).await
        }
        CommandPayload::ShipOrder(payload) => {
            crate::handlers::fulfillment::ship(tx, actor, command, payload, now).await
        }
        CommandPayload::ConfirmDelivery(payload) => {
            crate::handlers::fulfillment::confirm_delivery(tx, actor, command, payload, now).await
        }
        CommandPayload::SetPickupDetails(payload) => {
            crate::handlers::pickup::set(
                tx,
                actor,
                command,
                payload,
                state.pickup.as_deref(),
                state.config.sandbox_payments_enabled,
                now,
            )
            .await
        }
        CommandPayload::ClearPickupDetails(payload) => {
            crate::handlers::pickup::clear(
                tx,
                actor,
                command,
                payload,
                state.pickup.as_deref(),
                state.config.sandbox_payments_enabled,
                now,
            )
            .await
        }
        CommandPayload::MarkReadyForPickup(payload) => {
            crate::handlers::fulfillment::mark_ready(tx, actor, command, payload, now).await
        }
        CommandPayload::ConfirmPickup(payload) => {
            crate::handlers::fulfillment::confirm_pickup(tx, actor, command, payload, now).await
        }
        CommandPayload::RequestReturn(payload) => {
            crate::handlers::returns::request(tx, actor, command, payload, now).await
        }
        CommandPayload::ApproveReturn(payload) => {
            crate::handlers::returns::approve(tx, actor, command, payload, now).await
        }
        CommandPayload::ReceiveReturn(payload) => {
            crate::handlers::returns::receive(tx, actor, command, payload, now).await
        }
        CommandPayload::RecordExternalRefund(payload) => {
            crate::handlers::returns::record_external_refund(
                tx,
                actor,
                command,
                payload,
                state.attestor.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::CreateReview(payload) => {
            crate::handlers::reviews::create(
                tx,
                actor,
                command,
                payload,
                state.attestor.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::UpdateReview(payload) => {
            crate::handlers::reviews::update(tx, actor, command, payload, now).await
        }
        CommandPayload::SetBandConsent(payload) => {
            crate::handlers::attestation::set_band_consent(tx, actor, command, payload, now).await
        }
    }
}

fn failure_response(failure: &CommandFailure) -> (StatusCode, Value) {
    let status = StatusCode::from_u16(failure.http_status())
        .expect("error codes map to valid HTTP status codes");
    (status, failure.body())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505")
    )
}

/// The durable form of a `payment.prepare_locks` result: the plaintext
/// `client_reference` is replaced by its payment-and-buyer-bound seal, so
/// the stored row holds ciphertext only.
fn seal_prepare_locks_result(
    locks: &crate::locks::LocksRuntime,
    command: &Command,
    actor: &str,
    body: &Value,
) -> Option<Value> {
    let CommandPayload::PrepareLocks(payload) = &command.payload else {
        return None;
    };
    let reference = body.get("result")?.get("client_reference")?.as_str()?;
    let sealed = locks
        .keys
        .seal_result_client_reference(payload.payment_id, actor, reference);
    let mut stored = body.clone();
    let result = stored.get_mut("result")?.as_object_mut()?;
    result.remove("client_reference");
    result.insert(
        "client_reference_sealed".to_string(),
        Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            sealed,
        )),
    );
    Some(stored)
}

/// Reverses [`seal_prepare_locks_result`] for the same authenticated buyer
/// replaying their own command; any tamper, transplant, or key mismatch
/// fails closed.
fn unseal_prepare_locks_result(
    locks: &crate::locks::LocksRuntime,
    command: &Command,
    actor: &str,
    stored: &Value,
) -> Option<Value> {
    let CommandPayload::PrepareLocks(payload) = &command.payload else {
        return None;
    };
    let sealed = stored
        .get("result")?
        .get("client_reference_sealed")?
        .as_str()?;
    let sealed = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, sealed).ok()?;
    let reference = locks
        .keys
        .open_result_client_reference(payload.payment_id, actor, &sealed)?;
    let mut body = stored.clone();
    let result = body.get_mut("result")?.as_object_mut()?;
    result.remove("client_reference_sealed");
    result.insert("client_reference".to_string(), Value::String(reference));
    Some(body)
}

/// Appends one immutable domain event and returns its id.
pub async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    command_id: Uuid,
    aggregate_id: &str,
    revision: i64,
    actor: &str,
    kind: &str,
    occurred_at: DateTime<Utc>,
) -> Result<Uuid, sqlx::Error> {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(event_id)
    .bind(command_id)
    .bind(aggregate_id)
    .bind(revision)
    .bind(actor)
    .bind(kind)
    .bind(occurred_at)
    .execute(&mut **tx)
    .await?;
    Ok(event_id)
}
