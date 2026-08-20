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
use uuid::Uuid;

use crate::result::{success_body, CommandFailure, HandlerResult};
use crate::AppState;

pub async fn execute(
    state: &AppState,
    actor: &str,
    raw: &Value,
) -> Result<(StatusCode, Value), sqlx::Error> {
    if let Err(issues) = validate_actor(actor) {
        return Ok(failure_response(&CommandFailure::invalid_command(issues)));
    }
    let command = match parse_command(raw) {
        Ok(command) => command,
        Err(issues) => {
            return Ok(failure_response(&CommandFailure::invalid_command(issues)));
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
            tracing::info!(
                command_id = %command.command_id,
                kind = command.kind(),
                "returning stored idempotent result"
            );
            return Ok((StatusCode::OK, stored_result));
        }
        return Ok(failure_response(&CommandFailure::new(
            ErrorCode::IdempotencyConflict,
            "The command id was already used with different input.",
        )));
    }

    let now = state.clock.now();
    let outcome = dispatch(&mut tx, actor, &command, now).await;
    match outcome {
        Ok(Ok(success)) => {
            let body = success_body(&command, &success);
            sqlx::query(
                "INSERT INTO command_results (actor_pubky, command_id, request_hash, result, created_at) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(actor)
            .bind(command.command_id)
            .bind(&request_hash)
            .bind(&body)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            tracing::info!(
                command_id = %command.command_id,
                aggregate_id = %command.aggregate_id,
                kind = command.kind(),
                revision = success.revision,
                "command accepted"
            );
            Ok((StatusCode::OK, body))
        }
        Ok(Err(failure)) => {
            tx.rollback().await?;
            tracing::info!(
                command_id = %command.command_id,
                aggregate_id = %command.aggregate_id,
                kind = command.kind(),
                code = ?failure.code,
                "command rejected"
            );
            Ok(failure_response(&failure))
        }
        Err(error) => {
            tx.rollback().await?;
            if is_unique_violation(&error) {
                tracing::warn!(
                    command_id = %command.command_id,
                    kind = command.kind(),
                    "command rejected by database uniqueness constraint"
                );
                return Ok(failure_response(&CommandFailure::new(
                    ErrorCode::InvariantViolation,
                    "A uniqueness constraint rejected the command.",
                )));
            }
            Err(error)
        }
    }
}

async fn dispatch(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    match &command.payload {
        CommandPayload::RegisterListing(payload) => {
            crate::handlers::register_listing::handle(tx, actor, command, payload, now).await
        }
        CommandPayload::ReserveInventory(payload) => {
            crate::handlers::reserve_inventory::handle(tx, actor, command, payload, now).await
        }
        CommandPayload::CreateCheckout(payload) => {
            crate::handlers::checkout::handle(tx, actor, command, payload, now).await
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
