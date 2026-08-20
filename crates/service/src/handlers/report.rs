//! Trust reports (task 3.5). `trust.report` is ported from the prototype
//! engine; `trust.decide` is canonical to this service: moderators (a
//! configured pubky list, no broad admin role) record decisions append-only
//! in `report_decisions` while the report row tracks the resulting state.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{CreateReportPayload, DecideReportPayload};
use marketplace_domain::state_machines::{can_transition, report_machine};
use marketplace_domain::{ids, Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::model::ReportRow;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub const REPORT_COLUMNS: &str = "id, reporter_pubky, target_type, target_id, reason, details, \
     state, revision, created_at, updated_at";

pub async fn create(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &CreateReportPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if command.aggregate_id != ids::report_aggregate_id(command.command_id)
        || command.expected_revision != 0
    {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The report aggregate identity is invalid.",
        )));
    }

    let target_type = serde_json::to_value(payload.target_type).expect("enum serializes");
    let reason = serde_json::to_value(payload.reason).expect("enum serializes");
    let report: ReportRow = sqlx::query_as(&format!(
        "INSERT INTO reports (id, reporter_pubky, target_type, target_id, reason, details, \
         state, revision, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 'open', 1, $7, $7) RETURNING {REPORT_COLUMNS}"
    ))
    .bind(command.command_id)
    .bind(actor)
    .bind(target_type.as_str().expect("enum is a string"))
    .bind(&payload.target_id)
    .bind(reason.as_str().expect("enum is a string"))
    .bind(&payload.details)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        1,
        actor,
        "trust.reported",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: 1,
        event_ids: vec![event_id],
        result: json!({ "kind": "report", "report": report.view() }),
    }))
}

pub async fn decide(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &DecideReportPayload,
    moderator_pubkys: &[String],
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if !moderator_pubkys.iter().any(|entry| entry == actor) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only a configured moderator may decide reports.",
        )));
    }
    let report: Option<ReportRow> = sqlx::query_as(&format!(
        "SELECT {REPORT_COLUMNS} FROM reports WHERE id = $1 FOR UPDATE"
    ))
    .bind(payload.report_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(report) = report else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The report was not found.",
        )));
    };
    if command.aggregate_id != ids::report_aggregate_id(report.id) {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The report aggregate id is invalid.",
        )));
    }
    if command.expected_revision != report.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The report revision is stale.",
            report.revision,
        )));
    }
    let decision = payload.decision.as_str();
    if !can_transition(&report_machine(), &report.state, decision) || report.state != "open" {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            "The report is already decided.",
        )));
    }

    sqlx::query(
        "INSERT INTO report_decisions (id, report_id, moderator_pubky, decision, rationale, \
         created_at) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(report.id)
    .bind(actor)
    .bind(decision)
    .bind(&payload.rationale)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    let new_revision = report.revision + 1;
    let updated: ReportRow = sqlx::query_as(&format!(
        "UPDATE reports SET state = $2, revision = $3, updated_at = $4 \
         WHERE id = $1 RETURNING {REPORT_COLUMNS}"
    ))
    .bind(report.id)
    .bind(decision)
    .bind(new_revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        new_revision,
        actor,
        "trust.decided",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "report", "report": updated.view() }),
    }))
}
