use marketplace_domain::{Command, ErrorCode, ValidationIssue};
use serde_json::{json, Value};
use uuid::Uuid;

/// A designed Locks binding refusal for the executor to persist in the
/// refusing command's OWN transaction. Set only on refusal paths that made
/// no state changes: a refusal is a business outcome, not an error, so the
/// executor commits the transaction — the audit row and the refusal
/// response are atomic, and recording never acquires a second pool
/// connection while the command transaction holds the payment lock.
#[derive(Debug, Clone)]
pub struct LocksRefusal {
    pub payment_id: Uuid,
    pub outcome: &'static str,
}

/// A rejected command. Field names and messages match the TypeScript
/// prototype engine; wire casing is snake_case per ADR-0019 §3.
#[derive(Debug, Clone)]
pub struct CommandFailure {
    pub code: ErrorCode,
    pub message: String,
    pub current_revision: Option<i64>,
    pub issues: Option<Vec<ValidationIssue>>,
    pub locks_refusal: Option<LocksRefusal>,
}

impl CommandFailure {
    pub fn new(code: ErrorCode, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
            current_revision: None,
            issues: None,
            locks_refusal: None,
        }
    }

    /// Attaches the designed Locks binding outcome the executor records
    /// in-transaction when it refuses with this failure.
    pub fn with_locks_refusal(mut self, payment_id: Uuid, outcome: &'static str) -> Self {
        self.locks_refusal = Some(LocksRefusal {
            payment_id,
            outcome,
        });
        self
    }

    pub fn with_revision(code: ErrorCode, message: &str, current_revision: i64) -> Self {
        Self {
            current_revision: Some(current_revision),
            ..Self::new(code, message)
        }
    }

    pub fn invalid_command(issues: Vec<ValidationIssue>) -> Self {
        Self {
            issues: Some(issues),
            ..Self::new(
                ErrorCode::InvalidCommand,
                "The marketplace command is invalid.",
            )
        }
    }

    pub fn body(&self) -> Value {
        let mut error = json!({
            "code": self.code,
            "message": self.message,
        });
        if let Some(revision) = self.current_revision {
            error["current_revision"] = json!(revision);
        }
        if let Some(issues) = &self.issues {
            error["issues"] = json!(issues);
        }
        json!({ "ok": false, "error": error })
    }

    pub fn http_status(&self) -> u16 {
        self.code.http_status()
    }
}

/// The state written by a successful handler, before the response envelope
/// is assembled and stored for idempotent replay.
#[derive(Debug)]
pub struct HandlerSuccess {
    pub revision: i64,
    pub event_ids: Vec<Uuid>,
    pub result: Value,
}

pub fn success_body(command: &Command, success: &HandlerSuccess) -> Value {
    json!({
        "ok": true,
        "version": marketplace_domain::COMMERCE_CONTRACT_VERSION,
        "command_id": command.command_id,
        "aggregate_id": command.aggregate_id,
        "revision": success.revision,
        "event_ids": success.event_ids,
        "result": success.result,
    })
}

pub type HandlerResult = Result<HandlerSuccess, CommandFailure>;
