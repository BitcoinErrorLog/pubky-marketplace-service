//! Generated HTTP contracts for the shared-manual seller review endpoints.
//!
//! The request schemas are derived from the request types used by Axum. The
//! reason catalog is the single source for both the handler responses and the
//! checked-in contract manifest.

use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReviewReason {
    ConfirmationObservationMismatch,
    ConfirmationEffectsFailed,
    InvalidReason,
    InvalidIdempotencyKey,
    InvalidOutcome,
    InvalidRefundReference,
    NotOrderSeller,
    OrderNotFound,
    OrderNotAwaitingConfirmation,
    ResolutionNotApplicable,
    MissingPin,
    Conflict,
    AlreadyResolved,
    NotInManualReview,
    StockUnavailable,
}

impl ReviewReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfirmationObservationMismatch => "confirmation_observation_mismatch",
            Self::ConfirmationEffectsFailed => "confirmation_effects_failed",
            Self::InvalidReason => "invalid_reason",
            Self::InvalidIdempotencyKey => "invalid_idempotency_key",
            Self::InvalidOutcome => "invalid_outcome",
            Self::InvalidRefundReference => "invalid_refund_reference",
            Self::NotOrderSeller => "not_order_seller",
            Self::OrderNotFound => "order_not_found",
            Self::OrderNotAwaitingConfirmation => "order_not_awaiting_confirmation",
            Self::ResolutionNotApplicable => "resolution_not_applicable",
            Self::MissingPin => "missing_pin",
            Self::Conflict => "conflict",
            Self::AlreadyResolved => "already_resolved",
            Self::NotInManualReview => "not_in_manual_review",
            Self::StockUnavailable => "stock_unavailable",
        }
    }

    pub const fn all() -> &'static [Self] {
        &[
            Self::ConfirmationObservationMismatch,
            Self::ConfirmationEffectsFailed,
            Self::InvalidReason,
            Self::InvalidIdempotencyKey,
            Self::InvalidOutcome,
            Self::InvalidRefundReference,
            Self::NotOrderSeller,
            Self::OrderNotFound,
            Self::OrderNotAwaitingConfirmation,
            Self::ResolutionNotApplicable,
            Self::MissingPin,
            Self::Conflict,
            Self::AlreadyResolved,
            Self::NotInManualReview,
            Self::StockUnavailable,
        ]
    }

    pub fn for_endpoint(endpoint: &str) -> &'static [Self] {
        match endpoint {
            "confirm" => &[
                Self::ConfirmationObservationMismatch,
                Self::ConfirmationEffectsFailed,
                Self::InvalidReason,
                Self::NotOrderSeller,
                Self::OrderNotAwaitingConfirmation,
                Self::OrderNotFound,
            ],
            "resolve" => &[
                Self::AlreadyResolved,
                Self::Conflict,
                Self::InvalidIdempotencyKey,
                Self::InvalidOutcome,
                Self::InvalidReason,
                Self::InvalidRefundReference,
                Self::MissingPin,
                Self::NotInManualReview,
                Self::NotOrderSeller,
                Self::OrderNotFound,
                Self::ResolutionNotApplicable,
                Self::StockUnavailable,
            ],
            _ => &[],
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointContract {
    pub method: &'static str,
    pub path: &'static str,
    pub required_headers: &'static [&'static str],
    pub request_schema: Value,
    pub reasons: Vec<&'static str>,
}

pub fn endpoint_contracts() -> Vec<EndpointContract> {
    vec![
        EndpointContract {
            method: "POST",
            path: "/v0/orders/{id}/confirm-bitcoin-payment",
            required_headers: &["Authorization"],
            request_schema: serde_json::to_value(schema_for!(
                crate::bitcoin_review::ConfirmBitcoinPaymentBody
            ))
            .expect("confirm request schema serializes"),
            reasons: ReviewReason::for_endpoint("confirm")
                .iter()
                .map(|reason| reason.as_str())
                .collect(),
        },
        EndpointContract {
            method: "POST",
            path: "/v0/orders/{id}/bitcoin/resolve",
            required_headers: &["Authorization", "Idempotency-Key"],
            request_schema: serde_json::to_value(schema_for!(
                crate::bitcoin_review::ResolveBitcoinPaymentBody
            ))
            .expect("resolve request schema serializes"),
            reasons: ReviewReason::for_endpoint("resolve")
                .iter()
                .map(|reason| reason.as_str())
                .collect(),
        },
    ]
}

pub fn normalized_snapshot(value: &Value) -> Value {
    let mut normalizer = SnapshotNormalizer::default();
    normalizer.collect_paykit_references(value);
    normalizer.collect_pubkys(value);
    normalizer.collect_role_pubkys(value);
    normalizer.normalize(value, None)
}

#[derive(Default)]
struct SnapshotNormalizer {
    uuids: HashMap<String, String>,
    timestamps: HashMap<String, String>,
    paykit_references: HashMap<String, String>,
    pubkys: HashMap<String, String>,
    next_paykit_reference: usize,
    next_pubky: usize,
}

impl SnapshotNormalizer {
    fn collect_paykit_references(&mut self, value: &Value) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if key == "paykit_request_reference" {
                        if let Some(value) = value.as_str() {
                            numbered_with_counter(
                                &mut self.paykit_references,
                                value,
                                "paykit-reference",
                                &mut self.next_paykit_reference,
                            );
                        }
                    }
                    self.collect_paykit_references(value);
                }
            }
            Value::Array(values) => values
                .iter()
                .for_each(|value| self.collect_paykit_references(value)),
            _ => {}
        }
    }

    fn collect_pubkys(&mut self, value: &Value) {
        match value {
            Value::Object(object) => object.values().for_each(|value| self.collect_pubkys(value)),
            Value::Array(values) => values.iter().for_each(|value| self.collect_pubkys(value)),
            Value::String(value) => self.collect_pubky_runs(value),
            _ => {}
        }
    }

    fn collect_pubky_runs(&mut self, value: &str) {
        for run in value.split(|character: char| !is_z_base32(character)) {
            if is_pubky(run) {
                self.pubkys.entry(run.to_string()).or_insert_with(|| {
                    self.next_pubky += 1;
                    format!("<pubky:{}>", self.next_pubky)
                });
            }
        }
    }

    fn collect_role_pubkys(&mut self, value: &Value) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if let Some(value) = value.as_str().filter(|value| is_pubky(value)) {
                        let role = match key.as_str() {
                            "seller" | "seller_pubky" | "resolved_by_pubky"
                            | "confirmed_by_pubky" => Some("seller"),
                            "buyer" | "buyer_pubky" => Some("buyer"),
                            _ => None,
                        };
                        if let Some(role) = role {
                            self.pubkys
                                .insert(value.to_string(), format!("<pubky:{role}>"));
                        }
                    }
                    self.collect_role_pubkys(value);
                }
            }
            Value::Array(values) => {
                values
                    .iter()
                    .for_each(|value| self.collect_role_pubkys(value));
            }
            _ => {}
        }
    }

    fn normalize(&mut self, value: &Value, key: Option<&str>) -> Value {
        match value {
            Value::Object(object) => {
                let mut normalized = BTreeMap::new();
                for (key, value) in object {
                    normalized.insert(key.clone(), self.normalize(value, Some(key)));
                }
                let object: Map<String, Value> = normalized.into_iter().collect();
                Value::Object(object)
            }
            Value::Array(values) => Value::Array(
                values
                    .iter()
                    .map(|value| self.normalize(value, key))
                    .collect(),
            ),
            Value::String(value) => self.normalize_string(key, value),
            _ => value.clone(),
        }
    }

    fn normalize_string(&mut self, key: Option<&str>, value: &str) -> Value {
        if key == Some("paykit_request_reference") {
            return Value::String(self.paykit_references.get(value).cloned().unwrap_or_else(
                || {
                    numbered_with_counter(
                        &mut self.paykit_references,
                        value,
                        "paykit-reference",
                        &mut self.next_paykit_reference,
                    )
                },
            ));
        }
        if uuid::Uuid::parse_str(value).is_ok() {
            return Value::String(numbered(&mut self.uuids, value, "uuid"));
        }
        if chrono::DateTime::parse_from_rfc3339(value).is_ok() {
            return Value::String(numbered(&mut self.timestamps, value, "timestamp"));
        }
        let mut normalized = value.to_string();
        for (raw, placeholder) in &self.pubkys {
            normalized = normalized.replace(raw, placeholder);
        }
        normalized = replace_embedded_uuids(&normalized, &mut self.uuids);
        Value::String(normalized)
    }
}

fn numbered(values: &mut HashMap<String, String>, value: &str, kind: &str) -> String {
    let next = values.len() + 1;
    values
        .entry(value.to_string())
        .or_insert_with(|| format!("<{kind}:{next}>"))
        .clone()
}

fn numbered_with_counter(
    values: &mut HashMap<String, String>,
    value: &str,
    kind: &str,
    next: &mut usize,
) -> String {
    values
        .entry(value.to_string())
        .or_insert_with(|| {
            *next += 1;
            format!("<{kind}:{next}>")
        })
        .clone()
}

fn is_pubky(value: &str) -> bool {
    value.len() == 52
        && value
            .bytes()
            .all(|byte| b"ybndrfg8ejkmcpqxot1uwisza345h769".contains(&byte))
}

pub fn assert_no_sensitive_values(value: &Value) {
    fn walk(value: &Value, key: Option<&str>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    assert!(
                        !matches!(
                            key.as_str(),
                            "delivery_address"
                                | "endpoint"
                                | "invoice"
                                | "invoice_id"
                                | "request_hash"
                                | "provider_response"
                                | "token"
                        ),
                        "contract artifact contains forbidden field '{key}'"
                    );
                    walk(value, Some(key));
                }
            }
            Value::Array(values) => values.iter().for_each(|value| walk(value, key)),
            Value::String(value) => {
                let lower = value.to_ascii_lowercase();
                assert!(!lower.starts_with("bearer "), "residual bearer value");
                assert!(!contains_uuid(value), "residual UUID");
                assert!(!contains_rfc3339(value), "residual RFC3339 timestamp");
                assert!(!contains_pubky(value), "residual pubky");
                if key == Some("paykit_request_reference") {
                    assert!(
                        is_paykit_reference_placeholder(value),
                        "residual paykit request reference"
                    );
                }
            }
            _ => {}
        }
    }
    walk(value, None);
}

fn replace_embedded_uuids(value: &str, values: &mut HashMap<String, String>) -> String {
    let mut result = value.to_string();
    let mut start = 0;
    while start + 36 <= result.len() {
        let Some(candidate) = result.get(start..start + 36) else {
            start += 1;
            continue;
        };
        if uuid::Uuid::parse_str(candidate).is_ok() {
            let raw = candidate.to_string();
            let placeholder = numbered(values, &raw, "uuid");
            result.replace_range(start..start + 36, &placeholder);
            start += placeholder.len();
        } else {
            start += 1;
        }
    }
    result
}

fn contains_uuid(value: &str) -> bool {
    value.as_bytes().windows(36).any(|window| {
        std::str::from_utf8(window).is_ok_and(|value| uuid::Uuid::parse_str(value).is_ok())
    })
}

fn contains_rfc3339(value: &str) -> bool {
    value
        .split(|character: char| {
            character.is_whitespace()
                || matches!(character, '"' | '\'' | ',' | '[' | ']' | '{' | '}')
        })
        .any(|part| chrono::DateTime::parse_from_rfc3339(part).is_ok())
}

fn contains_pubky(value: &str) -> bool {
    value
        .split(|character: char| !is_z_base32(character))
        .any(is_pubky)
}

fn is_z_base32(character: char) -> bool {
    character.is_ascii() && b"ybndrfg8ejkmcpqxot1uwisza345h769".contains(&(character as u8))
}

fn is_paykit_reference_placeholder(value: &str) -> bool {
    value
        .strip_prefix("<paykit-reference:")
        .and_then(|value| value.strip_suffix('>'))
        .is_some_and(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
}
