use axum::extract::MatchedPath;
use axum::http::Request;
use axum::response::Response;
use marketplace_domain::ErrorCode;

pub(crate) fn actor_prefix(actor: &str) -> String {
    actor.chars().take(8).collect()
}

fn redact_pubkys(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut output = String::with_capacity(value.len());
    let mut index = 0;
    while index < chars.len() {
        if index + 52 <= chars.len() {
            let candidate: String = chars[index..index + 52].iter().collect();
            if marketplace_domain::pubky::is_valid_pubky(&candidate) {
                output.push_str(&actor_prefix(&candidate));
                output.push('…');
                index += 52;
                continue;
            }
        }
        output.push(chars[index]);
        index += 1;
    }
    output
}

pub(crate) fn error_code_name(code: Option<ErrorCode>) -> &'static str {
    match code {
        Some(ErrorCode::InvalidCommand) => "INVALID_COMMAND",
        Some(ErrorCode::Unauthorized) => "UNAUTHORIZED",
        Some(ErrorCode::NotFound) => "NOT_FOUND",
        Some(ErrorCode::RevisionConflict) => "REVISION_CONFLICT",
        Some(ErrorCode::IdempotencyConflict) => "IDEMPOTENCY_CONFLICT",
        Some(ErrorCode::InsufficientInventory) => "INSUFFICIENT_INVENTORY",
        Some(ErrorCode::InvariantViolation) => "INVARIANT_VIOLATION",
        Some(ErrorCode::OfferExpired) => "OFFER_EXPIRED",
        Some(ErrorCode::InvalidState) => "INVALID_STATE",
        Some(ErrorCode::AuctionClosed) => "AUCTION_CLOSED",
        Some(ErrorCode::BidTooLow) => "BID_TOO_LOW",
        Some(ErrorCode::UpstreamUnavailable) => "UPSTREAM_UNAVAILABLE",
        None => "",
    }
}

pub(crate) fn route_template<T>(request: &Request<T>) -> &str {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unknown")
}

pub(crate) fn log_http_request(method: &str, route: &str, response: &Response, latency_ms: u64) {
    let status = response.status().as_u16();
    let actor_prefix_value = response
        .extensions()
        .get::<crate::auth::Actor>()
        .map(|actor| actor_prefix(&actor.0));
    if route == "/health" {
        tracing::debug!(method, route, status, latency_ms, "http.request");
    } else if let Some(actor_prefix) = actor_prefix_value {
        tracing::info!(
            method,
            route,
            status,
            latency_ms,
            actor_prefix,
            "http.request"
        );
    } else {
        tracing::info!(method, route, status, latency_ms, "http.request");
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn log_command(
    actor: &str,
    kind: &str,
    command_id: &str,
    aggregate_id: &str,
    outcome: &str,
    error_code: Option<ErrorCode>,
    refusal_message: Option<&str>,
    revision: i64,
    latency_ms: u64,
) {
    let aggregate_id = redact_pubkys(aggregate_id);
    let actor_prefix = actor_prefix(actor);
    let error_code = error_code_name(error_code);
    let refusal_message = refusal_message.unwrap_or("");
    if outcome == "accepted" || outcome == "idempotent_replay" {
        tracing::info!(
            kind,
            command_id,
            aggregate_id,
            actor_prefix,
            outcome,
            error_code,
            refusal_message,
            revision,
            latency_ms,
            "command.executed"
        );
    } else {
        tracing::warn!(
            kind,
            command_id,
            aggregate_id,
            actor_prefix,
            outcome,
            error_code,
            refusal_message,
            revision,
            latency_ms,
            "command.executed"
        );
    }
}
