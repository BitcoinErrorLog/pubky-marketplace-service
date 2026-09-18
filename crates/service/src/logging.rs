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
        if is_zbase32(chars[index]) {
            let run_start = index;
            while index < chars.len() && is_zbase32(chars[index]) {
                index += 1;
            }
            let run = &chars[run_start..index];
            if run.len() >= 52 {
                output.push_str(&actor_prefix(&run.iter().collect::<String>()));
                output.push('…');
            } else {
                output.extend(run);
            }
            continue;
        }
        output.push(chars[index]);
        index += 1;
    }
    output
}

fn is_zbase32(value: char) -> bool {
    matches!(
        value.to_ascii_lowercase(),
        'y' | 'b'
            | 'n'
            | 'd'
            | 'r'
            | 'f'
            | 'g'
            | '8'
            | 'e'
            | 'j'
            | 'k'
            | 'm'
            | 'c'
            | 'p'
            | 'q'
            | 'x'
            | 'o'
            | 't'
            | '1'
            | 'u'
            | 'w'
            | 'i'
            | 's'
            | 'z'
            | 'a'
            | '3'
            | '4'
            | '5'
            | 'h'
            | '7'
            | '6'
            | '9'
    )
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
        Some(ErrorCode::SellerRegistrationRequired) => "SELLER_REGISTRATION_REQUIRED",
        Some(ErrorCode::UpstreamUnavailable) => "UPSTREAM_UNAVAILABLE",
        Some(ErrorCode::AwardExpired) => "AWARD_EXPIRED",
        Some(ErrorCode::AwardAlreadyConverted) => "AWARD_ALREADY_CONVERTED",
        Some(ErrorCode::AwardQuantityMismatch) => "AWARD_QUANTITY_MISMATCH",
        Some(ErrorCode::AwardVariantMismatch) => "AWARD_VARIANT_MISMATCH",
        Some(ErrorCode::AwardListingChanged) => "AWARD_LISTING_CHANGED",
        Some(ErrorCode::AwardHoldMissing) => "AWARD_HOLD_MISSING",
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

pub(crate) fn log_invalid_command(actor: Option<&str>, latency_ms: u64) {
    let actor_prefix = actor.map(actor_prefix);
    tracing::warn!(
        actor_prefix,
        outcome = "invalid",
        error_code = "INVALID_COMMAND",
        latency_ms,
        "command.invalid"
    );
}

#[cfg(test)]
mod tests {
    use super::redact_pubkys;

    #[test]
    fn redacts_pubky_runs_without_leaving_partial_values() {
        let pubky = "y".repeat(52);
        assert_eq!(redact_pubkys(&pubky), "yyyyyyyy…");
        assert_eq!(redact_pubkys(&format!("x{pubky}")), "xyyyyyyy…");
        assert_eq!(redact_pubkys(&format!("xxxxxxx{pubky}")), "xxxxxxxy…");
        assert_eq!(redact_pubkys(&pubky.to_uppercase()), "YYYYYYYY…");
        assert_eq!(redact_pubkys(&format!("{pubky}{pubky}")), "yyyyyyyy…");
    }

    #[test]
    fn preserves_short_runs_and_redacts_id_shapes() {
        let pubky = "y".repeat(52);
        assert_eq!(redact_pubkys(&"y".repeat(51)), "y".repeat(51));
        assert_eq!(
            redact_pubkys(&format!("listing:{pubky}_id")),
            "listing:yyyyyyyy…_id"
        );
        assert_eq!(redact_pubkys(&format!("order:{pubky}")), "order:yyyyyyyy…");
    }
}
