use serde::{Deserialize, Serialize};

/// Wire error codes shared with the TypeScript prototype engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidCommand,
    Unauthorized,
    NotFound,
    RevisionConflict,
    IdempotencyConflict,
    InsufficientInventory,
    InvariantViolation,
    OfferExpired,
    InvalidState,
    AuctionClosed,
    BidTooLow,
    /// An upstream dependency (the seller's homeserver) could not be
    /// reached or answered unusably. Retriable: the command may succeed
    /// once the upstream recovers, so it maps to 503 rather than a 4xx.
    UpstreamUnavailable,
}

impl ErrorCode {
    /// HTTP status carried by each error code. Idempotency and concurrency
    /// conflicts are 409 per ADR-0019 §3.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorCode::InvalidCommand => 422,
            ErrorCode::Unauthorized => 403,
            ErrorCode::NotFound => 404,
            ErrorCode::RevisionConflict
            | ErrorCode::IdempotencyConflict
            | ErrorCode::InsufficientInventory
            | ErrorCode::InvariantViolation
            | ErrorCode::OfferExpired
            | ErrorCode::InvalidState
            | ErrorCode::AuctionClosed
            | ErrorCode::BidTooLow => 409,
            ErrorCode::UpstreamUnavailable => 503,
        }
    }
}
