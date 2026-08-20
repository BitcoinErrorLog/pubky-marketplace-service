//! Canonical domain contracts for the Pubky Marketplace Transaction Service.
//!
//! This crate is the single source of truth for:
//! - the versioned command envelope (ADR-0019 §3, snake_case wire format);
//! - command payload schemas and validation (ported from the TypeScript
//!   Zod schemas in `src/libs/commerce/transaction-commands.ts`);
//! - the canonical aggregate state machines (task 3.3), which are emitted as
//!   `contracts/state-machines.json` for cross-language contract tests.

pub mod commands;
pub mod error;
pub mod ids;
pub mod money;
pub mod pubky;
pub mod state_machines;

pub use commands::{Command, CommandPayload, ValidationIssue, COMMERCE_CONTRACT_VERSION};
pub use error::ErrorCode;
pub use money::Money;
