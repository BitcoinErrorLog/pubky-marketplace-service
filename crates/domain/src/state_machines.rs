//! Canonical aggregate state machines (task 3.3).
//!
//! These tables are the single source of truth for both this service and the
//! TypeScript client contracts in `pubky-app` (`src/libs/commerce`). The
//! machine-readable artifact is emitted to `contracts/state-machines.json`
//! (see `bin/emit_contracts.rs`) and a test asserts the file is in sync.
//!
//! Divergences between the TypeScript prototype engine
//! (`services/marketplace/src/transaction-service.ts`) and the client-side
//! contracts (`src/libs/commerce/state-machines.ts`) are resolved here in
//! favor of the prototype engine, which ADR-0022 designates as the executable
//! specification. See the README section "Resolved contract divergences".

use serde::Serialize;
use serde_json::Value;

/// A transition trigger: either an authenticated command or a server-driven
/// action (server time expiry, payment observation, sweep workers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "trigger", content = "name")]
pub enum Via {
    Command(&'static str),
    Server(&'static str),
}

#[derive(Debug, Clone, Serialize)]
pub struct Transition {
    pub from: &'static str,
    pub to: &'static str,
    pub via: Vec<Via>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregateMachine {
    pub aggregate: &'static str,
    pub states: Vec<&'static str>,
    pub initial: &'static str,
    pub transitions: Vec<Transition>,
    /// Commands accepted by this aggregate (implemented plus specified).
    pub commands: Vec<&'static str>,
    /// States present in the canonical enum that no current transition
    /// produces; retained for engine parity and future commands.
    pub unreachable_states: Vec<&'static str>,
}

fn t(from: &'static str, to: &'static str, via: Vec<Via>) -> Transition {
    Transition { from, to, via }
}

use Via::{Command, Server};

/// Listing inventory availability, as tracked by the transaction authority.
/// Catalog lifecycle states (draft/active/paused/removed) belong to the
/// seller-signed homeserver records, not this service.
pub fn listing_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "listing",
        states: vec!["available", "reserved", "sold"],
        initial: "available",
        transitions: vec![
            t(
                "available",
                "reserved",
                vec![
                    Command("inventory.reserve"),
                    Command("checkout.create"),
                    Command("offer.accept"),
                    Command("auction.close"),
                ],
            ),
            t(
                "reserved",
                "available",
                vec![
                    Server("reservation_expiry"),
                    Command("order.cancel_request"),
                    Command("order.cancel_approve"),
                ],
            ),
            t(
                "reserved",
                "sold",
                vec![
                    Command("payment.sandbox_advance"),
                    Server("payment_confirmation"),
                ],
            ),
        ],
        commands: vec![
            "listing.register",
            "inventory.reserve",
            "checkout.create",
            "offer.accept",
            "auction.close",
        ],
        unreachable_states: vec![],
    }
}

pub fn reservation_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "reservation",
        states: vec!["active", "converted", "released", "expired"],
        initial: "active",
        transitions: vec![
            t("active", "expired", vec![Server("reservation_expiry")]),
            t(
                "active",
                "released",
                vec![
                    Command("order.cancel_request"),
                    Command("order.cancel_approve"),
                ],
            ),
            t(
                "active",
                "converted",
                vec![
                    Command("payment.sandbox_advance"),
                    Server("payment_confirmation"),
                ],
            ),
        ],
        commands: vec!["inventory.reserve"],
        unreachable_states: vec![],
    }
}

pub fn offer_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "offer",
        states: vec![
            "pending",
            "countered",
            "accepted",
            "rejected",
            "withdrawn",
            "expired",
        ],
        initial: "pending",
        transitions: vec![
            t("pending", "countered", vec![Command("offer.counter")]),
            t("pending", "accepted", vec![Command("offer.accept")]),
            t("pending", "rejected", vec![Command("offer.reject")]),
            t("pending", "withdrawn", vec![Command("offer.withdraw")]),
            t("pending", "expired", vec![Server("offer_expiry")]),
            t("countered", "countered", vec![Command("offer.counter")]),
            t("countered", "accepted", vec![Command("offer.accept")]),
            t("countered", "rejected", vec![Command("offer.reject")]),
            t("countered", "withdrawn", vec![Command("offer.withdraw")]),
            t("countered", "expired", vec![Server("offer_expiry")]),
        ],
        commands: vec![
            "offer.create",
            "offer.counter",
            "offer.accept",
            "offer.reject",
            "offer.withdraw",
        ],
        unreachable_states: vec![],
    }
}

pub fn auction_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "auction",
        states: vec!["scheduled", "active", "sold", "unsold", "cancelled"],
        initial: "scheduled",
        transitions: vec![
            t("scheduled", "active", vec![Server("auction_start")]),
            t(
                "active",
                "sold",
                vec![Command("auction.close"), Server("auction_close")],
            ),
            t(
                "active",
                "unsold",
                vec![Command("auction.close"), Server("auction_close")],
            ),
        ],
        commands: vec!["listing.register", "auction.place_bid", "auction.close"],
        unreachable_states: vec!["cancelled"],
    }
}

pub fn order_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "order",
        states: vec![
            "pending_payment",
            "paid",
            "processing",
            "shipped",
            "delivered",
            "completed",
            "cancel_requested",
            "cancelled",
            "return_requested",
            "return_approved",
            "return_received",
            "disputed",
            "refunded_external",
            "closed",
        ],
        initial: "pending_payment",
        transitions: vec![
            t(
                "pending_payment",
                "paid",
                vec![Command("payment.sandbox_advance")],
            ),
            t(
                "pending_payment",
                "cancelled",
                vec![Command("order.cancel_request")],
            ),
            t("paid", "shipped", vec![Command("fulfillment.ship")]),
            t(
                "paid",
                "cancel_requested",
                vec![Command("order.cancel_request")],
            ),
            t("paid", "disputed", vec![Command("dispute.open")]),
            t("processing", "shipped", vec![Command("fulfillment.ship")]),
            t(
                "processing",
                "cancel_requested",
                vec![Command("order.cancel_request")],
            ),
            t("processing", "disputed", vec![Command("dispute.open")]),
            t(
                "cancel_requested",
                "cancelled",
                vec![Command("order.cancel_approve")],
            ),
            t(
                "shipped",
                "delivered",
                vec![Command("fulfillment.confirm_delivery")],
            ),
            t("shipped", "disputed", vec![Command("dispute.open")]),
            t(
                "delivered",
                "return_requested",
                vec![Command("return.request")],
            ),
            t("delivered", "completed", vec![Command("review.create")]),
            t("delivered", "disputed", vec![Command("dispute.open")]),
            t(
                "completed",
                "return_requested",
                vec![Command("return.request")],
            ),
            t("completed", "disputed", vec![Command("dispute.open")]),
            t(
                "return_requested",
                "return_approved",
                vec![Command("return.approve")],
            ),
            t(
                "return_requested",
                "disputed",
                vec![Command("dispute.open")],
            ),
            t(
                "return_approved",
                "return_received",
                vec![Command("return.receive")],
            ),
            t("return_approved", "disputed", vec![Command("dispute.open")]),
            t(
                "return_received",
                "refunded_external",
                vec![Command("refund.record_external")],
            ),
            t("disputed", "completed", vec![Command("dispute.resolve")]),
            t(
                "disputed",
                "refunded_external",
                vec![Command("refund.record_external")],
            ),
            t(
                "cancelled",
                "refunded_external",
                vec![Command("refund.record_external")],
            ),
        ],
        commands: vec![
            "checkout.create",
            "payment.sandbox_advance",
            "order.cancel_request",
            "order.cancel_approve",
            "fulfillment.ship",
            "fulfillment.confirm_delivery",
            "return.request",
            "return.approve",
            "return.receive",
            "refund.record_external",
            "dispute.open",
            "dispute.evidence",
            "dispute.resolve",
            "review.create",
            "review.update",
        ],
        unreachable_states: vec!["processing", "closed"],
    }
}

/// The return request sub-state carried on an order (the client's
/// `orderSchema.returnRequest.state` enum). `refunded` is reached through the
/// externally evidenced refund record, never a funds movement by this
/// service.
pub fn return_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "return",
        states: vec!["requested", "approved", "received", "refunded"],
        initial: "requested",
        transitions: vec![
            t("requested", "approved", vec![Command("return.approve")]),
            t("approved", "received", vec![Command("return.receive")]),
            t(
                "received",
                "refunded",
                vec![Command("refund.record_external")],
            ),
        ],
        commands: vec![
            "return.request",
            "return.approve",
            "return.receive",
            "refund.record_external",
        ],
        unreachable_states: vec![],
    }
}

/// The dispute sub-state carried on an order (the client's
/// `orderSchema.dispute.state` enum). The moderator decision
/// (`dispute.resolve`) is the close transition; evidence
/// (`dispute.evidence`, this service only) appends to the record without a
/// state change.
pub fn dispute_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "dispute",
        states: vec!["open", "resolved"],
        initial: "open",
        transitions: vec![t("open", "resolved", vec![Command("dispute.resolve")])],
        commands: vec!["dispute.open", "dispute.evidence", "dispute.resolve"],
        unreachable_states: vec![],
    }
}

pub fn payment_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "payment",
        states: vec![
            "awaiting_entitlement",
            "detected",
            "confirmed",
            "expired",
            "manual_review",
        ],
        initial: "awaiting_entitlement",
        transitions: vec![
            t(
                "awaiting_entitlement",
                "detected",
                vec![Command("payment.sandbox_advance")],
            ),
            t(
                "awaiting_entitlement",
                "confirmed",
                vec![Command("payment.sandbox_advance")],
            ),
            t(
                "awaiting_entitlement",
                "expired",
                vec![Command("payment.sandbox_advance"), Server("payment_window")],
            ),
            t(
                "awaiting_entitlement",
                "manual_review",
                vec![Command("payment.sandbox_advance")],
            ),
            t(
                "detected",
                "confirmed",
                vec![Command("payment.sandbox_advance")],
            ),
            t(
                "detected",
                "manual_review",
                vec![Command("payment.sandbox_advance")],
            ),
        ],
        commands: vec!["payment.sandbox_advance"],
        unreachable_states: vec![],
    }
}

/// Trust reports (task 3.5). Reports are created by any authenticated user;
/// decisions are moderator-only and recorded append-only. The prototype
/// engine stored reports with a single `open` state and no decisions; the
/// decision flow is canonical to this service.
pub fn report_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "report",
        states: vec!["open", "dismissed", "actioned"],
        initial: "open",
        transitions: vec![
            t("open", "dismissed", vec![Command("trust.decide")]),
            t("open", "actioned", vec![Command("trust.decide")]),
        ],
        commands: vec!["trust.report", "trust.decide"],
        unreachable_states: vec![],
    }
}

pub fn all_machines() -> Vec<AggregateMachine> {
    vec![
        listing_machine(),
        reservation_machine(),
        offer_machine(),
        auction_machine(),
        order_machine(),
        payment_machine(),
        return_machine(),
        dispute_machine(),
        report_machine(),
    ]
}

/// Returns true when `from -> to` is allowed for the aggregate. Staying in
/// the same state (revision-only updates) is always allowed.
pub fn can_transition(machine: &AggregateMachine, from: &str, to: &str) -> bool {
    if from == to {
        return machine.states.contains(&from);
    }
    machine
        .transitions
        .iter()
        .any(|transition| transition.from == from && transition.to == to)
}

/// The machine-readable contract artifact validated by the pubky-app client
/// contracts in CI.
pub fn contract_document() -> Value {
    serde_json::json!({
        "contract_version": 1,
        "source": "marketplace-domain::state_machines",
        "aggregates": all_machines(),
    })
}

/// Pretty-printed contract JSON with a trailing newline, exactly as written
/// to `contracts/state-machines.json`.
pub fn contract_json_pretty() -> String {
    let mut rendered = serde_json::to_string_pretty(&contract_document())
        .expect("contract document serializes infallibly");
    rendered.push('\n');
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn transitions_reference_declared_states() {
        for machine in all_machines() {
            let states: HashSet<&str> = machine.states.iter().copied().collect();
            assert!(states.contains(machine.initial), "{}", machine.aggregate);
            for transition in &machine.transitions {
                assert!(states.contains(transition.from), "{}", machine.aggregate);
                assert!(states.contains(transition.to), "{}", machine.aggregate);
                assert!(!transition.via.is_empty(), "{}", machine.aggregate);
            }
            for state in &machine.unreachable_states {
                assert!(states.contains(state), "{}", machine.aggregate);
                assert!(
                    !machine.transitions.iter().any(|t| t.to == *state),
                    "{} declares {state} unreachable but a transition targets it",
                    machine.aggregate
                );
            }
        }
    }

    #[test]
    fn unreachable_states_are_exactly_the_untargeted_non_initial_states() {
        for machine in all_machines() {
            let targeted: HashSet<&str> = machine.transitions.iter().map(|t| t.to).collect();
            let expected: Vec<&str> = machine
                .states
                .iter()
                .copied()
                .filter(|state| *state != machine.initial && !targeted.contains(state))
                .collect();
            assert_eq!(
                machine.unreachable_states, expected,
                "{} unreachable states drifted",
                machine.aggregate
            );
        }
    }

    #[test]
    fn listing_machine_enforces_inventory_flow() {
        let machine = listing_machine();
        assert!(can_transition(&machine, "available", "reserved"));
        assert!(can_transition(&machine, "reserved", "available"));
        assert!(can_transition(&machine, "reserved", "sold"));
        assert!(can_transition(&machine, "available", "available"));
        assert!(!can_transition(&machine, "available", "sold"));
        assert!(!can_transition(&machine, "sold", "available"));
    }

    #[test]
    fn contract_document_is_stable_json() {
        let document = contract_document();
        assert_eq!(document["contract_version"], 1);
        assert_eq!(
            document["aggregates"]
                .as_array()
                .expect("aggregates array")
                .len(),
            9
        );
    }

    /// Fails when `contracts/state-machines.json` is stale. Regenerate with:
    /// `cargo run -p marketplace-domain --bin emit-contracts`
    #[test]
    fn contract_artifact_is_in_sync() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/state-machines.json"
        );
        let on_disk = std::fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "contracts/state-machines.json is missing ({error}); \
                 regenerate with `cargo run -p marketplace-domain --bin emit-contracts`"
            )
        });
        assert_eq!(
            on_disk,
            contract_json_pretty(),
            "contracts/state-machines.json is stale; \
             regenerate with `cargo run -p marketplace-domain --bin emit-contracts`"
        );
    }
}
