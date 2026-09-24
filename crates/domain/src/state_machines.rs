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
            // Approving the cancellation of a paid order reverses the sale:
            // payment confirmation moved the quantities reserved -> sold on
            // this durable ledger (the prototype engine left them reserved),
            // so returning the cancelled order's stock necessarily moves
            // sold -> available. See the README divergence table. The
            // unilateral buyer exits (post-payment pickup terms change and
            // the bounded post-reveal withdrawal, local pickup design §A3)
            // release through the same path via `order.cancel_request`.
            t(
                "sold",
                "available",
                vec![
                    Command("order.cancel_approve"),
                    Command("order.cancel_request"),
                ],
            ),
        ],
        commands: vec![
            "listing.register",
            "listing.sync",
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
            "converted",
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
            t("accepted", "converted", vec![Command("offer.checkout")]),
            t("accepted", "expired", vec![Server("award_expiry")]),
        ],
        commands: vec![
            "offer.create",
            "offer.counter",
            "offer.accept",
            "offer.reject",
            "offer.withdraw",
            "offer.checkout",
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
            "ready_for_pickup",
            "processing",
            "shipped",
            "delivered",
            "completed",
            "cancel_requested",
            "cancelled",
            "return_requested",
            "return_approved",
            "return_received",
            "refunded_external",
            "closed",
        ],
        initial: "pending_payment",
        transitions: vec![
            t(
                "pending_payment",
                "paid",
                vec![
                    Command("payment.sandbox_advance"),
                    Server("payment_confirmation"),
                ],
            ),
            // `payment_window` is the server-time hold-window expiry ("only
            // a payment locks an item"): a pending order whose armed
            // inventory hold lapses is cancelled with its stock restocked.
            t(
                "pending_payment",
                "cancelled",
                vec![Command("order.cancel_request"), Server("payment_window")],
            ),
            t("paid", "shipped", vec![Command("fulfillment.ship")]),
            // The pickup path (local pickup design §A6): the seller arms
            // pickup readiness, and EITHER party confirms the handover from
            // `paid` or `ready_for_pickup` — the same `delivered` fact a
            // shipped order's confirmation produces.
            t(
                "paid",
                "ready_for_pickup",
                vec![Command("fulfillment.mark_ready")],
            ),
            // `digital_delivery` is the confirmation of an order whose every
            // line is released on the order page (file, link, text): it
            // leaves `paid` for `delivered` in the receipt transaction
            // (digital delivery design §3.6).
            t(
                "paid",
                "delivered",
                vec![
                    Command("fulfillment.confirm_pickup"),
                    Command("fulfillment.deliver_digital"),
                    Server("digital_delivery"),
                ],
            ),
            t(
                "ready_for_pickup",
                "delivered",
                vec![Command("fulfillment.confirm_pickup")],
            ),
            t(
                "ready_for_pickup",
                "cancel_requested",
                vec![Command("order.cancel_request")],
            ),
            // The unilateral buyer exits (§A3): while a post-payment pickup
            // terms change exists, or while the bounded post-reveal
            // withdrawal window is open, `order.cancel_request` moves the
            // order straight to `cancelled` with no seller approval. The
            // contract format has no actor or condition field; the handlers
            // enforce both.
            t("paid", "cancelled", vec![Command("order.cancel_request")]),
            t(
                "ready_for_pickup",
                "cancelled",
                vec![Command("order.cancel_request")],
            ),
            t(
                "paid",
                "cancel_requested",
                vec![Command("order.cancel_request")],
            ),
            t("processing", "shipped", vec![Command("fulfillment.ship")]),
            t(
                "processing",
                "cancel_requested",
                vec![Command("order.cancel_request")],
            ),
            t(
                "cancel_requested",
                "cancelled",
                vec![Command("order.cancel_approve")],
            ),
            // `delivery_assume` is the server-time delivery assumption
            // (there is no carrier tracking feed): DELIVERY_ASSUME_DAYS
            // after shipment the worker marks the order delivered with
            // `delivery_assumed = true` on the projection. Same edge as the
            // buyer's confirmation, one extra trigger.
            t(
                "shipped",
                "delivered",
                vec![
                    Command("fulfillment.confirm_delivery"),
                    Server("delivery_assume"),
                ],
            ),
            t(
                "delivered",
                "return_requested",
                vec![Command("return.request")],
            ),
            // `order_auto_complete` is the server-time completion:
            // AUTO_COMPLETE_DAYS after delivery the worker completes the
            // order. An open return/cancel request is its own order state,
            // so it blocks the sweep by construction. Same edge as the
            // buyer's review, one extra trigger.
            t(
                "delivered",
                "completed",
                vec![Command("review.create"), Server("order_auto_complete")],
            ),
            t(
                "completed",
                "return_requested",
                vec![Command("return.request")],
            ),
            t(
                "return_requested",
                "return_approved",
                vec![Command("return.approve")],
            ),
            t(
                "return_approved",
                "return_received",
                vec![Command("return.receive")],
            ),
            t(
                "return_received",
                "refunded_external",
                vec![Command("refund.record_external"), Server("paypal_refund")],
            ),
            t(
                "cancelled",
                "refunded_external",
                vec![Command("refund.record_external"), Server("paypal_refund")],
            ),
            // `paypal_refund` is a postback-verified PayPal refund or
            // reversal IPN whose recorded refunds reach the order total
            // (docs/paypal-refund-ipn.md). A partial refund keeps the state;
            // an open cancel or return request is resolved by the refund.
            t("paid", "refunded_external", vec![Server("paypal_refund")]),
            t(
                "ready_for_pickup",
                "refunded_external",
                vec![Server("paypal_refund")],
            ),
            t(
                "shipped",
                "refunded_external",
                vec![Server("paypal_refund")],
            ),
            // The seller's refund after a digital delivery (digital orders
            // never enter the return states); the handler admits the
            // command only for `fulfillment = digital`.
            t(
                "delivered",
                "refunded_external",
                vec![Command("refund.record_external"), Server("paypal_refund")],
            ),
            t(
                "completed",
                "refunded_external",
                vec![Command("refund.record_external"), Server("paypal_refund")],
            ),
            t(
                "cancel_requested",
                "refunded_external",
                vec![Server("paypal_refund")],
            ),
            t(
                "return_requested",
                "refunded_external",
                vec![Server("paypal_refund")],
            ),
            t(
                "return_approved",
                "refunded_external",
                vec![Server("paypal_refund")],
            ),
            // `paypal_reversal_cancelled`: PayPal cancelled the reversal that
            // brought the order to `refunded_external`, and the funds went
            // back to the seller. The order returns to the state that
            // reversal replaced.
            t(
                "refunded_external",
                "paid",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "ready_for_pickup",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "shipped",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "delivered",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "completed",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "cancel_requested",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "cancelled",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "return_requested",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "return_approved",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded_external",
                "return_received",
                vec![Server("paypal_reversal_cancelled")],
            ),
            // A late Paykit settlement whose hold already lapsed (order
            // cancelled, stock released) can still complete via
            // `late_completion` when the unit is free, or via seller
            // `paykit_resolution` for a legacy `manual_review` row whose
            // `review_reason` is not `refund_required`.
            t(
                "cancelled",
                "paid",
                vec![Server("paykit_resolution"), Server("late_completion")],
            ),
        ],
        commands: vec![
            "checkout.create",
            "payment.sandbox_advance",
            "order.cancel_request",
            "order.cancel_approve",
            "fulfillment.ship",
            "fulfillment.confirm_delivery",
            "fulfillment.mark_ready",
            "fulfillment.confirm_pickup",
            "return.request",
            "return.approve",
            "return.receive",
            "refund.record_external",
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
                vec![Command("refund.record_external"), Server("paypal_refund")],
            ),
            t("requested", "refunded", vec![Server("paypal_refund")]),
            t("approved", "refunded", vec![Server("paypal_refund")]),
            t(
                "refunded",
                "requested",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded",
                "approved",
                vec![Server("paypal_reversal_cancelled")],
            ),
            t(
                "refunded",
                "received",
                vec![Server("paypal_reversal_cancelled")],
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

/// The payment machine. Locks-correlated payments are advanced exclusively
/// by server-side verification of the Locks lifecycle (ADR-0019 §7):
/// `locks_verification` confirms a payment on an independently verified
/// completed result, `payment_window` expires one whose marketplace window
/// elapsed while the lifecycle stayed pending, and `locks_late_completion`
/// routes a completion verified after that expiry to `manual_review` — it is
/// never silently discarded. No client claim drives any of these edges.
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
                vec![
                    Command("payment.sandbox_advance"),
                    Server("locks_verification"),
                    Server("late_completion"),
                ],
            ),
            // A buyer's cancel of an unpaid Paykit-rail order ends its
            // payment, so money that still reaches the request is late.
            t(
                "awaiting_entitlement",
                "expired",
                vec![
                    Command("payment.sandbox_advance"),
                    Command("order.cancel_request"),
                    Server("payment_window"),
                ],
            ),
            t(
                "awaiting_entitlement",
                "manual_review",
                vec![
                    Command("payment.sandbox_advance"),
                    Server("locks_verification"),
                    Server("late_completion"),
                ],
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
            t(
                "expired",
                "manual_review",
                vec![Server("locks_late_completion"), Server("late_completion")],
            ),
            t("expired", "confirmed", vec![Server("late_completion")]),
            // The Paykit manual-review resolution (design §B.9 r12): the
            // listing seller (or the seven-day inactivity reaper, same
            // abandoned branch) resolves a `manual_review` payment exactly
            // once — `confirmed` for the paid/refunded outcomes, `expired`
            // for abandoned. `resolution_outcome` distinguishes a resolved
            // payment from a naturally confirmed/expired one.
            t(
                "manual_review",
                "confirmed",
                vec![Server("paykit_resolution")],
            ),
            t(
                "manual_review",
                "expired",
                vec![Server("paykit_resolution")],
            ),
        ],
        commands: vec![
            "payment.sandbox_advance",
            "payment.prepare_locks",
            "payment.register_locks",
        ],
        unreachable_states: vec![],
    }
}

/// Timed, limited releases (ADR-0026). The drop's state always derives from
/// server time plus `paid_quantity`, never from the seller's record: gating
/// commands apply lazy server-time transitions inside their own transaction,
/// the sweep worker applies them for untouched drops, and `drop.cancel` is
/// the seller's only direct state command. The `live → ended_sold_out`
/// transition fires inside payment confirmation when `paid_quantity`
/// reaches the total; every ended state refuses new holds and is terminal.
/// `drop.release_listings` changes no drop state — it removes the ended
/// drop's listing bindings from gating consideration.
pub fn drop_machine() -> AggregateMachine {
    AggregateMachine {
        aggregate: "drop",
        states: vec![
            "announced",
            "live",
            "ended_sold_out",
            "ended_closed",
            "ended_cancelled",
        ],
        initial: "announced",
        transitions: vec![
            t(
                "announced",
                "live",
                vec![
                    Command("inventory.reserve"),
                    Command("checkout.create"),
                    Server("drop_start"),
                ],
            ),
            // A drop whose whole window elapsed while it sat announced
            // closes directly; the sweep worker never fabricates the
            // intermediate live state.
            t("announced", "ended_closed", vec![Server("drop_end")]),
            t("live", "ended_closed", vec![Server("drop_end")]),
            t(
                "live",
                "ended_sold_out",
                vec![
                    Command("payment.sandbox_advance"),
                    Server("payment_confirmation"),
                ],
            ),
            t("announced", "ended_cancelled", vec![Command("drop.cancel")]),
            t("live", "ended_cancelled", vec![Command("drop.cancel")]),
        ],
        commands: vec![
            "drop.sync",
            "drop.cancel",
            "drop.release_listings",
            "inventory.reserve",
            "checkout.create",
        ],
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
        drop_machine(),
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
        // A cancelled paid order returns its sold quantities to available.
        // The reversal fires on the seller's approval AND on the unilateral
        // buyer exits of the pickup design (§A3/§A6: post-payment terms
        // change, bounded post-reveal withdrawal), which release inventory
        // through approve's path via `order.cancel_request`.
        assert!(can_transition(&machine, "sold", "available"));
        assert_eq!(
            listing_machine()
                .transitions
                .iter()
                .find(|t| t.from == "sold" && t.to == "available")
                .map(|t| t.via.clone()),
            Some(vec![
                Command("order.cancel_approve"),
                Command("order.cancel_request")
            ])
        );
    }

    #[test]
    fn order_machine_declares_the_pickup_path() {
        let machine = order_machine();
        assert!(machine.states.contains(&"ready_for_pickup"));
        assert_eq!(machine.initial, "pending_payment");
        // The handover path: the seller arms readiness, and either party
        // confirms the pickup from `paid` or `ready_for_pickup`.
        assert!(can_transition(&machine, "paid", "ready_for_pickup"));
        assert!(can_transition(&machine, "paid", "delivered"));
        assert!(can_transition(&machine, "ready_for_pickup", "delivered"));
        // Cancellation: the ordinary request from `ready_for_pickup`, plus
        // the unilateral exits straight to `cancelled` from `paid` and
        // `ready_for_pickup` (conditions enforced handler-side, §A6).
        assert!(can_transition(
            &machine,
            "ready_for_pickup",
            "cancel_requested"
        ));
        assert!(can_transition(&machine, "paid", "cancelled"));
        assert!(can_transition(&machine, "ready_for_pickup", "cancelled"));
        // Shipped-order behavior is unchanged: no pickup edges leak onto it.
        assert!(!can_transition(&machine, "ready_for_pickup", "shipped"));
        assert!(!can_transition(&machine, "shipped", "ready_for_pickup"));
        assert!(machine.commands.contains(&"fulfillment.mark_ready"));
        assert!(machine.commands.contains(&"fulfillment.confirm_pickup"));
    }

    /// `cancelled -> paid` through the resolution server path or an
    /// automatic late_completion when stock is free — never a client
    /// command, and never the reverse (no un-pay edge).
    #[test]
    fn paykit_resolution_edges_are_server_only_and_one_way() {
        let payments = payment_machine();
        for target in ["confirmed", "expired"] {
            let transition = payments
                .transitions
                .iter()
                .find(|t| t.from == "manual_review" && t.to == target)
                .unwrap_or_else(|| panic!("manual_review -> {target} exists"));
            assert_eq!(transition.via, vec![Server("paykit_resolution")]);
        }
        // `confirmed` is terminal: no edge out of it at all (no un-pay).
        for target in [
            "manual_review",
            "awaiting_entitlement",
            "detected",
            "expired",
        ] {
            assert!(!can_transition(&payments, "confirmed", target));
        }
        assert!(!can_transition(
            &payments,
            "expired",
            "awaiting_entitlement"
        ));
        let expired_confirmed = payments
            .transitions
            .iter()
            .find(|t| t.from == "expired" && t.to == "confirmed")
            .expect("expired -> confirmed exists");
        assert_eq!(expired_confirmed.via, vec![Server("late_completion")]);
        let orders = order_machine();
        let transition = orders
            .transitions
            .iter()
            .find(|t| t.from == "cancelled" && t.to == "paid")
            .expect("cancelled -> paid exists");
        assert_eq!(
            transition.via,
            vec![Server("paykit_resolution"), Server("late_completion")]
        );
        assert!(!can_transition(&orders, "paid", "pending_payment"));
        // Only a canceled PayPal reversal leaves `refunded_external`.
        let unrefund = orders
            .transitions
            .iter()
            .find(|t| t.from == "refunded_external" && t.to == "paid")
            .expect("refunded_external -> paid exists");
        assert_eq!(unrefund.via, vec![Server("paypal_reversal_cancelled")]);
        assert!(!can_transition(&orders, "cancelled", "pending_payment"));
        let command_triggered = transition.via.iter().any(|via| matches!(via, Command(_)));
        assert!(!command_triggered);
    }

    /// A verified PayPal refund reaches `refunded_external` from every state
    /// that holds a confirmed payment, and a canceled reversal leads back to
    /// exactly those states; no client command gains an edge, and no state
    /// is added.
    #[test]
    fn paypal_refund_edges_are_server_only_and_reversible() {
        let refundable = [
            "cancel_requested",
            "cancelled",
            "completed",
            "delivered",
            "paid",
            "ready_for_pickup",
            "return_approved",
            "return_received",
            "return_requested",
            "shipped",
        ];
        let orders = order_machine();
        let mut sources: Vec<&str> = orders
            .transitions
            .iter()
            .filter(|t| t.via.contains(&Server("paypal_refund")))
            .map(|t| {
                assert_eq!(t.to, "refunded_external");
                t.from
            })
            .collect();
        sources.sort_unstable();
        assert_eq!(sources, refundable);
        let mut restores: Vec<&str> = orders
            .transitions
            .iter()
            .filter(|t| t.via.contains(&Server("paypal_reversal_cancelled")))
            .map(|t| {
                assert_eq!(t.from, "refunded_external");
                assert_eq!(t.via, vec![Server("paypal_reversal_cancelled")]);
                t.to
            })
            .collect();
        restores.sort_unstable();
        assert_eq!(restores, refundable);
        for transition in orders.transitions.iter().filter(|t| {
            t.to == "refunded_external" && !matches!(t.from, "cancelled" | "return_received")
        }) {
            // A delivered or completed digital order also takes the
            // seller's recorded refund; the handler admits it for
            // `fulfillment = digital` only.
            let expected = if matches!(transition.from, "delivered" | "completed") {
                vec![Command("refund.record_external"), Server("paypal_refund")]
            } else {
                vec![Server("paypal_refund")]
            };
            assert_eq!(transition.via, expected, "{}", transition.from);
        }
        assert!(!orders.states.contains(&"refunded_partial"));
        assert!(!can_transition(
            &orders,
            "pending_payment",
            "refunded_external"
        ));
        let returns = return_machine();
        for from in ["requested", "approved", "received"] {
            assert!(can_transition(&returns, from, "refunded"));
            let back = returns
                .transitions
                .iter()
                .find(|t| t.from == "refunded" && t.to == from)
                .expect("restore edge exists");
            assert_eq!(back.via, vec![Server("paypal_reversal_cancelled")]);
        }
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
            8
        );
    }

    #[test]
    fn drop_machine_enforces_the_release_lifecycle() {
        let machine = drop_machine();
        assert!(can_transition(&machine, "announced", "live"));
        assert!(can_transition(&machine, "announced", "ended_closed"));
        assert!(can_transition(&machine, "announced", "ended_cancelled"));
        assert!(can_transition(&machine, "live", "ended_closed"));
        assert!(can_transition(&machine, "live", "ended_sold_out"));
        assert!(can_transition(&machine, "live", "ended_cancelled"));
        // Every ended state is terminal: a lapsed hold restocks the counters
        // but nothing reopens the drop.
        for ended in ["ended_sold_out", "ended_closed", "ended_cancelled"] {
            assert!(!can_transition(&machine, ended, "live"), "{ended}");
            assert!(!can_transition(&machine, ended, "announced"), "{ended}");
        }
        assert!(!can_transition(&machine, "announced", "ended_sold_out"));
        assert!(!can_transition(&machine, "live", "announced"));
    }

    /// Walks the vendored contract JSON on disk and holds every declared
    /// drop transition against `can_transition`, so the artifact the client
    /// pins and the Rust table the service enforces cannot drift apart.
    #[test]
    fn contract_json_drop_transitions_match_can_transition() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/state-machines.json"
        );
        let on_disk: Value = serde_json::from_str(
            &std::fs::read_to_string(path).expect("contracts/state-machines.json is readable"),
        )
        .expect("contract JSON parses");
        let aggregates = on_disk["aggregates"].as_array().expect("aggregates array");
        let drop = aggregates
            .iter()
            .find(|aggregate| aggregate["aggregate"] == "drop")
            .expect("the drop aggregate is in the contract");
        let machine = drop_machine();
        assert_eq!(drop["initial"], "announced");
        let transitions = drop["transitions"].as_array().expect("transitions array");
        assert_eq!(transitions.len(), machine.transitions.len());
        for transition in transitions {
            let from = transition["from"].as_str().expect("from state");
            let to = transition["to"].as_str().expect("to state");
            assert!(
                can_transition(&machine, from, to),
                "contract declares {from} -> {to} but the Rust table refuses it"
            );
        }
        // And the reverse: every Rust transition is in the JSON.
        for transition in &machine.transitions {
            assert!(
                transitions
                    .iter()
                    .any(|declared| declared["from"] == transition.from
                        && declared["to"] == transition.to),
                "Rust table declares {} -> {} but the contract omits it",
                transition.from,
                transition.to
            );
        }
    }

    /// Fails when `contracts/state-machines.json` is stale. Regenerate with:
    /// `cargo run -p marketplace-domain --bin emit-contracts`
    #[test]
    fn order_machine_declares_the_digital_edges() {
        let machine = order_machine();
        let delivered = machine
            .transitions
            .iter()
            .find(|t| t.from == "paid" && t.to == "delivered")
            .expect("paid -> delivered");
        assert!(delivered.via.contains(&Server("digital_delivery")));
        assert!(delivered
            .via
            .contains(&Command("fulfillment.deliver_digital")));
        assert!(delivered
            .via
            .contains(&Command("fulfillment.confirm_pickup")));
        for from in ["delivered", "completed"] {
            let refund = machine
                .transitions
                .iter()
                .find(|t| t.from == from && t.to == "refunded_external")
                .expect("refund edge");
            assert!(
                refund.via.contains(&Command("refund.record_external")),
                "{from}"
            );
        }
        // Digital orders never need a return edge of their own.
        assert!(!can_transition(&machine, "paid", "return_requested"));
    }

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
