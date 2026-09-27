//! The Paykit client reads both paykit-server formats through the REAL
//! client over HTTP: the fork's (`BitcoinErrorLog/paykit-server@8fae84b`,
//! the shape the production `4d552ca` image serves) and upstream's
//! (`pubky/paykit-server@722ef26` master and `@7f1fec9`
//! `feat/lock-payment-draining`, identical for both routes).
//!
//! Fixture sources:
//! - fork `/health/ready`: `paykit-server/src/http/health.rs` `ReadyResponse`
//! - fork `/transactions/status`: `paykit-server/src/http/status.rs`
//!   `StatusResponse`
//! - upstream `/health/ready`: `paykit-server/src/http/health.rs`
//!   `ReadyResponse` (`status` is `ready` only when every component is,
//!   `runtime.rs::readiness`; `not_ready` answers 503)
//! - upstream `/transactions/status`: `paykit-server/src/http/status.rs`
//!   `StatusResponse`

mod common;

use common::*;
use marketplace_service::payments::{
    PaykitClient, PaykitDeliveryState, PaykitObservation, PaykitStatusFacts, PaykitStatusOutcome,
};
use serde_json::{json, Value};

const SELLER: &str = "gy1wnkhfwezwdnawnur1bc3kw1x3jf5ggjj3cm37e31i5ntq3pco";
const REFERENCE: &str = "0R8Y7ZQ3M5N9K2VJ6W4X1T8S0P";

fn client(paykit: &FakePaykit) -> PaykitClient {
    PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED).expect("paykit client")
}

fn fork_readiness(status: &str, electrum_state: &str, bitcoin_offer_available: bool) -> Value {
    json!({
        "status": status,
        "stack_id": "paykit-fork:7d0c1f5e",
        "postgres": "ready",
        "electrum": {
            "state": electrum_state,
            "available": electrum_state == "ready",
            "tip_height": 917_402,
            "tip_age_secs": 312,
            "last_probe_at": 1_790_500_000u64,
            "genesis_ok": true,
        },
        "bitcoin_creation_enabled": true,
        "bitcoin_offer_available": bitcoin_offer_available,
        "electrum_tip_height": 917_402,
        "electrum_tip_age_seconds": 312,
        "paykit_delivery": "degraded",
        "outbox": "ready",
        "outbox_terminal_failure_count": 0,
        "outbox_oldest_terminal_failure_age_seconds": null,
        "outbox_terminal_failures_by_class": {},
        "outbox_link_establishment_max_attempts": 8,
        "outbox_link_establishment_max_age_seconds": 3600,
    })
}

fn upstream_readiness(status: &str, electrum: &str, paykit_delivery: &str, outbox: &str) -> Value {
    json!({
        "status": status,
        "postgres": "ready",
        "electrum": electrum,
        "paykit_delivery": paykit_delivery,
        "outbox": outbox,
    })
}

fn fork_status(status: &str, confirmations: u32, amount_matched: bool) -> Value {
    json!({
        "status": status,
        "confirmations": confirmations,
        "amount_matched": amount_matched,
        "late_settlement": false,
        "allocation_mode": "exclusive",
        "contract_version": "paykit.bitcoin_status/v2",
        "delivery_generation": "7",
        "delivery_revision": 3,
        "paykit_delivery_state": "delivered",
    })
}

fn upstream_status(status: &str, confirmations: u32, amount_matched: bool) -> Value {
    json!({
        "status": status,
        "confirmations": confirmations,
        "amount_matched": amount_matched,
    })
}

#[tokio::test]
async fn fork_readiness_answers_from_bitcoin_offer_available_whatever_the_status() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    for (body, expected) in [
        (fork_readiness("degraded", "ready", true), true),
        (fork_readiness("ready", "ready", false), false),
        (fork_readiness("degraded", "ready", false), false),
        (fork_readiness("ready", "degraded", true), true),
    ] {
        paykit.set_rail_health(body.clone());
        assert_eq!(
            client.rail_health().await.expect("readiness reads"),
            expected,
            "{body}"
        );
    }
}

#[tokio::test]
async fn upstream_readiness_offers_bitcoin_only_when_status_is_ready() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    for (body, expected) in [
        (upstream_readiness("ready", "ready", "ready", "ready"), true),
        (
            upstream_readiness("degraded", "ready", "degraded", "ready"),
            false,
        ),
        (
            upstream_readiness("degraded", "ready", "ready", "degraded"),
            false,
        ),
        (
            upstream_readiness("degraded", "degraded", "ready", "ready"),
            false,
        ),
    ] {
        paykit.set_rail_health(body.clone());
        assert_eq!(
            client.rail_health().await.expect("readiness reads"),
            expected,
            "{body}"
        );
    }

    paykit.set_rail_health(upstream_readiness(
        "not_ready",
        "not_ready",
        "ready",
        "ready",
    ));
    paykit.fail_rail_health();
    assert!(
        client.rail_health().await.is_err(),
        "upstream not_ready answers 503, a refresh failure"
    );
}

#[tokio::test]
async fn fork_status_v2_bodies_parse_unchanged() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);

    paykit.set_status(REFERENCE, fork_status("confirmed", 2, true));
    assert_eq!(
        client.payment_status_with_delivery(SELLER, REFERENCE).await,
        (
            PaykitStatusOutcome::Confirmed {
                amount_matched: true,
                facts: PaykitStatusFacts {
                    allocation_mode: "exclusive".to_string(),
                    late_settlement: false,
                    observation: PaykitObservation {
                        txid: None,
                        observed_sats: None,
                        confirmations: Some(2),
                    },
                },
            },
            Some(PaykitDeliveryState::Delivered),
        )
    );

    let mut late = fork_status("detected", 0, true);
    late["late_settlement"] = json!(true);
    late["allocation_mode"] = json!("shared_manual");
    paykit.set_status(REFERENCE, late);
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Detected {
            facts: PaykitStatusFacts {
                allocation_mode: "shared_manual".to_string(),
                late_settlement: true,
                observation: PaykitObservation {
                    txid: None,
                    observed_sats: None,
                    confirmations: Some(0),
                },
            },
        }
    );

    paykit.set_status(REFERENCE, fork_status("undetected", 0, false));
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Undetected
    );
}

#[tokio::test]
async fn fork_status_contract_violations_still_fail_closed() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);

    // A fork body that lost only its mandatory late flag is not an upstream
    // body: the fork-only fields keep it on the strict path.
    let mut missing_late = fork_status("undetected", 0, false);
    missing_late
        .as_object_mut()
        .expect("object")
        .remove("late_settlement");
    let mut wrong_version = fork_status("confirmed", 2, true);
    wrong_version["contract_version"] = json!("paykit.bitcoin_status/v1");
    let mut unknown_mode = fork_status("confirmed", 2, true);
    unknown_mode["allocation_mode"] = json!("pooled");
    let mut null_late = fork_status("undetected", 0, false);
    null_late["late_settlement"] = Value::Null;
    let version_only = json!({
        "status": "undetected",
        "confirmations": 0,
        "amount_matched": false,
        "contract_version": "paykit.bitcoin_status/v2",
    });

    for body in [
        missing_late,
        wrong_version,
        unknown_mode,
        null_late,
        version_only,
    ] {
        paykit.set_status(REFERENCE, body.clone());
        assert_eq!(
            client.payment_status(SELLER, REFERENCE).await,
            PaykitStatusOutcome::Unavailable,
            "{body}"
        );
    }
}

#[tokio::test]
async fn upstream_undetected_is_undetected() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    paykit.set_status(REFERENCE, upstream_status("undetected", 0, false));
    assert_eq!(
        client.payment_status_with_delivery(SELLER, REFERENCE).await,
        (PaykitStatusOutcome::Undetected, None)
    );
}

#[tokio::test]
async fn upstream_detected_and_confirmed_fail_closed_without_late_and_mode_facts() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    for body in [
        upstream_status("detected", 0, true),
        upstream_status("confirmed", 3, true),
        upstream_status("confirmed", 3, false),
    ] {
        paykit.set_status(REFERENCE, body.clone());
        assert_eq!(
            client.payment_status_with_delivery(SELLER, REFERENCE).await,
            (PaykitStatusOutcome::Unavailable, None),
            "{body}"
        );
    }
}

#[tokio::test]
async fn off_contract_upstream_shaped_bodies_fail_closed() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    let mut extra_field = upstream_status("undetected", 0, false);
    extra_field["payment_state"] = json!("expired");
    let mut missing_confirmations = upstream_status("undetected", 0, false);
    missing_confirmations
        .as_object_mut()
        .expect("object")
        .remove("confirmations");
    let mut negative_confirmations = upstream_status("undetected", 0, false);
    negative_confirmations["confirmations"] = json!(-1);
    for body in [
        upstream_status("expired", 0, false),
        extra_field,
        missing_confirmations,
        negative_confirmations,
    ] {
        paykit.set_status(REFERENCE, body.clone());
        assert_eq!(
            client.payment_status(SELLER, REFERENCE).await,
            PaykitStatusOutcome::Unavailable,
            "{body}"
        );
    }
}

#[tokio::test]
async fn a_404_is_not_found_on_either_server() {
    let paykit = spawn_fake_paykit().await;
    let client = client(&paykit);
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::NotFound
    );
    paykit.fail_status_with(404);
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::NotFound
    );
}
