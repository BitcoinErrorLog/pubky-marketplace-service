//! The Paykit client against upstream `pubky/paykit-server` (rc9 plus #55):
//! the request-preimage signature and the signed `POST /setup/status`
//! seller-readiness call, selected by `PaykitApi::Upstream`. The fork API
//! stays the default and is covered by the existing suites.
//!
//! Upstream references (`pubky/paykit-server` PR #55 head `a109148`):
//! - verifier and preimage: `paykit-server/src/http/auth.rs`
//! - `/setup/status`: `paykit-server/src/http/setup_status.rs` and
//!   `paykit-server/tests/setup_status.rs` (the test vector below)

mod common;

use axum::http::StatusCode;
use base64::Engine;
use common::paykit_review::{create_sat_order, enable_bitcoin as enable_bitcoin_for_review};
use common::*;
use ed25519_dalek::{Signer, SigningKey, Verifier};
use marketplace_service::clock::Clock;
use marketplace_service::payments::{
    paykit_signature_preimage, PaykitApi, PaykitClient, PaykitLifecycleTarget, PaykitPrepared,
    PaykitRequestError, PaykitStatusOutcome,
};
use marketplace_service::resolve_delivery::deliver_due_resolve_rows;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;

const SELLER: &str = "gy1wnkhfwezwdnawnur1bc3kw1x3jf5ggjj3cm37e31i5ntq3pco";
const REFERENCE: &str = "0R8Y7ZQ3M5N9K2VJ6W4X1T8S0P";

#[derive(sqlx::FromRow)]
struct PersistedUpstreamBind {
    paykit_invoice_id: Option<uuid::Uuid>,
    paykit_api: Option<String>,
    paykit_stack_id: Option<String>,
    paykit_stack_endpoint: Option<String>,
    paykit_activation_state: Option<String>,
}

/// `paykit-server/tests/setup_status.rs`: `SigningKey::from_bytes(&[7; 32])`,
/// `CREATOR`, `canonical_body()`, and `signed_request()` over
/// `signature_preimage("POST", "/setup/status", &body)`.
const UPSTREAM_CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const UPSTREAM_BODY: &str =
    r#"{"creator":"pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"}"#;
/// `signature_preimage("POST", "/setup/status", UPSTREAM_BODY)` and its
/// Ed25519 signature under that key, produced by upstream's own
/// `paykit_server::http::auth::signature_preimage` at PR #55 head `a109148`.
const UPSTREAM_PREIMAGE_HEX: &str = "7061796b69742d687474702d7369676e61747572652d763100504f5354002f73657475702f737461747573007b2263726561746f72223a227075626b79746b7271387a6d77623861336d396b313563737533713137716d6667716e703964736b6272673975713172796470797870377179227d";
const UPSTREAM_SIGNATURE: &str =
    "uGwZVTdCxIRugXQo2gui7dAz7Ude3mobvcretytsMx55Iyr3rCzLEm-rwpfJcphCTPOj6VAzD4sNMr2wT60OAw";

fn upstream_client(paykit: &FakePaykit) -> PaykitClient {
    PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream)
}

async fn upstream_paykit() -> FakePaykit {
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    paykit
}

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn preimage_and_signature_match_the_bytes_upstream_signs_in_its_own_test() {
    assert_eq!(
        UPSTREAM_BODY,
        format!(r#"{{"creator":"{UPSTREAM_CREATOR}"}}"#)
    );
    let preimage = paykit_signature_preimage("POST", "/setup/status", UPSTREAM_BODY.as_bytes());
    assert_eq!(hex_of(&preimage), UPSTREAM_PREIMAGE_HEX);
    assert!(
        preimage.starts_with(b"paykit-http-signature-v1\0POST\0/setup/status\0{"),
        "domain, upper-case method, path and raw body, NUL separated"
    );

    let key = SigningKey::from_bytes(&[7; 32]);
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes());
    assert_eq!(signature, UPSTREAM_SIGNATURE);
}

#[test]
fn the_preimage_binds_method_path_and_body() {
    let body = UPSTREAM_BODY.as_bytes();
    let base = paykit_signature_preimage("POST", "/setup/status", body);
    assert_eq!(
        paykit_signature_preimage("post", "/setup/status", body),
        base,
        "the method is upper-cased"
    );
    for other in [
        paykit_signature_preimage("GET", "/setup/status", body),
        paykit_signature_preimage("POST", "/transactions/status", body),
        paykit_signature_preimage("POST", "/setup/status", br#"{"creator":"x"}"#),
        body.to_vec(),
    ] {
        assert_ne!(other, base);
    }
    let key = SigningKey::from_bytes(&[7; 32]);
    let signature = key.sign(&base);
    assert!(
        key.verifying_key().verify(body, &signature).is_err(),
        "a signature over the preimage never verifies over the bare body"
    );
}

#[test]
fn the_api_setting_parses_fork_and_upstream_only() {
    assert_eq!(PaykitApi::default(), PaykitApi::Fork);
    assert_eq!(PaykitApi::parse("fork").unwrap(), PaykitApi::Fork);
    assert_eq!(
        PaykitApi::parse(" upstream\n").unwrap(),
        PaykitApi::Upstream
    );
    for bad in ["", "Upstream", "rc9", "both"] {
        assert!(PaykitApi::parse(bad).is_err(), "{bad:?}");
    }
}

#[tokio::test]
async fn setup_status_asks_for_btc_with_a_signed_closed_canonical_body() {
    let paykit = upstream_paykit().await;
    paykit.set_claimed(SELLER);
    assert_eq!(
        upstream_client(&paykit).seller_ready(SELLER).await,
        Ok(true)
    );

    let calls = paykit.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "POST");
    assert_eq!(calls[0].path, "/setup/status");
    assert_eq!(
        calls[0].body,
        json!({ "asset": "BTC", "creator": format!("pubky{SELLER}") })
    );
}

#[tokio::test]
async fn bitcoin_is_offered_only_on_exactly_ready() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    for (status, expected) in [
        ("ready", Ok(true)),
        ("setup_required", Ok(false)),
        ("unavailable", Err(PaykitRequestError::Unavailable)),
        ("Ready", Err(PaykitRequestError::Unavailable)),
        ("degraded", Err(PaykitRequestError::Unavailable)),
        ("", Err(PaykitRequestError::Unavailable)),
    ] {
        paykit.set_setup_status(SELLER, status);
        assert_eq!(client.seller_ready(SELLER).await, expected, "{status:?}");
    }
}

#[tokio::test]
async fn a_non_2xx_or_off_contract_answer_is_an_outage_never_ready() {
    let paykit = upstream_paykit().await;
    paykit.set_claimed(SELLER);
    let client = upstream_client(&paykit);
    for status in [400, 401, 404, 429, 500, 503] {
        paykit.fail_setup_status_with(status);
        assert_eq!(
            client.seller_ready(SELLER).await,
            Err(PaykitRequestError::Unavailable),
            "{status}"
        );
    }
    paykit.clear_setup_status_failure();
    for body in [
        json!({}),
        json!({ "status": 7 }),
        json!({ "status": "ready", "extra": true }),
        json!(["ready"]),
    ] {
        paykit.set_setup_status_body(body.clone());
        assert_eq!(
            client.seller_ready(SELLER).await,
            Err(PaykitRequestError::Unavailable),
            "{body}"
        );
    }
}

#[tokio::test]
async fn an_unreachable_paykit_server_is_an_outage() {
    let client = PaykitClient::new("http://127.0.0.1:1", TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream);
    assert_eq!(
        client.seller_ready(SELLER).await,
        Err(PaykitRequestError::Unavailable)
    );
}

/// `paykit-server/src/http/health.rs` `ReadyResponse` at rc9 and #55: the
/// rail gate is the same public `GET /health/ready` in both APIs.
fn upstream_ready_body(status: &str, electrum: &str) -> Value {
    json!({
        "status": status,
        "postgres": "ready",
        "electrum": electrum,
        "paykit_delivery": "ready",
        "outbox": "ready",
    })
}

#[tokio::test]
async fn the_rail_gate_reads_upstream_health_ready_in_upstream_mode() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    for (body, expected) in [
        (upstream_ready_body("ready", "ready"), true),
        (upstream_ready_body("degraded", "ready"), false),
        (upstream_ready_body("degraded", "degraded"), false),
    ] {
        paykit.set_rail_health(body.clone());
        assert_eq!(client.rail_health().await, Ok(expected), "{body}");
    }
    paykit.set_rail_health(upstream_ready_body("not_ready", "not_ready"));
    paykit.fail_rail_health();
    assert_eq!(
        client.rail_health().await,
        Err(PaykitRequestError::Unavailable),
        "upstream answers 503 when not ready"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_public_rail_flag_follows_upstream_health_ready(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    for (body, expected) in [
        (upstream_ready_body("ready", "ready"), true),
        (upstream_ready_body("degraded", "ready"), false),
    ] {
        paykit.set_rail_health(body);
        app.clock.advance_seconds(16);
        let (status, config) = get_public_config(&app, &new_actor(&app).await.pubky).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(config["bitcoin_offer_available"], json!(expected));
    }
}

#[tokio::test]
async fn upstream_signs_every_request_with_the_preimage() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    assert_eq!(
        client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Unavailable
    );
    assert!(
        paykit.calls().is_empty(),
        "a Marketplace UUID is never guessed to be a Locks bundle_id"
    );
}

#[tokio::test]
async fn upstream_lifecycle_uses_the_closed_four_command_contract() {
    let paykit = upstream_paykit().await;
    let client = upstream_client(&paykit);
    let order_id = uuid::Uuid::new_v4();
    let prepared = client
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            order_id,
            1,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("prepare accepted");
    let PaykitPrepared::Upstream {
        invoice_id,
        total_sats,
        ..
    } = prepared
    else {
        panic!("upstream response stays typed upstream")
    };
    let prepare = &paykit.calls()[0];
    assert_eq!(prepare.path, "/marketplace/payment-requests/prepare");
    assert_eq!(prepare.body["amount_sats"], json!(42_000));
    assert_eq!(prepare.body["payment_window_seconds"], json!(3_600));
    assert_eq!(prepare.body["creator"], json!(format!("pubky{SELLER}")));
    assert_eq!(
        prepare.body["reader"],
        json!("pubkyw3g1m3s5rbuyer1111111111111111111111111111111111111111")
    );
    assert_eq!(
        prepare
            .body
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "amount_sats",
            "creator",
            "operation_id",
            "payment_window_seconds",
            "reader",
            "reference"
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );
    let reference = prepare.body["reference"].as_str().unwrap();
    assert_eq!(
        uuid::Uuid::parse_str(reference).unwrap().get_version_num(),
        4
    );
    assert_eq!(
        prepare.body["operation_id"],
        json!(format!("marketplace-payment:{reference}:1"))
    );

    let target = PaykitLifecycleTarget::Upstream {
        creator: SELLER.to_string(),
    };
    client
        .activate_payment_request(&target, invoice_id, total_sats, 99)
        .await
        .expect("activate accepted");
    let activate = &paykit.calls()[1];
    assert_eq!(activate.path, "/marketplace/payment-requests/activate");
    assert_eq!(
        activate.body,
        json!({"creator": format!("pubky{SELLER}"), "invoice_id": invoice_id, "total_sats": total_sats})
    );

    let resolved = client
        .resolve_payment_request(&target, invoice_id, "paid_manually", chrono::Utc::now())
        .await
        .expect("resolve accepted");
    assert_eq!(resolved.resolved.unwrap().outcome, "paid_manually");
    let resolve = &paykit.calls()[2];
    assert_eq!(resolve.path, "/marketplace/payment-requests/resolve");
    assert_eq!(
        resolve.body,
        json!({"creator": format!("pubky{SELLER}"), "invoice_id": invoice_id, "outcome": "paid_manually"})
    );

    let second = client
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            order_id,
            2,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("second prepare accepted");
    let PaykitPrepared::Upstream {
        invoice_id: second_invoice,
        ..
    } = second
    else {
        panic!("upstream response stays typed upstream")
    };
    client
        .void_payment_request(
            &target,
            second_invoice,
            "fork-only reason is not serialized",
        )
        .await
        .expect("void accepted");
    let void = &paykit.calls()[4];
    assert_eq!(void.path, "/marketplace/payment-requests/void");
    assert_eq!(
        void.body,
        json!({"creator": format!("pubky{SELLER}"), "invoice_id": second_invoice})
    );
}

#[tokio::test]
async fn durable_target_not_current_config_selects_lifecycle_signature_framing() {
    let upstream_server = upstream_paykit().await;
    let prepared = upstream_client(&upstream_server)
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            uuid::Uuid::new_v4(),
            1,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("upstream prepare accepted");
    let PaykitPrepared::Upstream {
        invoice_id,
        total_sats,
        ..
    } = prepared
    else {
        panic!("upstream response")
    };
    let rollback_client = PaykitClient::new(&upstream_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("fork-configured client");
    let upstream_target = PaykitLifecycleTarget::Upstream {
        creator: SELLER.to_string(),
    };
    rollback_client
        .activate_payment_request(&upstream_target, invoice_id, total_sats, 1)
        .await
        .expect("durable upstream target keeps preimage signing after rollback");
    rollback_client
        .resolve_payment_request(
            &upstream_target,
            invoice_id,
            "paid_manually",
            chrono::Utc::now(),
        )
        .await
        .expect("durable upstream resolve keeps preimage signing");
    let prepared_to_void = upstream_client(&upstream_server)
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            uuid::Uuid::new_v4(),
            1,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("second upstream prepare accepted");
    let PaykitPrepared::Upstream {
        invoice_id: upstream_void_invoice,
        ..
    } = prepared_to_void
    else {
        panic!("upstream response")
    };
    let upstream_voided = rollback_client
        .void_payment_request(&upstream_target, upstream_void_invoice, "fork-only reason")
        .await
        .expect("durable upstream void keeps preimage signing after rollback");
    assert_eq!(upstream_voided.state, "voided");

    let fork_server = spawn_fake_paykit().await;
    let prepared = PaykitClient::new(&fork_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("fork client")
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            uuid::Uuid::new_v4(),
            1,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("fork prepare accepted");
    let PaykitPrepared::Fork {
        invoice_id,
        stack_id,
        total_sats,
        ..
    } = prepared
    else {
        panic!("fork response")
    };
    let cutover_client = PaykitClient::new(&fork_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("client")
        .with_api(PaykitApi::Upstream);
    let fork_target = PaykitLifecycleTarget::Fork {
        endpoint: fork_server.base_url.clone(),
        stack_id,
    };
    cutover_client
        .activate_payment_request(&fork_target, invoice_id, total_sats, 1)
        .await
        .expect("durable fork target keeps body-only signing after cutover");
    let prepared_to_void = PaykitClient::new(&fork_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("fork client")
        .create_payment_request(
            SELLER,
            "w3g1m3s5rbuyer1111111111111111111111111111111111111111",
            uuid::Uuid::new_v4(),
            1,
            42_000,
            chrono::Utc::now() + chrono::Duration::hours(1),
            3_600,
        )
        .await
        .expect("second fork prepare accepted");
    let PaykitPrepared::Fork {
        invoice_id: fork_void_invoice,
        stack_id: fork_void_stack_id,
        ..
    } = prepared_to_void
    else {
        panic!("fork response")
    };
    let fork_void_target = PaykitLifecycleTarget::Fork {
        endpoint: fork_server.base_url.clone(),
        stack_id: fork_void_stack_id,
    };
    let fork_voided = cutover_client
        .void_payment_request(&fork_void_target, fork_void_invoice, "order_cancelled")
        .await
        .expect("durable fork void keeps body-only signing after cutover");
    assert_eq!(fork_voided.state, "void_cancelled");
}

#[tokio::test]
async fn the_two_signature_schemes_do_not_verify_against_each_other() {
    let upstream_server = upstream_paykit().await;
    let fork_client = PaykitClient::new(&upstream_server.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client");
    upstream_server.set_status(
        REFERENCE,
        json!({ "status": "undetected", "confirmations": 0, "amount_matched": false }),
    );
    assert_eq!(
        fork_client.payment_status(SELLER, REFERENCE).await,
        PaykitStatusOutcome::Unavailable,
        "a body-only signature is a 401 on upstream"
    );
    assert!(upstream_server.calls().is_empty());

    let fork_server = spawn_fake_paykit().await;
    fork_server.set_claimed(SELLER);
    let upstream_client = upstream_client(&fork_server);
    assert_eq!(
        upstream_client.seller_ready(SELLER).await,
        Err(PaykitRequestError::Unavailable),
        "a fork server verifies the bare body, so the preimage signature is a 401"
    );
    assert!(fork_server.calls().is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn upstream_bind_persists_api_and_recovers_activation_after_config_rollback(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin_for_review(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;

    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    let order_id = uuid::Uuid::parse_str(&order.order_id).expect("order id");
    let persisted: PersistedUpstreamBind = sqlx::query_as(
        "SELECT paykit_invoice_id, paykit_api, paykit_stack_id, \
         paykit_stack_endpoint, paykit_activation_state FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("bound order");
    let invoice_id = persisted
        .paykit_invoice_id
        .expect("server-issued invoice persisted");
    assert_eq!(persisted.paykit_api.as_deref(), Some("upstream"));
    assert_eq!(persisted.paykit_stack_id, None);
    assert_eq!(persisted.paykit_stack_endpoint, None);
    assert_eq!(
        persisted.paykit_activation_state.as_deref(),
        Some("preparing")
    );

    let payload: Value = sqlx::query_scalar(
        "SELECT payload FROM outbox WHERE kind = 'paykit.activate' \
         AND payload->>'order_id' = $1",
    )
    .bind(order_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("durable activation intent");
    assert_eq!(payload["paykit_api"], json!("upstream"));
    assert!(
        payload.get("creator").is_none(),
        "activation re-reads creator from the order"
    );
    assert!(payload.get("stack_id").is_none());
    assert!(payload.get("stack_endpoint").is_none());

    let rollback_client = PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("fork-configured replacement client");
    drain_outbox(&pool, Some(&rollback_client), app.clock.now(), 30)
        .await
        .expect("queued upstream activation drains after rollback");
    let activation: Option<String> =
        sqlx::query_scalar("SELECT paykit_activation_state FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("activation state");
    assert_eq!(activation.as_deref(), Some("active"));
    assert_eq!(
        paykit.invoice(invoice_id).expect("upstream invoice").state,
        "active"
    );

    let event_id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM events ORDER BY occurred_at DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("bind event");
    let payment_id = uuid::Uuid::parse_str(&order.payment_id).expect("payment id");
    let now = app.clock.now();
    sqlx::query(
        "INSERT INTO paykit_resolve_outbox (order_id, payment_id, event_id, invoice_id, \
         resolution, resolved_at, paykit_api, creator_pubky, stack_id, stack_endpoint, \
         next_attempt_at, delivery_deadline, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 'paid_manually', $5, 'upstream', $6, NULL, NULL, \
         $5, $7, $5, $5)",
    )
    .bind(order_id)
    .bind(payment_id)
    .bind(event_id)
    .bind(invoice_id)
    .bind(now)
    .bind(&seller.pubky)
    .bind(now + chrono::Duration::hours(1))
    .execute(&pool)
    .await
    .expect("migration accepts upstream resolve target without fork pins");
    assert_eq!(
        deliver_due_resolve_rows(&app.state, &rollback_client, now, 30)
            .await
            .expect("durable upstream resolve drains after rollback"),
        1
    );
    let delivery_state: String =
        sqlx::query_scalar("SELECT delivery_state FROM paykit_resolve_outbox WHERE order_id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("resolve row");
    assert_eq!(delivery_state, "delivered");
    assert_eq!(
        paykit.resolution(invoice_id).map(|value| value.0),
        Some("paid_manually".to_string())
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn upstream_expiry_queues_and_delivers_void_without_fork_pins(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin_for_review(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let order_id = uuid::Uuid::parse_str(&order.order_id).expect("order id");
    let invoice_id: uuid::Uuid =
        sqlx::query_scalar("SELECT paykit_invoice_id FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("invoice persisted");

    let later = app.clock.now() + chrono::Duration::hours(3);
    assert!(
        expire_due_payment_windows(&app.state, later)
            .await
            .expect("expiry runs")
            >= 1
    );
    let (api, stack_id, endpoint, activation): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT paykit_api, paykit_stack_id, paykit_stack_endpoint, \
         paykit_activation_state FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("expired order");
    assert_eq!(api.as_deref(), Some("upstream"));
    assert_eq!(stack_id, None);
    assert_eq!(endpoint, None);
    assert_eq!(activation.as_deref(), Some("voided"));
    assert_eq!(
        paykit.invoice(invoice_id).expect("upstream invoice").state,
        "voided"
    );
    assert!(paykit.calls().iter().any(|call| {
        call.path == "/marketplace/payment-requests/void"
            && call.body
                == json!({"creator": format!("pubky{}", seller.pubky), "invoice_id": invoice_id})
    }));
}

async fn get_public_config(app: &TestApp, seller_pubky: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v0/sellers/{seller_pubky}/payment-config"),
        None,
        &Value::Null,
    )
    .await
}

async fn enable_bitcoin(app: &TestApp, token: &str) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_config_offers_bitcoin_only_when_setup_status_is_ready(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;

    for (status, expected) in [
        ("ready", true),
        ("setup_required", false),
        ("ready", true),
        ("unavailable", true),
    ] {
        paykit.set_setup_status(&seller.pubky, status);
        app.clock.advance_seconds(16);
        let (http, body) = get_public_config(&app, &seller.pubky).await;
        assert_eq!(http, StatusCode::OK);
        assert_eq!(body["bitcoin_available"], json!(expected), "{status}");
        assert_eq!(
            body["bitcoin_offer_available"],
            json!(true),
            "the rail-wide gate is independent of the seller state"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unavailable_serves_the_last_known_value_until_stale_then_closes(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(body["bitcoin_available"], json!(true));

    paykit.set_setup_status(&seller.pubky, "unavailable");
    app.clock.advance_seconds(16);
    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(
        body["bitcoin_available"],
        json!(true),
        "within the stale window the last answer holds"
    );

    app.clock.advance_seconds(
        app.state.config.paykit_rail_stale_seconds + app.state.config.paykit_poll_seconds,
    );
    let (http, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(http, StatusCode::OK);
    assert_eq!(body["bitcoin_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_non_2xx_setup_status_never_offers_bitcoin_to_a_new_seller(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    enable_bitcoin(&app, &seller.token).await;
    paykit.set_claimed(&seller.pubky);
    paykit.fail_setup_status_with(503);

    let (http, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(
        http,
        StatusCode::OK,
        "the public read never turns into a 503"
    );
    assert_eq!(body["bitcoin_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn setup_status_is_not_called_for_a_seller_who_disabled_bitcoin(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    paykit.set_claimed(&seller.pubky);

    let (_, body) = get_public_config(&app, &seller.pubky).await;
    assert_eq!(body["bitcoin_available"], json!(false));
    assert_eq!(body["paypal_available"], json!(true));
    assert!(
        paykit.calls().is_empty(),
        "PayPal-only sellers never reach paykit-server"
    );
}
