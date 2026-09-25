//! Bitcoin binds against a live paykit-server: the marketplace bind handler,
//! activation worker, and signed client talk HTTP to the production
//! paykit-server composition (real Postgres, ephemeral pubky testnet), not
//! to the in-repo double.
//!
//! Start the server with paykit-server's `marketplace_live_harness`
//! (`paykit-server-e2e/tests/marketplace_live_harness.rs`), trusting
//! [`TEST_PAYKIT_SIGNING_SEED`], then run these tests with
//! `PAYKIT_LIVE_HANDOFF=<handoff file> cargo test -p marketplace-service
//! --test paykit_live_test -- --ignored --test-threads=1`.

mod common;

use axum::http::StatusCode;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

struct LiveHarness {
    base_url: String,
    creator_secret_hex: String,
    reader_secret_hex: String,
}

fn live_harness() -> LiveHarness {
    let path = std::env::var("PAYKIT_LIVE_HANDOFF")
        .expect("PAYKIT_LIVE_HANDOFF names the paykit-server harness handoff file");
    let handoff: Value =
        serde_json::from_slice(&std::fs::read(&path).expect("handoff file is readable"))
            .expect("handoff is JSON");
    let field = |name: &str| {
        handoff[name]
            .as_str()
            .unwrap_or_else(|| panic!("handoff is missing {name}"))
            .to_string()
    };
    LiveHarness {
        base_url: field("base_url"),
        creator_secret_hex: field("creator_secret_hex"),
        reader_secret_hex: field("reader_secret_hex"),
    }
}

/// The harness creator is the seller (claimed on paykit-server) and the
/// harness reader is the buyer (its Paykit receiver marker is published).
async fn live_parties(pool: PgPool) -> (TestApp, TestActor, TestActor) {
    let harness = live_harness();
    let app = test_app_with_live_paykit(pool, &harness.base_url).await;
    let seller = actor_from_secret(&app, &harness.creator_secret_hex).await;
    let buyer = actor_from_secret(&app, &harness.reader_secret_hex).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    (app, seller, buyer)
}

async fn create_sat_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let listing_id = format!("sat_{}", Uuid::new_v4().simple());
    let command_number = (Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64;
    let mut register = register_listing_command(&seller.pubky, &listing_id, 1, command_number);
    register["payload"]["unit_price"] =
        json!({ "amount_minor": 50_000, "currency": "SAT", "exponent": 0 });
    let (status, body) = execute(app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let aggregate = format!("listing:{}_{listing_id}", seller.pubky);
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["listing_aggregate_id"] = json!(aggregate);
    checkout["payload"]["lines"][0]["expected_revision"] = json!(1);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string()
}

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": "bitcoin" }),
    )
    .await
}

#[derive(sqlx::FromRow)]
struct PaykitPin {
    payment_method: Option<String>,
    paykit_invoice_id: Option<Uuid>,
    paykit_stack_id: Option<String>,
    paykit_stack_endpoint: Option<String>,
    paykit_activation_state: Option<String>,
    paykit_request_reference: Option<String>,
}

async fn paykit_pin(pool: &PgPool, order_id: &str) -> PaykitPin {
    sqlx::query_as(
        "SELECT payment_method, paykit_invoice_id, paykit_stack_id, paykit_stack_endpoint, \
         paykit_activation_state, paykit_request_reference FROM orders WHERE id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order id is a uuid"))
    .fetch_one(pool)
    .await
    .expect("order row exists")
}

fn live_client(app: &TestApp) -> &marketplace_service::payments::PaykitClient {
    app.state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client configured")
}

// Paykit finalizes a prepared invoice before the marketplace activates it
// (its prepare reaper, or any void that lands first). The activation worker
// sees the terminal refusal and releases the bind; the buyer pays again.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
#[ignore = "needs a live paykit-server (PAYKIT_LIVE_HANDOFF)"]
async fn live_a_rebind_after_a_void_prepares_a_new_invoice(pool: PgPool) {
    let (app, seller, buyer) = live_parties(pool.clone()).await;
    let order_id = create_sat_order(&app, &seller, &buyer).await;

    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "first bind failed: {body}");
    let first = paykit_pin(&pool, &order_id).await;
    let first_invoice = first.paykit_invoice_id.expect("first invoice pinned");
    let voided = live_client(&app)
        .void_payment_request(
            first.paykit_stack_endpoint.as_deref().expect("endpoint"),
            first_invoice,
            first.paykit_stack_id.as_deref().expect("stack id"),
            "marketplace_bind_rolled_back",
        )
        .await
        .expect("paykit voids the prepared invoice");
    assert_eq!(voided.state, "void_cancelled");

    drain_outbox(&app.pool, Some(live_client(&app)), app.clock.now(), 30)
        .await
        .expect("drain runs");
    let released = paykit_pin(&pool, &order_id).await;
    assert_eq!(released.paykit_activation_state.as_deref(), Some("voided"));
    assert_eq!(released.payment_method, None, "the bind is released");

    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the buyer's second bind must prepare a new invoice: {body}"
    );
    let second = paykit_pin(&pool, &order_id).await;
    assert_ne!(second.paykit_invoice_id, Some(first_invoice));
    assert_ne!(
        second.paykit_request_reference, first.paykit_request_reference,
        "each attempt carries its own Paykit reference"
    );
    assert_eq!(second.paykit_activation_state.as_deref(), Some("preparing"));

    drain_outbox(&app.pool, Some(live_client(&app)), app.clock.now(), 30)
        .await
        .expect("drain runs");
    let activated = paykit_pin(&pool, &order_id).await;
    assert_eq!(activated.paykit_activation_state.as_deref(), Some("active"));
}

// Phase 1 succeeds at paykit, the bind transaction then rolls back, and the
// courtesy void cancels the prepared invoice. The buyer's retry must not
// replay the voided attempt's identity.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
#[ignore = "needs a live paykit-server (PAYKIT_LIVE_HANDOFF)"]
async fn live_a_retry_after_a_rolled_back_bind_prepares_a_new_invoice(pool: PgPool) {
    install_log_capture();
    let (app, seller, buyer) = live_parties(pool.clone()).await;
    let order_id = create_sat_order(&app, &seller, &buyer).await;

    fail_activation_intent_inserts(&pool).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "the injected failure rolls the bind back: {body}"
    );
    restore_activation_intent_inserts(&pool).await;
    assert_eq!(paykit_pin(&pool, &order_id).await.payment_method, None);
    let mut delivered = false;
    for _ in 0..100 {
        if captured_logs().contains("courtesy void delivered") {
            delivered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(delivered, "the courtesy void reached paykit-server");

    let (status, body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the retry must prepare a new invoice: {body}"
    );
    let bound = paykit_pin(&pool, &order_id).await;
    assert_eq!(bound.payment_method.as_deref(), Some("bitcoin"));
    assert_eq!(bound.paykit_activation_state.as_deref(), Some("preparing"));

    drain_outbox(&app.pool, Some(live_client(&app)), app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(
        paykit_pin(&pool, &order_id)
            .await
            .paykit_activation_state
            .as_deref(),
        Some("active")
    );
}
