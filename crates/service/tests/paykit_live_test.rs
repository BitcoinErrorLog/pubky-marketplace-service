//! Cross-process acceptance against upstream paykit-server production routes,
//! PostgreSQL stores, workers, ephemeral Pubky testnet, and real SDK delivery.
//!
//! Start `paykit-server-e2e`'s ignored `marketplace_live_harness`, then run:
//!
//! `PAYKIT_LIVE_HANDOFF=<handoff> cargo test -p marketplace-service \
//!   --test paykit_live_test -- --ignored --test-threads=1`

mod common;

use std::{path::PathBuf, time::Duration};

use axum::http::StatusCode;
use common::*;
use marketplace_service::{
    clock::Clock,
    payments::PaykitPrepared,
    workers::{drain_outbox, verify_due_paykit_payments},
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

struct LiveHarness {
    base_url: String,
    paykit_database_url: String,
    creator_secret_hex: String,
    reader_secret_hex: String,
    stop: PathBuf,
}

impl Drop for LiveHarness {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.stop, b"stop");
    }
}

fn live_harness() -> LiveHarness {
    let path = PathBuf::from(
        std::env::var("PAYKIT_LIVE_HANDOFF")
            .expect("PAYKIT_LIVE_HANDOFF names the Paykit harness handoff file"),
    );
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
        paykit_database_url: field("paykit_database_url"),
        creator_secret_hex: field("creator_secret_hex"),
        reader_secret_hex: field("reader_secret_hex"),
        stop: PathBuf::from(format!("{}.stop", path.display())),
    }
}

async fn live_parties(pool: PgPool, harness: &LiveHarness) -> (TestApp, TestActor, TestActor) {
    let app = test_app_with_live_paykit(pool, &harness.base_url).await;
    assert_eq!(
        app.state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref())
            .expect("Paykit client configured")
            .api(),
        marketplace_service::payments::PaykitApi::Upstream
    );
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

async fn create_sat_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> Uuid {
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
    Uuid::parse_str(
        body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id present"),
    )
    .unwrap()
}

async fn bind_bitcoin(app: &TestApp, token: &str, order_id: Uuid) -> (StatusCode, Value) {
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
struct MarketplacePin {
    seller_pubky: String,
    payment_method: Option<String>,
    paykit_invoice_id: Option<Uuid>,
    paykit_api: Option<String>,
    paykit_stack_id: Option<String>,
    paykit_stack_endpoint: Option<String>,
    paykit_activation_state: Option<String>,
    paykit_total_sats: Option<i64>,
    paykit_delivery_state: Option<String>,
    paykit_last_checked_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn marketplace_pin(pool: &PgPool, order_id: Uuid) -> MarketplacePin {
    sqlx::query_as(
        "SELECT seller_pubky, payment_method, paykit_invoice_id, paykit_api, paykit_stack_id, \
         paykit_stack_endpoint, paykit_activation_state, paykit_total_sats \
         , paykit_delivery_state, paykit_last_checked_at \
         FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order row exists")
}

fn live_client(app: &TestApp) -> &marketplace_service::payments::PaykitClient {
    app.state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("Paykit client configured")
}

async fn wait_for_paykit_states(pool: &PgPool) -> Vec<(Uuid, String)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let rows = sqlx::query_as::<_, (Uuid, String)>(
            "SELECT id, state FROM marketplace_payment_preparations ORDER BY prepared_at",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        if rows.len() == 2
            && rows.iter().any(|(_, state)| state == "active")
            && rows.iter().any(|(_, state)| state == "voided")
        {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Paykit lifecycle did not reach one active and one voided row: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
#[ignore = "needs protocol-real upstream Paykit harness (PAYKIT_LIVE_HANDOFF)"]
async fn upstream_live_commit_activate_replay_and_rollback_void(pool: PgPool) {
    let harness = live_harness();
    let paykit_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&harness.paykit_database_url)
        .await
        .expect("Paykit disposable DB is reachable");
    let (app, seller, buyer) = live_parties(pool.clone(), &harness).await;

    let committed_order = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, committed_order).await;
    assert_eq!(status, StatusCode::OK, "upstream bind failed: {body}");
    let committed = marketplace_pin(&pool, committed_order).await;
    let committed_invoice = committed.paykit_invoice_id.expect("invoice durably bound");
    assert_eq!(committed.payment_method.as_deref(), Some("bitcoin"));
    assert_eq!(committed.seller_pubky, seller.pubky);
    assert_eq!(committed.paykit_api.as_deref(), Some("upstream"));
    assert_eq!(committed.paykit_stack_id, None, "upstream has no stack_id");
    assert_eq!(
        committed.paykit_stack_endpoint, None,
        "upstream has one configured endpoint, not a durable fork pin"
    );
    assert_eq!(
        committed.paykit_activation_state.as_deref(),
        Some("preparing")
    );

    // Emulate a lost prepare response: exact signed retry must replay one invoice
    // without allocating another address or creating publication work.
    let replay = live_client(&app)
        .create_payment_request(
            &seller.pubky,
            &buyer.pubky,
            committed_order,
            1,
            u64::try_from(committed.paykit_total_sats.unwrap()).unwrap(),
            app.clock.now() + chrono::Duration::hours(1),
            app.state.config.bitcoin_payment_window_seconds,
        )
        .await
        .expect("exact prepare replay succeeds");
    assert!(matches!(
        replay,
        PaykitPrepared::Upstream { invoice_id, .. } if invoice_id == committed_invoice
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM marketplace_payment_preparations")
            .fetch_one(&paykit_pool)
            .await
            .unwrap(),
        1
    );

    drain_outbox(&app.pool, Some(live_client(&app)), app.clock.now(), 30)
        .await
        .expect("Marketplace activates durable bind");
    assert_eq!(
        marketplace_pin(&pool, committed_order)
            .await
            .paykit_activation_state
            .as_deref(),
        Some("active")
    );

    let rolled_back_order = create_sat_order(&app, &seller, &buyer).await;
    fail_activation_intent_inserts(&pool).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, rolled_back_order).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "injected Marketplace transaction rollback expected: {body}"
    );
    restore_activation_intent_inserts(&pool).await;
    let rolled_back = marketplace_pin(&pool, rolled_back_order).await;
    assert_eq!(rolled_back.payment_method, None);
    assert_eq!(rolled_back.paykit_invoice_id, None);

    let paykit_states = wait_for_paykit_states(&paykit_pool).await;
    assert!(paykit_states
        .iter()
        .any(|(id, state)| *id == committed_invoice && state == "active"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let row: (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), count(DISTINCT sdk_event_id), count(DISTINCT sdk_payment_request_id) \
             FROM outbox WHERE intent_kind = 'payment_request_proposal' AND status = 'delivered'",
        )
        .fetch_one(&paykit_pool)
        .await
        .unwrap();
        if row == (1, 1, 1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected one delivered SDK proposal/outbox identity, got {row:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM outbox WHERE intent_kind = 'payment_request_proposal'",
        )
        .fetch_one(&paykit_pool)
        .await
        .unwrap(),
        1,
        "rolled-back Marketplace bind was voided without publication"
    );

    // Exercise the production consumer against the production signed status
    // route. A response can only project this invoice when both durable
    // identities are correct; the closed DTO then maps delivered + no chain
    // observation to an undetected outcome without advancing payment state.
    assert_eq!(
        verify_due_paykit_payments(
            &app.state,
            live_client(&app),
            app.clock.now() + chrono::Duration::seconds(1),
        )
        .await
        .expect("Marketplace worker polls signed upstream status"),
        0
    );
    let observed = marketplace_pin(&pool, committed_order).await;
    assert_eq!(observed.seller_pubky, seller.pubky);
    assert_eq!(observed.paykit_invoice_id, Some(committed_invoice));
    assert_eq!(observed.paykit_api.as_deref(), Some("upstream"));
    assert_eq!(observed.paykit_stack_id, None);
    assert_eq!(observed.paykit_stack_endpoint, None);
    assert_eq!(observed.paykit_delivery_state.as_deref(), Some("delivered"));
    assert!(
        observed.paykit_last_checked_at.is_some(),
        "production worker claimed and projected signed status"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM payments WHERE order_id = $1")
            .bind(committed_order)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "awaiting_entitlement",
        "no Bitcoin observation must not advance Marketplace payment"
    );

    paykit_pool.close().await;
    drop(harness);
}
