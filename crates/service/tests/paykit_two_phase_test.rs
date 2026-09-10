//! Two-phase paykit payment requests (§B.11.2, §B.11.3, §B.11.8): phase 1
//! prepares the invoice inside the bind transaction, the durable
//! `paykit.activate` outbox row drives phase 2 against the persisted stack
//! endpoint, terminal errors void the bind, and a `preparing` order's hold
//! expiry settles the invoice with paykit first. All tests run against the
//! contract-verbatim local double.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::payments::{order_reference, PaykitClient, PaykitStatusOutcome};
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const LISTING_SATS: i64 = 51_200;
const NONCE_SATS: i64 = 437;
const TOTAL_SATS: i64 = LISTING_SATS + NONCE_SATS;

async fn enable_bitcoin(app: &TestApp, paykit: &FakePaykit, seller: &TestActor) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    paykit.set_claimed(&seller.pubky);
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

async fn create_sat_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let (status, body) = execute(app, &seller.token, &register_sat_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    // A unique checkout command id per call: one buyer may place several
    // orders in a test without tripping command idempotency.
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id present")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id present")
            .to_string(),
    }
}

/// Binds bitcoin and returns `(order_id, payment_id, invoice_id)`, leaving
/// the order `preparing` with its one `paykit.activate` outbox row.
async fn bound_preparing_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String, Uuid) {
    enable_bitcoin(app, paykit, seller).await;
    let order = create_sat_order(app, seller, buyer).await;
    let (status, body) = bind_bitcoin(app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "bitcoin bind failed: {body}");
    let (invoice_id,): (Uuid,) =
        sqlx::query_as("SELECT paykit_invoice_id FROM orders WHERE id = $1")
            .bind(Uuid::parse_str(&order.order_id).expect("order id is a uuid"))
            .fetch_one(&app.pool)
            .await
            .expect("order row exists");
    (order.order_id, order.payment_id, invoice_id)
}

fn order_uuid(order_id: &str) -> Uuid {
    Uuid::parse_str(order_id).expect("order id is a uuid")
}

async fn order_paykit_row(
    pool: &PgPool,
    order_id: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
) {
    sqlx::query_as(
        "SELECT paykit_request_state, paykit_activation_state, paykit_total_sats, \
         paykit_stack_id, paykit_stack_endpoint FROM orders WHERE id = $1",
    )
    .bind(order_uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row exists")
}

async fn activate_row_undelivered(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate' AND delivered_at IS NULL",
    )
    .await
}

fn voids_reaching(paykit: &FakePaykit) -> Vec<FakePaykitCall> {
    paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/void"))
        .collect()
}

/// Polls until `expected` void calls reached the double (courtesy voids are
/// fire-and-forget) and returns what arrived.
async fn await_voids(paykit: &FakePaykit, expected: usize) -> Vec<FakePaykitCall> {
    for _ in 0..50 {
        let voids = voids_reaching(paykit);
        if voids.len() >= expected {
            return voids;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    voids_reaching(paykit)
}

fn host_of(base_url: &str) -> String {
    base_url.trim_start_matches("http://").to_string()
}

// 1. Phase 1 persists the prepared body, from the body, in the bind
//    transaction — and exactly one `paykit.activate` row with it.
#[sqlx::test(migrations = "./migrations")]
async fn phase_one_persists_the_prepared_body_and_one_activate_row(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    let invoice = paykit.invoice(invoice_id).expect("double prepared it");
    #[derive(sqlx::FromRow)]
    struct PhaseOnePin {
        paykit_invoice_id: Option<Uuid>,
        paykit_stack_id: Option<String>,
        paykit_stack_endpoint: Option<String>,
        paykit_total_sats: Option<i64>,
        paykit_expires_at: Option<DateTime<Utc>>,
        paykit_prepare_expires_at: Option<DateTime<Utc>>,
        paykit_allocation_mode: Option<String>,
        paykit_address_fingerprint: Option<String>,
        paykit_request_state: Option<String>,
        paykit_activation_state: Option<String>,
    }
    let row: PhaseOnePin = sqlx::query_as(
        "SELECT paykit_invoice_id, paykit_stack_id, paykit_stack_endpoint, paykit_total_sats, \
         paykit_expires_at, paykit_prepare_expires_at, paykit_allocation_mode, \
         paykit_address_fingerprint, paykit_request_state, paykit_activation_state \
         FROM orders WHERE id = $1",
    )
    .bind(order_uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("order row exists");
    assert_eq!(row.paykit_invoice_id, Some(invoice_id));
    assert_eq!(
        row.paykit_stack_id.as_deref(),
        Some(paykit.stack_id().as_str())
    );
    assert_eq!(
        row.paykit_stack_endpoint.as_deref(),
        Some(paykit.base_url.as_str())
    );
    assert_eq!(row.paykit_total_sats, Some(TOTAL_SATS));
    let expires_at: DateTime<Utc> = DateTime::parse_from_rfc3339(&invoice.expires_at)
        .expect("expires_at parses")
        .to_utc();
    let prepare_expires_at: DateTime<Utc> =
        DateTime::parse_from_rfc3339(&invoice.prepare_expires_at)
            .expect("prepare_expires_at parses")
            .to_utc();
    assert_eq!(row.paykit_expires_at, Some(expires_at));
    assert_eq!(row.paykit_prepare_expires_at, Some(prepare_expires_at));
    assert_eq!(row.paykit_allocation_mode.as_deref(), Some("shared_manual"));
    assert_eq!(
        row.paykit_address_fingerprint.as_deref(),
        Some("3f7a1c9e5b204d86")
    );
    assert_eq!(row.paykit_request_state.as_deref(), Some("preparing"));
    assert_eq!(row.paykit_activation_state.as_deref(), Some("preparing"));

    // Exactly one activation intent, carrying the pin.
    let payloads: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM outbox WHERE kind = 'paykit.activate'")
            .fetch_all(&pool)
            .await
            .expect("activate rows listed");
    assert_eq!(payloads.len(), 1, "exactly one paykit.activate row");
    let payload = &payloads[0];
    assert_eq!(payload["invoice_id"], json!(invoice_id));
    assert_eq!(payload["order_id"], json!(order_uuid(&order_id)));
    assert_eq!(payload["stack_id"], json!(paykit.stack_id()));
    assert_eq!(payload["total_sats"], json!(TOTAL_SATS));
    assert_eq!(payload["stack_endpoint"], json!(paykit.base_url));
    assert_eq!(payload["activation_attempt"], json!(0));
}

// 2. After a repoint, activation still routes to the PERSISTED endpoint;
//    a second bind under the new URL persists the new URL per order.
#[sqlx::test(migrations = "./migrations")]
async fn activation_routes_to_the_persisted_endpoint_after_a_repoint(pool: PgPool) {
    let (app, _stripe, paykit_a) = test_app_with_payments(pool.clone()).await;
    let paykit_b = spawn_fake_paykit().await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit_a, &seller, &buyer).await;

    // The worker now runs under a runtime whose PAYKIT_SERVER_URL names B.
    let client_b =
        PaykitClient::new(&paykit_b.base_url, TEST_PAYKIT_SIGNING_SEED).expect("client B builds");
    drain_outbox(&app.pool, Some(&client_b), app.clock.now(), 30)
        .await
        .expect("drain runs");

    // The activate request arrived at A — the persisted endpoint — and B
    // saw nothing.
    let activations_at_a: Vec<FakePaykitCall> = paykit_a
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/activate"))
        .collect();
    assert_eq!(activations_at_a.len(), 1, "the activate went to A");
    assert_eq!(activations_at_a[0].host, host_of(&paykit_a.base_url));
    assert_eq!(
        activations_at_a[0].path,
        format!("/v0/payment-requests/{invoice_id}/activate")
    );
    assert!(
        paykit_b.calls().is_empty(),
        "B saw nothing: {:?}",
        paykit_b.calls()
    );
    let (request_state, activation_state, _total, stack_id, endpoint) =
        order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));
    assert_eq!(stack_id.as_deref(), Some(paykit_a.stack_id().as_str()));
    assert_eq!(endpoint.as_deref(), Some(paykit_a.base_url.as_str()));

    // A second bind under the repointed runtime persists B's URL for ITS
    // order — the column tracks the issuing address per order.
    let app_b = test_app_with_paykit_base(pool.clone(), &paykit_b.base_url).await;
    let seller2 = new_actor(&app_b).await;
    let buyer2 = new_actor(&app_b).await;
    enable_bitcoin(&app_b, &paykit_b, &seller2).await;
    let order2 = create_sat_order(&app_b, &seller2, &buyer2).await;
    let (status, body) = bind_bitcoin(&app_b, &buyer2.token, &order2.order_id).await;
    assert_eq!(status, StatusCode::OK, "bind under B failed: {body}");
    let (_state, _activation, _total, stack_id2, endpoint2) =
        order_paykit_row(&pool, &order2.order_id).await;
    assert_eq!(endpoint2.as_deref(), Some(paykit_b.base_url.as_str()));
    assert_eq!(stack_id2.as_deref(), Some(paykit_b.stack_id().as_str()));
}

// 3. A genuine commit failure after a phase-1 200 rolls the whole bind
//    back (no bind, no outbox row): the ambiguous-commit re-read confirms
//    the bind ABSENT, so exactly one courtesy void fires.
#[sqlx::test(migrations = "./migrations")]
async fn a_failed_commit_after_phase_one_rolls_back_and_voids(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let outbox_before = count(&pool, "SELECT COUNT(*) FROM outbox").await;

    // Abort the bind transaction at COMMIT time, after phase 1 returned.
    sqlx::query(
        "CREATE OR REPLACE FUNCTION fail_paykit_activate_commit() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced commit failure'; END; $$",
    )
    .execute(&pool)
    .await
    .expect("trigger function installs");
    sqlx::query(
        "CREATE CONSTRAINT TRIGGER fail_paykit_activate AFTER INSERT ON outbox \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
         WHEN (NEW.kind = 'paykit.activate') \
         EXECUTE FUNCTION fail_paykit_activate_commit()",
    )
    .execute(&pool)
    .await
    .expect("constraint trigger installs");

    let (status, _body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // Nothing persisted: no bind, no hold, no outbox rows at all.
    let (payment_method, reference, adapter, stock_held): (
        Option<String>,
        Option<String>,
        String,
        bool,
    ) = sqlx::query_as(
        "SELECT o.payment_method, o.paykit_request_reference, p.adapter, o.stock_held \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid(&order.order_id))
    .fetch_one(&pool)
    .await
    .expect("order row exists");
    assert_eq!(payment_method, None);
    assert_eq!(reference, None);
    assert_eq!(adapter, "sandbox");
    assert!(!stock_held);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM outbox").await,
        outbox_before,
        "the rolled-back transaction left no outbox rows"
    );

    // One best-effort void with the rollback reason reached paykit.
    let mut voids = Vec::new();
    for _ in 0..50 {
        voids = paykit
            .calls()
            .into_iter()
            .filter(|call| call.path.ends_with("/void"))
            .collect::<Vec<_>>();
        if !voids.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(voids.len(), 1, "exactly one courtesy void: {voids:?}");
    assert_eq!(
        voids[0].body["reason"],
        json!("marketplace_bind_rolled_back")
    );
    assert_eq!(voids[0].body["stack_id"], json!(paykit.stack_id()));
}

// 3b. The other half of the ambiguous COMMIT: the commit call reports an
//     error while the write is durable (the connection dropped after COMMIT
//     reached the server). The re-read finds the bind, so ZERO voids may
//     leave for paykit — the activation outbox row carries the bind
//     forward. Driven through the store seam with the bind + outbox row
//     committed.
#[sqlx::test(migrations = "./migrations")]
async fn an_ambiguous_commit_with_a_durable_bind_never_voids(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    // The bind committed; now the commit path reports an error anyway.
    let (reference, attempt): (String, i32) = sqlx::query_as(
        "SELECT paykit_request_reference, paykit_bind_attempt FROM orders WHERE id = $1",
    )
    .bind(order_uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("order row exists");
    let payments = app.state.payments.clone().expect("payments runtime");
    let prepared = Some((invoice_id, paykit.stack_id(), paykit.base_url.clone()));
    marketplace_service::payment_methods::resolve_ambiguous_bind_commit(
        &app.state,
        &payments,
        &prepared,
        order_uuid(&order_id),
        &Some((reference, attempt)),
        &sqlx::Error::Io(std::io::Error::other("injected ambiguous commit")),
    )
    .await;

    // Nothing may leave for paykit; the bind and its outbox row stand.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        paykit
            .calls()
            .iter()
            .all(|call| !call.path.ends_with("/void")),
        "no void reached paykit: {:?}",
        paykit.calls()
    );
    assert_eq!(activate_row_undelivered(&pool).await, 1);
    let (_request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("preparing"));
}

// 4. Phase-1 refusals keep their semantics and write nothing.
#[sqlx::test(migrations = "./migrations")]
async fn phase_one_refusals_keep_their_semantics(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;

    for (outcome, reason) in [
        ("seller_account_unclaimed", Some("creator_session_invalid")),
        ("paykit_rejected", Some("invalid_request")),
        ("paykit_unavailable", None),
    ] {
        let order = create_sat_order(&app, &seller, &buyer).await;
        match outcome {
            "paykit_unavailable" => {
                paykit.fail_creation_with_status(503, "bitcoin_creation_disabled")
            }
            _ => paykit.fail_creation_with(reason.expect("a code")),
        }
        let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
        assert!(
            status == StatusCode::CONFLICT || status == StatusCode::SERVICE_UNAVAILABLE,
            "{outcome}: {status} {body}"
        );
        assert_eq!(body["error"]["reason"], json!(outcome), "{outcome}: {body}");
        paykit.clear_creation_failure();
        let (payment_method, activation, request_state): (
            Option<String>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT payment_method, paykit_activation_state, paykit_request_state \
                 FROM orders WHERE id = $1",
        )
        .bind(order_uuid(&order.order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
        assert_eq!(payment_method, None, "{outcome} wrote a bind");
        assert_eq!(activation, None, "{outcome} wrote an activation state");
        assert_eq!(request_state, None, "{outcome} wrote a request state");
    }
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate'"
        )
        .await,
        0,
        "a refused phase 1 writes no activation intent"
    );
}

// 5. A bodyless 204, a missing field, or an inconsistent total: refused,
//    nothing persisted.
#[sqlx::test(migrations = "./migrations")]
async fn phase_one_shape_violations_are_refused(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;

    for (shape, reason) in [
        (FakePaykitCreateShape::LegacyNoContent, "paykit_rejected"),
        (FakePaykitCreateShape::MissingField, "paykit_rejected"),
        (
            FakePaykitCreateShape::TotalInconsistent,
            "paykit_total_inconsistent",
        ),
    ] {
        paykit.set_create_shape(shape);
        let order = create_sat_order(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{shape:?}: {body}");
        assert_eq!(body["error"]["reason"], json!(reason), "{shape:?}: {body}");
        let persisted: (Option<String>, Option<Uuid>) =
            sqlx::query_as("SELECT payment_method, paykit_invoice_id FROM orders WHERE id = $1")
                .bind(order_uuid(&order.order_id))
                .fetch_one(&pool)
                .await
                .expect("order row exists");
        assert_eq!(persisted.0, None, "{shape:?} wrote a bind");
        assert_eq!(persisted.1, None, "{shape:?} persisted an invoice");
        if shape == FakePaykitCreateShape::TotalInconsistent {
            assert!(
                captured_logs()
                    .contains("ALERT paykit phase 1 total_sats != amount_sats + nonce_sats"),
                "the inconsistency is alerted"
            );
        }
    }
    paykit.set_create_shape(FakePaykitCreateShape::Full);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate'"
        )
        .await,
        0
    );
}

// 5b. Phase-1 `expires_at` echoes: only an exact echo of the hold deadline
//     binds. A later or earlier echo is a terminal refusal in the
//     total-mismatch class — no bind, no outbox row, one courtesy void,
//     alerted — and the bound order persists the LOCAL hold deadline.
#[sqlx::test(migrations = "./migrations")]
async fn phase_one_expires_at_echo_is_verified(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;

    for (case, shape) in [
        (1usize, FakePaykitCreateShape::ExpiryLater),
        (2, FakePaykitCreateShape::ExpiryEarlier),
    ] {
        paykit.set_create_shape(shape);
        let order = create_sat_order(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::CONFLICT, "{shape:?}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!("paykit_expiry_inconsistent"),
            "{shape:?}: {body}"
        );
        let persisted: (Option<String>, Option<Uuid>) =
            sqlx::query_as("SELECT payment_method, paykit_invoice_id FROM orders WHERE id = $1")
                .bind(order_uuid(&order.order_id))
                .fetch_one(&pool)
                .await
                .expect("order row exists");
        assert_eq!(persisted.0, None, "{shape:?} wrote a bind");
        assert_eq!(persisted.1, None, "{shape:?} persisted an invoice");
        let voids = await_voids(&paykit, case).await;
        assert_eq!(voids.len(), case, "{shape:?}: one courtesy void each");
        assert_eq!(
            voids[case - 1].body["reason"],
            json!("marketplace_bind_rolled_back")
        );
    }
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate'"
        )
        .await,
        0,
        "a refused expiry wrote no activation intent"
    );
    assert!(
        captured_logs()
            .matches("ALERT paykit phase 1 expires_at != the hold deadline")
            .count()
            >= 2,
        "both expiry mismatches alerted"
    );

    // (c) The exact echo binds, and the persisted expiry is the LOCAL hold
    // deadline.
    paykit.set_create_shape(FakePaykitCreateShape::Full);
    let order = create_sat_order(&app, &seller, &buyer).await;
    let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "the exact echo binds: {body}");
    let (persisted_expiry, hold_deadline): (DateTime<Utc>, DateTime<Utc>) =
        sqlx::query_as("SELECT paykit_expires_at, hold_expires_at FROM orders WHERE id = $1")
            .bind(order_uuid(&order.order_id))
            .fetch_one(&pool)
            .await
            .expect("order row exists");
    assert_eq!(
        persisted_expiry, hold_deadline,
        "the local hold deadline is persisted, not the echo"
    );
}

// 6. Activation flips `preparing → active` and `paykit_request_state →
//    pending` with the delivered mark in one transaction; a failure
//    between the HTTP call and the commit changes nothing.
#[sqlx::test(migrations = "./migrations")]
async fn activation_flips_preparing_to_active_atomically(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    // The failure variant first: a malformed 200 — the call happened, but
    // nothing may change and the row stays undelivered.
    paykit.script_activate(invoice_id, vec![FakePaykitReply::MalformedOk]);
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(activate_row_undelivered(&pool).await, 1);
    let (request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("preparing"));
    assert_eq!(request_state.as_deref(), Some("preparing"));

    // The success path: one activate call with the verbatim body, and the
    // flip plus the delivered mark commit together.
    sqlx::query("UPDATE outbox SET lease_until = NULL WHERE kind = 'paykit.activate'")
        .execute(&pool)
        .await
        .expect("lease released");
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(activate_row_undelivered(&pool).await, 0);
    let (request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));
    let activations: Vec<FakePaykitCall> = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/activate"))
        .collect();
    assert_eq!(activations.len(), 2, "the malformed try plus the success");
    let success = &activations[1];
    assert_eq!(success.body["invoice_id"], json!(invoice_id));
    assert_eq!(success.body["stack_id"], json!(paykit.stack_id()));
    assert_eq!(success.body["total_sats"], json!(TOTAL_SATS));
    assert_eq!(success.body["activation_attempt"], json!(2));
}

// 6b. A well-formed activation 200 whose `state` is not `observing` is a
//     malformed success: nothing flips locally, the row retries under its
//     lease, and a later well-formed 200 completes the activation.
#[sqlx::test(migrations = "./migrations")]
async fn activation_success_with_a_non_observing_state_retries(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());

    for bad_state in ["prepared", "collapsing"] {
        // A fresh seller per case keeps the single-unit listings
        // independent (a completed case holds its stock).
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        enable_bitcoin(&app, &paykit, &seller).await;
        let order = create_sat_order(&app, &seller, &buyer).await;
        let (status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
        assert_eq!(status, StatusCode::OK, "{bad_state} bind failed: {body}");
        let (invoice_id,): (Uuid,) =
            sqlx::query_as("SELECT paykit_invoice_id FROM orders WHERE id = $1")
                .bind(order_uuid(&order.order_id))
                .fetch_one(&pool)
                .await
                .expect("order row exists");
        paykit.script_activate(
            invoice_id,
            vec![
                FakePaykitReply::NonObservingOk(bad_state.to_string()),
                FakePaykitReply::Contract,
            ],
        );

        drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
            .await
            .expect("drain runs");
        let (request_state, activation_state, ..) = order_paykit_row(&pool, &order.order_id).await;
        assert_eq!(
            activation_state.as_deref(),
            Some("preparing"),
            "{bad_state}: nothing flips on a non-observing 200"
        );
        assert_eq!(request_state.as_deref(), Some("preparing"), "{bad_state}");
        let undelivered: i64 = count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate' AND delivered_at IS NULL",
        )
        .await;
        assert_eq!(undelivered, 1, "{bad_state}: the row retries");

        // The retry (a well-formed 200) completes the activation.
        sqlx::query(
            "UPDATE outbox SET lease_until = NULL \
             WHERE kind = 'paykit.activate' AND delivered_at IS NULL",
        )
        .execute(&pool)
        .await
        .expect("lease released");
        drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
            .await
            .expect("drain runs");
        let (request_state, activation_state, ..) = order_paykit_row(&pool, &order.order_id).await;
        assert_eq!(activation_state.as_deref(), Some("active"), "{bad_state}");
        assert_eq!(request_state.as_deref(), Some("pending"), "{bad_state}");
        let activations = paykit
            .calls()
            .into_iter()
            .filter(|call| call.path == format!("/v0/payment-requests/{invoice_id}/activate"))
            .count();
        assert_eq!(activations, 2, "{bad_state}: the bad 200 plus the retry");
    }
}

// 7. A redelivered activate row cannot apply twice: no second HTTP call,
//    no state change.
#[sqlx::test(migrations = "./migrations")]
async fn activation_redelivery_cannot_apply_twice(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, _invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("first delivery runs");
    let (request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));

    // Simulate a lost delivery mark: the row becomes due again.
    sqlx::query(
        "UPDATE outbox SET delivered_at = NULL, lease_until = NULL WHERE kind = 'paykit.activate'",
    )
    .execute(&pool)
    .await
    .expect("mark reset");
    let redelivered = drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("redelivery runs");
    assert_eq!(redelivered, 1, "the row is stamped again");
    let activations = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/activate"))
        .count();
    assert_eq!(activations, 1, "no second activate reached paykit");
    let (request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));
}

// 8. Every terminal error voids the bind, releases the hold, emits the
//    `payment.bitcoin_prepare_voided` intent and stamps the row; the
//    mismatch cases alert.
#[sqlx::test(migrations = "./migrations")]
async fn activation_terminal_errors_void_the_bind(pool: PgPool) {
    install_log_capture();
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;

    for (case_number, (status, code)) in [
        (409u16, "prepare_expired"),
        (409, "invoice_finalized"),
        (404, "unknown_invoice"),
        (409, "activation_total_mismatch"),
        (409, "stack_identity_mismatch"),
    ]
    .into_iter()
    .enumerate()
    {
        // A fresh seller per case keeps the single-unit listings
        // independent.
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        enable_bitcoin(&app, &paykit, &seller).await;
        let order = create_sat_order(&app, &seller, &buyer).await;
        let (bind_status, body) = bind_bitcoin(&app, &buyer.token, &order.order_id).await;
        assert_eq!(bind_status, StatusCode::OK, "{code} bind failed: {body}");
        let (invoice_id,): (Uuid,) =
            sqlx::query_as("SELECT paykit_invoice_id FROM orders WHERE id = $1")
                .bind(order_uuid(&order.order_id))
                .fetch_one(&pool)
                .await
                .expect("order row exists");
        paykit.script_activate(
            invoice_id,
            vec![FakePaykitReply::Error(status, code.to_string())],
        );

        let paykit_client = app
            .state
            .payments
            .as_ref()
            .and_then(|payments| payments.paykit.as_ref());
        drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
            .await
            .expect("drain runs");

        // The bind is voided: method cleared, hold released, payment row
        // back to its pre-bind state.
        let row: (
            Option<String>,
            Option<String>,
            Option<String>,
            bool,
            String,
            i64,
        ) = sqlx::query_as(
            "SELECT o.payment_method, o.paykit_activation_state, o.paykit_request_state, \
             o.stock_held, p.adapter, p.amount_minor \
             FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
        )
        .bind(order_uuid(&order.order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
        assert_eq!(row.0, None, "{code}: the method is cleared");
        assert_eq!(row.1.as_deref(), Some("voided"), "{code}");
        assert_eq!(row.2, None, "{code}: the request state is cleared");
        assert!(!row.3, "{code}: the hold flag is cleared");
        assert_eq!(row.4, "sandbox", "{code}: the adapter is pre-bind");
        assert_eq!(row.5, LISTING_SATS, "{code}: the amount is pre-bind");
        let (reserved, available): (i64, i64) = sqlx::query_as(
            "SELECT reserved_quantity, available_quantity FROM listings WHERE aggregate_id = $1",
        )
        .bind(listing_aggregate(&seller.pubky))
        .fetch_one(&pool)
        .await
        .expect("listing row exists");
        assert_eq!(reserved, 0, "{code}: the stock is back");
        assert_eq!(available, 1, "{code}: the stock is back");
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate' AND delivered_at IS NULL"
            )
            .await,
            0,
            "{code}: the row is stamped"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM events WHERE kind = 'payment.bitcoin_prepare_voided'"
            )
            .await,
            case_number as i64 + 1,
            "{code}: the void event is emitted"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_prepare_voided'"
            )
            .await,
            case_number as i64 + 1,
            "{code}: the buyer notification intent is emitted"
        );
    }

    // The mismatch cases alert (naming both totals / both stack ids).
    let logs = captured_logs();
    assert!(
        logs.contains("ALERT paykit refused activation with a total mismatch"),
        "total mismatch alerted"
    );
    assert!(
        logs.contains("different stack identity"),
        "stack mismatch alerted"
    );
    assert!(logs.contains("unknown invoice"), "unknown invoice alerted");
}

// A voided bind is released: the buyer binds again, and the retry is a
// fresh phase 1 with a fresh idempotency key.
#[sqlx::test(migrations = "./migrations")]
async fn a_voided_bind_retries_with_a_fresh_idempotency_key(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    paykit.script_activate(
        invoice_id,
        vec![FakePaykitReply::Error(409, "prepare_expired".to_string())],
    );
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    let (_request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("voided"));

    let (rebind_status, rebind_body) = bind_bitcoin(&app, &buyer.token, &order_id).await;
    assert_eq!(
        rebind_status,
        StatusCode::OK,
        "the buyer binds again: {rebind_body}"
    );
    let (attempt,): (i32,) = sqlx::query_as("SELECT paykit_bind_attempt FROM orders WHERE id = $1")
        .bind(order_uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert_eq!(attempt, 2, "the retry incremented the counter");
    let requests = paykit.requests();
    assert_eq!(requests.len(), 2, "two phase-1 calls");
    let reference = order_reference(order_uuid(&order_id));
    assert_eq!(requests[0].idempotency_key, format!("{reference}:1"));
    assert_eq!(requests[1].idempotency_key, format!("{reference}:2"));
    let (_request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("preparing"));
}

// 9. A 5xx leaves the row undelivered and re-leased; a later 200 completes
//    it.
#[sqlx::test(migrations = "./migrations")]
async fn activation_unavailable_retries_then_completes(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    paykit.script_activate(
        invoice_id,
        vec![
            FakePaykitReply::Error(500, "boom".to_string()),
            FakePaykitReply::Contract,
        ],
    );
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());

    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(
        activate_row_undelivered(&pool).await,
        1,
        "the 5xx left the row undelivered"
    );
    let (_request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("preparing"));

    sqlx::query("UPDATE outbox SET lease_until = NULL WHERE kind = 'paykit.activate'")
        .execute(&pool)
        .await
        .expect("lease released");
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(activate_row_undelivered(&pool).await, 0);
    let (request_state, activation_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(activation_state.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));
    let attempts: Vec<u64> = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/activate"))
        .map(|call| call.body["activation_attempt"].as_u64().expect("attempt"))
        .collect();
    assert_eq!(attempts, vec![1, 2], "the retry counted its attempt");
}

// 10. A `preparing` order is not claimed by the paykit poll; after
//     activation it is.
#[sqlx::test(migrations = "./migrations")]
async fn a_preparing_order_is_not_polled(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, _invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    let reference = order_reference(order_uuid(&order_id));

    let source = FakePaykitStatus::default();
    source.set_outcome(&reference, PaykitStatusOutcome::Detected);
    let applied = marketplace_service::workers::verify_due_paykit_payments(
        &app.state,
        &source,
        app.clock.now(),
    )
    .await
    .expect("poll runs");
    assert_eq!(applied, 0);
    let (last_checked,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT paykit_last_checked_at FROM orders WHERE id = $1")
            .bind(order_uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("order row exists");
    assert_eq!(last_checked, None, "a preparing order was never claimed");

    // Activation flips it to pending; the next poll claims it.
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    app.clock.advance_seconds(60);
    marketplace_service::workers::verify_due_paykit_payments(&app.state, &source, app.clock.now())
        .await
        .expect("poll runs");
    let (request_state, ..) = order_paykit_row(&pool, &order_id).await;
    assert_eq!(request_state.as_deref(), Some("detected"));
}

// 11. The buyer-facing total is the paykit total (price + nonce) on the
//     order projection, the payment step, and the receipt — never the
//     pre-nonce amount.
#[sqlx::test(migrations = "./migrations")]
async fn the_buyer_total_is_the_paykit_total(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, _invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    let reference = order_reference(order_uuid(&order_id));

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&buyer.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"]["amount_minor"], json!(TOTAL_SATS));
    assert_eq!(body["paykit_total_sats"], json!(TOTAL_SATS));
    assert_eq!(
        body["payment"]["amount"]["amount_minor"],
        json!(TOTAL_SATS),
        "the payment step shows the charged figure"
    );

    // Through confirmation, the receipt records the same figure.
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let source = FakePaykitStatus::default();
    source.set_outcome(
        &reference,
        PaykitStatusOutcome::Confirmed {
            amount_matched: true,
        },
    );
    app.clock.advance_seconds(60);
    marketplace_service::workers::verify_due_paykit_payments(&app.state, &source, app.clock.now())
        .await
        .expect("poll runs");
    let (receipt_total,): (i64,) = sqlx::query_as("SELECT total_minor FROM receipts LIMIT 1")
        .fetch_one(&pool)
        .await
        .expect("receipt exists");
    assert_eq!(receipt_total, TOTAL_SATS);
}

// 12a. Hold expiry on a `preparing` order: the void succeeds → the order
//      expires exactly as an unpaid order, with the void event.
#[sqlx::test(migrations = "./migrations")]
async fn hold_expiry_on_a_preparing_order_voids_and_expires(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    app.clock.advance_seconds(3_601);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 1);

    let voids: Vec<FakePaykitCall> = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/void"))
        .collect();
    assert_eq!(voids.len(), 1, "paykit was voided first");
    assert_eq!(
        voids[0].path,
        format!("/v0/payment-requests/{invoice_id}/void")
    );
    assert_eq!(voids[0].body["reason"], json!("hold_expired"));
    assert_eq!(voids[0].body["stack_id"], json!(paykit.stack_id()));

    let (state, activation, request_state, stock_held, payment_state): (
        String,
        Option<String>,
        Option<String>,
        bool,
        String,
    ) = sqlx::query_as(
        "SELECT o.state, o.paykit_activation_state, o.paykit_request_state, o.stock_held, \
         p.state FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(order_uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("order row exists");
    assert_eq!(state, "cancelled");
    assert_eq!(activation.as_deref(), Some("voided"));
    assert_eq!(request_state, None);
    assert!(!stock_held);
    assert_eq!(payment_state, "expired");
    let (available,): (i64,) =
        sqlx::query_as("SELECT available_quantity FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&pool)
            .await
            .expect("listing exists");
    assert_eq!(available, 1, "the stock is back");
    assert_eq!(activate_row_undelivered(&pool).await, 0);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.bitcoin_prepare_voided'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.bitcoin_prepare_voided'"
        )
        .await,
        1
    );
}

// 12b. Hold expiry with `invoice_finalized` (paykit already activated — a
//      lost 2xx): the order becomes active/pending and the ordinary expiry
//      takes it from the next tick.
#[sqlx::test(migrations = "./migrations")]
async fn hold_expiry_with_invoice_finalized_marks_the_order_active(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;
    paykit.script_void(
        invoice_id,
        vec![FakePaykitReply::Error(409, "invoice_finalized".to_string())],
    );

    app.clock.advance_seconds(3_601);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 0, "the order is not expired: it went live");
    let (state, activation, request_state): (String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT state, paykit_activation_state, paykit_request_state FROM orders WHERE id = $1",
        )
        .bind(order_uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert_eq!(state, "pending_payment");
    assert_eq!(activation.as_deref(), Some("active"));
    assert_eq!(request_state.as_deref(), Some("pending"));
    assert_eq!(activate_row_undelivered(&pool).await, 0);

    // The ordinary expiry for a pending bitcoin order runs on the next
    // tick (paykit's expires_at equals the hold deadline).
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 1);
    let (state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1")
        .bind(order_uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert_eq!(state, "cancelled");
}

// 12c. Hold expiry while the activate row's lease is held (a delivery in
//      flight): the order is skipped this tick.
#[sqlx::test(migrations = "./migrations")]
async fn hold_expiry_skips_an_order_whose_activation_is_in_flight(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, _invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    // A delivery is in flight: the row is leased into the future.
    app.clock.advance_seconds(3_601);
    let lease_until = app.clock.now() + chrono::Duration::hours(1);
    sqlx::query("UPDATE outbox SET lease_until = $1 WHERE kind = 'paykit.activate'")
        .bind(lease_until)
        .execute(&pool)
        .await
        .expect("lease armed");

    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 0, "the in-flight order is skipped");
    let (state, activation): (String, Option<String>) =
        sqlx::query_as("SELECT state, paykit_activation_state FROM orders WHERE id = $1")
            .bind(order_uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("order row exists");
    assert_eq!(state, "pending_payment");
    assert_eq!(activation.as_deref(), Some("preparing"));
    assert!(
        paykit
            .calls()
            .iter()
            .all(|call| !call.path.ends_with("/void")),
        "no void was attempted"
    );
}

// 12d. Paykit unreachable: within the 30-minute grace the tick retries;
//      past it the marketplace voids locally and enqueues a `paykit.void`
//      row that delivers once paykit returns.
#[sqlx::test(migrations = "./migrations")]
async fn hold_expiry_unreachable_voids_locally_past_the_grace(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, invoice_id) =
        bound_preparing_order(&app, &paykit, &seller, &buyer).await;

    // First the void times out (hang), then every command 500s.
    paykit.script_void(invoice_id, vec![FakePaykitReply::Hang]);
    app.clock.advance_seconds(3_601);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 0, "within the grace the tick retries");
    let (state, activation): (String, Option<String>) =
        sqlx::query_as("SELECT state, paykit_activation_state FROM orders WHERE id = $1")
            .bind(order_uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("order row exists");
    assert_eq!(state, "pending_payment");
    assert_eq!(activation.as_deref(), Some("preparing"));
    assert_eq!(
        activate_row_undelivered(&pool).await,
        1,
        "the lease was released for the next tick"
    );

    // Past hold + 30 min with paykit still down: local void + retry row.
    paykit.fail_commands_with(500, "down");
    app.clock.advance_seconds(1_801);
    let expired = expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("expiry runs");
    assert_eq!(expired, 1);
    let (state, activation, request_state): (String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT state, paykit_activation_state, paykit_request_state FROM orders WHERE id = $1",
        )
        .bind(order_uuid(&order_id))
        .fetch_one(&pool)
        .await
        .expect("order row exists");
    assert_eq!(state, "cancelled");
    assert_eq!(activation.as_deref(), Some("voided"));
    assert_eq!(request_state, None);
    assert_eq!(activate_row_undelivered(&pool).await, 0);
    let void_payloads: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM outbox WHERE kind = 'paykit.void'")
            .fetch_all(&pool)
            .await
            .expect("void rows listed");
    assert_eq!(void_payloads.len(), 1, "one paykit.void retry row");
    assert_eq!(void_payloads[0]["reason"], json!("hold_expired"));
    assert_eq!(void_payloads[0]["invoice_id"], json!(invoice_id));
    assert_eq!(void_payloads[0]["stack_endpoint"], json!(paykit.base_url));

    // When paykit returns, the ordinary lease delivers the deferred void.
    paykit.clear_command_failure();
    let paykit_client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, paykit_client, app.clock.now(), 30)
        .await
        .expect("drain runs");
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.void' AND delivered_at IS NULL"
        )
        .await,
        0,
        "the deferred void delivered"
    );
    let void_calls = paykit
        .calls()
        .into_iter()
        .filter(|call| call.path.ends_with("/void"))
        .count();
    assert_eq!(void_calls, 3, "hang + 500 + the delivered retry");
}
