//! Server-side Locks verification (plan task 4.5, ADR-0019 §7/§8).
//!
//! The lifecycle lookup is driven by a programmable fake so no live Lock
//! Server is required; the fake exercises completed, pending, in-progress,
//! failed, expired, not-found, unavailable, late, and duplicate outcomes.
//! Nothing here fakes verification semantics: the worker under test is the
//! production code path, and no client claim ever advances a payment.

mod common;

use axum::http::StatusCode;
use marketplace_service::clock::Clock;
use marketplace_service::locks::{LocksLookupOutcome, LocksTaskStatus};
use marketplace_service::workers::{self, run_once, try_acquire_lease};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    checkout_command_with_id, count, create_pending_order, execute, lock_resource_for, new_actor,
    payment_command, register_command, register_locks_command, send, test_app, test_app_with_locks,
    PendingOrder, TestApp, TEST_BUNDLE_ID, TEST_LOCK_ID,
};

/// A second canonical bundle id, distinct from [`TEST_BUNDLE_ID`].
const OTHER_BUNDLE_ID: &str = "111G40R40M30E209185GR38E1W";

async fn register_locks(
    app: &TestApp,
    buyer_token: &str,
    order: &PendingOrder,
    seller_pubky: &str,
) -> Value {
    let (status, body) = execute(
        app,
        buyer_token,
        &register_locks_command(
            &order.payment_id,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(seller_pubky),
            1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");
    body
}

async fn payment_state(pool: &PgPool, payment_id: &str) -> (String, String, i64) {
    let (state, adapter, revision): (String, String, i64) =
        sqlx::query_as("SELECT state, adapter, revision FROM payments WHERE id = $1::uuid")
            .bind(payment_id)
            .fetch_one(pool)
            .await
            .expect("payment row exists");
    (state, adapter, revision)
}

async fn order_state(pool: &PgPool, order_id: &str) -> String {
    let (state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row exists");
    state
}

// Registration stores only an encrypted correlation bound to the order's
// participants, amount, asset, policy version, and lock resource hash; the
// payment flips to the 'locks' adapter and the bundle id appears nowhere in
// plaintext.
#[sqlx::test]
async fn registration_stores_an_encrypted_bound_correlation(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let body = register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    assert_eq!(body["revision"], json!(2));
    assert_eq!(body["result"]["payment"]["adapter"], json!("locks"));
    assert_eq!(
        body["result"]["payment"]["state"],
        json!("awaiting_entitlement"),
        "registration must not advance the payment"
    );
    assert_eq!(body["result"]["verification"]["state"], json!("pending"));
    assert!(body["result"]["verification"]["window_expires_at"].is_string());

    let (payment_id, buyer_pubky, creator_pubky, amount, asset, policy, ciphertext, token): (
        Uuid,
        String,
        String,
        i64,
        String,
        i32,
        Vec<u8>,
        Vec<u8>,
    ) = sqlx::query_as(
        "SELECT payment_id, buyer_pubky, creator_pubky, amount_minor, asset, policy_version, \
         bundle_id_ciphertext, bundle_lookup_token FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert_eq!(payment_id.to_string(), order.payment_id);
    assert_eq!(buyer_pubky, buyer.pubky);
    assert_eq!(creator_pubky, seller.pubky);
    assert_eq!(amount, 14_796); // 12_500 + 1_200 shipping + 1_096 tax
    assert_eq!(asset, "USD");
    assert_eq!(policy, 1);
    assert!(
        !ciphertext
            .windows(TEST_BUNDLE_ID.len())
            .any(|window| window == TEST_BUNDLE_ID.as_bytes()),
        "the stored ciphertext must not contain the plaintext bundle id"
    );
    assert_ne!(token, TEST_BUNDLE_ID.as_bytes().to_vec());
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.locks_registered'"
        )
        .await,
        1
    );
}

// The registration guards: only the buyer, only the right aggregate and
// revision, only a payment awaiting entitlement, and only a lock resource
// created by the order's seller.
#[sqlx::test]
async fn registration_enforces_participant_state_and_creator_guards(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let outsider = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let resource = lock_resource_for(&seller.pubky);

    let command = register_locks_command(&order.payment_id, 1, TEST_BUNDLE_ID, &resource, 10);
    let (status, body) = execute(&app, &outsider.token, &command).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "only the buyer registers: {body}"
    );

    let missing = register_locks_command(
        &Uuid::new_v4().to_string(),
        1,
        TEST_BUNDLE_ID,
        &resource,
        11,
    );
    let (status, _) = execute(&app, &buyer.token, &missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let stale = register_locks_command(&order.payment_id, 7, TEST_BUNDLE_ID, &resource, 12);
    let (status, body) = execute(&app, &buyer.token, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));

    // A lock resource created by someone other than the order's seller is
    // refused: the correlation must bind the lifecycle to the seller.
    let foreign = register_locks_command(
        &order.payment_id,
        1,
        TEST_BUNDLE_ID,
        &lock_resource_for(&outsider.pubky),
        13,
    );
    let (status, body) = execute(&app, &buyer.token, &foreign).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    // A payment no longer awaiting entitlement refuses registration.
    let (status, _) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "confirmed", 1, 14),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let confirmed = register_locks_command(&order.payment_id, 2, TEST_BUNDLE_ID, &resource, 15);
    let (status, body) = execute(&app, &buyer.token, &confirmed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
}

// Replay discipline: exact replay returns the stored result; a changed
// payload under the same command id conflicts; a second registration for
// the same payment is refused; the same lifecycle identity can never
// correlate a second order (unique HMAC lookup token).
#[sqlx::test]
async fn registration_rejects_changed_replays_and_identity_reuse(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let resource = lock_resource_for(&seller.pubky);

    // Two units so the same buyer/seller pair can hold two orders.
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, first_checkout) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut second =
        checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001");
    second["payload"]["lines"][0]["expected_revision"] = json!(2);
    let (status, second_checkout) = execute(&app, &buyer.token, &second).await;
    assert_eq!(status, StatusCode::OK, "{second_checkout}");
    let first_payment = first_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");
    let second_payment = second_checkout["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id");

    let command = register_locks_command(first_payment, 1, TEST_BUNDLE_ID, &resource, 20);
    let (status, original) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{original}");

    // Exact replay: the stored result, no re-execution.
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, original);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        1
    );

    // Changed replay under the same command id: idempotency conflict.
    let mut changed = command.clone();
    changed["payload"]["bundle_id"] = json!(OTHER_BUNDLE_ID);
    let (status, body) = execute(&app, &buyer.token, &changed).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("IDEMPOTENCY_CONFLICT"));

    // A different registration for the already-correlated payment: refused.
    let second_registration =
        register_locks_command(first_payment, 2, OTHER_BUNDLE_ID, &resource, 21);
    let (status, body) = execute(&app, &buyer.token, &second_registration).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));

    // The same {creator, bundle_id} identity on another order: rejected by
    // the unique lookup token, not application logic.
    let reused_identity = register_locks_command(second_payment, 1, TEST_BUNDLE_ID, &resource, 22);
    let (status, body) = execute(&app, &buyer.token, &reused_identity).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
}

// Once a payment is correlated to a real Locks lifecycle, the sandbox
// command — a client claim — can no longer advance it.
#[sqlx::test]
async fn sandbox_advance_is_refused_for_a_locks_correlated_payment(pool: PgPool) {
    let (app, _fake) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    for target in ["detected", "confirmed", "expired", "manual_review"] {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &payment_command(&order.payment_id, 2, target, 1, 30),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{target}: {body}");
        assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    }
    let (state, adapter, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        (state.as_str(), adapter.as_str()),
        ("awaiting_entitlement", "locks")
    );
}

// Without the Locks runtime (sandbox-only deployment) the registration
// command is refused outright: fail closed, no correlation is stored.
#[sqlx::test]
async fn registration_is_refused_when_locks_is_not_configured(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &register_locks_command(
            &order.payment_id,
            1,
            TEST_BUNDLE_ID,
            &lock_resource_for(&seller.pubky),
            40,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM payment_locks_correlations").await,
        0
    );
}

// The worker independently verifies a completed lifecycle and applies the
// full confirmation exactly once: payment confirmed, order paid, receipt
// issued, inventory converted, seller notified. Repeats and simulated
// redeliveries are harmless — the unique payment-confirmed event index and
// the payment-state compare-and-swap decide, not application logic.
#[sqlx::test]
async fn worker_confirms_a_verified_completion_exactly_once(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );

    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    assert_eq!(
        fake.lookups(),
        vec![(seller.pubky.clone(), TEST_BUNDLE_ID.to_string())],
        "the service queried the lifecycle for exactly the registered identity"
    );

    let (state, adapter, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!((state.as_str(), adapter.as_str()), ("confirmed", "locks"));
    assert_eq!(order_state(&app.pool, &order.order_id).await, "paid");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.payment_confirmed'"
        )
        .await,
        1
    );
    let (sold, reserved): (i64, i64) =
        sqlx::query_as("SELECT sold_quantity, reserved_quantity FROM listings")
            .fetch_one(&app.pool)
            .await
            .expect("listing row exists");
    assert_eq!((sold, reserved), (1, 0), "the held unit converted to sold");
    let (verification_state, completed_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT verification_state, completed_at FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(verification_state, "completed");
    assert!(completed_at.is_some());

    // A later pass has nothing to verify: the correlation is terminal.
    app.clock.advance_seconds(60);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 0);
    assert_eq!(
        fake.lookup_count(),
        1,
        "terminal correlations are not re-polled"
    );

    // A duplicate/reordered completion delivery (simulated by resetting the
    // correlation claim, as a crash between lookup and effect would leave
    // it) cannot repeat any effect.
    sqlx::query(
        "UPDATE payment_locks_correlations SET verification_state = 'pending', \
         last_checked_at = NULL, completed_at = NULL",
    )
    .execute(&app.pool)
    .await
    .expect("reset runs");
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 0,
        "duplicate completion is harmless"
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        1,
        "still exactly one payment-confirmed event"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "confirmed");
}

// Pending, in-progress, not-found, and unavailable lookups leave the
// payment untouched and the correlation pending; polling is rate-limited by
// the poll interval and history rows are appended only on status change.
#[sqlx::test]
async fn non_terminal_lifecycles_stay_pending_and_poll_boundedly(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    // Not yet submitted upstream: not_found, still pending.
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    // Within the poll interval the correlation is not re-claimed.
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(fake.lookup_count(), 1, "polling respects the poll interval");

    for (advance, outcome, expected_status) in [
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::Pending),
            "pending",
        ),
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::Pending),
            "pending",
        ),
        (
            31,
            LocksLookupOutcome::Status(LocksTaskStatus::InProgress),
            "in_progress",
        ),
        (31, LocksLookupOutcome::Unavailable, "in_progress"),
    ] {
        app.clock.advance_seconds(advance);
        fake.set_outcome(TEST_BUNDLE_ID, outcome);
        let summary = run_once(&app.state, holder, app.clock.now())
            .await
            .expect("worker pass runs");
        assert_eq!(summary.locks_completions_applied, 0);
        let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
        assert_eq!(state, "awaiting_entitlement");
        let (verification_state, last_observed): (String, Option<String>) = sqlx::query_as(
            "SELECT verification_state, last_observed_status FROM payment_locks_correlations",
        )
        .fetch_one(&app.pool)
        .await
        .expect("correlation row exists");
        assert_eq!(verification_state, "pending");
        assert_eq!(last_observed.as_deref(), Some(expected_status));
    }
    // History: not_found, pending, in_progress — one row per change, none
    // for the repeat or the transport failure.
    let history: Vec<(String, String)> = sqlx::query_as(
        "SELECT observed_status, outcome FROM payment_locks_observations ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("observations listed");
    assert_eq!(
        history,
        vec![
            ("not_found".to_string(), "none".to_string()),
            ("pending".to_string(), "none".to_string()),
            ("in_progress".to_string(), "none".to_string()),
        ]
    );
}

// An upstream terminal failure is recorded and stops polling, but is NOT a
// marketplace expiry: the payment stays awaiting entitlement until the
// marketplace's own payment window elapses (ADR-0019 §7).
#[sqlx::test]
async fn upstream_failure_is_separate_from_marketplace_expiry(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Failed),
    );

    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 0);
    assert_eq!(summary.payment_windows_expired, 0);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(
        state, "awaiting_entitlement",
        "an upstream failure must not expire the payment"
    );
    let (verification_state,): (String,) =
        sqlx::query_as("SELECT verification_state FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(verification_state, "upstream_failed");

    // Terminal upstream state stops polling.
    app.clock.advance_seconds(60);
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(fake.lookup_count(), 1);

    // The marketplace window is what expires the payment, on its own clock.
    app.clock.advance_seconds(3_600);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.expired'"
        )
        .await,
        1
    );
}

// The marketplace payment window expires a still-pending payment; a
// completion verified after that expiry moves the payment to manual review
// with its history retained — never a confirmation, never dropped.
#[sqlx::test]
async fn late_completion_after_window_expiry_goes_to_manual_review(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Pending),
    );
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");

    // The window (3600 s) elapses while the lifecycle is still pending.
    app.clock.advance_seconds(3_601);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.payment_windows_expired, 1);
    let (state, _, revision) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "expired");
    assert_eq!(revision, 3);
    let (verification_state,): (String,) =
        sqlx::query_as("SELECT verification_state FROM payment_locks_correlations")
            .fetch_one(&app.pool)
            .await
            .expect("correlation row exists");
    assert_eq!(
        verification_state, "pending",
        "an expired window keeps polling so a late completion still surfaces"
    );

    // The completion arrives late: manual review, retained, no receipt.
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "manual_review");
    assert_eq!(
        order_state(&app.pool, &order.order_id).await,
        "pending_payment"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 0);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.manual_review'"
        )
        .await,
        1
    );
    let history: Vec<(String, String)> = sqlx::query_as(
        "SELECT observed_status, outcome FROM payment_locks_observations ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .expect("observations listed");
    assert_eq!(
        history,
        vec![
            ("pending".to_string(), "none".to_string()),
            ("window_elapsed".to_string(), "payment_expired".to_string()),
            ("completed".to_string(), "manual_review".to_string()),
        ],
        "the late completion and its reconciliation history are retained"
    );
    // The history is append-only by trigger.
    sqlx::query("UPDATE payment_locks_observations SET outcome = 'forged'")
        .execute(&app.pool)
        .await
        .expect_err("observations are append-only");
}

// A verified completion whose order can no longer be confirmed (the buyer
// cancelled while the lifecycle was pending) is retained under manual
// review instead of confirming a dead order — and instead of vanishing.
#[sqlx::test]
async fn completion_that_cannot_confirm_the_order_goes_to_manual_review(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;

    let cancel = common::order_command(
        "order.cancel_request",
        &order.order_id,
        1,
        json!({ "reason": "Changed my mind" }),
        50,
    );
    let (status, body) = execute(&app, &buyer.token, &cancel).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");

    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(summary.locks_completions_applied, 1);
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "manual_review");
    assert_eq!(order_state(&app.pool, &order.order_id).await, "cancelled");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM receipts").await, 0);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed'"
        )
        .await,
        0,
        "a dead order is never confirmed"
    );
}

// The verification task participates in the lease discipline like every
// other worker task: an instance that does not hold the lease skips it, and
// the task is recovered after the lease lapses.
#[sqlx::test]
async fn locks_verification_respects_worker_leases(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );

    let now = app.clock.now();
    assert!(try_acquire_lease(
        &app.pool,
        workers::TASK_LOCKS_VERIFICATION,
        instance_a,
        now,
        30
    )
    .await
    .expect("lease query runs"));

    let summary = run_once(&app.state, instance_b, now)
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 0,
        "excluded instance skips the task"
    );
    assert_eq!(fake.lookup_count(), 0);

    app.clock.advance_seconds(31);
    let summary = run_once(&app.state, instance_b, app.clock.now())
        .await
        .expect("worker pass runs");
    assert_eq!(
        summary.locks_completions_applied, 1,
        "the lapsed lease is recovered"
    );
    let (state, _, _) = payment_state(&app.pool, &order.payment_id).await;
    assert_eq!(state, "confirmed");
}

// Redaction (ADR-0019 §8): the bundle id and the lock resource appear in no
// command result, read projection, event, outbox intent, or notification —
// across the whole registration-to-confirmation flow.
#[sqlx::test]
async fn bundle_and_lock_resource_never_leave_the_correlation_store(pool: PgPool) {
    let (app, fake) = test_app_with_locks(pool).await;
    let holder = Uuid::new_v4();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let registration = register_locks(&app, &buyer.token, &order, &seller.pubky).await;
    fake.set_outcome(
        TEST_BUNDLE_ID,
        LocksLookupOutcome::Status(LocksTaskStatus::Completed),
    );
    run_once(&app.state, holder, app.clock.now())
        .await
        .expect("worker pass runs");

    let assert_redacted = |surface: &str, serialized: &str| {
        assert!(
            !serialized.contains(TEST_BUNDLE_ID),
            "{surface} leaks the bundle id"
        );
        assert!(
            !serialized.contains(TEST_LOCK_ID),
            "{surface} leaks the lock resource"
        );
        assert!(
            !serialized.contains("locks_bundle_id"),
            "{surface} reintroduces the removed bundle field"
        );
    };

    // The registration command result the buyer received.
    assert_redacted("registration result", &registration.to_string());

    // Every stored command result (idempotent replays serve these bytes).
    let stored_results: Vec<(Value,)> = sqlx::query_as("SELECT result FROM command_results")
        .fetch_all(&app.pool)
        .await
        .expect("command results listed");
    for (result,) in &stored_results {
        assert_redacted("stored command result", &result.to_string());
    }

    // Read projections, as each participant sees them.
    for actor in [&buyer, &seller] {
        let (status, payment) = send(
            app.router.clone(),
            "GET",
            &format!("/v1/payments/{}", order.payment_id),
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("payment projection", &payment.to_string());
        let (status, order_view) = send(
            app.router.clone(),
            "GET",
            &format!("/v1/orders/{}", order.order_id),
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("order projection", &order_view.to_string());
        let (status, notifications) = send(
            app.router.clone(),
            "GET",
            "/v1/notifications",
            Some(&actor.token),
            &json!(null),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_redacted("notifications projection", &notifications.to_string());
    }

    // Durable side-channel surfaces: events, outbox intents, notifications,
    // and the observation history hold statuses and ids only.
    for (surface, sql) in [
        (
            "events",
            "SELECT COALESCE(string_agg(kind, ','), '') FROM events",
        ),
        (
            "outbox",
            "SELECT COALESCE(string_agg(payload::text, ','), '') FROM outbox",
        ),
        (
            "notifications",
            "SELECT COALESCE(string_agg(type || aggregate_id, ','), '') FROM notifications",
        ),
        (
            "observations",
            "SELECT COALESCE(string_agg(observed_status || outcome, ','), '') \
             FROM payment_locks_observations",
        ),
    ] {
        let (serialized,): (String,) = sqlx::query_as(sql)
            .fetch_one(&app.pool)
            .await
            .expect("surface query runs");
        assert_redacted(surface, &serialized);
    }

    // At rest, the correlation row holds the bundle id only as ciphertext.
    let (ciphertext, token, hash): (Vec<u8>, Vec<u8>, String) = sqlx::query_as(
        "SELECT bundle_id_ciphertext, bundle_lookup_token, lock_resource_hash \
         FROM payment_locks_correlations",
    )
    .fetch_one(&app.pool)
    .await
    .expect("correlation row exists");
    assert!(!ciphertext
        .windows(TEST_BUNDLE_ID.len())
        .any(|window| window == TEST_BUNDLE_ID.as_bytes()));
    assert_ne!(token, TEST_BUNDLE_ID.as_bytes().to_vec());
    assert_redacted("lock resource hash", &hash);
}
