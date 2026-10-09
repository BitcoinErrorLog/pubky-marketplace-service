//! The USDT refund destination: the buyer confirms an Arbitrum One address
//! (`refund.confirm_destination`), the seller refunds from their own wallet
//! and records the Arbitrum transaction hash (`refund.record_external` or the
//! manual-review `refunded` resolution). Orders paid with any other method
//! behave exactly as before.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::StatusCode;
use chrono::Duration;
use common::fx_feed::{fx_body, FakeFxFeed};
use common::paykit_review::{delivered_notifications, enable_bitcoin, poll_now, resolve_call};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::PaymentTerms;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

/// A mixed-case address whose EIP-55 checksum is right (EIP-55 test vector).
const CHECKSUMMED: &str = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed";
const LOWERCASE: &str = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
const OTHER_LOWERCASE: &str = "0x27b1fdb04752bbc536007a920d24acb045561c26";
const RATE: &str = "77197";
const ORDER_TOTAL_CENTS: i64 = 13_700;

static COMMAND_NUMBER: AtomicU64 = AtomicU64::new(40_000);

fn next_command_number() -> u64 {
    COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed)
}

fn uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).expect("uuid")
}

fn tx_hash(byte: u8) -> String {
    format!("0x{}", format!("{byte:02x}").repeat(32))
}

async fn make_usdt(pool: &PgPool, order_id: &str, total_cents: i64) {
    let terms = PaymentTerms::usdt_at_parity("USD", 2, total_cents).expect("parity quote");
    let mut conn = pool.acquire().await.expect("connection");
    assert!(terms
        .store(&mut conn, uuid(order_id))
        .await
        .expect("terms store"));
    sqlx::query("UPDATE orders SET payment_method = 'usdt' WHERE id = $1")
        .bind(uuid(order_id))
        .execute(&mut *conn)
        .await
        .expect("usdt method");
}

/// A paid order (sandbox payment confirmed) relabelled as a USDT order, the
/// way S1's model carries one.
async fn paid_usdt_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PaidOrder {
    let order = create_paid_order(app, seller, buyer).await;
    make_usdt(&app.pool, &order.order_id, order.total_minor).await;
    order
}

async fn revision(pool: &PgPool, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1")
        .bind(uuid(order_id))
        .fetch_one(pool)
        .await
        .expect("order revision")
}

/// Moves a paid order to `cancelled`, the state a refund after cancellation
/// starts from.
async fn cancel(pool: &PgPool, order_id: &str) {
    sqlx::query("UPDATE orders SET state = 'cancelled', revision = revision + 1 WHERE id = $1")
        .bind(uuid(order_id))
        .execute(pool)
        .await
        .expect("cancel");
}

async fn confirm(app: &TestApp, token: &str, order_id: &str, address: &str) -> (StatusCode, Value) {
    let revision = revision(&app.pool, order_id).await;
    execute(
        app,
        token,
        &order_command(
            "refund.confirm_destination",
            order_id,
            revision,
            json!({ "address": address }),
            next_command_number(),
        ),
    )
    .await
}

async fn record(
    app: &TestApp,
    token: &str,
    order_id: &str,
    amount_minor: i64,
    transaction_id: &str,
) -> (StatusCode, Value) {
    let revision = revision(&app.pool, order_id).await;
    execute(
        app,
        token,
        &order_command(
            "refund.record_external",
            order_id,
            revision,
            json!({ "amount_minor": amount_minor, "transaction_id": transaction_id }),
            next_command_number(),
        ),
    )
    .await
}

async fn read_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

async fn list_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/orders",
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order list failed: {body}");
    body["orders"]
        .as_array()
        .expect("orders array")
        .iter()
        .find(|order| order["id"] == json!(order_id))
        .cloned()
        .expect("order in the list")
}

async fn stored_destination(pool: &PgPool, order_id: &str) -> Option<(String, String)> {
    sqlx::query_as(
        "SELECT address, address_source FROM order_refund_destinations WHERE order_id = $1",
    )
    .bind(uuid(order_id))
    .fetch_optional(pool)
    .await
    .expect("destination row")
}

async fn event_count(pool: &PgPool, order_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = $2")
        .bind(format!("order:{order_id}"))
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("event count")
}

fn assert_refusal(
    outcome: &(StatusCode, Value),
    status: StatusCode,
    code: &str,
    reason: Option<&str>,
) {
    let (actual, body) = outcome;
    assert_eq!(*actual, status, "unexpected status: {body}");
    assert_eq!(body["ok"], json!(false), "{body}");
    assert_eq!(body["error"]["code"], json!(code), "{body}");
    match reason {
        Some(reason) => assert_eq!(body["error"]["reason"], json!(reason), "{body}"),
        None => assert!(body["error"].get("reason").is_none(), "{body}"),
    }
}

// ---------------------------------------------------------------------------
// Confirming the address
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_buyer_confirms_an_address_and_both_participants_read_it(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;

    for token in [&buyer.token, &seller.token] {
        let before = read_order(&app, token, &order.order_id).await;
        assert_eq!(
            before["refund_destination"],
            Value::Null,
            "a USDT order without an address projects null"
        );
    }

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, CHECKSUMMED).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"]["order"];
    assert_eq!(result["id"], json!(order.order_id));
    assert_eq!(result["state"], json!("paid"));
    let destination = &result["refund_destination"];
    assert_eq!(destination["address"], json!(CHECKSUMMED));
    assert_eq!(destination["network"], json!("arbitrum-one"));
    assert_eq!(destination["asset"], json!("USDT"));
    assert_eq!(destination["source"], json!("buyer_entered"));
    assert!(destination["confirmed_at"].is_string());

    for token in [&buyer.token, &seller.token] {
        let single = read_order(&app, token, &order.order_id).await;
        assert_eq!(single["refund_destination"], *destination);
        let listed = list_order(&app, token, &order.order_id).await;
        assert_eq!(listed["refund_destination"], *destination);
    }
    assert_eq!(
        stored_destination(&pool, &order.order_id).await,
        Some((CHECKSUMMED.to_string(), "buyer_entered".to_string()))
    );

    // Not a participant: the order does not exist for them, so neither does
    // its destination.
    let stranger = new_actor(&app).await;
    let (status, _) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{}", order.order_id),
        Some(&stranger.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_confirmation_is_an_event_and_tells_the_seller(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;

    let revision_before = revision(&pool, &order.order_id).await;
    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        revision(&pool, &order.order_id).await,
        revision_before + 1,
        "a confirmation advances the order revision"
    );
    assert_eq!(
        event_count(&pool, &order.order_id, "refund.destination_confirmed").await,
        1
    );
    let notices =
        delivered_notifications(&app, &seller.token, "refund_destination_confirmed").await;
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert!(
        delivered_notifications(&app, &buyer.token, "refund_destination_confirmed")
            .await
            .is_empty(),
        "the buyer is not told about their own confirmation"
    );

    // Confirming the same address again is still a confirmation.
    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        event_count(&pool, &order.order_id, "refund.destination_confirmed").await,
        2
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_buyer_replaces_the_address_until_a_refund_is_recorded(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;
    cancel(&pool, &order.order_id).await;

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = confirm(&app, &buyer.token, &order.order_id, OTHER_LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["refund_destination"]["address"],
        json!(OTHER_LOWERCASE)
    );
    assert_eq!(
        stored_destination(&pool, &order.order_id).await,
        Some((OTHER_LOWERCASE.to_string(), "buyer_entered".to_string())),
        "one row per order, the latest address"
    );

    let hash = tx_hash(0xab);
    let (status, body) = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let refused = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_refusal(&refused, StatusCode::CONFLICT, "INVALID_STATE", None);
    assert_eq!(
        stored_destination(&pool, &order.order_id).await,
        Some((OTHER_LOWERCASE.to_string(), "buyer_entered".to_string())),
        "a recorded refund fixes the address"
    );
}

// ---------------------------------------------------------------------------
// Address validation, and the original-address choice
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn addresses_are_validated_and_a_bad_one_changes_nothing(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;

    let mut wrong_checksum = CHECKSUMMED.to_string();
    wrong_checksum.replace_range(5..6, "a");
    for address in [
        "0x1234".to_string(),
        "0x".to_string() + &"g".repeat(40),
        format!("0x{}", "a".repeat(41)),
        "d8da6bf26964af9d7eed9e03e53415d37aa96045".to_string(),
        "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq".to_string(),
        "ENS.eth".to_string(),
        wrong_checksum,
    ] {
        let revision_before = revision(&pool, &order.order_id).await;
        let refused = confirm(&app, &buyer.token, &order.order_id, &address).await;
        assert_refusal(
            &refused,
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_COMMAND",
            Some("invalid_refund_destination"),
        );
        assert_eq!(revision(&pool, &order.order_id).await, revision_before);
        assert_eq!(stored_destination(&pool, &order.order_id).await, None);
    }

    // An empty or oversized value is refused by the command contract.
    for address in [String::new(), "0x".to_string() + &"a".repeat(200)] {
        let refused = confirm(&app, &buyer.token, &order.order_id, &address).await;
        assert_eq!(refused.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", refused.1);
        assert_eq!(refused.1["error"]["code"], json!("INVALID_COMMAND"));
    }

    // A correct checksum, an unchecksummed lower-case address and
    // surrounding whitespace are all accepted.
    for (input, stored) in [
        (CHECKSUMMED.to_string(), CHECKSUMMED),
        (LOWERCASE.to_string(), LOWERCASE),
        (format!("  {OTHER_LOWERCASE}\n"), OTHER_LOWERCASE),
    ] {
        let (status, body) = confirm(&app, &buyer.token, &order.order_id, &input).await;
        assert_eq!(status, StatusCode::OK, "{input:?}: {body}");
        assert_eq!(
            body["result"]["order"]["refund_destination"]["address"],
            json!(stored)
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_a_buyer_entered_address_exists_until_paykit_exposes_the_payer(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;

    // There is no way to ask for "the address I paid from": the payer address
    // is not known to the service, and the command is closed.
    for extra in [
        json!({ "source": "payment_address" }),
        json!({ "use_original_address": true }),
        json!({ "address_source": "payment_address" }),
    ] {
        let mut payload = json!({ "address": LOWERCASE });
        for (key, value) in extra.as_object().expect("extra fields") {
            payload[key] = value.clone();
        }
        let revision = revision(&pool, &order.order_id).await;
        let (status, body) = execute(
            &app,
            &buyer.token,
            &order_command(
                "refund.confirm_destination",
                &order.order_id,
                revision,
                payload,
                next_command_number(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    }
    assert_eq!(stored_destination(&pool, &order.order_id).await, None);

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["refund_destination"]["source"],
        json!("buyer_entered")
    );

    let widened = sqlx::query(
        "UPDATE order_refund_destinations SET address_source = 'payment_address' WHERE order_id = $1",
    )
    .bind(uuid(&order.order_id))
    .execute(&pool)
    .await;
    assert!(
        widened.is_err(),
        "the database accepts only the buyer-entered source"
    );
}

// ---------------------------------------------------------------------------
// Authorization and eligibility
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_buyer_sets_the_address_and_the_seller_records_the_hash(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;
    cancel(&pool, &order.order_id).await;

    let refused = confirm(&app, &seller.token, &order.order_id, LOWERCASE).await;
    assert_refusal(&refused, StatusCode::FORBIDDEN, "UNAUTHORIZED", None);
    let refused = confirm(&app, &stranger.token, &order.order_id, LOWERCASE).await;
    assert_refusal(&refused, StatusCode::FORBIDDEN, "UNAUTHORIZED", None);
    assert_eq!(stored_destination(&pool, &order.order_id).await, None);

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let hash = tx_hash(0xcd);
    let refused = record(
        &app,
        &buyer.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_refusal(&refused, StatusCode::FORBIDDEN, "UNAUTHORIZED", None);
    let refused = record(
        &app,
        &stranger.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_refusal(&refused, StatusCode::FORBIDDEN, "UNAUTHORIZED", None);

    let (status, body) = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_a_received_usdt_payment_has_a_refund_address(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;

    // Any other method: nothing to confirm, and no row.
    let sandbox = create_paid_order(&app, &seller, &buyer).await;
    let refused = confirm(&app, &buyer.token, &sandbox.order_id, LOWERCASE).await;
    assert_refusal(&refused, StatusCode::CONFLICT, "INVALID_STATE", None);
    for method in ["bitcoin", "paypal", "stripe"] {
        sqlx::query("UPDATE orders SET payment_method = $2 WHERE id = $1")
            .bind(uuid(&sandbox.order_id))
            .bind(method)
            .execute(&pool)
            .await
            .expect("method");
        let refused = confirm(&app, &buyer.token, &sandbox.order_id, LOWERCASE).await;
        assert_refusal(&refused, StatusCode::CONFLICT, "INVALID_STATE", None);
    }
    assert_eq!(stored_destination(&pool, &sandbox.order_id).await, None);

    // A USDT order whose payment has not been received owes nothing.
    let order = paid_usdt_order(&app, &seller, &buyer).await;
    for state in ["awaiting_entitlement", "detected", "expired"] {
        sqlx::query("UPDATE payments SET state = $2 WHERE order_id = $1")
            .bind(uuid(&order.order_id))
            .bind(state)
            .execute(&pool)
            .await
            .expect("payment state");
        let refused = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
        assert_refusal(&refused, StatusCode::CONFLICT, "INVALID_STATE", None);
    }
    assert_eq!(stored_destination(&pool, &order.order_id).await, None);
}

// ---------------------------------------------------------------------------
// Recording the refund
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_needs_the_address_and_an_arbitrum_hash(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;
    cancel(&pool, &order.order_id).await;
    let hash = tx_hash(0x4f);

    let refused = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_refusal(
        &refused,
        StatusCode::CONFLICT,
        "INVALID_STATE",
        Some("refund_destination_required"),
    );

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, CHECKSUMMED).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for transaction_id in [
        "bitcoin-tx-evidence-123".to_string(),
        hash.to_uppercase().replace("0X", "0x"),
        hash[2..].to_string(),
        format!("{hash}ab"),
        hash[..65].to_string(),
    ] {
        let refused = record(
            &app,
            &seller.token,
            &order.order_id,
            order.total_minor,
            &transaction_id,
        )
        .await;
        assert_refusal(
            &refused,
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_COMMAND",
            Some("invalid_refund_reference"),
        );
    }
    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(uuid(&order.order_id))
        .fetch_one(&pool)
        .await
        .expect("state");
    assert_eq!(state, "cancelled", "refusals change nothing");

    let (status, body) = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        &hash,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let recorded = &body["result"]["order"];
    assert_eq!(recorded["state"], json!("refunded_external"));
    assert_eq!(recorded["external_refund"]["transaction_id"], json!(hash));
    assert_eq!(
        recorded["external_refund"]["amount_minor"],
        json!(order.total_minor)
    );
    assert_eq!(
        recorded["external_refund"]["destination_address"],
        json!(CHECKSUMMED),
        "the address is copied into the record"
    );

    // The record keeps the address even if the row could change later.
    sqlx::query("UPDATE order_refund_destinations SET address = $2 WHERE order_id = $1")
        .bind(uuid(&order.order_id))
        .bind(LOWERCASE)
        .execute(&pool)
        .await
        .expect("row rewrite");
    for token in [&buyer.token, &seller.token] {
        let order_view = read_order(&app, token, &order.order_id).await;
        assert_eq!(
            order_view["external_refund"]["destination_address"],
            json!(CHECKSUMMED)
        );
        assert_eq!(order_view["state"], json!("refunded_external"));
        assert!(order_view["refund_destination"].is_object());
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_digital_usdt_refund_after_delivery_follows_the_same_rules(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = paid_usdt_order(&app, &seller, &buyer).await;
    sqlx::query(
        "UPDATE orders SET fulfillment = 'digital', state = 'delivered', \
         revision = revision + 1 WHERE id = $1",
    )
    .bind(uuid(&order.order_id))
    .execute(&pool)
    .await
    .expect("digital delivered");

    let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let refused = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        "tx-evidence-123",
    )
    .await;
    assert_eq!(refused.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", refused.1);
    let (status, body) = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        &tx_hash(0x11),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["external_refund"]["destination_address"],
        json!(LOWERCASE)
    );
}

// ---------------------------------------------------------------------------
// Orders of any other method, and the flag
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn orders_of_other_methods_are_untouched(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;

    for token in [&buyer.token, &seller.token] {
        let view = read_order(&app, token, &order.order_id).await;
        assert!(
            view.get("refund_destination").is_none(),
            "no key on a non-USDT order"
        );
        let listed = list_order(&app, token, &order.order_id).await;
        assert!(listed.get("refund_destination").is_none());
    }

    // The original rules: any 8 to 200 character evidence string, no address,
    // and a record without a `destination_address`.
    cancel(&pool, &order.order_id).await;
    let (status, body) = record(
        &app,
        &seller.token,
        &order.order_id,
        order.total_minor,
        "bitcoin-tx-evidence-123",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let external_refund = &body["result"]["order"]["external_refund"];
    assert_eq!(
        external_refund["transaction_id"],
        json!("bitcoin-tx-evidence-123")
    );
    assert!(external_refund.get("destination_address").is_none());
    assert_eq!(stored_destination(&pool, &order.order_id).await, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_flag_never_strands_an_existing_usdt_refund(pool: PgPool) {
    for flag in [false, true] {
        let mut config = Config::for_tests();
        config.usdt_payments_enabled = flag;
        let app = test_app_with_config(pool.clone(), config).await;
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let order = paid_usdt_order(&app, &seller, &buyer).await;
        cancel(&pool, &order.order_id).await;

        let (status, body) = confirm(&app, &buyer.token, &order.order_id, LOWERCASE).await;
        assert_eq!(status, StatusCode::OK, "flag {flag}: {body}");
        let (status, body) = record(
            &app,
            &seller.token,
            &order.order_id,
            order.total_minor,
            &tx_hash(0x22),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "flag {flag}: {body}");
        let order_view = read_order(&app, &buyer.token, &order.order_id).await;
        assert_eq!(
            order_view["external_refund"]["destination_address"],
            json!(LOWERCASE)
        );
    }
}

// ---------------------------------------------------------------------------
// Manual review: `refunded` resolution
// ---------------------------------------------------------------------------

struct ReviewFixture {
    app: TestApp,
    paykit: FakePaykit,
    seller: TestActor,
    buyer: TestActor,
}

async fn review_fixture(pool: PgPool) -> ReviewFixture {
    let (app, paykit, fx): (TestApp, FakePaykit, FakeFxFeed) =
        test_app_with_payments_and_fx(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 20)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let now = app.clock.now();
    fx.set_body(fx_body(RATE, now.timestamp_millis()));
    for back in 0..3 {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = chrono::DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank')",
        )
        .bind(RATE)
        .bind(accepted)
        .bind(bucket)
        .execute(&pool)
        .await
        .expect("seed fx sample");
    }
    ReviewFixture {
        app,
        paykit,
        seller,
        buyer,
    }
}

/// A USD order bound to Bitcoin whose late settlement put the payment in
/// manual review (the order is cancelled and refundable).
async fn manual_review_order(fixture: &ReviewFixture) -> String {
    let app = &fixture.app;
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(listing_aggregate(&fixture.seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    sqlx::query(
        "UPDATE listings SET available_quantity = total_quantity, reserved_quantity = 0, \
         sold_quantity = 0, state = 'available' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&app.pool)
    .await
    .expect("restock");
    let mut checkout = checkout_command_with_id(&fixture.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &fixture.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&fixture.buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    drain_outbox(&app.pool, Some(client), app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let total_sats: i64 = sqlx::query_scalar("SELECT paykit_total_sats FROM orders WHERE id = $1")
        .bind(uuid(&order_id))
        .fetch_one(&app.pool)
        .await
        .expect("invoice total");
    let after_window = app.clock.now() + Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("window sweep runs")
            >= 1
    );
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&fixture.seller.pubky))
    .execute(&app.pool)
    .await
    .expect("sold out so late money cannot complete");
    let mut late = bitcoin_status_v2(
        "confirmed",
        true,
        "exclusive",
        Some("usdt-review-txid"),
        Some(total_sats as u64),
        Some(2),
    );
    late["late_settlement"] = json!(true);
    fixture
        .paykit
        .set_status(&attempt_reference(uuid(&order_id), 1), late);
    assert!(poll_now(app, after_window + Duration::seconds(60)).await >= 1);
    let payment_state: String =
        sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&app.pool)
            .await
            .expect("payment state");
    assert_eq!(payment_state, "manual_review");
    order_id
}

async fn resolve_refunded(
    fixture: &ReviewFixture,
    token: &str,
    order_id: &str,
    reference: &str,
) -> (StatusCode, Value) {
    resolve_call(
        &fixture.app,
        token,
        order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_manual_review_refund_of_usdt_takes_the_address_and_the_arbitrum_hash(pool: PgPool) {
    let fixture = review_fixture(pool.clone()).await;
    let order_id = manual_review_order(&fixture).await;
    make_usdt(&pool, &order_id, ORDER_TOTAL_CENTS).await;
    let app = &fixture.app;
    let hash = tx_hash(0x9a);

    // No address yet.
    let refused = resolve_refunded(&fixture, &fixture.seller.token, &order_id, &hash).await;
    assert_refusal(
        &refused,
        StatusCode::CONFLICT,
        "INVALID_STATE",
        Some("refund_destination_required"),
    );

    // The buyer can confirm an address while the payment is in review.
    let (status, body) = confirm(app, &fixture.buyer.token, &order_id, CHECKSUMMED).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A Bitcoin-shaped, upper-case or truncated reference is not a hash,
    // and only the seller may resolve.
    for reference in [
        "tx-refund-1".to_string(),
        hash.to_uppercase().replace("0X", "0x"),
        hash[..65].to_string(),
    ] {
        let refused =
            resolve_refunded(&fixture, &fixture.seller.token, &order_id, &reference).await;
        assert_refusal(
            &refused,
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_COMMAND",
            Some("invalid_refund_reference"),
        );
    }
    let refused = resolve_refunded(&fixture, &fixture.buyer.token, &order_id, &hash).await;
    assert_refusal(
        &refused,
        StatusCode::FORBIDDEN,
        "UNAUTHORIZED",
        Some("not_order_seller"),
    );
    let payment_state: String =
        sqlx::query_scalar("SELECT state FROM payments WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("payment state");
    assert_eq!(payment_state, "manual_review", "refusals change nothing");

    // The 66-character hash passes the manual-review reference cap.
    assert_eq!(hash.len(), 66);
    let (status, body) = resolve_refunded(&fixture, &fixture.seller.token, &order_id, &hash).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let recorded = &body["order"];
    assert_eq!(recorded["state"], json!("refunded_external"));
    assert_eq!(recorded["external_refund"]["transaction_id"], json!(hash));
    assert_eq!(
        recorded["external_refund"]["amount_minor"],
        json!(ORDER_TOTAL_CENTS),
        "the quoted USDT amount in order units"
    );
    assert_eq!(
        recorded["external_refund"]["destination_address"],
        json!(CHECKSUMMED)
    );
    let reference: Option<String> =
        sqlx::query_scalar("SELECT refund_reference FROM payments WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("refund reference");
    assert_eq!(reference.as_deref(), Some(hash.as_str()));

    let refused = confirm(app, &fixture.buyer.token, &order_id, LOWERCASE).await;
    assert_refusal(&refused, StatusCode::CONFLICT, "INVALID_STATE", None);
    let buyer_view = read_order(app, &fixture.buyer.token, &order_id).await;
    assert_eq!(
        buyer_view["refund_destination"]["address"],
        json!(CHECKSUMMED)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_bitcoin_reference_cap_is_unchanged(pool: PgPool) {
    let fixture = review_fixture(pool.clone()).await;
    let order_id = manual_review_order(&fixture).await;

    // A transaction hash is 66 characters: over the Bitcoin cap, so a Bitcoin
    // order refuses it, and the seller is still checked before the shape of
    // the reference reaches the order.
    let hash = tx_hash(0x9a);
    let refused = resolve_refunded(&fixture, &fixture.seller.token, &order_id, &hash).await;
    assert_refusal(
        &refused,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("invalid_refund_reference"),
    );
    let refused = resolve_refunded(&fixture, &fixture.buyer.token, &order_id, &hash).await;
    assert_refusal(
        &refused,
        StatusCode::FORBIDDEN,
        "UNAUTHORIZED",
        Some("not_order_seller"),
    );
    let too_long = "a".repeat(65);
    let refused = resolve_refunded(&fixture, &fixture.seller.token, &order_id, &too_long).await;
    assert_refusal(
        &refused,
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_COMMAND",
        Some("invalid_refund_reference"),
    );

    let at_the_cap = "a".repeat(64);
    let (status, body) =
        resolve_refunded(&fixture, &fixture.seller.token, &order_id, &at_the_cap).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let external_refund = &body["order"]["external_refund"];
    assert_eq!(external_refund["transaction_id"], json!(at_the_cap));
    assert!(external_refund.get("destination_address").is_none());
}
