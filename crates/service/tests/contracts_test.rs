mod common;

use axum::http::StatusCode;
use common::paykit_review::{
    bound_order, confirm_call, into_awaiting_confirmation, into_manual_review_held,
    into_manual_review_late, resolve_call, OBSERVED_TXID, TOTAL_SATS,
};
use common::{create_pending_order, new_actor, send, test_app_with_payments, TestActor, TestApp};
use marketplace_service::contracts::{
    assert_no_sensitive_values, endpoint_contracts, normalized_snapshot, ReviewReason,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use uuid::Uuid;

type ContractMap = BTreeMap<String, Value>;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn snapshot_path(name: &str) -> PathBuf {
    if name == "endpoints" {
        repository_root().join("contracts/endpoints.json")
    } else {
        repository_root().join(format!("contracts/samples/{name}.json"))
    }
}

fn rendered_snapshot(value: &Value) -> Vec<u8> {
    let normalized = normalized_snapshot(value);
    assert_no_sensitive_values(&normalized);
    let mut rendered = serde_json::to_vec_pretty(&normalized).expect("snapshot serializes");
    rendered.push(b'\n');
    rendered
}

fn compare_snapshot(path: &Path, actual: &[u8], update: bool) -> Result<(), String> {
    if update {
        std::fs::create_dir_all(path.parent().expect("snapshot parent"))
            .map_err(|error| error.to_string())?;
        std::fs::write(path, actual).map_err(|error| error.to_string())?;
        return Ok(());
    }
    let expected = std::fs::read(path)
        .map_err(|error| format!("missing snapshot {}: {error}", path.display()))?;
    if expected == actual {
        Ok(())
    } else {
        Err(format!("snapshot drift: {}", path.display()))
    }
}

fn assert_snapshot(name: &str, value: &Value) {
    let actual = rendered_snapshot(value);
    compare_snapshot(
        &snapshot_path(name),
        &actual,
        std::env::var_os("UPDATE_CONTRACT_SNAPSHOTS").is_some(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn request_record(
    method: &str,
    path: String,
    headers_present: &[&str],
    body: Value,
    status: StatusCode,
    response: Value,
) -> Value {
    json!({
        "request": {
            "method": method,
            "path": path,
            "headers_present": headers_present,
            "body": body,
        },
        "response": {
            "status": status.as_u16(),
            "body": response,
        },
    })
}

fn assert_reason(status: StatusCode, body: &Value, expected: StatusCode, reason: &str) {
    assert_eq!(status, expected, "unexpected response: {body}");
    assert_eq!(body["error"]["reason"], json!(reason), "{body}");
}

fn assert_exact_keys(map: &ContractMap, expected: &[&str]) {
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

fn confirm_path(order_id: &str) -> String {
    format!("/v0/orders/{order_id}/confirm-bitcoin-payment")
}

fn resolve_path(order_id: &str) -> String {
    format!("/v0/orders/{order_id}/bitcoin/resolve")
}

fn insert_confirm(
    map: &mut ContractMap,
    key: &str,
    order_id: &str,
    body: Value,
    status: StatusCode,
    response: Value,
) {
    assert!(
        map.insert(
            key.to_string(),
            request_record(
                "POST",
                confirm_path(order_id),
                &["authorization", "content-type"],
                body,
                status,
                response,
            ),
        )
        .is_none(),
        "duplicate confirm case {key}"
    );
}

fn insert_resolve(
    map: &mut ContractMap,
    key: &str,
    order_id: &str,
    idempotency_key_present: bool,
    body: Value,
    status: StatusCode,
    response: Value,
) {
    let headers = if idempotency_key_present {
        vec!["authorization", "content-type", "idempotency-key"]
    } else {
        vec!["authorization", "content-type"]
    };
    assert!(
        map.insert(
            key.to_string(),
            request_record(
                "POST",
                resolve_path(order_id),
                &headers,
                body,
                status,
                response,
            ),
        )
        .is_none(),
        "duplicate resolve case {key}"
    );
}

async fn fresh_held(app: &TestApp, paykit: &common::FakePaykit) -> (TestActor, TestActor, String) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (order_id, _) = into_manual_review_held(app, paykit, &seller, &buyer).await;
    (seller, buyer, order_id)
}

#[test]
fn endpoint_snapshot_uses_real_handler_request_types() {
    assert_snapshot(
        "endpoints",
        &serde_json::to_value(endpoint_contracts()).expect("endpoint contracts serialize"),
    );
}

#[test]
fn reason_catalog_is_central_and_complete() {
    let endpoints = endpoint_contracts();
    for (name, expected) in [
        ("confirm", ReviewReason::for_endpoint("confirm")),
        ("resolve", ReviewReason::for_endpoint("resolve")),
    ] {
        let actual = endpoints
            .iter()
            .find(|endpoint| endpoint.path.contains(name))
            .expect("endpoint exists")
            .reasons
            .clone();
        let expected = expected
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{name} reason membership drifted");
    }
    let all = ReviewReason::all()
        .iter()
        .map(|reason| reason.as_str())
        .collect::<BTreeSet<_>>();
    let emitted = endpoints
        .into_iter()
        .flat_map(|endpoint| endpoint.reasons)
        .collect::<BTreeSet<_>>();
    assert_eq!(emitted, all);
}

#[test]
fn nested_values_and_residual_sensitive_values_are_rejected() {
    for value in [
        json!({"paykit_observation": {"nested": {"token": "secret"}}}),
        json!({"value": Uuid::new_v4().to_string()}),
        json!({"value": "2026-09-12T11:00:00Z"}),
        json!({"value": "a".repeat(52)}),
        json!({"value": "Bearer secret"}),
    ] {
        let result = std::panic::catch_unwind(|| assert_no_sensitive_values(&value));
        assert!(result.is_err(), "sensitive nested value must fail closed");
    }
}

#[test]
fn normalizer_replaces_embedded_only_pubkys() {
    let pubky = "y".repeat(52);
    let raw = json!({"aggregate_id": format!("listing:{pubky}:boots")});
    let normalized = normalized_snapshot(&raw);

    assert_eq!(normalized["aggregate_id"], json!("listing:<pubky:1>:boots"));
    assert_no_sensitive_values(&normalized);
    assert!(
        std::panic::catch_unwind(|| assert_no_sensitive_values(&raw)).is_err(),
        "raw embedded pubky must fail closed"
    );
}

#[test]
fn role_mapping_overrides_an_earlier_embedded_pubky() {
    let pubky = "b".repeat(52);
    let raw = json!({
        "aggregate_id": format!("listing:{pubky}:boots"),
        "seller_pubky": pubky,
    });
    let normalized = normalized_snapshot(&raw);

    assert_eq!(
        normalized["aggregate_id"],
        json!("listing:<pubky:seller>:boots")
    );
    assert_eq!(normalized["seller_pubky"], json!("<pubky:seller>"));
    assert_no_sensitive_values(&normalized);
    assert!(
        std::panic::catch_unwind(|| assert_no_sensitive_values(&raw)).is_err(),
        "raw role and embedded pubky must fail closed"
    );
}

#[test]
fn repeated_embedded_pubkys_keep_one_placeholder() {
    let pubky = "e".repeat(52);
    let raw = json!({
        "first": format!("before-{pubky}"),
        "second": format!("{pubky}-after-{pubky}"),
    });
    let normalized = normalized_snapshot(&raw);

    assert_eq!(normalized["first"], json!("before-<pubky:1>"));
    assert_eq!(normalized["second"], json!("<pubky:1>-after-<pubky:1>"));
    assert_no_sensitive_values(&normalized);
}

#[test]
fn content_hash_longer_than_a_pubky_is_unchanged() {
    let content_hash = "a".repeat(64);
    let raw = json!({"content_hash": content_hash});

    assert_eq!(normalized_snapshot(&raw), raw);
}

#[test]
fn exact_and_embedded_pubkys_are_detected_by_maximal_runs() {
    let pubky = "y".repeat(52);
    let raw = json!({
        "exact": pubky,
        "embedded": format!("listing:{pubky}_id"),
    });
    let normalized = normalized_snapshot(&raw);

    assert_eq!(normalized["exact"], json!("<pubky:1>"));
    assert_eq!(normalized["embedded"], json!("listing:<pubky:1>_id"));
    assert_no_sensitive_values(&normalized);
}

#[test]
fn paykit_references_normalize_distinctly_and_repeatably() {
    let raw = json!({
        "first": {"paykit_request_reference": "1R06AAAAAAAAAAAAAAAAAAAAAA"},
        "second": {"paykit_request_reference": "V4GMBBBBBBBBBBBBBBBBBBBBBB"},
        "repeat": {"paykit_request_reference": "1R06AAAAAAAAAAAAAAAAAAAAAA"},
    });
    let normalized = normalized_snapshot(&raw);

    assert_eq!(
        normalized["first"]["paykit_request_reference"],
        json!("<paykit-reference:1>")
    );
    assert_eq!(
        normalized["second"]["paykit_request_reference"],
        json!("<paykit-reference:2>")
    );
    assert_eq!(
        normalized["repeat"]["paykit_request_reference"],
        json!("<paykit-reference:1>")
    );
    assert_no_sensitive_values(&normalized);
}

#[test]
fn raw_paykit_reference_fails_residual_assertion() {
    let raw = json!({"paykit_request_reference": "1R06AAAAAAAAAAAAAAAAAAAAAA"});

    assert!(
        std::panic::catch_unwind(|| assert_no_sensitive_values(&raw)).is_err(),
        "raw paykit request reference must fail closed"
    );
}

#[test]
fn one_byte_snapshot_mutation_is_rejected_by_canonical_compare() {
    let path = std::env::temp_dir().join(format!("contract-drift-{}.json", Uuid::new_v4()));
    let mut actual = rendered_snapshot(&json!({"case": {"status": 422}}));
    compare_snapshot(&path, &actual, true).expect("calibration snapshot writes");
    let index = actual
        .iter()
        .position(|byte| *byte == b'{')
        .expect("snapshot has an object");
    actual[index] = b'[';
    assert!(
        compare_snapshot(&path, &actual, false).is_err(),
        "the canonical compare must reject one-byte drift"
    );
    std::fs::remove_file(path).expect("calibration snapshot removes");
}

#[sqlx::test(migrations = "./migrations")]
async fn confirm_contract_map_executes_every_case(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool).await;
    let mut map = ContractMap::new();

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = into_awaiting_confirmation(&app, &paykit, &seller, &buyer).await;

    let body = json!({"txid": format!("{OBSERVED_TXID}0")});
    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "confirmation_observation_mismatch",
    );
    insert_confirm(&mut map, "txid_mismatch", &order_id, body, status, response);

    let body = json!({"confirmed_amount_sats": TOTAL_SATS + 1});
    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "confirmation_observation_mismatch",
    );
    insert_confirm(
        &mut map,
        "amount_mismatch",
        &order_id,
        body,
        status,
        response,
    );

    let body = json!({"reason": "X".repeat(501)});
    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_reason",
    );
    insert_confirm(
        &mut map,
        "invalid_reason",
        &order_id,
        body,
        status,
        response,
    );

    let body = json!({});
    let (status, response) = confirm_call(&app, &buyer.token, &order_id, &body).await;
    assert_reason(status, &response, StatusCode::FORBIDDEN, "not_order_seller");
    insert_confirm(
        &mut map,
        "not_order_seller",
        &order_id,
        body,
        status,
        response,
    );

    let body = json!({"reason": "checked in wallet"});
    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["order"]["state"], json!("paid"));
    insert_confirm(
        &mut map,
        "success",
        &order_id,
        body.clone(),
        status,
        response,
    );

    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(
        response["confirmation"]["confirmation_basis"],
        json!("seller_attestation")
    );
    insert_confirm(&mut map, "replay", &order_id, body, status, response);

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _, _) = bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
    let body = json!({});
    let (status, response) = confirm_call(&app, &seller.token, &order_id, &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::CONFLICT,
        "order_not_awaiting_confirmation",
    );
    insert_confirm(
        &mut map,
        "order_not_awaiting_confirmation",
        &order_id,
        body,
        status,
        response,
    );

    let missing = Uuid::new_v4().to_string();
    let body = json!({});
    let (status, response) = confirm_call(&app, &seller.token, &missing, &body).await;
    assert_reason(status, &response, StatusCode::NOT_FOUND, "order_not_found");
    insert_confirm(
        &mut map,
        "order_not_found",
        &missing,
        body,
        status,
        response,
    );

    assert_exact_keys(
        &map,
        &[
            "success",
            "replay",
            "order_not_awaiting_confirmation",
            "txid_mismatch",
            "amount_mismatch",
            "invalid_reason",
            "not_order_seller",
            "order_not_found",
        ],
    );
    assert_snapshot("confirm", &json!(map));
}

#[sqlx::test(migrations = "./migrations")]
async fn resolve_contract_map_executes_every_case(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool.clone()).await;
    let mut map = ContractMap::new();

    let (seller, _buyer, order_id) = fresh_held(&app, &paykit).await;
    let key = Uuid::new_v4();
    let body = json!({"outcome": "paid", "reason": "checked in wallet"});
    let (status, response) = resolve_call(&app, &seller.token, &order_id, Some(key), &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resolution"]["outcome"], json!("paid"));
    insert_resolve(
        &mut map,
        "paid",
        &order_id,
        true,
        body.clone(),
        status,
        response,
    );

    let (status, response) = resolve_call(&app, &seller.token, &order_id, Some(key), &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resolution"]["outcome"], json!("paid"));
    insert_resolve(
        &mut map,
        "replay",
        &order_id,
        true,
        body.clone(),
        status,
        response,
    );

    let conflict_body = json!({"outcome": "abandoned"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(key), &conflict_body).await;
    assert_reason(status, &response, StatusCode::CONFLICT, "conflict");
    insert_resolve(
        &mut map,
        "key_body_conflict",
        &order_id,
        true,
        conflict_body,
        status,
        response,
    );

    let already_body = json!({"outcome": "abandoned"});
    let (status, response) = resolve_call(
        &app,
        &seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &already_body,
    )
    .await;
    assert_reason(status, &response, StatusCode::CONFLICT, "already_resolved");
    insert_resolve(
        &mut map,
        "already_resolved",
        &order_id,
        true,
        already_body,
        status,
        response,
    );

    let (seller, _buyer, order_id) = fresh_held(&app, &paykit).await;
    let body = json!({"outcome": "refunded", "external_refund_reference": "tx-contract-refund"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resolution"]["outcome"], json!("refunded"));
    insert_resolve(
        &mut map, "refunded", &order_id, true, body, status, response,
    );

    let (seller, _buyer, order_id) = fresh_held(&app, &paykit).await;
    let body = json!({"outcome": "abandoned", "reason": "buyer unreachable"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["resolution"]["outcome"], json!("abandoned"));
    insert_resolve(
        &mut map,
        "abandoned",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _, _) = bound_order(&app, &paykit, &seller, &buyer, "shared_manual").await;
    let body = json!({"outcome": "paid"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::CONFLICT,
        "not_in_manual_review",
    );
    insert_resolve(
        &mut map,
        "not_in_manual_review",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let body = json!({"outcome": "paid"});
    let (status, response) = resolve_call(
        &app,
        &seller.token,
        &order.order_id,
        Some(Uuid::new_v4()),
        &body,
    )
    .await;
    assert_reason(
        status,
        &response,
        StatusCode::CONFLICT,
        "resolution_not_applicable",
    );
    insert_resolve(
        &mut map,
        "resolution_not_applicable",
        &order.order_id,
        true,
        body,
        status,
        response,
    );

    let (seller, _buyer, order_id) = fresh_held(&app, &paykit).await;
    sqlx::query("UPDATE orders SET paykit_stack_id = NULL WHERE id = $1")
        .bind(Uuid::parse_str(&order_id).expect("order uuid"))
        .execute(&pool)
        .await
        .expect("clear deliberately malformed pin");
    let body = json!({"outcome": "paid"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(status, &response, StatusCode::CONFLICT, "missing_pin");
    insert_resolve(
        &mut map,
        "missing_pin",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, sold_quantity = total_quantity \
         WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{}_boots_01", seller.pubky))
    .execute(&pool)
    .await
    .expect("create deliberate sold-out precondition");
    let body = json!({"outcome": "paid"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(status, &response, StatusCode::CONFLICT, "stock_unavailable");
    insert_resolve(
        &mut map,
        "stock_unavailable",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let (seller, buyer, order_id) = fresh_held(&app, &paykit).await;
    let body = json!({"outcome": "paid"});
    let (status, response) = resolve_call(&app, &seller.token, &order_id, None, &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_idempotency_key",
    );
    insert_resolve(
        &mut map,
        "omitted_idempotency_key",
        &order_id,
        false,
        body,
        status,
        response,
    );

    let body = json!({"outcome": "chargeback"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_outcome",
    );
    insert_resolve(
        &mut map,
        "invalid_outcome",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let body = json!({"outcome": "refunded"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_refund_reference",
    );
    insert_resolve(
        &mut map,
        "missing_refund_reference",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let body = json!({"outcome": "paid", "external_refund_reference": "tx-forbidden"});
    let (status, response) =
        resolve_call(&app, &seller.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(
        status,
        &response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_refund_reference",
    );
    insert_resolve(
        &mut map,
        "forbidden_refund_reference",
        &order_id,
        true,
        body,
        status,
        response,
    );

    let body = json!({"outcome": "paid"});
    let (status, response) =
        resolve_call(&app, &buyer.token, &order_id, Some(Uuid::new_v4()), &body).await;
    assert_reason(status, &response, StatusCode::FORBIDDEN, "not_order_seller");
    insert_resolve(
        &mut map,
        "not_order_seller",
        &order_id,
        true,
        body,
        status,
        response,
    );

    assert_exact_keys(
        &map,
        &[
            "paid",
            "refunded",
            "abandoned",
            "replay",
            "key_body_conflict",
            "already_resolved",
            "not_in_manual_review",
            "stock_unavailable",
            "resolution_not_applicable",
            "missing_pin",
            "omitted_idempotency_key",
            "invalid_outcome",
            "missing_refund_reference",
            "forbidden_refund_reference",
            "not_order_seller",
        ],
    );
    assert_snapshot("resolve", &json!(map));
}

async fn projection_request(
    app: &TestApp,
    actor: &TestActor,
    order_id: &str,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&actor.token),
        &Value::Null,
    )
    .await
}

fn assert_projection(
    role: &str,
    expected_order_state: &str,
    expected_payment_state: &str,
    status: StatusCode,
    body: &Value,
) {
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], json!(expected_order_state));
    assert_eq!(body["payment"]["state"], json!(expected_payment_state));
    assert_eq!(body["next_actor"], json!("seller"));
    if role == "seller" {
        assert!(body.get("paykit_observation").is_some(), "{body}");
        match expected_payment_state {
            "awaiting_entitlement" => {
                assert!(
                    body.get("paykit_observation")
                        .is_some_and(|value| !value.is_null()),
                    "{body}"
                );
                assert!(
                    body.get("paykit_seller_confirmation_entered_at")
                        .is_some_and(|value| !value.is_null()),
                    "{body}"
                );
                assert!(
                    body.get("paykit_seller_confirmation_deadline")
                        .is_some_and(|value| !value.is_null()),
                    "{body}"
                );
            }
            "manual_review" => {
                assert!(
                    body["payment"]
                        .get("manual_review_entered_at")
                        .is_some_and(|value| !value.is_null()),
                    "{body}"
                );
                assert!(
                    body.get("paykit_seller_confirmation_entered_at")
                        .map(Value::is_null)
                        .unwrap_or(true),
                    "{body}"
                );
                assert!(
                    body.get("paykit_seller_confirmation_deadline")
                        .map(Value::is_null)
                        .unwrap_or(true),
                    "{body}"
                );
            }
            _ => {}
        }
    } else {
        assert!(body.get("paykit_observation").is_none(), "{body}");
        assert!(
            body.get("paykit_seller_confirmation_entered_at").is_none(),
            "{body}"
        );
        assert!(
            body.get("paykit_seller_confirmation_deadline").is_none(),
            "{body}"
        );
        assert!(
            body["payment"].get("manual_review_entered_at").is_none(),
            "{body}"
        );
    }
}

fn insert_projection(
    map: &mut ContractMap,
    key: &str,
    order_id: &str,
    status: StatusCode,
    response: Value,
) {
    assert!(
        map.insert(
            key.to_string(),
            request_record(
                "GET",
                format!("/v1/orders/{order_id}"),
                &["authorization"],
                Value::Null,
                status,
                response,
            ),
        )
        .is_none(),
        "duplicate projection case {key}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn projection_contract_map_executes_every_role_and_state(pool: PgPool) {
    let (app, _stripe, paykit) = test_app_with_payments(pool).await;
    let mut map = ContractMap::new();

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = into_awaiting_confirmation(&app, &paykit, &seller, &buyer).await;
    let (status, response) = projection_request(&app, &seller, &order_id).await;
    assert_projection(
        "seller",
        "pending_payment",
        "awaiting_entitlement",
        status,
        &response,
    );
    insert_projection(
        &mut map,
        "seller_awaiting_confirmation",
        &order_id,
        status,
        response,
    );
    let (status, response) = projection_request(&app, &buyer, &order_id).await;
    assert_projection(
        "buyer",
        "pending_payment",
        "awaiting_entitlement",
        status,
        &response,
    );
    insert_projection(
        &mut map,
        "buyer_awaiting_confirmation",
        &order_id,
        status,
        response,
    );

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = into_manual_review_held(&app, &paykit, &seller, &buyer).await;
    let (status, response) = projection_request(&app, &seller, &order_id).await;
    assert_projection(
        "seller",
        "pending_payment",
        "manual_review",
        status,
        &response,
    );
    insert_projection(
        &mut map,
        "seller_manual_review_held",
        &order_id,
        status,
        response,
    );
    let (status, response) = projection_request(&app, &buyer, &order_id).await;
    assert_projection(
        "buyer",
        "pending_payment",
        "manual_review",
        status,
        &response,
    );
    insert_projection(
        &mut map,
        "buyer_manual_review_held",
        &order_id,
        status,
        response,
    );

    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _) = into_manual_review_late(&app, &paykit, &seller, &buyer).await;
    let (status, response) = projection_request(&app, &seller, &order_id).await;
    assert_projection("seller", "cancelled", "manual_review", status, &response);
    insert_projection(
        &mut map,
        "seller_manual_review_late",
        &order_id,
        status,
        response,
    );
    let (status, response) = projection_request(&app, &buyer, &order_id).await;
    assert_projection("buyer", "cancelled", "manual_review", status, &response);
    insert_projection(
        &mut map,
        "buyer_manual_review_late",
        &order_id,
        status,
        response,
    );

    assert_exact_keys(
        &map,
        &[
            "seller_awaiting_confirmation",
            "buyer_awaiting_confirmation",
            "seller_manual_review_held",
            "buyer_manual_review_held",
            "seller_manual_review_late",
            "buyer_manual_review_late",
        ],
    );
    assert_snapshot("projections", &json!(map));
}
