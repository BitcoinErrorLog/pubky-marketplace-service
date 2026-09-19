//! Executable deterministic JSON contract for Phase 6 Wave 1 inventory.
//! Samples are generated from the real router/PostgreSQL/AuthToken path and
//! are never hand-edited.

mod common;

use axum::http::StatusCode;
use common::{execute, listing_aggregate, new_actor, register_command, send, test_app};
use marketplace_service::contracts::normalized_snapshot;
use marketplace_service::inventory::{inventory_adjust_schema, inventory_projection_schema};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn snapshot_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/samples/inventory.json")
}

fn rendered_snapshot(value: &Value) -> Vec<u8> {
    let mut rendered =
        serde_json::to_vec_pretty(&normalized_snapshot(value)).expect("snapshot serializes");
    rendered.push(b'\n');
    rendered
}

fn compare_snapshot(actual: &[u8]) -> Result<(), String> {
    let path = snapshot_path();
    if std::env::var_os("UPDATE_CONTRACT_SNAPSHOTS").is_some() {
        std::fs::write(&path, actual).map_err(|error| error.to_string())?;
        return Ok(());
    }
    let expected =
        std::fs::read(&path).map_err(|error| format!("missing {}: {error}", path.display()))?;
    (expected == actual)
        .then_some(())
        .ok_or_else(|| format!("snapshot drift: {}", path.display()))
}

fn request(seller: &str, expected_revision: i64, delta: i64) -> Value {
    json!({
        "schema_version": 1,
        "kind": "inventory.adjust",
        "aggregate_id": listing_aggregate(seller),
        "listing_id": "boots_01",
        "expected_revision": expected_revision,
        "delta": delta,
        "idempotency_key": "00000000-0000-4000-8000-000000006001",
        "external_ref": {
            "channel": "shopify",
            "external_id": "evt-contract-1"
        }
    })
}

fn exchange(method: &str, path: &str, request: Value, status: StatusCode, body: Value) -> Value {
    json!({
        "request": {
            "method": method,
            "path": path,
            "headers_present": ["authorization", "content-type"],
            "body": request,
        },
        "response": {
            "status": status.as_u16(),
            "body": body,
        }
    })
}

#[sqlx::test(migrations = "./migrations")]
async fn inventory_contract_regenerates_from_executable_routes(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "register listing: {body}");
    let aggregate = listing_aggregate(&seller.pubky);
    let projection_path = format!("/v1/inventory/listings/{aggregate}");
    let adjust_path = "/v1/inventory/adjust";
    let mut cases = BTreeMap::new();

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &projection_path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    cases.insert(
        "projection_before",
        exchange("GET", &projection_path, Value::Null, status, body),
    );

    let adjust = request(&seller.pubky, 1, 2);
    let (status, body) = send(
        app.router.clone(),
        "POST",
        adjust_path,
        Some(&seller.token),
        &adjust,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    cases.insert(
        "adjust_success",
        exchange("POST", adjust_path, adjust.clone(), status, body.clone()),
    );

    let (status, replay) = send(
        app.router.clone(),
        "POST",
        adjust_path,
        Some(&seller.token),
        &adjust,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, body);
    cases.insert(
        "adjust_replay",
        exchange("POST", adjust_path, adjust.clone(), status, replay),
    );

    let changed = request(&seller.pubky, 1, 3);
    let (status, body) = send(
        app.router.clone(),
        "POST",
        adjust_path,
        Some(&seller.token),
        &changed,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    cases.insert(
        "changed_replay_quarantined",
        exchange("POST", adjust_path, changed, status, body),
    );

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &projection_path,
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("variants").is_none());
    cases.insert(
        "projection_after",
        exchange("GET", &projection_path, Value::Null, status, body),
    );

    let artifact = json!({
        "schema_version": 1,
        "kind": "inventory_http_contract",
        "request_schema": inventory_adjust_schema(),
        "projection_schema": inventory_projection_schema(),
        "cases": cases,
    });
    compare_snapshot(&rendered_snapshot(&artifact)).unwrap_or_else(|error| panic!("{error}"));
}
