//! Moderation tests (plan task 3.5): trust report submission (ported from
//! the prototype case "records structured trust reports without exposing
//! them to ordinary users"), role-scoped report queries with a cross-user
//! authorization rejection, moderator decisions recorded append-only, and
//! the configured moderator list replacing the prototype's hardcoded
//! sandbox moderator identity.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    authenticate, count, execute, listing_aggregate, new_actor, random_keypair, report_command,
    send, test_app_with_moderators,
};

fn decide_command(report_id: &str, expected_revision: i64, decision: &str) -> Value {
    json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000001300",
        "aggregate_id": format!("report:{report_id}"),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "trust.decide",
        "payload": {
            "report_id": report_id,
            "decision": decision,
            "rationale": "Evidence supports the reporter.",
        },
    })
}

async fn list_reports(app: &common::TestApp, token: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/reports",
        Some(token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "report query failed: {body}");
    body["reports"].clone()
}

// TS case: "records structured trust reports without exposing them to
// ordinary users" — adapted to the configured moderator list and the
// role-scoped GET /v1/reports endpoint.
#[sqlx::test]
async fn records_structured_trust_reports_without_exposing_them_to_ordinary_users(pool: PgPool) {
    let (moderator_signing, moderator_pubky) = random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky.clone()]).await;
    let moderator_token = authenticate(&app, &moderator_signing, &moderator_pubky).await;
    let buyer = new_actor(&app).await;
    let other_user = new_actor(&app).await;

    let command_id = "00000000-0000-4000-8000-000000001240";
    let (status, body) = execute(
        &app,
        &buyer.token,
        &report_command(command_id, &listing_aggregate(&buyer.pubky)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "report failed: {body}");
    assert_eq!(body["result"]["kind"], json!("report"));
    assert_eq!(body["result"]["report"]["state"], json!("open"));
    assert_eq!(
        body["result"]["report"]["reporter_pubky"],
        json!(buyer.pubky)
    );

    // Another ordinary user must not read the buyer's report.
    let other_view = list_reports(&app, &other_user.token).await;
    assert_eq!(other_view, json!([]));
    // The moderator reads every report.
    let moderator_view = list_reports(&app, &moderator_token).await;
    assert_eq!(moderator_view.as_array().map(Vec::len), Some(1));
    assert_eq!(moderator_view[0]["id"], json!(command_id));
}

// Cross-user authorization: each non-moderator sees only their own
// submissions, never another user's.
#[sqlx::test]
async fn non_moderators_read_only_their_own_reports(pool: PgPool) {
    let app = test_app_with_moderators(pool, Vec::new()).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;

    execute(
        &app,
        &first.token,
        &report_command(
            "00000000-0000-4000-8000-000000001241",
            &listing_aggregate(&first.pubky),
        ),
    )
    .await;
    execute(
        &app,
        &second.token,
        &report_command(
            "00000000-0000-4000-8000-000000001242",
            &listing_aggregate(&second.pubky),
        ),
    )
    .await;

    let first_view = list_reports(&app, &first.token).await;
    assert_eq!(first_view.as_array().map(Vec::len), Some(1));
    assert_eq!(first_view[0]["reporter_pubky"], json!(first.pubky));
    let second_view = list_reports(&app, &second.token).await;
    assert_eq!(second_view.as_array().map(Vec::len), Some(1));
    assert_eq!(second_view[0]["reporter_pubky"], json!(second.pubky));
}

// Moderator decisions: moderator-only, revision-checked, recorded
// append-only, and final (a decided report cannot be re-decided).
#[sqlx::test]
async fn moderator_decisions_are_recorded_append_only(pool: PgPool) {
    let (moderator_signing, moderator_pubky) = random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky.clone()]).await;
    let moderator_token = authenticate(&app, &moderator_signing, &moderator_pubky).await;
    let buyer = new_actor(&app).await;
    let report_id = "00000000-0000-4000-8000-000000001243";
    execute(
        &app,
        &buyer.token,
        &report_command(report_id, &listing_aggregate(&buyer.pubky)),
    )
    .await;

    // A non-moderator cannot decide, even their own report.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &decide_command(report_id, 1, "actioned"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(
        body["error"]["message"],
        json!("Only a configured moderator may decide reports.")
    );

    // A stale revision is rejected with the current one.
    let mut stale = decide_command(report_id, 0, "actioned");
    stale["command_id"] = json!("00000000-0000-4000-8000-000000001301");
    let (status, body) = execute(&app, &moderator_token, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));

    let (status, body) = execute(
        &app,
        &moderator_token,
        &decide_command(report_id, 1, "actioned"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "decision failed: {body}");
    assert_eq!(body["result"]["report"]["state"], json!("actioned"));
    assert_eq!(body["result"]["report"]["revision"], json!(2));
    let (decision, decided_by): (String, String) =
        sqlx::query_as("SELECT decision, moderator_pubky FROM report_decisions LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .expect("decision row exists");
    assert_eq!(decision, "actioned");
    assert_eq!(decided_by, moderator_pubky);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'trust.decided'"
        )
        .await,
        1
    );

    // The decision log is append-only at the database level.
    let update = sqlx::query("UPDATE report_decisions SET decision = 'dismissed'")
        .execute(&app.pool)
        .await;
    assert!(update.is_err(), "decision updates must be rejected");
    let delete = sqlx::query("DELETE FROM report_decisions")
        .execute(&app.pool)
        .await;
    assert!(delete.is_err(), "decision deletes must be rejected");

    // A decided report cannot be decided again.
    let mut again = decide_command(report_id, 2, "dismissed");
    again["command_id"] = json!("00000000-0000-4000-8000-000000001302");
    let (status, body) = execute(&app, &moderator_token, &again).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["message"],
        json!("The report is already decided.")
    );
}

// The moderator role is independent and narrowly scoped: it grants no
// authority over other users' aggregates.
#[sqlx::test]
async fn moderator_role_grants_no_broad_admin_authority(pool: PgPool) {
    let (moderator_signing, moderator_pubky) = random_keypair();
    let app = test_app_with_moderators(pool, vec![moderator_pubky.clone()]).await;
    let moderator_token = authenticate(&app, &moderator_signing, &moderator_pubky).await;
    let seller = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &common::register_command(&seller.pubky, 1),
    )
    .await;

    // A moderator cannot register inventory for another seller's listing.
    let (status, body) = execute(
        &app,
        &moderator_token,
        &common::register_command(&seller.pubky, 5),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
}
