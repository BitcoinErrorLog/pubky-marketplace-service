mod common;

use axum::http::StatusCode;
use common::{send, test_app};
use serde_json::{json, Value};
use sqlx::PgPool;

// GET /version needs no session and reports the build baked in by build.rs.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn version_reports_the_running_build(pool: PgPool) {
    let app = test_app(pool).await;
    let (status, body) = send(app.router.clone(), "GET", "/version", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for key in ["name", "version", "commit", "built_at"] {
        assert!(body[key].is_string(), "{key} missing: {body}");
    }
    assert_eq!(body["name"], json!("marketplace-service"));
    assert_eq!(body["version"], json!(env!("CARGO_PKG_VERSION")));
}
