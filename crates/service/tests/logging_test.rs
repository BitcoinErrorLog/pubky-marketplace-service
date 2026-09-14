mod common;

use common::{checkout_command_with_id, execute, new_actor, register_command, test_app};
use serde_json::Value;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};

struct Buffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn command_events_are_structured_and_privacy_safe(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer({
            let buffer = buffer.clone();
            move || Buffer(buffer.clone())
        })
        .finish();

    let guard = tracing::subscriber::set_default(subscriber);
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");

    let mut refused =
        checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000009901");
    refused["payload"]["delivery_address"]["line1"] =
        Value::String("FAKE_ADDRESS_THAT_MUST_NOT_REACH_LOGS".to_string());
    let (status, body) = execute(&app, &seller.token, &refused).await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "UNAUTHORIZED");

    let (status, body) = execute(&app, &seller.token, &serde_json::json!({})).await;
    assert_eq!(
        status,
        axum::http::StatusCode::UNPROCESSABLE_ENTITY,
        "{body}"
    );
    assert_eq!(body["error"]["code"], "INVALID_COMMAND");

    let accepted = checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000009902");
    let (status, body) = execute(&app, &buyer.token, &accepted).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    drop(guard);

    let logs = String::from_utf8(buffer.lock().expect("log buffer lock").clone()).unwrap();
    assert!(logs.contains("\"message\":\"command.executed\""), "{logs}");
    assert!(logs.contains("\"outcome\":\"refused\""), "{logs}");
    assert!(logs.contains("\"error_code\":\"UNAUTHORIZED\""), "{logs}");
    assert!(logs.contains("\"outcome\":\"accepted\""), "{logs}");
    assert!(logs.contains("\"message\":\"command.invalid\""), "{logs}");
    assert!(logs.contains("\"outcome\":\"invalid\""), "{logs}");
    assert!(
        logs.contains("\"error_code\":\"INVALID_COMMAND\""),
        "{logs}"
    );
    assert!(logs.contains("\"latency_ms\""), "{logs}");
    for field in [
        "\"kind\"",
        "\"command_id\"",
        "\"aggregate_id\"",
        "\"actor_prefix\"",
        "\"outcome\"",
        "\"error_code\"",
        "\"refusal_message\"",
        "\"revision\"",
        "\"latency_ms\"",
    ] {
        assert!(logs.contains(field), "missing {field}: {logs}");
    }
    assert!(!logs.contains("\"payload\""), "{logs}");
    assert!(!logs.contains("Invalid envelope"), "{logs}");
    assert!(!logs.contains("\"amount_minor\""), "{logs}");
    assert!(!logs.contains("12500"), "{logs}");
    assert!(!logs.contains("13700"), "{logs}");
    assert!(!logs.contains("1200"), "{logs}");
    assert!(!logs.contains("Bearer"), "{logs}");
    assert!(!logs.contains(&"y".repeat(52)), "{logs}");
    assert!(
        !logs.contains("FAKE_ADDRESS_THAT_MUST_NOT_REACH_LOGS"),
        "{logs}"
    );
    assert!(!logs.contains(&seller.pubky), "{logs}");
    assert!(!logs.contains(&buyer.pubky), "{logs}");
}
