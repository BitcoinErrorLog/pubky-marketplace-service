//! A notification outbox row that can never deliver is quarantined on its
//! own: the rows around it deliver in the same pass, and it is never
//! claimed again.

mod common;

use chrono::Duration;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::workers::{claim_outbox_batch, drain_outbox};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

type Stamp = Option<chrono::DateTime<chrono::Utc>>;
/// `(quarantined_at, quarantine_reason, delivered_at, lease_until)`.
type QuarantineFacts = (Stamp, Option<String>, Stamp, Stamp);

const SELLER: &str = "adjnbqbam6b6nkcjp8iarxorjmqycxo6cwfzxspeyxaqjxmnjdcy";

fn intent(event_id: Uuid) -> Value {
    json!({
        "event_id": event_id,
        "recipient_pubky": SELLER,
        "actor_pubky": "system",
        "aggregate_id": format!("order:{}", Uuid::new_v4()),
    })
}

/// Queues one outbox row whose payload `shape` derives from a valid intent.
async fn enqueue(pool: &PgPool, kind: &str, shape: impl FnOnce(Value) -> Value) -> (i64, Uuid) {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, 1, 'system', 'order.created', now())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(format!("order:{}", Uuid::new_v4()))
    .execute(pool)
    .await
    .expect("event row");
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO outbox (event_id, kind, payload, created_at) \
         VALUES ($1, $2, $3, now()) RETURNING id",
    )
    .bind(event_id)
    .bind(kind)
    .bind(shape(intent(event_id)))
    .fetch_one(pool)
    .await
    .expect("outbox row");
    (id, event_id)
}

fn without(field: &'static str) -> impl FnOnce(Value) -> Value {
    move |mut payload| {
        payload.as_object_mut().expect("object").remove(field);
        payload
    }
}

fn with(field: &'static str, value: Value) -> impl FnOnce(Value) -> Value {
    move |mut payload| {
        payload[field] = value;
        payload
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_malformed_notification_row_is_quarantined_alone(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    let valid_first = enqueue(&pool, "notification.order_created", |p| p).await;
    let mut malformed = Vec::new();
    for (label, kind, reason, shape) in [
        (
            "unroutable kind",
            "bogus.kind",
            "unroutable_kind",
            Box::new(|p| p) as Box<dyn FnOnce(Value) -> Value>,
        ),
        (
            "empty notification type",
            "notification.",
            "unroutable_kind",
            Box::new(|p| p),
        ),
        (
            "missing recipient",
            "notification.order_created",
            "missing_recipient_pubky",
            Box::new(without("recipient_pubky")),
        ),
        (
            "non-string recipient",
            "notification.order_created",
            "missing_recipient_pubky",
            Box::new(with("recipient_pubky", json!(42))),
        ),
        (
            "empty recipient",
            "notification.order_created",
            "missing_recipient_pubky",
            Box::new(with("recipient_pubky", json!(""))),
        ),
        (
            "payload not an object",
            "notification.order_created",
            "missing_recipient_pubky",
            Box::new(|_| json!(["not", "an", "intent"])),
        ),
        (
            "missing actor",
            "notification.order_created",
            "missing_actor_pubky",
            Box::new(without("actor_pubky")),
        ),
        (
            "non-string actor",
            "notification.order_created",
            "missing_actor_pubky",
            Box::new(with("actor_pubky", json!({"pubky": SELLER}))),
        ),
        (
            "missing aggregate",
            "notification.order_created",
            "missing_aggregate_id",
            Box::new(without("aggregate_id")),
        ),
        (
            "null aggregate",
            "notification.order_created",
            "missing_aggregate_id",
            Box::new(with("aggregate_id", Value::Null)),
        ),
    ] {
        let (id, event_id) = enqueue(&pool, kind, shape).await;
        malformed.push((label, id, event_id, reason));
    }
    let valid_last = enqueue(&pool, "notification.order_created", |p| p).await;

    let now = app.clock.now();
    let delivered = drain_outbox(&pool, None, now, 30)
        .await
        .expect("one pass completes past every malformed row");
    assert_eq!(delivered, 2, "both valid intents deliver in the same pass");
    for (_, event_id) in [valid_first, valid_last] {
        assert_eq!(
            count(
                &pool,
                &format!("SELECT COUNT(*) FROM notifications WHERE event_id = '{event_id}'")
            )
            .await,
            1
        );
    }
    for (label, id, event_id, reason) in &malformed {
        let (quarantined, stored_reason, delivered_at, lease): QuarantineFacts = sqlx::query_as(
            "SELECT quarantined_at, quarantine_reason, delivered_at, lease_until \
             FROM outbox WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("outbox row");
        assert_eq!(quarantined, Some(now), "{label}");
        assert_eq!(stored_reason.as_deref(), Some(*reason), "{label}");
        assert_eq!(delivered_at, None, "{label}: never marked delivered");
        assert_eq!(lease, None, "{label}");
        assert_eq!(
            count(
                &pool,
                &format!("SELECT COUNT(*) FROM notifications WHERE event_id = '{event_id}'")
            )
            .await,
            0,
            "{label}: nothing delivered"
        );
    }

    // After the lease would have lapsed, nothing is claimed again.
    let later = now + Duration::minutes(10);
    assert!(claim_outbox_batch(&pool, later, 30)
        .await
        .expect("claim runs")
        .is_empty());
    assert_eq!(
        drain_outbox(&pool, None, later, 30)
            .await
            .expect("drain runs"),
        0
    );
}
