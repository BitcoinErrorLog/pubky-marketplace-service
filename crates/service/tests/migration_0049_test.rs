use marketplace_service::workers::OutboxQuarantine;
use sqlx::PgPool;
use uuid::Uuid;

const QUARANTINE_REASONS: [&str; 4] = [
    "unroutable_kind",
    "missing_recipient_pubky",
    "missing_actor_pubky",
    "missing_aggregate_id",
];

async fn insert(pool: &PgPool, quarantined: bool, reason: Option<&str>) -> Result<(), sqlx::Error> {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, 1, 'system', 'order.created', now())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(format!("order:{}", Uuid::new_v4()))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO outbox (event_id, kind, payload, created_at, quarantined_at, \
         quarantine_reason) VALUES ($1, 'notification.order_created', '{}'::jsonb, now(), \
         CASE WHEN $2 THEN now() END, $3)",
    )
    .bind(event_id)
    .bind(quarantined)
    .bind(reason)
    .execute(pool)
    .await
    .map(|_| ())
}

#[test]
fn the_quarantine_vocabulary_is_the_0049_check() {
    let reasons: Vec<&str> = OutboxQuarantine::ALL
        .into_iter()
        .map(OutboxQuarantine::as_str)
        .collect();
    assert_eq!(reasons, QUARANTINE_REASONS);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0049_bounds_outbox_quarantine_and_is_rerunnable(pool: PgPool) {
    insert(&pool, false, None)
        .await
        .expect("an ordinary row inserts");
    for reason in QUARANTINE_REASONS {
        insert(&pool, true, Some(reason))
            .await
            .unwrap_or_else(|error| panic!("{reason} must insert: {error}"));
    }

    sqlx::raw_sql(include_str!("../migrations/0049_outbox_quarantine.sql"))
        .execute(&pool)
        .await
        .expect("0049 must be directly rerunnable");
    let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 5, "rerunning keeps existing rows");

    for (label, quarantined, reason) in [
        ("unknown reason", true, Some("bad_json")),
        ("reason without a time", false, Some("unroutable_kind")),
        ("time without a reason", true, None),
    ] {
        let error = insert(&pool, quarantined, reason)
            .await
            .expect_err("an inconsistent quarantine must be refused");
        assert!(
            error.to_string().contains("outbox_quarantine_check"),
            "{label}: {error}"
        );
    }
}
