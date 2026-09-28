use marketplace_service::handlers::BitcoinReviewNotice;
use sqlx::PgPool;
use uuid::Uuid;

const SELLER: &str = "adjnbqbam6b6nkcjp8iarxorjmqycxo6cwfzxspeyxaqjxmnjdcy";

async fn insert(pool: &PgPool, review_reason: Option<&str>) -> Result<(), sqlx::Error> {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, 1, 'system', 'payment.manual_review', now())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(format!("payment:{}", Uuid::new_v4()))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
         aggregate_id, review_reason, created_at) \
         VALUES ($1, $2, $3, 'system', 'bitcoin_manual_review', $4, $5, now())",
    )
    .bind(Uuid::new_v4())
    .bind(event_id)
    .bind(SELLER)
    .bind(format!("order:{}", Uuid::new_v4()))
    .bind(review_reason)
    .execute(pool)
    .await
    .map(|_| ())
}

const REVIEW_REASONS: [&str; 5] = [
    "late_settlement",
    "amount_mismatch",
    "confirmation_failed",
    "seller_confirmation_window_elapsed",
    "seller_response_overdue",
];

#[test]
fn the_notice_vocabulary_is_the_0048_check() {
    let notices: Vec<&str> = BitcoinReviewNotice::ALL
        .into_iter()
        .map(BitcoinReviewNotice::as_str)
        .collect();
    assert_eq!(notices, REVIEW_REASONS);
    for reason in REVIEW_REASONS {
        assert_eq!(
            BitcoinReviewNotice::parse(reason).map(BitcoinReviewNotice::as_str),
            Some(reason)
        );
    }
    assert_eq!(BitcoinReviewNotice::parse("refund_required"), None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0048_bounds_notification_review_reasons_and_is_rerunnable(pool: PgPool) {
    for reason in std::iter::once(None).chain(REVIEW_REASONS.map(Some)) {
        insert(&pool, reason)
            .await
            .unwrap_or_else(|error| panic!("{reason:?} must insert: {error}"));
    }

    sqlx::raw_sql(include_str!(
        "../migrations/0048_notification_review_reason.sql"
    ))
    .execute(&pool)
    .await
    .expect("0048 must be directly rerunnable");
    let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 6, "rerunning keeps existing rows");

    for reason in ["refund_required", "", "LATE_SETTLEMENT"] {
        let error = insert(&pool, Some(reason))
            .await
            .expect_err("an unknown review reason must be refused");
        assert!(
            error
                .to_string()
                .contains("notifications_review_reason_check"),
            "{reason:?}: {error}"
        );
    }
}
