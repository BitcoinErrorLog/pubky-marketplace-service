//! 0050: `worker_leases.fence` and the listing deletion guards. Existing
//! lease rows read 0, the file reruns over them and keeps its guards, every
//! acquisition (renewals included) raises the fence while a refused one
//! leaves it, and a fenced release never touches a later acquisition's
//! lease. The guards against older binaries are exercised in
//! `listing_deletion_test.rs`.

use chrono::Utc;
use marketplace_service::workers::{
    release_fenced_lease, try_acquire_fenced_lease, try_acquire_lease,
};
use sqlx::PgPool;
use uuid::Uuid;

async fn lease(pool: &PgPool, task: &str) -> (Uuid, i64, chrono::DateTime<Utc>) {
    sqlx::query_as("SELECT holder, fence, lease_until FROM worker_leases WHERE task = $1")
        .bind(task)
        .fetch_one(pool)
        .await
        .expect("lease row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0050_fences_every_lease_acquisition_and_is_rerunnable(pool: PgPool) {
    let now = Utc::now();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    sqlx::query("INSERT INTO worker_leases (task, holder, lease_until) VALUES ('legacy', $1, $2)")
        .bind(a)
        .bind(now)
        .execute(&pool)
        .await
        .expect("a lease row written without a fence");
    sqlx::raw_sql(include_str!(
        "../migrations/0050_listing_deletion_fence.sql"
    ))
    .execute(&pool)
    .await
    .expect("0050 must be directly rerunnable");
    assert_eq!(lease(&pool, "legacy").await.1, 0);
    let guards: Vec<String> = sqlx::query_scalar(
        "SELECT tgname::text FROM pg_trigger WHERE NOT tgisinternal \
         AND tgname IN ('listings_deletion_guard', 'listing_deletion_cursors_guard') \
         ORDER BY tgname",
    )
    .fetch_all(&pool)
    .await
    .expect("guard triggers");
    assert_eq!(
        guards,
        ["listing_deletion_cursors_guard", "listings_deletion_guard"],
        "a rerun keeps one of each guard"
    );
    let refused = sqlx::query(
        "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
         VALUES ('seller', '1', now())",
    )
    .execute(&pool)
    .await
    .expect_err("an undeclared cursor write is refused after a rerun");
    assert!(
        refused.to_string().contains("no current follower lease"),
        "{refused}"
    );

    let acquire = |holder: Uuid| try_acquire_fenced_lease(&pool, "probe", holder, now, 30);
    assert_eq!(acquire(a).await.expect("first"), Some(1));
    assert_eq!(acquire(a).await.expect("renewal"), Some(2));
    assert_eq!(acquire(b).await.expect("refused"), None);
    assert_eq!(
        lease(&pool, "probe").await.1,
        2,
        "a refusal leaves the fence"
    );
    assert!(try_acquire_lease(&pool, "probe", a, now, 30)
        .await
        .expect("unfenced renewal"));
    assert_eq!(lease(&pool, "probe").await.1, 3, "every acquisition fences");

    release_fenced_lease(&pool, "probe", a, 2, now)
        .await
        .expect("stale release");
    let (_, _, until) = lease(&pool, "probe").await;
    assert!(until > now, "a stale fence cannot end the current lease");
    release_fenced_lease(&pool, "probe", a, 3, now)
        .await
        .expect("release");
    let (_, _, until) = lease(&pool, "probe").await;
    assert!(until <= now, "the current fence ends the lease");
    release_fenced_lease(&pool, "probe", a, 3, now + chrono::Duration::seconds(60))
        .await
        .expect("late release");
    let (_, _, until) = lease(&pool, "probe").await;
    assert!(until <= now, "a release never extends a lease");

    assert_eq!(acquire(b).await.expect("takeover"), Some(4));
    assert_eq!(lease(&pool, "probe").await.0, b);
    assert_eq!(
        try_acquire_fenced_lease(&pool, "legacy", b, now, 30)
            .await
            .expect("legacy takeover"),
        Some(1)
    );
}
