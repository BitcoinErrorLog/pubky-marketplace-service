//! 0051: `listings.record_epoch` and the tombstone rule that reads it. A
//! row that predates the file keeps its state across a rerun and its stuck
//! revival stays due; the epoch advances on revivals, record fields and a
//! one-step confirmation, and cannot be set otherwise; a tombstone commits
//! only with the current epoch declared, so an older binary's commits
//! nothing, and the delete a revival superseded retires the row once the
//! current epoch is declared. The service paths are exercised in
//! `listing_deletion_test.rs`.

use sqlx::PgPool;

const AGGREGATE: &str = "listing:seller_boots_01";

async fn epoch(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT record_epoch FROM listings WHERE aggregate_id = $1")
        .bind(AGGREGATE)
        .fetch_one(pool)
        .await
        .expect("record epoch")
}

/// A tombstone as a binary writes it, declaring `observed_epoch` when it
/// is given.
async fn tombstone(pool: &PgPool, cursor: &str, observed_epoch: Option<i64>) -> Result<(), String> {
    let mut tx = pool.begin().await.expect("tombstone tx");
    sqlx::query("SELECT set_config('marketplace.listing_deletion_authority', 'command', true)")
        .execute(&mut *tx)
        .await
        .expect("authority");
    if let Some(observed) = observed_epoch {
        sqlx::query("SELECT set_config('marketplace.listing_deletion_observed_epoch', $1, true)")
            .bind(observed.to_string())
            .execute(&mut *tx)
            .await
            .expect("observed epoch");
    }
    let written = sqlx::query(
        "UPDATE listings SET deleted_at = now(), deleted_event_cursor = $2 \
         WHERE aggregate_id = $1 AND deleted_at IS NULL",
    )
    .bind(AGGREGATE)
    .bind(cursor)
    .execute(&mut *tx)
    .await;
    match written {
        Ok(_) => {
            tx.commit().await.expect("tombstone commits");
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

async fn revive(pool: &PgPool) {
    sqlx::query(
        "UPDATE listings SET deleted_at = NULL, deleted_event_cursor = NULL, \
         generation = generation + 1 WHERE aggregate_id = $1",
    )
    .bind(AGGREGATE)
    .execute(pool)
    .await
    .expect("revival");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0051_fences_tombstones_on_the_record_epoch_and_is_rerunnable(pool: PgPool) {
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, shipping_minor, sale_format, fulfillment_methods, updated_at) \
         VALUES ($1, 'seller', 'boots_01', 'Winter boots', 1, repeat('a', 64), 1, 'available', \
         1, 1, 0, 0, 12500, 'USD', 2, 0, 'fixed_price', ARRAY['shipping'], now())",
    )
    .bind(AGGREGATE)
    .execute(&pool)
    .await
    .expect("listing row");
    assert_eq!(epoch(&pool).await, 0);

    // The incident: tombstoned at DEL 5, then revived with no record.
    tombstone(&pool, "5", Some(0)).await.expect("first delete");
    revive(&pool).await;
    assert_eq!(epoch(&pool).await, 1, "a revival advances the epoch");

    sqlx::raw_sql(include_str!("../migrations/0051_listing_revival_proof.sql"))
        .execute(&pool)
        .await
        .expect("0051 must be directly rerunnable");
    let (marker, epoch_after, checks): (Option<String>, i64, i64) = sqlx::query_as(
        "SELECT revived_from_cursor, record_epoch, \
         (SELECT COUNT(*) FROM listing_revival_checks) FROM listings WHERE aggregate_id = $1",
    )
    .bind(AGGREGATE)
    .fetch_one(&pool)
    .await
    .expect("incident row");
    assert_eq!(marker.as_deref(), Some("5"));
    assert_eq!(epoch_after, 1, "a rerun keeps the epoch");
    assert_eq!(checks, 0, "the stuck revival is due");
    let triggers: Vec<String> = sqlx::query_scalar(
        "SELECT tgname::text FROM pg_trigger WHERE NOT tgisinternal \
         AND tgrelid = 'listings'::regclass ORDER BY tgname",
    )
    .fetch_all(&pool)
    .await
    .expect("listing triggers");
    assert_eq!(
        triggers
            .iter()
            .filter(|name| name.as_str() == "listings_record_epoch"
                || name.as_str() == "listings_deletion_guard")
            .count(),
        2,
        "a rerun keeps one of each trigger: {triggers:?}"
    );

    // Only a revival or a record field moves the epoch.
    for (statement, expected) in [
        (
            "UPDATE listings SET record_epoch = 99 WHERE aggregate_id = $1",
            1,
        ),
        (
            "UPDATE listings SET server_revision = 2, updated_at = now() \
             WHERE aggregate_id = $1",
            1,
        ),
        (
            "UPDATE listings SET shipping_minor = 100 WHERE aggregate_id = $1",
            2,
        ),
        // A sync that confirms an unchanged record advances it by one.
        (
            "UPDATE listings SET record_epoch = record_epoch + 1 WHERE aggregate_id = $1",
            3,
        ),
        (
            "UPDATE listings SET record_epoch = record_epoch + 2 WHERE aggregate_id = $1",
            3,
        ),
        (
            "UPDATE listings SET listing_revision = 2 WHERE aggregate_id = $1",
            4,
        ),
    ] {
        sqlx::query(statement)
            .bind(AGGREGATE)
            .execute(&pool)
            .await
            .expect("listing update");
        assert_eq!(epoch(&pool).await, expected, "{statement}");
    }

    // A binary that declares no epoch, or a stale one, commits no tombstone.
    for observed in [None, Some(1), Some(3)] {
        let refused = tombstone(&pool, "5", observed)
            .await
            .expect_err("a tombstone without the current epoch");
        assert!(
            refused.contains("the record changed after the delete was confirmed"),
            "{observed:?}: {refused}"
        );
    }
    // With it, a delete older than the superseded one is still refused,
    // and the superseded one retires the row.
    let refused = tombstone(&pool, "4", Some(4))
        .await
        .expect_err("an older delete");
    assert!(refused.contains("predates the revival"), "{refused}");
    tombstone(&pool, "5", Some(4))
        .await
        .expect("the superseded delete, confirmed at the current epoch");
    let cursor: Option<String> =
        sqlx::query_scalar("SELECT deleted_event_cursor FROM listings WHERE aggregate_id = $1")
            .bind(AGGREGATE)
            .fetch_one(&pool)
            .await
            .expect("tombstone");
    assert_eq!(cursor.as_deref(), Some("5"));
}
