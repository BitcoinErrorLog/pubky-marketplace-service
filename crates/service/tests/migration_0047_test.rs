use sqlx::PgPool;

const SELLER: &str = "yg4gxyy1sgwmfhcaofnqgtdxsknz6sdxgqf6t1nnxxbs5r7yx5gy";

async fn insert_listing(pool: &PgPool, listing_id: &str) {
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, sale_format, updated_at) \
         VALUES ($1, $2, $3, 'Boots', 1, $4, 1, 'available', 1, 1, 0, 0, 100, 'USD', 2, \
         'fixed_price', now())",
    )
    .bind(format!("listing:{SELLER}_{listing_id}"))
    .bind(SELLER)
    .bind(listing_id)
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .expect("listing inserts");
}

async fn mark(pool: &PgPool, listing_id: &str, deleted: bool, cursor: Option<&str>) -> bool {
    sqlx::query(
        "UPDATE listings SET deleted_at = CASE WHEN $2 THEN now() END, \
         deleted_event_cursor = $3 WHERE listing_id = $1",
    )
    .bind(listing_id)
    .bind(deleted)
    .bind(cursor)
    .execute(pool)
    .await
    .is_ok()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0047_keeps_existing_listings_live_and_is_rerunnable(pool: PgPool) {
    insert_listing(&pool, "boots_01").await;
    let (deleted, recreated): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT deleted_at::text, recreated_at::text FROM listings WHERE listing_id = 'boots_01'",
    )
    .fetch_one(&pool)
    .await
    .expect("listing row");
    assert_eq!((deleted, recreated), (None, None));

    assert!(mark(&pool, "boots_01", true, Some("431001")).await);
    sqlx::raw_sql(include_str!("../migrations/0047_listing_tombstones.sql"))
        .execute(&pool)
        .await
        .expect("0047 must be directly rerunnable over tombstoned rows");
    let (cursor,): (Option<String>,) =
        sqlx::query_as("SELECT deleted_event_cursor FROM listings WHERE listing_id = 'boots_01'")
            .fetch_one(&pool)
            .await
            .expect("listing row");
    assert_eq!(cursor.as_deref(), Some("431001"));

    let (valid,): (bool,) = sqlx::query_as(
        "SELECT convalidated FROM pg_constraint \
         WHERE conname = 'listings_deletion_evidence_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("constraint catalog");
    assert!(valid, "the evidence constraint is validated over existing rows");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_tombstone_always_carries_its_homeserver_evidence(pool: PgPool) {
    insert_listing(&pool, "boots_01").await;
    for (label, deleted, cursor) in [
        ("deleted without a cursor", true, None),
        ("cursor without a deletion", false, Some("1")),
        ("non-numeric cursor", true, Some("abc")),
        ("oversized cursor", true, Some("123456789012345678901")),
    ] {
        assert!(!mark(&pool, "boots_01", deleted, cursor).await, "{label}");
    }
    assert!(mark(&pool, "boots_01", true, Some("7")).await);
    assert!(mark(&pool, "boots_01", false, None).await);

    for (label, cursor, ok) in [("numeric", Some("42"), true), ("null", None, true), ("text", Some("x"), false)] {
        let inserted = sqlx::query(
            "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, polled_at) \
             VALUES ($1, $2, now())",
        )
        .bind(format!("{label}-seller"))
        .bind(cursor)
        .execute(&pool)
        .await;
        assert_eq!(inserted.is_ok(), ok, "{label}");
    }
}
