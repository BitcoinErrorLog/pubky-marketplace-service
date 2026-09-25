//! Migration 0034 catalog and constraint proof. Adjacent unmerged streams
//! reserve 0032 and 0033, so this branch intentionally contains 0001..0031
//! plus 0034 and edits no existing migration. Both tests fail against the
//! preimplementation 0001..0031-only catalog.

use sqlx::PgPool;
use uuid::Uuid;

use marketplace_service::inventory::MAX_CONFLICT_EVIDENCE_PER_IDEMPOTENCY_KEY;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

async fn seed_listing(pool: &PgPool, seller: &str, listing_id: &str) -> String {
    let aggregate = format!("listing:{seller}_{listing_id}");
    sqlx::query(
        "INSERT INTO listings \
             (aggregate_id, seller_pubky, listing_id, title, listing_revision, content_hash, \
              server_revision, state, total_quantity, available_quantity, reserved_quantity, \
              sold_quantity, unit_price_amount_minor, unit_price_currency, unit_price_exponent, \
              sale_format, auction, updated_at) \
         VALUES ($1, $2, $3, 'Inventory proof', 1, $4, 1, 'available', 2, 2, 0, 0, \
                 100, 'USD', 2, 'fixed_price', NULL, NOW())",
    )
    .bind(&aggregate)
    .bind(seller)
    .bind(listing_id)
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .expect("seed listing");
    aggregate
}

async fn seed_event(pool: &PgPool, aggregate: &str, seller: &str, revision: i64) -> Uuid {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events \
             (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, 'inventory.adjusted', NOW())",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(aggregate)
    .bind(revision)
    .bind(seller)
    .execute(pool)
    .await
    .expect("seed inventory event");
    event_id
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0034_enforces_inventory_evidence_constraints(pool: PgPool) {
    let seller = "y".repeat(52);
    let second_seller = "b".repeat(52);
    let aggregate = seed_listing(&pool, &seller, "one").await;
    let second_aggregate = seed_listing(&pool, &second_seller, "two").await;
    let event = seed_event(&pool, &aggregate, &seller, 2).await;
    let second_event = seed_event(&pool, &second_aggregate, &second_seller, 2).await;
    let key = Uuid::new_v4();
    let hash = "a".repeat(64);

    sqlx::query(
        "INSERT INTO inventory_adjustment_results \
             (seller_pubky, idempotency_key, request_hash, aggregate_id, event_id, result_json, created_at) \
         VALUES ($1, $2, $3, $4, $5, '{\"ok\":true}', NOW())",
    )
    .bind(&seller)
    .bind(key)
    .bind(&hash)
    .bind(&aggregate)
    .bind(event)
    .execute(&pool)
    .await
    .expect("valid immutable result");
    assert!(
        sqlx::query(
            "UPDATE inventory_adjustment_results SET result_json = '{\"ok\":false}' \
             WHERE seller_pubky = $1 AND idempotency_key = $2",
        )
        .bind(&seller)
        .bind(key)
        .execute(&pool)
        .await
        .is_err(),
        "result evidence is immutable"
    );
    assert!(
        sqlx::query(
            "INSERT INTO inventory_adjustment_conflicts \
                 (seller_pubky, idempotency_key, aggregate_id, original_request_hash, \
                  conflicting_request_hash, observed_at) \
             VALUES ($1, $2, $3, $4, $4, NOW())",
        )
        .bind(&seller)
        .bind(key)
        .bind(&aggregate)
        .bind(&hash)
        .execute(&pool)
        .await
        .is_err(),
        "a quarantine row must prove the body changed"
    );

    for (seller_value, aggregate_value, event_value) in [
        (&seller, &aggregate, event),
        (&second_seller, &second_aggregate, second_event),
    ] {
        sqlx::query(
            "INSERT INTO inventory_external_refs \
                 (seller_pubky, channel, external_id, aggregate_id, event_id, request_hash, created_at) \
             VALUES ($1, 'shopify', 'evt-1', $2, $3, $4, NOW())",
        )
        .bind(seller_value)
        .bind(aggregate_value)
        .bind(event_value)
        .bind(&hash)
        .execute(&pool)
        .await
        .expect("same external identity is allowed for a different seller");
    }
    assert!(
        sqlx::query(
            "INSERT INTO inventory_external_refs \
                 (seller_pubky, channel, external_id, aggregate_id, event_id, request_hash, created_at) \
             VALUES ($1, 'shopify', 'evt-1', $2, gen_random_uuid(), $3, NOW())",
        )
        .bind(&seller)
        .bind(&aggregate)
        .bind(&hash)
        .execute(&pool)
        .await
        .is_err(),
        "seller/channel/external id is unique"
    );
    for invalid_channel in ["Shopify", "x/control", &"x".repeat(33)] {
        assert!(
            sqlx::query(
                "INSERT INTO inventory_external_refs \
                     (seller_pubky, channel, external_id, aggregate_id, event_id, request_hash, created_at) \
                 VALUES ($1, $2, 'unique', $3, gen_random_uuid(), $4, NOW())",
            )
            .bind(&seller)
            .bind(invalid_channel)
            .bind(&aggregate)
            .bind(&hash)
            .execute(&pool)
            .await
            .is_err(),
            "invalid channel {invalid_channel:?} is uncommittable"
        );
    }

    let token_hash = vec![7u8; 32];
    sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, $2, '/:rw', NOW(), NOW() + INTERVAL '1 hour')",
    )
    .bind(&token_hash)
    .bind(&seller)
    .execute(&pool)
    .await
    .expect("seed auth session");
    assert!(
        sqlx::query(
            "INSERT INTO inventory_rate_limits (session_hash, endpoint_class, tokens, updated_at) \
             VALUES ($1, 'other', 1, NOW())",
        )
        .bind(&token_hash)
        .execute(&pool)
        .await
        .is_err(),
        "rate endpoint classes are closed"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0034_caps_conflicts_and_indexes_the_count_prefix(pool: PgPool) {
    let seller = "y".repeat(52);
    let aggregate = seed_listing(&pool, &seller, "bounded").await;
    let key = Uuid::new_v4();
    let original_hash = "a".repeat(64);

    for index in 0..=MAX_CONFLICT_EVIDENCE_PER_IDEMPOTENCY_KEY {
        let conflicting_hash = format!("{:064x}", index + 1);
        sqlx::query(
            "INSERT INTO inventory_adjustment_conflicts \
                 (seller_pubky, idempotency_key, aggregate_id, original_request_hash, \
                  conflicting_request_hash, observed_at) \
             VALUES ($1, $2, $3, $4, $5, NOW())",
        )
        .bind(&seller)
        .bind(key)
        .bind(&aggregate)
        .bind(&original_hash)
        .bind(conflicting_hash)
        .execute(&pool)
        .await
        .expect("direct conflict insert is accepted or capped");
    }

    let conflicts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_adjustment_conflicts \
         WHERE seller_pubky = $1 AND idempotency_key = $2",
    )
    .bind(&seller)
    .bind(key)
    .fetch_one(&pool)
    .await
    .expect("bounded direct conflict count");
    assert_eq!(
        conflicts, MAX_CONFLICT_EVIDENCE_PER_IDEMPOTENCY_KEY,
        "the database boundary must cap distinct conflict hashes per successful key"
    );

    let cap_trigger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM pg_trigger \
             WHERE tgrelid = 'inventory_adjustment_conflicts'::regclass \
               AND tgname = 'inventory_adjustment_conflicts_cap' \
               AND NOT tgisinternal \
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("cap trigger catalog lookup");
    assert!(cap_trigger_exists, "the database cap trigger must exist");

    let index_definition: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes \
         WHERE schemaname = current_schema() \
           AND tablename = 'inventory_adjustment_conflicts' \
           AND indexname = 'inventory_adjustment_conflict_unique'",
    )
    .fetch_one(&pool)
    .await
    .expect("conflict unique index");
    assert!(
        index_definition.contains("(seller_pubky, idempotency_key, conflicting_request_hash)"),
        "the unique index's leftmost seller/key prefix must support bounded counts: \
         {index_definition}"
    );
}

#[test]
fn migration_catalog_is_contiguous_through_0044() {
    let mut numbers: Vec<u32> = std::fs::read_dir(MIGRATIONS_DIR)
        .expect("migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".sql")?
                .get(..4)
                .and_then(|prefix| prefix.parse::<u32>().ok())
        })
        .collect();
    numbers.sort_unstable();
    assert_eq!(numbers, (1..=44).collect::<Vec<_>>());
}
