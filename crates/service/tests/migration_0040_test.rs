use sqlx::PgPool;

mod common;

async fn table_exists(pool: &PgPool, table: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)",
    )
    .bind(table)
    .fetch_one(pool)
    .await
    .expect("table existence")
}

async fn constraint_validated(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar("SELECT convalidated FROM pg_constraint WHERE conname = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("constraint")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0040_adds_digital_delivery_and_is_rerunnable(pool: PgPool) {
    // As for 0038: a fresh 0001..0039 apply touches cluster-global roles,
    // so the 0039→0040 upgrade is proven against a production-schema clone
    // (evidence digital-delivery-wave1/prod-schema-clone-slice1-0040.log). This
    // test proves the applied result and a direct re-run.
    for table in [
        "listing_digital_counters",
        "listing_digital_versions",
        "order_digital_pins",
        "order_digital_access",
        "order_delivery_emails",
    ] {
        assert!(table_exists(&pool, table).await, "{table}");
    }
    sqlx::raw_sql(include_str!("../migrations/0040_digital_delivery.sql"))
        .execute(&pool)
        .await
        .expect("0040 must be directly rerunnable");
    for constraint in [
        "listings_fulfillment_methods_check",
        "orders_fulfillment_check",
        "listings_digital_delivery_kind_check",
    ] {
        assert!(
            constraint_validated(&pool, constraint).await,
            "{constraint}"
        );
    }
    let kinds: Vec<(i16, String)> = sqlx::query_as(
        "SELECT id, name FROM command_refusal_command_kinds WHERE id >= 35 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("catalog");
    assert_eq!(
        kinds,
        vec![
            (35, "set_digital_delivery".to_string()),
            (36, "clear_digital_delivery".to_string())
        ]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0040_constraints_accept_digital_and_refuse_the_unknown(pool: PgPool) {
    let insert = |methods: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(&format!(
                "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
                 listing_revision, content_hash, server_revision, state, total_quantity, \
                 available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
                 unit_price_currency, unit_price_exponent, sale_format, fulfillment_methods, \
                 updated_at) \
                 VALUES (md5(random()::text), 's', md5(random()::text), 't', 1, 'h', 1, \
                 'available', 1, 1, 0, 0, 100, 'USD', 2, 'fixed_price', '{methods}', now())"
            ))
            .execute(&pool)
            .await
        }
    };
    for accepted in [
        "{digital}",
        "{shipping,digital}",
        "{shipping,pickup,digital}",
    ] {
        insert(accepted)
            .await
            .unwrap_or_else(|error| panic!("{accepted}: {error}"));
    }
    for refused in ["{email}", "{}", "{shipping,pickup,digital,shipping}"] {
        insert(refused).await.expect_err(refused);
    }
    sqlx::query("UPDATE listings SET digital_delivery_kind = 'key_list'")
        .execute(&pool)
        .await
        .expect_err("key lists are not a first-release kind");
}
