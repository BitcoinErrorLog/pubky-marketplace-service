//! Driven by `scripts/rehearse-production-schema-clone.sh` against a local
//! clone of the production schema and migration ledger (no domain rows):
//! the startup migrator from `main.rs` must apply every pending migration.

use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "run by scripts/rehearse-production-schema-clone.sh against a production-schema clone"]
async fn production_schema_clone_applies_pending_migrations() {
    let url = std::env::var("MARKETPLACE_CLONE_DATABASE_URL")
        .expect("MARKETPLACE_CLONE_DATABASE_URL names the clone");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("clone connects");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("pending migrations apply on the production-schema clone");
    let constraints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.offers'::regclass \
         AND convalidated AND conname IN ('offers_accepted_fulfillment_methods_check', \
         'offers_accepted_pickup_only_ships_free_check')",
    )
    .fetch_one(&pool)
    .await
    .expect("0044 constraints");
    assert_eq!(constraints, 2, "0044 constraints exist and are validated");
}
