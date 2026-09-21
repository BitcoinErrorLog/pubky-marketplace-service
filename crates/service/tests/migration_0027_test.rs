//! Schema proof for the additive Locks claim-scope migration (0027): the
//! lifecycle claim's partial index selects only `registered` correlations
//! with a non-null bundle, and the reciprocal registered-state CHECK makes
//! bundle presence biconditional with the registered state, so a prepared
//! row can never enter — or break — the claim batch.

mod common;

use sqlx::PgPool;

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0027_scopes_the_claim_index_and_registered_state(pool: PgPool) {
    let (definition,): (String,) = sqlx::query_as(
        "SELECT pg_get_indexdef('payment_locks_correlations_pending_idx'::regclass)",
    )
    .fetch_one(&pool)
    .await
    .expect("index definition is readable");
    assert!(
        definition.contains("preparation_state = 'registered'"),
        "the claim index excludes prepared rows: {definition}"
    );
    assert!(
        definition.contains("bundle_id_ciphertext IS NOT NULL"),
        "the claim index requires a bundle: {definition}"
    );

    let (present,): (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $1)")
            .bind("payment_locks_registered_bundle")
            .fetch_one(&pool)
            .await
            .expect("constraint catalog is readable");
    assert!(present, "payment_locks_registered_bundle is missing");

    // A registered row missing either bundle field is uncommittable.
    for columns in [
        "'registered', '\\x01'::bytea, NULL",
        "'registered', NULL, '\\x01'::bytea",
    ] {
        let error = sqlx::query(&format!(
            "INSERT INTO payment_locks_correlations (id, payment_id, order_id, buyer_pubky, \
             creator_pubky, lock_resource_hash, amount_minor, asset, exponent, policy_version, \
             verification_state, window_expires_at, preparation_state, bundle_id_ciphertext, \
             bundle_lookup_token, created_at, updated_at) \
             VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'b', 'c', 'h', \
             100, 'USD', 2, 1, 'pending', now(), {columns}, now(), now())"
        ))
        .execute(&pool)
        .await
        .expect_err("a registered row without both bundle fields violates the CHECK");
        assert!(
            error
                .to_string()
                .contains("payment_locks_registered_bundle"),
            "unexpected constraint error: {error}"
        );
    }
}
