use sqlx::PgPool;

const OWNER: &str = "yg4gxyy1sgwmfhcaofnqgtdxsknz6sdxgqf6t1nnxxbs5r7yx5gy";
const KEY_ID: &str = "0123456789abcdef0123456789abcdef";

async fn insert(
    pool: &PgPool,
    owner: &str,
    generation: i32,
    key_id: &str,
    sealed_len: usize,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO user_priv_keys (owner_pubky, generation, key_id, sealed_key, created_at, \
         updated_at) VALUES ($1, $2, $3, $4, now(), now())",
    )
    .bind(owner)
    .bind(generation)
    .bind(key_id)
    .bind(vec![0u8; sealed_len])
    .execute(pool)
    .await
    .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0046_creates_user_priv_keys_and_is_rerunnable(pool: PgPool) {
    insert(&pool, OWNER, 1, KEY_ID, 72)
        .await
        .expect("a well-formed row inserts");

    sqlx::raw_sql(include_str!("../migrations/0046_user_priv_keys.sql"))
        .execute(&pool)
        .await
        .expect("0046 must be directly rerunnable");
    let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM user_priv_keys")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 1, "rerunning keeps existing rows");

    let other_key_id = "fedcba9876543210fedcba9876543210";
    for (label, owner, generation, key_id, sealed_len) in [
        ("duplicate generation", OWNER, 1, other_key_id, 72),
        ("duplicate key id", OWNER, 2, KEY_ID, 72),
        ("generation zero", OWNER, 0, other_key_id, 72),
        ("short owner", "short", 1, other_key_id, 72),
        (
            "uppercase key id",
            OWNER,
            2,
            "0123456789ABCDEF0123456789ABCDEF",
            72,
        ),
        ("short key id", OWNER, 2, "0123", 72),
        ("short sealed key", OWNER, 2, other_key_id, 71),
        ("long sealed key", OWNER, 2, other_key_id, 73),
    ] {
        insert(&pool, owner, generation, key_id, sealed_len)
            .await
            .expect_err(label);
    }
    insert(&pool, OWNER, 2, other_key_id, 72)
        .await
        .expect("a second generation with its own key id inserts");
    let other_owner = "o".repeat(52);
    insert(&pool, &other_owner, 1, KEY_ID, 72)
        .await
        .expect("key ids and generations are scoped per owner");
}
