use sqlx::PgPool;

const OWNER: &str = "yg4gxyy1sgwmfhcaofnqgtdxsknz6sdxgqf6t1nnxxbs5r7yx5gy";

async fn insert(pool: &PgPool, owner: &str, key_count: i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO user_priv_key_custody_releases (owner_pubky, released_at, key_count) \
         VALUES ($1, now(), $2)",
    )
    .bind(owner)
    .bind(key_count)
    .execute(pool)
    .await
    .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0052_creates_the_release_table_and_is_rerunnable(pool: PgPool) {
    insert(&pool, OWNER, 1)
        .await
        .expect("a well-formed row inserts");

    sqlx::raw_sql(include_str!(
        "../migrations/0052_user_priv_key_custody_releases.sql"
    ))
    .execute(&pool)
    .await
    .expect("0052 must be directly rerunnable");
    let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM user_priv_key_custody_releases")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 1, "rerunning keeps existing rows");

    for (label, owner, key_count) in [
        ("a second row for the owner", OWNER, 1),
        (
            "zero keys",
            "yg4gxyy1sgwmfhcaofnqgtdxsknz6sdxgqf6t1nnxxbs5r7yx5gz",
            0,
        ),
        ("a short owner", "short", 1),
    ] {
        insert(&pool, owner, key_count)
            .await
            .expect_err(&format!("{label} is refused"));
    }
}
