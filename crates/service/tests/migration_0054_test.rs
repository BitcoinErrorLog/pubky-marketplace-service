//! Migration 0054: the order refund destination table and its refusal-audit
//! command kind. Directly rerunnable, with the shape checked in the database.

mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

const ADDRESS: &str = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";

async fn insert(
    pool: &PgPool,
    order_id: Uuid,
    asset: &str,
    network: &str,
    address: &str,
    source: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO order_refund_destinations \
         (order_id, buyer_pubky, asset, network, address, address_source, confirmed_at, updated_at) \
         VALUES ($1, 'buyer', $2, $3, $4, $5, now(), now())",
    )
    .bind(order_id)
    .bind(asset)
    .bind(network)
    .bind(address)
    .bind(source)
    .execute(pool)
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0054_adds_the_destination_table_and_is_rerunnable(pool: PgPool) {
    sqlx::raw_sql(include_str!("../migrations/0054_refund_destinations.sql"))
        .execute(&pool)
        .await
        .expect("0054 must be directly rerunnable");

    let app = test_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");

    for (asset, network, address, source) in [
        ("BTC", "arbitrum-one", ADDRESS, "buyer_entered"),
        ("USDT", "ethereum", ADDRESS, "buyer_entered"),
        ("USDT", "arbitrum-one", "0x1234", "buyer_entered"),
        (
            "USDT",
            "arbitrum-one",
            "d8da6bf26964af9d7eed9e03e53415d37aa96045",
            "buyer_entered",
        ),
        ("USDT", "arbitrum-one", ADDRESS, "payment_address"),
    ] {
        assert!(
            insert(&pool, order_id, asset, network, address, source)
                .await
                .is_err(),
            "{asset} {network} {address} {source} must be refused"
        );
    }
    assert!(
        insert(
            &pool,
            Uuid::new_v4(),
            "USDT",
            "arbitrum-one",
            ADDRESS,
            "buyer_entered"
        )
        .await
        .is_err(),
        "the order must exist"
    );
    insert(
        &pool,
        order_id,
        "USDT",
        "arbitrum-one",
        ADDRESS,
        "buyer_entered",
    )
    .await
    .expect("a valid row");
    assert!(
        insert(
            &pool,
            order_id,
            "USDT",
            "arbitrum-one",
            ADDRESS,
            "buyer_entered"
        )
        .await
        .is_err(),
        "one destination per order"
    );

    let kind: Option<String> =
        sqlx::query_scalar("SELECT name FROM command_refusal_command_kinds WHERE id = 39")
            .fetch_optional(&pool)
            .await
            .expect("command kind");
    assert_eq!(kind.as_deref(), Some("confirm_refund_destination"));
}
