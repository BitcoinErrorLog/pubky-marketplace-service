pub mod checkout;
pub mod register_listing;
pub mod reserve_inventory;

use sqlx::{Postgres, Transaction};

use crate::model::ListingRow;

pub const LISTING_COLUMNS: &str = "aggregate_id, seller_pubky, listing_id, title, \
     listing_revision, content_hash, server_revision, state, total_quantity, \
     available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
     unit_price_currency, unit_price_exponent, sale_format, auction, updated_at";

pub async fn fetch_listing(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

pub async fn fetch_listing_for_update(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<ListingRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {LISTING_COLUMNS} FROM listings WHERE aggregate_id = $1 FOR UPDATE"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

pub async fn current_listing_revision(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<i64, sqlx::Error> {
    let revision: Option<(i64,)> =
        sqlx::query_as("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(aggregate_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(revision.map(|(value,)| value).unwrap_or(0))
}
