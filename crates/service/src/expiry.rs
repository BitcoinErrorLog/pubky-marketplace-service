//! Server-time reservation expiry (ADR-0019: "inventory reservations and
//! expiration" belong to this service; server time decides deadlines).
//!
//! A background worker sweeps active reservations whose `expires_at` has
//! passed, marks them expired, returns the held quantity to the listing, and
//! appends an `inventory.reservation_expired` event (traceable through the
//! original reserve command id).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, sqlx::FromRow)]
struct DueReservation {
    id: Uuid,
    listing_aggregate_id: String,
    buyer_pubky: String,
    quantity: i64,
}

/// Expires every active reservation due at `now`. Returns the number of
/// reservations expired. Safe to run concurrently: due rows are locked with
/// `FOR UPDATE SKIP LOCKED` and the status flip is guarded by `status =
/// 'active'`.
pub async fn expire_due_reservations(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<DueReservation> = sqlx::query_as(
        "SELECT id, listing_aggregate_id, buyer_pubky, quantity FROM reservations \
         WHERE status = 'active' AND expires_at <= $1 \
         ORDER BY expires_at FOR UPDATE SKIP LOCKED",
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = 0u64;
    for reservation in due {
        let flipped = sqlx::query(
            "UPDATE reservations SET status = 'expired', updated_at = $2 \
             WHERE id = $1 AND status = 'active'",
        )
        .bind(reservation.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if flipped.rows_affected() != 1 {
            continue;
        }

        let new_revision: (i64,) = sqlx::query_as(
            "UPDATE listings SET server_revision = server_revision + 1, state = 'available', \
             available_quantity = available_quantity + $2, \
             reserved_quantity = reserved_quantity - $2, updated_at = $3 \
             WHERE aggregate_id = $1 AND reserved_quantity >= $2 \
             RETURNING server_revision",
        )
        .bind(&reservation.listing_aggregate_id)
        .bind(reservation.quantity)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
             occurred_at) VALUES ($1, $2, $3, $4, $5, 'inventory.reservation_expired', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(reservation.id)
        .bind(&reservation.listing_aggregate_id)
        .bind(new_revision.0)
        .bind(&reservation.buyer_pubky)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tracing::info!(
            reservation_id = %reservation.id,
            aggregate_id = %reservation.listing_aggregate_id,
            "expired reservation and released inventory"
        );
        expired += 1;
    }
    tx.commit().await?;
    Ok(expired)
}

/// Spawns the periodic expiry worker used by the production binary.
pub fn spawn_worker(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval =
            std::time::Duration::from_secs(state.config.reservation_sweep_interval_seconds);
        loop {
            tokio::time::sleep(interval).await;
            let now = state.clock.now();
            match expire_due_reservations(&state.pool, now).await {
                Ok(0) => {}
                Ok(count) => tracing::info!(count, "reservation expiry sweep completed"),
                Err(error) => tracing::error!(error = %error, "reservation expiry sweep failed"),
            }
        }
    })
}
