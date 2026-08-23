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

#[derive(Debug, sqlx::FromRow)]
struct DueReservation {
    id: Uuid,
    listing_aggregate_id: String,
    buyer_pubky: String,
    quantity: i64,
    drop_aggregate_id: Option<String>,
}

/// Expires every active reservation due at `now`. Returns the number of
/// reservations expired. Safe to run concurrently: due rows are locked with
/// `FOR UPDATE SKIP LOCKED` and the status flip is guarded by `status =
/// 'active'`.
pub async fn expire_due_reservations(pool: &PgPool, now: DateTime<Utc>) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let due: Vec<DueReservation> = sqlx::query_as(
        "SELECT id, listing_aggregate_id, buyer_pubky, quantity, drop_aggregate_id \
         FROM reservations \
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

        // A drop-stamped hold credits its drop first (drop lock before the
        // listing lock, the shared order): while live the unit restocks; an
        // ended drop keeps honest books but nothing reopens.
        if let Some(drop_aggregate_id) = &reservation.drop_aggregate_id {
            if !crate::handlers::drops::credit_drop_release(
                &mut tx,
                drop_aggregate_id,
                &reservation.buyer_pubky,
                reservation.quantity,
                now,
            )
            .await?
            {
                anyhow::bail!(
                    "expired reservation {} could not credit drop {drop_aggregate_id}",
                    reservation.id
                );
            }
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
