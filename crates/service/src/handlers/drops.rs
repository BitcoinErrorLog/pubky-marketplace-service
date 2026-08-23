//! Drop aggregate commands and gating (ADR-0026): timed, limited releases.
//!
//! `drop.sync` is a convergent homeserver-fetch command modeled on
//! `listing.sync`: any authenticated actor may ask the service to fetch the
//! seller-signed drop record from the seller-owned homeserver path and
//! register (or refresh) the drop aggregate from it. Path ownership is the
//! authority. Terms (schedule, caps, listings) may only change while the
//! drop is still announced — once live, terms are locked; the drop's STATE
//! always derives from server time plus paid quantity, never from the
//! record.
//!
//! Gating: `inventory.reserve` and `checkout.create` call
//! [`lock_bound_drop`] + [`enforce_drop_gate`] when a target listing is
//! bound to a drop. The drop row lock is taken BEFORE any listing row lock
//! in every transaction that touches both (gating, releases), so the two
//! never deadlock. All cap accounting lives in the same transaction as the
//! hold, with database CHECK constraints backing every handler guard.
//!
//! Releases: every path that returns held units to a listing credits the
//! stamped drop through [`credit_drop_release`] — reservation expiry, the
//! immediate cancel of an unpaid order, and cancellation approval. Credits
//! never reopen an ended drop; while live, a lapsed hold simply restocks.
//!
//! Editions (layer 2): the exactly-once payment confirmation calls
//! [`record_paid_unit`] under the drop lock — the new paid count is the
//! confirming order's edition, and reaching the total ends the drop
//! `ended_sold_out`. After any end the seller may `drop.release_listings`
//! ([`release_listings`]) to take the bindings out of gating and return the
//! listings to ordinary open sale.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    CancelDropPayload, Command, ReleaseDropListingsPayload, SyncDropPayload,
};
use marketplace_domain::state_machines::{can_transition, drop_machine};
use marketplace_domain::{ids, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::executor::insert_event;
use crate::homeserver::{
    validated_drop_record, DropRecord, HomeserverFetchOutcome, HomeserverListingClient,
};
use crate::model::DropRow;
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

pub const DROP_COLUMNS: &str = "aggregate_id, seller_pubky, drop_id, record_revision, revision, \
     state, format, starts_at, ends_at, total_quantity, per_buyer_limit, remaining_quantity, \
     paid_quantity, stock_display, listing_ids, created_at, updated_at";

/// Gating refusal copy, pinned by the client contract tests.
pub const DROP_NOT_STARTED: &str = "The drop has not started.";
pub const DROP_ENDED: &str = "The drop has ended.";
pub const DROP_SOLD_OUT: &str = "The drop is sold out.";
pub const DROP_PER_BUYER_LIMIT: &str = "You have reached this drop's per-buyer limit.";
/// The cart shape rule for drop checkouts: editions map one order to one
/// unit, so a checkout containing a drop-bound line must be exactly that one
/// line, holding one unit.
pub const DROP_SINGLE_LINE: &str = "A drop order is one unit of one listing per checkout.";
/// Refusal copy for `drop.release_listings` while the drop is still
/// announced or live.
pub const DROP_RELEASE_BEFORE_END: &str = "Listings release only after the drop ends.";

pub async fn fetch_drop_for_update(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<Option<DropRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {DROP_COLUMNS} FROM drops WHERE aggregate_id = $1 FOR UPDATE"
    ))
    .bind(aggregate_id)
    .fetch_optional(&mut **tx)
    .await
}

/// The state server time honestly assigns a drop with this schedule right
/// now. `paid_quantity` reaching the total (→ `ended_sold_out`) is driven by
/// payment confirmation, not by this derivation.
fn derived_state(
    starts_at: DateTime<Utc>,
    ends_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> &'static str {
    if ends_at.is_some_and(|ends_at| now >= ends_at) {
        "ended_closed"
    } else if now >= starts_at {
        "live"
    } else {
        "announced"
    }
}

fn is_active_state(state: &str) -> bool {
    matches!(state, "announced" | "live")
}

/// `drop.sync`: convergent registration from the seller's homeserver record.
pub async fn sync(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SyncDropPayload,
    homeserver: Option<&dyn HomeserverListingClient>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Fail closed: without a configured homeserver there is no canonical
    // record to derive the drop from.
    let Some(homeserver) = homeserver else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "Drop sync is not enabled on this deployment.",
        )));
    };

    let expected_aggregate_id = ids::drop_aggregate_id(&payload.seller_pubky, &payload.drop_id);
    if command.aggregate_id != expected_aggregate_id {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            "The drop aggregate id does not match its seller and drop.",
        )));
    }

    let record = match homeserver
        .fetch_drop(&payload.seller_pubky, &payload.drop_id)
        .await
    {
        HomeserverFetchOutcome::Found(record) => record,
        HomeserverFetchOutcome::NotFound => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::NotFound,
                "The seller's homeserver has no such drop record.",
            )));
        }
        HomeserverFetchOutcome::Unavailable => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::UpstreamUnavailable,
                "The seller's homeserver could not be reached. Try again shortly.",
            )));
        }
    };

    let record = match validated_drop_record(&payload.seller_pubky, &payload.drop_id, &record) {
        Ok(record) => record,
        Err(issues) => {
            return Ok(Err(CommandFailure {
                issues: Some(issues),
                ..CommandFailure::new(
                    ErrorCode::InvalidState,
                    "The seller's drop record does not satisfy drop invariants.",
                )
            }));
        }
    };

    // Every listed listing must already be a registered aggregate; a drop is
    // an overlay on real inventory, never a substitute for it. The missing
    // ids are named so the caller can `listing.sync` them first — this
    // command deliberately does NOT auto-register.
    let mut missing: Vec<&str> = Vec::new();
    for listing_id in &record.listing_ids {
        let registered: Option<(String,)> =
            sqlx::query_as("SELECT aggregate_id FROM listings WHERE aggregate_id = $1")
                .bind(ids::listing_aggregate_id(&payload.seller_pubky, listing_id))
                .fetch_optional(&mut **tx)
                .await?;
        if registered.is_none() {
            missing.push(listing_id);
        }
    }
    if !missing.is_empty() {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidCommand,
            &format!(
                "The drop references unregistered listings: {}.",
                missing.join(", ")
            ),
        )));
    }

    let current = fetch_drop_for_update(tx, &command.aggregate_id).await?;
    if let Some(current) = &current {
        // Convergent no-op: the aggregate already reflects this record
        // revision (or a newer one). Nothing changes and no event is
        // emitted; the caller learns the current server revision.
        if record.revision <= current.record_revision {
            return Ok(Ok(HandlerSuccess {
                revision: current.revision,
                event_ids: vec![],
                result: json!({ "kind": "drop", "drop": current.view() }),
            }));
        }
        return apply_resync(tx, actor, command, &record, current, now).await;
    }

    apply_registration(tx, actor, command, payload, &record, now).await
}

/// First sync: creates the aggregate, deriving the initial state honestly
/// from server time (a record synced after its start is live immediately; a
/// record whose whole window already elapsed registers ended).
async fn apply_registration(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SyncDropPayload,
    record: &DropRecord,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let state = derived_state(record.starts_at, record.ends_at, now);
    let listing_ids =
        serde_json::to_value(&record.listing_ids).expect("listing ids serialize infallibly");
    let drop: DropRow = sqlx::query_as(&format!(
        "INSERT INTO drops (aggregate_id, seller_pubky, drop_id, record_revision, revision, \
         state, format, starts_at, ends_at, total_quantity, per_buyer_limit, \
         remaining_quantity, paid_quantity, stock_display, listing_ids, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 1, $5, 'fcfs', $6, $7, $8, $9, $8, 0, $10, $11, $12, $12) \
         RETURNING {DROP_COLUMNS}"
    ))
    .bind(&command.aggregate_id)
    .bind(&payload.seller_pubky)
    .bind(&payload.drop_id)
    .bind(record.revision)
    .bind(state)
    .bind(record.starts_at)
    .bind(record.ends_at)
    .bind(record.total_quantity)
    .bind(record.per_buyer_limit)
    .bind(&record.stock_display)
    .bind(&listing_ids)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    insert_drop_listing_bindings(tx, &drop, &record.listing_ids, is_active_state(state)).await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        1,
        actor,
        "drop.synced",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: 1,
        event_ids: vec![event_id],
        result: json!({ "kind": "drop", "drop": drop.view() }),
    }))
}

/// Re-sync with an advanced record revision: enforcement terms (schedule,
/// caps, listings) may change ONLY while the drop is still announced on
/// server time — no holds can exist yet, so `remaining_quantity` resets to
/// the new total. Once live or ended, terms are locked at launch.
async fn apply_resync(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    record: &DropRecord,
    current: &DropRow,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let effective = match current.state.as_str() {
        state if is_active_state(state) => derived_state(current.starts_at, current.ends_at, now),
        state => state,
    };
    if effective != "announced" {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InvalidState,
            "The drop's terms are locked at launch.",
            current.revision,
        )));
    }

    let state = derived_state(record.starts_at, record.ends_at, now);
    let new_revision = current.revision + 1;
    let listing_ids =
        serde_json::to_value(&record.listing_ids).expect("listing ids serialize infallibly");
    let drop: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET record_revision = $2, revision = $3, state = $4, starts_at = $5, \
         ends_at = $6, total_quantity = $7, per_buyer_limit = $8, remaining_quantity = $7, \
         stock_display = $9, listing_ids = $10, updated_at = $11 \
         WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(&command.aggregate_id)
    .bind(record.revision)
    .bind(new_revision)
    .bind(state)
    .bind(record.starts_at)
    .bind(record.ends_at)
    .bind(record.total_quantity)
    .bind(record.per_buyer_limit)
    .bind(&record.stock_display)
    .bind(&listing_ids)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM drop_listings WHERE drop_aggregate_id = $1")
        .bind(&command.aggregate_id)
        .execute(&mut **tx)
        .await?;
    insert_drop_listing_bindings(tx, &drop, &record.listing_ids, is_active_state(state)).await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        new_revision,
        actor,
        "drop.synced",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "drop", "drop": drop.view() }),
    }))
}

async fn insert_drop_listing_bindings(
    tx: &mut Transaction<'_, Postgres>,
    drop: &DropRow,
    listing_ids: &[String],
    active: bool,
) -> Result<(), sqlx::Error> {
    for listing_id in listing_ids {
        // The partial unique index (one announced/live drop per listing)
        // rejects a second active binding; the executor maps that unique
        // violation to INVARIANT_VIOLATION.
        sqlx::query(
            "INSERT INTO drop_listings (drop_aggregate_id, seller_pubky, listing_id, active) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&drop.aggregate_id)
        .bind(&drop.seller_pubky)
        .bind(listing_id)
        .bind(active)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// `drop.cancel`: seller only, from announced or live, terminal. Outstanding
/// holds are untouched — they lapse (or cancel) through their own lifecycle
/// and credit the counters then — but new holds are refused immediately.
pub async fn cancel(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    _payload: &CancelDropPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(drop) = fetch_drop_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The drop was not found.",
        )));
    };
    if drop.seller_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the seller may cancel a drop.",
        )));
    }
    if command.expected_revision != drop.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The drop revision is stale.",
            drop.revision,
        )));
    }
    // Server time decides the effective state: a drop whose window already
    // elapsed is ended_closed whether or not the transition was persisted,
    // and an ended drop can no longer be cancelled.
    let effective = match drop.state.as_str() {
        state if is_active_state(state) => derived_state(drop.starts_at, drop.ends_at, now),
        state => state,
    };
    if !is_active_state(effective) {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InvalidState,
            "This drop can no longer be cancelled.",
            drop.revision,
        )));
    }
    debug_assert!(can_transition(
        &drop_machine(),
        effective,
        "ended_cancelled"
    ));

    let new_revision = drop.revision + 1;
    let cancelled: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET state = 'ended_cancelled', revision = $2, updated_at = $3 \
         WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(&command.aggregate_id)
    .bind(new_revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    deactivate_drop_listing_bindings(tx, &command.aggregate_id).await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        new_revision,
        actor,
        "drop.cancelled",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "drop", "drop": cancelled.view() }),
    }))
}

async fn deactivate_drop_listing_bindings(
    tx: &mut Transaction<'_, Postgres>,
    drop_aggregate_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE drop_listings SET active = FALSE WHERE drop_aggregate_id = $1")
        .bind(drop_aggregate_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `drop.release_listings`: seller only, `expected_revision` CAS, allowed
/// only once the drop has ended (any of the three terminal states — a due
/// server-time end is persisted first, so the seller never has to wait for
/// the sweep). Marks the drop's bindings released, removing them from
/// gating consideration entirely: the listings sell again as ordinary open
/// inventory. The drop's own state and counters are untouched.
pub async fn release_listings(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    _payload: &ReleaseDropListingsPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(drop) = fetch_drop_for_update(tx, &command.aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::NotFound,
            "The drop was not found.",
        )));
    };
    if drop.seller_pubky != actor {
        return Ok(Err(CommandFailure::new(
            ErrorCode::Unauthorized,
            "Only the seller may release a drop's listings.",
        )));
    }
    if command.expected_revision != drop.revision {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::RevisionConflict,
            "The drop revision is stale.",
            drop.revision,
        )));
    }
    let drop = apply_time_transitions(tx, drop, command.command_id, now).await?;
    if is_active_state(&drop.state) {
        return Ok(Err(CommandFailure::with_revision(
            ErrorCode::InvalidState,
            DROP_RELEASE_BEFORE_END,
            drop.revision,
        )));
    }

    sqlx::query(
        "UPDATE drop_listings SET active = FALSE, released = TRUE WHERE drop_aggregate_id = $1",
    )
    .bind(&command.aggregate_id)
    .execute(&mut **tx)
    .await?;
    let new_revision = drop.revision + 1;
    let released: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET revision = $2, updated_at = $3 \
         WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(&command.aggregate_id)
    .bind(new_revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let event_id = insert_event(
        tx,
        command.command_id,
        &command.aggregate_id,
        new_revision,
        actor,
        "drop.listings_released",
        now,
    )
    .await?;

    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "drop", "drop": released.view() }),
    }))
}

/// Finds and row-locks the drop a listing is bound to. An announced/live
/// (active) binding wins; failing that, the most recent ended binding is
/// returned so gating keeps refusing a listing whose drop ended or was
/// cancelled — until the seller binds it to a new drop or releases the
/// ended binding (`drop.release_listings`), the listing does not quietly
/// fall back to open sale. Released bindings are out of gating
/// consideration entirely.
pub async fn lock_bound_drop(
    tx: &mut Transaction<'_, Postgres>,
    seller_pubky: &str,
    listing_id: &str,
) -> Result<Option<DropRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {columns} FROM drops d \
         JOIN drop_listings dl ON dl.drop_aggregate_id = d.aggregate_id \
         WHERE dl.seller_pubky = $1 AND dl.listing_id = $2 AND NOT dl.released \
         ORDER BY dl.active DESC, d.updated_at DESC, d.aggregate_id \
         LIMIT 1 FOR UPDATE OF d",
        columns = DROP_COLUMNS
            .split(", ")
            .map(|column| format!("d.{column}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .bind(seller_pubky)
    .bind(listing_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Applies the lazy server-time transitions to a row-locked drop inside the
/// caller's transaction: announced → live at `starts_at`, and → ended_closed
/// at `ends_at` (directly from announced when the whole window elapsed
/// untouched). Bumps the revision, maintains `drop_listings.active`, and
/// appends the transition event. Shared by command gating and the sweep
/// worker so projections and gating always agree.
pub async fn apply_time_transitions(
    tx: &mut Transaction<'_, Postgres>,
    drop: DropRow,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<DropRow, sqlx::Error> {
    if !is_active_state(&drop.state) {
        return Ok(drop);
    }
    let target = derived_state(drop.starts_at, drop.ends_at, now);
    if target == drop.state {
        return Ok(drop);
    }
    debug_assert!(can_transition(&drop_machine(), &drop.state, target));

    let new_revision = drop.revision + 1;
    let transitioned: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET state = $2, revision = $3, updated_at = $4 \
         WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(&drop.aggregate_id)
    .bind(target)
    .bind(new_revision)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    if !is_active_state(target) {
        deactivate_drop_listing_bindings(tx, &drop.aggregate_id).await?;
    }
    insert_event(
        tx,
        command_id,
        &drop.aggregate_id,
        new_revision,
        &drop.seller_pubky,
        if target == "live" {
            "drop.started"
        } else {
            "drop.ended"
        },
        now,
    )
    .await?;
    Ok(transitioned)
}

/// The drop gate for one hold of `units` units by `buyer` against a
/// row-locked bound drop: lazy time transitions, the state checks, and — in
/// the same transaction — both caps (drop remaining and the buyer's
/// per-drop counter). Returns the drop to stamp onto the hold.
pub async fn enforce_drop_gate(
    tx: &mut Transaction<'_, Postgres>,
    drop: DropRow,
    buyer: &str,
    units: i64,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Result<DropRow, CommandFailure>, sqlx::Error> {
    let drop = apply_time_transitions(tx, drop, command_id, now).await?;
    match drop.state.as_str() {
        "announced" => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                DROP_NOT_STARTED,
            )));
        }
        "live" => {}
        _ => {
            return Ok(Err(CommandFailure::new(
                ErrorCode::InvalidState,
                DROP_ENDED,
            )));
        }
    }

    // The drop row lock serializes every path that touches these counters,
    // so the reads below are race-free; the CHECK constraints remain as the
    // database backstop should any future path skip the lock.
    if drop.remaining_quantity < units {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InsufficientInventory,
            DROP_SOLD_OUT,
        )));
    }
    let held: Option<(i64,)> = sqlx::query_as(
        "SELECT quantity::BIGINT FROM drop_purchases \
         WHERE drop_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&drop.aggregate_id)
    .bind(buyer)
    .fetch_optional(&mut **tx)
    .await?;
    if held.map(|(quantity,)| quantity).unwrap_or(0) + units > drop.per_buyer_limit {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvalidState,
            DROP_PER_BUYER_LIMIT,
        )));
    }

    let debited: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET remaining_quantity = remaining_quantity - $2, \
         revision = revision + 1, updated_at = $3 \
         WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(&drop.aggregate_id)
    .bind(units)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO drop_purchases (drop_aggregate_id, buyer_pubky, quantity, per_buyer_limit) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (drop_aggregate_id, buyer_pubky) \
         DO UPDATE SET quantity = drop_purchases.quantity + EXCLUDED.quantity",
    )
    .bind(&drop.aggregate_id)
    .bind(buyer)
    .bind(units)
    .bind(drop.per_buyer_limit)
    .execute(&mut **tx)
    .await?;

    Ok(Ok(debited))
}

/// Records one paid unit against a stamped drop inside the payment
/// confirmation transaction, taking the drop row lock BEFORE any listing
/// row lock (the shared order). Increments `paid_quantity`; the new value
/// IS the confirming order's edition — 1-based and gapless over paid
/// orders, because assignment happens only here, under the drop lock,
/// exactly once per order. When the paid count reaches the total, a live
/// drop transitions to the terminal `ended_sold_out`: bindings deactivate,
/// the revision bumps, a `drop.ended` event is appended, and a
/// `drop_sold_out` notification intent is enqueued to the seller. A drop
/// whose window already elapsed closes as `ended_closed` first (the lazy
/// time transition) and keeps honest paid books without a sold-out
/// transition — ended states are terminal.
pub async fn record_paid_unit(
    tx: &mut Transaction<'_, Postgres>,
    drop_aggregate_id: &str,
    actor: &str,
    command_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Result<i64, CommandFailure>, sqlx::Error> {
    let Some(drop) = fetch_drop_for_update(tx, drop_aggregate_id).await? else {
        return Ok(Err(CommandFailure::new(
            ErrorCode::InvariantViolation,
            "The order's drop is missing.",
        )));
    };
    // Persist any due server-time transition first (the UPDATE below
    // re-reads the row, so the returned state is post-transition).
    apply_time_transitions(tx, drop, command_id, now).await?;
    // The CHECK constraint (paid <= total) is the database backstop; every
    // confirming order holds a unit the gate debited, so the count fits.
    let drop: DropRow = sqlx::query_as(&format!(
        "UPDATE drops SET paid_quantity = paid_quantity + 1, revision = revision + 1, \
         updated_at = $2 WHERE aggregate_id = $1 RETURNING {DROP_COLUMNS}"
    ))
    .bind(drop_aggregate_id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    let edition = drop.paid_quantity;

    if drop.paid_quantity == drop.total_quantity && drop.state == "live" {
        debug_assert!(can_transition(&drop_machine(), "live", "ended_sold_out"));
        let new_revision = drop.revision + 1;
        sqlx::query(
            "UPDATE drops SET state = 'ended_sold_out', revision = $2, updated_at = $3 \
             WHERE aggregate_id = $1",
        )
        .bind(drop_aggregate_id)
        .bind(new_revision)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        deactivate_drop_listing_bindings(tx, drop_aggregate_id).await?;
        let event_id = insert_event(
            tx,
            command_id,
            drop_aggregate_id,
            new_revision,
            actor,
            "drop.ended",
            now,
        )
        .await?;
        // No amount rides the intent (ADR-0019 §8): the seller learns the
        // drop sold out; the figures live on their own drop projection.
        crate::handlers::insert_notification_intent(
            tx,
            event_id,
            "drop_sold_out",
            &drop.seller_pubky,
            actor,
            drop_aggregate_id,
            None,
            now,
        )
        .await?;
    }

    Ok(Ok(edition))
}

/// Credits a released hold back to its stamped drop: `remaining_quantity`
/// gains the units and the buyer's per-drop counter loses them, inside the
/// caller's transaction and under the drop row lock. State never changes —
/// an ended drop keeps honest books but nothing reopens; a live drop simply
/// restocks. Returns false when the accounting no longer adds up (a hold
/// credited twice, or credited to a drop it never debited).
pub async fn credit_drop_release(
    tx: &mut Transaction<'_, Postgres>,
    drop_aggregate_id: &str,
    buyer_pubky: &str,
    units: i64,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    if fetch_drop_for_update(tx, drop_aggregate_id)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let credited = sqlx::query(
        "UPDATE drops SET remaining_quantity = remaining_quantity + $2, \
         revision = revision + 1, updated_at = $3 \
         WHERE aggregate_id = $1 AND remaining_quantity + $2 <= total_quantity",
    )
    .bind(drop_aggregate_id)
    .bind(units)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    if credited.rows_affected() != 1 {
        return Ok(false);
    }
    let debited = sqlx::query(
        "UPDATE drop_purchases SET quantity = quantity - $3 \
         WHERE drop_aggregate_id = $1 AND buyer_pubky = $2 AND quantity >= $3",
    )
    .bind(drop_aggregate_id)
    .bind(buyer_pubky)
    .bind(units)
    .execute(&mut **tx)
    .await?;
    Ok(debited.rows_affected() == 1)
}
