# Listing deletion follower

The follower (`crates/service/src/listing_deletion.rs`) turns a seller's
homeserver `DEL` into a listing tombstone. It runs as a worker task under
the `listing_deletions` lease.

## Guarantees

- **Bounded by the lease.** Each database unit is one transaction whose
  statements the server cancels 100 ms before the lease deadline, awaited
  by the caller no longer than the deadline, connection acquisition
  included. Both deadlines count from before the lease acquisition.
- **Fenced.** `worker_leases.fence` grows with every acquisition. Each
  follower write share-locks its lease row and proceeds only while the row
  names the pass's holder and fence and has not expired; a takeover waits
  for a write in flight.
- **Monotonic cursor.** The cursor for a seller never moves backwards.
- **Revivals are confirmed.** Each pass first confirms up to five revived
  listings not yet confirmed at their current generation
  (`listing_revival_checks`, 0051), least recently tried first, within the
  first half of the pass; the forward read keeps its own 20 settles. A
  revival whose record is gone and whose superseded `DEL` is still the
  record's latest event is tombstoned, even when that `DEL` is behind the
  seller's cursor. Every attempt is recorded, so a revival the homeserver
  could not answer for moves behind the ones not yet tried and stays due.
- **A record accepted after a confirmation wins.** A delete that the
  service accepted a record for after confirming it is settled again on a
  later pass; the seller's cursor stops before it.

## Revivals

A tombstoned listing is revived only by its re-published record:
`listing.sync` revives from the record it fetched, and `listing.register`
at `expected_revision` 0 for a tombstoned id reads the seller's record
before its transaction and refuses the revival unless the record is there
(`NOT_FOUND`, `UPSTREAM_UNAVAILABLE`, or `INVALID_STATE` when the listing
was tombstoned after that read).

`listings.record_epoch` (0051) counts the record-derived writes the service
accepted for a row: a revival, or a change to a field the seller's record
supplies (a register, a sync, a sync's healing). A trigger maintains it for
every writer. Every tombstone caller reads the epoch before it confirms the
delete against the homeserver, and the tombstone commits only while the
locked row still has that epoch. So a register or sync that lands between
the confirmation and the tombstone, including a re-publish after a delete,
is never retired by the delete confirmed before it.

A revival records the `DEL` it superseded in `revived_from_cursor`. A later
tombstone needs that `DEL` or a newer one, confirmed at the current epoch.
The revival advanced the epoch, so the superseded `DEL` retires the row
only when it was confirmed after the revival and was then still the
record's latest event: nothing backs the revival.

## Database guards (migration 0050)

The guarantees above do not depend on which binary runs. Triggers refuse,
for every writer:

| Write | Refused when |
|---|---|
| Tombstone (`deleted_at` NULL to set) | the transaction declares no `marketplace.listing_deletion_authority`, or declares `follower:<holder>:<fence>` that is not the current lease row |
| Tombstone | the transaction does not declare the row's current `record_epoch` in `marketplace.listing_deletion_observed_epoch` (0051); a binary before 0051 declares none |
| Tombstone | its `DEL` cursor is older than the delete a revival of the listing superseded (`revived_from_cursor`) |
| `record_epoch` | set by a statement: the trigger keeps it unless a revival or a record field changes |
| Revival marker | it would move backwards |
| Follower cursor insert or update | no current follower authority, or the cursor would move backwards |
| Follower cursor delete | always |

The service declares `command` for a `listing.sync` that confirmed the
delete itself and `follower:<holder>:<fence>` for a follower pass
(`DeletionAuthority`). A binary built before 0050 declares nothing, so none
of its tombstones or cursor writes commit once 0050 is applied, and a
refused tombstone rolls back its drop-binding release and its
`listing.deleted` event with it.

`listing_deletion_test.rs` runs the pre-0050 follower SQL against the new
schema: a tombstone held across lease expiry and a new-image takeover, and
a cursor write that checks its lease before the takeover commits. Neither
commits.

## Deploying 0050

**Deploy mode: rolling.** A rolling replacement is safe for correctness:

- 0050's `CREATE TRIGGER` statements wait for every transaction already
  writing `listings` or `listing_deletion_cursors`, so a write that started
  under the old binary before the guards exist commits before any new
  follower starts. The migration gives up after `lock_timeout` 10 s; the
  replica then fails to start, and the next start retries.
- After 0050 is applied and until the old replica stops, the old replica
  refuses its own deletion work:
  - its follower passes fail with `listing tombstone refused` or
    `listing deletion cursor write refused` and release the lease each
    tick, so the new replica's follower takes it;
  - a `listing.sync` it serves for a deleted record fails with an internal
    error and changes nothing. The new replica, or a later sync, tombstones
    the listing.

Those errors in the old replica's logs during the overlap are expected.
Rollback to an image without 0050 is not possible once it is applied
(sqlx refuses a binary missing an applied migration); fix forward.

## Deploying 0051

**Deploy mode: rolling**, on the same terms as 0050. Once 0051 is applied,
an old replica declares no record epoch, so its tombstones are refused with
`the record changed after the delete was confirmed`: its follower passes
fail and release the lease, and a `listing.sync` it serves for a deleted
record fails with an internal error and changes nothing. The new replica
tombstones those listings. Rollback is fix-forward.
