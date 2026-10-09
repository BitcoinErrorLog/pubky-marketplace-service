# Migrations

Files under `crates/service/migrations/` are additive-only, carry no DOWN
migration, and are directly rerunnable unless a rule below says otherwise.
An applied file is immutable: sqlx checksums every applied migration, so a
changed byte stops the next start.

## Index builds on existing tables

A plain `CREATE INDEX` holds a `SHARE` lock on its table for the whole
build, which blocks every `INSERT`, `UPDATE`, and `DELETE`. On `listings`,
`orders`, `payments`, or `events` that pauses checkout, payment
confirmation, and the workers until the build ends. A plain `DROP INDEX`
takes an `ACCESS EXCLUSIVE` lock, which also blocks reads. A `UNIQUE`,
`PRIMARY KEY`, or `EXCLUDE` constraint added to an existing table, as a
table constraint or inline on a new column, builds its index the same way.

Every migration after 0049 that builds, drops, or rebuilds an index on a
table the same file did not create uses this shape:

```sql
-- no-transaction
CREATE INDEX CONCURRENTLY listings_example_idx
    ON listings (seller_pubky, updated_at)
    WHERE deleted_at IS NULL
```

- The first line is `-- no-transaction`, so sqlx runs the file outside a
  transaction.
- The file holds exactly one statement. Postgres runs a query string of
  several statements as one implicit transaction and refuses
  `CONCURRENTLY` inside it, and sqlx sends a no-transaction file as one
  query string.
- A concurrent build never uses `IF NOT EXISTS`. A concurrent build that
  fails (uniqueness violation, deadlock, cancellation, crash) leaves an
  `INVALID` index behind, and sqlx records a no-transaction migration only
  after it succeeds, so the next start reruns the file. `IF NOT EXISTS`
  would skip the invalid index and record the migration as applied. Without
  it the start fails on the leftover name and the deployment stops.
- Drops use `DROP INDEX CONCURRENTLY [IF EXISTS] name`, also alone in a
  no-transaction file. Rebuilds are a concurrent build under a new name,
  then a concurrent drop of the old one, in two files.
- A unique constraint on an existing table is a concurrent unique index
  first, then `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE USING INDEX name`
  in a later, ordinary migration.
- An index on a table the same file creates may use a plain build: the
  table is empty.

These files are the exception to "directly rerunnable": sqlx never reruns
a recorded migration, and a rerun after a failure fails loudly instead of
recording an invalid index.

### Recovering from a failed concurrent build

1. List invalid indexes:
   `SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;`
2. Drop the one the failed migration names:
   `DROP INDEX CONCURRENTLY <name>;`
3. Redeploy. The migration reruns from the start.

### What a concurrent build waits for

A concurrent build waits for the transactions writing the table when it
starts and again between its scans, and at the end for every transaction
whose snapshot predates the build. Writes continue meanwhile. During a rolling deployment the old
replica keeps serving; a long transaction on either replica makes the build
take longer, not the writes.

### Enforcement

`crates/service/tests/migration_rules_test.rs` checks every migration after
0049 against these rules, shows the checker refusing each unsafe shape, and
runs the fixtures in `crates/service/tests/fixtures/migration_rules/`
through the sqlx migrator against Postgres: a concurrent build lets a write
through while it waits, and the same build inside a transaction or beside a
second statement fails before creating anything.

0001–0049 predate the rule and are applied. 0047 built
`listings_live_seller_idx` and rebuilt `drop_listings_one_active_per_listing`
with plain builds.

### Indexes the listing tombstones use

| Query | Index |
|---|---|
| Deletion follower: sellers with live listings | `listings_live_seller_idx` (0047) |
| Tombstone and drop gating: bindings of one listing | `drop_listings_listing_idx` (0010) |
| One active drop per listing generation | `drop_listings_one_active_per_listing` (0047) |
| Follower cursors, leases | primary keys of `listing_deletion_cursors`, `worker_leases` |

0050 (fenced listing deletion) adds `worker_leases.fence`,
`listings.revived_from_cursor`, and two guard triggers, and no index. It
sets `lock_timeout` so its brief `listings` locks cannot queue behind a
long transaction.

0051 (listing revival proof) adds `listings.record_epoch` with a constant
default (no rewrite), a `listings` trigger that maintains it, replaces the
`listings` guard function, and creates `listing_revival_checks` with its
primary key. It builds no index on an existing table and sets
`lock_timeout` like 0050. No index serves the follower's revival read (live
listings with `revived_from_cursor` set); it filters the live listings.

0052 (Paykit prepared attempts) adds four nullable `orders` columns for an
attempt the upstream paykit API prepares (`paykit_payment_reference`,
`paykit_operation_id`, `paykit_payment_window_seconds`, `paykit_asset`) and
one `NOT VALID` CHECK that keeps them all-or-nothing and the window positive.
No default, no rewrite, no index; it sets `lock_timeout` like 0050. A binary
from before it lists its `orders` columns and ignores them. `paykit_asset` has
no CHECK on purpose: a later asset is a new value, not a migration.

0053 (payment assets) widens `orders.payment_method` to `usdt`, adds five
nullable `orders` columns for what the buyer sends (`payment_asset`,
`payment_network`, `payment_amount_minor`, `payment_exponent`,
`payment_quote_basis`), and creates `seller_accepted_payment_options` with its
primary key. The two `orders` CHECKs (the method list; the five columns all
set or all NULL with a positive amount) are `NOT VALID`: new and updated rows
are checked, no table scan runs under the lock. No default, no rewrite, no
index on an existing table; it sets `lock_timeout` like 0050. A binary from
before it lists its `orders` columns and ignores them. The asset, network and
quote basis have no CHECK on purpose: a later value is not a migration.
