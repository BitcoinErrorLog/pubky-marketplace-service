# Pubky Marketplace Transaction Service

Server-authoritative Rust service for marketplace inventory, reservations,
checkout/orders, and payments, per
[ADR-0019](https://github.com/pubky) (Marketplace Transaction Authority) and
ADR-0022 (Rust implementation). The TypeScript prototype engine
(`services/marketplace/src/transaction-service.ts` in `pubky-app-marketplace`)
is the executable specification; its test cases are ported command by command
before each command is enabled here.

## Workspace layout

```
crates/domain      Canonical contracts: command envelope + payload validation,
                   money, aggregate ids, error codes, state machines, and the
                   emit-contracts binary.
crates/service     axum HTTP service: Postgres schema (sqlx migrations),
                   Pubky AuthToken auth, command executor with
                   idempotency, command handlers, background worker runtime
                   (expiry, auction close, outbox delivery with leases).
contracts/         state-machines.json — the machine-readable state machine
                   contract emitted from crates/domain.
docker-compose.yml Dev/test PostgreSQL 17 on port 55432.
```

## Setup

Requirements: Rust (1.89+), Docker.

```sh
docker compose up -d --wait          # PostgreSQL 17 on localhost:55432
export DATABASE_URL='postgres://marketplace:marketplace@localhost:55432/marketplace'
```

## Run

```sh
cargo run -p marketplace-service
```

Migrations in `crates/service/migrations/` are applied automatically at boot
(they can also be applied manually with `sqlx migrate run` from
`crates/service/`). Configuration is environment-based:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string |
| `BIND_ADDR` | `127.0.0.1:8080` | HTTP listen address |
| `ALLOWED_ORIGINS` | empty | comma-separated exact CORS origins |
| `AUTH_TOKEN_WINDOW_SECONDS` | `120` | acceptance window around server time for an AuthToken's signing timestamp |
| `AUTH_SESSION_TTL_SECONDS` | `86400` | session token lifetime |
| `WORKER_INTERVAL_SECONDS` | `10` | background worker pass interval |
| `WORKER_LEASE_SECONDS` | `30` | worker task/outbox lease duration |
| `MODERATOR_PUBKYS` | empty | comma-separated moderator pubkys, validated as z-base-32 at startup |

Endpoints: `GET /health`, `GET /ready` (checks the database),
`POST /v1/auth/sessions`,
`POST /v1/commands` (Bearer session required), `GET /v1/reports`
(Bearer session required; role-scoped — see Moderation below), and the
role-scoped read projections (Bearer session required — see Read
projections below): `GET /v1/listings/{aggregate_id}`, `GET /v1/offers`,
`GET /v1/orders`, `GET /v1/orders/{id}`, `GET /v1/payments/{id}`, and
`GET /v1/notifications`.

## Test

Integration tests run against real Postgres; `#[sqlx::test]` provisions an
isolated database per test and applies the migrations.

```sh
docker compose up -d --wait
export DATABASE_URL='postgres://marketplace:marketplace@localhost:55432/marketplace'
cargo test --workspace
```

Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test --workspace`.

## Command envelope (ADR-0019 §3)

All mutations go through `POST /v1/commands` with a closed, versioned JSON
envelope; unknown fields and unsupported versions are rejected
(`deny_unknown_fields`):

```json
{
  "version": 1,
  "command_id": "uuid",
  "aggregate_id": "listing:<seller-pubky>_<listing-id>",
  "expected_revision": 7,
  "issued_at": "2026-08-19T22:00:00.000Z",
  "kind": "listing.register",
  "payload": {}
}
```

- `command_id` is the idempotency key, unique per authenticated actor. An
  exact replay returns the stored prior result without re-executing; the same
  `command_id` with a different canonical payload returns HTTP 409
  `IDEMPOTENCY_CONFLICT`. Canonicalization hashes the parsed, normalized
  command (key order, timestamp formatting, and schema defaults do not affect
  the hash).
- `expected_revision` is enforced with a compare-and-swap in the `UPDATE`
  statement; a stale revision returns 409 `REVISION_CONFLICT` with
  `current_revision`.
- Server time is authoritative; `issued_at` is diagnostic only.
- Failures are not stored: retrying a rejected command re-executes it
  (matching the prototype engine).

Commands implemented: `listing.register`, `inventory.reserve`,
`checkout.create`, `offer.create`, `offer.counter`, `offer.accept`,
`offer.reject`, `offer.withdraw`, `auction.place_bid`, `auction.close`,
`trust.report`, and `trust.decide` (this service only; see Moderation).
Server-driven transitions (reservation expiry, offer expiry, auction close
on server time, outbox delivery) run in the background worker runtime, not
as client commands. All other command kinds are rejected by the envelope
contract until they are ported with their tests.

## Authentication (task 3.2)

Session establishment verifies a **Pubky AuthToken** — a signed, time-bound
proof of key ownership produced by the Pubky auth flow. The original
challenge–response handshake required the client to sign a nonce, which no
real browser client can do: in the normal Pubky App flow the user signs in
through Pubky Ring (an external signer device) and the app never holds the
secret key. The challenge endpoint has been removed.

1. The app runs the Pubky auth flow; `awaitToken()` resolves after the user
   approves on their signer device and yields an `AuthToken`. The app sends
   `token.toBytes()` (canonical postcard binary) as the raw request body of
   `POST /v1/auth/sessions`.
2. The service verifies the bytes with the
   [`pubky-common`](https://crates.io/crates/pubky-common) crate (pinned at
   0.11.0), built from the same `pubky/pubky-homeserver` repository as the
   `@synonymdev/pubky` SDK, so client and server share one implementation of
   the token format. The token's public key becomes the authenticated actor;
   its capabilities are recorded as the session's granted scope.

   Client and server do **not** have to be on the same version. The signature
   encoding was refactored between minor versions, so this was measured rather
   than assumed: a token signed with 0.8.0 verifies under 0.11.0, and a token
   signed with 0.11.0 verifies under the 0.8.0 SDK. Both directions round-trip.
   Reproduce by signing with `AuthToken::sign` under each version and
   cross-verifying with `AuthToken::verify` and the SDK's `AuthToken.verify`.
   This matters in practice because `pubky-app` currently ships SDK 0.8.0.
3. Replay protection is enforced by the service, not assumed from the
   token: the token's `(public key, timestamp)` identity is recorded in
   `auth_token_uses`, so each token is single-use; its signing timestamp
   must fall within `AUTH_TOKEN_WINDOW_SECONDS` of the authoritative server
   clock (the library independently rejects tokens more than 3 minutes from
   system time). Server time is authoritative throughout.
4. On success the service returns its own opaque 32-byte session token
   (base64url) with a TTL; only the SHA-256 of the token is stored.
5. `POST /v1/commands` requires `Authorization: Bearer <token>`; middleware
   resolves the actor pubky from the stored hash. There are no trust-me
   actor headers, and body fields can never select a different actor.

Establishing a marketplace session therefore requires a signer approval
(`awaitToken()` blocks on it). Whether that approval is folded into the
app's existing sign-in (one prompt granting marketplace capability to every
user) or kept as a separate first-transaction approval (scoped authority,
one extra prompt) is an open product decision, not a technical one.

CORS is restricted to the exact origins in `ALLOWED_ORIGINS`.

## Read projections

Role-scoped snake_case read models so a client can render state and supply
`expected_revision` on its next command. Every endpoint requires the same
Bearer session as `/v1/commands`; every projection carries the aggregate's
current revision (`server_revision` on listings, `revision` on offers,
orders, and payments).

| Endpoint | Scope | Body |
| --- | --- | --- |
| `GET /v1/listings/{aggregate_id}` | any authenticated user (public catalog data) | the listing/inventory projection: quantities, state, `server_revision`, unit price, sale format, and the auction state when present |
| `GET /v1/offers` | offers where the caller is buyer or seller | `{ "offers": [...] }` |
| `GET /v1/orders` | orders where the caller is buyer or seller | `{ "orders": [...] }`, each order with its embedded `payment` projection |
| `GET /v1/orders/{id}` | participants only | one order with its embedded `payment` projection |
| `GET /v1/payments/{id}` | participants only | one payment projection |
| `GET /v1/notifications` | the recipient only | `{ "notifications": [...] }` |

Object-level participation is enforced in the SQL `WHERE` clause, exactly
like `GET /v1/reports`: the authenticated actor is bound as a query
parameter, so a non-participant's query cannot match another user's rows.
Single-object endpoints return 404 for absent **and** foreign aggregates,
so they do not reveal whether an aggregate exists.

**Pagination.** List endpoints accept `?limit=` between 1 and 200
(default 50); out-of-range values are rejected with 422
`INVALID_COMMAND`. Ordering is newest-first and stable:
`created_at DESC, id DESC`. There is no cursor yet; the bounded limit is
the whole convention.

**Redaction (ADR-0019 §8).** Projections never expose:

- `orders.delivery_address` — private delivery detail. The buyer receives
  it back once, in the checkout command result they authored; read
  projections omit it for both participants.
- `payments.locks_bundle_id` — the Locks correlation id (`access
  credentials or bundle_id` in ADR-0019 §8).
- Reservation buyer identities and auction proxy-bid maximums; the
  auction's current `leader_pubky` stays visible because the auction state
  machine already exposes the leader to every bidder.

Offer `message`/`history` are returned: they are negotiation content
between exactly the two offer participants, the projection is readable by
exactly those two participants, and the offer command results already
return the same view to the same audience.

Not served, deliberately (no fabricated or empty-by-default reads):

- **Receipts** — the durable schema has no receipts table and no command
  populates `orders.receipt_id`; a `GET /v1/receipts/{id}` would never
  return anything. The column is projected truthfully (currently always
  null).
- **Conversations/messages** — no durable tables exist; the `message.*`
  commands have not been ported.
- **Notification preferences** — the `notification.*` commands have not
  been ported.

## Background workers (task 3.4)

One worker runtime (`crates/service/src/workers.rs`) drains four server-time
tasks: reservation expiry, offer expiry, auction close, and the outbox.

- **Leases.** Each task has a row in `worker_leases`; an instance takes (or
  renews) it with a conditional upsert and holds it for
  `WORKER_LEASE_SECONDS`. Two instances never drain the same task
  concurrently, and a holder that dies mid-lease is recovered by any
  instance once the lease lapses. Inside a task, due rows are additionally
  locked with `FOR UPDATE SKIP LOCKED`, so even a lease violation cannot
  double-process a row.
- **Outbox.** Intents are written in the command transaction (ADR-0019 §4)
  and delivered at least once: a claim stamps `lease_until` on the row, then
  each row's notification insert and `delivered_at` mark commit in one
  transaction. A crash between claim and delivery leaves the row leased but
  undelivered; it is redelivered after the lease lapses. The consumer side
  (the `notifications` table) dedups by `(event_id, recipient_pubky)`, so
  redelivery never duplicates the effect.
- **Auction close on server time.** The worker closes active auctions whose
  `ends_at` has passed through the same code path as the seller's
  `auction.close` command; the `active` status guard plus the partial unique
  index `orders_one_winner_per_auction` make the close exactly-once even
  when the command and the worker race.

## Moderation (task 3.5)

Moderators are the pubkys configured in `MODERATOR_PUBKYS` (validated as
z-base-32 at startup; there is no hardcoded moderator identity and no broad
admin role — the moderator role only grants the powers below).

- `trust.report` (any authenticated user) files a report.
- `GET /v1/reports` is role-scoped: a moderator reads every report; any
  other user reads only the reports they submitted, never another user's.
- `trust.decide` (moderators only) records a decision (`dismissed` or
  `actioned`). Decisions are appended to `report_decisions`, which rejects
  `UPDATE`/`DELETE` by trigger; the report row tracks the resulting state
  under the usual revision compare-and-swap.

## PostgreSQL invariants (ADR-0019 §4)

Each accepted command atomically persists the aggregate state/revision, one
immutable domain event, the idempotency result, and complete outbox intents,
in one transaction. Constraints enforce:

- one accepted result per actor + command id (`command_results` primary key);
- one winning order per auction (partial unique index on
  `orders.auction_aggregate_id`);
- at most one order per checkout command + seller group (partial unique index);
- non-negative inventory (`CHECK` on every quantity column) and quantity
  balance (`available + reserved + sold = total`);
- one payment-confirmed event per payment (partial unique index on `events`);
- one current revision per aggregate (compare-and-swap `UPDATE` plus
  `UNIQUE (aggregate_id, revision)` on `events`);
- the event log is append-only (`UPDATE`/`DELETE` rejected by trigger).

## State machine contract (task 3.3)

The canonical state/transition tables for listing, reservation, offer,
auction, order, payment, and report live in one module:
`crates/domain/src/state_machines.rs`. The machine-readable artifact is
emitted to `contracts/state-machines.json`:

```sh
cargo run -p marketplace-domain --bin emit-contracts
```

The domain test `contract_artifact_is_in_sync` fails whenever the JSON on
disk is stale, so the artifact cannot drift from the Rust tables. The
pubky-app client validates its TypeScript contracts in
`src/libs/commerce` against this artifact in CI (cross-language contract
tests, per ADR-0022).

## Resolved contract divergences

The prototype service enums and the client-side TypeScript contracts
(`src/libs/commerce/transaction-contracts.ts`, `state-machines.ts`) diverge.
This service is canonical and resolves them as follows:

| Area | Client contracts said | Canonical (this service) | Rationale |
| --- | --- | --- | --- |
| Wire casing | camelCase (prototype engine) | snake_case | ADR-0019 §3 shows the envelope in snake_case; the ADR is the authority. |
| Listing states | `draft/active/paused/reserved/sold/expired/removed` | `available/reserved/sold` | Catalog lifecycle belongs to seller-signed homeserver records; the transaction authority tracks inventory availability only (prototype engine semantics). |
| Reservation states | none (engine only had `active`) | `active/converted/released/expired` | The engine stored `expiresAt` but never swept it; this service implements server-time expiry, so the terminal states are modeled. |
| Payment states | missing `detected`, extra `created/window_elapsed/external_refund_required/refunded_external` | `awaiting_entitlement/detected/confirmed/expired/manual_review` | Prototype engine enums are the executable spec (ADR-0022). |
| Order states | extra `ready_for_pickup/return_in_transit/return_inspection`, missing `return_approved/return_received` | the 14 prototype engine states | Same; transitions are derived from the engine's command handlers. |
| Offer expiry | engine stored `expiresAt` and rejected late actions, but never swept | server-time sweep moves due offers to `expired` and emits `offer.expired` | Offer expiry is a real server-time transition here (offer machine already declared `offer_expiry`); the event kind is new because the engine had no sweep. |
| Auction close on server time | seller-only sandbox command | seller command **and** a worker close on `ends_at` | ADR-0019 makes server time authoritative; an auction must close without seller cooperation. Both paths share one implementation. |
| Auction close result | `auction_result` carried `listing` + `reservation` only | also creates and returns exactly one winning `order` + sandbox `payment` (delivery address null until provided) | The schema's `orders_one_winner_per_auction` invariant (ADR-0019 §4) is enforced at close; the order uses the final visible price and the checkout shipping/tax policy so payment flows are uniform. |
| Moderator identity | hardcoded `MARKETPLACE_SANDBOX_MODERATOR` (`m…m`) | configured `MODERATOR_PUBKYS` list, validated at startup | Task 3.5: no hardcoded roles; independent moderator role, no broad admin. |
| Report reads | non-moderators always got `[]` (own reports invisible) | moderators read all; other users read exactly their own submissions | A reporter can see what they filed; cross-user reads stay forbidden. |
| Report decisions | none (reports stayed `open` forever) | `trust.decide` (moderator-only) records append-only decisions; report states `open/dismissed/actioned` | Task 3.5 requires moderator decisions recorded append-only. |
| Notifications | synchronous in-memory append inside the command | outbox intents delivered at least once by the worker, deduped by `(event_id, recipient)` | ADR-0019 §4: side effects leave the command transaction only through the outbox. Notification preferences belong to the not-yet-ported `notification.*` commands. |
| Auth handshake | ed25519 challenge–response: the service issued a nonce for the client to sign | Pubky AuthToken verified with `pubky-common`; challenge endpoint removed | A browser client cannot sign — the secret key lives in the user's signer (Pubky Ring), and the SDK `Keypair` exposes no signing method. The AuthToken is the mechanism Pubky provides for exactly this proof; the service adds single-use and acceptance-window enforcement. |
| Payment projection | client `paymentSchema` requires `locks_bundle_id` | omitted from read projections | ADR-0019 §8 forbids exposing `access credentials or bundle_id`; the client schema must drop the field or mark it optional. |
| Order delivery address | prototype order views carried the address | omitted from read projections (the client `orderSchema` never wanted it) | ADR-0019 §8: no private delivery details in exposed records. |
| Receipts | client `receiptSchema` + sandbox `GET /v1/receipts/{id}` | no endpoint | The durable schema has no receipts table and no command populates `orders.receipt_id`; serving the endpoint would fabricate data. |
| Conversations | client `conversationSchema` + sandbox `GET /v1/conversations` | no endpoint | No durable conversation/message tables exist; `message.*` commands are not ported. |
| Notification projection | client `notificationSchema` requires a positive `revision` | no `revision` field | Delivered notifications are immutable outbox-consumer rows, not revisioned aggregates; no `notification.*` command exists that would need an `expected_revision`. |

`processing` and `closed` (order) and `cancelled` (auction) exist in the
canonical enums but no current transition produces them; they are declared in
`unreachable_states` in the contract artifact and are reserved for future
commands, exactly as in the engine.

Further deliberate deviations from the prototype engine are listed in the
repository report (order/payment views omit the not-yet-implemented
`shipment/return/dispute/refund/review` sub-objects rather than emitting
hardcoded empty values).

## Operations

- Structured logs via `tracing`; log fields are command kind, command id,
  aggregate id, revision, and outcome code — no payload contents, no
  addresses, no message bodies.
- `/health` is liveness; `/ready` verifies database connectivity.
- The worker runtime runs in-process; every drain is guarded by leases plus
  `FOR UPDATE SKIP LOCKED`, so multiple service instances are safe.
