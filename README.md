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
`GET /v1/orders`, `GET /v1/orders/{id}`, `GET /v1/orders/{id}/evidence`,
`GET /v1/disputes`, `GET /v1/payments/{id}`, `GET /v1/receipts/{id}`, and
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
`payment.sandbox_advance`, `order.cancel_request`, `order.cancel_approve`,
`fulfillment.ship`,
`fulfillment.confirm_delivery`, `return.request`, `return.approve`,
`return.receive`, `refund.record_external`, `dispute.open`,
`dispute.evidence` (this service only; see Post-purchase lifecycle),
`dispute.resolve`, `review.create`, `review.update` (this service only),
`trust.report`, and `trust.decide` (this service only; see Moderation).
Server-driven transitions (reservation expiry, offer expiry, auction close
on server time, outbox delivery) run in the background worker runtime, not
as client commands. All other command kinds are rejected by the envelope
contract until they are ported with their tests.

## Post-purchase lifecycle

The post-purchase commands drive the canonical order machine
(`processing → shipped → delivered → completed` plus the return, dispute,
and refund branches) exactly as the prototype engine did:

- **Payments and receipts.** `payment.sandbox_advance` (buyer only) drives
  the sandbox payment adapter; the service records these transitions and
  **never observes, holds, or moves funds**. Confirmation is the transition
  that issues the durable receipt (`receipts` table, one per order/payment,
  append-only) and moves the order to `paid`. The receipt `content_hash` is
  the BLAKE3 hex digest of the canonical snake_case receipt payload
  (`order_id`, `payment_id`, `total`, `issued_at`, in that field order).
  Confirmation also converts the held inventory from reserved to sold and
  marks the winning auction reservation converted — the
  `payment_confirmation` triggers declared in the contract. If a winning
  auction hold has already lapsed on server time, confirmation is refused
  (`INVALID_STATE`) rather than overselling. (The pubky-app client
  currently refuses to send `payment.sandbox_advance` to the durable
  service as a matter of client policy; the command exists here so the
  post-purchase lifecycle is real and testable end to end.)
- **Cancellation.** `order.cancel_request` (buyer only, from
  `pending_payment`/`paid`/`processing`): an unpaid order cancels
  immediately and its held stock returns to the listing; a paid order moves
  to `cancel_requested` awaiting the seller. `order.cancel_approve` (seller
  only, from `cancel_requested`) moves the order to `cancelled` and returns
  the sold quantities to available under the same quantity-balance
  constraint (see the divergence table for the resulting listing
  `sold → available` transition). Cancellation never touches the payment
  record: a confirmed payment stays confirmed with its receipt intact, and
  the only money path out of a cancelled order is the externally evidenced
  `refund.record_external` — the service never claims to move funds. An
  auction winner's cancel releases the winning hold through the reservation
  compare-and-swap (`active → released`); if the hold already lapsed on
  server time, the expiry sweep has returned the unit and the cancel
  succeeds without double-releasing. A cancelled order refuses every
  subsequent payment, fulfillment, return, dispute, and cancellation
  command.
- **Fulfillment.** `fulfillment.ship` (seller, with carrier and tracking
  number) and `fulfillment.confirm_delivery` (buyer) drive
  `paid/processing → shipped → delivered`. There is no separate
  seller-marks-delivered command: the buyer's confirmation is the delivery
  transition, as in the prototype. `delivered → completed` is reached
  through `review.create` (or a seller-favor dispute resolution).
- **Returns.** `return.request` (buyer, from `delivered`/`completed`, capped
  at the order total), `return.approve` and `return.receive` (seller). The
  return sub-state (`requested → approved → received → refunded`) is a
  canonical machine in the contract artifact.
- **External refunds.** `refund.record_external` (seller, from
  `return_received`/`disputed`/`cancelled`) records **independently supplied
  seller evidence** — an external transaction id of at least 8 characters —
  and advances the order to `refunded_external`. The service never claims
  to have moved funds (ADR-0019 §7); a CHECK constraint keeps the recorded
  amount within the order total.
- **Disputes.** `dispute.open` (either participant, one dispute per order),
  `dispute.evidence` (participants, open disputes only; this service only),
  and `dispute.resolve` (configured moderators only). Resolution **is** the
  close: the dispute machine is `open → resolved`. Buyer remedies
  (`buyer_refund`/`partial_refund`) leave the order `disputed` awaiting the
  external refund record; other resolutions complete it. Adjudication reads
  are `GET /v1/disputes` (the moderator queue) and
  `GET /v1/orders/{id}/evidence` (the case file — see Read projections and
  Moderation below).
- **Reviews.** `review.create` (participants, from
  `delivered`/`completed`/`closed`): one review per participant per order
  per role, enforced by the `reviews_one_per_order_role` database
  constraint rather than application logic — the insert happens before the
  order's revision compare-and-swap, so a same-role race is decided by the
  constraint. `review.update` (this service only) lets the reviewer revise
  rating and text within 24 hours of creation.

Every post-purchase command is participant-scoped (moderator-scoped for
`dispute.resolve`), enforces `expected_revision` compare-and-swap, replays
idempotently by `command_id`, appends one immutable event per accepted
command, and emits outbox notifications exactly where the prototype did
(`payment_confirmed`, `order_cancelled` — to the seller on a cancellation
request, to the buyer on approval — `order_shipped`, `order_delivered`,
`return_updated`,
`refund_recorded`, `dispute_updated` to both parties on resolution,
`review_received`). `dispute.evidence` and `review.update` emit none — the
prototype had no counterpart to copy.

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
| `GET /v1/orders` | orders where the caller is buyer or seller | `{ "orders": [...] }`, each order with its embedded `payment` projection plus the `shipment`, `return_request`, `dispute`, `external_refund`, and `reviews` sub-objects (client `orderSchema` field names) |
| `GET /v1/orders/{id}` | participants; also configured moderators, but only when the order is under (or was previously under) dispute | one order with the same embedded projections |
| `GET /v1/orders/{id}/evidence` | the two dispute participants and configured moderators (moderators only for orders under, or previously under, dispute) | `{ "order_id", "evidence": [...] }` — the dispute case file: each item's `id`, `submitter_pubky`, `body`, `body_bytes`, `created_at` |
| `GET /v1/disputes` | configured moderators only; everyone else is refused 403, never handed `[]` | `{ "disputes": [...] }` — the adjudication queue: the order projection of every order under (or previously under) dispute |
| `GET /v1/payments/{id}` | participants only | one payment projection |
| `GET /v1/receipts/{id}` | issuer (seller) and recipient (buyer) only | one receipt: ids, participants, `total`, `content_hash`, `issued_at` (client `receiptSchema` shape) |
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
  projections and every post-purchase command result omit it for both
  participants.
- `payments.locks_bundle_id` — the Locks correlation id (`access
  credentials or bundle_id` in ADR-0019 §8). Withheld from **command
  results as well as read projections**: it is bearer material, nothing
  client-side consumes it, and leaving it in one response shape but not
  the other would put it in logs and telemetry for no benefit.
- **Dispute evidence bodies** — private order evidence (ADR-0019 §8).
  Stored append-only in `dispute_evidence`; never served by any general
  read projection or command result, not even back to the submitter, and
  never logged. The dispute sub-object carries only a content-free
  `evidence_count`. The **only** exposure path is the scoped case-file
  read `GET /v1/orders/{id}/evidence`, whose audience is exactly the two
  dispute participants plus the configured moderator role. §8's intent is
  that evidence never leaks into public records, general projections, or
  telemetry — while §8 itself requires that "operator queries return
  role-scoped, deliberately redacted views", and a moderator who cannot
  read the case file cannot execute `dispute.resolve` honestly. The
  audience matches the one already documented for dispute
  `reason`/`rationale` (the participants plus the deciding moderator).
  Both parties see the full file deliberately: a dispute where one side
  cannot see what the other alleged cannot be answered, and a resolution
  based on evidence hidden from a party could not be contested. Moderator
  reads are audited (see Moderation); a non-authorized reader gets the
  same 404 an absent order returns, never an empty list.
- Reservation buyer identities and auction proxy-bid maximums; the
  auction's current `leader_pubky` stays visible because the auction state
  machine already exposes the leader to every bidder.

Participant-visible by decision (ambiguities resolved and documented):

- **Shipment carrier and tracking numbers** — participants need them to
  follow the shipment; they identify a parcel, not a person or address.
- **Dispute `reason`/`rationale`, return `reason`, and cancellation
  `reason`** — like offer messages, they are content exchanged between
  exactly the participants (plus the deciding moderator for disputes),
  served only to that same audience.
- **Review text** — reviews are authored for publication to the counter
  party; the projection audience is the two participants.

Offer `message`/`history` are returned: they are negotiation content
between exactly the two offer participants, the projection is readable by
exactly those two participants, and the offer command results already
return the same view to the same audience.

Not served, deliberately (no fabricated or empty-by-default reads):

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
- **Dispute adjudication reads.** The moderator role also grants
  `GET /v1/disputes` (the queue of orders under, or previously under,
  dispute — the projection carries the dispute `reason`, state, and the
  order `revision` that `dispute.resolve` requires), the single-order
  projection for disputed orders, and the evidence case file
  `GET /v1/orders/{id}/evidence`. The scope is enforced in SQL
  (`dispute IS NOT NULL`): the role reaches no undisputed order, and the
  delivery address stays redacted from every moderator view.
- **Evidence read audit.** Reading evidence through the moderator role is
  privileged cross-user access, so every such read appends a row to
  `dispute_evidence_reads` (reader, order, items served, timestamp) —
  append-only by trigger, mirroring `report_decisions` — written in the
  same transaction as the read itself, so a failed audit write refuses the
  read instead of serving unaudited data. Participant reads of their own
  case file are ordinary object participation, like every other
  participant-scoped projection, and are not audited.

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
auction, order, payment, return, dispute, and report live in one module:
`crates/domain/src/state_machines.rs`. The return and dispute machines are
the order's sub-state vocabularies the client's `orderSchema` already
declares (`returnRequest.state`, `dispute.state`). The machine-readable
artifact is emitted to `contracts/state-machines.json`:

```sh
cargo run -p marketplace-domain --bin emit-contracts
```

The domain test `contract_artifact_is_in_sync` fails whenever the JSON on
disk is stale, so the artifact cannot drift from the Rust tables. The
pubky-app client validates its TypeScript contracts in
`src/libs/commerce` against this artifact in CI (cross-language contract
tests, per ADR-0022). Any artifact change here fails the client's
`state-machines.contract.test.ts` until the JSON is re-vendored — and the
addition of the `return` and `dispute` machines additionally requires the
client to register them in `commerceAggregateMachines` with matching state
enums, since that test asserts the aggregate sets match exactly. The
cancellation port added one listing edge (`sold → available` via
`order.cancel_approve`), so re-vendoring for it also requires the client's
listing transition table to accept that edge.

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
| Payment projection | client `paymentSchema` requires `locks_bundle_id` | omitted from every response, reads and command results alike | ADR-0019 §8 forbids exposing `access credentials or bundle_id`. The client has made the field optional, so the sandbox may still send it. |
| Order delivery address | prototype order views carried the address | omitted from read projections (the client `orderSchema` never wanted it) | ADR-0019 §8: no private delivery details in exposed records. |
| Receipts | client `receiptSchema` + sandbox `GET /v1/receipts/{id}` | durable `receipts` table (append-only), issued exactly once on payment confirmation; `GET /v1/receipts/{id}` for issuer and recipient | The table and issuing transition now exist, so the endpoint serves real rows; `orders.receipt_id` is populated on confirmation. |
| Inventory after payment confirmation | prototype left the sold quantity in `reservedQuantity` forever | confirmation moves the order's line quantities reserved → sold and marks the winning auction reservation `converted` | The contract already declared listing `reserved → sold` and reservation `active → converted` under `payment_confirmation`; a durable ledger cannot hold quantities in a hold state forever. |
| Confirming a lapsed auction hold | impossible in the prototype (no reservation expiry) | refused with `INVALID_STATE` once the winner's 30-minute hold has expired on server time | This service sweeps expired holds back to `available`; confirming afterwards would sell inventory the order no longer holds. |
| Inventory release on cancelling a paid order | the prototype's `releaseOrderInventory` moved the quantity reserved → available (it never moved paid quantities out of `reservedQuantity`) | `order.cancel_approve` returns the cancelled order's sold quantities to available; the listing machine declares `sold → available` via `order.cancel_approve` | Payment confirmation here moves quantities reserved → sold (see above), so reversing a cancelled paid sale on the durable ledger necessarily reverses the sold quantities — otherwise every cancelled paid order would understock the listing forever. Same prototype semantics (held stock returns to available), expressed against the durable columns. |
| Cancelling a lapsed auction hold | impossible in the prototype (no reservation expiry) | the cancel succeeds; the reservation compare-and-swap (`active → released`) finds the hold already `expired`, so no quantities move | The sweep already returned the unit; releasing again would double-count it, and refusing would strand an order that can be neither paid (confirmation is refused, see above) nor cancelled. |
| Review uniqueness | application check on the in-memory order's `reviews` array | `reviews_one_per_order_role` UNIQUE constraint; the review insert precedes the order revision CAS so a same-role race is decided by the constraint (`INVARIANT_VIOLATION`), sequential duplicates by the ported check (`INVALID_STATE`) | ADR-0019 §4: one participant review per order/role is a database invariant, not code. |
| Review editing | none (reviews were immutable) | `review.update` (this service only): the reviewer may revise rating/text within 24 hours of creation, under the order's revision CAS with a `review.updated` event | The task's bounded edit window; the window is a documented policy constant (`REVIEW_EDIT_WINDOW_SECONDS`). No notification is emitted — the prototype had none to copy. |
| Dispute evidence | none (disputes carried only the opening reason) | `dispute.evidence` (this service only): participants append evidence to an open dispute; bodies stay out of general projections and command results (only `evidence_count` is visible there) and are served solely by the scoped case-file read `GET /v1/orders/{id}/evidence` to the two participants and configured moderators, with moderator reads audited append-only in `dispute_evidence_reads` | ADR-0019 lists order evidence as service-owned private data, and §8 directs operator queries to role-scoped, deliberately redacted views; without a scoped read path the moderator required by `dispute.resolve` could never see the case they are deciding. |
| Dispute adjudication reads | none (no moderator read surface existed) | `GET /v1/disputes` (moderator-only queue) and moderator access to disputed-order projections, scoped in SQL to `dispute IS NOT NULL` | The deciding moderator needs the dispute reason and the order revision that `dispute.resolve` enforces; without a read surface the moderator-only resolve command was structurally unusable. |
| Dispute close | `dispute.resolve` was the terminal action | same — resolution **is** the close (`open → resolved`); no separate close command | Neither the prototype nor the client contracts have a distinct close; inventing one would add unspecified surface. |
| Dispute moderator | hardcoded `MARKETPLACE_SANDBOX_MODERATOR` | configured `MODERATOR_PUBKYS`, the same role as `trust.decide` | Task 3.5 precedent: no hardcoded roles. |
| Conversations | client `conversationSchema` + sandbox `GET /v1/conversations` | no endpoint | No durable conversation/message tables exist; `message.*` commands are not ported. |
| Notification projection | client `notificationSchema` requires a positive `revision` | no `revision` field | Delivered notifications are immutable outbox-consumer rows, not revisioned aggregates; no `notification.*` command exists that would need an `expected_revision`. |

`processing` and `closed` (order) and `cancelled` (auction) exist in the
canonical enums but no current transition produces them; they are declared in
`unreachable_states` in the contract artifact and are reserved for future
commands, exactly as in the engine.

## Operations

- Structured logs via `tracing`; log fields are command kind, command id,
  aggregate id, revision, and outcome code — no payload contents, no
  addresses, no message bodies.
- `/health` is liveness; `/ready` verifies database connectivity.
- The worker runtime runs in-process; every drain is guarded by leases plus
  `FOR UPDATE SKIP LOCKED`, so multiple service instances are safe.
