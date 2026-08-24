# Pubky Marketplace Transaction Service

Server-authoritative Rust service for marketplace inventory, reservations,
checkout/orders, and payments, per
[ADR-0019](https://github.com/BitcoinErrorLog/pubky-app/blob/marketplace/pr25-ux/docs/adr/0019-marketplace-transaction-authority.md)
(Marketplace Transaction Authority) and
[ADR-0022](https://github.com/BitcoinErrorLog/pubky-app/blob/marketplace/pr25-ux/docs/adr/0022-marketplace-transaction-service-rust.md)
(Rust implementation). The TypeScript prototype engine
(`services/marketplace/src/transaction-service.ts` in
[`BitcoinErrorLog/pubky-app`](https://github.com/BitcoinErrorLog/pubky-app))
is the executable specification; its test cases are ported command by command
before each command is enabled here.

## Workspace layout

```
crates/domain      Canonical contracts: command envelope + payload validation,
                   money, aggregate ids, error codes, state machines, and the
                   emit-contracts binary.
crates/service     axum HTTP service: Postgres schema (sqlx migrations),
                   Pubky AuthToken auth, command executor with
                   idempotency, command handlers, Locks lifecycle
                   verification (encrypted correlations, HMAC lookup
                   tokens, Lock Server client), background worker runtime
                   (expiry, auction close, outbox delivery, Locks
                   verification, payment window — all with leases).
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
| `LOCKS_SERVER_URL` | unset | Lock Server base URL; setting it enables Locks verification |
| `LOCKS_BUNDLE_ENCRYPTION_KEY` | unset | 32-byte hex key encrypting bundle ids at rest (XChaCha20-Poly1305) |
| `LOCKS_LOOKUP_HMAC_KEY` | unset | 32-byte hex key for the HMAC-SHA256 correlation lookup token; must differ from the encryption key |
| `LOCKS_PAYMENT_WINDOW_SECONDS` | `3600` | hold window armed by the `payment.register_locks` lock point (≥ 60); the correlation window IS the hold window |
| `FIAT_PAYMENT_WINDOW_SECONDS` | `3600` | hold window armed by the payment-method bind lock point (`POST /v0/orders/{id}/payment-method`, all three rails) (≥ 60) |
| `SANDBOX_PAYMENT_WINDOW_SECONDS` | `900` | hold window armed by the sandbox lock point (`payment.sandbox_advance`'s first transition out of `awaiting_entitlement`) (≥ 60) |
| `DROP_CLAIM_WINDOW_SECONDS` | `600` | hold window armed at checkout for drop-bound orders (lock-at-claim) (≥ 60) |
| `LOCKS_POLL_SECONDS` | `30` | minimum interval between lifecycle lookups per pending correlation |
| `ATTESTOR_SECRET_KEY` | unset | 32-byte hex Ed25519 secret of the attestor identity (ADR 0024); its z-base-32 public key is the attestor pubky |
| `ATTESTOR_ORDER_SALT` | unset | 32-byte hex salt for `order_ref` hashing; must stay stable for the attestor identity's lifetime |
| `STRIPE_KEY_ENCRYPTION_KEY` | unset | 32-byte hex key sealing seller Stripe restricted keys at rest; setting it enables the `/v0` payment-methods surface |
| `STRIPE_API_BASE` | `https://api.stripe.com` | Stripe API base URL (overridden only by tests) |
| `PAYKIT_SERVER_URL` | unset | paykit-server base URL; setting it enables the bitcoin method |
| `PAYKIT_REQUEST_SIGNING_KEY` | unset | 32-byte hex ed25519 seed signing paykit-server requests; its pubky-formatted public key is paykit-server's `marketplace.trusted_public_key` |
| `PAYKIT_POLL_SECONDS` | `15` | minimum interval between paykit status polls per pending bitcoin order |
| `PUBLIC_APP_ORIGIN` | unset | the web app's public origin (e.g. `https://shop.pubky.app`); when set, hosted checkouts that support a return destination (PayPal `_xclick`) send the buyer back to `{origin}/marketplace/orders` after commit/cancel |
| `PUBLIC_SERVICE_ORIGIN` | unset | this service's own public origin; when set, PayPal checkout links carry `notify_url={origin}/v0/paypal/ipn` so PayPal's IPN pays the order automatically (postback-verified, matched against seller email + exact order total). Unset, PayPal stays participant-attested |
| `PAYPAL_IPN_VERIFY_URL` | `https://ipnpb.paypal.com/cgi-bin/webscr` | PayPal's IPN validation endpoint; tests point it at a local double |
| `SANDBOX_PAYMENTS_ENABLED` | `false` | accept `payment.sandbox_advance` at all; must stay `false` on any deployment handling real orders |

The three `LOCKS_*` secrets/URL are all-or-nothing: the service **fails
closed at startup** on a partial configuration (a URL without keys, or keys
without a URL), rather than running with verification silently disabled or
bearer material unprotected. With none of them set the deployment is
sandbox-only: `payment.register_locks` is refused and the lifecycle poller
is not scheduled.

`SANDBOX_PAYMENTS_ENABLED` is the server-side boundary for the sandbox
payment adapter. Every checkout starts on the `sandbox` adapter until a real
method is bound, and `payment.sandbox_advance` lets the buyer drive that
payment to `paid` by command — which on a durable deployment would be a
self-serve paid state with no money moving. The client's transport
allowlist refuses to send the command to the durable service, but that is a
UX courtesy, not a boundary; with the flag at its default the service
rejects the command outright (`INVALID_COMMAND`, 422) regardless of what
any client sends.

The two `ATTESTOR_*` secrets are likewise all-or-nothing (fail closed at
startup on a partial pair). With neither set, reviews still work but no
purchase attestations are issued (`review.create` results carry no
`attestation`), no attestor annotations are recorded, the weekly seller
stat-attestation job does not run, and `attestation.disavow` is refused.
With both set, `review.create` issues a durable compact-JWS purchase
attestation inside the review transaction (re-fetchable via
`GET /v1/orders/{id}/review-attestation`, participant-scoped, idempotent),
the D2 amount-band consent gate applies (`attestation.set_band_consent`
command + `GET /v1/sellers/{pubky}/band-consent` read), dispute resolutions
and external refunds append `attestation_annotations` rows keyed by the
salted `order_ref`, and the worker signs weekly per-seller stat attestations
into `seller_stat_attestations`. Publication of annotations and stat
attestations to the attestor's homeserver is Phase 3 of the trust &
reputation plan (pubky-app `docs/ecommerce/trust-reputation-plan.md`) and is
not part of this service yet — the rows accumulate here as the publisher's
ground truth.

With an attestor configured, participants can also fetch a **receipt
attestation** (`GET /v1/receipts/{receipt_id}/attestation`,
participant-scoped exactly like `GET /v1/receipts/{id}`; without an
attestor the fetch is 404, like the review-attestation re-fetch): a compact
JWS (`alg: EdDSA`, `typ: pubky-order-receipt+v1`) signed by the attestor,
attesting the paid order's facts so the portable receipt document a buyer
or seller publishes on their own homeserver stays verifiable after this
operator disappears ("credible exit for orders"). The response is
`{ "receipt_attestation": { "jws", "claims" } }`. Claims, in normative
order:

| Claim | Value |
| --- | --- |
| `v` | `1` |
| `iss` | the attestor pubky (z-base-32; decodes to the Ed25519 verification key) |
| `buyer` | the order's buyer pubky |
| `seller` | the order's seller pubky |
| `order` | the order UUID, lowercase hyphenated |
| `receipt` | the receipt UUID, lowercase hyphenated |
| `total_minor` | the order's total in minor units |
| `currency` | the order's currency |
| `exponent` | the order's currency exponent |
| `paid_at` | the receipt's stored creation instant as the canonical wire timestamp (RFC 3339, milliseconds, `Z`) — string-equal to `issued_at` on `GET /v1/receipts/{id}` |
| `iat` | the receipt's stored creation instant as epoch seconds |

Every claim derives from stored rows (the receipt's creation instant, the
order's totals) — never from the current time — so the JWS is
deterministic: repeated fetches by either participant return the
byte-identical token, and nothing is stored; the endpoint re-signs on
demand.

Participants in a paid DROP order (see Drops below) can additionally fetch
an **edition attestation**
(`GET /v1/receipts/{receipt_id}/edition-attestation`): a compact JWS
(`alg: EdDSA`, `typ: pubky-drop-edition+v1`) attesting which numbered
edition of the drop the order received. The response is
`{ "edition_attestation": { "jws", "claims" } }`. Absent receipts, foreign
receipts, non-drop orders (no `drop_aggregate_id`/`edition` on the order),
and attestor-less deployments are all 404 with the same body as
`GET /v1/receipts/{id}`, indistinguishably. Claims, in normative order (the
specs-fork verifier for `pubky-drop-edition+v1` checks the serialized
payload byte-for-byte, so the order is the wire contract):

| Claim | Value |
| --- | --- |
| `v` | `1` |
| `iss` | the attestor pubky (z-base-32; decodes to the Ed25519 verification key) |
| `buyer` | the order's buyer pubky |
| `seller` | the drop owner's pubky (the order's seller) |
| `drop` | the DROP ID (the seller's record identifier, parsed from the aggregate id's `drop:{seller}_{drop_id}` shape) — never the aggregate id |
| `edition` | the order's edition, 1-based |
| `of` | the drop's `total_quantity` |
| `receipt` | the receipt UUID, lowercase hyphenated |
| `iat` | the receipt's stored creation instant as epoch seconds — deterministic, never wall clock |

Like the receipt attestation, every claim derives from stored rows, so
repeated fetches by either participant return the byte-identical token and
nothing is stored.

Endpoints: `GET /health`, `GET /ready` (checks the database),
`POST /v1/auth/sessions`,
`POST /v1/commands` (Bearer session required), `GET /v1/reports`
(Bearer session required; role-scoped — see Moderation below),
the public drop projection `GET /v0/drops/{seller_pubky}/{drop_id}`
(no session — see Drops below), and the role-scoped read projections
(Bearer session required — see Read projections below):
`GET /v1/listings/{aggregate_id}`, `GET /v1/drops/{aggregate_id}`,
`GET /v1/drops/{aggregate_id}/me`, `GET /v1/offers`,
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

Commands implemented: `listing.register`, `drop.sync`, `drop.cancel`,
`drop.release_listings` (see Drops), `inventory.reserve`,
`checkout.create`, `offer.create`, `offer.counter`, `offer.accept`,
`offer.reject`, `offer.withdraw`, `auction.place_bid`, `auction.close`,
`payment.sandbox_advance`, `payment.register_locks`,
`order.cancel_request`, `order.cancel_approve`,
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

## Inventory semantics: only a payment locks an item

Checkout does not hold stock. `checkout.create` validates the cart against
the pinned listing snapshot and **refuses when the requested quantity is
not available at that instant** (`INSUFFICIENT_INVENTORY`, the pinned copy
`Checkout quantity is unavailable.`) — but that check is advisory: the
command creates the immutable order snapshot and its sandbox-adapter
payment and moves **no inventory**. The order starts with `stock_held =
false` and no hold window; the listing's `server_revision` does not bump.
Abandoned carts therefore block nobody, and racing checkouts never contend:
any number of buyers can hold orders against the same stock — only payments
compete for it.

**Payment lock points.** The inventory hold is acquired at the moment a
payment starts. Each lock point atomically moves `available → reserved` for
the order's quantities under the listing row lock — failing with
`INSUFFICIENT_INVENTORY` and the pinned copy
`The listing sold out before this payment started.` when the stock is gone —
and arms a bounded server-time hold window on the order
(`orders.hold_expires_at`):

| Lock point | Window |
| --- | --- |
| `payment.register_locks` | `LOCKS_PAYMENT_WINDOW_SECONDS` (default 3600) — the existing correlation window IS the hold window; one window concept, not two |
| `POST /v0/orders/{id}/payment-method` (bind, all three rails) | `FIAT_PAYMENT_WINDOW_SECONDS` (default 3600, min 60) |
| `payment.sandbox_advance` transitioning OUT of `awaiting_entitlement` (the first transition; gated by `SANDBOX_PAYMENTS_ENABLED` as always) | `SANDBOX_PAYMENT_WINDOW_SECONDS` (default 900, min 60) |

A lock point on an order that ALREADY holds stock never double-decrements
(idempotent by order): a drop-bound order re-arms its window to the lock
point's own span (see Drops), an ordinary order re-arms nothing. A lock
point on an order that is no longer `pending_payment` is refused — a
cancelled order can never grab stock. Auction orders are outside this
mechanism entirely: the winner's hold is the winning `reservations` row,
taken at close and swept by reservation expiry exactly as before.

**Confirmation.** `confirm_order` (sandbox command, Locks worker, paykit
worker, and both fiat verification legs all converge there) requires the
order to hold stock — every confirming path passes a lock point first by
construction, so a missing hold is an `INVARIANT_VIOLATION` — and converts
`reserved → sold` exactly as before, clearing the hold flags in the same
update.

**Window expiry.** The worker's `payment_window` task sweeps pending,
unconfirmed orders whose hold window elapsed on server time: the hold
releases (`reserved → available`, crediting a stamped drop first under the
shared lock order), the payment moves to `expired`
(`awaiting_entitlement → expired`, the machine's `payment_window` edge),
and the ORDER is cancelled (`pending_payment → cancelled`, the contract's
`server`-triggered `payment_window` variant) with the stored
`cancellation_reason` `payment window elapsed`. Post-expiry the buyer
simply checks out again. Payments under `manual_review` are deliberately
not swept (a human decides those — their money may be real); a sandbox
payment the buyer drove to `detected` has no `detected → expired` edge, so
its lapsed order cancels and restocks with the payment record left
untouched, exactly as buyer cancellation leaves it. Confirmed orders are
`paid` and are never touched by the sweep. Money observed AFTER expiry is
never dropped: a late Locks completion, a late paykit settlement (when the
payment was already detected on-chain), and a late fiat verification all
route the expired payment to `manual_review`.

**Cancellation.** Buyer/seller cancellation of a pending order releases the
hold ONLY when one exists — an unheld pending order releases nothing, and
the quantity-balance `CHECK` constraints prove no negative movement can
happen.

**Drops keep lock-at-claim.** See Drops below: the FCFS race is the
product, so drop gating still debits the drop AND moves the listing unit at
checkout — but the claim is bounded by `DROP_CLAIM_WINDOW_SECONDS` (default
600) from checkout, and a payment lock point re-arms the window to its own
(longer) span. A claimed-but-never-paid drop order expires through the same
sweep, restocking the drop, freeing the buyer's per-drop cap, and releasing
the listing unit.

**Migration 0012 backfill.** Under the old semantics every
`pending_payment` checkout order had decremented its listings at checkout,
so migration 0012 marks them all `stock_held = true` (auction orders
excluded — their hold is the reservation row): orders with a Locks
correlation keep the correlation's window as their hold window; orders with
a bound payment method get `now() + interval '1 hour'` (the fiat window
default); every other pending order — legacy abandoned carts included —
gets `now() + interval '1 hour'`, so listings bricked by abandoned carts
self-clean through the new worker within the hour.

## Post-purchase lifecycle

The post-purchase commands drive the canonical order machine
(`processing → shipped → delivered → completed` plus the return, dispute,
and refund branches) exactly as the prototype engine did:

- **Payments and receipts.** `payment.sandbox_advance` (buyer only) drives
  the sandbox payment adapter; the service records these transitions and
  **never observes, holds, or moves funds**. Its first transition out of
  `awaiting_entitlement` is a payment lock point (see Inventory semantics
  above): it acquires the order's inventory hold and arms the sandbox hold
  window. Confirmation is the transition
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
  post-purchase lifecycle is real and testable end to end.) Real payments
  are advanced only by server-side verification of the Locks lifecycle —
  see "Locks verification" below; the same confirmation transition (receipt,
  inventory conversion, notification) is shared by both paths.
- **Cancellation.** `order.cancel_request` (buyer only, from
  `pending_payment`/`paid`/`processing`): an unpaid order cancels
  immediately, and its held stock — ONLY when a payment lock point (or a
  drop claim) actually acquired one; an unheld pending order releases
  nothing — returns to the listing; a paid order moves
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
  number — both trimmed printable strings, deliberately not enums; the soft
  carrier vocabulary the reference client writes is documented on
  `ShipOrderPayload.carrier`) and `fulfillment.confirm_delivery` (buyer) drive
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

## Drops (ADR-0026)

Timed, limited releases. A drop is registered by the convergent `drop.sync`
command from the seller-signed homeserver record, gates `inventory.reserve`
and `checkout.create` on its bound listings (schedule, remaining stock, and
the per-buyer cap, all inside the hold's transaction), and always derives
its state from server time plus the paid count: `announced` → `live` →
`ended_closed` / `ended_sold_out` / `ended_cancelled` (all ends terminal).

**Lock-at-claim, bounded.** Unlike ordinary listings ("only a payment locks
an item" — see Inventory semantics above), drop-bound checkout still holds
stock AND debits the drop's remaining/per-buyer counters AT CHECKOUT: the
FCFS race is the product. The claim is no longer forever, though — the
order's hold window arms immediately with `DROP_CLAIM_WINDOW_SECONDS`
(default 600), and a payment lock point re-arms it to its own (longer)
span. A claimed-but-never-paid order expires through the payment-window
sweep, which credits the drop (restocking a live drop and freeing the
buyer's per-drop cap), releases the listing unit, and cancels the order.
Editions and the sell-out transition on confirmation are unchanged.

**Single-line drop checkout.** Editions map one order to one unit, so a
checkout containing a drop-bound line must contain EXACTLY that one line
with quantity 1. Anything else is refused with 422 `INVALID_COMMAND` and
the pinned copy `A drop order is one unit of one listing per checkout.`

**Editions.** The exactly-once payment confirmation path (sandbox command,
Locks worker, fiat verify all converge there) increments the drop's
`paid_quantity` under the drop row lock; the new value IS the order's
edition — 1-based and gapless over paid orders, stored on
`orders.edition` and serialized on order projections alongside
`drop_aggregate_id`. When `paid_quantity` reaches `total_quantity` the drop
transitions to the terminal `ended_sold_out` and the seller receives a
`drop_sold_out` notification. Participants can fetch the signed edition
attestation for the order's receipt (see the claim table above).

**`drop.release_listings`** (seller only, `expected_revision`
compare-and-swap): allowed only from the three ended states; from
`announced`/`live` it is refused with 409 `INVALID_STATE` and the pinned
copy `Listings release only after the drop ends.` Releasing removes the
drop's listing bindings from gating consideration entirely
(`drop_listings.released`), so the listings sell again as ordinary open
inventory — until then an ended binding keeps refusing checkout so a
concluded drop's listing never quietly falls back to open sale.

**Public projection** — `GET /v0/drops/{seller_pubky}/{drop_id}`, no
session: `{ "drop": { seller_pubky, drop_id, aggregate_id, state, format,
starts_at, ends_at, stock_display, total_quantity, per_buyer_limit,
remaining, remaining_band, revision, server_time } }`. `server_time` is the
service clock now, in the canonical wire timestamp format — clients correct
countdowns from it. The read applies the same lazy server-time transitions
gating uses (inside a transaction with the drop row locked), so a public
read never shows `announced` after `startsAt`. Stock-display redaction is
SERVER-side; an exact count never leaves the service under bands/hidden:

| `stock_display` | `remaining` | `remaining_band` |
| --- | --- | --- |
| `exact` | the exact remaining count | `null` |
| `bands` | `null` | `plenty` (> 25% of total), `low` (≤ 25%), `last_few` (≤ 5%, minimum threshold one unit) |
| `hidden` | `null` | `null` |

**Seller projection** — `GET /v1/drops/{aggregate_id}` (Bearer session):
the seller only; absent and foreign drops are both 404. Full facts: exact
remaining, `paid_quantity`, distinct buyer count, state, schedule, caps,
`stock_display`, listing ids, revision, `server_time`.

**Buyer ready-check** — `GET /v1/drops/{aggregate_id}/me` (Bearer session):
any authenticated user reads their own per-drop counters:
`{ "purchased", "per_buyer_limit", "remaining_allowance" }` from
`drop_purchases` (purchased is zero without a counter row).

## Locks verification (task 4.5)

Real (non-sandbox) payments are settled outside this service: the buyer
submits a proof bundle to a Lock Server, Locks obtains an invoice from
Paykit Server, Bitcoin is observed, and the Locks verification lifecycle
completes. This service **independently verifies that completion** against
the Lock Server before advancing the order — a client claim can never
advance a payment (ADR-0019 §7).

- **Registration.** `payment.register_locks` (buyer only, payment in
  `awaiting_entitlement`) stores the encrypted correlation between the
  payment/order and the Locks lifecycle identity `{creator, bundle_id}`.
  The bundle id is the buyer's cryptographically random lifecycle handle —
  a bearer secret — and the lock resource's creator must equal the order's
  seller.   The correlation binds order id, buyer, creator, BLAKE3 lock
  resource hash, amount, asset, and guarantee policy version
  (upstream-integration "Transaction-service correlation"). Registration
  is a payment lock point (see Inventory semantics above): it acquires the
  order's inventory hold and arms the payment window — the correlation
  window IS the hold window. It flips the payment to the `locks` adapter,
  which permanently refuses `payment.sandbox_advance`, and does **not**
  advance the payment state.
- **Correlation secrecy.** The bundle id is stored only as
  XChaCha20-Poly1305 ciphertext (random 24-byte nonce, payment id as
  associated data, so ciphertexts cannot be transplanted between rows);
  queries and uniqueness use an HMAC-SHA256 lookup token, never the raw
  value. The unique token also means one lifecycle identity can never
  correlate two orders. Nothing serializes the correlation row: no read
  projection, command result, event, outbox intent, log, or error carries
  the bundle id or lock resource
  (`bundle_and_lock_resource_never_leave_the_correlation_store` asserts
  this across every surface).
- **Verification.** The worker's `locks_verification` task claims due
  pending correlations (bounded batches, one lookup per correlation per
  `LOCKS_POLL_SECONDS`), decrypts the bundle id, and performs a real
  `POST /verification-task-lookups` against the configured Lock Server
  (pinned contract commit `ba49a777`; tests drive the same code path
  through a fake lifecycle client — production only ever constructs the
  HTTP client). Verification is a pure function of what Locks reports:
  - `completed` → the payment advances `awaiting_entitlement → confirmed`
    **exactly once**, enforced by the payment-state compare-and-swap plus
    the `events_one_payment_confirmed` unique index rather than
    application logic; the confirmation applies the same effects as the
    sandbox path (order `paid`, receipt issued, inventory converted,
    `payment_confirmed` outbox intent). Duplicate or reordered completions
    are harmless no-ops.
  - `pending` / `in_progress` / not-found / transport or status failure →
    the payment stays untouched and the correlation stays pending (Locks
    v1 leaves transport/status failures pending and exposes no terminal
    payment failure to the viewer).
  - `failed` / `expired` (the Locks task aged out) → recorded as an
    upstream fact and polling stops; the payment is **not** expired by it.
- **Expiry vs late completion.** The `payment_window` task sweeps the
  ORDER's hold window (`LOCKS_PAYMENT_WINDOW_SECONDS`, armed at
  registration — the correlation window IS the hold window): once it
  elapses on server time with the payment still `awaiting_entitlement`,
  the hold restocks, the payment moves to `expired`, and the order is
  cancelled with the stored reason `payment window elapsed` — deliberately
  separate from upstream failure. The correlation keeps polling after the
  window (bounded by the Lock Server's own task ageing), so a completion
  verified **after** marketplace expiry moves the payment
  `expired → manual_review` and is retained — never a confirmation of a
  dead order, never silently discarded. A verified completion whose order
  can no longer be confirmed (e.g. cancelled while pending, or a lapsed
  auction hold) also goes to `manual_review`.
- **History.** Every observed upstream status change and every marketplace
  action on it is appended to `payment_locks_observations` (append-only by
  trigger, statuses only — no bearer material), so late-completion and
  reconciliation history is durable.
- **Crash safety.** A verification claim only stamps `last_checked_at`;
  every effect commits in its own guarded transaction. A holder that dies
  between lookup and effect leaves the correlation pending, and any
  instance resumes it after the poll interval — bounded, abortable,
  resumable, with the same lease discipline as every other worker task.

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
| `GET /v1/drops/{aggregate_id}` | the drop's seller only; absent and foreign drops are both 404 | the seller's full drop facts: exact remaining, `paid_quantity`, distinct buyer count, state, schedule, caps, `stock_display`, listing ids, revision, `server_time` |
| `GET /v1/drops/{aggregate_id}/me` | any authenticated user (their own counters only) | `{ "purchased", "per_buyer_limit", "remaining_allowance" }` from the caller's per-drop purchase row (zeros without one) |
| `GET /v1/offers` | offers where the caller is buyer or seller | `{ "offers": [...] }` |
| `GET /v1/orders` | orders where the caller is buyer or seller | `{ "orders": [...] }`, each order with its embedded `payment` projection plus the `shipment`, `return_request`, `dispute`, `external_refund`, and `reviews` sub-objects (client `orderSchema` field names) |
| `GET /v1/orders/{id}` | participants; also configured moderators, but only when the order is under (or was previously under) dispute | one order with the same embedded projections |
| `GET /v1/orders/{id}/evidence` | the two dispute participants and configured moderators (moderators only for orders under, or previously under, dispute) | `{ "order_id", "evidence": [...] }` — the dispute case file: each item's `id`, `submitter_pubky`, `body`, `body_bytes`, `created_at` |
| `GET /v1/disputes` | configured moderators only; everyone else is refused 403, never handed `[]` | `{ "disputes": [...] }` — the adjudication queue: the order projection of every order under (or previously under) dispute |
| `GET /v1/payments/{id}` | participants only | one payment projection |
| `GET /v1/receipts/{id}` | issuer (seller) and recipient (buyer) only | one receipt: ids, participants, `total`, `content_hash`, `issued_at` (client `receiptSchema` shape) |
| `GET /v1/notifications` | the recipient only | `{ "notifications": [...] }` — each with an optional `amount` (money JSON, null on rows delivered before amounts existed): present only where the recipient already sees the figure in a role-scoped projection — the offer amount on offer notifications, the auction's visible price on `outbid`/`auction_won`/`auction_ended` |

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
- **The Locks bundle id and lock resource** (`access credentials or
  bundle_id` in ADR-0019 §8). The former plaintext
  `payments.locks_bundle_id` column has been **removed entirely**: the
  bundle id now exists only as XChaCha20-Poly1305 ciphertext in
  `payment_locks_correlations`, is queried only through an HMAC lookup
  token, and has no serialization path — no command result, read
  projection, event, outbox intent, notification, log, or error carries it
  (asserted by the redaction test in `tests/locks_test.rs`). The lock
  resource is stored only as a BLAKE3 hash.
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
- **Notification amounts** — an optional money JSON on a delivered
  notification, carried only where the recipient already reads that exact
  figure in a role-scoped projection: the offer amount (both offer
  participants fetch the offer projection) and the auction's visible price
  (every bidder fetches the listing projection). Nothing address- or
  payment-bearing ever rides a notification payload.
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

## Background workers (tasks 3.4, 4.5)

One worker runtime (`crates/service/src/workers.rs`) drains six server-time
tasks: reservation expiry, offer expiry, auction close, the outbox, Locks
lifecycle verification, and the marketplace payment window.

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
- **Locks verification and the payment window.** The verification task
  (see "Locks verification" above) runs only when the deployment has Locks
  configured. The `payment_window` sweep always runs and covers EVERY armed
  hold window (Locks, fiat/bitcoin bind, sandbox, drop claims — see
  Inventory semantics above): a lapsed hold restocks, the payment expires,
  and the order cancels with the stored reason `payment window elapsed`.
  Both hold the same leases and carry the same crash-safety and
  at-most-once effect properties as the other tasks (payment CAS + unique
  event index).

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

The Locks verification port (task 4.5) changes the artifact again — the
client must re-vendor and its transition tables change (no state enums or
aggregate sets change):

- payment `awaiting_entitlement → confirmed` and
  `awaiting_entitlement → manual_review` gain the server trigger
  `locks_verification`;
- payment gains a new edge `expired → manual_review` via the server trigger
  `locks_late_completion`;
- order `pending_payment → paid` gains the server trigger
  `payment_confirmation` (the trigger the listing and reservation machines
  already declared);
- the payment aggregate's command list gains `payment.register_locks`.

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
| Payment projection | client `paymentSchema` requires `locks_bundle_id` | the field no longer exists: the bundle id lives only encrypted in `payment_locks_correlations` and is never serialized | ADR-0019 §8 forbids exposing `access credentials or bundle_id`. The client has made the field optional; it will never be sent. |
| Payment adapter | sandbox only (`adapter = 'sandbox'`) | `sandbox` or `locks`; `payment.register_locks` flips a payment to `locks`, which permanently refuses `payment.sandbox_advance` | ADR-0019 §7: a Locks-correlated payment advances only by independent server-side verification of a completed Locks result — never by a client claim. |
| Real payment advancement | none (the prototype had only the buyer-driven sandbox command) | server-driven: the worker verifies the Locks lifecycle (`POST /verification-task-lookups`) and confirms exactly once; the marketplace payment window expires on server time; a late completion goes to `manual_review` | ADR-0019 §7 and the upstream integration contract; Locks v1 leaves transport/status failures pending, so upstream trouble never expires a payment by itself. |
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
