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
                   Pubky challenge–response auth, command executor with
                   idempotency, command handlers, reservation expiry worker.
contracts/         state-machines.json — the machine-readable state machine
                   contract emitted from crates/domain.
docker-compose.yml Dev/test PostgreSQL 17 on port 55432.
```

## Setup

Requirements: Rust (1.85+), Docker.

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
| `AUTH_CHALLENGE_TTL_SECONDS` | `120` | auth challenge lifetime |
| `AUTH_SESSION_TTL_SECONDS` | `86400` | session token lifetime |
| `RESERVATION_SWEEP_INTERVAL_SECONDS` | `10` | expiry worker interval |

Endpoints: `GET /health`, `GET /ready` (checks the database),
`POST /v1/auth/challenges`, `POST /v1/auth/sessions`,
`POST /v1/commands` (Bearer session required).

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

Commands implemented in this vertical slice: `listing.register`,
`inventory.reserve`, `checkout.create`, plus server-time reservation expiry
(a background sweep, not a client command). All other command kinds are
rejected by the envelope contract until they are ported with their tests.

## Authentication (task 3.2)

Challenge–response against the actor's Pubky (ed25519, z-base-32):

1. `POST /v1/auth/challenges` `{ "pubky": "<52-char z-base-32>" }` returns a
   random 32-byte nonce (base64url), bound to that pubky, single-use, short
   TTL (120 s).
2. The client signs `"pubky-marketplace-transaction-service:auth:v1\n" || nonce`
   with its ed25519 key (domain separation binds the signature to this
   service and protocol version).
3. `POST /v1/auth/sessions` `{ pubky, challenge_id, signature }` verifies the
   signature against the z-base-32-decoded public key (`ed25519-dalek`
   `verify_strict`) and returns an opaque 32-byte session token. Only the
   SHA-256 of the token is stored.
4. `POST /v1/commands` requires `Authorization: Bearer <token>`; middleware
   resolves the actor pubky from the stored hash. There are no trust-me
   actor headers, and body fields can never select a different actor.

CORS is restricted to the exact origins in `ALLOWED_ORIGINS`.

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
auction, order, and payment live in one module:
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
- The reservation expiry worker runs in-process; sweeps are guarded with
  `FOR UPDATE SKIP LOCKED` and are safe to run concurrently.
