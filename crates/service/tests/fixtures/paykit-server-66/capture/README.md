# paykit-server #66 prepare fixtures

`../prepare_*.json` are exchanges captured from a running paykit-server at
[`pubky/paykit-server` #66](https://github.com/pubky/paykit-server/pull/66)
(`POST /marketplace/payment-requests/prepare`), head
`f9079d50424f31ff0a7ca3df3a12ddc43c398ea8`. Nothing in them is written by
hand. The tests read them through `tests/common/paykit_server_66.rs`.

Each file holds the request as sent (`signature` is the `x-paykit-signature`
header, `body` the exact bytes), the answer (`status`, `content_type`,
`body`), `sent_at` (this machine's clock just before the request, so the
15-minute default activation deadline can be checked against
`prepare_expires_at`, which is on the server's database clock), and the
server revision.

## What was real

The production `Server` (router, signed-service authentication layer,
readiness gating, workers), PostgreSQL 16, the `MarketplacePreparationStore`
with its migrations, the creator's session validated against a real Pubky
homeserver (`pubky-testnet`), Paykit App Registry discovery against that
homeserver, and the creator's Bitcoin receiving credentials from the
`CreatorStore`. Requests went over TCP. The capture ran one server with the
default `marketplace_prepare_ttl` (15 minutes) and a second with
`marketplace_prepare_ttl = "1s"` for the replay after expiry.

The two `prepare_*deadline_exceeded` exchanges are the one composed capture:
the production `PrepareMarketplaceService`, route, signed-service layer and
`MarketplacePreparationStore`, with a 2 second request deadline (the server
fixes 15) and a store that holds a new preparation 3 seconds after it
committed, so the answer is `503 dependency_timeout` for a commit that is
durable. The creator session and the Reader's registry answer "valid" and
"capable" there, because the server keeps those validators private and they
are not what is under test. The exact retry then replays the committed
preparation.

The signing key is the service test key (seed `66…66`), listed in the
server's `[signed_services] trusted_public_keys`, plus a second key that is
not, for `prepare_invalid_signature`. The attempt identities (`operation_id`,
`reference`) come from `inputs.json`, which `payment_attempt` derived; a test
fails if the derivation stops reproducing them.

## How to capture again

```sh
git clone https://github.com/pubky/paykit-server && cd paykit-server
git checkout f9079d50424f31ff0a7ca3df3a12ddc43c398ea8
git apply /path/to/pubky-marketplace-service/crates/service/tests/fixtures/paykit-server-66/capture/capture-harness.patch
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
CAPTURE_INPUTS=/path/to/capture/inputs.json CAPTURE_DIR=/tmp/out \
  cargo test --locked -p paykit-server-e2e --test capture_marketplace_prepare -- --nocapture --test-threads=1
```

The toolchain is the one `rust-toolchain.toml` pins (1.91.1). The harness
asserts the contract as it captures (a replay answers the stored body, a
changed binding is a `409`, a new operation id is a new invoice).

A capture run mints new random identities and invoice ids, so a re-capture
replaces every fixture together; commit the whole directory and update
`SERVER_REVISION` in `tests/common/paykit_server_66.rs`.
