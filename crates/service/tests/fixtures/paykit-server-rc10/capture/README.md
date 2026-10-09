# paykit-server rc10 `/setup/status` fixtures

`../setup_status_*.json` are exchanges captured from a running paykit-server
at tag `v0.1.0-rc10` (`7326a3f9a035d79d8b6977b7c1a5fb742327ff37`), all of
signed `POST /setup/status`. Nothing in them is written by hand. The tests
read them through `tests/common/paykit_server_rc10.rs`.

Each file holds the request as sent (`signature` is the `x-paykit-signature`
header, `body` the exact bytes), the answer (`status`, `content_type`,
`body`), `sent_at`, which of the two servers answered (`server`:
`usdt_configured` or `usdt_not_configured`), and the server revision.

## What was real

The production `Server` (router, signed-service authentication layer,
`SetupStatusService`, the creator session validator), PostgreSQL 16, a real
Pubky homeserver and HTTP relay (`pubky-testnet`), and the production
`GET /setup` and `GET /setup/reconnect` routes. Sellers were created the way
the Shop's iframe creates them: the route serves Bitkit's authorization URL,
a wallet (a Pubky account with a Paykit App Registry and Noise key
authorization) approves it with its companion claim, and the route's
completion poll finishes the setup. Requests went over TCP. No transport is
stubbed: `[usdt] rpc_url` is configured on the `usdt_configured` server but
`/setup/status` never calls it, so it points at a closed local port, and the
Electrum port reports no observations.

The sellers:

| Seller | What they approved at setup |
|---|---|
| `dual` | a Bitcoin account and a USDT address |
| `bitcoin_only` | a Bitcoin account; the USDT address was declined |
| `reconnected` | a Bitcoin account; later a reconnect that added a USDT address |
| `plain_wallet` | a Bitcoin account, set up on a deployment without `[usdt]` (no USDT permission offered) |
| `never_set_up` | a Pubky account that never ran setup with this server |

The signing key is the service test key (seed `66…66`), the only entry in
the server's `[signed_services] trusted_public_keys`; the invalid-signature
exchange uses a key that is not listed. The homeserver-down exchanges stop
the homeserver, so the seller's Pubky session cannot be read.

## What the captures show

- Authority (no `asset`): `ready` for a seller with a live setup,
  `setup_required` for one that never ran setup, `unavailable` when the
  homeserver is down.
- `asset: "USDT"` on a `[usdt]` deployment is `ready` for a Bitcoin-only
  seller who declined the USDT address (`setup_status_usdt_bitcoin_only`,
  `setup_status_usdt_before_reconnect`), and stays `ready` after a reconnect
  that adds one. rc10 (and `master` at `c351f15`) check only that `[usdt]` is
  configured and that the seller has some receiving detail
  (`application/setup_status.rs`, `status_for_asset`), not that the seller
  approved the asset asked about. The service reports the answer as it is.
- `asset: "USDT"` on a deployment without `[usdt]` is `setup_required` for a
  seller whose authority is `ready`: the only captured state where the
  service offers the reconnect action.

## How to capture again

```sh
git clone https://github.com/pubky/paykit-server && cd paykit-server
git checkout v0.1.0-rc10
git apply /path/to/pubky-marketplace-service/crates/service/tests/fixtures/paykit-server-rc10/capture/capture-harness.patch
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
CAPTURE_INPUTS=/path/to/capture/inputs.json CAPTURE_DIR=/tmp/out \
  cargo test --locked -p paykit-server-e2e --test capture_setup_status -- --nocapture --test-threads=1
```

The toolchain is the one `rust-toolchain.toml` pins (1.91.1). The harness
asserts every answer as it captures, binds the HTTP relay on port 15412 (the
relay `Server::build` dials for a testnet deployment), and answers each
status read with a server built for that read: in this harness a server that
restored a seller's credentials right after the setups answered
`setup_required` for that seller from then on, while a server built for the
read restored them on its first read. A capture run mints new random
identities, so a re-capture replaces every fixture together; commit the
whole directory and update `SERVER_REVISION` in
`tests/common/paykit_server_rc10.rs`.
