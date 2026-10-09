# paykit-server `/setup/status` fixtures (with `accepted_asset`)

`../setup_status_*.json` are exchanges captured from a running paykit-server
from [pubky/paykit-server#70](https://github.com/pubky/paykit-server/pull/70)
at head `69222059ee30b44379a85521106859a697b70811` (draft): three commits on
`v0.1.0-rc11` (`662dca0619a9aa2962bcd677bd5ddd4563cd2784`) that add
`accepted_asset` to signed `POST /setup/status`. One capture,
`setup_status_accepted_asset_pre_u4`, is from plain `v0.1.0-rc11` with no U4:
how a server without the field refuses it. Nothing in them is written by
hand. The tests read them through `tests/common/paykit_server_u4.rs`.

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

- Authority (no asset field): `ready` for a seller with a live setup,
  `setup_required` for one that never ran setup, `unavailable` when the
  homeserver is down.
- `asset: "BTC"` (the denomination) is unchanged by U4.
- `accepted_asset: "USDT"` on a `[usdt]` deployment: `ready` for a seller who
  approved a USDT address, `setup_required` for a Bitcoin-only seller who
  declined it (and before a reconnect), `ready` after a reconnect that adds
  one. Without `[usdt]` it is `setup_required`.
- `accepted_asset` values other than `BTC` and `USDT` (`"USD"`, `"btc"`) are
  `400 invalid_request`, and so is `accepted_asset` on a server without U4.

rc10 and rc11 (before U4) answered `ready` to `asset: "USDT"` for a Bitcoin-only
seller who declined the USDT address; this directory replaces the rc10
captures that showed it.

## How to capture again

```sh
git clone https://github.com/pubky/paykit-server && cd paykit-server
git fetch origin pull/70/head:pr70 && git checkout 69222059ee30b44379a85521106859a697b70811   # #70's head
git apply /path/to/pubky-marketplace-service/crates/service/tests/fixtures/paykit-server-u4/capture/capture-harness.patch
CAPTURE_SERVER_REVISION='pubky/paykit-server#70 at 69222059ee30b44379a85521106859a697b70811 (v0.1.0-rc11 plus 3 commits)' \
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
CAPTURE_INPUTS=/path/to/capture/inputs.json CAPTURE_DIR=/tmp/out \
  cargo test --locked -p paykit-server-e2e --test capture_setup_status -- --nocapture --test-threads=1
```

For the pre-U4 refusal, run the same harness on plain `v0.1.0-rc11` with `CAPTURE_PRE_U4=1` and `CAPTURE_SERVER_REVISION='662dca0619a9aa2962bcd677bd5ddd4563cd2784 (v0.1.0-rc11, no U4)'`.

The toolchain is the one `rust-toolchain.toml` pins (1.91.1). The harness
asserts every answer as it captures, binds the HTTP relay on port 15412 (the
relay `Server::build` dials for a testnet deployment), and answers each
status read with a server built for that read: in this harness a server that
restored a seller's credentials right after the setups answered
`setup_required` for that seller from then on, while a server built for the
read restored them on its first read. A capture run mints new random
identities, so a re-capture replaces every fixture together; commit the
whole directory and update the revisions in
`tests/common/paykit_server_u4.rs`.
