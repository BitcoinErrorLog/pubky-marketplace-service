#!/usr/bin/env bash
# Pre-push gate. Last line on success: PREPUSH OK <sha> <seconds>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Same toolchain as .github/workflows/ci.yml (dtolnay/rust-toolchain@1.89.0).
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.89.0}"

start=$(date +%s)
LOCK="/Volumes/t7/vibes-dev/.locks/heavy.lock"

run_heavy() {
  mkdir -p "$(dirname "$LOCK")"
  lockf "$LOCK" "$@"
}

# Hook stdin lists the refs being pushed. A push whose every commit message
# contains [skip ci] does not run the gate.
if [ ! -t 0 ]; then
  skip=1
  saw=0
  while read -r _local_ref local_sha _remote_ref remote_sha; do
    [ -n "${local_sha:-}" ] || continue
    saw=1
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
      continue
    fi
    if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
      range="$local_sha"
    else
      range="${remote_sha}..${local_sha}"
    fi
    if git log --format=%s "$range" | grep -qv '\[skip ci\]'; then
      skip=0
    fi
  done
  if [ "$saw" = 1 ] && [ "$skip" = 1 ]; then
    echo "prepush: every commit has [skip ci]; gate not run"
    exit 0
  fi
fi

port="${PREPUSH_PG_PORT:-55433}"
name="${PREPUSH_PG_CONTAINER:-prepush-ms-pg}"
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:${port}/postgres}"

if ! docker info >/dev/null 2>&1; then
  echo "prepush: docker is required for the scram-sha-256 Postgres" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
    -p "127.0.0.1:${port}:5432" \
    postgres:16 >/dev/null
fi

ready=0
for _ in $(seq 1 60); do
  if docker exec "$name" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "prepush: Postgres did not become ready" >&2
  exit 1
fi

encryption="$(docker exec "$name" psql -U postgres -d postgres -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
trust_hosts="$(docker exec "$name" psql -U postgres -d postgres -tAc "SELECT count(*) FROM pg_hba_file_rules WHERE type = 'host' AND auth_method = 'trust'")"
if [ "$encryption" != "scram-sha-256" ] || [ "${trust_hosts:-1}" != "0" ]; then
  echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} trust_host_rules=${trust_hosts})" >&2
  exit 1
fi

echo "prepush: cargo fmt"
cargo fmt --check

echo "prepush: cargo clippy"
run_heavy cargo clippy --workspace --all-targets -- -D warnings

echo "prepush: cargo test"
run_heavy bash scripts/ci-test.sh

sha="$(git rev-parse HEAD)"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds}"
