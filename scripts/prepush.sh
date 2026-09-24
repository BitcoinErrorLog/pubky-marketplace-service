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
    # grep -q under pipefail exits 141 when git log is still writing,
    # and the gate then skips every push. Read the subjects first.
    subjects="$(git log --format=%s "$range" || true)"
    if printf '%s\n' "$subjects" | grep -v '\[skip ci\]' >/dev/null; then
      skip=0
    fi
  done
  if [ "$saw" = 1 ] && [ "$skip" = 1 ]; then
    echo "prepush: every commit has [skip ci]; gate not run"
    exit 0
  fi
fi

# Sibling worktrees share one Cargo target unless this gate overrides it.
# A shared target can run another tree's test binary. This checkout gets its own.
shared_target="${CARGO_TARGET_DIR:-}"
private_target="/Volumes/t7/vibes-dev/.cargo-target/marketplace-service/$(basename "$ROOT")"
if [ ! -d "$private_target" ] && [ -n "$shared_target" ] && [ -d "$shared_target" ] && [ "$shared_target" != "$private_target" ]; then
  seed="${private_target}.partial"
  rm -rf "$seed"
  mkdir -p "$(dirname "$private_target")"
  if cp -cR "$shared_target" "$seed" 2>/dev/null || cp -R "$shared_target" "$seed"; then
    mv "$seed" "$private_target"
  else
    rm -rf "$seed"
    mkdir -p "$private_target"
  fi
fi
mkdir -p "$private_target"
export CARGO_TARGET_DIR="$private_target"

echo "prepush: cargo fmt"
cargo fmt --check

# One container per worktree. A shared name let this gate remove a Postgres
# container another lane's tests were using, because the name check raced
# the start. This gate does not remove containers.
worktree_slug="$(printf '%s' "$(basename "$ROOT")" | tr -c 'A-Za-z0-9_.-' '-')"
name="${PREPUSH_PG_CONTAINER:-prepush-ms-pg-${worktree_slug}}"
owner_label="pubky.prepush.worktree"
if [ -n "${PREPUSH_PG_PORT:-}" ]; then
  port="$PREPUSH_PG_PORT"
else
  sum="$(printf '%s' "$ROOT" | cksum | awk '{print $1}')"
  port="$((56000 + sum % 1000))"
fi

if ! docker info >/dev/null 2>&1; then
  echo "prepush: docker is required for the scram-sha-256 Postgres" >&2
  exit 1
fi

container_names() {
  docker ps -a --format '{{.Names}}'
}

if container_names | grep -qx "$name"; then
  owner="$(docker inspect -f "{{index .Config.Labels \"${owner_label}\"}}" "$name")"
  if [ "$owner" != "$ROOT" ]; then
    echo "prepush: container ${name} exists and this gate did not create it" >&2
    exit 1
  fi
  if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
    docker start "$name" >/dev/null
  fi
  port="$(docker inspect -f '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$name")"
else
  docker run -d --name "$name" \
    --label "${owner_label}=${ROOT}" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
    -p "127.0.0.1:${port}:5432" \
    postgres:16 >/dev/null
fi

export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${port}/postgres"

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

encryption="$(docker exec "$name" psql -h 127.0.0.1 -U postgres -d postgres -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
host_all="$(docker exec "$name" psql -h 127.0.0.1 -U postgres -d postgres -tAc "SELECT auth_method FROM pg_hba_file_rules WHERE type = 'host' AND address = 'all'" | tr -d '[:space:]')"
# The official image keeps trust on 127.0.0.1/::1. Published-port clients match address "all".
if [ "$encryption" != "scram-sha-256" ] || [ "$host_all" != "scram-sha-256" ]; then
  echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} host_all=${host_all})" >&2
  exit 1
fi

echo "prepush: cargo clippy"
run_heavy cargo clippy --workspace --all-targets -- -D warnings

echo "prepush: cargo test"
run_heavy bash scripts/ci-test.sh

sha="$(git rev-parse HEAD)"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds}"
