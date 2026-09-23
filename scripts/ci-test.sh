#!/usr/bin/env bash
# Workspace tests with database-per-test parallelism.
#
# #[sqlx::test] creates one database per test. Migrations of those databases
# take a cluster-wide session lock in TEST_MIGRATOR so 0032's fail-closed
# CONNECTION LIMIT check cannot race. The test bodies below still touch
# cluster-global roles (fixed CREATE/ALTER ROLE, shared login passwords,
# CONNECTION LIMIT). They run afterwards, one test at a time.
#
# --jobs 1 keeps a single test binary's pools open. Each #[sqlx::test] pool
# defaults to 10 connections and Postgres max_connections is 100.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Advisory locks are per-database, so the test migrator's lock does not
# serialize CREATE/ALTER ROLE across sqlx::test databases. One thread keeps
# those cluster-wide catalog updates from racing.
THREADS="${PREPUSH_TEST_THREADS:-1}"

# Integration-test binaries that mutate cluster-global roles.
SERIAL=(
  refusal_audit_test
  migration_0033_test
  migration_0035_test
  migration_0037_test
  rolling_overlap_test
  manual_review_resolve_test
)

is_serial() {
  local name="$1" item
  for item in "${SERIAL[@]}"; do
    if [ "$item" = "$name" ]; then
      return 0
    fi
  done
  return 1
}

echo "ci-test: lib and bins, ${THREADS} threads"
cargo test --workspace --jobs 1 --lib --bins --no-fail-fast -- --test-threads="$THREADS"

parallel_args=()
for file in crates/service/tests/*.rs; do
  name="$(basename "$file" .rs)"
  if is_serial "$name"; then
    continue
  fi
  parallel_args+=(--test "$name")
done

echo "ci-test: parallel integration tests (${#parallel_args[@]} binaries, ${THREADS} threads)"
cargo test -p marketplace-service --jobs 1 "${parallel_args[@]}" --no-fail-fast -- --test-threads="$THREADS"

serial_args=()
for name in "${SERIAL[@]}"; do
  serial_args+=(--test "$name")
done

echo "ci-test: serial role tests (${#SERIAL[@]} binaries, 1 thread)"
echo "  ${SERIAL[*]}"
cargo test -p marketplace-service --jobs 1 "${serial_args[@]}" --no-fail-fast -- --test-threads=1
