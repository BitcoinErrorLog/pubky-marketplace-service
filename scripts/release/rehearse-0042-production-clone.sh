#!/usr/bin/env bash
# Rehearses migration 0042 on a production-schema clone. Read-only against
# production: roles (no passwords), schema only, and the migration ledger are
# dumped through a Railway SSH tunnel and restored into a disposable local
# cluster; no production row leaves the database. The ignored test
# `production_clone_0042_test` then seeds synthetic rows in production's
# released shape, applies the pending migrations with the service migrator,
# and routes confirmed money on one of them.
#
# Required: RAILWAY_PROJECT_ID, RAILWAY_ENVIRONMENT, RAILWAY_DATABASE_SERVICE
# (exact IDs). Optional: MARKETPLACE_PG_BIN (local PostgreSQL bin directory
# matching production's major), CARGO_TARGET_DIR.
set -Eeuo pipefail
umask 077
export LC_ALL=C

readonly EXPECTED_SOURCE_VERSIONS="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39"
readonly CLONE_DATABASE="marketplace_production_clone"
# Production grants name `postgres` as grantor, so the clone bootstraps as it.
readonly CLONE_BOOTSTRAP_USER="postgres"

: "${RAILWAY_PROJECT_ID:?RAILWAY_PROJECT_ID is required}"
: "${RAILWAY_ENVIRONMENT:?RAILWAY_ENVIRONMENT is required}"
: "${RAILWAY_DATABASE_SERVICE:?RAILWAY_DATABASE_SERVICE is required}"

pg_bin="${MARKETPLACE_PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}"
export PATH="$pg_bin:$PATH"
for tool in railway python3 psql pg_dump pg_dumpall pg_restore initdb pg_ctl cargo; do
  command -v "$tool" >/dev/null || {
    printf 'required tool is unavailable: %s\n' "$tool" >&2
    exit 1
  }
done

root="$(cd "$(dirname "$0")/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/marketplace-production-clone.XXXXXX")"
tunnel_pid=""
cluster_started=false

stop_process_tree() {
  local parent_pid=$1 child_pid
  while read -r child_pid; do
    [[ -n "$child_pid" ]] && stop_process_tree "$child_pid"
  done < <(pgrep -P "$parent_pid" 2>/dev/null || true)
  kill "$parent_pid" 2>/dev/null || true
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "$tunnel_pid" ]] && kill -0 "$tunnel_pid" 2>/dev/null; then
    stop_process_tree "$tunnel_pid"
    wait "$tunnel_pid" 2>/dev/null || true
  fi
  if [[ "$cluster_started" == true ]]; then
    pg_ctl -D "$scratch/pgdata" -m immediate -w stop >/dev/null 2>&1 || true
  fi
  rm -rf "$scratch"
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

tunnel_log="$scratch/railway-tunnel.log"
: >"$tunnel_log"
railway connect "$RAILWAY_DATABASE_SERVICE" \
  --project "$RAILWAY_PROJECT_ID" \
  --environment "$RAILWAY_ENVIRONMENT" \
  --tunnel-only >"$tunnel_log" 2>&1 &
tunnel_pid=$!

for _ in {1..120}; do
  if ! kill -0 "$tunnel_pid" 2>/dev/null; then
    printf 'Railway SSH database tunnel exited before becoming ready\n' >&2
    exit 1
  fi
  if python3 - "$tunnel_log" "$scratch" <<'PY'
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", Path(sys.argv[1]).read_text(errors="replace"))
match = re.search(r"postgres(?:ql)?://[^\s]+", text)
if match is None:
    raise SystemExit(1)
url = urlsplit(match.group(0).rstrip(".,;"))
if not all((url.hostname, url.port, url.username, url.path.lstrip("/"))):
    raise SystemExit(1)
out = Path(sys.argv[2])
for name, value in {
    "source-host": url.hostname,
    "source-port": str(url.port),
    "source-user": unquote(url.username),
    "source-database": unquote(url.path.lstrip("/")),
}.items():
    (out / name).write_text(value)
esc = lambda value: value.replace("\\", "\\\\").replace(":", "\\:")
(out / "source.pgpass").write_text(":".join((
    esc(url.hostname), str(url.port), esc(unquote(url.path.lstrip("/"))),
    esc(unquote(url.username)), esc(unquote(url.password or "")),
)) + "\n")
PY
  then
    break
  fi
  sleep 0.5
done
[[ -f "$scratch/source-database" ]] || {
  printf 'Railway SSH database tunnel did not expose parseable connection details\n' >&2
  exit 1
}

source_host="$(<"$scratch/source-host")"
source_port="$(<"$scratch/source-port")"
source_user="$(<"$scratch/source-user")"
source_database="$(<"$scratch/source-database")"
chmod 0600 "$scratch/source.pgpass"
export PGPASSFILE="$scratch/source.pgpass"
source_psql() {
  psql -X -A -t -v ON_ERROR_STOP=1 --host "$source_host" --port "$source_port" \
    --username "$source_user" --dbname "$source_database" --command "$1"
}

source_major=$(( $(source_psql "SHOW server_version_num") / 10000 ))
for tool in pg_dump initdb; do
  major="$("$tool" --version | awk '{split($3, v, "."); print v[1]}')"
  if [[ "$major" != "$source_major" ]]; then
    printf '%s major %s does not match production PostgreSQL major %s\n' "$tool" "$major" "$source_major" >&2
    exit 1
  fi
done

source_versions="$(source_psql "SELECT string_agg(version::text, ',' ORDER BY version) || '|' || bool_and(success)::text FROM public._sqlx_migrations")"
if [[ "$source_versions" != "${EXPECTED_SOURCE_VERSIONS}|true" ]]; then
  printf 'production is not exactly successful migrations 1..39; refusing rehearsal\n' >&2
  exit 1
fi
printf 'production shape (count of released attempts 0042 must backfill): %s\n' \
  "$(source_psql "SELECT count(*) FROM orders WHERE paykit_activation_state = 'voided' AND paykit_request_state IS NULL AND paykit_invoice_id IS NOT NULL AND paykit_stack_id IS NOT NULL AND paykit_stack_endpoint IS NOT NULL AND paykit_total_sats > 0")"

pg_dumpall --host "$source_host" --port "$source_port" --username "$source_user" \
  --database "$source_database" --roles-only --no-role-passwords >"$scratch/roles.sql"
pg_dump --host "$source_host" --port "$source_port" --username "$source_user" \
  --dbname "$source_database" --format=custom --schema-only --file "$scratch/schema.dump"
pg_dump --host "$source_host" --port "$source_port" --username "$source_user" \
  --dbname "$source_database" --format=custom --data-only \
  --table public._sqlx_migrations --file "$scratch/ledger.dump"
unset PGPASSFILE

if pg_restore --list "$scratch/schema.dump" |
  awk '$4 == "BLOB" || ($4 == "TABLE" && $5 == "DATA") || ($4 == "SEQUENCE" && $5 == "SET") { found = 1 } END { exit !found }'
then
  printf 'schema dump unexpectedly contains table data; refusing restore\n' >&2
  exit 1
fi
if [[ "$(pg_restore --list "$scratch/ledger.dump" | awk '$4 == "TABLE" && $5 == "DATA" { print $6 "." $7 }')" != "public._sqlx_migrations" ]]; then
  printf 'ledger dump contains an object other than public._sqlx_migrations; refusing restore\n' >&2
  exit 1
fi

initdb -D "$scratch/pgdata" --username="$CLONE_BOOTSTRAP_USER" \
  --auth-local=trust --auth-host=reject --no-instructions >"$scratch/initdb.log"
clone_port=5432
pg_ctl -D "$scratch/pgdata" -l "$scratch/postgres.log" -w start \
  -o "-h '' -k $scratch -p $clone_port" >/dev/null
cluster_started=true
clone_psql() {
  psql -X -v ON_ERROR_STOP=1 --host "$scratch" --port "$clone_port" \
    --username "$CLONE_BOOTSTRAP_USER" "$@"
}
# The role replay repeats the bootstrap superuser (`already exists`); any
# other error stops the rehearsal.
psql -X --host "$scratch" --port "$clone_port" --username "$CLONE_BOOTSTRAP_USER" \
  --dbname postgres --file "$scratch/roles.sql" >/dev/null 2>"$scratch/roles.err"
if grep 'ERROR' "$scratch/roles.err" | grep -v 'already exists' | grep -q .; then
  cat "$scratch/roles.err" >&2
  exit 1
fi
clone_psql --dbname postgres --command "CREATE DATABASE $CLONE_DATABASE" >/dev/null
pg_restore --exit-on-error --host "$scratch" --port "$clone_port" \
  --username "$CLONE_BOOTSTRAP_USER" --dbname "$CLONE_DATABASE" "$scratch/schema.dump"
pg_restore --exit-on-error --host "$scratch" --port "$clone_port" \
  --username "$CLONE_BOOTSTRAP_USER" --dbname "$CLONE_DATABASE" "$scratch/ledger.dump"

encoded_socket="$(python3 -c 'from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=""))' "$scratch")"
clone_url="postgresql://${CLONE_BOOTSTRAP_USER}@localhost/${CLONE_DATABASE}?host=${encoded_socket}&port=${clone_port}"
(
  cd "$root"
  MARKETPLACE_CLONE_DATABASE_URL="$clone_url" DATABASE_URL="$clone_url" \
    cargo test -p marketplace-service --test production_clone_0042_test \
    -- --ignored --exact --nocapture \
    migration_0042_on_a_production_schema_clone_recovers_released_attempts
)

printf 'production-schema clone rehearsal of 0042 passed without production mutation\n'
