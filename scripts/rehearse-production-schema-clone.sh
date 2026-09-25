#!/usr/bin/env bash
# Rehearses pending migrations against a disposable local clone of the
# production schema: roles (no passwords), schema, and the _sqlx_migrations
# ledger only — never domain rows. Production is read, never written.
#
# Usage (exact production project and environment IDs; `railway connect`
# takes the database service by name):
#   RAILWAY_PROJECT_ID=75faa4fe-466c-4277-977f-1d8e4e31df8c \
#   RAILWAY_ENVIRONMENT=404919ad-fb95-4621-9f45-b7f993dfa8ae \
#   RAILWAY_DATABASE_SERVICE=Postgres \
#   EXPECTED_SOURCE_VERSIONS=1,...,39,42 EXPECTED_CLONE_VERSIONS=1,...,44 \
#   scripts/rehearse-production-schema-clone.sh
set -Eeuo pipefail
umask 077
export LC_ALL=C

readonly CLONE_DATABASE="marketplace_production_clone"

: "${RAILWAY_PROJECT_ID:?RAILWAY_PROJECT_ID is required}"
: "${RAILWAY_ENVIRONMENT:?RAILWAY_ENVIRONMENT is required}"
: "${RAILWAY_DATABASE_SERVICE:?RAILWAY_DATABASE_SERVICE is required}"
: "${EXPECTED_SOURCE_VERSIONS:?EXPECTED_SOURCE_VERSIONS is required (comma-separated)}"
: "${EXPECTED_CLONE_VERSIONS:?EXPECTED_CLONE_VERSIONS is required (comma-separated)}"

pg_client_bin="${MARKETPLACE_PG_CLIENT_BIN:-/opt/homebrew/opt/libpq/bin}"
pg_server_bin="${MARKETPLACE_PG_SERVER_BIN:-/opt/homebrew/opt/postgresql@18/bin}"
export PATH="$pg_server_bin:$pg_client_bin:$PATH"

for tool in railway python3 psql pg_dump pg_dumpall pg_restore initdb pg_ctl cargo; do
  command -v "$tool" >/dev/null || {
    printf 'required tool is unavailable: %s\n' "$tool" >&2
    exit 1
  }
done

scratch="$(mktemp -d "${TMPDIR:-/tmp}/marketplace-production-clone.XXXXXX")"
tunnel_pid=""
cluster_started=false

stop_process_tree() {
  local parent_pid=$1
  local child_pid
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
chmod 0600 "$tunnel_log"
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

text = Path(sys.argv[1]).read_text(errors="replace")
output_dir = Path(sys.argv[2])
text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
match = re.search(r"postgres(?:ql)?://[^\s]+", text)
if match is None:
    raise SystemExit(1)
url = urlsplit(match.group(0).rstrip(".,;"))
if not all((url.hostname, url.port, url.username, url.path.lstrip("/"))):
    raise SystemExit(1)
parts = {
    "source-host": url.hostname,
    "source-port": str(url.port),
    "source-user": unquote(url.username),
    "source-database": unquote(url.path.lstrip("/")),
}
for name, value in parts.items():
    (output_dir / name).write_text(value)
def pgpass_escape(value):
    return value.replace("\\", "\\\\").replace(":", "\\:")
(output_dir / "source.pgpass").write_text(":".join((
    pgpass_escape(url.hostname),
    str(url.port),
    pgpass_escape(unquote(url.path.lstrip("/"))),
    pgpass_escape(unquote(url.username)),
    pgpass_escape(unquote(url.password or "")),
)) + "\n")
PY
  then
    break
  fi
  sleep 0.5
done
if [[ ! -f "$scratch/source-database" ]]; then
  printf 'Railway SSH database tunnel did not expose parseable connection details\n' >&2
  exit 1
fi

source_host="$(<"$scratch/source-host")"
source_port="$(<"$scratch/source-port")"
source_user="$(<"$scratch/source-user")"
source_database="$(<"$scratch/source-database")"
chmod 0600 "$scratch/source.pgpass"
export PGPASSFILE="$scratch/source.pgpass"
source_psql=(psql -X -A -t -v ON_ERROR_STOP=1 --host "$source_host" --port "$source_port"
  --username "$source_user" --dbname "$source_database")

source_server_major=$(( $("${source_psql[@]}" --command "SHOW server_version_num") / 10000 ))
dump_client_major="$(pg_dump --version | awk '{split($3, version, "."); print version[1]}')"
scratch_server_major="$(initdb --version | awk '{split($3, version, "."); print version[1]}')"
if [[ "$dump_client_major" != "$source_server_major" || "$scratch_server_major" != "$source_server_major" ]]; then
  printf 'pg_dump %s / initdb %s do not match production PostgreSQL major %s\n' \
    "$dump_client_major" "$scratch_server_major" "$source_server_major" >&2
  exit 1
fi

ledger_query="SELECT string_agg(version::text, ',' ORDER BY version) || '|' || bool_and(success)::text
               FROM public._sqlx_migrations"
source_versions="$("${source_psql[@]}" --command "$ledger_query")"
if [[ "$source_versions" != "${EXPECTED_SOURCE_VERSIONS}|true" ]]; then
  printf 'production ledger is %s, not %s|true; refusing rehearsal\n' \
    "$source_versions" "$EXPECTED_SOURCE_VERSIONS" >&2
  exit 1
fi
printf 'production ledger: %s\n' "$source_versions"

roles_dump="$scratch/roles.sql"
schema_dump="$scratch/schema.dump"
migration_data_dump="$scratch/migration-data.dump"
pg_dumpall --host "$source_host" --port "$source_port" --username "$source_user" \
  --database "$source_database" --roles-only --no-role-passwords >"$roles_dump"
pg_dump --host "$source_host" --port "$source_port" --username "$source_user" \
  --dbname "$source_database" --format=custom --schema-only --file "$schema_dump"
pg_dump --host "$source_host" --port "$source_port" --username "$source_user" \
  --dbname "$source_database" --format=custom --data-only \
  --table public._sqlx_migrations --file "$migration_data_dump"
unset PGPASSFILE
stop_process_tree "$tunnel_pid"
wait "$tunnel_pid" 2>/dev/null || true
tunnel_pid=""

if pg_restore --list "$schema_dump" |
  awk '$4 == "BLOB" || ($4 == "TABLE" && $5 == "DATA") || ($4 == "SEQUENCE" && $5 == "SET") { found = 1 }
       END { exit !found }'
then
  printf 'schema dump unexpectedly contains table data; refusing restore\n' >&2
  exit 1
fi
dumped_data_objects="$(pg_restore --list "$migration_data_dump" |
  awk '$4 == "TABLE" && $5 == "DATA" { print $6 "." $7 }')"
if [[ "$dumped_data_objects" != "public._sqlx_migrations" ]]; then
  printf 'data dump contains an object other than public._sqlx_migrations; refusing restore\n' >&2
  exit 1
fi

# The production owner role is the clone's bootstrap superuser, so the dump's
# `GRANT ... GRANTED BY <owner>` memberships restore; its CREATE ROLE is dropped.
CLONE_BOOTSTRAP_USER="$source_user"
python3 - "$roles_dump" "$source_user" <<'PY'
import sys
from pathlib import Path
path, owner = Path(sys.argv[1]), sys.argv[2]
lines = path.read_text().splitlines(keepends=True)
drop = {f"CREATE ROLE {owner};\n", f'CREATE ROLE "{owner}";\n'}
path.write_text("".join(line for line in lines if line not in drop))
PY
initdb -D "$scratch/pgdata" --username="$CLONE_BOOTSTRAP_USER" \
  --auth-local=trust --auth-host=reject --no-instructions >"$scratch/initdb.log"
clone_port=5432
pg_ctl -D "$scratch/pgdata" -l "$scratch/postgres.log" -w start \
  -o "-h '' -k $scratch -p $clone_port" >/dev/null
cluster_started=true

clone_psql=(psql -X -v ON_ERROR_STOP=1 --host "$scratch" --port "$clone_port")
"${clone_psql[@]}" --username "$CLONE_BOOTSTRAP_USER" --dbname postgres --file "$roles_dump" >/dev/null
"${clone_psql[@]}" --username "$CLONE_BOOTSTRAP_USER" --dbname postgres \
  --command "CREATE DATABASE $CLONE_DATABASE OWNER $source_user" >/dev/null
pg_restore --exit-on-error --host "$scratch" --port "$clone_port" \
  --username "$CLONE_BOOTSTRAP_USER" --dbname "$CLONE_DATABASE" "$schema_dump"
pg_restore --exit-on-error --host "$scratch" --port "$clone_port" \
  --username "$CLONE_BOOTSTRAP_USER" --dbname "$CLONE_DATABASE" "$migration_data_dump"

encoded_socket="$(python3 -c 'from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=""))' "$scratch")"
MARKETPLACE_CLONE_DATABASE_URL="postgresql://${source_user}@localhost/${CLONE_DATABASE}?host=${encoded_socket}&port=${clone_port}" \
  cargo test --locked -p marketplace-service --test production_clone_test \
    production_schema_clone_applies_pending_migrations -- --ignored --exact --nocapture

clone_versions="$(psql -X -A -t -v ON_ERROR_STOP=1 --host "$scratch" --port "$clone_port" \
  --username "$source_user" --dbname "$CLONE_DATABASE" --command "$ledger_query")"
if [[ "$clone_versions" != "${EXPECTED_CLONE_VERSIONS}|true" ]]; then
  printf 'clone ledger is %s, not %s|true\n' "$clone_versions" "$EXPECTED_CLONE_VERSIONS" >&2
  exit 1
fi
printf 'clone ledger: %s\n' "$clone_versions"
printf 'production-schema clone rehearsal passed without production mutation\n'
