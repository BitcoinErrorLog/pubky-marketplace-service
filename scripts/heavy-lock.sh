#!/usr/bin/env bash
# Fair slots for pre-push work that used to share one heavy.lock.
#
# lockf without -k removes the lock file on exit, so waiters are not ordered.
# Each class has its own ticket queue. A process may run only when fewer than
# SLOTS older live tickets exist, then it holds one slot with lockf -k.
# Dead PIDs and reused PIDs are removed from the queue.
#
#   vrt    1 slot   Playwright in Docker. Browsers and /dev/shm are what
#                   exhaust the Docker VM, so this stays exclusive.
#   cargo  2 slots  Rust clippy and tests, plus their Postgres or Neo4j.
#                   Those containers are small next to a 16GB Docker VM, and
#                   the compilers run on the host.
#   node   1 slot   Host typecheck and vitest. Separate from cargo and vrt,
#                   so a Shop typecheck does not wait behind either.
#
# Classes do not exclude each other. A VRT run and two cargo runs can overlap.
# HEAVY_LOCK_DIR overrides the directory (tests). The default is the shared
# lock directory. This does not take heavy.lock; in-flight worktrees that
# still lock that file keep their old behavior until they update.

HEAVY_LOCK_DIR="${HEAVY_LOCK_DIR:-/Volumes/t7/vibes-dev/.locks}"

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source scripts/heavy-lock.sh" >&2
  exit 1
fi

heavy_slots() {
  case "$1" in
    vrt) echo 1 ;;
    cargo) echo 2 ;;
    node) echo 1 ;;
    *)
      echo "heavy-lock: unknown class: $1" >&2
      return 1
      ;;
  esac
}

etime_to_seconds() {
  local t="$1" days=0 n a b c
  t="${t#-}"
  if [[ "$t" == *-* ]]; then
    days="${t%%-*}"
    t="${t#*-}"
  fi
  n=$(printf '%s\n' "$t" | awk -F: '{print NF}')
  IFS=: read -r a b c <<< "$t"
  a=$((10#${a:-0}))
  b=$((10#${b:-0}))
  c=$((10#${c:-0}))
  days=$((10#${days:-0}))
  if [ "$n" -le 1 ]; then
    echo "$a"
  elif [ "$n" -eq 2 ]; then
    echo $((days * 86400 + a * 60 + b))
  else
    echo $((days * 86400 + a * 3600 + b * 60 + c))
  fi
}

heavy_release() {
  if [ -n "${HEAVY_TICKET:-}" ]; then
    rm -f "$HEAVY_TICKET"
    HEAVY_TICKET=""
  fi
}

heavy_issue() {
  local class="$1"
  local qdir="$HEAVY_LOCK_DIR/${class}.q"
  local seqfile="$HEAVY_LOCK_DIR/${class}.seq"
  local issue="$HEAVY_LOCK_DIR/${class}.issue.lock"
  mkdir -p "$qdir"
  lockf -k "$issue" bash -c '
    set -euo pipefail
    seqfile="$1"
    qdir="$2"
    pid="$3"
    n=0
    if [ -f "$seqfile" ]; then
      n=$(tr -cd "0-9" < "$seqfile" || true)
    fi
    [ -n "$n" ] || n=0
    n=$((n + 1))
    tmp=$(mktemp "${seqfile}.XXXXXX")
    printf "%s\n" "$n" > "$tmp"
    mv "$tmp" "$seqfile"
    name=$(printf "%020d" "$n")
    printf "%s\n" "$pid" > "$qdir/${name}.ticket"
    printf "%s\n" "$n"
  ' _ "$seqfile" "$qdir" "$$"
}

heavy_ahead() {
  local class="$1" myseq="$2"
  local dir="$HEAVY_LOCK_DIR/${class}.q"
  local ahead=0 f base seq pid etime es age now mtime
  for f in "$dir"/*.ticket; do
    [ -e "$f" ] || continue
    base=$(basename "$f")
    seq=$((10#${base%.ticket}))
    pid=$(tr -cd '0-9' < "$f" || true)
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$f"
      continue
    fi
    etime=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
    if [ -n "$etime" ]; then
      es=$(etime_to_seconds "$etime")
      mtime=$(stat -f %m "$f")
      now=$(date +%s)
      age=$((now - mtime))
      if [ "$age" -gt 3 ] && [ "$es" -lt $((age - 3)) ]; then
        rm -f "$f"
        continue
      fi
    fi
    if [ "$seq" -lt "$myseq" ]; then
      ahead=$((ahead + 1))
    fi
  done
  echo "$ahead"
}

# Run the command inside one slot. Returns 75 when every try lost the race
# and the command did not start. Otherwise returns the command status.
heavy_run_in_slot() {
  local slotfile="$1"
  local marker hold_msg slot_status=0
  shift
  hold_msg="$1"
  shift
  marker=$(mktemp "${HEAVY_LOCK_DIR}/ran.XXXXXX")
  : > "$marker"
  # `||` keeps a non-zero status under set -e. A bare assignment from $? after
  # `if` is wrong on bash 3.2: a failed if with no else reports status 0.
  lockf -k -s -t 0 "$slotfile" bash -c '
    printf 1 > "$1"
    shift
    printf "%s\n" "$1" >&2
    shift
    "$@"
  ' _ "$marker" "$hold_msg" "$@" || slot_status=$?
  if [ ! -s "$marker" ]; then
    rm -f "$marker"
    return 75
  fi
  rm -f "$marker"
  return "$slot_status"
}

run_heavy() {
  local class="$1"
  shift
  local slots seq ahead last i st
  slots=$(heavy_slots "$class") || return 1
  mkdir -p "$HEAVY_LOCK_DIR"
  seq=$(heavy_issue "$class")
  if ! [[ "$seq" =~ ^[0-9]+$ ]]; then
    echo "heavy-lock: could not take a ${class} ticket" >&2
    return 1
  fi
  HEAVY_TICKET="$HEAVY_LOCK_DIR/${class}.q/$(printf '%020d' "$seq").ticket"
  last=""
  while true; do
    ahead=$(heavy_ahead "$class" "$seq")
    if [ "$ahead" -lt "$slots" ]; then
      break
    fi
    if [ "$ahead" != "$last" ]; then
      echo "heavy-lock: waiting class=${class} ticket=${seq} ahead=${ahead} slots=${slots}" >&2
      last=$ahead
    fi
    sleep 0.25
  done
  while true; do
    i=1
    while [ "$i" -le "$slots" ]; do
      st=0
      heavy_run_in_slot "$HEAVY_LOCK_DIR/${class}.${i}.lock" \
        "heavy-lock: hold class=${class} slot=${i} ticket=${seq}" \
        "$@" || st=$?
      if [ "$st" -eq 0 ]; then
        heavy_release
        return 0
      fi
      if [ "$st" -ne 75 ]; then
        heavy_release
        return "$st"
      fi
      i=$((i + 1))
    done
    sleep 0.2
  done
}
