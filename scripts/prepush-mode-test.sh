#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo and docker are
# stand-ins on PATH that log their arguments; nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

mkdir -p "$WORK/bin"
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/cargo"
EOF
cat > "$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/docker"
case "$*" in
  *password_encryption*|*pg_hba_file_rules*) echo scram-sha-256 ;;
esac
exit 0
EOF
chmod +x "$WORK/bin/cargo" "$WORK/bin/docker"

manifest() {
  mkdir -p "$1"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\n' "$2" > "$1/Cargo.toml"
}

git init -q "$WORK/ws"
cd "$WORK/ws"
git config user.email test@example.invalid
git config user.name test
printf '[workspace]\nmembers = ["crates/domain", "crates/service"]\n' > Cargo.toml
printf '# lock\n' > Cargo.lock
manifest crates/domain marketplace-domain
manifest crates/service marketplace-service
mkdir -p crates/domain/src crates/service/src crates/service/tests docs contracts scripts
printf '// lib\n' > crates/domain/src/lib.rs
printf '// lib\n' > crates/service/src/lib.rs
printf '// t\n' > crates/service/tests/orders_test.rs
printf '// t\n' > crates/service/tests/refusal_audit_test.rs
printf 'doc\n' > docs/readme.md
printf '{}\n' > contracts/endpoints.json
cp "$DIR/prepush.sh" "$DIR/prepush-stamp.sh" "$DIR/prepush-packages.sh" \
  "$DIR/heavy-lock.sh" "$DIR/ci-test.sh" scripts/
git add -A
git commit -qm base
BASE="$(git rev-parse HEAD)"

# shellcheck source=prepush-packages.sh
source "$DIR/prepush-packages.sh"
pkgs() {
  prepush_changed_packages "$BASE" Cargo.toml Cargo.lock 'contracts/*' | tr '\n' ' ' | sed 's/ $//'
}
reset_ws() {
  git reset -q --hard "$BASE"
  git clean -qfd
}

echo "test: packages: no change selects nothing"
[ -z "$(pkgs)" ] || fail "clean tree: $(pkgs)"

echo "test: packages: a committed change selects its package"
printf '// x\n' >> crates/domain/src/lib.rs
git commit -qam domain
[ "$(pkgs)" = marketplace-domain ] || fail "domain commit: $(pkgs)"
reset_ws

echo "test: packages: staged, unstaged and untracked files count"
printf '// x\n' >> crates/domain/src/lib.rs
git add crates/domain/src/lib.rs
[ "$(pkgs)" = marketplace-domain ] || fail "staged: $(pkgs)"
reset_ws
printf '// x\n' >> crates/service/src/lib.rs
[ "$(pkgs)" = marketplace-service ] || fail "unstaged: $(pkgs)"
reset_ws
printf '// new\n' > crates/service/tests/new_test.rs
[ "$(pkgs)" = marketplace-service ] || fail "untracked: $(pkgs)"
reset_ws

echo "test: packages: deletes and both sides of a rename count"
git rm -q crates/domain/src/lib.rs
[ "$(pkgs)" = marketplace-domain ] || fail "delete: $(pkgs)"
reset_ws
git mv crates/domain/src/lib.rs crates/service/src/moved.rs
[ "$(pkgs)" = "marketplace-domain marketplace-service" ] || fail "rename: $(pkgs)"
reset_ws

echo "test: packages: a file outside every package selects nothing"
printf 'more\n' >> docs/readme.md
printf 'echo\n' >> scripts/prepush.sh
[ -z "$(pkgs)" ] || fail "docs and scripts: $(pkgs)"
reset_ws

echo "test: packages: a workspace-wide file selects --workspace"
for file in Cargo.toml Cargo.lock contracts/endpoints.json; do
  printf '\n' >> "$file"
  [ "$(pkgs)" = --workspace ] || fail "$file: $(pkgs)"
  reset_ws
done

echo "test: packages: a package manifest without a readable name selects --workspace"
printf '[package]\nversion = "0.1.0"\n' > crates/domain/Cargo.toml
[ "$(pkgs)" = --workspace ] || fail "nameless manifest: $(pkgs)"
reset_ws

echo "test: packages: a base git cannot read fails"
if prepush_changed_packages 0000000000000000000000000000000000000001 >/dev/null 2>&1; then
  fail "unknown base returned 0"
fi

# The real gate. Each case commits one change on top of the base, so the
# selection is that change alone and no stamp is reused.
gate() {
  rm -rf "$WORK/log"
  mkdir -p "$WORK/log"
  local st=0
  FAKE_LOG="$WORK/log" PATH="$WORK/bin:$PATH" PREPUSH_BASE="$BASE" \
    PREPUSH_CARGO_TARGET="$WORK/target" HEAVY_LOCK_DIR="$WORK/locks" \
    HEAVY_DISK_VOLUMES="" bash scripts/prepush.sh </dev/null >"$WORK/out" 2>&1 || st=$?
  printf '%s\n' "$st"
}
cargo_log() { cat "$WORK/log/cargo" 2>/dev/null || true; }
expect_ok() {
  [ "$1" = 0 ] || fail "$2: status $1: $(cat "$WORK/out")"
  tail -1 "$WORK/out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) [0-9]* $3$" \
    || fail "$2: last line: $(tail -1 "$WORK/out")"
}
commit_change() {
  reset_ws
  printf '// %s %s\n' "$RANDOM" "$(date +%s)" >> "$1"
  git commit -qam "change $1"
}

echo "test: gate: fast runs fmt, clippy and lib tests for a domain-only change"
commit_change crates/domain/src/lib.rs
st="$(gate)"
expect_ok "$st" "domain fast" fast
cargo_log | grep -qx 'fmt --check' || fail "domain fast: no fmt: $(cargo_log)"
cargo_log | grep -qx 'clippy -p marketplace-domain --all-targets -- -D warnings' \
  || fail "domain fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-domain --jobs 1 --lib --bins ' \
  || fail "domain fast: lib tests: $(cargo_log)"
if cargo_log | grep -q -- '--workspace\|--test \|marketplace-service'; then
  fail "domain fast ran more than the domain: $(cargo_log)"
fi

echo "test: gate: fast runs the integration binaries for a service change"
commit_change crates/service/src/lib.rs
st="$(gate)"
expect_ok "$st" "service fast" fast
cargo_log | grep -qx 'clippy -p marketplace-service --all-targets -- -D warnings' \
  || fail "service fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --lib --bins ' \
  || fail "service fast: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test orders_test ' \
  || fail "service fast: parallel binaries: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test refusal_audit_test --test ' \
  || fail "service fast: serial binaries: $(cargo_log)"
if cargo_log | grep -q -- '--workspace'; then
  fail "service fast used --workspace: $(cargo_log)"
fi

echo "test: gate: fast runs fmt only when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
[ ! -e "$WORK/log/docker" ] || fail "docs fast started Postgres: $(cat "$WORK/log/docker")"

echo "test: gate: fast runs the workspace for a Cargo.lock change"
commit_change Cargo.lock
st="$(gate)"
expect_ok "$st" "lock fast" fast
cargo_log | grep -qx 'clippy --workspace --all-targets -- -D warnings' \
  || fail "lock fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test --workspace --jobs 1 --lib --bins ' \
  || fail "lock fast: tests: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=1 runs the whole workspace for a docs-only change"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast before full" fast
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "docs full" full
grep -q 'reused' "$WORK/out" && fail "full reused the fast stamp: $(cat "$WORK/out")"
cargo_log | grep -qx 'clippy --workspace --all-targets -- -D warnings' \
  || fail "docs full: clippy: $(cargo_log)"
cargo_log | grep -q '^test --workspace --jobs 1 --lib --bins ' \
  || fail "docs full: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test orders_test ' \
  || fail "docs full: integration binaries: $(cargo_log)"

echo "test: gate: a fast run after a full pass reuses it"
commit_change docs/readme.md
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "full before fast" full
st="$(gate)"
expect_ok "$st" "fast after full" "fast reused:full .*"
[ -z "$(cargo_log)" ] || fail "fast after full ran cargo: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=yes is refused before any step"
commit_change docs/readme.md
st="$(PREPUSH_FULL=yes gate)"
[ "$st" = 1 ] || fail "PREPUSH_FULL=yes: status $st"
grep -q 'PREPUSH_FULL must be 1, 0 or unset' "$WORK/out" || fail "PREPUSH_FULL=yes: $(cat "$WORK/out")"
[ -z "$(cargo_log)" ] || fail "PREPUSH_FULL=yes ran cargo: $(cargo_log)"

echo "ALL OK"
