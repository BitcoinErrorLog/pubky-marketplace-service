#!/usr/bin/env bash
# Proves scripts/prepush-stamp.sh reuses a pass only for the same clean tree.
# Runs in a throwaway repository; the gate is a counter, not the real gate.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-stamp-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

git init -q "$WORK/repo"
cd "$WORK/repo"
git config user.email test@example.invalid
git config user.name test
printf 'a\n' > file.txt
git add file.txt
git commit -qm one

# Same shape as scripts/prepush.sh: reuse check, gate, stamp after a pass.
# GATE_RESULT=1 makes the gate fail. Each run is a fresh shell, like a push.
gate() {
  GATE_RESULT="${GATE_RESULT:-0}" bash -c '
    set -euo pipefail
    source "$1"
    sha="$(git rev-parse HEAD)"
    if prepush_reuse "$sha"; then
      exit 0
    fi
    echo "gate ran" >> "$2"
    [ "$GATE_RESULT" = 0 ] || exit 1
    prepush_stamp "$sha"
    echo "PREPUSH OK ${sha} 1"
  ' _ "$DIR/prepush-stamp.sh" "$WORK/runs"
}

runs() {
  if [ -f "$WORK/runs" ]; then
    wc -l < "$WORK/runs" | tr -d ' '
  else
    echo 0
  fi
}

stamp_dir="$(git rev-parse --git-common-dir)/prepush-ok"

echo "test: a failed gate writes no stamp"
if GATE_RESULT=1 gate > /dev/null; then
  fail "failing gate returned 0"
fi
[ "$(runs)" = 1 ] || fail "failing gate did not run"
[ ! -e "$stamp_dir" ] || fail "failing gate wrote a stamp"

echo "test: the first pass runs the gate and writes a stamp"
out="$(gate)"
[ "$(runs)" = 2 ] || fail "first pass did not run the gate"
[ -f "$stamp_dir/$(git rev-parse 'HEAD^{tree}')" ] || fail "no stamp after a pass"
printf '%s\n' "$out" | grep -q '^PREPUSH OK [0-9a-f]* 1$' || fail "pass line: $out"

echo "test: a second run on the same tree reuses the stamp"
out="$(gate)"
[ "$(runs)" = 2 ] || fail "second run ran the gate"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 reused:" \
  || fail "reuse line: $out"

echo "test: PREPUSH_FORCE=1 runs the gate"
PREPUSH_FORCE=1 gate > /dev/null
[ "$(runs)" = 3 ] || fail "PREPUSH_FORCE=1 reused the stamp"

echo "test: a dirty tree runs the gate and writes no stamp"
printf 'dirty\n' >> file.txt
before="$(ls "$stamp_dir" | wc -l | tr -d ' ')"
gate > /dev/null
[ "$(runs)" = 4 ] || fail "dirty tree reused the stamp"
after="$(ls "$stamp_dir" | wc -l | tr -d ' ')"
[ "$before" = "$after" ] || fail "dirty tree wrote a stamp"
git checkout -q -- file.txt

echo "test: a staged change counts as dirty"
printf 'staged\n' >> file.txt
git add file.txt
gate > /dev/null
[ "$(runs)" = 5 ] || fail "staged change reused the stamp"
git reset -q --hard

echo "test: an untracked file does not block reuse"
printf 'x\n' > untracked.txt
gate > /dev/null
[ "$(runs)" = 5 ] || fail "untracked file blocked reuse"
rm -f untracked.txt

echo "test: a changed tree runs the gate"
printf 'b\n' > file.txt
git commit -qam two
gate > /dev/null
[ "$(runs)" = 6 ] || fail "changed tree reused the stamp"
gate > /dev/null
[ "$(runs)" = 6 ] || fail "changed tree's own pass was not reused"

echo "test: a pass on a dirty tree does not stamp HEAD's tree"
printf 'c\n' > file.txt
git commit -qam three
printf 'dirty\n' >> file.txt
gate > /dev/null
[ "$(runs)" = 7 ] || fail "dirty run on a new tree did not run the gate"
[ ! -e "$stamp_dir/$(git rev-parse 'HEAD^{tree}')" ] || fail "dirty run stamped HEAD's tree"
git checkout -q -- file.txt
gate > /dev/null
[ "$(runs)" = 8 ] || fail "clean run reused a stamp from a dirty run"

echo "test: a commit with an identical tree reuses the stamp"
git commit -q --allow-empty -m empty
out="$(gate)"
[ "$(runs)" = 8 ] || fail "identical tree ran the gate"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 reused:" \
  || fail "identical-tree reuse line: $out"

echo "test: a linked worktree shares the stamp"
git worktree add -q "$WORK/linked" HEAD
out="$(cd "$WORK/linked" && gate)"
[ "$(runs)" = 8 ] || fail "linked worktree ran the gate"
printf '%s\n' "$out" | grep -q ' 0 reused:' || fail "linked reuse line: $out"

echo "ALL OK"
