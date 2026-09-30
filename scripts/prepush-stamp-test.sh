#!/usr/bin/env bash
# Proves scripts/prepush-stamp.sh reuses a pass only for the same clean tree,
# and only from the same mode or full.
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
printf 'ignored.txt\n' > .gitignore
git add file.txt .gitignore
git commit -qm one

# Same shape as scripts/prepush.sh: mode, reuse check, gate, stamp after a
# pass. GATE_RESULT=1 makes the gate fail. GATE_DURING runs inside the gate,
# after the reuse check and before the stamp. PREPUSH_FULL passes through.
# Each run is a fresh shell, like a push.
gate() {
  GATE_RESULT="${GATE_RESULT:-0}" bash -c '
    set -euo pipefail
    source "$1"
    prepush_mode_init || exit 1
    sha="$(git rev-parse HEAD)"
    if prepush_reuse "$sha"; then
      exit 0
    fi
    echo "gate ran ${PREPUSH_MODE}" >> "$2"
    [ "$GATE_RESULT" = 0 ] || exit 1
    if [ -n "${GATE_DURING:-}" ]; then
      eval "$GATE_DURING"
    fi
    prepush_stamp "$sha"
    echo "PREPUSH OK ${sha} 1 ${PREPUSH_MODE}"
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
fast_stamp() { printf '%s/fast/%s\n' "$stamp_dir" "$(git rev-parse 'HEAD^{tree}')"; }
full_stamp() { printf '%s/%s\n' "$stamp_dir" "$(git rev-parse 'HEAD^{tree}')"; }
stamp_count() {
  if [ -d "$stamp_dir" ]; then
    find "$stamp_dir" -type f | wc -l | tr -d ' '
  else
    echo 0
  fi
}

echo "test: a failed gate writes no stamp"
if GATE_RESULT=1 gate > /dev/null; then
  fail "failing gate returned 0"
fi
[ "$(runs)" = 1 ] || fail "failing gate did not run"
[ ! -e "$stamp_dir" ] || fail "failing gate wrote a stamp"

echo "test: the first pass runs the fast gate and writes only the fast stamp"
out="$(gate)"
[ "$(runs)" = 2 ] || fail "first pass did not run the gate"
[ -f "$(fast_stamp)" ] || fail "no fast stamp after a pass"
[ ! -e "$(full_stamp)" ] || fail "a fast pass wrote the full stamp"
printf '%s\n' "$out" | grep -q '^PREPUSH OK [0-9a-f]* 1 fast$' || fail "pass line: $out"
tail -1 "$WORK/runs" | grep -qx 'gate ran fast' || fail "default mode is not fast"

echo "test: a second run on the same tree reuses the stamp"
out="$(gate)"
[ "$(runs)" = 2 ] || fail "second run ran the gate"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 fast reused:fast " \
  || fail "reuse line: $out"

echo "test: PREPUSH_FULL=0 is fast"
out="$(PREPUSH_FULL=0 gate)"
[ "$(runs)" = 2 ] || fail "PREPUSH_FULL=0 did not reuse the fast stamp"
printf '%s\n' "$out" | grep -q ' 0 fast reused:fast ' || fail "PREPUSH_FULL=0 line: $out"

echo "test: a full run does not reuse a fast stamp"
out="$(PREPUSH_FULL=1 gate)"
[ "$(runs)" = 3 ] || fail "full run reused the fast stamp"
tail -1 "$WORK/runs" | grep -qx 'gate ran full' || fail "PREPUSH_FULL=1 did not run full"
[ -f "$(full_stamp)" ] || fail "no full stamp after a full pass"
printf '%s\n' "$out" | grep -q '^PREPUSH OK [0-9a-f]* 1 full$' || fail "full pass line: $out"

echo "test: a full run reuses the full stamp"
out="$(PREPUSH_FULL=1 gate)"
[ "$(runs)" = 3 ] || fail "second full run ran the gate"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 full reused:full " \
  || fail "full reuse line: $out"

echo "test: PREPUSH_FULL other than 0 or 1 is refused"
for bad in yes true 2; do
  st=0
  err="$(PREPUSH_FULL="$bad" gate 2>&1 >/dev/null)" || st=$?
  [ "$st" != 0 ] || fail "PREPUSH_FULL=$bad returned 0"
  [ "$(runs)" = 3 ] || fail "PREPUSH_FULL=$bad ran a gate"
  printf '%s\n' "$err" | grep -q 'PREPUSH_FULL must be 1, 0 or unset' \
    || fail "PREPUSH_FULL=$bad message: $err"
done

echo "test: a fast run reuses a full stamp"
printf 'f\n' > file.txt
git commit -qam full-only
out="$(PREPUSH_FULL=1 gate)"
[ "$(runs)" = 4 ] || fail "full run on a new tree did not run"
[ ! -e "$(fast_stamp)" ] || fail "a full pass wrote the fast stamp"
out="$(gate)"
[ "$(runs)" = 4 ] || fail "fast run did not reuse the full stamp"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 fast reused:full " \
  || fail "fast-over-full reuse line: $out"

echo "test: a stamp written before modes counts as full"
printf 'g\n' > file.txt
git commit -qam legacy
printf '%s legacy\n' "$(git rev-parse HEAD)" > "$(full_stamp)"
out="$(PREPUSH_FULL=1 gate)"
[ "$(runs)" = 4 ] || fail "full run did not reuse a legacy stamp"
printf '%s\n' "$out" | grep -q ' 0 full reused:full .* legacy$' || fail "legacy reuse line: $out"

echo "test: PREPUSH_FORCE=1 runs the gate"
PREPUSH_FORCE=1 gate > /dev/null
[ "$(runs)" = 5 ] || fail "PREPUSH_FORCE=1 reused the stamp"

echo "test: a dirty tree runs the gate and writes no stamp"
printf 'dirty\n' >> file.txt
before="$(stamp_count)"
gate > /dev/null
[ "$(runs)" = 6 ] || fail "dirty tree reused the stamp"
after="$(stamp_count)"
[ "$before" = "$after" ] || fail "dirty tree wrote a stamp"
PREPUSH_FULL=1 gate > /dev/null
[ "$(runs)" = 7 ] || fail "dirty tree reused the full stamp"
[ "$before" = "$(stamp_count)" ] || fail "dirty full run wrote a stamp"
git checkout -q -- file.txt

echo "test: a staged change counts as dirty"
printf 'staged\n' >> file.txt
git add file.txt
gate > /dev/null
[ "$(runs)" = 8 ] || fail "staged change reused the stamp"
git reset -q --hard

echo "test: an untracked file that is not ignored runs the gate"
printf 'x\n' > untracked.txt
gate > /dev/null
[ "$(runs)" = 9 ] || fail "untracked file reused the stamp"
git config status.showUntrackedFiles no
gate > /dev/null
[ "$(runs)" = 10 ] || fail "status.showUntrackedFiles=no hid an untracked file"
git config --unset status.showUntrackedFiles
rm -f untracked.txt

echo "test: an ignored file does not block reuse"
printf 'x\n' > ignored.txt
gate > /dev/null
[ "$(runs)" = 10 ] || fail "ignored file blocked reuse"
rm -f ignored.txt

echo "test: a changed tree runs the gate"
printf 'b\n' > file.txt
git commit -qam two
gate > /dev/null
[ "$(runs)" = 11 ] || fail "changed tree reused the stamp"
gate > /dev/null
[ "$(runs)" = 11 ] || fail "changed tree's own pass was not reused"

echo "test: a pass on a dirty tree does not stamp HEAD's tree"
printf 'c\n' > file.txt
git commit -qam three
printf 'dirty\n' >> file.txt
gate > /dev/null
[ "$(runs)" = 12 ] || fail "dirty run on a new tree did not run the gate"
[ ! -e "$(fast_stamp)" ] || fail "dirty run stamped HEAD's tree"
git checkout -q -- file.txt
gate > /dev/null
[ "$(runs)" = 13 ] || fail "clean run reused a stamp from a dirty run"

echo "test: a pass with an untracked file does not stamp HEAD's tree"
printf 'd\n' > file.txt
git commit -qam four
printf 'x\n' > untracked.txt
gate > /dev/null
[ "$(runs)" = 14 ] || fail "untracked run on a new tree did not run the gate"
[ ! -e "$(fast_stamp)" ] || fail "untracked run stamped HEAD's tree"
rm -f untracked.txt
gate > /dev/null
[ "$(runs)" = 15 ] || fail "clean run reused a stamp from an untracked run"

echo "test: a file that appears during the gate blocks the stamp"
for during in "printf 'x\\n' > mid-untracked.txt" "printf 'mid\\n' >> file.txt"; do
  printf 'e\n' >> file.txt
  git commit -qam "mid $during"
  before="$(runs)"
  GATE_DURING="$during" gate > /dev/null
  [ "$(runs)" = $((before + 1)) ] || fail "clean start did not run the gate ($during)"
  [ ! -e "$(fast_stamp)" ] || fail "stamped after: $during"
  rm -f mid-untracked.txt
  git checkout -q -- file.txt
done
gate > /dev/null
[ "$(runs)" = 18 ] || fail "clean run after a mid-gate change reused a stamp"

echo "test: a commit with an identical tree reuses the stamp"
git commit -q --allow-empty -m empty
out="$(gate)"
[ "$(runs)" = 18 ] || fail "identical tree ran the gate"
printf '%s\n' "$out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) 0 fast reused:fast " \
  || fail "identical-tree reuse line: $out"

echo "test: a linked worktree shares the stamp"
git worktree add -q "$WORK/linked" HEAD
out="$(cd "$WORK/linked" && gate)"
[ "$(runs)" = 18 ] || fail "linked worktree ran the gate"
printf '%s\n' "$out" | grep -q ' 0 fast reused:fast ' || fail "linked reuse line: $out"

echo "ALL OK"
