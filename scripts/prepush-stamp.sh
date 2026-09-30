#!/usr/bin/env bash
# Reuse of a passing pre-push gate, keyed by HEAD's tree.
#
# A full pass writes <git-common-dir>/prepush-ok/<tree>. Every worktree of
# this clone shares that directory. A later run on the same tree with no
# tracked changes prints the PREPUSH OK line and skips the gate, so the push
# hook does not repeat a gate the lane already ran. A rebase that keeps the
# tree reuses the pass; one that changes content does not.
#
# PREPUSH_FORCE=1 always runs the gate. A gate that started or ended with
# tracked changes, or whose HEAD tree moved, checked files that are not the
# tree, so it writes no stamp. Untracked files are not part of the key.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source scripts/prepush-stamp.sh" >&2
  exit 1
fi

prepush_stamp_dir() {
  local common
  common="$(git rev-parse --git-common-dir)"
  printf '%s/prepush-ok\n' "$(cd "$common" && pwd)"
}

prepush_tree_clean() {
  [ -z "$(git status --porcelain --untracked-files=no)" ]
}

# Returns 0 and prints the PREPUSH OK line when this tree already passed.
prepush_reuse() {
  local sha="$1" stamp
  PREPUSH_TREE="$(git rev-parse 'HEAD^{tree}')"
  PREPUSH_CLEAN_START=0
  if prepush_tree_clean; then
    PREPUSH_CLEAN_START=1
  fi
  if [ -n "${PREPUSH_FORCE:-}" ] || [ "$PREPUSH_CLEAN_START" != 1 ]; then
    return 1
  fi
  stamp="$(prepush_stamp_dir)/${PREPUSH_TREE}"
  [ -f "$stamp" ] || return 1
  echo "PREPUSH OK ${sha} 0 reused:$(cat "$stamp")"
}

# Call only after every step of the gate passed.
prepush_stamp() {
  local sha="$1" dir tmp
  if [ "${PREPUSH_CLEAN_START:-0}" != 1 ] || ! prepush_tree_clean \
    || [ "$(git rev-parse 'HEAD^{tree}')" != "${PREPUSH_TREE:-}" ]; then
    echo "prepush: tracked changes or a moved HEAD; no reuse stamp written"
    return 0
  fi
  dir="$(prepush_stamp_dir)"
  mkdir -p "$dir"
  tmp="$(mktemp "${dir}/.stamp.XXXXXX")"
  printf '%s %s\n' "$sha" "$(date -u +%FT%TZ)" > "$tmp"
  mv "$tmp" "${dir}/${PREPUSH_TREE}"
}
