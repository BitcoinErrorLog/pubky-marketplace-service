#!/usr/bin/env bash
# Workspace packages a change touches, for the fast pre-push gate. Source it.
#
#   prepush_changed_packages <base> [<workspace-wide pattern>...]
#
# Looks at every file that differs between <base> and the working tree:
# committed, staged, unstaged, deleted, renamed (both names), and untracked
# files that are not ignored. A file belongs to the package of the nearest
# Cargo.toml above it that has a [package] table. A file that matches one of
# the shell patterns (root Cargo.toml and Cargo.lock, toolchain files, shared
# fixtures) affects every package, so the function prints the single line
# --workspace. A file under no package that matches no pattern (docs, CI,
# the gate scripts) selects nothing.
#
# The changed packages are then widened to every workspace member that
# depends on one of them, directly or through other members, by any kind of
# dependency (normal, dev or build), from `cargo metadata` resolve. A change
# to a library therefore tests everything built on it. The result is one
# package name per line, in byte order.
#
# A package whose name cannot be read, or a `cargo metadata` or jq failure,
# selects --workspace: an unknown graph runs everything.
#
# Returns 1 when git cannot diff against <base>.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source scripts/prepush-packages.sh" >&2
  exit 1
fi

# Prints the package name for a repo-relative directory, or nothing when no
# package manifest is above it. Returns 1 for a manifest without a name.
prepush_package_of_dir() {
  local dir="$1" manifest name
  while :; do
    manifest="${dir}/Cargo.toml"
    if [ -f "$manifest" ] && grep -q '^\[package\]' "$manifest"; then
      name="$(awk '
        /^\[/ { section = $0; next }
        section == "[package]" && $1 == "name" && $2 == "=" {
          value = $3
          gsub(/"/, "", value)
          print value
          exit
        }
      ' "$manifest")"
      [ -n "$name" ] || return 1
      printf '%s\n' "$name"
      return 0
    fi
    [ "$dir" != . ] || return 0
    dir="$(dirname "$dir")"
  done
}

# Prints the named workspace members plus every member that depends on one
# of them, transitively, one per line in byte order. Runs in the workspace
# root. Returns 1 when the graph cannot be read.
prepush_with_dependents() {
  local metadata names
  names="$(printf '%s\n' "$@" | jq -R . | jq -sc .)" || return 1
  # --locked: the gate never rewrites Cargo.lock. A lock that needs changes
  # means a root manifest changed, which already selects --workspace.
  metadata="$(cargo metadata --format-version 1 --locked 2>/dev/null)" || return 1
  printf '%s' "$metadata" | jq -r --argjson changed "$names" '
    .workspace_members as $members
    | (.packages
        | map(select(.id as $id | $members | index($id)))
        | map({key: .id, value: .name})
        | from_entries) as $name
    | [.resolve.nodes[]
        | select($name[.id] != null)
        | {name: $name[.id], deps: [.deps[].pkg | $name[.] | select(. != null)]}
      ] as $nodes
    | def widen($set):
        ($set + [$nodes[] | select(any(.deps[]; . as $d | $set | index($d))) | .name]
          | unique) as $next
        | if ($next | length) == ($set | length) then $set else widen($next) end;
      if ($changed - [$name[]] | length) > 0 then error("not a workspace member") else . end
      | widen($changed | unique)
      | .[]
  '
}

prepush_changed_packages() {
  local base="$1" diff_out untracked file pattern pkg widened
  shift
  diff_out="$(git diff --name-only --no-renames "$base")" || return 1
  untracked="$(git ls-files --others --exclude-standard)"
  local names=()
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    for pattern in "$@"; do
      # shellcheck disable=SC2254
      case "$file" in
        $pattern)
          echo "--workspace"
          return 0
          ;;
      esac
    done
    if ! pkg="$(prepush_package_of_dir "$(dirname "$file")")"; then
      echo "--workspace"
      return 0
    fi
    [ -n "$pkg" ] && names+=("$pkg")
  done < <(printf '%s\n%s\n' "$diff_out" "$untracked")
  [ "${#names[@]}" -gt 0 ] || return 0
  if ! widened="$(prepush_with_dependents "${names[@]}")" || [ -z "$widened" ]; then
    echo "prepush: cannot read the package graph (cargo metadata); testing the workspace" >&2
    echo "--workspace"
    return 0
  fi
  printf '%s\n' "$widened"
}
