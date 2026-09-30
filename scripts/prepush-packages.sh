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
# the gate scripts) selects nothing. Otherwise it prints one package name per
# line, sorted. A package whose name cannot be read also selects --workspace.
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

prepush_changed_packages() {
  local base="$1" diff_out untracked file pattern pkg
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
  if [ "${#names[@]}" -gt 0 ]; then
    printf '%s\n' "${names[@]}" | sort -u
  fi
}
