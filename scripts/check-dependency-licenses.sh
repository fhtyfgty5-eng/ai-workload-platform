#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test -f "$root/go.mod"
test -f "$root/web/package-lock.json"
test -f "$root/LICENSE"
# lockfiles are inspected without downloading packages; incompatible license text must be reviewed before release.
if rg -n -i '"license"[[:space:]]*:[[:space:]]*"(GPL|AGPL|SSPL|BUSL)' "$root/web/package-lock.json" "$root/go.mod"; then
  printf 'FAIL dependency license requires manual review\n' >&2
  exit 1
fi
printf 'PASS dependency manifests are locked and no incompatible license marker was found\n'
