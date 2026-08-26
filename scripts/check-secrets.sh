#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
patterns='(BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|gh[pousr]_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9]{20,}|AIza[0-9A-Za-z_-]{20,}|AKIA[0-9A-Z]{16}|postgres://[^[:space:]]+:[^@[:space:]]+@|/Users/[A-Za-z0-9._-]+/|/home/[A-Za-z0-9._-]+/)'

if rg -n --hidden --glob '!.git/**' --glob '!web/node_modules/**' --glob '!web/dist/**' \
  --glob '!.env.local' --glob '!.env' --glob '!.env.example' \
  --glob '!README.md' --glob '!docs/实验/**' --glob '!scripts/test-verify-docs.sh' \
  --glob '!scripts/check-secrets.sh' --glob '!internal/testpostgres/**' "$patterns" "$root"; then
  printf 'FAIL potential secret or local-only value found\n' >&2
  exit 1
fi
printf 'PASS secret scan\n'
