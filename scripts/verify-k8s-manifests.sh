#!/usr/bin/env bash
set -euo pipefail

manifest_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy/k8s" && pwd)"
if rg -n '(BEGIN (RSA|OPENSSH|PRIVATE)|password:|token:|hostPath:|docker.sock|/Users/|/home/)' "$manifest_dir"; then
  echo "unsafe Kubernetes manifest content found" >&2
  exit 1
fi
if rg -n 'privileged: true|allowPrivilegeEscalation: true|automountServiceAccountToken: true|backoffLimit: [1-9]' "$manifest_dir"; then
  echo "unsafe Kubernetes security setting found" >&2
  exit 1
fi
echo "Kubernetes manifests passed static safety checks"
