#!/usr/bin/env bash
set -euo pipefail

scenario=""
output_dir="artifacts/module5"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario) scenario="${2:-}"; shift 2 ;;
    --output-dir) output_dir="${2:-}"; shift 2 ;;
    -h|--help)
      printf '%s\n' 'usage: scripts/run-module5-faults.sh --scenario worker-kill|claim-delay|complete-error|heartbeat-error|coordinator-lock|lease-expiry|pool-exhaustion|postgres-unavailable'
      exit 0
      ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

case "$scenario" in
  worker-kill|claim-delay|complete-error|heartbeat-error|coordinator-lock|lease-expiry|pool-exhaustion|postgres-unavailable) ;;
  *) printf '%s\n' '--scenario is required and must be a supported scenario' >&2; exit 2 ;;
esac

if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  printf '%s\n' 'TEST_DATABASE_URL is required; start local PostgreSQL and load .env.local first.' >&2
  exit 2
fi

case "$scenario" in
  worker-kill) test_name='TestWorkerProcessCrashIsRecoveredBySecondProcess' ;;
  claim-delay) test_name='TestFaultInjectionClaimDelayIsCancelableThenRecovers' ;;
  complete-error) test_name='TestFaultInjectionCompleteErrorThenRetryConverges' ;;
  heartbeat-error) test_name='TestFaultInjectionHeartbeatErrorThenRenewsLease' ;;
  coordinator-lock) test_name='TestFaultInjectionCoordinatorLockCheckThenRecovers' ;;
  lease-expiry) test_name='TestWorkerHTTPRejectsLateResultAfterLeaseReassignment' ;;
  pool-exhaustion) test_name='TestFaultInjectionPoolExhaustionRecoversAfterRelease' ;;
  postgres-unavailable) test_name='TestFaultInjectionPostgresUnavailableThenRecovers' ;;
esac

mkdir -p "$output_dir"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output_file="$output_dir/fault-${scenario}-${timestamp}.txt"
{
  printf 'scenario=%s\n' "$scenario"
  printf 'test=%s\n' "$test_name"
  printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  GOCACHE="${GOCACHE:-/tmp/ai-workload-gocache}" \
    go test ./internal/e2e -run "^${test_name}$" -count=1 -timeout=5m -v
  printf '%s\n' 'note=The test uses an isolated PostgreSQL database. postgres-unavailable injects a one-shot repository error without stopping the shared Docker container.'
  printf 'finished_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} 2>&1 | tee "$output_file"
printf 'fault evidence: %s\n' "$output_file"
