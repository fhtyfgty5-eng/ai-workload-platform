#!/usr/bin/env bash
set -euo pipefail

workers=1
runs=1
mode=off
output_dir="artifacts/module5"
iterations=5

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) workers="${2:-}"; shift 2 ;;
    --runs) runs="${2:-}"; shift 2 ;;
    --observability) mode="${2:-}"; shift 2 ;;
    --output-dir) output_dir="${2:-}"; shift 2 ;;
    --iterations) iterations="${2:-}"; shift 2 ;;
    -h|--help)
      printf '%s\n' 'usage: scripts/run-module5-benchmark.sh [--workers 1|4|16] [--runs 1|8] [--observability off|logs|logs_metrics|logs_metrics_tracing] [--iterations N] [--output-dir DIR]'
      exit 0
      ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

case "$workers" in 1|4|16) ;; *) printf '%s\n' '--workers must be 1, 4 or 16' >&2; exit 2 ;; esac
case "$runs" in 1|8) ;; *) printf '%s\n' '--runs must be 1 or 8' >&2; exit 2 ;; esac
case "$mode" in off|logs|logs_metrics|logs_metrics_tracing) ;; *) printf '%s\n' '--observability is invalid' >&2; exit 2 ;; esac
if ! [[ "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' '--iterations must be a positive integer' >&2
  exit 2
fi
if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  printf '%s\n' 'TEST_DATABASE_URL is required; start local PostgreSQL and load .env.local first.' >&2
  exit 2
fi

mkdir -p "$output_dir"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output_file="$output_dir/benchmark-${workers}workers-${runs}runs-${mode}-${timestamp}.txt"
{
  printf 'observability=%s workers=%s runs=%s warmup=1 iterations=%s\n' "$mode" "$workers" "$runs" "$iterations"
  printf 'go=%s\n' "$(go version)"
  printf 'os=%s arch=%s cpu=%s\n' "$(uname -s)" "$(uname -m)" "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  printf 'postgres_image=%s\n' "$(docker compose config --images 2>/dev/null | tr '\n' ' ' || true)"
  printf '\nwarmup\n'
  GOCACHE="${GOCACHE:-/tmp/ai-workload-gocache}" WORKLOAD_OBSERVABILITY_MODE="$mode" WORKLOAD_BENCHMARK_RUNS="$runs" \
    go test ./internal/e2e -run '^$' -bench "^BenchmarkWorkerFleetThousandTasks/workers-${workers}$" -benchtime=1x -count=1 -timeout=10m >/dev/null
  printf 'warmup=passed\n\nformal_runs=%s\n' "$iterations"
  GOCACHE="${GOCACHE:-/tmp/ai-workload-gocache}" WORKLOAD_OBSERVABILITY_MODE="$mode" WORKLOAD_BENCHMARK_RUNS="$runs" \
    go test ./internal/e2e -run '^$' -bench "^BenchmarkWorkerFleetThousandTasks/workers-${workers}$" -benchmem -benchtime=1x -count="$iterations" -timeout=30m
  printf '\nfinished_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} 2>&1 | tee "$output_file"
printf 'benchmark evidence: %s\n' "$output_file"
