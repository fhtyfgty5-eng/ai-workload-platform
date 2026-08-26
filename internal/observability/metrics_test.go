package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAlertCountersOnlyIncludeRequestedTimeWindow(t *testing.T) {
	m := NewMetrics(nil)
	start := time.Unix(1_000, 0)
	m.now = func() time.Time { return start }
	m.ObserveOperation("complete", "error", "unknown", time.Millisecond)
	m.ObserveOperation("lease_reap", "error", "database_unavailable", time.Millisecond)

	m.now = func() time.Time { return start.Add(31 * time.Second) }
	m.ObserveOperation("complete", "success", "", time.Millisecond)

	completeTotal, completeErrors, leaseReclaimErrors := m.AlertCounters(start.Add(31*time.Second), 30*time.Second)
	if completeTotal != 1 || completeErrors != 0 || leaseReclaimErrors != 0 {
		t.Fatalf("window counters = (%d, %d, %d), want (1, 0, 0)", completeTotal, completeErrors, leaseReclaimErrors)
	}
}

func TestMetricsExposeLowCardinalityOperationalValues(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.ObserveHTTP("GET", "/api/v1/runs/{run-id}", 200, 10*time.Millisecond)
	m.ObserveOperation("claim", "success", "", 20*time.Millisecond)
	m.SetQueueDepth("pending", 3)
	m.SetWorkers("active", 2)
	m.SetActiveLeases(1)
	m.SetDBPool("in_use", 4)
	m.ObserveAlert("queue_backlog", "firing", "success")
	text, err := GatherText(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"workload_http_requests_total", "workload_operation_duration_seconds", "workload_queue_depth", "workload_workers", "workload_active_leases", "workload_db_pool_connections", "workload_alerts_total"} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q: %s", want, text)
		}
	}
	for _, forbidden := range []string{"run_id=", "task_key=", "worker_id=", "dispatch_id="} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("metrics contains forbidden label %q: %s", forbidden, text)
		}
	}
}

func TestDBPoolWaitCounterAddsOnlyCumulativeDelta(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.ObserveDBPoolWait(2 * time.Second)
	m.ObserveDBPoolWait(3 * time.Second)
	text, err := GatherText(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "workload_db_pool_wait_seconds_total 3") {
		t.Fatalf("database wait counter repeated cumulative samples: %s", text)
	}
}

func TestMetricsNormalizeUnknownLabels(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.ObserveOperation("arbitrary-user-operation", "arbitrary-outcome", "arbitrary-error", time.Millisecond)
	m.ObserveHTTP("arbitrary-method", "/api/v1/runs/raw-user-id", 499, time.Millisecond)
	text, err := GatherText(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `operation="unknown"`) || !strings.Contains(text, `outcome="unknown"`) || !strings.Contains(text, `error_code="unknown"`) {
		t.Fatalf("unknown labels not normalized: %s", text)
	}
	if !strings.Contains(text, `method="unknown"`) || !strings.Contains(text, `route="unknown"`) {
		t.Fatalf("unknown HTTP labels not normalized: %s", text)
	}
}

func TestMetricsNormalizeInvalidHTTPStatus(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.ObserveHTTP("GET", "/metrics", 999, time.Millisecond)
	text, err := GatherText(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `status="unknown"`) {
		t.Fatalf("invalid HTTP status was not normalized: %s", text)
	}
}

func TestMetricsKeepKnownWorkerErrorCode(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	m.ObserveOperation("complete", "error", "result_conflict", time.Millisecond)
	text, err := GatherText(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `error_code="result_conflict"`) {
		t.Fatalf("known Worker error code was not retained: %s", text)
	}
}

func TestNormalizeErrorCodeKeepsKnownControlPlaneCodes(t *testing.T) {
	for _, code := range []string{
		"auth_config_invalid",
		"forbidden",
		"idempotency_conflict",
		"method_not_allowed",
		"workflow_exists",
	} {
		if got := NormalizeErrorCode(code); got != code {
			t.Errorf("NormalizeErrorCode(%q) = %q, want unchanged", code, got)
		}
	}
}

func TestNewMetricsReusesCollectorsAlreadyRegisteredInRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	first := NewMetrics(registry)
	first.SetActiveLeases(1)

	second := NewMetrics(registry)
	second.SetActiveLeases(2)
	text, err := GatherText(second)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "workload_active_leases 2") {
		t.Fatalf("second metrics instance did not reuse registered collectors: %s", text)
	}
}
