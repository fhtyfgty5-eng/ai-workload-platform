package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/alerting"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsEndpointIsReadOnlyAndExposesLowCardinalityMetrics(t *testing.T) {
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	metrics.ObserveHTTP(http.MethodGet, "/metrics", http.StatusOK, 2)
	handler := httpapi.NewHandler(httpapi.Dependencies{Ready: func() bool { return true }, ViewerToken: "viewer", OperatorToken: "operator", Metrics: metrics})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer viewer")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("metrics response = %d %q", response.Code, response.Header().Get("Content-Type"))
	}
	if strings.Contains(response.Body.String(), "run_id=") || strings.Contains(response.Body.String(), "worker_id=") {
		t.Fatalf("metrics response contains high-cardinality label: %s", response.Body.String())
	}
}

func TestAlertRulesSendFiringAndResolvedWebhookEvents(t *testing.T) {
	var mu sync.Mutex
	var received []alerting.Event
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event alerting.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("decode Webhook event: %v", err)
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	sink, err := alerting.NewWebhookSink(receiver.Client(), receiver.URL, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	engine := alerting.NewEngine(alerting.Rules{
		QueueBacklogFor: time.Second, WorkersOfflineFor: time.Second,
		LeaseReclaimErrorsFor: time.Second, CompleteErrorRateFor: time.Second,
		DBPoolExhaustionFor: time.Second, RecoveryFor: time.Second, DBPoolRecoveryFor: 10 * time.Second,
	})
	start := time.Unix(1000, 0)
	bad := alerting.Snapshot{Now: start, QueueDepth: 2, AvailableSlots: 0, OnlineWorkers: 0, LeaseReclaimErrors: 1, CompleteTotal: 20, CompleteErrors: 2, DBInUse: 9, DBMax: 10}
	engine.Evaluate(bad)
	bad.Now = start.Add(2 * time.Second)
	events := engine.Evaluate(bad)
	good := alerting.Snapshot{Now: start.Add(3 * time.Second), AvailableSlots: 1, OnlineWorkers: 1, CompleteTotal: 20, DBMax: 10}
	engine.Evaluate(good)
	good.Now = start.Add(5 * time.Second)
	events = append(events, engine.Evaluate(good)...)
	if len(events) != 9 {
		t.Fatalf("early recovery events = %+v, want five firing and four general resolved", events)
	}
	good.Now = start.Add(13 * time.Second)
	events = append(events, engine.Evaluate(good)...)
	if len(events) != 10 {
		t.Fatalf("alert events = %+v, want five firing and five resolved", events)
	}
	for _, event := range events {
		if err := sink.Send(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 10 {
		t.Fatalf("Webhook events = %d, want 10", len(received))
	}
	counts := make(map[alerting.RuleName]map[alerting.Status]int)
	for _, event := range received {
		if counts[event.Rule] == nil {
			counts[event.Rule] = make(map[alerting.Status]int)
		}
		counts[event.Rule][event.Status]++
	}
	for _, rule := range []alerting.RuleName{alerting.QueueBacklog, alerting.WorkersOffline, alerting.LeaseReclaimErrors, alerting.CompleteErrorRate, alerting.DBPoolNearExhaustion} {
		if counts[rule][alerting.StatusFiring] != 1 || counts[rule][alerting.StatusResolved] != 1 {
			t.Fatalf("rule %s Webhook counts = %+v", rule, counts[rule])
		}
	}
}

func TestAlertWebhookFailureDoesNotStopSnapshotCollection(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer receiver.Close()
	sink, err := alerting.NewWebhookSink(receiver.Client(), receiver.URL, 5*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	clock := time.Unix(2000, 0)
	provider := func(context.Context) (alerting.Snapshot, error) {
		calls.Add(1)
		clock = clock.Add(2 * time.Second)
		return alerting.Snapshot{Now: clock, QueueDepth: 1, OnlineWorkers: 0, DBMax: 10}, nil
	}
	runner := alerting.NewRunner(alerting.NewEngine(alerting.Rules{WorkersOfflineFor: time.Second, RecoveryFor: time.Second}), provider, sink, time.Millisecond, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	runner.Run(ctx)
	if calls.Load() < 5 {
		t.Fatalf("snapshot calls = %d, Webhook failure blocked evaluation", calls.Load())
	}
}
