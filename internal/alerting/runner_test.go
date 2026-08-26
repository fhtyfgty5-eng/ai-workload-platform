package alerting

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRunnerEvaluatesSnapshotsWithoutBlockingOnSink(t *testing.T) {
	clock := time.Unix(500, 0)
	provider := func(context.Context) (Snapshot, error) {
		clock = clock.Add(2 * time.Second)
		return Snapshot{Now: clock, QueueDepth: 1, OnlineWorkers: 0, DBMax: 10}, nil
	}
	sink := &recordingSink{sent: make(chan Event, 2)}
	runner := NewRunner(NewEngine(Rules{WorkersOfflineFor: time.Second, RecoveryFor: time.Second}), provider, sink, 5*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runner.Run(ctx); close(done) }()
	select {
	case event := <-sink.sent:
		if event.Rule != WorkersOffline || event.Status != StatusFiring {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not emit alert")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestRunnerRecordsLifecycleAndNotificationMetrics(t *testing.T) {
	clock := time.Unix(600, 0)
	provider := func(context.Context) (Snapshot, error) {
		clock = clock.Add(2 * time.Second)
		return Snapshot{Now: clock, QueueDepth: 1, OnlineWorkers: 0, DBMax: 10}, nil
	}
	sink := &recordingSink{sent: make(chan Event, 2)}
	observer := &recordingAlertObserver{observed: make(chan alertObservation, 4)}
	runner := NewRunner(NewEngine(Rules{WorkersOfflineFor: time.Second, RecoveryFor: time.Second}), provider, sink, 5*time.Millisecond, nil, observer)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runner.Run(ctx); close(done) }()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case observation := <-observer.observed:
			seen[observation.operation+":"+observation.outcome] = true
		case <-time.After(time.Second):
			t.Fatalf("alert observations = %+v, want firing and successful notify", seen)
		}
	}
	if !seen["firing:success"] || !seen["notify:success"] {
		t.Fatalf("alert observations = %+v", seen)
	}
	cancel()
	<-done
}

type recordingSink struct {
	mu   sync.Mutex
	sent chan Event
}

func (s *recordingSink) Send(_ context.Context, event Event) error { s.sent <- event; return nil }

type alertObservation struct {
	operation string
	outcome   string
}

type recordingAlertObserver struct {
	observed chan alertObservation
}

func (o *recordingAlertObserver) ObserveAlert(_ string, operation, outcome string) {
	o.observed <- alertObservation{operation: operation, outcome: outcome}
}
