package dispatch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCoordinatorRecordsLeaseReclaimDuration(t *testing.T) {
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	store := &fakeDispatchStore{}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{
		ScanInterval: time.Hour, LockCheckInterval: time.Hour, Metrics: metrics,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !waitForCount(&store.mu, &store.reaps, 1, time.Second) {
		t.Fatal("coordinator did not run lease reaper")
	}
	text, err := observability.GatherText(metrics)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "workload_lease_reclaim_duration_seconds_count 1") {
		t.Fatalf("lease reclaim metric was not observed: %s", text)
	}
	if !strings.Contains(text, `operation="dispatch_schedule"`) {
		t.Fatalf("dispatch schedule metric was not observed: %s", text)
	}
}

func TestCoordinatorStartsWithImmediateScanAndCoalescesWakeups(t *testing.T) {
	store := &fakeDispatchStore{scanBlock: make(chan struct{}), scanStarted: make(chan struct{})}
	lock := &fakeDispatchLock{}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return lock, nil }, CoordinatorOptions{
		ScanInterval:      time.Hour,
		LockCheckInterval: time.Hour,
		LockCheckTimeout:  time.Second,
		BatchSize:         7,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !coordinator.Ready() {
		t.Fatal("coordinator is not ready after Start")
	}
	select {
	case <-store.scanStarted:
	case <-time.After(time.Second):
		t.Fatal("initial scan did not start")
	}
	for range 10 {
		coordinator.Wake()
	}
	close(store.scanBlock)
	if !waitForCount(&store.mu, &store.scans, 2, time.Second) {
		t.Fatalf("scans = %d, want initial scan plus one coalesced wake scan", store.scans)
	}
	if store.lastLimit != 7 {
		t.Fatalf("scan limit = %d, want 7", store.lastLimit)
	}
}

func TestCoordinatorPeriodicScanRecoversLostWakeup(t *testing.T) {
	store := &fakeDispatchStore{}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{
		ScanInterval:      10 * time.Millisecond,
		LockCheckInterval: time.Hour,
		LockCheckTimeout:  time.Second,
		BatchSize:         1,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !waitForCount(&store.mu, &store.scans, 2, time.Second) {
		t.Fatalf("periodic scans = %d, want at least 2", store.scans)
	}
	store.mu.Lock()
	reaps := store.reaps
	store.mu.Unlock()
	if reaps < 2 {
		t.Fatalf("periodic reaps = %d, want at least 2", reaps)
	}
}

func TestCoordinatorPeriodicScanReconcilesPersistedCancellationRequests(t *testing.T) {
	store := &fakeDispatchStore{}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{
		ScanInterval:      10 * time.Millisecond,
		LockCheckInterval: time.Hour,
		LockCheckTimeout:  time.Second,
		BatchSize:         3,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !waitForCount(&store.mu, &store.cancelScans, 2, time.Second) {
		t.Fatalf("cancellation scans = %d, want startup scan plus periodic scan", store.cancelScans)
	}
	if store.lastCancelLimit != 3 {
		t.Fatalf("cancellation scan limit = %d, want 3", store.lastCancelLimit)
	}
}

func TestCoordinatorContinuesScanningWhileDispatchCreationMakesProgress(t *testing.T) {
	store := &fakeDispatchStore{scanResults: []int{1, 1, 0}}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{
		ScanInterval:      time.Hour,
		LockCheckInterval: time.Hour,
		LockCheckTimeout:  time.Second,
		BatchSize:         1,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !waitForCount(&store.mu, &store.scans, 3, time.Second) {
		t.Fatalf("scans = %d, want progress scans to continue until Store returns zero", store.scans)
	}
}

func TestCoordinatorStoreFailureIsFailStop(t *testing.T) {
	store := &fakeDispatchStore{scanErr: errors.New("database unavailable")}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{
		ScanInterval:      time.Hour,
		LockCheckInterval: time.Hour,
		LockCheckTimeout:  time.Second,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-coordinator.Errors():
		if !strings.Contains(err.Error(), "database unavailable") {
			t.Fatalf("fatal error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator did not report store failure")
	}
	if coordinator.Ready() {
		t.Fatal("coordinator remained ready after Store failure")
	}
}

func TestCoordinatorLockFailureIsFailStopAndCloseIsIdempotent(t *testing.T) {
	lock := &fakeDispatchLock{checkErr: errors.New("lock lost")}
	coordinator := NewCoordinator(&fakeDispatchStore{}, func(context.Context) (Lock, error) { return lock, nil }, CoordinatorOptions{
		ScanInterval:      time.Hour,
		LockCheckInterval: 10 * time.Millisecond,
		LockCheckTimeout:  time.Second,
	})
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-coordinator.Errors():
		if !strings.Contains(err.Error(), "lock lost") {
			t.Fatalf("fatal error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator did not report lock failure")
	}
	if coordinator.Ready() {
		t.Fatal("coordinator remained ready after lock failure")
	}
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lock.closeCalls != 1 {
		t.Fatalf("lock close calls = %d, want 1", lock.closeCalls)
	}
}

func TestCoordinatorCancelRequiresReadyAndDelegatesToStore(t *testing.T) {
	store := &fakeDispatchStore{}
	coordinator := NewCoordinator(store, func(context.Context) (Lock, error) { return &fakeDispatchLock{}, nil }, CoordinatorOptions{ScanInterval: time.Hour, LockCheckInterval: time.Hour})
	if err := coordinator.Cancel(context.Background(), "run-one"); !errors.Is(err, ErrCoordinatorNotReady) {
		t.Fatalf("Cancel() before Start = %v", err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if err := coordinator.Cancel(context.Background(), "run-one"); err != nil {
		t.Fatal(err)
	}
	if store.cancelID != "run-one" {
		t.Fatalf("cancel ID = %s, want run-one", store.cancelID)
	}
}

type fakeDispatchStore struct {
	mu              sync.Mutex
	scans           int
	reaps           int
	cancelScans     int
	lastLimit       int
	lastCancelLimit int
	scanErr         error
	scanResults     []int
	scanBlock       chan struct{}
	scanStarted     chan struct{}
	scanOnce        sync.Once
	cancelID        workflow.RunID
}

func (s *fakeDispatchStore) CreateDispatches(_ context.Context, limit int) (int, error) {
	s.mu.Lock()
	s.scans++
	s.lastLimit = limit
	if s.scanStarted != nil {
		s.scanOnce.Do(func() { close(s.scanStarted) })
	}
	block := s.scanBlock
	err := s.scanErr
	result := 0
	if len(s.scanResults) > 0 {
		result = s.scanResults[0]
		s.scanResults = s.scanResults[1:]
	}
	s.mu.Unlock()
	if block != nil {
		<-block
	}
	return result, err
}
func (s *fakeDispatchStore) Claim(context.Context, string, int) ([]workerprotocol.Lease, error) {
	return nil, nil
}
func (s *fakeDispatchStore) Heartbeat(context.Context, string, []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error) {
	return workerprotocol.HeartbeatResponse{}, nil
}
func (s *fakeDispatchStore) Complete(context.Context, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	return workerprotocol.CompleteResponse{}, nil
}
func (s *fakeDispatchStore) ReapExpired(context.Context, int) (int, error) {
	s.mu.Lock()
	s.reaps++
	s.mu.Unlock()
	return 0, nil
}
func (s *fakeDispatchStore) CancelRequestedRuns(_ context.Context, limit int) (int, error) {
	s.mu.Lock()
	s.cancelScans++
	s.lastCancelLimit = limit
	s.mu.Unlock()
	return 0, nil
}
func (s *fakeDispatchStore) CancelRun(_ context.Context, id workflow.RunID) error {
	s.mu.Lock()
	s.cancelID = id
	s.mu.Unlock()
	return nil
}
func (s *fakeDispatchStore) ListWorkerSummaries(context.Context, string, int) ([]workerprotocol.WorkerSummary, bool, error) {
	return nil, false, nil
}

type fakeDispatchLock struct {
	checkErr   error
	closeCalls int
}

func (l *fakeDispatchLock) Check(context.Context) error { return l.checkErr }
func (l *fakeDispatchLock) Close() error {
	l.closeCalls++
	return nil
}

func waitForCount(mu *sync.Mutex, count *int, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := *count
		mu.Unlock()
		if got >= want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
