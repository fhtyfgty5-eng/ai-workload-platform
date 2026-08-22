package runtime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestCoordinatorStartDoesNotWaitForRecoveredRunToFinish(t *testing.T) {
	engine := newBlockingRunEngine()
	coordinator := NewCoordinator(
		&fakeRecoveryStore{ids: []workflow.RunID{"run-one"}},
		engine,
		func(context.Context) (Lock, error) { return &fakeLock{}, nil },
		CoordinatorOptions{LockCheckInterval: time.Hour, LockCheckTimeout: time.Second},
	)
	startDone := make(chan error, 1)
	go func() { startDone <- coordinator.Start(context.Background()) }()

	waitForRunID(t, engine.resumeStarted, "run-one")
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		close(engine.release)
		t.Fatal("Start() waited for recovered Run to finish")
	}
	if !coordinator.Ready() {
		t.Fatal("Coordinator is not ready after recovery ownership was transferred")
	}
	closeCoordinator(t, coordinator)
}

func TestCoordinatorExecuteErrorTriggersFailStop(t *testing.T) {
	engine := &fakeRunEngine{executeErr: errors.New("database write failed")}
	coordinator := startedCoordinator(t, engine, &fakeLock{})

	coordinator.Enqueue("run-one")
	if err := receiveFatal(t, coordinator.Errors()); !strings.Contains(err.Error(), "run-one") {
		t.Fatalf("fatal error = %v, want RunID", err)
	}
	if coordinator.Ready() {
		t.Fatal("Coordinator remained ready after Execute error")
	}
}

func TestCoordinatorLockFailureCancelsOtherRunsOnlyOnce(t *testing.T) {
	lock := newControllableLock()
	engine := newBlockingRunEngine()
	coordinator := startedCoordinator(t, engine, lock)
	coordinator.Enqueue("run-one")
	waitForRunID(t, engine.executeStarted, "run-one")

	lock.fail(errors.New("lock connection lost"))
	if err := receiveFatal(t, coordinator.Errors()); !strings.Contains(err.Error(), "lock connection lost") {
		t.Fatalf("fatal error = %v, want lock failure", err)
	}
	waitForSignal(t, engine.executeStopped, "Execute context cancellation")
	assertNoSecondFatal(t, coordinator.Errors())
}

func TestCoordinatorKeepsServiceUnreadyWhenStartupFails(t *testing.T) {
	coordinator := NewCoordinator(
		&fakeRecoveryStore{},
		&fakeRunEngine{},
		func(context.Context) (Lock, error) { return nil, errors.New("lock busy") },
		CoordinatorOptions{},
	)
	if err := coordinator.Start(context.Background()); err == nil || coordinator.Ready() {
		t.Fatalf("Start() error=%v Ready()=%v, want error and false", err, coordinator.Ready())
	}

	lock := &fakeLock{}
	store := &fakeRecoveryStore{ids: []workflow.RunID{"run-one"}, loadErr: errors.New("corrupt run")}
	coordinator = NewCoordinator(store, &fakeRunEngine{}, func(context.Context) (Lock, error) { return lock, nil }, CoordinatorOptions{})
	if err := coordinator.Start(context.Background()); err == nil || coordinator.Ready() {
		t.Fatalf("recovery Start() error=%v Ready()=%v, want error and false", err, coordinator.Ready())
	}
	if !lock.isClosed() {
		t.Fatal("startup lock was not closed after recovery validation failed")
	}
}

func TestCoordinatorCloseCancelsRunsAndIsIdempotent(t *testing.T) {
	lock := &fakeLock{}
	engine := newBlockingRunEngine()
	coordinator := startedCoordinator(t, engine, lock)
	coordinator.Enqueue("run-one")
	waitForRunID(t, engine.executeStarted, "run-one")

	closeCoordinator(t, coordinator)
	waitForSignal(t, engine.executeStopped, "Execute context cancellation")
	if coordinator.Ready() {
		t.Fatal("Coordinator remained ready after Close")
	}
	if !lock.isClosed() {
		t.Fatal("Close() did not release coordinator lock")
	}
	closeCoordinator(t, coordinator)
}

func TestCoordinatorLogsRecoveryAndEnqueuedRunLifecycle(t *testing.T) {
	var output bytes.Buffer
	engine := newObservedRunEngine()
	coordinator := NewCoordinator(
		&fakeRecoveryStore{ids: []workflow.RunID{"recovered-run"}},
		engine,
		func(context.Context) (Lock, error) { return &fakeLock{}, nil },
		CoordinatorOptions{
			LockCheckInterval: time.Hour,
			LockCheckTimeout:  time.Second,
			Logger:            slog.New(slog.NewJSONHandler(&output, nil)),
		},
	)
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForRunID(t, engine.resumeDone, "recovered-run")
	coordinator.Enqueue("new-run")
	waitForRunID(t, engine.executeDone, "new-run")
	closeCoordinator(t, coordinator)

	got := output.String()
	for _, want := range []string{
		`"msg":"run recovery started"`, `"run_id":"recovered-run"`,
		`"msg":"run recovery completed"`, `"msg":"run enqueued"`,
		`"msg":"run execution started"`, `"msg":"run execution completed"`, `"run_id":"new-run"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("Coordinator logs do not contain %q: %s", want, got)
		}
	}
}

func startedCoordinator(t *testing.T, engine RunEngine, lock Lock) *Coordinator {
	t.Helper()
	coordinator := NewCoordinator(
		&fakeRecoveryStore{},
		engine,
		func(context.Context) (Lock, error) { return lock, nil },
		CoordinatorOptions{LockCheckInterval: time.Millisecond, LockCheckTimeout: time.Second},
	)
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeCoordinator(t, coordinator) })
	return coordinator
}

func closeCoordinator(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func receiveFatal(t *testing.T, errors <-chan error) error {
	t.Helper()
	select {
	case err := <-errors:
		return err
	case <-time.After(time.Second):
		t.Fatal("Coordinator did not report a fatal error")
		return nil
	}
}

func assertNoSecondFatal(t *testing.T, errors <-chan error) {
	t.Helper()
	select {
	case err := <-errors:
		t.Fatalf("unexpected second fatal error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForRunID(t *testing.T, started <-chan workflow.RunID, want workflow.RunID) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("started RunID = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run %q did not start", want)
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type fakeLock struct {
	mu     sync.Mutex
	closed bool
}

func (f *fakeLock) Check(context.Context) error { return nil }

func (f *fakeLock) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeLock) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type controllableLock struct {
	*fakeLock
	failures chan error
}

func newControllableLock() *controllableLock {
	return &controllableLock{fakeLock: &fakeLock{}, failures: make(chan error, 1)}
}

func (l *controllableLock) Check(ctx context.Context) error {
	select {
	case err := <-l.failures:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *controllableLock) fail(err error) { l.failures <- err }

type fakeRecoveryStore struct {
	ids     []workflow.RunID
	listErr error
	loadErr error
}

func (f *fakeRecoveryStore) ListNonTerminal(context.Context) ([]workflow.RunID, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.ids, nil
}

func (f *fakeRecoveryStore) Load(context.Context, workflow.RunID) (workflow.RunSnapshot, error) {
	if f.loadErr != nil {
		return workflow.RunSnapshot{}, f.loadErr
	}
	return workflow.RunSnapshot{}, nil
}

type fakeRunEngine struct {
	executeErr error
	resumeErr  error
}

func (f *fakeRunEngine) Execute(context.Context, workflow.RunID) (workflow.WorkflowRun, error) {
	return workflow.WorkflowRun{}, f.executeErr
}

func (f *fakeRunEngine) Resume(context.Context, workflow.RunID) (workflow.WorkflowRun, error) {
	return workflow.WorkflowRun{}, f.resumeErr
}

type blockingRunEngine struct {
	executeStarted chan workflow.RunID
	resumeStarted  chan workflow.RunID
	executeStopped chan struct{}
	release        chan struct{}
	stopOnce       sync.Once
}

type observedRunEngine struct {
	executeDone chan workflow.RunID
	resumeDone  chan workflow.RunID
}

func newObservedRunEngine() *observedRunEngine {
	return &observedRunEngine{executeDone: make(chan workflow.RunID, 1), resumeDone: make(chan workflow.RunID, 1)}
}

func (e *observedRunEngine) Execute(_ context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	e.executeDone <- id
	return workflow.WorkflowRun{ID: id, Status: workflow.WorkflowSucceeded}, nil
}

func (e *observedRunEngine) Resume(_ context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	e.resumeDone <- id
	return workflow.WorkflowRun{ID: id, Status: workflow.WorkflowSucceeded}, nil
}

func newBlockingRunEngine() *blockingRunEngine {
	return &blockingRunEngine{
		executeStarted: make(chan workflow.RunID, 1),
		resumeStarted:  make(chan workflow.RunID, 1),
		executeStopped: make(chan struct{}),
		release:        make(chan struct{}),
	}
}

func (e *blockingRunEngine) Execute(ctx context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	e.executeStarted <- id
	select {
	case <-ctx.Done():
		e.stopOnce.Do(func() { close(e.executeStopped) })
		return workflow.WorkflowRun{}, ctx.Err()
	case <-e.release:
		return workflow.WorkflowRun{ID: id}, nil
	}
}

func (e *blockingRunEngine) Resume(ctx context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	e.resumeStarted <- id
	select {
	case <-ctx.Done():
		return workflow.WorkflowRun{}, ctx.Err()
	case <-e.release:
		return workflow.WorkflowRun{ID: id}, nil
	}
}
