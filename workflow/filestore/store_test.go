package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type cancelBlockingExecutor struct {
	started chan struct{}
	once    sync.Once
}

type recordingSuccessExecutor struct {
	called chan struct{}
}

type controlledSuccessExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *controlledSuccessExecutor) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	close(e.started)
	<-e.release
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess}
}

type blockingApplyStore struct {
	base         *Store
	blockAt      int
	applyCount   int
	applyStarted chan struct{}
	releaseApply chan struct{}
}

func (s *blockingApplyStore) Create(ctx context.Context, snapshot workflow.RunSnapshot) error {
	return s.base.Create(ctx, snapshot)
}

func (s *blockingApplyStore) Load(ctx context.Context, id workflow.RunID) (workflow.RunSnapshot, error) {
	return s.base.Load(ctx, id)
}

func (s *blockingApplyStore) Apply(ctx context.Context, change workflow.ChangeSet) error {
	s.applyCount++
	if s.applyCount == s.blockAt {
		close(s.applyStarted)
		<-s.releaseApply
	}
	return s.base.Apply(ctx, change)
}

func (e *recordingSuccessExecutor) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	e.called <- struct{}{}
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess}
}

func (e *cancelBlockingExecutor) Execute(ctx context.Context, _ workflow.ExecutionRequest) workflow.ExecutionResponse {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: "execution canceled"}
}

func TestStoreCreateSaveLoad(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one", DefinitionID: "w", Status: workflow.WorkflowPending, CreatedAt: time.Unix(1, 0)}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Run.Status = workflow.WorkflowRunning
	if err := store.Save(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Run.Status != workflow.WorkflowRunning {
		t.Fatalf("status = %s, want running", loaded.Run.Status)
	}
}

func TestStoreDoesNotReplaceValidSnapshotWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	original := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one", Status: workflow.WorkflowPending, CreatedAt: time.Unix(1, 0)}}
	if err := store.Create(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	store.rename = func(string, string) error { return errors.New("rename failed") }
	original.Run.Status = workflow.WorkflowRunning
	if err := store.Save(context.Background(), original); err == nil {
		t.Fatal("Save() error = nil, want rename failure")
	}
	loaded, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Run.Status != workflow.WorkflowPending {
		t.Fatalf("status = %s, want pending", loaded.Run.Status)
	}
}

func TestStoreRejectsDuplicateAndMissingRuns(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one"}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), snapshot); !errors.Is(err, workflow.ErrRunExists) {
		t.Fatalf("duplicate Create() error = %v, want ErrRunExists", err)
	}
	if err := store.Save(context.Background(), workflow.RunSnapshot{Run: workflow.WorkflowRun{ID: "missing"}}); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("missing Save() error = %v, want ErrRunNotFound", err)
	}
	if _, err := store.Load(context.Background(), "missing"); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("missing Load() error = %v, want ErrRunNotFound", err)
	}
}

func TestStoreRejectsCorruptJSONAndPathTraversal(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupt.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "corrupt"); err == nil {
		t.Fatal("Load() error = nil, want corrupt JSON error")
	}
	for _, id := range []workflow.RunID{"", ".", "..", "nested/run", `nested\\run`} {
		snapshot := workflow.RunSnapshot{Run: workflow.WorkflowRun{ID: id}}
		if err := store.Create(context.Background(), snapshot); err == nil {
			t.Fatalf("Create() id %q error = nil, want invalid run id", id)
		}
	}
}

func TestStoreRejectsSnapshotRunIDMismatchAcrossOperations(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "expected.json")
	original := []byte(`{"version":1,"run":{"id":"other","status":"pending"}}`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := store.Load(context.Background(), "expected"); err == nil {
		t.Fatal("Load() error = nil, want mismatched RunID error")
	}
	if _, err := store.RequestCancel(context.Background(), "expected", time.Now()); err == nil {
		t.Fatal("RequestCancel() error = nil, want mismatched RunID error")
	}
	if _, err := store.ListNonTerminal(context.Background()); err == nil {
		t.Fatal("ListNonTerminal() error = nil, want mismatched RunID error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !reflect.DeepEqual(after, original) {
		t.Fatalf("mismatched snapshot was modified: got %q, want %q", after, original)
	}
}

func TestStoreConcurrentCreateSameRunHasSingleWinner(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one"}}
	results := make(chan error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer group.Done()
			results <- store.Create(context.Background(), snapshot)
		}()
	}
	group.Wait()
	close(results)

	winners := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			winners++
		} else if errors.Is(err, workflow.ErrRunExists) {
			duplicates++
		}
	}
	if winners != 1 || duplicates != 1 {
		t.Fatalf("winners = %d, duplicates = %d, want one each", winners, duplicates)
	}
}

func TestStoreRequestCancelPersistsRevision(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "cancel-run", Status: workflow.WorkflowPending}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	at := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	got, err := store.RequestCancel(context.Background(), "cancel-run", at)
	if err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}
	if got.Revision != snapshot.Run.Revision+1 {
		t.Fatalf("revision = %d, want %d", got.Revision, snapshot.Run.Revision+1)
	}
	if got.CancelRequestedAt == nil || !got.CancelRequestedAt.Equal(at) {
		t.Fatalf("cancel_requested_at = %v, want %v", got.CancelRequestedAt, at)
	}
}

func TestStoreRequestCancelIsIdempotent(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "cancel-run", Status: workflow.WorkflowPending}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	firstAt := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	first, err := store.RequestCancel(context.Background(), "cancel-run", firstAt)
	if err != nil {
		t.Fatalf("first RequestCancel() error = %v", err)
	}
	second, err := store.RequestCancel(context.Background(), "cancel-run", firstAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("second RequestCancel() error = %v", err)
	}
	if second.Revision != first.Revision {
		t.Fatalf("second revision = %d, want unchanged %d", second.Revision, first.Revision)
	}
	if second.CancelRequestedAt == nil || !second.CancelRequestedAt.Equal(firstAt) {
		t.Fatalf("second cancel_requested_at = %v, want first request time %v", second.CancelRequestedAt, firstAt)
	}
}

func TestStoreConcurrentRequestCancelReturnsSinglePersistedRequest(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "cancel-run", Status: workflow.WorkflowPending}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	const callers = 16
	start := make(chan struct{})
	results := make(chan workflow.WorkflowRun, callers)
	errors := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for i := 0; i < callers; i++ {
		i := i
		go func() {
			defer group.Done()
			<-start
			at := time.Date(2026, 8, 19, 0, i, 0, 0, time.UTC)
			run, err := store.RequestCancel(context.Background(), "cancel-run", at)
			results <- run
			errors <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent RequestCancel() error = %v", err)
		}
	}
	var requestedAt *time.Time
	for run := range results {
		if run.Revision != 1 || run.CancelRequestedAt == nil {
			t.Fatalf("concurrent result = %+v, want revision 1 with cancel time", run)
		}
		if requestedAt == nil {
			requestedAt = run.CancelRequestedAt
		} else if !run.CancelRequestedAt.Equal(*requestedAt) {
			t.Fatalf("cancel_requested_at = %v, want shared first request time %v", run.CancelRequestedAt, requestedAt)
		}
	}
}

func TestStoreApplyRejectsInvalidChangeWithoutReplacingSnapshot(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := workflow.RunSnapshot{
		Version: 1,
		Run: workflow.WorkflowRun{
			ID:                    "run-one",
			Status:                workflow.WorkflowPending,
			Tasks:                 []workflow.TaskRun{{Key: "task", Status: workflow.TaskReady}},
			RemainingDependencies: []int{0},
		},
	}
	if err := store.Create(context.Background(), original); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	invalidRun := original.Run
	invalidRun.ID = "other"
	invalidRun.Revision = 1
	invalidRun.Tasks = nil
	invalidRun.RemainingDependencies = nil
	change := workflow.ChangeSet{RunID: "run-one", ExpectedRevision: 0, Run: &invalidRun}
	if err := store.Apply(context.Background(), change); !errors.Is(err, workflow.ErrInvalidChangeSet) {
		t.Fatalf("Apply() error = %v, want ErrInvalidChangeSet", err)
	}
	loaded, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(loaded, original) {
		t.Fatalf("snapshot changed after rejected Apply(): got %+v, want %+v", loaded, original)
	}
}

func TestStoreListNonTerminalReturnsOnlyRecoverableRuns(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runs := []workflow.WorkflowRun{
		{ID: "a-pending", Status: workflow.WorkflowPending},
		{ID: "b-running", Status: workflow.WorkflowRunning},
		{ID: "c-succeeded", Status: workflow.WorkflowSucceeded},
		{ID: "d-failed", Status: workflow.WorkflowFailed},
		{ID: "e-canceled", Status: workflow.WorkflowCanceled},
	}
	for _, run := range runs {
		if err := store.Create(context.Background(), workflow.RunSnapshot{Version: 1, Run: run}); err != nil {
			t.Fatalf("Create(%q) error = %v", run.ID, err)
		}
	}

	got, err := store.ListNonTerminal(context.Background())
	if err != nil {
		t.Fatalf("ListNonTerminal() error = %v", err)
	}
	want := []workflow.RunID{"a-pending", "b-running"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListNonTerminal() = %v, want %v", got, want)
	}
}

func TestPersistedCancellationConvergesActiveEngine(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executor := &cancelBlockingExecutor{started: make(chan struct{})}
	engine, err := workflow.NewEngine(store, executor, workflow.EngineOptions{
		NewRunID: func() (workflow.RunID, error) { return "cancel-run", nil },
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	compiled, err := workflow.Compile(workflow.WorkflowDefinition{
		ID:          "cancel-workflow",
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "task", Action: "block", TimeoutMillis: 1000}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	type executeResult struct {
		run workflow.WorkflowRun
		err error
	}
	result := make(chan executeResult, 1)
	go func() {
		run, err := engine.Execute(context.Background(), id)
		result <- executeResult{run: run, err: err}
	}()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	if _, err := store.RequestCancel(context.Background(), id, time.Date(2026, 8, 19, 5, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}
	if err := engine.Cancel(context.Background(), id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("Execute() error = %v", got.err)
		}
		if got.run.Status != workflow.WorkflowCanceled {
			t.Fatalf("Execute() status = %s, want canceled", got.run.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not finish after cancellation")
	}
	persisted, err := store.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if persisted.Run.Status != workflow.WorkflowCanceled || persisted.Run.CancelRequestedAt == nil {
		t.Fatalf("persisted run = %+v, want canceled with durable request", persisted.Run)
	}
}

func TestPersistedCancellationIsHonoredDuringResume(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	creator, err := workflow.NewEngine(store, &recordingSuccessExecutor{called: make(chan struct{}, 1)}, workflow.EngineOptions{
		NewRunID: func() (workflow.RunID, error) { return "cancel-run", nil },
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	compiled, err := workflow.Compile(workflow.WorkflowDefinition{
		ID:          "cancel-workflow",
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := creator.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if _, err := store.RequestCancel(context.Background(), id, time.Date(2026, 8, 19, 6, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}

	executor := &recordingSuccessExecutor{called: make(chan struct{}, 1)}
	recovered, err := workflow.NewEngine(store, executor, workflow.EngineOptions{})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	run, err := recovered.Resume(context.Background(), id)
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if run.Status != workflow.WorkflowCanceled {
		t.Fatalf("Resume() status = %s, want canceled", run.Status)
	}
	select {
	case <-executor.called:
		t.Fatal("Resume() called Executor after persisted cancellation")
	default:
	}
}

func TestPersistedCancellationIsHonoredBeforeExecute(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executor := &recordingSuccessExecutor{called: make(chan struct{}, 1)}
	engine, err := workflow.NewEngine(store, executor, workflow.EngineOptions{
		NewRunID: func() (workflow.RunID, error) { return "cancel-run", nil },
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	compiled, err := workflow.Compile(workflow.WorkflowDefinition{
		ID:          "cancel-workflow",
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if _, err := store.RequestCancel(context.Background(), id, time.Date(2026, 8, 19, 7, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}

	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if run.Status != workflow.WorkflowCanceled {
		t.Fatalf("Execute() status = %s, want canceled", run.Status)
	}
	select {
	case <-executor.called:
		t.Fatal("Execute() called Executor after persisted cancellation")
	default:
	}
}

func TestRevisionConflictReconcilesPersistedCancellationBeforeNotification(t *testing.T) {
	base, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingApplyStore{
		base:         base,
		blockAt:      3,
		applyStarted: make(chan struct{}),
		releaseApply: make(chan struct{}),
	}
	executor := &controlledSuccessExecutor{started: make(chan struct{}), release: make(chan struct{})}
	engine, err := workflow.NewEngine(store, executor, workflow.EngineOptions{
		NewRunID: func() (workflow.RunID, error) { return "cancel-run", nil },
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	compiled, err := workflow.Compile(workflow.WorkflowDefinition{
		ID:          "cancel-workflow",
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	type executeResult struct {
		run workflow.WorkflowRun
		err error
	}
	result := make(chan executeResult, 1)
	go func() {
		run, err := engine.Execute(context.Background(), id)
		result <- executeResult{run: run, err: err}
	}()
	<-executor.started
	close(executor.release)
	select {
	case <-store.applyStarted:
	case <-time.After(time.Second):
		t.Fatal("completion Apply did not block")
	}
	if _, err := base.RequestCancel(context.Background(), id, time.Date(2026, 8, 19, 8, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}
	close(store.releaseApply)

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("Execute() error = %v", got.err)
		}
		if got.run.Status != workflow.WorkflowCanceled || got.run.CancelRequestedAt == nil {
			t.Fatalf("Execute() run = %+v, want canceled with durable request", got.run)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not reconcile persisted cancellation")
	}
}

func TestNewRejectsUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		return
	}
	if _, err := New(t.TempDir()); !errors.Is(err, workflow.ErrAtomicReplaceUnsupported) {
		t.Fatalf("New() error = %v, want ErrAtomicReplaceUnsupported", err)
	}
}
