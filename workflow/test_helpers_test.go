package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func cloneSnapshot(in RunSnapshot) RunSnapshot {
	data, _ := json.Marshal(in)
	var out RunSnapshot
	_ = json.Unmarshal(data, &out)
	return out
}

type memoryStore struct {
	mu        sync.Mutex
	snapshots map[RunID]RunSnapshot
	writes    int
	failAt    int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{snapshots: make(map[RunID]RunSnapshot)}
}

func newFailingStore(failAt int) *memoryStore {
	store := newMemoryStore()
	store.failAt = failAt
	return store
}

func (s *memoryStore) mutate() error {
	s.writes++
	if s.failAt > 0 && s.writes == s.failAt {
		return errors.New("injected store failure")
	}
	return nil
}

func (s *memoryStore) Create(ctx context.Context, snapshot RunSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutate(); err != nil {
		return err
	}
	if _, exists := s.snapshots[snapshot.Run.ID]; exists {
		return ErrRunExists
	}
	s.snapshots[snapshot.Run.ID] = cloneSnapshot(snapshot)
	return nil
}

func (s *memoryStore) Save(ctx context.Context, snapshot RunSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutate(); err != nil {
		return err
	}
	if _, exists := s.snapshots[snapshot.Run.ID]; !exists {
		return ErrRunNotFound
	}
	s.snapshots[snapshot.Run.ID] = cloneSnapshot(snapshot)
	return nil
}

func (s *memoryStore) Load(ctx context.Context, id RunID) (RunSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return RunSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, exists := s.snapshots[id]
	if !exists {
		return RunSnapshot{}, ErrRunNotFound
	}
	return cloneSnapshot(snapshot), nil
}

type manualTimer struct {
	at time.Time
	ch chan time.Time
}

// manualClock 允许后续重试和超时测试主动推进时间，避免测试依赖真实等待。
type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []manualTimer
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		close(ch)
		return ch
	}
	c.timers = append(c.timers, manualTimer{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, timer := range c.timers {
		if !timer.at.After(c.now) {
			timer.ch <- c.now
			close(timer.ch)
			continue
		}
		remaining = append(remaining, timer)
	}
	c.timers = remaining
	c.mu.Unlock()
}

func (c *manualClock) waitForTimers(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		got := len(c.timers)
		c.mu.Unlock()
		if got >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timer count = %d, want at least %d", got, count)
		}
		runtime.Gosched()
	}
}

type recordedCall struct {
	action       string
	definitionID string
	sequence     int
}

type recordingExecutor struct {
	mu        sync.Mutex
	responses map[string]ExecutionResponse
	calls     []recordedCall
}

func newRecordingExecutor(responses map[string]ExecutionResponse) *recordingExecutor {
	return &recordingExecutor{responses: responses}
}

func (e *recordingExecutor) Execute(_ context.Context, request ExecutionRequest) ExecutionResponse {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, recordedCall{
		action:       request.Action,
		definitionID: request.DefinitionID,
		sequence:     len(e.calls),
	})
	if response, ok := e.responses[request.Action]; ok {
		return response
	}
	return ExecutionResponse{Kind: ResultSuccess}
}

func (e *recordingExecutor) definitionIDFor(action string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, call := range e.calls {
		if call.action == action {
			return call.definitionID
		}
	}
	return ""
}

func (e *recordingExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *recordingExecutor) wasCalled(action string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, call := range e.calls {
		if call.action == action {
			return true
		}
	}
	return false
}

func (e *recordingExecutor) startedBefore(action, dependency string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	positions := make(map[string]int, len(e.calls))
	for _, call := range e.calls {
		positions[call.action] = call.sequence
	}
	a, actionStarted := positions[action]
	dependencyIndex, dependencyStarted := positions[dependency]
	return actionStarted && (!dependencyStarted || a < dependencyIndex)
}

// gateExecutor 在测试释放前阻塞，以观测调度器实际启动的并发数。
type gateExecutor struct {
	mu      sync.Mutex
	active  int
	maximum int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGateExecutor() *gateExecutor {
	return &gateExecutor{
		started: make(chan struct{}, 1024),
		release: make(chan struct{}),
	}
}

func (e *gateExecutor) Execute(ctx context.Context, _ ExecutionRequest) ExecutionResponse {
	e.mu.Lock()
	e.active++
	if e.active > e.maximum {
		e.maximum = e.active
	}
	e.mu.Unlock()
	e.started <- struct{}{}
	select {
	case <-ctx.Done():
	case <-e.release:
	}
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	if ctx.Err() != nil {
		return ExecutionResponse{Kind: ResultCanceled}
	}
	return ExecutionResponse{Kind: ResultSuccess}
}

func (e *gateExecutor) waitForStarted(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-e.started:
		case <-time.After(time.Second):
			t.Fatal("executor did not start")
		}
	}
}

func (e *gateExecutor) maxActive() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.maximum
}

func (e *gateExecutor) releaseAll() {
	e.once.Do(func() { close(e.release) })
}

func mustCompile(t *testing.T, definition WorkflowDefinition) *CompiledWorkflow {
	t.Helper()
	compiled, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func newTestEngine(store Store, executor Executor) *Engine {
	return newTestEngineWithClock(store, executor, RealClock{})
}

func newTestEngineWithClock(store Store, executor Executor, clock Clock) *Engine {
	engine, err := NewEngine(store, executor, EngineOptions{
		Clock: clock,
		NewRunID: func() (RunID, error) {
			return "run-one", nil
		},
	})
	if err != nil {
		panic(err)
	}
	return engine
}

type sequenceExecutor struct {
	mu        sync.Mutex
	responses []ExecutionResponse
	calls     chan struct{}
}

type blockingThenSuccessExecutor struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
}

type alwaysBlockingExecutor struct {
	started chan struct{}
}

type delayedReturnExecutor struct {
	started  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func newDelayedReturnExecutor() *delayedReturnExecutor {
	return &delayedReturnExecutor{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (e *delayedReturnExecutor) Execute(context.Context, ExecutionRequest) ExecutionResponse {
	e.started <- struct{}{}
	<-e.release
	close(e.returned)
	return ExecutionResponse{Kind: ResultSuccess}
}

func newAlwaysBlockingExecutor() *alwaysBlockingExecutor {
	return &alwaysBlockingExecutor{started: make(chan struct{}, 1)}
}

func (e *alwaysBlockingExecutor) Execute(ctx context.Context, _ ExecutionRequest) ExecutionResponse {
	e.started <- struct{}{}
	<-ctx.Done()
	return ExecutionResponse{Kind: ResultCanceled}
}

type retryWhileBlockedExecutor struct {
	mu             sync.Mutex
	retryCalls     int
	retryStarted   chan struct{}
	blockedStarted chan struct{}
	releaseBlocked chan struct{}
}

func newRetryWhileBlockedExecutor() *retryWhileBlockedExecutor {
	return &retryWhileBlockedExecutor{
		retryStarted:   make(chan struct{}, 2),
		blockedStarted: make(chan struct{}, 1),
		releaseBlocked: make(chan struct{}),
	}
}

func (e *retryWhileBlockedExecutor) Execute(ctx context.Context, request ExecutionRequest) ExecutionResponse {
	if request.Action == "retry" {
		e.mu.Lock()
		e.retryCalls++
		call := e.retryCalls
		e.mu.Unlock()
		e.retryStarted <- struct{}{}
		if call == 1 {
			return ExecutionResponse{Kind: ResultTemporaryFailure}
		}
		return ExecutionResponse{Kind: ResultSuccess}
	}
	e.blockedStarted <- struct{}{}
	select {
	case <-ctx.Done():
		return ExecutionResponse{Kind: ResultCanceled}
	case <-e.releaseBlocked:
		return ExecutionResponse{Kind: ResultSuccess}
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("%s not observed", name)
	}
}

func newBlockingThenSuccessExecutor() *blockingThenSuccessExecutor {
	return &blockingThenSuccessExecutor{started: make(chan struct{}, 2)}
}

func (e *blockingThenSuccessExecutor) Execute(ctx context.Context, _ ExecutionRequest) ExecutionResponse {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	e.started <- struct{}{}
	if call == 1 {
		<-ctx.Done()
		return ExecutionResponse{Kind: ResultCanceled}
	}
	return ExecutionResponse{Kind: ResultSuccess}
}

func (e *blockingThenSuccessExecutor) waitForCalls(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-e.started:
		case <-time.After(time.Second):
			t.Fatal("executor call not observed")
		}
	}
}

func newSequenceExecutor(responses []ExecutionResponse) *sequenceExecutor {
	return &sequenceExecutor{responses: responses, calls: make(chan struct{}, 1024)}
}

func (e *sequenceExecutor) Execute(_ context.Context, _ ExecutionRequest) ExecutionResponse {
	e.mu.Lock()
	response := e.responses[0]
	e.responses = e.responses[1:]
	e.mu.Unlock()
	e.calls <- struct{}{}
	return response
}

func (e *sequenceExecutor) waitForCalls(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-e.calls:
		case <-time.After(time.Second):
			t.Fatal("executor call not observed")
		}
	}
}

func createSingleTaskRun(t *testing.T, engine *Engine, retry RetryPolicy, timeout int64) RunID {
	t.Helper()
	return createRun(t, engine, WorkflowDefinition{ID: "single", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "a", Action: "a", Retry: retry, TimeoutMillis: timeout,
	}}})
}

func createTwoTaskRun(t *testing.T, engine *Engine, retry RetryPolicy) RunID {
	t.Helper()
	return createRun(t, engine, WorkflowDefinition{ID: "two", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", Retry: retry, TimeoutMillis: 1000},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
	}})
}

func createRun(t *testing.T, engine *Engine, definition WorkflowDefinition) RunID {
	t.Helper()
	id, err := engine.CreateRun(context.Background(), mustCompile(t, definition))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type runResult struct {
	run WorkflowRun
	err error
}

func executeAsync(engine *Engine, id RunID) <-chan runResult {
	done := make(chan runResult, 1)
	go func() {
		run, err := engine.Execute(context.Background(), id)
		done <- runResult{run: run, err: err}
	}()
	return done
}

func receiveRun(t *testing.T, done <-chan runResult) WorkflowRun {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.run
	case <-time.After(time.Second):
		t.Fatal("engine did not finish")
		return WorkflowRun{}
	}
}

func independentWorkflow(tasks, concurrency int) WorkflowDefinition {
	definition := WorkflowDefinition{ID: "independent", Concurrency: concurrency}
	for i := range tasks {
		definition.Tasks = append(definition.Tasks, TaskDefinition{
			Key:           TaskKey(fmt.Sprintf("task-%d", i)),
			Action:        fmt.Sprintf("task-%d", i),
			TimeoutMillis: 1000,
		})
	}
	return definition
}
