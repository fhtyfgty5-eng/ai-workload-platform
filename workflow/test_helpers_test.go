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

// staleFirstLoadStore 在第一次 Load 复制快照后暂停，用于稳定复现 Execute 与 Cancel 的注册竞态。
type staleFirstLoadStore struct {
	*memoryStore
	// firstLoadStarted 表示第一次 Load 已复制旧快照；releaseFirstLoad 控制它何时返回。
	firstLoadStarted chan struct{}
	releaseFirstLoad chan struct{}
	// firstLoadOnce 保证只有第一次 Load 被暂停，Cancel 的并发 Load 可以立即完成。
	firstLoadOnce sync.Once
}

// blockingSaveStore 在指定写入序号暂停 Save，用于把取消请求固定在持久化窗口内。
type blockingSaveStore struct {
	*memoryStore
	// blockAt 使用与 memoryStore.writes 相同的从 1 开始写入序号。
	blockAt int
	// saveBlocked 表示目标 Save 已进入暂停点；releaseSave 允许该 Save 继续提交。
	saveBlocked chan struct{}
	releaseSave chan struct{}
	// blockOnce 防止异常重复写入序号造成 channel 重复关闭。
	blockOnce sync.Once
}

// cancelOnSaveStore 在指定 Save 前取消父 Context，用于验证清理写入不继承取消信号。
type cancelOnSaveStore struct {
	*memoryStore
	// cancelAt 是触发取消的写入序号，cancel 是测试持有的父 Context 取消函数。
	cancelAt int
	cancel   context.CancelFunc
	// once 保证父 Context 只被该夹具取消一次。
	once sync.Once
}

func newCancelOnSaveStore(cancelAt int, cancel context.CancelFunc) *cancelOnSaveStore {
	return &cancelOnSaveStore{memoryStore: newMemoryStore(), cancelAt: cancelAt, cancel: cancel}
}

func (s *cancelOnSaveStore) Save(ctx context.Context, snapshot RunSnapshot) error {
	s.memoryStore.mu.Lock()
	nextWrite := s.memoryStore.writes + 1
	s.memoryStore.mu.Unlock()
	if nextWrite == s.cancelAt {
		s.once.Do(s.cancel)
	}
	return s.memoryStore.Save(ctx, snapshot)
}

func newBlockingSaveStore(blockAt int) *blockingSaveStore {
	return &blockingSaveStore{
		memoryStore: newMemoryStore(),
		blockAt:     blockAt,
		saveBlocked: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
}

func (s *blockingSaveStore) Save(ctx context.Context, snapshot RunSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.memoryStore.mu.Lock()
	defer s.memoryStore.mu.Unlock()
	if err := s.memoryStore.mutate(); err != nil {
		return err
	}
	if s.memoryStore.writes == s.blockAt {
		s.blockOnce.Do(func() {
			close(s.saveBlocked)
			<-s.releaseSave
		})
	}
	if _, exists := s.memoryStore.snapshots[snapshot.Run.ID]; !exists {
		return ErrRunNotFound
	}
	s.memoryStore.snapshots[snapshot.Run.ID] = cloneSnapshot(snapshot)
	return nil
}

func newStaleFirstLoadStore() *staleFirstLoadStore {
	return &staleFirstLoadStore{
		memoryStore:      newMemoryStore(),
		firstLoadStarted: make(chan struct{}),
		releaseFirstLoad: make(chan struct{}),
	}
}

func (s *staleFirstLoadStore) Load(ctx context.Context, id RunID) (RunSnapshot, error) {
	s.memoryStore.mu.Lock()
	snapshot, exists := s.memoryStore.snapshots[id]
	s.memoryStore.mu.Unlock()
	if !exists {
		return RunSnapshot{}, ErrRunNotFound
	}
	blocked := false
	s.firstLoadOnce.Do(func() {
		blocked = true
		close(s.firstLoadStarted)
	})
	if blocked {
		select {
		case <-s.releaseFirstLoad:
		case <-ctx.Done():
			return RunSnapshot{}, ctx.Err()
		}
	}
	return cloneSnapshot(snapshot), nil
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
	input        map[string]any
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
		input:        request.Input,
		sequence:     len(e.calls),
	})
	if response, ok := e.responses[request.Action]; ok {
		return response
	}
	return ExecutionResponse{Kind: ResultSuccess}
}

func (e *recordingExecutor) inputFor(action string) map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, call := range e.calls {
		if call.action == action {
			return call.input
		}
	}
	return nil
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

func newTestEngine(store RunStore, executor Executor) *Engine {
	return newTestEngineWithClock(store, executor, RealClock{})
}

func newTestEngineWithClock(store RunStore, executor Executor, clock Clock) *Engine {
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

type systemErrorExecutor struct {
	// started 报告两个并行任务均已进入 Executor，避免测试依赖 goroutine 调度顺序。
	started chan TaskKey
	// releaseInvalid 控制非法结果何时返回；blockedCanceled 证明另一 Attempt 收到取消。
	releaseInvalid  chan struct{}
	blockedCanceled chan struct{}
}

func newSystemErrorExecutor() *systemErrorExecutor {
	return &systemErrorExecutor{
		started:         make(chan TaskKey, 2),
		releaseInvalid:  make(chan struct{}),
		blockedCanceled: make(chan struct{}),
	}
}

func (e *systemErrorExecutor) Execute(ctx context.Context, request ExecutionRequest) ExecutionResponse {
	e.started <- request.TaskKey
	if request.Action == "invalid" {
		<-e.releaseInvalid
		return ExecutionResponse{Kind: ResultKind("unknown")}
	}
	<-ctx.Done()
	close(e.blockedCanceled)
	return ExecutionResponse{Kind: ResultCanceled}
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

func waitForTaskStarts(t *testing.T, started <-chan TaskKey, want ...TaskKey) {
	t.Helper()
	remaining := make(map[TaskKey]bool, len(want))
	for _, key := range want {
		remaining[key] = true
	}
	deadline := time.After(time.Second)
	for len(remaining) > 0 {
		select {
		case key := <-started:
			delete(remaining, key)
		case <-deadline:
			t.Fatalf("tasks not started: %v", remaining)
		}
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
	return executeAsyncWithContext(engine, context.Background(), id)
}

func executeAsyncWithContext(engine *Engine, ctx context.Context, id RunID) <-chan runResult {
	done := make(chan runResult, 1)
	go func() {
		run, err := engine.Execute(ctx, id)
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
