package workflow

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestEngineExecutesDAGAfterDependenciesSucceed(t *testing.T) {
	store := newMemoryStore()
	executor := newRecordingExecutor(map[string]ExecutionResponse{
		"a": {Kind: ResultSuccess},
		"b": {Kind: ResultSuccess},
		"c": {Kind: ResultSuccess},
	})
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 2, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
		{Key: "c", Action: "c", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if executor.startedBefore("b", "a") || executor.startedBefore("c", "a") {
		t.Fatal("downstream task started before dependency succeeded")
	}
}

func TestEnginePassesDefinitionIDToExecutor(t *testing.T) {
	store := newMemoryStore()
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "clean", Action: "clean", TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if got := executor.definitionIDFor("clean"); got != "document-pipeline" {
		t.Fatalf("DefinitionID = %q, want document-pipeline", got)
	}
}

func TestEngineNeverExceedsConcurrencyLimit(t *testing.T) {
	store := newMemoryStore()
	executor := newGateExecutor()
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, independentWorkflow(5, 2))
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.Execute(context.Background(), id)
		done <- err
	}()
	executor.waitForStarted(t, 2)
	if got := executor.maxActive(); got != 2 {
		t.Fatalf("max active = %d, want 2", got)
	}
	executor.releaseAll()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEngineDoesNotStartExecutorWhenRunningStateCannotBeSaved(t *testing.T) {
	// Create 和 WorkflowRunning 保存成功，任务转为 running 的保存失败。
	store := newFailingStore(3)
	executor := newGateExecutor()
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(context.Background(), id); err == nil {
		t.Fatal("Execute() error = nil, want save failure")
	}
	select {
	case <-executor.started:
		t.Fatal("executor started before running state was saved")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRandomRunIDUses32HexCharacters(t *testing.T) {
	id, err := randomRunID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 32 {
		t.Fatalf("RunID length = %d, want 32", len(id))
	}
	if _, err := hex.DecodeString(string(id)); err != nil {
		t.Fatalf("RunID is not hexadecimal: %v", err)
	}
}

func TestEngineDoesNotUnlockDependencyTwice(t *testing.T) {
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
	}})
	now := time.Unix(100, 0)
	snapshot := newRunSnapshot("run-one", compiled, now)
	if err := transitionWorkflow(&snapshot, WorkflowRunning, now, "test"); err != nil {
		t.Fatal(err)
	}
	if err := transitionTask(&snapshot, 0, TaskRunning, now, "test"); err != nil {
		t.Fatal(err)
	}
	snapshot.Run.Tasks[0].Attempts = append(snapshot.Run.Tasks[0].Attempts, Attempt{Number: 1, Status: AttemptRunning, StartedAt: now})
	completion := executionCompletion{taskIndex: 0, attempt: 1, response: ExecutionResponse{Kind: ResultSuccess}, finished: now}
	if applied, err := applyCompletion(&snapshot, compiled, completion); err != nil || !applied {
		t.Fatalf("first completion = applied:%t err:%v, want applied", applied, err)
	}
	if applied, err := applyCompletion(&snapshot, compiled, completion); err != nil || applied {
		t.Fatalf("duplicate completion = applied:%t err:%v, want ignored", applied, err)
	}
	if got := snapshot.Run.RemainingDependencies[1]; got != 0 {
		t.Fatalf("remaining dependencies = %d, want 0", got)
	}
	if got := snapshot.Run.Tasks[1].Status; got != TaskReady {
		t.Fatalf("successor status = %s, want ready", got)
	}
}

func TestEngineTemporaryFailureWithoutRetriesFailsWorkflow(t *testing.T) {
	store := newMemoryStore()
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultTemporaryFailure, ErrorCode: "busy"}})
	engine := newTestEngine(store, executor)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 1}, 5000)
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowFailed || run.Tasks[0].Status != TaskFailed {
		t.Fatalf("workflow = %s, task = %s, want failed", run.Status, run.Tasks[0].Status)
	}
}

func TestEngineRetriesTemporaryFailureThenSucceeds(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMemoryStore()
	executor := newSequenceExecutor([]ExecutionResponse{
		{Kind: ResultTemporaryFailure, ErrorCode: "busy"},
		{Kind: ResultSuccess, Output: "ok"},
	})
	engine := newTestEngineWithClock(store, executor, clock)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 2, IntervalMillis: 1000}, 5000)
	done := executeAsync(engine, id)
	executor.waitForCalls(t, 1)
	clock.waitForTimers(t, 2)
	clock.Advance(time.Second)
	run := receiveRun(t, done)
	if run.Status != WorkflowSucceeded || len(run.Tasks[0].Attempts) != 2 {
		t.Fatalf("run = %+v, want succeeded with two attempts", run)
	}
	if run.Tasks[0].Attempts[0].Status != AttemptFailed || run.Tasks[0].Attempts[1].Status != AttemptSucceeded {
		t.Fatalf("attempt statuses = %s, %s", run.Tasks[0].Attempts[0].Status, run.Tasks[0].Attempts[1].Status)
	}
}

func TestEngineExhaustsRetryAndSkipsDescendants(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMemoryStore()
	executor := newSequenceExecutor([]ExecutionResponse{
		{Kind: ResultTemporaryFailure},
		{Kind: ResultTemporaryFailure},
	})
	engine := newTestEngineWithClock(store, executor, clock)
	id := createTwoTaskRun(t, engine, RetryPolicy{MaxAttempts: 2, IntervalMillis: 1000})
	done := executeAsync(engine, id)
	executor.waitForCalls(t, 1)
	clock.waitForTimers(t, 2)
	clock.Advance(time.Second)
	run := receiveRun(t, done)
	if run.Status != WorkflowFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	if run.Tasks[0].Status != TaskFailed || run.Tasks[1].Status != TaskSkipped {
		t.Fatalf("task statuses = %s, %s", run.Tasks[0].Status, run.Tasks[1].Status)
	}
}

func TestEngineTimesOutAttemptAndRetries(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMemoryStore()
	executor := newBlockingThenSuccessExecutor()
	engine := newTestEngineWithClock(store, executor, clock)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 2}, 1000)
	done := executeAsync(engine, id)
	executor.waitForCalls(t, 1)
	clock.waitForTimers(t, 1)
	clock.Advance(time.Second)
	run := receiveRun(t, done)
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if run.Tasks[0].Attempts[0].Status != AttemptTimedOut {
		t.Fatalf("first attempt = %s, want timed_out", run.Tasks[0].Attempts[0].Status)
	}
}

func TestEnginePermanentFailureDoesNotRetryAndSavesError(t *testing.T) {
	store := newMemoryStore()
	executor := newSequenceExecutor([]ExecutionResponse{{
		Kind: ResultPermanentFailure, ErrorCode: "invalid_input", ErrorMessage: "document is malformed",
	}})
	engine := newTestEngine(store, executor)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 3, IntervalMillis: 1000}, 5000)
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowFailed || len(run.Tasks[0].Attempts) != 1 {
		t.Fatalf("run = %+v, want failed with one attempt", run)
	}
	attempt := run.Tasks[0].Attempts[0]
	if attempt.Status != AttemptFailed || attempt.Result.ErrorCode != "invalid_input" || attempt.Result.ErrorMessage != "document is malformed" {
		t.Fatalf("attempt = %+v, want saved permanent failure", attempt)
	}
}

func TestEngineSkipsFailureDescendantsAndFinishesIndependentBranch(t *testing.T) {
	store := newMemoryStore()
	executor := newRecordingExecutor(map[string]ExecutionResponse{
		"fail":        {Kind: ResultPermanentFailure, ErrorCode: "failed"},
		"independent": {Kind: ResultSuccess},
	})
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "branches", Concurrency: 2, Tasks: []TaskDefinition{
		{Key: "fail", Action: "fail", TimeoutMillis: 1000},
		{Key: "child", Action: "child", DependsOn: []TaskKey{"fail"}, TimeoutMillis: 1000},
		{Key: "grandchild", Action: "grandchild", DependsOn: []TaskKey{"child"}, TimeoutMillis: 1000},
		{Key: "independent", Action: "independent", TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	want := []TaskStatus{TaskFailed, TaskSkipped, TaskSkipped, TaskSucceeded}
	for i, status := range want {
		if run.Tasks[i].Status != status {
			t.Fatalf("task %d status = %s, want %s", i, run.Tasks[i].Status, status)
		}
	}
	if executor.wasCalled("child") || executor.wasCalled("grandchild") || !executor.wasCalled("independent") {
		t.Fatal("descendants must not run and independent branch must run")
	}
}

func TestEngineDoesNotRetryWhenFailureStateCannotBeSaved(t *testing.T) {
	// Create、WorkflowRunning 和 AttemptRunning 保存成功，临时失败状态保存失败。
	store := newFailingStore(4)
	executor := newSequenceExecutor([]ExecutionResponse{
		{Kind: ResultTemporaryFailure},
		{Kind: ResultSuccess},
	})
	engine := newTestEngine(store, executor)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 2}, 5000)
	_, err := engine.Execute(context.Background(), id)
	if err == nil {
		t.Fatal("Execute() error = nil, want save failure")
	}
	executor.waitForCalls(t, 1)
	select {
	case <-executor.calls:
		t.Fatal("retry started before failure state was saved")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEngineDoesNotRetryWhenTimeoutStateCannotBeSaved(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	// Create、WorkflowRunning 和 AttemptRunning 保存成功，超时状态保存失败。
	store := newFailingStore(4)
	executor := newAlwaysBlockingExecutor()
	engine := newTestEngineWithClock(store, executor, clock)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 2}, 1000)
	done := executeAsync(engine, id)
	waitForSignal(t, executor.started, "first attempt")
	clock.waitForTimers(t, 1)
	clock.Advance(time.Second)

	select {
	case result := <-done:
		if result.err == nil {
			t.Fatalf("Execute() = %+v, nil; want save failure", result.run)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not return after timeout save failure")
	}
	select {
	case <-executor.started:
		t.Fatal("retry started before timeout state was saved")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEngineRetriesWhileIndependentTaskIsRunning(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMemoryStore()
	executor := newRetryWhileBlockedExecutor()
	engine := newTestEngineWithClock(store, executor, clock)
	compiled := mustCompile(t, WorkflowDefinition{ID: "parallel-retry", Concurrency: 2, Tasks: []TaskDefinition{
		{Key: "retry", Action: "retry", Retry: RetryPolicy{MaxAttempts: 2, IntervalMillis: 1000}, TimeoutMillis: 10_000},
		{Key: "blocked", Action: "blocked", TimeoutMillis: 10_000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	done := executeAsync(engine, id)
	waitForSignal(t, executor.retryStarted, "first retry attempt")
	waitForSignal(t, executor.blockedStarted, "independent task")
	// 两个 Attempt 超时计时器之外，还应注册固定间隔重试计时器。
	clock.waitForTimers(t, 3)
	clock.Advance(time.Second)
	waitForSignal(t, executor.retryStarted, "second retry attempt")
	close(executor.releaseBlocked)
	run := receiveRun(t, done)
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
}

func TestEngineTimeoutExhaustionSkipsDescendant(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMemoryStore()
	executor := newAlwaysBlockingExecutor()
	engine := newTestEngineWithClock(store, executor, clock)
	id := createTwoTaskRun(t, engine, RetryPolicy{MaxAttempts: 1})
	done := executeAsync(engine, id)
	waitForSignal(t, executor.started, "blocking attempt")
	clock.waitForTimers(t, 1)
	clock.Advance(time.Second)
	run := receiveRun(t, done)
	if run.Status != WorkflowFailed || run.Tasks[0].Status != TaskFailed || run.Tasks[1].Status != TaskSkipped {
		t.Fatalf("run = %+v, want timeout failure with skipped descendant", run)
	}
	if run.Tasks[0].Attempts[0].Status != AttemptTimedOut {
		t.Fatalf("attempt = %s, want timed_out", run.Tasks[0].Attempts[0].Status)
	}
}

func TestEngineUnknownResultLeavesSavedAttemptRunning(t *testing.T) {
	store := newMemoryStore()
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultKind("unknown")}})
	engine := newTestEngine(store, executor)
	id := createSingleTaskRun(t, engine, RetryPolicy{MaxAttempts: 1}, 5000)
	if _, err := engine.Execute(context.Background(), id); err == nil || !strings.Contains(err.Error(), "unsupported execution result") {
		t.Fatalf("Execute() error = %v, want unsupported execution result", err)
	}
	snapshot, err := store.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != WorkflowRunning || snapshot.Run.Tasks[0].Status != TaskRunning || snapshot.Run.Tasks[0].Attempts[0].Status != AttemptRunning {
		t.Fatalf("snapshot = %+v, want last saved running state", snapshot.Run)
	}
}

func TestExecuteTaskDropsCompletionAfterRunStops(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	executor := newDelayedReturnExecutor()
	engine := newTestEngine(newMemoryStore(), executor)
	completions := make(chan executionCompletion)
	control := engine.executeTask(
		runCtx,
		ExecutionRequest{RunID: "run-one", TaskKey: "a", Action: "a", Attempt: 1},
		0,
		1,
		time.Hour,
		completions,
		make(chan executionTimeout),
	)
	t.Cleanup(func() {
		control.cancel()
		close(control.done)
	})

	waitForSignal(t, executor.started, "executor start")
	cancelRun()
	close(executor.release)
	waitForSignal(t, executor.returned, "executor return")

	select {
	case completion := <-completions:
		t.Fatalf("received completion after run stopped: %+v", completion)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestExecuteTaskDropsTimeoutAfterRunStops(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	cancelRun()
	engine := newTestEngine(newMemoryStore(), newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}}))
	timeouts := make(chan executionTimeout, 1)
	control := engine.executeTask(
		runCtx,
		ExecutionRequest{RunID: "run-one", TaskKey: "a", Action: "a", Attempt: 1},
		0,
		1,
		20*time.Millisecond,
		make(chan executionCompletion),
		timeouts,
	)
	t.Cleanup(func() {
		control.cancel()
		close(control.done)
	})

	select {
	case timeout := <-timeouts:
		t.Fatalf("received timeout after run stopped: %+v", timeout)
	case <-time.After(100 * time.Millisecond):
	}
}
