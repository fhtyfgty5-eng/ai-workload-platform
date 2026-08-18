package workflow

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestResumeMarksRunningAttemptInterruptedAndRetries(t *testing.T) {
	store := newMemoryStore()
	snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	store.snapshots["run-one"] = snapshot
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if got := run.Tasks[0].Attempts[0].Status; got != AttemptInterrupted {
		t.Fatalf("first attempt = %s, want interrupted", got)
	}
	if got := len(run.Tasks[0].Attempts); got != 2 {
		t.Fatalf("attempt count = %d, want 2", got)
	}
	if got := run.Tasks[0].Attempts[1].Status; got != AttemptSucceeded {
		t.Fatalf("second attempt = %s, want succeeded", got)
	}
}

func TestResumePreservesFutureRetryReadyAt(t *testing.T) {
	now := time.Unix(100, 0)
	readyAt := now.Add(5 * time.Second)
	clock := newManualClock(now)
	store := newMemoryStore()
	store.snapshots["run-one"] = waitingRetrySnapshot(t, "run-one", readyAt)
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngineWithClock(store, executor, clock)

	done := resumeAsync(engine, "run-one")
	clock.waitForTimers(t, 1)
	stored, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Run.Tasks[0].ReadyAt; got == nil || !got.Equal(readyAt) {
		t.Fatalf("ReadyAt = %v, want %v", got, readyAt)
	}

	clock.Advance(4 * time.Second)
	assertExecutorNotCalled(t, executor)
	clock.Advance(time.Second)
	executor.waitForCalls(t, 1)
	run := receiveRun(t, done)
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
}

func TestResumeRecomputesDependenciesInsteadOfTrustingSavedCount(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["run-one"] = snapshotWithSucceededParentAndCorruptRemainingCount(t, "run-one", 99)
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if got := run.RemainingDependencies[1]; got != 0 {
		t.Fatalf("remaining dependencies = %d, want 0", got)
	}
	if got := run.Tasks[1].Status; got != TaskSucceeded {
		t.Fatalf("child status = %s, want succeeded", got)
	}
	if executor.wasCalled("a") || !executor.wasCalled("b") {
		t.Fatalf("executor calls = %+v, want only child task b", executor.calls)
	}
}

func TestResumeRetriesImmediatelyWhenReadyAtExpired(t *testing.T) {
	now := time.Unix(100, 0)
	clock := newManualClock(now)
	store := newMemoryStore()
	store.snapshots["run-one"] = waitingRetrySnapshot(t, "run-one", now.Add(-time.Second))
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngineWithClock(store, executor, clock)

	// 不推进人工时钟；过期的 ReadyAt 必须在恢复后立即触发下一次 Attempt。
	done := resumeAsync(engine, "run-one")
	executor.waitForCalls(t, 1)
	run := receiveRun(t, done)
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if got := len(run.Tasks[0].Attempts); got != 2 {
		t.Fatalf("attempt count = %d, want 2", got)
	}
}

func TestResumePropagatesFailureWhenTaskArrayIsNotTopological(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["run-one"] = nonTopologicalFailureSnapshot(t, "run-one")
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	if got := run.Tasks[0].Status; got != TaskSkipped {
		t.Fatalf("grandchild status = %s, want skipped", got)
	}
	if got := run.Tasks[1].Status; got != TaskSkipped {
		t.Fatalf("child status = %s, want skipped", got)
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
}

func TestResumeMayExecuteAgainWhenSuccessWasNotSaved(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	// 外部执行可能已成功，但持久化事实仍是 running；恢复只能按未完成处理并再次执行。
	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if got := executor.callCount(); got != 1 {
		t.Fatalf("executor call count = %d, want 1", got)
	}
	if got := run.Tasks[0].Attempts[0].Status; got != AttemptInterrupted {
		t.Fatalf("first attempt = %s, want interrupted", got)
	}
	if got := run.Tasks[0].Attempts[1].Number; got != 2 {
		t.Fatalf("second attempt number = %d, want 2", got)
	}
}

func TestResumeDoesNotExecuteSucceededTask(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["run-one"] = succeededSingleTaskSnapshot(t, "run-one")
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
	if got := len(run.Tasks[0].Attempts); got != 1 {
		t.Fatalf("attempt count = %d, want 1", got)
	}
}

func TestResumeInterruptedAttemptExhaustsMaxAttempts(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 1})
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowFailed || run.Tasks[0].Status != TaskFailed {
		t.Fatalf("workflow = %s, task = %s; want failed", run.Status, run.Tasks[0].Status)
	}
	if got := run.Tasks[0].Attempts[0].Status; got != AttemptInterrupted {
		t.Fatalf("attempt = %s, want interrupted", got)
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
}

func TestResumeReturnsTerminalWorkflowUnchanged(t *testing.T) {
	for _, status := range []WorkflowStatus{WorkflowSucceeded, WorkflowFailed, WorkflowCanceled} {
		t.Run(string(status), func(t *testing.T) {
			store := newMemoryStore()
			snapshot := terminalSingleTaskSnapshot(t, "run-one", status)
			// 终态幂等返回不依赖定义，即使定义缺失也不能重新编译或写回。
			snapshot.Definition = nil
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			run, err := engine.Resume(context.Background(), "run-one")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(run, snapshot.Run) {
				t.Fatalf("run = %+v, want unchanged %+v", run, snapshot.Run)
			}
			if store.writes != 0 {
				t.Fatalf("store writes = %d, want 0", store.writes)
			}
			if got := executor.callCount(); got != 0 {
				t.Fatalf("executor call count = %d, want 0", got)
			}
		})
	}
}

func TestResumeRejectsMissingOrInvalidDefinitionWithoutExecution(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
		want   string
	}{
		{
			name: "missing definition",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Definition = nil
			},
			want: "no stored definition",
		},
		{
			name: "invalid definition",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Definition.ID = "INVALID"
			},
			want: "compile stored definition",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
			test.mutate(&snapshot)
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			_, err := engine.Resume(context.Background(), "run-one")
			assertResumeRejectedWithoutSideEffects(t, err, test.want, store, executor)
		})
	}
}

func TestResumeRejectsTaskKeyMismatchWithoutExecution(t *testing.T) {
	store := newMemoryStore()
	snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	snapshot.Run.Tasks[0].Key = "other"
	store.snapshots["run-one"] = snapshot
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	_, err := engine.Resume(context.Background(), "run-one")
	assertResumeRejectedWithoutSideEffects(t, err, "does not match definition key", store, executor)
}

func TestResumeRejectsRunIDMismatchWithoutCrossRunWrite(t *testing.T) {
	store := newMemoryStore()
	mismatched := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	mismatched.Run.ID = "run-two"
	other := succeededSingleTaskSnapshot(t, "run-two")
	store.snapshots["run-one"] = mismatched
	store.snapshots["run-two"] = other
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	_, err := engine.Resume(context.Background(), "run-one")
	assertResumeRejectedWithoutSideEffects(t, err, "does not match requested run", store, executor)
	storedOther, loadErr := store.Load(context.Background(), "run-two")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if !reflect.DeepEqual(storedOther, other) {
		t.Fatalf("run-two changed through mismatched run-one snapshot\ngot:  %+v\nwant: %+v", storedOther, other)
	}
}

func TestResumeRejectsSnapshotIdentityAndVersionWithoutExecution(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
		want   string
	}{
		{
			name: "unsupported version",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Version = 2
			},
			want: "unsupported snapshot version",
		},
		{
			name: "definition id mismatch",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.DefinitionID = "other-definition"
			},
			want: "definition id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
			test.mutate(&snapshot)
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			_, err := engine.Resume(context.Background(), "run-one")
			assertResumeRejectedWithoutSideEffects(t, err, test.want, store, executor)
		})
	}
}

func TestResumeRejectsTerminalSnapshotIdentityAndVersion(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
		want   string
	}{
		{
			name: "unsupported version",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Version = 2
			},
			want: "unsupported snapshot version",
		},
		{
			name: "run id mismatch",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.ID = "run-two"
			},
			want: "does not match requested run",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			snapshot := terminalSingleTaskSnapshot(t, "run-one", WorkflowSucceeded)
			test.mutate(&snapshot)
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			_, err := engine.Resume(context.Background(), "run-one")
			assertResumeRejectedWithoutSideEffects(t, err, test.want, store, executor)
		})
	}
}

func TestResumeAcceptsValidPersistedTaskStates(t *testing.T) {
	for _, status := range []AttemptStatus{AttemptFailed, AttemptTimedOut, AttemptInterrupted} {
		t.Run("ready after "+string(status), func(t *testing.T) {
			store := newMemoryStore()
			snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
			finished := time.Unix(3, 0)
			task := &snapshot.Run.Tasks[0]
			task.Status = TaskReady
			task.Attempts[0].Status = status
			task.Attempts[0].FinishedAt = &finished
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			run, err := engine.Resume(context.Background(), "run-one")
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != WorkflowSucceeded || len(run.Tasks[0].Attempts) != 2 {
				t.Fatalf("run = %+v, want succeeded with second attempt", run)
			}
		})
	}

	t.Run("permanent failure before max attempts", func(t *testing.T) {
		store := newMemoryStore()
		snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
		finished := time.Unix(3, 0)
		task := &snapshot.Run.Tasks[0]
		task.Status = TaskFailed
		task.FinishedAt = &finished
		task.Attempts[0].Status = AttemptFailed
		task.Attempts[0].FinishedAt = &finished
		store.snapshots["run-one"] = snapshot
		executor := newRecordingExecutor(nil)
		engine := newTestEngine(store, executor)

		run, err := engine.Resume(context.Background(), "run-one")
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != WorkflowFailed {
			t.Fatalf("status = %s, want failed", run.Status)
		}
		if got := executor.callCount(); got != 0 {
			t.Fatalf("executor call count = %d, want 0", got)
		}
	})
}

func TestResumeRejectsInvalidAttemptHistoryWithoutExecution(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
		want   string
	}{
		{
			name: "waiting retry exhausted attempts",
			mutate: func(snapshot *RunSnapshot) {
				finished := time.Unix(4, 0)
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskWaitingRetry
				task.ReadyAt = &finished
				task.Attempts[0].Status = AttemptFailed
				task.Attempts[0].FinishedAt = &finished
				task.Attempts = append(task.Attempts, Attempt{
					Number: 2, Status: AttemptFailed, StartedAt: time.Unix(3, 0), FinishedAt: &finished,
				})
			},
			want: "exhausted retry attempts",
		},
		{
			name: "ready exhausted attempts",
			mutate: func(snapshot *RunSnapshot) {
				finished := time.Unix(4, 0)
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskReady
				task.Attempts[0].Status = AttemptFailed
				task.Attempts[0].FinishedAt = &finished
				task.Attempts = append(task.Attempts, Attempt{
					Number: 2, Status: AttemptFailed, StartedAt: time.Unix(3, 0), FinishedAt: &finished,
				})
			},
			want: "exhausted retry attempts",
		},
		{
			name: "nonsequential attempt number",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.Tasks[0].Attempts[0].Number = 2
			},
			want: "attempt number",
		},
		{
			name: "historical running attempt",
			mutate: func(snapshot *RunSnapshot) {
				finished := time.Unix(4, 0)
				snapshot.Definition.Tasks[0].Retry.MaxAttempts = 3
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskWaitingRetry
				task.ReadyAt = &finished
				last := task.Attempts[0]
				last.Number = 2
				last.Status = AttemptFailed
				last.FinishedAt = &finished
				task.Attempts = []Attempt{
					{Number: 1, Status: AttemptRunning, StartedAt: time.Unix(2, 0)},
					last,
				}
			},
			want: "historical running attempt",
		},
		{
			name: "succeeded task after failed attempt",
			mutate: func(snapshot *RunSnapshot) {
				finished := time.Unix(4, 0)
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskSucceeded
				task.FinishedAt = &finished
				task.Attempts[0].Status = AttemptFailed
				task.Attempts[0].FinishedAt = &finished
			},
			want: "succeeded after attempt status",
		},
		{
			name: "ready with retry time",
			mutate: func(snapshot *RunSnapshot) {
				readyAt := time.Unix(100, 0)
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskReady
				task.ReadyAt = &readyAt
				task.Attempts = nil
			},
			want: "ready_at outside waiting_retry",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
			test.mutate(&snapshot)
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			_, err := engine.Resume(context.Background(), "run-one")
			assertResumeRejectedWithoutSideEffects(t, err, test.want, store, executor)
		})
	}
}

func TestResumeRejectsCorruptSnapshotWithoutExecution(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
		want   string
	}{
		{
			name: "unsupported workflow status",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.Status = WorkflowStatus("corrupt")
			},
			want: "unsupported workflow status",
		},
		{
			name: "task count mismatch",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.Tasks = nil
			},
			want: "stored task count",
		},
		{
			name: "running without running attempt",
			mutate: func(snapshot *RunSnapshot) {
				snapshot.Run.Tasks[0].Attempts = nil
			},
			want: "running without a running attempt",
		},
		{
			name: "waiting retry without ready at",
			mutate: func(snapshot *RunSnapshot) {
				task := &snapshot.Run.Tasks[0]
				task.Status = TaskWaitingRetry
				task.Attempts[0].Status = AttemptFailed
			},
			want: "waiting_retry without attempt or ready_at",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore()
			snapshot := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
			test.mutate(&snapshot)
			store.snapshots["run-one"] = snapshot
			executor := newRecordingExecutor(nil)
			engine := newTestEngine(store, executor)

			_, err := engine.Resume(context.Background(), "run-one")
			assertResumeRejectedWithoutSideEffects(t, err, test.want, store, executor)
		})
	}
}

func TestResumeDoesNotExecuteWhenRecoverySaveFails(t *testing.T) {
	store := newFailingStore(1)
	original := runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	store.snapshots["run-one"] = original
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	if _, err := engine.Resume(context.Background(), "run-one"); err == nil {
		t.Fatal("Resume() error = nil, want recovery save failure")
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
	engine.mu.Lock()
	cached := engine.compiled["run-one"]
	engine.mu.Unlock()
	if cached != nil {
		t.Fatal("compiled definition cached before recovery snapshot was saved")
	}
	stored, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, original) {
		t.Fatalf("stored snapshot changed after failed recovery save\ngot:  %+v\nwant: %+v", stored, original)
	}
}

func TestResumeAndExecuteRejectConcurrentRun(t *testing.T) {
	store := newStaleFirstLoadStore()
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngine(store, executor)

	done := resumeAsync(engine, "run-one")
	waitForSignal(t, store.firstLoadStarted, "Resume first Load")
	_, err := engine.Execute(context.Background(), "run-one")
	close(store.releaseFirstLoad)
	if err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("Execute() error = %v, want already executing", err)
	}
	if run := receiveRun(t, done); run.Status != WorkflowSucceeded {
		t.Fatalf("resumed status = %s, want succeeded", run.Status)
	}
}

func TestExecuteAndResumeRejectConcurrentRun(t *testing.T) {
	store := newStaleFirstLoadStore()
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "execute-first",
		Concurrency: 1,
		Tasks:       []TaskDefinition{{Key: "a", Action: "a", TimeoutMillis: 1000}},
	})
	store.snapshots["run-one"] = newRunSnapshot("run-one", compiled, time.Unix(1, 0))
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngine(store, executor)

	done := executeAsync(engine, "run-one")
	waitForSignal(t, store.firstLoadStarted, "Execute first Load")
	_, err := engine.Resume(context.Background(), "run-one")
	close(store.releaseFirstLoad)
	if err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("Resume() error = %v, want already executing", err)
	}
	if run := receiveRun(t, done); run.Status != WorkflowSucceeded {
		t.Fatalf("executed status = %s, want succeeded", run.Status)
	}
}

func TestResumeRejectsConcurrentResume(t *testing.T) {
	store := newStaleFirstLoadStore()
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngine(store, executor)

	done := resumeAsync(engine, "run-one")
	waitForSignal(t, store.firstLoadStarted, "first Resume Load")
	_, err := engine.Resume(context.Background(), "run-one")
	close(store.releaseFirstLoad)
	if err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("second Resume() error = %v, want already executing", err)
	}
	if run := receiveRun(t, done); run.Status != WorkflowSucceeded {
		t.Fatalf("resumed status = %s, want succeeded", run.Status)
	}
}

func TestResumeReleasesActiveAfterFailure(t *testing.T) {
	store := newFailingStore(1)
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	executor := newSequenceExecutor([]ExecutionResponse{{Kind: ResultSuccess}})
	engine := newTestEngine(store, executor)

	if _, err := engine.Resume(context.Background(), "run-one"); err == nil {
		t.Fatal("first Resume() error = nil, want recovery save failure")
	}
	run, err := engine.Resume(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("second Resume() status = %s, want succeeded", run.Status)
	}
}

func TestResumeParentCancellationPersistsCanceledState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newCancelOnSaveStore(1, cancel)
	store.snapshots["run-one"] = runningSnapshotWithOneAttempt(t, "run-one", RetryPolicy{MaxAttempts: 2})
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)

	run, err := engine.Resume(ctx, "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowCanceled || run.Tasks[0].Status != TaskCanceled {
		t.Fatalf("workflow = %s, task = %s; want canceled", run.Status, run.Tasks[0].Status)
	}
	if got := run.Tasks[0].Attempts[0].Status; got != AttemptCanceled {
		t.Fatalf("attempt = %s, want canceled instead of interrupted", got)
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
	stored, loadErr := store.Load(context.Background(), "run-one")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if stored.Run.Status != WorkflowCanceled {
		t.Fatalf("stored status = %s, want canceled", stored.Run.Status)
	}
}

// runningSnapshotWithOneAttempt 构造进程崩溃前已持久化 running Attempt 的恢复输入。
func runningSnapshotWithOneAttempt(t *testing.T, id RunID, retry RetryPolicy) RunSnapshot {
	t.Helper()
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "resume-one",
		Concurrency: 1,
		Tasks: []TaskDefinition{{
			Key:           "a",
			Action:        "a",
			Retry:         retry,
			TimeoutMillis: 1000,
		}},
	})
	snapshot := newRunSnapshot(id, compiled, time.Unix(1, 0))
	started := time.Unix(2, 0)
	snapshot.Run.Status = WorkflowRunning
	snapshot.Run.StartedAt = &started
	snapshot.Run.Tasks[0].Status = TaskRunning
	snapshot.Run.Tasks[0].Attempts = []Attempt{{
		Number:    1,
		Status:    AttemptRunning,
		StartedAt: started,
	}}
	return snapshot
}

// waitingRetrySnapshot 保留崩溃前已经确定的绝对重试时间，恢复不能重新计算等待区间。
func waitingRetrySnapshot(t *testing.T, id RunID, readyAt time.Time) RunSnapshot {
	t.Helper()
	snapshot := runningSnapshotWithOneAttempt(t, id, RetryPolicy{MaxAttempts: 2, IntervalMillis: 5000})
	finished := readyAt.Add(-5 * time.Second)
	snapshot.Run.Tasks[0].Status = TaskWaitingRetry
	snapshot.Run.Tasks[0].ReadyAt = &readyAt
	snapshot.Run.Tasks[0].Attempts[0].Status = AttemptFailed
	snapshot.Run.Tasks[0].Attempts[0].FinishedAt = &finished
	return snapshot
}

// snapshotWithSucceededParentAndCorruptRemainingCount 故意写入错误计数，固定恢复时重新推导的要求。
func snapshotWithSucceededParentAndCorruptRemainingCount(t *testing.T, id RunID, corrupt int) RunSnapshot {
	t.Helper()
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "resume-dag",
		Concurrency: 1,
		Tasks: []TaskDefinition{
			{Key: "a", Action: "a", TimeoutMillis: 1000},
			{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
		},
	})
	snapshot := newRunSnapshot(id, compiled, time.Unix(1, 0))
	started := time.Unix(2, 0)
	finished := time.Unix(3, 0)
	snapshot.Run.Status = WorkflowRunning
	snapshot.Run.StartedAt = &started
	snapshot.Run.Tasks[0].Status = TaskSucceeded
	snapshot.Run.Tasks[0].FinishedAt = &finished
	snapshot.Run.Tasks[0].Attempts = []Attempt{{
		Number:     1,
		Status:     AttemptSucceeded,
		StartedAt:  started,
		FinishedAt: &finished,
	}}
	snapshot.Run.Tasks[1].Status = TaskWaitingDependencies
	snapshot.Run.RemainingDependencies[1] = corrupt
	return snapshot
}

// nonTopologicalFailureSnapshot 把后代放在祖先前面，验证失败传播不依赖任务数组顺序。
func nonTopologicalFailureSnapshot(t *testing.T, id RunID) RunSnapshot {
	t.Helper()
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "reverse-ordered-dag",
		Concurrency: 1,
		Tasks: []TaskDefinition{
			{Key: "c", Action: "c", DependsOn: []TaskKey{"b"}, TimeoutMillis: 1000},
			{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
			{Key: "a", Action: "a", TimeoutMillis: 1000},
		},
	})
	snapshot := newRunSnapshot(id, compiled, time.Unix(1, 0))
	started := time.Unix(2, 0)
	finished := time.Unix(3, 0)
	snapshot.Run.Status = WorkflowRunning
	snapshot.Run.StartedAt = &started
	snapshot.Run.Tasks[2].Status = TaskFailed
	snapshot.Run.Tasks[2].FinishedAt = &finished
	snapshot.Run.Tasks[2].Attempts = []Attempt{{
		Number:     1,
		Status:     AttemptFailed,
		StartedAt:  started,
		FinishedAt: &finished,
	}}
	return snapshot
}

func succeededSingleTaskSnapshot(t *testing.T, id RunID) RunSnapshot {
	t.Helper()
	snapshot := runningSnapshotWithOneAttempt(t, id, RetryPolicy{MaxAttempts: 1})
	finished := time.Unix(3, 0)
	task := &snapshot.Run.Tasks[0]
	task.Status = TaskSucceeded
	task.FinishedAt = &finished
	task.Attempts[0].Status = AttemptSucceeded
	task.Attempts[0].FinishedAt = &finished
	return snapshot
}

func terminalSingleTaskSnapshot(t *testing.T, id RunID, status WorkflowStatus) RunSnapshot {
	t.Helper()
	snapshot := succeededSingleTaskSnapshot(t, id)
	finished := time.Unix(4, 0)
	snapshot.Run.Status = status
	snapshot.Run.FinishedAt = &finished
	task := &snapshot.Run.Tasks[0]
	switch status {
	case WorkflowSucceeded:
	case WorkflowFailed:
		task.Status = TaskFailed
		task.Attempts[0].Status = AttemptFailed
	case WorkflowCanceled:
		task.Status = TaskCanceled
		task.Attempts[0].Status = AttemptCanceled
	default:
		t.Fatalf("unsupported terminal status %q", status)
	}
	return snapshot
}

func resumeAsync(engine *Engine, id RunID) <-chan runResult {
	done := make(chan runResult, 1)
	go func() {
		run, err := engine.Resume(context.Background(), id)
		done <- runResult{run: run, err: err}
	}()
	return done
}

func assertExecutorNotCalled(t *testing.T, executor *sequenceExecutor) {
	t.Helper()
	select {
	case <-executor.calls:
		t.Fatal("executor called before retry ReadyAt")
	case <-time.After(50 * time.Millisecond):
	}
}

func assertResumeRejectedWithoutSideEffects(
	t *testing.T,
	err error,
	want string,
	store *memoryStore,
	executor *recordingExecutor,
) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Resume() error = %v, want containing %q", err, want)
	}
	if store.writes != 0 {
		t.Fatalf("store writes = %d, want 0", store.writes)
	}
	if got := executor.callCount(); got != 0 {
		t.Fatalf("executor call count = %d, want 0", got)
	}
}
