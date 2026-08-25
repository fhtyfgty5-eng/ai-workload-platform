package workflow

import (
	"reflect"
	"testing"
	"time"
)

func TestDistributedQueueAndClaimCreateAttemptOnlyAfterLease(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-claim", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", TimeoutMillis: 1000,
	}}})
	at := time.Unix(10, 0)
	snapshot := newRunSnapshot("run-one", compiled, at)

	if err := QueueTask(&snapshot, 0, at); err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != WorkflowRunning || snapshot.Run.Tasks[0].Status != TaskQueued {
		t.Fatalf("statuses = %s/%s, want running/queued", snapshot.Run.Status, snapshot.Run.Tasks[0].Status)
	}
	if len(snapshot.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("queued attempts = %d, want 0", len(snapshot.Run.Tasks[0].Attempts))
	}

	request, err := StartQueuedAttempt(&snapshot, compiled, 0, at.Add(time.Second), "worker-one", "dispatch-one")
	if err != nil {
		t.Fatal(err)
	}
	attempt := snapshot.Run.Tasks[0].Attempts[0]
	if snapshot.Run.Tasks[0].Status != TaskRunning || attempt.Number != 1 || attempt.WorkerID != "worker-one" || attempt.DispatchID != "dispatch-one" {
		t.Fatalf("claimed task = %+v", snapshot.Run.Tasks[0])
	}
	if request.Attempt != 1 || request.Action != "run" || request.RunID != "run-one" {
		t.Fatalf("request = %+v", request)
	}
}

func TestDistributedQueuedTaskReturnsToReadyWithoutAttempt(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-return-ready", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	if err := QueueTask(&snapshot, 0, time.Unix(11, 0)); err != nil {
		t.Fatal(err)
	}
	eventsBefore := len(snapshot.Events)

	if err := ReturnQueuedTaskToReady(&snapshot, 0, time.Unix(12, 0), "no compatible active Worker"); err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != WorkflowRunning || snapshot.Run.Tasks[0].Status != TaskReady {
		t.Fatalf("statuses = %s/%s, want running/ready", snapshot.Run.Status, snapshot.Run.Tasks[0].Status)
	}
	if len(snapshot.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("attempts = %d, want 0", len(snapshot.Run.Tasks[0].Attempts))
	}
	if len(snapshot.Events) != eventsBefore+1 {
		t.Fatalf("events = %d, want %d", len(snapshot.Events), eventsBefore+1)
	}
	event := snapshot.Events[len(snapshot.Events)-1]
	if event.From != string(TaskQueued) || event.To != string(TaskReady) || event.Reason != "no compatible active Worker" {
		t.Fatalf("return event = %+v", event)
	}
}

func TestDistributedReturnToReadyRejectsNonQueuedTask(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-invalid-return", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	if err := ReturnQueuedTaskToReady(&snapshot, 0, time.Unix(11, 0), "invalid"); err == nil {
		t.Fatal("ReturnQueuedTaskToReady() error = nil for ready task")
	}
}

func TestDistributedSuccessUnlocksDirectSuccessor(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-success", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "parent", Action: "parent", TimeoutMillis: 1000},
		{Key: "child", Action: "child", DependsOn: []TaskKey{"parent"}, TimeoutMillis: 1000},
	}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	if err := QueueTask(&snapshot, 0, time.Unix(11, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := StartQueuedAttempt(&snapshot, compiled, 0, time.Unix(12, 0), "worker-one", "dispatch-one"); err != nil {
		t.Fatal(err)
	}
	applied, err := ApplyAttemptResult(&snapshot, compiled, 0, 1, ExecutionResponse{Kind: ResultSuccess, Output: "ok"}, time.Unix(13, 0))
	if err != nil || !applied {
		t.Fatalf("ApplyAttemptResult() = %v, %v", applied, err)
	}
	if snapshot.Run.Tasks[0].Status != TaskSucceeded || snapshot.Run.Tasks[1].Status != TaskReady || snapshot.Run.RemainingDependencies[1] != 0 {
		t.Fatalf("tasks = %+v, remaining = %v", snapshot.Run.Tasks, snapshot.Run.RemainingDependencies)
	}
}

func TestDistributedInterruptionUsesExistingRetryPolicy(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-interrupt", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", Retry: RetryPolicy{MaxAttempts: 2, IntervalMillis: 100}, TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	if err := QueueTask(&snapshot, 0, time.Unix(11, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := StartQueuedAttempt(&snapshot, compiled, 0, time.Unix(12, 0), "worker-one", "dispatch-one"); err != nil {
		t.Fatal(err)
	}
	applied, err := InterruptAttempt(&snapshot, compiled, 0, 1, time.Unix(13, 0), "lease expired")
	if err != nil || !applied {
		t.Fatalf("InterruptAttempt() = %v, %v", applied, err)
	}
	task := snapshot.Run.Tasks[0]
	if task.Status != TaskWaitingRetry || task.Attempts[0].Status != AttemptInterrupted || task.ReadyAt == nil {
		t.Fatalf("task after interruption = %+v", task)
	}
	if want := time.Unix(13, 0).Add(100 * time.Millisecond); !task.ReadyAt.Equal(want) {
		t.Fatalf("ReadyAt = %v, want %v", task.ReadyAt, want)
	}
}

func TestDistributedTemporaryFailureBecomesReadyAndCreatesNextAttempt(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-retry", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", Retry: RetryPolicy{MaxAttempts: 2, IntervalMillis: 100}, TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	queueAndStartDistributed(t, &snapshot, compiled, 0, time.Unix(11, 0), "worker-one", "dispatch-one")

	applied, err := ApplyAttemptResult(&snapshot, compiled, 0, 1, ExecutionResponse{
		Kind: ResultTemporaryFailure, ErrorCode: "temporary", ErrorMessage: "try again",
	}, time.Unix(12, 0))
	if err != nil || !applied {
		t.Fatalf("ApplyAttemptResult() = %v, %v", applied, err)
	}
	readyAt := time.Unix(12, 0).Add(100 * time.Millisecond)
	if task := snapshot.Run.Tasks[0]; task.Status != TaskWaitingRetry || task.Attempts[0].Status != AttemptFailed || task.ReadyAt == nil || !task.ReadyAt.Equal(readyAt) {
		t.Fatalf("task after temporary failure = %+v", task)
	}

	if applied, err := MakeRetryReady(&snapshot, 0, 1, readyAt.Add(-time.Nanosecond)); err != nil || applied {
		t.Fatalf("early MakeRetryReady() = %v, %v, want false, nil", applied, err)
	}
	if applied, err := MakeRetryReady(&snapshot, 0, 1, readyAt); err != nil || !applied {
		t.Fatalf("due MakeRetryReady() = %v, %v, want true, nil", applied, err)
	}
	queueAndStartDistributed(t, &snapshot, compiled, 0, readyAt.Add(time.Second), "worker-two", "dispatch-two")

	attempts := snapshot.Run.Tasks[0].Attempts
	if len(attempts) != 2 || attempts[1].Number != 2 || attempts[1].WorkerID != "worker-two" || attempts[1].DispatchID != "dispatch-two" {
		t.Fatalf("attempts after retry claim = %+v", attempts)
	}
}

func TestDistributedPermanentFailureSkipsAllDescendants(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-permanent-failure", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "root", Action: "root", TimeoutMillis: 1000},
		{Key: "child", Action: "child", DependsOn: []TaskKey{"root"}, TimeoutMillis: 1000},
		{Key: "grandchild", Action: "grandchild", DependsOn: []TaskKey{"child"}, TimeoutMillis: 1000},
	}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	queueAndStartDistributed(t, &snapshot, compiled, 0, time.Unix(11, 0), "worker-one", "dispatch-one")

	applied, err := ApplyAttemptResult(&snapshot, compiled, 0, 1, ExecutionResponse{
		Kind: ResultPermanentFailure, ErrorCode: "invalid_input", ErrorMessage: "cannot retry",
	}, time.Unix(12, 0))
	if err != nil || !applied {
		t.Fatalf("ApplyAttemptResult() = %v, %v", applied, err)
	}
	if got := []TaskStatus{snapshot.Run.Tasks[0].Status, snapshot.Run.Tasks[1].Status, snapshot.Run.Tasks[2].Status}; !reflect.DeepEqual(got, []TaskStatus{TaskFailed, TaskSkipped, TaskSkipped}) {
		t.Fatalf("task statuses = %v, want failed/skipped/skipped", got)
	}
}

func TestDistributedTimeoutExhaustsAttemptsAndSkipsDescendant(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-timeout", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "root", Action: "root", TimeoutMillis: 1000},
		{Key: "child", Action: "child", DependsOn: []TaskKey{"root"}, TimeoutMillis: 1000},
	}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	queueAndStartDistributed(t, &snapshot, compiled, 0, time.Unix(11, 0), "worker-one", "dispatch-one")

	applied, err := TimeoutAttempt(&snapshot, compiled, 0, 1, time.Unix(12, 0))
	if err != nil || !applied {
		t.Fatalf("TimeoutAttempt() = %v, %v", applied, err)
	}
	if task := snapshot.Run.Tasks[0]; task.Status != TaskFailed || task.Attempts[0].Status != AttemptTimedOut || task.Attempts[0].Result.ErrorCode != "timeout" {
		t.Fatalf("timed-out task = %+v", task)
	}
	if snapshot.Run.Tasks[1].Status != TaskSkipped {
		t.Fatalf("descendant status = %s, want skipped", snapshot.Run.Tasks[1].Status)
	}
}

func TestDistributedLateResultForPreviousAttemptIsIgnored(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-late-result", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", Retry: RetryPolicy{MaxAttempts: 2}, TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	queueAndStartDistributed(t, &snapshot, compiled, 0, time.Unix(11, 0), "worker-one", "dispatch-one")
	if applied, err := InterruptAttempt(&snapshot, compiled, 0, 1, time.Unix(12, 0), "lease expired"); err != nil || !applied {
		t.Fatalf("InterruptAttempt() = %v, %v", applied, err)
	}
	if applied, err := MakeRetryReady(&snapshot, 0, 1, time.Unix(12, 0)); err != nil || !applied {
		t.Fatalf("MakeRetryReady() = %v, %v", applied, err)
	}
	queueAndStartDistributed(t, &snapshot, compiled, 0, time.Unix(13, 0), "worker-two", "dispatch-two")
	before := cloneRunSnapshot(snapshot)

	applied, err := ApplyAttemptResult(&snapshot, compiled, 0, 1, ExecutionResponse{Kind: ResultSuccess, Output: "late"}, time.Unix(14, 0))
	if err != nil || applied {
		t.Fatalf("late ApplyAttemptResult() = %v, %v, want false, nil", applied, err)
	}
	if !reflect.DeepEqual(snapshot, before) {
		t.Fatalf("late result mutated snapshot\nbefore: %+v\nafter:  %+v", before, snapshot)
	}
}

func TestChangeSetBetweenRequiresNextRevision(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-change", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", TimeoutMillis: 1000,
	}}})
	before := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	after := cloneRunSnapshot(before)
	if err := QueueTask(&after, 0, time.Unix(11, 0)); err != nil {
		t.Fatal(err)
	}

	if _, err := ChangeSetBetween(before, after); err == nil {
		t.Fatal("ChangeSetBetween() error = nil, want unchanged revision rejected")
	}
	after.Run.Revision = before.Run.Revision + 1
	change, err := ChangeSetBetween(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if change.ExpectedRevision != before.Run.Revision || change.Run == nil || change.Run.Revision != after.Run.Revision || len(change.Tasks) != 1 || len(change.Events) != 2 {
		t.Fatalf("change = %+v", change)
	}
	applied, err := ApplyChangeSetForStore(before, change)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(applied, after) {
		t.Fatalf("applied snapshot differs\nwant: %+v\ngot:  %+v", after, applied)
	}
}

func TestDistributedCancellationCancelsQueuedTaskWithoutAttempt(t *testing.T) {
	compiled := mustCompileDistributed(t, WorkflowDefinition{ID: "distributed-cancel", Concurrency: 1, Tasks: []TaskDefinition{{
		Key: "task", Action: "run", TimeoutMillis: 1000,
	}}})
	snapshot := newRunSnapshot("run-one", compiled, time.Unix(10, 0))
	if err := QueueTask(&snapshot, 0, time.Unix(11, 0)); err != nil {
		t.Fatal(err)
	}
	canceled, err := CancelRunSnapshot(snapshot, time.Unix(12, 0))
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Run.Status != WorkflowCanceled || canceled.Run.Tasks[0].Status != TaskCanceled || len(canceled.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("canceled snapshot = %+v", canceled.Run)
	}
}

func mustCompileDistributed(t *testing.T, definition WorkflowDefinition) *CompiledWorkflow {
	t.Helper()
	compiled, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func queueAndStartDistributed(t *testing.T, snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex int, at time.Time, workerID, dispatchID string) {
	t.Helper()
	if err := QueueTask(snapshot, taskIndex, at); err != nil {
		t.Fatal(err)
	}
	if _, err := StartQueuedAttempt(snapshot, compiled, taskIndex, at, workerID, dispatchID); err != nil {
		t.Fatal(err)
	}
}
