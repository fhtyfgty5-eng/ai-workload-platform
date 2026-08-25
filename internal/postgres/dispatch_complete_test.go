package postgres

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestCompleteAppliesSuccessAndFinalizesRun(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-success-workflow", "complete-success-run", workflow.RetryPolicy{MaxAttempts: 1}, 60_000)
	request := workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "completed"},
	}
	response, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, request)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Applied {
		t.Fatalf("Complete() = %+v, want applied", response)
	}

	snapshot, err := repository.Load(context.Background(), "complete-success-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowSucceeded || snapshot.Run.FinishedAt == nil {
		t.Fatalf("completed Run = %+v, want succeeded", snapshot.Run)
	}
	if task := snapshot.Run.Tasks[0]; task.Status != workflow.TaskSucceeded || len(task.Attempts) != 1 || task.Attempts[0].Status != workflow.AttemptSucceeded || task.Attempts[0].Result.Output != "completed" {
		t.Fatalf("completed Task = %+v", task)
	}
	var dispatchStatus string
	var resultHash []byte
	if err := repository.pool.QueryRow(context.Background(), `
		SELECT status, result_hash FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&dispatchStatus, &resultHash); err != nil {
		t.Fatal(err)
	}
	if dispatchStatus != "completed" || len(resultHash) != 32 {
		t.Fatalf("completed Dispatch = %s/hash-length-%d", dispatchStatus, len(resultHash))
	}
}

func TestCompleteSuccessUnlocksAndDispatchesSuccessor(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	definition := workflow.WorkflowDefinition{ID: "complete-chain-workflow", Concurrency: 1, Tasks: []workflow.TaskDefinition{
		{Key: "parent", Action: "parent", TimeoutMillis: 60_000},
		{Key: "child", Action: "child", DependsOn: []workflow.TaskKey{"parent"}, TimeoutMillis: 60_000},
	}}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition = compiled.Definition()
	if _, err := repository.CreateDefinition(ctx, definition, "operator", "create-complete-chain", "hash-complete-chain"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := workflow.NewRunSnapshotForVersion("complete-chain-run", compiled, 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "chain worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 1 {
		t.Fatalf("parent CreateDispatches() = %d, %v", created, err)
	}
	parentLeases, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil || len(parentLeases) != 1 || parentLeases[0].TaskKey != "parent" {
		t.Fatalf("parent Claim() = %+v, %v", parentLeases, err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, parentLeases[0].DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: parentLeases[0].LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess},
	}); err != nil {
		t.Fatal(err)
	}
	between, err := repository.Load(ctx, "complete-chain-run")
	if err != nil {
		t.Fatal(err)
	}
	if between.Run.Tasks[1].Status != workflow.TaskReady || between.Run.RemainingDependencies[1] != 0 || between.Run.Status != workflow.WorkflowRunning {
		t.Fatalf("Run after parent = %+v, remaining=%v", between.Run, between.Run.RemainingDependencies)
	}
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 1 {
		t.Fatalf("child CreateDispatches() = %d, %v", created, err)
	}
	childLeases, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil || len(childLeases) != 1 || childLeases[0].TaskKey != "child" {
		t.Fatalf("child Claim() = %+v, %v", childLeases, err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, childLeases[0].DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: childLeases[0].LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess},
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := repository.Load(ctx, "complete-chain-run")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Run.Status != workflow.WorkflowSucceeded || completed.Run.Tasks[1].Status != workflow.TaskSucceeded {
		t.Fatalf("completed chain = %+v", completed.Run)
	}
}

func TestCompleteRejectsOversizedResultWithoutChangingRun(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-limit-workflow", "complete-limit-run", workflow.RetryPolicy{MaxAttempts: 1}, 60_000)
	request := workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: strings.Repeat("x", 64*1024+1)},
	}
	if _, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, request); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("Complete() error = %v, want ErrInvalidResult", err)
	}
	snapshot, err := repository.Load(context.Background(), "complete-limit-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Revision != 2 || snapshot.Run.Tasks[0].Status != workflow.TaskRunning || len(snapshot.Events) != 3 {
		t.Fatalf("Run changed after oversized result = revision:%d task:%s events:%d", snapshot.Run.Revision, snapshot.Run.Tasks[0].Status, len(snapshot.Events))
	}
}

func TestDigestWorkerResultRejectsUnsupportedOrOversizedFields(t *testing.T) {
	for name, result := range map[string]workflow.ExecutionResponse{
		"unknown kind":      {Kind: workflow.ResultKind("unknown")},
		"oversized output":  {Kind: workflow.ResultSuccess, Output: strings.Repeat("x", maxResultOutputBytes+1)},
		"oversized code":    {Kind: workflow.ResultPermanentFailure, ErrorCode: strings.Repeat("x", maxResultErrorCodeBytes+1)},
		"oversized message": {Kind: workflow.ResultTemporaryFailure, ErrorMessage: strings.Repeat("x", maxResultErrorMessageBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := digestWorkerResult(result); !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("digestWorkerResult() error = %v, want ErrInvalidResult", err)
			}
		})
	}
	boundary := workflow.ExecutionResponse{
		Kind:         workflow.ResultTemporaryFailure,
		Output:       strings.Repeat("o", maxResultOutputBytes),
		ErrorCode:    strings.Repeat("c", maxResultErrorCodeBytes),
		ErrorMessage: strings.Repeat("m", maxResultErrorMessageBytes),
	}
	if _, err := digestWorkerResult(boundary); err != nil {
		t.Fatalf("digestWorkerResult() rejected boundary values: %v", err)
	}
}

func TestCompleteAppliesTemporaryAndPermanentFailures(t *testing.T) {
	t.Run("temporary failure waits for retry", func(t *testing.T) {
		repository := newTestRepository(t)
		worker, lease := createClaimedDispatch(t, repository, "complete-temporary-workflow", "complete-temporary-run", workflow.RetryPolicy{MaxAttempts: 2, IntervalMillis: 1000}, 60_000)
		_, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
			LeaseToken: lease.LeaseToken,
			Result: workflow.ExecutionResponse{
				Kind: workflow.ResultTemporaryFailure, ErrorCode: "busy", ErrorMessage: "retry later",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := repository.Load(context.Background(), "complete-temporary-run")
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Run.Status != workflow.WorkflowRunning {
			t.Fatalf("Run status = %s, want running", snapshot.Run.Status)
		}
		if task := snapshot.Run.Tasks[0]; task.Status != workflow.TaskWaitingRetry || task.ReadyAt == nil || task.Attempts[0].Status != workflow.AttemptFailed || task.Attempts[0].Result.ErrorCode != "busy" {
			t.Fatalf("temporary-failure Task = %+v", task)
		}
	})

	t.Run("permanent failure ends Run", func(t *testing.T) {
		repository := newTestRepository(t)
		worker, lease := createClaimedDispatch(t, repository, "complete-permanent-workflow", "complete-permanent-run", workflow.RetryPolicy{MaxAttempts: 3}, 60_000)
		_, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
			LeaseToken: lease.LeaseToken,
			Result: workflow.ExecutionResponse{
				Kind: workflow.ResultPermanentFailure, ErrorCode: "invalid", ErrorMessage: "cannot retry",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := repository.Load(context.Background(), "complete-permanent-run")
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Run.Status != workflow.WorkflowFailed || snapshot.Run.FinishedAt == nil {
			t.Fatalf("Run = %+v, want failed", snapshot.Run)
		}
		if task := snapshot.Run.Tasks[0]; task.Status != workflow.TaskFailed || task.Attempts[0].Status != workflow.AttemptFailed {
			t.Fatalf("permanent-failure Task = %+v", task)
		}
	})
}

func TestCompleteIsIdempotentAndRejectsConflictingReplay(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-replay-workflow", "complete-replay-run", workflow.RetryPolicy{MaxAttempts: 1}, 60_000)
	request := workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "stable"},
	}
	first, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, request)
	if err != nil {
		t.Fatal(err)
	}
	beforeReplay, err := repository.Load(context.Background(), "complete-replay-run")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != first || !replayed.Applied {
		t.Fatalf("replayed response = %+v, want first response %+v", replayed, first)
	}
	conflict := request
	conflict.Result.Output = "different"
	if _, err := repository.Complete(context.Background(), worker.Summary.WorkerID, lease.DispatchID, conflict); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("conflicting Complete() error = %v, want ErrResultConflict", err)
	}
	afterReplay, err := repository.Load(context.Background(), "complete-replay-run")
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.Run.Revision != beforeReplay.Run.Revision || len(afterReplay.Events) != len(beforeReplay.Events) {
		t.Fatalf("replay changed Run revision/events from %d/%d to %d/%d", beforeReplay.Run.Revision, len(beforeReplay.Events), afterReplay.Run.Revision, len(afterReplay.Events))
	}
}

func TestCompleteRollsBackRunWhenDispatchUpdateFails(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-rollback-workflow", "complete-rollback-run", workflow.RetryPolicy{MaxAttempts: 1}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		ALTER TABLE task_dispatches
		ADD CONSTRAINT reject_completed_dispatch_for_test CHECK (status <> 'completed')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "must roll back"},
	}); err == nil {
		t.Fatal("Complete() error = nil, want injected Dispatch update failure")
	}
	snapshot, err := repository.Load(ctx, "complete-rollback-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Revision != 2 || snapshot.Run.Status != workflow.WorkflowRunning || snapshot.Run.Tasks[0].Status != workflow.TaskRunning || snapshot.Run.Tasks[0].Attempts[0].Status != workflow.AttemptRunning {
		t.Fatalf("Run changed despite completion rollback = %+v", snapshot.Run)
	}
	var status string
	var resultHash []byte
	if err := repository.pool.QueryRow(ctx, `
		SELECT status, result_hash FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&status, &resultHash); err != nil {
		t.Fatal(err)
	}
	if status != "leased" || resultHash != nil {
		t.Fatalf("Dispatch after rollback = %s/hash-%x", status, resultHash)
	}
}

func TestCompleteRejectsLostLeaseWithoutChangingRun(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-lost-workflow", "complete-lost-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	request := workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "late"},
	}
	for name, test := range map[string]struct {
		workerID string
		token    string
	}{
		"wrong Worker": {workerID: "other-worker", token: lease.LeaseToken},
		"wrong token":  {workerID: worker.Summary.WorkerID, token: "wrong-token"},
	} {
		t.Run(name, func(t *testing.T) {
			attempt := request
			attempt.LeaseToken = test.token
			if _, err := repository.Complete(ctx, test.workerID, lease.DispatchID, attempt); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("Complete() error = %v, want ErrLeaseLost", err)
			}
		})
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
		WHERE dispatch_id = $1
	`, lease.DispatchID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, request); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired Complete() error = %v, want ErrLeaseLost", err)
	}
	beforeReap, err := repository.Load(ctx, "complete-lost-run")
	if err != nil {
		t.Fatal(err)
	}
	if beforeReap.Run.Revision != 2 {
		t.Fatalf("lost lease requests changed revision to %d", beforeReap.Run.Revision)
	}
	if reaped, err := repository.ReapExpired(ctx, 10); err != nil || reaped != 1 {
		t.Fatalf("ReapExpired() = %d, %v", reaped, err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, request); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("reaped Complete() error = %v, want ErrLeaseLost", err)
	}
}

func TestCancelRunRevokesLeaseAndRejectsLateCompletion(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "cancel-dispatch-workflow", "cancel-dispatch-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.RequestCancel(ctx, "cancel-dispatch-run", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := repository.CancelRun(ctx, "cancel-dispatch-run"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.Load(ctx, "cancel-dispatch-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowCanceled || snapshot.Run.CancelRequestedAt == nil || snapshot.Run.FinishedAt == nil {
		t.Fatalf("canceled Run = %+v", snapshot.Run)
	}
	if task := snapshot.Run.Tasks[0]; task.Status != workflow.TaskCanceled || task.Attempts[0].Status != workflow.AttemptCanceled {
		t.Fatalf("canceled Task = %+v", task)
	}
	heartbeat, err := repository.Heartbeat(ctx, worker.Summary.WorkerID, []workerprotocol.LeaseRef{{
		DispatchID: lease.DispatchID,
		LeaseToken: lease.LeaseToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.Leases) != 1 || heartbeat.Leases[0].Status != workerprotocol.LeaseRevoked {
		t.Fatalf("heartbeat after cancellation = %+v", heartbeat)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "late"},
	}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late Complete() error = %v, want ErrLeaseLost", err)
	}
}

func TestCancelRequestedRunsRecoversPersistedCancellationIntent(t *testing.T) {
	repository := newTestRepository(t)
	_, lease := createClaimedDispatch(t, repository, "cancel-recovery-workflow", "cancel-recovery-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.RequestCancel(ctx, "cancel-recovery-run", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	reconciled, err := repository.CancelRequestedRuns(ctx, 10)
	if err != nil || reconciled != 1 {
		t.Fatalf("CancelRequestedRuns() = %d, %v, want 1, nil", reconciled, err)
	}
	snapshot, err := repository.Load(ctx, "cancel-recovery-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowCanceled || snapshot.Run.FinishedAt == nil {
		t.Fatalf("reconciled Run = %+v, want canceled terminal Run", snapshot.Run)
	}
	var dispatchStatus string
	if err := repository.pool.QueryRow(ctx, `
		SELECT status FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&dispatchStatus); err != nil {
		t.Fatal(err)
	}
	if dispatchStatus != "canceled" {
		t.Fatalf("Dispatch status = %s, want canceled", dispatchStatus)
	}
}

func TestCancelAndCompleteRaceAcceptsOnlyCancellation(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "cancel-complete-race-workflow", "cancel-complete-race-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.RequestCancel(ctx, "cancel-complete-race-run", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	type result struct{ err error }
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	go func() {
		start.Wait()
		results <- result{err: repository.CancelRun(ctx, "cancel-complete-race-run")}
	}()
	go func() {
		start.Wait()
		_, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
			LeaseToken: lease.LeaseToken,
			Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "late"},
		})
		results <- result{err: err}
	}()
	start.Done()
	var nilErrors int
	var leaseLostErrors int
	for range 2 {
		err := (<-results).err
		switch {
		case err == nil:
			nilErrors++
		case errors.Is(err, ErrLeaseLost):
			leaseLostErrors++
		default:
			t.Fatalf("race error = %v", err)
		}
	}
	if nilErrors != 1 || leaseLostErrors != 1 {
		t.Fatalf("race results = nil:%d lease_lost:%d, want 1/1", nilErrors, leaseLostErrors)
	}
	snapshot, err := repository.Load(ctx, "cancel-complete-race-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowCanceled || snapshot.Run.Revision != 4 || snapshot.Run.LastEventSequence != uint64(len(snapshot.Events)) {
		t.Fatalf("race Run = status:%s revision:%d sequence/events:%d/%d", snapshot.Run.Status, snapshot.Run.Revision, snapshot.Run.LastEventSequence, len(snapshot.Events))
	}
}
