package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestDispatchCreationRequiresCompatibleCapacityAndIsFairAcrossRuns(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	createDispatchTestRun(t, repository, "fair-workflow", "run-a", 2, 2)
	createDispatchTestRun(t, repository, "other-workflow", "run-b", 1, 1)

	created, err := repository.CreateDispatches(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("dispatches without Worker = %d, want 0", created)
	}
	assertDispatchTaskCounts(t, repository, "run-a", 2, 0)

	if _, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 10,
	}); err != nil {
		t.Fatal(err)
	}
	created, err = repository.CreateDispatches(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if created != 2 {
		t.Fatalf("first fair scan created = %d, want one Dispatch per Run", created)
	}
	assertDispatchTaskCounts(t, repository, "run-a", 1, 1)
	assertDispatchTaskCounts(t, repository, "run-b", 0, 1)

	created, err = repository.CreateDispatches(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("scan at global active limit created = %d, want 0", created)
	}
	created, err = repository.CreateDispatches(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("scan with one free global slot created = %d, want 1", created)
	}
	assertDispatchTaskCounts(t, repository, "run-a", 0, 2)
}

func TestDispatchCreationRotatesFairlyAcrossScanRounds(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	createDispatchTestRun(t, repository, "round-fair-workflow", "round-fair-run-a", 2, 1)
	createDispatchTestRun(t, repository, "round-fair-other", "round-fair-run-b", 1, 1)
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "round fairness worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if created, err := repository.CreateDispatches(ctx, 1); err != nil || created != 1 {
		t.Fatalf("first CreateDispatches() = %d, %v, want 1, nil", created, err)
	}
	first, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil || len(first) != 1 || first[0].RunID != "round-fair-run-a" {
		t.Fatalf("first Claim() = %+v, %v, want run-a", first, err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, first[0].DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: first[0].LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess},
	}); err != nil {
		t.Fatal(err)
	}

	if created, err := repository.CreateDispatches(ctx, 1); err != nil || created != 1 {
		t.Fatalf("second CreateDispatches() = %d, %v, want 1, nil", created, err)
	}
	second, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("second Claim() = %+v, %v, want one lease", second, err)
	}
	if second[0].RunID != "round-fair-run-b" {
		t.Fatalf("second Dispatch Run = %s, want never-dispatched run-b before run-a receives another slot", second[0].RunID)
	}
}

func TestDispatchCreationRollsBackRunStateWhenInsertFails(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	createDispatchTestRun(t, repository, "rollback-workflow", "rollback-run", 1, 1)
	if _, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx, `
		ALTER TABLE task_dispatches
		ADD CONSTRAINT reject_pending_dispatch_for_test CHECK (status <> 'pending')
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := repository.CreateDispatches(ctx, 10); err == nil {
		t.Fatal("CreateDispatches() error = nil, want injected insert failure")
	}
	snapshot, err := repository.Load(ctx, "rollback-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Revision != 0 || snapshot.Run.Status != workflow.WorkflowPending || snapshot.Run.Tasks[0].Status != workflow.TaskReady || len(snapshot.Events) != 0 {
		t.Fatalf("Run changed despite rollback = %+v events=%+v", snapshot.Run, snapshot.Events)
	}
	var dispatches int
	if err := repository.pool.QueryRow(ctx, "SELECT count(*) FROM task_dispatches WHERE run_id = 'rollback-run'").Scan(&dispatches); err != nil {
		t.Fatal(err)
	}
	if dispatches != 0 {
		t.Fatalf("dispatch rows after rollback = %d, want 0", dispatches)
	}
}

func TestDispatchCreationReturnsPendingTaskToReadyWithoutCompatibleWorker(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	createDispatchTestRun(t, repository, "return-workflow", "return-run", 1, 1)
	registration, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() = %d, %v, want 1, nil", created, err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE worker_sessions SET status = 'offline' WHERE worker_id = $1
	`, registration.Summary.WorkerID); err != nil {
		t.Fatal(err)
	}
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 0 {
		t.Fatalf("CreateDispatches() after Worker offline = %d, %v", created, err)
	}
	snapshot, err := repository.Load(ctx, "return-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowRunning || snapshot.Run.Tasks[0].Status != workflow.TaskReady || len(snapshot.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("returned Run = %+v", snapshot.Run)
	}
	if snapshot.Run.Revision != 2 || len(snapshot.Events) != 3 {
		t.Fatalf("returned revision/events = %d/%d, want 2/3", snapshot.Run.Revision, len(snapshot.Events))
	}
	var status string
	var completedAt *time.Time
	if err := repository.pool.QueryRow(ctx, `
		SELECT status, completed_at FROM task_dispatches WHERE run_id = 'return-run'
	`).Scan(&status, &completedAt); err != nil {
		t.Fatal(err)
	}
	if status != "canceled" || completedAt == nil {
		t.Fatalf("orphan Dispatch = status:%s completed_at:%v", status, completedAt)
	}
}
