package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func createClaimedDispatch(
	t *testing.T,
	repository *Repository,
	workflowID string,
	runID workflow.RunID,
	retry workflow.RetryPolicy,
	timeoutMillis int64,
) (WorkerRegistration, workerprotocol.Lease) {
	t.Helper()
	ctx := context.Background()
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: workflowID + " worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 1, Tasks: []workflow.TaskDefinition{{
		Key: "task", Action: "run", Retry: retry, TimeoutMillis: timeoutMillis,
	}}}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition = compiled.Definition()
	if _, err := repository.CreateDefinition(ctx, definition, "operator", "create-"+workflowID, "hash-"+workflowID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := workflow.NewRunSnapshotForVersion(runID, compiled, 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if created, err := repository.CreateDispatches(ctx, 100); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() = %d, %v", created, err)
	}
	leases, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %+v, want one", leases)
	}
	return worker, leases[0]
}

func createDispatchTestRun(t *testing.T, repository *Repository, workflowID string, runID workflow.RunID, taskCount, concurrency int) {
	t.Helper()
	tasks := make([]workflow.TaskDefinition, taskCount)
	for index := range tasks {
		tasks[index] = workflow.TaskDefinition{
			Key: workflow.TaskKey("task-" + string(rune('a'+index))), Action: "run", TimeoutMillis: 1000,
		}
	}
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: concurrency, Tasks: tasks}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition = compiled.Definition()
	if _, err := repository.CreateDefinition(context.Background(), definition, "operator", "create-"+workflowID, "hash-"+workflowID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := workflow.NewRunSnapshotForVersion(runID, compiled, 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

func assertDispatchTaskCounts(t *testing.T, repository *Repository, runID workflow.RunID, ready, queued int) {
	t.Helper()
	var gotReady int
	var gotQueued int
	if err := repository.pool.QueryRow(context.Background(), `
		SELECT
			count(*) FILTER (WHERE status = 'ready'),
			count(*) FILTER (WHERE status = 'queued')
		FROM task_runs WHERE run_id = $1
	`, runID).Scan(&gotReady, &gotQueued); err != nil {
		t.Fatal(err)
	}
	if gotReady != ready || gotQueued != queued {
		t.Fatalf("Run %s ready/queued = %d/%d, want %d/%d", runID, gotReady, gotQueued, ready, queued)
	}
}
