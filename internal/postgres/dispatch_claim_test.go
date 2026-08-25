package postgres

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestDispatchClaimIsUniqueAcrossConcurrentWorkers(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	workers := make([]WorkerRegistration, 2)
	for index := range workers {
		registration, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
			DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		workers[index] = registration
	}
	createDispatchTestRun(t, repository, "claim-workflow", "claim-run", 1, 1)
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() = %d, %v", created, err)
	}

	type result struct {
		leases []workerprotocol.Lease
		err    error
	}
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, worker := range workers {
		go func(workerID string) {
			start.Wait()
			leases, err := repository.Claim(ctx, workerID, 1)
			results <- result{leases: leases, err: err}
		}(worker.Summary.WorkerID)
	}
	start.Done()
	claimed := make([]workerprotocol.Lease, 0, 1)
	for range workers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		claimed = append(claimed, result.leases...)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed leases = %+v, want exactly one", claimed)
	}
	lease := claimed[0]
	if lease.LeaseToken == "" || lease.Attempt != 1 || lease.RunID != "claim-run" || lease.TaskKey != "task-a" || lease.ExecutorKind != workflow.ExecutorMock || lease.Action != "run" {
		t.Fatalf("lease = %+v", lease)
	}
	if lease.LeaseExpiresAt.After(lease.AttemptDeadline) || lease.AttemptDeadline.Sub(lease.LeaseExpiresAt) < 0 {
		t.Fatalf("lease expiry/deadline = %v/%v", lease.LeaseExpiresAt, lease.AttemptDeadline)
	}
	var storedHash []byte
	if err := repository.pool.QueryRow(ctx, "SELECT lease_token_hash FROM task_dispatches WHERE dispatch_id = $1", lease.DispatchID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(storedHash, []byte(lease.LeaseToken)) || !workerauth.MatchesToken(lease.LeaseToken, storedHash) {
		t.Fatal("database did not store only the matching lease-token digest")
	}
	snapshot, err := repository.Load(ctx, "claim-run")
	if err != nil {
		t.Fatal(err)
	}
	attempt := snapshot.Run.Tasks[0].Attempts[0]
	if snapshot.Run.Tasks[0].Status != workflow.TaskRunning || attempt.WorkerID == "" || attempt.DispatchID != lease.DispatchID {
		t.Fatalf("claimed snapshot = %+v", snapshot.Run)
	}
	taskDetail, err := repository.LoadTaskRun(ctx, "claim-run", "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := taskDetail.Attempts[0]; got.WorkerID != attempt.WorkerID || got.DispatchID != attempt.DispatchID {
		t.Fatalf("Task detail Attempt ownership = %q/%q, want %q/%q", got.WorkerID, got.DispatchID, attempt.WorkerID, attempt.DispatchID)
	}
}

func TestDispatchClaimSkipsLockedRow(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	createDispatchTestRun(t, repository, "skip-locked-a", "skip-run-a", 1, 1)
	createDispatchTestRun(t, repository, "skip-locked-b", "skip-run-b", 1, 1)
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 2 {
		t.Fatalf("CreateDispatches() = %d, %v", created, err)
	}
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var lockedDispatch string
	if err := tx.QueryRow(ctx, `
		SELECT dispatch_id FROM task_dispatches
		WHERE status = 'pending' ORDER BY created_at, dispatch_id
		LIMIT 1 FOR UPDATE
	`).Scan(&lockedDispatch); err != nil {
		t.Fatal(err)
	}
	leases, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].DispatchID == lockedDispatch {
		t.Fatalf("Claim() = %+v, locked Dispatch = %s", leases, lockedDispatch)
	}
}

func TestDispatchClaimEnforcesWorkerCapacity(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "small worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Claim(ctx, worker.Summary.WorkerID, 2); !errors.Is(err, ErrWorkerCapacityExceeded) {
		t.Fatalf("Claim() error = %v, want ErrWorkerCapacityExceeded", err)
	}
	if _, err := repository.DrainWorker(ctx, worker.Summary.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Claim(ctx, worker.Summary.WorkerID, 1); !errors.Is(err, ErrWorkerSessionInvalid) {
		t.Fatalf("stopped Claim() error = %v, want ErrWorkerSessionInvalid", err)
	}
}

func TestDispatchClaimRollsBackLeaseAndAttemptOnFailure(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	worker, err := repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	createDispatchTestRun(t, repository, "claim-rollback", "claim-rollback-run", 1, 1)
	if created, err := repository.CreateDispatches(ctx, 10); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() = %d, %v", created, err)
	}
	if _, err := repository.pool.Exec(ctx, `
		ALTER TABLE attempts ADD CONSTRAINT reject_running_attempt_for_test CHECK (status <> 'running')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Claim(ctx, worker.Summary.WorkerID, 1); err == nil {
		t.Fatal("Claim() error = nil, want injected Attempt failure")
	}
	var dispatchStatus string
	var dispatchWorker *string
	if err := repository.pool.QueryRow(ctx, `
		SELECT status, worker_id FROM task_dispatches WHERE run_id = 'claim-rollback-run'
	`).Scan(&dispatchStatus, &dispatchWorker); err != nil {
		t.Fatal(err)
	}
	if dispatchStatus != "pending" || dispatchWorker != nil {
		t.Fatalf("Dispatch after rollback = %s/%v", dispatchStatus, dispatchWorker)
	}
	snapshot, err := repository.Load(ctx, "claim-rollback-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Tasks[0].Status != workflow.TaskQueued || len(snapshot.Run.Tasks[0].Attempts) != 0 || snapshot.Run.Revision != 1 {
		t.Fatalf("Run after claim rollback = %+v", snapshot.Run)
	}
}
