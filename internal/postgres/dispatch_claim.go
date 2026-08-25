package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

// Claim 领取兼容的 pending Dispatch，并在同一事务中启动对应 Attempt。
func (r *Repository) Claim(ctx context.Context, workerID string, slots int) ([]workerprotocol.Lease, error) {
	if slots <= 0 {
		return nil, ErrWorkerCapacityExceeded
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claiming Dispatches: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status workerprotocol.WorkerStatus
	var executorKinds []string
	var maxConcurrency int
	err = tx.QueryRow(ctx, `
		SELECT status, executor_kinds, max_concurrency
		FROM worker_sessions
		WHERE worker_id = $1
		FOR UPDATE
	`, workerID).Scan(&status, &executorKinds, &maxConcurrency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrWorkerSessionInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("lock Worker session for claim: %w", err)
	}
	if status == workerprotocol.WorkerDraining {
		return nil, ErrWorkerDraining
	}
	if status != workerprotocol.WorkerActive {
		return nil, ErrWorkerSessionInvalid
	}
	var activeLeases int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM task_dispatches
		WHERE worker_id = $1 AND status = 'leased'
	`, workerID).Scan(&activeLeases); err != nil {
		return nil, fmt.Errorf("count Worker leases: %w", err)
	}
	freeSlots := maxConcurrency - activeLeases
	if slots > freeSlots {
		return nil, ErrWorkerCapacityExceeded
	}

	rows, err := tx.Query(ctx, `
		SELECT d.dispatch_id, d.run_id, d.task_key
		FROM task_dispatches d
		JOIN task_runs t ON t.run_id = d.run_id AND t.task_key = d.task_key
		JOIN workflow_runs r ON r.run_id = d.run_id
		WHERE d.status = 'pending'
		  AND t.status = 'queued'
		  AND r.status = 'running'
		  AND r.cancel_requested_at IS NULL
		  AND d.executor_kind = ANY($1)
		ORDER BY d.created_at, d.dispatch_id
		FOR UPDATE OF r, d SKIP LOCKED
		LIMIT $2
	`, executorKinds, slots)
	if err != nil {
		return nil, fmt.Errorf("query claimable Dispatches: %w", err)
	}
	type candidate struct {
		dispatchID string
		runID      workflow.RunID
		taskKey    workflow.TaskKey
	}
	candidates := make([]candidate, 0, slots)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.dispatchID, &item.runID, &item.taskKey); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claimable Dispatch: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate claimable Dispatches: %w", err)
	}
	rows.Close()
	if len(candidates) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty Dispatch claim: %w", err)
		}
		return []workerprotocol.Lease{}, nil
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&now); err != nil {
		return nil, fmt.Errorf("read database time for claim: %w", err)
	}
	leases := make([]workerprotocol.Lease, 0, len(candidates))
	for _, item := range candidates {
		snapshot, err := loadSnapshot(ctx, tx, item.runID)
		if err != nil {
			return nil, err
		}
		taskIndex := taskIndexByKey(snapshot.Run.Tasks, item.taskKey)
		if taskIndex < 0 || snapshot.Run.Tasks[taskIndex].Status != workflow.TaskQueued {
			return nil, fmt.Errorf("Dispatch %s does not match a queued Task", item.dispatchID)
		}
		compiled, err := workflow.Compile(*snapshot.Definition)
		if err != nil {
			return nil, fmt.Errorf("compile Run definition for claim: %w", err)
		}
		updated := cloneRepositorySnapshot(snapshot)
		request, err := workflow.StartQueuedAttempt(&updated, compiled, taskIndex, now, workerID, item.dispatchID)
		if err != nil {
			return nil, err
		}
		definition := snapshot.Definition.Tasks[taskIndex]
		attemptDeadline := now.Add(time.Duration(definition.TimeoutMillis) * time.Millisecond)
		leaseExpiresAt := now.Add(r.leaseDuration)
		if leaseExpiresAt.After(attemptDeadline) {
			leaseExpiresAt = attemptDeadline
		}
		leaseToken, err := workerauth.GenerateToken()
		if err != nil {
			return nil, err
		}
		result, err := tx.Exec(ctx, `
			UPDATE task_dispatches SET
				status = 'leased', worker_id = $2, attempt_number = $3,
				lease_token_hash = $4, lease_expires_at = $5,
				attempt_deadline = $6, leased_at = $7
			WHERE dispatch_id = $1 AND status = 'pending'
		`,
			item.dispatchID,
			workerID,
			request.Attempt,
			workerauth.DigestToken(leaseToken),
			leaseExpiresAt,
			attemptDeadline,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("lease Dispatch %s: %w", item.dispatchID, err)
		}
		if result.RowsAffected() != 1 {
			return nil, fmt.Errorf("Dispatch %s is no longer pending", item.dispatchID)
		}
		updated.Run.Revision = snapshot.Run.Revision + 1
		change, err := workflow.ChangeSetBetween(snapshot, updated)
		if err != nil {
			return nil, err
		}
		if err := applyChangeSetTx(ctx, tx, change); err != nil {
			return nil, err
		}
		leases = append(leases, workerprotocol.Lease{
			DispatchID:        item.dispatchID,
			LeaseToken:        leaseToken,
			DefinitionID:      request.DefinitionID,
			DefinitionVersion: snapshot.Run.DefinitionVersion,
			RunID:             request.RunID,
			TaskKey:           request.TaskKey,
			ExecutorKind:      definition.Executor,
			Action:            request.Action,
			Input:             request.Input,
			Attempt:           request.Attempt,
			AttemptDeadline:   attemptDeadline,
			LeaseExpiresAt:    leaseExpiresAt,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Dispatch claim: %w", err)
	}
	return leases, nil
}
