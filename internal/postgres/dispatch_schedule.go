package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

// CreateDispatches 在限制 Worker 和全局容量的同时，每个 Run 最多推进一个 Ready 任务。
// limit 既是活动 Dispatch 总上限，也是单次扫描最多检查的工作量。
func (r *Repository) CreateDispatches(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("dispatch limit must be positive")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin dispatch scan: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&now); err != nil {
		return 0, fmt.Errorf("read database time for dispatch scan: %w", err)
	}
	if err := returnOrphanedPendingDispatches(ctx, tx, now, limit); err != nil {
		return 0, err
	}
	if err := promoteDueRetries(ctx, tx, now, limit); err != nil {
		return 0, err
	}

	var active int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM task_dispatches WHERE status IN ('pending', 'leased')
	`).Scan(&active); err != nil {
		return 0, fmt.Errorf("count active Dispatches: %w", err)
	}
	available := limit - active
	if available <= 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit dispatch cleanup: %w", err)
		}
		return 0, nil
	}
	capacity, err := loadExecutorCapacity(ctx, tx)
	if err != nil {
		return 0, err
	}
	if len(capacity) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit dispatch cleanup: %w", err)
		}
		return 0, nil
	}

	runRows, err := tx.Query(ctx, `
		SELECT r.run_id
		FROM workflow_runs r
		JOIN workflow_versions v
		  ON v.workflow_id = r.workflow_id AND v.version = r.workflow_version
		WHERE r.status IN ('pending', 'running')
		  AND r.cancel_requested_at IS NULL
		  AND EXISTS (
		      SELECT 1 FROM task_runs ready
		      WHERE ready.run_id = r.run_id AND ready.status = 'ready'
		  )
		  AND (
		      SELECT count(*) FROM task_runs active_task
		      WHERE active_task.run_id = r.run_id AND active_task.status IN ('queued', 'running')
		  ) < (v.definition_json->>'concurrency')::integer
		ORDER BY (
		    SELECT max(previous_dispatch.created_at)
		    FROM task_dispatches previous_dispatch
		    WHERE previous_dispatch.run_id = r.run_id
		) ASC NULLS FIRST,
		r.created_at,
		r.run_id
		FOR UPDATE OF r SKIP LOCKED
		LIMIT $1
	`, available)
	if err != nil {
		return 0, fmt.Errorf("query dispatch candidate Runs: %w", err)
	}
	runIDs := make([]workflow.RunID, 0, available)
	for runRows.Next() {
		var runID workflow.RunID
		if err := runRows.Scan(&runID); err != nil {
			runRows.Close()
			return 0, fmt.Errorf("scan dispatch candidate Run: %w", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := runRows.Err(); err != nil {
		runRows.Close()
		return 0, fmt.Errorf("iterate dispatch candidate Runs: %w", err)
	}
	runRows.Close()

	created := 0
	for _, runID := range runIDs {
		if created >= available {
			break
		}
		snapshot, err := loadSnapshot(ctx, tx, runID)
		if err != nil {
			return 0, err
		}
		taskIndex := firstTaskWithStatus(snapshot.Run.Tasks, workflow.TaskReady)
		if taskIndex < 0 {
			continue
		}
		kind := snapshot.Definition.Tasks[taskIndex].Executor
		if capacity[kind] <= 0 {
			continue
		}
		dispatchSuffix, err := workerauth.GenerateToken()
		if err != nil {
			return 0, err
		}
		dispatchID := "dsp_" + dispatchSuffix
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_dispatches (
				dispatch_id, run_id, task_key, executor_kind, status, created_at
			) VALUES ($1, $2, $3, $4, 'pending', $5)
		`, dispatchID, runID, snapshot.Run.Tasks[taskIndex].Key, kind, now); err != nil {
			return 0, fmt.Errorf("insert pending Dispatch for %s/%s: %w", runID, snapshot.Run.Tasks[taskIndex].Key, err)
		}
		updated := cloneRepositorySnapshot(snapshot)
		if err := workflow.QueueTask(&updated, taskIndex, now); err != nil {
			return 0, err
		}
		updated.Run.Revision = snapshot.Run.Revision + 1
		change, err := workflow.ChangeSetBetween(snapshot, updated)
		if err != nil {
			return 0, err
		}
		if err := applyChangeSetTx(ctx, tx, change); err != nil {
			return 0, err
		}
		capacity[kind]--
		created++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit dispatch scan: %w", err)
	}
	return created, nil
}

// promoteDueRetries 在同一扫描查找 Ready 任务前，先处理已经到达持久化截止时间的重试。
// 锁定 Run 可让 revision 和事件序号与领取、回收及后续结果写入串行提交。
func promoteDueRetries(ctx context.Context, tx pgx.Tx, at time.Time, limit int) error {
	rows, err := tx.Query(ctx, `
		SELECT r.run_id
		FROM workflow_runs r
		WHERE r.status IN ('pending', 'running')
		  AND r.cancel_requested_at IS NULL
		  AND EXISTS (
		      SELECT 1 FROM task_runs retry
		      WHERE retry.run_id = r.run_id
		        AND retry.status = 'waiting_retry'
		        AND retry.ready_at <= $1
		  )
		ORDER BY r.created_at, r.run_id
		FOR UPDATE OF r SKIP LOCKED
		LIMIT $2
	`, at, limit)
	if err != nil {
		return fmt.Errorf("query Runs with due retries: %w", err)
	}
	runIDs := make([]workflow.RunID, 0, limit)
	for rows.Next() {
		var runID workflow.RunID
		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return fmt.Errorf("scan Run with due retry: %w", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate Runs with due retries: %w", err)
	}
	rows.Close()

	for _, runID := range runIDs {
		snapshot, err := loadSnapshot(ctx, tx, runID)
		if err != nil {
			return err
		}
		taskIndex := firstDueRetry(snapshot.Run.Tasks, at)
		if taskIndex < 0 {
			continue
		}
		attempts := snapshot.Run.Tasks[taskIndex].Attempts
		attemptNumber := attempts[len(attempts)-1].Number
		updated := cloneRepositorySnapshot(snapshot)
		applied, err := workflow.MakeRetryReady(&updated, taskIndex, attemptNumber, at)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("due retry %s/%s no longer matches its current Attempt", runID, snapshot.Run.Tasks[taskIndex].Key)
		}
		updated.Run.Revision = snapshot.Run.Revision + 1
		change, err := workflow.ChangeSetBetween(snapshot, updated)
		if err != nil {
			return err
		}
		if err := applyChangeSetTx(ctx, tx, change); err != nil {
			return err
		}
	}
	return nil
}

func returnOrphanedPendingDispatches(ctx context.Context, tx pgx.Tx, at time.Time, limit int) error {
	rows, err := tx.Query(ctx, `
		SELECT d.dispatch_id, d.run_id, d.task_key
		FROM task_dispatches d
		JOIN workflow_runs r ON r.run_id = d.run_id
		WHERE d.status = 'pending'
		  AND r.cancel_requested_at IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM worker_sessions w
		      WHERE w.status = 'active' AND d.executor_kind = ANY(w.executor_kinds)
		  )
		ORDER BY d.created_at, d.dispatch_id
		LIMIT $1
	`, limit)
	if err != nil {
		return fmt.Errorf("query orphaned pending Dispatches: %w", err)
	}
	type orphan struct {
		dispatchID string
		runID      workflow.RunID
		taskKey    workflow.TaskKey
	}
	orphans := make([]orphan, 0, limit)
	for rows.Next() {
		var item orphan
		if err := rows.Scan(&item.dispatchID, &item.runID, &item.taskKey); err != nil {
			rows.Close()
			return fmt.Errorf("scan orphaned pending Dispatch: %w", err)
		}
		orphans = append(orphans, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate orphaned pending Dispatches: %w", err)
	}
	rows.Close()

	for _, item := range orphans {
		var lockedRunID workflow.RunID
		if err := tx.QueryRow(ctx, `SELECT run_id FROM workflow_runs WHERE run_id = $1 FOR UPDATE`, item.runID).Scan(&lockedRunID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return workflow.ErrRunNotFound
			}
			return fmt.Errorf("lock Run for orphaned Dispatch %s: %w", item.dispatchID, err)
		}
		var dispatchStatus string
		var dispatchTaskKey workflow.TaskKey
		if err := tx.QueryRow(ctx, `
			SELECT status, task_key FROM task_dispatches WHERE dispatch_id = $1 FOR UPDATE
		`, item.dispatchID).Scan(&dispatchStatus, &dispatchTaskKey); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return fmt.Errorf("lock orphaned Dispatch %s: %w", item.dispatchID, err)
		}
		if dispatchStatus != "pending" {
			continue
		}
		snapshot, err := loadSnapshot(ctx, tx, item.runID)
		if err != nil {
			return err
		}
		taskIndex := taskIndexByKey(snapshot.Run.Tasks, dispatchTaskKey)
		if taskIndex < 0 || snapshot.Run.Tasks[taskIndex].Status != workflow.TaskQueued {
			return fmt.Errorf("pending Dispatch %s does not match a queued Task", item.dispatchID)
		}
		updated := cloneRepositorySnapshot(snapshot)
		if err := workflow.ReturnQueuedTaskToReady(&updated, taskIndex, at, "no compatible active Worker"); err != nil {
			return err
		}
		updated.Run.Revision = snapshot.Run.Revision + 1
		change, err := workflow.ChangeSetBetween(snapshot, updated)
		if err != nil {
			return err
		}
		if err := applyChangeSetTx(ctx, tx, change); err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `
			UPDATE task_dispatches
			SET status = 'canceled', completed_at = $2
			WHERE dispatch_id = $1 AND status = 'pending'
		`, item.dispatchID, at)
		if err != nil {
			return fmt.Errorf("cancel orphaned pending Dispatch %s: %w", item.dispatchID, err)
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("orphaned pending Dispatch %s changed concurrently", item.dispatchID)
		}
	}
	return nil
}

func loadExecutorCapacity(ctx context.Context, tx pgx.Tx) (map[workflow.ExecutorKind]int, error) {
	rows, err := tx.Query(ctx, `
		SELECT w.executor_kinds, w.max_concurrency - count(d.dispatch_id)
		FROM worker_sessions w
		LEFT JOIN task_dispatches d ON d.worker_id = w.worker_id AND d.status = 'leased'
		WHERE w.status = 'active'
		GROUP BY w.worker_id, w.executor_kinds, w.max_concurrency
	`)
	if err != nil {
		return nil, fmt.Errorf("query active Worker capacity: %w", err)
	}
	capacity := make(map[workflow.ExecutorKind]int)
	for rows.Next() {
		var kinds []string
		var free int
		if err := rows.Scan(&kinds, &free); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan active Worker capacity: %w", err)
		}
		if free < 0 {
			free = 0
		}
		for _, kind := range kinds {
			capacity[workflow.ExecutorKind(kind)] += free
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate active Worker capacity: %w", err)
	}
	rows.Close()

	pendingRows, err := tx.Query(ctx, `
		SELECT executor_kind, count(*)
		FROM task_dispatches
		WHERE status = 'pending'
		GROUP BY executor_kind
	`)
	if err != nil {
		return nil, fmt.Errorf("query pending Dispatch capacity: %w", err)
	}
	for pendingRows.Next() {
		var kind workflow.ExecutorKind
		var pending int
		if err := pendingRows.Scan(&kind, &pending); err != nil {
			pendingRows.Close()
			return nil, fmt.Errorf("scan pending Dispatch capacity: %w", err)
		}
		capacity[kind] -= pending
		if capacity[kind] < 0 {
			capacity[kind] = 0
		}
	}
	if err := pendingRows.Err(); err != nil {
		pendingRows.Close()
		return nil, fmt.Errorf("iterate pending Dispatch capacity: %w", err)
	}
	pendingRows.Close()
	return capacity, nil
}

func firstTaskWithStatus(tasks []workflow.TaskRun, status workflow.TaskStatus) int {
	for index := range tasks {
		if tasks[index].Status == status {
			return index
		}
	}
	return -1
}

func firstDueRetry(tasks []workflow.TaskRun, at time.Time) int {
	for index := range tasks {
		if tasks[index].Status == workflow.TaskWaitingRetry && tasks[index].ReadyAt != nil && !at.Before(*tasks[index].ReadyAt) {
			return index
		}
	}
	return -1
}

func taskIndexByKey(tasks []workflow.TaskRun, key workflow.TaskKey) int {
	for index := range tasks {
		if tasks[index].Key == key {
			return index
		}
	}
	return -1
}

func cloneRepositorySnapshot(snapshot workflow.RunSnapshot) workflow.RunSnapshot {
	clone := snapshot
	clone.Run.Tasks = append([]workflow.TaskRun(nil), snapshot.Run.Tasks...)
	for index := range clone.Run.Tasks {
		clone.Run.Tasks[index].Attempts = append([]workflow.Attempt(nil), snapshot.Run.Tasks[index].Attempts...)
	}
	clone.Run.RemainingDependencies = append([]int(nil), snapshot.Run.RemainingDependencies...)
	clone.Events = append([]workflow.StateEvent(nil), snapshot.Events...)
	return clone
}
