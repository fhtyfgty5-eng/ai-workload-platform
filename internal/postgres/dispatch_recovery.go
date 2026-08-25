package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

// CancelRequestedRuns 收敛已经持久化、但尚未进入终态的取消请求。
// HTTP 请求与取消收敛使用不同事务，因此协调器必须周期扫描以覆盖两步之间的进程崩溃。
func (r *Repository) CancelRequestedRuns(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("cancellation reconciliation limit must be positive")
	}
	rows, err := r.pool.Query(ctx, `
		SELECT run_id
		FROM workflow_runs
		WHERE cancel_requested_at IS NOT NULL
		  AND status IN ('pending', 'running')
		ORDER BY cancel_requested_at, run_id
		LIMIT $1
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("query requested Run cancellations: %w", err)
	}
	ids := make([]workflow.RunID, 0, limit)
	for rows.Next() {
		var id workflow.RunID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan requested Run cancellation: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate requested Run cancellations: %w", err)
	}
	rows.Close()

	for _, id := range ids {
		if err := r.CancelRun(ctx, id); err != nil {
			return 0, fmt.Errorf("reconcile cancellation for Run %s: %w", id, err)
		}
	}
	return len(ids), nil
}

// ReapExpired 把失联 Worker 标记为 offline，并结算租约或截止时间已经过期的 Attempt。
func (r *Repository) ReapExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("reap limit must be positive")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin expired lease reap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&now); err != nil {
		return 0, fmt.Errorf("read database time for lease reap: %w", err)
	}
	offlineBefore := now.Add(-3 * r.workerHeartbeatInterval)
	if _, err := tx.Exec(ctx, `
		UPDATE worker_sessions
		SET status = 'offline'
		WHERE status IN ('active', 'draining') AND last_heartbeat_at < $1
	`, offlineBefore); err != nil {
		return 0, fmt.Errorf("mark silent Workers offline: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT d.dispatch_id, d.run_id, d.task_key, d.attempt_number
		FROM task_dispatches d
		JOIN workflow_runs r ON r.run_id = d.run_id
		WHERE d.status = 'leased'
		  AND (d.lease_expires_at <= $1 OR d.attempt_deadline <= $1)
		ORDER BY LEAST(d.lease_expires_at, d.attempt_deadline), d.dispatch_id
		FOR UPDATE OF r, d SKIP LOCKED
		LIMIT $2
	`, now, limit)
	if err != nil {
		return 0, fmt.Errorf("query expired Dispatches: %w", err)
	}
	type expiredDispatch struct {
		dispatchID    string
		runID         workflow.RunID
		taskKey       workflow.TaskKey
		attemptNumber int
	}
	expired := make([]expiredDispatch, 0, limit)
	for rows.Next() {
		var item expiredDispatch
		if err := rows.Scan(&item.dispatchID, &item.runID, &item.taskKey, &item.attemptNumber); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired Dispatch: %w", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate expired Dispatches: %w", err)
	}
	rows.Close()

	for _, item := range expired {
		var (
			dispatchStatus  string
			leaseExpiresAt  time.Time
			attemptDeadline time.Time
			currentTime     time.Time
		)
		if err := tx.QueryRow(ctx, `
			SELECT status, lease_expires_at, attempt_deadline, clock_timestamp()
			FROM task_dispatches
			WHERE dispatch_id = $1
			FOR UPDATE
		`, item.dispatchID).Scan(&dispatchStatus, &leaseExpiresAt, &attemptDeadline, &currentTime); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return 0, fmt.Errorf("lock expired Dispatch %s: %w", item.dispatchID, err)
		}
		if dispatchStatus != "leased" {
			continue
		}
		// 候选查询与实际结算之间可能经历锁等待或前序任务处理，必须依据最新期限再次判断。
		if leaseExpiresAt.After(currentTime) && attemptDeadline.After(currentTime) {
			continue
		}
		snapshot, err := loadSnapshot(ctx, tx, item.runID)
		if err != nil {
			return 0, err
		}
		taskIndex := taskIndexByKey(snapshot.Run.Tasks, item.taskKey)
		if taskIndex < 0 {
			return 0, fmt.Errorf("expired Dispatch %s has no matching Task", item.dispatchID)
		}
		compiled, err := workflow.Compile(*snapshot.Definition)
		if err != nil {
			return 0, fmt.Errorf("compile Run definition for lease reap: %w", err)
		}
		updated := cloneRepositorySnapshot(snapshot)
		var applied bool
		if !attemptDeadline.After(currentTime) {
			applied, err = workflow.TimeoutAttempt(&updated, compiled, taskIndex, item.attemptNumber, currentTime)
		} else {
			applied, err = workflow.InterruptAttempt(&updated, compiled, taskIndex, item.attemptNumber, currentTime, "Worker lease expired")
		}
		if err != nil {
			return 0, err
		}
		if !applied {
			return 0, fmt.Errorf("expired Dispatch %s no longer owns the current Attempt", item.dispatchID)
		}
		updated.Run.Revision = snapshot.Run.Revision + 1
		change, err := workflow.ChangeSetBetween(snapshot, updated)
		if err != nil {
			return 0, err
		}
		if err := applyChangeSetTx(ctx, tx, change); err != nil {
			return 0, err
		}
		result, err := tx.Exec(ctx, `
			UPDATE task_dispatches
			SET status = 'expired', completed_at = $2
			WHERE dispatch_id = $1 AND status = 'leased'
			`, item.dispatchID, currentTime)
		if err != nil {
			return 0, fmt.Errorf("expire Dispatch %s: %w", item.dispatchID, err)
		}
		if result.RowsAffected() != 1 {
			return 0, fmt.Errorf("expired Dispatch %s changed concurrently", item.dispatchID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit expired lease reap: %w", err)
	}
	return len(expired), nil
}

// CancelRun 处理已持久化的取消请求，并原子撤销全部活动 Dispatch。
func (r *Repository) CancelRun(ctx context.Context, id workflow.RunID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin distributed Run cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&now); err != nil {
		return fmt.Errorf("read database time for Run cancellation: %w", err)
	}
	// 必须先锁 Run、再锁 Dispatch 行，与 CreateDispatches、Claim 和 Complete 保持一致。
	var lockedRunID workflow.RunID
	if err := tx.QueryRow(ctx, `SELECT run_id FROM workflow_runs WHERE run_id = $1 FOR UPDATE`, id).Scan(&lockedRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflow.ErrRunNotFound
		}
		return fmt.Errorf("lock Run for distributed cancellation: %w", err)
	}
	// Dispatch 行是心跳、完成和租约回收共同使用的执行所有权边界。
	rows, err := tx.Query(ctx, `
		SELECT dispatch_id
		FROM task_dispatches
		WHERE run_id = $1 AND status IN ('pending', 'leased')
		ORDER BY dispatch_id
		FOR UPDATE
	`, id)
	if err != nil {
		return fmt.Errorf("lock active Dispatches for cancellation: %w", err)
	}
	for rows.Next() {
		var dispatchID string
		if err := rows.Scan(&dispatchID); err != nil {
			rows.Close()
			return fmt.Errorf("scan active Dispatch for cancellation: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate active Dispatches for cancellation: %w", err)
	}
	rows.Close()

	snapshot, err := loadSnapshot(ctx, tx, id)
	if err != nil {
		return err
	}
	if workflow.IsWorkflowTerminalForStore(snapshot.Run.Status) {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit terminal Run cancellation: %w", err)
		}
		return nil
	}
	if snapshot.Run.CancelRequestedAt == nil {
		return fmt.Errorf("Run %s cancellation was not requested", id)
	}
	updated, err := workflow.CancelRunSnapshot(snapshot, now)
	if err != nil {
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
	if _, err := tx.Exec(ctx, `
		UPDATE task_dispatches
		SET status = 'canceled', completed_at = $2
		WHERE run_id = $1 AND status IN ('pending', 'leased')
	`, id, now); err != nil {
		return fmt.Errorf("revoke Dispatches for Run %s: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit distributed Run cancellation: %w", err)
	}
	return nil
}
