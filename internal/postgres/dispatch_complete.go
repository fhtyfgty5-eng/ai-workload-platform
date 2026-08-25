package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

// Complete 接受一个当前租约的结果，并在同一事务中推进工作流。
func (r *Repository) Complete(ctx context.Context, workerID, dispatchID string, request workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	resultHash, err := digestWorkerResult(request.Result)
	if err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("begin Worker result completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&now); err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("read database time for completion: %w", err)
	}
	var runID workflow.RunID
	var taskKey workflow.TaskKey
	var storedWorkerID *string
	var status string
	var attemptNumber *int
	var tokenHash []byte
	var leaseExpiresAt *time.Time
	var attemptDeadline *time.Time
	var storedResultHash []byte
	err = tx.QueryRow(ctx, `SELECT run_id FROM task_dispatches WHERE dispatch_id = $1`, dispatchID).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	if err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("load Dispatch for completion: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT run_id FROM workflow_runs WHERE run_id = $1 FOR UPDATE`, runID).Scan(&runID); err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("lock Run for completion: %w", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT task_key, worker_id, status, attempt_number,
		       lease_token_hash, lease_expires_at, attempt_deadline, result_hash
		FROM task_dispatches
		WHERE dispatch_id = $1
		FOR UPDATE
	`, dispatchID).Scan(
		&taskKey,
		&storedWorkerID,
		&status,
		&attemptNumber,
		&tokenHash,
		&leaseExpiresAt,
		&attemptDeadline,
		&storedResultHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	if err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("lock Dispatch for completion: %w", err)
	}
	if storedWorkerID == nil || *storedWorkerID != workerID || !workerauth.MatchesToken(request.LeaseToken, tokenHash) {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	if status == "completed" {
		if !bytes.Equal(storedResultHash, resultHash[:]) {
			return workerprotocol.CompleteResponse{}, ErrResultConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return workerprotocol.CompleteResponse{}, fmt.Errorf("commit replayed Worker result: %w", err)
		}
		return workerprotocol.CompleteResponse{Applied: true}, nil
	}
	if status != "leased" || attemptNumber == nil || leaseExpiresAt == nil || attemptDeadline == nil ||
		!leaseExpiresAt.After(now) || !attemptDeadline.After(now) {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}

	snapshot, err := loadSnapshot(ctx, tx, runID)
	if err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	if snapshot.Run.CancelRequestedAt != nil || snapshot.Run.Status != workflow.WorkflowRunning {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	taskIndex := taskIndexByKey(snapshot.Run.Tasks, taskKey)
	if taskIndex < 0 {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	task := snapshot.Run.Tasks[taskIndex]
	if task.Status != workflow.TaskRunning || len(task.Attempts) == 0 {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	attempt := task.Attempts[len(task.Attempts)-1]
	if attempt.Number != *attemptNumber || attempt.WorkerID != workerID || attempt.DispatchID != dispatchID {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	compiled, err := workflow.Compile(*snapshot.Definition)
	if err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("compile Run definition for completion: %w", err)
	}
	updated := cloneRepositorySnapshot(snapshot)
	applied, err := workflow.ApplyAttemptResult(&updated, compiled, taskIndex, *attemptNumber, request.Result, now)
	if err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	if !applied {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	updated.Run.Revision = snapshot.Run.Revision + 1
	change, err := workflow.ChangeSetBetween(snapshot, updated)
	if err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	if err := applyChangeSetTx(ctx, tx, change); err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	result, err := tx.Exec(ctx, `
		UPDATE task_dispatches
		SET status = 'completed', result_hash = $2, completed_at = $3
		WHERE dispatch_id = $1 AND status = 'leased'
	`, dispatchID, resultHash[:], now)
	if err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("complete Dispatch %s: %w", dispatchID, err)
	}
	if result.RowsAffected() != 1 {
		return workerprotocol.CompleteResponse{}, ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return workerprotocol.CompleteResponse{}, fmt.Errorf("commit Worker result completion: %w", err)
	}
	return workerprotocol.CompleteResponse{Applied: true}, nil
}

func digestWorkerResult(result workflow.ExecutionResponse) ([sha256.Size]byte, error) {
	if result.Kind != workflow.ResultSuccess && result.Kind != workflow.ResultTemporaryFailure && result.Kind != workflow.ResultPermanentFailure {
		return [sha256.Size]byte{}, fmt.Errorf("%w: unsupported result kind %q", ErrInvalidResult, result.Kind)
	}
	if len(result.Output) > maxResultOutputBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: output exceeds %d bytes", ErrInvalidResult, maxResultOutputBytes)
	}
	if len(result.ErrorCode) > maxResultErrorCodeBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: error code exceeds %d bytes", ErrInvalidResult, maxResultErrorCodeBytes)
	}
	if len(result.ErrorMessage) > maxResultErrorMessageBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: error message exceeds %d bytes", ErrInvalidResult, maxResultErrorMessageBytes)
	}
	// 固定字段 JSON 为重试结果生成不受 HTTP 格式影响的唯一规范字节表示。
	canonical, err := json.Marshal(struct {
		Kind         workflow.ResultKind `json:"kind"`
		Output       string              `json:"output"`
		ErrorCode    string              `json:"error_code"`
		ErrorMessage string              `json:"error_message"`
	}{result.Kind, result.Output, result.ErrorCode, result.ErrorMessage})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode canonical Worker result: %w", err)
	}
	return sha256.Sum256(canonical), nil
}
