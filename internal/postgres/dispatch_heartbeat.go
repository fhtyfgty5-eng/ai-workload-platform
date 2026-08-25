package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/jackc/pgx/v5"
)

// Heartbeat 更新 Worker 存活时间，并且只续期由秘密令牌证明仍然有效的当前租约。
func (r *Repository) Heartbeat(ctx context.Context, workerID string, leases []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workerprotocol.HeartbeatResponse{}, fmt.Errorf("begin Worker heartbeat: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var now time.Time
	var status workerprotocol.WorkerStatus
	err = tx.QueryRow(ctx, `
		SELECT CURRENT_TIMESTAMP, status
		FROM worker_sessions
		WHERE worker_id = $1
		FOR UPDATE
	`, workerID).Scan(&now, &status)
	if errors.Is(err, pgx.ErrNoRows) || status == workerprotocol.WorkerOffline || status == workerprotocol.WorkerStopped {
		return workerprotocol.HeartbeatResponse{}, ErrWorkerSessionInvalid
	}
	if err != nil {
		return workerprotocol.HeartbeatResponse{}, fmt.Errorf("lock Worker heartbeat session: %w", err)
	}
	if status != workerprotocol.WorkerActive && status != workerprotocol.WorkerDraining {
		return workerprotocol.HeartbeatResponse{}, ErrWorkerSessionInvalid
	}
	if _, err := tx.Exec(ctx, `
		UPDATE worker_sessions SET last_heartbeat_at = $2 WHERE worker_id = $1
	`, workerID, now); err != nil {
		return workerprotocol.HeartbeatResponse{}, fmt.Errorf("update Worker heartbeat: %w", err)
	}

	response := workerprotocol.HeartbeatResponse{Leases: make([]workerprotocol.LeaseHeartbeat, 0, len(leases))}
	for _, ref := range leases {
		item := workerprotocol.LeaseHeartbeat{DispatchID: ref.DispatchID, Status: workerprotocol.LeaseUnknown}
		var storedWorkerID string
		var dispatchStatus string
		var tokenHash []byte
		var leaseExpiresAt time.Time
		var attemptDeadline time.Time
		err := tx.QueryRow(ctx, `
			SELECT worker_id, status, lease_token_hash, lease_expires_at, attempt_deadline
			FROM task_dispatches
			WHERE dispatch_id = $1
			FOR UPDATE
		`, ref.DispatchID).Scan(&storedWorkerID, &dispatchStatus, &tokenHash, &leaseExpiresAt, &attemptDeadline)
		if errors.Is(err, pgx.ErrNoRows) {
			response.Leases = append(response.Leases, item)
			continue
		}
		if err != nil {
			return workerprotocol.HeartbeatResponse{}, fmt.Errorf("load heartbeat Dispatch %s: %w", ref.DispatchID, err)
		}
		if storedWorkerID != workerID || !workerauth.MatchesToken(ref.LeaseToken, tokenHash) {
			response.Leases = append(response.Leases, item)
			continue
		}
		if dispatchStatus != "leased" || !leaseExpiresAt.After(now) || !attemptDeadline.After(now) {
			item.Status = workerprotocol.LeaseRevoked
			response.Leases = append(response.Leases, item)
			continue
		}
		newExpiry := now.Add(r.leaseDuration)
		if newExpiry.After(attemptDeadline) {
			newExpiry = attemptDeadline
		}
		if _, err := tx.Exec(ctx, `
			UPDATE task_dispatches SET lease_expires_at = $2
			WHERE dispatch_id = $1 AND status = 'leased'
		`, ref.DispatchID, newExpiry); err != nil {
			return workerprotocol.HeartbeatResponse{}, fmt.Errorf("renew Dispatch %s: %w", ref.DispatchID, err)
		}
		item.Status = workerprotocol.LeaseRenewed
		item.LeaseRemainingMillis = newExpiry.Sub(now).Milliseconds()
		response.Leases = append(response.Leases, item)
	}
	if err := tx.Commit(ctx); err != nil {
		return workerprotocol.HeartbeatResponse{}, fmt.Errorf("commit Worker heartbeat: %w", err)
	}
	return response, nil
}
