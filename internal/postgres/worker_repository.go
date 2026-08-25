package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerauth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidWorkerRegistration = errors.New("invalid Worker registration")
	ErrWorkerProtocolUnsupported = errors.New("Worker protocol version is not supported")
	ErrWorkerSessionInvalid      = errors.New("Worker session is invalid")
	ErrWorkerDraining            = errors.New("Worker is draining")
	ErrWorkerCapacityExceeded    = errors.New("Worker capacity is exceeded")
	ErrWorkerNotFound            = errors.New("Worker not found")
)

const (
	maxWorkerDisplayNameRunes = 128
	maxWorkerExecutorKinds    = 16
	maxWorkerConcurrency      = 1024
	maxWorkerPageSize         = 100
	workerRegistrationRetries = 3
)

// WorkerRegistration 作为 Repository 调用方的兼容别名保留。
type WorkerRegistration = workerprotocol.Registration

// RegisterWorker 校验执行能力并创建新会话；显示名称重复时也不会复用旧会话。
func (r *Repository) RegisterWorker(ctx context.Context, request workerprotocol.RegisterRequest) (WorkerRegistration, error) {
	normalized, err := normalizeWorkerRegistration(request)
	if err != nil {
		return WorkerRegistration{}, err
	}
	executorKinds := executorKindStrings(normalized.ExecutorKinds)
	for attempt := 0; attempt < workerRegistrationRetries; attempt++ {
		workerSuffix, err := workerauth.GenerateToken()
		if err != nil {
			return WorkerRegistration{}, err
		}
		sessionToken, err := workerauth.GenerateToken()
		if err != nil {
			return WorkerRegistration{}, err
		}
		summary := workerprotocol.WorkerSummary{
			WorkerID:        "wrk_" + workerSuffix,
			DisplayName:     normalized.DisplayName,
			ProtocolVersion: normalized.ProtocolVersion,
			ExecutorKinds:   append([]workflow.ExecutorKind(nil), normalized.ExecutorKinds...),
			MaxConcurrency:  normalized.MaxConcurrency,
			Status:          workerprotocol.WorkerActive,
		}
		err = r.pool.QueryRow(ctx, `
			INSERT INTO worker_sessions (
				worker_id, display_name, protocol_version, executor_kinds,
				max_concurrency, status, session_token_hash,
				registered_at, last_heartbeat_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			RETURNING registered_at, last_heartbeat_at
		`,
			summary.WorkerID,
			summary.DisplayName,
			summary.ProtocolVersion,
			executorKinds,
			summary.MaxConcurrency,
			summary.Status,
			workerauth.DigestToken(sessionToken),
		).Scan(&summary.RegisteredAt, &summary.LastHeartbeatAt)
		if err == nil {
			return WorkerRegistration{Summary: summary, SessionToken: sessionToken}, nil
		}
		if !isUniqueViolation(err, "worker_sessions_pkey") && !isUniqueViolation(err, "worker_sessions_token_hash_unique") {
			return WorkerRegistration{}, fmt.Errorf("insert Worker session: %w", err)
		}
	}
	return WorkerRegistration{}, fmt.Errorf("generate unique Worker credentials")
}

// AuthenticateWorker 验证活动会话，并根据具体操作应用 draining 权限规则。
func (r *Repository) AuthenticateWorker(
	ctx context.Context,
	workerID, sessionToken string,
	protocolVersion int,
	operation workerprotocol.WorkerOperation,
) (workerprotocol.WorkerSession, error) {
	if protocolVersion != workerprotocol.ProtocolVersion {
		return workerprotocol.WorkerSession{}, ErrWorkerProtocolUnsupported
	}
	var session workerprotocol.WorkerSession
	var executorKinds []string
	var tokenHash []byte
	err := r.pool.QueryRow(ctx, `
		SELECT worker_id, protocol_version, executor_kinds, max_concurrency, status, session_token_hash
		FROM worker_sessions
		WHERE worker_id = $1
	`, workerID).Scan(
		&session.WorkerID,
		&session.ProtocolVersion,
		&executorKinds,
		&session.MaxConcurrency,
		&session.Status,
		&tokenHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workerprotocol.WorkerSession{}, ErrWorkerSessionInvalid
	}
	if err != nil {
		return workerprotocol.WorkerSession{}, fmt.Errorf("load Worker session: %w", err)
	}
	if session.ProtocolVersion != workerprotocol.ProtocolVersion {
		return workerprotocol.WorkerSession{}, ErrWorkerProtocolUnsupported
	}
	if !workerauth.MatchesToken(sessionToken, tokenHash) {
		return workerprotocol.WorkerSession{}, ErrWorkerSessionInvalid
	}
	if operation != workerprotocol.OperationClaim && operation != workerprotocol.OperationHeartbeat &&
		operation != workerprotocol.OperationComplete && operation != workerprotocol.OperationDrain {
		return workerprotocol.WorkerSession{}, ErrWorkerSessionInvalid
	}
	if session.Status == workerprotocol.WorkerOffline || session.Status == workerprotocol.WorkerStopped {
		return workerprotocol.WorkerSession{}, ErrWorkerSessionInvalid
	}
	if session.Status == workerprotocol.WorkerDraining && operation == workerprotocol.OperationClaim {
		return workerprotocol.WorkerSession{}, ErrWorkerDraining
	}
	if session.Status != workerprotocol.WorkerActive && session.Status != workerprotocol.WorkerDraining {
		return workerprotocol.WorkerSession{}, ErrWorkerSessionInvalid
	}
	session.ExecutorKinds = executorKindsFromStrings(executorKinds)
	return session, nil
}

// DrainWorker 停止新领取，并在会话不再持有活动租约后将其标记为 stopped。
func (r *Repository) DrainWorker(ctx context.Context, workerID string) (workerprotocol.WorkerSummary, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workerprotocol.WorkerSummary{}, fmt.Errorf("begin draining Worker session: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var summary workerprotocol.WorkerSummary
	var executorKinds []string
	// Claim 分配任务前会锁定同一会话记录。统计租约期间保持该行锁，
	// 可以防止作出“零活动租约”判断后又并发领取新任务。
	err = tx.QueryRow(ctx, `
		SELECT worker_id, display_name, protocol_version, executor_kinds,
		       max_concurrency, status, registered_at, last_heartbeat_at, stopped_at
		FROM worker_sessions
		WHERE worker_id = $1 AND status IN ('active', 'draining')
		FOR UPDATE
	`, workerID).Scan(
		&summary.WorkerID,
		&summary.DisplayName,
		&summary.ProtocolVersion,
		&executorKinds,
		&summary.MaxConcurrency,
		&summary.Status,
		&summary.RegisteredAt,
		&summary.LastHeartbeatAt,
		&summary.StoppedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workerprotocol.WorkerSummary{}, ErrWorkerSessionInvalid
	}
	if err != nil {
		return workerprotocol.WorkerSummary{}, fmt.Errorf("lock Worker session for drain: %w", err)
	}
	summary.ExecutorKinds = executorKindsFromStrings(executorKinds)
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM task_dispatches WHERE worker_id = $1 AND status = 'leased'
	`, summary.WorkerID).Scan(&summary.ActiveLeases); err != nil {
		return workerprotocol.WorkerSummary{}, fmt.Errorf("count active Worker leases: %w", err)
	}

	nextStatus := workerprotocol.WorkerDraining
	if summary.ActiveLeases == 0 {
		nextStatus = workerprotocol.WorkerStopped
	}
	if err := tx.QueryRow(ctx, `
		UPDATE worker_sessions
		SET status = $2,
		    stopped_at = CASE WHEN $2 = 'stopped' THEN CURRENT_TIMESTAMP ELSE NULL END
		WHERE worker_id = $1
		RETURNING status, stopped_at
	`, summary.WorkerID, nextStatus).Scan(&summary.Status, &summary.StoppedAt); err != nil {
		return workerprotocol.WorkerSummary{}, fmt.Errorf("update drained Worker session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return workerprotocol.WorkerSummary{}, fmt.Errorf("commit drained Worker session: %w", err)
	}
	return summary, nil
}

func (r *Repository) GetWorkerSummary(ctx context.Context, workerID string) (workerprotocol.WorkerSummary, error) {
	summaries, err := r.queryWorkerSummaries(ctx, workerID, "", 1, true)
	if err != nil {
		return workerprotocol.WorkerSummary{}, err
	}
	if len(summaries) == 0 {
		return workerprotocol.WorkerSummary{}, ErrWorkerNotFound
	}
	return summaries[0], nil
}

// ListWorkerSummaries 返回按稳定 WorkerID 键集分页的结果，以及是否还有下一条记录。
func (r *Repository) ListWorkerSummaries(ctx context.Context, afterWorkerID string, limit int) ([]workerprotocol.WorkerSummary, bool, error) {
	if limit <= 0 || limit > maxWorkerPageSize {
		return nil, false, fmt.Errorf("Worker page limit must be between 1 and %d", maxWorkerPageSize)
	}
	summaries, err := r.queryWorkerSummaries(ctx, "", afterWorkerID, limit+1, false)
	if err != nil {
		return nil, false, err
	}
	more := len(summaries) > limit
	if more {
		summaries = summaries[:limit]
	}
	return summaries, more, nil
}

func (r *Repository) queryWorkerSummaries(
	ctx context.Context,
	workerID, afterWorkerID string,
	limit int,
	exact bool,
) ([]workerprotocol.WorkerSummary, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			w.worker_id, w.display_name, w.protocol_version, w.executor_kinds,
			w.max_concurrency, w.status, w.registered_at, w.last_heartbeat_at, w.stopped_at,
			(SELECT count(*) FROM task_dispatches d WHERE d.worker_id = w.worker_id AND d.status = 'leased')
		FROM worker_sessions w
		WHERE ($4 AND w.worker_id = $1) OR (NOT $4 AND w.worker_id > $2)
		ORDER BY w.worker_id
		LIMIT $3
	`, workerID, afterWorkerID, limit, exact)
	if err != nil {
		return nil, fmt.Errorf("query Worker summaries: %w", err)
	}
	defer rows.Close()
	summaries := make([]workerprotocol.WorkerSummary, 0, limit)
	for rows.Next() {
		var summary workerprotocol.WorkerSummary
		var executorKinds []string
		if err := rows.Scan(
			&summary.WorkerID,
			&summary.DisplayName,
			&summary.ProtocolVersion,
			&executorKinds,
			&summary.MaxConcurrency,
			&summary.Status,
			&summary.RegisteredAt,
			&summary.LastHeartbeatAt,
			&summary.StoppedAt,
			&summary.ActiveLeases,
		); err != nil {
			return nil, fmt.Errorf("scan Worker summary: %w", err)
		}
		summary.ExecutorKinds = executorKindsFromStrings(executorKinds)
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query Worker summaries: %w", err)
	}
	return summaries, nil
}

func normalizeWorkerRegistration(request workerprotocol.RegisterRequest) (workerprotocol.RegisterRequest, error) {
	request.DisplayName = strings.TrimSpace(request.DisplayName)
	if request.ProtocolVersion != workerprotocol.ProtocolVersion {
		return workerprotocol.RegisterRequest{}, ErrWorkerProtocolUnsupported
	}
	if request.DisplayName == "" || utf8.RuneCountInString(request.DisplayName) > maxWorkerDisplayNameRunes {
		return workerprotocol.RegisterRequest{}, ErrInvalidWorkerRegistration
	}
	if request.MaxConcurrency <= 0 || request.MaxConcurrency > maxWorkerConcurrency {
		return workerprotocol.RegisterRequest{}, ErrInvalidWorkerRegistration
	}
	if len(request.ExecutorKinds) == 0 || len(request.ExecutorKinds) > maxWorkerExecutorKinds {
		return workerprotocol.RegisterRequest{}, ErrInvalidWorkerRegistration
	}
	seen := make(map[workflow.ExecutorKind]struct{}, len(request.ExecutorKinds))
	normalizedKinds := make([]workflow.ExecutorKind, 0, len(request.ExecutorKinds))
	for _, kind := range request.ExecutorKinds {
		if kind != workflow.ExecutorMock {
			return workerprotocol.RegisterRequest{}, ErrInvalidWorkerRegistration
		}
		if _, duplicate := seen[kind]; duplicate {
			continue
		}
		seen[kind] = struct{}{}
		normalizedKinds = append(normalizedKinds, kind)
	}
	request.ExecutorKinds = normalizedKinds
	return request, nil
}

func executorKindStrings(kinds []workflow.ExecutorKind) []string {
	values := make([]string, len(kinds))
	for index, kind := range kinds {
		values[index] = string(kind)
	}
	return values
}

func executorKindsFromStrings(values []string) []workflow.ExecutorKind {
	kinds := make([]workflow.ExecutorKind, len(values))
	for index, value := range values {
		kinds[index] = workflow.ExecutorKind(value)
	}
	return kinds
}
