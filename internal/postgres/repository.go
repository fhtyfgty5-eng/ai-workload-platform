package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
	ErrWorkflowExists      = errors.New("workflow already exists")
	ErrWorkflowNotFound    = errors.New("workflow not found")
	ErrDefinitionNotFound  = errors.New("workflow definition version not found")
	ErrTaskNotFound        = errors.New("workflow task run not found")
	ErrCoordinatorLockLost = errors.New("coordinator advisory lock is no longer held")
)

const (
	operationCreateWorkflow = "create_workflow"
	operationCreateVersion  = "create_workflow_version"
	operationStartRun       = "start_run"
	resourceWorkflowVersion = "workflow_version"
	resourceRun             = "run"
)

// DefinitionRecord identifies one immutable workflow definition version.
type DefinitionRecord struct {
	// WorkflowID is the stable logical workflow identifier.
	WorkflowID string
	// Version is the immutable version number within WorkflowID.
	Version int
}

type WorkflowRecord struct {
	WorkflowID    string
	LatestVersion int
	CreatedAt     time.Time
	CreatedBy     string
}

type VersionRecord struct {
	WorkflowID string
	Version    int
	CreatedAt  time.Time
	CreatedBy  string
}

type RunRecord struct {
	ID                workflow.RunID
	DefinitionID      string
	DefinitionVersion int
	Status            workflow.WorkflowStatus
	Revision          uint64
	TaskCount         int
	CreatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        *time.Time
}

// RunQuery 描述 Run 列表的可选过滤条件和上一页最后一个稳定排序键。
type RunQuery struct {
	WorkflowID   string
	Status       workflow.WorkflowStatus
	AfterCreated *time.Time
	AfterRunID   workflow.RunID
}

// TaskRecord 保留数据库内部 task_index，应用层用它生成稳定游标但不会公开该字段。
type TaskRecord struct {
	Index int
	Task  workflow.TaskRun
}

// Repository stores workflow definitions and Run state in PostgreSQL.
type Repository struct {
	pool *pgxpool.Pool
}

const coordinatorAdvisoryLockKey int64 = 82473619

type advisoryLock struct {
	// mu 防止健康检查与释放连接并发；Coordinator 正常关闭会先停止检查，再调用 Close。
	mu   sync.Mutex
	conn *pgxpool.Conn
}

func (l *advisoryLock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil
	}
	_, err := l.conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", coordinatorAdvisoryLockKey)
	l.conn.Release()
	l.conn = nil
	return err
}

// Check 在持锁连接对应的 PostgreSQL 会话中确认 Advisory Lock 仍存在，不增加可重入锁计数。
func (l *advisoryLock) Check(ctx context.Context) error {
	if l == nil {
		return ErrCoordinatorLockLost
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return ErrCoordinatorLockLost
	}
	var held bool
	err := l.conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND pid = pg_backend_pid()
			  AND classid::bigint = (($1::bigint >> 32) & 4294967295)
			  AND objid::bigint = ($1::bigint & 4294967295)
		)
	`, coordinatorAdvisoryLockKey).Scan(&held)
	if err != nil {
		return fmt.Errorf("check coordinator advisory lock: %w", err)
	}
	if !held {
		return ErrCoordinatorLockLost
	}
	return nil
}

// AcquireAdvisoryLock keeps one dedicated pool connection while the control plane is active.
func (r *Repository) AcquireAdvisoryLock(ctx context.Context) (*advisoryLock, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire database connection for coordinator lock: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", coordinatorAdvisoryLockKey).Scan(&acquired); err != nil {
		conn.Release()
		return nil, fmt.Errorf("try coordinator advisory lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("coordinator advisory lock is already held")
	}
	return &advisoryLock{conn: conn}, nil
}

// New connects to PostgreSQL and verifies connectivity without changing the schema.
func New(ctx context.Context, databaseURL string) (*Repository, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("database URL is required")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return &Repository{pool: pool}, nil
}

// Close releases all PostgreSQL connections owned by the Repository.
func (r *Repository) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

type idempotencyRecord struct {
	requestHash     string
	resourceType    string
	resourceID      string
	resourceVersion *int
}

func loadIdempotency(ctx context.Context, tx pgx.Tx, principal, operation, key string) (idempotencyRecord, bool, error) {
	var record idempotencyRecord
	err := tx.QueryRow(ctx, `
		SELECT request_hash, resource_type, resource_id, resource_version
		FROM idempotency_records
		WHERE principal_id = $1 AND operation = $2 AND idempotency_key = $3
	`, principal, operation, key).Scan(
		&record.requestHash,
		&record.resourceType,
		&record.resourceID,
		&record.resourceVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return idempotencyRecord{}, false, nil
	}
	if err != nil {
		return idempotencyRecord{}, false, fmt.Errorf("load idempotency record: %w", err)
	}
	return record, true, nil
}

func lockIdempotency(ctx context.Context, tx pgx.Tx, principal, operation, key string) error {
	// 长度前缀避免用户输入中的分隔符改变组合键边界，也满足 PostgreSQL text 的 UTF-8 约束。
	lockKey := fmt.Sprintf("%d:%s%d:%s%d:%s", len(principal), principal, len(operation), operation, len(key), key)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return fmt.Errorf("lock idempotency key: %w", err)
	}
	return nil
}

func insertIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	principal, operation, key, requestHash, resourceType, resourceID string,
	resourceVersion *int,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO idempotency_records (
			principal_id, operation, idempotency_key, request_hash,
			resource_type, resource_id, resource_version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
	`, principal, operation, key, requestHash, resourceType, resourceID, resourceVersion)
	if err != nil {
		return fmt.Errorf("insert idempotency record: %w", err)
	}
	return nil
}

func validateIdempotencyInput(principal, key, requestHash string) error {
	if strings.TrimSpace(principal) == "" {
		return fmt.Errorf("principal is required")
	}
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("idempotency key is required")
	}
	if strings.TrimSpace(requestHash) == "" {
		return fmt.Errorf("request hash is required")
	}
	return nil
}

func validateReplay(record idempotencyRecord, requestHash, resourceType string) error {
	if record.requestHash != requestHash || record.resourceType != resourceType {
		return ErrIdempotencyConflict
	}
	return nil
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}
