package faultinject

import (
	"context"
	"errors"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Client 定义 Worker Runtime 所需的协议操作；包装器只在测试夹具中实现它。
type Client interface {
	Register(context.Context, string, workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error)
	Claim(context.Context, string, string, int) (workerprotocol.ClaimResponse, error)
	Heartbeat(context.Context, string, string, workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error)
	Complete(context.Context, string, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error)
	Drain(context.Context, string, string) (workerprotocol.WorkerSummary, error)
}

// ClientWrapper 在 Claim、Heartbeat 和 Complete 的调用边界注入测试故障。
type ClientWrapper struct {
	inner Client
	plan  *Plan
}

func NewClientWrapper(inner Client, plan *Plan) *ClientWrapper {
	return &ClientWrapper{inner: inner, plan: plan}
}

func (c *ClientWrapper) Register(ctx context.Context, token string, request workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error) {
	return c.inner.Register(ctx, token, request)
}

func (c *ClientWrapper) Claim(ctx context.Context, workerID, sessionToken string, slots int) (workerprotocol.ClaimResponse, error) {
	if err := c.before(ctx, OperationClaim); err != nil {
		return workerprotocol.ClaimResponse{}, err
	}
	return c.inner.Claim(ctx, workerID, sessionToken, slots)
}

func (c *ClientWrapper) Heartbeat(ctx context.Context, workerID, sessionToken string, request workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error) {
	if err := c.before(ctx, OperationHeartbeat); err != nil {
		return workerprotocol.HeartbeatResponse{}, err
	}
	return c.inner.Heartbeat(ctx, workerID, sessionToken, request)
}

func (c *ClientWrapper) Complete(ctx context.Context, workerID, dispatchID, sessionToken string, request workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	if err := c.before(ctx, OperationComplete); err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	return c.inner.Complete(ctx, workerID, dispatchID, sessionToken, request)
}

func (c *ClientWrapper) Drain(ctx context.Context, workerID, sessionToken string) (workerprotocol.WorkerSummary, error) {
	return c.inner.Drain(ctx, workerID, sessionToken)
}

func (c *ClientWrapper) before(ctx context.Context, operation Operation) error {
	if c == nil || c.plan == nil {
		return nil
	}
	return c.plan.Before(ctx, operation)
}

// Repository 定义故障实验需要的持久化操作边界。
type Repository interface {
	Claim(context.Context, string, int) ([]workerprotocol.Lease, error)
	Heartbeat(context.Context, string, []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error)
	Complete(context.Context, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error)
	CreateDispatches(context.Context, int) (int, error)
	ReapExpired(context.Context, int) (int, error)
}

// RepositoryWrapper 在数据库操作前注入 postgres、claim、heartbeat、complete 和扫描故障。
type RepositoryWrapper struct {
	inner Repository
	plan  *Plan
}

func NewRepositoryWrapper(inner Repository, plan *Plan) *RepositoryWrapper {
	return &RepositoryWrapper{inner: inner, plan: plan}
}

func (r *RepositoryWrapper) Claim(ctx context.Context, workerID string, slots int) ([]workerprotocol.Lease, error) {
	if err := r.before(ctx, OperationClaim); err != nil {
		return nil, err
	}
	return r.inner.Claim(ctx, workerID, slots)
}

func (r *RepositoryWrapper) Heartbeat(ctx context.Context, workerID string, leases []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error) {
	if err := r.before(ctx, OperationHeartbeat); err != nil {
		return workerprotocol.HeartbeatResponse{}, err
	}
	return r.inner.Heartbeat(ctx, workerID, leases)
}

func (r *RepositoryWrapper) Complete(ctx context.Context, workerID, dispatchID string, request workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	if err := r.before(ctx, OperationComplete); err != nil {
		return workerprotocol.CompleteResponse{}, err
	}
	return r.inner.Complete(ctx, workerID, dispatchID, request)
}

func (r *RepositoryWrapper) CreateDispatches(ctx context.Context, limit int) (int, error) {
	if err := r.before(ctx, OperationCoordinatorScan); err != nil {
		return 0, err
	}
	return r.inner.CreateDispatches(ctx, limit)
}

func (r *RepositoryWrapper) ReapExpired(ctx context.Context, limit int) (int, error) {
	if err := r.before(ctx, OperationPostgres); err != nil {
		return 0, err
	}
	return r.inner.ReapExpired(ctx, limit)
}

func (r *RepositoryWrapper) before(ctx context.Context, operation Operation) error {
	if r == nil || r.plan == nil {
		return nil
	}
	return r.plan.Before(ctx, operation)
}

// Lock 定义协调器锁的最小操作边界。
type Lock interface {
	Check(context.Context) error
	Close() error
}

// LockWrapper 在健康检查前注入协调器锁失效。
type LockWrapper struct {
	inner Lock
	plan  *Plan
}

func NewLockWrapper(inner Lock, plan *Plan) *LockWrapper {
	return &LockWrapper{inner: inner, plan: plan}
}

func (l *LockWrapper) Check(ctx context.Context) error {
	if l != nil && l.plan != nil {
		if err := l.plan.Before(ctx, OperationCoordinatorLock); err != nil {
			return err
		}
	}
	return l.inner.Check(ctx)
}

func (l *LockWrapper) Close() error { return l.inner.Close() }

// ExecutorWrapper 在一次任务执行前注入错误、取消或延迟。
type ExecutorWrapper struct {
	inner workflow.Executor
	plan  *Plan
}

func NewExecutorWrapper(inner workflow.Executor, plan *Plan) *ExecutorWrapper {
	return &ExecutorWrapper{inner: inner, plan: plan}
}

func (e *ExecutorWrapper) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if e != nil && e.plan != nil {
		if err := e.plan.Before(ctx, OperationWorkerExecute); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "fault_injected", ErrorMessage: err.Error()}
			}
			return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "fault_injected", ErrorMessage: err.Error()}
		}
	}
	return e.inner.Execute(ctx, request)
}

// ClockWrapper 保持 workflow.Clock 兼容，并为需要 Context 的实验提供可取消等待。
type ClockWrapper struct {
	inner workflow.Clock
	plan  *Plan
}

func NewClockWrapper(inner workflow.Clock, plan *Plan) *ClockWrapper {
	if inner == nil {
		inner = workflow.RealClock{}
	}
	return &ClockWrapper{inner: inner, plan: plan}
}

func (c *ClockWrapper) Now() time.Time { return c.inner.Now() }

func (c *ClockWrapper) After(delay time.Duration) <-chan time.Time { return c.inner.After(delay) }

// Wait 在 Clock 等待前应用 worker_execute 故障计划，供测试夹具获得错误返回和取消语义。
func (c *ClockWrapper) Wait(ctx context.Context, delay time.Duration) error {
	if c != nil && c.plan != nil {
		if err := c.plan.Before(ctx, OperationWorkerExecute); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.inner.After(delay):
		return nil
	}
}

var _ workflow.Clock = (*ClockWrapper)(nil)
