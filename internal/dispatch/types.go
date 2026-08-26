package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

var (
	ErrCoordinatorNotReady = errors.New("dispatch coordinator is not ready")
	ErrCoordinatorClosed   = errors.New("dispatch coordinator is closed")
)

// Store 是分发协调器与 Worker API 共享的持久化边界。
type Store interface {
	CancelRequestedRuns(context.Context, int) (int, error)
	CreateDispatches(context.Context, int) (int, error)
	Claim(context.Context, string, int) ([]workerprotocol.Lease, error)
	Heartbeat(context.Context, string, []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error)
	Complete(context.Context, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error)
	ReapExpired(context.Context, int) (int, error)
	CancelRun(context.Context, workflow.RunID) error
	ListWorkerSummaries(context.Context, string, int) ([]workerprotocol.WorkerSummary, bool, error)
}

// Lock 表示协调器持有的单实例数据库锁，支持周期检查所有权和显式释放。
type Lock interface {
	Check(context.Context) error
	Close() error
}

// LockAcquirer 获取一个在连接存活期间持续持有的协调器锁。
type LockAcquirer func(context.Context) (Lock, error)

// CoordinatorOptions 控制扫描频率、锁健康检查和单轮分发规模。
type CoordinatorOptions struct {
	// Metrics 可选；为空时不记录指标，避免改变旧调用方行为。
	Metrics *observability.Metrics
	// ObserveMetrics 在每轮扫描后刷新数据库聚合 Gauge；回调错误不会阻断调度。
	ObserveMetrics func(context.Context)
	// ScanInterval 是无唤醒信号时重新扫描数据库的最长等待时间。
	ScanInterval time.Duration
	// LockCheckInterval 和 LockCheckTimeout 控制协调锁的周期健康检查。
	LockCheckInterval time.Duration
	LockCheckTimeout  time.Duration
	// BatchSize 限制一次创建或回收的 Dispatch 数量，避免单轮事务无限扩大。
	BatchSize int
}

// Coordinator 是控制面启动、唤醒、取消和关闭分发循环所依赖的接口。
type Coordinator interface {
	Start(context.Context) error
	Wake()
	Cancel(context.Context, workflow.RunID) error
	Ready() bool
	Errors() <-chan error
	Close(context.Context) error
}
