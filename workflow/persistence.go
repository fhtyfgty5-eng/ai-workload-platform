package workflow

import (
	"context"
	"fmt"
	"time"
)

// Persistence provides incremental commits, recovery queries, and durable cancellation requests.
// 模块 1 的 Store 通过内部适配器继续运行；模块 2 的 PostgreSQL Repository 直接实现该接口。
type Persistence interface {
	RunStore
	// Apply 使用 ExpectedRevision 校验并提交一次状态变更。
	Apply(context.Context, ChangeSet) error
	// ListNonTerminal 返回需要在服务启动时恢复的非终态 Run。
	ListNonTerminal(context.Context) ([]RunID, error)
	// RequestCancel 持久化取消请求，不依赖进程内执行 Context。
	RequestCancel(context.Context, RunID, time.Time) (WorkflowRun, error)
}

type enginePersistence interface {
	RunStore
	Apply(context.Context, ChangeSet) error
}

// IsWorkflowTerminalForStore reports whether a store can exclude a Run from startup recovery.
func IsWorkflowTerminalForStore(status WorkflowStatus) bool {
	return isWorkflowTerminal(status)
}

// snapshotStoreAdapter 将模块 1 的完整快照 Store 适配到 Engine 的增量提交语义。
// 它依赖 Engine 单写者约束；Load、revision 校验和 Save 并不是面向任意并发写者的原子 CAS。
type snapshotStoreAdapter struct {
	base Store
}

func (s snapshotStoreAdapter) Create(ctx context.Context, snapshot RunSnapshot) error {
	return s.base.Create(ctx, snapshot)
}

func (s snapshotStoreAdapter) Apply(ctx context.Context, change ChangeSet) error {
	before, err := s.base.Load(ctx, change.RunID)
	if err != nil {
		return err
	}
	if before.Run.Revision != change.ExpectedRevision {
		return ErrRevisionConflict
	}
	after, err := applyChangeSet(before, change)
	if err != nil {
		return err
	}
	return s.base.Save(ctx, after)
}

func (s snapshotStoreAdapter) Load(ctx context.Context, id RunID) (RunSnapshot, error) {
	return s.base.Load(ctx, id)
}

func asEnginePersistence(store RunStore) (enginePersistence, error) {
	if persistence, ok := store.(enginePersistence); ok {
		return persistence, nil
	}
	if snapshotStore, ok := store.(Store); ok {
		return snapshotStoreAdapter{base: snapshotStore}, nil
	}
	return nil, fmt.Errorf("store must implement Apply or Save")
}
