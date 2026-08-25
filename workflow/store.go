package workflow

import (
	"context"
	"errors"
)

var (
	ErrRunExists                = errors.New("workflow run already exists")
	ErrRunNotFound              = errors.New("workflow run not found")
	ErrRevisionConflict         = errors.New("workflow run revision conflict")
	ErrInvalidChangeSet         = errors.New("invalid workflow change set")
	ErrAtomicReplaceUnsupported = errors.New("atomic file replacement is unsupported on this platform")
)

// RunStore 定义各持久化实现共享的最小创建和加载操作。
type RunStore interface {
	// Create 持久化新 Run，存在相同 RunID 时返回 ErrRunExists。
	Create(context.Context, RunSnapshot) error
	// Load 返回指定 RunID 的完整快照。
	Load(context.Context, RunID) (RunSnapshot, error)
}

// Store 保留模块 1 的完整快照持久化契约。
type Store interface {
	RunStore
	// Save 原子更新已有 Run，不存在时返回 ErrRunNotFound。
	Save(context.Context, RunSnapshot) error
}
