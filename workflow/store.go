package workflow

import (
	"context"
	"errors"
)

var (
	ErrRunExists                = errors.New("workflow run already exists")
	ErrRunNotFound              = errors.New("workflow run not found")
	ErrAtomicReplaceUnsupported = errors.New("atomic file replacement is unsupported on this platform")
)

// Store 持久化完整运行快照；Engine 只在保存成功后推进调度。
type Store interface {
	// Create 持久化新 Run，存在相同 RunID 时返回 ErrRunExists。
	Create(context.Context, RunSnapshot) error
	// Save 原子更新已有 Run，不存在时返回 ErrRunNotFound。
	Save(context.Context, RunSnapshot) error
	// Load 返回指定 RunID 的完整快照。
	Load(context.Context, RunID) (RunSnapshot, error)
}
