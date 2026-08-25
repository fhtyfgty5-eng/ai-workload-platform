package agent

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Limits 限制一次 Agent 生成会话的循环次数、响应大小和总时长。
type Limits struct {
	// MaxModelTurns 和 MaxToolCalls 限制模型续轮与受控工具调用总次数。
	MaxModelTurns int
	MaxToolCalls  int
	// MaxResponseBytes 限制单次模型或工具响应；两个 Timeout 分别限制整次会话和单次工具调用。
	MaxResponseBytes int
	RuntimeTimeout   time.Duration
	ToolTimeout      time.Duration
}

// DefaultLimits 返回模块 3 第一版的保守默认上限。
func DefaultLimits() Limits {
	return Limits{MaxModelTurns: 4, MaxToolCalls: 8, MaxResponseBytes: 64 * 1024, RuntimeTimeout: 30 * time.Second, ToolTimeout: 5 * time.Second}
}

// Budget 是一次生成会话独享的并发安全计数器。
type Budget struct {
	// mu 保护两个已用计数；limits 在构造后只读，可由并发模型与工具路径共享。
	mu     sync.Mutex
	limits Limits
	// modelTurns 和 toolCalls 记录已消耗额度，而不是剩余额度。
	modelTurns int
	toolCalls  int
}

// NewBudget 创建一次生成会话独享的预算计数器。
func NewBudget(limits Limits) *Budget { return &Budget{limits: limits} }

// Context 返回受 Runtime 总时长限制的子 Context。
func (b *Budget) Context(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, b.limits.RuntimeTimeout)
}

// UseModelTurn 消耗一次模型调用额度，额度耗尽时拒绝调用。
func (b *Budget) UseModelTurn() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.modelTurns >= b.limits.MaxModelTurns {
		return &Error{Code: CodeBudgetExceeded, Message: "model turn limit reached"}
	}
	b.modelTurns++
	return nil
}

// UseToolCall 消耗一次工具调用额度，额度耗尽时拒绝调用。
func (b *Budget) UseToolCall() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.toolCalls >= b.limits.MaxToolCalls {
		return &Error{Code: CodeBudgetExceeded, Message: "tool call limit reached"}
	}
	b.toolCalls++
	return nil
}

// CheckResponseSize 拒绝超过单次响应字节上限的模型或工具结果。
func (b *Budget) CheckResponseSize(size int) error {
	if size > b.limits.MaxResponseBytes {
		return &Error{Code: CodeBudgetExceeded, Message: fmt.Sprintf("response exceeds %d bytes", b.limits.MaxResponseBytes)}
	}
	return nil
}

// Usage 返回当前已经消耗的模型轮数和工具调用数快照。
func (b *Budget) Usage() (modelTurns, toolCalls int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.modelTurns, b.toolCalls
}

// ModelLimits 返回下一次模型请求可见的剩余额度。
func (b *Budget) ModelLimits() ModelLimits {
	b.mu.Lock()
	defer b.mu.Unlock()
	return ModelLimits{
		MaxResponseBytes:    b.limits.MaxResponseBytes,
		RemainingModelTurns: b.limits.MaxModelTurns - b.modelTurns,
		RemainingToolCalls:  b.limits.MaxToolCalls - b.toolCalls,
	}
}
