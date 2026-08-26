// Package faultinject 提供仅用于测试和实验夹具的可关闭故障计划。
package faultinject

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Operation 是允许注入故障的封闭操作名称，生产配置不能动态扩展它。
type Operation string

const (
	OperationPostgres        Operation = "postgres"
	OperationClaim           Operation = "claim"
	OperationHeartbeat       Operation = "heartbeat"
	OperationComplete        Operation = "complete"
	OperationCoordinatorLock Operation = "coordinator_lock"
	OperationCoordinatorScan Operation = "coordinator_scan"
	OperationWorkerExecute   Operation = "worker_execute"
	OperationPoolAcquire     Operation = "pool_acquire"
)

// Action 描述一次测试故障。Remaining 表示首次执行后额外重复次数，0 表示只执行一次。
type Action struct {
	Delay time.Duration
	Err   error
	// Cancel 让调用表现为 Context 已取消；它不调用外部 cancel 函数。
	Cancel bool
	// Remaining 每次消费后递减；动作耗尽后才读取同一 Operation 的下一项。
	Remaining int
}

// Plan 按 Operation 串行消费动作；互斥锁保证并发测试不会重复消费同一次故障。
type Plan struct {
	mu      sync.Mutex
	actions map[Operation][]Action
	closed  bool
}

// NewPlan 深拷贝动作切片，使调用方后续修改输入不会改变正在运行的实验。
func NewPlan(input map[Operation][]Action) (*Plan, error) {
	plan := &Plan{actions: make(map[Operation][]Action, len(input))}
	for operation, actions := range input {
		if !validOperation(operation) {
			return nil, fmt.Errorf("unsupported fault operation %q", operation)
		}
		copyActions := make([]Action, len(actions))
		copy(copyActions, actions)
		for i := range copyActions {
			if copyActions[i].Delay < 0 || copyActions[i].Remaining < 0 {
				return nil, fmt.Errorf("invalid fault action for %s", operation)
			}
		}
		plan.actions[operation] = copyActions
	}
	return plan, nil
}

// Close 幂等关闭后续注入；已经离开互斥区并开始等待的动作仍由其 Context 控制。
func (p *Plan) Close() { p.mu.Lock(); p.closed = true; p.mu.Unlock() }

// Before 在被包装操作执行前消费一个动作。
func (p *Plan) Before(ctx context.Context, operation Operation) error { return p.apply(ctx, operation) }

// After 在被包装操作成功返回后消费一个动作。
func (p *Plan) After(ctx context.Context, operation Operation) error { return p.apply(ctx, operation) }

func (p *Plan) apply(ctx context.Context, operation Operation) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	actions := p.actions[operation]
	if len(actions) == 0 {
		p.mu.Unlock()
		return nil
	}
	action := actions[0]
	if action.Remaining == 0 {
		p.actions[operation] = actions[1:]
	} else {
		action.Remaining--
		p.actions[operation][0] = action
	}
	p.mu.Unlock()
	if action.Delay > 0 {
		timer := time.NewTimer(action.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if action.Cancel {
		return context.Canceled
	}
	return action.Err
}

func validOperation(operation Operation) bool {
	switch operation {
	case OperationPostgres, OperationClaim, OperationHeartbeat, OperationComplete, OperationCoordinatorLock, OperationCoordinatorScan, OperationWorkerExecute, OperationPoolAcquire:
		return true
	default:
		return false
	}
}
