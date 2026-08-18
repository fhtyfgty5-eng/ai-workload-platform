package workflow

import "time"

// RunID 是工作流运行实例的唯一标识。
type RunID string

// ExecutionResult 保存执行器返回的最小结果和错误信息。
type ExecutionResult struct {
	// Output 保存 Executor 返回的最小文本结果。
	Output string `json:"output,omitempty"`
	// ErrorCode 保存稳定的机器可读错误码。
	ErrorCode string `json:"error_code,omitempty"`
	// ErrorMessage 保存面向人的错误说明。
	ErrorMessage string `json:"error_message,omitempty"`
}

// Attempt 记录任务的一次具体执行，不会被后续重试覆盖。
type Attempt struct {
	// Number 是同一任务内从 1 开始递增的尝试编号，也是过滤迟到结果的一部分。
	Number int `json:"number"`
	// Status 只描述本次尝试，不等同于所属 Task 的最终状态。
	Status AttemptStatus `json:"status"`
	// StartedAt 记录本次 Attempt 转为 running 的时间。
	StartedAt time.Time `json:"started_at"`
	// FinishedAt 仅在 Attempt 进入终态后设置。
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Result 保存本次 Attempt 的输出或错误，后续重试不会覆盖它。
	Result ExecutionResult `json:"result"`
}

// TaskRun 保存任务在某次工作流运行中的状态和全部尝试历史。
type TaskRun struct {
	// Key 对应工作流定义中的 TaskKey。
	Key TaskKey `json:"key"`
	// Status 是任务级调度状态，可跨越多个 Attempt。
	Status TaskStatus `json:"status"`
	// Attempts 按 Number 顺序保存全部执行历史。
	Attempts []Attempt `json:"attempts"`
	// ReadyAt 仅在 waiting_retry 状态设置，表示下次 Attempt 最早可启动的时间。
	ReadyAt *time.Time `json:"ready_at,omitempty"`
	// FinishedAt 仅在任务进入成功、失败、取消或跳过终态后设置。
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// StateEvent 记录一次已持久化状态转换的审计信息。
type StateEvent struct {
	// Sequence 是快照内从 1 递增的事件序号，表示状态变化的原始顺序。
	Sequence uint64 `json:"sequence"`
	// At 是 Engine 记录的状态转换发生时间。
	At time.Time `json:"at"`
	// Entity 标识 workflow、task 或 attempt 三种事件主体。
	Entity string `json:"entity"`
	// Key 定位主体；Attempt 使用“TaskKey/AttemptNumber”形式。
	Key string `json:"key"`
	// From 和 To 保存本次状态转换的起点与终点。
	From string `json:"from"`
	To   string `json:"to"`
	// Reason 说明触发转换的业务原因。
	Reason string `json:"reason,omitempty"`
}

// WorkflowRun 保存一次工作流运行的状态、任务和依赖计数。
type WorkflowRun struct {
	// ID 是系统为本次运行生成的唯一 RunID。
	ID RunID `json:"id"`
	// DefinitionID 标识本次运行采用的 WorkflowDefinition。
	DefinitionID string `json:"definition_id"`
	// Status 是整个 Run 的生命周期状态。
	Status WorkflowStatus `json:"status"`
	// Tasks 与编译后定义保持相同下标，调度器据此进行 O(1) 定位。
	Tasks []TaskRun `json:"tasks"`
	// RemainingDependencies[i] 是第 i 个任务尚未成功的直接依赖数量，每个 Run 独立维护。
	RemainingDependencies []int `json:"remaining_dependencies"`
	// CreatedAt、StartedAt 和 FinishedAt 分别记录创建、首次启动与进入终态的时间。
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// RunSnapshot 是 Store 原子读写的完整持久化单元。
type RunSnapshot struct {
	// Version 是持久化结构版本，为后续兼容迁移保留。
	Version int `json:"version"`
	// Definition 保存恢复调度所需的完整定义副本。
	Definition *WorkflowDefinition `json:"definition"`
	// Run 保存当前聚合状态和各任务执行历史。
	Run WorkflowRun `json:"run"`
	// Events 按 Sequence 保存状态转换审计记录。
	Events []StateEvent `json:"events"`
}

// newRunSnapshot 从只读编译结果创建每个 Run 独享的状态、Attempt 历史和依赖计数。
func newRunSnapshot(id RunID, compiled *CompiledWorkflow, now time.Time) RunSnapshot {
	tasks := make([]TaskRun, len(compiled.definition.Tasks))
	remaining := make([]int, len(tasks))
	// 没有依赖的任务初始即可调度；其余任务等待各自计数降到零。
	for i, task := range compiled.definition.Tasks {
		status := TaskWaitingDependencies
		remaining[i] = len(compiled.dependencies[i])
		if remaining[i] == 0 {
			status = TaskReady
		}
		tasks[i] = TaskRun{Key: task.Key, Status: status, Attempts: []Attempt{}}
	}

	return RunSnapshot{
		Version:    1,
		Definition: compiled.definition,
		Run: WorkflowRun{
			ID:                    id,
			DefinitionID:          compiled.definition.ID,
			Status:                WorkflowPending,
			Tasks:                 tasks,
			RemainingDependencies: remaining,
			CreatedAt:             now,
		},
		Events: []StateEvent{},
	}
}
