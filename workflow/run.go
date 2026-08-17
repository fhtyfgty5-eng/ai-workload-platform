package workflow

import "time"

// RunID 是工作流运行实例的唯一标识。
type RunID string

// ExecutionResult 保存执行器返回的最小结果和错误信息。
type ExecutionResult struct {
	Output       string `json:"output,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// Attempt 记录任务的一次具体执行，不会被后续重试覆盖。
type Attempt struct {
	Number     int             `json:"number"`
	Status     AttemptStatus   `json:"status"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	Result     ExecutionResult `json:"result"`
}

// TaskRun 保存任务在某次工作流运行中的状态和全部尝试历史。
type TaskRun struct {
	Key        TaskKey    `json:"key"`
	Status     TaskStatus `json:"status"`
	Attempts   []Attempt  `json:"attempts"`
	ReadyAt    *time.Time `json:"ready_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// StateEvent 记录一次已持久化状态转换的审计信息。
type StateEvent struct {
	Sequence uint64    `json:"sequence"`
	At       time.Time `json:"at"`
	Entity   string    `json:"entity"`
	Key      string    `json:"key"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	Reason   string    `json:"reason,omitempty"`
}

// WorkflowRun 保存一次工作流运行的状态、任务和依赖计数。
type WorkflowRun struct {
	ID                    RunID          `json:"id"`
	DefinitionID          string         `json:"definition_id"`
	Status                WorkflowStatus `json:"status"`
	Tasks                 []TaskRun      `json:"tasks"`
	RemainingDependencies []int          `json:"remaining_dependencies"`
	CreatedAt             time.Time      `json:"created_at"`
	StartedAt             *time.Time     `json:"started_at,omitempty"`
	FinishedAt            *time.Time     `json:"finished_at,omitempty"`
}

// RunSnapshot 是 Store 原子读写的完整持久化单元。
type RunSnapshot struct {
	Version    int                 `json:"version"`
	Definition *WorkflowDefinition `json:"definition"`
	Run        WorkflowRun         `json:"run"`
	Events     []StateEvent        `json:"events"`
}

func newRunSnapshot(id RunID, compiled *CompiledWorkflow, now time.Time) RunSnapshot {
	tasks := make([]TaskRun, len(compiled.definition.Tasks))
	remaining := make([]int, len(tasks))
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
