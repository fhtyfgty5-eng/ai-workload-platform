package workflow

import "context"

// ResultKind 是 Executor 返回给 Engine 的结构化结果分类。
type ResultKind string

const (
	ResultSuccess          ResultKind = "success"
	ResultTemporaryFailure ResultKind = "temporary_failure"
	ResultPermanentFailure ResultKind = "permanent_failure"
	ResultCanceled         ResultKind = "canceled"
)

// ExecutionRequest 描述一次具体任务尝试的执行输入。
// DefinitionID 来自已编译的 WorkflowDefinition，用于区分不同工作流中的同名任务。
type ExecutionRequest struct {
	DefinitionID string
	RunID        RunID
	TaskKey      TaskKey
	Action       string
	Attempt      int
}

// ExecutionResponse 保存 Executor 的结构化结果，不直接改变调度状态。
type ExecutionResponse struct {
	Kind         ResultKind
	Output       string
	ErrorCode    string
	ErrorMessage string
}

// Executor 执行单次任务尝试，并通过 Context 响应超时和取消。
type Executor interface {
	// Execute 执行一次任务尝试并返回结构化结果。
	Execute(context.Context, ExecutionRequest) ExecutionResponse
}
