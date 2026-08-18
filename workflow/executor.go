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
type ExecutionRequest struct {
	// DefinitionID 用于区分不同工作流定义中可能重复出现的 TaskKey。
	DefinitionID string
	// RunID 标识本次运行实例，使同一任务在不同 Run 中拥有独立执行历史。
	RunID RunID
	// TaskKey 标识本次 Attempt 所属的任务。
	TaskKey TaskKey
	// Action 是由具体 Executor 解释的动作名称。
	Action string
	// Attempt 是从 1 开始递增的本任务执行尝试编号。
	Attempt int
}

// ExecutionResponse 保存 Executor 的结构化结果，不直接改变调度状态。
type ExecutionResponse struct {
	// Kind 标识执行成功、临时失败、永久失败或取消等结果类别。
	Kind ResultKind
	// Output 是执行器返回的最小结果；模块 1 暂不提供独立产物存储。
	Output string
	// ErrorCode 是供调度与调用方识别错误类别的稳定机器码。
	ErrorCode string
	// ErrorMessage 是供人阅读的错误说明，不参与重试分类判断。
	ErrorMessage string
}

// Executor 执行单次任务尝试，并通过 Context 响应超时和取消。
type Executor interface {
	// Execute 执行一次任务尝试并返回结构化结果。
	Execute(context.Context, ExecutionRequest) ExecutionResponse
}
