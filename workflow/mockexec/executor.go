package mockexec

import (
	"context"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Step 描述 Mock Executor 为一个脚本模板配置的一次行为。
type Step struct {
	Kind                workflow.ResultKind
	Output              string
	ErrorCode           string
	ErrorMessage        string
	Delay               time.Duration
	WaitForCancellation bool
}

// ScriptKey 唯一定位一个已通过编译校验的工作流定义中的任务脚本。
// DefinitionID 和 TaskKey 必须来自该定义；零值不是有效脚本键。
type ScriptKey struct {
	DefinitionID string
	TaskKey      workflow.TaskKey
}

// Executor 以 ScriptKey 查找脚本模板，并按每个 Run 内任务的顺序消费 Step。
type Executor struct {
	clock workflow.Clock
	mu    sync.Mutex
	// scripts 以定义和任务共同定位脚本，避免不同工作流的同名任务混用测试结果。
	scripts map[ScriptKey][]Step
	// next 为每个 Run 内任务记录已消费的 Step 数量，受 mu 保护以避免并发重复消费。
	next map[runTaskKey]int
}

type runTaskKey struct {
	runID   workflow.RunID
	taskKey workflow.TaskKey
}

// New 创建使用给定可控时钟和任务脚本的 Mock Executor。
func New(clock workflow.Clock, scripts map[ScriptKey][]Step) *Executor {
	if clock == nil {
		clock = workflow.RealClock{}
	}
	clonedScripts := make(map[ScriptKey][]Step, len(scripts))
	for scriptKey, steps := range scripts {
		clonedScripts[scriptKey] = append([]Step(nil), steps...)
	}
	return &Executor{clock: clock, scripts: clonedScripts, next: make(map[runTaskKey]int)}
}

// Execute 消费对应 Run 内任务的下一个 Step，并协作响应取消信号。
func (e *Executor) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if request.DefinitionID == "" {
		return workflow.ExecutionResponse{
			Kind:         workflow.ResultPermanentFailure,
			ErrorCode:    "invalid_execution_request",
			ErrorMessage: "definition id is required",
		}
	}

	e.mu.Lock()
	key := runTaskKey{runID: request.RunID, taskKey: request.TaskKey}
	steps := e.scripts[ScriptKey{DefinitionID: request.DefinitionID, TaskKey: request.TaskKey}]
	index := e.next[key]
	if index >= len(steps) {
		e.mu.Unlock()
		return workflow.ExecutionResponse{
			Kind:         workflow.ResultPermanentFailure,
			ErrorCode:    "script_exhausted",
			ErrorMessage: "no scripted result",
		}
	}
	step := steps[index]
	e.next[key] = index + 1
	e.mu.Unlock()

	if step.WaitForCancellation {
		<-ctx.Done()
		return canceledResponse(ctx)
	}
	if step.Delay > 0 {
		select {
		case <-ctx.Done():
			return canceledResponse(ctx)
		case <-e.clock.After(step.Delay):
		}
	}
	return workflow.ExecutionResponse{
		Kind:         step.Kind,
		Output:       step.Output,
		ErrorCode:    step.ErrorCode,
		ErrorMessage: step.ErrorMessage,
	}
}

func canceledResponse(ctx context.Context) workflow.ExecutionResponse {
	response := workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled"}
	if err := ctx.Err(); err != nil {
		response.ErrorMessage = err.Error()
	}
	return response
}
