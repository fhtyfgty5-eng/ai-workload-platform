package containerexec

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// ContainerHandle 是运行时返回的非业务容器句柄。
type ContainerHandle string

// ExitStatus 保存容器进程退出码和运行时是否判定为 OOM。
type ExitStatus struct {
	Code      int
	OOMKilled bool
}

// LogOutput 保存已经受运行时客户端限制的输出内容。
type LogOutput struct {
	Stdout string
	Stderr string
}

// RuntimeClient 抽象 Docker Engine 生命周期，便于单元测试替换真实 SDK。
type RuntimeClient interface {
	Create(context.Context, ContainerSpec) (ContainerHandle, error)
	Start(context.Context, ContainerHandle) error
	Wait(context.Context, ContainerHandle) (ExitStatus, error)
	Stop(context.Context, ContainerHandle) error
	Remove(context.Context, ContainerHandle) error
	Logs(context.Context, ContainerHandle, int64) (LogOutput, error)
}

// DockerExecutor 通过固定 Action 注册表运行一次受限容器 Attempt。
type DockerExecutor struct {
	Runtime  RuntimeClient
	Registry Registry
	Logger   *slog.Logger
}

// Execute 执行容器并把运行时结果映射为工作流封闭结果类型。
func (e *DockerExecutor) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if e == nil || e.Runtime == nil || e.Registry == nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "container_executor_unavailable", ErrorMessage: "container executor dependencies are unavailable"}
	}
	if err := ctx.Err(); err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: err.Error()}
	}
	action, ok := e.Registry.Resolve(request.Action)
	if !ok {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "action_not_registered", ErrorMessage: "action is not registered"}
	}
	if err := e.Registry.ValidateInput(action, request.Input); err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "invalid_action_input", ErrorMessage: err.Error()}
	}
	spec, err := BuildContainerSpec(action, request.Input, string(request.RunID), string(request.TaskKey))
	if err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "invalid_container_spec", ErrorMessage: err.Error()}
	}
	handle, err := e.Runtime.Create(ctx, spec)
	if err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "container_create_failed", ErrorMessage: sanitizeRuntimeError(err)}
	}
	defer func() {
		if removeErr := e.Runtime.Remove(context.Background(), handle); removeErr != nil && e.Logger != nil {
			e.Logger.Warn("container cleanup failed", "error", sanitizeRuntimeError(removeErr))
		}
	}()
	if err := e.Runtime.Start(ctx, handle); err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "container_start_failed", ErrorMessage: sanitizeRuntimeError(err)}
	}
	waitCh := make(chan struct {
		status ExitStatus
		err    error
	}, 1)
	go func() {
		status, waitErr := e.Runtime.Wait(context.Background(), handle)
		waitCh <- struct {
			status ExitStatus
			err    error
		}{status: status, err: waitErr}
	}()
	var waited struct {
		status ExitStatus
		err    error
	}
	select {
	case waited = <-waitCh:
	case <-ctx.Done():
		_ = e.Runtime.Stop(context.Background(), handle)
		return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: ctx.Err().Error()}
	}
	if waited.err != nil {
		if ctx.Err() != nil {
			_ = e.Runtime.Stop(context.Background(), handle)
			return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: ctx.Err().Error()}
		}
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "container_wait_failed", ErrorMessage: sanitizeRuntimeError(waited.err)}
	}
	logs, logErr := e.Runtime.Logs(context.Background(), handle, spec.OutputLimitBytes)
	if logErr != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "container_logs_failed", ErrorMessage: sanitizeRuntimeError(logErr)}
	}
	output := strings.TrimSpace(logs.Stdout)
	if waited.status.OOMKilled {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "oom_killed", ErrorMessage: "container exceeded memory limit"}
	}
	if spec.OutputLimitBytes > 0 && int64(len(logs.Stdout)+len(logs.Stderr)) > spec.OutputLimitBytes {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "output_limit_exceeded", ErrorMessage: "container output exceeded configured limit"}
	}
	if waited.status.Code == 0 {
		return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: output}
	}
	return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "container_exit_nonzero", ErrorMessage: fmt.Sprintf("container exited with code %d", waited.status.Code)}
}

func sanitizeRuntimeError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}
