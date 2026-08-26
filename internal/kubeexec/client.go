package kubeexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// JobHandle 标识一个 Kubernetes Job，不是业务 RunID。
type JobHandle string

// JobStatus 保存 Kubernetes Job 的最小终态信息。
type JobStatus struct {
	Succeeded bool
	Failed    bool
	OOMKilled bool
	Reason    string
}

// KubernetesClient 抽象 Job 创建、等待、日志和删除操作。
type KubernetesClient interface {
	CreateJob(context.Context, JobSpec) (JobHandle, error)
	WaitJob(context.Context, JobHandle) (JobStatus, error)
	DeleteJob(context.Context, JobHandle) error
	Logs(context.Context, JobHandle, int64) (containerexec.LogOutput, error)
}

// KubernetesExecutor 通过固定 Job 描述运行一次受限 Attempt。
type KubernetesExecutor struct {
	Client   KubernetesClient
	Registry containerexec.Registry
}

// Execute 创建 Job、等待终态、读取有限输出并清理 Job。
func (e *KubernetesExecutor) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if e == nil || e.Client == nil || e.Registry == nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "kubernetes_executor_unavailable", ErrorMessage: "kubernetes executor dependencies are unavailable"}
	}
	action, ok := e.Registry.Resolve(request.Action)
	if !ok {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "action_not_registered", ErrorMessage: "action is not registered"}
	}
	spec, err := BuildJobSpec(action, request.Input, JobIdentity{RunID: string(request.RunID), TaskKey: string(request.TaskKey), Attempt: request.Attempt})
	if err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "invalid_job_spec", ErrorMessage: err.Error()}
	}
	handle, err := e.Client.CreateJob(ctx, spec)
	if err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "job_create_failed", ErrorMessage: sanitizeError(err)}
	}
	defer func() { _ = e.Client.DeleteJob(context.Background(), handle) }()
	status, err := e.Client.WaitJob(ctx, handle)
	if err != nil {
		if ctx.Err() != nil {
			return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: ctx.Err().Error()}
		}
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "job_wait_failed", ErrorMessage: sanitizeError(err)}
	}
	if ctx.Err() != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled", ErrorMessage: ctx.Err().Error()}
	}
	if status.OOMKilled {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "oom_killed", ErrorMessage: "job exceeded memory limit"}
	}
	logs, err := e.Client.Logs(context.Background(), handle, action.OutputLimitBytes)
	if err != nil {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "job_logs_failed", ErrorMessage: sanitizeError(err)}
	}
	output := strings.TrimSpace(logs.Stdout)
	if action.OutputLimitBytes > 0 && int64(len(output)) > action.OutputLimitBytes {
		return workflow.ExecutionResponse{Kind: workflow.ResultPermanentFailure, ErrorCode: "output_limit_exceeded", ErrorMessage: "job output exceeded configured limit"}
	}
	if status.Succeeded {
		return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: output}
	}
	if status.Failed {
		return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "job_failed", ErrorMessage: status.Reason}
	}
	return workflow.ExecutionResponse{Kind: workflow.ResultTemporaryFailure, ErrorCode: "job_unknown_status", ErrorMessage: fmt.Sprintf("unknown job status for %s", handle)}
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 512 {
		return message[:512]
	}
	return message
}
