package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/kubeexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerconfig"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerruntime"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := workerconfig.Load(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger, err := observability.NewLogger(os.Stderr, cfg.LogFormat, cfg.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	closeTracer, err := newWorkerTracer(cfg, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer closeTracer(context.Background())
	client, err := workerclient.New(workerclient.Config{BaseURL: cfg.ServerURL, Logger: logger})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	mockExecutor := newMockExecutor(cfg.MockExecutionDelay)
	executors := map[workflow.ExecutorKind]workflow.Executor{workflow.ExecutorMock: mockExecutor}
	executorKinds := []workflow.ExecutorKind{workflow.ExecutorMock}
	if cfg.Runtime == "docker" {
		dockerRuntime, dockerErr := containerexec.NewDockerRuntimeClientFromEnv()
		if dockerErr != nil {
			fmt.Fprintln(os.Stderr, dockerErr)
			os.Exit(1)
		}
		executors[workflow.ExecutorContainer] = &containerexec.DockerExecutor{Runtime: dockerRuntime, Registry: defaultActionRegistry(cfg.ActionImage)}
		executorKinds = append(executorKinds, workflow.ExecutorContainer)
	}
	if cfg.Runtime == "kubernetes" {
		kubeRuntime, kubeErr := kubeexec.NewClientFromKubeconfig(os.Getenv("KUBECONFIG"))
		if kubeErr != nil {
			fmt.Fprintln(os.Stderr, kubeErr)
			os.Exit(1)
		}
		executors[workflow.ExecutorContainer] = &kubeexec.KubernetesExecutor{Client: kubeRuntime, Registry: defaultActionRegistry(cfg.ActionImage)}
		executorKinds = append(executorKinds, workflow.ExecutorContainer)
	}
	runtime, err := workerruntime.New(client, mockExecutor, workerruntime.Options{
		BootstrapToken: cfg.BootstrapToken,
		Registration: workerprotocol.RegisterRequest{
			DisplayName: cfg.DisplayName, ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: executorKinds, MaxConcurrency: cfg.MaxConcurrency,
		},
		PollMin: cfg.PollMin, PollMax: cfg.PollMax, HeartbeatInterval: cfg.HeartbeatInterval,
		RetryInterval: 250 * time.Millisecond, SafetyMargin: time.Second, ShutdownTimeout: cfg.ShutdownTimeout,
		Logger:    logger,
		Executors: executors,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runtime.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func defaultActionRegistry(image string) containerexec.Registry {
	specs, limits := containerexec.DefaultActionSpecs(image)
	registry, err := containerexec.NewRegistry(specs, limits)
	if err != nil {
		panic(fmt.Sprintf("build default action registry: %v", err))
	}
	return registry
}

func newWorkerTracer(cfg workerconfig.Config, output io.Writer) (func(context.Context) error, error) {
	_, closeTracer, err := observability.NewTracerProvider(observability.TracingConfig{
		Mode: cfg.TracingMode, ServiceName: cfg.TracingServiceName, Writer: output,
	})
	return closeTracer, err
}

// mockExecutor 刻意保持封闭：Action 只是标签，绝不能被解释为 Shell 命令、
// 文件路径、URL 或动态加载的程序。
type mockExecutor struct {
	delay time.Duration
}

func newMockExecutor(delay time.Duration) workflow.Executor {
	return mockExecutor{delay: delay}
}

func (e mockExecutor) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled"}
		case <-timer.C:
		}
	}
	select {
	case <-ctx.Done():
		return workflow.ExecutionResponse{Kind: workflow.ResultCanceled, ErrorCode: "canceled"}
	default:
	}
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "mock-completed:" + string(request.TaskKey)}
}
