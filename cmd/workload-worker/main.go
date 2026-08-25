package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	client, err := workerclient.New(workerclient.Config{BaseURL: cfg.ServerURL, Logger: slog.Default()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	runtime, err := workerruntime.New(client, newMockExecutor(cfg.MockExecutionDelay), workerruntime.Options{
		BootstrapToken: cfg.BootstrapToken,
		Registration: workerprotocol.RegisterRequest{
			DisplayName: cfg.DisplayName, ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: cfg.MaxConcurrency,
		},
		PollMin: cfg.PollMin, PollMax: cfg.PollMax, HeartbeatInterval: cfg.HeartbeatInterval,
		RetryInterval: 250 * time.Millisecond, SafetyMargin: time.Second, ShutdownTimeout: cfg.ShutdownTimeout,
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
