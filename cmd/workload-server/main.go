package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/config"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/dispatch"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "migrate" && os.Args[2] == "up" {
		if err := migrate(context.Background(), os.Getenv("DATABASE_URL")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := serve(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context, databaseURL string) error {
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer repository.Close()
	return repository.Migrate(ctx)
}

func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	logger, err := newLogger(cfg, os.Stderr)
	if err != nil {
		return err
	}
	repository, err := postgres.NewWithOptions(ctx, cfg.DatabaseURL, postgres.Options{
		WorkerHeartbeatInterval: cfg.HeartbeatInterval,
		LeaseDuration:           cfg.LeaseDuration,
	})
	if err != nil {
		return err
	}
	defer repository.Close()
	if err := repository.CheckMigrations(ctx); err != nil {
		return err
	}
	definitions := app.NewWorkflowService(repository)
	coordinator := dispatch.NewCoordinator(repository, func(ctx context.Context) (dispatch.Lock, error) {
		return repository.AcquireAdvisoryLock(ctx)
	}, dispatch.CoordinatorOptions{
		ScanInterval: cfg.ReaperInterval, BatchSize: cfg.DispatchLimit,
	})
	if err := coordinator.Start(ctx); err != nil {
		return err
	}
	runs, err := app.NewRunService(repository, definitions, coordinator, nil)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coordinator.Close(closeCtx)
		return err
	}
	workers, err := workerapi.New(repository, workerapi.Options{
		BootstrapToken:    cfg.WorkerBootstrapToken,
		HeartbeatInterval: cfg.HeartbeatInterval,
		LeaseDuration:     cfg.LeaseDuration,
		Wake:              coordinator.Wake,
	})
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coordinator.Close(closeCtx)
		return err
	}
	logger.Info("workload server ready", "addr", cfg.HTTPAddr)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewHandler(httpapi.Dependencies{Workflows: definitions, Runs: runs, Workers: workers, ViewerToken: cfg.ViewerToken, OperatorToken: cfg.OperatorToken, Ready: coordinator.Ready, Logger: logger})}
	return superviseServer(ctx, server, coordinator, 5*time.Second)
}

type lifecycleHTTPServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

type lifecycleCoordinator interface {
	Errors() <-chan error
	Close(context.Context) error
}

// superviseServer 把正常信号、Coordinator 致命错误和 listener 错误收敛到同一关闭顺序。
func superviseServer(ctx context.Context, server lifecycleHTTPServer, coordinator lifecycleCoordinator, shutdownTimeout time.Duration) error {
	if shutdownTimeout <= 0 {
		shutdownTimeout = 5 * time.Second
	}
	httpErrors := make(chan error, 1)
	go func() { httpErrors <- server.ListenAndServe() }()

	var resultErr error
	select {
	case <-ctx.Done():
		// SIGINT 和 SIGTERM 属于正常关闭，不转换为进程错误。
	case err := <-coordinator.Errors():
		resultErr = err
	case err := <-httpErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			resultErr = err
		}
	}

	// 清理不能继承触发关闭的取消信号，否则 Shutdown 和锁释放会在开始前立即失败。
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(cleanupCtx)
	closeErr := coordinator.Close(cleanupCtx)
	cleanupErr := errors.Join(shutdownErr, closeErr)
	if resultErr == nil {
		return cleanupErr
	}
	if cleanupErr != nil {
		return errors.Join(resultErr, cleanupErr)
	}
	return resultErr
}

func newLogger(cfg config.Config, output io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", cfg.LogLevel)
	}
	options := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(output, options)), nil
	}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(output, options)), nil
	}
	return nil, fmt.Errorf("unsupported log format %q", cfg.LogFormat)
}

// successExecutor 是模块 2 的安全演示执行器：只等待固定时长，不解释或运行 Action。
type successExecutor struct {
	delay  time.Duration
	logger *slog.Logger
}

func newDefaultExecutor(delay time.Duration, logger *slog.Logger) workflow.Executor {
	if logger == nil {
		logger = slog.Default()
	}
	return successExecutor{delay: delay, logger: logger}
}

func (e successExecutor) Execute(ctx context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return workflow.ExecutionResponse{Kind: workflow.ResultCanceled}
		case <-timer.C:
		}
	}
	e.logger.Info("mock attempt completed",
		"workflow_id", request.DefinitionID,
		"run_id", request.RunID,
		"task_key", request.TaskKey,
		"attempt_number", request.Attempt,
	)
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "mock-completed:" + request.Action}
}
