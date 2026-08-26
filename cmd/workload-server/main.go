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

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/agentapp"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/alerting"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/config"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/dispatch"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerapi"
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
	metrics := observability.NewMetrics(nil)
	drafts, err := agentapp.NewService(cfg.ModelAdapter, os.Getenv)
	if err != nil {
		return err
	}
	tracerProvider, closeTracer, err := observability.NewTracerProvider(observability.TracingConfig{Mode: cfg.TracingMode, ServiceName: cfg.TracingServiceName, Writer: os.Stderr})
	if err != nil {
		return err
	}
	defer closeTracer(context.Background())
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
		ScanInterval: cfg.ReaperInterval, BatchSize: cfg.DispatchLimit, Metrics: metrics,
		ObserveMetrics: func(ctx context.Context) {
			snapshot, err := repository.AlertSnapshot(ctx)
			if err != nil {
				logger.Warn("observe dispatch state failed", "operation", "metrics_snapshot", "error", err)
				return
			}
			metrics.SetQueueDepth("pending", snapshot.QueueDepth)
			metrics.SetWorkers("active", snapshot.OnlineWorkers)
			metrics.SetActiveLeases(snapshot.ActiveLeases)
			total, inUse, idle, wait := repository.PoolObservation()
			metrics.SetDBPool("total", total)
			metrics.SetDBPool("in_use", inUse)
			metrics.SetDBPool("idle", idle)
			metrics.ObserveDBPoolWait(wait)
		},
	})
	if err := coordinator.Start(ctx); err != nil {
		return err
	}
	if cfg.AlertWebhookURL != "" {
		sink, err := alerting.NewWebhookSink(http.DefaultClient, cfg.AlertWebhookURL, cfg.AlertWebhookTimeout, 2)
		if err != nil {
			return err
		}
		provider := func(ctx context.Context) (alerting.Snapshot, error) {
			snapshot, err := repository.AlertSnapshot(ctx)
			if err != nil {
				return alerting.Snapshot{}, err
			}
			total, inUse, idle, wait := repository.PoolObservation()
			metrics.SetQueueDepth("pending", snapshot.QueueDepth)
			metrics.SetWorkers("active", snapshot.OnlineWorkers)
			metrics.SetActiveLeases(snapshot.ActiveLeases)
			metrics.SetDBPool("total", total)
			metrics.SetDBPool("in_use", inUse)
			metrics.SetDBPool("idle", idle)
			metrics.ObserveDBPoolWait(wait)
			snapshot.CompleteTotal, snapshot.CompleteErrors, snapshot.LeaseReclaimErrors = metrics.AlertCounters(snapshot.Now, 30*time.Second)
			return snapshot, nil
		}
		runner := alerting.NewRunner(alerting.NewEngine(alerting.DefaultRules()), provider, sink, time.Second, logger, metrics)
		go runner.Run(ctx)
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
		Metrics:           metrics,
		Tracer:            tracerProvider.Tracer("workload-worker-api"),
		Logger:            logger,
	})
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = coordinator.Close(closeCtx)
		return err
	}
	logger.Info("workload server ready", "addr", cfg.HTTPAddr)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewHandler(httpapi.Dependencies{Workflows: definitions, Runs: runs, Drafts: drafts, Workers: workers, ViewerToken: cfg.ViewerToken, OperatorToken: cfg.OperatorToken, Ready: coordinator.Ready, Logger: logger, Metrics: metrics, Tracer: tracerProvider.Tracer("workload-server")})}
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
	return observability.NewLogger(output, cfg.LogFormat, cfg.LogLevel)
}
