package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const workerBenchmarkTaskCount = 1_000

const (
	workerBenchmarkPollMin = 25 * time.Millisecond
	workerBenchmarkPollMax = 500 * time.Millisecond
	workerBenchmarkTimeout = 2 * time.Minute
)

func BenchmarkWorkerFleetThousandTasks(b *testing.B) {
	for _, workerCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers-%d", workerCount), func(b *testing.B) {
			benchmarkWorkerFleet(b, workerCount)
		})
	}
}

func benchmarkWorkerFleet(b *testing.B, workerCount int) {
	b.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		b.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	isolatedURL := testpostgres.NewIsolatedDatabaseURL(b, databaseURL)
	parsedURL, err := url.Parse(isolatedURL)
	if err != nil {
		b.Fatal(err)
	}
	query := parsedURL.Query()
	query.Set("pool_max_conns", "64")
	parsedURL.RawQuery = query.Encode()
	repository, err := postgres.NewWithOptions(ctx, parsedURL.String(), postgres.Options{
		WorkerHeartbeatInterval: 50 * time.Millisecond,
		LeaseDuration:           500 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(repository.Close)
	if err := repository.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	metrics, tracer, logger, shutdownObservability := benchmarkObservability(b)
	defer shutdownObservability()
	runCount := benchmarkRunCount(b)
	taskCount := workerBenchmarkTaskCount / runCount
	definition := benchmarkParallelDefinition(workerCount, taskCount)
	compiled, err := workflow.Compile(definition)
	if err != nil {
		b.Fatal(err)
	}
	definition = compiled.Definition()
	if _, err := repository.CreateDefinition(ctx, definition, "benchmark", fmt.Sprintf("worker-fleet-definition-%d", workerCount), fmt.Sprintf("worker-fleet-hash-%d", workerCount)); err != nil {
		b.Fatal(err)
	}
	workers := make([]workerprotocol.Registration, workerCount)
	for index := range workers {
		workers[index], err = repository.RegisterWorker(ctx, workerprotocol.RegisterRequest{
			DisplayName: fmt.Sprintf("benchmark-worker-%d", index), ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	var claimLatencies, heartbeatLatencies, completionLatencies durationSamples
	var emptyClaims atomic.Int64
	b.ResetTimer()
	for iteration := range b.N {
		runIDs := make([]workflow.RunID, runCount)
		for runIndex := range runIDs {
			runID := workflow.RunID(fmt.Sprintf("benchmark-%d-%d-%d-%d", workerCount, runCount, time.Now().UnixNano(), iteration*runCount+runIndex))
			snapshot, err := workflow.NewRunSnapshotForVersion(runID, compiled, 1, time.Now().UTC())
			if err != nil {
				b.Fatal(err)
			}
			if err := repository.Create(ctx, snapshot); err != nil {
				b.Fatal(err)
			}
			runIDs[runIndex] = runID
		}
		completed := atomic.Int64{}
		totalTasks := int64(taskCount * runCount)
		runCtx, cancel := context.WithTimeout(ctx, workerBenchmarkTimeout)
		var workerGroup sync.WaitGroup
		workerErrors := make(chan error, workerCount)
		workerGroup.Add(workerCount)
		for _, registration := range workers {
			registration := registration
			go func() {
				defer workerGroup.Done()
				pollDelay := workerBenchmarkPollMin
				for runCtx.Err() == nil && completed.Load() < totalTasks {
					startedAt := time.Now()
					leases, claimErr := repository.Claim(runCtx, registration.Summary.WorkerID, 1)
					elapsed := time.Since(startedAt)
					benchmarkObserve(metrics, tracer, logger, "claim", elapsed, claimErr)
					if claimErr != nil {
						workerErrors <- claimErr
						return
					}
					if len(leases) == 0 {
						emptyClaims.Add(1)
						if !waitBenchmarkPoll(runCtx, pollDelay) {
							return
						}
						if pollDelay < workerBenchmarkPollMax {
							pollDelay *= 2
							if pollDelay > workerBenchmarkPollMax {
								pollDelay = workerBenchmarkPollMax
							}
						}
						continue
					}
					pollDelay = workerBenchmarkPollMin
					claimLatencies.add(elapsed)
					lease := leases[0]
					startedAt = time.Now()
					_, heartbeatErr := repository.Heartbeat(runCtx, registration.Summary.WorkerID, []workerprotocol.LeaseRef{{DispatchID: lease.DispatchID, LeaseToken: lease.LeaseToken}})
					heartbeatElapsed := time.Since(startedAt)
					heartbeatLatencies.add(heartbeatElapsed)
					benchmarkObserve(metrics, tracer, logger, "heartbeat", heartbeatElapsed, heartbeatErr)
					if heartbeatErr != nil {
						workerErrors <- heartbeatErr
						return
					}
					startedAt = time.Now()
					_, completeErr := repository.Complete(runCtx, registration.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
						LeaseToken: lease.LeaseToken,
						Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "benchmark"},
					})
					completionElapsed := time.Since(startedAt)
					completionLatencies.add(completionElapsed)
					benchmarkObserve(metrics, tracer, logger, "complete", completionElapsed, completeErr)
					if completeErr != nil {
						workerErrors <- completeErr
						return
					}
					completed.Add(1)
				}
			}()
		}
		var workerErr error
		for runCtx.Err() == nil && completed.Load() < totalTasks && workerErr == nil {
			select {
			case workerErr = <-workerErrors:
				continue
			default:
			}
			if _, err := repository.CreateDispatches(runCtx, workerCount); err != nil {
				cancel()
				workerGroup.Wait()
				b.Fatal(err)
			}
			time.Sleep(100 * time.Microsecond)
		}
		workerGroup.Wait()
		cancel()
		if workerErr == nil {
			select {
			case workerErr = <-workerErrors:
			default:
			}
		}
		if workerErr != nil {
			b.Fatal(workerErr)
		}
		if completed.Load() != totalTasks {
			b.Fatalf("completed tasks = %d, want %d", completed.Load(), totalTasks)
		}
		for _, runID := range runIDs {
			finished, err := repository.Load(ctx, runID)
			if err != nil {
				b.Fatal(err)
			}
			if finished.Run.Status != workflow.WorkflowSucceeded {
				b.Fatalf("Run status = %s, want succeeded", finished.Run.Status)
			}
		}
	}
	b.StopTimer()
	taskTotal := float64(workerBenchmarkTaskCount * b.N)
	b.ReportMetric(taskTotal/b.Elapsed().Seconds(), "tasks/s")
	b.ReportMetric(claimLatencies.percentile(0.50).Seconds()*1000, "claim-p50-ms")
	b.ReportMetric(claimLatencies.percentile(0.95).Seconds()*1000, "claim-p95-ms")
	b.ReportMetric(claimLatencies.percentile(0.99).Seconds()*1000, "claim-p99-ms")
	b.ReportMetric(heartbeatLatencies.percentile(0.50).Seconds()*1000, "heartbeat-p50-ms")
	b.ReportMetric(heartbeatLatencies.percentile(0.95).Seconds()*1000, "heartbeat-p95-ms")
	b.ReportMetric(heartbeatLatencies.percentile(0.99).Seconds()*1000, "heartbeat-p99-ms")
	b.ReportMetric(completionLatencies.percentile(0.50).Seconds()*1000, "complete-p50-ms")
	b.ReportMetric(completionLatencies.percentile(0.95).Seconds()*1000, "complete-p95-ms")
	b.ReportMetric(completionLatencies.percentile(0.99).Seconds()*1000, "complete-p99-ms")
	b.ReportMetric(float64(emptyClaims.Load())/float64(b.N), "empty-claims/op")
}

func benchmarkRunCount(b *testing.B) int {
	b.Helper()
	value := os.Getenv("WORKLOAD_BENCHMARK_RUNS")
	if value == "" {
		return 1
	}
	count, err := strconv.Atoi(value)
	if err != nil || (count != 1 && count != 8) {
		b.Fatalf("WORKLOAD_BENCHMARK_RUNS must be 1 or 8, got %q", value)
	}
	return count
}

func waitBenchmarkPoll(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func benchmarkObservability(b *testing.B) (*observability.Metrics, oteltrace.Tracer, *slog.Logger, func()) {
	b.Helper()
	mode := os.Getenv("WORKLOAD_OBSERVABILITY_MODE")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var metrics *observability.Metrics
	if mode == "logs_metrics" || mode == "logs_metrics_tracing" {
		metrics = observability.NewMetrics(nil)
	}
	closeTracer := func() {}
	var tracer oteltrace.Tracer
	if mode == "logs_metrics_tracing" {
		provider, closeProvider, err := observability.NewTracerProvider(observability.TracingConfig{Mode: "stdout", ServiceName: "module5-benchmark", Writer: io.Discard})
		if err != nil {
			b.Fatal(err)
		}
		tracer = provider.Tracer("module5-benchmark")
		closeTracer = func() { _ = closeProvider(context.Background()) }
	}
	return metrics, tracer, logger, closeTracer
}

func benchmarkObserve(metrics *observability.Metrics, tracer oteltrace.Tracer, logger *slog.Logger, operation string, duration time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if metrics != nil {
		metrics.ObserveOperation(operation, outcome, "", duration)
	}
	if os.Getenv("WORKLOAD_OBSERVABILITY_MODE") != "off" {
		logger.Info("benchmark operation", "operation", operation, "duration_ms", duration.Milliseconds(), "outcome", outcome)
	}
	if tracer != nil {
		_, span := observability.StartSpan(context.Background(), tracer, "benchmark."+operation)
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}
}

func benchmarkParallelDefinition(workerCount, taskCount int) workflow.WorkflowDefinition {
	tasks := make([]workflow.TaskDefinition, taskCount)
	for index := range tasks {
		tasks[index] = workflow.TaskDefinition{
			Key: workflow.TaskKey(fmt.Sprintf("task-%04d", index)), Action: "mock", TimeoutMillis: 30_000,
		}
	}
	return workflow.WorkflowDefinition{
		ID: fmt.Sprintf("worker-fleet-%d", workerCount), Concurrency: workerCount, Tasks: tasks,
	}
}

type durationSamples struct {
	mu     sync.Mutex
	values []time.Duration
}

func (s *durationSamples) add(value time.Duration) {
	s.mu.Lock()
	s.values = append(s.values, value)
	s.mu.Unlock()
}

func (s *durationSamples) percentile(fraction float64) time.Duration {
	s.mu.Lock()
	values := append([]time.Duration(nil), s.values...)
	s.mu.Unlock()
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	index := int(float64(len(values)-1) * fraction)
	return values[index]
}
