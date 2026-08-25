package e2e

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
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
	definition := benchmarkParallelDefinition(workerCount)
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
		runID := workflow.RunID(fmt.Sprintf("benchmark-%d-%d-%d", workerCount, time.Now().UnixNano(), iteration))
		snapshot, err := workflow.NewRunSnapshotForVersion(runID, compiled, 1, time.Now().UTC())
		if err != nil {
			b.Fatal(err)
		}
		if err := repository.Create(ctx, snapshot); err != nil {
			b.Fatal(err)
		}
		completed := atomic.Int64{}
		runCtx, cancel := context.WithTimeout(ctx, workerBenchmarkTimeout)
		var workerGroup sync.WaitGroup
		workerErrors := make(chan error, workerCount)
		workerGroup.Add(workerCount)
		for _, registration := range workers {
			registration := registration
			go func() {
				defer workerGroup.Done()
				pollDelay := workerBenchmarkPollMin
				for runCtx.Err() == nil && completed.Load() < workerBenchmarkTaskCount {
					startedAt := time.Now()
					leases, claimErr := repository.Claim(runCtx, registration.Summary.WorkerID, 1)
					elapsed := time.Since(startedAt)
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
					heartbeatLatencies.add(time.Since(startedAt))
					if heartbeatErr != nil {
						workerErrors <- heartbeatErr
						return
					}
					startedAt = time.Now()
					_, completeErr := repository.Complete(runCtx, registration.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
						LeaseToken: lease.LeaseToken,
						Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "benchmark"},
					})
					completionLatencies.add(time.Since(startedAt))
					if completeErr != nil {
						workerErrors <- completeErr
						return
					}
					completed.Add(1)
				}
			}()
		}
		var workerErr error
		for runCtx.Err() == nil && completed.Load() < workerBenchmarkTaskCount && workerErr == nil {
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
		if completed.Load() != workerBenchmarkTaskCount {
			b.Fatalf("completed tasks = %d, want %d", completed.Load(), workerBenchmarkTaskCount)
		}
		finished, err := repository.Load(ctx, runID)
		if err != nil {
			b.Fatal(err)
		}
		if finished.Run.Status != workflow.WorkflowSucceeded {
			b.Fatalf("Run status = %s, want succeeded", finished.Run.Status)
		}
	}
	b.StopTimer()
	taskTotal := float64(workerBenchmarkTaskCount * b.N)
	b.ReportMetric(taskTotal/b.Elapsed().Seconds(), "tasks/s")
	b.ReportMetric(claimLatencies.percentile(0.95).Seconds()*1000, "claim-p95-ms")
	b.ReportMetric(claimLatencies.percentile(0.99).Seconds()*1000, "claim-p99-ms")
	b.ReportMetric(heartbeatLatencies.percentile(0.95).Seconds()*1000, "heartbeat-p95-ms")
	b.ReportMetric(heartbeatLatencies.percentile(0.99).Seconds()*1000, "heartbeat-p99-ms")
	b.ReportMetric(completionLatencies.percentile(0.95).Seconds()*1000, "complete-p95-ms")
	b.ReportMetric(completionLatencies.percentile(0.99).Seconds()*1000, "complete-p99-ms")
	b.ReportMetric(float64(emptyClaims.Load())/float64(b.N), "empty-claims/op")
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

func benchmarkParallelDefinition(workerCount int) workflow.WorkflowDefinition {
	tasks := make([]workflow.TaskDefinition, workerBenchmarkTaskCount)
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
