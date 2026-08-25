package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/dispatch"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestWorkerProcessCrashIsRecoveredBySecondProcess(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.NewWithOptions(ctx, databaseURL, postgres.Options{
		WorkerHeartbeatInterval: 50 * time.Millisecond,
		// 独立 Worker 使用 1 秒本地安全余量；测试租约必须明显大于该值，
		// 否则 Worker 会在领取后立即取消执行，无法验证真正的进程崩溃接管。
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	definitions := app.NewWorkflowService(repository)
	coordinator := dispatch.NewCoordinator(repository, func(ctx context.Context) (dispatch.Lock, error) {
		return repository.AcquireAdvisoryLock(ctx)
	}, dispatch.CoordinatorOptions{ScanInterval: 5 * time.Millisecond, BatchSize: 10})
	if err := coordinator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	runs, err := app.NewRunService(repository, definitions, coordinator, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newWorkerHTTPServer(repository, definitions, runs, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	workflowID := "worker-process-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 1, Tasks: []workflow.TaskDefinition{{
		Key: "long", Action: "mock-long", TimeoutMillis: 60_000, Retry: workflow.RetryPolicy{MaxAttempts: 2},
	}}}
	if _, err := definitions.Create(ctx, "operator", "worker-process-definition", definition); err != nil {
		t.Fatal(err)
	}
	started, err := runs.Start(ctx, "operator", workflowID, 1, "worker-process-run")
	if err != nil {
		t.Fatal(err)
	}

	workerBinary := filepath.Join(t.TempDir(), "workload-worker")
	build := exec.Command("go", "build", "-o", workerBinary, "./cmd/workload-worker")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Worker binary: %v\n%s", err, output)
	}
	first := startWorkerProcess(t, workerBinary, server.URL, "worker-process-a", "5s")
	defer stopWorkerProcess(first)
	waitForWorkerLease(t, server.URL, "worker-process-a", 5*time.Second)
	crashedAt := time.Now()
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()

	second := startWorkerProcess(t, workerBinary, server.URL, "worker-process-b", "0s")
	defer stopWorkerProcess(second)
	waitForRunStatusWithTimeout(t, server.URL, started.RunID, workflow.WorkflowSucceeded, 30*time.Second)
	t.Logf("Worker crash recovery completed in %s", time.Since(crashedAt))
	detail := request(t, server.URL+"/api/v1/runs/"+string(started.RunID)+"/tasks/long", http.MethodGet, "viewer", "", nil)
	var task app.TaskDetail
	if err := json.Unmarshal(detail.body, &task); err != nil {
		t.Fatal(err)
	}
	if len(task.Task.Attempts) != 2 || task.Task.Attempts[0].Status != workflow.AttemptInterrupted || task.Task.Attempts[1].Status != workflow.AttemptSucceeded {
		t.Fatalf("task attempts = %+v", task.Task.Attempts)
	}
}

func waitForRunStatusWithTimeout(t *testing.T, serverURL string, runID workflow.RunID, want workflow.WorkflowStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response := request(t, serverURL+"/api/v1/runs/"+string(runID), http.MethodGet, "viewer", "", nil)
		if response.code == http.StatusOK {
			var summary app.RunSummary
			if json.Unmarshal(response.body, &summary) == nil && summary.Status == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Run %s did not reach %s before timeout", runID, want)
}

func startWorkerProcess(t *testing.T, binary, serverURL, name, delay string) *exec.Cmd {
	t.Helper()
	command := exec.Command(binary)
	command.Env = append(os.Environ(),
		"WORKLOAD_SERVER_URL="+serverURL,
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN=bootstrap-e2e",
		"WORKLOAD_WORKER_NAME="+name,
		"WORKLOAD_WORKER_CONCURRENCY=1",
		"WORKLOAD_WORKER_POLL_MIN=10ms",
		"WORKLOAD_WORKER_POLL_MAX=50ms",
		"WORKLOAD_WORKER_HEARTBEAT=50ms",
		"WORKLOAD_WORKER_SHUTDOWN_TIMEOUT=200ms",
		"WORKLOAD_MOCK_EXECUTION_DELAY="+delay,
	)
	if err := command.Start(); err != nil {
		t.Fatalf("start Worker %s: %v", name, err)
	}
	return command
}

func stopWorkerProcess(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Signal(os.Interrupt)
	_, _ = command.Process.Wait()
}

func waitForWorkerLease(t *testing.T, serverURL, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response := request(t, serverURL+"/api/v1/workers", http.MethodGet, "viewer", "", nil)
		if response.code == http.StatusOK {
			var page struct {
				Items []struct {
					DisplayName  string `json:"display_name"`
					ActiveLeases int    `json:"active_leases"`
				} `json:"items"`
			}
			if json.Unmarshal(response.body, &page) == nil {
				for _, item := range page.Items {
					if item.DisplayName == name && item.ActiveLeases == 1 {
						return
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Worker did not acquire a lease before timeout")
}
