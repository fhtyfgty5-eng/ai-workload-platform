package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/dispatch"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type workerRecoveryEnvironment struct {
	ctx         context.Context
	databaseURL string
	repository  *postgres.Repository
	definitions app.WorkflowService
	runs        app.RunService
	coordinator *dispatch.DispatchCoordinator
	server      *httpTestServer
}

// newWorkerRecoveryEnvironment 独占一个隔离数据库和一个协调器，
// 使恢复测试能够只替换 HTTP 进程而保留持久化状态。
func newWorkerRecoveryEnvironment(t *testing.T) *workerRecoveryEnvironment {
	return newWorkerRecoveryEnvironmentWithScanInterval(t, 5*time.Millisecond)
}

func newWorkerRecoveryEnvironmentWithScanInterval(t *testing.T, scanInterval time.Duration) *workerRecoveryEnvironment {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	repository, err := postgres.NewWithOptions(ctx, databaseURL, postgres.Options{
		WorkerHeartbeatInterval: 50 * time.Millisecond,
		LeaseDuration:           200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Migrate(ctx); err != nil {
		repository.Close()
		t.Fatal(err)
	}
	definitions := app.NewWorkflowService(repository)
	coordinator := dispatch.NewCoordinator(repository, func(ctx context.Context) (dispatch.Lock, error) {
		return repository.AcquireAdvisoryLock(ctx)
	}, dispatch.CoordinatorOptions{ScanInterval: scanInterval, BatchSize: 20})
	if err := coordinator.Start(ctx); err != nil {
		repository.Close()
		t.Fatal(err)
	}
	runs, err := app.NewRunService(repository, definitions, coordinator, nil)
	if err != nil {
		coordinator.Close(context.Background())
		repository.Close()
		t.Fatal(err)
	}
	server, err := startWorkerHTTPTestServer(repository, definitions, runs, coordinator)
	if err != nil {
		coordinator.Close(context.Background())
		repository.Close()
		t.Fatal(err)
	}
	env := &workerRecoveryEnvironment{ctx: ctx, databaseURL: databaseURL, repository: repository, definitions: definitions, runs: runs, coordinator: coordinator, server: server}
	t.Cleanup(func() {
		if env.server != nil {
			env.server.Close()
		}
		if env.coordinator != nil {
			_ = env.coordinator.Close(context.Background())
		}
		if env.repository != nil {
			env.repository.Close()
		}
	})
	return env
}

// httpTestServer 把客户端和 URL 保存在一起，避免各测试场景了解仅供测试的服务构造细节。
type httpTestServer struct {
	URL    string
	Client *http.Client
	close  func()
}

func (s *httpTestServer) Close() { s.close() }

func startWorkerHTTPTestServer(repository *postgres.Repository, definitions app.WorkflowService, runs app.RunService, coordinator *dispatch.DispatchCoordinator) (*httpTestServer, error) {
	server, err := newWorkerHTTPServer(repository, definitions, runs, coordinator)
	if err != nil {
		return nil, err
	}
	return &httpTestServer{URL: server.URL, Client: server.Client(), close: server.Close}, nil
}

func registerRecoveryWorker(t *testing.T, client *workerclient.Client, name string) workerprotocol.RegisterResponse {
	t.Helper()
	registration, err := client.Register(context.Background(), "bootstrap-e2e", workerprotocol.RegisterRequest{
		DisplayName: name, ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registration
}

func createRecoveryRun(t *testing.T, env *workerRecoveryEnvironment, id string, task workflow.TaskDefinition) workflow.RunID {
	t.Helper()
	definition := workflow.WorkflowDefinition{ID: id, Concurrency: 1, Tasks: []workflow.TaskDefinition{task}}
	if _, err := env.definitions.Create(env.ctx, "operator", id+"-definition", definition); err != nil {
		t.Fatal(err)
	}
	started, err := env.runs.Start(env.ctx, "operator", id, 1, id+"-run")
	if err != nil {
		t.Fatal(err)
	}
	return started.RunID
}

func TestWorkerHTTPRejectsLateResultAfterLeaseReassignment(t *testing.T) {
	env := newWorkerRecoveryEnvironmentWithScanInterval(t, time.Hour)
	client, err := workerclient.New(workerclient.Config{BaseURL: env.server.URL, HTTPClient: env.server.Client})
	if err != nil {
		t.Fatal(err)
	}
	first := registerRecoveryWorker(t, client, "late-result-first")
	second := registerRecoveryWorker(t, client, "late-result-second")
	runID := createRecoveryRun(t, env, "late-result-"+fmt.Sprint(time.Now().UnixNano()), workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 10, Retry: workflow.RetryPolicy{MaxAttempts: 2}})
	deadline := time.Now().Add(2 * time.Second)
	var lease workerprotocol.Lease
	for time.Now().Before(deadline) {
		claim, claimErr := client.Claim(env.ctx, first.WorkerID, first.SessionToken, 1)
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if len(claim.Leases) == 1 {
			lease = claim.Leases[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lease.DispatchID == "" {
		t.Fatal("first Worker did not receive a lease")
	}
	time.Sleep(20 * time.Millisecond)
	if reaped, err := env.repository.ReapExpired(env.ctx, 20); err != nil || reaped != 1 {
		t.Fatalf("ReapExpired() = %d, %v", reaped, err)
	}
	if created, err := env.repository.CreateDispatches(env.ctx, 20); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() = %d, %v", created, err)
	}
	claim, err := client.Claim(env.ctx, second.WorkerID, second.SessionToken, 1)
	if err != nil || len(claim.Leases) != 1 {
		t.Fatalf("second Worker Claim() = %+v, %v", claim.Leases, err)
	}
	newLease := claim.Leases[0]
	if newLease.Attempt != 2 {
		t.Fatalf("reassigned Attempt = %d, want 2", newLease.Attempt)
	}
	_, err = client.Complete(env.ctx, first.WorkerID, lease.DispatchID, first.SessionToken, workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "late"},
	})
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "lease_lost" {
		t.Fatalf("late Complete() error = %v, want lease_lost", err)
	}
	if _, err := client.Complete(env.ctx, second.WorkerID, newLease.DispatchID, second.SessionToken, workerprotocol.CompleteRequest{
		LeaseToken: newLease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "current"},
	}); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, env.server.URL, runID, workflow.WorkflowSucceeded)
}

func TestWorkerSessionSurvivesHTTPControlPlaneRestart(t *testing.T) {
	env := newWorkerRecoveryEnvironment(t)
	client, err := workerclient.New(workerclient.Config{BaseURL: env.server.URL, HTTPClient: env.server.Client})
	if err != nil {
		t.Fatal(err)
	}
	worker := registerRecoveryWorker(t, client, "restart-session")
	runID := createRecoveryRun(t, env, "restart-session-"+fmt.Sprint(time.Now().UnixNano()), workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 5000})
	var lease workerprotocol.Lease
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		claim, claimErr := client.Claim(env.ctx, worker.WorkerID, worker.SessionToken, 1)
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if len(claim.Leases) == 1 {
			lease = claim.Leases[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lease.DispatchID == "" {
		t.Fatal("Worker did not receive a lease")
	}
	env.server.Close()
	if err := env.coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	env.repository.Close()
	repository, err := postgres.NewWithOptions(env.ctx, env.databaseURL, postgres.Options{
		WorkerHeartbeatInterval: 50 * time.Millisecond,
		LeaseDuration:           200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.CheckMigrations(env.ctx); err != nil {
		repository.Close()
		t.Fatal(err)
	}
	definitions := app.NewWorkflowService(repository)
	coordinator := dispatch.NewCoordinator(repository, func(ctx context.Context) (dispatch.Lock, error) {
		return repository.AcquireAdvisoryLock(ctx)
	}, dispatch.CoordinatorOptions{ScanInterval: 5 * time.Millisecond, BatchSize: 20})
	if err := coordinator.Start(env.ctx); err != nil {
		repository.Close()
		t.Fatal(err)
	}
	runs, err := app.NewRunService(repository, definitions, coordinator, nil)
	if err != nil {
		_ = coordinator.Close(context.Background())
		repository.Close()
		t.Fatal(err)
	}
	restarted, err := startWorkerHTTPTestServer(repository, definitions, runs, coordinator)
	if err != nil {
		_ = coordinator.Close(context.Background())
		repository.Close()
		t.Fatal(err)
	}
	env.repository = repository
	env.definitions = definitions
	env.runs = runs
	env.coordinator = coordinator
	env.server = restarted
	client, err = workerclient.New(workerclient.Config{BaseURL: restarted.URL, HTTPClient: restarted.Client})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := client.Heartbeat(env.ctx, worker.WorkerID, worker.SessionToken, workerprotocol.HeartbeatRequest{Leases: []workerprotocol.LeaseRef{{DispatchID: lease.DispatchID, LeaseToken: lease.LeaseToken}}})
	if err != nil || len(heartbeat.Leases) != 1 || heartbeat.Leases[0].Status != workerprotocol.LeaseRenewed {
		t.Fatalf("heartbeat after HTTP restart = %+v, %v", heartbeat, err)
	}
	if _, err := client.Complete(env.ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "after-restart"},
	}); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, restarted.URL, runID, workflow.WorkflowSucceeded)
}

func TestRunCreatedBeforeWorkerRegistrationStartsAfterWorkerAppears(t *testing.T) {
	env := newWorkerRecoveryEnvironment(t)
	workflowID := "late-worker-" + fmt.Sprint(time.Now().UnixNano())
	runID := createRecoveryRun(t, env, workflowID, workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 5000})
	time.Sleep(30 * time.Millisecond)
	detail, err := env.runs.GetTask(env.ctx, runID, "task")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Task.Status != workflow.TaskReady || len(detail.Task.Attempts) != 0 {
		t.Fatalf("task before Worker registration = %+v, want ready without Attempts", detail.Task)
	}
	client, err := workerclient.New(workerclient.Config{BaseURL: env.server.URL, HTTPClient: env.server.Client})
	if err != nil {
		t.Fatal(err)
	}
	worker := registerRecoveryWorker(t, client, "late-worker")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		claim, claimErr := client.Claim(env.ctx, worker.WorkerID, worker.SessionToken, 1)
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if len(claim.Leases) == 1 {
			lease := claim.Leases[0]
			if _, err := client.Complete(env.ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, workerprotocol.CompleteRequest{
				LeaseToken: lease.LeaseToken,
				Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "worker-appeared"},
			}); err != nil {
				t.Fatal(err)
			}
			waitForRunStatus(t, env.server.URL, runID, workflow.WorkflowSucceeded)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Run did not become claimable after Worker registration")
}

func TestDistributedRunCancellationRevokesWorkerLease(t *testing.T) {
	env := newWorkerRecoveryEnvironment(t)
	client, err := workerclient.New(workerclient.Config{BaseURL: env.server.URL, HTTPClient: env.server.Client})
	if err != nil {
		t.Fatal(err)
	}
	worker := registerRecoveryWorker(t, client, "cancel-distributed")
	runID := createRecoveryRun(t, env, "cancel-distributed-"+fmt.Sprint(time.Now().UnixNano()), workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 5000})
	var lease workerprotocol.Lease
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		claim, claimErr := client.Claim(env.ctx, worker.WorkerID, worker.SessionToken, 1)
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if len(claim.Leases) == 1 {
			lease = claim.Leases[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lease.DispatchID == "" {
		t.Fatal("Worker did not receive a lease")
	}
	response := request(t, env.server.URL+"/api/v1/runs/"+string(runID)+"/cancel", http.MethodPost, "operator", "", []byte(`{}`))
	if response.code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", response.code, response.body)
	}
	waitForRunStatus(t, env.server.URL, runID, workflow.WorkflowCanceled)
	heartbeat, err := client.Heartbeat(env.ctx, worker.WorkerID, worker.SessionToken, workerprotocol.HeartbeatRequest{Leases: []workerprotocol.LeaseRef{{DispatchID: lease.DispatchID, LeaseToken: lease.LeaseToken}}})
	if err != nil || len(heartbeat.Leases) != 1 || heartbeat.Leases[0].Status != workerprotocol.LeaseRevoked {
		t.Fatalf("heartbeat after distributed cancellation = %+v, %v", heartbeat, err)
	}
	_, err = client.Complete(env.ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, workerprotocol.CompleteRequest{LeaseToken: lease.LeaseToken, Result: workflow.ExecutionResponse{Kind: workflow.ResultSuccess}})
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "lease_lost" {
		t.Fatalf("late Complete() after cancellation = %v, want lease_lost", err)
	}
}
