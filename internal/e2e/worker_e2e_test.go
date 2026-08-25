package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/dispatch"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestWorkerHTTPExecutesParallelDAGWithTwoWorkers(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.NewWithOptions(ctx, databaseURL, postgres.Options{
		WorkerHeartbeatInterval: 50 * time.Millisecond,
		LeaseDuration:           200 * time.Millisecond,
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
	workerServer, err := newWorkerHTTPServer(repository, definitions, runs, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	defer workerServer.Close()

	client, err := workerclient.New(workerclient.Config{BaseURL: workerServer.URL, HTTPClient: workerServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	workers := make([]workerprotocol.RegisterResponse, 0, 2)
	for _, name := range []string{"worker-a", "worker-b"} {
		registration, err := client.Register(ctx, "bootstrap-e2e", workerprotocol.RegisterRequest{
			DisplayName: name, ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, registration)
	}

	workflowID := "worker-e2e-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 2, Tasks: []workflow.TaskDefinition{
		{Key: "first", Action: "mock-first", TimeoutMillis: 5000},
		{Key: "second", Action: "mock-second", TimeoutMillis: 5000},
	}}
	if _, err := definitions.Create(ctx, "operator", "worker-e2e-definition", definition); err != nil {
		t.Fatal(err)
	}
	started, err := runs.Start(ctx, "operator", workflowID, 1, "worker-e2e-run")
	if err != nil {
		t.Fatal(err)
	}

	completed := make(map[string]bool)
	deadline := time.Now().Add(5 * time.Second)
	for len(completed) < 2 && time.Now().Before(deadline) {
		for _, worker := range workers {
			claim, err := client.Claim(ctx, worker.WorkerID, worker.SessionToken, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, lease := range claim.Leases {
				if completed[string(lease.TaskKey)] {
					continue
				}
				if _, err := client.Complete(ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, workerprotocol.CompleteRequest{
					LeaseToken: lease.LeaseToken,
					Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "done:" + string(lease.TaskKey)},
				}); err != nil {
					t.Fatal(err)
				}
				completed[string(lease.TaskKey)] = true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(completed) != 2 {
		t.Fatalf("completed task keys = %v", completed)
	}
	waitForRunStatus(t, workerServer.URL, started.RunID, workflow.WorkflowSucceeded)

	request, err := http.NewRequest(http.MethodGet, workerServer.URL+"/api/v1/workers", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer viewer")
	response, err := workerServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Worker query status = %d", response.StatusCode)
	}
	var page struct {
		Items []workerprotocol.WorkerSummary `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("Worker summaries = %+v", page.Items)
	}
	for _, summary := range page.Items {
		if summary.WorkerID == "" || summary.ActiveLeases != 0 {
			t.Fatalf("Worker summary = %+v", summary)
		}
	}
}

func newWorkerHTTPServer(repository *postgres.Repository, definitions app.WorkflowService, runs app.RunService, coordinator *dispatch.DispatchCoordinator) (*httptest.Server, error) {
	workers, err := workerapi.New(repository, workerapi.Options{BootstrapToken: "bootstrap-e2e", HeartbeatInterval: 50 * time.Millisecond, LeaseDuration: 200 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	return httptest.NewServer(httpapi.NewHandler(httpapi.Dependencies{
		Workflows: definitions, Runs: runs, Workers: workers,
		ViewerToken: "viewer", OperatorToken: "operator", Ready: coordinator.Ready,
	})), nil
}
