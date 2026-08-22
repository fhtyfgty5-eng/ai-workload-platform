package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	runtimecoord "github.com/fhtyfgty5-eng/ai-workload-platform/internal/runtime"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/mockexec"
)

func TestControlPlaneCreatesVersionAndIdempotentRun(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	definitionService := app.NewWorkflowService(repository)
	workflowID := "e2e-demo-" + time.Now().UTC().Format("150405")
	runID := workflow.RunID("e2e-run-" + time.Now().UTC().Format("150405") + "-" + fmt.Sprint(time.Now().UnixNano()))
	workflowKey := "workflow-" + fmt.Sprint(time.Now().UnixNano())
	runKey := "run-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "noop", TimeoutMillis: 1000}}}
	engine, err := workflow.NewEngine(repository, mockexec.New(workflow.RealClock{}, map[mockexec.ScriptKey][]mockexec.Step{{DefinitionID: workflowID, TaskKey: "one"}: {{Kind: workflow.ResultSuccess}}}), workflow.EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := startTestCoordinator(t, ctx, repository, engine)
	defer closeTestCoordinator(t, coordinator)
	runs, err := app.NewRunService(repository, definitionService, engine, coordinator.Enqueue, func() (workflow.RunID, error) { return runID, nil })
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewHandler(httpapi.Dependencies{Workflows: definitionService, Runs: runs, ViewerToken: "viewer", OperatorToken: "operator", Ready: coordinator.Ready}))
	defer server.Close()

	createBody, _ := json.Marshal(definition)
	create := request(t, server.URL+"/api/v1/workflows", http.MethodPost, "operator", workflowKey, createBody)
	if create.code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", create.code, create.body)
	}
	replay := request(t, server.URL+"/api/v1/workflows", http.MethodPost, "operator", workflowKey, createBody)
	if replay.code != http.StatusCreated {
		t.Fatalf("workflow replay status = %d", replay.code)
	}
	if loaded, err := definitionService.Get(ctx, workflowID, 1); err != nil {
		t.Fatalf("direct Get error: %v", err)
	} else if loaded.ID != workflowID {
		t.Fatalf("direct Get definition=%s, want %s", loaded.ID, workflowID)
	}
	start := request(t, server.URL+"/api/v1/workflows/"+workflowID+"/versions/1/runs", http.MethodPost, "operator", runKey, []byte(`{}`))
	if start.code != http.StatusAccepted {
		t.Fatalf("start status = %d, body=%s", start.code, start.body)
	}
	startReplay := request(t, server.URL+"/api/v1/workflows/"+workflowID+"/versions/1/runs", http.MethodPost, "operator", runKey, []byte(`{}`))
	if startReplay.code != http.StatusAccepted {
		t.Fatalf("run replay status = %d, body=%s", startReplay.code, startReplay.body)
	}
	var response struct {
		RunID workflow.RunID `json:"run_id"`
	}
	if err := json.Unmarshal(startReplay.body, &response); err != nil || response.RunID != runID {
		t.Fatalf("run response = %s, err=%v", startReplay.body, err)
	}
	waitForRunStatus(t, server.URL, response.RunID, workflow.WorkflowSucceeded)

	v2 := definition
	v2.Tasks[0].Action = "noop-v2"
	v2Body, _ := json.Marshal(v2)
	v2Key := "workflow-v2-" + fmt.Sprint(time.Now().UnixNano())
	createV2 := request(t, server.URL+"/api/v1/workflows/"+workflowID+"/versions", http.MethodPost, "operator", v2Key, v2Body)
	if createV2.code != http.StatusCreated {
		t.Fatalf("create v2 status = %d, body=%s", createV2.code, createV2.body)
	}
	var v2Ref app.DefinitionRef
	if err := json.Unmarshal(createV2.body, &v2Ref); err != nil || v2Ref.Version != 2 {
		t.Fatalf("v2 response = %s, err=%v", createV2.body, err)
	}

	runResult := request(t, server.URL+"/api/v1/runs/"+string(runID), http.MethodGet, "viewer", "", nil)
	var summary app.RunSummary
	if err := json.Unmarshal(runResult.body, &summary); err != nil || summary.DefinitionVersion != 1 || summary.Status != workflow.WorkflowSucceeded {
		t.Fatalf("run summary = %s, err=%v", runResult.body, err)
	}
	tasks := request(t, server.URL+"/api/v1/runs/"+string(runID)+"/tasks", http.MethodGet, "viewer", "", nil)
	var taskPage app.TaskPage
	if err := json.Unmarshal(tasks.body, &taskPage); err != nil || len(taskPage.Items) != 1 || taskPage.Items[0].Status != workflow.TaskSucceeded {
		t.Fatalf("task page = %s, err=%v", tasks.body, err)
	}
	task := request(t, server.URL+"/api/v1/runs/"+string(runID)+"/tasks/one", http.MethodGet, "viewer", "", nil)
	var taskDetail app.TaskDetail
	if err := json.Unmarshal(task.body, &taskDetail); err != nil || len(taskDetail.Task.Attempts) != 1 || taskDetail.Task.Attempts[0].Status != workflow.AttemptSucceeded {
		t.Fatalf("task detail = %s, err=%v", task.body, err)
	}
	events := request(t, server.URL+"/api/v1/runs/"+string(runID)+"/events?limit=2", http.MethodGet, "viewer", "", nil)
	var eventPage app.EventPage
	if err := json.Unmarshal(events.body, &eventPage); err != nil || len(eventPage.Items) != 2 || eventPage.NextCursor == "" {
		t.Fatalf("event page = %s, err=%v", events.body, err)
	}
}

func TestControlPlaneCancelsRunningRun(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	workflowID := "cancel-demo-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "wait", TimeoutMillis: 10_000}}}
	definitions := app.NewWorkflowService(repository)
	engine, err := workflow.NewEngine(repository, mockexec.New(workflow.RealClock{}, map[mockexec.ScriptKey][]mockexec.Step{{DefinitionID: workflowID, TaskKey: "one"}: {{WaitForCancellation: true}}}), workflow.EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := startTestCoordinator(t, ctx, repository, engine)
	defer closeTestCoordinator(t, coordinator)
	runs, err := app.NewRunService(repository, definitions, engine, coordinator.Enqueue, func() (workflow.RunID, error) {
		return workflow.RunID("cancel-run-" + fmt.Sprint(time.Now().UnixNano())), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewHandler(httpapi.Dependencies{Workflows: definitions, Runs: runs, ViewerToken: "viewer", OperatorToken: "operator", Ready: coordinator.Ready}))
	defer server.Close()

	body, _ := json.Marshal(definition)
	created := request(t, server.URL+"/api/v1/workflows", http.MethodPost, "operator", "create-"+fmt.Sprint(time.Now().UnixNano()), body)
	if created.code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.code, created.body)
	}
	started := request(t, server.URL+"/api/v1/workflows/"+workflowID+"/versions/1/runs", http.MethodPost, "operator", "start-"+fmt.Sprint(time.Now().UnixNano()), []byte(`{}`))
	var startResponse app.StartRunResponse
	if started.code != http.StatusAccepted {
		t.Fatalf("start status = %d, body=%s", started.code, started.body)
	}
	if err := json.Unmarshal(started.body, &startResponse); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, server.URL, startResponse.RunID, workflow.WorkflowRunning)
	canceled := request(t, server.URL+"/api/v1/runs/"+string(startResponse.RunID)+"/cancel", http.MethodPost, "operator", "", []byte(`{}`))
	if canceled.code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", canceled.code, canceled.body)
	}
	waitForRunStatus(t, server.URL, startResponse.RunID, workflow.WorkflowCanceled)

	detail := request(t, server.URL+"/api/v1/runs/"+string(startResponse.RunID)+"/tasks/one", http.MethodGet, "viewer", "", nil)
	var taskDetail app.TaskDetail
	if err := json.Unmarshal(detail.body, &taskDetail); err != nil || len(taskDetail.Task.Attempts) != 1 || taskDetail.Task.Attempts[0].Status != workflow.AttemptCanceled {
		t.Fatalf("canceled task detail = %s, err=%v", detail.body, err)
	}
}

func TestControlPlanePersistsRetryAndTimeoutAttempts(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	workflowID := "failure-demo-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{
		ID:          workflowID,
		Concurrency: 1,
		Tasks: []workflow.TaskDefinition{
			{Key: "retry", Action: "temporary", TimeoutMillis: 1000, Retry: workflow.RetryPolicy{MaxAttempts: 2, IntervalMillis: 1}},
			{Key: "timeout", Action: "slow", DependsOn: []workflow.TaskKey{"retry"}, TimeoutMillis: 2},
		},
	}
	scripts := map[mockexec.ScriptKey][]mockexec.Step{
		{DefinitionID: workflowID, TaskKey: "retry"}: {
			{Kind: workflow.ResultTemporaryFailure, ErrorCode: "temporary"},
			{Kind: workflow.ResultSuccess, Delay: time.Millisecond},
		},
		{DefinitionID: workflowID, TaskKey: "timeout"}: {{Kind: workflow.ResultSuccess, Delay: 20 * time.Millisecond}},
	}
	definitions := app.NewWorkflowService(repository)
	engine, err := workflow.NewEngine(repository, mockexec.New(workflow.RealClock{}, scripts), workflow.EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := startTestCoordinator(t, ctx, repository, engine)
	defer closeTestCoordinator(t, coordinator)
	runs, err := app.NewRunService(repository, definitions, engine, coordinator.Enqueue, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewHandler(httpapi.Dependencies{Workflows: definitions, Runs: runs, ViewerToken: "viewer", OperatorToken: "operator", Ready: coordinator.Ready}))
	defer server.Close()

	body, _ := json.Marshal(definition)
	created := request(t, server.URL+"/api/v1/workflows", http.MethodPost, "operator", "create-"+fmt.Sprint(time.Now().UnixNano()), body)
	if created.code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.code, created.body)
	}
	started := request(t, server.URL+"/api/v1/workflows/"+workflowID+"/versions/1/runs", http.MethodPost, "operator", "start-"+fmt.Sprint(time.Now().UnixNano()), []byte(`{}`))
	var startResponse app.StartRunResponse
	if started.code != http.StatusAccepted {
		t.Fatalf("start status = %d, body=%s", started.code, started.body)
	}
	if err := json.Unmarshal(started.body, &startResponse); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, server.URL, startResponse.RunID, workflow.WorkflowFailed)

	retryResult := request(t, server.URL+"/api/v1/runs/"+string(startResponse.RunID)+"/tasks/retry", http.MethodGet, "viewer", "", nil)
	var retryDetail app.TaskDetail
	if err := json.Unmarshal(retryResult.body, &retryDetail); err != nil || len(retryDetail.Task.Attempts) != 2 ||
		retryDetail.Task.Attempts[0].Status != workflow.AttemptFailed || retryDetail.Task.Attempts[1].Status != workflow.AttemptSucceeded {
		t.Fatalf("retry task detail = %s, err=%v", retryResult.body, err)
	}
	timeoutResult := request(t, server.URL+"/api/v1/runs/"+string(startResponse.RunID)+"/tasks/timeout", http.MethodGet, "viewer", "", nil)
	var timeoutDetail app.TaskDetail
	if err := json.Unmarshal(timeoutResult.body, &timeoutDetail); err != nil || len(timeoutDetail.Task.Attempts) != 1 || timeoutDetail.Task.Attempts[0].Status != workflow.AttemptTimedOut {
		t.Fatalf("timeout task detail = %s, err=%v", timeoutResult.body, err)
	}
}

func TestCommittedPendingRunIsFoundAndResumedAfterEnqueueGap(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	workflowID := "recovery-demo-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{ID: workflowID, Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "noop", TimeoutMillis: 1000}}}
	definitions := app.NewWorkflowService(repository)
	if _, err := definitions.Create(ctx, "operator", "recovery-workflow-"+fmt.Sprint(time.Now().UnixNano()), definition); err != nil {
		t.Fatal(err)
	}
	engine, err := workflow.NewEngine(repository, mockexec.New(workflow.RealClock{}, map[mockexec.ScriptKey][]mockexec.Step{{DefinitionID: workflowID, TaskKey: "one"}: {{Kind: workflow.ResultSuccess}}}), workflow.EngineOptions{NewRunID: func() (workflow.RunID, error) {
		return workflow.RunID("recovery-run-" + fmt.Sprint(time.Now().UnixNano())), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := app.NewRunService(repository, definitions, engine, func(workflow.RunID) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := runs.Start(ctx, "operator", workflowID, 1, "recovery-run-"+fmt.Sprint(time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	ids, err := repository.ListNonTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range ids {
		if id == response.RunID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListNonTerminal() = %v, want %s", ids, response.RunID)
	}
	coordinator := startTestCoordinator(t, ctx, repository, engine)
	defer closeTestCoordinator(t, coordinator)
	if !coordinator.Ready() {
		t.Fatal("coordinator is not ready after recovery")
	}
	waitForRepositoryRunStatus(t, ctx, repository, response.RunID, workflow.WorkflowSucceeded)
}

func startTestCoordinator(t *testing.T, ctx context.Context, repository *postgres.Repository, engine *workflow.Engine) *runtimecoord.Coordinator {
	t.Helper()
	coordinator := runtimecoord.NewCoordinator(repository, engine, func(ctx context.Context) (runtimecoord.Lock, error) {
		return repository.AcquireAdvisoryLock(ctx)
	}, runtimecoord.CoordinatorOptions{LockCheckInterval: time.Hour, LockCheckTimeout: time.Second})
	if err := coordinator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func closeTestCoordinator(t *testing.T, coordinator *runtimecoord.Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Close(ctx); err != nil {
		t.Errorf("Coordinator.Close() error = %v", err)
	}
}

func waitForRepositoryRunStatus(t *testing.T, ctx context.Context, repository *postgres.Repository, id workflow.RunID, want workflow.WorkflowStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := repository.LoadRun(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Run %s did not reach %s", id, want)
}

type result struct {
	code int
	body []byte
}

func request(t *testing.T, url, method, token, key string, body []byte) result {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return result{code: response.StatusCode, body: data}
}

func waitForRunStatus(t *testing.T, serverURL string, runID workflow.RunID, want workflow.WorkflowStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response := request(t, serverURL+"/api/v1/runs/"+string(runID), http.MethodGet, "viewer", "", nil)
		if response.code != http.StatusOK {
			t.Fatalf("get Run status = %d, body=%s", response.code, response.body)
		}
		var summary app.RunSummary
		if err := json.Unmarshal(response.body, &summary); err != nil {
			t.Fatal(err)
		}
		if summary.Status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Run %s did not reach %s", runID, want)
}

var _ = time.Second
