package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestWorkflowServiceRejectsInvalidDefinitionBeforeRepository(t *testing.T) {
	repository := newFakeRepository()
	service := NewWorkflowService(repository).(*workflowService)

	_, err := service.Create(context.Background(), "operator", "create-key", workflow.WorkflowDefinition{
		ID:          "invalid",
		Concurrency: 1,
		Tasks: []workflow.TaskDefinition{
			{Key: "a", Action: "", TimeoutMillis: 1},
		},
	})
	if err == nil {
		t.Fatal("Create() error = nil, want invalid definition error")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Create() error = %v, want ErrInvalidArgument", err)
	}
	if repository.createDefinitionCalls != 0 {
		t.Fatalf("CreateDefinition() calls = %d, want 0", repository.createDefinitionCalls)
	}
}

func TestWorkflowServiceCreatesAndCachesVersion(t *testing.T) {
	repository := newFakeRepository()
	repository.createDefinitionResult = postgres.DefinitionRecord{WorkflowID: "document-pipeline", Version: 1}
	service := NewWorkflowService(repository).(*workflowService)
	definition := appDefinition()

	ref, err := service.Create(context.Background(), "operator", "create-key", definition)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if ref != (DefinitionRef{WorkflowID: "document-pipeline", Version: 1}) {
		t.Fatalf("Create() = %+v", ref)
	}
	compiled, err := service.compiled(context.Background(), ref.WorkflowID, ref.Version)
	if err != nil {
		t.Fatalf("compiled() error = %v", err)
	}
	wantDefinition := definition
	wantDefinition.Tasks[0].Executor = workflow.ExecutorMock
	wantDefinition.Tasks[0].Retry.MaxAttempts = 1
	if !reflect.DeepEqual(compiled.Definition(), wantDefinition) {
		t.Fatalf("cached definition = %+v, want %+v", compiled.Definition(), wantDefinition)
	}
	if repository.createDefinitionCalls != 1 {
		t.Fatalf("CreateDefinition() calls = %d, want 1", repository.createDefinitionCalls)
	}
}

func TestDefinitionHashTreatsMissingAndExplicitDefaultExecutorAsEquivalent(t *testing.T) {
	withoutExecutor := appDefinition()
	explicitMock := appDefinition()
	explicitMock.Tasks[0].Executor = workflow.ExecutorMock

	if got, want := definitionHash(explicitMock), definitionHash(withoutExecutor); got != want {
		t.Fatalf("explicit mock hash = %s, want legacy missing-executor hash %s", got, want)
	}

	explicitAgent := appDefinition()
	explicitAgent.Tasks[0].Executor = workflow.ExecutorKind("agent")
	if definitionHash(explicitAgent) == definitionHash(withoutExecutor) {
		t.Fatal("non-default executor unexpectedly shares the default executor hash")
	}
}

func TestWorkflowServiceRejectsMismatchedVersionDefinitionIDBeforeRepository(t *testing.T) {
	repository := newFakeRepository()
	service := NewWorkflowService(repository)
	definition := appDefinition()
	definition.ID = "different-workflow"

	_, err := service.CreateVersion(context.Background(), "operator", "document-pipeline", "version-key", definition)

	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("CreateVersion() error = %v, want ErrInvalidArgument", err)
	}
	if repository.createVersionCalls != 0 {
		t.Fatalf("CreateVersion() repository calls = %d, want 0", repository.createVersionCalls)
	}
}

func TestWorkflowServiceUsesScopedKeysetCursors(t *testing.T) {
	repository := newFakeRepository()
	repository.workflowRecords = []postgres.WorkflowRecord{{WorkflowID: "workflow-a"}}
	repository.workflowRecordsMore = true
	service := NewWorkflowService(repository)

	page, err := service.List(context.Background(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.NextCursor == "" || page.NextCursor == "1" || repository.lastWorkflowAfterID != "" {
		t.Fatalf("first workflow page = %+v, after=%q", page, repository.lastWorkflowAfterID)
	}
	if _, err := service.List(context.Background(), page.NextCursor, 1); err != nil {
		t.Fatal(err)
	}
	if repository.lastWorkflowAfterID != "workflow-a" {
		t.Fatalf("workflow after ID = %q, want workflow-a", repository.lastWorkflowAfterID)
	}

	repository.versionRecords = []postgres.VersionRecord{{WorkflowID: "workflow-a", Version: 2}}
	repository.versionRecordsMore = true
	versions, err := service.ListVersions(context.Background(), "workflow-a", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if versions.NextCursor == "" || versions.NextCursor == "1" {
		t.Fatalf("version page cursor = %q, want opaque cursor", versions.NextCursor)
	}
	if _, err := service.ListVersions(context.Background(), "workflow-b", versions.NextCursor, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("cross-workflow version cursor error = %v, want ErrInvalidArgument", err)
	}
}

func TestRunServiceStartsPendingRunAndWakesControllerAfterPersistence(t *testing.T) {
	repository := newFakeRepository()
	repository.definitions["document-pipeline/1"] = appDefinition()
	runID := workflow.RunID("run-one")
	repository.createRunID = runID
	controller := &fakeEngine{onWake: func() {
		if repository.createRunCalls != 1 {
			t.Fatalf("Wake() observed CreateRun calls = %d, want persisted Run first", repository.createRunCalls)
		}
	}}
	service := mustNewRunService(t, repository, controller, func() (workflow.RunID, error) { return runID, nil })

	response, err := service.Start(context.Background(), "operator", "document-pipeline", 1, "start-key")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if response != (StartRunResponse{RunID: runID, Status: workflow.WorkflowPending}) {
		t.Fatalf("Start() = %+v", response)
	}
	if controller.wakeCalls != 1 {
		t.Fatalf("Wake() calls = %d, want 1", controller.wakeCalls)
	}
	if repository.createRunCalls != 1 {
		t.Fatalf("CreateRun() calls = %d, want 1", repository.createRunCalls)
	}
}

func TestNewRunServiceRejectsMissingController(t *testing.T) {
	repository := newFakeRepository()
	_, err := NewRunService(repository, NewWorkflowService(repository), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "run controller is required") {
		t.Fatalf("NewRunService() error = %v, want missing controller error", err)
	}
}

func TestRunServiceRejectsUnknownDefinitionVersion(t *testing.T) {
	repository := newFakeRepository()
	service := mustNewRunService(t, repository, &fakeEngine{}, func() (workflow.RunID, error) { return "run-one", nil })

	if _, err := service.Start(context.Background(), "operator", "missing", 2, "start-key"); !errors.Is(err, postgres.ErrDefinitionNotFound) {
		t.Fatalf("Start() error = %v, want ErrDefinitionNotFound", err)
	}
	if repository.createRunCalls != 0 {
		t.Fatalf("CreateRun() calls = %d, want 0", repository.createRunCalls)
	}
}

func TestRunServiceDoesNotEnqueueIdempotentReplay(t *testing.T) {
	repository := newFakeRepository()
	repository.definitions["document-pipeline/1"] = appDefinition()
	repository.createRunID = "run-one"
	repository.createRunCreated = []bool{true, false}
	controller := &fakeEngine{}
	service := mustNewRunService(t, repository, controller, func() (workflow.RunID, error) { return "run-one", nil })

	for range 2 {
		if _, err := service.Start(context.Background(), "operator", "document-pipeline", 1, "same-key"); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}
	if controller.wakeCalls != 1 {
		t.Fatalf("Wake() calls = %d, want 1", controller.wakeCalls)
	}
}

func TestRunServiceFiltersWithStableCursor(t *testing.T) {
	repository := newFakeRepository()
	createdAt := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	repository.runRecords = []postgres.RunRecord{{
		ID: "run-one", DefinitionID: "document-pipeline", Status: workflow.WorkflowRunning, CreatedAt: createdAt,
	}}
	repository.runRecordsMore = true
	service := mustNewRunService(t, repository, &fakeEngine{}, nil)
	options := RunListOptions{WorkflowID: "document-pipeline", Status: "running", Limit: 1}

	first, err := service.List(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want one item and cursor", first)
	}
	if repository.lastRunQuery.WorkflowID != "document-pipeline" || repository.lastRunQuery.Status != workflow.WorkflowRunning || repository.lastRunQuery.AfterCreated != nil {
		t.Fatalf("first RunQuery = %+v", repository.lastRunQuery)
	}

	options.Cursor = first.NextCursor
	if _, err := service.List(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	if repository.lastRunQuery.AfterCreated == nil || !repository.lastRunQuery.AfterCreated.Equal(createdAt) || repository.lastRunQuery.AfterRunID != "run-one" {
		t.Fatalf("second RunQuery = %+v, want last record key", repository.lastRunQuery)
	}

	options.WorkflowID = "other-workflow"
	if _, err := service.List(context.Background(), options); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("List() mismatched filter error = %v, want ErrInvalidArgument", err)
	}
	if _, err := service.List(context.Background(), RunListOptions{Status: "unknown"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("List() invalid status error = %v, want ErrInvalidArgument", err)
	}
}

func TestRunServiceSummaryDoesNotLoadAttemptHistory(t *testing.T) {
	repository := newFakeRepository()
	run := appRun()
	repository.runs[run.ID] = run
	repository.tasks[run.ID] = []workflow.TaskRun{{Key: "clean"}, {Key: "summarize"}}
	service := mustNewRunService(t, repository, &fakeEngine{}, nil)

	summary, err := service.Get(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if summary.TaskCount != 2 {
		t.Fatalf("TaskCount = %d, want 2", summary.TaskCount)
	}
	if repository.loadTaskRunCalls != 0 {
		t.Fatalf("LoadTaskRun() calls = %d, want 0", repository.loadTaskRunCalls)
	}
}

func TestRunServicePaginatesTasksAndEvents(t *testing.T) {
	repository := newFakeRepository()
	run := appRun()
	repository.runs[run.ID] = run
	repository.tasks[run.ID] = []workflow.TaskRun{
		{Key: "clean", Status: workflow.TaskSucceeded},
		{Key: "summarize", Status: workflow.TaskReady},
		{Key: "publish", Status: workflow.TaskWaitingDependencies},
	}
	repository.events[run.ID] = []workflow.StateEvent{
		{Sequence: 1, Entity: "workflow", To: "running"},
		{Sequence: 2, Entity: "task", To: "running"},
		{Sequence: 3, Entity: "task", To: "succeeded"},
	}
	service := mustNewRunService(t, repository, &fakeEngine{}, nil)

	tasks, err := service.ListTasks(context.Background(), run.ID, "", 2)
	if err != nil {
		t.Fatalf("ListTasks() error = %v", err)
	}
	if len(tasks.Items) != 2 || tasks.NextCursor == "" || tasks.NextCursor == "2" {
		t.Fatalf("first task page = %+v, want 2 items and opaque cursor", tasks)
	}
	tasks, err = service.ListTasks(context.Background(), run.ID, tasks.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListTasks() second page error = %v", err)
	}
	if len(tasks.Items) != 1 || tasks.NextCursor != "" || tasks.Items[0].Key != "publish" {
		t.Fatalf("second task page = %+v, want final publish item", tasks)
	}

	events, err := service.ListEvents(context.Background(), run.ID, "", 2)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events.Items) != 2 || events.NextCursor == "" || events.NextCursor == "2" {
		t.Fatalf("first event page = %+v, want 2 items and opaque cursor", events)
	}
	events, err = service.ListEvents(context.Background(), run.ID, events.NextCursor, 2)
	if err != nil {
		t.Fatalf("ListEvents() second page error = %v", err)
	}
	if len(events.Items) != 1 || events.NextCursor != "" || events.Items[0].Sequence != 3 {
		t.Fatalf("second event page = %+v, want sequence 3", events)
	}
}

func TestRunServiceRejectsInvalidPageParametersBeforeRepository(t *testing.T) {
	repository := newFakeRepository()
	service := mustNewRunService(t, repository, &fakeEngine{}, nil)

	if _, err := service.ListTasks(context.Background(), "run-one", "bad", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("ListTasks() error = nil, want invalid cursor error")
	}
	if _, err := service.ListEvents(context.Background(), "run-one", "-1", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("ListEvents() error = nil, want invalid cursor error")
	}
	if _, err := service.ListTasks(context.Background(), "run-one", "", 101); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("ListTasks() error = nil, want limit error")
	}
	if repository.listTaskRunsCalls != 0 || repository.listStateEventsCalls != 0 {
		t.Fatalf("repository calls = tasks:%d events:%d, want 0/0", repository.listTaskRunsCalls, repository.listStateEventsCalls)
	}
}

func TestRunServiceCancelPersistsBeforeNotifyingEngine(t *testing.T) {
	repository := newFakeRepository()
	run := appRun()
	repository.runs[run.ID] = run
	engine := &fakeEngine{onCancel: func() {
		if repository.cancelCalls != 1 || repository.cancelAt.IsZero() {
			t.Fatalf("Controller observed cancellation before persistence: calls=%d at=%v", repository.cancelCalls, repository.cancelAt)
		}
	}}
	service := mustNewRunService(t, repository, engine, func() (workflow.RunID, error) { return "run-two", nil })

	if _, err := service.Cancel(context.Background(), "operator", run.ID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if repository.cancelCalls != 1 || engine.cancelCalls != 1 {
		t.Fatalf("cancel calls = repository:%d engine:%d, want 1/1", repository.cancelCalls, engine.cancelCalls)
	}
	if repository.cancelAt.After(time.Now().UTC()) {
		t.Fatalf("cancel timestamp = %v, cannot be in the future", repository.cancelAt)
	}
}

func appDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:          "document-pipeline",
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "clean", Action: "clean", TimeoutMillis: 1000}},
	}
}

func appRun() workflow.WorkflowRun {
	return workflow.WorkflowRun{ID: "run-one", DefinitionID: "document-pipeline", DefinitionVersion: 1, Status: workflow.WorkflowPending, CreatedAt: time.Unix(1, 0).UTC()}
}

func mustNewRunService(
	t *testing.T,
	repository *fakeRepository,
	controller RunController,
	newRunID runIDGenerator,
) RunService {
	t.Helper()
	service, err := NewRunService(repository, NewWorkflowService(repository), controller, newRunID)
	if err != nil {
		t.Fatal(err)
	}
	return service
}
