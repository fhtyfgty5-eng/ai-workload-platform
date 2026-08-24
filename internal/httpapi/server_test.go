package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestHandlerHealthDoesNotRequireToken(t *testing.T) {
	handler := NewHandler(Dependencies{Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("live status = %d, want 200", recorder.Code)
	}
}

func TestHandlerRejectsBusinessRequestsBeforeRecoveryIsReady(t *testing.T) {
	handler := NewHandler(Dependencies{ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return false }})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1", nil)
	request.Header.Set("Authorization", "Bearer viewer-secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("business request status = %d, want 503", recorder.Code)
	}
}

func TestHandlerLogsRequestMetadataWithoutCredentials(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := NewHandler(Dependencies{ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }, Logger: logger})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1", nil)
	request.Header.Set("Authorization", "Bearer viewer-secret")
	request.Header.Set("X-Request-ID", "req_test")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	logLine := output.String()
	if !strings.Contains(logLine, `"request_id":"req_test"`) || !strings.Contains(logLine, `"status":500`) ||
		!strings.Contains(logLine, `"error_code":"internal_error"`) || !strings.Contains(logLine, "run service is unavailable") ||
		!strings.Contains(logLine, `"path":"/api/v1/runs/{run-id}"`) {
		t.Fatalf("request log = %s, want request_id, status, error code, and internal diagnostic", logLine)
	}
	if strings.Contains(logLine, "viewer-secret") || strings.Contains(strings.ToLower(logLine), "authorization") {
		t.Fatalf("request log leaked credentials: %s", logLine)
	}
}

func TestHandlerLogsBusinessIdentifiersWithoutCredentialsOrBody(t *testing.T) {
	services := newFakeServices()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := NewHandler(Dependencies{
		Workflows: services.workflows, Runs: services.runs,
		ViewerToken: "viewer-top-secret", OperatorToken: "operator-top-secret",
		Ready: func() bool { return true }, Logger: logger,
	})

	create := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"do-not-log-body"}]}`))
	create.Header.Set("Authorization", "Bearer operator-top-secret")
	create.Header.Set("Idempotency-Key", "create-one")
	handler.ServeHTTP(httptest.NewRecorder(), create)

	start := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/demo/versions/1/runs", strings.NewReader(`{}`))
	start.Header.Set("Authorization", "Bearer operator-top-secret")
	start.Header.Set("Idempotency-Key", "start-one")
	handler.ServeHTTP(httptest.NewRecorder(), start)

	cancel := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", strings.NewReader(`{}`))
	cancel.Header.Set("Authorization", "Bearer operator-top-secret")
	handler.ServeHTTP(httptest.NewRecorder(), cancel)

	got := output.String()
	for _, want := range []string{
		`"msg":"workflow version created"`, `"workflow_id":"demo"`, `"workflow_version":1`,
		`"msg":"run created"`, `"run_id":"run-1"`, `"msg":"run cancellation requested"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs do not contain %q: %s", want, got)
		}
	}
	for _, secret := range []string{"operator-top-secret", "viewer-top-secret", "do-not-log-body", "Authorization"} {
		if strings.Contains(got, secret) {
			t.Fatalf("logs leaked %q: %s", secret, got)
		}
	}
}

func TestHandlerMapsApplicationInvalidArgumentToBadRequest(t *testing.T) {
	services := newFakeServices()
	services.workflows.createErr = app.ErrInvalidArgument
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}]}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "create-1")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid definition status = %d, want 400", recorder.Code)
	}
}

func TestHandlerPreservesLargeTaskInputNumber(t *testing.T) {
	services := newFakeServices()
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"run","input":{"value":9007199254740993},"timeout_ms":1000}]}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "large-number")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	value, ok := services.workflows.definition.Tasks[0].Input["value"].(json.Number)
	if !ok || value.String() != "9007199254740993" {
		t.Fatalf("input value = %#v, want exact json.Number", services.workflows.definition.Tasks[0].Input["value"])
	}
}

func TestHandlerRejectsMissingTokenAndViewerCannotCreateWorkflow(t *testing.T) {
	services := newFakeServices()
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}]}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}]}`))
	request.Header.Set("Authorization", "Bearer viewer-secret")
	request.Header.Set("Idempotency-Key", "create-1")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("viewer create status = %d, want 403", recorder.Code)
	}
}

func TestHandlerStrictJSONAndWorkflowCreateResponse(t *testing.T) {
	services := newFakeServices()
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}],"unknown":true}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "create-1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}]} {}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "create-1")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("multiple JSON values status = %d, want 400", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/workflows", strings.NewReader(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"noop"}]}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "create-1")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", recorder.Code)
	}
	if recorder.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", recorder.Header().Get("Content-Type"))
	}
}

func TestHandlerStartRunReturnsAcceptedAndMapsIdempotencyConflict(t *testing.T) {
	services := newFakeServices()
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/demo/versions/1/runs", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "start-1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("start status = %d, want 202", recorder.Code)
	}

	services.runs.startErr = postgres.ErrIdempotencyConflict
	request = httptest.NewRequest(http.MethodPost, "/api/v1/workflows/demo/versions/1/runs", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	request.Header.Set("Idempotency-Key", "start-2")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", recorder.Code)
	}
	var body errorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Code == "" || body.Error.RequestID == "" {
		t.Fatalf("error body = %+v, want code and request_id", body)
	}
}

func TestHandlerPassesRunListFiltersAndReturnsBoundedCancelResponse(t *testing.T) {
	services := newFakeServices()
	handler := NewHandler(Dependencies{Workflows: services.workflows, Runs: services.runs, ViewerToken: "viewer-secret", OperatorToken: "operator-secret", Ready: func() bool { return true }})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs?workflow_id=demo&status=running&cursor=opaque&limit=5", nil)
	request.Header.Set("Authorization", "Bearer viewer-secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if services.runs.listOptions != (app.RunListOptions{WorkflowID: "demo", Status: "running", Cursor: "opaque", Limit: 5}) {
		t.Fatalf("RunListOptions = %+v", services.runs.listOptions)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "tasks") || !strings.Contains(recorder.Body.String(), `"run_id":"run-1"`) {
		t.Fatalf("cancel status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

type fakeServices struct {
	workflows *fakeWorkflowService
	runs      *fakeRunService
}

func newFakeServices() fakeServices {
	return fakeServices{workflows: &fakeWorkflowService{}, runs: &fakeRunService{}}
}

type fakeWorkflowService struct {
	createErr  error
	definition workflow.WorkflowDefinition
}

func (f *fakeWorkflowService) Create(_ context.Context, _, _ string, definition workflow.WorkflowDefinition) (app.DefinitionRef, error) {
	if f.createErr != nil {
		return app.DefinitionRef{}, f.createErr
	}
	f.definition = definition
	return app.DefinitionRef{WorkflowID: "demo", Version: 1}, nil
}
func (*fakeWorkflowService) CreateVersion(context.Context, string, string, string, workflow.WorkflowDefinition) (app.DefinitionRef, error) {
	return app.DefinitionRef{WorkflowID: "demo", Version: 2}, nil
}
func (*fakeWorkflowService) Get(context.Context, string, int) (workflow.WorkflowDefinition, error) {
	return workflow.WorkflowDefinition{ID: "demo", Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "noop"}}}, nil
}
func (*fakeWorkflowService) GetSummary(context.Context, string) (app.WorkflowSummary, error) {
	return app.WorkflowSummary{WorkflowID: "demo", LatestVersion: 1}, nil
}
func (*fakeWorkflowService) List(context.Context, string, int) (app.WorkflowPage, error) {
	return app.WorkflowPage{}, nil
}
func (*fakeWorkflowService) ListVersions(context.Context, string, string, int) (app.VersionPage, error) {
	return app.VersionPage{}, nil
}

type fakeRunService struct {
	startErr    error
	listOptions app.RunListOptions
}

func (f *fakeRunService) Start(context.Context, string, string, int, string) (app.StartRunResponse, error) {
	if f.startErr != nil {
		return app.StartRunResponse{}, f.startErr
	}
	return app.StartRunResponse{RunID: "run-1", Status: workflow.WorkflowPending}, nil
}
func (f *fakeRunService) List(_ context.Context, options app.RunListOptions) (app.RunPage, error) {
	f.listOptions = options
	return app.RunPage{}, nil
}
func (*fakeRunService) Get(context.Context, workflow.RunID) (app.RunSummary, error) {
	return app.RunSummary{ID: "run-1", DefinitionID: "demo", DefinitionVersion: 1}, nil
}
func (*fakeRunService) ListTasks(context.Context, workflow.RunID, string, int) (app.TaskPage, error) {
	return app.TaskPage{}, nil
}
func (*fakeRunService) GetTask(context.Context, workflow.RunID, workflow.TaskKey) (app.TaskDetail, error) {
	return app.TaskDetail{}, nil
}
func (*fakeRunService) ListEvents(context.Context, workflow.RunID, string, int) (app.EventPage, error) {
	return app.EventPage{}, nil
}
func (*fakeRunService) Cancel(_ context.Context, _ string, id workflow.RunID) (app.CancelRunResponse, error) {
	return app.CancelRunResponse{RunID: id, Status: workflow.WorkflowRunning, CancelRequestedAt: time.Unix(1, 0).UTC()}, nil
}

type errorEnvelope struct {
	Error struct {
		Code      string `json:"code"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

var _ = errors.Is
var _ = time.Time{}
