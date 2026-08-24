package workloadclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestClientAddsBearerIdempotencyAndJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer operator-secret" || r.Header.Get("Idempotency-Key") != "create-1" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("headers = %+v", r.Header)
		}
		var definition workflow.WorkflowDefinition
		if err := json.NewDecoder(r.Body).Decode(&definition); err != nil || definition.ID != "demo" {
			t.Fatalf("body = %+v, err=%v", definition, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(DefinitionRef{WorkflowID: "demo", Version: 1})
	}))
	defer server.Close()
	client := New(server.URL, "operator-secret")
	ref, err := client.CreateWorkflow(context.Background(), "create-1", workflow.WorkflowDefinition{ID: "demo", Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "noop"}}})
	if err != nil || ref.Version != 1 {
		t.Fatalf("CreateWorkflow() = %+v, %v", ref, err)
	}
}

func TestClientPreservesLargeTaskInputNumber(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"demo","concurrency":1,"tasks":[{"key":"one","action":"run","input":{"value":9007199254740993},"timeout_ms":1000}]}`))
	}))
	defer server.Close()
	definition, err := New(server.URL, "viewer-secret").GetWorkflowVersion(context.Background(), "demo", 1)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := definition.Tasks[0].Input["value"].(json.Number)
	if !ok || value.String() != "9007199254740993" {
		t.Fatalf("input value = %#v, want exact json.Number", definition.Tasks[0].Input["value"])
	}
}

func TestClientParsesAPIErrorAndDoesNotRetryWrite(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"idempotency_conflict","message":"conflict","request_id":"req-1"}}`))
	}))
	defer server.Close()
	client := New(server.URL, "operator-secret")
	_, err := client.StartRun(context.Background(), "demo", 1, "start-1")
	if err == nil || calls != 1 {
		t.Fatalf("StartRun() err=%v calls=%d, want one request", err, calls)
	}
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Code != "idempotency_conflict" || apiErr.RequestID != "req-1" {
		t.Fatalf("error = %#v", err)
	}
}

func TestClientPassesContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	client := New(server.URL, "viewer-secret")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.GetRun(ctx, "run-1")
	if err == nil {
		t.Fatal("GetRun() error = nil, want context cancellation")
	}
}

func TestClientSendsPaginationCursorAndLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "next" || r.URL.Query().Get("limit") != "10" {
			t.Fatalf("query = %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(EventPage{Items: []workflow.StateEvent{{Sequence: 2}}, NextCursor: "next-2"})
	}))
	defer server.Close()
	client := New(server.URL, "viewer-secret")
	page, err := client.ListRunEvents(context.Background(), "run-1", "next", 10)
	if err != nil || len(page.Items) != 1 || page.NextCursor != "next-2" {
		t.Fatalf("ListRunEvents() = %+v, %v", page, err)
	}
}

func TestClientListRunsFilteredEncodesOptionsAndKeepsLegacyMethod(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		query := r.URL.Query()
		if calls == 1 {
			if query.Get("cursor") != "opaque" || query.Get("limit") != "7" || query.Get("workflow_id") != "demo/space value" || query.Get("status") != "running" {
				t.Fatalf("filtered query = %s", r.URL.RawQuery)
			}
		} else if query.Get("cursor") != "legacy" || query.Get("limit") != "3" || query.Get("workflow_id") != "" || query.Get("status") != "" {
			t.Fatalf("legacy query = %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RunPage{})
	}))
	defer server.Close()
	client := New(server.URL, "viewer-secret")
	if _, err := client.ListRunsFiltered(context.Background(), RunListOptions{
		Cursor: "opaque", Limit: 7, WorkflowID: "demo/space value", Status: workflow.WorkflowRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListRuns(context.Background(), "legacy", 3); err != nil {
		t.Fatal(err)
	}
}

func TestClientCancelRunWithResponseKeepsLegacyMethod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CancelRunResponse{
			RunID: "run-1", Status: workflow.WorkflowRunning, CancelRequestedAt: time.Unix(1, 0).UTC(),
		})
	}))
	defer server.Close()
	client := New(server.URL, "operator-secret")
	response, err := client.CancelRunWithResponse(context.Background(), "run-1")
	if err != nil || response.RunID != "run-1" || response.CancelRequestedAt.IsZero() {
		t.Fatalf("CancelRunWithResponse() = %+v, %v", response, err)
	}
	if err := client.CancelRun(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}
}

var _ = strings.TrimSpace
var _ = time.Second
