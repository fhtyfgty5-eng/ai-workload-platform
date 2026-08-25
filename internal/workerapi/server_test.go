package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	testBootstrapToken = "bootstrap-test-token"
	testSessionToken   = "session-test-token"
	testWorkerID       = "wrk_test"
)

func TestServerHandlesWorkerWriteOperations(t *testing.T) {
	store := newFakeStore()
	server := newTestServer(t, store, 1024)

	tests := []struct {
		name      string
		path      string
		token     string
		body      string
		wantCode  int
		wantField string
	}{
		{
			name: "register", path: "/api/v1/workers/register", token: testBootstrapToken,
			body:     `{"display_name":"worker","protocol_version":1,"executor_kinds":["mock"],"max_concurrency":2}`,
			wantCode: http.StatusCreated, wantField: `"session_token":"session-test-token"`,
		},
		{
			name: "claim", path: "/api/v1/workers/wrk_test/claims", token: testSessionToken,
			body: `{"slots":1}`, wantCode: http.StatusOK, wantField: `"leases":[]`,
		},
		{
			name: "heartbeat", path: "/api/v1/workers/wrk_test/heartbeat", token: testSessionToken,
			body: `{"leases":[]}`, wantCode: http.StatusOK, wantField: `"leases":[]`,
		},
		{
			name: "complete", path: "/api/v1/workers/wrk_test/leases/dsp_test/complete", token: testSessionToken,
			body:     `{"lease_token":"lease-test-token","result":{"kind":"success","output":"done"}}`,
			wantCode: http.StatusOK, wantField: `"applied":true`,
		},
		{
			name: "drain", path: "/api/v1/workers/wrk_test/drain", token: testSessionToken,
			body: `{}`, wantCode: http.StatusOK, wantField: `"status":"draining"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer "+test.token)
			if test.name != "register" {
				request.Header.Set(workerprotocol.ProtocolVersionHeader, "1")
			}
			recorder := httptest.NewRecorder()
			if apiErr := server.Handle(recorder, request, "req-worker-write"); apiErr != nil {
				t.Fatalf("Handle() error = %v", apiErr)
			}
			if recorder.Code != test.wantCode || !strings.Contains(recorder.Body.String(), test.wantField) {
				t.Fatalf("response = %d %s, want %d containing %s", recorder.Code, recorder.Body.String(), test.wantCode, test.wantField)
			}
			if got := recorder.Header().Get(workerprotocol.ProtocolVersionHeader); got != "1" {
				t.Fatalf("protocol response header = %q, want 1", got)
			}
		})
	}

	if len(store.operations) != 4 {
		t.Fatalf("authenticated operations = %v, want four session operations", store.operations)
	}
	wantOperations := []workerprotocol.WorkerOperation{
		workerprotocol.OperationClaim,
		workerprotocol.OperationHeartbeat,
		workerprotocol.OperationComplete,
		workerprotocol.OperationDrain,
	}
	for index := range wantOperations {
		if store.operations[index] != wantOperations[index] {
			t.Fatalf("operation[%d] = %q, want %q", index, store.operations[index], wantOperations[index])
		}
	}
}

func TestServerWakesCoordinatorAfterCapacityOrTaskStateChanges(t *testing.T) {
	store := newFakeStore()
	wakeCount := 0
	server, err := New(store, Options{
		BootstrapToken:    testBootstrapToken,
		HeartbeatInterval: 5 * time.Second,
		LeaseDuration:     15 * time.Second,
		Wake:              func() { wakeCount++ },
	})
	if err != nil {
		t.Fatal(err)
	}

	requests := []struct {
		path     string
		token    string
		protocol bool
		body     string
	}{
		{
			path: "/api/v1/workers/register", token: testBootstrapToken,
			body: `{"display_name":"worker","protocol_version":1,"executor_kinds":["mock"],"max_concurrency":1}`,
		},
		{
			path: "/api/v1/workers/wrk_test/leases/dsp_test/complete", token: testSessionToken, protocol: true,
			body: `{"lease_token":"lease-test-token","result":{"kind":"success","output":"done"}}`,
		},
	}
	for _, item := range requests {
		request := httptest.NewRequest(http.MethodPost, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+item.token)
		if item.protocol {
			request.Header.Set(workerprotocol.ProtocolVersionHeader, "1")
		}
		if apiErr := server.Handle(httptest.NewRecorder(), request, "req-wake"); apiErr != nil {
			t.Fatalf("Handle(%s) error = %v", item.path, apiErr)
		}
	}
	if wakeCount != 2 {
		t.Fatalf("coordinator wake count = %d, want 2", wakeCount)
	}
}

func TestServerHandlesWorkerQueriesWithoutCredentialFields(t *testing.T) {
	server := newTestServer(t, newFakeStore(), 1024)
	for _, test := range []struct {
		path      string
		wantField string
	}{
		{path: "/api/v1/workers?limit=1", wantField: `"next_cursor":"`},
		{path: "/api/v1/workers/wrk_test", wantField: `"worker_id":"wrk_test"`},
	} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		recorder := httptest.NewRecorder()
		if apiErr := server.Handle(recorder, request, "req-worker-query"); apiErr != nil {
			t.Fatalf("Handle(%s) error = %v", test.path, apiErr)
		}
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), test.wantField) {
			t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
		}
		for _, forbidden := range []string{"session_token", "lease_token", "token_hash", `"input"`} {
			if strings.Contains(recorder.Body.String(), forbidden) {
				t.Fatalf("query response contains %q: %s", forbidden, recorder.Body.String())
			}
		}
	}
}

func TestServerRejectsInvalidAuthenticationProtocolAndJSON(t *testing.T) {
	store := newFakeStore()
	server := newTestServer(t, store, 128)
	tests := []struct {
		name     string
		path     string
		token    string
		protocol string
		body     string
		wantCode int
		wantErr  string
	}{
		{name: "wrong bootstrap", path: "/api/v1/workers/register", token: "operator-token", body: `{}`, wantCode: 401, wantErr: "unauthorized"},
		{name: "wrong session", path: "/api/v1/workers/wrk_test/claims", token: "operator-token", protocol: "1", body: `{"slots":1}`, wantCode: 401, wantErr: "worker_session_invalid"},
		{name: "missing protocol", path: "/api/v1/workers/wrk_test/claims", token: testSessionToken, body: `{"slots":1}`, wantCode: 400, wantErr: "worker_protocol_unsupported"},
		{name: "unsupported protocol", path: "/api/v1/workers/wrk_test/claims", token: testSessionToken, protocol: "2", body: `{"slots":1}`, wantCode: 400, wantErr: "worker_protocol_unsupported"},
		{name: "unknown field", path: "/api/v1/workers/wrk_test/claims", token: testSessionToken, protocol: "1", body: `{"slots":1,"unknown":true}`, wantCode: 400, wantErr: "invalid_request"},
		{name: "body too large", path: "/api/v1/workers/wrk_test/heartbeat", token: testSessionToken, protocol: "1", body: `{"leases":[]}` + strings.Repeat(" ", 256), wantCode: 400, wantErr: "invalid_request"},
		{name: "wrong method", path: "/api/v1/workers/wrk_test/claims", token: testSessionToken, protocol: "1", body: `{"slots":1}`, wantCode: 405, wantErr: "method_not_allowed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			method := http.MethodPost
			if test.name == "wrong method" {
				method = http.MethodPut
			}
			request := httptest.NewRequest(method, test.path, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer "+test.token)
			if test.protocol != "" {
				request.Header.Set(workerprotocol.ProtocolVersionHeader, test.protocol)
			}
			recorder := httptest.NewRecorder()
			apiErr := server.Handle(recorder, request, "req-invalid-worker")
			if apiErr == nil || apiErr.Status != test.wantCode || apiErr.Code != test.wantErr {
				t.Fatalf("Handle() error = %+v, want status %d code %s", apiErr, test.wantCode, test.wantErr)
			}
			if apiErr.RequestID != "req-invalid-worker" {
				t.Fatalf("request ID = %q", apiErr.RequestID)
			}
		})
	}
}

func TestServerMapsStableRepositoryErrors(t *testing.T) {
	tests := []struct {
		err      error
		wantCode int
		wantName string
	}{
		{err: postgres.ErrWorkerDraining, wantCode: 409, wantName: "worker_draining"},
		{err: postgres.ErrWorkerCapacityExceeded, wantCode: 409, wantName: "worker_capacity_exceeded"},
		{err: postgres.ErrLeaseLost, wantCode: 409, wantName: "lease_lost"},
		{err: postgres.ErrResultConflict, wantCode: 409, wantName: "result_conflict"},
		{err: postgres.ErrInvalidResult, wantCode: 400, wantName: "invalid_request"},
		{err: postgres.ErrWorkerNotFound, wantCode: 404, wantName: "not_found"},
	}
	for _, test := range tests {
		store := newFakeStore()
		store.claimErr = test.err
		server := newTestServer(t, store, 1024)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/workers/wrk_test/claims", strings.NewReader(`{"slots":1}`))
		request.Header.Set("Authorization", "Bearer "+testSessionToken)
		request.Header.Set(workerprotocol.ProtocolVersionHeader, "1")
		apiErr := server.Handle(httptest.NewRecorder(), request, "req-stable-error")
		if apiErr == nil || apiErr.Status != test.wantCode || apiErr.Code != test.wantName {
			t.Fatalf("error %v mapped to %+v, want %d/%s", test.err, apiErr, test.wantCode, test.wantName)
		}
	}
}

func TestServerRouteClassificationSeparatesWorkerAndUserCredentials(t *testing.T) {
	server := newTestServer(t, newFakeStore(), 1024)
	tests := []struct {
		method            string
		path              string
		matches           bool
		workerCredentials bool
	}{
		{method: http.MethodPost, path: "/api/v1/workers/register", matches: true, workerCredentials: true},
		{method: http.MethodGet, path: "/api/v1/workers", matches: true, workerCredentials: false},
		{method: http.MethodGet, path: "/api/v1/workers/wrk_test", matches: true, workerCredentials: false},
		{method: http.MethodPost, path: "/api/v1/workers/wrk_test/claims", matches: true, workerCredentials: true},
		{method: http.MethodGet, path: "/api/v1/runs/run-one", matches: false, workerCredentials: false},
	}
	for _, test := range tests {
		request := httptest.NewRequest(test.method, test.path, nil)
		if got := server.Matches(request.URL.Path); got != test.matches {
			t.Errorf("Matches(%s) = %t, want %t", test.path, got, test.matches)
		}
		if got := server.RequiresWorkerCredentials(request.URL.Path); got != test.workerCredentials {
			t.Errorf("RequiresWorkerCredentials(%s) = %t, want %t", test.path, got, test.workerCredentials)
		}
	}
}

func newTestServer(t *testing.T, store Store, maxBodyBytes int64) *Server {
	t.Helper()
	server, err := New(store, Options{
		BootstrapToken:    testBootstrapToken,
		HeartbeatInterval: 5 * time.Second,
		LeaseDuration:     15 * time.Second,
		MaxBodyBytes:      maxBodyBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

type fakeStore struct {
	registration postgres.WorkerRegistration
	summary      workerprotocol.WorkerSummary
	operations   []workerprotocol.WorkerOperation
	claimErr     error
}

func newFakeStore() *fakeStore {
	summary := workerprotocol.WorkerSummary{
		WorkerID: testWorkerID, DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 2,
		Status: workerprotocol.WorkerActive, RegisteredAt: time.Unix(1, 0).UTC(), LastHeartbeatAt: time.Unix(2, 0).UTC(),
	}
	return &fakeStore{
		registration: postgres.WorkerRegistration{Summary: summary, SessionToken: testSessionToken},
		summary:      summary,
	}
}

func (f *fakeStore) RegisterWorker(context.Context, workerprotocol.RegisterRequest) (postgres.WorkerRegistration, error) {
	return f.registration, nil
}

func (f *fakeStore) AuthenticateWorker(_ context.Context, workerID, token string, protocol int, operation workerprotocol.WorkerOperation) (workerprotocol.WorkerSession, error) {
	if workerID != testWorkerID || token != testSessionToken {
		return workerprotocol.WorkerSession{}, postgres.ErrWorkerSessionInvalid
	}
	if protocol != workerprotocol.ProtocolVersion {
		return workerprotocol.WorkerSession{}, postgres.ErrWorkerProtocolUnsupported
	}
	f.operations = append(f.operations, operation)
	return workerprotocol.WorkerSession{WorkerID: workerID, ProtocolVersion: protocol, Status: workerprotocol.WorkerActive}, nil
}

func (f *fakeStore) Claim(context.Context, string, int) ([]workerprotocol.Lease, error) {
	return []workerprotocol.Lease{}, f.claimErr
}

func (f *fakeStore) Heartbeat(context.Context, string, []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error) {
	return workerprotocol.HeartbeatResponse{Leases: []workerprotocol.LeaseHeartbeat{}}, nil
}

func (f *fakeStore) Complete(context.Context, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	return workerprotocol.CompleteResponse{Applied: true}, nil
}

func (f *fakeStore) DrainWorker(context.Context, string) (workerprotocol.WorkerSummary, error) {
	summary := f.summary
	summary.Status = workerprotocol.WorkerDraining
	return summary, nil
}

func (f *fakeStore) GetWorkerSummary(context.Context, string) (workerprotocol.WorkerSummary, error) {
	return f.summary, nil
}

func (f *fakeStore) ListWorkerSummaries(context.Context, string, int) ([]workerprotocol.WorkerSummary, bool, error) {
	return []workerprotocol.WorkerSummary{f.summary}, true, nil
}

func decodeErrorBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(bytes.NewReader(recorder.Body.Bytes())).Decode(&body); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	return body
}
