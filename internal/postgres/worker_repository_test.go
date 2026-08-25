package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestWorkerRegistrationCreatesDistinctSessionsAndNormalizesCapabilities(t *testing.T) {
	repository := newTestRepository(t)
	request := workerprotocol.RegisterRequest{
		DisplayName:     "  build worker  ",
		ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds:   []workflow.ExecutorKind{workflow.ExecutorMock, workflow.ExecutorMock},
		MaxConcurrency:  4,
	}
	first, err := repository.RegisterWorker(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.RegisterWorker(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Summary.WorkerID == "" || first.SessionToken == "" {
		t.Fatalf("first registration = %+v, want generated ID and token", first)
	}
	if first.Summary.WorkerID == second.Summary.WorkerID || first.SessionToken == second.SessionToken {
		t.Fatal("same display name reused WorkerID or session token")
	}
	if first.Summary.DisplayName != "build worker" || first.Summary.Status != workerprotocol.WorkerActive {
		t.Fatalf("first summary = %+v", first.Summary)
	}
	if !reflect.DeepEqual(first.Summary.ExecutorKinds, []workflow.ExecutorKind{workflow.ExecutorMock}) {
		t.Fatalf("normalized executors = %v", first.Summary.ExecutorKinds)
	}
}

func TestWorkerRegistrationRejectsInvalidInputBeforeInsert(t *testing.T) {
	repository := newTestRepository(t)
	valid := workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	}
	tests := []struct {
		name    string
		mutate  func(*workerprotocol.RegisterRequest)
		wantErr error
	}{
		{name: "empty name", mutate: func(request *workerprotocol.RegisterRequest) { request.DisplayName = " " }, wantErr: ErrInvalidWorkerRegistration},
		{name: "long name", mutate: func(request *workerprotocol.RegisterRequest) { request.DisplayName = strings.Repeat("a", 129) }, wantErr: ErrInvalidWorkerRegistration},
		{name: "unsupported protocol", mutate: func(request *workerprotocol.RegisterRequest) { request.ProtocolVersion++ }, wantErr: ErrWorkerProtocolUnsupported},
		{name: "no executors", mutate: func(request *workerprotocol.RegisterRequest) { request.ExecutorKinds = nil }, wantErr: ErrInvalidWorkerRegistration},
		{name: "unsupported executor", mutate: func(request *workerprotocol.RegisterRequest) {
			request.ExecutorKinds = []workflow.ExecutorKind{"shell"}
		}, wantErr: ErrInvalidWorkerRegistration},
		{name: "zero concurrency", mutate: func(request *workerprotocol.RegisterRequest) { request.MaxConcurrency = 0 }, wantErr: ErrInvalidWorkerRegistration},
		{name: "excess concurrency", mutate: func(request *workerprotocol.RegisterRequest) { request.MaxConcurrency = 1025 }, wantErr: ErrInvalidWorkerRegistration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if _, err := repository.RegisterWorker(context.Background(), request); !errors.Is(err, test.wantErr) {
				t.Fatalf("RegisterWorker() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	var count int
	if err := repository.pool.QueryRow(context.Background(), "SELECT count(*) FROM worker_sessions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("Worker rows after invalid registration = %d, want 0", count)
	}
}

func TestWorkerAuthenticationEnforcesTokenProtocolAndSessionStatus(t *testing.T) {
	repository := newTestRepository(t)
	registration, err := repository.RegisterWorker(context.Background(), workerprotocol.RegisterRequest{
		DisplayName: "worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := repository.AuthenticateWorker(
		context.Background(), registration.Summary.WorkerID, registration.SessionToken,
		workerprotocol.ProtocolVersion, workerprotocol.OperationClaim,
	)
	if err != nil || authenticated.WorkerID != registration.Summary.WorkerID {
		t.Fatalf("AuthenticateWorker() = %+v, %v", authenticated, err)
	}
	for _, test := range []struct {
		name     string
		workerID string
		token    string
		protocol int
		wantErr  error
	}{
		{name: "unknown ID", workerID: "missing", token: registration.SessionToken, protocol: workerprotocol.ProtocolVersion, wantErr: ErrWorkerSessionInvalid},
		{name: "wrong token", workerID: registration.Summary.WorkerID, token: "wrong", protocol: workerprotocol.ProtocolVersion, wantErr: ErrWorkerSessionInvalid},
		{name: "wrong protocol", workerID: registration.Summary.WorkerID, token: registration.SessionToken, protocol: workerprotocol.ProtocolVersion + 1, wantErr: ErrWorkerProtocolUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.AuthenticateWorker(context.Background(), test.workerID, test.token, test.protocol, workerprotocol.OperationHeartbeat); !errors.Is(err, test.wantErr) {
				t.Fatalf("AuthenticateWorker() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	if _, err := repository.pool.Exec(context.Background(), `
		UPDATE worker_sessions SET status = 'draining' WHERE worker_id = $1
	`, registration.Summary.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.AuthenticateWorker(context.Background(), registration.Summary.WorkerID, registration.SessionToken, workerprotocol.ProtocolVersion, workerprotocol.OperationHeartbeat); err != nil {
		t.Fatalf("draining heartbeat authentication: %v", err)
	}
	if _, err := repository.AuthenticateWorker(context.Background(), registration.Summary.WorkerID, registration.SessionToken, workerprotocol.ProtocolVersion, workerprotocol.OperationComplete); err != nil {
		t.Fatalf("draining completion authentication: %v", err)
	}
	if _, err := repository.AuthenticateWorker(context.Background(), registration.Summary.WorkerID, registration.SessionToken, workerprotocol.ProtocolVersion, workerprotocol.OperationClaim); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("draining claim error = %v, want ErrWorkerDraining", err)
	}

	for _, status := range []workerprotocol.WorkerStatus{workerprotocol.WorkerOffline, workerprotocol.WorkerStopped} {
		stoppedAt := "NULL"
		if status == workerprotocol.WorkerStopped {
			stoppedAt = "CURRENT_TIMESTAMP"
		}
		if _, err := repository.pool.Exec(context.Background(), "UPDATE worker_sessions SET status = $2, stopped_at = "+stoppedAt+" WHERE worker_id = $1", registration.Summary.WorkerID, status); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.AuthenticateWorker(context.Background(), registration.Summary.WorkerID, registration.SessionToken, workerprotocol.ProtocolVersion, workerprotocol.OperationHeartbeat); !errors.Is(err, ErrWorkerSessionInvalid) {
			t.Fatalf("status %s authentication error = %v, want ErrWorkerSessionInvalid", status, err)
		}
	}
}

func TestDrainWorkerStopsSessionWithoutActiveLeases(t *testing.T) {
	repository := newTestRepository(t)
	registration, err := repository.RegisterWorker(context.Background(), workerprotocol.RegisterRequest{
		DisplayName: "idle-worker", ProtocolVersion: workerprotocol.ProtocolVersion,
		ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := repository.DrainWorker(context.Background(), registration.Summary.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Status != workerprotocol.WorkerStopped || summary.StoppedAt == nil {
		t.Fatalf("drained idle Worker = %+v, want stopped with stopped_at", summary)
	}
}

func TestDrainWorkerStopsSessionAfterActiveLeaseCompletes(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	worker, lease := createClaimedDispatch(
		t, repository, "drain-workflow", "drain-run",
		workflow.RetryPolicy{MaxAttempts: 1}, 60_000,
	)

	draining, err := repository.DrainWorker(ctx, worker.Summary.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if draining.Status != workerprotocol.WorkerDraining || draining.ActiveLeases != 1 || draining.StoppedAt != nil {
		t.Fatalf("Worker with active lease = %+v, want draining with one lease", draining)
	}
	if _, err := repository.Claim(ctx, worker.Summary.WorkerID, 1); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("draining Claim() error = %v, want ErrWorkerDraining", err)
	}
	if _, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
		LeaseToken: lease.LeaseToken,
		Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "done"},
	}); err != nil {
		t.Fatal(err)
	}

	stopped, err := repository.DrainWorker(ctx, worker.Summary.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Status != workerprotocol.WorkerStopped || stopped.ActiveLeases != 0 || stopped.StoppedAt == nil {
		t.Fatalf("Worker after lease completion = %+v, want stopped without active leases", stopped)
	}
	if _, err := repository.AuthenticateWorker(
		ctx, worker.Summary.WorkerID, worker.SessionToken,
		workerprotocol.ProtocolVersion, workerprotocol.OperationHeartbeat,
	); !errors.Is(err, ErrWorkerSessionInvalid) {
		t.Fatalf("stopped Worker authentication error = %v, want ErrWorkerSessionInvalid", err)
	}
}

func TestWorkerQueriesUseStablePaginationAndOmitSecrets(t *testing.T) {
	repository := newTestRepository(t)
	registrations := make([]WorkerRegistration, 0, 3)
	for _, name := range []string{"one", "two", "three"} {
		registration, err := repository.RegisterWorker(context.Background(), workerprotocol.RegisterRequest{
			DisplayName: name, ProtocolVersion: workerprotocol.ProtocolVersion,
			ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		registrations = append(registrations, registration)
	}
	firstPage, more, err := repository.ListWorkerSummaries(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage) != 2 || !more || firstPage[0].WorkerID >= firstPage[1].WorkerID {
		t.Fatalf("first page = %+v, more=%v", firstPage, more)
	}
	secondPage, more, err := repository.ListWorkerSummaries(context.Background(), firstPage[1].WorkerID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage) != 1 || more || secondPage[0].WorkerID <= firstPage[1].WorkerID {
		t.Fatalf("second page = %+v, more=%v", secondPage, more)
	}
	got, err := repository.GetWorkerSummary(context.Background(), registrations[0].Summary.WorkerID)
	if err != nil || got.WorkerID != registrations[0].Summary.WorkerID {
		t.Fatalf("GetWorkerSummary() = %+v, %v", got, err)
	}
	if _, err := repository.GetWorkerSummary(context.Background(), "missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("missing Worker error = %v, want ErrWorkerNotFound", err)
	}
	for _, registration := range registrations {
		var hash []byte
		if err := repository.pool.QueryRow(context.Background(), "SELECT session_token_hash FROM worker_sessions WHERE worker_id = $1", registration.Summary.WorkerID).Scan(&hash); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(registration.Summary)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), registration.SessionToken) || string(hash) == registration.SessionToken {
			t.Fatal("registration summary or stored digest exposed plaintext token")
		}
	}
}
