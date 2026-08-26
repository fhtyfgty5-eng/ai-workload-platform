package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRuntimeLogsLeaseIdentityWithoutActionInputOrToken(t *testing.T) {
	client := newFakeClient()
	lease := testLease("dispatch-log")
	lease.Input = map[string]any{"secret": "private-input"}
	client.claims = [][]workerprotocol.Lease{{lease}}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runtime, err := New(client, immediateExecutor{}, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "log-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        5 * time.Millisecond, PollMax: 20 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		ShutdownTimeout: 20 * time.Millisecond, RetryInterval: 5 * time.Millisecond, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = runtime.Run(ctx)
	got := output.String()
	for _, want := range []string{"run_id=run", "task_key=task", "attempt=1", "worker_id=worker", "dispatch_id=dispatch-log"} {
		if !strings.Contains(got, want) {
			t.Fatalf("lease log = %q, missing %q", got, want)
		}
	}
	for _, forbidden := range []string{"private-input", "session-runtime-secret", "lease-dispatch-log", "action=mock"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("lease log leaked %q: %s", forbidden, got)
		}
	}
}

func TestRuntimeObserveUsesStableWorkerAPIErrorCode(t *testing.T) {
	var output bytes.Buffer
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	runtime := &Runtime{options: Options{
		Logger:  slog.New(slog.NewTextHandler(&output, nil)),
		Metrics: metrics,
	}}
	runtime.observe("complete", time.Now().Add(-time.Millisecond), &workerclient.APIError{Status: 409, Code: "lease_lost"})

	if !strings.Contains(output.String(), "error_code=lease_lost") {
		t.Fatalf("runtime log = %q, want stable error code", output.String())
	}
	text, err := observability.GatherText(metrics)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `error_code="lease_lost"`) {
		t.Fatalf("runtime metrics did not retain stable error code: %s", text)
	}
}

const (
	clientBootstrapToken = "bootstrap-runtime-secret"
	clientSessionToken   = "session-runtime-secret"
)

func TestPollDelayUsesExponentialBackoffAndResetsAfterClaim(t *testing.T) {
	if got := nextPollDelay(0, 100*time.Millisecond, 2*time.Second); got != 100*time.Millisecond {
		t.Fatalf("first delay = %v", got)
	}
	if got := nextPollDelay(100*time.Millisecond, 100*time.Millisecond, 2*time.Second); got != 200*time.Millisecond {
		t.Fatalf("second delay = %v", got)
	}
	if got := nextPollDelay(1500*time.Millisecond, 100*time.Millisecond, 2*time.Second); got != 2*time.Second {
		t.Fatalf("capped delay = %v", got)
	}
	if got := jitterDelay(1*time.Second, 0.2, func() float64 { return 0 }); got != 800*time.Millisecond {
		t.Fatalf("minimum jitter = %v", got)
	}
	if got := jitterDelay(1*time.Second, 0.2, func() float64 { return 1 }); got != 1200*time.Millisecond {
		t.Fatalf("maximum jitter = %v", got)
	}
}

func TestRuntimeCancelsExecutorWhenHeartbeatRevokesLease(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-revoked")}}
	client.heartbeats = []workerprotocol.HeartbeatResponse{{Leases: []workerprotocol.LeaseHeartbeat{{DispatchID: "dispatch-revoked", Status: workerprotocol.LeaseRevoked}}}}
	executor := &blockingExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
	runtime := newTestRuntime(t, client, executor)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := runtime.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case <-executor.canceled:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("executor was not canceled after revoked heartbeat")
	}
}

func TestRuntimeExitsWhenClaimReportsPermanentSessionError(t *testing.T) {
	client := newFakeClient()
	client.claimErrors = []error{&workerclient.APIError{Status: 401, Code: "worker_session_invalid", Message: "session expired"}}
	runtime := newTestRuntime(t, client, immediateExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := runtime.Run(ctx)
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "worker_session_invalid" {
		t.Fatalf("Run() error = %v, want worker_session_invalid", err)
	}
	if client.drainCalls != 0 {
		t.Fatalf("Drain() calls = %d, want 0 for an invalid session", client.drainCalls)
	}
}

func TestRuntimeExitsAndCancelsExecutorWhenHeartbeatReportsPermanentSessionError(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-invalid-session")}}
	client.heartbeatErrors = []error{&workerclient.APIError{Status: 401, Code: "worker_session_invalid", Message: "session expired"}}
	executor := &blockingExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
	runtime := newTestRuntime(t, client, executor)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := runtime.Run(ctx)
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "worker_session_invalid" {
		t.Fatalf("Run() error = %v, want worker_session_invalid", err)
	}
	select {
	case <-executor.canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("executor was not canceled after the Worker session became invalid")
	}
	if client.drainCalls != 0 {
		t.Fatalf("Drain() calls = %d, want 0 for an invalid session", client.drainCalls)
	}
}

func TestRuntimePermanentHeartbeatErrorCancelsInFlightClaim(t *testing.T) {
	client := newFakeClient()
	client.claimBlocks = true
	client.heartbeatErrors = []error{&workerclient.APIError{Status: 401, Code: "worker_session_invalid", Message: "session expired"}}
	runtime := newTestRuntime(t, client, immediateExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := runtime.Run(ctx)
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "worker_session_invalid" {
		t.Fatalf("Run() error = %v, want worker_session_invalid before the parent Context expires", err)
	}
}

func TestRuntimeRetriesSameCompletionAfterTransientNetworkError(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-retry")}}
	client.completeErrors = []error{errors.New("temporary network failure"), nil}
	executor := immediateExecutor{}
	runtime := newTestRuntime(t, client, executor)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = runtime.Run(ctx)
	if len(client.completions) != 2 {
		t.Fatalf("completion calls = %d, want two", len(client.completions))
	}
	if client.completions[0] != client.completions[1] || client.completionTokens[0] != client.completionTokens[1] {
		t.Fatalf("completion request/token changed: %+v / %v", client.completions, client.completionTokens)
	}
}

func TestRuntimeStopsCompletionRetryOnLeaseLost(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-lost")}}
	client.completeErrors = []error{&workerclient.APIError{Status: 409, Code: "lease_lost"}}
	runtime := newTestRuntime(t, client, immediateExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = runtime.Run(ctx)
	if len(client.completions) != 1 {
		t.Fatalf("completion calls = %d, want one after lease_lost", len(client.completions))
	}
}

func TestRuntimeHeartbeatsWhileIdle(t *testing.T) {
	client := newFakeClient()
	runtime := newTestRuntime(t, client, immediateExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_ = runtime.Run(ctx)
	client.mu.Lock()
	calls := client.heartbeatCalls
	requests := append([]workerprotocol.HeartbeatRequest(nil), client.heartbeatRequests...)
	client.mu.Unlock()
	if calls == 0 {
		t.Fatal("idle Worker did not send a heartbeat")
	}
	if len(requests[0].Leases) != 0 {
		t.Fatalf("idle heartbeat leases = %+v, want empty", requests[0].Leases)
	}
}

func TestRuntimeUsesServerHeartbeatInterval(t *testing.T) {
	client := newFakeClient()
	client.registration.HeartbeatIntervalMillis = 5
	runtime, err := New(client, immediateExecutor{}, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "server-timing-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        time.Second, PollMax: time.Second, HeartbeatInterval: time.Second,
		ShutdownTimeout: 20 * time.Millisecond, RetryInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_ = runtime.Run(ctx)
	client.mu.Lock()
	heartbeatCalls := client.heartbeatCalls
	client.mu.Unlock()
	if heartbeatCalls == 0 {
		t.Fatal("Worker ignored the heartbeat interval returned by the server")
	}
}

func TestRuntimeCompletesActiveLeaseDuringGracefulDrain(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-drain")}}
	executor := &releasableExecutor{started: make(chan struct{}), release: make(chan struct{})}
	runtime := newTestRuntime(t, client, executor)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-executor.started:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("executor did not start")
	}
	cancel()
	close(executor.release)
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Runtime did not finish graceful drain")
	}
	client.mu.Lock()
	drainCalls := client.drainCalls
	completionCount := len(client.completions)
	client.mu.Unlock()
	if drainCalls != 2 || completionCount != 1 {
		t.Fatalf("drain/completion calls = %d/%d, want 2/1", drainCalls, completionCount)
	}
}

func TestRuntimeClaimsAgainWhenAnActiveLeaseCompletes(t *testing.T) {
	client := newFakeClient()
	client.claims = [][]workerprotocol.Lease{{testLease("dispatch-capacity")}}
	executor := &releasableExecutor{started: make(chan struct{}), release: make(chan struct{})}
	runtime, err := New(client, executor, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "capacity-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        time.Second, PollMax: time.Second, HeartbeatInterval: time.Second,
		ShutdownTimeout: 50 * time.Millisecond, RetryInterval: 5 * time.Millisecond, Random: func() float64 { return 0.5 },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-executor.started:
	case <-time.After(300 * time.Millisecond):
		cancel()
		t.Fatal("executor did not start")
	}
	// 让轮询循环观察到唯一槽位已占用并进入等待，随后才能验证容量通知会唤醒它。
	time.Sleep(20 * time.Millisecond)
	close(executor.release)
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		claimCalls := client.claimCalls
		client.mu.Unlock()
		if claimCalls >= 2 {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	client.mu.Lock()
	claimCalls := client.claimCalls
	client.mu.Unlock()
	t.Fatalf("claim calls after lease completion = %d, want at least 2 before poll timeout", claimCalls)
}

func TestRuntimeCancelsLeaseAtLocalSafetyDeadlineWhenHeartbeatFails(t *testing.T) {
	client := newFakeClient()
	lease := testLease("dispatch-safety")
	lease.LeaseExpiresAt = time.Now().Add(40 * time.Millisecond)
	lease.AttemptDeadline = time.Now().Add(time.Second)
	client.claims = [][]workerprotocol.Lease{{lease}}
	client.heartbeatErrors = []error{errors.New("database unavailable")}
	executor := &blockingExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
	runtime, err := New(client, executor, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "safety-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        5 * time.Millisecond, PollMax: 20 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		ShutdownTimeout: 20 * time.Millisecond, RetryInterval: 5 * time.Millisecond, SafetyMargin: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _ = runtime.Run(ctx) }()
	select {
	case <-executor.canceled:
	case <-time.After(120 * time.Millisecond):
		t.Fatal("executor was not canceled at the local safety deadline")
	}
}

func TestRuntimeExtendsLocalSafetyDeadlineAfterLeaseRenewal(t *testing.T) {
	client := newFakeClient()
	lease := testLease("dispatch-renewed-safety")
	lease.LeaseExpiresAt = time.Now().Add(40 * time.Millisecond)
	lease.AttemptDeadline = time.Now().Add(250 * time.Millisecond)
	client.claims = [][]workerprotocol.Lease{{lease}}
	client.registration.HeartbeatIntervalMillis = 5
	client.heartbeats = []workerprotocol.HeartbeatResponse{{Leases: []workerprotocol.LeaseHeartbeat{{
		DispatchID: lease.DispatchID, Status: workerprotocol.LeaseRenewed, LeaseRemainingMillis: 100,
	}}}}
	executor := &blockingExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
	runtime, err := New(client, executor, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "renewal-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        time.Second, PollMax: time.Second, HeartbeatInterval: time.Second,
		ShutdownTimeout: 20 * time.Millisecond, RetryInterval: 5 * time.Millisecond, SafetyMargin: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _ = runtime.Run(ctx) }()
	select {
	case <-executor.canceled:
		t.Fatal("executor was canceled at the original lease expiry despite a renewal")
	case <-time.After(60 * time.Millisecond):
	}
	select {
	case <-executor.canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("executor was not canceled when the renewed local safety deadline elapsed")
	}
}

func newTestRuntime(t *testing.T, client *fakeClient, executor workflow.Executor) *Runtime {
	t.Helper()
	runtime, err := New(client, executor, Options{
		BootstrapToken: clientBootstrapToken,
		Registration:   workerprotocol.RegisterRequest{DisplayName: "test-worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1},
		PollMin:        5 * time.Millisecond, PollMax: 20 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		ShutdownTimeout: 20 * time.Millisecond, RetryInterval: 5 * time.Millisecond, Random: func() float64 { return 0.5 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func testLease(dispatchID string) workerprotocol.Lease {
	deadline := time.Now().Add(time.Second)
	return workerprotocol.Lease{DispatchID: dispatchID, LeaseToken: "lease-" + dispatchID, DefinitionID: "definition", RunID: "run", TaskKey: "task", ExecutorKind: workflow.ExecutorMock, Action: "mock", Attempt: 1, AttemptDeadline: deadline, LeaseExpiresAt: deadline}
}

type fakeClient struct {
	mu                sync.Mutex
	registration      workerprotocol.RegisterResponse
	claims            [][]workerprotocol.Lease
	claimErrors       []error
	claimBlocks       bool
	claimCalls        int
	heartbeats        []workerprotocol.HeartbeatResponse
	heartbeatCalls    int
	heartbeatRequests []workerprotocol.HeartbeatRequest
	heartbeatErrors   []error
	completions       []workerprotocol.CompleteRequest
	completionTokens  []string
	completeErrors    []error
	drainCalls        int
}

func newFakeClient() *fakeClient {
	return &fakeClient{registration: workerprotocol.RegisterResponse{WorkerID: "worker", SessionToken: clientSessionToken, ProtocolVersion: 1}}
}

func (f *fakeClient) Register(context.Context, string, workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error) {
	return f.registration, nil
}
func (f *fakeClient) Claim(ctx context.Context, _ string, _ string, _ int) (workerprotocol.ClaimResponse, error) {
	f.mu.Lock()
	index := f.claimCalls
	f.claimCalls++
	blocks := f.claimBlocks
	if index < len(f.claimErrors) && f.claimErrors[index] != nil {
		err := f.claimErrors[index]
		f.mu.Unlock()
		return workerprotocol.ClaimResponse{}, err
	}
	if index >= len(f.claims) {
		f.mu.Unlock()
		if blocks {
			<-ctx.Done()
			return workerprotocol.ClaimResponse{}, ctx.Err()
		}
		return workerprotocol.ClaimResponse{}, nil
	}
	leases := f.claims[index]
	f.mu.Unlock()
	return workerprotocol.ClaimResponse{Leases: leases}, nil
}
func (f *fakeClient) Heartbeat(_ context.Context, _ string, _ string, request workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeatRequests = append(f.heartbeatRequests, request)
	index := f.heartbeatCalls
	f.heartbeatCalls++
	if index < len(f.heartbeatErrors) && f.heartbeatErrors[index] != nil {
		return workerprotocol.HeartbeatResponse{}, f.heartbeatErrors[index]
	}
	if index >= len(f.heartbeats) {
		return workerprotocol.HeartbeatResponse{}, nil
	}
	return f.heartbeats[index], nil
}
func (f *fakeClient) Complete(_ context.Context, _ string, _ string, token string, request workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completions = append(f.completions, request)
	f.completionTokens = append(f.completionTokens, token)
	index := len(f.completions) - 1
	if index < len(f.completeErrors) && f.completeErrors[index] != nil {
		return workerprotocol.CompleteResponse{}, f.completeErrors[index]
	}
	return workerprotocol.CompleteResponse{Applied: true}, nil
}
func (f *fakeClient) Drain(context.Context, string, string) (workerprotocol.WorkerSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drainCalls++
	return workerprotocol.WorkerSummary{Status: workerprotocol.WorkerDraining}, nil
}

type immediateExecutor struct{}

func (immediateExecutor) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "done"}
}

type blockingExecutor struct {
	started  chan struct{}
	canceled chan struct{}
}

type releasableExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *releasableExecutor) Execute(ctx context.Context, _ workflow.ExecutionRequest) workflow.ExecutionResponse {
	close(e.started)
	select {
	case <-e.release:
		return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "drained"}
	case <-ctx.Done():
		return workflow.ExecutionResponse{Kind: workflow.ResultCanceled}
	}
}

func (e *blockingExecutor) Execute(ctx context.Context, _ workflow.ExecutionRequest) workflow.ExecutionResponse {
	select {
	case <-e.started:
	default:
		close(e.started)
	}
	<-ctx.Done()
	select {
	case <-e.canceled:
	default:
		close(e.canceled)
	}
	return workflow.ExecutionResponse{Kind: workflow.ResultCanceled}
}
