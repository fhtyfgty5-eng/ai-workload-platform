package faultinject

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type wrapperClientStub struct {
	claimCalls     int
	heartbeatCalls int
	completeCalls  int
}

func (s *wrapperClientStub) Register(context.Context, string, workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error) {
	return workerprotocol.RegisterResponse{WorkerID: "worker", SessionToken: "session"}, nil
}
func (s *wrapperClientStub) Claim(context.Context, string, string, int) (workerprotocol.ClaimResponse, error) {
	s.claimCalls++
	return workerprotocol.ClaimResponse{}, nil
}
func (s *wrapperClientStub) Heartbeat(context.Context, string, string, workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error) {
	s.heartbeatCalls++
	return workerprotocol.HeartbeatResponse{}, nil
}
func (s *wrapperClientStub) Complete(context.Context, string, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	s.completeCalls++
	return workerprotocol.CompleteResponse{Applied: true}, nil
}
func (s *wrapperClientStub) Drain(context.Context, string, string) (workerprotocol.WorkerSummary, error) {
	return workerprotocol.WorkerSummary{}, nil
}

func TestClientWrapperInjectsOnlyConfiguredOperation(t *testing.T) {
	stub := &wrapperClientStub{}
	injected := errors.New("claim failed")
	plan, err := NewPlan(map[Operation][]Action{OperationClaim: {{Err: injected}}})
	if err != nil {
		t.Fatal(err)
	}
	client := NewClientWrapper(stub, plan)
	if _, err := client.Claim(context.Background(), "worker", "session", 1); !errors.Is(err, injected) {
		t.Fatalf("Claim() error = %v, want injected error", err)
	}
	if _, err := client.Heartbeat(context.Background(), "worker", "session", workerprotocol.HeartbeatRequest{}); err != nil {
		t.Fatalf("Heartbeat() error = %v, want passthrough", err)
	}
	if stub.claimCalls != 0 || stub.heartbeatCalls != 1 {
		t.Fatalf("underlying calls = claim:%d heartbeat:%d", stub.claimCalls, stub.heartbeatCalls)
	}
}

type executorStub struct{}

func (executorStub) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "ok"}
}

func TestExecutorWrapperReturnsInjectedCancellation(t *testing.T) {
	plan, err := NewPlan(map[Operation][]Action{OperationWorkerExecute: {{Cancel: true}}})
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutorWrapper(executorStub{}, plan)
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{})
	if response.Kind != workflow.ResultCanceled || response.ErrorCode != "fault_injected" {
		t.Fatalf("response = %+v, want injected cancellation", response)
	}
}

func TestClockWrapperPreservesClockContract(t *testing.T) {
	clock := NewClockWrapper(workflow.RealClock{}, nil)
	started := clock.Now()
	select {
	case <-clock.After(time.Millisecond):
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wrapped clock did not return timer")
	}
	if clock.Now().Before(started) {
		t.Fatal("wrapped clock moved backwards")
	}
}
