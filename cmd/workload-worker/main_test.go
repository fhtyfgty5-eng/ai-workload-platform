package main

import (
	"context"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestMockWorkerExecutorReturnsDeterministicResultWithoutInterpretingAction(t *testing.T) {
	executor := newMockExecutor(0)
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{
		DefinitionID: "definition", RunID: "run-one", TaskKey: "task", Action: "file:///do-not-open", Attempt: 1,
	})
	if response.Kind != workflow.ResultSuccess || response.Output != "mock-completed:task" {
		t.Fatalf("response = %+v", response)
	}
}

func TestMockWorkerExecutorStopsOnCancellationDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan workflow.ExecutionResponse, 1)
	go func() { result <- newMockExecutor(time.Minute).Execute(ctx, workflow.ExecutionRequest{}) }()
	cancel()
	select {
	case response := <-result:
		if response.Kind != workflow.ResultCanceled {
			t.Fatalf("response = %+v, want canceled", response)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("mock executor did not stop after cancellation")
	}
}
