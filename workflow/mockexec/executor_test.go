package mockexec

import (
	"context"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestExecutorConsumesScriptInOrder(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{
		{DefinitionID: "cleanup", TaskKey: "clean"}: {
			{Kind: workflow.ResultTemporaryFailure, ErrorCode: "temporary"},
			{Kind: workflow.ResultSuccess, Output: "cleaned"},
		},
	})
	request := workflow.ExecutionRequest{DefinitionID: "cleanup", RunID: "run-one", TaskKey: "clean", Action: "mock-clean", Attempt: 1}
	first := executor.Execute(context.Background(), request)
	request.Attempt = 2
	second := executor.Execute(context.Background(), request)
	if first.Kind != workflow.ResultTemporaryFailure || second.Kind != workflow.ResultSuccess {
		t.Fatalf("results = %s, %s", first.Kind, second.Kind)
	}
}

func TestExecutorReturnsCanceledWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{{DefinitionID: "cleanup", TaskKey: "clean"}: {{WaitForCancellation: true}}})
	result := executor.Execute(ctx, workflow.ExecutionRequest{DefinitionID: "cleanup", TaskKey: "clean", Action: "mock-clean"})
	if result.Kind != workflow.ResultCanceled {
		t.Fatalf("kind = %s, want canceled", result.Kind)
	}
}

func TestExecutorReturnsPermanentFailureWhenScriptIsExhausted(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{{DefinitionID: "cleanup", TaskKey: "clean"}: {}})
	result := executor.Execute(context.Background(), workflow.ExecutionRequest{DefinitionID: "cleanup", TaskKey: "clean", Action: "mock-clean"})
	if result.Kind != workflow.ResultPermanentFailure || result.ErrorCode != "script_exhausted" {
		t.Fatalf("result = %+v, want script_exhausted permanent failure", result)
	}
}

func TestExecutorSeparatesSameActionByTaskKey(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{
		{DefinitionID: "cleanup", TaskKey: "first"}: {
			{Kind: workflow.ResultTemporaryFailure},
			{Kind: workflow.ResultSuccess},
		},
		{DefinitionID: "cleanup", TaskKey: "second"}: {{Kind: workflow.ResultPermanentFailure}},
	})
	first := workflow.ExecutionRequest{DefinitionID: "cleanup", RunID: "run-one", TaskKey: "first", Action: "mock-clean", Attempt: 1}
	second := workflow.ExecutionRequest{DefinitionID: "cleanup", RunID: "run-one", TaskKey: "second", Action: "mock-clean", Attempt: 1}
	if got := executor.Execute(context.Background(), first).Kind; got != workflow.ResultTemporaryFailure {
		t.Fatalf("first task result = %s, want temporary failure", got)
	}
	if got := executor.Execute(context.Background(), second).Kind; got != workflow.ResultPermanentFailure {
		t.Fatalf("second task result = %s, want permanent failure", got)
	}
	first.Attempt = 2
	if got := executor.Execute(context.Background(), first).Kind; got != workflow.ResultSuccess {
		t.Fatalf("first task retry result = %s, want success", got)
	}
}

func TestExecutorSeparatesSameTaskKeyByRunID(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{
		{DefinitionID: "cleanup", TaskKey: "clean"}: {
			{Kind: workflow.ResultTemporaryFailure},
			{Kind: workflow.ResultSuccess},
		},
	})
	firstRun := workflow.ExecutionRequest{DefinitionID: "cleanup", RunID: "run-one", TaskKey: "clean", Action: "mock-clean", Attempt: 1}
	secondRun := workflow.ExecutionRequest{DefinitionID: "cleanup", RunID: "run-two", TaskKey: "clean", Action: "mock-clean", Attempt: 1}
	if got := executor.Execute(context.Background(), firstRun).Kind; got != workflow.ResultTemporaryFailure {
		t.Fatalf("first run result = %s, want temporary failure", got)
	}
	if got := executor.Execute(context.Background(), secondRun).Kind; got != workflow.ResultTemporaryFailure {
		t.Fatalf("second run result = %s, want temporary failure", got)
	}
	firstRun.Attempt = 2
	if got := executor.Execute(context.Background(), firstRun).Kind; got != workflow.ResultSuccess {
		t.Fatalf("first run retry result = %s, want success", got)
	}
}

func TestExecutorSeparatesSameTaskKeyByDefinitionID(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{
		{DefinitionID: "daily-cleanup", TaskKey: "clean"}:  {{Kind: workflow.ResultSuccess, Output: "daily"}},
		{DefinitionID: "weekly-cleanup", TaskKey: "clean"}: {{Kind: workflow.ResultPermanentFailure, ErrorCode: "weekly-failed"}},
	})
	daily := workflow.ExecutionRequest{DefinitionID: "daily-cleanup", RunID: "daily-run", TaskKey: "clean", Action: "cleanup", Attempt: 1}
	weekly := workflow.ExecutionRequest{DefinitionID: "weekly-cleanup", RunID: "weekly-run", TaskKey: "clean", Action: "cleanup", Attempt: 1}
	if got := executor.Execute(context.Background(), daily); got.Kind != workflow.ResultSuccess || got.Output != "daily" {
		t.Fatalf("daily result = %+v, want daily success", got)
	}
	if got := executor.Execute(context.Background(), weekly); got.Kind != workflow.ResultPermanentFailure || got.ErrorCode != "weekly-failed" {
		t.Fatalf("weekly result = %+v, want weekly permanent failure", got)
	}
}

func TestExecutorRejectsEmptyDefinitionID(t *testing.T) {
	executor := New(workflow.RealClock{}, map[ScriptKey][]Step{
		{DefinitionID: "cleanup", TaskKey: "clean"}: {{Kind: workflow.ResultSuccess}},
	})
	result := executor.Execute(context.Background(), workflow.ExecutionRequest{RunID: "run-one", TaskKey: "clean", Action: "cleanup", Attempt: 1})
	if result.Kind != workflow.ResultPermanentFailure || result.ErrorCode != "invalid_execution_request" {
		t.Fatalf("result = %+v, want invalid_execution_request permanent failure", result)
	}
}
