package workflow

import (
	"context"
	"encoding/hex"
	"testing"
	"time"
)

func TestEngineExecutesDAGAfterDependenciesSucceed(t *testing.T) {
	store := newMemoryStore()
	executor := newRecordingExecutor(map[string]ExecutionResponse{
		"a": {Kind: ResultSuccess},
		"b": {Kind: ResultSuccess},
		"c": {Kind: ResultSuccess},
	})
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 2, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
		{Key: "c", Action: "c", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != WorkflowSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if executor.startedBefore("b", "a") || executor.startedBefore("c", "a") {
		t.Fatal("downstream task started before dependency succeeded")
	}
}

func TestEnginePassesDefinitionIDToExecutor(t *testing.T) {
	store := newMemoryStore()
	executor := newRecordingExecutor(nil)
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "clean", Action: "clean", TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if got := executor.definitionIDFor("clean"); got != "document-pipeline" {
		t.Fatalf("DefinitionID = %q, want document-pipeline", got)
	}
}

func TestEngineNeverExceedsConcurrencyLimit(t *testing.T) {
	store := newMemoryStore()
	executor := newGateExecutor()
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, independentWorkflow(5, 2))
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.Execute(context.Background(), id)
		done <- err
	}()
	executor.waitForStarted(t, 2)
	if got := executor.maxActive(); got != 2 {
		t.Fatalf("max active = %d, want 2", got)
	}
	executor.releaseAll()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEngineDoesNotStartExecutorWhenRunningStateCannotBeSaved(t *testing.T) {
	// Create 和 WorkflowRunning 保存成功，任务转为 running 的保存失败。
	store := newFailingStore(3)
	executor := newGateExecutor()
	engine := newTestEngine(store, executor)
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
	}})
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(context.Background(), id); err == nil {
		t.Fatal("Execute() error = nil, want save failure")
	}
	select {
	case <-executor.started:
		t.Fatal("executor started before running state was saved")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRandomRunIDUses32HexCharacters(t *testing.T) {
	id, err := randomRunID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 32 {
		t.Fatalf("RunID length = %d, want 32", len(id))
	}
	if _, err := hex.DecodeString(string(id)); err != nil {
		t.Fatalf("RunID is not hexadecimal: %v", err)
	}
}

func TestEngineDoesNotUnlockDependencyTwice(t *testing.T) {
	compiled := mustCompile(t, WorkflowDefinition{ID: "workflow", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1000},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1000},
	}})
	now := time.Unix(100, 0)
	snapshot := newRunSnapshot("run-one", compiled, now)
	if err := transitionWorkflow(&snapshot, WorkflowRunning, now, "test"); err != nil {
		t.Fatal(err)
	}
	if err := transitionTask(&snapshot, 0, TaskRunning, now, "test"); err != nil {
		t.Fatal(err)
	}
	snapshot.Run.Tasks[0].Attempts = append(snapshot.Run.Tasks[0].Attempts, Attempt{Number: 1, Status: AttemptRunning, StartedAt: now})
	completion := executionCompletion{taskIndex: 0, attempt: 1, response: ExecutionResponse{Kind: ResultSuccess}, finished: now}
	if applied, err := applyCompletion(&snapshot, compiled, completion); err != nil || !applied {
		t.Fatalf("first completion = applied:%t err:%v, want applied", applied, err)
	}
	if applied, err := applyCompletion(&snapshot, compiled, completion); err != nil || applied {
		t.Fatalf("duplicate completion = applied:%t err:%v, want ignored", applied, err)
	}
	if got := snapshot.Run.RemainingDependencies[1]; got != 0 {
		t.Fatalf("remaining dependencies = %d, want 0", got)
	}
	if got := snapshot.Run.Tasks[1].Status; got != TaskReady {
		t.Fatalf("successor status = %s, want ready", got)
	}
}
