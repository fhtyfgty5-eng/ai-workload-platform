package filestore

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func BenchmarkFileStoreSnapshot100Tasks(b *testing.B) {
	benchmarkFileStoreSnapshot(b, 100)
}

func BenchmarkFileStoreSnapshot10000Tasks(b *testing.B) {
	benchmarkFileStoreSnapshot(b, 10_000)
}

func benchmarkFileStoreSnapshot(b *testing.B, taskCount int) {
	b.Helper()
	ctx := context.Background()
	store, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	compiled, err := workflow.Compile(fileStoreBenchmarkWorkflow(taskCount))
	if err != nil {
		b.Fatal(err)
	}
	engine, err := workflow.NewEngine(store, fileStoreBenchmarkExecutor{}, workflow.EngineOptions{
		NewRunID: func() (workflow.RunID, error) { return "benchmark-run", nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	id, err := engine.CreateRun(ctx, compiled)
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := store.Load(ctx, id)
	if err != nil {
		b.Fatal(err)
	}
	// 使用与任务数相同的事件数，避免只测几乎为空的初始快照。
	snapshot.Events = make([]workflow.StateEvent, taskCount)
	for i := range snapshot.Events {
		snapshot.Events[i] = workflow.StateEvent{
			Sequence: uint64(i + 1),
			At:       time.Unix(1, int64(i)),
			Entity:   "task",
			Key:      fmt.Sprintf("task-%d", i),
			From:     "ready",
			To:       "running",
			Reason:   "benchmark event",
		}
	}
	if err := store.Save(ctx, snapshot); err != nil {
		b.Fatal(err)
	}

	b.Run("Save", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := store.Save(ctx, snapshot); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Load", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			loaded, err := store.Load(ctx, id)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(loaded)
		}
	})
}

func fileStoreBenchmarkWorkflow(taskCount int) workflow.WorkflowDefinition {
	definition := workflow.WorkflowDefinition{
		ID:          fmt.Sprintf("snapshot-%d", taskCount),
		Concurrency: 64,
		Tasks:       make([]workflow.TaskDefinition, taskCount),
	}
	for i := range definition.Tasks {
		definition.Tasks[i] = workflow.TaskDefinition{
			Key:           workflow.TaskKey(fmt.Sprintf("task-%d", i)),
			Action:        "noop",
			TimeoutMillis: 1_000,
		}
		if i > 0 {
			definition.Tasks[i].DependsOn = []workflow.TaskKey{workflow.TaskKey(fmt.Sprintf("task-%d", i-1))}
		}
	}
	return definition
}

type fileStoreBenchmarkExecutor struct{}

func (fileStoreBenchmarkExecutor) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess}
}
