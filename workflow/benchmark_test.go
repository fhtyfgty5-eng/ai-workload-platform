package workflow

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCompileTenThousandTasks(b *testing.B) {
	definition := linearWorkflow(10_000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Compile(definition); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompileRejectsTenThousandAndOneTasks(b *testing.B) {
	definition := linearWorkflow(10_001)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Compile(definition); err == nil {
			b.Fatal("Compile accepted more than 10000 tasks")
		}
	}
}

func BenchmarkCreateOneThousandRunsSharedDefinition(b *testing.B) {
	compiled, err := Compile(linearWorkflow(100))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		runs := make([]RunSnapshot, 1_000)
		for i := range runs {
			runs[i] = newRunSnapshot(RunID(strconv.Itoa(i)), compiled, time.Unix(1, 0))
			if runs[i].Definition != compiled.definition {
				b.Fatal("run did not retain shared compiled definition")
			}
		}
		runtime.KeepAlive(runs)
	}
}

func BenchmarkCreateOneThousandRunsSeparateDefinitions(b *testing.B) {
	definition := linearWorkflow(100)
	b.ReportAllocs()
	for b.Loop() {
		runs := make([]RunSnapshot, 1_000)
		seen := make(map[*WorkflowDefinition]struct{}, 1_000)
		for i := range runs {
			compiled, err := Compile(definition)
			if err != nil {
				b.Fatal(err)
			}
			runs[i] = newRunSnapshot(RunID(strconv.Itoa(i)), compiled, time.Unix(1, 0))
			seen[runs[i].Definition] = struct{}{}
		}
		if len(seen) != 1_000 {
			b.Fatalf("definition pointers = %d, want 1000", len(seen))
		}
		runtime.KeepAlive(runs)
	}
}

func BenchmarkEngineCompiledCacheHit(b *testing.B) {
	compiled, err := Compile(linearWorkflow(100))
	if err != nil {
		b.Fatal(err)
	}
	store := newMemoryStore()
	engine, err := NewEngine(store, benchmarkExecutor{}, EngineOptions{
		NewRunID: func() (RunID, error) { return "benchmark-run", nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := store.Load(context.Background(), id)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		got, err := engine.compiledForRun(id, snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if got != compiled {
			b.Fatal("compiled cache did not return the original pointer")
		}
	}
}

func BenchmarkDependencyUnlock(b *testing.B) {
	compiled, err := Compile(fanOutWorkflow(10_000))
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(1, 0)
	base := newRunSnapshot("benchmark-run", compiled, now)
	base.Run.Status = WorkflowRunning
	base.Run.StartedAt = &now
	base.Run.Tasks[0].Status = TaskRunning
	base.Run.Tasks[0].Attempts = []Attempt{{Number: 1, Status: AttemptRunning, StartedAt: now}}
	completion := executionCompletion{
		taskIndex: 0,
		attempt:   1,
		response:  ExecutionResponse{Kind: ResultSuccess},
		finished:  now.Add(time.Second),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// 快照复制属于调用方的写前复制成本，本基准只测一次成功事件沿 9,999 条边解锁下游。
		b.StopTimer()
		snapshot := cloneRunSnapshot(base)
		b.StartTimer()
		applied, err := applyCompletion(&snapshot, compiled, completion)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if !applied || snapshot.Run.Tasks[len(snapshot.Run.Tasks)-1].Status != TaskReady {
			b.Fatal("completion did not unlock all direct successors")
		}
		runtime.KeepAlive(snapshot)
		b.StartTimer()
	}
}

func BenchmarkResumeTenThousandTasks(b *testing.B) {
	compiled, err := Compile(linearWorkflow(10_000))
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(1, 0)
	waiting := newRunSnapshot("benchmark-run", compiled, now)
	waiting.Run.Status = WorkflowRunning
	waiting.Run.StartedAt = &now

	failed := cloneRunSnapshot(waiting)
	failedAt := now.Add(time.Second)
	failed.Run.Tasks[0] = TaskRun{
		Key:        failed.Run.Tasks[0].Key,
		Status:     TaskFailed,
		Attempts:   []Attempt{{Number: 1, Status: AttemptFailed, StartedAt: now, FinishedAt: &failedAt}},
		FinishedAt: &failedAt,
	}

	b.Run("WaitingDependencies", func(b *testing.B) {
		benchmarkPrepareForResume(b, waiting, compiled, now)
	})
	b.Run("FailedAncestor", func(b *testing.B) {
		benchmarkPrepareForResume(b, failed, compiled, now)
	})
}

func benchmarkPrepareForResume(b *testing.B, base RunSnapshot, compiled *CompiledWorkflow, now time.Time) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// 每次从相同持久化事实开始；复制不计入恢复预处理本身。
		b.StopTimer()
		snapshot := cloneRunSnapshot(base)
		b.StartTimer()
		err := prepareForResume(&snapshot, compiled, now)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(snapshot)
		b.StartTimer()
	}
}

func linearWorkflow(taskCount int) WorkflowDefinition {
	definition := WorkflowDefinition{
		ID:          fmt.Sprintf("linear-%d", taskCount),
		Concurrency: 64,
		Tasks:       make([]TaskDefinition, taskCount),
	}
	for i := range definition.Tasks {
		definition.Tasks[i] = TaskDefinition{
			Key:           TaskKey(fmt.Sprintf("task-%d", i)),
			Action:        "noop",
			TimeoutMillis: 1_000,
		}
		if i > 0 {
			definition.Tasks[i].DependsOn = []TaskKey{TaskKey(fmt.Sprintf("task-%d", i-1))}
		}
	}
	return definition
}

func fanOutWorkflow(taskCount int) WorkflowDefinition {
	definition := WorkflowDefinition{
		ID:          fmt.Sprintf("fan-out-%d", taskCount),
		Concurrency: 64,
		Tasks:       make([]TaskDefinition, taskCount),
	}
	for i := range definition.Tasks {
		definition.Tasks[i] = TaskDefinition{
			Key:           TaskKey(fmt.Sprintf("task-%d", i)),
			Action:        "noop",
			TimeoutMillis: 1_000,
		}
		if i > 0 {
			definition.Tasks[i].DependsOn = []TaskKey{"task-0"}
		}
	}
	return definition
}

type benchmarkExecutor struct{}

func (benchmarkExecutor) Execute(context.Context, ExecutionRequest) ExecutionResponse {
	return ExecutionResponse{Kind: ResultSuccess}
}
