package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestRepositoryMigratesAndCreatesVersion(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	definition := testDefinition()

	created, err := repository.CreateDefinition(ctx, definition, "operator", "create-workflow", "hash-v1")
	if err != nil {
		t.Fatalf("CreateDefinition() error = %v", err)
	}
	if created.WorkflowID != definition.ID || created.Version != 1 {
		t.Fatalf("CreateDefinition() = %+v, want %s@1", created, definition.ID)
	}

	definition.Tasks[0].Action = "clean-v2"
	updated, err := repository.CreateVersion(ctx, definition.ID, definition, "operator", "create-v2", "hash-v2")
	if err != nil {
		t.Fatalf("CreateVersion() error = %v", err)
	}
	if updated.WorkflowID != definition.ID || updated.Version != 2 {
		t.Fatalf("CreateVersion() = %+v, want %s@2", updated, definition.ID)
	}

	loaded, err := repository.LoadDefinition(ctx, definition.ID, 2)
	if err != nil {
		t.Fatalf("LoadDefinition() error = %v", err)
	}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	wantDefinition := compiled.Definition()
	if !reflect.DeepEqual(loaded, wantDefinition) {
		t.Fatalf("LoadDefinition() = %+v, want %+v", loaded, wantDefinition)
	}
}

func TestRepositoryPreservesLargeTaskInputNumber(t *testing.T) {
	repository := newTestRepository(t)
	definition := testDefinition()
	definition.ID = "large-number"
	definition.Tasks[0].Input = map[string]any{"value": json.Number("9007199254740993")}
	if _, err := repository.CreateDefinition(context.Background(), definition, "operator", "large-number", "hash"); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.LoadDefinition(context.Background(), definition.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := loaded.Tasks[0].Input["value"].(json.Number)
	if !ok || value.String() != "9007199254740993" {
		t.Fatalf("input value = %#v, want exact json.Number", loaded.Tasks[0].Input["value"])
	}
}

func TestRepositoryResumePreservesLargeTaskInputNumber(t *testing.T) {
	repository := newTestRepository(t)
	definition := testDefinition()
	definition.ID = "resume-large-number"
	definition.Tasks = definition.Tasks[:1]
	definition.Concurrency = 1
	definition.Tasks[0].Input = map[string]any{"value": json.Number("9007199254740993")}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition = compiled.Definition()
	if _, err := repository.CreateDefinition(context.Background(), definition, "operator", "resume-large-number", "hash"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := workflow.NewRunSnapshotForVersion("run-large-number", compiled, 1, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	executor := &inputCapturingExecutor{requests: make(chan workflow.ExecutionRequest, 1)}
	engine, err := workflow.NewEngine(repository, executor, workflow.EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(context.Background(), snapshot.Run.ID); err != nil {
		t.Fatal(err)
	}
	request := <-executor.requests
	value, ok := request.Input["value"].(json.Number)
	if !ok || value.String() != "9007199254740993" {
		t.Fatalf("execution input = %#v, want exact json.Number", request.Input["value"])
	}
}

type inputCapturingExecutor struct {
	requests chan workflow.ExecutionRequest
}

func (e *inputCapturingExecutor) Execute(_ context.Context, request workflow.ExecutionRequest) workflow.ExecutionResponse {
	e.requests <- request
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess}
}

func TestRepositoryApplyRejectsStaleRevision(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	startedAt := time.Unix(20, 0).UTC()
	run := snapshot.Run
	run.Status = workflow.WorkflowRunning
	run.Revision = 1
	run.StartedAt = &startedAt
	run.Tasks = nil
	run.RemainingDependencies = nil
	change := workflow.ChangeSet{
		RunID:            snapshot.Run.ID,
		ExpectedRevision: 0,
		Run:              &run,
		Events: []workflow.StateEvent{{
			Sequence: 1,
			At:       startedAt,
			Entity:   "workflow",
			Key:      string(snapshot.Run.ID),
			From:     string(workflow.WorkflowPending),
			To:       string(workflow.WorkflowRunning),
			Reason:   "execute",
		}},
	}
	change.Run.LastEventSequence = 1

	if err := repository.Apply(ctx, change); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if err := repository.Apply(ctx, change); !errors.Is(err, workflow.ErrRevisionConflict) {
		t.Fatalf("stale Apply() error = %v, want ErrRevisionConflict", err)
	}
	loaded, err := repository.Load(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Run.Revision != 1 || loaded.Run.Status != workflow.WorkflowRunning {
		t.Fatalf("loaded Run = %+v, want running revision 1", loaded.Run)
	}
}

func TestRepositoryRebuildsSnapshot(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	startedAt := time.Unix(20, 0).UTC()
	run := snapshot.Run
	run.Status = workflow.WorkflowRunning
	run.Revision = 1
	run.LastEventSequence = 3
	run.StartedAt = &startedAt
	run.Tasks = nil
	run.RemainingDependencies = nil
	task := snapshot.Run.Tasks[0]
	task.Status = workflow.TaskRunning
	attempt := workflow.Attempt{Number: 1, Status: workflow.AttemptRunning, StartedAt: startedAt}
	change := workflow.ChangeSet{
		RunID:            snapshot.Run.ID,
		ExpectedRevision: 0,
		Run:              &run,
		Tasks: []workflow.TaskRunChange{{
			RunID:                 snapshot.Run.ID,
			Index:                 0,
			Task:                  task,
			RemainingDependencies: 0,
		}},
		Attempts: []workflow.AttemptChange{{
			RunID:     snapshot.Run.ID,
			TaskKey:   task.Key,
			TaskIndex: 0,
			Attempt:   attempt,
			Operation: workflow.ChangeInsert,
		}},
		Events: []workflow.StateEvent{
			{Sequence: 1, At: startedAt, Entity: "workflow", Key: string(snapshot.Run.ID), From: string(workflow.WorkflowPending), To: string(workflow.WorkflowRunning), Reason: "execute"},
			{Sequence: 2, At: startedAt, Entity: "task", Key: string(task.Key), From: string(workflow.TaskReady), To: string(workflow.TaskRunning), Reason: "schedule"},
			{Sequence: 3, At: startedAt, Entity: "attempt", Key: string(task.Key) + "/1", From: "", To: string(workflow.AttemptRunning), Reason: "start"},
		},
	}
	if err := repository.Apply(ctx, change); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	loaded, err := repository.Load(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Run.DefinitionVersion != 1 || loaded.Run.Revision != 1 || loaded.Run.LastEventSequence != 3 {
		t.Fatalf("loaded Run = %+v", loaded.Run)
	}
	if len(loaded.Run.Tasks) != 2 || loaded.Run.Tasks[0].Status != workflow.TaskRunning {
		t.Fatalf("loaded Tasks = %+v", loaded.Run.Tasks)
	}
	if len(loaded.Run.Tasks[0].Attempts) != 1 || loaded.Run.Tasks[0].Attempts[0].Status != workflow.AttemptRunning {
		t.Fatalf("loaded Attempts = %+v", loaded.Run.Tasks[0].Attempts)
	}
	if !reflect.DeepEqual(loaded.Run.RemainingDependencies, []int{0, 1}) {
		t.Fatalf("loaded RemainingDependencies = %v, want [0 1]", loaded.Run.RemainingDependencies)
	}
	if len(loaded.Events) != 3 || loaded.Events[2].Sequence != 3 {
		t.Fatalf("loaded Events = %+v", loaded.Events)
	}
	compiled, err := workflow.Compile(*snapshot.Definition)
	if err != nil {
		t.Fatal(err)
	}
	wantDefinition := compiled.Definition()
	if loaded.Definition == nil || !reflect.DeepEqual(*loaded.Definition, wantDefinition) {
		t.Fatalf("loaded Definition = %+v, want %+v", loaded.Definition, wantDefinition)
	}
}

func TestRepositoryIdempotencyConflict(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	definition := testDefinition()

	first, err := repository.CreateDefinition(ctx, definition, "operator", "same-key", "same-hash")
	if err != nil {
		t.Fatalf("first CreateDefinition() error = %v", err)
	}
	replayed, err := repository.CreateDefinition(ctx, definition, "operator", "same-key", "same-hash")
	if err != nil {
		t.Fatalf("replayed CreateDefinition() error = %v", err)
	}
	if replayed != first {
		t.Fatalf("replayed ref = %+v, want %+v", replayed, first)
	}
	if _, err := repository.CreateDefinition(ctx, definition, "operator", "same-key", "different-hash"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting CreateDefinition() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestRepositoryCreatesRunWithIdempotencyAndRecoversCancellation(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)

	id, created, err := repository.CreateRun(ctx, snapshot, "operator", "start-run", "start-hash")
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if id != snapshot.Run.ID {
		t.Fatalf("CreateRun() = %q, want %q", id, snapshot.Run.ID)
	}
	if !created {
		t.Fatal("first CreateRun() created = false, want true")
	}
	replayed, created, err := repository.CreateRun(ctx, snapshot, "operator", "start-run", "start-hash")
	if err != nil {
		t.Fatalf("replayed CreateRun() error = %v", err)
	}
	if replayed != id {
		t.Fatalf("replayed CreateRun() = %q, want %q", replayed, id)
	}
	if created {
		t.Fatal("replayed CreateRun() created = true, want false")
	}
	if _, _, err := repository.CreateRun(ctx, snapshot, "operator", "start-run", "different-hash"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting CreateRun() error = %v, want ErrIdempotencyConflict", err)
	}

	ids, err := repository.ListNonTerminal(ctx)
	if err != nil {
		t.Fatalf("ListNonTerminal() error = %v", err)
	}
	if !reflect.DeepEqual(ids, []workflow.RunID{id}) {
		t.Fatalf("ListNonTerminal() = %v, want [%s]", ids, id)
	}

	requestedAt := time.Unix(30, 0).UTC()
	first, err := repository.RequestCancel(ctx, id, requestedAt)
	if err != nil {
		t.Fatalf("RequestCancel() error = %v", err)
	}
	second, err := repository.RequestCancel(ctx, id, requestedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed RequestCancel() error = %v", err)
	}
	if first.CancelRequestedAt == nil || !first.CancelRequestedAt.Equal(requestedAt) {
		t.Fatalf("first CancelRequestedAt = %v, want %v", first.CancelRequestedAt, requestedAt)
	}
	if second.CancelRequestedAt == nil || !second.CancelRequestedAt.Equal(requestedAt) || second.Revision != first.Revision {
		t.Fatalf("replayed cancellation = %+v, want first request and revision %+v", second, first)
	}
}

func TestRepositoryRunCreationRollsBackWhenIdempotencyInsertFails(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if _, err := repository.pool.Exec(ctx, `
		ALTER TABLE idempotency_records
		ADD CONSTRAINT reject_test_run_resource CHECK (resource_type <> 'run')
	`); err != nil {
		t.Fatalf("install failure constraint: %v", err)
	}

	if _, _, err := repository.CreateRun(ctx, snapshot, "operator", "start-run", "start-hash"); err == nil {
		t.Fatal("CreateRun() error = nil, want idempotency insert failure")
	}
	if _, err := repository.Load(ctx, snapshot.Run.ID); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("Load() after rollback error = %v, want ErrRunNotFound", err)
	}
}

func TestRepositoryRejectsRunSnapshotThatDoesNotMatchDefinitionVersion(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	snapshot.Definition.Tasks[0].Action = "different-definition"

	if _, _, err := repository.CreateRun(ctx, snapshot, "operator", "mismatched-definition", "hash"); err == nil {
		t.Fatal("CreateRun() error = nil, want definition version mismatch")
	}
	if _, err := repository.Load(ctx, snapshot.Run.ID); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("Load() after rejected Run = %v, want ErrRunNotFound", err)
	}
}

func TestRepositoryRejectsAttemptNumberGap(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	run := snapshot.Run
	run.Revision = 1
	run.Tasks = nil
	run.RemainingDependencies = nil
	change := workflow.ChangeSet{
		RunID:            snapshot.Run.ID,
		ExpectedRevision: 0,
		Run:              &run,
		Attempts: []workflow.AttemptChange{{
			RunID:     snapshot.Run.ID,
			TaskKey:   "clean",
			TaskIndex: 0,
			Attempt:   workflow.Attempt{Number: 2, Status: workflow.AttemptRunning, StartedAt: time.Unix(20, 0).UTC()},
			Operation: workflow.ChangeInsert,
		}},
	}
	if err := repository.Apply(ctx, change); err == nil {
		t.Fatal("Apply() error = nil, want Attempt number gap rejection")
	}
	loaded, err := repository.Load(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Run.Revision != 0 || len(loaded.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("Run after rejected Attempt gap = %+v, want revision 0 without attempts", loaded.Run)
	}
}

func TestRepositoryConcurrentRunCreationWithSameIdempotencyKeyCreatesOneRun(t *testing.T) {
	repository := newTestRepository(t)
	snapshot := testSnapshot(t, repository)

	var wait sync.WaitGroup
	wait.Add(2)
	ids := make([]workflow.RunID, 2)
	errs := make([]error, 2)
	for index := range ids {
		go func(index int) {
			defer wait.Done()
			ids[index], _, errs[index] = repository.CreateRun(
				context.Background(), snapshot, "operator", "concurrent-start", "same-hash",
			)
		}(index)
	}
	wait.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("CreateRun(%d) error = %v", index, err)
		}
	}
	if ids[0] != snapshot.Run.ID || ids[1] != snapshot.Run.ID {
		t.Fatalf("concurrent Run IDs = %v, want [%s %s]", ids, snapshot.Run.ID, snapshot.Run.ID)
	}
	var count int
	if err := repository.pool.QueryRow(context.Background(), "SELECT count(*) FROM workflow_runs").Scan(&count); err != nil {
		t.Fatalf("count workflow_runs: %v", err)
	}
	if count != 1 {
		t.Fatalf("workflow_runs count = %d, want 1", count)
	}
}

func TestRepositoryRunQueryPagesKeepAttemptHistoryBounded(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	startedAt := time.Unix(20, 0).UTC()
	run := snapshot.Run
	run.Status = workflow.WorkflowRunning
	run.Revision = 1
	run.LastEventSequence = 3
	run.StartedAt = &startedAt
	run.Tasks = nil
	run.RemainingDependencies = nil
	task := snapshot.Run.Tasks[0]
	task.Status = workflow.TaskRunning
	change := workflow.ChangeSet{
		RunID:            snapshot.Run.ID,
		ExpectedRevision: 0,
		Run:              &run,
		Tasks: []workflow.TaskRunChange{{
			RunID:                 snapshot.Run.ID,
			Index:                 0,
			Task:                  task,
			RemainingDependencies: 0,
		}},
		Attempts: []workflow.AttemptChange{{
			RunID:     snapshot.Run.ID,
			TaskKey:   task.Key,
			TaskIndex: 0,
			Attempt:   workflow.Attempt{Number: 1, Status: workflow.AttemptRunning, StartedAt: startedAt, Result: workflow.ExecutionResult{Output: "in progress"}},
			Operation: workflow.ChangeInsert,
		}},
		Events: []workflow.StateEvent{
			{Sequence: 1, At: startedAt, Entity: "workflow", Key: string(snapshot.Run.ID), From: string(workflow.WorkflowPending), To: string(workflow.WorkflowRunning), Reason: "execute"},
			{Sequence: 2, At: startedAt, Entity: "task", Key: string(task.Key), From: string(workflow.TaskReady), To: string(workflow.TaskRunning), Reason: "schedule"},
			{Sequence: 3, At: startedAt, Entity: "attempt", Key: string(task.Key) + "/1", From: "", To: string(workflow.AttemptRunning), Reason: "start"},
		},
	}
	if err := repository.Apply(ctx, change); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	loaded, err := repository.LoadRun(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatalf("LoadRun() error = %v", err)
	}
	if len(loaded.Tasks) != 2 || len(loaded.Tasks[0].Attempts) != 0 {
		t.Fatalf("LoadRun() = %+v, want task count only without Attempts", loaded)
	}

	page, more, err := repository.ListTaskRuns(ctx, snapshot.Run.ID, -1, 1)
	if err != nil {
		t.Fatalf("ListTaskRuns() error = %v", err)
	}
	if len(page) != 1 || !more || page[0].Task.Key != "clean" || len(page[0].Task.Attempts) != 0 {
		t.Fatalf("first task page = %+v, more=%v", page, more)
	}
	page, more, err = repository.ListTaskRuns(ctx, snapshot.Run.ID, page[0].Index, 1)
	if err != nil {
		t.Fatalf("ListTaskRuns() second page error = %v", err)
	}
	if len(page) != 1 || more || page[0].Task.Key != "summarize" {
		t.Fatalf("second task page = %+v, more=%v", page, more)
	}

	detail, err := repository.LoadTaskRun(ctx, snapshot.Run.ID, task.Key)
	if err != nil {
		t.Fatalf("LoadTaskRun() error = %v", err)
	}
	if len(detail.Attempts) != 1 || detail.Attempts[0].Result.Output != "in progress" {
		t.Fatalf("LoadTaskRun() = %+v, want one Attempt", detail)
	}

	events, more, err := repository.ListStateEvents(ctx, snapshot.Run.ID, 0, 2)
	if err != nil {
		t.Fatalf("ListStateEvents() error = %v", err)
	}
	if len(events) != 2 || !more || events[1].Sequence != 2 {
		t.Fatalf("first event page = %+v, more=%v", events, more)
	}
	events, more, err = repository.ListStateEvents(ctx, snapshot.Run.ID, 2, 2)
	if err != nil {
		t.Fatalf("ListStateEvents() second page error = %v", err)
	}
	if len(events) != 1 || more || events[0].Sequence != 3 {
		t.Fatalf("second event page = %+v, more=%v", events, more)
	}
}

func TestRepositoryListsWorkflowVersionsAndRunSummaries(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	definition := testDefinition()
	definition.Tasks[0].Action = "clean-v2"
	if _, err := repository.CreateVersion(ctx, definition.ID, definition, "operator", "workflow-2", "hash-2"); err != nil {
		t.Fatalf("CreateVersion() error = %v", err)
	}
	summary, err := repository.LoadWorkflowSummary(ctx, definition.ID)
	if err != nil {
		t.Fatalf("LoadWorkflowSummary() error = %v", err)
	}
	if summary.WorkflowID != definition.ID || summary.LatestVersion != 2 {
		t.Fatalf("workflow summary = %+v", summary)
	}
	workflows, more, err := repository.ListWorkflows(ctx, "", 1)
	if err != nil {
		t.Fatalf("ListWorkflows() error = %v", err)
	}
	if len(workflows) != 1 || more || workflows[0].WorkflowID != definition.ID {
		t.Fatalf("workflows = %+v, more=%v", workflows, more)
	}
	versions, more, err := repository.ListVersions(ctx, definition.ID, 0, 1)
	if err != nil {
		t.Fatalf("ListVersions() error = %v", err)
	}
	if len(versions) != 1 || !more || versions[0].Version != 1 {
		t.Fatalf("versions = %+v, more=%v", versions, more)
	}

	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	runs, more, err := repository.ListRunSummaries(ctx, RunQuery{}, 1)
	if err != nil {
		t.Fatalf("ListRunSummaries() error = %v", err)
	}
	if len(runs) != 1 || more || runs[0].ID != snapshot.Run.ID || runs[0].TaskCount != 2 {
		t.Fatalf("Run summaries = %+v, more=%v", runs, more)
	}
}

func TestRepositoryKeysetPaginationAndRunFilters(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	definition := testDefinition()
	definition.ID = "workflow-b"
	definition = canonicalDefinition(t, definition)
	if _, err := repository.CreateDefinition(ctx, definition, "operator", "create-b", "hash-b"); err != nil {
		t.Fatal(err)
	}
	definitionA := definition
	definitionA.ID = "workflow-a"
	definitionA = canonicalDefinition(t, definitionA)
	if _, err := repository.CreateDefinition(ctx, definitionA, "operator", "create-a", "hash-a"); err != nil {
		t.Fatal(err)
	}

	workflows, more, err := repository.ListWorkflows(ctx, "", 1)
	if err != nil || len(workflows) != 1 || !more || workflows[0].WorkflowID != "workflow-a" {
		t.Fatalf("first workflow page = %+v, more=%v, err=%v", workflows, more, err)
	}
	definitionBefore := definition
	definitionBefore.ID = "workflow-0"
	definitionBefore = canonicalDefinition(t, definitionBefore)
	if _, err := repository.CreateDefinition(ctx, definitionBefore, "operator", "create-before", "hash-before"); err != nil {
		t.Fatal(err)
	}
	workflows, _, err = repository.ListWorkflows(ctx, "workflow-a", 10)
	if err != nil || len(workflows) != 1 || workflows[0].WorkflowID != "workflow-b" {
		t.Fatalf("second workflow page = %+v, err=%v", workflows, err)
	}

	versionTwo := definitionA
	versionTwo.Tasks = append([]workflow.TaskDefinition(nil), definitionA.Tasks...)
	versionTwo.Tasks[0].Action = "v2"
	versionTwo = canonicalDefinition(t, versionTwo)
	if _, err := repository.CreateVersion(ctx, definitionA.ID, versionTwo, "operator", "create-v2", "hash-v2"); err != nil {
		t.Fatal(err)
	}
	versions, more, err := repository.ListVersions(ctx, definitionA.ID, 0, 1)
	if err != nil || len(versions) != 1 || !more || versions[0].Version != 1 {
		t.Fatalf("first version page = %+v, more=%v, err=%v", versions, more, err)
	}
	versions, _, err = repository.ListVersions(ctx, definitionA.ID, 1, 10)
	if err != nil || len(versions) != 1 || versions[0].Version != 2 {
		t.Fatalf("second version page = %+v, err=%v", versions, err)
	}

	createRunRecord(t, repository, definitionA, "run-b", workflow.WorkflowSucceeded, time.Unix(20, 0).UTC())
	createRunRecord(t, repository, definitionA, "run-c", workflow.WorkflowSucceeded, time.Unix(30, 0).UTC())
	createRunRecord(t, repository, definition, "run-other", workflow.WorkflowSucceeded, time.Unix(25, 0).UTC())
	query := RunQuery{WorkflowID: definitionA.ID, Status: workflow.WorkflowSucceeded}
	runs, more, err := repository.ListRunSummaries(ctx, query, 1)
	if err != nil || len(runs) != 1 || !more || runs[0].ID != "run-b" {
		t.Fatalf("first Run page = %+v, more=%v, err=%v", runs, more, err)
	}
	createRunRecord(t, repository, definitionA, "run-a", workflow.WorkflowSucceeded, time.Unix(10, 0).UTC())
	query.AfterCreated = &runs[0].CreatedAt
	query.AfterRunID = runs[0].ID
	runs, more, err = repository.ListRunSummaries(ctx, query, 10)
	if err != nil || more || len(runs) != 1 || runs[0].ID != "run-c" {
		t.Fatalf("second Run page = %+v, more=%v, err=%v", runs, more, err)
	}

	tasks, more, err := repository.ListTaskRuns(ctx, "run-b", -1, 1)
	if err != nil || len(tasks) != 1 || !more || tasks[0].Index != 0 {
		t.Fatalf("first Task page = %+v, more=%v, err=%v", tasks, more, err)
	}
	tasks, more, err = repository.ListTaskRuns(ctx, "run-b", tasks[0].Index, 10)
	if err != nil || more || len(tasks) != 1 || tasks[0].Index != 1 {
		t.Fatalf("second Task page = %+v, more=%v, err=%v", tasks, more, err)
	}
}

func TestRepositoryAdvisoryLockIsExclusiveUntilClose(t *testing.T) {
	first := newTestRepository(t)
	second := first
	lock, err := first.AcquireAdvisoryLock(context.Background())
	if err != nil {
		t.Fatalf("first AcquireAdvisoryLock() error = %v", err)
	}
	defer lock.Close()
	if _, err := second.AcquireAdvisoryLock(context.Background()); err == nil {
		t.Fatal("second AcquireAdvisoryLock() error = nil, want lock contention")
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	secondLock, err := second.AcquireAdvisoryLock(context.Background())
	if err != nil {
		t.Fatalf("AcquireAdvisoryLock() after close error = %v", err)
	}
	_ = secondLock.Close()
}

func TestRepositoryAdvisoryLockCheckUsesOwningConnectionAndDetectsLoss(t *testing.T) {
	repository := newTestRepository(t)
	lock, err := repository.AcquireAdvisoryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := lock.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if _, err := lock.conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", coordinatorAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	if err := lock.Check(context.Background()); !errors.Is(err, ErrCoordinatorLockLost) {
		t.Fatalf("Check() error = %v, want ErrCoordinatorLockLost", err)
	}
}

func TestRepositoryCheckMigrationsRejectsMissingSchemaAndAcceptsCurrentSchema(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	if err := repository.CheckMigrations(ctx); err != nil {
		t.Fatalf("CheckMigrations() after Migrate error = %v", err)
	}
	var latestVersion int64
	if err := repository.pool.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&latestVersion); err != nil {
		t.Fatalf("load latest migration version: %v", err)
	}
	if _, err := repository.pool.Exec(ctx, "DELETE FROM schema_migrations WHERE version = $1", latestVersion); err != nil {
		t.Fatalf("delete migration record: %v", err)
	}
	if err := repository.CheckMigrations(ctx); err == nil {
		t.Fatal("CheckMigrations() error = nil, want version mismatch")
	}
	if _, err := repository.pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", latestVersion); err != nil {
		t.Fatalf("restore migration record: %v", err)
	}
}

func TestCheckMigrationsRejectsUnknownVersion(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES (999)"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CheckMigrations(ctx); err == nil || !strings.Contains(err.Error(), "999") {
		t.Fatalf("CheckMigrations() error = %v, want unknown version 999", err)
	}
	if err := repository.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "999") {
		t.Fatalf("Migrate() error = %v, want unknown version 999", err)
	}
}

func TestValidateMigrationPrefixAndExactSet(t *testing.T) {
	embedded := []int64{1, 2}
	for _, applied := range [][]int64{nil, {1}, {1, 2}} {
		if err := validateMigrationPrefix(embedded, applied); err != nil {
			t.Fatalf("validateMigrationPrefix(%v, %v) error = %v", embedded, applied, err)
		}
	}
	for _, applied := range [][]int64{{2}, {1, 2, 999}} {
		if err := validateMigrationPrefix(embedded, applied); err == nil {
			t.Fatalf("validateMigrationPrefix(%v, %v) error = nil", embedded, applied)
		}
	}
	if err := validateMigrationSet(embedded, []int64{1, 2}); err != nil {
		t.Fatalf("validateMigrationSet() error = %v", err)
	}
	for _, applied := range [][]int64{nil, {1}, {2}, {1, 2, 999}} {
		if err := validateMigrationSet(embedded, applied); err == nil {
			t.Fatalf("validateMigrationSet(%v, %v) error = nil", embedded, applied)
		}
	}
}

func newTestRepository(t *testing.T) *Repository {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	repository, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(repository.Close)
	if err := repository.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return repository
}

func testDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:          "document-pipeline",
		Concurrency: 2,
		Tasks: []workflow.TaskDefinition{
			{Key: "clean", Action: "clean", TimeoutMillis: 1000},
			{Key: "summarize", Action: "summarize", DependsOn: []workflow.TaskKey{"clean"}, TimeoutMillis: 1000},
		},
	}
}

func testSnapshot(t *testing.T, repository *Repository) workflow.RunSnapshot {
	t.Helper()
	definition := testDefinition()
	if _, err := repository.CreateDefinition(context.Background(), definition, "operator", "create-definition", "definition-hash"); err != nil {
		t.Fatalf("CreateDefinition() error = %v", err)
	}
	return workflow.RunSnapshot{
		Version:    1,
		Definition: &definition,
		Run: workflow.WorkflowRun{
			ID:                    "run-one",
			DefinitionID:          definition.ID,
			DefinitionVersion:     1,
			Status:                workflow.WorkflowPending,
			Tasks:                 []workflow.TaskRun{{Key: "clean", Status: workflow.TaskReady, Attempts: []workflow.Attempt{}}, {Key: "summarize", Status: workflow.TaskWaitingDependencies, Attempts: []workflow.Attempt{}}},
			RemainingDependencies: []int{0, 1},
			CreatedAt:             time.Unix(10, 0).UTC(),
		},
		Events: []workflow.StateEvent{},
	}
}

func createRunRecord(
	t *testing.T,
	repository *Repository,
	definition workflow.WorkflowDefinition,
	id workflow.RunID,
	status workflow.WorkflowStatus,
	createdAt time.Time,
) {
	t.Helper()
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := workflow.NewRunSnapshotForVersion(id, compiled, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if status != workflow.WorkflowPending {
		if _, err := repository.pool.Exec(context.Background(), "UPDATE workflow_runs SET status = $1 WHERE run_id = $2", status, id); err != nil {
			t.Fatal(err)
		}
	}
}

func canonicalDefinition(t *testing.T, definition workflow.WorkflowDefinition) workflow.WorkflowDefinition {
	t.Helper()
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	return compiled.Definition()
}
