package app

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type fakeRepository struct {
	mu                     sync.Mutex
	definitions            map[string]workflow.WorkflowDefinition
	createDefinitionResult postgres.DefinitionRecord
	createDefinitionCalls  int
	createVersionCalls     int
	createRunID            workflow.RunID
	createRunCalls         int
	createRunCreated       []bool
	runs                   map[workflow.RunID]workflow.WorkflowRun
	tasks                  map[workflow.RunID][]workflow.TaskRun
	events                 map[workflow.RunID][]workflow.StateEvent
	loadTaskRunCalls       int
	listTaskRunsCalls      int
	listStateEventsCalls   int
	cancelCalls            int
	cancelAt               time.Time
	runRecords             []postgres.RunRecord
	runRecordsMore         bool
	lastRunQuery           postgres.RunQuery
	workflowRecords        []postgres.WorkflowRecord
	workflowRecordsMore    bool
	lastWorkflowAfterID    string
	versionRecords         []postgres.VersionRecord
	versionRecordsMore     bool
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{definitions: make(map[string]workflow.WorkflowDefinition), runs: make(map[workflow.RunID]workflow.WorkflowRun), tasks: make(map[workflow.RunID][]workflow.TaskRun), events: make(map[workflow.RunID][]workflow.StateEvent)}
}

func (f *fakeRepository) CreateDefinition(context.Context, workflow.WorkflowDefinition, string, string, string) (postgres.DefinitionRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createDefinitionCalls++
	return f.createDefinitionResult, nil
}

func (f *fakeRepository) CreateVersion(context.Context, string, workflow.WorkflowDefinition, string, string, string) (postgres.DefinitionRecord, error) {
	f.createVersionCalls++
	return postgres.DefinitionRecord{WorkflowID: "document-pipeline", Version: 2}, nil
}

func (f *fakeRepository) LoadDefinition(_ context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	definition, ok := f.definitions[workflowID+"/"+strconv.Itoa(version)]
	if !ok {
		return workflow.WorkflowDefinition{}, postgres.ErrDefinitionNotFound
	}
	return definition, nil
}

func (f *fakeRepository) LoadWorkflowSummary(context.Context, string) (postgres.WorkflowRecord, error) {
	return postgres.WorkflowRecord{WorkflowID: "document-pipeline", LatestVersion: 1, CreatedAt: time.Unix(1, 0).UTC(), CreatedBy: "operator"}, nil
}

func (f *fakeRepository) ListWorkflows(_ context.Context, afterID string, _ int) ([]postgres.WorkflowRecord, bool, error) {
	f.lastWorkflowAfterID = afterID
	return append([]postgres.WorkflowRecord(nil), f.workflowRecords...), f.workflowRecordsMore, nil
}

func (f *fakeRepository) ListVersions(context.Context, string, int, int) ([]postgres.VersionRecord, bool, error) {
	return append([]postgres.VersionRecord(nil), f.versionRecords...), f.versionRecordsMore, nil
}

func (f *fakeRepository) CreateRun(_ context.Context, snapshot workflow.RunSnapshot, _ string, _ string, _ string) (workflow.RunID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createRunCalls++
	f.runs[snapshot.Run.ID] = snapshot.Run
	if f.createRunID == "" {
		f.createRunID = snapshot.Run.ID
	}
	created := true
	if index := f.createRunCalls - 1; index >= 0 && index < len(f.createRunCreated) {
		created = f.createRunCreated[index]
	}
	return f.createRunID, created, nil
}

func (f *fakeRepository) LoadRun(_ context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[id]
	if !ok {
		return workflow.WorkflowRun{}, workflow.ErrRunNotFound
	}
	run.Tasks = make([]workflow.TaskRun, len(f.tasks[id]))
	return run, nil
}

func (f *fakeRepository) ListRunSummaries(_ context.Context, query postgres.RunQuery, _ int) ([]postgres.RunRecord, bool, error) {
	f.lastRunQuery = query
	return append([]postgres.RunRecord(nil), f.runRecords...), f.runRecordsMore, nil
}

func (f *fakeRepository) ListTaskRuns(_ context.Context, id workflow.RunID, afterIndex, limit int) ([]postgres.TaskRecord, bool, error) {
	f.listTaskRunsCalls++
	tasks := f.tasks[id]
	start := afterIndex + 1
	if start > len(tasks) {
		return nil, false, nil
	}
	end := min(start+limit, len(tasks))
	records := make([]postgres.TaskRecord, 0, end-start)
	for index := start; index < end; index++ {
		records = append(records, postgres.TaskRecord{Index: index, Task: tasks[index]})
	}
	return records, end < len(tasks), nil
}

func (f *fakeRepository) LoadTaskRun(_ context.Context, id workflow.RunID, key workflow.TaskKey) (workflow.TaskRun, error) {
	f.loadTaskRunCalls++
	for _, task := range f.tasks[id] {
		if task.Key == key {
			return task, nil
		}
	}
	return workflow.TaskRun{}, postgres.ErrTaskNotFound
}

func (f *fakeRepository) ListStateEvents(_ context.Context, id workflow.RunID, after uint64, limit int) ([]workflow.StateEvent, bool, error) {
	f.listStateEventsCalls++
	items := make([]workflow.StateEvent, 0, limit)
	for _, event := range f.events[id] {
		if event.Sequence <= after {
			continue
		}
		if len(items) == limit {
			return items, true, nil
		}
		items = append(items, event)
	}
	return items, false, nil
}

func (f *fakeRepository) RequestCancel(_ context.Context, id workflow.RunID, at time.Time) (workflow.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	f.cancelAt = at
	run := f.runs[id]
	run.CancelRequestedAt = &at
	f.runs[id] = run
	return run, nil
}

type fakeEngine struct {
	cancelCalls int
	wakeCalls   int
	onWake      func()
	onCancel    func()
}

func (f *fakeEngine) Wake() {
	f.wakeCalls++
	if f.onWake != nil {
		f.onWake()
	}
}

func (f *fakeEngine) Cancel(context.Context, workflow.RunID) error {
	if f.onCancel != nil {
		f.onCancel()
	}
	f.cancelCalls++
	return nil
}
