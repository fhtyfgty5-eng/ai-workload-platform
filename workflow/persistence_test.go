package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

type deltaTestStore struct {
	snapshot RunSnapshot
	apply    int
	changes  []ChangeSet
}

type applyOnlyTestStore struct {
	snapshot RunSnapshot
}

func (s *applyOnlyTestStore) Create(_ context.Context, snapshot RunSnapshot) error {
	s.snapshot = cloneRunSnapshot(snapshot)
	return nil
}

func (s *applyOnlyTestStore) Load(_ context.Context, _ RunID) (RunSnapshot, error) {
	return cloneRunSnapshot(s.snapshot), nil
}

func (s *applyOnlyTestStore) Apply(_ context.Context, change ChangeSet) error {
	if change.ExpectedRevision != s.snapshot.Run.Revision {
		return ErrRevisionConflict
	}
	after, err := applyChangeSet(s.snapshot, change)
	if err != nil {
		return err
	}
	s.snapshot = after
	return nil
}

func (s *applyOnlyTestStore) ListNonTerminal(context.Context) ([]RunID, error) {
	return nil, nil
}

func (s *applyOnlyTestStore) RequestCancel(context.Context, RunID, time.Time) (WorkflowRun, error) {
	return s.snapshot.Run, nil
}

type cancelAfterFirstLoadStore struct {
	*deltaTestStore
	loads int
	at    time.Time
}

func (s *cancelAfterFirstLoadStore) Load(_ context.Context, _ RunID) (RunSnapshot, error) {
	s.loads++
	loaded := cloneRunSnapshot(s.snapshot)
	if s.loads == 1 {
		s.snapshot.Run.CancelRequestedAt = &s.at
		s.snapshot.Run.Revision++
	}
	return loaded, nil
}

func newDeltaTestStore(snapshot RunSnapshot) *deltaTestStore {
	return &deltaTestStore{snapshot: cloneRunSnapshot(snapshot)}
}

func (s *deltaTestStore) Create(_ context.Context, snapshot RunSnapshot) error {
	s.snapshot = cloneRunSnapshot(snapshot)
	return nil
}

func (s *deltaTestStore) Save(_ context.Context, snapshot RunSnapshot) error {
	s.snapshot = cloneRunSnapshot(snapshot)
	return nil
}

func (s *deltaTestStore) Load(_ context.Context, _ RunID) (RunSnapshot, error) {
	return cloneRunSnapshot(s.snapshot), nil
}

func (s *deltaTestStore) Apply(_ context.Context, change ChangeSet) error {
	s.apply++
	if change.ExpectedRevision != s.snapshot.Run.Revision {
		return ErrRevisionConflict
	}
	after, err := applyChangeSet(s.snapshot, change)
	if err != nil {
		return err
	}
	s.changes = append(s.changes, change)
	s.snapshot = after
	return nil
}

func (s *deltaTestStore) ListNonTerminal(context.Context) ([]RunID, error) {
	if isWorkflowTerminal(s.snapshot.Run.Status) {
		return nil, nil
	}
	return []RunID{s.snapshot.Run.ID}, nil
}

func (s *deltaTestStore) RequestCancel(_ context.Context, _ RunID, _ time.Time) (WorkflowRun, error) {
	return s.snapshot.Run, nil
}

func TestPersistenceApplyRejectsStaleRevision(t *testing.T) {
	store := newDeltaTestStore(RunSnapshot{Run: WorkflowRun{ID: "run-1", Revision: 0}})
	change := ChangeSet{
		RunID:            "run-1",
		ExpectedRevision: 0,
		Run:              &WorkflowRun{ID: "run-1", Revision: 1},
	}
	if err := store.Apply(context.Background(), change); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if err := store.Apply(context.Background(), change); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("second Apply() error = %v, want ErrRevisionConflict", err)
	}
}

func TestEngineUsesDeltaPersistence(t *testing.T) {
	store := newDeltaTestStore(RunSnapshot{})
	engine := newTestEngine(store, newRecordingExecutor(nil))
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "delta-store",
		Concurrency: 1,
		Tasks:       []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})

	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if _, err := engine.Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if store.apply == 0 {
		t.Fatal("Engine did not call Persistence.Apply")
	}
}

func TestEngineEmitsOrderedRowLevelChangeSets(t *testing.T) {
	store := newDeltaTestStore(RunSnapshot{})
	engine := newTestEngine(store, newRecordingExecutor(nil))
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "ordered-changes",
		Concurrency: 1,
		Tasks:       []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})

	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if _, err := engine.Execute(context.Background(), id); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var lastEventSequence uint64
	var sawInsert, sawUpdate bool
	for index, change := range store.changes {
		wantRevision := uint64(index)
		if change.RunID != id || change.ExpectedRevision != wantRevision || change.Run == nil || change.Run.Revision != wantRevision+1 {
			t.Fatalf("change %d identity/revision = %+v, want RunID %q revision %d -> %d", index, change, id, wantRevision, wantRevision+1)
		}
		for _, task := range change.Tasks {
			if task.RunID != id || task.Index != 0 || task.Task.Key != "task" || len(task.Task.Attempts) != 0 {
				t.Fatalf("change %d task row = %+v, want isolated run/task row", index, task)
			}
		}
		for _, attempt := range change.Attempts {
			if attempt.RunID != id || attempt.TaskIndex != 0 || attempt.TaskKey != "task" || attempt.Attempt.Number != 1 {
				t.Fatalf("change %d attempt row = %+v, want run/task/1", index, attempt)
			}
			switch attempt.Operation {
			case ChangeInsert:
				sawInsert = true
			case ChangeUpdate:
				sawUpdate = true
			default:
				t.Fatalf("change %d attempt operation = %q", index, attempt.Operation)
			}
		}
		for _, event := range change.Events {
			lastEventSequence++
			if event.Sequence != lastEventSequence {
				t.Fatalf("change %d event sequence = %d, want %d", index, event.Sequence, lastEventSequence)
			}
		}
		if change.Run.LastEventSequence != lastEventSequence {
			t.Fatalf("change %d last event sequence = %d, want %d", index, change.Run.LastEventSequence, lastEventSequence)
		}
	}
	if !sawInsert || !sawUpdate {
		t.Fatalf("attempt operations: insert=%t update=%t, want both", sawInsert, sawUpdate)
	}
}

func TestNewEngineAcceptsPersistenceWithoutSnapshotSave(t *testing.T) {
	store := &applyOnlyTestStore{}
	if _, err := NewEngine(store, newRecordingExecutor(nil), EngineOptions{}); err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
}

func TestStoreRetainsSnapshotSaveContract(t *testing.T) {
	var store Store = newMemoryStore()
	snapshot := RunSnapshot{Run: WorkflowRun{ID: "run-1"}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	snapshot.Run.Revision = 1
	if err := store.Save(context.Background(), snapshot); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestEngineReturnsPersistedRevision(t *testing.T) {
	store := newDeltaTestStore(RunSnapshot{})
	engine := newTestEngine(store, newRecordingExecutor(nil))
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "revision",
		Concurrency: 1,
		Tasks:       []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})

	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	persisted, err := store.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if run.Revision == 0 || run.Revision != persisted.Run.Revision {
		t.Fatalf("returned revision = %d, persisted revision = %d", run.Revision, persisted.Run.Revision)
	}
}

func TestEngineReconcilesCandidateOlderThanPersistedCancelRequest(t *testing.T) {
	at := time.Date(2026, 8, 19, 1, 0, 0, 0, time.UTC)
	base := newDeltaTestStore(RunSnapshot{})
	store := &cancelAfterFirstLoadStore{deltaTestStore: base, at: at}
	engine := newTestEngine(store, newRecordingExecutor(nil))
	compiled := mustCompile(t, WorkflowDefinition{
		ID:          "cancel-race",
		Concurrency: 1,
		Tasks:       []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}},
	})

	id, err := engine.CreateRun(context.Background(), compiled)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	run, err := engine.Execute(context.Background(), id)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if run.Status != WorkflowCanceled {
		t.Fatalf("Execute() status = %s, want canceled", run.Status)
	}
	if store.snapshot.Run.CancelRequestedAt == nil || !store.snapshot.Run.CancelRequestedAt.Equal(at) {
		t.Fatalf("cancel_requested_at = %v, want preserved %v", store.snapshot.Run.CancelRequestedAt, at)
	}
}

func TestEngineReadOperationsRejectMismatchedStoredRunID(t *testing.T) {
	store := newMemoryStore()
	store.snapshots["requested"] = RunSnapshot{Run: WorkflowRun{ID: "other", Status: WorkflowSucceeded}}
	engine := newTestEngine(store, newRecordingExecutor(nil))

	if _, err := engine.Execute(context.Background(), "requested"); err == nil {
		t.Fatal("Execute() error = nil, want mismatched stored RunID error")
	}
	if _, err := engine.GetRun(context.Background(), "requested"); err == nil {
		t.Fatal("GetRun() error = nil, want mismatched stored RunID error")
	}
}

func TestChangeSetStoresDependencyCountOnChangedTask(t *testing.T) {
	before := RunSnapshot{Run: WorkflowRun{
		ID:                    "run-1",
		Tasks:                 []TaskRun{{Key: "task", Status: TaskWaitingDependencies}},
		RemainingDependencies: []int{1},
	}}
	after := cloneRunSnapshot(before)
	after.Run.Revision = 1
	after.Run.RemainingDependencies[0] = 0

	change := changeSetFromSnapshots(before, after)
	if len(change.Run.RemainingDependencies) != 0 {
		t.Fatalf("Run.RemainingDependencies = %v, want task-row changes only", change.Run.RemainingDependencies)
	}
	if len(change.Tasks) != 1 || change.Tasks[0].RemainingDependencies != 0 {
		t.Fatalf("Tasks = %+v, want changed task with remaining_dependencies=0", change.Tasks)
	}

	applied, err := applyChangeSet(before, change)
	if err != nil {
		t.Fatalf("applyChangeSet() error = %v", err)
	}
	if got := applied.Run.RemainingDependencies[0]; got != 0 {
		t.Fatalf("RemainingDependencies[0] = %d, want 0", got)
	}
}

func TestChangeSetStoresRowLevelTaskAndAttemptChanges(t *testing.T) {
	startedAt := time.Date(2026, 8, 19, 2, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(time.Second)
	before := RunSnapshot{Run: WorkflowRun{
		ID:                    "run-1",
		Revision:              4,
		Tasks:                 []TaskRun{{Key: "task", Status: TaskRunning, Attempts: []Attempt{{Number: 1, Status: AttemptRunning, StartedAt: startedAt}}}},
		RemainingDependencies: []int{0},
	}}
	after := cloneRunSnapshot(before)
	after.Run.Revision = 5
	after.Run.Tasks[0].Status = TaskWaitingRetry
	after.Run.Tasks[0].Attempts[0].Status = AttemptFailed
	after.Run.Tasks[0].Attempts[0].FinishedAt = &finishedAt
	after.Run.Tasks[0].Attempts = append(after.Run.Tasks[0].Attempts, Attempt{
		Number:    2,
		Status:    AttemptRunning,
		StartedAt: finishedAt,
	})

	change := changeSetFromSnapshots(before, after)
	if len(change.Tasks) != 1 {
		t.Fatalf("task changes = %d, want 1", len(change.Tasks))
	}
	taskChange := change.Tasks[0]
	if taskChange.RunID != "run-1" || taskChange.Index != 0 || taskChange.Task.Key != "task" {
		t.Fatalf("task change identity = %+v, want run-1/task at index 0", taskChange)
	}
	if len(taskChange.Task.Attempts) != 0 {
		t.Fatalf("task change attempts = %v, want attempts stored as separate rows", taskChange.Task.Attempts)
	}
	if len(change.Attempts) != 2 {
		t.Fatalf("attempt changes = %d, want update and insert", len(change.Attempts))
	}
	if got := change.Attempts[0]; got.RunID != "run-1" || got.TaskKey != "task" || got.TaskIndex != 0 || got.Attempt.Number != 1 || got.Operation != ChangeUpdate {
		t.Fatalf("first attempt change = %+v, want update of task/1", got)
	}
	if got := change.Attempts[1]; got.RunID != "run-1" || got.TaskKey != "task" || got.TaskIndex != 0 || got.Attempt.Number != 2 || got.Operation != ChangeInsert {
		t.Fatalf("second attempt change = %+v, want insert of task/2", got)
	}
}

func TestApplyChangeSetRejectsInvalidRowChanges(t *testing.T) {
	startedAt := time.Date(2026, 8, 19, 3, 0, 0, 0, time.UTC)
	base := RunSnapshot{
		Version: 1,
		Run: WorkflowRun{
			ID:                    "run-1",
			Status:                WorkflowRunning,
			Revision:              3,
			LastEventSequence:     5,
			Tasks:                 []TaskRun{{Key: "task", Status: TaskRunning, Attempts: []Attempt{{Number: 1, Status: AttemptRunning, StartedAt: startedAt}}}},
			RemainingDependencies: []int{0},
		},
		Events: []StateEvent{{Sequence: 5, Entity: "attempt", Key: "task/1"}},
	}
	validChange := func() ChangeSet {
		run := base.Run
		run.Revision = 4
		run.LastEventSequence = 6
		run.Tasks = nil
		run.RemainingDependencies = nil
		return ChangeSet{
			RunID:            "run-1",
			ExpectedRevision: 3,
			Run:              &run,
			Tasks: []TaskRunChange{{
				RunID:                 "run-1",
				Index:                 0,
				Task:                  TaskRun{Key: "task", Status: TaskRunning},
				RemainingDependencies: 0,
			}},
			Attempts: []AttemptChange{{
				RunID:     "run-1",
				TaskKey:   "task",
				TaskIndex: 0,
				Attempt:   Attempt{Number: 2, Status: AttemptRunning, StartedAt: startedAt.Add(time.Second)},
				Operation: ChangeInsert,
			}},
			Events: []StateEvent{{Sequence: 6, Entity: "attempt", Key: "task/2"}},
		}
	}

	tests := []struct {
		name   string
		mutate func(*ChangeSet)
		want   error
	}{
		{name: "change run id", mutate: func(change *ChangeSet) { change.RunID = "other" }, want: ErrInvalidChangeSet},
		{name: "expected revision", mutate: func(change *ChangeSet) { change.ExpectedRevision = 2 }, want: ErrRevisionConflict},
		{name: "missing run row", mutate: func(change *ChangeSet) { change.Run = nil }},
		{name: "run row id", mutate: func(change *ChangeSet) { change.Run.ID = "other" }},
		{name: "next revision", mutate: func(change *ChangeSet) { change.Run.Revision = 5 }},
		{name: "task run id", mutate: func(change *ChangeSet) { change.Tasks[0].RunID = "other" }},
		{name: "task index", mutate: func(change *ChangeSet) { change.Tasks[0].Index = 1 }},
		{name: "task key", mutate: func(change *ChangeSet) { change.Tasks[0].Task.Key = "other" }},
		{name: "task embeds attempts", mutate: func(change *ChangeSet) { change.Tasks[0].Task.Attempts = []Attempt{{Number: 1}} }},
		{name: "negative remaining dependencies", mutate: func(change *ChangeSet) { change.Tasks[0].RemainingDependencies = -1 }},
		{name: "attempt run id", mutate: func(change *ChangeSet) { change.Attempts[0].RunID = "other" }},
		{name: "attempt task index", mutate: func(change *ChangeSet) { change.Attempts[0].TaskIndex = 1 }},
		{name: "attempt task key", mutate: func(change *ChangeSet) { change.Attempts[0].TaskKey = "other" }},
		{name: "insert attempt number", mutate: func(change *ChangeSet) { change.Attempts[0].Attempt.Number = 3 }},
		{name: "unknown attempt operation", mutate: func(change *ChangeSet) { change.Attempts[0].Operation = "delete" }},
		{name: "event sequence", mutate: func(change *ChangeSet) { change.Events[0].Sequence = 7 }},
		{name: "last event sequence", mutate: func(change *ChangeSet) { change.Run.LastEventSequence = 5 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			change := validChange()
			tt.mutate(&change)
			want := tt.want
			if want == nil {
				want = ErrInvalidChangeSet
			}
			if _, err := ApplyChangeSetForStore(base, change); !errors.Is(err, want) {
				t.Fatalf("ApplyChangeSetForStore() error = %v, want %v", err, want)
			}
		})
	}

	applied, err := ApplyChangeSetForStore(base, validChange())
	if err != nil {
		t.Fatalf("valid ApplyChangeSetForStore() error = %v", err)
	}
	if applied.Run.Revision != 4 || applied.Run.LastEventSequence != 6 || len(applied.Run.Tasks[0].Attempts) != 2 || len(applied.Events) != 2 {
		t.Fatalf("applied snapshot = %+v, want revision 4 with second attempt and event", applied)
	}
}

func TestApplyChangeSetUpdatesExistingAttemptRow(t *testing.T) {
	startedAt := time.Date(2026, 8, 19, 4, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(time.Second)
	base := RunSnapshot{Run: WorkflowRun{
		ID:                    "run-1",
		Revision:              1,
		Tasks:                 []TaskRun{{Key: "task", Status: TaskRunning, Attempts: []Attempt{{Number: 1, Status: AttemptRunning, StartedAt: startedAt}}}},
		RemainingDependencies: []int{0},
	}}
	run := base.Run
	run.Revision = 2
	run.Tasks = nil
	run.RemainingDependencies = nil
	change := ChangeSet{
		RunID:            "run-1",
		ExpectedRevision: 1,
		Run:              &run,
		Attempts: []AttemptChange{{
			RunID:     "run-1",
			TaskKey:   "task",
			TaskIndex: 0,
			Attempt:   Attempt{Number: 1, Status: AttemptSucceeded, StartedAt: startedAt, FinishedAt: &finishedAt},
			Operation: ChangeUpdate,
		}},
	}

	applied, err := ApplyChangeSetForStore(base, change)
	if err != nil {
		t.Fatalf("ApplyChangeSetForStore() error = %v", err)
	}
	if attempts := applied.Run.Tasks[0].Attempts; len(attempts) != 1 || attempts[0].Status != AttemptSucceeded {
		t.Fatalf("attempts = %+v, want one succeeded attempt", attempts)
	}
}

func TestApplyChangeSetAcceptsLegacySnapshotEventSequence(t *testing.T) {
	base := RunSnapshot{
		Run:    WorkflowRun{ID: "run-1", Revision: 2},
		Events: []StateEvent{{Sequence: 5}},
	}
	run := base.Run
	run.Revision = 3
	run.LastEventSequence = 6
	change := ChangeSet{
		RunID:            "run-1",
		ExpectedRevision: 2,
		Run:              &run,
		Events:           []StateEvent{{Sequence: 6}},
	}

	applied, err := ApplyChangeSetForStore(base, change)
	if err != nil {
		t.Fatalf("ApplyChangeSetForStore() error = %v", err)
	}
	if applied.Run.LastEventSequence != 6 || len(applied.Events) != 2 {
		t.Fatalf("applied snapshot = %+v, want legacy event 5 followed by event 6", applied)
	}
}

func TestChangeSetNormalizesLegacyEventSequenceWithoutNewEvent(t *testing.T) {
	before := RunSnapshot{
		Run:    WorkflowRun{ID: "run-1", Revision: 2},
		Events: []StateEvent{{Sequence: 5}},
	}
	after := cloneRunSnapshot(before)
	after.Run.Revision = 3

	change := changeSetFromSnapshots(before, after)
	if change.Run == nil || change.Run.LastEventSequence != 5 || len(change.Events) != 0 {
		t.Fatalf("change = %+v, want normalized last sequence 5 without new events", change)
	}
	if _, err := ApplyChangeSetForStore(before, change); err != nil {
		t.Fatalf("ApplyChangeSetForStore() error = %v", err)
	}
}
