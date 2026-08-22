package workflow

import (
	"testing"
	"time"
)

func TestTaskTransitionRejectsIllegalJump(t *testing.T) {
	snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{Key: "clean", Status: TaskWaitingDependencies}}}}
	if err := transitionTask(&snapshot, 0, TaskSucceeded, time.Unix(1, 0), "illegal jump"); err == nil {
		t.Fatal("transitionTask() error = nil, want illegal transition")
	}
}

func TestNewRunSnapshotHasIndependentDependencyCounts(t *testing.T) {
	compiled, err := Compile(WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1},
		{Key: "b", Action: "b", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	one := newRunSnapshot("run-one", compiled, time.Unix(1, 0))
	two := newRunSnapshot("run-two", compiled, time.Unix(1, 0))
	one.Run.RemainingDependencies[1]--
	if got := two.Run.RemainingDependencies[1]; got != 1 {
		t.Fatalf("second run remaining dependency = %d, want 1", got)
	}
}

func TestNewRunSnapshotForVersionSetsImmutableDefinitionReference(t *testing.T) {
	compiled, err := Compile(WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "clean", Action: "clean", TimeoutMillis: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := NewRunSnapshotForVersion("run-one", compiled, 3, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("NewRunSnapshotForVersion() error = %v", err)
	}
	if snapshot.Run.DefinitionID != "document-pipeline" || snapshot.Run.DefinitionVersion != 3 {
		t.Fatalf("definition reference = %s@%d, want document-pipeline@3", snapshot.Run.DefinitionID, snapshot.Run.DefinitionVersion)
	}
}

func TestNewRunSnapshotForVersionRejectsInvalidInput(t *testing.T) {
	compiled, err := Compile(WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "clean", Action: "clean", TimeoutMillis: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		id       RunID
		compiled *CompiledWorkflow
		version  int
	}{
		{name: "empty run id", compiled: compiled, version: 1},
		{name: "nil compiled workflow", id: "run-one", version: 1},
		{name: "zero version", id: "run-one", compiled: compiled},
		{name: "negative version", id: "run-one", compiled: compiled, version: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRunSnapshotForVersion(tt.id, tt.compiled, tt.version, time.Unix(1, 0)); err == nil {
				t.Fatalf("NewRunSnapshotForVersion(%q, version=%d) error = nil, want non-nil", tt.id, tt.version)
			}
		})
	}
}

func TestWorkflowTransitionAppendsSequencedEvent(t *testing.T) {
	snapshot := RunSnapshot{Run: WorkflowRun{ID: "run-one", Status: WorkflowPending}}
	at := time.Unix(2, 0)
	if err := transitionWorkflow(&snapshot, WorkflowRunning, at, "execute"); err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.StartedAt == nil || !snapshot.Run.StartedAt.Equal(at) {
		t.Fatalf("started_at = %v", snapshot.Run.StartedAt)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Sequence != 1 || snapshot.Events[0].Entity != "workflow" {
		t.Fatalf("events = %+v", snapshot.Events)
	}
	if err := transitionWorkflow(&snapshot, WorkflowPending, at, "illegal"); err == nil {
		t.Fatal("transitionWorkflow() error = nil, want illegal transition")
	}
}

func TestWorkflowTransitionContinuesPersistedEventSequence(t *testing.T) {
	tests := []struct {
		name     string
		snapshot RunSnapshot
		want     uint64
	}{
		{
			name: "relation store omits event history",
			snapshot: RunSnapshot{Run: WorkflowRun{
				ID:                "run-one",
				Status:            WorkflowPending,
				LastEventSequence: 10,
			}},
			want: 11,
		},
		{
			name: "legacy file snapshot omits last sequence",
			snapshot: RunSnapshot{
				Run:    WorkflowRun{ID: "run-one", Status: WorkflowPending},
				Events: []StateEvent{{Sequence: 5}},
			},
			want: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := tt.snapshot
			if err := transitionWorkflow(&snapshot, WorkflowRunning, time.Unix(2, 0), "execute"); err != nil {
				t.Fatalf("transitionWorkflow() error = %v", err)
			}
			got := snapshot.Events[len(snapshot.Events)-1].Sequence
			if got != tt.want || snapshot.Run.LastEventSequence != tt.want {
				t.Fatalf("event sequence = %d, last_event_sequence = %d, want %d", got, snapshot.Run.LastEventSequence, tt.want)
			}
		})
	}
}

func TestTaskTransitionRecordsTerminalTimeAndEvent(t *testing.T) {
	at := time.Unix(3, 0)
	snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{Key: "clean", Status: TaskRunning}}}}
	if err := transitionTask(&snapshot, 0, TaskSucceeded, at, "completed"); err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Tasks[0].FinishedAt == nil || !snapshot.Run.Tasks[0].FinishedAt.Equal(at) {
		t.Fatalf("finished_at = %v", snapshot.Run.Tasks[0].FinishedAt)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].To != string(TaskSucceeded) {
		t.Fatalf("events = %+v", snapshot.Events)
	}
}

func TestTaskTransitionClearsRetryTimeWhenCanceled(t *testing.T) {
	readyAt := time.Unix(10, 0)
	snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{
		Key:     "clean",
		Status:  TaskWaitingRetry,
		ReadyAt: &readyAt,
	}}}}
	if err := transitionTask(&snapshot, 0, TaskCanceled, time.Unix(5, 0), "canceled"); err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Tasks[0].ReadyAt != nil {
		t.Fatalf("ReadyAt = %v, want nil after cancellation", snapshot.Run.Tasks[0].ReadyAt)
	}
}

func TestAttemptTransitionRejectsTerminalStateExit(t *testing.T) {
	attempt := Attempt{Number: 1, Status: AttemptSucceeded}
	snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{Key: "clean", Attempts: []Attempt{attempt}}}}}
	if err := transitionAttempt(&snapshot, "clean", &snapshot.Run.Tasks[0].Attempts[0], AttemptRunning, time.Unix(3, 0), "illegal exit"); err == nil {
		t.Fatal("transitionAttempt() error = nil, want illegal transition")
	}
}

func TestRunSnapshotStateSlicesAreIndependent(t *testing.T) {
	compiled, err := Compile(WorkflowDefinition{ID: "independent", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "a", Action: "a", TimeoutMillis: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	one := newRunSnapshot("run-one", compiled, time.Unix(1, 0))
	two := newRunSnapshot("run-two", compiled, time.Unix(1, 0))
	one.Run.Tasks[0].Attempts = append(one.Run.Tasks[0].Attempts, Attempt{Number: 1})
	one.Run.RemainingDependencies[0] = 99
	one.Events = append(one.Events, StateEvent{Sequence: 1})
	if len(two.Run.Tasks[0].Attempts) != 0 {
		t.Fatalf("second run attempts = %+v, want empty", two.Run.Tasks[0].Attempts)
	}
	if two.Run.RemainingDependencies[0] != 0 {
		t.Fatalf("second run remaining dependencies = %d, want 0", two.Run.RemainingDependencies[0])
	}
	if len(two.Events) != 0 {
		t.Fatalf("second run events = %+v, want empty", two.Events)
	}
}

func TestWorkflowTransitionsAcceptLifecycleStates(t *testing.T) {
	for _, target := range []WorkflowStatus{WorkflowSucceeded, WorkflowFailed, WorkflowCanceled} {
		snapshot := RunSnapshot{Run: WorkflowRun{ID: "run-one", Status: WorkflowPending}}
		at := time.Unix(4, 0)
		if err := transitionWorkflow(&snapshot, WorkflowRunning, at, "start"); err != nil {
			t.Fatal(err)
		}
		if err := transitionWorkflow(&snapshot, target, at, "finish"); err != nil {
			t.Fatalf("target %s: %v", target, err)
		}
		if snapshot.Run.FinishedAt == nil {
			t.Fatalf("target %s: FinishedAt is nil", target)
		}
	}
}

func TestAttemptTransitionsAcceptAllTerminalStates(t *testing.T) {
	for _, target := range []AttemptStatus{AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptInterrupted} {
		attempt := Attempt{Number: 1, Status: AttemptRunning}
		snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{Key: "task", Attempts: []Attempt{attempt}}}}}
		if err := transitionAttempt(&snapshot, "task", &snapshot.Run.Tasks[0].Attempts[0], target, time.Unix(5, 0), "finish"); err != nil {
			t.Fatalf("target %s: %v", target, err)
		}
	}
}

func TestTaskTransitionsAcceptLifecycleStates(t *testing.T) {
	snapshot := RunSnapshot{Run: WorkflowRun{Tasks: []TaskRun{{Key: "task", Status: TaskWaitingDependencies}}}}
	at := time.Unix(6, 0)
	for _, target := range []TaskStatus{TaskReady, TaskRunning, TaskWaitingRetry, TaskReady, TaskRunning, TaskSucceeded} {
		if err := transitionTask(&snapshot, 0, target, at, "advance"); err != nil {
			t.Fatalf("target %s: %v", target, err)
		}
	}
}
