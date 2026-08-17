package workflow

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompileBuildsIndexAndDependencies(t *testing.T) {
	def := WorkflowDefinition{
		ID:          "document-pipeline",
		Concurrency: 2,
		Tasks: []TaskDefinition{
			{Key: "read", Action: "mock-read", TimeoutMillis: 1000},
			{Key: "clean", Action: "mock-clean", DependsOn: []TaskKey{"read"}, TimeoutMillis: 1000},
			{Key: "publish", Action: "mock-publish", DependsOn: []TaskKey{"clean"}, TimeoutMillis: 1000},
		},
	}

	compiled, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if got := compiled.index["clean"]; got != 1 {
		t.Fatalf("clean index = %d, want 1", got)
	}
	if got := compiled.successors[0]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("read successors = %v, want [1]", got)
	}
	if got := compiled.dependencies[2]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("publish dependencies = %v, want [1]", got)
	}
}

func TestCompileRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name string
		def  WorkflowDefinition
	}{
		{name: "empty workflow id", def: WorkflowDefinition{Concurrency: 1}},
		{name: "invalid workflow id", def: WorkflowDefinition{ID: "Bad ID", Concurrency: 1}},
		{name: "zero concurrency", def: WorkflowDefinition{ID: "w"}},
		{name: "empty tasks", def: WorkflowDefinition{ID: "w", Concurrency: 1}},
		{name: "invalid key", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "Bad Key", Action: "run", TimeoutMillis: 1}}}},
		{name: "duplicate key", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "a", Action: "first", TimeoutMillis: 1}, {Key: "a", Action: "second", TimeoutMillis: 1}}}},
		{name: "blank action", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "a", Action: "  ", TimeoutMillis: 1}}}},
		{name: "missing dependency", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "a", Action: "run", DependsOn: []TaskKey{"missing"}, TimeoutMillis: 1}}}},
		{name: "cycle", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "a", Action: "first", DependsOn: []TaskKey{"b"}, TimeoutMillis: 1}, {Key: "b", Action: "second", DependsOn: []TaskKey{"a"}, TimeoutMillis: 1}}}},
		{name: "zero timeout", def: WorkflowDefinition{ID: "w", Concurrency: 1, Tasks: []TaskDefinition{{Key: "a", Action: "run"}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Compile(tt.def); err == nil {
				t.Fatal("Compile() error = nil, want non-nil")
			}
		})
	}
}

func TestCompileRejectsInvalidWorkflowID(t *testing.T) {
	for _, id := range []string{" ", "Bad ID", strings.Repeat("a", 65)} {
		definition := WorkflowDefinition{ID: id, Concurrency: 1, Tasks: []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}}}
		_, err := Compile(definition)
		if err == nil || !strings.Contains(err.Error(), "invalid workflow id") {
			t.Fatalf("Compile() id %q error = %v, want invalid workflow id", id, err)
		}
	}
}

func TestCompileAcceptsWorkflowIDAtLengthLimit(t *testing.T) {
	definition := WorkflowDefinition{ID: strings.Repeat("a", 64), Concurrency: 1, Tasks: []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}}}
	if _, err := Compile(definition); err != nil {
		t.Fatalf("Compile() error = %v, want nil", err)
	}
}

func TestCompileRejectsWorkflowWithoutTasks(t *testing.T) {
	definition := WorkflowDefinition{ID: "empty", Concurrency: 1, Tasks: []TaskDefinition{}}
	_, err := Compile(definition)
	if err == nil || err.Error() != "workflow must contain at least one task" {
		t.Fatalf("Compile() error = %v, want workflow must contain at least one task", err)
	}
}

func TestCompileRejectsBlankAction(t *testing.T) {
	for _, action := range []string{"", " \t"} {
		definition := WorkflowDefinition{ID: "blank-action", Concurrency: 1, Tasks: []TaskDefinition{{Key: "task", Action: action, TimeoutMillis: 1000}}}
		_, err := Compile(definition)
		if err == nil || !strings.Contains(err.Error(), "action is required") {
			t.Fatalf("Compile() action %q error = %v, want action is required", action, err)
		}
	}
}

func TestCompileRejectsTaskKeyOutsideIdentifierRule(t *testing.T) {
	for _, key := range []TaskKey{"Bad", TaskKey(strings.Repeat("a", 65))} {
		definition := WorkflowDefinition{ID: "task-key-boundary", Concurrency: 1, Tasks: []TaskDefinition{{Key: key, Action: "run", TimeoutMillis: 1000}}}
		if _, err := Compile(definition); err == nil {
			t.Fatalf("Compile() key %q error = nil, want non-nil", key)
		}
	}
}

func TestCompileRejectsRepeatedDependency(t *testing.T) {
	definition := WorkflowDefinition{ID: "repeated-dependency", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "parent", Action: "run", TimeoutMillis: 1000},
		{Key: "child", Action: "run", DependsOn: []TaskKey{"parent", "parent"}, TimeoutMillis: 1000},
	}}
	if _, err := Compile(definition); err == nil {
		t.Fatal("Compile() error = nil, want non-nil")
	}
}

func TestCompileRejectsInvalidRetryPolicy(t *testing.T) {
	for _, retry := range []RetryPolicy{{MaxAttempts: -1}, {IntervalMillis: -1}} {
		definition := WorkflowDefinition{ID: "invalid-retry", Concurrency: 1, Tasks: []TaskDefinition{{Key: "task", Action: "run", Retry: retry, TimeoutMillis: 1000}}}
		if _, err := Compile(definition); err == nil {
			t.Fatalf("Compile() retry %+v error = nil, want non-nil", retry)
		}
	}
}

func TestCompileAppliesDefaultRetryPolicy(t *testing.T) {
	definition := WorkflowDefinition{ID: "default-retry", Concurrency: 1, Tasks: []TaskDefinition{{Key: "task", Action: "run", TimeoutMillis: 1000}}}
	compiled, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.definition.Tasks[0].Retry.MaxAttempts; got != 1 {
		t.Fatalf("max attempts = %d, want 1", got)
	}
}

func TestCompileCopiesMutableDefinitionFields(t *testing.T) {
	definition := WorkflowDefinition{ID: "copied-definition", Concurrency: 1, Tasks: []TaskDefinition{
		{Key: "parent", Action: "parent", TimeoutMillis: 1000},
		{Key: "child", Action: "child", DependsOn: []TaskKey{"parent"}, TimeoutMillis: 1000},
	}}
	compiled, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}

	definition.Tasks[1].DependsOn[0] = "changed"
	if got := compiled.definition.Tasks[1].DependsOn[0]; got != "parent" {
		t.Fatalf("compiled dependency = %q, want parent", got)
	}
}

func TestCompileAcceptsTenThousandTasks(t *testing.T) {
	definition := WorkflowDefinition{ID: "large", Concurrency: 16, Tasks: make([]TaskDefinition, 10_000)}
	for i := range definition.Tasks {
		key := TaskKey(fmt.Sprintf("task-%d", i))
		definition.Tasks[i] = TaskDefinition{Key: key, Action: "noop", TimeoutMillis: 1000}
		if i > 0 {
			definition.Tasks[i].DependsOn = []TaskKey{TaskKey(fmt.Sprintf("task-%d", i-1))}
		}
	}
	compiled, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.index["task-9999"]; got != 9999 {
		t.Fatalf("last index = %d, want 9999", got)
	}
}

func TestCompileRejectsMoreThanTenThousandTasks(t *testing.T) {
	definition := WorkflowDefinition{ID: "too-large", Concurrency: 16, Tasks: make([]TaskDefinition, 10_001)}
	for i := range definition.Tasks {
		definition.Tasks[i] = TaskDefinition{Key: TaskKey(fmt.Sprintf("task-%d", i)), Action: "noop", TimeoutMillis: 1000}
	}
	_, err := Compile(definition)
	if err == nil || err.Error() != "workflow cannot contain more than 10000 tasks" {
		t.Fatalf("Compile() error = %v, want workflow cannot contain more than 10000 tasks", err)
	}
}
