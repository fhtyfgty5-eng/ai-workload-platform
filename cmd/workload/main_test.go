package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/filestore"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/mockexec"
)

func TestRunCommandExecutesExampleAndPrintsRunID(t *testing.T) {
	dataDir := t.TempDir()
	var stdout, stderr bytes.Buffer

	exitCode := run(
		context.Background(),
		[]string{"run", filepath.Join("..", "..", "examples", "document-pipeline.json")},
		dataDir,
		&stdout,
		&stderr,
	)

	if exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("run_id=")) ||
		!bytes.Contains(stdout.Bytes(), []byte("status=succeeded")) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCommandReturnsFailureWhenResultCannotBeWritten(t *testing.T) {
	var stderr bytes.Buffer
	exitCode := run(
		context.Background(),
		[]string{"run", filepath.Join("..", "..", "examples", "document-pipeline.json")},
		t.TempDir(),
		errorWriter{},
		&stderr,
	)
	if exitCode == 0 || !strings.Contains(stderr.String(), "write result") {
		t.Fatalf("exit = %d, stderr = %q", exitCode, stderr.String())
	}
}

func TestStatusCommandReadsPersistedSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	runID := createSucceededRunForCLI(t, dataDir)
	var stdout, stderr bytes.Buffer

	exitCode := run(
		context.Background(),
		[]string{"status", string(runID)},
		dataDir,
		&stdout,
		&stderr,
	)

	if exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"status":"succeeded"`)) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestLocalRunCommandExecutesExampleAndPrintsRunID(t *testing.T) {
	dataDir := t.TempDir()
	var stdout, stderr bytes.Buffer

	exitCode := run(
		context.Background(),
		[]string{"local", "run", filepath.Join("..", "..", "examples", "document-pipeline.json")},
		dataDir,
		&stdout,
		&stderr,
	)

	if exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("run_id=")) ||
		!bytes.Contains(stdout.Bytes(), []byte("status=succeeded")) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestLocalStatusCommandReadsPersistedSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	runID := createSucceededRunForCLI(t, dataDir)
	var stdout, stderr bytes.Buffer

	exitCode := run(
		context.Background(),
		[]string{"local", "status", string(runID)},
		dataDir,
		&stdout,
		&stderr,
	)

	if exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"status":"succeeded"`)) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCommandRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing command", args: nil},
		{name: "missing workflow path", args: []string{"run"}},
		{name: "missing run id", args: []string{"status"}},
		{name: "unknown command", args: []string{"resume", "run-id"}},
		{name: "extra argument", args: []string{"status", "run-id", "extra"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := run(context.Background(), tt.args, t.TempDir(), &stdout, &stderr)
			if exitCode != 2 {
				t.Fatalf("exit = %d, want 2", exitCode)
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), usage) {
				t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestControlPlaneCommandRequiresServerTokenAndIdempotencyKey(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := runWithEnvironment(context.Background(), []string{"workflow", "create", "workflow.json"}, &stdout, &stderr, func(string) string { return "" }); exit != 2 {
		t.Fatalf("missing configuration exit = %d, stderr=%q", exit, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if exit := runWithEnvironment(context.Background(), []string{"run", "start", "demo", "--version", "1"}, &stdout, &stderr, func(key string) string {
		if key == "WORKLOAD_SERVER_URL" {
			return "http://127.0.0.1:1"
		}
		if key == "WORKLOAD_TOKEN" {
			return "token"
		}
		return ""
	}); exit != 2 {
		t.Fatalf("missing key exit = %d, stderr=%q", exit, stderr.String())
	}
}

func TestRunCommandRejectsInvalidWorkflowJSON(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "malformed", content: `{"id":`},
		{name: "unknown field", content: `{"id":"invalid","concurrency":1,"tasks":[],"owner":"unknown"}`},
		{name: "multiple values", content: `{"id":"first","concurrency":1,"tasks":[]} {}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workflow.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			exitCode := run(context.Background(), []string{"run", path}, t.TempDir(), &stdout, &stderr)
			if exitCode == 0 {
				t.Fatalf("exit = 0, stdout = %q", stdout.String())
			}
			if stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestStatusCommandRejectsUnknownRunID(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(
		context.Background(),
		[]string{"status", "missing-run"},
		t.TempDir(),
		&stdout,
		&stderr,
	)
	if exitCode == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), workflow.ErrRunNotFound.Error()) {
		t.Fatalf("exit = %d, stdout = %q, stderr = %q", exitCode, stdout.String(), stderr.String())
	}
}

func TestStatusCommandDoesNotModifySnapshot(t *testing.T) {
	dataDir := t.TempDir()
	runID := createSucceededRunForCLI(t, dataDir)
	path := filepath.Join(dataDir, string(runID)+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if exit := run(
		context.Background(),
		[]string{"status", string(runID)},
		dataDir,
		&stdout,
		&stderr,
	); exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("status command modified the persisted snapshot")
	}
}

func TestRunCommandCancellationPersistsCanceledState(t *testing.T) {
	dataDir := t.TempDir()
	definitionPath := writeDefinitionForCLI(t, workflow.WorkflowDefinition{
		ID:          "cancel-example",
		Concurrency: 1,
		Tasks: []workflow.TaskDefinition{
			{Key: "running", Action: "wait", TimeoutMillis: 10_000},
			{Key: "blocked", Action: "not-started", DependsOn: []workflow.TaskKey{"running"}, TimeoutMillis: 10_000},
		},
	})
	factory := func(definition workflow.WorkflowDefinition) workflow.Executor {
		scripts := make(map[mockexec.ScriptKey][]mockexec.Step, len(definition.Tasks))
		for _, task := range definition.Tasks {
			scripts[mockexec.ScriptKey{DefinitionID: definition.ID, TaskKey: task.Key}] = []mockexec.Step{{
				WaitForCancellation: true,
			}}
		}
		return mockexec.New(workflow.RealClock{}, scripts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		exitCode int
		stdout   string
		stderr   string
	}
	resultCh := make(chan result, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		exitCode := runWithExecutorFactory(
			ctx,
			[]string{"run", definitionPath},
			dataDir,
			&stdout,
			&stderr,
			factory,
		)
		resultCh <- result{exitCode: exitCode, stdout: stdout.String(), stderr: stderr.String()}
	}()

	runID := waitForRunningCLI(t, dataDir)
	cancel()
	select {
	case got := <-resultCh:
		if got.exitCode == 0 || !strings.Contains(got.stdout+got.stderr, "status=canceled") {
			t.Fatalf("exit = %d, stdout = %q, stderr = %q", got.exitCode, got.stdout, got.stderr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run command did not return after cancellation")
	}

	store, err := filestore.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Load(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != workflow.WorkflowCanceled {
		t.Fatalf("workflow status = %s, want canceled", snapshot.Run.Status)
	}
	if got := snapshot.Run.Tasks[0]; got.Status != workflow.TaskCanceled ||
		len(got.Attempts) != 1 || got.Attempts[0].Status != workflow.AttemptCanceled {
		t.Fatalf("running task = %+v, want one canceled attempt", got)
	}
	if got := snapshot.Run.Tasks[1]; got.Status != workflow.TaskCanceled || len(got.Attempts) != 0 {
		t.Fatalf("blocked task = %+v, want canceled without attempts", got)
	}
}

func createSucceededRunForCLI(t *testing.T, dataDir string) workflow.RunID {
	t.Helper()
	var stdout, stderr bytes.Buffer
	example := filepath.Join("..", "..", "examples", "document-pipeline.json")
	if exit := run(
		context.Background(),
		[]string{"run", example},
		dataDir,
		&stdout,
		&stderr,
	); exit != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exit, stderr.String())
	}
	for _, field := range strings.Fields(stdout.String()) {
		if strings.HasPrefix(field, "run_id=") {
			return workflow.RunID(strings.TrimPrefix(field, "run_id="))
		}
	}
	t.Fatalf("run id missing from %q", stdout.String())
	return ""
}

func writeDefinitionForCLI(t *testing.T, definition workflow.WorkflowDefinition) string {
	t.Helper()
	data, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "workflow.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForRunningCLI(t *testing.T, dataDir string) workflow.RunID {
	t.Helper()
	store, err := filestore.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			runID := workflow.RunID(strings.TrimSuffix(entry.Name(), ".json"))
			snapshot, err := store.Load(context.Background(), runID)
			if err != nil {
				continue
			}
			if snapshot.Run.Status == workflow.WorkflowRunning &&
				len(snapshot.Run.Tasks[0].Attempts) == 1 &&
				snapshot.Run.Tasks[0].Attempts[0].Status == workflow.AttemptRunning {
				return runID
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("run did not persist a running attempt")
	return ""
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
