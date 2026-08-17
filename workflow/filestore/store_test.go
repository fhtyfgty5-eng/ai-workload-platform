package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestStoreCreateSaveLoad(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one", DefinitionID: "w", Status: workflow.WorkflowPending, CreatedAt: time.Unix(1, 0)}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Run.Status = workflow.WorkflowRunning
	if err := store.Save(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Run.Status != workflow.WorkflowRunning {
		t.Fatalf("status = %s, want running", loaded.Run.Status)
	}
}

func TestStoreDoesNotReplaceValidSnapshotWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	original := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one", Status: workflow.WorkflowPending, CreatedAt: time.Unix(1, 0)}}
	if err := store.Create(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	store.rename = func(string, string) error { return errors.New("rename failed") }
	original.Run.Status = workflow.WorkflowRunning
	if err := store.Save(context.Background(), original); err == nil {
		t.Fatal("Save() error = nil, want rename failure")
	}
	loaded, err := store.Load(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Run.Status != workflow.WorkflowPending {
		t.Fatalf("status = %s, want pending", loaded.Run.Status)
	}
}

func TestStoreRejectsDuplicateAndMissingRuns(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one"}}
	if err := store.Create(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), snapshot); !errors.Is(err, workflow.ErrRunExists) {
		t.Fatalf("duplicate Create() error = %v, want ErrRunExists", err)
	}
	if err := store.Save(context.Background(), workflow.RunSnapshot{Run: workflow.WorkflowRun{ID: "missing"}}); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("missing Save() error = %v, want ErrRunNotFound", err)
	}
	if _, err := store.Load(context.Background(), "missing"); !errors.Is(err, workflow.ErrRunNotFound) {
		t.Fatalf("missing Load() error = %v, want ErrRunNotFound", err)
	}
}

func TestStoreRejectsCorruptJSONAndPathTraversal(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupt.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), "corrupt"); err == nil {
		t.Fatal("Load() error = nil, want corrupt JSON error")
	}
	for _, id := range []workflow.RunID{"", ".", "..", "nested/run", `nested\\run`} {
		snapshot := workflow.RunSnapshot{Run: workflow.WorkflowRun{ID: id}}
		if err := store.Create(context.Background(), snapshot); err == nil {
			t.Fatalf("Create() id %q error = nil, want invalid run id", id)
		}
	}
}

func TestStoreConcurrentCreateSameRunHasSingleWinner(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workflow.RunSnapshot{Version: 1, Run: workflow.WorkflowRun{ID: "run-one"}}
	results := make(chan error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer group.Done()
			results <- store.Create(context.Background(), snapshot)
		}()
	}
	group.Wait()
	close(results)

	winners := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			winners++
		} else if errors.Is(err, workflow.ErrRunExists) {
			duplicates++
		}
	}
	if winners != 1 || duplicates != 1 {
		t.Fatalf("winners = %d, duplicates = %d, want one each", winners, duplicates)
	}
}

func TestNewRejectsUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		return
	}
	if _, err := New(t.TempDir()); !errors.Is(err, workflow.ErrAtomicReplaceUnsupported) {
		t.Fatalf("New() error = %v, want ErrAtomicReplaceUnsupported", err)
	}
}
