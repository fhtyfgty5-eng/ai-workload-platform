package postgres

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestMigrationUpgradesLegacySchemaAndPreservesRun(t *testing.T) {
	repository := newUnmigratedTestRepository(t)
	ctx := context.Background()
	applyLegacyMigrations(t, repository)
	seedLegacyRun(t, repository)

	if err := repository.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() legacy schema error = %v", err)
	}
	if err := repository.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
	if err := repository.CheckMigrations(ctx); err != nil {
		t.Fatalf("CheckMigrations() error = %v", err)
	}

	for _, table := range []string{"worker_sessions", "task_dispatches"} {
		var exists bool
		if err := repository.pool.QueryRow(ctx, "SELECT to_regclass(current_schema() || '.' || $1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("table %q does not exist after migration", table)
		}
	}
	var fairnessIndexExists bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT to_regclass(current_schema() || '.task_dispatches_run_created_idx') IS NOT NULL
	`).Scan(&fairnessIndexExists); err != nil {
		t.Fatal(err)
	}
	if !fairnessIndexExists {
		t.Fatal("cross-round fairness index does not exist after migration")
	}
	definition, err := repository.LoadDefinition(ctx, "legacy", 1)
	if err != nil {
		t.Fatalf("LoadDefinition() legacy definition error = %v", err)
	}
	if definition.Tasks[0].Executor != workflow.ExecutorMock {
		t.Fatalf("legacy LoadDefinition executor = %q, want %q", definition.Tasks[0].Executor, workflow.ExecutorMock)
	}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	newSnapshot, err := workflow.NewRunSnapshotForVersion("legacy-new-run", compiled, 1, time.Unix(20, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Create(ctx, newSnapshot); err != nil {
		t.Fatalf("create new Run from migrated legacy definition: %v", err)
	}

	loaded, err := repository.Load(ctx, "legacy-run")
	if err != nil {
		t.Fatalf("Load() legacy Run error = %v", err)
	}
	if loaded.Definition == nil || loaded.Definition.Tasks[0].Executor != workflow.ExecutorMock {
		t.Fatalf("legacy definition executor = %q, want %q", loaded.Definition.Tasks[0].Executor, workflow.ExecutorMock)
	}
	if attempt := loaded.Run.Tasks[0].Attempts[0]; attempt.WorkerID != "" || attempt.DispatchID != "" {
		t.Fatalf("legacy Attempt ownership = %q/%q, want empty", attempt.WorkerID, attempt.DispatchID)
	}
}

func TestWorkerSchemaRejectsInvalidRows(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	validHash := bytes.Repeat([]byte{1}, 32)
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO worker_sessions (
			worker_id, display_name, protocol_version, executor_kinds,
			max_concurrency, status, session_token_hash, registered_at, last_heartbeat_at
		) VALUES ('worker-valid', 'valid worker', 1, ARRAY['mock'], 2, 'active', $1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, validHash); err != nil {
		t.Fatalf("insert valid Worker: %v", err)
	}

	tests := []struct {
		name        string
		workerID    string
		protocol    int
		executors   []string
		concurrency int
		status      string
		hash        []byte
	}{
		{name: "empty worker ID", workerID: "", protocol: 1, executors: []string{"mock"}, concurrency: 1, status: "active", hash: bytes.Repeat([]byte{2}, 32)},
		{name: "invalid protocol", workerID: "worker-protocol", protocol: 0, executors: []string{"mock"}, concurrency: 1, status: "active", hash: bytes.Repeat([]byte{3}, 32)},
		{name: "empty executors", workerID: "worker-executors", protocol: 1, executors: []string{}, concurrency: 1, status: "active", hash: bytes.Repeat([]byte{4}, 32)},
		{name: "invalid concurrency", workerID: "worker-concurrency", protocol: 1, executors: []string{"mock"}, concurrency: 0, status: "active", hash: bytes.Repeat([]byte{5}, 32)},
		{name: "invalid status", workerID: "worker-status", protocol: 1, executors: []string{"mock"}, concurrency: 1, status: "unknown", hash: bytes.Repeat([]byte{6}, 32)},
		{name: "invalid hash length", workerID: "worker-hash", protocol: 1, executors: []string{"mock"}, concurrency: 1, status: "active", hash: []byte{1}},
		{name: "duplicate token hash", workerID: "worker-duplicate", protocol: 1, executors: []string{"mock"}, concurrency: 1, status: "active", hash: validHash},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := repository.pool.Exec(ctx, `
				INSERT INTO worker_sessions (
					worker_id, display_name, protocol_version, executor_kinds,
					max_concurrency, status, session_token_hash, registered_at, last_heartbeat_at
				) VALUES ($1, 'worker', $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			`, test.workerID, test.protocol, test.executors, test.concurrency, test.status, test.hash)
			if err == nil {
				t.Fatal("insert invalid Worker error = nil")
			}
		})
	}
}

func TestDispatchConstraintPreventsTwoActiveRowsForTask(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	insertTestWorker(t, repository, "worker-one", 11)

	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status, created_at
		) VALUES ('dispatch-one', 'run-one', 'clean', 'mock', 'pending', CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("insert first active Dispatch: %v", err)
	}
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status, created_at
		) VALUES ('dispatch-two', 'run-one', 'clean', 'mock', 'pending', CURRENT_TIMESTAMP)
	`); err == nil {
		t.Fatal("insert second active Dispatch error = nil, want unique violation")
	}
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status, created_at
		) VALUES ('dispatch-invalid-lease', 'run-one', 'summarize', 'mock', 'leased', CURRENT_TIMESTAMP)
	`); err == nil {
		t.Fatal("insert leased Dispatch without ownership error = nil")
	}
	leaseHash := bytes.Repeat([]byte{21}, 32)
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status,
			worker_id, attempt_number, lease_token_hash, lease_expires_at,
			attempt_deadline, created_at, leased_at
		) VALUES (
			'dispatch-invalid-time', 'run-one', 'summarize', 'mock', 'leased',
			'worker-one', 1, $1, $2, $3, $4, $4
		)
	`, bytes.Repeat([]byte{20}, 32), time.Unix(40, 0).UTC(), time.Unix(30, 0).UTC(), time.Unix(10, 0).UTC()); err == nil {
		t.Fatal("insert Dispatch with lease past Attempt deadline error = nil")
	}
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status,
			worker_id, attempt_number, lease_token_hash, lease_expires_at,
			attempt_deadline, created_at, leased_at
		) VALUES (
			'dispatch-lease-one', 'run-one', 'summarize', 'mock', 'leased',
			'worker-one', 1, $1, $2, $3, $4, $4
		)
	`, leaseHash, time.Unix(20, 0).UTC(), time.Unix(30, 0).UTC(), time.Unix(10, 0).UTC()); err != nil {
		t.Fatalf("insert valid leased Dispatch: %v", err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET status = 'expired', completed_at = $2
		WHERE dispatch_id = $1
	`, "dispatch-lease-one", time.Unix(25, 0).UTC()); err != nil {
		t.Fatalf("expire leased Dispatch: %v", err)
	}
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status,
			worker_id, attempt_number, lease_token_hash, lease_expires_at,
			attempt_deadline, created_at, leased_at
		) VALUES (
			'dispatch-lease-two', 'run-one', 'summarize', 'mock', 'leased',
			'worker-one', 2, $1, $2, $3, $4, $4
		)
	`, leaseHash, time.Unix(40, 0).UTC(), time.Unix(50, 0).UTC(), time.Unix(30, 0).UTC()); err == nil {
		t.Fatal("reuse lease token hash error = nil, want unique violation")
	}
}

func TestRepositoryPersistsQueuedStateAndDistributedAttemptOwnership(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	snapshot := testSnapshot(t, repository)
	if err := repository.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	compiled, err := workflow.Compile(*snapshot.Definition)
	if err != nil {
		t.Fatal(err)
	}

	queued := clonePostgresTestSnapshot(snapshot)
	if err := workflow.QueueTask(&queued, 0, time.Unix(11, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	queued.Run.Revision = snapshot.Run.Revision + 1
	queueChange, err := workflow.ChangeSetBetween(snapshot, queued)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Apply(ctx, queueChange); err != nil {
		t.Fatalf("persist queued Task: %v", err)
	}
	persistedQueued, err := repository.Load(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedQueued.Run.Tasks[0].Status != workflow.TaskQueued {
		t.Fatalf("persisted Task status = %s, want queued", persistedQueued.Run.Tasks[0].Status)
	}

	insertTestWorker(t, repository, "worker-one", 12)
	leaseTokenHash := bytes.Repeat([]byte{13}, 32)
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO task_dispatches (
			dispatch_id, run_id, task_key, executor_kind, status,
			worker_id, attempt_number, lease_token_hash, lease_expires_at,
			attempt_deadline, created_at, leased_at
		) VALUES (
			'dispatch-one', 'run-one', 'clean', 'mock', 'leased',
			'worker-one', 1, $1, $2, $3, $4, $4
		)
	`, leaseTokenHash, time.Unix(30, 0).UTC(), time.Unix(60, 0).UTC(), time.Unix(12, 0).UTC()); err != nil {
		t.Fatalf("insert leased Dispatch: %v", err)
	}

	running := clonePostgresTestSnapshot(persistedQueued)
	if _, err := workflow.StartQueuedAttempt(&running, compiled, 0, time.Unix(12, 0).UTC(), "worker-one", "dispatch-one"); err != nil {
		t.Fatal(err)
	}
	running.Run.Revision = persistedQueued.Run.Revision + 1
	claimChange, err := workflow.ChangeSetBetween(persistedQueued, running)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Apply(ctx, claimChange); err != nil {
		t.Fatalf("persist distributed Attempt: %v", err)
	}
	loaded, err := repository.Load(ctx, snapshot.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	attempt := loaded.Run.Tasks[0].Attempts[0]
	if attempt.WorkerID != "worker-one" || attempt.DispatchID != "dispatch-one" {
		t.Fatalf("Attempt ownership = %q/%q, want worker-one/dispatch-one", attempt.WorkerID, attempt.DispatchID)
	}
}

func newUnmigratedTestRepository(t *testing.T) *Repository {
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
		t.Fatal(err)
	}
	t.Cleanup(repository.Close)
	return repository
}

func applyLegacyMigrations(t *testing.T, repository *Repository) {
	t.Helper()
	ctx := context.Background()
	for version, name := range []string{"0001_initial.sql", "0002_constraints.sql"} {
		body, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := repository.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("apply legacy migration %s: %v", name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version+1); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func seedLegacyRun(t *testing.T, repository *Repository) {
	t.Helper()
	ctx := context.Background()
	definition := `{"id":"legacy","concurrency":1,"tasks":[{"key":"task","action":"run","timeout_ms":1000}]}`
	statements := []struct {
		query string
		args  []any
	}{
		{query: "INSERT INTO workflows (id, latest_version, created_at, created_by) VALUES ('legacy', 1, CURRENT_TIMESTAMP, 'test')"},
		{query: `INSERT INTO workflow_versions (
			workflow_id, version, definition_schema_version, definition_json,
			definition_hash, created_at, created_by
		) VALUES ('legacy', 1, 1, $1::jsonb, 'hash', CURRENT_TIMESTAMP, 'test')`, args: []any{definition}},
		{query: `INSERT INTO workflow_runs (
			run_id, workflow_id, workflow_version, snapshot_version, status,
			revision, last_event_sequence, created_at, started_at
		) VALUES ('legacy-run', 'legacy', 1, 1, 'running', 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`},
		{query: `INSERT INTO task_runs (
			run_id, task_key, task_index, status, remaining_dependencies
		) VALUES ('legacy-run', 'task', 0, 'running', 0)`},
		{query: `INSERT INTO attempts (
			run_id, task_key, attempt_number, status, started_at
		) VALUES ('legacy-run', 'task', 1, 'running', CURRENT_TIMESTAMP)`},
	}
	for _, statement := range statements {
		if _, err := repository.pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed legacy Run: %v", err)
		}
	}
}

func insertTestWorker(t *testing.T, repository *Repository, workerID string, hashByte byte) {
	t.Helper()
	if _, err := repository.pool.Exec(context.Background(), `
		INSERT INTO worker_sessions (
			worker_id, display_name, protocol_version, executor_kinds,
			max_concurrency, status, session_token_hash, registered_at, last_heartbeat_at
		) VALUES ($1, $1, 1, ARRAY['mock'], 2, 'active', $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, workerID, bytes.Repeat([]byte{hashByte}, 32)); err != nil {
		t.Fatalf("insert test Worker: %v", err)
	}
}

func clonePostgresTestSnapshot(snapshot workflow.RunSnapshot) workflow.RunSnapshot {
	clone := snapshot
	clone.Run.Tasks = append([]workflow.TaskRun(nil), snapshot.Run.Tasks...)
	for index := range clone.Run.Tasks {
		clone.Run.Tasks[index].Attempts = append([]workflow.Attempt(nil), snapshot.Run.Tasks[index].Attempts...)
	}
	clone.Run.RemainingDependencies = append([]int(nil), snapshot.Run.RemainingDependencies...)
	clone.Events = append([]workflow.StateEvent(nil), snapshot.Events...)
	return clone
}
