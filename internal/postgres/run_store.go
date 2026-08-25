package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

// Create 为不需要控制面幂等语义的调用方实现 workflow.RunStore。
func (r *Repository) Create(ctx context.Context, snapshot workflow.RunSnapshot) error {
	if err := validateInitialSnapshot(snapshot); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create Run: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := insertSnapshot(ctx, tx, snapshot, nil); err != nil {
		if isUniqueViolation(err, "workflow_runs_pkey") {
			return workflow.ErrRunExists
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create Run: %w", err)
	}
	return nil
}

// CreateRun 原子创建绑定版本的 Run 及其幂等记录。
// 重放相同请求时返回首次创建的 RunID，不插入第二份 Run 或 TaskRun。
func (r *Repository) CreateRun(
	ctx context.Context,
	snapshot workflow.RunSnapshot,
	principal, idemKey, requestHash string,
) (workflow.RunID, bool, error) {
	if err := validateInitialSnapshot(snapshot); err != nil {
		return "", false, err
	}
	if err := validateIdempotencyInput(principal, idemKey, requestHash); err != nil {
		return "", false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin idempotent Run creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdempotency(ctx, tx, principal, operationStartRun, idemKey); err != nil {
		return "", false, err
	}

	if record, found, err := loadIdempotency(ctx, tx, principal, operationStartRun, idemKey); err != nil {
		return "", false, err
	} else if found {
		if err := validateReplay(record, requestHash, resourceRun); err != nil {
			return "", false, err
		}
		return workflow.RunID(record.resourceID), false, nil
	}
	if err := insertSnapshot(ctx, tx, snapshot, &principal); err != nil {
		if isUniqueViolation(err, "workflow_runs_pkey") {
			return "", false, workflow.ErrRunExists
		}
		return "", false, err
	}
	if err := insertIdempotency(
		ctx, tx, principal, operationStartRun, idemKey, requestHash,
		resourceRun, string(snapshot.Run.ID), nil,
	); err != nil {
		return "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("commit idempotent Run creation: %w", err)
	}
	return snapshot.Run.ID, true, nil
}

func validateInitialSnapshot(snapshot workflow.RunSnapshot) error {
	if snapshot.Version <= 0 {
		return fmt.Errorf("snapshot version must be positive")
	}
	if snapshot.Definition == nil {
		return fmt.Errorf("snapshot definition is required")
	}
	if snapshot.Run.ID == "" {
		return fmt.Errorf("RunID is required")
	}
	if snapshot.Run.DefinitionID != snapshot.Definition.ID {
		return fmt.Errorf("Run definition ID %q does not match snapshot definition ID %q", snapshot.Run.DefinitionID, snapshot.Definition.ID)
	}
	if snapshot.Run.DefinitionVersion <= 0 {
		return fmt.Errorf("Run definition version must be positive")
	}
	if snapshot.Run.Status != workflow.WorkflowPending || snapshot.Run.Revision != 0 {
		return fmt.Errorf("new Run must be pending at revision 0")
	}
	if len(snapshot.Run.Tasks) != len(snapshot.Definition.Tasks) || len(snapshot.Run.Tasks) != len(snapshot.Run.RemainingDependencies) {
		return fmt.Errorf("Run task, definition, and dependency counts must match")
	}
	for index, task := range snapshot.Run.Tasks {
		if task.Key != snapshot.Definition.Tasks[index].Key {
			return fmt.Errorf("task key %q does not match definition index %d", task.Key, index)
		}
		if snapshot.Run.RemainingDependencies[index] < 0 {
			return fmt.Errorf("task %q has negative remaining dependencies", task.Key)
		}
	}
	var lastSequence uint64
	for index, event := range snapshot.Events {
		want := uint64(index + 1)
		if event.Sequence != want {
			return fmt.Errorf("initial event sequence %d must be %d", event.Sequence, want)
		}
		lastSequence = event.Sequence
	}
	if snapshot.Run.LastEventSequence != lastSequence {
		return fmt.Errorf("Run last event sequence %d must be %d", snapshot.Run.LastEventSequence, lastSequence)
	}
	return nil
}

func insertSnapshot(ctx context.Context, tx pgx.Tx, snapshot workflow.RunSnapshot, createdBy *string) error {
	if err := ensureDefinitionMatches(ctx, tx, snapshot); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_runs (
			run_id, workflow_id, workflow_version, snapshot_version, status,
			revision, last_event_sequence, cancel_requested_at,
			created_at, started_at, finished_at, created_by
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		snapshot.Run.ID,
		snapshot.Run.DefinitionID,
		snapshot.Run.DefinitionVersion,
		snapshot.Version,
		snapshot.Run.Status,
		int64(snapshot.Run.Revision),
		int64(snapshot.Run.LastEventSequence),
		snapshot.Run.CancelRequestedAt,
		snapshot.Run.CreatedAt,
		snapshot.Run.StartedAt,
		snapshot.Run.FinishedAt,
		createdBy,
	)
	if err != nil {
		return fmt.Errorf("insert workflow Run: %w", err)
	}
	for index, task := range snapshot.Run.Tasks {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_runs (
				run_id, task_key, task_index, status,
				remaining_dependencies, ready_at, finished_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, snapshot.Run.ID, task.Key, index, task.Status, snapshot.Run.RemainingDependencies[index], task.ReadyAt, task.FinishedAt)
		if err != nil {
			return fmt.Errorf("insert TaskRun %q: %w", task.Key, err)
		}
		for _, attempt := range task.Attempts {
			if err := insertAttempt(ctx, tx, snapshot.Run.ID, task.Key, index, attempt); err != nil {
				return err
			}
		}
	}
	for _, event := range snapshot.Events {
		if err := insertEvent(ctx, tx, snapshot.Run.ID, event); err != nil {
			return err
		}
	}
	return nil
}

// Apply 使用乐观 revision 控制提交一组行级 workflow.ChangeSet。
func (r *Repository) Apply(ctx context.Context, change workflow.ChangeSet) error {
	if err := validateChangeSetShape(change); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin applying ChangeSet: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := applyChangeSetTx(ctx, tx, change); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ChangeSet: %w", err)
	}
	return nil
}

func applyChangeSetTx(ctx context.Context, tx pgx.Tx, change workflow.ChangeSet) error {
	if err := validateChangeSetShape(change); err != nil {
		return err
	}
	expectedLastSequence := change.Run.LastEventSequence
	if len(change.Events) > 0 {
		expectedLastSequence = change.Events[0].Sequence - 1
	}
	result, err := tx.Exec(ctx, `
		UPDATE workflow_runs SET
			status = $2,
			revision = $3,
			last_event_sequence = $4,
			cancel_requested_at = $5,
			created_at = $6,
			started_at = $7,
			finished_at = $8
		WHERE run_id = $1 AND revision = $9 AND last_event_sequence = $10
	`,
		change.RunID,
		change.Run.Status,
		int64(change.Run.Revision),
		int64(change.Run.LastEventSequence),
		change.Run.CancelRequestedAt,
		change.Run.CreatedAt,
		change.Run.StartedAt,
		change.Run.FinishedAt,
		int64(change.ExpectedRevision),
		int64(expectedLastSequence),
	)
	if err != nil {
		return fmt.Errorf("update workflow Run: %w", err)
	}
	if result.RowsAffected() != 1 {
		if err := classifyRunUpdateMiss(ctx, tx, change.RunID, change.ExpectedRevision, expectedLastSequence); err != nil {
			return err
		}
		return workflow.ErrRevisionConflict
	}

	for _, task := range change.Tasks {
		result, err := tx.Exec(ctx, `
			UPDATE task_runs SET
				status = $4,
				remaining_dependencies = $5,
				ready_at = $6,
				finished_at = $7
			WHERE run_id = $1 AND task_index = $2 AND task_key = $3
		`, task.RunID, task.Index, task.Task.Key, task.Task.Status, task.RemainingDependencies, task.Task.ReadyAt, task.Task.FinishedAt)
		if err != nil {
			return fmt.Errorf("update TaskRun %q: %w", task.Task.Key, err)
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("%w: TaskRun %q does not match index %d", workflow.ErrInvalidChangeSet, task.Task.Key, task.Index)
		}
	}
	for _, attempt := range change.Attempts {
		switch attempt.Operation {
		case workflow.ChangeInsert:
			if err := insertAttempt(ctx, tx, attempt.RunID, attempt.TaskKey, attempt.TaskIndex, attempt.Attempt); err != nil {
				return err
			}
		case workflow.ChangeUpdate:
			result, err := tx.Exec(ctx, `
				UPDATE attempts SET
					status = $5,
					started_at = $6,
					finished_at = $7,
					output = $8,
					error_code = $9,
					error_message = $10,
					worker_id = $11,
					dispatch_id = $12
				WHERE run_id = $1 AND task_key = $2 AND attempt_number = $3
				AND EXISTS (
					SELECT 1 FROM task_runs
					WHERE run_id = $1 AND task_key = $2 AND task_index = $4
				)
			`,
				attempt.RunID,
				attempt.TaskKey,
				attempt.Attempt.Number,
				attempt.TaskIndex,
				attempt.Attempt.Status,
				attempt.Attempt.StartedAt,
				attempt.Attempt.FinishedAt,
				attempt.Attempt.Result.Output,
				attempt.Attempt.Result.ErrorCode,
				attempt.Attempt.Result.ErrorMessage,
				nullableText(attempt.Attempt.WorkerID),
				nullableText(attempt.Attempt.DispatchID),
			)
			if err != nil {
				return fmt.Errorf("update Attempt %s/%d: %w", attempt.TaskKey, attempt.Attempt.Number, err)
			}
			if result.RowsAffected() != 1 {
				return fmt.Errorf("%w: Attempt %s/%d does not exist", workflow.ErrInvalidChangeSet, attempt.TaskKey, attempt.Attempt.Number)
			}
		default:
			return fmt.Errorf("%w: unknown Attempt operation %q", workflow.ErrInvalidChangeSet, attempt.Operation)
		}
	}
	for _, event := range change.Events {
		if err := insertEvent(ctx, tx, change.RunID, event); err != nil {
			return err
		}
	}
	return nil
}

func validateChangeSetShape(change workflow.ChangeSet) error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", workflow.ErrInvalidChangeSet, fmt.Sprintf(format, args...))
	}
	if change.RunID == "" || change.Run == nil {
		return invalid("RunID and Run row are required")
	}
	if change.Run.ID != change.RunID {
		return invalid("Run row ID %q does not match ChangeSet RunID %q", change.Run.ID, change.RunID)
	}
	if change.Run.Revision != change.ExpectedRevision+1 {
		return invalid("Run revision %d must follow expected revision %d", change.Run.Revision, change.ExpectedRevision)
	}
	if len(change.Run.Tasks) != 0 || len(change.Run.RemainingDependencies) != 0 {
		return invalid("Run row must not embed TaskRun rows")
	}
	seenTasks := make(map[int]struct{}, len(change.Tasks))
	for _, task := range change.Tasks {
		if task.RunID != change.RunID || task.Index < 0 || task.Task.Key == "" || len(task.Task.Attempts) != 0 || task.RemainingDependencies < 0 {
			return invalid("invalid TaskRun change for key %q", task.Task.Key)
		}
		if _, exists := seenTasks[task.Index]; exists {
			return invalid("task index %d is changed more than once", task.Index)
		}
		seenTasks[task.Index] = struct{}{}
	}
	seenAttempts := make(map[string]struct{}, len(change.Attempts))
	for _, attempt := range change.Attempts {
		if attempt.RunID != change.RunID || attempt.TaskIndex < 0 || attempt.TaskKey == "" || attempt.Attempt.Number <= 0 {
			return invalid("invalid Attempt change for task %q", attempt.TaskKey)
		}
		key := fmt.Sprintf("%s/%d", attempt.TaskKey, attempt.Attempt.Number)
		if _, exists := seenAttempts[key]; exists {
			return invalid("Attempt %s is changed more than once", key)
		}
		seenAttempts[key] = struct{}{}
	}
	expectedSequence := change.Run.LastEventSequence
	if len(change.Events) > 0 {
		expectedSequence = change.Events[0].Sequence
		if expectedSequence == 0 {
			return invalid("event sequence must be positive")
		}
		for _, event := range change.Events {
			if event.Sequence != expectedSequence {
				return invalid("event sequence %d must be %d", event.Sequence, expectedSequence)
			}
			expectedSequence++
		}
		if change.Run.LastEventSequence != expectedSequence-1 {
			return invalid("Run last event sequence does not match appended events")
		}
	}
	return nil
}

func classifyRunUpdateMiss(ctx context.Context, tx pgx.Tx, id workflow.RunID, revision, lastSequence uint64) error {
	var storedRevision int64
	var storedSequence int64
	err := tx.QueryRow(ctx, "SELECT revision, last_event_sequence FROM workflow_runs WHERE run_id = $1", id).Scan(&storedRevision, &storedSequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.ErrRunNotFound
	}
	if err != nil {
		return fmt.Errorf("classify Run update: %w", err)
	}
	if uint64(storedRevision) != revision {
		return workflow.ErrRevisionConflict
	}
	if uint64(storedSequence) != lastSequence {
		return fmt.Errorf("%w: stored event sequence %d does not match expected %d", workflow.ErrInvalidChangeSet, storedSequence, lastSequence)
	}
	return nil
}

func insertAttempt(ctx context.Context, tx pgx.Tx, runID workflow.RunID, taskKey workflow.TaskKey, taskIndex int, attempt workflow.Attempt) error {
	result, err := tx.Exec(ctx, `
		INSERT INTO attempts (
			run_id, task_key, attempt_number, status, started_at,
			finished_at, output, error_code, error_message, worker_id, dispatch_id
		)
		SELECT $1, $2, $4, $5, $6, $7, $8, $9, $10, $11, $12
		FROM task_runs
		WHERE run_id = $1 AND task_key = $2 AND task_index = $3
		  AND $4 = COALESCE((SELECT MAX(attempt_number) + 1 FROM attempts WHERE run_id = $1 AND task_key = $2), 1)
	`,
		runID,
		taskKey,
		taskIndex,
		attempt.Number,
		attempt.Status,
		attempt.StartedAt,
		attempt.FinishedAt,
		attempt.Result.Output,
		attempt.Result.ErrorCode,
		attempt.Result.ErrorMessage,
		nullableText(attempt.WorkerID),
		nullableText(attempt.DispatchID),
	)
	if err != nil {
		return fmt.Errorf("insert Attempt %s/%d: %w", taskKey, attempt.Number, err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: Attempt task %q does not match index %d", workflow.ErrInvalidChangeSet, taskKey, taskIndex)
	}
	return nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func ensureDefinitionMatches(ctx context.Context, tx pgx.Tx, snapshot workflow.RunSnapshot) error {
	stored, err := tx.Query(ctx, `
		SELECT definition_json
		FROM workflow_versions
		WHERE workflow_id = $1 AND version = $2
	`, snapshot.Run.DefinitionID, snapshot.Run.DefinitionVersion)
	if err != nil {
		return fmt.Errorf("query Run definition version: %w", err)
	}
	defer stored.Close()
	if !stored.Next() {
		if err := stored.Err(); err != nil {
			return fmt.Errorf("read Run definition version: %w", err)
		}
		return ErrDefinitionNotFound
	}
	var body []byte
	if err := stored.Scan(&body); err != nil {
		return fmt.Errorf("scan Run definition version: %w", err)
	}
	var storedDefinition workflow.WorkflowDefinition
	if err := decodeJSONNumber(body, &storedDefinition); err != nil {
		return fmt.Errorf("decode stored Run definition: %w", err)
	}
	storedCompiled, err := workflow.Compile(storedDefinition)
	if err != nil {
		return fmt.Errorf("compile stored Run definition: %w", err)
	}
	wantedCompiled, err := workflow.Compile(*snapshot.Definition)
	if err != nil {
		return fmt.Errorf("compile Run definition: %w", err)
	}
	storedCanonical, err := json.Marshal(storedCompiled.Definition())
	if err != nil {
		return fmt.Errorf("encode stored Run definition: %w", err)
	}
	wantedCanonical, err := json.Marshal(wantedCompiled.Definition())
	if err != nil {
		return fmt.Errorf("encode Run definition: %w", err)
	}
	if string(storedCanonical) != string(wantedCanonical) {
		return fmt.Errorf("Run snapshot definition does not match workflow version")
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, runID workflow.RunID, event workflow.StateEvent) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO state_events (
			run_id, sequence, entity_type, entity_key,
			from_status, to_status, reason, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, runID, int64(event.Sequence), event.Entity, event.Key, event.From, event.To, event.Reason, event.At)
	if err != nil {
		return fmt.Errorf("insert StateEvent %d: %w", event.Sequence, err)
	}
	return nil
}

// Load 根据规范化的关系表记录重建一个 workflow.RunSnapshot。
func (r *Repository) Load(ctx context.Context, id workflow.RunID) (workflow.RunSnapshot, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("begin loading Run: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	snapshot, err := loadSnapshot(ctx, tx, id)
	if err != nil {
		return workflow.RunSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("commit loading Run: %w", err)
	}
	return snapshot, nil
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, id workflow.RunID) (workflow.RunSnapshot, error) {
	var snapshot workflow.RunSnapshot
	var definitionJSON []byte
	var revision int64
	var lastSequence int64
	err := tx.QueryRow(ctx, `
		SELECT
			r.snapshot_version, r.run_id, r.workflow_id, r.workflow_version,
			r.status, r.revision, r.last_event_sequence, r.cancel_requested_at,
			r.created_at, r.started_at, r.finished_at, v.definition_json
		FROM workflow_runs r
		JOIN workflow_versions v
			ON v.workflow_id = r.workflow_id AND v.version = r.workflow_version
		WHERE r.run_id = $1
	`, id).Scan(
		&snapshot.Version,
		&snapshot.Run.ID,
		&snapshot.Run.DefinitionID,
		&snapshot.Run.DefinitionVersion,
		&snapshot.Run.Status,
		&revision,
		&lastSequence,
		&snapshot.Run.CancelRequestedAt,
		&snapshot.Run.CreatedAt,
		&snapshot.Run.StartedAt,
		&snapshot.Run.FinishedAt,
		&definitionJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.RunSnapshot{}, workflow.ErrRunNotFound
	}
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("load workflow Run: %w", err)
	}
	if revision < 0 || lastSequence < 0 {
		return workflow.RunSnapshot{}, fmt.Errorf("stored Run has negative revision or event sequence")
	}
	snapshot.Run.Revision = uint64(revision)
	snapshot.Run.LastEventSequence = uint64(lastSequence)
	var definition workflow.WorkflowDefinition
	if err := decodeJSONNumber(definitionJSON, &definition); err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("decode stored workflow definition: %w", err)
	}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("compile stored workflow definition: %w", err)
	}
	definition = compiled.Definition()
	snapshot.Definition = &definition
	snapshot.Run.Tasks = make([]workflow.TaskRun, 0, len(definition.Tasks))
	snapshot.Run.RemainingDependencies = make([]int, 0, len(definition.Tasks))
	snapshot.Events = []workflow.StateEvent{}

	taskRows, err := tx.Query(ctx, `
		SELECT task_key, task_index, status, remaining_dependencies, ready_at, finished_at
		FROM task_runs
		WHERE run_id = $1
		ORDER BY task_index
	`, id)
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("query TaskRuns: %w", err)
	}
	for taskRows.Next() {
		var task workflow.TaskRun
		var index int
		var remaining int
		if err := taskRows.Scan(&task.Key, &index, &task.Status, &remaining, &task.ReadyAt, &task.FinishedAt); err != nil {
			taskRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("scan TaskRun: %w", err)
		}
		if index != len(snapshot.Run.Tasks) {
			taskRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("stored TaskRun index %d is not contiguous", index)
		}
		task.Attempts = []workflow.Attempt{}
		snapshot.Run.Tasks = append(snapshot.Run.Tasks, task)
		snapshot.Run.RemainingDependencies = append(snapshot.Run.RemainingDependencies, remaining)
	}
	if err := taskRows.Err(); err != nil {
		taskRows.Close()
		return workflow.RunSnapshot{}, fmt.Errorf("iterate TaskRuns: %w", err)
	}
	taskRows.Close()
	if len(snapshot.Run.Tasks) != len(definition.Tasks) {
		return workflow.RunSnapshot{}, fmt.Errorf("stored TaskRun count %d does not match definition count %d", len(snapshot.Run.Tasks), len(definition.Tasks))
	}

	attemptRows, err := tx.Query(ctx, `
		SELECT
			t.task_index, a.task_key, a.attempt_number, a.status,
			a.started_at, a.finished_at, a.output, a.error_code, a.error_message,
			COALESCE(a.worker_id, ''), COALESCE(a.dispatch_id, '')
		FROM attempts a
		JOIN task_runs t ON t.run_id = a.run_id AND t.task_key = a.task_key
		WHERE a.run_id = $1
		ORDER BY t.task_index, a.attempt_number
	`, id)
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("query Attempts: %w", err)
	}
	for attemptRows.Next() {
		var index int
		var taskKey workflow.TaskKey
		var attempt workflow.Attempt
		if err := attemptRows.Scan(
			&index,
			&taskKey,
			&attempt.Number,
			&attempt.Status,
			&attempt.StartedAt,
			&attempt.FinishedAt,
			&attempt.Result.Output,
			&attempt.Result.ErrorCode,
			&attempt.Result.ErrorMessage,
			&attempt.WorkerID,
			&attempt.DispatchID,
		); err != nil {
			attemptRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("scan Attempt: %w", err)
		}
		if index < 0 || index >= len(snapshot.Run.Tasks) || snapshot.Run.Tasks[index].Key != taskKey {
			attemptRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("stored Attempt task %q has invalid index %d", taskKey, index)
		}
		wantNumber := len(snapshot.Run.Tasks[index].Attempts) + 1
		if attempt.Number != wantNumber {
			attemptRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("stored Attempt %s/%d is not contiguous", taskKey, attempt.Number)
		}
		snapshot.Run.Tasks[index].Attempts = append(snapshot.Run.Tasks[index].Attempts, attempt)
	}
	if err := attemptRows.Err(); err != nil {
		attemptRows.Close()
		return workflow.RunSnapshot{}, fmt.Errorf("iterate Attempts: %w", err)
	}
	attemptRows.Close()

	eventRows, err := tx.Query(ctx, `
		SELECT sequence, occurred_at, entity_type, entity_key, from_status, to_status, reason
		FROM state_events
		WHERE run_id = $1
		ORDER BY sequence
	`, id)
	if err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("query StateEvents: %w", err)
	}
	for eventRows.Next() {
		var event workflow.StateEvent
		var sequence int64
		if err := eventRows.Scan(&sequence, &event.At, &event.Entity, &event.Key, &event.From, &event.To, &event.Reason); err != nil {
			eventRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("scan StateEvent: %w", err)
		}
		if sequence <= 0 || uint64(sequence) != uint64(len(snapshot.Events)+1) {
			eventRows.Close()
			return workflow.RunSnapshot{}, fmt.Errorf("stored StateEvent sequence %d is not contiguous", sequence)
		}
		event.Sequence = uint64(sequence)
		snapshot.Events = append(snapshot.Events, event)
	}
	if err := eventRows.Err(); err != nil {
		eventRows.Close()
		return workflow.RunSnapshot{}, fmt.Errorf("iterate StateEvents: %w", err)
	}
	eventRows.Close()
	if snapshot.Run.LastEventSequence != uint64(len(snapshot.Events)) {
		return workflow.RunSnapshot{}, fmt.Errorf("stored last event sequence %d does not match event count %d", snapshot.Run.LastEventSequence, len(snapshot.Events))
	}
	return snapshot, nil
}

// ListNonTerminal 按稳定创建顺序返回需要启动恢复的 Run ID。
func (r *Repository) ListNonTerminal(ctx context.Context) ([]workflow.RunID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT run_id
		FROM workflow_runs
		WHERE status NOT IN ('succeeded', 'failed', 'canceled')
		ORDER BY created_at, run_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query non-terminal Runs: %w", err)
	}
	defer rows.Close()
	ids := make([]workflow.RunID, 0)
	for rows.Next() {
		var id workflow.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan non-terminal Run: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate non-terminal Runs: %w", err)
	}
	return ids, nil
}

// RequestCancel 为非终态 Run 记录第一次取消请求。
func (r *Repository) RequestCancel(ctx context.Context, id workflow.RunID, at time.Time) (workflow.WorkflowRun, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return workflow.WorkflowRun{}, fmt.Errorf("begin cancellation request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status workflow.WorkflowStatus
	var requestedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT status, cancel_requested_at
		FROM workflow_runs
		WHERE run_id = $1
		FOR UPDATE
	`, id).Scan(&status, &requestedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflow.WorkflowRun{}, workflow.ErrRunNotFound
		}
		return workflow.WorkflowRun{}, fmt.Errorf("lock Run for cancellation: %w", err)
	}
	if !workflow.IsWorkflowTerminalForStore(status) && requestedAt == nil {
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_runs
			SET cancel_requested_at = $2, revision = revision + 1
			WHERE run_id = $1
		`, id, at); err != nil {
			return workflow.WorkflowRun{}, fmt.Errorf("record cancellation request: %w", err)
		}
	}
	snapshot, err := loadSnapshot(ctx, tx, id)
	if err != nil {
		return workflow.WorkflowRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return workflow.WorkflowRun{}, fmt.Errorf("commit cancellation request: %w", err)
	}
	return snapshot.Run, nil
}

// 编译期接口检查用于明确约束 Engine 的持久化边界。
var _ workflow.Persistence = (*Repository)(nil)
