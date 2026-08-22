package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5"
)

func (r *Repository) LoadWorkflowSummary(ctx context.Context, id string) (WorkflowRecord, error) {
	var record WorkflowRecord
	err := r.pool.QueryRow(ctx, `
		SELECT id, latest_version, created_at, created_by
		FROM workflows WHERE id = $1
	`, id).Scan(&record.WorkflowID, &record.LatestVersion, &record.CreatedAt, &record.CreatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowRecord{}, ErrWorkflowNotFound
	}
	if err != nil {
		return WorkflowRecord{}, fmt.Errorf("load workflow summary: %w", err)
	}
	return record, nil
}

func (r *Repository) ListWorkflows(ctx context.Context, afterID string, limit int) ([]WorkflowRecord, bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, latest_version, created_at, created_by
		FROM workflows WHERE id > $1 ORDER BY id LIMIT $2
	`, afterID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("query workflows: %w", err)
	}
	defer rows.Close()
	items := make([]WorkflowRecord, 0, limit+1)
	for rows.Next() {
		var item WorkflowRecord
		if err := rows.Scan(&item.WorkflowID, &item.LatestVersion, &item.CreatedAt, &item.CreatedBy); err != nil {
			return nil, false, fmt.Errorf("scan workflow: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate workflows: %w", err)
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

func (r *Repository) ListVersions(ctx context.Context, workflowID string, afterVersion, limit int) ([]VersionRecord, bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT workflow_id, version, created_at, created_by
		FROM workflow_versions
		WHERE workflow_id = $1 AND version > $2
		ORDER BY version LIMIT $3
	`, workflowID, afterVersion, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("query workflow versions: %w", err)
	}
	defer rows.Close()
	items := make([]VersionRecord, 0, limit+1)
	for rows.Next() {
		var item VersionRecord
		if err := rows.Scan(&item.WorkflowID, &item.Version, &item.CreatedAt, &item.CreatedBy); err != nil {
			return nil, false, fmt.Errorf("scan workflow version: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate workflow versions: %w", err)
	}
	if len(items) == 0 {
		if err := r.ensureWorkflowExists(ctx, workflowID); err != nil {
			return nil, false, err
		}
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

func (r *Repository) ListRunSummaries(ctx context.Context, query RunQuery, limit int) ([]RunRecord, bool, error) {
	if (query.AfterCreated == nil) != (query.AfterRunID == "") {
		return nil, false, fmt.Errorf("Run keyset cursor requires both created_at and run_id")
	}
	rows, err := r.pool.Query(ctx, `
		SELECT r.run_id, r.workflow_id, r.workflow_version, r.status,
		       r.revision, r.created_at, r.started_at, r.finished_at,
		       (SELECT count(*) FROM task_runs t WHERE t.run_id = r.run_id)
		FROM workflow_runs r
		WHERE ($1 = '' OR r.workflow_id = $1)
		  AND ($2 = '' OR r.status = $2)
		  AND ($3::timestamptz IS NULL OR (r.created_at, r.run_id) > ($3, $4))
		ORDER BY r.created_at, r.run_id
		LIMIT $5
	`, query.WorkflowID, query.Status, query.AfterCreated, query.AfterRunID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("query Run summaries: %w", err)
	}
	defer rows.Close()
	items := make([]RunRecord, 0, limit+1)
	for rows.Next() {
		var item RunRecord
		var revision int64
		if err := rows.Scan(&item.ID, &item.DefinitionID, &item.DefinitionVersion, &item.Status, &revision, &item.CreatedAt, &item.StartedAt, &item.FinishedAt, &item.TaskCount); err != nil {
			return nil, false, fmt.Errorf("scan Run summary: %w", err)
		}
		if revision < 0 {
			return nil, false, fmt.Errorf("stored Run has negative revision")
		}
		item.Revision = uint64(revision)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate Run summaries: %w", err)
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

// LoadRun reads only the workflow_runs row and task count, without Attempt history.
func (r *Repository) LoadRun(ctx context.Context, id workflow.RunID) (workflow.WorkflowRun, error) {
	var run workflow.WorkflowRun
	var revision int64
	var lastSequence int64
	var taskCount int
	err := r.pool.QueryRow(ctx, `
		SELECT
			r.run_id, r.workflow_id, r.workflow_version, r.status,
			r.revision, r.last_event_sequence, r.cancel_requested_at,
			r.created_at, r.started_at, r.finished_at,
			(SELECT count(*) FROM task_runs t WHERE t.run_id = r.run_id)
		FROM workflow_runs r
		WHERE r.run_id = $1
	`, id).Scan(
		&run.ID,
		&run.DefinitionID,
		&run.DefinitionVersion,
		&run.Status,
		&revision,
		&lastSequence,
		&run.CancelRequestedAt,
		&run.CreatedAt,
		&run.StartedAt,
		&run.FinishedAt,
		&taskCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.WorkflowRun{}, workflow.ErrRunNotFound
	}
	if err != nil {
		return workflow.WorkflowRun{}, fmt.Errorf("load Run summary: %w", err)
	}
	if revision < 0 || lastSequence < 0 {
		return workflow.WorkflowRun{}, fmt.Errorf("stored Run has negative revision or event sequence")
	}
	run.Revision = uint64(revision)
	run.LastEventSequence = uint64(lastSequence)
	// 只保留任务数量，不读取 TaskRun 行或 Attempt 历史；应用层据此生成 TaskCount。
	run.Tasks = make([]workflow.TaskRun, taskCount)
	return run, nil
}

// ListTaskRuns reads one stable task_index page without Attempt history.
func (r *Repository) ListTaskRuns(ctx context.Context, id workflow.RunID, afterIndex, limit int) ([]TaskRecord, bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT task_index, task_key, status, ready_at, finished_at
		FROM task_runs
		WHERE run_id = $1 AND task_index > $2
		ORDER BY task_index
		LIMIT $3
	`, id, afterIndex, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("query TaskRun page: %w", err)
	}
	defer rows.Close()
	items := make([]TaskRecord, 0, limit+1)
	for rows.Next() {
		var item TaskRecord
		if err := rows.Scan(&item.Index, &item.Task.Key, &item.Task.Status, &item.Task.ReadyAt, &item.Task.FinishedAt); err != nil {
			return nil, false, fmt.Errorf("scan TaskRun page: %w", err)
		}
		item.Task.Attempts = []workflow.Attempt{}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate TaskRun page: %w", err)
	}
	if len(items) == 0 {
		if err := r.ensureRunExists(ctx, id); err != nil {
			return nil, false, err
		}
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

// LoadTaskRun reads one task and its ordered Attempt history.
func (r *Repository) LoadTaskRun(ctx context.Context, id workflow.RunID, key workflow.TaskKey) (workflow.TaskRun, error) {
	var task workflow.TaskRun
	err := r.pool.QueryRow(ctx, `
		SELECT task_key, status, ready_at, finished_at
		FROM task_runs
		WHERE run_id = $1 AND task_key = $2
	`, id, key).Scan(&task.Key, &task.Status, &task.ReadyAt, &task.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := r.ensureRunExists(ctx, id); err != nil {
			return workflow.TaskRun{}, err
		}
		return workflow.TaskRun{}, ErrTaskNotFound
	}
	if err != nil {
		return workflow.TaskRun{}, fmt.Errorf("load TaskRun: %w", err)
	}
	task.Attempts = []workflow.Attempt{}
	rows, err := r.pool.Query(ctx, `
		SELECT attempt_number, status, started_at, finished_at, output, error_code, error_message
		FROM attempts
		WHERE run_id = $1 AND task_key = $2
		ORDER BY attempt_number
	`, id, key)
	if err != nil {
		return workflow.TaskRun{}, fmt.Errorf("query task Attempts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var attempt workflow.Attempt
		if err := rows.Scan(&attempt.Number, &attempt.Status, &attempt.StartedAt, &attempt.FinishedAt, &attempt.Result.Output, &attempt.Result.ErrorCode, &attempt.Result.ErrorMessage); err != nil {
			return workflow.TaskRun{}, fmt.Errorf("scan task Attempt: %w", err)
		}
		task.Attempts = append(task.Attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return workflow.TaskRun{}, fmt.Errorf("iterate task Attempts: %w", err)
	}
	return task, nil
}

// ListStateEvents reads events after one sequence and reports whether another page exists.
func (r *Repository) ListStateEvents(ctx context.Context, id workflow.RunID, after uint64, limit int) ([]workflow.StateEvent, bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT sequence, occurred_at, entity_type, entity_key, from_status, to_status, reason
		FROM state_events
		WHERE run_id = $1 AND sequence > $2
		ORDER BY sequence
		LIMIT $3
	`, id, int64(after), limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("query StateEvent page: %w", err)
	}
	defer rows.Close()
	items := make([]workflow.StateEvent, 0, limit+1)
	for rows.Next() {
		var event workflow.StateEvent
		var sequence int64
		if err := rows.Scan(&sequence, &event.At, &event.Entity, &event.Key, &event.From, &event.To, &event.Reason); err != nil {
			return nil, false, fmt.Errorf("scan StateEvent page: %w", err)
		}
		if sequence <= 0 {
			return nil, false, fmt.Errorf("stored StateEvent has invalid sequence %d", sequence)
		}
		event.Sequence = uint64(sequence)
		items = append(items, event)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate StateEvent page: %w", err)
	}
	if len(items) == 0 {
		if err := r.ensureRunExists(ctx, id); err != nil {
			return nil, false, err
		}
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return items, more, nil
}

func (r *Repository) ensureRunExists(ctx context.Context, id workflow.RunID) error {
	var exists bool
	if err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = $1)", id).Scan(&exists); err != nil {
		return fmt.Errorf("check Run existence: %w", err)
	}
	if !exists {
		return workflow.ErrRunNotFound
	}
	return nil
}

func (r *Repository) ensureWorkflowExists(ctx context.Context, id string) error {
	var exists bool
	if err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM workflows WHERE id = $1)", id).Scan(&exists); err != nil {
		return fmt.Errorf("check workflow existence: %w", err)
	}
	if !exists {
		return ErrWorkflowNotFound
	}
	return nil
}
