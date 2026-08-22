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

// CreateDefinition atomically creates a logical Workflow, version 1, and its idempotency record.
func (r *Repository) CreateDefinition(
	ctx context.Context,
	definition workflow.WorkflowDefinition,
	principal, idemKey, requestHash string,
) (DefinitionRecord, error) {
	if err := validateIdempotencyInput(principal, idemKey, requestHash); err != nil {
		return DefinitionRecord{}, err
	}
	body, err := json.Marshal(definition)
	if err != nil {
		return DefinitionRecord{}, fmt.Errorf("encode workflow definition: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return DefinitionRecord{}, fmt.Errorf("begin create definition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdempotency(ctx, tx, principal, operationCreateWorkflow, idemKey); err != nil {
		return DefinitionRecord{}, err
	}

	if record, found, err := loadIdempotency(ctx, tx, principal, operationCreateWorkflow, idemKey); err != nil {
		return DefinitionRecord{}, err
	} else if found {
		if err := validateReplay(record, requestHash, resourceWorkflowVersion); err != nil {
			return DefinitionRecord{}, err
		}
		if record.resourceVersion == nil {
			return DefinitionRecord{}, fmt.Errorf("stored workflow idempotency record has no version")
		}
		return DefinitionRecord{WorkflowID: record.resourceID, Version: *record.resourceVersion}, nil
	}

	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflows (id, latest_version, created_at, created_by)
		VALUES ($1, 1, $2, $3)
	`, definition.ID, now, principal); err != nil {
		if isUniqueViolation(err, "workflows_pkey") {
			return DefinitionRecord{}, ErrWorkflowExists
		}
		return DefinitionRecord{}, fmt.Errorf("insert workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflow_versions (
			workflow_id, version, definition_schema_version, definition_json,
			definition_hash, created_at, created_by
		) VALUES ($1, 1, 1, $2, $3, $4, $5)
	`, definition.ID, body, requestHash, now, principal); err != nil {
		return DefinitionRecord{}, fmt.Errorf("insert workflow version: %w", err)
	}
	version := 1
	if err := insertIdempotency(
		ctx, tx, principal, operationCreateWorkflow, idemKey, requestHash,
		resourceWorkflowVersion, definition.ID, &version,
	); err != nil {
		return DefinitionRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DefinitionRecord{}, fmt.Errorf("commit create definition: %w", err)
	}
	return DefinitionRecord{WorkflowID: definition.ID, Version: version}, nil
}

// CreateVersion atomically increments latest_version and stores an immutable definition.
func (r *Repository) CreateVersion(
	ctx context.Context,
	workflowID string,
	definition workflow.WorkflowDefinition,
	principal, idemKey, requestHash string,
) (DefinitionRecord, error) {
	if definition.ID != workflowID {
		return DefinitionRecord{}, fmt.Errorf("definition ID %q does not match workflow ID %q", definition.ID, workflowID)
	}
	if err := validateIdempotencyInput(principal, idemKey, requestHash); err != nil {
		return DefinitionRecord{}, err
	}
	body, err := json.Marshal(definition)
	if err != nil {
		return DefinitionRecord{}, fmt.Errorf("encode workflow definition: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return DefinitionRecord{}, fmt.Errorf("begin create version: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockIdempotency(ctx, tx, principal, operationCreateVersion, idemKey); err != nil {
		return DefinitionRecord{}, err
	}

	if record, found, err := loadIdempotency(ctx, tx, principal, operationCreateVersion, idemKey); err != nil {
		return DefinitionRecord{}, err
	} else if found {
		if err := validateReplay(record, requestHash, resourceWorkflowVersion); err != nil {
			return DefinitionRecord{}, err
		}
		if record.resourceVersion == nil {
			return DefinitionRecord{}, fmt.Errorf("stored workflow idempotency record has no version")
		}
		return DefinitionRecord{WorkflowID: record.resourceID, Version: *record.resourceVersion}, nil
	}

	var latest int
	if err := tx.QueryRow(ctx, "SELECT latest_version FROM workflows WHERE id = $1 FOR UPDATE", workflowID).Scan(&latest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DefinitionRecord{}, ErrWorkflowNotFound
		}
		return DefinitionRecord{}, fmt.Errorf("lock workflow: %w", err)
	}
	version := latest + 1
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, "UPDATE workflows SET latest_version = $2 WHERE id = $1", workflowID, version); err != nil {
		return DefinitionRecord{}, fmt.Errorf("update latest workflow version: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflow_versions (
			workflow_id, version, definition_schema_version, definition_json,
			definition_hash, created_at, created_by
		) VALUES ($1, $2, 1, $3, $4, $5, $6)
	`, workflowID, version, body, requestHash, now, principal); err != nil {
		return DefinitionRecord{}, fmt.Errorf("insert workflow version: %w", err)
	}
	if err := insertIdempotency(
		ctx, tx, principal, operationCreateVersion, idemKey, requestHash,
		resourceWorkflowVersion, workflowID, &version,
	); err != nil {
		return DefinitionRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DefinitionRecord{}, fmt.Errorf("commit create version: %w", err)
	}
	return DefinitionRecord{WorkflowID: workflowID, Version: version}, nil
}

// LoadDefinition returns one exact immutable workflow definition version.
func (r *Repository) LoadDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	var body []byte
	err := r.pool.QueryRow(ctx, `
		SELECT definition_json
		FROM workflow_versions
		WHERE workflow_id = $1 AND version = $2
	`, workflowID, version).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.WorkflowDefinition{}, ErrDefinitionNotFound
	}
	if err != nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("load workflow definition: %w", err)
	}
	var definition workflow.WorkflowDefinition
	if err := json.Unmarshal(body, &definition); err != nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("decode workflow definition: %w", err)
	}
	return definition, nil
}
