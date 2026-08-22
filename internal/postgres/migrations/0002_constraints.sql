ALTER TABLE workflows
    ADD CONSTRAINT workflows_id_format CHECK (id ~ '^[a-z0-9_-]{1,64}$'),
    ADD CONSTRAINT workflows_latest_version_positive CHECK (latest_version > 0),
    ADD CONSTRAINT workflows_created_by_nonempty CHECK (length(created_by) > 0);

ALTER TABLE workflow_versions
    ADD CONSTRAINT workflow_versions_version_positive CHECK (version > 0),
    ADD CONSTRAINT workflow_versions_schema_version_positive CHECK (definition_schema_version > 0),
    ADD CONSTRAINT workflow_versions_hash_nonempty CHECK (length(definition_hash) > 0),
    ADD CONSTRAINT workflow_versions_created_by_nonempty CHECK (length(created_by) > 0);

ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_version_positive CHECK (workflow_version > 0),
    ADD CONSTRAINT workflow_runs_snapshot_version_positive CHECK (snapshot_version > 0),
    ADD CONSTRAINT workflow_runs_status_valid CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
    ADD CONSTRAINT workflow_runs_revision_nonnegative CHECK (revision >= 0),
    ADD CONSTRAINT workflow_runs_sequence_nonnegative CHECK (last_event_sequence >= 0);

ALTER TABLE task_runs
    ADD CONSTRAINT task_runs_index_nonnegative CHECK (task_index >= 0),
    ADD CONSTRAINT task_runs_remaining_nonnegative CHECK (remaining_dependencies >= 0),
    ADD CONSTRAINT task_runs_status_valid CHECK (status IN ('waiting_dependencies', 'ready', 'running', 'waiting_retry', 'succeeded', 'failed', 'canceled', 'skipped'));

ALTER TABLE attempts
    ADD CONSTRAINT attempts_number_positive CHECK (attempt_number > 0),
    ADD CONSTRAINT attempts_status_valid CHECK (status IN ('running', 'succeeded', 'failed', 'timed_out', 'canceled', 'interrupted'));

ALTER TABLE state_events
    ADD CONSTRAINT state_events_sequence_positive CHECK (sequence > 0),
    ADD CONSTRAINT state_events_entity_valid CHECK (entity_type IN ('workflow', 'task', 'attempt'));

ALTER TABLE idempotency_records
    ADD CONSTRAINT idempotency_principal_nonempty CHECK (length(principal_id) > 0),
    ADD CONSTRAINT idempotency_operation_nonempty CHECK (length(operation) > 0),
    ADD CONSTRAINT idempotency_key_nonempty CHECK (length(idempotency_key) > 0),
    ADD CONSTRAINT idempotency_hash_nonempty CHECK (length(request_hash) > 0),
    ADD CONSTRAINT idempotency_resource_type_nonempty CHECK (length(resource_type) > 0),
    ADD CONSTRAINT idempotency_resource_id_nonempty CHECK (length(resource_id) > 0);

CREATE INDEX workflow_runs_recovery_idx
    ON workflow_runs (status, created_at, run_id);
CREATE INDEX workflow_runs_definition_idx
    ON workflow_runs (workflow_id, workflow_version);
CREATE INDEX workflow_runs_cancel_requested_idx
    ON workflow_runs (cancel_requested_at)
    WHERE cancel_requested_at IS NOT NULL;
CREATE INDEX task_runs_ready_idx
    ON task_runs (run_id, ready_at)
    WHERE status = 'waiting_retry';
CREATE INDEX attempts_task_idx
    ON attempts (run_id, task_key, attempt_number);
CREATE INDEX state_events_occurred_idx
    ON state_events (run_id, occurred_at, sequence);
