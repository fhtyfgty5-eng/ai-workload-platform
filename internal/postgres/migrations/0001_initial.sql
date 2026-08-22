CREATE TABLE schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE workflows (
    id TEXT PRIMARY KEY,
    latest_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL
);

CREATE TABLE workflow_versions (
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL,
    definition_schema_version INTEGER NOT NULL,
    definition_json JSONB NOT NULL,
    definition_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL,
    PRIMARY KEY (workflow_id, version)
);

CREATE TABLE workflow_runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    revision BIGINT NOT NULL,
    last_event_sequence BIGINT NOT NULL,
    cancel_requested_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_by TEXT,
    FOREIGN KEY (workflow_id, workflow_version)
        REFERENCES workflow_versions(workflow_id, version) ON DELETE RESTRICT
);

CREATE TABLE task_runs (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    task_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    remaining_dependencies INTEGER NOT NULL,
    ready_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, task_key),
    UNIQUE (run_id, task_index)
);

CREATE TABLE attempts (
    run_id TEXT NOT NULL,
    task_key TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    output TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, task_key, attempt_number),
    FOREIGN KEY (run_id, task_key)
        REFERENCES task_runs(run_id, task_key) ON DELETE CASCADE
);

CREATE TABLE state_events (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE TABLE idempotency_records (
    principal_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    resource_version INTEGER,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (principal_id, operation, idempotency_key)
);
