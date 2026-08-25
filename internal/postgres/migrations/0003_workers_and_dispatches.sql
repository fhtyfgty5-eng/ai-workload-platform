ALTER TABLE task_runs
    DROP CONSTRAINT task_runs_status_valid,
    ADD CONSTRAINT task_runs_status_valid CHECK (status IN (
        'waiting_dependencies', 'ready', 'queued', 'running', 'waiting_retry',
        'succeeded', 'failed', 'canceled', 'skipped'
    ));

CREATE TABLE worker_sessions (
    worker_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    executor_kinds TEXT[] NOT NULL,
    max_concurrency INTEGER NOT NULL,
    status TEXT NOT NULL,
    session_token_hash BYTEA NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,
    CONSTRAINT worker_sessions_id_length CHECK (length(worker_id) BETWEEN 1 AND 128),
    CONSTRAINT worker_sessions_display_name_length CHECK (length(display_name) BETWEEN 1 AND 128),
    CONSTRAINT worker_sessions_protocol_positive CHECK (protocol_version > 0),
    CONSTRAINT worker_sessions_executors_nonempty CHECK (cardinality(executor_kinds) BETWEEN 1 AND 16),
    CONSTRAINT worker_sessions_concurrency_range CHECK (max_concurrency BETWEEN 1 AND 1024),
    CONSTRAINT worker_sessions_status_valid CHECK (status IN ('active', 'draining', 'offline', 'stopped')),
    CONSTRAINT worker_sessions_token_hash_length CHECK (octet_length(session_token_hash) = 32),
    CONSTRAINT worker_sessions_token_hash_unique UNIQUE (session_token_hash),
    CONSTRAINT worker_sessions_heartbeat_order CHECK (last_heartbeat_at >= registered_at),
    CONSTRAINT worker_sessions_stopped_order CHECK (stopped_at IS NULL OR stopped_at >= registered_at),
    CONSTRAINT worker_sessions_stopped_state CHECK ((status = 'stopped') = (stopped_at IS NOT NULL))
);

CREATE TABLE task_dispatches (
    dispatch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    task_key TEXT NOT NULL,
    executor_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT REFERENCES worker_sessions(worker_id) ON DELETE RESTRICT,
    attempt_number INTEGER,
    lease_token_hash BYTEA,
    lease_expires_at TIMESTAMPTZ,
    attempt_deadline TIMESTAMPTZ,
    result_hash BYTEA,
    created_at TIMESTAMPTZ NOT NULL,
    leased_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (run_id, task_key)
        REFERENCES task_runs(run_id, task_key) ON DELETE CASCADE,
    CONSTRAINT task_dispatches_id_length CHECK (length(dispatch_id) BETWEEN 1 AND 128),
    CONSTRAINT task_dispatches_executor_nonempty CHECK (length(executor_kind) > 0),
    CONSTRAINT task_dispatches_status_valid CHECK (status IN ('pending', 'leased', 'completed', 'expired', 'canceled')),
    CONSTRAINT task_dispatches_attempt_positive CHECK (attempt_number IS NULL OR attempt_number > 0),
    CONSTRAINT task_dispatches_lease_hash_length CHECK (lease_token_hash IS NULL OR octet_length(lease_token_hash) = 32),
    CONSTRAINT task_dispatches_result_hash_length CHECK (result_hash IS NULL OR octet_length(result_hash) = 32),
    CONSTRAINT task_dispatches_ownership_complete CHECK (
        (worker_id IS NULL AND attempt_number IS NULL AND lease_token_hash IS NULL AND
         lease_expires_at IS NULL AND attempt_deadline IS NULL AND leased_at IS NULL)
        OR
        (worker_id IS NOT NULL AND attempt_number IS NOT NULL AND lease_token_hash IS NOT NULL AND
         lease_expires_at IS NOT NULL AND attempt_deadline IS NOT NULL AND leased_at IS NOT NULL)
    ),
    CONSTRAINT task_dispatches_status_shape CHECK (
        (status = 'pending' AND worker_id IS NULL AND completed_at IS NULL AND result_hash IS NULL)
        OR (status = 'leased' AND worker_id IS NOT NULL AND completed_at IS NULL AND result_hash IS NULL)
        OR (status = 'completed' AND worker_id IS NOT NULL AND completed_at IS NOT NULL AND result_hash IS NOT NULL)
        OR (status = 'expired' AND worker_id IS NOT NULL AND completed_at IS NOT NULL AND result_hash IS NULL)
        OR (status = 'canceled' AND completed_at IS NOT NULL AND result_hash IS NULL)
    ),
    CONSTRAINT task_dispatches_lease_deadline_order CHECK (
        lease_expires_at IS NULL OR lease_expires_at <= attempt_deadline
    ),
    CONSTRAINT task_dispatches_time_order CHECK (
        (leased_at IS NULL OR leased_at >= created_at) AND
        (completed_at IS NULL OR completed_at >= created_at)
    )
);

CREATE UNIQUE INDEX task_dispatches_active_task_unique
    ON task_dispatches (run_id, task_key)
    WHERE status IN ('pending', 'leased');
CREATE UNIQUE INDEX task_dispatches_lease_token_unique
    ON task_dispatches (lease_token_hash)
    WHERE lease_token_hash IS NOT NULL;
CREATE UNIQUE INDEX task_dispatches_attempt_unique
    ON task_dispatches (run_id, task_key, attempt_number)
    WHERE attempt_number IS NOT NULL;
CREATE INDEX task_dispatches_pending_match_idx
    ON task_dispatches (executor_kind, created_at, dispatch_id)
    WHERE status = 'pending';
CREATE INDEX task_dispatches_worker_lease_idx
    ON task_dispatches (worker_id, lease_expires_at, dispatch_id)
    WHERE status = 'leased';
CREATE INDEX task_dispatches_expiry_idx
    ON task_dispatches (lease_expires_at, dispatch_id)
    WHERE status = 'leased';
CREATE INDEX worker_sessions_status_heartbeat_idx
    ON worker_sessions (status, last_heartbeat_at, worker_id);

ALTER TABLE attempts
    ADD COLUMN worker_id TEXT REFERENCES worker_sessions(worker_id) ON DELETE RESTRICT,
    ADD COLUMN dispatch_id TEXT REFERENCES task_dispatches(dispatch_id) ON DELETE RESTRICT,
    ADD CONSTRAINT attempts_distributed_ownership_complete CHECK (
        (worker_id IS NULL AND dispatch_id IS NULL) OR
        (worker_id IS NOT NULL AND dispatch_id IS NOT NULL)
    );

CREATE UNIQUE INDEX attempts_dispatch_unique
    ON attempts (dispatch_id)
    WHERE dispatch_id IS NOT NULL;
