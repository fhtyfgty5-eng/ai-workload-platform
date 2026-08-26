export type Role = "viewer" | "operator";

export type Session = {
  baseUrl: string;
  token: string;
  role: Role;
};

export type WorkflowStatus = "pending" | "running" | "succeeded" | "failed" | "canceled";
export type TaskStatus = "waiting_dependencies" | "ready" | "queued" | "running" | "waiting_retry" | "succeeded" | "failed" | "canceled" | "skipped";
export type AttemptStatus = "running" | "succeeded" | "failed" | "timed_out" | "canceled" | "interrupted";

export type RetryPolicy = { max_attempts?: number; interval_ms?: number };
export type TaskDefinition = {
  key: string;
  executor?: "mock" | "container";
  action: string;
  input?: Record<string, unknown>;
  depends_on?: string[];
  retry?: RetryPolicy;
  timeout_ms: number;
};
export type WorkflowDefinition = { id: string; concurrency: number; tasks: TaskDefinition[] };

export type Evidence = { statement: string; source: string };
export type Question = { id: string; text: string; answer?: string; resolved: boolean };
export type ValidationIssue = { code: string; path?: string; message: string };
export type ValidationReport = { errors: ValidationIssue[]; warnings: ValidationIssue[] };
export type ToolCallRecord = { call_id: string; name: string; result: string };
export type DraftStatus = "generated" | "validated" | "needs_confirmation" | "confirmed" | "rejected";
export type WorkflowDraft = {
  draft_id: string;
  goal: string;
  definition: WorkflowDefinition;
  facts: Evidence[];
  assumptions: Evidence[];
  questions: Question[];
  validation: ValidationReport;
  tool_calls: ToolCallRecord[];
  status: DraftStatus;
  content_hash: string;
  created_at: string;
  confirmed_at?: string;
};

export type DefinitionRef = { workflow_id: string; version: number };
export type WorkflowSummary = { workflow_id: string; latest_version: number; created_at: string; created_by: string };
export type WorkflowPage = { items: WorkflowSummary[]; next_cursor?: string };
export type RunSummary = {
  run_id: string;
  workflow_id: string;
  workflow_version: number;
  status: WorkflowStatus;
  revision: number;
  task_count: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
};
export type RunPage = { items: RunSummary[]; next_cursor?: string };
export type TaskSummary = { key: string; status: TaskStatus; ready_at?: string; finished_at?: string };
export type TaskPage = { items: TaskSummary[]; next_cursor?: string };
export type Attempt = { number: number; status: AttemptStatus; worker_id?: string; dispatch_id?: string; started_at: string; finished_at?: string; result: { output?: string; error_code?: string; error_message?: string } };
export type TaskRun = { key: string; status: TaskStatus; attempts: Attempt[]; ready_at?: string; finished_at?: string };
export type TaskDetail = { task: TaskRun };
export type StateEvent = { sequence: number; at: string; entity: "workflow" | "task" | "attempt"; key: string; from: string; to: string; reason?: string };
export type EventPage = { items: StateEvent[]; next_cursor?: string };
export type WorkerSummary = { worker_id: string; display_name: string; protocol_version: number; executor_kinds: ("mock" | "container")[]; max_concurrency: number; status: string; active_leases: number; registered_at: string; last_heartbeat_at: string; stopped_at?: string };
export type WorkerPage = { items: WorkerSummary[]; next_cursor?: string };
export type StartRunResponse = { run_id: string; status: "pending" };
export type CancelRunResponse = { run_id: string; status: WorkflowStatus; cancel_requested_at: string };
