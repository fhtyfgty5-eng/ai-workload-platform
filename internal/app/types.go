package app

import (
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// DefinitionRef identifies one immutable workflow version exposed by the control plane.
type DefinitionRef struct {
	WorkflowID string `json:"workflow_id"`
	Version    int    `json:"version"`
}

// WorkflowSummary is the bounded representation returned by workflow list/detail APIs.
type WorkflowSummary struct {
	WorkflowID    string    `json:"workflow_id"`
	LatestVersion int       `json:"latest_version"`
	CreatedAt     time.Time `json:"created_at"`
	CreatedBy     string    `json:"created_by"`
}

type WorkflowPage struct {
	Items      []WorkflowSummary `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type VersionSummary struct {
	WorkflowID string    `json:"workflow_id"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	CreatedBy  string    `json:"created_by"`
}

type VersionPage struct {
	Items      []VersionSummary `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// StartRunResponse is returned after a pending Run has been durably created.
type StartRunResponse struct {
	RunID  workflow.RunID          `json:"run_id"`
	Status workflow.WorkflowStatus `json:"status"`
}

// RunSummary omits Attempt history so list and summary responses remain bounded.
type RunSummary struct {
	ID                workflow.RunID          `json:"run_id"`
	DefinitionID      string                  `json:"workflow_id"`
	DefinitionVersion int                     `json:"workflow_version"`
	Status            workflow.WorkflowStatus `json:"status"`
	Revision          uint64                  `json:"revision"`
	TaskCount         int                     `json:"task_count"`
	CreatedAt         time.Time               `json:"created_at"`
	StartedAt         *time.Time              `json:"started_at,omitempty"`
	FinishedAt        *time.Time              `json:"finished_at,omitempty"`
}

type RunPage struct {
	Items      []RunSummary `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// RunListOptions 是 Run 列表的分页和过滤输入；空过滤值表示不过滤。
type RunListOptions struct {
	Cursor     string
	Limit      int
	WorkflowID string
	Status     string
}

// CancelRunResponse 是取消接口的有界响应，不包含 Task 或 Attempt 历史。
type CancelRunResponse struct {
	RunID             workflow.RunID          `json:"run_id"`
	Status            workflow.WorkflowStatus `json:"status"`
	CancelRequestedAt time.Time               `json:"cancel_requested_at"`
}

// TaskSummary is the bounded representation used by task list pages.
type TaskSummary struct {
	Key        workflow.TaskKey    `json:"key"`
	Status     workflow.TaskStatus `json:"status"`
	ReadyAt    *time.Time          `json:"ready_at,omitempty"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
}

// TaskDetail includes the complete Attempt history for one requested task.
type TaskDetail struct {
	Task workflow.TaskRun `json:"task"`
}

// TaskPage is a stable task-index page.
type TaskPage struct {
	Items      []TaskSummary `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// EventPage is a stable event-sequence page.
type EventPage struct {
	Items      []workflow.StateEvent `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}
