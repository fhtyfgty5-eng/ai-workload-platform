package workloadclient

import (
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// DefinitionRef 标识一个不可变的工作流定义版本。
type DefinitionRef struct {
	WorkflowID string `json:"workflow_id"`
	Version    int    `json:"version"`
}

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

type StartRunResponse struct {
	RunID  workflow.RunID          `json:"run_id"`
	Status workflow.WorkflowStatus `json:"status"`
}

// RunSummary 不包含 Attempt 历史，避免列表和详情响应无限增长。
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

// RunListOptions 提供 Run 列表的分页和可选过滤条件。
type RunListOptions struct {
	Cursor     string
	Limit      int
	WorkflowID string
	Status     workflow.WorkflowStatus
}

type CancelRunResponse struct {
	RunID             workflow.RunID          `json:"run_id"`
	Status            workflow.WorkflowStatus `json:"status"`
	CancelRequestedAt time.Time               `json:"cancel_requested_at"`
}

type TaskSummary struct {
	Key        workflow.TaskKey    `json:"key"`
	Status     workflow.TaskStatus `json:"status"`
	ReadyAt    *time.Time          `json:"ready_at,omitempty"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
}

type TaskDetail struct {
	Task workflow.TaskRun `json:"task"`
}

type TaskPage struct {
	Items      []TaskSummary `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type EventPage struct {
	Items      []workflow.StateEvent `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}
