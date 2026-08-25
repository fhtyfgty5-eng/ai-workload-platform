package app

import (
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// DefinitionRef 标识控制面对外暴露的一个不可变工作流版本。
type DefinitionRef struct {
	WorkflowID string `json:"workflow_id"`
	Version    int    `json:"version"`
}

// WorkflowSummary 是工作流列表和详情 API 返回的有界表示。
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

// StartRunResponse 在 pending Run 持久创建后返回。
type StartRunResponse struct {
	RunID  workflow.RunID          `json:"run_id"`
	Status workflow.WorkflowStatus `json:"status"`
}

// RunSummary 不包含 Attempt 历史，避免列表和摘要响应无限增长。
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

// TaskSummary 是任务列表页使用的有界表示。
type TaskSummary struct {
	Key        workflow.TaskKey    `json:"key"`
	Status     workflow.TaskStatus `json:"status"`
	ReadyAt    *time.Time          `json:"ready_at,omitempty"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
}

// TaskDetail 包含指定任务的完整 Attempt 历史。
type TaskDetail struct {
	Task workflow.TaskRun `json:"task"`
}

// TaskPage 是按稳定任务索引分页的结果。
type TaskPage struct {
	Items      []TaskSummary `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// EventPage 是按稳定事件序号分页的结果。
type EventPage struct {
	Items      []workflow.StateEvent `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}
