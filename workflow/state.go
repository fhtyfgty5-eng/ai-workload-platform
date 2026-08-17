package workflow

import (
	"fmt"
	"time"
)

// WorkflowStatus 表示一次工作流运行的生命周期状态。
type WorkflowStatus string

// TaskStatus 表示某个任务在一次运行中的调度状态。
type TaskStatus string

// AttemptStatus 表示任务单次执行尝试的结果状态。
type AttemptStatus string

const (
	WorkflowPending   WorkflowStatus = "pending"
	WorkflowRunning   WorkflowStatus = "running"
	WorkflowSucceeded WorkflowStatus = "succeeded"
	WorkflowFailed    WorkflowStatus = "failed"
	WorkflowCanceled  WorkflowStatus = "canceled"
)

const (
	TaskWaitingDependencies TaskStatus = "waiting_dependencies"
	TaskReady               TaskStatus = "ready"
	TaskRunning             TaskStatus = "running"
	TaskWaitingRetry        TaskStatus = "waiting_retry"
	TaskSucceeded           TaskStatus = "succeeded"
	TaskFailed              TaskStatus = "failed"
	TaskCanceled            TaskStatus = "canceled"
	TaskSkipped             TaskStatus = "skipped"
)

const (
	AttemptRunning     AttemptStatus = "running"
	AttemptSucceeded   AttemptStatus = "succeeded"
	AttemptFailed      AttemptStatus = "failed"
	AttemptTimedOut    AttemptStatus = "timed_out"
	AttemptCanceled    AttemptStatus = "canceled"
	AttemptInterrupted AttemptStatus = "interrupted"
)

var workflowTransitions = map[WorkflowStatus]map[WorkflowStatus]bool{
	WorkflowPending: {WorkflowRunning: true, WorkflowCanceled: true},
	WorkflowRunning: {WorkflowSucceeded: true, WorkflowFailed: true, WorkflowCanceled: true},
}

var taskTransitions = map[TaskStatus]map[TaskStatus]bool{
	TaskWaitingDependencies: {TaskReady: true, TaskCanceled: true, TaskSkipped: true},
	TaskReady:               {TaskRunning: true, TaskCanceled: true, TaskSkipped: true},
	TaskRunning:             {TaskSucceeded: true, TaskWaitingRetry: true, TaskFailed: true, TaskCanceled: true},
	TaskWaitingRetry:        {TaskReady: true, TaskFailed: true, TaskCanceled: true},
}

var attemptTransitions = map[AttemptStatus]map[AttemptStatus]bool{
	AttemptRunning: {
		AttemptSucceeded:   true,
		AttemptFailed:      true,
		AttemptTimedOut:    true,
		AttemptCanceled:    true,
		AttemptInterrupted: true,
	},
}

// transitionWorkflow 校验并记录一次工作流状态转换。
func transitionWorkflow(snapshot *RunSnapshot, to WorkflowStatus, at time.Time, reason string) error {
	from := snapshot.Run.Status
	if !workflowTransitions[from][to] {
		return fmt.Errorf("illegal workflow transition %s -> %s", from, to)
	}
	snapshot.Run.Status = to
	if to == WorkflowRunning && snapshot.Run.StartedAt == nil {
		snapshot.Run.StartedAt = &at
	}
	if isWorkflowTerminal(to) {
		snapshot.Run.FinishedAt = &at
	}
	appendEvent(snapshot, at, "workflow", string(snapshot.Run.ID), string(from), string(to), reason)
	return nil
}

// transitionTask 校验并记录一次任务状态转换。
func transitionTask(snapshot *RunSnapshot, taskIndex int, to TaskStatus, at time.Time, reason string) error {
	task := &snapshot.Run.Tasks[taskIndex]
	from := task.Status
	if !taskTransitions[from][to] {
		return fmt.Errorf("illegal task transition %s -> %s", from, to)
	}
	task.Status = to
	if to == TaskReady {
		task.ReadyAt = nil
	}
	if to == TaskSucceeded || to == TaskFailed || to == TaskCanceled || to == TaskSkipped {
		task.FinishedAt = &at
	}
	appendEvent(snapshot, at, "task", string(task.Key), string(from), string(to), reason)
	return nil
}

// transitionAttempt 校验并记录一次执行尝试的结果转换。
func transitionAttempt(snapshot *RunSnapshot, taskKey TaskKey, attempt *Attempt, to AttemptStatus, at time.Time, reason string) error {
	from := attempt.Status
	if !attemptTransitions[from][to] {
		return fmt.Errorf("illegal attempt transition %s -> %s", from, to)
	}
	attempt.Status = to
	attempt.FinishedAt = &at
	appendEvent(snapshot, at, "attempt", fmt.Sprintf("%s/%d", taskKey, attempt.Number), string(from), string(to), reason)
	return nil
}

func appendEvent(snapshot *RunSnapshot, at time.Time, entity, key, from, to, reason string) {
	snapshot.Events = append(snapshot.Events, StateEvent{
		Sequence: uint64(len(snapshot.Events) + 1),
		At:       at,
		Entity:   entity,
		Key:      key,
		From:     from,
		To:       to,
		Reason:   reason,
	})
}

func isWorkflowTerminal(status WorkflowStatus) bool {
	return status == WorkflowSucceeded || status == WorkflowFailed || status == WorkflowCanceled
}
