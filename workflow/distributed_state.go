package workflow

import (
	"fmt"
	"time"
)

// QueueTask 持久化分布式调度边界，但不消耗 Attempt 次数。
func QueueTask(snapshot *RunSnapshot, taskIndex int, at time.Time) error {
	if snapshot == nil {
		return fmt.Errorf("snapshot is required")
	}
	if taskIndex < 0 || taskIndex >= len(snapshot.Run.Tasks) {
		return fmt.Errorf("task index %d is out of range", taskIndex)
	}
	if snapshot.Run.Status == WorkflowPending {
		if err := transitionWorkflow(snapshot, WorkflowRunning, at, "distributed execution started"); err != nil {
			return err
		}
	}
	if snapshot.Run.Status != WorkflowRunning {
		return fmt.Errorf("workflow must be running before queueing a task")
	}
	return transitionTask(snapshot, taskIndex, TaskQueued, at, "dispatch created")
}

// ReturnQueuedTaskToReady 撤销待领取的分发边界，但不消耗 Attempt 次数。
func ReturnQueuedTaskToReady(snapshot *RunSnapshot, taskIndex int, at time.Time, reason string) error {
	if snapshot == nil {
		return fmt.Errorf("snapshot is required")
	}
	if taskIndex < 0 || taskIndex >= len(snapshot.Run.Tasks) {
		return fmt.Errorf("task index %d is out of range", taskIndex)
	}
	return transitionTask(snapshot, taskIndex, TaskReady, at, reason)
}

// StartQueuedAttempt 在 Worker 成功领取租约后，把已持久化的 queued 任务推进到 running。
func StartQueuedAttempt(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex int, at time.Time, workerID, dispatchID string) (ExecutionRequest, error) {
	if snapshot == nil || compiled == nil {
		return ExecutionRequest{}, fmt.Errorf("snapshot and compiled workflow are required")
	}
	if taskIndex < 0 || taskIndex >= len(snapshot.Run.Tasks) || taskIndex >= len(compiled.definition.Tasks) {
		return ExecutionRequest{}, fmt.Errorf("task index %d is out of range", taskIndex)
	}
	if workerID == "" || dispatchID == "" {
		return ExecutionRequest{}, fmt.Errorf("worker and dispatch IDs are required")
	}
	if snapshot.Run.Tasks[taskIndex].Key != compiled.definition.Tasks[taskIndex].Key {
		return ExecutionRequest{}, fmt.Errorf("task identity does not match compiled workflow")
	}
	if err := transitionTask(snapshot, taskIndex, TaskRunning, at, "lease claimed"); err != nil {
		return ExecutionRequest{}, err
	}
	task := &snapshot.Run.Tasks[taskIndex]
	attempt := Attempt{
		Number:     len(task.Attempts) + 1,
		Status:     AttemptRunning,
		WorkerID:   workerID,
		DispatchID: dispatchID,
		StartedAt:  at,
	}
	task.Attempts = append(task.Attempts, attempt)
	definition := compiled.definition.Tasks[taskIndex]
	return ExecutionRequest{
		DefinitionID: snapshot.Run.DefinitionID,
		RunID:        snapshot.Run.ID,
		TaskKey:      definition.Key,
		Action:       definition.Action,
		Input:        cloneTaskInput(definition.Input),
		Attempt:      attempt.Number,
	}, nil
}

// ApplyAttemptResult 使用与本地 Engine 相同的状态规则应用 Worker 结果。
func ApplyAttemptResult(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex, attemptNumber int, response ExecutionResponse, at time.Time) (bool, error) {
	if compiled == nil {
		return false, fmt.Errorf("compiled workflow is required")
	}
	task, attempt, ok, err := activeAttempt(snapshot, taskIndex, attemptNumber)
	if err != nil || !ok {
		return false, err
	}
	// ResultKind 是 Engine 与 Executor 的封闭协议；未知值属于系统错误，不能猜测业务终态。
	if response.Kind != ResultSuccess && response.Kind != ResultTemporaryFailure && response.Kind != ResultPermanentFailure {
		return false, fmt.Errorf("unsupported execution result %q", response.Kind)
	}
	attempt.Result = ExecutionResult{
		Output:       response.Output,
		ErrorCode:    response.ErrorCode,
		ErrorMessage: response.ErrorMessage,
	}
	if response.Kind == ResultTemporaryFailure {
		if err := transitionAttempt(snapshot, task.Key, attempt, AttemptFailed, at, "temporary execution failure"); err != nil {
			return false, err
		}
		return applyRetryDecision(snapshot, compiled, taskIndex, attempt, at, "temporary execution failure")
	}
	if response.Kind == ResultPermanentFailure {
		if err := transitionAttempt(snapshot, task.Key, attempt, AttemptFailed, at, "permanent execution failure"); err != nil {
			return false, err
		}
		if err := transitionTask(snapshot, taskIndex, TaskFailed, at, "permanent execution failure"); err != nil {
			return false, err
		}
		if err := skipDescendants(snapshot, compiled, taskIndex, at); err != nil {
			return false, err
		}
		if err := settleWorkflowIfComplete(snapshot, at); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptSucceeded, at, "execution succeeded"); err != nil {
		return false, err
	}
	if err := transitionTask(snapshot, taskIndex, TaskSucceeded, at, "execution succeeded"); err != nil {
		return false, err
	}
	// 每条依赖边只减少一次直接下游的未完成依赖数，计数归零时才能进入 ready。
	for _, successorIndex := range compiled.successors[taskIndex] {
		snapshot.Run.RemainingDependencies[successorIndex]--
		if snapshot.Run.RemainingDependencies[successorIndex] < 0 {
			return false, fmt.Errorf("task %q dependency count became negative", snapshot.Run.Tasks[successorIndex].Key)
		}
		if snapshot.Run.RemainingDependencies[successorIndex] == 0 {
			if err := transitionTask(snapshot, successorIndex, TaskReady, at, "dependencies succeeded"); err != nil {
				return false, err
			}
		}
	}
	if err := settleWorkflowIfComplete(snapshot, at); err != nil {
		return false, err
	}
	return true, nil
}

// InterruptAttempt 记录执行所有权丢失，并应用既有重试策略。
func InterruptAttempt(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex, attemptNumber int, at time.Time, reason string) (bool, error) {
	task, attempt, ok, err := activeAttempt(snapshot, taskIndex, attemptNumber)
	if err != nil || !ok {
		return false, err
	}
	attempt.Result = ExecutionResult{ErrorCode: "interrupted", ErrorMessage: reason}
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptInterrupted, at, reason); err != nil {
		return false, err
	}
	return applyRetryDecision(snapshot, compiled, taskIndex, attempt, at, reason)
}

// TimeoutAttempt 记录 Attempt 超时，并应用既有重试策略。
func TimeoutAttempt(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex, attemptNumber int, at time.Time) (bool, error) {
	if compiled == nil {
		return false, fmt.Errorf("compiled workflow is required")
	}
	task, attempt, ok, err := activeAttempt(snapshot, taskIndex, attemptNumber)
	if err != nil || !ok {
		return false, err
	}
	attempt.Result = ExecutionResult{ErrorCode: "timeout", ErrorMessage: "execution timed out"}
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptTimedOut, at, "execution timed out"); err != nil {
		return false, err
	}
	return applyRetryDecision(snapshot, compiled, taskIndex, attempt, at, "execution timed out")
}

// MakeRetryReady 在持久化重试时间到达后，把等待重试的任务重新推进到 ready。
func MakeRetryReady(snapshot *RunSnapshot, taskIndex, attemptNumber int, at time.Time) (bool, error) {
	if snapshot == nil {
		return false, fmt.Errorf("snapshot is required")
	}
	if taskIndex < 0 || taskIndex >= len(snapshot.Run.Tasks) {
		return false, fmt.Errorf("invalid retry task index %d", taskIndex)
	}
	task := &snapshot.Run.Tasks[taskIndex]
	if task.Status != TaskWaitingRetry || task.ReadyAt == nil || len(task.Attempts) == 0 {
		return false, nil
	}
	if task.Attempts[len(task.Attempts)-1].Number != attemptNumber || at.Before(*task.ReadyAt) {
		return false, nil
	}
	if err := transitionTask(snapshot, taskIndex, TaskReady, at, "retry interval elapsed"); err != nil {
		return false, err
	}
	return true, nil
}

// CancelRunSnapshot 返回取消后的候选快照，不修改调用方持有的原快照。
func CancelRunSnapshot(snapshot RunSnapshot, at time.Time) (RunSnapshot, error) {
	updated := cloneRunSnapshot(snapshot)
	for taskIndex := range updated.Run.Tasks {
		task := &updated.Run.Tasks[taskIndex]
		if isTaskTerminal(task.Status) {
			continue
		}
		if task.Status == TaskRunning && len(task.Attempts) > 0 {
			attempt := &task.Attempts[len(task.Attempts)-1]
			if attempt.Status == AttemptRunning {
				attempt.Result = ExecutionResult{ErrorCode: "canceled", ErrorMessage: "execution canceled"}
				if err := transitionAttempt(&updated, task.Key, attempt, AttemptCanceled, at, "workflow canceled"); err != nil {
					return RunSnapshot{}, err
				}
			}
		}
		if err := transitionTask(&updated, taskIndex, TaskCanceled, at, "workflow canceled"); err != nil {
			return RunSnapshot{}, err
		}
	}
	if err := transitionWorkflow(&updated, WorkflowCanceled, at, "cancellation requested"); err != nil {
		return RunSnapshot{}, err
	}
	return updated, nil
}

// ChangeSetBetween 向分布式持久化层提供经过校验的快照差异边界。
func ChangeSetBetween(before, after RunSnapshot) (ChangeSet, error) {
	if before.Run.ID == "" || before.Run.ID != after.Run.ID {
		return ChangeSet{}, fmt.Errorf("before and after snapshots must identify the same Run")
	}
	if after.Run.Revision != before.Run.Revision+1 {
		return ChangeSet{}, fmt.Errorf("after revision %d must follow before revision %d", after.Run.Revision, before.Run.Revision)
	}
	return changeSetFromSnapshots(before, after), nil
}

func activeAttempt(snapshot *RunSnapshot, taskIndex, attemptNumber int) (*TaskRun, *Attempt, bool, error) {
	if snapshot == nil {
		return nil, nil, false, fmt.Errorf("snapshot is required")
	}
	if taskIndex < 0 || taskIndex >= len(snapshot.Run.Tasks) {
		return nil, nil, false, fmt.Errorf("task index %d is out of range", taskIndex)
	}
	task := &snapshot.Run.Tasks[taskIndex]
	if task.Status != TaskRunning || len(task.Attempts) == 0 {
		return task, nil, false, nil
	}
	attempt := &task.Attempts[len(task.Attempts)-1]
	if attempt.Number != attemptNumber || attempt.Status != AttemptRunning {
		return task, attempt, false, nil
	}
	return task, attempt, true, nil
}

// applyRetryDecision 根据剩余尝试次数把失败任务转入等待重试或最终失败。
func applyRetryDecision(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex int, attempt *Attempt, at time.Time, reason string) (bool, error) {
	task := &snapshot.Run.Tasks[taskIndex]
	definition := compiled.definition.Tasks[taskIndex]
	if attempt.Number < definition.Retry.MaxAttempts {
		if err := transitionTask(snapshot, taskIndex, TaskWaitingRetry, at, reason); err != nil {
			return false, err
		}
		readyAt := at.Add(time.Duration(definition.Retry.IntervalMillis) * time.Millisecond)
		task.ReadyAt = &readyAt
		return true, nil
	}
	if err := transitionTask(snapshot, taskIndex, TaskFailed, at, "retry attempts exhausted"); err != nil {
		return false, err
	}
	if err := skipDescendants(snapshot, compiled, taskIndex, at); err != nil {
		return false, err
	}
	if err := settleWorkflowIfComplete(snapshot, at); err != nil {
		return false, err
	}
	return true, nil
}

func settleWorkflowIfComplete(snapshot *RunSnapshot, at time.Time) error {
	for _, task := range snapshot.Run.Tasks {
		if !isTaskTerminal(task.Status) {
			return nil
		}
	}
	status := WorkflowSucceeded
	reason := "all tasks succeeded"
	if hasFailedTask(snapshot.Run.Tasks) {
		status = WorkflowFailed
		reason = "one or more tasks failed"
	} else if !allTasksSucceeded(snapshot.Run.Tasks) {
		return fmt.Errorf("workflow %q reached mixed terminal tasks without failure", snapshot.Run.ID)
	}
	return transitionWorkflow(snapshot, status, at, reason)
}

// skipDescendants 从失败任务开始向下传播，只跳过尚未进入终态的后代。
func skipDescendants(snapshot *RunSnapshot, compiled *CompiledWorkflow, failedTask int, at time.Time) error {
	return skipDescendantsFrom(snapshot, compiled, []int{failedTask}, at)
}

// skipDescendantsFrom 从多个失败源一次遍历全部后代，复杂度为 O(V + E)。
func skipDescendantsFrom(snapshot *RunSnapshot, compiled *CompiledWorkflow, failedTasks []int, at time.Time) error {
	queue := make([]int, 0)
	for _, failedTask := range failedTasks {
		queue = append(queue, compiled.successors[failedTask]...)
	}
	visited := make([]bool, len(snapshot.Run.Tasks))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current] {
			continue
		}
		visited[current] = true
		if !isTaskTerminal(snapshot.Run.Tasks[current].Status) {
			if err := transitionTask(snapshot, current, TaskSkipped, at, "upstream task failed"); err != nil {
				return err
			}
		}
		queue = append(queue, compiled.successors[current]...)
	}
	return nil
}
