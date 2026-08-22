package workflow

import (
	"context"
	"fmt"
	"time"
)

// Resume 从持久化快照重建运行时计数，并按至少执行一次语义继续运行。
func (e *Engine) Resume(ctx context.Context, id RunID) (WorkflowRun, error) {
	runCtx, stop, release, err := e.registerActiveRun(ctx, id)
	if err != nil {
		return WorkflowRun{}, err
	}
	defer release()

	// 即使父 Context 已取消也读取事实快照，以便判断终态或已持久化的业务取消请求。
	snapshot, err := e.store.Load(context.WithoutCancel(ctx), id)
	if err != nil {
		return WorkflowRun{}, err
	}
	// Store 按快照内 Run.ID 选择 Save 目标；身份校验必须早于终态返回和任何写入。
	if snapshot.Version != currentSnapshotVersion {
		return WorkflowRun{}, fmt.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	if err := validateStoredRunID(snapshot, id); err != nil {
		return WorkflowRun{}, err
	}
	if isWorkflowTerminal(snapshot.Run.Status) {
		return snapshot.Run, nil
	}
	if snapshot.Run.Status != WorkflowPending && snapshot.Run.Status != WorkflowRunning {
		return WorkflowRun{}, fmt.Errorf("run %q has unsupported workflow status %q", id, snapshot.Run.Status)
	}
	if snapshot.Run.CancelRequestedAt != nil {
		return e.finalizeCancellation(ctx, snapshot)
	}
	if runCtx.Err() != nil {
		return e.stoppedRunResult(ctx, runCtx, snapshot)
	}
	if snapshot.Definition == nil {
		return WorkflowRun{}, fmt.Errorf("run %q has no stored definition", id)
	}
	compiled, err := Compile(*snapshot.Definition)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("compile stored definition: %w", err)
	}
	if snapshot.Run.DefinitionID != compiled.definition.ID {
		return WorkflowRun{}, fmt.Errorf(
			"run %q definition id %q does not match stored definition %q",
			id,
			snapshot.Run.DefinitionID,
			compiled.definition.ID,
		)
	}

	// 所有恢复修正先作用于副本；只有完整预处理和保存成功后，调度器才能看到新状态。
	updated := cloneRunSnapshot(snapshot)
	if err := prepareForResume(&updated, compiled, e.clock.Now()); err != nil {
		return WorkflowRun{}, err
	}
	if err := e.saveSnapshot(ctx, snapshot, &updated); err != nil {
		if runCtx.Err() != nil {
			return e.stoppedRunResult(ctx, runCtx, snapshot)
		}
		return e.reconcilePersistedCancellation(ctx, id, err)
	}
	if runCtx.Err() != nil {
		return e.stoppedRunResult(ctx, runCtx, updated)
	}

	// 恢复快照保存成功后才允许提交编译缓存并启动 Executor。
	e.mu.Lock()
	e.compiled[id] = compiled
	e.mu.Unlock()
	run, err := e.executeSnapshot(ctx, runCtx, stop, compiled, updated)
	if err == nil {
		return run, nil
	}
	return e.reconcilePersistedCancellation(ctx, id, err)
}

// prepareForResume 校验持久化任务状态，并从定义和任务事实重新构造调度状态。
func prepareForResume(snapshot *RunSnapshot, compiled *CompiledWorkflow, now time.Time) error {
	if len(snapshot.Run.Tasks) != len(compiled.definition.Tasks) {
		return fmt.Errorf("stored task count does not match definition")
	}

	// 保存的计数可能停留在崩溃前的中间状态，恢复时必须由 DAG 和任务终态重新推导。
	snapshot.Run.RemainingDependencies = make([]int, len(snapshot.Run.Tasks))
	for i := range snapshot.Run.Tasks {
		task := &snapshot.Run.Tasks[i]
		definition := compiled.definition.Tasks[i]
		if task.Key != definition.Key {
			return fmt.Errorf("stored task %d key %q does not match definition key %q", i, task.Key, definition.Key)
		}
		if err := validateAttemptHistoryForResume(*task, definition); err != nil {
			return err
		}

		switch task.Status {
		case TaskRunning:
			attempt := &task.Attempts[len(task.Attempts)-1]
			if err := transitionAttempt(snapshot, task.Key, attempt, AttemptInterrupted, now, "process restarted"); err != nil {
				return err
			}
			if len(task.Attempts) >= definition.Retry.MaxAttempts {
				if err := transitionTask(snapshot, i, TaskFailed, now, "attempts exhausted after restart"); err != nil {
					return err
				}
			} else {
				if err := transitionTask(snapshot, i, TaskWaitingRetry, now, "retry interrupted attempt"); err != nil {
					return err
				}
				// 被进程中断的 Attempt 有剩余次数时立即具备重试资格。
				task.ReadyAt = &now
			}
		case TaskWaitingRetry, TaskWaitingDependencies, TaskReady, TaskSucceeded, TaskFailed, TaskCanceled, TaskSkipped:
		}
	}

	// 先收集已确定的失败终态，再用一次多源遍历传播，避免任务数组顺序遗漏后代或重复遍历退化为 O(V²)。
	blockedSources := make([]int, 0)
	for i := range snapshot.Run.Tasks {
		status := snapshot.Run.Tasks[i].Status
		if status == TaskFailed || status == TaskCanceled || status == TaskSkipped {
			blockedSources = append(blockedSources, i)
		}
	}
	if err := skipDescendantsFrom(snapshot, compiled, blockedSources, now); err != nil {
		return err
	}

	// 失败传播收敛后，再依据每条直接依赖的真实状态重算未满足依赖数。
	for i := range snapshot.Run.Tasks {
		task := &snapshot.Run.Tasks[i]
		if isTaskTerminal(task.Status) {
			continue
		}
		remaining := 0
		blocked := false
		for _, dependency := range compiled.dependencies[i] {
			switch snapshot.Run.Tasks[dependency].Status {
			case TaskSucceeded:
			case TaskFailed, TaskCanceled, TaskSkipped:
				blocked = true
			default:
				remaining++
			}
		}
		snapshot.Run.RemainingDependencies[i] = remaining
		if blocked {
			if err := transitionTask(snapshot, i, TaskSkipped, now, "upstream terminal failure"); err != nil {
				return err
			}
			continue
		}
		if remaining == 0 && task.Status == TaskWaitingDependencies {
			if err := transitionTask(snapshot, i, TaskReady, now, "dependencies restored"); err != nil {
				return err
			}
		}
		if remaining > 0 && (task.Status == TaskReady || task.Status == TaskWaitingRetry) {
			return fmt.Errorf("task %q is %s before dependencies succeeded", task.Key, task.Status)
		}
	}
	return nil
}

// validateAttemptHistoryForResume 拒绝会突破重试上限或混淆当前 Attempt 的损坏历史。
func validateAttemptHistoryForResume(task TaskRun, definition TaskDefinition) error {
	attempts := task.Attempts
	if task.Status != TaskWaitingRetry && task.ReadyAt != nil {
		return fmt.Errorf("task %q has ready_at outside waiting_retry", task.Key)
	}
	if len(attempts) > definition.Retry.MaxAttempts {
		return fmt.Errorf("task %q has %d attempts, maximum is %d", task.Key, len(attempts), definition.Retry.MaxAttempts)
	}
	lastStatus := AttemptStatus("")
	if len(attempts) > 0 {
		lastStatus = attempts[len(attempts)-1].Status
	}

	switch task.Status {
	case TaskWaitingDependencies:
		if len(attempts) != 0 {
			return fmt.Errorf("task %q is waiting_dependencies with attempt history", task.Key)
		}
	case TaskReady:
		if len(attempts) >= definition.Retry.MaxAttempts {
			return fmt.Errorf("task %q is ready after exhausted retry attempts", task.Key)
		}
		if len(attempts) > 0 && !isRetryableAttemptStatus(lastStatus) {
			return fmt.Errorf("task %q is ready after attempt status %q", task.Key, lastStatus)
		}
	case TaskRunning:
		if len(attempts) == 0 || lastStatus != AttemptRunning {
			return fmt.Errorf("task %q is running without a running attempt", task.Key)
		}
	case TaskWaitingRetry:
		if len(attempts) == 0 || task.ReadyAt == nil {
			return fmt.Errorf("task %q is waiting_retry without attempt or ready_at", task.Key)
		}
		if len(attempts) >= definition.Retry.MaxAttempts {
			return fmt.Errorf("task %q is waiting_retry after exhausted retry attempts", task.Key)
		}
		if !isRetryableAttemptStatus(lastStatus) {
			return fmt.Errorf("task %q is waiting_retry after attempt status %q", task.Key, lastStatus)
		}
	case TaskSucceeded:
		if len(attempts) == 0 || lastStatus != AttemptSucceeded {
			return fmt.Errorf("task %q succeeded after attempt status %q", task.Key, lastStatus)
		}
	case TaskFailed:
		if len(attempts) == 0 || (lastStatus != AttemptFailed && lastStatus != AttemptTimedOut && lastStatus != AttemptInterrupted) {
			return fmt.Errorf("task %q failed after attempt status %q", task.Key, lastStatus)
		}
		if (lastStatus == AttemptTimedOut || lastStatus == AttemptInterrupted) && len(attempts) < definition.Retry.MaxAttempts {
			return fmt.Errorf("task %q failed before retry attempts were exhausted", task.Key)
		}
	case TaskCanceled:
		if len(attempts) > 0 && lastStatus != AttemptCanceled && !isRetryableAttemptStatus(lastStatus) {
			return fmt.Errorf("task %q canceled after attempt status %q", task.Key, lastStatus)
		}
	case TaskSkipped:
		if len(attempts) != 0 {
			return fmt.Errorf("task %q is skipped with attempt history", task.Key)
		}
	default:
		return fmt.Errorf("task %q has unsupported stored status %q", task.Key, task.Status)
	}

	for i, attempt := range attempts {
		expected := i + 1
		if attempt.Number != expected {
			return fmt.Errorf("task %q attempt number %d at index %d, want %d", task.Key, attempt.Number, i, expected)
		}
		switch attempt.Status {
		case AttemptRunning:
			if i != len(attempts)-1 || task.Status != TaskRunning {
				return fmt.Errorf("task %q has historical running attempt %d", task.Key, attempt.Number)
			}
		case AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptInterrupted:
		default:
			return fmt.Errorf("task %q attempt %d has unsupported status %q", task.Key, attempt.Number, attempt.Status)
		}
		if i < len(attempts)-1 && !isRetryableAttemptStatus(attempt.Status) {
			return fmt.Errorf("task %q has non-retryable historical attempt %d with status %q", task.Key, attempt.Number, attempt.Status)
		}
	}
	return nil
}

func isRetryableAttemptStatus(status AttemptStatus) bool {
	return status == AttemptFailed || status == AttemptTimedOut || status == AttemptInterrupted
}
