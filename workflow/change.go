package workflow

import (
	"fmt"
	"reflect"
)

// ChangeSet 描述一次必须原子提交的 Run 状态变化。
// Run、Tasks、Attempts 和 Events 分别对应关系存储中的行，存储实现不能拆成多次提交。
type ChangeSet struct {
	// RunID 是所有行级变更共同所属的 Run，也是存储定位事务目标的主键。
	RunID RunID
	// ExpectedRevision 是 Engine 生成候选状态时读到的版本；不匹配表示事实状态已被其他写入推进。
	ExpectedRevision uint64
	// Run 是不含 Tasks 和 RemainingDependencies 的 Run 行；其 Revision 必须恰好递增一次。
	Run *WorkflowRun
	// Tasks、Attempts 和 Events 只包含本次状态转换实际影响的行。
	Tasks    []TaskRunChange
	Attempts []AttemptChange
	Events   []StateEvent
}

// TaskRunChange 描述一条任务记录中发生变化的非 Attempt 字段。
type TaskRunChange struct {
	// RunID 和 Task.Key 组成任务行的稳定身份；Index 只供 Engine 和 FileStore O(1) 定位数组元素。
	RunID RunID
	Index int
	// Task 不得嵌入 Attempts，Attempt 历史由 AttemptChange 独立增量提交。
	Task TaskRun
	// RemainingDependencies 与 Task 同属 task_runs 行，用于恢复调度时判断尚未完成的直接依赖数。
	RemainingDependencies int
}

// ChangeOperation 区分新增记录和更新已有记录。
type ChangeOperation string

const (
	// ChangeInsert 表示该记录尚未存在于已持久化的 Attempt 历史中。
	ChangeInsert ChangeOperation = "insert"
	// ChangeUpdate 表示该记录已经存在，本次把它推进到新状态。
	ChangeUpdate ChangeOperation = "update"
)

// AttemptChange 描述一条 Attempt 记录的新增或更新。
type AttemptChange struct {
	// RunID、TaskKey 和 Attempt.Number 共同构成 Attempt 行的稳定身份。
	RunID   RunID
	TaskKey TaskKey
	// TaskIndex 只供 Engine 和 FileStore 定位内存数组，不作为 PostgreSQL 的业务主键。
	TaskIndex int
	Attempt   Attempt
	Operation ChangeOperation
}

// changeSetFromSnapshots 将前后状态的差异转换成可由关系型 Repository 消费的行级变更集。
func changeSetFromSnapshots(before, after RunSnapshot) ChangeSet {
	run := after.Run
	run.LastEventSequence = snapshotLastEventSequence(after)
	run.Tasks = nil
	run.RemainingDependencies = nil
	change := ChangeSet{
		RunID:            after.Run.ID,
		ExpectedRevision: before.Run.Revision,
		Run:              &run,
		Events:           append([]StateEvent(nil), after.Events[len(before.Events):]...),
	}

	for i := range after.Run.Tasks {
		beforeTask := TaskRun{}
		beforeRemaining := 0
		if i < len(before.Run.Tasks) {
			beforeTask = before.Run.Tasks[i]
		}
		if i < len(before.Run.RemainingDependencies) {
			beforeRemaining = before.Run.RemainingDependencies[i]
		}
		afterTask := after.Run.Tasks[i]
		afterRemaining := 0
		if i < len(after.Run.RemainingDependencies) {
			afterRemaining = after.Run.RemainingDependencies[i]
		}
		if beforeTask.Key != afterTask.Key ||
			beforeTask.Status != afterTask.Status ||
			beforeRemaining != afterRemaining ||
			!reflect.DeepEqual(beforeTask.ReadyAt, afterTask.ReadyAt) ||
			!reflect.DeepEqual(beforeTask.FinishedAt, afterTask.FinishedAt) {
			task := afterTask
			task.Attempts = nil
			change.Tasks = append(change.Tasks, TaskRunChange{
				RunID:                 after.Run.ID,
				Index:                 i,
				Task:                  task,
				RemainingDependencies: afterRemaining,
			})
		}
		// 已有编号只在状态完成时更新；超出旧历史长度的编号是本次新插入的 Attempt。
		for attemptIndex, attempt := range afterTask.Attempts {
			operation := ChangeInsert
			if attemptIndex < len(beforeTask.Attempts) {
				if reflect.DeepEqual(beforeTask.Attempts[attemptIndex], attempt) {
					continue
				}
				operation = ChangeUpdate
			}
			change.Attempts = append(change.Attempts, AttemptChange{
				RunID:     after.Run.ID,
				TaskKey:   afterTask.Key,
				TaskIndex: i,
				Attempt:   attempt,
				Operation: operation,
			})
		}
	}
	return change
}

// applyChangeSet 校验行身份和顺序后在快照副本上应用变更，避免失败时留下半应用状态。
func applyChangeSet(snapshot RunSnapshot, change ChangeSet) (RunSnapshot, error) {
	if change.RunID != snapshot.Run.ID {
		return RunSnapshot{}, invalidChangeSet("change RunID %q does not match snapshot RunID %q", change.RunID, snapshot.Run.ID)
	}
	if change.ExpectedRevision != snapshot.Run.Revision {
		return RunSnapshot{}, ErrRevisionConflict
	}
	if change.Run == nil {
		return RunSnapshot{}, invalidChangeSet("run row is required")
	}
	if change.Run.ID != change.RunID {
		return RunSnapshot{}, invalidChangeSet("run row ID %q does not match ChangeSet RunID %q", change.Run.ID, change.RunID)
	}
	if change.Run.Revision != change.ExpectedRevision+1 {
		return RunSnapshot{}, invalidChangeSet("run revision %d must follow expected revision %d", change.Run.Revision, change.ExpectedRevision)
	}
	if len(change.Run.Tasks) != 0 || len(change.Run.RemainingDependencies) != 0 {
		return RunSnapshot{}, invalidChangeSet("run row must not embed task rows")
	}
	if len(snapshot.Run.Tasks) != len(snapshot.Run.RemainingDependencies) {
		return RunSnapshot{}, invalidChangeSet("snapshot task and dependency counts differ")
	}

	// 所有写入都落在深拷贝上；下方任一校验失败时，调用方持有的原快照保持不变。
	updated := cloneRunSnapshot(snapshot)
	tasks := updated.Run.Tasks
	remaining := updated.Run.RemainingDependencies
	run := *change.Run
	run.Tasks = tasks
	run.RemainingDependencies = remaining
	updated.Run = run

	changedTasks := make(map[int]struct{}, len(change.Tasks))
	for _, taskChange := range change.Tasks {
		if taskChange.RunID != change.RunID {
			return RunSnapshot{}, invalidChangeSet("task RunID %q does not match ChangeSet RunID %q", taskChange.RunID, change.RunID)
		}
		if taskChange.Index < 0 || taskChange.Index >= len(updated.Run.Tasks) {
			return RunSnapshot{}, invalidChangeSet("task index %d is out of range", taskChange.Index)
		}
		if _, exists := changedTasks[taskChange.Index]; exists {
			return RunSnapshot{}, invalidChangeSet("task index %d is changed more than once", taskChange.Index)
		}
		changedTasks[taskChange.Index] = struct{}{}
		if taskChange.Task.Key != updated.Run.Tasks[taskChange.Index].Key {
			return RunSnapshot{}, invalidChangeSet("task key %q does not match index %d", taskChange.Task.Key, taskChange.Index)
		}
		if len(taskChange.Task.Attempts) != 0 {
			return RunSnapshot{}, invalidChangeSet("task %q embeds attempt rows", taskChange.Task.Key)
		}
		if taskChange.RemainingDependencies < 0 {
			return RunSnapshot{}, invalidChangeSet("task %q has negative remaining dependencies", taskChange.Task.Key)
		}
		attempts := updated.Run.Tasks[taskChange.Index].Attempts
		task := taskChange.Task
		task.Attempts = attempts
		updated.Run.Tasks[taskChange.Index] = task
		updated.Run.RemainingDependencies[taskChange.Index] = taskChange.RemainingDependencies
	}

	changedAttempts := make(map[[2]int]struct{}, len(change.Attempts))
	for _, attemptChange := range change.Attempts {
		if attemptChange.RunID != change.RunID {
			return RunSnapshot{}, invalidChangeSet("attempt RunID %q does not match ChangeSet RunID %q", attemptChange.RunID, change.RunID)
		}
		if attemptChange.TaskIndex < 0 || attemptChange.TaskIndex >= len(updated.Run.Tasks) {
			return RunSnapshot{}, invalidChangeSet("attempt task index %d is out of range", attemptChange.TaskIndex)
		}
		task := &updated.Run.Tasks[attemptChange.TaskIndex]
		if attemptChange.TaskKey != task.Key {
			return RunSnapshot{}, invalidChangeSet("attempt task key %q does not match index %d", attemptChange.TaskKey, attemptChange.TaskIndex)
		}
		attemptKey := [2]int{attemptChange.TaskIndex, attemptChange.Attempt.Number}
		if _, exists := changedAttempts[attemptKey]; exists {
			return RunSnapshot{}, invalidChangeSet("attempt %q/%d is changed more than once", attemptChange.TaskKey, attemptChange.Attempt.Number)
		}
		changedAttempts[attemptKey] = struct{}{}
		switch attemptChange.Operation {
		case ChangeInsert:
			if attemptChange.Attempt.Number != len(task.Attempts)+1 {
				return RunSnapshot{}, invalidChangeSet("inserted attempt number %d must follow existing history length %d", attemptChange.Attempt.Number, len(task.Attempts))
			}
			task.Attempts = append(task.Attempts, attemptChange.Attempt)
		case ChangeUpdate:
			index := attemptChange.Attempt.Number - 1
			if index < 0 || index >= len(task.Attempts) || task.Attempts[index].Number != attemptChange.Attempt.Number {
				return RunSnapshot{}, invalidChangeSet("updated attempt number %d does not exist", attemptChange.Attempt.Number)
			}
			task.Attempts[index] = attemptChange.Attempt
		default:
			return RunSnapshot{}, invalidChangeSet("unknown attempt operation %q", attemptChange.Operation)
		}
	}

	persistedLastSequence := snapshotLastEventSequence(snapshot)
	nextSequence := persistedLastSequence + 1
	for _, event := range change.Events {
		if event.Sequence != nextSequence {
			return RunSnapshot{}, invalidChangeSet("event sequence %d must be %d", event.Sequence, nextSequence)
		}
		nextSequence++
	}
	wantLastSequence := persistedLastSequence
	if len(change.Events) > 0 {
		wantLastSequence = change.Events[len(change.Events)-1].Sequence
	}
	if updated.Run.LastEventSequence != wantLastSequence {
		return RunSnapshot{}, invalidChangeSet("run last event sequence %d must be %d", updated.Run.LastEventSequence, wantLastSequence)
	}
	updated.Events = append(updated.Events, change.Events...)
	return updated, nil
}

// ApplyChangeSetForStore 根据经过校验的行级变更重建兼容快照。
func ApplyChangeSetForStore(snapshot RunSnapshot, change ChangeSet) (RunSnapshot, error) {
	return applyChangeSet(snapshot, change)
}

func invalidChangeSet(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidChangeSet, fmt.Sprintf(format, args...))
}

func snapshotLastEventSequence(snapshot RunSnapshot) uint64 {
	lastSequence := snapshot.Run.LastEventSequence
	if count := len(snapshot.Events); count > 0 && snapshot.Events[count-1].Sequence > lastSequence {
		lastSequence = snapshot.Events[count-1].Sequence
	}
	return lastSequence
}
