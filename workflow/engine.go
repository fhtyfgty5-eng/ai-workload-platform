package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// EngineOptions 提供时钟和 RunID 生成器等可替换依赖。
type EngineOptions struct {
	// Clock 隔离当前时间和等待能力，便于确定性测试重试与超时。
	Clock Clock
	// NewRunID 允许测试注入固定 ID；生产默认使用加密随机 ID。
	NewRunID func() (RunID, error)
}

// Engine 串行提交单个 Run 的状态，并在定义的并发上限内协调 Executor。
type Engine struct {
	// store 是所有可恢复状态的事实来源，executor 只负责执行一次 Attempt。
	store    Store
	executor Executor
	// clock 和 newRunID 是可替换的系统依赖，不保存业务状态。
	clock    Clock
	newRunID func() (RunID, error)

	// mu 同时保护 active 和 compiled；Execute 的单 Run 事件循环不依赖该锁提交状态。
	mu sync.Mutex
	// active 保存当前进程内正在 Execute 的 Run 取消函数，防止同一 Run 被重复调度。
	active map[RunID]context.CancelFunc
	// compiled 缓存可跨 Run 共享的只读编译结果；终态 Run 会释放对应条目。
	compiled map[RunID]*CompiledWorkflow
}

// NewEngine 使用显式 Store、Executor 和可选依赖创建工作流内核。
func NewEngine(store Store, executor Executor, options EngineOptions) (*Engine, error) {
	if store == nil || executor == nil {
		return nil, fmt.Errorf("store and executor are required")
	}
	if options.Clock == nil {
		options.Clock = RealClock{}
	}
	if options.NewRunID == nil {
		options.NewRunID = randomRunID
	}
	return &Engine{
		store:    store,
		executor: executor,
		clock:    options.Clock,
		newRunID: options.NewRunID,
		active:   make(map[RunID]context.CancelFunc),
		compiled: make(map[RunID]*CompiledWorkflow),
	}, nil
}

// CreateRun 持久化独立运行快照，并缓存可由同一进程后续调度共享的编译结果。
func (e *Engine) CreateRun(ctx context.Context, compiled *CompiledWorkflow) (RunID, error) {
	if compiled == nil {
		return "", fmt.Errorf("compiled workflow is required")
	}
	id, err := e.newRunID()
	if err != nil {
		return "", err
	}
	snapshot := newRunSnapshot(id, compiled, e.clock.Now())
	if err := e.store.Create(ctx, snapshot); err != nil {
		return "", err
	}
	// 只有初始快照落盘成功后才缓存编译结果，避免出现无法查询的内存 Run。
	e.mu.Lock()
	e.compiled[id] = compiled
	e.mu.Unlock()
	return id, nil
}

// Execute 调度指定 Run 的可执行任务，直到工作流成功、失败或发生系统错误。
func (e *Engine) Execute(ctx context.Context, id RunID) (WorkflowRun, error) {
	// 必须先登记 active 再读取快照，避免并发 Cancel 保存 canceled 后，本次 Execute 用旧快照覆盖终态。
	runCtx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	if _, exists := e.active[id]; exists {
		e.mu.Unlock()
		cancel()
		return WorkflowRun{}, fmt.Errorf("run %q is already executing", id)
	}
	e.active[id] = cancel
	e.mu.Unlock()
	defer func() {
		cancel()
		e.mu.Lock()
		delete(e.active, id)
		e.mu.Unlock()
	}()

	// 父 Context 可能在加载前已取消；仍需读取事实快照，才能把非终态 Run 持久化为 canceled。
	snapshot, err := e.store.Load(context.WithoutCancel(ctx), id)
	if err != nil {
		return WorkflowRun{}, err
	}
	if isWorkflowTerminal(snapshot.Run.Status) {
		return snapshot.Run, nil
	}
	if runCtx.Err() != nil {
		return e.finalizeCancellation(ctx, snapshot)
	}
	compiled, err := e.compiledForRun(id, snapshot)
	if err != nil {
		return WorkflowRun{}, err
	}
	if runCtx.Err() != nil {
		return e.finalizeCancellation(ctx, snapshot)
	}

	if snapshot.Run.Status == WorkflowPending {
		// 先在副本中推进并保存，再替换内存快照；保存失败时旧状态仍是事实来源。
		updated := cloneRunSnapshot(snapshot)
		if err := transitionWorkflow(&updated, WorkflowRunning, e.clock.Now(), "execution started"); err != nil {
			return WorkflowRun{}, err
		}
		if err := e.store.Save(ctx, updated); err != nil {
			if runCtx.Err() != nil {
				return e.finalizeCancellation(ctx, snapshot)
			}
			return WorkflowRun{}, err
		}
		snapshot = updated
		if runCtx.Err() != nil {
			return e.finalizeCancellation(ctx, snapshot)
		}
	}

	// 三类异步来源只发送事实事件；下面的 Execute goroutine 是状态快照的唯一写者。
	completions := make(chan executionCompletion, len(snapshot.Run.Tasks))
	timeouts := make(chan executionTimeout, len(snapshot.Run.Tasks))
	retries := make(chan retryReady, len(snapshot.Run.Tasks))
	// activeAttempts 只由事件循环访问；键存在表示该 Attempt 尚未接受过完成或超时事件。
	activeAttempts := make(map[attemptKey]attemptControl)
	defer func() {
		for _, control := range activeAttempts {
			control.cancel()
			close(control.done)
		}
	}()
	running := 0
	// startReadyTasks 填满并发槽位。每个 Attempt 必须先持久化 running 状态，才能启动 Executor。
	startReadyTasks := func() error {
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
		for taskIndex, task := range snapshot.Run.Tasks {
			if running == compiled.definition.Concurrency {
				break
			}
			if task.Status != TaskReady {
				continue
			}
			updated := cloneRunSnapshot(snapshot)
			request, attempt, err := e.prepareTask(&updated, compiled, taskIndex)
			if err != nil {
				return err
			}
			if err := e.store.Save(ctx, updated); err != nil {
				return err
			}
			// 保存成功后才提交内存快照并启动外部执行，保证崩溃恢复至少能看到 running Attempt。
			snapshot = updated
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			key := attemptKey{taskIndex: taskIndex, attempt: attempt}
			activeAttempts[key] = e.executeTask(
				runCtx,
				request,
				taskIndex,
				attempt,
				time.Duration(compiled.definition.Tasks[taskIndex].TimeoutMillis)*time.Millisecond,
				completions,
				timeouts,
			)
			running++
		}
		return nil
	}

	if err := startReadyTasks(); err != nil {
		if runCtx.Err() != nil {
			return e.finalizeCancellation(ctx, snapshot)
		}
		return WorkflowRun{}, err
	}
	// 事件循环串行处理完成、超时和重试到期，避免多个 goroutine 并发修改同一快照。
	for {
		// Run 取消优先于业务完成、Attempt 超时和重试到期，统一由事件循环写入取消终态。
		if runCtx.Err() != nil {
			return e.finalizeCancellation(ctx, snapshot)
		}
		if running > 0 {
			select {
			case <-runCtx.Done():
				return e.finalizeCancellation(ctx, snapshot)
			case completion := <-completions:
				if runCtx.Err() != nil {
					return e.finalizeCancellation(ctx, snapshot)
				}
				// 活动表中不存在的键属于已超时或已结算 Attempt 的迟到结果，直接丢弃。
				key := attemptKey{taskIndex: completion.taskIndex, attempt: completion.attempt}
				if !finishActiveAttempt(activeAttempts, key) {
					continue
				}
				running--
				if completion.response.Kind == ResultCanceled {
					// 单个 Executor 主动取消同样结束整个 Run；先通知其他 Attempt，再统一保存取消终态。
					cancel()
					return e.finalizeCancellation(ctx, snapshot)
				}
				updated := cloneRunSnapshot(snapshot)
				applied, err := applyCompletion(&updated, compiled, completion)
				if err != nil {
					return WorkflowRun{}, err
				}
				if applied {
					if err := e.store.Save(ctx, updated); err != nil {
						if runCtx.Err() != nil {
							return e.finalizeCancellation(ctx, snapshot)
						}
						return WorkflowRun{}, err
					}
					// 提交新快照后，才允许基于其中的 waiting_retry 状态注册计时器。
					snapshot = updated
					if snapshot.Run.Tasks[completion.taskIndex].Status == TaskWaitingRetry {
						e.scheduleRetry(runCtx, completion.taskIndex, snapshot.Run.Tasks[completion.taskIndex], retries)
					}
				}
			case timeout := <-timeouts:
				if runCtx.Err() != nil {
					return e.finalizeCancellation(ctx, snapshot)
				}
				// 超时事件先通过活动表的一次性门闩并取消 Executor，再写入快照副本。
				key := attemptKey{taskIndex: timeout.taskIndex, attempt: timeout.attempt}
				if !finishActiveAttempt(activeAttempts, key) {
					continue
				}
				running--
				updated := cloneRunSnapshot(snapshot)
				applied, err := applyTimeout(&updated, compiled, timeout)
				if err != nil {
					return WorkflowRun{}, err
				}
				if applied {
					if err := e.store.Save(ctx, updated); err != nil {
						if runCtx.Err() != nil {
							return e.finalizeCancellation(ctx, snapshot)
						}
						return WorkflowRun{}, err
					}
					snapshot = updated
					if snapshot.Run.Tasks[timeout.taskIndex].Status == TaskWaitingRetry {
						e.scheduleRetry(runCtx, timeout.taskIndex, snapshot.Run.Tasks[timeout.taskIndex], retries)
					}
				}
			case retry := <-retries:
				if runCtx.Err() != nil {
					return e.finalizeCancellation(ctx, snapshot)
				}
				// 计时器只报告“可以重试”；事件循环仍需复核 Attempt 编号和 ReadyAt。
				updated := cloneRunSnapshot(snapshot)
				applied, err := applyRetryReady(&updated, retry)
				if err != nil {
					return WorkflowRun{}, err
				}
				if applied {
					if err := e.store.Save(ctx, updated); err != nil {
						if runCtx.Err() != nil {
							return e.finalizeCancellation(ctx, snapshot)
						}
						return WorkflowRun{}, err
					}
					snapshot = updated
				}
			}
			if err := startReadyTasks(); err != nil {
				if runCtx.Err() != nil {
					return e.finalizeCancellation(ctx, snapshot)
				}
				return WorkflowRun{}, err
			}
			continue
		}

		// 没有运行中 Attempt 但仍有 waiting_retry 时，Run 尚未结束，只等待下一次重试到期。
		if !hasWaitingRetry(snapshot.Run.Tasks) {
			break
		}
		var retry retryReady
		select {
		case <-runCtx.Done():
			return e.finalizeCancellation(ctx, snapshot)
		case retry = <-retries:
			if runCtx.Err() != nil {
				return e.finalizeCancellation(ctx, snapshot)
			}
		}
		updated := cloneRunSnapshot(snapshot)
		applied, err := applyRetryReady(&updated, retry)
		if err != nil {
			return WorkflowRun{}, err
		}
		if applied {
			if err := e.store.Save(ctx, updated); err != nil {
				if runCtx.Err() != nil {
					return e.finalizeCancellation(ctx, snapshot)
				}
				return WorkflowRun{}, err
			}
			snapshot = updated
		}
		if err := startReadyTasks(); err != nil {
			if runCtx.Err() != nil {
				return e.finalizeCancellation(ctx, snapshot)
			}
			return WorkflowRun{}, err
		}
	}

	// 所有活动 Attempt 和等待重试都已收敛后，才计算并持久化 Workflow 终态。
	finalStatus := WorkflowSucceeded
	reason := "all tasks succeeded"
	if hasFailedTask(snapshot.Run.Tasks) {
		finalStatus = WorkflowFailed
		reason = "one or more tasks failed"
	} else if !allTasksSucceeded(snapshot.Run.Tasks) {
		return WorkflowRun{}, fmt.Errorf("workflow %q has no runnable tasks", id)
	}
	updated := cloneRunSnapshot(snapshot)
	if err := transitionWorkflow(&updated, finalStatus, e.clock.Now(), reason); err != nil {
		return WorkflowRun{}, err
	}
	if err := e.store.Save(ctx, updated); err != nil {
		if runCtx.Err() != nil {
			return e.finalizeCancellation(ctx, snapshot)
		}
		return WorkflowRun{}, err
	}
	e.forgetCompiled(id)
	return updated.Run, nil
}

// GetRun 返回持久化的运行状态，不会启动或修改调度。
func (e *Engine) GetRun(ctx context.Context, id RunID) (WorkflowRun, error) {
	snapshot, err := e.store.Load(ctx, id)
	if err != nil {
		return WorkflowRun{}, err
	}
	return snapshot.Run, nil
}

// Cancel 请求取消指定 Run。活动 Run 由 Execute 收敛状态，非活动 Run 在此直接保存取消快照。
func (e *Engine) Cancel(ctx context.Context, id RunID) error {
	e.mu.Lock()
	cancel, active := e.active[id]
	if active {
		e.mu.Unlock()
		// nil 只表示请求已送达；最终取消状态仍由 Execute 的事件循环持久化。
		cancel()
		return nil
	}
	// 未活动检查和快照写入共用 Engine 锁，防止 Execute 同时基于旧快照注册并启动任务。
	snapshot, err := e.store.Load(ctx, id)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	if isWorkflowTerminal(snapshot.Run.Status) {
		e.mu.Unlock()
		return nil
	}
	updated, err := cancellationSnapshot(snapshot, e.clock.Now())
	if err == nil {
		err = e.store.Save(ctx, updated)
	}
	if err == nil {
		delete(e.compiled, id)
	}
	e.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// executionCompletion 把 Executor 返回结果转换为事件循环可串行处理的消息。
type executionCompletion struct {
	// taskIndex 对应编译定义和 Run.Tasks 的同一数组下标。
	taskIndex int
	// attempt 用于判断结果是否仍属于该任务当前活动的 Attempt。
	attempt  int
	response ExecutionResponse
	// finished 由 Engine 时钟生成，作为状态和审计事件的完成时间。
	finished time.Time
}

// executionTimeout 表示某个 Attempt 的超时计时器已经触发。
type executionTimeout struct {
	// taskIndex 和 attempt 共同定位创建该计时器的 Attempt。
	taskIndex int
	attempt   int
	// at 是超时计时器触发时间，不是 Executor 实际返回时间。
	at time.Time
}

// retryReady 表示固定重试间隔已经结束，但不保证对应任务仍在等待该次重试。
type retryReady struct {
	// taskIndex 对应等待重试任务在 Run.Tasks 中的下标。
	taskIndex int
	// attempt 绑定注册计时器时的失败 Attempt，防止旧计时器唤醒后续重试。
	attempt int
	// at 是计时器触发时间，必须不早于快照中的 ReadyAt。
	at time.Time
}

// attemptKey 使事件循环能够丢弃已经超时或被后续重试取代的迟到结果。
type attemptKey struct {
	taskIndex int
	attempt   int
}

// attemptControl 同时停止 Executor 和该 Attempt 的超时监听。
type attemptControl struct {
	// cancel 终止 Executor 使用的 Attempt Context。
	cancel context.CancelFunc
	// done 仅停止超时监听；每个活动 Attempt 只允许关闭一次。
	done chan struct{}
}

// prepareTask 只修改快照副本；调用方必须先保存该副本，再调用 executeTask。
func (e *Engine) prepareTask(snapshot *RunSnapshot, compiled *CompiledWorkflow, taskIndex int) (ExecutionRequest, int, error) {
	task := &snapshot.Run.Tasks[taskIndex]
	if err := transitionTask(snapshot, taskIndex, TaskRunning, e.clock.Now(), "execution started"); err != nil {
		return ExecutionRequest{}, 0, err
	}
	attempt := Attempt{
		Number:    len(task.Attempts) + 1,
		Status:    AttemptRunning,
		StartedAt: e.clock.Now(),
	}
	task.Attempts = append(task.Attempts, attempt)
	definition := compiled.definition.Tasks[taskIndex]
	return ExecutionRequest{
		DefinitionID: snapshot.Run.DefinitionID,
		RunID:        snapshot.Run.ID,
		TaskKey:      definition.Key,
		Action:       definition.Action,
		Attempt:      attempt.Number,
	}, attempt.Number, nil
}

// executeTask 只在任务 running 快照保存成功后启动 Executor，并把完成或超时交回事件循环。
func (e *Engine) executeTask(
	ctx context.Context,
	request ExecutionRequest,
	taskIndex int,
	attempt int,
	timeout time.Duration,
	completions chan<- executionCompletion,
	timeouts chan<- executionTimeout,
) attemptControl {
	attemptCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	// Executor 使用更细粒度的 attemptCtx；单次超时不会取消整个 Run。
	go func() {
		response := e.executor.Execute(attemptCtx, request)
		completion := executionCompletion{
			taskIndex: taskIndex,
			attempt:   attempt,
			response:  response,
			finished:  e.clock.Now(),
		}
		// Run 结束后事件循环不再接收，直接丢弃迟到结果以避免 goroutine 阻塞。
		select {
		case completions <- completion:
		case <-ctx.Done():
		}
	}()
	// 超时监听与 Executor 分离，使不主动返回的执行器仍能被调度器按时取消。
	go func() {
		select {
		case at := <-e.clock.After(timeout):
			select {
			case timeouts <- executionTimeout{taskIndex: taskIndex, attempt: attempt, at: at}:
			case <-ctx.Done():
			case <-done:
			}
		case <-ctx.Done():
		case <-done:
		}
	}()
	return attemptControl{cancel: cancel, done: done}
}

// finishActiveAttempt 以键作为一次性结算门闩，并同时释放 Executor 与超时监听资源。
func finishActiveAttempt(active map[attemptKey]attemptControl, key attemptKey) bool {
	control, exists := active[key]
	if !exists {
		return false
	}
	control.cancel()
	close(control.done)
	delete(active, key)
	return true
}

// scheduleRetry 只在 waiting_retry 快照保存成功后注册固定间隔计时器。
func (e *Engine) scheduleRetry(ctx context.Context, taskIndex int, task TaskRun, retries chan<- retryReady) {
	if task.ReadyAt == nil || len(task.Attempts) == 0 {
		return
	}
	attempt := task.Attempts[len(task.Attempts)-1].Number
	delay := task.ReadyAt.Sub(e.clock.Now())
	// 恢复或事件处理延迟可能使 ReadyAt 已经过期，此时立即投递重试事件。
	if delay < 0 {
		delay = 0
	}
	go func() {
		select {
		case at := <-e.clock.After(delay):
			select {
			case retries <- retryReady{taskIndex: taskIndex, attempt: attempt, at: at}:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
}

// applyCompletion 只接受当前 running Attempt 的首次完成消息，防止重复结果重复解锁下游。
func applyCompletion(snapshot *RunSnapshot, compiled *CompiledWorkflow, completion executionCompletion) (bool, error) {
	if completion.taskIndex < 0 || completion.taskIndex >= len(snapshot.Run.Tasks) {
		return false, fmt.Errorf("invalid completion task index %d", completion.taskIndex)
	}
	task := &snapshot.Run.Tasks[completion.taskIndex]
	if task.Status != TaskRunning || len(task.Attempts) == 0 {
		return false, nil
	}
	attempt := &task.Attempts[len(task.Attempts)-1]
	if attempt.Number != completion.attempt || attempt.Status != AttemptRunning {
		return false, nil
	}
	// ResultKind 是 Engine 与 Executor 的封闭协议；未知值属于系统错误，不能猜测业务终态。
	if completion.response.Kind != ResultSuccess &&
		completion.response.Kind != ResultTemporaryFailure &&
		completion.response.Kind != ResultPermanentFailure {
		return false, fmt.Errorf("unsupported execution result %q", completion.response.Kind)
	}
	attempt.Result = ExecutionResult{
		Output:       completion.response.Output,
		ErrorCode:    completion.response.ErrorCode,
		ErrorMessage: completion.response.ErrorMessage,
	}
	// 临时失败与永久失败都结束当前 Attempt，但只有临时失败进入统一重试决策。
	if completion.response.Kind == ResultTemporaryFailure {
		if err := transitionAttempt(snapshot, task.Key, attempt, AttemptFailed, completion.finished, "temporary execution failure"); err != nil {
			return false, err
		}
		return applyRetryDecision(snapshot, compiled, completion.taskIndex, attempt, completion.finished, "temporary execution failure")
	}
	if completion.response.Kind == ResultPermanentFailure {
		if err := transitionAttempt(snapshot, task.Key, attempt, AttemptFailed, completion.finished, "permanent execution failure"); err != nil {
			return false, err
		}
		if err := transitionTask(snapshot, completion.taskIndex, TaskFailed, completion.finished, "permanent execution failure"); err != nil {
			return false, err
		}
		if err := skipDescendants(snapshot, compiled, completion.taskIndex, completion.finished); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptSucceeded, completion.finished, "execution succeeded"); err != nil {
		return false, err
	}
	if err := transitionTask(snapshot, completion.taskIndex, TaskSucceeded, completion.finished, "execution succeeded"); err != nil {
		return false, err
	}
	// 成功只解锁直接下游；每条依赖边恰好使对应计数减一，降到零时任务才 ready。
	for _, successorIndex := range compiled.successors[completion.taskIndex] {
		snapshot.Run.RemainingDependencies[successorIndex]--
		if snapshot.Run.RemainingDependencies[successorIndex] < 0 {
			return false, fmt.Errorf("task %q dependency count became negative", snapshot.Run.Tasks[successorIndex].Key)
		}
		if snapshot.Run.RemainingDependencies[successorIndex] == 0 {
			if err := transitionTask(snapshot, successorIndex, TaskReady, completion.finished, "dependencies succeeded"); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// applyTimeout 只结算仍处于 running 的同编号 Attempt，并复用临时失败的重试策略。
func applyTimeout(snapshot *RunSnapshot, compiled *CompiledWorkflow, timeout executionTimeout) (bool, error) {
	if timeout.taskIndex < 0 || timeout.taskIndex >= len(snapshot.Run.Tasks) {
		return false, fmt.Errorf("invalid timeout task index %d", timeout.taskIndex)
	}
	task := &snapshot.Run.Tasks[timeout.taskIndex]
	if task.Status != TaskRunning || len(task.Attempts) == 0 {
		return false, nil
	}
	attempt := &task.Attempts[len(task.Attempts)-1]
	if attempt.Number != timeout.attempt || attempt.Status != AttemptRunning {
		return false, nil
	}
	attempt.Result = ExecutionResult{ErrorCode: "timeout", ErrorMessage: "execution timed out"}
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptTimedOut, timeout.at, "execution timed out"); err != nil {
		return false, err
	}
	return applyRetryDecision(snapshot, compiled, timeout.taskIndex, attempt, timeout.at, "execution timed out")
}

// applyRetryDecision 在剩余次数与最终失败之间二选一；调用方负责保存副本后再启动计时器。
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
	return true, nil
}

// applyRetryReady 忽略过期或提前到达的计时器事件，只把当前 waiting_retry 任务转回 ready。
func applyRetryReady(snapshot *RunSnapshot, retry retryReady) (bool, error) {
	if retry.taskIndex < 0 || retry.taskIndex >= len(snapshot.Run.Tasks) {
		return false, fmt.Errorf("invalid retry task index %d", retry.taskIndex)
	}
	task := &snapshot.Run.Tasks[retry.taskIndex]
	if task.Status != TaskWaitingRetry || task.ReadyAt == nil || len(task.Attempts) == 0 {
		return false, nil
	}
	if task.Attempts[len(task.Attempts)-1].Number != retry.attempt || retry.at.Before(*task.ReadyAt) {
		return false, nil
	}
	if err := transitionTask(snapshot, retry.taskIndex, TaskReady, retry.at, "retry interval elapsed"); err != nil {
		return false, err
	}
	return true, nil
}

func hasWaitingRetry(tasks []TaskRun) bool {
	for _, task := range tasks {
		if task.Status == TaskWaitingRetry {
			return true
		}
	}
	return false
}

// finalizeCancellation 在快照副本中取消所有未终止任务，并在保存成功后返回取消终态。
func (e *Engine) finalizeCancellation(ctx context.Context, snapshot RunSnapshot) (WorkflowRun, error) {
	updated, err := cancellationSnapshot(snapshot, e.clock.Now())
	if err != nil {
		return WorkflowRun{}, err
	}
	// 父 Context 可能正是取消来源；清理写入必须保留其值但忽略取消信号。
	if err := e.store.Save(context.WithoutCancel(ctx), updated); err != nil {
		return WorkflowRun{}, err
	}
	e.forgetCompiled(updated.Run.ID)
	return updated.Run, nil
}

// cancellationSnapshot 使用同一时间点把所有未终止任务和 Workflow 收敛为取消状态。
func cancellationSnapshot(snapshot RunSnapshot, at time.Time) (RunSnapshot, error) {
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

// skipDescendants 沿 DAG 向下传播失败，只跳过尚未进入终态的后代。
func skipDescendants(snapshot *RunSnapshot, compiled *CompiledWorkflow, failedTask int, at time.Time) error {
	queue := append([]int(nil), compiled.successors[failedTask]...)
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

func isTaskTerminal(status TaskStatus) bool {
	return status == TaskSucceeded || status == TaskFailed || status == TaskCanceled || status == TaskSkipped
}

func allTasksSucceeded(tasks []TaskRun) bool {
	for _, task := range tasks {
		if task.Status != TaskSucceeded {
			return false
		}
	}
	return true
}

func hasFailedTask(tasks []TaskRun) bool {
	for _, task := range tasks {
		if task.Status == TaskFailed {
			return true
		}
	}
	return false
}

func (e *Engine) compiledForRun(id RunID, snapshot RunSnapshot) (*CompiledWorkflow, error) {
	e.mu.Lock()
	compiled := e.compiled[id]
	e.mu.Unlock()
	if compiled != nil {
		return compiled, nil
	}
	// 进程重启后缓存为空，必须从快照内定义重编译，不能依赖 CreateRun 时的内存对象。
	if snapshot.Definition == nil {
		return nil, fmt.Errorf("run %q has no stored definition", id)
	}
	compiled, err := Compile(*snapshot.Definition)
	if err != nil {
		return nil, fmt.Errorf("compile stored definition: %w", err)
	}
	e.mu.Lock()
	e.compiled[id] = compiled
	e.mu.Unlock()
	return compiled, nil
}

func (e *Engine) forgetCompiled(id RunID) {
	e.mu.Lock()
	delete(e.compiled, id)
	e.mu.Unlock()
}

// cloneRunSnapshot 深拷贝会被状态转换修改的切片，确保保存失败时原快照完全不变。
func cloneRunSnapshot(snapshot RunSnapshot) RunSnapshot {
	clone := snapshot
	clone.Run.Tasks = append([]TaskRun(nil), snapshot.Run.Tasks...)
	for i := range clone.Run.Tasks {
		clone.Run.Tasks[i].Attempts = append([]Attempt(nil), snapshot.Run.Tasks[i].Attempts...)
	}
	clone.Run.RemainingDependencies = append([]int(nil), snapshot.Run.RemainingDependencies...)
	clone.Events = append([]StateEvent(nil), snapshot.Events...)
	return clone
}

func randomRunID() (RunID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}
	return RunID(hex.EncodeToString(raw[:])), nil
}
