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
	Clock    Clock
	NewRunID func() (RunID, error)
}

// Engine 串行提交单个 Run 的状态，并在定义的并发上限内协调 Executor。
type Engine struct {
	store    Store
	executor Executor
	clock    Clock
	newRunID func() (RunID, error)

	mu       sync.Mutex
	active   map[RunID]context.CancelFunc
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
	e.mu.Lock()
	e.compiled[id] = compiled
	e.mu.Unlock()
	return id, nil
}

// Execute 调度指定 Run 的可执行任务，当前只支持全部任务成功的路径。
func (e *Engine) Execute(ctx context.Context, id RunID) (WorkflowRun, error) {
	snapshot, err := e.store.Load(ctx, id)
	if err != nil {
		return WorkflowRun{}, err
	}
	if isWorkflowTerminal(snapshot.Run.Status) {
		return snapshot.Run, nil
	}
	compiled, err := e.compiledForRun(id, snapshot)
	if err != nil {
		return WorkflowRun{}, err
	}

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

	if snapshot.Run.Status == WorkflowPending {
		updated := cloneRunSnapshot(snapshot)
		if err := transitionWorkflow(&updated, WorkflowRunning, e.clock.Now(), "execution started"); err != nil {
			return WorkflowRun{}, err
		}
		if err := e.store.Save(ctx, updated); err != nil {
			return WorkflowRun{}, err
		}
		snapshot = updated
	}

	completions := make(chan executionCompletion, len(snapshot.Run.Tasks))
	running := 0
	startReadyTasks := func() error {
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
			snapshot = updated
			e.executeTask(runCtx, request, taskIndex, attempt, completions)
			running++
		}
		return nil
	}

	if err := startReadyTasks(); err != nil {
		return WorkflowRun{}, err
	}
	for running > 0 {
		completion := <-completions
		running--
		if completion.response.Kind != ResultSuccess {
			return WorkflowRun{}, fmt.Errorf("unsupported execution result %q", completion.response.Kind)
		}
		updated := cloneRunSnapshot(snapshot)
		applied, err := applyCompletion(&updated, compiled, completion)
		if err != nil {
			return WorkflowRun{}, err
		}
		if applied {
			if err := e.store.Save(ctx, updated); err != nil {
				return WorkflowRun{}, err
			}
			snapshot = updated
		}
		if err := startReadyTasks(); err != nil {
			return WorkflowRun{}, err
		}
	}

	if !allTasksSucceeded(snapshot.Run.Tasks) {
		return WorkflowRun{}, fmt.Errorf("workflow %q has no runnable tasks", id)
	}
	updated := cloneRunSnapshot(snapshot)
	if err := transitionWorkflow(&updated, WorkflowSucceeded, e.clock.Now(), "all tasks succeeded"); err != nil {
		return WorkflowRun{}, err
	}
	if err := e.store.Save(ctx, updated); err != nil {
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

type executionCompletion struct {
	taskIndex int
	attempt   int
	response  ExecutionResponse
	finished  time.Time
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

// executeTask 只在任务 running 快照保存成功后启动 Executor，并把结果交回事件循环。
func (e *Engine) executeTask(ctx context.Context, request ExecutionRequest, taskIndex, attempt int, completions chan<- executionCompletion) {
	go func() {
		response := e.executor.Execute(ctx, request)
		completions <- executionCompletion{
			taskIndex: taskIndex,
			attempt:   attempt,
			response:  response,
			finished:  e.clock.Now(),
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
	if err := transitionAttempt(snapshot, task.Key, attempt, AttemptSucceeded, completion.finished, "execution succeeded"); err != nil {
		return false, err
	}
	attempt.Result = ExecutionResult{
		Output:       completion.response.Output,
		ErrorCode:    completion.response.ErrorCode,
		ErrorMessage: completion.response.ErrorMessage,
	}
	if err := transitionTask(snapshot, completion.taskIndex, TaskSucceeded, completion.finished, "execution succeeded"); err != nil {
		return false, err
	}
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

func allTasksSucceeded(tasks []TaskRun) bool {
	for _, task := range tasks {
		if task.Status != TaskSucceeded {
			return false
		}
	}
	return true
}

func (e *Engine) compiledForRun(id RunID, snapshot RunSnapshot) (*CompiledWorkflow, error) {
	e.mu.Lock()
	compiled := e.compiled[id]
	e.mu.Unlock()
	if compiled != nil {
		return compiled, nil
	}
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
