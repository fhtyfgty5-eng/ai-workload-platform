package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	defaultLockCheckInterval = 5 * time.Second
	defaultLockCheckTimeout  = 2 * time.Second
)

// Lock 表示本进程持有的协调所有权；Check 必须在取得锁的同一数据库连接上执行。
type Lock interface {
	Check(context.Context) error
	Close() error
}

type RecoveryStore interface {
	ListNonTerminal(context.Context) ([]workflow.RunID, error)
	Load(context.Context, workflow.RunID) (workflow.RunSnapshot, error)
}

// RunEngine 是 Coordinator 监督的新 Run 与恢复 Run 执行边界。
type RunEngine interface {
	Execute(context.Context, workflow.RunID) (workflow.WorkflowRun, error)
	Resume(context.Context, workflow.RunID) (workflow.WorkflowRun, error)
}

type LockAcquirer func(context.Context) (Lock, error)

// CoordinatorOptions 控制锁健康检查周期，并允许调用方注入统一日志器。
type CoordinatorOptions struct {
	LockCheckInterval time.Duration
	LockCheckTimeout  time.Duration
	Logger            *slog.Logger
}

// Coordinator 统一监督 Run 执行和单实例锁；任一系统错误都会取消同进程的其他执行。
type Coordinator struct {
	store   RecoveryStore
	engine  RunEngine
	acquire LockAcquirer
	options CoordinatorOptions

	mu        sync.RWMutex
	ready     bool
	started   bool
	closing   bool
	lock      Lock
	runCtx    context.Context
	cancel    context.CancelFunc
	closeDone chan struct{}
	closeErr  error

	errors chan error
	fatal  sync.Once
	wg     sync.WaitGroup
}

func NewCoordinator(store RecoveryStore, engine RunEngine, acquire LockAcquirer, options CoordinatorOptions) *Coordinator {
	if options.LockCheckInterval <= 0 {
		options.LockCheckInterval = defaultLockCheckInterval
	}
	if options.LockCheckTimeout <= 0 {
		options.LockCheckTimeout = defaultLockCheckTimeout
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Coordinator{
		store:     store,
		engine:    engine,
		acquire:   acquire,
		options:   options,
		errors:    make(chan error, 1),
		closeDone: make(chan struct{}),
	}
}

// Start 取得协调锁并验证恢复输入；Run 的实际恢复在后台执行，不阻塞服务进入就绪状态。
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started || c.closing {
		c.mu.Unlock()
		return fmt.Errorf("coordinator has already been started or closed")
	}
	if c.store == nil || c.engine == nil || c.acquire == nil {
		c.mu.Unlock()
		return fmt.Errorf("coordinator dependencies are required")
	}
	c.started = true
	c.mu.Unlock()

	lock, err := c.acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire coordinator lock: %w", err)
	}
	ids, err := c.store.ListNonTerminal(ctx)
	if err != nil {
		_ = lock.Close()
		return fmt.Errorf("list non-terminal Runs: %w", err)
	}
	for _, id := range ids {
		// 预先 Load 可在服务就绪前拒绝损坏快照；Resume 本身稍后仍会重新读取数据库事实。
		if _, err := c.store.Load(ctx, id); err != nil {
			_ = lock.Close()
			return fmt.Errorf("load Run %s for recovery: %w", id, err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.lock = lock
	c.runCtx = runCtx
	c.cancel = cancel
	c.ready = true
	// Add 与 ready 状态在同一临界区完成，避免 Close 在 Wait 开始后再出现新的 goroutine。
	for _, id := range ids {
		c.startRunLocked(id, true)
	}
	c.wg.Add(1)
	go c.checkLock(runCtx, lock)
	c.mu.Unlock()
	return nil
}

// Enqueue 把已持久化的新 Run 交给监督器；关闭期间拒绝接管，Run 将由下次启动恢复。
func (c *Coordinator) Enqueue(id workflow.RunID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ready || c.closing {
		return
	}
	c.options.Logger.Info("run enqueued", "run_id", id)
	c.startRunLocked(id, false)
}

// startRunLocked 要求调用方持有 c.mu，以保证 WaitGroup.Add 不会与 Close 的 Wait 竞态。
func (c *Coordinator) startRunLocked(id workflow.RunID, resume bool) {
	c.wg.Add(1)
	ctx := c.runCtx
	operation := "execution"
	if resume {
		operation = "recovery"
	}
	c.options.Logger.Info("run "+operation+" started", "run_id", id)
	go func() {
		defer c.wg.Done()
		var (
			run workflow.WorkflowRun
			err error
		)
		if resume {
			run, err = c.engine.Resume(ctx, id)
		} else {
			run, err = c.engine.Execute(ctx, id)
		}
		if err == nil {
			c.options.Logger.Info("run "+operation+" completed", "run_id", id, "run_status", run.Status)
			return
		}
		if ctx.Err() != nil {
			return
		}
		failureOperation := "execute"
		if resume {
			failureOperation = "resume"
		}
		c.options.Logger.Error("run "+operation+" failed", "run_id", id, "error", err)
		c.fail(fmt.Errorf("%s Run %s: %w", failureOperation, id, err))
	}()
}

func (c *Coordinator) checkLock(ctx context.Context, lock Lock) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.options.LockCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, c.options.LockCheckTimeout)
			err := lock.Check(checkCtx)
			cancel()
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			c.fail(fmt.Errorf("check coordinator lock: %w", err))
			return
		}
	}
}

// fail 只发布第一次致命错误，并立即撤销就绪状态、停止本进程内的全部 Run。
func (c *Coordinator) fail(err error) {
	c.fatal.Do(func() {
		c.mu.Lock()
		c.ready = false
		cancel := c.cancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		c.errors <- err
	})
}

func (c *Coordinator) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// Errors 返回首次致命错误通道；调用方收到错误后应停止 HTTP 并让进程错误退出。
func (c *Coordinator) Errors() <-chan error { return c.errors }

// Close 停止接管新 Run，并在调用方给定的期限内等待执行退出和协调锁释放。
func (c *Coordinator) Close(ctx context.Context) error {
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		c.ready = false
		if c.cancel != nil {
			c.cancel()
		}
		lock := c.lock
		go c.finishClose(lock)
	}
	done := c.closeDone
	c.mu.Unlock()

	select {
	case <-done:
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) finishClose(lock Lock) {
	c.wg.Wait()
	var err error
	if lock != nil {
		err = lock.Close()
	}
	c.mu.Lock()
	c.lock = nil
	c.closeErr = err
	close(c.closeDone)
	c.mu.Unlock()
}
