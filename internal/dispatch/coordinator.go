package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	defaultScanInterval      = time.Second
	defaultLockCheckInterval = 5 * time.Second
	defaultLockCheckTimeout  = 2 * time.Second
	defaultBatchSize         = 100
)

// DispatchCoordinator 监督持久化分发创建和单协调器锁。
type DispatchCoordinator struct {
	// store 推进数据库中的分发状态；acquire 获取并持续持有单协调器锁。
	store   Store
	acquire LockAcquirer
	// options 定义扫描、锁检查和单轮批量上限。
	options CoordinatorOptions

	// mu 保护以下生命周期字段，避免启动、失败和关闭并发改变状态。
	mu sync.RWMutex
	// ready 表示可以接收业务唤醒；started 防止重复启动；closing 阻止关闭后重新工作。
	ready   bool
	started bool
	closing bool
	// lock 是当前进程持有的协调锁；runCtx/cancel 控制两个后台循环的共同生命周期。
	lock   Lock
	runCtx context.Context
	cancel context.CancelFunc

	// wake 是可合并的扫描提示；errors 只发布首个致命错误；fatal 保证失败处理只执行一次。
	wake   chan struct{}
	errors chan error
	fatal  sync.Once
	// wg 等待后台循环退出；closeOnce/closeDone/closeErr 协调并发 Close 调用。
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// NewCoordinator 使用默认值补齐未配置的时间参数，但不会立即获取锁或启动后台循环。
func NewCoordinator(store Store, acquire LockAcquirer, options CoordinatorOptions) *DispatchCoordinator {
	if options.ScanInterval <= 0 {
		options.ScanInterval = defaultScanInterval
	}
	if options.LockCheckInterval <= 0 {
		options.LockCheckInterval = defaultLockCheckInterval
	}
	if options.LockCheckTimeout <= 0 {
		options.LockCheckTimeout = defaultLockCheckTimeout
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultBatchSize
	}
	return &DispatchCoordinator{
		store:     store,
		acquire:   acquire,
		options:   options,
		wake:      make(chan struct{}, 1),
		errors:    make(chan error, 1),
		closeDone: make(chan struct{}),
	}
}

// Start 获取单协调器锁并启动分发扫描与锁检查；同一实例只能启动一次。
func (c *DispatchCoordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started || c.closing {
		c.mu.Unlock()
		return fmt.Errorf("dispatch coordinator has already been started or closed")
	}
	if c.store == nil || c.acquire == nil {
		c.mu.Unlock()
		return fmt.Errorf("dispatch coordinator dependencies are required")
	}
	c.started = true
	c.mu.Unlock()

	lock, err := c.acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire dispatch coordinator lock: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.lock = lock
	c.runCtx = runCtx
	c.cancel = cancel
	c.ready = true
	c.wg.Add(2)
	go c.scanLoop(runCtx)
	go c.lockLoop(runCtx, lock)
	c.mu.Unlock()
	return nil
}

// Wake 请求尽快执行一次扫描。重复提示可以合并，因为 PostgreSQL 始终是事实来源。
func (c *DispatchCoordinator) Wake() {
	c.mu.RLock()
	ready := c.ready && !c.closing
	c.mu.RUnlock()
	if !ready {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Cancel 先持久化并收敛 Run 取消状态，再唤醒分发扫描处理后续变化。
func (c *DispatchCoordinator) Cancel(ctx context.Context, id workflow.RunID) error {
	c.mu.RLock()
	ready := c.ready && !c.closing
	c.mu.RUnlock()
	if !ready {
		return ErrCoordinatorNotReady
	}
	if err := c.store.CancelRun(ctx, id); err != nil {
		return err
	}
	c.Wake()
	return nil
}

// Ready 表示协调器已经持锁、后台循环尚未失败且没有进入关闭流程。
func (c *DispatchCoordinator) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready && !c.closing
}

// Errors 返回只发布首个致命后台错误的只读通道。
func (c *DispatchCoordinator) Errors() <-chan error { return c.errors }

func (c *DispatchCoordinator) scanLoop(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.options.ScanInterval)
	defer ticker.Stop()
	for {
		if _, err := c.store.CancelRequestedRuns(ctx, c.options.BatchSize); err != nil {
			if ctx.Err() == nil {
				c.fail(fmt.Errorf("reconcile requested Run cancellations: %w", err))
			}
			return
		}
		if _, err := c.store.ReapExpired(ctx, c.options.BatchSize); err != nil {
			if ctx.Err() == nil {
				c.fail(fmt.Errorf("reap expired task leases: %w", err))
			}
			return
		}
		created, err := c.store.CreateDispatches(ctx, c.options.BatchSize)
		if err != nil {
			if ctx.Err() == nil {
				c.fail(fmt.Errorf("create task dispatches: %w", err))
			}
			return
		}
		// CreateDispatches 每轮最多推进每个 Run 的一个任务以保持公平。
		// 只要一轮公平扫描仍有进展就立即继续，使少量大 Run 也能填满 Worker 容量，
		// 不必让每个任务都等待下一次周期扫描。
		if created > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		case <-ticker.C:
		}
	}
}

func (c *DispatchCoordinator) lockLoop(ctx context.Context, lock Lock) {
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
			if err != nil {
				if ctx.Err() == nil {
					c.fail(fmt.Errorf("check dispatch coordinator lock: %w", err))
				}
				return
			}
		}
	}
}

func (c *DispatchCoordinator) fail(err error) {
	c.fatal.Do(func() {
		c.mu.Lock()
		c.ready = false
		cancel := c.cancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		select {
		case c.errors <- err:
		default:
		}
	})
}

// Close 幂等停止后台循环、等待退出并释放协调锁。
func (c *DispatchCoordinator) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closing = true
		c.ready = false
		cancel := c.cancel
		lock := c.lock
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		go func() {
			c.wg.Wait()
			if lock != nil {
				c.closeErr = lock.Close()
			}
			close(c.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeDone:
		return c.closeErr
	}
}

var _ Coordinator = (*DispatchCoordinator)(nil)
