// Package workerruntime 运行一个独立注册的 Worker 进程。
package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Client 定义 Worker 运行循环访问控制面所需的最小协议操作。
type Client interface {
	Register(context.Context, string, workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error)
	Claim(context.Context, string, string, int) (workerprotocol.ClaimResponse, error)
	Heartbeat(context.Context, string, string, workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error)
	Complete(context.Context, string, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error)
	Drain(context.Context, string, string) (workerprotocol.WorkerSummary, error)
}

// Options 保存 Worker 会话、轮询、心跳、重试和退出策略。
type Options struct {
	// BootstrapToken 只用于注册新会话；后续请求使用服务端签发的 SessionToken。
	BootstrapToken string
	// Registration 声明 Worker 名称、协议版本、执行能力和最大并发。
	Registration workerprotocol.RegisterRequest
	// PollMin 和 PollMax 定义空领取后的指数退避范围。
	PollMin time.Duration
	PollMax time.Duration
	// HeartbeatInterval 是服务端未返回有效周期时使用的本地默认值。
	HeartbeatInterval time.Duration
	// RetryInterval 控制完成请求遇到可重试网络错误后的等待时间。
	RetryInterval time.Duration
	// SafetyMargin 让本地执行在服务端租约或 Attempt 截止时间之前提前停止。
	SafetyMargin time.Duration
	// ShutdownTimeout 限制 draining 阶段等待活动租约结束的最长时间。
	ShutdownTimeout time.Duration
	// Random 注入 [0,1] 随机数源，用于轮询抖动并支持确定性测试。
	Random func() float64
}

// Runtime 组合控制面客户端、执行器和运行策略，管理一个 Worker 会话的完整生命周期。
type Runtime struct {
	client   Client
	executor workflow.Executor
	options  Options
}

// activeLease 保存一个本地执行中的租约及其取消和续租信号。
type activeLease struct {
	// lease 是控制面签发且在本地执行期间保持不变的任务与所有权信息。
	lease workerprotocol.Lease
	// cancel 在租约撤销、本地安全期限到达或进程退出时停止对应执行器。
	cancel context.CancelFunc
	// renew 只保留最新剩余租约时长，由心跳循环发送、期限计时器消费。
	renew chan time.Duration
}

// New 校验 Worker 依赖与运行策略，并构造尚未注册会话的 Runtime。
func New(client Client, executor workflow.Executor, options Options) (*Runtime, error) {
	if client == nil || executor == nil {
		return nil, fmt.Errorf("Worker client and executor are required")
	}
	if options.BootstrapToken == "" {
		return nil, fmt.Errorf("Worker bootstrap token is required")
	}
	if options.Registration.ProtocolVersion != workerprotocol.ProtocolVersion {
		return nil, fmt.Errorf("unsupported Worker protocol version")
	}
	if options.Registration.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("Worker max concurrency must be positive")
	}
	if options.PollMin <= 0 || options.PollMax < options.PollMin {
		return nil, fmt.Errorf("invalid Worker poll interval range")
	}
	if options.HeartbeatInterval <= 0 || options.RetryInterval <= 0 {
		return nil, fmt.Errorf("Worker heartbeat and retry intervals must be positive")
	}
	if options.SafetyMargin < 0 || options.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("invalid Worker shutdown timing")
	}
	if options.Random == nil {
		options.Random = func() float64 { return 0.5 }
	}
	return &Runtime{client: client, executor: executor, options: options}, nil
}

// Run 注册一个会话并持续领取和执行任务；父 Context 取消后先 drain，
// 再在关闭期限内等待或停止剩余本地任务。
func (r *Runtime) Run(ctx context.Context) error {
	registration, err := r.client.Register(ctx, r.options.BootstrapToken, r.options.Registration)
	if err != nil {
		return err
	}
	if registration.WorkerID == "" || registration.SessionToken == "" {
		return fmt.Errorf("Worker registration returned incomplete credentials")
	}
	heartbeatInterval := r.options.HeartbeatInterval
	if registration.HeartbeatIntervalMillis > 0 {
		heartbeatInterval = time.Duration(registration.HeartbeatIntervalMillis) * time.Millisecond
	}
	// 会话 Context 与进程信号分离，使优雅退出能够先停止领取新任务，
	// 同时让已有租约继续发送心跳并完成。
	runCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	pollCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()
	state := newLeaseState()
	heartbeatDone := make(chan error, 1)
	go func() {
		heartbeatErr := r.heartbeatLoop(runCtx, registration.WorkerID, registration.SessionToken, heartbeatInterval, state)
		heartbeatDone <- heartbeatErr
		if heartbeatErr != nil {
			stopPolling()
			stop()
		}
	}()

	pollDelay := r.options.PollMin
	var sessionErr error
	for ctx.Err() == nil && runCtx.Err() == nil {
		slots := r.options.Registration.MaxConcurrency - state.count()
		if slots > 0 {
			response, claimErr := r.client.Claim(pollCtx, registration.WorkerID, registration.SessionToken, slots)
			if isPermanentSessionError(claimErr) {
				sessionErr = fmt.Errorf("claim tasks for Worker session: %w", claimErr)
				stopPolling()
				stop()
				break
			}
			if claimErr == nil && len(response.Leases) > 0 {
				pollDelay = r.options.PollMin
				for _, lease := range response.Leases {
					r.startLease(runCtx, registration.WorkerID, registration.SessionToken, lease, state)
				}
				continue
			}
		}
		delay := jitterDelay(pollDelay, 0.2, r.options.Random)
		pollDelay = nextPollDelay(pollDelay, r.options.PollMin, r.options.PollMax)
		capacityChanged, waiting := state.waitForCapacity(pollCtx, delay)
		if !waiting {
			break
		}
		if capacityChanged {
			pollDelay = r.options.PollMin
		}
	}

	heartbeatFinished := false
	if sessionErr == nil && ctx.Err() == nil && runCtx.Err() != nil {
		sessionErr = <-heartbeatDone
		heartbeatFinished = true
	}
	if sessionErr != nil {
		stop()
		state.cancelAll()
		state.wait()
		if !heartbeatFinished {
			<-heartbeatDone
		}
		return sessionErr
	}

	// 此时父 Context 已取消，因此使用脱离父取消信号但有超时上限的 Context，
	// 完成 drain 请求并等待已经领取的任务。
	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), r.options.ShutdownTimeout)
	defer cancelDrain()
	drainSummary, drainErr := r.client.Drain(drainCtx, registration.WorkerID, registration.SessionToken)

	// draining Worker 可以完成已有租约但不能领取新任务；在任务完成或
	// 关闭期限到达前必须保持心跳循环运行。
	timedOut := false
	for state.count() > 0 {
		select {
		case <-drainCtx.Done():
			state.cancelAll()
			timedOut = true
		case <-time.After(time.Millisecond):
		}
		if timedOut {
			break
		}
	}
	state.wait()
	if drainSummary.Status != workerprotocol.WorkerStopped && drainCtx.Err() == nil {
		// 第一次调用把仍有活动租约的会话改为 draining；租约清空后第二次调用
		// 才把会话推进到 stopped 并记录 stopped_at。
		_, finalDrainErr := r.client.Drain(drainCtx, registration.WorkerID, registration.SessionToken)
		switch {
		case finalDrainErr == nil:
			drainErr = nil
		case drainErr == nil:
			drainErr = finalDrainErr
		default:
			drainErr = errors.Join(drainErr, finalDrainErr)
		}
	}
	stop()
	<-heartbeatDone
	if drainErr != nil && ctx.Err() == nil {
		return drainErr
	}
	return ctx.Err()
}

// startLease 为租约创建独立 Context，并发执行任务并提交最终结果。
func (r *Runtime) startLease(runCtx context.Context, workerID, sessionToken string, lease workerprotocol.Lease, state *leaseState) {
	leaseCtx, cancel := context.WithCancel(runCtx)
	active := activeLease{lease: lease, cancel: cancel, renew: make(chan time.Duration, 1)}
	state.add(active)
	go func() {
		defer state.remove(lease.DispatchID)
		defer cancel()
		go r.enforceLocalSafetyDeadline(leaseCtx, active)
		response := r.executor.Execute(leaseCtx, workflow.ExecutionRequest{
			DefinitionID: lease.DefinitionID,
			RunID:        lease.RunID,
			TaskKey:      lease.TaskKey,
			Action:       lease.Action,
			Input:        lease.Input,
			Attempt:      lease.Attempt,
		})
		if leaseCtx.Err() != nil || response.Kind == workflow.ResultCanceled {
			return
		}
		request := workerprotocol.CompleteRequest{LeaseToken: lease.LeaseToken, Result: response}
		_ = r.submitWithRetry(leaseCtx, workerID, lease.DispatchID, sessionToken, request)
	}()
}

// enforceLocalSafetyDeadline 使用服务端剩余租约时间更新本地计时器，
// 并保证本地执行不会越过 Attempt 的最终截止时间。
func (r *Runtime) enforceLocalSafetyDeadline(ctx context.Context, active activeLease) {
	delay := localSafetyDelay(active.lease, r.options.SafetyMargin)
	if delay <= 0 {
		active.cancel()
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			active.cancel()
			return
		case remaining := <-active.renew:
			delay = remaining - r.options.SafetyMargin
			attemptDelay := time.Until(active.lease.AttemptDeadline) - r.options.SafetyMargin
			if delay > attemptDelay {
				delay = attemptDelay
			}
			if delay <= 0 {
				active.cancel()
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}
	}
}

func localSafetyDelay(lease workerprotocol.Lease, margin time.Duration) time.Duration {
	deadline := lease.AttemptDeadline
	if lease.LeaseExpiresAt.Before(deadline) {
		deadline = lease.LeaseExpiresAt
	}
	return time.Until(deadline) - margin
}

// submitWithRetry 只重试可能恢复的传输或服务错误；所有权丢失和结果冲突必须立即停止。
func (r *Runtime) submitWithRetry(ctx context.Context, workerID, dispatchID, sessionToken string, request workerprotocol.CompleteRequest) error {
	for ctx.Err() == nil {
		_, err := r.client.Complete(ctx, workerID, dispatchID, sessionToken, request)
		if err == nil {
			return nil
		}
		var apiErr *workerclient.APIError
		if errors.As(err, &apiErr) && (apiErr.Code == "lease_lost" || apiErr.Code == "result_conflict") {
			return err
		}
		if !waitFor(ctx, r.options.RetryInterval) {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// heartbeatLoop 即使没有活动租约也持续维护 Worker 会话；有租约时还负责续租或撤销本地执行。
func (r *Runtime) heartbeatLoop(ctx context.Context, workerID, sessionToken string, interval time.Duration, state *leaseState) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			leases := state.refs()
			response, err := r.client.Heartbeat(ctx, workerID, sessionToken, workerprotocol.HeartbeatRequest{Leases: leases})
			if err != nil {
				if isPermanentSessionError(err) {
					return fmt.Errorf("heartbeat Worker session: %w", err)
				}
				continue
			}
			for _, heartbeat := range response.Leases {
				if heartbeat.Status == workerprotocol.LeaseRenewed {
					state.renew(heartbeat.DispatchID, time.Duration(heartbeat.LeaseRemainingMillis)*time.Millisecond)
				} else if heartbeat.Status == workerprotocol.LeaseRevoked || heartbeat.Status == workerprotocol.LeaseUnknown {
					state.cancel(heartbeat.DispatchID)
				}
			}
		}
	}
}

// isPermanentSessionError 区分需要进程重新注册的会话错误与可以继续重试的网络或服务错误。
func isPermanentSessionError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *workerclient.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Status == 401 || apiErr.Code == "worker_session_invalid" || apiErr.Code == "worker_protocol_unsupported"
}

// leaseState 保存当前进程的活动租约，并协调心跳、执行 goroutine 和领取循环。
type leaseState struct {
	// mu 保护 active；所有读取、加入、删除、取消和续租查找都必须持锁。
	mu sync.Mutex
	// active 以 DispatchID 区分任务，即使多个任务使用相同 Action 也不会混淆。
	active map[string]activeLease
	// capacityAvailable 是容量变化提示；容量为 1 可合并多个同时完成通知。
	capacityAvailable chan struct{}
	// wg 等待全部已经启动的租约 goroutine 退出，不承担互斥保护。
	wg sync.WaitGroup
}

func newLeaseState() *leaseState {
	return &leaseState{
		active:            make(map[string]activeLease),
		capacityAvailable: make(chan struct{}, 1),
	}
}

func (s *leaseState) add(lease activeLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[lease.lease.DispatchID] = lease
	s.wg.Add(1)
}

func (s *leaseState) remove(dispatchID string) {
	s.mu.Lock()
	removed := false
	if _, ok := s.active[dispatchID]; ok {
		delete(s.active, dispatchID)
		s.wg.Done()
		removed = true
	}
	s.mu.Unlock()
	if removed {
		// 多个任务可能同时释放槽位；领取循环每次都会重新计算容量，
		// 因此只需保留一个尚未消费的容量变化通知。
		select {
		case s.capacityAvailable <- struct{}{}:
		default:
		}
	}
}

func (s *leaseState) waitForCapacity(ctx context.Context, delay time.Duration) (capacityChanged, waiting bool) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, false
	case <-s.capacityAvailable:
		return true, true
	case <-timer.C:
		return false, true
	}
}

func (s *leaseState) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

func (s *leaseState) refs() []workerprotocol.LeaseRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make([]workerprotocol.LeaseRef, 0, len(s.active))
	for _, item := range s.active {
		refs = append(refs, workerprotocol.LeaseRef{DispatchID: item.lease.DispatchID, LeaseToken: item.lease.LeaseToken})
	}
	return refs
}

func (s *leaseState) cancel(dispatchID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item, ok := s.active[dispatchID]; ok {
		item.cancel()
	}
}

func (s *leaseState) renew(dispatchID string, remaining time.Duration) {
	s.mu.Lock()
	item, ok := s.active[dispatchID]
	s.mu.Unlock()
	if !ok {
		return
	}
	if remaining <= 0 {
		item.cancel()
		return
	}
	// 本地计时器只关心服务端返回的最新剩余时长。替换尚未消费的旧值，
	// 可以避免心跳循环阻塞，同时让租约 goroutine 独占计时器。
	select {
	case item.renew <- remaining:
	default:
		select {
		case <-item.renew:
		default:
		}
		select {
		case item.renew <- remaining:
		default:
		}
	}
}

func (s *leaseState) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.active {
		item.cancel()
	}
}

func (s *leaseState) wait() { s.wg.Wait() }

func nextPollDelay(current, minimum, maximum time.Duration) time.Duration {
	if current <= 0 {
		return minimum
	}
	if current > maximum/2 {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func jitterDelay(base time.Duration, fraction float64, random func() float64) time.Duration {
	if fraction <= 0 || random == nil {
		return base
	}
	if fraction > 1 {
		fraction = 1
	}
	value := random()
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	multiplier := 1 - fraction + 2*fraction*value
	return time.Duration(math.Round(float64(base) * multiplier))
}

func waitFor(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
