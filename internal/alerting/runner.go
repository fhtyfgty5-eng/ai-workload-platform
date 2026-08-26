package alerting

import (
	"context"
	"log/slog"
	"time"
)

// SnapshotProvider 返回一次规则评估所需的低基数聚合快照。
type SnapshotProvider func(context.Context) (Snapshot, error)

// Runner 周期读取聚合快照并评估告警。通知使用有界异步发送，
// Webhook 变慢或失败不能阻塞数据库状态推进和下一轮采集。
type Runner struct {
	engine   *Engine
	provider SnapshotProvider
	sink     AlertSink
	interval time.Duration
	logger   *slog.Logger
	observer Observer
	queue    chan Event
}

// NewRunner 组合规则、快照、通知出口和可选指标观察器，但不启动 goroutine。
func NewRunner(engine *Engine, provider SnapshotProvider, sink AlertSink, interval time.Duration, logger *slog.Logger, observers ...Observer) *Runner {
	if engine == nil {
		engine = NewEngine(DefaultRules())
	}
	if interval <= 0 {
		interval = time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &Runner{engine: engine, provider: provider, sink: sink, interval: interval, logger: logger, observer: observer, queue: make(chan Event, 16)}
}

// Run 周期评估快照，并用独立发送循环隔离 Webhook 延迟；Context 取消后等待发送循环退出。
func (r *Runner) Run(ctx context.Context) {
	if r.provider == nil || r.sink == nil {
		return
	}
	done := make(chan struct{})
	go func() { defer close(done); r.sendLoop(ctx) }()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.evaluate(ctx)
	for {
		select {
		case <-ctx.Done():
			<-done
			return
		case <-ticker.C:
			r.evaluate(ctx)
		}
	}
}

func (r *Runner) evaluate(ctx context.Context) {
	snapshot, err := r.provider(ctx)
	if err != nil {
		r.logger.Warn("collect alert snapshot failed", "operation", "alert_snapshot", "error", err)
		return
	}
	for _, event := range r.engine.Evaluate(snapshot) {
		if r.observer != nil {
			r.observer.ObserveAlert(string(event.Rule), string(event.Status), "success")
		}
		select {
		case r.queue <- event:
		default:
			if r.observer != nil {
				r.observer.ObserveAlert(string(event.Rule), "notify", "error")
			}
			r.logger.Warn("alert notification queue is full", "alert_name", event.Rule)
		}
	}
}

func (r *Runner) sendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-r.queue:
			if err := r.sink.Send(ctx, event); err != nil {
				if r.observer != nil {
					r.observer.ObserveAlert(string(event.Rule), "notify", "error")
				}
				r.logger.Warn("send alert webhook failed", "alert_name", event.Rule, "status", event.Status, "error", err)
			} else if r.observer != nil {
				r.observer.ObserveAlert(string(event.Rule), "notify", "success")
			}
		}
	}
}
