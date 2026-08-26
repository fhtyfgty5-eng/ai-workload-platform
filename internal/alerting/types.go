// Package alerting 评估低基数系统快照并向可替换通知出口发送告警。
package alerting

import (
	"context"
	"time"
)

type RuleName string

const (
	QueueBacklog         RuleName = "queue_backlog"
	WorkersOffline       RuleName = "workers_offline"
	LeaseReclaimErrors   RuleName = "lease_reclaim_errors"
	CompleteErrorRate    RuleName = "complete_error_rate"
	DBPoolNearExhaustion RuleName = "db_pool_near_exhaustion"
)

type Status string

const (
	StatusFiring   Status = "firing"
	StatusResolved Status = "resolved"
	StatusUnknown  Status = "unknown"
)

// Snapshot 是告警规则使用的聚合事实，不包含单个业务 ID。
type Snapshot struct {
	// Now 使用数据库时间确定持续窗口，避免控制面本地时钟与持久化事实不一致。
	Now time.Time
	// QueueDepth、AvailableSlots 和 OnlineWorkers 描述当前是否还有任务等待以及可用执行容量。
	QueueDepth     int
	AvailableSlots int
	OnlineWorkers  int
	// ActiveLeases 只用于刷新系统 Gauge，不参与当前五条告警规则。
	ActiveLeases int
	// 三个操作计数来自固定时间窗口，不是进程生命周期累计值。
	LeaseReclaimErrors int
	CompleteTotal      int
	CompleteErrors     int
	// DBInUse/DBMax 是同一时刻连接池使用量和容量上限。
	DBInUse int
	DBMax   int
}

// Event 是可发送到通知出口的有限告警负载，不包含业务 ID、凭据或请求正文。
type Event struct {
	Rule        RuleName          `json:"alert_name"`
	Status      Status            `json:"status"`
	Summary     string            `json:"summary"`
	StartsAt    time.Time         `json:"starts_at,omitempty"`
	EndsAt      time.Time         `json:"ends_at,omitempty"`
	RuleVersion string            `json:"rule_version"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// AlertSink 隔离规则状态机与具体通知方式；模块 5 的实现是本地 Webhook。
type AlertSink interface {
	Send(ctx context.Context, event Event) error
}

// Observer 记录低基数告警生命周期指标，不参与规则状态转换或通知重试。
type Observer interface {
	ObserveAlert(rule, operation, outcome string)
}
