// Package workerprotocol 定义控制面与 Worker 之间交换的稳定协议数据。
package workerprotocol

import (
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const ProtocolVersion = 1

// ProtocolVersionHeader 让每个已认证 Worker 请求在请求体之外声明使用的协议版本。
const ProtocolVersionHeader = "X-Worker-Protocol-Version"

// WorkerStatus 描述一个 Worker 会话是否还能领取任务、仅能完成旧租约或已经离线。
type WorkerStatus string

const (
	WorkerActive   WorkerStatus = "active"
	WorkerDraining WorkerStatus = "draining"
	WorkerOffline  WorkerStatus = "offline"
	WorkerStopped  WorkerStatus = "stopped"
)

// WorkerOperation 让认证层能够限制 draining 会话只执行允许的操作。
type WorkerOperation string

const (
	OperationClaim     WorkerOperation = "claim"
	OperationHeartbeat WorkerOperation = "heartbeat"
	OperationComplete  WorkerOperation = "complete"
	OperationDrain     WorkerOperation = "drain"
)

type RegisterRequest struct {
	// DisplayName 只用于人工识别会话，同名 Worker 再注册不会复用原会话。
	DisplayName string `json:"display_name"`
	// ProtocolVersion 声明客户端实现的 Worker 协议版本。
	ProtocolVersion int `json:"protocol_version"`
	// ExecutorKinds 声明当前进程能够解释的任务执行器类型。
	ExecutorKinds []workflow.ExecutorKind `json:"executor_kinds"`
	// MaxConcurrency 是该 Worker 同时持有活动租约的最大数量。
	MaxConcurrency int `json:"max_concurrency"`
}

type RegisterResponse struct {
	// WorkerID 是本次注册会话的稳定标识，不等同于显示名称。
	WorkerID string `json:"worker_id"`
	// SessionToken 是只在注册响应中返回一次的明文会话凭据。
	SessionToken string `json:"session_token"`
	// HeartbeatIntervalMillis 是服务端要求的心跳周期，单位为毫秒。
	HeartbeatIntervalMillis int64 `json:"heartbeat_interval_ms"`
	// LeaseDurationMillis 是服务端默认租约时长，单位为毫秒。
	LeaseDurationMillis int64 `json:"lease_duration_ms"`
	// ProtocolVersion 是控制面接受的 Worker 协议版本。
	ProtocolVersion int `json:"protocol_version"`
}

// Registration 保存新签发会话令牌唯一的明文副本。
// 它是内部服务结果，HTTP 响应通过 RegisterResponse 暴露对应字段。
type Registration struct {
	// Summary 是可公开查询的会话信息；SessionToken 只在本次注册结果中保留明文。
	Summary      WorkerSummary
	SessionToken string
}

// WorkerSummary 是 viewer/operator 查询 API 可安全返回的会话摘要，刻意不包含任何凭据。
type WorkerSummary struct {
	// WorkerID 定位一次注册会话；DisplayName 只供人工识别，不能作为唯一键。
	WorkerID    string `json:"worker_id"`
	DisplayName string `json:"display_name"`
	// ProtocolVersion 和 ExecutorKinds 描述该会话能够理解的协议与任务类型。
	ProtocolVersion int                     `json:"protocol_version"`
	ExecutorKinds   []workflow.ExecutorKind `json:"executor_kinds"`
	// MaxConcurrency 是会话容量上限；ActiveLeases 是查询时仍由该会话持有的租约数。
	MaxConcurrency int          `json:"max_concurrency"`
	Status         WorkerStatus `json:"status"`
	ActiveLeases   int          `json:"active_leases"`
	// 三个时间字段分别记录注册、最近存活上报和最终停止的数据库时间。
	RegisteredAt    time.Time  `json:"registered_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	StoppedAt       *time.Time `json:"stopped_at,omitempty"`
}

// WorkerSession 是 Worker 操作通过认证后使用的最小调度视图。
type WorkerSession struct {
	// 这里只保留认证和调度决策需要的字段，不包含明文会话令牌。
	WorkerID        string
	ProtocolVersion int
	ExecutorKinds   []workflow.ExecutorKind
	MaxConcurrency  int
	Status          WorkerStatus
}

type ClaimRequest struct {
	// Slots 是客户端当前空闲槽位数；服务端仍会按数据库中的最大并发重新限制。
	Slots int `json:"slots"`
}

type LeaseRef struct {
	// DispatchID 定位需要续租的持久化分发记录。
	DispatchID string `json:"dispatch_id"`
	// LeaseToken 证明请求方仍持有该 Dispatch 的当前租约。
	LeaseToken string `json:"lease_token"`
}

// Lease 是 Worker 成功领取后执行一次 Attempt 所需的完整协议数据。
type Lease struct {
	// DispatchID 和 LeaseToken 共同标识并证明当前临时执行所有权。
	DispatchID string `json:"dispatch_id"`
	LeaseToken string `json:"lease_token"`
	// DefinitionID、DefinitionVersion、RunID 和 TaskKey 定位本次任务尝试。
	DefinitionID      string           `json:"definition_id"`
	DefinitionVersion int              `json:"definition_version"`
	RunID             workflow.RunID   `json:"run_id"`
	TaskKey           workflow.TaskKey `json:"task_key"`
	// ExecutorKind 决定执行器类型；Action 和 Input 是该执行器解释的业务输入。
	ExecutorKind workflow.ExecutorKind `json:"executor_kind"`
	Action       string                `json:"action"`
	Input        map[string]any        `json:"input,omitempty"`
	// Attempt 是本次真实执行次数；两个截止时间均由服务端数据库时间决定。
	Attempt         int       `json:"attempt"`
	AttemptDeadline time.Time `json:"attempt_deadline"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
}

type ClaimResponse struct {
	// Leases 只包含本次成功领取的任务；空切片表示当前没有兼容任务，不是错误。
	Leases []Lease `json:"leases"`
}

type HeartbeatRequest struct {
	// Leases 是 Worker 当前仍在执行并希望续期的租约引用。
	Leases []LeaseRef `json:"leases"`
}

type LeaseHeartbeatStatus string

const (
	LeaseRenewed LeaseHeartbeatStatus = "renewed"
	LeaseRevoked LeaseHeartbeatStatus = "revoked"
	LeaseUnknown LeaseHeartbeatStatus = "unknown"
)

type LeaseHeartbeat struct {
	DispatchID string               `json:"dispatch_id"`
	Status     LeaseHeartbeatStatus `json:"status"`
	// LeaseRemainingMillis 只在续租成功时返回，供 Worker 更新本地安全期限。
	LeaseRemainingMillis int64 `json:"lease_remaining_ms,omitempty"`
}

type HeartbeatResponse struct {
	// Leases 按 Dispatch 返回续租、撤销或未知结果，Worker 据此更新本地执行期限。
	Leases []LeaseHeartbeat `json:"leases"`
}

type CompleteRequest struct {
	// LeaseToken 证明结果来自当前租约；Result 只允许工作流封闭结果类型。
	LeaseToken string                     `json:"lease_token"`
	Result     workflow.ExecutionResponse `json:"result"`
}

type CompleteResponse struct {
	// Applied 表示结果已经成为数据库事实；幂等重放同一结果时也返回 true。
	Applied bool `json:"applied"`
}
