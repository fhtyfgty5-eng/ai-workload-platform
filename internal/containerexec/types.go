// Package containerexec 定义受控容器动作和运行时边界。
package containerexec

import "time"

// MilliCPU 表示千分之一 CPU 核心，例如 500 表示半个 CPU 核心。
type MilliCPU int64

// ResourceLimits 定义动作允许使用的资源上限。
type ResourceLimits struct {
	CPU                   MilliCPU
	MemoryBytes           int64
	EphemeralStorageBytes int64
	PidsLimit             int64
	Timeout               time.Duration
}

// InputSchema 描述注册动作允许的输入字段及其简单类型。
type InputSchema map[string]string

// NetworkPolicy 定义动作的网络访问模式。
type NetworkPolicy string

const (
	// NetworkNone 表示不允许外部网络访问。
	NetworkNone NetworkPolicy = "none"
)

// ActionSpec 是一个经过批准的固定动作执行描述。
type ActionSpec struct {
	Name             string
	Image            string
	ImageDigest      string
	Entrypoint       []string
	InputSchema      InputSchema
	Limits           ResourceLimits
	Network          NetworkPolicy
	OutputLimitBytes int64
}
