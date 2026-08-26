// Package workerconfig 加载独立 Worker 进程所需的环境变量。
package workerconfig

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config 保存一个独立 Worker 进程启动和运行所需的全部本机配置。
type Config struct {
	// ServerURL 和 BootstrapToken 用于连接控制面并注册新 Worker 会话。
	ServerURL      string
	BootstrapToken string
	// DisplayName 供人工识别；MaxConcurrency 限制本进程同时执行的任务数。
	DisplayName    string
	MaxConcurrency int
	// PollMin 和 PollMax 定义空领取后的指数退避范围。
	PollMin time.Duration
	PollMax time.Duration
	// HeartbeatInterval 控制空闲和忙碌会话的存活上报周期。
	HeartbeatInterval time.Duration
	// ShutdownTimeout 限制优雅退出等待活动租约完成的最长时间。
	ShutdownTimeout time.Duration
	// MockExecutionDelay 只用于演示和故障测试，不代表真实任务耗时。
	MockExecutionDelay time.Duration
	// Runtime 选择 mock、docker 或 kubernetes 执行器，默认使用 mock 保持兼容。
	Runtime string
	// ActionImage 是受控执行器使用的固定本地任务镜像；请求不能覆盖它。
	ActionImage string
	// TracingMode 控制独立 Worker 的 Span 导出；TracingServiceName 标识导出来源。
	TracingMode        string
	TracingServiceName string
	// LogLevel 和 LogFormat 与控制面使用同一组环境变量，保证跨进程日志契约一致。
	LogLevel  string
	LogFormat string
}

// Load 从指定环境变量读取器加载配置，补齐默认值后执行完整校验。
func Load(getenv func(string) string) (Config, error) {
	config := Config{
		ServerURL:          strings.TrimSpace(getenv("WORKLOAD_SERVER_URL")),
		BootstrapToken:     strings.TrimSpace(getenv("WORKLOAD_WORKER_BOOTSTRAP_TOKEN")),
		DisplayName:        strings.TrimSpace(getenv("WORKLOAD_WORKER_NAME")),
		TracingMode:        strings.TrimSpace(getenv("WORKLOAD_WORKER_TRACING_MODE")),
		TracingServiceName: strings.TrimSpace(getenv("WORKLOAD_WORKER_TRACING_SERVICE_NAME")),
		LogLevel:           strings.TrimSpace(getenv("WORKLOAD_LOG_LEVEL")),
		LogFormat:          strings.TrimSpace(getenv("WORKLOAD_LOG_FORMAT")),
		Runtime:            strings.TrimSpace(getenv("WORKLOAD_WORKER_RUNTIME")),
		ActionImage:        strings.TrimSpace(getenv("WORKLOAD_WORKER_ACTION_IMAGE")),
	}
	var err error
	config.MaxConcurrency, err = positiveInt(getenv("WORKLOAD_WORKER_CONCURRENCY"), 1)
	if err != nil {
		return Config{}, fmt.Errorf("WORKLOAD_WORKER_CONCURRENCY: %w", err)
	}
	config.PollMin, err = duration(getenv("WORKLOAD_WORKER_POLL_MIN"), 250*time.Millisecond)
	if err != nil {
		return Config{}, fmt.Errorf("WORKLOAD_WORKER_POLL_MIN: %w", err)
	}
	config.PollMax, err = duration(getenv("WORKLOAD_WORKER_POLL_MAX"), 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("WORKLOAD_WORKER_POLL_MAX: %w", err)
	}
	config.HeartbeatInterval, err = duration(getenv("WORKLOAD_WORKER_HEARTBEAT"), 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("WORKLOAD_WORKER_HEARTBEAT: %w", err)
	}
	config.ShutdownTimeout, err = duration(getenv("WORKLOAD_WORKER_SHUTDOWN_TIMEOUT"), 30*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("WORKLOAD_WORKER_SHUTDOWN_TIMEOUT: %w", err)
	}
	if value := strings.TrimSpace(getenv("WORKLOAD_MOCK_EXECUTION_DELAY")); value != "" {
		config.MockExecutionDelay, err = time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("WORKLOAD_MOCK_EXECUTION_DELAY: %w", err)
		}
	}
	if config.TracingMode == "" {
		config.TracingMode = "off"
	}
	if config.Runtime == "" {
		config.Runtime = "mock"
	}
	if config.ActionImage == "" {
		config.ActionImage = "workload-action:local"
	}
	if config.TracingServiceName == "" {
		config.TracingServiceName = "workload-worker"
	}
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	if config.LogFormat == "" {
		config.LogFormat = "text"
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate 拒绝不安全的远程 HTTP、无效容量和可能破坏租约时序的时间参数。
func (c Config) Validate() error {
	parsed, err := url.Parse(strings.TrimSpace(c.ServerURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("WORKLOAD_SERVER_URL must be an absolute HTTP(S) URL without query or fragment")
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return fmt.Errorf("non-loopback Worker server must use HTTPS")
	}
	if c.BootstrapToken == "" || c.DisplayName == "" {
		return fmt.Errorf("Worker bootstrap token and display name are required")
	}
	if c.MaxConcurrency <= 0 || c.MaxConcurrency > 1024 {
		return fmt.Errorf("Worker concurrency must be between 1 and 1024")
	}
	if c.PollMin <= 0 || c.PollMax < c.PollMin || c.PollMax > 5*time.Minute {
		return fmt.Errorf("Worker poll interval range is invalid")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("Worker heartbeat interval is invalid")
	}
	if c.ShutdownTimeout <= 0 || c.ShutdownTimeout > 10*time.Minute {
		return fmt.Errorf("Worker shutdown timeout is invalid")
	}
	if c.MockExecutionDelay < 0 || c.MockExecutionDelay > 5*time.Minute {
		return fmt.Errorf("WORKLOAD_MOCK_EXECUTION_DELAY must be between 0s and 5m")
	}
	if c.Runtime != "mock" && c.Runtime != "docker" && c.Runtime != "kubernetes" {
		return fmt.Errorf("unsupported Worker runtime %q", c.Runtime)
	}
	if c.Runtime != "mock" && c.ActionImage != "workload-action:local" && !strings.Contains(c.ActionImage, "@sha256:") {
		return fmt.Errorf("WORKLOAD_WORKER_ACTION_IMAGE must use a digest reference")
	}
	if c.TracingMode != "off" && c.TracingMode != "stdout" && c.TracingMode != "memory" {
		return fmt.Errorf("unsupported Worker tracing mode %q", c.TracingMode)
	}
	if strings.TrimSpace(c.TracingServiceName) == "" {
		return fmt.Errorf("Worker tracing service name is required")
	}
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("unsupported Worker log level %q", c.LogLevel)
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		return fmt.Errorf("unsupported Worker log format %q", c.LogFormat)
	}
	return nil
}

func duration(value string, fallback time.Duration) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func positiveInt(value string, fallback int) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("must be a positive integer")
	}
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
