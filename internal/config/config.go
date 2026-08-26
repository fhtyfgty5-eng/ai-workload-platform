package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxMockExecutionDelay = 5 * time.Minute

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultLeaseDuration     = 15 * time.Second
	defaultReaperInterval    = time.Second
	defaultDispatchLimit     = 100
)

type Config struct {
	DatabaseURL          string
	HTTPAddr             string
	ViewerToken          string
	OperatorToken        string
	WorkerBootstrapToken string
	HeartbeatInterval    time.Duration
	LeaseDuration        time.Duration
	ReaperInterval       time.Duration
	DispatchLimit        int
	LogLevel             string
	LogFormat            string
	MockExecutionDelay   time.Duration
	TracingMode          string
	TracingServiceName   string
	AlertWebhookURL      string
	AlertWebhookTimeout  time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	config := Config{
		DatabaseURL:          strings.TrimSpace(getenv("DATABASE_URL")),
		HTTPAddr:             strings.TrimSpace(getenv("WORKLOAD_HTTP_ADDR")),
		ViewerToken:          strings.TrimSpace(getenv("WORKLOAD_VIEWER_TOKEN")),
		OperatorToken:        strings.TrimSpace(getenv("WORKLOAD_OPERATOR_TOKEN")),
		WorkerBootstrapToken: strings.TrimSpace(getenv("WORKLOAD_WORKER_BOOTSTRAP_TOKEN")),
		HeartbeatInterval:    defaultHeartbeatInterval,
		LeaseDuration:        defaultLeaseDuration,
		ReaperInterval:       defaultReaperInterval,
		DispatchLimit:        defaultDispatchLimit,
		LogLevel:             strings.TrimSpace(getenv("WORKLOAD_LOG_LEVEL")),
		LogFormat:            strings.TrimSpace(getenv("WORKLOAD_LOG_FORMAT")),
		TracingMode:          strings.TrimSpace(getenv("WORKLOAD_TRACING_MODE")),
		TracingServiceName:   strings.TrimSpace(getenv("WORKLOAD_TRACING_SERVICE_NAME")),
		AlertWebhookURL:      strings.TrimSpace(getenv("WORKLOAD_ALERT_WEBHOOK_URL")),
		AlertWebhookTimeout:  time.Second,
	}
	var parseErr error
	if value := strings.TrimSpace(getenv("WORKLOAD_HEARTBEAT_INTERVAL")); value != "" {
		config.HeartbeatInterval, parseErr = time.ParseDuration(value)
		if parseErr != nil {
			return Config{}, fmt.Errorf("WORKLOAD_HEARTBEAT_INTERVAL must be a Go duration: %w", parseErr)
		}
	}
	if value := strings.TrimSpace(getenv("WORKLOAD_LEASE_TTL")); value != "" {
		config.LeaseDuration, parseErr = time.ParseDuration(value)
		if parseErr != nil {
			return Config{}, fmt.Errorf("WORKLOAD_LEASE_TTL must be a Go duration: %w", parseErr)
		}
	}
	if value := strings.TrimSpace(getenv("WORKLOAD_LEASE_REAPER_INTERVAL")); value != "" {
		config.ReaperInterval, parseErr = time.ParseDuration(value)
		if parseErr != nil {
			return Config{}, fmt.Errorf("WORKLOAD_LEASE_REAPER_INTERVAL must be a Go duration: %w", parseErr)
		}
	}
	if value := strings.TrimSpace(getenv("WORKLOAD_DISPATCH_LIMIT")); value != "" {
		config.DispatchLimit, parseErr = strconv.Atoi(value)
		if parseErr != nil {
			return Config{}, fmt.Errorf("WORKLOAD_DISPATCH_LIMIT must be an integer: %w", parseErr)
		}
	}
	delayValue := strings.TrimSpace(getenv("WORKLOAD_MOCK_EXECUTION_DELAY"))
	if delayValue != "" {
		delay, err := time.ParseDuration(delayValue)
		if err != nil {
			return Config{}, fmt.Errorf("WORKLOAD_MOCK_EXECUTION_DELAY must be a Go duration: %w", err)
		}
		config.MockExecutionDelay = delay
	}
	if config.HTTPAddr == "" {
		config.HTTPAddr = ":8080"
	}
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	if config.LogFormat == "" {
		config.LogFormat = "text"
	}
	if config.TracingMode == "" {
		config.TracingMode = "off"
	}
	if config.TracingServiceName == "" {
		config.TracingServiceName = "workload-server"
	}
	if value := strings.TrimSpace(getenv("WORKLOAD_ALERT_WEBHOOK_TIMEOUT")); value != "" {
		config.AlertWebhookTimeout, parseErr = time.ParseDuration(value)
		if parseErr != nil {
			return Config{}, fmt.Errorf("WORKLOAD_ALERT_WEBHOOK_TIMEOUT must be a Go duration: %w", parseErr)
		}
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	parsed, err := url.Parse(c.DatabaseURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return fmt.Errorf("DATABASE_URL must be a PostgreSQL URL")
	}
	if strings.TrimSpace(c.ViewerToken) == "" || strings.TrimSpace(c.OperatorToken) == "" {
		return fmt.Errorf("viewer and operator tokens are required")
	}
	if c.ViewerToken == c.OperatorToken {
		return fmt.Errorf("viewer and operator tokens must differ")
	}
	if strings.TrimSpace(c.WorkerBootstrapToken) == "" {
		return fmt.Errorf("WORKLOAD_WORKER_BOOTSTRAP_TOKEN is required")
	}
	if c.WorkerBootstrapToken == c.ViewerToken || c.WorkerBootstrapToken == c.OperatorToken {
		return fmt.Errorf("Worker bootstrap token must differ from viewer and operator tokens")
	}
	if c.HeartbeatInterval <= 0 || c.LeaseDuration <= c.HeartbeatInterval || c.ReaperInterval <= 0 || c.DispatchLimit <= 0 || c.DispatchLimit > 10000 {
		return fmt.Errorf("Worker heartbeat, lease, reaper and dispatch limits are invalid")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return fmt.Errorf("WORKLOAD_HTTP_ADDR must be host:port: %w", err)
	}
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("unsupported log level %q", c.LogLevel)
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		return fmt.Errorf("unsupported log format %q", c.LogFormat)
	}
	if c.TracingMode != "off" && c.TracingMode != "stdout" && c.TracingMode != "memory" {
		return fmt.Errorf("unsupported tracing mode %q", c.TracingMode)
	}
	if strings.TrimSpace(c.TracingServiceName) == "" {
		return fmt.Errorf("tracing service name is required")
	}
	if c.AlertWebhookTimeout <= 0 {
		return fmt.Errorf("alert webhook timeout must be positive")
	}
	if c.AlertWebhookURL != "" {
		parsed, err := url.Parse(c.AlertWebhookURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("alert webhook URL must be an HTTP(S) URL without credentials")
		}
	}
	if c.MockExecutionDelay < 0 || c.MockExecutionDelay > maxMockExecutionDelay {
		return fmt.Errorf("WORKLOAD_MOCK_EXECUTION_DELAY must be between 0s and %s", maxMockExecutionDelay)
	}
	return nil
}
