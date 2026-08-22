package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const maxMockExecutionDelay = 5 * time.Minute

type Config struct {
	DatabaseURL        string
	HTTPAddr           string
	ViewerToken        string
	OperatorToken      string
	LogLevel           string
	LogFormat          string
	MockExecutionDelay time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	config := Config{
		DatabaseURL:   strings.TrimSpace(getenv("DATABASE_URL")),
		HTTPAddr:      strings.TrimSpace(getenv("WORKLOAD_HTTP_ADDR")),
		ViewerToken:   strings.TrimSpace(getenv("WORKLOAD_VIEWER_TOKEN")),
		OperatorToken: strings.TrimSpace(getenv("WORKLOAD_OPERATOR_TOKEN")),
		LogLevel:      strings.TrimSpace(getenv("WORKLOAD_LOG_LEVEL")),
		LogFormat:     strings.TrimSpace(getenv("WORKLOAD_LOG_FORMAT")),
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
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return fmt.Errorf("WORKLOAD_HTTP_ADDR must be host:port: %w", err)
	}
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("unsupported log level %q", c.LogLevel)
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		return fmt.Errorf("unsupported log format %q", c.LogFormat)
	}
	if c.MockExecutionDelay < 0 || c.MockExecutionDelay > maxMockExecutionDelay {
		return fmt.Errorf("WORKLOAD_MOCK_EXECUTION_DELAY must be between 0s and %s", maxMockExecutionDelay)
	}
	return nil
}
