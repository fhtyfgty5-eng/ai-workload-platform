package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// WebhookSink 把有限 Event 编码为 HTTP JSON，并执行有超时和次数上限的重试。
type WebhookSink struct {
	client      *http.Client
	endpoint    string
	timeout     time.Duration
	maxAttempts int
}

var alertLabelValues = map[string]map[string]struct{}{
	"component": {"control_plane": {}, "worker": {}, "database": {}},
	"severity":  {"warning": {}, "critical": {}},
}

// NewWebhookSink 拒绝带 URL 凭据的地址，避免配置内容进入请求或错误信息。
func NewWebhookSink(client *http.Client, endpoint string, timeout time.Duration, maxAttempts int) (*WebhookSink, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return nil, fmt.Errorf("webhook endpoint must be an HTTP(S) URL without credentials")
	}
	if timeout <= 0 || maxAttempts <= 0 {
		return nil, fmt.Errorf("webhook timeout and attempts must be positive")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &WebhookSink{client: client, endpoint: parsed.String(), timeout: timeout, maxAttempts: maxAttempts}, nil
}

// Send 只发送白名单字段；失败返回给 Runner 记录，不影响业务状态。
func (s *WebhookSink) Send(ctx context.Context, event Event) error {
	payload := struct {
		AlertName   string            `json:"alert_name"`
		Status      Status            `json:"status"`
		Summary     string            `json:"summary"`
		StartsAt    time.Time         `json:"starts_at,omitempty"`
		EndsAt      time.Time         `json:"ends_at,omitempty"`
		RuleVersion string            `json:"rule_version"`
		Labels      map[string]string `json:"labels,omitempty"`
	}{string(event.Rule), event.Status, event.Summary, event.StartsAt, event.EndsAt, event.RuleVersion, sanitizeAlertLabels(event.Labels)}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				cancel()
				return nil
			}
			err = fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
			if resp.StatusCode < 500 {
				cancel()
				return err
			}
		} else {
			err = redactWebhookRequestError(err)
		}
		cancel()
		last = err
		if attempt+1 < s.maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
			}
		}
	}
	return last
}

// redactWebhookRequestError 剥离 url.Error 携带的完整配置 URL，同时保留底层错误链。
func redactWebhookRequestError(err error) error {
	for {
		var urlErr *url.Error
		if !errors.As(err, &urlErr) {
			return fmt.Errorf("webhook request failed: %w", err)
		}
		err = urlErr.Err
	}
}

func sanitizeAlertLabels(labels map[string]string) map[string]string {
	clean := make(map[string]string)
	for key, value := range labels {
		allowedValues, ok := alertLabelValues[key]
		if !ok {
			continue
		}
		if _, ok := allowedValues[value]; ok {
			clean[key] = value
		}
	}
	if len(clean) == 0 {
		return nil
	}
	return clean
}
