package agent

import (
	"context"
	"strings"
	"sync"
	"time"
)

const redactedValue = "[REDACTED]"

// AuditEvent 记录模型、工具、校验和审批边界的必要非敏感元数据。
type AuditEvent struct {
	Type     string         `json:"type"`
	At       time.Time      `json:"at"`
	DraftID  string         `json:"draft_id,omitempty"`
	Result   string         `json:"result,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// AuditSink 接收 Agent Runtime 产生的结构化审计事件。
type AuditSink interface {
	Record(context.Context, AuditEvent) error
}

// MemoryAuditSink 是测试和本地演示使用的线程安全审计接收器。
type MemoryAuditSink struct {
	mu     sync.Mutex
	events []AuditEvent
}

// NewMemoryAuditSink 创建只在当前进程内保存事件的审计接收器。
func NewMemoryAuditSink() *MemoryAuditSink { return &MemoryAuditSink{} }

func (s *MemoryAuditSink) Record(ctx context.Context, event AuditEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	event.Metadata = redactMetadata(event.Metadata)
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	return nil
}

func (s *MemoryAuditSink) Events() []AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]AuditEvent, len(s.events))
	copy(events, s.events)
	for i := range events {
		events[i].Metadata = redactMetadata(events[i].Metadata)
	}
	return events
}

func redactMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	redacted := make(map[string]any, len(metadata))
	for key, value := range metadata {
		normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
		if strings.Contains(normalized, "authorization") || strings.Contains(normalized, "api_key") || strings.Contains(normalized, "password") || strings.Contains(normalized, "token") {
			redacted[key] = redactedValue
			continue
		}
		redacted[key] = redactAuditValue(value)
	}
	return redacted
}

func redactAuditValue(value any) any {
	switch nested := value.(type) {
	case map[string]any:
		return redactMetadata(nested)
	case map[string]string:
		redacted := make(map[string]string, len(nested))
		for key, item := range nested {
			normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
			if strings.Contains(normalized, "authorization") || strings.Contains(normalized, "api_key") || strings.Contains(normalized, "password") || strings.Contains(normalized, "token") {
				redacted[key] = redactedValue
				continue
			}
			redacted[key] = item
		}
		return redacted
	case []any:
		redacted := make([]any, len(nested))
		for i, item := range nested {
			redacted[i] = redactAuditValue(item)
		}
		return redacted
	default:
		return value
	}
}
