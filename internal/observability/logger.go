// Package observability 提供不改变业务状态的日志、指标和调用链能力。
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"unicode/utf8"

	oteltrace "go.opentelemetry.io/otel/trace"
)

type logFieldsKey struct{}

// NewLogger 创建支持 text/json、级别过滤和统一脱敏的生产 logger。
func NewLogger(output io.Writer, format, levelName string) (*slog.Logger, error) {
	var level slog.Level
	switch levelName {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", levelName)
	}
	options := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(output, options)
	case "text":
		handler = slog.NewTextHandler(output, options)
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
	return slog.New(NewSanitizingHandler(handler)), nil
}

// LogFields 是一次请求或执行操作可以关联的稳定诊断字段。
type LogFields struct {
	RequestID  string
	RunID      string
	TaskKey    string
	Attempt    int
	WorkerID   string
	DispatchID string
	Operation  string
	Duration   int64
	ErrorCode  string
	TraceID    string
	SpanID     string
}

// WithFields 把日志字段放入 Context；字段不会改变业务 Context 的取消语义。
func WithFields(ctx context.Context, fields LogFields) context.Context {
	return context.WithValue(ctx, logFieldsKey{}, fields)
}

// LoggerFromContext 返回带有当前诊断字段和敏感信息过滤的 Logger。
func LoggerFromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	fields, _ := ctx.Value(logFieldsKey{}).(LogFields)
	spanContext := oteltrace.SpanContextFromContext(ctx)
	if fields.TraceID == "" && spanContext.TraceID().IsValid() {
		fields.TraceID = spanContext.TraceID().String()
	}
	if fields.SpanID == "" && spanContext.SpanID().IsValid() {
		fields.SpanID = spanContext.SpanID().String()
	}
	attrs := make([]any, 0, 11)
	appendString := func(key, value string) {
		if value != "" {
			attrs = append(attrs, key, value)
		}
	}
	appendString("request_id", fields.RequestID)
	appendString("run_id", fields.RunID)
	appendString("task_key", fields.TaskKey)
	if fields.Attempt > 0 {
		attrs = append(attrs, "attempt", fields.Attempt)
	}
	appendString("worker_id", fields.WorkerID)
	appendString("dispatch_id", fields.DispatchID)
	appendString("operation", fields.Operation)
	if fields.Duration > 0 {
		attrs = append(attrs, "duration_ms", fields.Duration)
	}
	appendString("error_code", fields.ErrorCode)
	appendString("trace_id", fields.TraceID)
	appendString("span_id", fields.SpanID)
	return slog.New(NewSanitizingHandler(base.Handler())).With(attrs...)
}

var (
	bearerPattern   = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+|bearer\s+)[^\s,;]+`)
	passwordPattern = regexp.MustCompile(`(?i)(password|passwd|api[_-]?key|token)\s*[=:]\s*[^\s,;]+`)
)

// SanitizeError 限制错误文本长度并移除常见凭据格式，避免诊断日志泄露密钥。
func SanitizeError(err error, maxBytes int) string {
	if err == nil {
		return ""
	}
	if maxBytes <= 0 {
		maxBytes = 512
	}
	value := bearerPattern.ReplaceAllString(err.Error(), `${1}<redacted>`)
	value = passwordPattern.ReplaceAllString(value, `${1}=<redacted>`)
	return truncateUTF8(value, maxBytes)
}

type sanitizingHandler struct {
	next slog.Handler
}

// NewSanitizingHandler 包装生产 Handler，统一过滤字符串和 error 属性中的常见凭据。
func NewSanitizingHandler(next slog.Handler) slog.Handler {
	if next == nil {
		next = slog.Default().Handler()
	}
	return &sanitizingHandler{next: next}
}

func (h *sanitizingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *sanitizingHandler) Handle(ctx context.Context, record slog.Record) error {
	clean := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		clean.AddAttrs(sanitizeAttr(attr))
		return true
	})
	return h.next.Handle(ctx, clean)
}

func (h *sanitizingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		clean = append(clean, sanitizeAttr(attr))
	}
	return &sanitizingHandler{next: h.next.WithAttrs(clean)}
}

func (h *sanitizingHandler) WithGroup(name string) slog.Handler {
	return &sanitizingHandler{next: h.next.WithGroup(name)}
}

func sanitizeAttr(attr slog.Attr) slog.Attr {
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindString {
		return slog.String(attr.Key, sanitizeAttributeText(attr.Key, value.String()))
	}
	if value.Kind() == slog.KindAny {
		return slog.String(attr.Key, sanitizeAttributeText(attr.Key, fmt.Sprint(value.Any())))
	}
	return slog.Attr{Key: attr.Key, Value: value}
}

func sanitizeAttributeText(key, value string) string {
	value = sanitizeText(value)
	if key != "error" && !strings.HasSuffix(key, "_error") {
		return value
	}
	const maxErrorBytes = 512
	return truncateUTF8(value, maxErrorBytes)
}

func truncateUTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func sanitizeText(value string) string {
	value = bearerPattern.ReplaceAllString(value, `${1}<redacted>`)
	value = passwordPattern.ReplaceAllString(value, `${1}=<redacted>`)
	return strings.TrimSpace(value)
}
