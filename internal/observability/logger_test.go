package observability

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"
)

func TestLoggerFromContextAddsFieldsAndSanitizesSecrets(t *testing.T) {
	var output strings.Builder
	base := slog.New(slog.NewTextHandler(&output, nil))
	ctx := WithFields(context.Background(), LogFields{
		RequestID:  "req-1",
		RunID:      "run-1",
		Operation:  "complete",
		Duration:   12,
		ErrorCode:  "lease_lost",
		WorkerID:   "worker-1",
		DispatchID: "dispatch-1",
	})
	LoggerFromContext(ctx, base).Error("operation failed", "error", errors.New("Authorization: Bearer secret-token"))
	line := output.String()
	for _, want := range []string{"request_id=req-1", "run_id=run-1", "operation=complete", "duration_ms=12", "error_code=lease_lost", "worker_id=worker-1", "dispatch_id=dispatch-1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log = %q, missing %q", line, want)
		}
	}
	if strings.Contains(line, "secret-token") {
		t.Fatalf("log leaked secret: %q", line)
	}
}

func TestLoggerFromContextAddsTraceIdentityWhenAvailable(t *testing.T) {
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)

	var output strings.Builder
	LoggerFromContext(ctx, slog.New(slog.NewTextHandler(&output, nil))).Info("traced operation")
	got := output.String()
	if !strings.Contains(got, "trace_id="+traceID.String()) || !strings.Contains(got, "span_id="+spanID.String()) {
		t.Fatalf("trace log = %q, want trace_id and span_id", got)
	}
}

func TestSanitizeErrorLimitsAndRedactsSensitiveText(t *testing.T) {
	got := SanitizeError(errors.New("Authorization: Bearer abc123 password=hunter2 api_key=key-xyz "+strings.Repeat("x", 200)), 48)
	if strings.Contains(got, "abc123") || strings.Contains(got, "hunter2") || strings.Contains(got, "key-xyz") {
		t.Fatalf("sanitized error leaked secret: %q", got)
	}
	if len(got) > 48 {
		t.Fatalf("sanitized error length = %d, want <= 48", len(got))
	}
}

func TestSanitizeErrorDoesNotSplitUTF8Encoding(t *testing.T) {
	got := SanitizeError(errors.New(strings.Repeat("中", 10)), 5)
	if !utf8.ValidString(got) {
		t.Fatalf("sanitized error contains invalid UTF-8: %q", got)
	}
	if len(got) > 5 {
		t.Fatalf("sanitized error length = %d, want <= 5", len(got))
	}
}

func TestSanitizingHandlerLimitsErrorAttributeLength(t *testing.T) {
	var output strings.Builder
	logger := slog.New(NewSanitizingHandler(slog.NewTextHandler(&output, nil)))
	logger.Error("operation failed", "error", errors.New(strings.Repeat("x", 2_000)))
	if got := output.String(); len(got) > 800 {
		t.Fatalf("sanitized production log is too long: %d bytes", len(got))
	}
}
