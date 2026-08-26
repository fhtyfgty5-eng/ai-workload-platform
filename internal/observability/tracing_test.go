package observability

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStdoutExporterDoesNotBlockSpanEnd(t *testing.T) {
	writer := &blockingWriter{release: make(chan struct{})}
	provider, closeProvider, err := NewTracerProvider(TracingConfig{Mode: "stdout", Writer: writer})
	if err != nil {
		t.Fatal(err)
	}
	_, span := provider.Tracer("test").Start(context.Background(), "non-blocking")
	ended := make(chan struct{})
	go func() {
		span.End()
		close(ended)
	}()
	select {
	case <-ended:
	case <-time.After(100 * time.Millisecond):
		close(writer.release)
		_ = closeProvider(context.Background())
		t.Fatal("stdout exporter blocked Span.End")
	}
	close(writer.release)
	if err := closeProvider(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blockingWriter struct {
	release chan struct{}
}

func (w *blockingWriter) Write(data []byte) (int, error) {
	<-w.release
	return len(data), nil
}

var _ io.Writer = (*blockingWriter)(nil)

func TestFinishSpanRecordsStableOutcomeWithoutErrorText(t *testing.T) {
	provider, closeProvider, err := NewTracerProvider(TracingConfig{Mode: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeProvider(context.Background())
	recorder := tracetest.NewSpanRecorder()
	provider.RegisterSpanProcessor(recorder)
	_, span := provider.Tracer("test").Start(context.Background(), "worker.complete")
	FinishSpan(span, "error", "lease_lost")
	span.End()
	ended := recorder.Ended()
	if len(ended) != 1 || ended[0].Status().Code != codes.Error {
		t.Fatalf("span status = %+v", ended)
	}
	attributes := map[string]string{}
	for _, attr := range ended[0].Attributes() {
		attributes[string(attr.Key)] = attr.Value.AsString()
	}
	if attributes["outcome"] != "error" || attributes["error_code"] != "lease_lost" {
		t.Fatalf("span attributes = %+v", attributes)
	}
}

func TestTracingServiceNameIsRecordedAsResourceAttribute(t *testing.T) {
	provider, closeProvider, err := NewTracerProvider(TracingConfig{Mode: "memory", ServiceName: "workload-worker-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeProvider(context.Background())
	recorder := tracetest.NewSpanRecorder()
	provider.RegisterSpanProcessor(recorder)
	_, span := provider.Tracer("test").Start(context.Background(), "worker.claim")
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	for _, attr := range ended[0].Resource().Attributes() {
		if string(attr.Key) == "service.name" && attr.Value.AsString() == "workload-worker-test" {
			return
		}
	}
	t.Fatalf("resource attributes = %+v, want service.name", ended[0].Resource().Attributes())
}

func TestTracingMemoryModeAndHTTPPropagation(t *testing.T) {
	provider, closeProvider, err := NewTracerProvider(TracingConfig{Mode: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeProvider(context.Background())
	recorder := tracetest.NewSpanRecorder()
	provider.RegisterSpanProcessor(recorder)
	tracer := otel.Tracer("test")
	ctx, span := StartSpan(context.Background(), tracer, "http.request", attribute.String("operation", "run.start"))
	request, _ := http.NewRequest(http.MethodPost, "http://example.test", nil)
	InjectHTTP(ctx, request)
	extracted := ExtractHTTP(context.Background(), request)
	_, childSpan := StartSpan(extracted, tracer, "worker.claim")
	childSpan.End()
	span.End()
	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans = %+v", spans)
	}
	parent, childReadOnly := spans[0], spans[1]
	if childReadOnly.Name() != "worker.claim" {
		parent, childReadOnly = childReadOnly, parent
	}
	if childReadOnly.SpanContext().TraceID() != parent.SpanContext().TraceID() || childReadOnly.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("parent = %+v child = %+v", parent, childReadOnly)
	}
}

func TestTracingRejectsUnknownModeAndCloseIsIdempotent(t *testing.T) {
	if _, _, err := NewTracerProvider(TracingConfig{Mode: "invalid"}); err == nil {
		t.Fatal("accepted invalid mode")
	}
	provider, closeProvider, err := NewTracerProvider(TracingConfig{Mode: "off"})
	if err != nil {
		t.Fatal(err)
	}
	_, span := provider.Tracer("test").Start(context.Background(), "off-span")
	if span.IsRecording() {
		t.Fatal("off tracing mode created a recording span")
	}
	span.End()
	if err := closeProvider(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := closeProvider(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = provider
}
