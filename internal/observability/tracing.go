package observability

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TracingConfig 选择本地导出模式和进程级服务名；Writer 只供 stdout 模式使用。
type TracingConfig struct {
	Mode        string
	ServiceName string
	Writer      io.Writer
}

// NewTracerProvider 创建模块 5 使用的本地 TracerProvider。memory 模式用于自动化断言，off 模式不导出。
func NewTracerProvider(cfg TracingConfig) (*trace.TracerProvider, func(context.Context) error, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "workload-server"
	}
	if cfg.Mode == "" {
		cfg.Mode = "off"
	}
	if cfg.Mode != "off" && cfg.Mode != "memory" && cfg.Mode != "stdout" {
		return nil, nil, fmt.Errorf("unsupported tracing mode %q", cfg.Mode)
	}
	providerOptions := []trace.TracerProviderOption{
		trace.WithResource(resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName))),
	}
	if cfg.Mode == "off" {
		providerOptions = append(providerOptions, trace.WithSampler(trace.NeverSample()))
	}
	provider := trace.NewTracerProvider(providerOptions...)
	if cfg.Mode == "stdout" {
		writer := cfg.Writer
		if writer == nil {
			writer = io.Discard
		}
		provider.RegisterSpanProcessor(trace.NewBatchSpanProcessor(
			stdoutExporter{writer: writer},
			trace.WithMaxQueueSize(256),
			trace.WithMaxExportBatchSize(64),
			trace.WithBatchTimeout(100*time.Millisecond),
			trace.WithExportTimeout(time.Second),
		))
	}
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var once sync.Once
	closeProvider := func(ctx context.Context) error {
		var err error
		once.Do(func() { err = provider.Shutdown(ctx); otel.SetTracerProvider(previous) })
		return err
	}
	return provider, closeProvider, nil
}

type stdoutExporter struct{ writer io.Writer }

func (e stdoutExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	for _, span := range spans {
		if _, err := fmt.Fprintf(e.writer, "trace_id=%s span_id=%s name=%s status=%s\n", span.SpanContext().TraceID(), span.SpanContext().SpanID(), span.Name(), span.Status().Code.String()); err != nil {
			return err
		}
	}
	return nil
}
func (e stdoutExporter) Shutdown(context.Context) error { return nil }

func StartSpan(ctx context.Context, tracer oteltrace.Tracer, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	if tracer == nil {
		tracer = otel.Tracer("workload")
	}
	return tracer.Start(ctx, name, oteltrace.WithAttributes(attrs...))
}

// FinishSpan 只写入有限结果字段，不记录可能包含凭据或业务输入的原始错误文本。
func FinishSpan(span oteltrace.Span, outcome, errorCode string) {
	if span == nil {
		return
	}
	outcome = normalize(outcome, []string{"success", "error", "unknown"})
	span.SetAttributes(attribute.String("outcome", outcome))
	switch outcome {
	case "success":
		span.SetStatus(codes.Ok, "")
	case "error":
		errorCode = NormalizeErrorCode(errorCode)
		span.SetAttributes(attribute.String("error_code", errorCode))
		span.SetStatus(codes.Error, errorCode)
	}
}

func InjectHTTP(ctx context.Context, req *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}
func ExtractHTTP(ctx context.Context, req *http.Request) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(req.Header))
}
