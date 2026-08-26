package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerconfig"
	"go.opentelemetry.io/otel"
)

func TestNewWorkerTracerExportsClientSpansInStdoutMode(t *testing.T) {
	var output bytes.Buffer
	closeTracer, err := newWorkerTracer(workerconfig.Config{
		TracingMode:        "stdout",
		TracingServiceName: "workload-worker-test",
	}, &output)
	if err != nil {
		t.Fatalf("newWorkerTracer() error = %v", err)
	}
	_, span := otel.Tracer("workload-worker").Start(context.Background(), "worker.claim")
	span.End()
	if err := closeTracer(context.Background()); err != nil {
		t.Fatalf("closeTracer() error = %v", err)
	}
	if !strings.Contains(output.String(), "name=worker.claim") {
		t.Fatalf("trace output = %q, want worker.claim", output.String())
	}
}
