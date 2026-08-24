package agent_test

import (
	"context"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/catalog"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/mockmodel"
)

func BenchmarkGenerateDraftMock(b *testing.B) {
	service := benchmarkService(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateDraft(b *testing.B) {
	service := benchmarkService(b)
	draft, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := service.ValidateDraft(context.Background(), draft); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCanceledGenerateDraft(b *testing.B) {
	service := benchmarkService(b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := service.GenerateDraft(ctx, "goal"); agent.CodeOf(err) != agent.CodeCanceled {
			b.Fatalf("error = %v, want canceled", err)
		}
	}
}

func benchmarkService(b *testing.B) *agent.Service {
	b.Helper()
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		b.Fatal(err)
	}
	registry, err := agent.NewToolRegistry(agent.RegisteredTool{Tool: catalog.NewTool(directory), RequiredPermission: "catalog:read"})
	if err != nil {
		b.Fatal(err)
	}
	service, err := agent.NewService(mockmodel.New(), registry, agent.NewDraftValidator(directory, []string{"document:read"}), agent.NewMemoryAuditSink(), agent.DefaultLimits(), []string{"catalog:read"})
	if err != nil {
		b.Fatal(err)
	}
	return service
}
