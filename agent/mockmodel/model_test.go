package mockmodel

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
)

func TestMockModelProducesDeterministicDocumentPipelineDraft(t *testing.T) {
	model := New()
	first, err := model.Generate(context.Background(), agent.ModelRequest{Goal: "先读取 article.md，再清洗内容，最后生成摘要"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ToolCalls) != 1 || first.ToolCalls[0].Name != "workflow_catalog_query" {
		t.Fatalf("first response = %#v, want catalog tool call", first)
	}
	request := agent.ModelRequest{Goal: "先读取 article.md，再清洗内容，最后生成摘要", ToolResults: []agent.ToolResult{{CallID: first.ToolCalls[0].ID, Name: first.ToolCalls[0].Name, Content: []byte(`{"items":[]}`)}}}
	second, err := model.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	third, _ := model.Generate(context.Background(), request)
	if string(second.DraftJSON) != string(third.DraftJSON) || len(second.DraftJSON) == 0 {
		t.Fatalf("mock output is not deterministic: %s != %s", second.DraftJSON, third.DraftJSON)
	}
	var draft agent.WorkflowDraft
	if err := json.Unmarshal(second.DraftJSON, &draft); err != nil {
		t.Fatal(err)
	}
	if len(draft.Facts) != 1 || draft.Facts[0].Statement != "用户要求读取 article.md，并按读取、清洗、摘要的顺序处理" {
		t.Fatalf("Facts = %#v, want article.md and task order from the user goal", draft.Facts)
	}
	if len(draft.Assumptions) != 1 || draft.Assumptions[0].Statement != "清洗模式使用 standard，摘要上限为 200 词" {
		t.Fatalf("Assumptions = %#v, want only model-provided defaults", draft.Assumptions)
	}
}
