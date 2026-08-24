// Package mockmodel 提供不访问网络的确定性模型模拟器。
package mockmodel

import (
	"context"
	"encoding/json"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Model 用固定两轮响应离线复现目录查询和草稿生成。
type Model struct{}

// New 创建不访问网络的确定性 Mock Model。
func New() *Model { return &Model{} }

func (m *Model) Generate(ctx context.Context, request agent.ModelRequest) (agent.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeCanceled, Message: "mock model canceled", Cause: err}
	}
	if len(request.ToolResults) == 0 {
		return agent.ModelResponse{Model: "mock-model", ToolCalls: []agent.ModelToolCall{{
			ID: "catalog-1", Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":"document"}`),
		}}}, nil
	}
	draft := agent.WorkflowDraft{
		Goal: request.Goal,
		Definition: workflow.WorkflowDefinition{ID: "agent-document-pipeline", Concurrency: 1, Tasks: []workflow.TaskDefinition{
			{Key: "read", Action: "read-document", Input: map[string]any{"source": "article.md"}, TimeoutMillis: 30_000},
			{Key: "clean", Action: "clean-document", Input: map[string]any{"mode": "standard"}, DependsOn: []workflow.TaskKey{"read"}, TimeoutMillis: 30_000},
			{Key: "summarize", Action: "summarize-document", Input: map[string]any{"max_words": 200}, DependsOn: []workflow.TaskKey{"clean"}, TimeoutMillis: 60_000},
		}},
		Facts: []agent.Evidence{
			{Statement: "用户要求读取 article.md，并按读取、清洗、摘要的顺序处理", Source: "user"},
		},
		Assumptions: []agent.Evidence{
			{Statement: "清洗模式使用 standard，摘要上限为 200 词", Source: "agent"},
		},
		Questions: []agent.Question{},
		Status:    agent.DraftGenerated,
	}
	body, err := json.Marshal(draft)
	if err != nil {
		return agent.ModelResponse{}, err
	}
	return agent.ModelResponse{Model: "mock-model", DraftJSON: body}, nil
}
