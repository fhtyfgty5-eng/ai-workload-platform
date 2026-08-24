package agent

import (
	"context"
	"encoding/json"
)

// ToolSummary 只向模型公开工具名称、说明和输入结构，不暴露实现对象。
type ToolSummary struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ToolResult 是一次模型工具调用对应的结构化返回值。
type ToolResult struct {
	// CallID 关联模型请求中的同名调用，不能在同一轮响应中重复。
	CallID  string          `json:"call_id"`
	Name    string          `json:"name"`
	Content json.RawMessage `json:"content"`
}

// ToolExchange 保存一轮模型工具请求及其结果，供有会话协议的适配器重建消息历史。
type ToolExchange struct {
	Calls   []ModelToolCall `json:"calls"`
	Results []ToolResult    `json:"results"`
}

// ModelLimits 告知适配器当前会话仍可使用的调用次数和单次响应大小。
type ModelLimits struct {
	MaxResponseBytes    int `json:"max_response_bytes"`
	RemainingModelTurns int `json:"remaining_model_turns"`
	RemainingToolCalls  int `json:"remaining_tool_calls"`
}

// ModelRequest 是 Runtime 传给模型适配器的供应商无关请求。
type ModelRequest struct {
	Goal        string         `json:"goal"`
	Tools       []ToolSummary  `json:"tools,omitempty"`
	ToolResults []ToolResult   `json:"tool_results,omitempty"`
	ToolHistory []ToolExchange `json:"tool_history,omitempty"`
	Limits      ModelLimits    `json:"limits"`
}

// ModelToolCall 是模型提出、仍需 Runtime 校验和授权的工具调用建议。
type ModelToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// ModelResponse 表示模型返回的最终草稿或下一轮工具调用。
type ModelResponse struct {
	DraftJSON json.RawMessage `json:"draft_json,omitempty"`
	ToolCalls []ModelToolCall `json:"tool_calls,omitempty"`
	Model     string          `json:"model,omitempty"`
}

// ModelAdapter 隔离不同模型供应商的请求和响应协议。
type ModelAdapter interface {
	Generate(context.Context, ModelRequest) (ModelResponse, error)
}
