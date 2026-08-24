package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// ParameterSpec 描述一个任务输入参数的 JSON 类型和取值约束。
type ParameterSpec struct {
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Enum     []string `json:"enum,omitempty"`
}

// ResourceLimits 保存模块 3 当前能够校验的任务模板执行上限。
type ResourceLimits struct {
	// MaxTimeoutMillis 是单次任务 Attempt 可声明的最大超时时间。
	MaxTimeoutMillis int64 `json:"max_timeout_ms"`
}

// TaskTemplate 描述 Agent 可发现但不能修改的任务能力。
type TaskTemplate struct {
	ID                 string                   `json:"id"`
	Action             string                   `json:"action"`
	Description        string                   `json:"description"`
	Parameters         map[string]ParameterSpec `json:"parameters,omitempty"`
	RequiredPermission string                   `json:"required_permission,omitempty"`
	Resources          ResourceLimits           `json:"resources"`
	AgentAllowed       bool                     `json:"agent_allowed"`
}

// Tool 是 Runtime 可以授权调用的受控能力接口。
type Tool interface {
	Name() string
	Description() string
	InputSchema() json.RawMessage
	Invoke(context.Context, json.RawMessage) (json.RawMessage, error)
}

// RegisteredTool 把工具实现与调用它所需的平台权限绑定。
type RegisteredTool struct {
	Tool               Tool
	RequiredPermission string
}

// ToolRequest 是 Runtime 完成模型响应预校验后发给注册表的调用请求。
type ToolRequest struct {
	Name        string
	Arguments   json.RawMessage
	Permissions []string
}

// ToolResponse 保存工具返回给模型的结构化 JSON 内容。
type ToolResponse struct {
	Content json.RawMessage
}

// ToolRegistry 保存显式注册的工具并在调用前检查权限。
type ToolRegistry struct {
	tools map[string]RegisteredTool
}

// NewToolRegistry 创建工具注册表，并拒绝空名称和重复名称。
func NewToolRegistry(tools ...RegisteredTool) (*ToolRegistry, error) {
	registry := &ToolRegistry{tools: make(map[string]RegisteredTool, len(tools))}
	for _, registered := range tools {
		if registered.Tool == nil || registered.Tool.Name() == "" {
			return nil, fmt.Errorf("registered tool and name are required")
		}
		if _, exists := registry.tools[registered.Tool.Name()]; exists {
			return nil, fmt.Errorf("duplicate tool %q", registered.Tool.Name())
		}
		registry.tools[registered.Tool.Name()] = registered
	}
	return registry, nil
}

// Summaries 返回按名称排序的模型可见工具说明副本。
func (r *ToolRegistry) Summaries() []ToolSummary {
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	slices.Sort(names)
	summaries := make([]ToolSummary, 0, len(names))
	for _, name := range names {
		tool := r.tools[name].Tool
		summaries = append(summaries, ToolSummary{Name: name, Description: tool.Description(), InputSchema: append(json.RawMessage(nil), tool.InputSchema()...)})
	}
	return summaries
}

// Invoke 查找工具、校验权限并执行一次调用。
func (r *ToolRegistry) Invoke(ctx context.Context, request ToolRequest) (ToolResponse, error) {
	registered, exists := r.tools[request.Name]
	if !exists {
		return ToolResponse{}, &Error{Code: CodeToolNotAllowed, Message: "tool is not registered"}
	}
	if registered.RequiredPermission != "" && !slices.Contains(request.Permissions, registered.RequiredPermission) {
		return ToolResponse{}, &Error{Code: CodeToolNotAllowed, Message: "tool permission is missing"}
	}
	content, err := registered.Tool.Invoke(ctx, request.Arguments)
	if err != nil {
		return ToolResponse{}, err
	}
	return ToolResponse{Content: content}, nil
}
