// Package agentapp 组装控制面和 CLI 共用的 Agent Runtime 依赖。
package agentapp

import (
	"fmt"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/catalog"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/httpmodel"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/mockmodel"
)

// NewService 创建默认 Agent Runtime。modelName 只接受 mock 或 http，真实模型配置
// 通过 getenv 读取，避免把 API Key 放入浏览器、命令参数或工作流定义。
func NewService(modelName string, getenv func(string) string) (*agent.Service, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		return nil, err
	}
	registry, err := agent.NewToolRegistry(agent.RegisteredTool{
		Tool:               catalog.NewTool(directory),
		RequiredPermission: "catalog:read",
	})
	if err != nil {
		return nil, err
	}
	var model agent.ModelAdapter
	switch modelName {
	case "", "mock":
		model = mockmodel.New()
	case "http":
		model, err = httpmodel.New(httpmodel.Config{
			Endpoint: getenv("WORKLOAD_MODEL_ENDPOINT"),
			Model:    getenv("WORKLOAD_MODEL_NAME"),
			APIKey:   getenv("WORKLOAD_MODEL_API_KEY"),
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported model adapter %q", modelName)
	}
	return agent.NewService(
		model,
		registry,
		agent.NewDraftValidator(directory, []string{"document:read"}),
		agent.NewMemoryAuditSink(),
		agent.DefaultLimits(),
		[]string{"catalog:read"},
	)
}
