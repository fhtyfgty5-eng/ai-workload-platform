// Package catalog 提供模块 3 的静态只读任务模板目录。
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
)

// Catalog 是按模板 ID 和 Action 建立索引的进程内只读目录。
type Catalog struct {
	templates map[string]agent.TaskTemplate
	byAction  map[string]string
}

// New 创建目录并拒绝重复模板 ID 或 Action。
func New(templates []agent.TaskTemplate) (*Catalog, error) {
	catalog := &Catalog{templates: make(map[string]agent.TaskTemplate, len(templates)), byAction: make(map[string]string, len(templates))}
	for _, template := range templates {
		if template.ID == "" || template.Action == "" {
			return nil, fmt.Errorf("template ID and action are required")
		}
		if _, exists := catalog.templates[template.ID]; exists {
			return nil, fmt.Errorf("duplicate template %q", template.ID)
		}
		if _, exists := catalog.byAction[template.Action]; exists {
			return nil, fmt.Errorf("duplicate action %q", template.Action)
		}
		catalog.templates[template.ID] = cloneTemplate(template)
		catalog.byAction[template.Action] = template.ID
	}
	return catalog, nil
}

// DefaultTemplates 返回模块 3 文档处理演示允许使用的任务模板。
func DefaultTemplates() []agent.TaskTemplate {
	return []agent.TaskTemplate{
		{ID: "read-document", Action: "read-document", Description: "读取指定的公开示例文档", Parameters: map[string]agent.ParameterSpec{"source": {Type: "string", Required: true}}, RequiredPermission: "document:read", Resources: agent.ResourceLimits{MaxTimeoutMillis: 30_000}, AgentAllowed: true},
		{ID: "clean-document", Action: "clean-document", Description: "清洗文档内容", Parameters: map[string]agent.ParameterSpec{"mode": {Type: "string", Enum: []string{"standard", "strict"}}}, Resources: agent.ResourceLimits{MaxTimeoutMillis: 30_000}, AgentAllowed: true},
		{ID: "summarize-document", Action: "summarize-document", Description: "生成文档摘要", Parameters: map[string]agent.ParameterSpec{"max_words": {Type: "number"}}, Resources: agent.ResourceLimits{MaxTimeoutMillis: 60_000}, AgentAllowed: true},
	}
}

// Query 按模板 ID、Action 或说明筛选并返回隔离副本。
func (c *Catalog) Query(query string) []agent.TaskTemplate {
	query = strings.ToLower(strings.TrimSpace(query))
	ids := make([]string, 0, len(c.templates))
	for id, template := range c.templates {
		if query == "" || strings.Contains(strings.ToLower(id+" "+template.Action+" "+template.Description), query) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	items := make([]agent.TaskTemplate, 0, len(ids))
	for _, id := range ids {
		items = append(items, cloneTemplate(c.templates[id]))
	}
	return items
}

// FindByAction 返回指定 Action 的模板隔离副本。
func (c *Catalog) FindByAction(action string) (agent.TaskTemplate, bool) {
	id, found := c.byAction[action]
	if !found {
		return agent.TaskTemplate{}, false
	}
	return cloneTemplate(c.templates[id]), true
}

// Tool 把只读目录查询暴露为 Agent Runtime 工具。
type Tool struct{ catalog *Catalog }

// NewTool 创建仅能查询指定目录的工具。
func NewTool(catalog *Catalog) *Tool { return &Tool{catalog: catalog} }
func (t *Tool) Name() string         { return "workflow_catalog_query" }
func (t *Tool) Description() string  { return "查询允许 Agent 使用的工作流任务模板" }
func (t *Tool) InputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","maxLength":1024}},"additionalProperties":false}`)
}

func (t *Tool) Invoke(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, &agent.Error{Code: agent.CodeCanceled, Message: "catalog query canceled", Cause: err}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var input struct {
		Query string `json:"query"`
	}
	if err := decoder.Decode(&input); err != nil {
		return nil, &agent.Error{Code: agent.CodeToolInvalidInput, Message: "catalog query must contain only query", Cause: err}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, &agent.Error{Code: agent.CodeToolInvalidInput, Message: "catalog query contains trailing JSON", Cause: err}
	}
	if len(input.Query) > 1024 {
		return nil, &agent.Error{Code: agent.CodeToolInvalidInput, Message: "catalog query exceeds 1024 bytes"}
	}
	body, err := json.Marshal(struct {
		Items []agent.TaskTemplate `json:"items"`
	}{Items: t.catalog.Query(input.Query)})
	if err != nil {
		return nil, &agent.Error{Code: agent.CodeInternal, Message: "encode catalog response", Cause: err}
	}
	return body, nil
}

func cloneTemplate(template agent.TaskTemplate) agent.TaskTemplate {
	clone := template
	clone.Parameters = make(map[string]agent.ParameterSpec, len(template.Parameters))
	for name, parameter := range template.Parameters {
		parameter.Enum = append([]string(nil), parameter.Enum...)
		clone.Parameters[name] = parameter
	}
	return clone
}
