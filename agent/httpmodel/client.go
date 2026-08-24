// Package httpmodel 提供人工显式启用的 OpenAI-compatible HTTP 模型适配器。
package httpmodel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
)

// Config 保存人工启用 HTTP 模型适配器所需的显式配置。
type Config struct {
	Endpoint         string
	Model            string
	APIKey           string
	HTTPClient       *http.Client
	Logger           *slog.Logger
	MaxResponseBytes int64
}

// Client 把平台模型协议转换为 OpenAI-compatible HTTP 请求。
type Client struct {
	endpoint         string
	model            string
	apiKey           string
	httpClient       *http.Client
	logger           *slog.Logger
	maxResponseBytes int64
}

// New 校验端点和凭证配置，并创建禁止自动重定向的模型客户端。
func New(config Config) (*Client, error) {
	if config.Endpoint == "" || config.Model == "" || config.APIKey == "" {
		return nil, fmt.Errorf("model endpoint, model and API key are required")
	}
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("model endpoint must be an absolute HTTP URL")
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("remote model endpoint must use HTTPS")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	} else {
		clone := *config.HTTPClient
		config.HTTPClient = &clone
	}
	config.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = 64 * 1024
	}
	return &Client{endpoint: config.Endpoint, model: config.Model, apiKey: config.APIKey, httpClient: config.HTTPClient, logger: config.Logger, maxResponseBytes: config.MaxResponseBytes}, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Client) Generate(ctx context.Context, request agent.ModelRequest) (agent.ModelResponse, error) {
	body, err := c.encodeRequest(request)
	if err != nil {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "encode model request", Cause: err}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelUnavailable, Message: "build model request", Cause: err}
	}
	httpRequest.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	c.logger.DebugContext(ctx, "model request started", "model", c.model)
	response, err := c.httpClient.Do(httpRequest)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelTimeout, Message: "model request timed out", Cause: err}
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return agent.ModelResponse{}, &agent.Error{Code: agent.CodeCanceled, Message: "model request canceled", Cause: err}
		}
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelUnavailable, Message: "model request failed", Temporary: true, Cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		temporary := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelUnavailable, Message: fmt.Sprintf("model HTTP status %d", response.StatusCode), Temporary: temporary}
	}
	limited := io.LimitReader(response.Body, c.maxResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelUnavailable, Message: "read model response", Temporary: true, Cause: err}
	}
	if int64(len(responseBody)) > c.maxResponseBytes {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeBudgetExceeded, Message: "model response exceeds byte limit"}
	}
	result, err := decodeResponse(responseBody)
	if err != nil {
		return agent.ModelResponse{}, err
	}
	c.logger.DebugContext(ctx, "model request completed", "model", result.Model)
	return result, nil
}

func (c *Client) encodeRequest(request agent.ModelRequest) ([]byte, error) {
	type function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	}
	type tool struct {
		Type     string   `json:"type"`
		Function function `json:"function"`
	}
	type toolCall struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	type message struct {
		Role       string     `json:"role"`
		Content    string     `json:"content,omitempty"`
		ToolCalls  []toolCall `json:"tool_calls,omitempty"`
		ToolCallID string     `json:"tool_call_id,omitempty"`
		Name       string     `json:"name,omitempty"`
	}
	tools := make([]tool, 0, len(request.Tools))
	for _, summary := range request.Tools {
		tools = append(tools, tool{Type: "function", Function: function{Name: summary.Name, Description: summary.Description, Parameters: summary.InputSchema}})
	}
	messages := []message{
		{Role: "system", Content: fmt.Sprintf(
			"Generate a workflow draft that matches the provided JSON schema. Use only declared tools and distinguish user facts from model assumptions. Keep the response within %d bytes; %d model turns and %d tool calls remain.",
			request.Limits.MaxResponseBytes, request.Limits.RemainingModelTurns, request.Limits.RemainingToolCalls,
		)},
		{Role: "user", Content: request.Goal},
	}
	for _, exchange := range request.ToolHistory {
		assistantCalls := make([]toolCall, 0, len(exchange.Calls))
		for _, call := range exchange.Calls {
			encoded := toolCall{ID: call.ID, Type: "function"}
			encoded.Function.Name = call.Name
			encoded.Function.Arguments = string(call.Arguments)
			assistantCalls = append(assistantCalls, encoded)
		}
		messages = append(messages, message{Role: "assistant", ToolCalls: assistantCalls})
		for _, result := range exchange.Results {
			messages = append(messages, message{Role: "tool", ToolCallID: result.CallID, Name: result.Name, Content: string(result.Content)})
		}
	}
	payload := struct {
		Model          string         `json:"model"`
		Messages       []message      `json:"messages"`
		ResponseFormat map[string]any `json:"response_format"`
		Tools          []tool         `json:"tools,omitempty"`
	}{
		Model:    c.model,
		Messages: messages,
		ResponseFormat: map[string]any{
			"type":        "json_schema",
			"json_schema": map[string]any{"name": "workflow_draft", "strict": false, "schema": workflowDraftSchema()},
		},
		Tools: tools,
	}
	return json.Marshal(payload)
}

func workflowDraftSchema() map[string]any {
	evidence := map[string]any{
		"type": "object", "additionalProperties": false,
		"required":   []string{"statement", "source"},
		"properties": map[string]any{"statement": map[string]any{"type": "string"}, "source": map[string]any{"type": "string"}},
	}
	question := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"id", "text", "resolved"},
		"properties": map[string]any{
			"id": map[string]any{"type": "string"}, "text": map[string]any{"type": "string"},
			"answer": map[string]any{"type": "string"}, "resolved": map[string]any{"type": "boolean"},
		},
	}
	task := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"key", "action", "timeout_ms"},
		"properties": map[string]any{
			"key": map[string]any{"type": "string"}, "action": map[string]any{"type": "string"},
			"input":      map[string]any{"type": "object", "additionalProperties": true},
			"depends_on": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"retry": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{
				"max_attempts": map[string]any{"type": "integer"}, "interval_ms": map[string]any{"type": "integer"},
			}},
			"timeout_ms": map[string]any{"type": "integer"},
		},
	}
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"definition", "facts", "assumptions", "questions"},
		"properties": map[string]any{
			"definition": map[string]any{
				"type": "object", "additionalProperties": false, "required": []string{"id", "concurrency", "tasks"},
				"properties": map[string]any{"id": map[string]any{"type": "string"}, "concurrency": map[string]any{"type": "integer"}, "tasks": map[string]any{"type": "array", "items": task}},
			},
			"facts":       map[string]any{"type": "array", "items": evidence},
			"assumptions": map[string]any{"type": "array", "items": evidence},
			"questions":   map[string]any{"type": "array", "items": question},
		},
	}
}

func decodeResponse(body []byte) (agent.ModelResponse, error) {
	var payload struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || len(payload.Choices) == 0 {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "model response has invalid JSON or no choices", Cause: err}
	}
	message := payload.Choices[0].Message
	result := agent.ModelResponse{Model: payload.Model}
	if message.Content != "" {
		if !json.Valid([]byte(message.Content)) {
			return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "model content is not JSON"}
		}
		result.DraftJSON = json.RawMessage(message.Content)
	}
	for _, call := range message.ToolCalls {
		arguments := json.RawMessage(call.Function.Arguments)
		if call.ID == "" || call.Function.Name == "" || !json.Valid(arguments) {
			return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "model tool call is invalid"}
		}
		result.ToolCalls = append(result.ToolCalls, agent.ModelToolCall{ID: call.ID, Name: call.Function.Name, Arguments: arguments})
	}
	if len(result.DraftJSON) == 0 && len(result.ToolCalls) == 0 {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "model response is empty"}
	}
	return result, nil
}
