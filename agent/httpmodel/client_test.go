package httpmodel

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
)

func TestClientSendsStructuredRequestWithoutLoggingAPIKey(t *testing.T) {
	var logs bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-secret" {
			t.Fatalf("Authorization = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"type":"json_schema"`)) || !bytes.Contains(body, []byte(`"name":"workflow_draft"`)) {
			t.Fatalf("request body = %s, want structured response format", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"test-model","choices":[{"message":{"content":"{\"definition\":{\"id\":\"demo\"}}"}}]}`)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL, Model: "test-model", APIKey: "test-secret", Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Generate(context.Background(), agent.ModelRequest{Goal: "goal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.DraftJSON) == 0 || strings.Contains(logs.String(), "test-secret") {
		t.Fatalf("response = %s logs = %q", response.DraftJSON, logs.String())
	}
}

func TestClientSendsToolConversationAndWorkflowDraftSchema(t *testing.T) {
	var call int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		var payload struct {
			Messages []struct {
				Role       string `json:"role"`
				Content    string `json:"content"`
				ToolCallID string `json:"tool_call_id"`
				ToolCalls  []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"messages"`
			ResponseFormat struct {
				Type       string `json:"type"`
				JSONSchema struct {
					Name   string         `json:"name"`
					Schema map[string]any `json:"schema"`
				} `json:"json_schema"`
			} `json:"response_format"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if call == 1 {
			if payload.ResponseFormat.Type != "json_schema" || payload.ResponseFormat.JSONSchema.Name != "workflow_draft" || payload.ResponseFormat.JSONSchema.Schema["type"] != "object" {
				t.Fatalf("response_format = %#v, want workflow draft JSON schema", payload.ResponseFormat)
			}
			if !strings.Contains(payload.Messages[0].Content, "65536 bytes") {
				t.Fatalf("system message = %q, want response byte limit", payload.Messages[0].Content)
			}
			_, _ = io.WriteString(w, `{"model":"test-model","choices":[{"message":{"tool_calls":[{"id":"catalog-1","type":"function","function":{"name":"workflow_catalog_query","arguments":"{\"query\":\"document\"}"}}]}}]}`)
			return
		}
		if len(payload.Messages) != 4 || payload.Messages[2].Role != "assistant" || len(payload.Messages[2].ToolCalls) != 1 || payload.Messages[2].ToolCalls[0].ID != "catalog-1" || payload.Messages[2].ToolCalls[0].Function.Name != "workflow_catalog_query" || payload.Messages[2].ToolCalls[0].Function.Arguments != `{"query":"document"}` || payload.Messages[3].Role != "tool" || payload.Messages[3].ToolCallID != "catalog-1" {
			t.Fatalf("messages = %#v, want system/user/assistant/tool conversation", payload.Messages)
		}
		_, _ = io.WriteString(w, `{"model":"test-model","choices":[{"message":{"content":"{\"definition\":{\"id\":\"demo\",\"concurrency\":1,\"tasks\":[{\"key\":\"one\",\"action\":\"read-document\",\"input\":{\"source\":\"article.md\"},\"timeout_ms\":1000}]},\"facts\":[],\"assumptions\":[],\"questions\":[]}"}}]}`)
	}))
	defer server.Close()
	client := mustClient(t, server.URL)
	request := agent.ModelRequest{Goal: "读取 article.md", Tools: []agent.ToolSummary{{Name: "workflow_catalog_query", InputSchema: json.RawMessage(`{"type":"object"}`)}}, Limits: agent.ModelLimits{MaxResponseBytes: 65536, RemainingModelTurns: 3, RemainingToolCalls: 8}}
	first, err := client.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result := agent.ToolResult{CallID: first.ToolCalls[0].ID, Name: first.ToolCalls[0].Name, Content: json.RawMessage(`{"items":[]}`)}
	request.ToolResults = []agent.ToolResult{result}
	request.ToolHistory = []agent.ToolExchange{{Calls: first.ToolCalls, Results: []agent.ToolResult{result}}}
	second, err := client.Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var draft agent.WorkflowDraft
	if err := json.Unmarshal(second.DraftJSON, &draft); err != nil || draft.Definition.ID != "demo" {
		t.Fatalf("draft = %#v error = %v", draft, err)
	}
}

func TestClientMapsHTTP429ToTemporaryModelError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "limited", http.StatusTooManyRequests) }))
	defer server.Close()
	client := mustClient(t, server.URL)
	_, err := client.Generate(context.Background(), agent.ModelRequest{Goal: "goal"})
	var runtimeErr *agent.Error
	if agent.CodeOf(err) != agent.CodeModelUnavailable || !asAgentError(err, &runtimeErr) || !runtimeErr.Temporary {
		t.Fatalf("Generate() error = %#v, want temporary model_unavailable", err)
	}
}

func TestClientMapsMalformedResponseToInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{bad`) }))
	defer server.Close()
	_, err := mustClient(t, server.URL).Generate(context.Background(), agent.ModelRequest{Goal: "goal"})
	if agent.CodeOf(err) != agent.CodeModelInvalidResponse {
		t.Fatalf("Generate() error = %v, want model_invalid_response", err)
	}
}

func TestClientHonorsContextCancellation(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := mustClient(t, server.URL).Generate(ctx, agent.ModelRequest{Goal: "goal"})
	if agent.CodeOf(err) != agent.CodeModelTimeout {
		t.Fatalf("Generate() error = %v, want model_timeout", err)
	}
}

func TestClientDoesNotRunWhenConfigurationIsMissing(t *testing.T) {
	for _, config := range []Config{{Model: "model", APIKey: "key"}, {Endpoint: "http://example.test", APIKey: "key"}, {Endpoint: "http://example.test", Model: "model"}} {
		if _, err := New(config); err == nil {
			t.Fatalf("New(%+v) error = nil, want configuration error", config)
		}
	}
}

func TestClientRejectsRemotePlainHTTPButAllowsLoopback(t *testing.T) {
	if _, err := New(Config{Endpoint: "http://models.example.com/v1/chat/completions", Model: "model", APIKey: "secret"}); err == nil {
		t.Fatal("New() accepted remote plain HTTP endpoint")
	}
	for _, endpoint := range []string{"http://127.0.0.1:8080/v1/chat/completions", "http://localhost:8080/v1/chat/completions", "http://[::1]:8080/v1/chat/completions"} {
		if _, err := New(Config{Endpoint: endpoint, Model: "model", APIKey: "secret"}); err != nil {
			t.Fatalf("New(%q) error = %v, want loopback HTTP allowed", endpoint, err)
		}
	}
}

func TestClientDoesNotForwardAPIKeyAcrossRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	_, err := mustClient(t, redirect.URL).Generate(context.Background(), agent.ModelRequest{Goal: "goal"})
	if agent.CodeOf(err) != agent.CodeModelUnavailable || targetCalls.Load() != 0 {
		t.Fatalf("Generate() error = %v target calls = %d, want blocked redirect", err, targetCalls.Load())
	}
}

func mustClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := New(Config{Endpoint: endpoint, Model: "model", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func asAgentError(err error, target **agent.Error) bool {
	for err != nil {
		if value, ok := err.(*agent.Error); ok {
			*target = value
			return true
		}
		type unwrapper interface{ Unwrap() error }
		value, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = value.Unwrap()
	}
	return false
}
