package agent

import (
	"context"
	"encoding/json"
	"testing"
)

type testTool struct{ calls int }

func (t *testTool) Name() string        { return "catalog.query" }
func (t *testTool) Description() string { return "query templates" }
func (t *testTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object"}`)
}
func (t *testTool) Invoke(context.Context, json.RawMessage) (json.RawMessage, error) {
	t.calls++
	return json.RawMessage(`{"items":[]}`), nil
}

func TestRegistryRejectsUnknownTool(t *testing.T) {
	registry, err := NewToolRegistry(RegisteredTool{Tool: &testTool{}, RequiredPermission: "catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Invoke(context.Background(), ToolRequest{Name: "missing", Permissions: []string{"catalog:read"}})
	if CodeOf(err) != CodeToolNotAllowed {
		t.Fatalf("Invoke() error = %v, want tool_not_allowed", err)
	}
}

func TestRegistryRejectsToolWithoutPermission(t *testing.T) {
	tool := &testTool{}
	registry, err := NewToolRegistry(RegisteredTool{Tool: tool, RequiredPermission: "catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Invoke(context.Background(), ToolRequest{Name: tool.Name(), Permissions: []string{"other"}})
	if CodeOf(err) != CodeToolNotAllowed || tool.calls != 0 {
		t.Fatalf("Invoke() error = %v calls = %d, want denied before call", err, tool.calls)
	}
}
