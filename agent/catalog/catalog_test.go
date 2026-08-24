package catalog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
)

func TestCatalogQueryReturnsOnlyRegisteredTemplates(t *testing.T) {
	catalog, err := New(DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	items := catalog.Query("summarize")
	if len(items) != 1 || items[0].Action != "summarize-document" {
		t.Fatalf("Query() = %#v, want summarize template", items)
	}
}

func TestCatalogQueryReturnsIsolatedCopies(t *testing.T) {
	catalog, err := New(DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	items := catalog.Query("read")
	items[0].Parameters["source"] = agent.ParameterSpec{Type: "changed"}
	if got := catalog.Query("read")[0].Parameters["source"].Type; got != "string" {
		t.Fatalf("stored parameter type = %q, want string", got)
	}
}

func TestCatalogRejectsDuplicateAction(t *testing.T) {
	_, err := New([]agent.TaskTemplate{
		{ID: "strict", Action: "read-document", RequiredPermission: "document:strict"},
		{ID: "public", Action: "read-document"},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate action") {
		t.Fatalf("New() error = %v, want duplicate action", err)
	}
}

func TestCatalogToolRejectsUnknownFieldsAndOversizedQuery(t *testing.T) {
	catalog, err := New(DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	tool := NewTool(catalog)
	for _, input := range []json.RawMessage{
		json.RawMessage(`{"query":"read","path":"/tmp"}`),
		json.RawMessage(`{"query":"read"}{"query":"clean"}`),
		json.RawMessage(`{"query":"` + strings.Repeat("x", 1025) + `"}`),
	} {
		if _, err := tool.Invoke(context.Background(), input); agent.CodeOf(err) != agent.CodeToolInvalidInput {
			t.Fatalf("Invoke(%s) error = %v, want tool_invalid_input", input, err)
		}
	}
}

func TestCatalogToolHasNoFileCommandOrNetworkInput(t *testing.T) {
	catalog, _ := New(DefaultTemplates())
	schema := string(NewTool(catalog).InputSchema())
	for _, forbidden := range []string{"path", "command", "url", "method", "body"} {
		if strings.Contains(schema, forbidden) {
			t.Fatalf("tool input schema contains forbidden field %q: %s", forbidden, schema)
		}
	}
}
