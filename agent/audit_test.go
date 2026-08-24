package agent

import (
	"context"
	"testing"
)

func TestAuditRedactsAuthorizationAndAPIKey(t *testing.T) {
	sink := NewMemoryAuditSink()
	err := sink.Record(context.Background(), AuditEvent{Type: "model.request", Metadata: map[string]any{
		"Authorization": "Bearer secret",
		"api_key":       "secret-key",
		"safe":          "visible",
		"nested": []any{
			map[string]any{"token": "nested-secret"},
			map[string]string{"password": "nested-password"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	events := sink.Events()
	if got := events[0].Metadata["Authorization"]; got != redactedValue {
		t.Fatalf("Authorization = %q, want redacted", got)
	}
	if got := events[0].Metadata["api_key"]; got != redactedValue {
		t.Fatalf("api_key = %q, want redacted", got)
	}
	if got := events[0].Metadata["safe"]; got != "visible" {
		t.Fatalf("safe = %q, want visible", got)
	}
	nested := events[0].Metadata["nested"].([]any)
	if nested[0].(map[string]any)["token"] != redactedValue || nested[1].(map[string]string)["password"] != redactedValue {
		t.Fatalf("nested metadata was not redacted: %#v", nested)
	}
}
