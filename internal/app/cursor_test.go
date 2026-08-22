package app

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestRunCursorRoundTripAndFilterBinding(t *testing.T) {
	createdAt := time.Date(2026, 8, 21, 12, 34, 56, 123, time.UTC)
	encoded, err := encodeRunCursor(createdAt, "run-one", "workflow-one", workflow.WorkflowRunning)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRunCursor(encoded, "workflow-one", workflow.WorkflowRunning)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.CreatedAt.Equal(createdAt) || decoded.RunID != "run-one" {
		t.Fatalf("decoded cursor = %+v", decoded)
	}
	if _, err := decodeRunCursor(encoded, "workflow-two", workflow.WorkflowRunning); err == nil {
		t.Fatal("decodeRunCursor() accepted a cursor from another filter")
	}
}

func TestRunCursorRejectsMalformedOrUnsupportedInput(t *testing.T) {
	validTime := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	tests := []struct {
		name  string
		value string
	}{
		{name: "invalid base64", value: "%%%"},
		{name: "unknown version", value: rawCursor(`{"v":2,"kind":"run","created_at":"` + validTime + `","run_id":"run-one","filter_hash":"hash"}`)},
		{name: "wrong kind", value: rawCursor(`{"v":1,"kind":"task","created_at":"` + validTime + `","run_id":"run-one","filter_hash":"hash"}`)},
		{name: "missing created at", value: rawCursor(`{"v":1,"kind":"run","run_id":"run-one","filter_hash":"hash"}`)},
		{name: "missing run id", value: rawCursor(`{"v":1,"kind":"run","created_at":"` + validTime + `","filter_hash":"hash"}`)},
		{name: "oversized", value: rawCursor(`{"v":1,"kind":"run","created_at":"` + validTime + `","run_id":"run-one","filter_hash":"` + strings.Repeat("a", 4096) + `"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRunCursor(test.value, "", ""); err == nil {
				t.Fatal("decodeRunCursor() error = nil")
			}
		})
	}
}

func TestParseRunStatusRejectsUnknownValue(t *testing.T) {
	for _, value := range []string{"", "pending", "running", "succeeded", "failed", "canceled"} {
		if _, err := parseRunStatus(value); err != nil {
			t.Fatalf("parseRunStatus(%q) error = %v", value, err)
		}
	}
	if _, err := parseRunStatus("unknown"); err == nil {
		t.Fatal("parseRunStatus() accepted unknown status")
	}
}

func rawCursor(body string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(body))
}
