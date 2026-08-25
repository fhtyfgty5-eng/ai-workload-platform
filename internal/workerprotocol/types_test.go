package workerprotocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestWorkerSummaryJSONContainsNoCredentialFields(t *testing.T) {
	summary := WorkerSummary{
		WorkerID:        "worker-one",
		DisplayName:     "worker",
		ProtocolVersion: ProtocolVersion,
		ExecutorKinds:   []workflow.ExecutorKind{workflow.ExecutorMock},
		MaxConcurrency:  2,
		Status:          WorkerActive,
		RegisteredAt:    time.Unix(1, 0).UTC(),
		LastHeartbeatAt: time.Unix(2, 0).UTC(),
	}
	body, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"session_token", "token_hash", "lease_token", "input"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("WorkerSummary JSON %s contains forbidden field %q", body, forbidden)
		}
	}
}

func TestWorkerProtocolConstantsRemainStable(t *testing.T) {
	if ProtocolVersion != 1 {
		t.Fatalf("ProtocolVersion = %d, want 1", ProtocolVersion)
	}
	statuses := []WorkerStatus{WorkerActive, WorkerDraining, WorkerOffline, WorkerStopped}
	want := []string{"active", "draining", "offline", "stopped"}
	for index := range statuses {
		if string(statuses[index]) != want[index] {
			t.Fatalf("status[%d] = %q, want %q", index, statuses[index], want[index])
		}
	}
}

func TestCompleteRequestUsesStableSnakeCaseJSON(t *testing.T) {
	body, err := json.Marshal(CompleteRequest{
		LeaseToken: "lease-token",
		Result: workflow.ExecutionResponse{
			Kind: workflow.ResultTemporaryFailure, ErrorCode: "retry_later", ErrorMessage: "temporary failure",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(body)
	for _, required := range []string{`"lease_token"`, `"kind"`, `"error_code"`, `"error_message"`} {
		if !strings.Contains(jsonText, required) {
			t.Fatalf("CompleteRequest JSON %s is missing %s", jsonText, required)
		}
	}
	for _, forbidden := range []string{`"Kind"`, `"ErrorCode"`, `"ErrorMessage"`} {
		if strings.Contains(jsonText, forbidden) {
			t.Fatalf("CompleteRequest JSON %s contains unstable field %s", jsonText, forbidden)
		}
	}
}
