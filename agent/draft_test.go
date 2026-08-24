package agent

import (
	"encoding/json"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestDraftHashIsStableAcrossMapInsertionOrder(t *testing.T) {
	first := validDraft()
	first.Definition.Tasks[0].Input = map[string]any{"source": "article.md", "format": "markdown"}
	second := validDraft()
	second.Definition.Tasks[0].Input = map[string]any{"format": "markdown", "source": "article.md"}
	firstHash, err := ComputeDraftHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ComputeDraftHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("hashes differ: %s != %s", firstHash, secondHash)
	}
}

func TestDraftHashChangesWhenDefinitionChanges(t *testing.T) {
	draft := validDraft()
	before, _ := ComputeDraftHash(draft)
	draft.Definition.Tasks[0].Action = "changed"
	after, _ := ComputeDraftHash(draft)
	if before == after {
		t.Fatal("hash did not change after definition mutation")
	}
}

func TestDraftHashChangesWhenReviewStatusChanges(t *testing.T) {
	draft := validDraft()
	first, err := ComputeDraftHash(draft)
	if err != nil {
		t.Fatal(err)
	}
	draft.Status = DraftNeedsConfirmation
	second, err := ComputeDraftHash(draft)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("content hash did not change when review status changed")
	}
}

func TestDraftRejectsConfirmationWithOpenQuestion(t *testing.T) {
	draft := validDraft()
	draft.Status = DraftNeedsConfirmation
	draft.Questions = []Question{{ID: "source", Text: "使用哪个文档？"}}
	if err := TransitionDraft(&draft, DraftConfirmed); CodeOf(err) != CodeApprovalRequired {
		t.Fatalf("TransitionDraft() error = %v, want approval_required", err)
	}
}

func TestDraftRejectsIllegalStatusTransition(t *testing.T) {
	draft := validDraft()
	if err := TransitionDraft(&draft, DraftConfirmed); CodeOf(err) != CodeDraftInvalid {
		t.Fatalf("TransitionDraft() error = %v, want draft_invalid", err)
	}
}

func TestDraftJSONRoundTripPreservesEvidenceAndValidation(t *testing.T) {
	draft := validDraft()
	draft.Facts = []Evidence{{Statement: "用户要求先读取文档", Source: "user"}}
	draft.Assumptions = []Evidence{{Statement: "默认清洗模式为 standard", Source: "agent"}}
	draft.Validation = ValidationReport{Warnings: []ValidationIssue{{Code: "assumption_present", Message: "存在假设"}}}
	body, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	var decoded WorkflowDraft
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Facts) != 1 || len(decoded.Assumptions) != 1 || len(decoded.Validation.Warnings) != 1 {
		t.Fatalf("round trip lost fields: %#v", decoded)
	}
}

func validDraft() WorkflowDraft {
	return WorkflowDraft{
		DraftID: "draft-1",
		Goal:    "读取 article.md",
		Definition: workflow.WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []workflow.TaskDefinition{{
			Key: "read", Action: "read-document", Input: map[string]any{"source": "article.md"}, TimeoutMillis: 1000,
		}}},
		Status: DraftGenerated,
	}
}
