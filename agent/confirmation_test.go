package agent_test

import (
	"context"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/mockmodel"
)

func TestConfirmDraftRequiresMatchingHash(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft := generatedAndValidatedDraft(t, service)
	draft.Definition.Tasks[0].Input["source"] = "changed.md"
	_, err := service.ConfirmDraft(context.Background(), draft, draft.ContentHash)
	if agent.CodeOf(err) != agent.CodeDraftChanged {
		t.Fatalf("ConfirmDraft() error = %v, want draft_changed", err)
	}
}

func TestConfirmDraftRejectsUnvalidatedDraft(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ConfirmDraft(context.Background(), draft, draft.ContentHash)
	if agent.CodeOf(err) != agent.CodeApprovalRequired {
		t.Fatalf("ConfirmDraft() error = %v, want approval_required", err)
	}
}

func TestConfirmDraftRejectsOpenQuestions(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft := generatedAndValidatedDraft(t, service)
	draft.Questions = []agent.Question{{ID: "source", Text: "使用哪个文档？"}}
	draft.ContentHash, _ = agent.ComputeDraftHash(draft)
	_, err := service.ConfirmDraft(context.Background(), draft, draft.ContentHash)
	if agent.CodeOf(err) != agent.CodeApprovalRequired {
		t.Fatalf("ConfirmDraft() error = %v, want approval_required", err)
	}
}

func TestConfirmDraftReturnsCompiledDefinitionWithoutStartingRun(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft := generatedAndValidatedDraft(t, service)
	definition, err := service.ConfirmDraft(context.Background(), draft, draft.ContentHash)
	if err != nil {
		t.Fatal(err)
	}
	if definition.ID != "agent-document-pipeline" || len(definition.Tasks) != 3 {
		t.Fatalf("definition = %#v, want three-task pipeline", definition)
	}
}

func TestConfirmDraftRecordsApprovalAudit(t *testing.T) {
	service, audit := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft := generatedAndValidatedDraft(t, service)
	if _, err := service.ConfirmDraft(context.Background(), draft, draft.ContentHash); err != nil {
		t.Fatal(err)
	}
	for _, event := range audit.Events() {
		if event.Type == "draft.confirmed" && event.DraftID == draft.DraftID {
			return
		}
	}
	t.Fatal("draft.confirmed audit event not found")
}

func TestDraftCanBeRevalidatedAndConfirmedAfterQuestionIsResolved(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft := validDraft()
	draft.Questions = []agent.Question{{ID: "format", Text: "使用哪种格式？"}}
	validated, err := service.ValidateDraft(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Status != agent.DraftValidated || len(validated.Validation.Errors) == 0 {
		t.Fatalf("first validation = %#v, want validated with unresolved question", validated)
	}
	validated.Questions[0].Answer = "markdown"
	validated.Questions[0].Resolved = true
	revalidated, err := service.ValidateDraft(context.Background(), validated)
	if err != nil {
		t.Fatal(err)
	}
	if revalidated.Status != agent.DraftNeedsConfirmation || len(revalidated.Validation.Errors) != 0 {
		t.Fatalf("second validation = %#v, want needs_confirmation without errors", revalidated)
	}
	if _, err := service.ConfirmDraft(context.Background(), revalidated, revalidated.ContentHash); err != nil {
		t.Fatalf("ConfirmDraft() error = %v", err)
	}
}

func generatedAndValidatedDraft(t *testing.T, service *agent.Service) agent.WorkflowDraft {
	t.Helper()
	draft, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatal(err)
	}
	draft, err = service.ValidateDraft(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.Validation.Errors) != 0 || draft.Status != agent.DraftNeedsConfirmation {
		t.Fatalf("validated draft = %#v, want needs_confirmation without errors", draft)
	}
	return draft
}
