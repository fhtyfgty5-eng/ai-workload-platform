package agent_test

import (
	"context"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/catalog"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestValidatorRejectsUnknownAction(t *testing.T) {
	draft := validDraft()
	draft.Definition.Tasks[0].Action = "unknown"
	assertValidationCode(t, validate(t, draft), "unknown_action")
}

func TestValidatorRejectsMissingRequiredInput(t *testing.T) {
	draft := validDraft()
	delete(draft.Definition.Tasks[0].Input, "source")
	assertValidationCode(t, validate(t, draft), "required_input_missing")
}

func TestValidatorRejectsInputTypeAndEnumMismatch(t *testing.T) {
	draft := validDraft()
	draft.Definition.Tasks = append(draft.Definition.Tasks, workflow.TaskDefinition{
		Key: "clean", Action: "clean-document", Input: map[string]any{"mode": "unsupported"}, DependsOn: []workflow.TaskKey{"read"}, TimeoutMillis: 1000,
	})
	assertValidationCode(t, validate(t, draft), "input_enum_mismatch")
}

func TestValidatorRejectsOpenQuestion(t *testing.T) {
	draft := validDraft()
	draft.Questions = []agent.Question{{ID: "source", Text: "使用哪个文档？"}}
	assertValidationCode(t, validate(t, draft), "question_unresolved")
}

func TestValidatorRejectsTimeoutAndPermissionOverflow(t *testing.T) {
	draft := validDraft()
	draft.Definition.Tasks[0].TimeoutMillis = 60_000
	report, err := newValidator(t, nil).Validate(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	assertValidationCode(t, report, "permission_missing")
	assertValidationCode(t, report, "timeout_limit_exceeded")
}

func TestValidatorRejectsAgentWorkflowConcurrencyAboveSafeLimit(t *testing.T) {
	draft := validDraft()
	draft.Definition.Concurrency = agent.MaxWorkflowConcurrency + 1
	assertValidationCode(t, validate(t, draft), "concurrency_limit_exceeded")
}

func TestValidatorDelegatesDAGRulesToWorkflowCompile(t *testing.T) {
	draft := validDraft()
	draft.Definition.Tasks[0].DependsOn = []workflow.TaskKey{"read"}
	assertValidationCode(t, validate(t, draft), "workflow_definition_invalid")
}

func TestValidatorReturnsWarningsForAgentAssumptions(t *testing.T) {
	draft := validDraft()
	draft.Assumptions = []agent.Evidence{{Statement: "默认使用 article.md", Source: "agent"}}
	report := validate(t, draft)
	if len(report.Warnings) != 1 || report.Warnings[0].Code != "assumption_present" {
		t.Fatalf("Warnings = %#v, want assumption_present", report.Warnings)
	}
}

func TestValidatorRejectsMissingCatalogWithoutPanicking(t *testing.T) {
	validator := agent.NewDraftValidator(nil, nil)
	_, err := validator.Validate(context.Background(), validDraft())
	if agent.CodeOf(err) != agent.CodeDraftInvalid {
		t.Fatalf("Validate() error = %v, want draft_invalid", err)
	}
}

func validDraft() agent.WorkflowDraft {
	return agent.WorkflowDraft{DraftID: "draft-1", Goal: "读取 article.md", Status: agent.DraftGenerated,
		Definition: workflow.WorkflowDefinition{ID: "document-pipeline", Concurrency: 1, Tasks: []workflow.TaskDefinition{{
			Key: "read", Action: "read-document", Input: map[string]any{"source": "article.md"}, TimeoutMillis: 1000,
		}}}}
}

func newValidator(t *testing.T, permissions []string) *agent.DraftValidator {
	t.Helper()
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	return agent.NewDraftValidator(directory, permissions)
}

func validate(t *testing.T, draft agent.WorkflowDraft) agent.ValidationReport {
	t.Helper()
	report, err := newValidator(t, []string{"document:read"}).Validate(context.Background(), draft)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func assertValidationCode(t *testing.T, report agent.ValidationReport, code string) {
	t.Helper()
	for _, issue := range report.Errors {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("validation errors = %#v, want code %q", report.Errors, code)
}
