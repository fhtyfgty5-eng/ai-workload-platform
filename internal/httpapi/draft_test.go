package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestDraftCreateRequiresOperatorAndReturnsDraft(t *testing.T) {
	draft := testDraft()
	service := &fakeDraftService{draft: draft}
	handler := NewHandler(Dependencies{
		Drafts:        service,
		ViewerToken:   "viewer",
		OperatorToken: "operator",
		Ready:         func() bool { return true },
	})

	viewer := httptest.NewRequest(http.MethodPost, "/api/v1/agent/drafts", strings.NewReader(`{"goal":"goal"}`))
	viewer.Header.Set("Authorization", "Bearer viewer")
	viewerResponse := httptest.NewRecorder()
	handler.ServeHTTP(viewerResponse, viewer)
	if viewerResponse.Code != http.StatusForbidden {
		t.Fatalf("viewer status = %d, want 403", viewerResponse.Code)
	}

	operator := httptest.NewRequest(http.MethodPost, "/api/v1/agent/drafts", strings.NewReader(`{"goal":"goal"}`))
	operator.Header.Set("Authorization", "Bearer operator")
	operatorResponse := httptest.NewRecorder()
	handler.ServeHTTP(operatorResponse, operator)
	if operatorResponse.Code != http.StatusCreated {
		t.Fatalf("operator status = %d, want 201", operatorResponse.Code)
	}
	var got agent.WorkflowDraft
	if err := json.NewDecoder(operatorResponse.Body).Decode(&got); err != nil {
		t.Fatalf("decode draft: %v", err)
	}
	if got.DraftID != draft.DraftID || service.generateCalls != 1 {
		t.Fatalf("draft/calls = %q/%d, want %q/1", got.DraftID, service.generateCalls, draft.DraftID)
	}
}

func TestDraftValidateRejectsMismatchedPathID(t *testing.T) {
	service := &fakeDraftService{}
	handler := NewHandler(Dependencies{Drafts: service, ViewerToken: "viewer", OperatorToken: "operator", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/drafts/path-id/validate", strings.NewReader(`{"draft":{"draft_id":"body-id"}}`))
	request.Header.Set("Authorization", "Bearer operator")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	if service.validateCalls != 0 {
		t.Fatalf("validate calls = %d, want 0", service.validateCalls)
	}
}

func TestDraftConfirmMapsChangedHashToConflict(t *testing.T) {
	service := &fakeDraftService{confirmErr: &agent.Error{Code: agent.CodeDraftChanged, Message: "draft content changed"}}
	handler := NewHandler(Dependencies{Drafts: service, ViewerToken: "viewer", OperatorToken: "operator", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/drafts/draft-1/confirm", strings.NewReader(`{"draft":{"draft_id":"draft-1"},"content_hash":"wrong"}`))
	request.Header.Set("Authorization", "Bearer operator")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"code":"draft_changed"`) {
		t.Fatalf("response = %s, want draft_changed", response.Body.String())
	}
}

func TestDraftConfirmReturnsDefinitionWithoutPersisting(t *testing.T) {
	definition := testDraft().Definition
	service := &fakeDraftService{definition: definition}
	handler := NewHandler(Dependencies{Drafts: service, ViewerToken: "viewer", OperatorToken: "operator", Ready: func() bool { return true }})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/drafts/draft-1/confirm", strings.NewReader(`{"draft":{"draft_id":"draft-1"},"content_hash":"hash"}`))
	request.Header.Set("Authorization", "Bearer operator")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	var got workflow.WorkflowDefinition
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode definition: %v", err)
	}
	if got.ID != definition.ID || service.confirmCalls != 1 {
		t.Fatalf("definition/calls = %q/%d, want %q/1", got.ID, service.confirmCalls, definition.ID)
	}
}

type fakeDraftService struct {
	draft         agent.WorkflowDraft
	definition    workflow.WorkflowDefinition
	generateErr   error
	validateErr   error
	confirmErr    error
	generateCalls int
	validateCalls int
	confirmCalls  int
}

func (f *fakeDraftService) GenerateDraft(context.Context, string) (agent.WorkflowDraft, error) {
	f.generateCalls++
	return f.draft, f.generateErr
}

func (f *fakeDraftService) ValidateDraft(context.Context, agent.WorkflowDraft) (agent.WorkflowDraft, error) {
	f.validateCalls++
	if f.validateErr != nil {
		return agent.WorkflowDraft{}, f.validateErr
	}
	return f.draft, nil
}

func (f *fakeDraftService) ConfirmDraft(context.Context, agent.WorkflowDraft, string) (workflow.WorkflowDefinition, error) {
	f.confirmCalls++
	if f.confirmErr != nil {
		return workflow.WorkflowDefinition{}, f.confirmErr
	}
	return f.definition, nil
}

func testDraft() agent.WorkflowDraft {
	return agent.WorkflowDraft{
		DraftID:     "draft-1",
		Goal:        "goal",
		Definition:  workflow.WorkflowDefinition{ID: "demo", Concurrency: 1, Tasks: []workflow.TaskDefinition{{Key: "one", Action: "noop", TimeoutMillis: 1000}}},
		Status:      agent.DraftNeedsConfirmation,
		ContentHash: "hash",
	}
}
