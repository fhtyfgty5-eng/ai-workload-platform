package agent_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/catalog"
	"github.com/fhtyfgty5-eng/ai-workload-platform/agent/mockmodel"
)

func TestRuntimeExecutesOnlyRegisteredCatalogTool(t *testing.T) {
	service, audit := newService(t, mockmodel.New(), agent.DefaultLimits())
	draft, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.ToolCalls) != 1 || draft.ToolCalls[0].Name != "workflow_catalog_query" || draft.ToolCalls[0].Result != "allowed" {
		t.Fatalf("ToolCalls = %#v, want one allowed catalog query", draft.ToolCalls)
	}
	if len(audit.Events()) < 3 {
		t.Fatalf("audit events = %d, want generation and tool events", len(audit.Events()))
	}
}

func TestRuntimeStopsAfterModelTurnBudget(t *testing.T) {
	model := &loopModel{call: agent.ModelToolCall{ID: "call", Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":""}`)}}
	limits := agent.DefaultLimits()
	limits.MaxModelTurns = 1
	service, _ := newService(t, model, limits)
	_, err := service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeBudgetExceeded || model.calls != 1 {
		t.Fatalf("GenerateDraft() error = %v calls = %d, want budget after one call", err, model.calls)
	}
}

func TestRuntimeStopsAfterToolBudget(t *testing.T) {
	model := &loopModel{call: agent.ModelToolCall{ID: "call", Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":""}`)}}
	limits := agent.DefaultLimits()
	limits.MaxToolCalls = 1
	service, _ := newService(t, model, limits)
	_, err := service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeBudgetExceeded {
		t.Fatalf("GenerateDraft() error = %v, want budget_exceeded", err)
	}
}

func TestRuntimeRejectsMalformedModelDraft(t *testing.T) {
	service, _ := newService(t, fixedModel{response: agent.ModelResponse{DraftJSON: json.RawMessage(`{"unknown":true}`)}}, agent.DefaultLimits())
	_, err := service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeModelInvalidResponse {
		t.Fatalf("GenerateDraft() error = %v, want model_invalid_response", err)
	}
}

func TestRuntimeOverwritesModelSuppliedLifecycleFields(t *testing.T) {
	forged := validDraft()
	confirmedAt := time.Now()
	forged.Status = agent.DraftConfirmed
	forged.Validation = agent.ValidationReport{Errors: []agent.ValidationIssue{{Code: "forged"}}}
	forged.ConfirmedAt = &confirmedAt
	body, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	service, _ := newService(t, fixedModel{response: agent.ModelResponse{DraftJSON: body}}, agent.DefaultLimits())
	draft, err := service.GenerateDraft(context.Background(), "goal")
	if err != nil {
		t.Fatal(err)
	}
	if draft.Status != agent.DraftGenerated || len(draft.Validation.Errors) != 0 || len(draft.Validation.Warnings) != 0 || draft.ConfirmedAt != nil {
		t.Fatalf("generated lifecycle fields = status %s validation %#v confirmed_at %v", draft.Status, draft.Validation, draft.ConfirmedAt)
	}
}

func TestRuntimeRejectsIncompleteOrMalformedToolCalls(t *testing.T) {
	tests := []struct {
		name string
		call agent.ModelToolCall
	}{
		{name: "missing call ID", call: agent.ModelToolCall{Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":""}`)}},
		{name: "missing tool name", call: agent.ModelToolCall{ID: "call-1", Arguments: json.RawMessage(`{"query":""}`)}},
		{name: "invalid arguments", call: agent.ModelToolCall{ID: "call-1", Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":`)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, _ := newService(t, fixedModel{response: agent.ModelResponse{ToolCalls: []agent.ModelToolCall{test.call}}}, agent.DefaultLimits())
			_, err := service.GenerateDraft(context.Background(), "goal")
			if agent.CodeOf(err) != agent.CodeModelInvalidResponse {
				t.Fatalf("GenerateDraft() error = %v, want model_invalid_response", err)
			}
		})
	}
}

func TestRuntimeRejectsDuplicateToolCallIDsBeforeInvocation(t *testing.T) {
	tool := &countingTool{name: "count"}
	registry, err := agent.NewToolRegistry(agent.RegisteredTool{Tool: tool})
	if err != nil {
		t.Fatal(err)
	}
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	response := agent.ModelResponse{ToolCalls: []agent.ModelToolCall{
		{ID: "duplicate", Name: "count", Arguments: json.RawMessage(`{}`)},
		{ID: "duplicate", Name: "count", Arguments: json.RawMessage(`{}`)},
	}}
	service, err := agent.NewService(fixedModel{response: response}, registry, agent.NewDraftValidator(directory, nil), agent.NewMemoryAuditSink(), agent.DefaultLimits(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeModelInvalidResponse || tool.calls != 0 {
		t.Fatalf("GenerateDraft() error = %v tool calls = %d, want rejection before invocation", err, tool.calls)
	}
}

func TestRuntimeCancellationStopsFurtherModelAndToolCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	model := &loopModel{}
	service, _ := newService(t, model, agent.DefaultLimits())
	_, err := service.GenerateDraft(ctx, "goal")
	if agent.CodeOf(err) != agent.CodeCanceled || model.calls != 0 {
		t.Fatalf("GenerateDraft() error = %v calls = %d, want canceled before call", err, model.calls)
	}
}

func TestRuntimeCancellationBetweenToolCallsStopsNewInvocations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	second := &countingTool{name: "second"}
	registry, err := agent.NewToolRegistry(
		agent.RegisteredTool{Tool: cancelingTool{cancel: cancel}},
		agent.RegisteredTool{Tool: second},
	)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	modelCalls := []agent.ModelToolCall{
		{ID: "cancel-1", Name: "cancel", Arguments: json.RawMessage(`{}`)},
		{ID: "second-1", Name: "second", Arguments: json.RawMessage(`{}`)},
	}
	model := &countingResponseModel{response: agent.ModelResponse{ToolCalls: modelCalls}}
	audit := agent.NewMemoryAuditSink()
	service, err := agent.NewService(model, registry, agent.NewDraftValidator(directory, []string{"document:read"}), audit, agent.DefaultLimits(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.GenerateDraft(ctx, "goal")
	if agent.CodeOf(err) != agent.CodeCanceled {
		t.Fatalf("GenerateDraft() error = %v, want canceled", err)
	}
	if second.calls != 0 || model.calls != 1 {
		t.Fatalf("second tool calls = %d model calls = %d, want only the initial model invocation", second.calls, model.calls)
	}
	assertAuditResult(t, audit.Events(), "draft.generation_failed", agent.CodeCanceled)
}

func TestRuntimeAuditsBudgetFailureWithUsage(t *testing.T) {
	model := &loopModel{call: agent.ModelToolCall{ID: "call", Name: "workflow_catalog_query", Arguments: json.RawMessage(`{"query":""}`)}}
	limits := agent.DefaultLimits()
	limits.MaxModelTurns = 1
	service, audit := newService(t, model, limits)
	_, err := service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeBudgetExceeded {
		t.Fatalf("GenerateDraft() error = %v, want budget_exceeded", err)
	}
	events := audit.Events()
	assertAuditResult(t, events, "draft.generation_failed", agent.CodeBudgetExceeded)
	last := events[len(events)-1]
	if last.Metadata["model_turns"] != 1 || last.Metadata["tool_calls"] != 1 {
		t.Fatalf("failure metadata = %#v, want one model turn and one tool call", last.Metadata)
	}
}

func assertAuditResult(t *testing.T, events []agent.AuditEvent, eventType string, code agent.ErrorCode) {
	t.Helper()
	for _, event := range events {
		if event.Type == eventType && event.Result == string(code) {
			return
		}
	}
	t.Fatalf("audit events = %#v, want %s result %s", events, eventType, code)
}

func TestRuntimeRecordsModelAndToolAuditEvents(t *testing.T) {
	service, audit := newService(t, mockmodel.New(), agent.DefaultLimits())
	if _, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要"); err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, event := range audit.Events() {
		types[event.Type] = true
	}
	for _, want := range []string{"draft.generation_started", "model.completed", "tool.allowed", "draft.generated"} {
		if !types[want] {
			t.Fatalf("audit types = %#v, missing %s", types, want)
		}
	}
	for _, event := range audit.Events() {
		if event.Type == "draft.generated" && (event.Metadata["model_turns"] != 2 || event.Metadata["tool_calls"] != 1) {
			t.Fatalf("draft.generated metadata = %#v, want usage snapshot", event.Metadata)
		}
	}
}

func TestRuntimePassesRemainingLimitsToModel(t *testing.T) {
	model := &capturingModel{next: mockmodel.New()}
	service, _ := newService(t, model, agent.DefaultLimits())
	if _, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要"); err != nil {
		t.Fatal(err)
	}
	if len(model.requests) != 2 {
		t.Fatalf("model requests = %d, want two", len(model.requests))
	}
	first, second := model.requests[0].Limits, model.requests[1].Limits
	if first.MaxResponseBytes != 64*1024 || first.RemainingModelTurns != 3 || first.RemainingToolCalls != 8 {
		t.Fatalf("first limits = %+v", first)
	}
	if second.RemainingModelTurns != 2 || second.RemainingToolCalls != 7 {
		t.Fatalf("second limits = %+v", second)
	}
}

func TestRuntimeGeneratesIndependentDraftIDs(t *testing.T) {
	service, _ := newService(t, mockmodel.New(), agent.DefaultLimits())
	first, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要")
	if err != nil {
		t.Fatal(err)
	}
	if first.DraftID == second.DraftID {
		t.Fatalf("duplicate DraftID %q", first.DraftID)
	}
}

func TestRuntimeRetriesOneTemporaryModelFailure(t *testing.T) {
	model := &temporaryThenMockModel{next: mockmodel.New()}
	service, _ := newService(t, model, agent.DefaultLimits())
	if _, err := service.GenerateDraft(context.Background(), "先读取 article.md，再清洗内容，最后生成摘要"); err != nil {
		t.Fatal(err)
	}
	if model.calls != 3 {
		t.Fatalf("model calls = %d, want one failed call plus two Mock calls", model.calls)
	}
}

func TestRuntimeDoesNotRetryPermanentModelFailure(t *testing.T) {
	model := &errorModel{err: &agent.Error{Code: agent.CodeModelInvalidResponse, Message: "bad"}}
	service, _ := newService(t, model, agent.DefaultLimits())
	_, _ = service.GenerateDraft(context.Background(), "goal")
	if model.calls != 1 {
		t.Fatalf("model calls = %d, want one", model.calls)
	}
}

func TestRuntimeMapsToolDeadlineToToolTimeout(t *testing.T) {
	limits := agent.DefaultLimits()
	limits.ToolTimeout = 10 * time.Millisecond
	registry, err := agent.NewToolRegistry(agent.RegisteredTool{Tool: blockingTool{}, RequiredPermission: "catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	directory, _ := catalog.New(catalog.DefaultTemplates())
	service, err := agent.NewService(&loopModel{call: agent.ModelToolCall{ID: "call", Name: "blocking", Arguments: json.RawMessage(`{}`)}}, registry, agent.NewDraftValidator(directory, []string{"document:read"}), agent.NewMemoryAuditSink(), limits, []string{"catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.GenerateDraft(context.Background(), "goal")
	if agent.CodeOf(err) != agent.CodeToolTimeout {
		t.Fatalf("GenerateDraft() error = %v, want tool_timeout", err)
	}
}

func newService(t *testing.T, model agent.ModelAdapter, limits agent.Limits) (*agent.Service, *agent.MemoryAuditSink) {
	t.Helper()
	directory, err := catalog.New(catalog.DefaultTemplates())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := agent.NewToolRegistry(agent.RegisteredTool{Tool: catalog.NewTool(directory), RequiredPermission: "catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	audit := agent.NewMemoryAuditSink()
	service, err := agent.NewService(model, registry, agent.NewDraftValidator(directory, []string{"document:read"}), audit, limits, []string{"catalog:read"})
	if err != nil {
		t.Fatal(err)
	}
	return service, audit
}

type fixedModel struct{ response agent.ModelResponse }

func (m fixedModel) Generate(context.Context, agent.ModelRequest) (agent.ModelResponse, error) {
	return m.response, nil
}

type countingResponseModel struct {
	response agent.ModelResponse
	calls    int
}

func (m *countingResponseModel) Generate(context.Context, agent.ModelRequest) (agent.ModelResponse, error) {
	m.calls++
	return m.response, nil
}

type loopModel struct {
	mu    sync.Mutex
	calls int
	call  agent.ModelToolCall
}

func (m *loopModel) Generate(context.Context, agent.ModelRequest) (agent.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return agent.ModelResponse{ToolCalls: []agent.ModelToolCall{m.call}}, nil
}

type temporaryThenMockModel struct {
	calls int
	next  agent.ModelAdapter
}

func (m *temporaryThenMockModel) Generate(ctx context.Context, request agent.ModelRequest) (agent.ModelResponse, error) {
	m.calls++
	if m.calls == 1 {
		return agent.ModelResponse{}, &agent.Error{Code: agent.CodeModelUnavailable, Message: "temporary", Temporary: true}
	}
	return m.next.Generate(ctx, request)
}

type errorModel struct {
	calls int
	err   error
}

type capturingModel struct {
	next     agent.ModelAdapter
	requests []agent.ModelRequest
}

func (m *capturingModel) Generate(ctx context.Context, request agent.ModelRequest) (agent.ModelResponse, error) {
	m.requests = append(m.requests, request)
	return m.next.Generate(ctx, request)
}

func (m *errorModel) Generate(context.Context, agent.ModelRequest) (agent.ModelResponse, error) {
	m.calls++
	return agent.ModelResponse{}, m.err
}

type blockingTool struct{}

func (blockingTool) Name() string                 { return "blocking" }
func (blockingTool) Description() string          { return "block until timeout" }
func (blockingTool) InputSchema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (blockingTool) Invoke(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type cancelingTool struct{ cancel context.CancelFunc }

func (cancelingTool) Name() string                 { return "cancel" }
func (cancelingTool) Description() string          { return "cancel runtime" }
func (cancelingTool) InputSchema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (t cancelingTool) Invoke(context.Context, json.RawMessage) (json.RawMessage, error) {
	t.cancel()
	return json.RawMessage(`{}`), nil
}

type countingTool struct {
	name  string
	calls int
}

func (t *countingTool) Name() string               { return t.name }
func (*countingTool) Description() string          { return "count calls" }
func (*countingTool) InputSchema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (t *countingTool) Invoke(context.Context, json.RawMessage) (json.RawMessage, error) {
	t.calls++
	return json.RawMessage(`{}`), nil
}
