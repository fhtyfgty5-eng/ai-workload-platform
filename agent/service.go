package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Service 协调模型、受控工具、校验器、预算和审计，不直接访问控制面。
type Service struct {
	// model 只负责生成协议响应；tools 和 validator 分别约束模型可调用的能力与最终草稿。
	model     ModelAdapter
	tools     *ToolRegistry
	validator *DraftValidator
	// audit 记录非敏感运行事件；limits 为每次 GenerateDraft 创建独立预算。
	audit  AuditSink
	limits Limits
	// permissions 是 Service 创建时取得的权限快照，工具调用不会接受模型自行声明权限。
	permissions []string
	// now 允许测试固定审计和确认时间，生产环境使用 time.Now。
	now func() time.Time
}

// NewService 创建只依赖显式模型、工具、校验器和审计接收器的 Agent 服务。
func NewService(model ModelAdapter, tools *ToolRegistry, validator *DraftValidator, audit AuditSink, limits Limits, permissions []string) (*Service, error) {
	if model == nil || tools == nil || validator == nil || audit == nil {
		return nil, fmt.Errorf("model, tools, validator and audit are required")
	}
	if limits.MaxModelTurns <= 0 || limits.MaxToolCalls <= 0 || limits.MaxResponseBytes <= 0 || limits.RuntimeTimeout <= 0 || limits.ToolTimeout <= 0 {
		return nil, fmt.Errorf("all runtime limits must be positive")
	}
	return &Service{model: model, tools: tools, validator: validator, audit: audit, limits: limits, permissions: append([]string(nil), permissions...), now: time.Now}, nil
}

// GenerateDraft 运行受预算和取消约束的模型工具循环，并返回未校验草稿。
func (s *Service) GenerateDraft(parent context.Context, goal string) (result WorkflowDraft, resultErr error) {
	budget := NewBudget(s.limits)
	defer func() {
		if resultErr == nil {
			return
		}
		modelTurns, toolCalls := budget.Usage()
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), time.Second)
		defer cancel()
		_ = s.record(auditCtx, AuditEvent{
			Type: "draft.generation_failed", At: s.now(), Result: string(CodeOf(resultErr)),
			Metadata: map[string]any{"model_turns": modelTurns, "tool_calls": toolCalls},
		})
	}()
	if err := parent.Err(); err != nil {
		return WorkflowDraft{}, canceledError(err)
	}
	if goal == "" {
		return WorkflowDraft{}, &Error{Code: CodeDraftInvalid, Message: "goal is required"}
	}
	ctx, cancel := budget.Context(parent)
	defer cancel()
	if err := s.record(ctx, AuditEvent{Type: "draft.generation_started", At: s.now()}); err != nil {
		return WorkflowDraft{}, mapContextError(ctx, err)
	}
	request := ModelRequest{Goal: goal, Tools: s.tools.Summaries()}
	records := []ToolCallRecord{}
	for {
		if err := ctx.Err(); err != nil {
			return WorkflowDraft{}, mapContextError(ctx, err)
		}
		if err := budget.UseModelTurn(); err != nil {
			return WorkflowDraft{}, err
		}
		request.Limits = budget.ModelLimits()
		response, err := s.model.Generate(ctx, request)
		if err != nil {
			var runtimeErr *Error
			if errors.As(err, &runtimeErr) && runtimeErr.Temporary {
				if err := budget.UseModelTurn(); err != nil {
					return WorkflowDraft{}, err
				}
				if recordErr := s.record(ctx, AuditEvent{Type: "model.retry", At: s.now(), Result: string(runtimeErr.Code)}); recordErr != nil {
					return WorkflowDraft{}, mapContextError(ctx, recordErr)
				}
				if err := ctx.Err(); err != nil {
					return WorkflowDraft{}, mapContextError(ctx, err)
				}
				request.Limits = budget.ModelLimits()
				response, err = s.model.Generate(ctx, request)
			}
			if err != nil {
				return WorkflowDraft{}, mapContextError(ctx, err)
			}
		}
		if err := ctx.Err(); err != nil {
			return WorkflowDraft{}, mapContextError(ctx, err)
		}
		if err := budget.CheckResponseSize(responseSize(response)); err != nil {
			return WorkflowDraft{}, err
		}
		if err := s.record(ctx, AuditEvent{Type: "model.completed", At: s.now(), Result: "success", Metadata: map[string]any{"model": response.Model}}); err != nil {
			return WorkflowDraft{}, mapContextError(ctx, err)
		}
		if len(response.ToolCalls) > 0 {
			// 先校验整批调用，防止同一响应中的后续坏数据造成前面的工具产生副作用。
			if err := validateToolCalls(response.ToolCalls); err != nil {
				return WorkflowDraft{}, err
			}
			exchange := ToolExchange{Calls: append([]ModelToolCall(nil), response.ToolCalls...)}
			for _, call := range response.ToolCalls {
				if err := ctx.Err(); err != nil {
					return WorkflowDraft{}, mapContextError(ctx, err)
				}
				if err := budget.UseToolCall(); err != nil {
					return WorkflowDraft{}, err
				}
				toolCtx, toolCancel := context.WithTimeout(ctx, s.limits.ToolTimeout)
				toolResponse, err := s.tools.Invoke(toolCtx, ToolRequest{Name: call.Name, Arguments: call.Arguments, Permissions: s.permissions})
				toolContextErr := toolCtx.Err()
				toolCancel()
				if ctx.Err() != nil {
					return WorkflowDraft{}, mapContextError(ctx, ctx.Err())
				}
				if err != nil {
					if errors.Is(toolContextErr, context.DeadlineExceeded) && ctx.Err() == nil {
						err = &Error{Code: CodeToolTimeout, Message: "tool call timed out", Cause: err}
					} else if ctx.Err() != nil {
						err = mapContextError(ctx, err)
					}
					_ = s.record(ctx, AuditEvent{Type: "tool.denied", At: s.now(), Result: string(CodeOf(err)), Metadata: map[string]any{"tool": call.Name}})
					return WorkflowDraft{}, err
				}
				if err := budget.CheckResponseSize(len(toolResponse.Content)); err != nil {
					return WorkflowDraft{}, err
				}
				result := ToolResult{CallID: call.ID, Name: call.Name, Content: toolResponse.Content}
				request.ToolResults = append(request.ToolResults, result)
				exchange.Results = append(exchange.Results, result)
				records = append(records, ToolCallRecord{CallID: call.ID, Name: call.Name, Result: "allowed"})
				if err := s.record(ctx, AuditEvent{Type: "tool.allowed", At: s.now(), Result: "success", Metadata: map[string]any{"tool": call.Name}}); err != nil {
					return WorkflowDraft{}, mapContextError(ctx, err)
				}
			}
			request.ToolHistory = append(request.ToolHistory, exchange)
			continue
		}
		if len(response.DraftJSON) == 0 {
			return WorkflowDraft{}, &Error{Code: CodeModelInvalidResponse, Message: "model returned neither tool calls nor draft"}
		}
		draft, err := decodeDraft(response.DraftJSON)
		if err != nil {
			return WorkflowDraft{}, err
		}
		draft.Goal = goal
		draft.ToolCalls = records
		draft.Validation = ValidationReport{}
		draft.ConfirmedAt = nil
		draft.DraftID, err = newDraftID()
		if err != nil {
			return WorkflowDraft{}, &Error{Code: CodeInternal, Message: "generate draft ID", Cause: err}
		}
		draft.CreatedAt = s.now().UTC()
		draft.Status = DraftGenerated
		draft.ContentHash, err = ComputeDraftHash(draft)
		if err != nil {
			return WorkflowDraft{}, &Error{Code: CodeModelInvalidResponse, Message: "draft cannot be hashed", Cause: err}
		}
		modelTurns, toolCalls := budget.Usage()
		if err := s.record(ctx, AuditEvent{Type: "draft.generated", At: s.now(), DraftID: draft.DraftID, Result: "success", Metadata: map[string]any{
			"content_hash": draft.ContentHash,
			"model_turns":  modelTurns,
			"tool_calls":   toolCalls,
		}}); err != nil {
			return WorkflowDraft{}, mapContextError(ctx, err)
		}
		return draft, nil
	}
}

// ValidateDraft 使用静态目录和工作流编译器校验草稿，并刷新状态与内容哈希。
func (s *Service) ValidateDraft(ctx context.Context, draft WorkflowDraft) (WorkflowDraft, error) {
	report, err := s.validator.Validate(ctx, draft)
	if err != nil {
		return WorkflowDraft{}, err
	}
	draft.Validation = report
	if err := TransitionDraft(&draft, DraftValidated); err != nil {
		return WorkflowDraft{}, err
	}
	if len(report.Errors) == 0 {
		if err := TransitionDraft(&draft, DraftNeedsConfirmation); err != nil {
			return WorkflowDraft{}, err
		}
	}
	draft.ContentHash, err = ComputeDraftHash(draft)
	if err != nil {
		return WorkflowDraft{}, &Error{Code: CodeDraftInvalid, Message: "validated draft cannot be hashed", Cause: err}
	}
	if err := s.record(ctx, AuditEvent{Type: "draft.validated", At: s.now(), DraftID: draft.DraftID, Result: validationResult(report), Metadata: map[string]any{"content_hash": draft.ContentHash}}); err != nil {
		return WorkflowDraft{}, err
	}
	return draft, nil
}

// ConfirmDraft 重新校验用户审核的内容并只返回最终定义，不提交控制面或启动 Run。
func (s *Service) ConfirmDraft(ctx context.Context, draft WorkflowDraft, expectedHash string) (workflow.WorkflowDefinition, error) {
	if draft.Status != DraftNeedsConfirmation {
		return workflow.WorkflowDefinition{}, &Error{Code: CodeApprovalRequired, Message: "draft must be validated before confirmation"}
	}
	currentHash, err := ComputeDraftHash(draft)
	if err != nil {
		return workflow.WorkflowDefinition{}, &Error{Code: CodeDraftInvalid, Message: "draft cannot be hashed", Cause: err}
	}
	if expectedHash == "" || expectedHash != draft.ContentHash || expectedHash != currentHash {
		return workflow.WorkflowDefinition{}, &Error{Code: CodeDraftChanged, Message: "draft content changed after review"}
	}
	for _, question := range draft.Questions {
		if !question.Resolved {
			return workflow.WorkflowDefinition{}, &Error{Code: CodeApprovalRequired, Message: "draft contains unresolved questions"}
		}
	}
	report, err := s.validator.Validate(ctx, draft)
	if err != nil {
		return workflow.WorkflowDefinition{}, err
	}
	if len(report.Errors) > 0 {
		return workflow.WorkflowDefinition{}, &Error{Code: CodeDraftInvalid, Message: "draft no longer passes validation"}
	}
	compiled, err := workflow.Compile(draft.Definition)
	if err != nil {
		return workflow.WorkflowDefinition{}, &Error{Code: CodeDraftInvalid, Message: "confirmed definition cannot be compiled", Cause: err}
	}
	if err := TransitionDraft(&draft, DraftConfirmed); err != nil {
		return workflow.WorkflowDefinition{}, err
	}
	now := s.now().UTC()
	draft.ConfirmedAt = &now
	if err := s.record(ctx, AuditEvent{Type: "draft.confirmed", At: now, DraftID: draft.DraftID, Result: "success", Metadata: map[string]any{"content_hash": expectedHash}}); err != nil {
		return workflow.WorkflowDefinition{}, err
	}
	return compiled.Definition(), nil
}

func decodeDraft(body []byte) (WorkflowDraft, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var draft WorkflowDraft
	if err := decoder.Decode(&draft); err != nil {
		return WorkflowDraft{}, &Error{Code: CodeModelInvalidResponse, Message: "model draft is not valid structured JSON", Cause: err}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WorkflowDraft{}, &Error{Code: CodeModelInvalidResponse, Message: "model response contains trailing JSON"}
	}
	if draft.Definition.ID == "" || len(draft.Definition.Tasks) == 0 {
		return WorkflowDraft{}, &Error{Code: CodeModelInvalidResponse, Message: "model draft is missing workflow definition"}
	}
	return draft, nil
}

func newDraftID() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "draft_" + hex.EncodeToString(bytes), nil
}

func responseSize(response ModelResponse) int {
	size := len(response.DraftJSON)
	for _, call := range response.ToolCalls {
		size += len(call.ID) + len(call.Name) + len(call.Arguments)
	}
	return size
}

func validateToolCalls(calls []ModelToolCall) error {
	seen := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		callID := strings.TrimSpace(call.ID)
		if callID == "" || strings.TrimSpace(call.Name) == "" || !json.Valid(call.Arguments) {
			return &Error{Code: CodeModelInvalidResponse, Message: "model returned an incomplete or malformed tool call"}
		}
		if _, exists := seen[callID]; exists {
			return &Error{Code: CodeModelInvalidResponse, Message: "model returned duplicate tool call IDs"}
		}
		seen[callID] = struct{}{}
	}
	return nil
}

func (s *Service) record(ctx context.Context, event AuditEvent) error {
	event.Metadata = redactMetadata(event.Metadata)
	if err := s.audit.Record(ctx, event); err != nil {
		return &Error{Code: CodeInternal, Message: "record audit event", Cause: err}
	}
	return nil
}

func canceledError(err error) error {
	return &Error{Code: CodeCanceled, Message: "Agent Runtime canceled", Cause: err}
}

func mapContextError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: CodeModelTimeout, Message: "model call exceeded Runtime deadline", Cause: err}
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return canceledError(err)
	}
	return err
}

func validationResult(report ValidationReport) string {
	if len(report.Errors) > 0 {
		return "invalid"
	}
	return "valid"
}
