package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// DraftStatus 表示草稿从生成到确认或拒绝的生命周期状态。
type DraftStatus string

const (
	DraftGenerated         DraftStatus = "generated"
	DraftValidated         DraftStatus = "validated"
	DraftNeedsConfirmation DraftStatus = "needs_confirmation"
	DraftConfirmed         DraftStatus = "confirmed"
	DraftRejected          DraftStatus = "rejected"
)

// Evidence 记录一条陈述及其来源，避免混淆用户事实和 Agent 假设。
type Evidence struct {
	Statement string `json:"statement"`
	Source    string `json:"source"`
}

// Question 表示必须由用户补全或确认的未决信息。
type Question struct {
	ID       string `json:"id"`
	Text     string `json:"text"`
	Answer   string `json:"answer,omitempty"`
	Resolved bool   `json:"resolved"`
}

// ValidationIssue 是可由机器码和字段路径稳定定位的校验问题。
type ValidationIssue struct {
	Code    string `json:"code"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

// ValidationReport 分开保存阻塞确认的错误和仅需审核的警告。
type ValidationReport struct {
	Errors   []ValidationIssue `json:"errors"`
	Warnings []ValidationIssue `json:"warnings"`
}

// ToolCallRecord 保存进入当前草稿的非敏感工具调用结果摘要。
type ToolCallRecord struct {
	CallID string `json:"call_id"`
	Name   string `json:"name"`
	Result string `json:"result"`
}

// WorkflowDraft 是模型建议、平台校验和用户确认共同使用的唯一审核对象。
type WorkflowDraft struct {
	DraftID     string                      `json:"draft_id"`
	Goal        string                      `json:"goal"`
	Definition  workflow.WorkflowDefinition `json:"definition"`
	Facts       []Evidence                  `json:"facts"`
	Assumptions []Evidence                  `json:"assumptions"`
	Questions   []Question                  `json:"questions"`
	Validation  ValidationReport            `json:"validation"`
	ToolCalls   []ToolCallRecord            `json:"tool_calls"`
	Status      DraftStatus                 `json:"status"`
	ContentHash string                      `json:"content_hash"`
	CreatedAt   time.Time                   `json:"created_at"`
	ConfirmedAt *time.Time                  `json:"confirmed_at,omitempty"`
}

// ComputeDraftHash 对用户实际审核的内容计算 SHA-256；时间戳等生命周期字段不参与哈希。
func ComputeDraftHash(draft WorkflowDraft) (string, error) {
	reviewed := struct {
		DraftID     string                      `json:"draft_id"`
		Goal        string                      `json:"goal"`
		Definition  workflow.WorkflowDefinition `json:"definition"`
		Facts       []Evidence                  `json:"facts"`
		Assumptions []Evidence                  `json:"assumptions"`
		Questions   []Question                  `json:"questions"`
		Validation  ValidationReport            `json:"validation"`
		ToolCalls   []ToolCallRecord            `json:"tool_calls"`
		Status      DraftStatus                 `json:"status"`
	}{
		DraftID:     draft.DraftID,
		Goal:        draft.Goal,
		Definition:  draft.Definition,
		Facts:       draft.Facts,
		Assumptions: draft.Assumptions,
		Questions:   draft.Questions,
		Validation:  draft.Validation,
		ToolCalls:   draft.ToolCalls,
		Status:      draft.Status,
	}
	body, err := json.Marshal(reviewed)
	if err != nil {
		return "", fmt.Errorf("encode draft hash input: %w", err)
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

// TransitionDraft 校验并执行一次草稿状态转换。
func TransitionDraft(draft *WorkflowDraft, next DraftStatus) error {
	if draft == nil {
		return &Error{Code: CodeDraftInvalid, Message: "draft is required"}
	}
	allowed := map[DraftStatus]map[DraftStatus]bool{
		DraftGenerated:         {DraftValidated: true, DraftRejected: true},
		DraftValidated:         {DraftValidated: true, DraftNeedsConfirmation: true, DraftRejected: true},
		DraftNeedsConfirmation: {DraftValidated: true, DraftConfirmed: true, DraftRejected: true},
	}
	if !allowed[draft.Status][next] {
		return &Error{Code: CodeDraftInvalid, Message: fmt.Sprintf("cannot transition draft from %s to %s", draft.Status, next)}
	}
	if next == DraftConfirmed {
		for _, question := range draft.Questions {
			if !question.Resolved {
				return &Error{Code: CodeApprovalRequired, Message: "draft contains unresolved questions"}
			}
		}
	}
	draft.Status = next
	return nil
}
