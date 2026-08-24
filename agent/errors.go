// Package agent 提供受控模型、工具和工作流草稿的运行时边界。
package agent

import (
	"errors"
	"fmt"
)

// ErrorCode 是 Agent Runtime 对调用方公开的稳定机器错误码。
type ErrorCode string

const (
	CodeModelTimeout         ErrorCode = "model_timeout"
	CodeModelUnavailable     ErrorCode = "model_unavailable"
	CodeModelInvalidResponse ErrorCode = "model_invalid_response"
	CodeToolNotAllowed       ErrorCode = "tool_not_allowed"
	CodeToolInvalidInput     ErrorCode = "tool_invalid_input"
	CodeToolTimeout          ErrorCode = "tool_timeout"
	CodeBudgetExceeded       ErrorCode = "budget_exceeded"
	CodeDraftInvalid         ErrorCode = "draft_invalid"
	CodeApprovalRequired     ErrorCode = "approval_required"
	CodeDraftChanged         ErrorCode = "draft_changed"
	CodeCanceled             ErrorCode = "canceled"
	CodeInternal             ErrorCode = "internal"
)

// Error 同时保存稳定错误码和面向人的非敏感说明。
type Error struct {
	Code      ErrorCode
	Message   string
	Temporary bool
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error { return e.Cause }

// CodeOf 提取 Agent 错误码；未知错误统一映射为 internal。
func CodeOf(err error) ErrorCode {
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		return runtimeErr.Code
	}
	return CodeInternal
}
