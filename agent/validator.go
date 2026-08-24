package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// TemplateCatalog 提供按 Action 查询只读任务模板的最小接口。
type TemplateCatalog interface {
	FindByAction(string) (TaskTemplate, bool)
}

// MaxWorkflowConcurrency 是自然语言草稿在单个 Run 内允许声明的最大并发 Attempt 数。
const MaxWorkflowConcurrency = 32

// DraftValidator 只读取草稿和目录，不调用模型、工具或控制面。
type DraftValidator struct {
	catalog     TemplateCatalog
	permissions []string
}

// NewDraftValidator 创建使用指定任务目录和权限集合的草稿校验器。
func NewDraftValidator(catalog TemplateCatalog, permissions []string) *DraftValidator {
	return &DraftValidator{catalog: catalog, permissions: append([]string(nil), permissions...)}
}

func (v *DraftValidator) Validate(ctx context.Context, draft WorkflowDraft) (ValidationReport, error) {
	if v == nil || v.catalog == nil {
		return ValidationReport{}, &Error{Code: CodeDraftInvalid, Message: "template catalog is required"}
	}
	if err := ctx.Err(); err != nil {
		return ValidationReport{}, &Error{Code: CodeCanceled, Message: "draft validation canceled", Cause: err}
	}
	report := ValidationReport{Errors: []ValidationIssue{}, Warnings: []ValidationIssue{}}
	if draft.DraftID == "" {
		report.Errors = append(report.Errors, issue("draft_id_required", "draft_id", "draft ID is required"))
	}
	if draft.Goal == "" {
		report.Errors = append(report.Errors, issue("goal_required", "goal", "goal is required"))
	}
	if draft.Definition.Concurrency > MaxWorkflowConcurrency || draft.Definition.Concurrency > len(draft.Definition.Tasks) {
		report.Errors = append(report.Errors, issue("concurrency_limit_exceeded", "definition.concurrency", "Agent workflow concurrency exceeds the safe limit"))
	}

	// DAG、标识符、任务数量和通用输入 JSON 边界继续由模块 1 的唯一编译入口判断。
	if _, err := workflow.Compile(draft.Definition); err != nil {
		report.Errors = append(report.Errors, issue("workflow_definition_invalid", "definition", err.Error()))
	}
	for index, task := range draft.Definition.Tasks {
		path := fmt.Sprintf("definition.tasks[%d]", index)
		template, found := v.catalog.FindByAction(task.Action)
		if !found || !template.AgentAllowed {
			report.Errors = append(report.Errors, issue("unknown_action", path+".action", "action is not available to Agent"))
			continue
		}
		if template.RequiredPermission != "" && !slices.Contains(v.permissions, template.RequiredPermission) {
			report.Errors = append(report.Errors, issue("permission_missing", path+".action", "required task permission is missing"))
		}
		if template.Resources.MaxTimeoutMillis > 0 && task.TimeoutMillis > template.Resources.MaxTimeoutMillis {
			report.Errors = append(report.Errors, issue("timeout_limit_exceeded", path+".timeout_ms", "task timeout exceeds template limit"))
		}
		validateInput(&report, path+".input", task.Input, template.Parameters)
	}
	for index, question := range draft.Questions {
		if !question.Resolved {
			report.Errors = append(report.Errors, issue("question_unresolved", fmt.Sprintf("questions[%d]", index), "question must be resolved before confirmation"))
		}
	}
	for index := range draft.Assumptions {
		report.Warnings = append(report.Warnings, issue("assumption_present", fmt.Sprintf("assumptions[%d]", index), "Agent assumption requires user review"))
	}
	return report, nil
}

func validateInput(report *ValidationReport, path string, input map[string]any, parameters map[string]ParameterSpec) {
	for name, spec := range parameters {
		value, exists := input[name]
		if spec.Required && !exists {
			report.Errors = append(report.Errors, issue("required_input_missing", path+"."+name, "required input is missing"))
			continue
		}
		if !exists {
			continue
		}
		if !matchesType(value, spec.Type) {
			report.Errors = append(report.Errors, issue("input_type_mismatch", path+"."+name, "input type does not match template"))
			continue
		}
		if len(spec.Enum) > 0 {
			text, ok := value.(string)
			if !ok || !slices.Contains(spec.Enum, text) {
				report.Errors = append(report.Errors, issue("input_enum_mismatch", path+"."+name, "input value is outside the allowed enum"))
			}
		}
	}
	for name := range input {
		if _, exists := parameters[name]; !exists {
			report.Errors = append(report.Errors, issue("unknown_input", path+"."+name, "input is not declared by the task template"))
		}
	}
}

func matchesType(value any, expected string) bool {
	switch expected {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
			return true
		}
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	}
	return false
}

func issue(code, path, message string) ValidationIssue {
	return ValidationIssue{Code: code, Path: path, Message: message}
}
