package containerexec

import (
	"fmt"
	"regexp"
	"strings"
)

var actionNamePattern = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)+$`)

// Registry 提供动作解析和输入校验能力。
type Registry interface {
	Resolve(name string) (ActionSpec, bool)
	ValidateInput(spec ActionSpec, input map[string]any) error
}

// ActionRegistry 保存启动前可查询的不可变动作集合。
type ActionRegistry struct {
	actions map[string]ActionSpec
	limits  ResourceLimits
}

// NewRegistry 校验并复制动作描述，拒绝超出平台上限的动作。
func NewRegistry(specs []ActionSpec, limits ResourceLimits) (*ActionRegistry, error) {
	if limits.CPU <= 0 || limits.MemoryBytes <= 0 || limits.EphemeralStorageBytes <= 0 || limits.PidsLimit <= 0 || limits.Timeout <= 0 {
		return nil, fmt.Errorf("registry resource limits must be positive")
	}
	registry := &ActionRegistry{actions: make(map[string]ActionSpec, len(specs)), limits: limits}
	for _, spec := range specs {
		if !actionNamePattern.MatchString(spec.Name) {
			return nil, fmt.Errorf("invalid action name %q", spec.Name)
		}
		if _, exists := registry.actions[spec.Name]; exists {
			return nil, fmt.Errorf("duplicate action %q", spec.Name)
		}
		if !approvedImageReference(spec) || len(spec.Entrypoint) == 0 {
			return nil, fmt.Errorf("action %q must use a digest image and non-empty entrypoint", spec.Name)
		}
		if spec.Limits.CPU <= 0 || spec.Limits.MemoryBytes <= 0 || spec.Limits.EphemeralStorageBytes <= 0 || spec.Limits.PidsLimit <= 0 || spec.Limits.Timeout <= 0 {
			return nil, fmt.Errorf("action %q resource limits must be positive", spec.Name)
		}
		if spec.Limits.CPU > limits.CPU || spec.Limits.MemoryBytes > limits.MemoryBytes || spec.Limits.EphemeralStorageBytes > limits.EphemeralStorageBytes || spec.Limits.PidsLimit > limits.PidsLimit || spec.Limits.Timeout > limits.Timeout {
			return nil, fmt.Errorf("action %q exceeds registry resource limits", spec.Name)
		}
		if spec.Network == "" {
			spec.Network = NetworkNone
		}
		if spec.Network != NetworkNone {
			return nil, fmt.Errorf("action %q has unsupported network policy", spec.Name)
		}
		copySpec := spec
		copySpec.Entrypoint = append([]string(nil), spec.Entrypoint...)
		copySpec.InputSchema = cloneSchema(spec.InputSchema)
		registry.actions[spec.Name] = copySpec
	}
	return registry, nil
}

func approvedImageReference(spec ActionSpec) bool {
	// 本地演示使用不可变的仓库内固定标签；其他环境必须显式提供 digest。
	return strings.Contains(spec.Image, "@sha256:") || (spec.Image == "workload-action:local" && strings.Contains(spec.ImageDigest, "sha256:"))
}

// Resolve 返回动作的隔离副本，调用方不能修改注册表内容。
func (r *ActionRegistry) Resolve(name string) (ActionSpec, bool) {
	if r == nil {
		return ActionSpec{}, false
	}
	spec, ok := r.actions[name]
	if !ok {
		return ActionSpec{}, false
	}
	spec.Entrypoint = append([]string(nil), spec.Entrypoint...)
	spec.InputSchema = cloneSchema(spec.InputSchema)
	return spec, true
}

// ValidateInput 执行注册表声明的最小对象字段校验。
func (r *ActionRegistry) ValidateInput(spec ActionSpec, input map[string]any) error {
	for key, value := range input {
		typeName, ok := spec.InputSchema[key]
		if !ok {
			return fmt.Errorf("input field %q is not allowed", key)
		}
		switch typeName {
		case "string":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("input field %q must be string", key)
			}
		case "number":
			if _, ok := value.(float64); !ok {
				return fmt.Errorf("input field %q must be number", key)
			}
		case "boolean":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("input field %q must be boolean", key)
			}
		default:
			return fmt.Errorf("input field %q has unsupported type", key)
		}
	}
	return nil
}

func cloneSchema(input InputSchema) InputSchema {
	if input == nil {
		return nil
	}
	output := make(InputSchema, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
