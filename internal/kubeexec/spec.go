// Package kubeexec 定义 Kubernetes Job/Pod 执行边界。
package kubeexec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
)

// JobIdentity 用于生成可清理且不含路径分隔符的 Job 标识。
type JobIdentity struct {
	RunID   string
	TaskKey string
	Attempt int
}

// SecurityContext 保存任务 Pod 的安全默认值。
type SecurityContext struct {
	RunAsUser                int64
	ReadOnlyRootFilesystem   bool
	AllowPrivilegeEscalation bool
	Privileged               bool
	AutomountServiceAccount  bool
}

// JobSpec 是与 Kubernetes API 解耦的受限 Job 描述。
type JobSpec struct {
	Namespace             string
	Name                  string
	Image                 string
	Entrypoint            []string
	Arguments             []string
	Resources             containerexec.ResourceLimits
	Security              SecurityContext
	Labels                map[string]string
	OwnerReference        string
	ActiveDeadlineSeconds int64
	BackoffLimit          int32
}

var safeIdentityPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$`)

// BuildJobSpec 把动作和运行身份转换为固定 Namespace 下的安全 Job 描述。
func BuildJobSpec(action containerexec.ActionSpec, input map[string]any, identity JobIdentity) (JobSpec, error) {
	if action.Name == "" || action.Image == "" || len(action.Entrypoint) == 0 || action.Limits.Timeout <= 0 {
		return JobSpec{}, fmt.Errorf("action specification is incomplete")
	}
	if !safeIdentityPattern.MatchString(identity.RunID) || !safeIdentityPattern.MatchString(identity.TaskKey) || identity.Attempt <= 0 {
		return JobSpec{}, fmt.Errorf("invalid job identity")
	}
	if err := validateInput(action.InputSchema, input); err != nil {
		return JobSpec{}, err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return JobSpec{}, fmt.Errorf("encode action input: %w", err)
	}
	deadline := int64(action.Limits.Timeout / time.Second)
	if deadline < 1 {
		deadline = 1
	}
	return JobSpec{
		Namespace: "workload-tasks",
		Name:      fmt.Sprintf("run-%s-task-%s-a%d", identity.RunID, identity.TaskKey, identity.Attempt),
		Image:     action.Image, Entrypoint: append([]string(nil), action.Entrypoint...), Arguments: []string{action.Name, string(payload)},
		Resources: action.Limits, Security: SecurityContext{RunAsUser: 65532, ReadOnlyRootFilesystem: true, AllowPrivilegeEscalation: false, Privileged: false, AutomountServiceAccount: false},
		Labels:         map[string]string{"app.kubernetes.io/name": "workload-task", "workload.run": identity.RunID, "workload.task": identity.TaskKey, "workload.owner": identity.RunID},
		OwnerReference: identity.RunID, ActiveDeadlineSeconds: deadline, BackoffLimit: 0,
	}, nil
}

func validateInput(schema containerexec.InputSchema, input map[string]any) error {
	for key, value := range input {
		typeName, ok := schema[key]
		if !ok {
			return fmt.Errorf("input field %q is not allowed", key)
		}
		if typeName == "string" {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("input field %q must be string", key)
			}
		}
	}
	return nil
}
