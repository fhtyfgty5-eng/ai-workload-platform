package containerexec

import (
	"encoding/json"
	"fmt"
	"time"
)

// Mount 描述容器内部的挂载点；模块 6 默认不挂载宿主机路径。
type Mount struct {
	Source   string
	Target   string
	ReadOnly bool
}

// ContainerSpec 是已经完成安全归一化的容器启动描述。
type ContainerSpec struct {
	Image                 string
	Entrypoint            []string
	Arguments             []string
	User                  string
	ReadOnlyRootFS        bool
	NoNewPrivileges       bool
	Privileged            bool
	CapDrop               []string
	NetworkMode           string
	Mounts                []Mount
	CPU                   MilliCPU
	MemoryBytes           int64
	EphemeralStorageBytes int64
	PidsLimit             int64
	Timeout               time.Duration
	OutputLimitBytes      int64
}

// BuildContainerSpec 将注册动作和结构化输入转换为受限容器描述。
func BuildContainerSpec(spec ActionSpec, input map[string]any, runID string, taskKey string) (ContainerSpec, error) {
	if spec.Name == "" || spec.Image == "" || len(spec.Entrypoint) == 0 {
		return ContainerSpec{}, fmt.Errorf("action specification is incomplete")
	}
	if err := validateInputSchema(spec.InputSchema, input); err != nil {
		return ContainerSpec{}, err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return ContainerSpec{}, fmt.Errorf("encode action input: %w", err)
	}
	if runID == "" || taskKey == "" {
		return ContainerSpec{}, fmt.Errorf("run and task identity are required")
	}
	networkMode := string(spec.Network)
	if networkMode == "" {
		networkMode = string(NetworkNone)
	}
	return ContainerSpec{
		Image:      spec.Image,
		Entrypoint: append([]string(nil), spec.Entrypoint...),
		// 动作名和 JSON 参数作为独立参数传给固定入口，避免拼接 Shell 命令。
		Arguments:             []string{spec.Name, string(payload)},
		User:                  "65532:65532",
		ReadOnlyRootFS:        true,
		NoNewPrivileges:       true,
		Privileged:            false,
		CapDrop:               []string{"ALL"},
		NetworkMode:           networkMode,
		Mounts:                nil,
		CPU:                   spec.Limits.CPU,
		MemoryBytes:           spec.Limits.MemoryBytes,
		EphemeralStorageBytes: spec.Limits.EphemeralStorageBytes,
		PidsLimit:             spec.Limits.PidsLimit,
		Timeout:               spec.Limits.Timeout,
		OutputLimitBytes:      spec.OutputLimitBytes,
	}, nil
}

func validateInputSchema(schema InputSchema, input map[string]any) error {
	for key, value := range input {
		typeName, ok := schema[key]
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
