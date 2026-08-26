package containerexec

import (
	"testing"
	"time"
)

func TestBuildContainerSpecAppliesSecurityDefaults(t *testing.T) {
	spec := ActionSpec{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		InputSchema: InputSchema{"source": "string"},
		Limits:      ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second},
		Network:     NetworkNone, OutputLimitBytes: 4096,
	}
	container, err := BuildContainerSpec(spec, map[string]any{"source": "sample.txt"}, "run-1", "task")
	if err != nil {
		t.Fatalf("BuildContainerSpec() error = %v", err)
	}
	if container.Privileged || !container.ReadOnlyRootFS || !container.NoNewPrivileges || container.NetworkMode != "none" {
		t.Fatalf("unsafe container defaults: %+v", container)
	}
	if container.User == "" || len(container.Mounts) != 0 || container.OutputLimitBytes != 4096 {
		t.Fatalf("incomplete container defaults: %+v", container)
	}
}

func TestBuildContainerSpecRejectsInputOverride(t *testing.T) {
	spec := ActionSpec{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		InputSchema: InputSchema{"source": "string"},
		Limits:      ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second},
		Network:     NetworkNone,
	}
	if _, err := BuildContainerSpec(spec, map[string]any{"image": "evil", "source": "sample.txt"}, "run-1", "task"); err == nil {
		t.Fatal("BuildContainerSpec() error = nil, want disallowed input field error")
	}
}
