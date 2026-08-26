package kubeexec

import (
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
)

func testAction(t *testing.T) containerexec.ActionSpec {
	t.Helper()
	return containerexec.ActionSpec{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		InputSchema: containerexec.InputSchema{"source": "string"},
		Limits:      containerexec.ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second},
		Network:     containerexec.NetworkNone, OutputLimitBytes: 4096,
	}
}

func TestBuildJobSpecUsesSafeDefaults(t *testing.T) {
	spec, err := BuildJobSpec(testAction(t), map[string]any{"source": "sample.txt"}, JobIdentity{RunID: "run-1", TaskKey: "task", Attempt: 1})
	if err != nil {
		t.Fatalf("BuildJobSpec() error = %v", err)
	}
	if spec.BackoffLimit != 0 || spec.Security.Privileged || spec.Security.AllowPrivilegeEscalation || !spec.Security.ReadOnlyRootFilesystem || spec.Security.RunAsUser == 0 {
		t.Fatalf("unsafe JobSpec = %+v", spec)
	}
	if spec.Namespace == "" || spec.Name == "" || spec.ActiveDeadlineSeconds <= 0 || spec.Resources.MemoryBytes <= 0 {
		t.Fatalf("incomplete JobSpec = %+v", spec)
	}
}

func TestBuildJobSpecRejectsInvalidIdentity(t *testing.T) {
	if _, err := BuildJobSpec(testAction(t), nil, JobIdentity{RunID: "../escape", TaskKey: "task", Attempt: 1}); err == nil {
		t.Fatal("BuildJobSpec() error = nil, want invalid identity error")
	}
}
