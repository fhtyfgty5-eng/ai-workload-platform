package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/kubeexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestModule6KubernetesActionRunsInKind(t *testing.T) {
	if os.Getenv("WORKLOAD_MODULE6_KUBERNETES_E2E") != "1" {
		t.Skip("WORKLOAD_MODULE6_KUBERNETES_E2E=1 is required")
	}
	image := os.Getenv("WORKLOAD_WORKER_ACTION_IMAGE")
	if image == "" {
		image = "workload-action:local"
	}
	specs, limits := containerexec.DefaultActionSpecs(image)
	registry, err := containerexec.NewRegistry(specs, limits)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubeexec.NewClientFromKubeconfig(os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	response := (&kubeexec.KubernetesExecutor{Client: client, Registry: registry}).Execute(context.Background(), workflow.ExecutionRequest{
		RunID: "module6-kubernetes-e2e", TaskKey: "normalize", Action: "document.normalize", Input: map[string]any{"source": "  kind   ok  "}, Attempt: 1,
	})
	if response.Kind != workflow.ResultSuccess || response.Output != "kind ok" {
		t.Fatalf("Kubernetes response = %+v", response)
	}
}

func TestModule6KubernetesMapsOOMKilled(t *testing.T) {
	if os.Getenv("WORKLOAD_MODULE6_KUBERNETES_E2E") != "1" {
		t.Skip("WORKLOAD_MODULE6_KUBERNETES_E2E=1 is required")
	}
	image := os.Getenv("WORKLOAD_WORKER_ACTION_IMAGE")
	if image == "" {
		image = "workload-action:local"
	}
	action := containerexec.ActionSpec{
		Name: "resource.memory-burn", Image: image, ImageDigest: "sha256:local", Entrypoint: []string{"/action"},
		InputSchema: containerexec.InputSchema{"megabytes": "number"},
		Limits:      containerexec.ResourceLimits{CPU: 250, MemoryBytes: 32 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: time.Minute},
		Network:     containerexec.NetworkNone, OutputLimitBytes: 4096,
	}
	registry, err := containerexec.NewRegistry([]containerexec.ActionSpec{action}, containerexec.ResourceLimits{
		CPU: 1000, MemoryBytes: 256 << 20, EphemeralStorageBytes: 32 << 20, PidsLimit: 64, Timeout: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubeexec.NewClientFromKubeconfig(os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	response := (&kubeexec.KubernetesExecutor{Client: client, Registry: registry}).Execute(context.Background(), workflow.ExecutionRequest{
		RunID: "module6-kube-oom-e2e", TaskKey: "memory", Action: "resource.memory-burn", Input: map[string]any{"megabytes": float64(64)}, Attempt: 1,
	})
	if response.Kind != workflow.ResultTemporaryFailure || response.ErrorCode != "oom_killed" {
		t.Fatalf("Kubernetes OOM response = %+v", response)
	}
}
