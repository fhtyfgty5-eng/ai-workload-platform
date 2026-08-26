package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestModule6DockerActionRunsWithApprovedImage(t *testing.T) {
	if os.Getenv("WORKLOAD_MODULE6_DOCKER_E2E") != "1" {
		t.Skip("WORKLOAD_MODULE6_DOCKER_E2E=1 is required")
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
	runtime, err := containerexec.NewDockerRuntimeClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	response := (&containerexec.DockerExecutor{Runtime: runtime, Registry: registry}).Execute(context.Background(), workflow.ExecutionRequest{
		RunID: "module6-docker-e2e", TaskKey: "normalize", Action: "document.normalize", Input: map[string]any{"source": "  docker   ok  "}, Attempt: 1,
	})
	if response.Kind != workflow.ResultSuccess || response.Output != "docker ok" {
		t.Fatalf("Docker response = %+v", response)
	}
}
