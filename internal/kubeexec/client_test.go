package kubeexec

import (
	"context"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type fakeKubeClient struct {
	created bool
	deleted bool
	status  JobStatus
	logs    containerexec.LogOutput
}

func (f *fakeKubeClient) CreateJob(context.Context, JobSpec) (JobHandle, error) {
	f.created = true
	return JobHandle("job-1"), nil
}
func (f *fakeKubeClient) WaitJob(context.Context, JobHandle) (JobStatus, error) { return f.status, nil }
func (f *fakeKubeClient) DeleteJob(context.Context, JobHandle) error            { f.deleted = true; return nil }
func (f *fakeKubeClient) Logs(context.Context, JobHandle, int64) (containerexec.LogOutput, error) {
	return f.logs, nil
}

func TestKubernetesExecutorMapsSucceededJob(t *testing.T) {
	client := &fakeKubeClient{status: JobStatus{Succeeded: true}, logs: containerexec.LogOutput{Stdout: "ok"}}
	executor := &KubernetesExecutor{Client: client, Registry: testRegistry(t)}
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{RunID: "run-1", TaskKey: "task", Attempt: 1, Action: "document.normalize", Input: map[string]any{"source": "sample.txt"}})
	if response.Kind != workflow.ResultSuccess || response.Output != "ok" || !client.created || !client.deleted {
		t.Fatalf("response = %+v, created = %v, deleted = %v", response, client.created, client.deleted)
	}
}

func testRegistry(t *testing.T) containerexec.Registry {
	t.Helper()
	registry, err := containerexec.NewRegistry([]containerexec.ActionSpec{{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		InputSchema: containerexec.InputSchema{"source": "string"}, Limits: containerexec.ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second}, Network: containerexec.NetworkNone,
	}}, containerexec.ResourceLimits{CPU: 500, MemoryBytes: 32 << 20, EphemeralStorageBytes: 2 << 20, PidsLimit: 32, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
