package containerexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type fakeRuntime struct {
	created   bool
	started   bool
	stopped   bool
	removed   bool
	status    ExitStatus
	logs      LogOutput
	waitErr   error
	startErr  error
	startedCh chan struct{}
	waitCh    chan struct{}
}

func (f *fakeRuntime) Create(context.Context, ContainerSpec) (ContainerHandle, error) {
	f.created = true
	return ContainerHandle("container-1"), nil
}
func (f *fakeRuntime) Start(context.Context, ContainerHandle) error {
	f.started = true
	if f.startedCh != nil {
		close(f.startedCh)
	}
	return f.startErr
}
func (f *fakeRuntime) Wait(context.Context, ContainerHandle) (ExitStatus, error) {
	if f.waitCh != nil {
		<-f.waitCh
	}
	return f.status, f.waitErr
}
func (f *fakeRuntime) Stop(context.Context, ContainerHandle) error   { f.stopped = true; return nil }
func (f *fakeRuntime) Remove(context.Context, ContainerHandle) error { f.removed = true; return nil }
func (f *fakeRuntime) Logs(context.Context, ContainerHandle, int64) (LogOutput, error) {
	return f.logs, nil
}

func testDockerRegistry(t *testing.T) Registry {
	t.Helper()
	registry, err := NewRegistry([]ActionSpec{{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		InputSchema: InputSchema{"source": "string"},
		Limits:      ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second},
		Network:     NetworkNone, OutputLimitBytes: 16,
	}}, ResourceLimits{CPU: 500, MemoryBytes: 32 << 20, EphemeralStorageBytes: 2 << 20, PidsLimit: 32, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestDockerExecutorSuccessCleansContainer(t *testing.T) {
	runtime := &fakeRuntime{status: ExitStatus{Code: 0}, logs: LogOutput{Stdout: "ok"}}
	executor := &DockerExecutor{Runtime: runtime, Registry: testDockerRegistry(t)}
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{RunID: "run-1", TaskKey: "task", Action: "document.normalize", Input: map[string]any{"source": "sample.txt"}})
	if response.Kind != workflow.ResultSuccess || response.Output != "ok" {
		t.Fatalf("response = %+v, want success", response)
	}
	if !runtime.created || !runtime.started || !runtime.removed {
		t.Fatalf("runtime lifecycle = created:%v started:%v removed:%v", runtime.created, runtime.started, runtime.removed)
	}
}

func TestDockerExecutorCancellationStopsContainer(t *testing.T) {
	runtime := &fakeRuntime{startedCh: make(chan struct{}), waitCh: make(chan struct{})}
	executor := &DockerExecutor{Runtime: runtime, Registry: testDockerRegistry(t)}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-runtime.startedCh
		cancel()
	}()
	response := executor.Execute(ctx, workflow.ExecutionRequest{RunID: "run-1", TaskKey: "task", Action: "document.normalize", Input: map[string]any{"source": "sample.txt"}})
	if response.Kind != workflow.ResultCanceled || !runtime.stopped {
		t.Fatalf("response = %+v, stopped = %v, want canceled and stopped", response, runtime.stopped)
	}
}

func TestDockerExecutorMapsOOMAndOutputLimit(t *testing.T) {
	runtime := &fakeRuntime{status: ExitStatus{Code: 137, OOMKilled: true}, logs: LogOutput{Stdout: "this output is too long"}}
	executor := &DockerExecutor{Runtime: runtime, Registry: testDockerRegistry(t)}
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{RunID: "run-1", TaskKey: "task", Action: "document.normalize", Input: map[string]any{"source": "sample.txt"}})
	if response.Kind != workflow.ResultTemporaryFailure || response.ErrorCode != "oom_killed" {
		t.Fatalf("response = %+v, want oom_killed temporary failure", response)
	}
	if errors.Is(errors.New(response.ErrorMessage), errors.New("this output")) {
		t.Fatal("error message unexpectedly contains full output")
	}
}
