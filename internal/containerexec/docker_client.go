package containerexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// DockerRuntimeClient 是 Docker Engine API 的 RuntimeClient 适配器。
type DockerRuntimeClient struct {
	Client *client.Client
}

// NewDockerRuntimeClientFromEnv 根据 Docker Desktop 环境变量创建客户端。
func NewDockerRuntimeClientFromEnv() (*DockerRuntimeClient, error) {
	options := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	// Docker Desktop macOS 的当前用户 Socket 不一定映射到 /var/run/docker.sock；
	// 没有 DOCKER_HOST 时优先发现 Desktop 的标准 Socket，兼容 context desktop-linux。
	if os.Getenv("DOCKER_HOST") == "" {
		if home, err := os.UserHomeDir(); err == nil {
			socket := filepath.Join(home, ".docker", "run", "docker.sock")
			if _, statErr := os.Stat(socket); statErr == nil {
				options = []client.Opt{client.WithHost("unix://" + socket), client.WithAPIVersionNegotiation()}
			}
		}
	}
	cli, err := client.NewClientWithOpts(options...)
	if err != nil {
		return nil, err
	}
	return &DockerRuntimeClient{Client: cli}, nil
}

func buildDockerConfig(spec ContainerSpec) (*containertypes.Config, *containertypes.HostConfig) {
	pids := spec.PidsLimit
	config := &containertypes.Config{
		Image: spec.Image, Entrypoint: spec.Entrypoint, Cmd: spec.Arguments,
		User: spec.User, AttachStdout: true, AttachStderr: true,
		NetworkDisabled: spec.NetworkMode == "none",
	}
	host := &containertypes.HostConfig{
		AutoRemove: false, NetworkMode: containertypes.NetworkMode(spec.NetworkMode),
		ReadonlyRootfs: spec.ReadOnlyRootFS, CapDrop: spec.CapDrop,
		SecurityOpt: []string{"no-new-privileges"}, Privileged: spec.Privileged,
		Resources: containertypes.Resources{PidsLimit: &pids, Memory: spec.MemoryBytes, NanoCPUs: int64(spec.CPU) * 1_000_000},
		Tmpfs:     map[string]string{"/tmp": "rw,noexec,nosuid,size=" + formatBytes(spec.EphemeralStorageBytes)},
	}
	return config, host
}

func (d *DockerRuntimeClient) Create(ctx context.Context, spec ContainerSpec) (ContainerHandle, error) {
	config, host := buildDockerConfig(spec)
	response, err := d.Client.ContainerCreate(ctx, config, host, nil, nil, "")
	if err != nil {
		return "", err
	}
	return ContainerHandle(response.ID), nil
}

func (d *DockerRuntimeClient) Start(ctx context.Context, handle ContainerHandle) error {
	return d.Client.ContainerStart(ctx, string(handle), containertypes.StartOptions{})
}

func (d *DockerRuntimeClient) Wait(ctx context.Context, handle ContainerHandle) (ExitStatus, error) {
	statusCh, errCh := d.Client.ContainerWait(ctx, string(handle), containertypes.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		return ExitStatus{}, err
	case status := <-statusCh:
		inspected, inspectErr := d.Client.ContainerInspect(ctx, string(handle))
		if inspectErr == nil {
			return ExitStatus{Code: int(status.StatusCode), OOMKilled: inspected.State != nil && inspected.State.OOMKilled}, nil
		}
		// 容器可能已被外部清理；退出码仍可用于有限结果映射，不能因此伪造成功。
		return ExitStatus{Code: int(status.StatusCode)}, nil
	case <-ctx.Done():
		return ExitStatus{}, ctx.Err()
	}
}

func (d *DockerRuntimeClient) Stop(ctx context.Context, handle ContainerHandle) error {
	timeout := 1
	return d.Client.ContainerStop(ctx, string(handle), containertypes.StopOptions{Timeout: &timeout})
}

func (d *DockerRuntimeClient) Remove(ctx context.Context, handle ContainerHandle) error {
	return d.Client.ContainerRemove(ctx, string(handle), containertypes.RemoveOptions{Force: true})
}

func (d *DockerRuntimeClient) Logs(ctx context.Context, handle ContainerHandle, limit int64) (LogOutput, error) {
	reader, err := d.Client.ContainerLogs(ctx, string(handle), containertypes.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return LogOutput{}, err
	}
	defer reader.Close()
	var stdout, stderr bytes.Buffer
	if limit <= 0 {
		limit = 1 << 20
	}
	if _, err := stdcopy.StdCopy(&limitedBuffer{Buffer: &stdout, Limit: limit + 1}, &limitedBuffer{Buffer: &stderr, Limit: limit + 1}, io.LimitReader(reader, limit*2+1024)); err != nil {
		return LogOutput{}, err
	}
	return LogOutput{Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

type limitedBuffer struct {
	Buffer *bytes.Buffer
	Limit  int64
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.Limit - int64(b.Buffer.Len())
	if remaining <= 0 {
		return len(p), nil
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	return b.Buffer.Write(p)
}

func formatBytes(value int64) string {
	if value <= 0 {
		return "1m"
	}
	return fmt.Sprintf("%dm", (value+1023)/1024)
}
