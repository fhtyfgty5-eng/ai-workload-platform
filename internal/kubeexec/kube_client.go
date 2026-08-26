package kubeexec

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/containerexec"
)

// Client 是 Kubernetes API 的真实适配器。
type Client struct {
	Jobs      kubernetes.Interface
	Namespace string
}

// NewClientFromKubeconfig 从 KUBECONFIG 或当前集群配置创建客户端。
func NewClientFromKubeconfig(path string) (*Client, error) {
	var config *rest.Config
	var err error
	if path == "" {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return nil, err
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Client{Jobs: clientset, Namespace: "workload-tasks"}, nil
}

func (c *Client) namespace() string {
	if c.Namespace == "" {
		return "workload-tasks"
	}
	return c.Namespace
}

func (c *Client) CreateJob(ctx context.Context, spec JobSpec) (JobHandle, error) {
	deadline := spec.ActiveDeadlineSeconds
	backoff := spec.BackoffLimit
	container := corev1.Container{
		Name: spec.Name, Image: spec.Image, Command: spec.Entrypoint, Args: spec.Arguments,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              *resource.NewMilliQuantity(int64(spec.Resources.CPU), resource.DecimalSI),
				corev1.ResourceMemory:           *resource.NewQuantity(spec.Resources.MemoryBytes, resource.BinarySI),
				corev1.ResourceEphemeralStorage: *resource.NewQuantity(spec.Resources.EphemeralStorageBytes, resource.BinarySI),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              *resource.NewMilliQuantity(int64(spec.Resources.CPU), resource.DecimalSI),
				corev1.ResourceMemory:           *resource.NewQuantity(spec.Resources.MemoryBytes, resource.BinarySI),
				corev1.ResourceEphemeralStorage: *resource.NewQuantity(spec.Resources.EphemeralStorageBytes, resource.BinarySI),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: ptr(true), RunAsUser: ptr(spec.Security.RunAsUser),
			ReadOnlyRootFilesystem:   ptr(spec.Security.ReadOnlyRootFilesystem),
			AllowPrivilegeEscalation: ptr(spec.Security.AllowPrivilegeEscalation),
			Privileged:               ptr(spec.Security.Privileged),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: spec.Namespace, Labels: spec.Labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: ptr(int32(60)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: spec.Labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr(false),
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr(true), RunAsUser: ptr(spec.Security.RunAsUser)},
					Containers:      []corev1.Container{container},
				},
			},
		},
	}
	created, err := c.Jobs.BatchV1().Jobs(spec.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return JobHandle(created.Name), nil
}

func (c *Client) WaitJob(ctx context.Context, handle JobHandle) (JobStatus, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := c.Jobs.BatchV1().Jobs(c.namespace()).Get(ctx, string(handle), metav1.GetOptions{})
		if err != nil {
			return JobStatus{}, err
		}
		if job.Status.Succeeded > 0 {
			return JobStatus{Succeeded: true}, nil
		}
		if job.Status.Failed > 0 {
			status := JobStatus{Failed: true, Reason: "job_failed"}
			pods, podErr := c.Jobs.CoreV1().Pods(c.namespace()).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + string(handle)})
			if podErr == nil && len(pods.Items) > 0 {
				for _, containerStatus := range pods.Items[0].Status.ContainerStatuses {
					if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == "OOMKilled" {
						status.OOMKilled = true
						status.Reason = "oom_killed"
					}
				}
			}
			return status, nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return JobStatus{}, ctx.Err()
		}
	}
}

func (c *Client) DeleteJob(ctx context.Context, handle JobHandle) error {
	return c.Jobs.BatchV1().Jobs(c.namespace()).Delete(ctx, string(handle), metav1.DeleteOptions{PropagationPolicy: ptr(metav1.DeletePropagationBackground)})
}

func (c *Client) Logs(ctx context.Context, handle JobHandle, limit int64) (containerexec.LogOutput, error) {
	pods, err := c.Jobs.CoreV1().Pods(c.namespace()).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + string(handle)})
	if err != nil {
		return containerexec.LogOutput{}, fmt.Errorf("find job pod: %w", err)
	}
	if len(pods.Items) == 0 {
		return containerexec.LogOutput{}, fmt.Errorf("find job pod: no pod for job %s", handle)
	}
	request := c.Jobs.CoreV1().Pods(c.namespace()).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{})
	reader, err := request.Stream(ctx)
	if err != nil {
		return containerexec.LogOutput{}, err
	}
	defer reader.Close()
	if limit <= 0 {
		limit = 1 << 20
	}
	b, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return containerexec.LogOutput{}, err
	}
	return containerexec.LogOutput{Stdout: strings.TrimSpace(string(b))}, nil
}

func ptr[T any](value T) *T { return &value }
