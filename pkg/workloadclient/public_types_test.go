package workloadclient_test

import (
	"context"

	"github.com/fhtyfgty5-eng/ai-workload-platform/pkg/workloadclient"
)

// publicClient 固定外部 Go 模块必须能够引用的 SDK 接口范围。
type publicClient interface {
	GetWorkflow(context.Context, string) (workloadclient.WorkflowSummary, error)
	ListRuns(context.Context, string, int) (workloadclient.RunPage, error)
}

var _ publicClient = (*workloadclient.Client)(nil)
