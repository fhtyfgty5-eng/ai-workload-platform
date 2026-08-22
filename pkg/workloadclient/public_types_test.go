package workloadclient_test

import (
	"context"

	"github.com/fhtyfgty5-eng/ai-workload-platform/pkg/workloadclient"
)

// publicClient captures the SDK surface an external Go module must be able to name.
type publicClient interface {
	GetWorkflow(context.Context, string) (workloadclient.WorkflowSummary, error)
	ListRuns(context.Context, string, int) (workloadclient.RunPage, error)
}

var _ publicClient = (*workloadclient.Client)(nil)
