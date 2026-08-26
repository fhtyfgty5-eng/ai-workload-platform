package postgres

import (
	"context"
	"testing"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func TestAlertSnapshotAggregatesWithoutExposingBusinessIDs(t *testing.T) {
	repository := newTestRepository(t)
	ctx := context.Background()
	createClaimedDispatch(t, repository, "observability-workflow", "observability-run", workflow.RetryPolicy{}, 1_000)
	snapshot, err := repository.AlertSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.OnlineWorkers != 1 || snapshot.AvailableSlots != 0 || snapshot.ActiveLeases != 1 || snapshot.DBMax <= 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	total, inUse, idle, _ := repository.PoolObservation()
	if total < 1 || inUse < 0 || idle < 0 {
		t.Fatalf("pool observation = %d/%d/%d", total, inUse, idle)
	}
}
