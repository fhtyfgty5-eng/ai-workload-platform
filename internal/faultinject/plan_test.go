package faultinject

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPlanConsumesOneShotActionsAndCopiesInput(t *testing.T) {
	actions := map[Operation][]Action{OperationClaim: {{Err: errors.New("claim unavailable")}}}
	plan, err := NewPlan(actions)
	if err != nil {
		t.Fatal(err)
	}
	actions[OperationClaim][0].Err = nil
	if err := plan.Before(context.Background(), OperationClaim); err == nil {
		t.Fatal("first action did not fail")
	}
	if err := plan.Before(context.Background(), OperationClaim); err != nil {
		t.Fatalf("second action error = %v, want nil", err)
	}
}

func TestPlanDelayCanBeCanceledAndConcurrentPlansAreIsolated(t *testing.T) {
	plan, err := NewPlan(map[Operation][]Action{OperationHeartbeat: {{Delay: time.Second}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := plan.Before(ctx, OperationHeartbeat); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delay error = %v, want deadline", err)
	}

	first, _ := NewPlan(map[Operation][]Action{OperationClaim: {{Err: errors.New("once")}}})
	second, _ := NewPlan(map[Operation][]Action{OperationClaim: {{Err: errors.New("once")}}})
	var wg sync.WaitGroup
	for _, p := range []*Plan{first, second} {
		wg.Add(1)
		go func(p *Plan) {
			defer wg.Done()
			if err := p.Before(context.Background(), OperationClaim); err == nil {
				t.Error("isolated plan did not fail")
			}
		}(p)
	}
	wg.Wait()
}
