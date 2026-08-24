package agent

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBudgetRejectsNinthToolCall(t *testing.T) {
	budget := NewBudget(DefaultLimits())
	for range 8 {
		if err := budget.UseToolCall(); err != nil {
			t.Fatal(err)
		}
	}
	if err := budget.UseToolCall(); CodeOf(err) != CodeBudgetExceeded {
		t.Fatalf("UseToolCall() error = %v, want budget_exceeded", err)
	}
}

func TestBudgetRejectsFifthModelTurn(t *testing.T) {
	budget := NewBudget(DefaultLimits())
	for range 4 {
		if err := budget.UseModelTurn(); err != nil {
			t.Fatal(err)
		}
	}
	if err := budget.UseModelTurn(); CodeOf(err) != CodeBudgetExceeded {
		t.Fatalf("UseModelTurn() error = %v, want budget_exceeded", err)
	}
}

func TestBudgetRejectsResponseOver64KiB(t *testing.T) {
	budget := NewBudget(DefaultLimits())
	if err := budget.CheckResponseSize(len(strings.Repeat("x", 64*1024+1))); CodeOf(err) != CodeBudgetExceeded {
		t.Fatalf("CheckResponseSize() error = %v, want budget_exceeded", err)
	}
}

func TestBudgetUsesRuntimeDeadline(t *testing.T) {
	limits := DefaultLimits()
	limits.RuntimeTimeout = 20 * time.Millisecond
	ctx, cancel := NewBudget(limits).Context(context.Background())
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("budget context did not reach deadline")
	}
}
