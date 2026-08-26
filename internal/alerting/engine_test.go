package alerting

import (
	"testing"
	"time"
)

func TestEngineTriggersOnceAndResolvesAfterRecoveryWindow(t *testing.T) {
	start := time.Unix(100, 0)
	engine := NewEngine(DefaultRules())
	problem := Snapshot{Now: start, QueueDepth: 3, AvailableSlots: 0, OnlineWorkers: 1, DBMax: 10}
	if events := engine.Evaluate(problem); len(events) != 0 {
		t.Fatalf("first evaluation events = %+v, want pending", events)
	}
	problem.Now = start.Add(11 * time.Second)
	events := engine.Evaluate(problem)
	assertSingleEvent(t, events, QueueBacklog, StatusFiring)
	problem.Now = start.Add(12 * time.Second)
	if events := engine.Evaluate(problem); len(events) != 0 {
		t.Fatalf("repeated firing events = %+v", events)
	}

	recovered := Snapshot{Now: start.Add(13 * time.Second), QueueDepth: 0, AvailableSlots: 1, OnlineWorkers: 1, DBMax: 10}
	if events := engine.Evaluate(recovered); len(events) != 0 {
		t.Fatalf("early recovery events = %+v", events)
	}
	recovered.Now = start.Add(19 * time.Second)
	events = engine.Evaluate(recovered)
	assertSingleEvent(t, events, QueueBacklog, StatusResolved)
}

func TestEngineEvaluatesAllRequiredRules(t *testing.T) {
	start := time.Unix(200, 0)
	engine := NewEngine(Rules{
		QueueBacklogFor:       time.Second,
		WorkersOfflineFor:     time.Second,
		LeaseReclaimErrorsFor: time.Second,
		CompleteErrorRateFor:  time.Second,
		DBPoolExhaustionFor:   time.Second,
		RecoveryFor:           time.Second,
	})
	problem := Snapshot{
		Now: start, QueueDepth: 5, AvailableSlots: 0, OnlineWorkers: 0,
		LeaseReclaimErrors: 1, CompleteTotal: 20, CompleteErrors: 2, DBInUse: 9, DBMax: 10,
	}
	engine.Evaluate(problem)
	problem.Now = start.Add(2 * time.Second)
	events := engine.Evaluate(problem)
	got := make(map[RuleName]Status, len(events))
	for _, event := range events {
		got[event.Rule] = event.Status
	}
	for _, rule := range []RuleName{QueueBacklog, WorkersOffline, LeaseReclaimErrors, CompleteErrorRate, DBPoolNearExhaustion} {
		if got[rule] != StatusFiring {
			t.Fatalf("rule %s status = %s, events = %+v", rule, got[rule], events)
		}
	}
}

func TestEngineReportsUnknownWithoutResolvingActiveAlert(t *testing.T) {
	start := time.Unix(300, 0)
	engine := NewEngine(Rules{DBPoolExhaustionFor: time.Second, RecoveryFor: time.Second})
	problem := Snapshot{Now: start, DBInUse: 9, DBMax: 10}
	engine.Evaluate(problem)
	problem.Now = start.Add(2 * time.Second)
	assertSingleEvent(t, engine.Evaluate(problem), DBPoolNearExhaustion, StatusFiring)
	unknown := Snapshot{Now: start.Add(3 * time.Second), DBMax: 0}
	assertSingleEvent(t, engine.Evaluate(unknown), DBPoolNearExhaustion, StatusUnknown)
	unknown.Now = start.Add(5 * time.Second)
	if events := engine.Evaluate(unknown); len(events) != 0 {
		t.Fatalf("repeated unknown events = %+v", events)
	}
}

func TestEngineUsesSeparateRecoveryThresholds(t *testing.T) {
	start := time.Unix(350, 0)
	engine := NewEngine(Rules{
		CompleteErrorRateFor: time.Second,
		DBPoolExhaustionFor:  time.Second,
		RecoveryFor:          time.Second,
		DBPoolRecoveryFor:    time.Second,
	})
	bad := Snapshot{Now: start, CompleteTotal: 100, CompleteErrors: 10, DBInUse: 9, DBMax: 10}
	engine.Evaluate(bad)
	bad.Now = start.Add(2 * time.Second)
	if events := engine.Evaluate(bad); len(events) != 2 {
		t.Fatalf("firing events = %+v, want complete and db pool alerts", events)
	}

	// 2% Complete 错误率和 75% 连接使用率已经低于触发阈值，但尚未达到恢复阈值。
	neutral := Snapshot{Now: start.Add(3 * time.Second), CompleteTotal: 100, CompleteErrors: 2, DBInUse: 3, DBMax: 4}
	engine.Evaluate(neutral)
	neutral.Now = start.Add(5 * time.Second)
	if events := engine.Evaluate(neutral); len(events) != 0 {
		t.Fatalf("neutral-zone events = %+v, want alerts to remain firing", events)
	}

	recovered := Snapshot{Now: start.Add(6 * time.Second), CompleteTotal: 100, CompleteErrors: 0, DBInUse: 6, DBMax: 10}
	engine.Evaluate(recovered)
	recovered.Now = start.Add(8 * time.Second)
	events := engine.Evaluate(recovered)
	if len(events) != 2 {
		t.Fatalf("resolved events = %+v, want two", events)
	}
	for _, event := range events {
		if event.Status != StatusResolved {
			t.Fatalf("event = %+v, want resolved", event)
		}
	}
}

func TestDBPoolAlertUsesDedicatedRecoveryWindow(t *testing.T) {
	start := time.Unix(360, 0)
	engine := NewEngine(Rules{
		DBPoolExhaustionFor: time.Second,
		RecoveryFor:         time.Second,
		DBPoolRecoveryFor:   10 * time.Second,
	})
	bad := Snapshot{Now: start, DBInUse: 9, DBMax: 10}
	engine.Evaluate(bad)
	bad.Now = start.Add(2 * time.Second)
	assertSingleEvent(t, engine.Evaluate(bad), DBPoolNearExhaustion, StatusFiring)

	recovered := Snapshot{Now: start.Add(3 * time.Second), DBInUse: 6, DBMax: 10}
	engine.Evaluate(recovered)
	recovered.Now = start.Add(5 * time.Second)
	if events := engine.Evaluate(recovered); len(events) != 0 {
		t.Fatalf("database pool resolved using the general recovery window: %+v", events)
	}
	recovered.Now = start.Add(13 * time.Second)
	assertSingleEvent(t, engine.Evaluate(recovered), DBPoolNearExhaustion, StatusResolved)
}

func TestCompleteAlertTreatsInsufficientWindowSamplesAsUnknown(t *testing.T) {
	start := time.Unix(375, 0)
	engine := NewEngine(Rules{CompleteErrorRateFor: time.Second, RecoveryFor: time.Second})
	bad := Snapshot{Now: start, CompleteTotal: 20, CompleteErrors: 2, DBMax: 10}
	engine.Evaluate(bad)
	bad.Now = start.Add(2 * time.Second)
	assertSingleEvent(t, engine.Evaluate(bad), CompleteErrorRate, StatusFiring)

	unknown := Snapshot{Now: start.Add(3 * time.Second), CompleteTotal: 0, DBMax: 10}
	assertSingleEvent(t, engine.Evaluate(unknown), CompleteErrorRate, StatusUnknown)
	unknown.Now = start.Add(6 * time.Second)
	if events := engine.Evaluate(unknown); len(events) != 0 {
		t.Fatalf("insufficient samples resolved or repeated unknown: %+v", events)
	}
}

func TestEngineEachRequiredRuleFiresAndResolvesOnce(t *testing.T) {
	start := time.Unix(400, 0)
	rules := Rules{
		QueueBacklogFor: time.Second, WorkersOfflineFor: time.Second,
		LeaseReclaimErrorsFor: time.Second, CompleteErrorRateFor: time.Second,
		DBPoolExhaustionFor: time.Second, RecoveryFor: time.Second, DBPoolRecoveryFor: time.Second,
	}
	engine := NewEngine(rules)
	bad := Snapshot{Now: start, QueueDepth: 3, AvailableSlots: 0, OnlineWorkers: 0, LeaseReclaimErrors: 1, CompleteTotal: 20, CompleteErrors: 2, DBInUse: 9, DBMax: 10}
	engine.Evaluate(bad)
	bad.Now = start.Add(2 * time.Second)
	firing := engine.Evaluate(bad)
	if len(firing) != 5 {
		t.Fatalf("firing events = %+v, want five", firing)
	}
	if repeated := engine.Evaluate(bad); len(repeated) != 0 {
		t.Fatalf("repeated firing events = %+v", repeated)
	}
	good := Snapshot{Now: start.Add(3 * time.Second), QueueDepth: 0, AvailableSlots: 1, OnlineWorkers: 1, CompleteTotal: 20, DBMax: 10}
	engine.Evaluate(good)
	good.Now = start.Add(5 * time.Second)
	resolved := engine.Evaluate(good)
	if len(resolved) != 5 {
		t.Fatalf("resolved events = %+v, want five", resolved)
	}
	if repeated := engine.Evaluate(good); len(repeated) != 0 {
		t.Fatalf("repeated resolved events = %+v", repeated)
	}
}

func assertSingleEvent(t *testing.T, events []Event, rule RuleName, status Status) {
	t.Helper()
	if len(events) != 1 || events[0].Rule != rule || events[0].Status != status {
		t.Fatalf("events = %+v, want one %s %s", events, rule, status)
	}
}
