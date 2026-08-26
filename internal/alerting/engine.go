package alerting

import "time"

// Rules 定义每条异常条件的最短持续时间，以及通用和连接池专用的恢复持续时间。
type Rules struct {
	QueueBacklogFor       time.Duration
	WorkersOfflineFor     time.Duration
	LeaseReclaimErrorsFor time.Duration
	CompleteErrorRateFor  time.Duration
	DBPoolExhaustionFor   time.Duration
	RecoveryFor           time.Duration
	DBPoolRecoveryFor     time.Duration
}

// DefaultRules 返回本地实验阈值；这些值不是生产 SLO。
func DefaultRules() Rules {
	return Rules{
		QueueBacklogFor:       10 * time.Second,
		WorkersOfflineFor:     5 * time.Second,
		LeaseReclaimErrorsFor: time.Second,
		CompleteErrorRateFor:  30 * time.Second,
		DBPoolExhaustionFor:   10 * time.Second,
		RecoveryFor:           5 * time.Second,
		DBPoolRecoveryFor:     10 * time.Second,
	}
}

// Engine 保存每条规则的 pending、firing、unknown 和恢复滞回状态。
// Evaluate 必须由同一 goroutine 串行调用。
type Engine struct {
	rules  Rules
	active map[RuleName]*ruleState
}

type ruleState struct {
	conditionSince time.Time
	recoverySince  time.Time
	firing         bool
	unknown        bool
}

// NewEngine 用默认值补齐非正持续时间并创建尚无活动告警的状态机。
func NewEngine(rules Rules) *Engine {
	defaults := DefaultRules()
	if rules.QueueBacklogFor <= 0 {
		rules.QueueBacklogFor = defaults.QueueBacklogFor
	}
	if rules.WorkersOfflineFor <= 0 {
		rules.WorkersOfflineFor = defaults.WorkersOfflineFor
	}
	if rules.LeaseReclaimErrorsFor <= 0 {
		rules.LeaseReclaimErrorsFor = defaults.LeaseReclaimErrorsFor
	}
	if rules.CompleteErrorRateFor <= 0 {
		rules.CompleteErrorRateFor = defaults.CompleteErrorRateFor
	}
	if rules.DBPoolExhaustionFor <= 0 {
		rules.DBPoolExhaustionFor = defaults.DBPoolExhaustionFor
	}
	if rules.RecoveryFor <= 0 {
		rules.RecoveryFor = defaults.RecoveryFor
	}
	if rules.DBPoolRecoveryFor <= 0 {
		rules.DBPoolRecoveryFor = defaults.DBPoolRecoveryFor
	}
	return &Engine{rules: rules, active: make(map[RuleName]*ruleState)}
}

// Evaluate 只根据聚合快照推进告警状态，不修改 Workflow 或调度数据。
func (e *Engine) Evaluate(snapshot Snapshot) []Event {
	if snapshot.Now.IsZero() {
		snapshot.Now = time.Now()
	}
	checks := []struct {
		rule      RuleName
		bad       bool
		recovered bool
		duration  time.Duration
		recovery  time.Duration
		unknown   bool
	}{
		{QueueBacklog, snapshot.QueueDepth > 0 && snapshot.AvailableSlots == 0, snapshot.QueueDepth == 0 || snapshot.AvailableSlots > 0, e.rules.QueueBacklogFor, e.rules.RecoveryFor, false},
		{WorkersOffline, snapshot.QueueDepth > 0 && snapshot.OnlineWorkers == 0, snapshot.QueueDepth == 0 || snapshot.OnlineWorkers > 0, e.rules.WorkersOfflineFor, e.rules.RecoveryFor, false},
		{LeaseReclaimErrors, snapshot.LeaseReclaimErrors > 0, snapshot.LeaseReclaimErrors == 0, e.rules.LeaseReclaimErrorsFor, e.rules.RecoveryFor, false},
		{
			CompleteErrorRate,
			snapshot.CompleteTotal >= 20 && float64(snapshot.CompleteErrors)/float64(snapshot.CompleteTotal) > 0.05,
			snapshot.CompleteTotal >= 20 && float64(snapshot.CompleteErrors)/float64(snapshot.CompleteTotal) < 0.01,
			e.rules.CompleteErrorRateFor,
			e.rules.RecoveryFor,
			snapshot.CompleteTotal < 20,
		},
		{
			DBPoolNearExhaustion,
			snapshot.DBMax > 0 && float64(snapshot.DBInUse)/float64(snapshot.DBMax) >= 0.8,
			snapshot.DBMax > 0 && float64(snapshot.DBInUse)/float64(snapshot.DBMax) < 0.7,
			e.rules.DBPoolExhaustionFor,
			e.rules.DBPoolRecoveryFor,
			snapshot.DBMax <= 0,
		},
	}
	var events []Event
	for _, check := range checks {
		state := e.active[check.rule]
		if state == nil {
			state = &ruleState{}
			e.active[check.rule] = state
		}
		if check.unknown {
			state.conditionSince = time.Time{}
			state.recoverySince = time.Time{}
			if state.firing && !state.unknown {
				events = append(events, Event{Rule: check.rule, Status: StatusUnknown, Summary: "alert input is insufficient", RuleVersion: "v1"})
			}
			state.unknown = true
			continue
		}
		state.unknown = false
		if check.bad {
			state.recoverySince = time.Time{}
			if state.conditionSince.IsZero() {
				state.conditionSince = snapshot.Now
			}
			if !state.firing && snapshot.Now.Sub(state.conditionSince) >= check.duration {
				state.firing = true
				events = append(events, Event{Rule: check.rule, Status: StatusFiring, Summary: summary(check.rule), StartsAt: state.conditionSince, RuleVersion: "v1"})
			}
			continue
		}
		state.conditionSince = time.Time{}
		if !state.firing {
			continue
		}
		// 触发阈值和恢复阈值之间是滞回区间；进入该区间时保持 firing，避免边界抖动。
		if !check.recovered {
			state.recoverySince = time.Time{}
			continue
		}
		if state.recoverySince.IsZero() {
			state.recoverySince = snapshot.Now
		}
		if snapshot.Now.Sub(state.recoverySince) >= check.recovery {
			state.firing = false
			state.recoverySince = time.Time{}
			events = append(events, Event{Rule: check.rule, Status: StatusResolved, Summary: summary(check.rule), EndsAt: snapshot.Now, RuleVersion: "v1"})
		}
	}
	return events
}

func summary(rule RuleName) string { return string(rule) + " condition is active" }
