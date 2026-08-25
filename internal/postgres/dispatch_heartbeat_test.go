package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

type leaseReapTestResult struct {
	count int
	err   error
}

func TestHeartbeatRenewsValidLeaseWithoutPassingAttemptDeadline(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "heartbeat-workflow", "heartbeat-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	response, err := repository.Heartbeat(context.Background(), worker.Summary.WorkerID, []workerprotocol.LeaseRef{
		{DispatchID: lease.DispatchID, LeaseToken: lease.LeaseToken},
		{DispatchID: lease.DispatchID, LeaseToken: "wrong"},
		{DispatchID: "missing", LeaseToken: "unknown"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Leases) != 3 {
		t.Fatalf("heartbeat results = %+v", response.Leases)
	}
	if response.Leases[0].Status != workerprotocol.LeaseRenewed || response.Leases[0].LeaseRemainingMillis <= 0 {
		t.Fatalf("valid heartbeat = %+v", response.Leases[0])
	}
	if response.Leases[1].Status != workerprotocol.LeaseUnknown || response.Leases[2].Status != workerprotocol.LeaseUnknown {
		t.Fatalf("invalid heartbeat results = %+v", response.Leases[1:])
	}
	var leaseExpiresAt, attemptDeadline time.Time
	if err := repository.pool.QueryRow(context.Background(), `
		SELECT lease_expires_at, attempt_deadline FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&leaseExpiresAt, &attemptDeadline); err != nil {
		t.Fatal(err)
	}
	if leaseExpiresAt.After(attemptDeadline) {
		t.Fatalf("lease expiry %v passed Attempt deadline %v", leaseExpiresAt, attemptDeadline)
	}
	var lastHeartbeat time.Time
	if err := repository.pool.QueryRow(context.Background(), `
		SELECT last_heartbeat_at FROM worker_sessions WHERE worker_id = $1
	`, worker.Summary.WorkerID).Scan(&lastHeartbeat); err != nil {
		t.Fatal(err)
	}
	if lastHeartbeat.Before(worker.Summary.LastHeartbeatAt) {
		t.Fatalf("last heartbeat moved backward: %v < %v", lastHeartbeat, worker.Summary.LastHeartbeatAt)
	}
}

func TestLeaseReapDistinguishesInterruptionFromAttemptTimeout(t *testing.T) {
	repository := newTestRepository(t)
	_, interruptedLease := createClaimedDispatch(t, repository, "interrupt-workflow", "interrupt-run", workflow.RetryPolicy{MaxAttempts: 2, IntervalMillis: 100}, 60_000)
	_, timedOutLease := createClaimedDispatch(t, repository, "timeout-workflow", "timeout-run", workflow.RetryPolicy{MaxAttempts: 1}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '2 seconds',
		    attempt_deadline = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
		WHERE dispatch_id = $1
	`, interruptedLease.DispatchID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '2 seconds',
		    attempt_deadline = CURRENT_TIMESTAMP - INTERVAL '1 second'
		WHERE dispatch_id = $1
	`, timedOutLease.DispatchID); err != nil {
		t.Fatal(err)
	}
	reaped, err := repository.ReapExpired(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 2 {
		t.Fatalf("ReapExpired() = %d, want 2", reaped)
	}
	interrupted, err := repository.Load(ctx, "interrupt-run")
	if err != nil {
		t.Fatal(err)
	}
	if task := interrupted.Run.Tasks[0]; task.Status != workflow.TaskWaitingRetry || task.Attempts[0].Status != workflow.AttemptInterrupted {
		t.Fatalf("interrupted task = %+v", task)
	}
	timedOut, err := repository.Load(ctx, "timeout-run")
	if err != nil {
		t.Fatal(err)
	}
	if task := timedOut.Run.Tasks[0]; task.Status != workflow.TaskFailed || task.Attempts[0].Status != workflow.AttemptTimedOut {
		t.Fatalf("timed-out task = %+v", task)
	}
	for _, dispatchID := range []string{interruptedLease.DispatchID, timedOutLease.DispatchID} {
		var status string
		if err := repository.pool.QueryRow(ctx, "SELECT status FROM task_dispatches WHERE dispatch_id = $1", dispatchID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "expired" {
			t.Fatalf("Dispatch %s status = %s, want expired", dispatchID, status)
		}
	}
}

func TestLeaseReapRetryBecomesClaimableAfterReadyAt(t *testing.T) {
	repository := newTestRepository(t)
	worker, firstLease := createClaimedDispatch(t, repository, "retry-reap-workflow", "retry-reap-run", workflow.RetryPolicy{MaxAttempts: 2, IntervalMillis: 100}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second',
		    attempt_deadline = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
		WHERE dispatch_id = $1
	`, firstLease.DispatchID); err != nil {
		t.Fatal(err)
	}
	if reaped, err := repository.ReapExpired(ctx, 10); err != nil || reaped != 1 {
		t.Fatalf("ReapExpired() = %d, %v", reaped, err)
	}
	if created, err := repository.CreateDispatches(ctx, 100); err != nil || created != 0 {
		t.Fatalf("CreateDispatches() before ReadyAt = %d, %v, want 0", created, err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_runs SET ready_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
		WHERE run_id = 'retry-reap-run' AND task_key = 'task'
	`); err != nil {
		t.Fatal(err)
	}
	if created, err := repository.CreateDispatches(ctx, 100); err != nil || created != 1 {
		t.Fatalf("CreateDispatches() after ReadyAt = %d, %v, want 1", created, err)
	}
	leases, err := repository.Claim(ctx, worker.Summary.WorkerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].Attempt != 2 {
		t.Fatalf("retry leases = %+v, want Attempt 2", leases)
	}
}

func TestHeartbeatAndLeaseReapSerializeExpiredLease(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "heartbeat-reap-workflow", "heartbeat-reap-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second',
		    attempt_deadline = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
		WHERE dispatch_id = $1
	`, lease.DispatchID); err != nil {
		t.Fatal(err)
	}

	type heartbeatResult struct {
		response workerprotocol.HeartbeatResponse
		err      error
	}
	type reapResult struct {
		count int
		err   error
	}
	heartbeats := make(chan heartbeatResult, 1)
	reaps := make(chan reapResult, 1)
	var start sync.WaitGroup
	start.Add(1)
	go func() {
		start.Wait()
		response, err := repository.Heartbeat(ctx, worker.Summary.WorkerID, []workerprotocol.LeaseRef{{
			DispatchID: lease.DispatchID,
			LeaseToken: lease.LeaseToken,
		}})
		heartbeats <- heartbeatResult{response: response, err: err}
	}()
	go func() {
		start.Wait()
		count, err := repository.ReapExpired(ctx, 10)
		reaps <- reapResult{count: count, err: err}
	}()
	start.Done()

	heartbeat := <-heartbeats
	if heartbeat.err != nil {
		t.Fatal(heartbeat.err)
	}
	if len(heartbeat.response.Leases) != 1 || heartbeat.response.Leases[0].Status != workerprotocol.LeaseRevoked {
		t.Fatalf("Heartbeat() = %+v, want one revoked lease", heartbeat.response)
	}
	reap := <-reaps
	if reap.err != nil || reap.count != 1 {
		t.Fatalf("ReapExpired() = %d, %v, want 1, nil", reap.count, reap.err)
	}

	snapshot, err := repository.Load(ctx, "heartbeat-reap-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Revision != 3 || snapshot.Run.LastEventSequence != uint64(len(snapshot.Events)) {
		t.Fatalf("Run revision/last sequence/events = %d/%d/%d, want revision 3 with continuous events", snapshot.Run.Revision, snapshot.Run.LastEventSequence, len(snapshot.Events))
	}
	for index, event := range snapshot.Events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event[%d].Sequence = %d, want %d", index, event.Sequence, index+1)
		}
	}
	if task := snapshot.Run.Tasks[0]; task.Status != workflow.TaskWaitingRetry || len(task.Attempts) != 1 || task.Attempts[0].Status != workflow.AttemptInterrupted {
		t.Fatalf("settled task = %+v", task)
	}
}

func TestLeaseReapDoesNotExpireLeaseRenewedAfterCandidateScan(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	databaseURL := testpostgres.NewIsolatedDatabaseURL(t, baseURL)
	ctx := context.Background()
	repository, err := NewWithOptions(ctx, databaseURL, Options{
		WorkerHeartbeatInterval: 200 * time.Millisecond,
		LeaseDuration:           250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	worker, lease := createClaimedDispatch(t, repository, "renew-reap-workflow", "renew-reap-run", workflow.RetryPolicy{MaxAttempts: 2}, 5000)

	var leaseExpiresAt time.Time
	if err := repository.pool.QueryRow(ctx, `
		SELECT lease_expires_at FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&leaseExpiresAt); err != nil {
		t.Fatal(err)
	}
	blocker, err := repository.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `
		SELECT dispatch_id FROM task_dispatches WHERE dispatch_id = $1 FOR UPDATE
	`, lease.DispatchID); err != nil {
		t.Fatal(err)
	}

	type heartbeatResult struct {
		response workerprotocol.HeartbeatResponse
		err      error
	}
	heartbeats := make(chan heartbeatResult, 1)
	go func() {
		response, err := repository.Heartbeat(ctx, worker.Summary.WorkerID, []workerprotocol.LeaseRef{{
			DispatchID: lease.DispatchID,
			LeaseToken: lease.LeaseToken,
		}})
		heartbeats <- heartbeatResult{response: response, err: err}
	}()
	waitForDispatchLockWaiter(t, repository, 1)
	if wait := time.Until(leaseExpiresAt.Add(20 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	reaps := make(chan leaseReapTestResult, 1)
	go func() {
		count, err := repository.ReapExpired(ctx, 10)
		reaps <- leaseReapTestResult{count: count, err: err}
	}()
	// 旧实现会在首次扫描后排队等待 Dispatch 锁；修复后会跳过被心跳占用的行并直接返回。
	earlyReap := waitForReapResultOrSecondLockWaiter(t, repository, reaps)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	heartbeat := <-heartbeats
	if heartbeat.err != nil {
		t.Fatal(heartbeat.err)
	}
	if len(heartbeat.response.Leases) != 1 || heartbeat.response.Leases[0].Status != workerprotocol.LeaseRenewed {
		t.Fatalf("Heartbeat() = %+v, want one renewed lease", heartbeat.response)
	}
	reap := leaseReapTestResult{}
	if earlyReap != nil {
		reap = *earlyReap
	} else {
		reap = <-reaps
	}
	if reap.err != nil || reap.count != 0 {
		t.Fatalf("ReapExpired() = %d, %v, want 0, nil after concurrent renewal", reap.count, reap.err)
	}
	var status string
	if err := repository.pool.QueryRow(ctx, `
		SELECT status FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "leased" {
		t.Fatalf("Dispatch status = %s, want leased after renewal", status)
	}
}

func waitForDispatchLockWaiter(t *testing.T, repository *Repository, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := repository.pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND query LIKE '%FROM task_dispatches%'
			  AND query LIKE '%FOR UPDATE%'
		`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("did not observe %d blocked Dispatch lock waiter(s)", want)
}

func waitForReapResultOrSecondLockWaiter(t *testing.T, repository *Repository, reaps <-chan leaseReapTestResult) *leaseReapTestResult {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case result := <-reaps:
			return &result
		default:
		}
		var count int
		if err := repository.pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND query LIKE '%task_dispatches%'
		`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count >= 2 {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	rows, err := repository.pool.Query(context.Background(), `
		SELECT state, COALESCE(wait_event_type, ''), COALESCE(wait_event, ''), query
		FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()
		ORDER BY pid
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var state, waitType, waitEvent, query string
		if err := rows.Scan(&state, &waitType, &waitEvent, &query); err != nil {
			t.Fatal(err)
		}
		t.Logf("database activity: state=%s wait=%s/%s query=%q", state, waitType, waitEvent, query)
	}
	t.Fatal("ReapExpired neither returned nor waited for the Dispatch lock")
	return nil
}

func TestCompleteAndReapRaceAcceptsOnlyLeaseExpiry(t *testing.T) {
	repository := newTestRepository(t)
	worker, lease := createClaimedDispatch(t, repository, "complete-reap-race-workflow", "complete-reap-race-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_dispatches
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second',
		    attempt_deadline = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
		WHERE dispatch_id = $1
	`, lease.DispatchID); err != nil {
		t.Fatal(err)
	}
	type result struct {
		kind  string
		count int
		err   error
	}
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	go func() {
		start.Wait()
		_, err := repository.Complete(ctx, worker.Summary.WorkerID, lease.DispatchID, workerprotocol.CompleteRequest{
			LeaseToken: lease.LeaseToken,
			Result:     workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "late"},
		})
		results <- result{kind: "complete", err: err}
	}()
	go func() {
		start.Wait()
		count, err := repository.ReapExpired(ctx, 10)
		results <- result{kind: "reap", count: count, err: err}
	}()
	start.Done()
	for range 2 {
		got := <-results
		if got.kind == "complete" && !errors.Is(got.err, ErrLeaseLost) {
			t.Fatalf("Complete() error = %v, want ErrLeaseLost", got.err)
		}
		if got.kind == "reap" && (got.err != nil || got.count != 1) {
			t.Fatalf("ReapExpired() = %d, %v, want 1, nil", got.count, got.err)
		}
	}
	snapshot, err := repository.Load(ctx, "complete-reap-race-run")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Revision != 3 || snapshot.Run.Tasks[0].Status != workflow.TaskWaitingRetry || snapshot.Run.Tasks[0].Attempts[0].Status != workflow.AttemptInterrupted || snapshot.Run.LastEventSequence != uint64(len(snapshot.Events)) {
		t.Fatalf("reaped Run = %+v events=%d", snapshot.Run, len(snapshot.Events))
	}
}

func TestWorkerOfflineAfterThreeMissedHeartbeatIntervals(t *testing.T) {
	repository := newTestRepository(t)
	worker, _ := createClaimedDispatch(t, repository, "offline-workflow", "offline-run", workflow.RetryPolicy{MaxAttempts: 2}, 60_000)
	ctx := context.Background()
	if _, err := repository.pool.Exec(ctx, `
		UPDATE worker_sessions
		SET registered_at = CURRENT_TIMESTAMP - INTERVAL '20 seconds',
		    last_heartbeat_at = CURRENT_TIMESTAMP - INTERVAL '16 seconds'
		WHERE worker_id = $1
	`, worker.Summary.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ReapExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
	summary, err := repository.GetWorkerSummary(ctx, worker.Summary.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Status != workerprotocol.WorkerOffline {
		t.Fatalf("Worker status = %s, want offline", summary.Status)
	}
	if _, err := repository.AuthenticateWorker(ctx, worker.Summary.WorkerID, worker.SessionToken, workerprotocol.ProtocolVersion, workerprotocol.OperationHeartbeat); !errors.Is(err, ErrWorkerSessionInvalid) {
		t.Fatalf("offline authentication error = %v, want ErrWorkerSessionInvalid", err)
	}
}

func TestRepositoryWorkerTimingOptionsControlLeaseAndOfflineThreshold(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	ctx := context.Background()
	repository, err := NewWithOptions(ctx, databaseURL, Options{
		WorkerHeartbeatInterval: 20 * time.Millisecond,
		LeaseDuration:           100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	worker, lease := createClaimedDispatch(t, repository, "configured-timing-workflow", "configured-timing-run", workflow.RetryPolicy{MaxAttempts: 2}, 5000)
	var leasedAt, leaseExpiresAt time.Time
	if err := repository.pool.QueryRow(ctx, `
		SELECT leased_at, lease_expires_at FROM task_dispatches WHERE dispatch_id = $1
	`, lease.DispatchID).Scan(&leasedAt, &leaseExpiresAt); err != nil {
		t.Fatal(err)
	}
	remaining := leaseExpiresAt.Sub(leasedAt)
	if remaining < 90*time.Millisecond || remaining > 110*time.Millisecond {
		t.Fatalf("configured lease duration = %v, want approximately 100ms", remaining)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE worker_sessions
		SET registered_at = CURRENT_TIMESTAMP - INTERVAL '1 second',
		    last_heartbeat_at = CURRENT_TIMESTAMP - INTERVAL '70 milliseconds'
		WHERE worker_id = $1
	`, worker.Summary.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ReapExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
	summary, err := repository.GetWorkerSummary(ctx, worker.Summary.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Status != workerprotocol.WorkerOffline {
		t.Fatalf("Worker status = %s, want offline with configured heartbeat interval", summary.Status)
	}
}
