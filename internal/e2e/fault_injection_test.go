package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/faultinject"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestFaultInjectionWrappersCoverPlannedOperations 验证八类实验都有明确的
// 构造函数注入点；测试不依赖网络或真实模型，只检查故障不会泄漏到未配置操作。
func TestFaultInjectionWrappersCoverPlannedOperations(t *testing.T) {
	operations := []faultinject.Operation{
		faultinject.OperationPostgres,
		faultinject.OperationClaim,
		faultinject.OperationHeartbeat,
		faultinject.OperationComplete,
		faultinject.OperationCoordinatorLock,
		faultinject.OperationCoordinatorScan,
		faultinject.OperationWorkerExecute,
		faultinject.OperationPoolAcquire,
	}
	for _, operation := range operations {
		t.Run(string(operation), func(t *testing.T) {
			injected := errors.New("injected " + string(operation))
			plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
				operation: {{Err: injected}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := plan.Before(context.Background(), operation); !errors.Is(err, injected) {
				t.Fatalf("first %s injection = %v, want injected error", operation, err)
			}
			if err := plan.Before(context.Background(), operation); err != nil {
				t.Fatalf("second %s call = %v, want recovery", operation, err)
			}
		})
	}
}

func TestFaultInjectionWrappersPreserveSuccessPath(t *testing.T) {
	client := &faultClientStub{}
	clientWrapper := faultinject.NewClientWrapper(client, nil)
	if _, err := clientWrapper.Claim(context.Background(), "worker", "session", 1); err != nil {
		t.Fatal(err)
	}
	if client.claimCalls != 1 {
		t.Fatalf("underlying Claim calls = %d, want one", client.claimCalls)
	}

	executor := faultinject.NewExecutorWrapper(successExecutorStub{}, nil)
	response := executor.Execute(context.Background(), workflow.ExecutionRequest{})
	if response.Kind != workflow.ResultSuccess {
		t.Fatalf("executor response = %+v", response)
	}

	clock := faultinject.NewClockWrapper(workflow.RealClock{}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := clock.Wait(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestFaultInjectionPostgresUnavailableThenRecovers(t *testing.T) {
	env := newWorkerRecoveryEnvironment(t)
	injected := errors.New("PostgreSQL temporarily unavailable")
	plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
		faultinject.OperationPostgres: {{Err: injected}},
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := faultinject.NewRepositoryWrapper(env.repository, plan)
	if _, err := repository.ReapExpired(env.ctx, 10); !errors.Is(err, injected) {
		t.Fatalf("first ReapExpired() error = %v, want injected database error", err)
	}
	if _, err := repository.ReapExpired(env.ctx, 10); err != nil {
		t.Fatalf("second ReapExpired() error = %v, want recovered PostgreSQL call", err)
	}
}

func TestFaultInjectionClaimDelayIsCancelableThenRecovers(t *testing.T) {
	env := newWorkerRecoveryEnvironment(t)
	client := newRecoveryClient(t, env)
	worker := registerRecoveryWorker(t, client, "fault-claim-delay")
	plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
		faultinject.OperationClaim: {{Delay: 100 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper := faultinject.NewClientWrapper(client, plan)
	ctx, cancel := context.WithTimeout(env.ctx, 5*time.Millisecond)
	defer cancel()
	if _, err := wrapper.Claim(ctx, worker.WorkerID, worker.SessionToken, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed Claim() error = %v, want deadline", err)
	}
	if _, err := wrapper.Claim(env.ctx, worker.WorkerID, worker.SessionToken, 1); err != nil {
		t.Fatalf("Claim() after one-shot delay = %v", err)
	}
}

func TestFaultInjectionCompleteErrorThenRetryConverges(t *testing.T) {
	env := newWorkerRecoveryEnvironmentWithScanInterval(t, time.Hour)
	client := newRecoveryClient(t, env)
	worker := registerRecoveryWorker(t, client, "fault-complete")
	runID := createRecoveryRun(t, env, "fault-complete-"+fmt.Sprint(time.Now().UnixNano()), workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 5000})
	lease := claimRecoveryLease(t, env, client, worker)
	injected := errors.New("complete temporarily unavailable")
	plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
		faultinject.OperationComplete: {{Err: injected}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper := faultinject.NewClientWrapper(client, plan)
	request := workerprotocol.CompleteRequest{LeaseToken: lease.LeaseToken, Result: workflow.ExecutionResponse{Kind: workflow.ResultSuccess, Output: "done"}}
	if _, err := wrapper.Complete(env.ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, request); !errors.Is(err, injected) {
		t.Fatalf("first Complete() error = %v, want injected error", err)
	}
	if _, err := wrapper.Complete(env.ctx, worker.WorkerID, lease.DispatchID, worker.SessionToken, request); err != nil {
		t.Fatalf("retried Complete() error = %v", err)
	}
	waitForRunStatus(t, env.server.URL, runID, workflow.WorkflowSucceeded)
}

func TestFaultInjectionHeartbeatErrorThenRenewsLease(t *testing.T) {
	env := newWorkerRecoveryEnvironmentWithScanInterval(t, time.Hour)
	client := newRecoveryClient(t, env)
	worker := registerRecoveryWorker(t, client, "fault-heartbeat")
	createRecoveryRun(t, env, "fault-heartbeat-"+fmt.Sprint(time.Now().UnixNano()), workflow.TaskDefinition{Key: "task", Action: "mock", TimeoutMillis: 5000})
	lease := claimRecoveryLease(t, env, client, worker)
	injected := errors.New("heartbeat temporarily unavailable")
	plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
		faultinject.OperationHeartbeat: {{Err: injected}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper := faultinject.NewClientWrapper(client, plan)
	request := workerprotocol.HeartbeatRequest{Leases: []workerprotocol.LeaseRef{{DispatchID: lease.DispatchID, LeaseToken: lease.LeaseToken}}}
	if _, err := wrapper.Heartbeat(env.ctx, worker.WorkerID, worker.SessionToken, request); !errors.Is(err, injected) {
		t.Fatalf("first Heartbeat() error = %v, want injected error", err)
	}
	response, err := wrapper.Heartbeat(env.ctx, worker.WorkerID, worker.SessionToken, request)
	if err != nil || len(response.Leases) != 1 || response.Leases[0].Status != workerprotocol.LeaseRenewed {
		t.Fatalf("recovered Heartbeat() = %+v, %v", response, err)
	}
}

func TestFaultInjectionCoordinatorLockCheckThenRecovers(t *testing.T) {
	databaseURL := requiredTestDatabaseURL(t)
	databaseURL = testpostgres.NewIsolatedDatabaseURL(t, databaseURL)
	repository, err := postgres.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	lock, err := repository.AcquireAdvisoryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	injected := errors.New("coordinator lock check failed")
	plan, err := faultinject.NewPlan(map[faultinject.Operation][]faultinject.Action{
		faultinject.OperationCoordinatorLock: {{Err: injected}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper := faultinject.NewLockWrapper(lock, plan)
	if err := wrapper.Check(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("first lock Check() = %v, want injected error", err)
	}
	if err := wrapper.Check(context.Background()); err != nil {
		t.Fatalf("second lock Check() = %v, want underlying lock healthy", err)
	}
}

func TestFaultInjectionPoolExhaustionRecoversAfterRelease(t *testing.T) {
	databaseURL := testpostgres.NewIsolatedDatabaseURL(t, requiredTestDatabaseURL(t))
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		first.Release()
		t.Fatalf("second Acquire() error = %v, want pool exhaustion deadline", err)
	}
	first.Release()
	recovered, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() after release = %v", err)
	}
	recovered.Release()
}

func newRecoveryClient(t *testing.T, env *workerRecoveryEnvironment) *workerclient.Client {
	t.Helper()
	client, err := workerclient.New(workerclient.Config{BaseURL: env.server.URL, HTTPClient: env.server.Client})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func claimRecoveryLease(t *testing.T, env *workerRecoveryEnvironment, client *workerclient.Client, worker workerprotocol.RegisterResponse) workerprotocol.Lease {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		claim, err := client.Claim(env.ctx, worker.WorkerID, worker.SessionToken, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(claim.Leases) == 1 {
			return claim.Leases[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Worker did not receive a lease")
	return workerprotocol.Lease{}
}

func requiredTestDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	return databaseURL
}

type faultClientStub struct{ claimCalls int }

func (s *faultClientStub) Register(context.Context, string, workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error) {
	return workerprotocol.RegisterResponse{WorkerID: "worker", SessionToken: "session"}, nil
}
func (s *faultClientStub) Claim(context.Context, string, string, int) (workerprotocol.ClaimResponse, error) {
	s.claimCalls++
	return workerprotocol.ClaimResponse{}, nil
}
func (s *faultClientStub) Heartbeat(context.Context, string, string, workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error) {
	return workerprotocol.HeartbeatResponse{}, nil
}
func (s *faultClientStub) Complete(context.Context, string, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	return workerprotocol.CompleteResponse{Applied: true}, nil
}
func (s *faultClientStub) Drain(context.Context, string, string) (workerprotocol.WorkerSummary, error) {
	return workerprotocol.WorkerSummary{}, nil
}

type successExecutorStub struct{}

func (successExecutorStub) Execute(context.Context, workflow.ExecutionRequest) workflow.ExecutionResponse {
	return workflow.ExecutionResponse{Kind: workflow.ResultSuccess}
}
