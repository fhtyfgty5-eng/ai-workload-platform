package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/httpapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/testpostgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

func BenchmarkControlPlaneGetRun(b *testing.B) {
	server, runs, workflowID := newBenchmarkControlPlane(b)
	created, err := runs.Start(context.Background(), "operator", workflowID, 1, "benchmark-query-run")
	if err != nil {
		b.Fatal(err)
	}
	path := "/api/v1/runs/" + string(created.RunID)

	b.Run("sequential", func(b *testing.B) {
		for range b.N {
			benchmarkRequest(b, server.URL+path, http.MethodGet, "viewer", "", nil)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchmarkRequest(b, server.URL+path, http.MethodGet, "viewer", "", nil)
			}
		})
	})
}

func BenchmarkControlPlaneStartRun(b *testing.B) {
	server, _, workflowID := newBenchmarkControlPlane(b)
	path := server.URL + "/api/v1/workflows/" + workflowID + "/versions/1/runs"
	var keySequence atomic.Uint64

	b.ResetTimer()
	for range b.N {
		key := fmt.Sprintf("benchmark-start-%d", keySequence.Add(1))
		benchmarkRequest(b, path, http.MethodPost, "operator", key, []byte(`{}`))
	}
}

func newBenchmarkControlPlane(b *testing.B) (*httptest.Server, app.RunService, string) {
	b.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		b.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL = testpostgres.NewIsolatedDatabaseURL(b, databaseURL)
	ctx := context.Background()
	repository, err := postgres.New(ctx, databaseURL)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(repository.Close)
	if err := repository.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	workflowID := "benchmark-" + fmt.Sprint(time.Now().UnixNano())
	definition := workflow.WorkflowDefinition{
		ID:          workflowID,
		Concurrency: 1,
		Tasks:       []workflow.TaskDefinition{{Key: "one", Action: "noop", TimeoutMillis: 1000}},
	}
	definitions := app.NewWorkflowService(repository)
	if _, err := definitions.Create(ctx, "operator", "benchmark-workflow", definition); err != nil {
		b.Fatal(err)
	}
	var runSequence atomic.Uint64
	runs, err := app.NewRunService(
		repository,
		definitions,
		benchmarkEngine{},
		func(workflow.RunID) {},
		func() (workflow.RunID, error) { return workflow.RunID(fmt.Sprintf("%032x", runSequence.Add(1))), nil },
	)
	if err != nil {
		b.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(httpapi.NewHandler(httpapi.Dependencies{
		Workflows: definitions, Runs: runs,
		ViewerToken: "viewer", OperatorToken: "operator",
		Ready: func() bool { return true }, Logger: logger,
	}))
	b.Cleanup(server.Close)
	return server, runs, workflowID
}

func benchmarkRequest(b *testing.B, url, method, token, key string, body []byte) {
	b.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		b.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		b.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(response.Body)
		b.Fatalf("HTTP status = %d, body=%s", response.StatusCode, data)
	}
	var payload any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		b.Fatal(err)
	}
}

type benchmarkEngine struct{}

func (benchmarkEngine) Cancel(context.Context, workflow.RunID) error { return nil }
