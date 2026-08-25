package workerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	clientBootstrapToken = "bootstrap-client-secret"
	clientSessionToken   = "session-client-secret"
)

func TestClientSendsProtocolHeadersAndEscapesPathSegments(t *testing.T) {
	var requests []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r)
		if r.URL.Path != "/api/v1/workers/worker%2Fone/claims" && r.URL.EscapedPath() != "/api/v1/workers/worker%2Fone/claims" {
			t.Fatalf("request path = %s escaped=%s, want escaped worker ID", r.URL.Path, r.URL.EscapedPath())
		}
		if r.Header.Get("Authorization") != "Bearer "+clientSessionToken || r.Header.Get(workerprotocol.ProtocolVersionHeader) != "1" {
			t.Fatalf("request headers = %+v", r.Header)
		}
		_, _ = io.WriteString(w, `{"leases":[]}`)
	}))
	defer server.Close()

	client, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Claim(context.Background(), "worker/one", clientSessionToken, 1)
	if err != nil || len(response.Leases) != 0 {
		t.Fatalf("Claim() = %+v, %v", response, err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want one", len(requests))
	}
}

func TestClientRegistrationUsesBootstrapTokenAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/workers/register" || r.Header.Get("Authorization") != "Bearer "+clientBootstrapToken {
			t.Fatalf("registration request = %s %s", r.URL.Path, r.Header.Get("Authorization"))
		}
		var request workerprotocol.RegisterRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.DisplayName != "worker" {
			t.Fatalf("registration body = %+v, %v", request, err)
		}
		writeJSON(w, http.StatusCreated, workerprotocol.RegisterResponse{WorkerID: "wrk-one", SessionToken: clientSessionToken, ProtocolVersion: 1})
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Register(context.Background(), clientBootstrapToken, workerprotocol.RegisterRequest{DisplayName: "worker", ProtocolVersion: 1, ExecutorKinds: []workflow.ExecutorKind{workflow.ExecutorMock}, MaxConcurrency: 1})
	if err != nil || response.WorkerID != "wrk-one" || response.SessionToken != clientSessionToken {
		t.Fatalf("Register() = %+v, %v", response, err)
	}
}

func TestClientLimitsResponseBodyAndMapsAPIError(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.Handler
		wantCode string
		wantErr  bool
	}{
		{name: "large response", handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 1024)) }), wantErr: true},
		{name: "api error", handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]string{"code": "lease_lost", "message": "lease expired", "request_id": "req-client"}})
		}), wantCode: "lease_lost", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			maxBody := int64(64)
			if test.wantCode != "" {
				maxBody = 1024
			}
			client, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client(), MaxBodyBytes: maxBody})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Heartbeat(context.Background(), "worker", clientSessionToken, workerprotocol.HeartbeatRequest{})
			if !test.wantErr || err == nil {
				t.Fatalf("Heartbeat() error = %v, want error", err)
			}
			var apiErr *APIError
			if errors.As(err, &apiErr) && test.wantCode != "" && apiErr.Code != test.wantCode {
				t.Fatalf("API error = %+v, want code %s", apiErr, test.wantCode)
			}
		})
	}
}

func TestClientHonorsContextCancellationAndDoesNotLogCredentials(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client(), Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Heartbeat(ctx, "worker", clientSessionToken, workerprotocol.HeartbeatRequest{})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Heartbeat() error = %v, want context cancellation", err)
	}
	if strings.Contains(logs.String(), clientSessionToken) || strings.Contains(logs.String(), clientBootstrapToken) {
		t.Fatalf("logs contain credential: %s", logs.String())
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
