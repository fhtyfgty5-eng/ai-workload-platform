package alerting

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWebhookSinkSendsLimitedPayloadAndRetriesServerError(t *testing.T) {
	var calls atomic.Int32
	var body string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		body = string(data)
		if calls.Add(1) == 1 {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	sink, err := NewWebhookSink(client, "http://example.test/hook", time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Send(context.Background(), Event{
		Rule: QueueBacklog, Status: StatusFiring, Summary: "queue remains backlogged", StartsAt: time.Unix(1, 0), RuleVersion: "v1",
		Labels: map[string]string{"component": "control_plane", "token": "webhook-secret", "run_id": "run-secret"},
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("webhook calls = %d, want 2", calls.Load())
	}
	for _, forbidden := range []string{"token", "run_id", "task_key", "stack"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("webhook body leaked forbidden field %q: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, `"component":"control_plane"`) {
		t.Fatalf("webhook body discarded allowed label: %s", body)
	}
}

func TestWebhookSinkDoesNotRetryClientError(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusBadRequest, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	sink, err := NewWebhookSink(client, "http://example.test/hook", time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Send(context.Background(), Event{Rule: QueueBacklog, Status: StatusFiring}); err == nil {
		t.Fatal("Send() error = nil, want client error")
	}
	if calls.Load() != 1 {
		t.Fatalf("webhook calls = %d, want one for non-retryable 4xx", calls.Load())
	}
}

func TestWebhookSinkRejectsCredentialsAndHonorsTimeout(t *testing.T) {
	if _, err := NewWebhookSink(http.DefaultClient, "https://user:secret@example.com/hook", time.Second, 1); err == nil {
		t.Fatal("NewWebhookSink accepted URL credentials")
	}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })}
	sink, err := NewWebhookSink(client, "http://example.test/hook", 20*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Send(context.Background(), Event{Rule: QueueBacklog, Status: StatusFiring}); err == nil {
		t.Fatal("Send() error = nil, want timeout")
	}
}

func TestWebhookSinkDoesNotReturnConfiguredURLOnNetworkError(t *testing.T) {
	const endpoint = "https://example.test/hook?signature=private-webhook-signature"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Post", URL: request.URL.String(), Err: errors.New("connection failed")}
	})}
	sink, err := NewWebhookSink(client, endpoint, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = sink.Send(context.Background(), Event{Rule: QueueBacklog, Status: StatusFiring})
	if err == nil {
		t.Fatal("Send() error = nil, want network error")
	}
	if strings.Contains(err.Error(), "private-webhook-signature") || strings.Contains(err.Error(), endpoint) {
		t.Fatalf("Send() error leaked configured Webhook URL: %q", err)
	}
}
