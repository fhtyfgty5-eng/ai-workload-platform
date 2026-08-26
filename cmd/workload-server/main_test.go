package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/config"
)

func TestMigrateRequiresDatabaseURL(t *testing.T) {
	if err := migrate(context.Background(), ""); err == nil {
		t.Fatal("migrate() error = nil, want missing database URL")
	}
}

func TestNewLoggerUsesConfiguredFormatAndLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := newLogger(config.Config{LogLevel: "warn", LogFormat: "json"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Warn("visible", "error", errors.New("Authorization: Bearer production-secret"))
	got := output.String()
	if strings.Contains(got, "hidden") || !strings.Contains(got, `"level":"WARN"`) || !strings.Contains(got, `"msg":"visible"`) {
		t.Fatalf("logger output = %q", got)
	}
	if strings.Contains(got, "production-secret") || !strings.Contains(got, "<redacted>") {
		t.Fatalf("logger leaked secret: %q", got)
	}
}

func TestSuperviseServerReturnsCoordinatorFatalAfterShutdown(t *testing.T) {
	fatal := errors.New("coordinator lock lost")
	coordinator := newFakeLifecycleCoordinator(fatal)
	server := newBlockingHTTPServer()

	err := superviseServer(context.Background(), server, coordinator, time.Second)
	if !errors.Is(err, fatal) {
		t.Fatalf("superviseServer() error = %v, want %v", err, fatal)
	}
	if !server.shutdownCalled || !coordinator.closeCalled {
		t.Fatalf("shutdownCalled=%v closeCalled=%v, want true/true", server.shutdownCalled, coordinator.closeCalled)
	}
}

func TestSuperviseServerReturnsNilAfterNormalContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	coordinator := newFakeLifecycleCoordinator(nil)
	server := newBlockingHTTPServer()

	if err := superviseServer(ctx, server, coordinator, time.Second); err != nil {
		t.Fatalf("superviseServer() error = %v, want nil", err)
	}
	if !server.shutdownCalled || !coordinator.closeCalled {
		t.Fatalf("shutdownCalled=%v closeCalled=%v, want true/true", server.shutdownCalled, coordinator.closeCalled)
	}
}

func TestSuperviseServerReturnsHTTPListenerErrorAfterShutdown(t *testing.T) {
	listenerErr := errors.New("listener failed")
	coordinator := newFakeLifecycleCoordinator(nil)
	server := &failingHTTPServer{err: listenerErr}

	err := superviseServer(context.Background(), server, coordinator, time.Second)
	if !errors.Is(err, listenerErr) {
		t.Fatalf("superviseServer() error = %v, want %v", err, listenerErr)
	}
	if !server.shutdownCalled || !coordinator.closeCalled {
		t.Fatalf("shutdownCalled=%v closeCalled=%v, want true/true", server.shutdownCalled, coordinator.closeCalled)
	}
}

type fakeLifecycleCoordinator struct {
	errors      chan error
	closeCalled bool
}

func newFakeLifecycleCoordinator(fatal error) *fakeLifecycleCoordinator {
	errors := make(chan error, 1)
	if fatal != nil {
		errors <- fatal
	}
	return &fakeLifecycleCoordinator{errors: errors}
}

func (c *fakeLifecycleCoordinator) Errors() <-chan error { return c.errors }

func (c *fakeLifecycleCoordinator) Close(context.Context) error {
	c.closeCalled = true
	return nil
}

type blockingHTTPServer struct {
	stopped        chan struct{}
	stopOnce       sync.Once
	shutdownCalled bool
}

func newBlockingHTTPServer() *blockingHTTPServer {
	return &blockingHTTPServer{stopped: make(chan struct{})}
}

func (s *blockingHTTPServer) ListenAndServe() error {
	<-s.stopped
	return http.ErrServerClosed
}

func (s *blockingHTTPServer) Shutdown(context.Context) error {
	s.shutdownCalled = true
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

type failingHTTPServer struct {
	err            error
	shutdownCalled bool
}

func (s *failingHTTPServer) ListenAndServe() error { return s.err }

func (s *failingHTTPServer) Shutdown(context.Context) error {
	s.shutdownCalled = true
	return nil
}
