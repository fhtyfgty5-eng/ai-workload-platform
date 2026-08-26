package workerconfig

import "testing"

func TestLoadWorkerConfigRequiresCredentialsAndValidIntervals(t *testing.T) {
	base := map[string]string{
		"WORKLOAD_SERVER_URL":              "http://127.0.0.1:8080",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN":  "worker-bootstrap-secret",
		"WORKLOAD_WORKER_NAME":             "worker-one",
		"WORKLOAD_WORKER_CONCURRENCY":      "2",
		"WORKLOAD_WORKER_POLL_MIN":         "100ms",
		"WORKLOAD_WORKER_POLL_MAX":         "2s",
		"WORKLOAD_WORKER_HEARTBEAT":        "5s",
		"WORKLOAD_WORKER_SHUTDOWN_TIMEOUT": "10s",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	config, err := Load(lookup(base))
	if err != nil || config.ServerURL != base["WORKLOAD_SERVER_URL"] || config.MaxConcurrency != 2 {
		t.Fatalf("Load() = %+v, %v", config, err)
	}
	if config.TracingMode != "off" || config.TracingServiceName != "workload-worker" {
		t.Fatalf("tracing defaults = (%q, %q)", config.TracingMode, config.TracingServiceName)
	}
	if config.LogLevel != "info" || config.LogFormat != "text" {
		t.Fatalf("logging defaults = (%q, %q)", config.LogLevel, config.LogFormat)
	}
	for _, key := range []string{"WORKLOAD_SERVER_URL", "WORKLOAD_WORKER_BOOTSTRAP_TOKEN", "WORKLOAD_WORKER_NAME"} {
		invalid := clone(base)
		delete(invalid, key)
		if _, err := Load(lookup(invalid)); err == nil {
			t.Fatalf("Load() accepted missing %s", key)
		}
	}
}

func TestWorkerConfigValidatesTracing(t *testing.T) {
	base := map[string]string{
		"WORKLOAD_SERVER_URL":             "http://127.0.0.1:8080",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
		"WORKLOAD_WORKER_NAME":            "worker-one",
		"WORKLOAD_WORKER_TRACING_MODE":    "stdout",
	}
	config, err := Load(func(key string) string { return base[key] })
	if err != nil || config.TracingMode != "stdout" {
		t.Fatalf("Load() = %+v, %v", config, err)
	}
	base["WORKLOAD_WORKER_TRACING_MODE"] = "invalid"
	if _, err := Load(func(key string) string { return base[key] }); err == nil {
		t.Fatal("Load() accepted invalid Worker tracing mode")
	}
}

func TestWorkerConfigValidatesLogging(t *testing.T) {
	base := map[string]string{
		"WORKLOAD_SERVER_URL":             "http://127.0.0.1:8080",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
		"WORKLOAD_WORKER_NAME":            "worker-one",
		"WORKLOAD_LOG_LEVEL":              "debug",
		"WORKLOAD_LOG_FORMAT":             "json",
	}
	config, err := Load(func(key string) string { return base[key] })
	if err != nil || config.LogLevel != "debug" || config.LogFormat != "json" {
		t.Fatalf("Load() = %+v, %v", config, err)
	}
	base["WORKLOAD_LOG_LEVEL"] = "verbose"
	if _, err := Load(func(key string) string { return base[key] }); err == nil {
		t.Fatal("Load() accepted invalid Worker log level")
	}
}

func TestWorkerConfigValidatesExecutionRuntime(t *testing.T) {
	base := map[string]string{
		"WORKLOAD_SERVER_URL":             "http://127.0.0.1:8080",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
		"WORKLOAD_WORKER_NAME":            "worker-one",
	}
	config, err := Load(func(key string) string { return base[key] })
	if err != nil || config.Runtime != "mock" {
		t.Fatalf("runtime default = %q, error = %v", config.Runtime, err)
	}
	for _, runtime := range []string{"docker", "kubernetes"} {
		base["WORKLOAD_WORKER_RUNTIME"] = runtime
		config, err := Load(func(key string) string { return base[key] })
		if err != nil || config.Runtime != runtime {
			t.Fatalf("runtime %q = %+v, %v", runtime, config, err)
		}
	}
	base["WORKLOAD_WORKER_RUNTIME"] = "shell"
	if _, err := Load(func(key string) string { return base[key] }); err == nil {
		t.Fatal("Load() accepted shell runtime")
	}
}

func TestWorkerConfigRequiresHTTPSForNonLoopbackServer(t *testing.T) {
	base := map[string]string{
		"WORKLOAD_SERVER_URL":              "http://example.test:8080",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN":  "worker-bootstrap-secret",
		"WORKLOAD_WORKER_NAME":             "worker-one",
		"WORKLOAD_WORKER_CONCURRENCY":      "2",
		"WORKLOAD_WORKER_POLL_MIN":         "100ms",
		"WORKLOAD_WORKER_POLL_MAX":         "2s",
		"WORKLOAD_WORKER_HEARTBEAT":        "5s",
		"WORKLOAD_WORKER_SHUTDOWN_TIMEOUT": "10s",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	if _, err := Load(lookup(base)); err == nil {
		t.Fatal("Load() accepted non-loopback HTTP Worker server")
	}
	base["WORKLOAD_SERVER_URL"] = "https://example.test:8443"
	if _, err := Load(lookup(base)); err != nil {
		t.Fatalf("Load() rejected HTTPS Worker server: %v", err)
	}
}

func clone(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
