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
	for _, key := range []string{"WORKLOAD_SERVER_URL", "WORKLOAD_WORKER_BOOTSTRAP_TOKEN", "WORKLOAD_WORKER_NAME"} {
		invalid := clone(base)
		delete(invalid, key)
		if _, err := Load(lookup(invalid)); err == nil {
			t.Fatalf("Load() accepted missing %s", key)
		}
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
