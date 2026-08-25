package config

import (
	"testing"
	"time"
)

func TestLoadRequiresDatabaseAndDistinctTokens(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL":                    "postgres://localhost/workload",
		"WORKLOAD_HTTP_ADDR":              ":8080",
		"WORKLOAD_VIEWER_TOKEN":           "viewer-secret",
		"WORKLOAD_OPERATOR_TOKEN":         "operator-secret",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}

	if _, err := Load(lookup(map[string]string{})); err == nil {
		t.Fatal("Load() error = nil, want missing configuration error")
	}
	duplicate := clone(base)
	duplicate["WORKLOAD_OPERATOR_TOKEN"] = duplicate["WORKLOAD_VIEWER_TOKEN"]
	if _, err := Load(lookup(duplicate)); err == nil {
		t.Fatal("Load() error = nil, want duplicate token error")
	}
	missingBootstrap := clone(base)
	missingBootstrap["WORKLOAD_WORKER_BOOTSTRAP_TOKEN"] = ""
	if _, err := Load(lookup(missingBootstrap)); err == nil {
		t.Fatal("Load() error = nil, want missing Worker bootstrap token error")
	}
	config, err := Load(lookup(base))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.DatabaseURL != base["DATABASE_URL"] || config.HTTPAddr != ":8080" {
		t.Fatalf("Config = %+v", config)
	}
}

func TestLoadRequiresWorkerBootstrapTokenToDifferFromUserTokens(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL": "postgres://localhost/workload", "WORKLOAD_HTTP_ADDR": ":8080",
		"WORKLOAD_VIEWER_TOKEN": "viewer-secret", "WORKLOAD_OPERATOR_TOKEN": "operator-secret",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	for _, value := range []string{"viewer-secret", "operator-secret"} {
		invalid := clone(base)
		invalid["WORKLOAD_WORKER_BOOTSTRAP_TOKEN"] = value
		if _, err := Load(lookup(invalid)); err == nil {
			t.Fatalf("Load() accepted Worker bootstrap token equal to %q", value)
		}
	}
}

func TestLoadValidatesDistributedExecutionIntervalsAndLimit(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL": "postgres://localhost/workload", "WORKLOAD_HTTP_ADDR": ":8080",
		"WORKLOAD_VIEWER_TOKEN": "viewer-secret", "WORKLOAD_OPERATOR_TOKEN": "operator-secret",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	valid := clone(base)
	valid["WORKLOAD_HEARTBEAT_INTERVAL"] = "5s"
	valid["WORKLOAD_LEASE_TTL"] = "15s"
	valid["WORKLOAD_LEASE_REAPER_INTERVAL"] = "1s"
	valid["WORKLOAD_DISPATCH_LIMIT"] = "100"
	if config, err := Load(lookup(valid)); err != nil || config.LeaseDuration != 15*time.Second || config.DispatchLimit != 100 {
		t.Fatalf("Load() = %+v, %v", config, err)
	}
	for _, mutation := range []func(map[string]string){
		func(values map[string]string) { values["WORKLOAD_LEASE_TTL"] = "5s" },
		func(values map[string]string) { values["WORKLOAD_DISPATCH_LIMIT"] = "0" },
		func(values map[string]string) { values["WORKLOAD_HEARTBEAT_INTERVAL"] = "invalid" },
	} {
		invalid := clone(valid)
		mutation(invalid)
		if _, err := Load(lookup(invalid)); err == nil {
			t.Fatal("Load() accepted invalid distributed execution configuration")
		}
	}
}

func TestLoadMockExecutionDelay(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL":                    "postgres://localhost/workload",
		"WORKLOAD_HTTP_ADDR":              ":8080",
		"WORKLOAD_VIEWER_TOKEN":           "viewer-secret",
		"WORKLOAD_OPERATOR_TOKEN":         "operator-secret",
		"WORKLOAD_WORKER_BOOTSTRAP_TOKEN": "worker-bootstrap-secret",
	}
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}

	config, err := Load(lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if config.MockExecutionDelay != 0 {
		t.Fatalf("default MockExecutionDelay = %v, want 0", config.MockExecutionDelay)
	}

	valid := clone(base)
	valid["WORKLOAD_MOCK_EXECUTION_DELAY"] = "30s"
	config, err = Load(lookup(valid))
	if err != nil || config.MockExecutionDelay != 30*time.Second {
		t.Fatalf("Load() = %+v, %v", config, err)
	}

	for _, value := range []string{"invalid", "-1s", "5m1ms"} {
		invalid := clone(base)
		invalid["WORKLOAD_MOCK_EXECUTION_DELAY"] = value
		if _, err := Load(lookup(invalid)); err == nil {
			t.Fatalf("Load() accepted WORKLOAD_MOCK_EXECUTION_DELAY=%q", value)
		}
	}
}

func TestConfigRejectsInvalidHTTPAddress(t *testing.T) {
	config := Config{DatabaseURL: "postgres://localhost/workload", HTTPAddr: "not-an-address", ViewerToken: "viewer", OperatorToken: "operator"}
	if err := config.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid address error")
	}
}

func clone(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
