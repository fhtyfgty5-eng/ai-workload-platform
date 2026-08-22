package testpostgres

import (
	"net/url"
	"testing"
)

func TestDatabaseURLForNameReplacesDatabaseAndPreservesSettings(t *testing.T) {
	got, err := databaseURLForName(
		"postgres://workload:secret@localhost:5432/workload?sslmode=disable&connect_timeout=5",
		"awp_test_123",
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/awp_test_123" {
		t.Fatalf("database path = %q, want %q", parsed.Path, "/awp_test_123")
	}
	if parsed.User.Username() != "workload" || parsed.Host != "localhost:5432" {
		t.Fatalf("connection authority changed: %s", parsed.Redacted())
	}
	if parsed.Query().Get("sslmode") != "disable" || parsed.Query().Get("connect_timeout") != "5" {
		t.Fatalf("connection settings changed: %s", parsed.RawQuery)
	}
}

func TestDatabaseURLForNameRejectsUnsupportedConnectionString(t *testing.T) {
	if _, err := databaseURLForName("host=localhost dbname=workload", "awp_test_123"); err == nil {
		t.Fatal("databaseURLForName() error = nil, want unsupported URL error")
	}
}
