// Package testpostgres 为集成测试提供相互隔离的 PostgreSQL 数据库。
package testpostgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testingTB interface {
	Helper()
	Cleanup(func())
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// NewIsolatedDatabaseURL 根据 baseURL 创建临时数据库。
// 测试关闭后续注册的 Repository 连接后，该数据库会被删除。
func NewIsolatedDatabaseURL(t testingTB, baseURL string) string {
	t.Helper()
	if strings.TrimSpace(baseURL) == "" {
		t.Fatalf("TEST_DATABASE_URL is required")
	}

	databaseName := randomDatabaseName(t)
	targetURL, err := databaseURLForName(baseURL, databaseName)
	if err != nil {
		t.Fatalf("build isolated database URL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL for test database creation: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create isolated test database: %v", err)
	}
	admin.Close()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		cleanupPool, err := pgxpool.New(cleanupCtx, baseURL)
		if err != nil {
			t.Errorf("connect to PostgreSQL for test database cleanup: %v", err)
			return
		}
		defer cleanupPool.Close()
		if _, err := cleanupPool.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop isolated test database %s: %v", databaseName, err)
		}
	})

	return targetURL
}

func randomDatabaseName(t testingTB) string {
	t.Helper()
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("generate isolated database name: %v", err)
	}
	return fmt.Sprintf("awp_test_%d_%s", os.Getpid(), hex.EncodeToString(suffix[:]))
}

func databaseURLForName(baseURL, databaseName string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	if (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return "", fmt.Errorf("TEST_DATABASE_URL must use a postgres:// or postgresql:// URL")
	}
	if databaseName == "" || strings.ContainsAny(databaseName, "/?#") {
		return "", fmt.Errorf("invalid database name")
	}
	parsed.Path = "/" + databaseName
	parsed.RawPath = ""
	return parsed.String(), nil
}
