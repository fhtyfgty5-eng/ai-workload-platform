package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type embeddedMigration struct {
	name    string
	version int64
}

// Migrate applies every pending embedded migration in version order.
// 每份迁移及其版本记录位于同一事务中，SQL 失败不会留下半套表结构。
func (r *Repository) Migrate(ctx context.Context) error {
	migrations, err := embeddedMigrations()
	if err != nil {
		return err
	}
	embedded := migrationVersions(migrations)
	applied, _, err := r.appliedMigrationVersions(ctx)
	if err != nil {
		return err
	}
	if err := validateMigrationPrefix(embedded, applied); err != nil {
		return err
	}
	for index, migration := range migrations {
		if index < len(applied) {
			continue
		}
		body, err := migrationFiles.ReadFile("migrations/" + migration.name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", migration.name, err)
		}
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", migration.name, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", migration.name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", migration.version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", migration.name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", migration.name, err)
		}
	}
	applied, _, err = r.appliedMigrationVersions(ctx)
	if err != nil {
		return err
	}
	return validateMigrationSet(embedded, applied)
}

// CheckMigrations verifies the embedded schema is already applied without changing it.
func (r *Repository) CheckMigrations(ctx context.Context) error {
	embedded, err := embeddedMigrationVersions()
	if err != nil {
		return err
	}
	applied, _, err := r.appliedMigrationVersions(ctx)
	if err != nil {
		return err
	}
	return validateMigrationSet(embedded, applied)
}

func embeddedMigrations() ([]embeddedMigration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	migrations := make([]embeddedMigration, 0, len(entries))
	var previous int64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version, err := migrationVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		if version <= previous {
			return nil, fmt.Errorf("migration version %d in %q is duplicate or not increasing", version, entry.Name())
		}
		migrations = append(migrations, embeddedMigration{name: entry.Name(), version: version})
		previous = version
	}
	return migrations, nil
}

func embeddedMigrationVersions() ([]int64, error) {
	migrations, err := embeddedMigrations()
	if err != nil {
		return nil, err
	}
	return migrationVersions(migrations), nil
}

func migrationVersions(migrations []embeddedMigration) []int64 {
	versions := make([]int64, len(migrations))
	for index, migration := range migrations {
		versions[index] = migration.version
	}
	return versions
}

// appliedMigrationVersions 返回排序后的数据库版本；迁移表不存在时 exists 为 false。
func (r *Repository) appliedMigrationVersions(ctx context.Context) ([]int64, bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'schema_migrations'
		)
	`).Scan(&exists)
	if err != nil {
		return nil, false, fmt.Errorf("inspect migration table: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	rows, err := r.pool.Query(ctx, "SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, true, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()
	versions := make([]int64, 0)
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, true, fmt.Errorf("scan applied migration: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, true, fmt.Errorf("list applied migrations: %w", err)
	}
	return versions, true, nil
}

func validateMigrationPrefix(embedded, applied []int64) error {
	if len(applied) > len(embedded) || !slices.Equal(embedded[:len(applied)], applied) {
		return fmt.Errorf("database migrations %v are not a valid prefix of embedded migrations %v", applied, embedded)
	}
	return nil
}

func validateMigrationSet(embedded, applied []int64) error {
	if !slices.Equal(embedded, applied) {
		return fmt.Errorf("database migrations %v do not match embedded migrations %v", applied, embedded)
	}
	return nil
}

func migrationVersion(name string) (int64, error) {
	prefix := strings.SplitN(name, "_", 2)[0]
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("invalid migration filename %q", name)
	}
	return version, nil
}
