package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/alerting"
)

// AlertSnapshot 只读取低基数聚合状态，不返回任何 Run、Task、Worker 或 Dispatch ID。
func (r *Repository) AlertSnapshot(ctx context.Context) (alerting.Snapshot, error) {
	if r == nil || r.pool == nil {
		return alerting.Snapshot{}, fmt.Errorf("PostgreSQL repository is unavailable")
	}
	var snapshot alerting.Snapshot
	if err := r.pool.QueryRow(ctx, `
		SELECT
			clock_timestamp(),
				(SELECT count(*) FROM task_dispatches WHERE status = 'pending'),
				(SELECT count(*) FROM worker_sessions WHERE status = 'active'),
				(SELECT count(*) FROM task_dispatches WHERE status = 'leased'),
				(
				SELECT COALESCE(sum(GREATEST(workers.max_concurrency - workers.active_leases, 0)), 0)
				FROM (
					SELECT w.max_concurrency,
					       count(d.dispatch_id) FILTER (WHERE d.status = 'leased') AS active_leases
					FROM worker_sessions w
					LEFT JOIN task_dispatches d ON d.worker_id = w.worker_id
					WHERE w.status = 'active'
					GROUP BY w.worker_id, w.max_concurrency
				) workers
			)
		`).Scan(&snapshot.Now, &snapshot.QueueDepth, &snapshot.OnlineWorkers, &snapshot.ActiveLeases, &snapshot.AvailableSlots); err != nil {
		return alerting.Snapshot{}, fmt.Errorf("query alert snapshot: %w", err)
	}
	stats := r.pool.Stat()
	snapshot.DBInUse = int(stats.AcquiredConns())
	snapshot.DBMax = int(stats.MaxConns())
	return snapshot, nil
}

// PoolObservation 返回连接池当前值和累计等待时长，供低基数指标采集。
func (r *Repository) PoolObservation() (total, inUse, idle int, wait time.Duration) {
	if r == nil || r.pool == nil {
		return 0, 0, 0, 0
	}
	stats := r.pool.Stat()
	return int(stats.TotalConns()), int(stats.AcquiredConns()), int(stats.IdleConns()), stats.EmptyAcquireWaitTime()
}
