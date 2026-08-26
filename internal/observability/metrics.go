package observability

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var httpMetricRoutes = []string{
	"/health/live", "/health/ready", "/metrics", "unmatched",
	"/api/v1/workflows", "/api/v1/workflows/{workflow-id}",
	"/api/v1/workflows/{workflow-id}/versions", "/api/v1/workflows/{workflow-id}/versions/{version}",
	"/api/v1/workflows/{workflow-id}/versions/{version}/runs",
	"/api/v1/runs", "/api/v1/runs/{run-id}", "/api/v1/runs/{run-id}/tasks",
	"/api/v1/runs/{run-id}/tasks/{task-key}", "/api/v1/runs/{run-id}/events", "/api/v1/runs/{run-id}/cancel",
	"/api/v1/workers", "/api/v1/workers/register", "/api/v1/workers/{worker-id}",
	"/api/v1/workers/{worker-id}/claims", "/api/v1/workers/{worker-id}/heartbeat", "/api/v1/workers/{worker-id}/drain",
	"/api/v1/workers/{worker-id}/leases/{dispatch-id}/complete",
}

var operationalErrorCodes = []string{
	"unknown", "auth_config_invalid", "database_unavailable", "forbidden", "idempotency_conflict",
	"internal_error", "invalid_request", "lease_lost", "method_not_allowed", "not_found", "not_ready",
	"result_conflict", "timeout", "unauthorized", "worker_capacity_exceeded", "worker_draining",
	"worker_protocol_unsupported", "worker_session_invalid", "workflow_exists",
}

// Metrics 持有模块 5 的低基数指标；注册器由调用方传入以隔离测试实例。
type Metrics struct {
	Registry     *prometheus.Registry
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	operations   *prometheus.HistogramVec
	queueDepth   *prometheus.GaugeVec
	workers      *prometheus.GaugeVec
	activeLeases prometheus.Gauge
	dbPool       *prometheus.GaugeVec
	alerts       *prometheus.CounterVec
	leaseReclaim prometheus.Histogram
	dbPoolWait   prometheus.Counter
	dbPoolWaitMu sync.Mutex
	lastDBWait   time.Duration
	// alertMu 保护固定容量的秒级窗口桶；固定数组避免控制面长期运行时累计样本占用内存。
	alertMu      sync.Mutex
	alertBuckets [64]alertCounterBucket
	now          func() time.Time
}

type alertCounterBucket struct {
	second             int64
	completeTotal      int
	completeErrors     int
	leaseReclaimErrors int
}

// NewMetrics 在调用方提供的独立 Registry 注册全部模块 5 指标。
func NewMetrics(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{Registry: reg, now: time.Now,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "workload_http_requests_total", Help: "HTTP requests handled by route and status."}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "workload_http_request_duration_seconds", Help: "HTTP request duration in seconds."}, []string{"method", "route"}),
		operations:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "workload_operation_duration_seconds", Help: "Workload operation duration in seconds."}, []string{"operation", "outcome", "error_code"}),
		queueDepth:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "workload_queue_depth", Help: "Current queue depth by dispatch status."}, []string{"status"}),
		workers:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "workload_workers", Help: "Current Worker count by status."}, []string{"status"}),
		activeLeases: prometheus.NewGauge(prometheus.GaugeOpts{Name: "workload_active_leases", Help: "Current active lease count."}),
		dbPool:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "workload_db_pool_connections", Help: "Current database pool connections by state."}, []string{"state"}),
		alerts:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "workload_alerts_total", Help: "Alert lifecycle events."}, []string{"rule", "operation", "outcome"}),
		leaseReclaim: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "workload_lease_reclaim_duration_seconds", Help: "Lease reclaim duration in seconds."}),
		dbPoolWait:   prometheus.NewCounter(prometheus.CounterOpts{Name: "workload_db_pool_wait_seconds_total", Help: "Cumulative time spent waiting for a database pool connection."}),
	}
	// 同一 Registry 可能被同一进程的多个组件复用；重复构造时必须继续操作已注册的 Collector。
	m.httpRequests = registerOrReuse(reg, m.httpRequests).(*prometheus.CounterVec)
	m.httpDuration = registerOrReuse(reg, m.httpDuration).(*prometheus.HistogramVec)
	m.operations = registerOrReuse(reg, m.operations).(*prometheus.HistogramVec)
	m.queueDepth = registerOrReuse(reg, m.queueDepth).(*prometheus.GaugeVec)
	m.workers = registerOrReuse(reg, m.workers).(*prometheus.GaugeVec)
	m.activeLeases = registerOrReuse(reg, m.activeLeases).(prometheus.Gauge)
	m.dbPool = registerOrReuse(reg, m.dbPool).(*prometheus.GaugeVec)
	m.alerts = registerOrReuse(reg, m.alerts).(*prometheus.CounterVec)
	m.leaseReclaim = registerOrReuse(reg, m.leaseReclaim).(prometheus.Histogram)
	m.dbPoolWait = registerOrReuse(reg, m.dbPoolWait).(prometheus.Counter)
	return m
}

func registerOrReuse(reg *prometheus.Registry, collector prometheus.Collector) prometheus.Collector {
	if err := reg.Register(collector); err != nil {
		if alreadyRegistered, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return alreadyRegistered.ExistingCollector
		}
		panic(err)
	}
	return collector
}

// ObserveHTTP 只接受有限方法和路由模板，拒绝把原始 URL 或资源 ID 变成标签。
func (m *Metrics) ObserveHTTP(method, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	method = normalize(method, []string{"GET", "POST", "PUT", "PATCH", "DELETE"})
	route = normalize(route, httpMetricRoutes)
	m.httpRequests.WithLabelValues(method, route, normalizeHTTPStatus(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}

// ObserveOperation 记录控制面和 Worker 协议操作，并把 Complete/回收结果写入滚动告警窗口。
func (m *Metrics) ObserveOperation(operation, outcome, errorCode string, duration time.Duration) {
	if m != nil {
		m.operations.WithLabelValues(normalize(operation, []string{"claim", "heartbeat", "complete", "dispatch_schedule", "lease_reap", "worker_register", "worker_drain", "http_request"}), normalize(outcome, []string{"success", "error", "unknown"}), NormalizeErrorCode(errorCode)).Observe(duration.Seconds())
		m.recordAlertCounter(operation, outcome)
	}
}

// AlertCounters 返回指定时间窗口内的 Complete 和租约回收计数。
// 模块 5 使用 30 秒窗口；超过固定桶容量的窗口会被限制在最近 64 秒。
func (m *Metrics) AlertCounters(now time.Time, window time.Duration) (completeTotal, completeErrors, leaseReclaimErrors int) {
	if m == nil || window <= 0 {
		return 0, 0, 0
	}
	cutoff := now.Add(-window).Unix()
	m.alertMu.Lock()
	defer m.alertMu.Unlock()
	for _, bucket := range m.alertBuckets {
		if bucket.second < cutoff || bucket.second > now.Unix() {
			continue
		}
		completeTotal += bucket.completeTotal
		completeErrors += bucket.completeErrors
		leaseReclaimErrors += bucket.leaseReclaimErrors
	}
	return completeTotal, completeErrors, leaseReclaimErrors
}

func (m *Metrics) recordAlertCounter(operation, outcome string) {
	if operation != "complete" && !(operation == "lease_reap" && outcome == "error") {
		return
	}
	second := m.now().Unix()
	index := second % int64(len(m.alertBuckets))
	if index < 0 {
		index += int64(len(m.alertBuckets))
	}
	m.alertMu.Lock()
	defer m.alertMu.Unlock()
	bucket := &m.alertBuckets[index]
	if bucket.second != second {
		*bucket = alertCounterBucket{second: second}
	}
	if operation == "complete" {
		bucket.completeTotal++
		if outcome == "error" {
			bucket.completeErrors++
		}
	} else {
		bucket.leaseReclaimErrors++
	}
}
func (m *Metrics) SetQueueDepth(status string, depth int) {
	if m != nil {
		m.queueDepth.WithLabelValues(normalize(status, []string{"pending", "leased", "queued", "unknown"})).Set(float64(depth))
	}
}
func (m *Metrics) SetWorkers(status string, count int) {
	if m != nil {
		m.workers.WithLabelValues(normalize(status, []string{"active", "draining", "offline", "stopped", "unknown"})).Set(float64(count))
	}
}
func (m *Metrics) SetActiveLeases(count int) {
	if m != nil {
		m.activeLeases.Set(float64(count))
	}
}
func (m *Metrics) SetDBPool(state string, count int) {
	if m != nil {
		m.dbPool.WithLabelValues(normalize(state, []string{"total", "in_use", "idle", "unknown"})).Set(float64(count))
	}
}
func (m *Metrics) ObserveAlert(rule, operation, outcome string) {
	if m != nil {
		m.alerts.WithLabelValues(normalize(rule, []string{"queue_backlog", "workers_offline", "lease_reclaim_errors", "complete_error_rate", "db_pool_near_exhaustion"}), normalize(operation, []string{"firing", "resolved", "notify"}), normalize(outcome, []string{"success", "error", "unknown"})).Inc()
	}
}

func (m *Metrics) ObserveLeaseReclaim(duration time.Duration) {
	if m != nil {
		m.leaseReclaim.Observe(duration.Seconds())
	}
}
func (m *Metrics) ObserveDBPoolWait(duration time.Duration) {
	if m == nil || duration < 0 {
		return
	}
	m.dbPoolWaitMu.Lock()
	defer m.dbPoolWaitMu.Unlock()
	if duration < m.lastDBWait {
		m.lastDBWait = 0
	}
	delta := duration - m.lastDBWait
	if delta > 0 {
		m.dbPoolWait.Add(delta.Seconds())
	}
	m.lastDBWait = duration
}

func GatherText(m *Metrics) (string, error) {
	if m == nil {
		m = NewMetrics(nil)
	}
	// promhttp 的 handler 负责标准文本编码；该辅助函数只供测试读取。
	collector := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	response := &captureResponse{header: make(http.Header)}
	collector.ServeHTTP(response, &http.Request{})
	return string(response.body), nil
}

type captureResponse struct {
	header http.Header
	body   []byte
	status int
}

func (c *captureResponse) Header() http.Header    { return c.header }
func (c *captureResponse) WriteHeader(status int) { c.status = status }
func (c *captureResponse) Write(data []byte) (int, error) {
	c.body = append(c.body, data...)
	return len(data), nil
}

// MetricsHandler 返回只读取 Registry 的 Prometheus 文本处理器，不访问业务数据库。
func MetricsHandler(m *Metrics) http.Handler {
	if m == nil {
		m = NewMetrics(nil)
	}
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func normalize(value string, allowed []string) string {
	if value == "" {
		return "unknown"
	}
	if allowed == nil {
		return value
	}
	for _, item := range allowed {
		if value == item {
			return value
		}
	}
	return "unknown"
}

// NormalizeErrorCode 把日志、指标和 Span 的错误分类限制为统一的低基数集合。
func NormalizeErrorCode(value string) string {
	return normalize(value, operationalErrorCodes)
}

func normalizeHTTPStatus(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return strconv.Itoa(status)
}
