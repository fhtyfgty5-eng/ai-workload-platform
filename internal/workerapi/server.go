// Package workerapi 暴露远程 Worker 使用的认证 HTTP 边界。
package workerapi

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/auth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/observability"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	defaultBodyLimit int64 = 1 << 20
	defaultPageLimit       = 50
	maxPageLimit           = 100
)

// Store 是 Worker HTTP 操作所需的最小持久化边界。
// 接口只面向协议，使认证和路由测试不依赖 PostgreSQL。
type Store interface {
	RegisterWorker(context.Context, workerprotocol.RegisterRequest) (workerprotocol.Registration, error)
	AuthenticateWorker(context.Context, string, string, int, workerprotocol.WorkerOperation) (workerprotocol.WorkerSession, error)
	Claim(context.Context, string, int) ([]workerprotocol.Lease, error)
	Heartbeat(context.Context, string, []workerprotocol.LeaseRef) (workerprotocol.HeartbeatResponse, error)
	Complete(context.Context, string, string, workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error)
	DrainWorker(context.Context, string) (workerprotocol.WorkerSummary, error)
	GetWorkerSummary(context.Context, string) (workerprotocol.WorkerSummary, error)
	ListWorkerSummaries(context.Context, string, int) ([]workerprotocol.WorkerSummary, bool, error)
}

type Options struct {
	BootstrapToken    string
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	MaxBodyBytes      int64
	Wake              func()
	Metrics           *observability.Metrics
	Tracer            oteltrace.Tracer
	Logger            *slog.Logger
}

// APIError 保存稳定的公开错误分类，以及外层 HTTP Handler 生成的 request ID。
type APIError struct {
	Status    int
	Code      string
	Message   string
	RequestID string
	Cause     error
}

func (e *APIError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Message
}

func (e *APIError) Unwrap() error { return e.Cause }

type Server struct {
	store             Store
	bootstrapToken    string
	heartbeatInterval time.Duration
	leaseDuration     time.Duration
	maxBodyBytes      int64
	wake              func()
	metrics           *observability.Metrics
	tracer            oteltrace.Tracer
	logger            *slog.Logger
}

func New(store Store, options Options) (*Server, error) {
	if store == nil {
		return nil, fmt.Errorf("Worker store is required")
	}
	if strings.TrimSpace(options.BootstrapToken) == "" {
		return nil, fmt.Errorf("Worker bootstrap token is required")
	}
	if options.HeartbeatInterval <= 0 || options.LeaseDuration <= options.HeartbeatInterval {
		return nil, fmt.Errorf("Worker lease duration must exceed the heartbeat interval")
	}
	if options.MaxBodyBytes <= 0 {
		options.MaxBodyBytes = defaultBodyLimit
	}
	if options.Wake == nil {
		options.Wake = func() {}
	}
	if options.Tracer == nil {
		options.Tracer = otel.Tracer("workload-worker-api")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Server{
		store:             store,
		bootstrapToken:    options.BootstrapToken,
		heartbeatInterval: options.HeartbeatInterval,
		leaseDuration:     options.LeaseDuration,
		maxBodyBytes:      options.MaxBodyBytes,
		wake:              options.Wake,
		metrics:           options.Metrics,
		tracer:            options.Tracer,
		logger:            options.Logger,
	}, nil
}

func Routes() []string {
	return []string{
		"/api/v1/workers/register",
		"/api/v1/workers/{worker-id}/claims",
		"/api/v1/workers/{worker-id}/heartbeat",
		"/api/v1/workers/{worker-id}/leases/{dispatch-id}/complete",
		"/api/v1/workers/{worker-id}/drain",
		"/api/v1/workers",
		"/api/v1/workers/{worker-id}",
	}
}

func (s *Server) Matches(path string) bool {
	parts := splitPath(path)
	return len(parts) >= 3 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "workers"
}

// RequiresWorkerCredentials 区分使用 bootstrap/session 凭据的路由和共用
// /workers 前缀但使用 viewer/operator 凭据的查询路由。
func (s *Server) RequiresWorkerCredentials(path string) bool {
	parts := splitPath(path)
	if len(parts) == 4 && parts[3] == "register" {
		return true
	}
	return len(parts) >= 5
}

// Handle 处理一条 Worker 路由。外层控制面 Handler 统一负责就绪检查、
// request ID、访问日志、用户角色认证和错误 JSON。
func (s *Server) Handle(w http.ResponseWriter, r *http.Request, requestID string) *APIError {
	w.Header().Set(workerprotocol.ProtocolVersionHeader, strconv.Itoa(workerprotocol.ProtocolVersion))
	parts := splitPath(r.URL.Path)
	if len(parts) < 3 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "workers" {
		return apiError(requestID, http.StatusNotFound, "not_found", "resource not found", nil)
	}

	if len(parts) == 4 && parts[3] == "register" {
		if r.Method != http.MethodPost {
			return methodError(requestID)
		}
		if !s.matchesBootstrapToken(r.Header.Get("Authorization")) {
			return apiError(requestID, http.StatusUnauthorized, "unauthorized", "authentication required", auth.ErrUnauthorized)
		}
		var request workerprotocol.RegisterRequest
		if err := s.decodeJSON(w, r, &request, true); err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		startedAt := time.Now()
		operationCtx, span := observability.StartSpan(r.Context(), s.tracer, "worker.register")
		registration, err := s.store.RegisterWorker(operationCtx, request)
		finishOperationSpan(span, err)
		fields := observability.LogFields{RequestID: requestID, Operation: "worker_register"}
		if err == nil {
			fields.WorkerID = registration.Summary.WorkerID
		}
		s.observe(operationCtx, fields, startedAt, err)
		if err != nil {
			return mapError(requestID, err)
		}
		// 新增兼容容量后，数据库中已有的 Ready 任务可能立即具备分发条件。
		s.wake()
		writeJSON(w, http.StatusCreated, workerprotocol.RegisterResponse{
			WorkerID:                registration.Summary.WorkerID,
			SessionToken:            registration.SessionToken,
			HeartbeatIntervalMillis: s.heartbeatInterval.Milliseconds(),
			LeaseDurationMillis:     s.leaseDuration.Milliseconds(),
			ProtocolVersion:         workerprotocol.ProtocolVersion,
		})
		return nil
	}

	if len(parts) == 3 {
		if r.Method != http.MethodGet {
			return methodError(requestID)
		}
		limit, err := pageLimit(r)
		if err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		after, err := decodeCursor(r.URL.Query().Get("cursor"))
		if err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", "invalid Worker cursor", err)
		}
		items, more, err := s.store.ListWorkerSummaries(r.Context(), after, limit)
		if err != nil {
			return mapError(requestID, err)
		}
		response := workerPage{Items: items}
		if more && len(items) > 0 {
			response.NextCursor = encodeCursor(items[len(items)-1].WorkerID)
		}
		writeJSON(w, http.StatusOK, response)
		return nil
	}

	workerID, err := url.PathUnescape(pathPart(parts, 3))
	if err != nil || strings.TrimSpace(workerID) == "" {
		return apiError(requestID, http.StatusBadRequest, "invalid_request", "invalid Worker ID", err)
	}
	if len(parts) == 4 {
		if r.Method != http.MethodGet {
			return methodError(requestID)
		}
		summary, err := s.store.GetWorkerSummary(r.Context(), workerID)
		if err != nil {
			return mapError(requestID, err)
		}
		writeJSON(w, http.StatusOK, summary)
		return nil
	}

	if r.Method != http.MethodPost {
		return methodError(requestID)
	}
	operation, dispatchID, ok := workerOperation(parts)
	if !ok {
		return apiError(requestID, http.StatusNotFound, "not_found", "resource not found", nil)
	}
	protocolVersion, err := requestProtocolVersion(r)
	if err != nil {
		return apiError(requestID, http.StatusBadRequest, "worker_protocol_unsupported", "Worker protocol version is not supported", err)
	}
	sessionToken, err := auth.ParseBearerToken(r.Header.Get("Authorization"))
	if err != nil {
		return apiError(requestID, http.StatusUnauthorized, "worker_session_invalid", "Worker session is invalid", err)
	}
	if _, err := s.store.AuthenticateWorker(r.Context(), workerID, sessionToken, protocolVersion, operation); err != nil {
		return mapError(requestID, err)
	}

	switch operation {
	case workerprotocol.OperationClaim:
		var request workerprotocol.ClaimRequest
		if err := s.decodeJSON(w, r, &request, true); err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		startedAt := time.Now()
		operationCtx, span := observability.StartSpan(r.Context(), s.tracer, "worker.claim")
		leases, err := s.store.Claim(operationCtx, workerID, request.Slots)
		finishOperationSpan(span, err)
		s.observe(operationCtx, observability.LogFields{RequestID: requestID, WorkerID: workerID, Operation: "claim"}, startedAt, err)
		if err != nil {
			return mapError(requestID, err)
		}
		if leases == nil {
			leases = []workerprotocol.Lease{}
		}
		writeJSON(w, http.StatusOK, workerprotocol.ClaimResponse{Leases: leases})
	case workerprotocol.OperationHeartbeat:
		var request workerprotocol.HeartbeatRequest
		if err := s.decodeJSON(w, r, &request, true); err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		startedAt := time.Now()
		operationCtx, span := observability.StartSpan(r.Context(), s.tracer, "worker.heartbeat")
		response, err := s.store.Heartbeat(operationCtx, workerID, request.Leases)
		finishOperationSpan(span, err)
		s.observe(operationCtx, observability.LogFields{RequestID: requestID, WorkerID: workerID, Operation: "heartbeat"}, startedAt, err)
		if err != nil {
			return mapError(requestID, err)
		}
		if response.Leases == nil {
			response.Leases = []workerprotocol.LeaseHeartbeat{}
		}
		writeJSON(w, http.StatusOK, response)
	case workerprotocol.OperationComplete:
		var request workerprotocol.CompleteRequest
		if err := s.decodeJSON(w, r, &request, true); err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		startedAt := time.Now()
		operationCtx, span := observability.StartSpan(r.Context(), s.tracer, "worker.complete")
		response, err := s.store.Complete(operationCtx, workerID, dispatchID, request)
		finishOperationSpan(span, err)
		s.observe(operationCtx, observability.LogFields{RequestID: requestID, WorkerID: workerID, DispatchID: dispatchID, Operation: "complete"}, startedAt, err)
		if err != nil {
			return mapError(requestID, err)
		}
		// 任务完成可能释放 Workflow 并发额度，并使下游任务进入 Ready。
		s.wake()
		writeJSON(w, http.StatusOK, response)
	case workerprotocol.OperationDrain:
		if err := s.decodeJSON(w, r, &struct{}{}, false); err != nil {
			return apiError(requestID, http.StatusBadRequest, "invalid_request", err.Error(), err)
		}
		startedAt := time.Now()
		operationCtx, span := observability.StartSpan(r.Context(), s.tracer, "worker.drain")
		response, err := s.store.DrainWorker(operationCtx, workerID)
		finishOperationSpan(span, err)
		s.observe(operationCtx, observability.LogFields{RequestID: requestID, WorkerID: workerID, Operation: "worker_drain"}, startedAt, err)
		if err != nil {
			return mapError(requestID, err)
		}
		writeJSON(w, http.StatusOK, response)
	}
	return nil
}

func finishOperationSpan(span oteltrace.Span, err error) {
	outcome := "success"
	errorCode := ""
	if err != nil {
		outcome = "error"
		errorCode = workerOperationErrorCode(err)
	}
	observability.FinishSpan(span, outcome, errorCode)
	span.End()
}

// observe 只记录低基数操作结果；观测失败不会改变 Worker API 的返回值。
func (s *Server) observe(ctx context.Context, fields observability.LogFields, startedAt time.Time, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
		fields.ErrorCode = workerOperationErrorCode(err)
	}
	fields.Duration = time.Since(startedAt).Milliseconds()
	if s.metrics != nil {
		s.metrics.ObserveOperation(fields.Operation, outcome, fields.ErrorCode, time.Since(startedAt))
	}
	logger := observability.LoggerFromContext(observability.WithFields(ctx, fields), s.logger)
	if err != nil {
		logger.Warn("worker operation failed", "error", err)
	} else {
		logger.Debug("worker operation completed")
	}
}

func workerOperationErrorCode(err error) string {
	if err == nil {
		return ""
	}
	return observability.NormalizeErrorCode(mapError("", err).Code)
}

type workerPage struct {
	Items      []workerprotocol.WorkerSummary `json:"items"`
	NextCursor string                         `json:"next_cursor,omitempty"`
}

func workerOperation(parts []string) (workerprotocol.WorkerOperation, string, bool) {
	switch {
	case len(parts) == 5 && parts[4] == "claims":
		return workerprotocol.OperationClaim, "", true
	case len(parts) == 5 && parts[4] == "heartbeat":
		return workerprotocol.OperationHeartbeat, "", true
	case len(parts) == 5 && parts[4] == "drain":
		return workerprotocol.OperationDrain, "", true
	case len(parts) == 7 && parts[4] == "leases" && parts[6] == "complete":
		dispatchID, err := url.PathUnescape(parts[5])
		return workerprotocol.OperationComplete, dispatchID, err == nil && strings.TrimSpace(dispatchID) != ""
	default:
		return "", "", false
	}
}

func requestProtocolVersion(r *http.Request) (int, error) {
	value := r.Header.Get(workerprotocol.ProtocolVersionHeader)
	version, err := strconv.Atoi(value)
	if err != nil || version != workerprotocol.ProtocolVersion {
		return 0, postgres.ErrWorkerProtocolUnsupported
	}
	return version, nil
}

func (s *Server) matchesBootstrapToken(header string) bool {
	token, err := auth.ParseBearerToken(header)
	if err != nil || len(token) != len(s.bootstrapToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.bootstrapToken)) == 1
}

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, target any, required bool) error {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		if !required && errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func mapError(requestID string, err error) *APIError {
	switch {
	case errors.Is(err, postgres.ErrWorkerSessionInvalid):
		return apiError(requestID, http.StatusUnauthorized, "worker_session_invalid", "Worker session is invalid", err)
	case errors.Is(err, postgres.ErrWorkerProtocolUnsupported):
		return apiError(requestID, http.StatusBadRequest, "worker_protocol_unsupported", "Worker protocol version is not supported", err)
	case errors.Is(err, postgres.ErrWorkerDraining):
		return apiError(requestID, http.StatusConflict, "worker_draining", "Worker is draining", err)
	case errors.Is(err, postgres.ErrWorkerCapacityExceeded):
		return apiError(requestID, http.StatusConflict, "worker_capacity_exceeded", "Worker capacity is exceeded", err)
	case errors.Is(err, postgres.ErrLeaseLost):
		return apiError(requestID, http.StatusConflict, "lease_lost", "Worker lease is no longer current", err)
	case errors.Is(err, postgres.ErrResultConflict):
		return apiError(requestID, http.StatusConflict, "result_conflict", "Worker lease result conflicts with an earlier result", err)
	case errors.Is(err, postgres.ErrInvalidWorkerRegistration), errors.Is(err, postgres.ErrInvalidResult):
		return apiError(requestID, http.StatusBadRequest, "invalid_request", "request is invalid", err)
	case errors.Is(err, postgres.ErrWorkerNotFound):
		return apiError(requestID, http.StatusNotFound, "not_found", "resource not found", err)
	default:
		return apiError(requestID, http.StatusInternalServerError, "internal_error", "internal server error", err)
	}
}

func methodError(requestID string) *APIError {
	return apiError(requestID, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
}

func apiError(requestID string, status int, code, message string, cause error) *APIError {
	return &APIError{Status: status, Code: code, Message: message, RequestID: requestID, Cause: cause}
}

func pageLimit(r *http.Request) (int, error) {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return defaultPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 || limit > maxPageLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", maxPageLimit)
	}
	return limit, nil
}

func encodeCursor(workerID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(workerID))
}

func decodeCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(decoded) == 0 {
		return "", fmt.Errorf("invalid cursor")
	}
	return string(decoded), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func pathPart(parts []string, index int) string {
	if index < 0 || index >= len(parts) {
		return ""
	}
	return parts[index]
}

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}
