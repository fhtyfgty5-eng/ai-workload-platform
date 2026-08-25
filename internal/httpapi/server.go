// Package httpapi 暴露版本化控制面的 HTTP 契约。
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/app"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/auth"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerapi"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const defaultBodyLimit int64 = 1 << 20

// Dependencies 保存应用服务和进程就绪状态。
type Dependencies struct {
	Workflows     app.WorkflowService
	Runs          app.RunService
	Workers       WorkerHandler
	ViewerToken   string
	OperatorToken string
	Ready         func() bool
	MaxBodyBytes  int64
	Logger        *slog.Logger
}

// WorkerHandler 使共享 HTTP 中间件不依赖 Worker 的具体存储实现。
type WorkerHandler interface {
	Matches(string) bool
	RequiresWorkerCredentials(string) bool
	Handle(http.ResponseWriter, *http.Request, string) *workerapi.APIError
}

// Routes 返回当前 Handler 实现的路径。OpenAPI 契约测试也使用该列表，
// 以保证公开文档与实际路由注册保持一致。
func Routes() []string {
	routes := []string{
		"/api/v1/workflows",
		"/api/v1/workflows/{workflow-id}",
		"/api/v1/workflows/{workflow-id}/versions",
		"/api/v1/workflows/{workflow-id}/versions/{version}",
		"/api/v1/workflows/{workflow-id}/versions/{version}/runs",
		"/api/v1/runs",
		"/api/v1/runs/{run-id}",
		"/api/v1/runs/{run-id}/tasks",
		"/api/v1/runs/{run-id}/tasks/{task-key}",
		"/api/v1/runs/{run-id}/events",
		"/api/v1/runs/{run-id}/cancel",
	}
	return append(routes, workerapi.Routes()...)
}

// NewServer 创建 HTTP 服务，但不启动网络监听。
func NewServer(deps Dependencies) *http.Server {
	return &http.Server{Handler: NewHandler(deps)}
}

func NewHandler(deps Dependencies) http.Handler {
	if deps.MaxBodyBytes <= 0 {
		deps.MaxBodyBytes = defaultBodyLimit
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	authenticator, authErr := auth.NewAuthenticator(deps.ViewerToken, deps.OperatorToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := requestID(r)
		routePath := normalizedPath(r.URL.Path)
		w.Header().Set("X-Request-ID", requestID)
		logged := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			attributes := []any{"request_id", requestID, "method", r.Method, "path", routePath, "status", logged.status}
			if logged.errorCode != "" {
				attributes = append(attributes, "error_code", logged.errorCode)
			}
			deps.Logger.Info("http request", attributes...)
		}()
		w = logged
		if r.URL.Path == "/health/live" {
			writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
			return
		}
		if r.URL.Path == "/health/ready" {
			if deps.Ready != nil && deps.Ready() {
				writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
				return
			}
			writeAPIError(w, requestID, http.StatusServiceUnavailable, "not_ready", "service is not ready")
			return
		}
		if deps.Ready != nil && !deps.Ready() {
			writeAPIError(w, requestID, http.StatusServiceUnavailable, "not_ready", "service is not ready")
			return
		}
		// Bootstrap 和会话路由拥有独立 Bearer Token，必须在 viewer/operator
		// 认证前分发，避免两类凭据进入同一个认证域。
		if deps.Workers != nil && deps.Workers.Matches(r.URL.Path) && deps.Workers.RequiresWorkerCredentials(r.URL.Path) {
			handleWorkerRequest(w, r, deps, requestID, routePath)
			return
		}
		if authErr != nil {
			writeAPIError(w, requestID, http.StatusInternalServerError, "auth_config_invalid", "authentication configuration is invalid")
			return
		}
		role, err := authenticator.Role(r.Header.Get("Authorization"))
		if err != nil {
			writeAPIError(w, requestID, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if deps.Workers != nil && deps.Workers.Matches(r.URL.Path) {
			handleWorkerRequest(w, r, deps, requestID, routePath)
			return
		}
		if err := dispatch(r.Context(), w, r, deps, role, requestID); err != nil {
			status, code, message := mappedError(err)
			if status == http.StatusInternalServerError {
				// 只记录服务端诊断，不记录请求 Header、Body 或配置中的凭据。
				deps.Logger.Error("http request failed", "request_id", requestID, "method", r.Method, "path", routePath, "error_code", code, "error", err)
			}
			writeAPIError(w, requestID, status, code, message)
		}
	})
}

func handleWorkerRequest(w http.ResponseWriter, r *http.Request, deps Dependencies, requestID, routePath string) {
	apiErr := deps.Workers.Handle(w, r, requestID)
	if apiErr == nil {
		return
	}
	if apiErr.Status == http.StatusInternalServerError {
		// Worker 错误沿用控制面错误的规则，只记录非敏感元数据。
		deps.Logger.Error("http request failed", "request_id", requestID, "method", r.Method, "path", routePath, "error_code", apiErr.Code, "error", apiErr)
	}
	writeAPIError(w, requestID, apiErr.Status, apiErr.Code, apiErr.Message)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status    int
	errorCode string
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func dispatch(ctx context.Context, w http.ResponseWriter, r *http.Request, deps Dependencies, role auth.Role, requestID string) error {
	parts := splitPath(r.URL.Path)
	if len(parts) == 3 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "workflows" {
		if r.Method == http.MethodGet {
			if deps.Workflows == nil {
				return errors.New("workflow service is unavailable")
			}
			page, err := deps.Workflows.List(ctx, r.URL.Query().Get("cursor"), queryLimit(r))
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, page)
			return nil
		}
		if r.Method != http.MethodPost {
			return errMethodNotAllowed
		}
		if role != auth.OperatorRole {
			writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
			return nil
		}
		if deps.Workflows == nil {
			return errors.New("workflow service is unavailable")
		}
		var definition workflow.WorkflowDefinition
		if err := decodeJSON(w, r, &definition, deps.MaxBodyBytes, true); err != nil {
			return errBadRequest(err)
		}
		key, err := requiredHeader(r, "Idempotency-Key")
		if err != nil {
			return errBadRequest(err)
		}
		ref, err := deps.Workflows.Create(ctx, principal(role), key, definition)
		if err != nil {
			return err
		}
		deps.Logger.Info("workflow version created", "workflow_id", ref.WorkflowID, "workflow_version", ref.Version)
		writeJSON(w, http.StatusCreated, ref)
		return nil
	}
	if len(parts) == 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "workflows" && r.Method == http.MethodGet {
		if deps.Workflows == nil {
			return errors.New("workflow service is unavailable")
		}
		workflowID, err := url.PathUnescape(parts[3])
		if err != nil {
			return errBadRequest(err)
		}
		summary, err := deps.Workflows.GetSummary(ctx, workflowID)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, summary)
		return nil
	}
	if len(parts) >= 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "workflows" {
		workflowID, err := url.PathUnescape(parts[3])
		if err != nil {
			return errBadRequest(err)
		}
		if parts[4] == "versions" && len(parts) == 5 {
			if r.Method == http.MethodGet {
				if deps.Workflows == nil {
					return errors.New("workflow service is unavailable")
				}
				page, err := deps.Workflows.ListVersions(ctx, workflowID, r.URL.Query().Get("cursor"), queryLimit(r))
				if err != nil {
					return err
				}
				writeJSON(w, http.StatusOK, page)
				return nil
			}
			if r.Method != http.MethodPost {
				return errMethodNotAllowed
			}
			if role != auth.OperatorRole {
				writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
				return nil
			}
			if deps.Workflows == nil {
				return errors.New("workflow service is unavailable")
			}
			var definition workflow.WorkflowDefinition
			if err := decodeJSON(w, r, &definition, deps.MaxBodyBytes, true); err != nil {
				return errBadRequest(err)
			}
			key, err := requiredHeader(r, "Idempotency-Key")
			if err != nil {
				return errBadRequest(err)
			}
			ref, err := deps.Workflows.CreateVersion(ctx, principal(role), workflowID, key, definition)
			if err != nil {
				return err
			}
			deps.Logger.Info("workflow version created", "workflow_id", ref.WorkflowID, "workflow_version", ref.Version)
			writeJSON(w, http.StatusCreated, ref)
			return nil
		}
		if len(parts) < 6 || parts[4] != "versions" {
			return errNotFound
		}
		version, err := strconv.Atoi(parts[5])
		if err != nil || version <= 0 {
			return errBadRequest(fmt.Errorf("invalid version"))
		}
		if len(parts) == 7 && parts[6] == "runs" {
			if r.Method != http.MethodPost {
				return errMethodNotAllowed
			}
			if role != auth.OperatorRole {
				writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
				return nil
			}
			if deps.Runs == nil {
				return errors.New("run service is unavailable")
			}
			if err := decodeJSON(w, r, &struct{}{}, deps.MaxBodyBytes, false); err != nil {
				return errBadRequest(err)
			}
			key, err := requiredHeader(r, "Idempotency-Key")
			if err != nil {
				return errBadRequest(err)
			}
			response, err := deps.Runs.Start(ctx, principal(role), workflowID, version, key)
			if err != nil {
				return err
			}
			deps.Logger.Info("run created",
				"workflow_id", workflowID,
				"workflow_version", version,
				"run_id", response.RunID,
				"run_status", response.Status,
			)
			writeJSON(w, http.StatusAccepted, response)
			return nil
		}
		if len(parts) == 6 && r.Method == http.MethodGet {
			if deps.Workflows == nil {
				return errors.New("workflow service is unavailable")
			}
			definition, err := deps.Workflows.Get(ctx, workflowID, version)
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, definition)
			return nil
		}
	}
	if len(parts) == 3 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "runs" && r.Method == http.MethodGet {
		if deps.Runs == nil {
			return errors.New("run service is unavailable")
		}
		page, err := deps.Runs.List(ctx, app.RunListOptions{
			Cursor:     r.URL.Query().Get("cursor"),
			Limit:      queryLimit(r),
			WorkflowID: r.URL.Query().Get("workflow_id"),
			Status:     r.URL.Query().Get("status"),
		})
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, page)
		return nil
	}
	if len(parts) >= 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "runs" {
		if deps.Runs == nil {
			return errors.New("run service is unavailable")
		}
		id := workflow.RunID(parts[3])
		if len(parts) == 4 && r.Method == http.MethodGet {
			summary, err := deps.Runs.Get(ctx, id)
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, summary)
			return nil
		}
		if len(parts) == 5 && parts[4] == "tasks" && r.Method == http.MethodGet {
			page, err := deps.Runs.ListTasks(ctx, id, r.URL.Query().Get("cursor"), queryLimit(r))
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, page)
			return nil
		}
		if len(parts) == 6 && parts[4] == "tasks" && r.Method == http.MethodGet {
			key, err := url.PathUnescape(parts[5])
			if err != nil {
				return errBadRequest(err)
			}
			detail, err := deps.Runs.GetTask(ctx, id, workflow.TaskKey(key))
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, detail)
			return nil
		}
		if len(parts) == 5 && parts[4] == "events" && r.Method == http.MethodGet {
			page, err := deps.Runs.ListEvents(ctx, id, r.URL.Query().Get("cursor"), queryLimit(r))
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, page)
			return nil
		}
		if len(parts) == 5 && parts[4] == "cancel" && r.Method == http.MethodPost {
			if role != auth.OperatorRole {
				writeAPIError(w, requestID, http.StatusForbidden, "forbidden", "operator role required")
				return nil
			}
			if err := decodeJSON(w, r, &struct{}{}, deps.MaxBodyBytes, false); err != nil {
				return errBadRequest(err)
			}
			run, err := deps.Runs.Cancel(ctx, principal(role), id)
			if err != nil {
				return err
			}
			deps.Logger.Info("run cancellation requested", "run_id", id, "run_status", run.Status)
			writeJSON(w, http.StatusOK, run)
			return nil
		}
	}
	return errNotFound
}

// normalizedPath 把具体资源 ID 替换为公开路由模板，避免日志字段出现无界基数。
func normalizedPath(path string) string {
	if path == "/health/live" || path == "/health/ready" {
		return path
	}
	parts := splitPath(path)
	if len(parts) < 3 || parts[0] != "api" || parts[1] != "v1" {
		return "unmatched"
	}
	if len(parts) == 3 && (parts[2] == "workflows" || parts[2] == "runs") {
		return "/api/v1/" + parts[2]
	}
	if parts[2] == "workflows" {
		switch {
		case len(parts) == 4:
			return "/api/v1/workflows/{workflow-id}"
		case len(parts) == 5 && parts[4] == "versions":
			return "/api/v1/workflows/{workflow-id}/versions"
		case len(parts) == 6 && parts[4] == "versions":
			return "/api/v1/workflows/{workflow-id}/versions/{version}"
		case len(parts) == 7 && parts[4] == "versions" && parts[6] == "runs":
			return "/api/v1/workflows/{workflow-id}/versions/{version}/runs"
		}
	}
	if parts[2] == "runs" {
		switch {
		case len(parts) == 4:
			return "/api/v1/runs/{run-id}"
		case len(parts) == 5 && (parts[4] == "tasks" || parts[4] == "events" || parts[4] == "cancel"):
			return "/api/v1/runs/{run-id}/" + parts[4]
		case len(parts) == 6 && parts[4] == "tasks":
			return "/api/v1/runs/{run-id}/tasks/{task-key}"
		}
	}
	if parts[2] == "workers" {
		switch {
		case len(parts) == 3:
			return "/api/v1/workers"
		case len(parts) == 4 && parts[3] == "register":
			return "/api/v1/workers/register"
		case len(parts) == 4:
			return "/api/v1/workers/{worker-id}"
		case len(parts) == 5 && (parts[4] == "claims" || parts[4] == "heartbeat" || parts[4] == "drain"):
			return "/api/v1/workers/{worker-id}/" + parts[4]
		case len(parts) == 7 && parts[4] == "leases" && parts[6] == "complete":
			return "/api/v1/workers/{worker-id}/leases/{dispatch-id}/complete"
		}
	}
	return "unmatched"
}

var (
	errNotFound         = errors.New("route not found")
	errMethodNotAllowed = errors.New("method not allowed")
)

type apiError struct{ err error }

func errBadRequest(err error) error { return apiError{err: err} }

func (e apiError) Error() string { return e.err.Error() }
func (e apiError) Unwrap() error { return e.err }

func mappedError(err error) (int, string, string) {
	status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
	var bad apiError
	switch {
	case errors.Is(err, errNotFound), errors.Is(err, postgres.ErrDefinitionNotFound), errors.Is(err, postgres.ErrWorkflowNotFound), errors.Is(err, workflow.ErrRunNotFound), errors.Is(err, postgres.ErrTaskNotFound):
		status, code, message = http.StatusNotFound, "not_found", "resource not found"
	case errors.Is(err, postgres.ErrWorkflowExists):
		status, code, message = http.StatusConflict, "workflow_exists", "workflow already exists"
	case errors.Is(err, postgres.ErrIdempotencyConflict):
		status, code, message = http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with an earlier request"
	case errors.Is(err, postgres.ErrWorkerSessionInvalid):
		status, code, message = http.StatusUnauthorized, "worker_session_invalid", "Worker session is invalid"
	case errors.Is(err, postgres.ErrWorkerProtocolUnsupported):
		status, code, message = http.StatusBadRequest, "worker_protocol_unsupported", "Worker protocol version is not supported"
	case errors.Is(err, postgres.ErrWorkerDraining):
		status, code, message = http.StatusConflict, "worker_draining", "Worker is draining"
	case errors.Is(err, postgres.ErrWorkerCapacityExceeded):
		status, code, message = http.StatusConflict, "worker_capacity_exceeded", "Worker capacity is exceeded"
	case errors.Is(err, postgres.ErrLeaseLost):
		status, code, message = http.StatusConflict, "lease_lost", "Worker lease is no longer current"
	case errors.Is(err, postgres.ErrResultConflict):
		status, code, message = http.StatusConflict, "result_conflict", "Worker lease result conflicts with an earlier result"
	case errors.Is(err, app.ErrInvalidArgument):
		status, code, message = http.StatusBadRequest, "invalid_request", err.Error()
	case errors.As(err, &bad):
		status, code, message = http.StatusBadRequest, "invalid_request", bad.Error()
	case errors.Is(err, errMethodNotAllowed):
		status, code, message = http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed"
	}
	return status, code, message
}

type errorBody struct {
	Error errorDetail `json:"error"`
}
type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeAPIError(w http.ResponseWriter, requestID string, status int, code, message string) {
	if logged, ok := w.(*loggingResponseWriter); ok {
		logged.errorCode = code
	}
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message, RequestID: requestID}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any, maxBytes int64, required bool) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
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

func requiredHeader(r *http.Request, name string) (string, error) {
	value := strings.TrimSpace(r.Header.Get(name))
	if value == "" {
		return "", fmt.Errorf("%s header is required", name)
	}
	return value, nil
}

func queryLimit(r *http.Request) int {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return parsed
}

func principal(role auth.Role) string { return string(role) }

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func requestID(r *http.Request) string {
	if candidate := r.Header.Get("X-Request-ID"); validRequestID(candidate) {
		return candidate
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "req_unknown"
	}
	return "req_" + hex.EncodeToString(raw[:])
}

func validRequestID(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '_' && char != '.' {
			return false
		}
	}
	return true
}
