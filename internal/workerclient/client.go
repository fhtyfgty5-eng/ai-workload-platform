// Package workerclient 实现独立 Worker 进程使用的 HTTP 客户端。
package workerclient

import (
	"bytes"
	"context"
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
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/workerprotocol"
	"go.opentelemetry.io/otel"
)

const defaultMaxBodyBytes int64 = 1 << 20

type Config struct {
	BaseURL      string
	HTTPClient   *http.Client
	MaxBodyBytes int64
	Logger       *slog.Logger
}

type Client struct {
	baseURL      url.URL
	httpClient   *http.Client
	maxBodyBytes int64
	logger       *slog.Logger
}

// APIError 保存服务端稳定错误码，但不保留响应正文或凭据。
type APIError struct {
	Status    int
	Code      string
	Message   string
	RequestID string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("Worker API returned HTTP %d", e.Status)
	}
	return fmt.Sprintf("Worker API returned HTTP %d (%s): %s", e.Status, e.Code, e.Message)
}

func New(config Config) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("Worker server URL must be an absolute HTTP(S) URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("Worker server URL must not contain query or fragment")
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaultMaxBodyBytes
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Client{baseURL: *parsed, httpClient: config.HTTPClient, maxBodyBytes: config.MaxBodyBytes, logger: config.Logger}, nil
}

func (c *Client) Register(ctx context.Context, bootstrapToken string, request workerprotocol.RegisterRequest) (workerprotocol.RegisterResponse, error) {
	var response workerprotocol.RegisterResponse
	err := c.do(ctx, http.MethodPost, []string{"api", "v1", "workers", "register"}, bootstrapToken, request, &response)
	return response, err
}

func (c *Client) Claim(ctx context.Context, workerID, sessionToken string, slots int) (workerprotocol.ClaimResponse, error) {
	var response workerprotocol.ClaimResponse
	err := c.do(ctx, http.MethodPost, []string{"api", "v1", "workers", workerID, "claims"}, sessionToken, workerprotocol.ClaimRequest{Slots: slots}, &response)
	return response, err
}

func (c *Client) Heartbeat(ctx context.Context, workerID, sessionToken string, request workerprotocol.HeartbeatRequest) (workerprotocol.HeartbeatResponse, error) {
	var response workerprotocol.HeartbeatResponse
	err := c.do(ctx, http.MethodPost, []string{"api", "v1", "workers", workerID, "heartbeat"}, sessionToken, request, &response)
	return response, err
}

func (c *Client) Complete(ctx context.Context, workerID, dispatchID, sessionToken string, request workerprotocol.CompleteRequest) (workerprotocol.CompleteResponse, error) {
	var response workerprotocol.CompleteResponse
	err := c.do(ctx, http.MethodPost, []string{"api", "v1", "workers", workerID, "leases", dispatchID, "complete"}, sessionToken, request, &response)
	return response, err
}

func (c *Client) Drain(ctx context.Context, workerID, sessionToken string) (workerprotocol.WorkerSummary, error) {
	var response workerprotocol.WorkerSummary
	err := c.do(ctx, http.MethodPost, []string{"api", "v1", "workers", workerID, "drain"}, sessionToken, struct{}{}, &response)
	return response, err
}

func (c *Client) do(ctx context.Context, method string, segments []string, token string, body any, target any) (resultErr error) {
	startedAt := time.Now()
	operation := workerOperation(segments)
	ctx, span := observability.StartSpan(ctx, otel.Tracer("workload-worker"), "worker."+operation)
	defer func() {
		outcome := "success"
		errorCode := ""
		if resultErr != nil {
			outcome = "error"
			errorCode = ErrorCode(resultErr)
		}
		observability.FinishSpan(span, outcome, errorCode)
		span.End()
	}()
	if strings.TrimSpace(token) == "" {
		return auth.ErrUnauthorized
	}
	endpoint, err := c.endpoint(segments...)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode Worker request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("create Worker request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set(workerprotocol.ProtocolVersionHeader, strconv.Itoa(workerprotocol.ProtocolVersion))
	request.Header.Set("Content-Type", "application/json")
	observability.InjectHTTP(ctx, request)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	observability.LoggerFromContext(ctx, c.logger).Debug("worker HTTP request", "method", method, "path", endpoint.EscapedPath(), "status", response.StatusCode, "duration_ms", time.Since(startedAt).Milliseconds(), "operation", operation)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return c.decodeAPIError(response)
	}
	if target == nil {
		return nil
	}
	return decodeLimited(response.Body, c.maxBodyBytes, target)
}

// ErrorCode 把 Worker Client 错误归一为日志、指标和 Span 共用的稳定分类。
func ErrorCode(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Code != "" {
		return observability.NormalizeErrorCode(apiErr.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, auth.ErrUnauthorized) {
		return "unauthorized"
	}
	return "unknown"
}

func workerOperation(segments []string) string {
	if len(segments) == 0 {
		return "unknown"
	}
	last := segments[len(segments)-1]
	switch last {
	case "register":
		return "register"
	case "claims":
		return "claim"
	case "heartbeat":
		return "heartbeat"
	case "complete":
		return "complete"
	case "drain":
		return "drain"
	default:
		return "unknown"
	}
}

func (c *Client) endpoint(segments ...string) (*url.URL, error) {
	base := c.baseURL
	rawPath := strings.TrimRight(base.EscapedPath(), "/")
	for _, segment := range segments {
		if segment == "" {
			return nil, fmt.Errorf("Worker URL path segment is empty")
		}
		rawPath += "/" + url.PathEscape(segment)
	}
	decoded, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, fmt.Errorf("encode Worker URL path: %w", err)
	}
	base.Path = decoded
	base.RawPath = rawPath
	return &base, nil
}

func (c *Client) decodeAPIError(response *http.Response) error {
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := decodeLimited(response.Body, c.maxBodyBytes, &envelope); err != nil {
		return &APIError{Status: response.StatusCode, Message: "Worker API returned an invalid error response"}
	}
	return &APIError{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, RequestID: envelope.Error.RequestID}
}

func decodeLimited(reader io.Reader, maxBytes int64, target any) error {
	if maxBytes <= 0 {
		maxBytes = defaultMaxBodyBytes
	}
	limited := io.LimitReader(reader, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read Worker response: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return fmt.Errorf("Worker response exceeds %d bytes", maxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode Worker response: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode Worker response: multiple JSON values")
	}
	return nil
}
