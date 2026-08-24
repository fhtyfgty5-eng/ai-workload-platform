// Package workloadclient is the public Go client for the versioned control plane.
package workloadclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const maxResponseBytes int64 = 4 << 20

type Client struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTPClient: http.DefaultClient}
}

type APIError struct {
	StatusCode int
	Code       string `json:"code"`
	Message    string `json:"message"`
	RequestID  string `json:"request_id"`
}

func (e *APIError) Error() string {
	if e.RequestID == "" {
		return fmt.Sprintf("API request failed (%d): %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("API request failed (%d, %s): %s (request_id=%s)", e.StatusCode, e.Code, e.Message, e.RequestID)
}

func (c *Client) CreateWorkflow(ctx context.Context, key string, definition workflow.WorkflowDefinition) (DefinitionRef, error) {
	var response DefinitionRef
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/workflows", key, definition, http.StatusCreated, &response)
	return response, err
}

func (c *Client) CreateWorkflowVersion(ctx context.Context, workflowID, key string, definition workflow.WorkflowDefinition) (DefinitionRef, error) {
	var response DefinitionRef
	path := "/api/v1/workflows/" + url.PathEscape(workflowID) + "/versions"
	err := c.doJSON(ctx, http.MethodPost, path, key, definition, http.StatusCreated, &response)
	return response, err
}

func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (WorkflowSummary, error) {
	var response WorkflowSummary
	err := c.doJSON(ctx, http.MethodGet, "/api/v1/workflows/"+url.PathEscape(workflowID), "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) ListWorkflows(ctx context.Context, cursor string, limit int) (WorkflowPage, error) {
	var response WorkflowPage
	err := c.doJSON(ctx, http.MethodGet, pagePath("/api/v1/workflows", cursor, limit), "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) ListWorkflowVersions(ctx context.Context, workflowID, cursor string, limit int) (VersionPage, error) {
	var response VersionPage
	path := pagePath("/api/v1/workflows/"+url.PathEscape(workflowID)+"/versions", cursor, limit)
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) GetWorkflowVersion(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	var response workflow.WorkflowDefinition
	path := "/api/v1/workflows/" + url.PathEscape(workflowID) + "/versions/" + strconv.Itoa(version)
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) StartRun(ctx context.Context, workflowID string, version int, key string) (StartRunResponse, error) {
	var response StartRunResponse
	path := "/api/v1/workflows/" + url.PathEscape(workflowID) + "/versions/" + strconv.Itoa(version) + "/runs"
	err := c.doJSON(ctx, http.MethodPost, path, key, struct{}{}, http.StatusAccepted, &response)
	return response, err
}

func (c *Client) GetRun(ctx context.Context, runID workflow.RunID) (RunSummary, error) {
	var response RunSummary
	err := c.doJSON(ctx, http.MethodGet, "/api/v1/runs/"+url.PathEscape(string(runID)), "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) ListRuns(ctx context.Context, cursor string, limit int) (RunPage, error) {
	return c.ListRunsFiltered(ctx, RunListOptions{Cursor: cursor, Limit: limit})
}

func (c *Client) ListRunsFiltered(ctx context.Context, options RunListOptions) (RunPage, error) {
	var response RunPage
	values := url.Values{}
	if options.Cursor != "" {
		values.Set("cursor", options.Cursor)
	}
	if options.Limit != 0 {
		values.Set("limit", strconv.Itoa(options.Limit))
	}
	if options.WorkflowID != "" {
		values.Set("workflow_id", options.WorkflowID)
	}
	if options.Status != "" {
		values.Set("status", string(options.Status))
	}
	path := "/api/v1/runs"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) ListRunTasks(ctx context.Context, runID workflow.RunID, cursor string, limit int) (TaskPage, error) {
	var response TaskPage
	path := pagePath("/api/v1/runs/"+url.PathEscape(string(runID))+"/tasks", cursor, limit)
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) GetTask(ctx context.Context, runID workflow.RunID, taskKey workflow.TaskKey) (TaskDetail, error) {
	var response TaskDetail
	path := "/api/v1/runs/" + url.PathEscape(string(runID)) + "/tasks/" + url.PathEscape(string(taskKey))
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) ListRunEvents(ctx context.Context, runID workflow.RunID, cursor string, limit int) (EventPage, error) {
	var response EventPage
	path := pagePath("/api/v1/runs/"+url.PathEscape(string(runID))+"/events", cursor, limit)
	err := c.doJSON(ctx, http.MethodGet, path, "", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) CancelRun(ctx context.Context, runID workflow.RunID) error {
	_, err := c.CancelRunWithResponse(ctx, runID)
	return err
}

func (c *Client) CancelRunWithResponse(ctx context.Context, runID workflow.RunID) (CancelRunResponse, error) {
	var response CancelRunResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/runs/"+url.PathEscape(string(runID))+"/cancel", "", struct{}{}, http.StatusOK, &response)
	return response, err
}

func (c *Client) doJSON(ctx context.Context, method, path, idempotencyKey string, requestBody any, wantStatus int, responseBody any) error {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return fmt.Errorf("client base URL is required")
	}
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxResponseBytes)
	if response.StatusCode != wantStatus {
		var envelope struct {
			Error APIError `json:"error"`
		}
		if err := json.NewDecoder(limited).Decode(&envelope); err != nil {
			return &APIError{StatusCode: response.StatusCode, Message: response.Status}
		}
		envelope.Error.StatusCode = response.StatusCode
		return &envelope.Error
	}
	if responseBody == nil {
		return nil
	}
	decoder := json.NewDecoder(limited)
	decoder.UseNumber()
	if err := decoder.Decode(responseBody); err != nil {
		return fmt.Errorf("decode API response: %w", err)
	}
	return nil
}

func pagePath(path, cursor string, limit int) string {
	values := url.Values{}
	if cursor != "" {
		values.Set("cursor", cursor)
	}
	if limit != 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	if encoded := values.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}
