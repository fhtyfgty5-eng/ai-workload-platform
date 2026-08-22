package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	cursorVersion      = 1
	maxCursorBytes     = 4 << 10
	workflowCursorKind = "workflow"
	versionCursorKind  = "version"
	runCursorKind      = "run"
	taskCursorKind     = "task"
	eventCursorKind    = "event"
)

type workflowCursor struct {
	Version    int    `json:"v"`
	Kind       string `json:"kind"`
	WorkflowID string `json:"workflow_id"`
}

type versionCursor struct {
	Version      int    `json:"v"`
	Kind         string `json:"kind"`
	WorkflowID   string `json:"workflow_id"`
	AfterVersion int    `json:"after_version"`
}

type runCursor struct {
	Version    int            `json:"v"`
	Kind       string         `json:"kind"`
	CreatedAt  time.Time      `json:"created_at"`
	RunID      workflow.RunID `json:"run_id"`
	FilterHash string         `json:"filter_hash"`
}

type taskCursor struct {
	Version   int            `json:"v"`
	Kind      string         `json:"kind"`
	RunID     workflow.RunID `json:"run_id"`
	TaskIndex int            `json:"task_index"`
}

type eventCursor struct {
	Version  int            `json:"v"`
	Kind     string         `json:"kind"`
	RunID    workflow.RunID `json:"run_id"`
	Sequence uint64         `json:"sequence"`
}

func encodeRunCursor(createdAt time.Time, runID workflow.RunID, workflowID string, status workflow.WorkflowStatus) (string, error) {
	return encodeCursor(runCursor{
		Version: cursorVersion, Kind: runCursorKind, CreatedAt: createdAt, RunID: runID,
		FilterHash: runFilterHash(workflowID, status),
	})
}

func decodeRunCursor(value, workflowID string, status workflow.WorkflowStatus) (runCursor, error) {
	var cursor runCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return runCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.Kind != runCursorKind || cursor.CreatedAt.IsZero() || cursor.RunID == "" ||
		cursor.FilterHash != runFilterHash(workflowID, status) {
		return runCursor{}, invalidCursor()
	}
	return cursor, nil
}

func encodeWorkflowCursor(workflowID string) (string, error) {
	return encodeCursor(workflowCursor{Version: cursorVersion, Kind: workflowCursorKind, WorkflowID: workflowID})
}

func decodeWorkflowCursor(value string) (workflowCursor, error) {
	var cursor workflowCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return workflowCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.Kind != workflowCursorKind || cursor.WorkflowID == "" {
		return workflowCursor{}, invalidCursor()
	}
	return cursor, nil
}

func encodeVersionCursor(workflowID string, version int) (string, error) {
	return encodeCursor(versionCursor{Version: cursorVersion, Kind: versionCursorKind, WorkflowID: workflowID, AfterVersion: version})
}

func decodeVersionCursor(value, workflowID string) (versionCursor, error) {
	var cursor versionCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return versionCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.Kind != versionCursorKind || cursor.WorkflowID != workflowID || cursor.AfterVersion <= 0 {
		return versionCursor{}, invalidCursor()
	}
	return cursor, nil
}

func encodeTaskCursor(runID workflow.RunID, taskIndex int) (string, error) {
	return encodeCursor(taskCursor{Version: cursorVersion, Kind: taskCursorKind, RunID: runID, TaskIndex: taskIndex})
}

func decodeTaskCursor(value string, runID workflow.RunID) (taskCursor, error) {
	var cursor taskCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return taskCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.Kind != taskCursorKind || cursor.RunID != runID || cursor.TaskIndex < 0 {
		return taskCursor{}, invalidCursor()
	}
	return cursor, nil
}

func encodeEventCursor(runID workflow.RunID, sequence uint64) (string, error) {
	return encodeCursor(eventCursor{Version: cursorVersion, Kind: eventCursorKind, RunID: runID, Sequence: sequence})
}

func decodeEventCursor(value string, runID workflow.RunID) (eventCursor, error) {
	var cursor eventCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return eventCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.Kind != eventCursorKind || cursor.RunID != runID || cursor.Sequence == 0 {
		return eventCursor{}, invalidCursor()
	}
	return cursor, nil
}

func encodeCursor(cursor any) (string, error) {
	body, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode page cursor: %w", err)
	}
	if len(body) > maxCursorBytes {
		return "", invalidCursor()
	}
	return base64.RawURLEncoding.EncodeToString(body), nil
}

func decodeCursor(value string, cursor any) error {
	if value == "" || base64.RawURLEncoding.DecodedLen(len(value)) > maxCursorBytes {
		return invalidCursor()
	}
	body, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(body) > maxCursorBytes {
		return invalidCursor()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(cursor); err != nil {
		return invalidCursor()
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return invalidCursor()
	}
	return nil
}

func runFilterHash(workflowID string, status workflow.WorkflowStatus) string {
	hash := sha256.Sum256([]byte(workflowID + "\x00" + string(status)))
	return hex.EncodeToString(hash[:])
}

func parseRunStatus(value string) (workflow.WorkflowStatus, error) {
	status := workflow.WorkflowStatus(value)
	switch status {
	case "", workflow.WorkflowPending, workflow.WorkflowRunning,
		workflow.WorkflowSucceeded, workflow.WorkflowFailed, workflow.WorkflowCanceled:
		return status, nil
	default:
		return "", fmt.Errorf("%w: invalid Run status %q", ErrInvalidArgument, value)
	}
}

func invalidCursor() error {
	return fmt.Errorf("%w: invalid page cursor", ErrInvalidArgument)
}
