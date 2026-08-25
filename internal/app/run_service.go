package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// RunService 负责版本化 Run 创建、有界查询和取消顺序。
type RunService interface {
	Start(context.Context, string, string, int, string) (StartRunResponse, error)
	List(context.Context, RunListOptions) (RunPage, error)
	Get(context.Context, workflow.RunID) (RunSummary, error)
	ListTasks(context.Context, workflow.RunID, string, int) (TaskPage, error)
	GetTask(context.Context, workflow.RunID, workflow.TaskKey) (TaskDetail, error)
	ListEvents(context.Context, workflow.RunID, string, int) (EventPage, error)
	Cancel(context.Context, string, workflow.RunID) (CancelRunResponse, error)
}

type runRepository interface {
	CreateRun(context.Context, workflow.RunSnapshot, string, string, string) (workflow.RunID, bool, error)
	LoadRun(context.Context, workflow.RunID) (workflow.WorkflowRun, error)
	ListRunSummaries(context.Context, postgres.RunQuery, int) ([]postgres.RunRecord, bool, error)
	ListTaskRuns(context.Context, workflow.RunID, int, int) ([]postgres.TaskRecord, bool, error)
	LoadTaskRun(context.Context, workflow.RunID, workflow.TaskKey) (workflow.TaskRun, error)
	ListStateEvents(context.Context, workflow.RunID, uint64, int) ([]workflow.StateEvent, bool, error)
	RequestCancel(context.Context, workflow.RunID, time.Time) (workflow.WorkflowRun, error)
}

// RunController 隔离 Run 持久化与本地执行或分布式分发方式。
type RunController interface {
	Wake()
	Cancel(context.Context, workflow.RunID) error
}

type runIDGenerator func() (workflow.RunID, error)

type runService struct {
	repository  runRepository
	definitions WorkflowService
	controller  RunController
	newRunID    runIDGenerator
}

// NewRunService 创建始终先持久化、再通知执行控制器的服务。
func NewRunService(repository runRepository, definitions WorkflowService, controller RunController, newRunID runIDGenerator) (RunService, error) {
	if controller == nil {
		return nil, fmt.Errorf("run controller is required")
	}
	if newRunID == nil {
		newRunID = randomRunID
	}
	return &runService{repository: repository, definitions: definitions, controller: controller, newRunID: newRunID}, nil
}

func (s *runService) Start(ctx context.Context, principal, workflowID string, version int, key string) (StartRunResponse, error) {
	compiled, err := s.compiled(ctx, workflowID, version)
	if err != nil {
		return StartRunResponse{}, err
	}
	id, err := s.newRunID()
	if err != nil {
		return StartRunResponse{}, err
	}
	snapshot, err := workflow.NewRunSnapshotForVersion(id, compiled, version, time.Now().UTC())
	if err != nil {
		return StartRunResponse{}, err
	}
	createdID, created, err := s.repository.CreateRun(ctx, snapshot, principal, key, startRequestHash(workflowID, version))
	if err != nil {
		return StartRunResponse{}, err
	}
	if created {
		s.controller.Wake()
	}
	return StartRunResponse{RunID: createdID, Status: workflow.WorkflowPending}, nil
}

func (s *runService) compiled(ctx context.Context, workflowID string, version int) (*workflow.CompiledWorkflow, error) {
	if definitions, ok := s.definitions.(*workflowService); ok {
		return definitions.compiled(ctx, workflowID, version)
	}
	definition, err := s.definitions.Get(ctx, workflowID, version)
	if err != nil {
		return nil, err
	}
	return workflow.Compile(definition)
}

func (s *runService) Get(ctx context.Context, id workflow.RunID) (RunSummary, error) {
	run, err := s.repository.LoadRun(ctx, id)
	if err != nil {
		return RunSummary{}, err
	}
	return RunSummary{ID: run.ID, DefinitionID: run.DefinitionID, DefinitionVersion: run.DefinitionVersion, Status: run.Status, Revision: run.Revision, TaskCount: len(run.Tasks), CreatedAt: run.CreatedAt, StartedAt: run.StartedAt, FinishedAt: run.FinishedAt}, nil
}

func (s *runService) List(ctx context.Context, options RunListOptions) (RunPage, error) {
	size, err := normalizeLimit(options.Limit)
	if err != nil {
		return RunPage{}, err
	}
	status, err := parseRunStatus(options.Status)
	if err != nil {
		return RunPage{}, err
	}
	query := postgres.RunQuery{WorkflowID: options.WorkflowID, Status: status}
	if options.Cursor != "" {
		cursor, err := decodeRunCursor(options.Cursor, options.WorkflowID, status)
		if err != nil {
			return RunPage{}, err
		}
		query.AfterCreated = &cursor.CreatedAt
		query.AfterRunID = cursor.RunID
	}
	items, more, err := s.repository.ListRunSummaries(ctx, query, size)
	if err != nil {
		return RunPage{}, err
	}
	page := RunPage{Items: make([]RunSummary, 0, len(items))}
	for _, item := range items {
		page.Items = append(page.Items, RunSummary{ID: item.ID, DefinitionID: item.DefinitionID, DefinitionVersion: item.DefinitionVersion, Status: item.Status, Revision: item.Revision, TaskCount: item.TaskCount, CreatedAt: item.CreatedAt, StartedAt: item.StartedAt, FinishedAt: item.FinishedAt})
	}
	if more && len(items) > 0 {
		last := items[len(items)-1]
		page.NextCursor, err = encodeRunCursor(last.CreatedAt, last.ID, options.WorkflowID, status)
		if err != nil {
			return RunPage{}, err
		}
	}
	return page, nil
}

func (s *runService) ListTasks(ctx context.Context, id workflow.RunID, cursor string, limit int) (TaskPage, error) {
	size, err := normalizeLimit(limit)
	if err != nil {
		return TaskPage{}, err
	}
	afterIndex := -1
	if cursor != "" {
		decoded, err := decodeTaskCursor(cursor, id)
		if err != nil {
			return TaskPage{}, err
		}
		afterIndex = decoded.TaskIndex
	}
	tasks, more, err := s.repository.ListTaskRuns(ctx, id, afterIndex, size)
	if err != nil {
		return TaskPage{}, err
	}
	page := TaskPage{Items: make([]TaskSummary, 0, len(tasks))}
	for _, record := range tasks {
		task := record.Task
		page.Items = append(page.Items, TaskSummary{Key: task.Key, Status: task.Status, ReadyAt: task.ReadyAt, FinishedAt: task.FinishedAt})
	}
	if more && len(tasks) > 0 {
		page.NextCursor, err = encodeTaskCursor(id, tasks[len(tasks)-1].Index)
		if err != nil {
			return TaskPage{}, err
		}
	}
	return page, nil
}

func (s *runService) GetTask(ctx context.Context, id workflow.RunID, taskKey workflow.TaskKey) (TaskDetail, error) {
	task, err := s.repository.LoadTaskRun(ctx, id, taskKey)
	if err != nil {
		return TaskDetail{}, err
	}
	return TaskDetail{Task: task}, nil
}

func (s *runService) ListEvents(ctx context.Context, id workflow.RunID, cursor string, limit int) (EventPage, error) {
	size, err := normalizeLimit(limit)
	if err != nil {
		return EventPage{}, err
	}
	startSequence := uint64(0)
	if cursor != "" {
		decoded, err := decodeEventCursor(cursor, id)
		if err != nil {
			return EventPage{}, err
		}
		startSequence = decoded.Sequence
	}
	events, more, err := s.repository.ListStateEvents(ctx, id, startSequence, size)
	if err != nil {
		return EventPage{}, err
	}
	page := EventPage{Items: events}
	if more && len(events) > 0 {
		page.NextCursor, err = encodeEventCursor(id, events[len(events)-1].Sequence)
		if err != nil {
			return EventPage{}, err
		}
	}
	return page, nil
}

func (s *runService) Cancel(ctx context.Context, _ string, id workflow.RunID) (CancelRunResponse, error) {
	run, err := s.repository.RequestCancel(ctx, id, time.Now().UTC())
	if err != nil {
		return CancelRunResponse{}, err
	}
	if err := s.controller.Cancel(ctx, id); err != nil {
		return CancelRunResponse{}, err
	}
	if run.CancelRequestedAt == nil {
		return CancelRunResponse{}, fmt.Errorf("cancel request for Run %s was not persisted", id)
	}
	return CancelRunResponse{RunID: run.ID, Status: run.Status, CancelRequestedAt: *run.CancelRequestedAt}, nil
}

func normalizeLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultPageSize, nil
	}
	if limit < 0 || limit > maxPageSize {
		return 0, fmt.Errorf("%w: page limit must be between 1 and %d", ErrInvalidArgument, maxPageSize)
	}
	return limit, nil
}

func startRequestHash(workflowID string, version int) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s@%d", workflowID, version)))
	return hex.EncodeToString(hash[:])
}

func randomRunID() (workflow.RunID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return workflow.RunID(fmt.Sprintf("%016x%016x", binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:]))), nil
}
