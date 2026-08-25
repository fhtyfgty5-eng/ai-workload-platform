package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/postgres"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// WorkflowService 创建经过编译的不可变工作流版本，不处理 HTTP 细节。
type WorkflowService interface {
	Create(context.Context, string, string, workflow.WorkflowDefinition) (DefinitionRef, error)
	CreateVersion(context.Context, string, string, string, workflow.WorkflowDefinition) (DefinitionRef, error)
	Get(context.Context, string, int) (workflow.WorkflowDefinition, error)
	GetSummary(context.Context, string) (WorkflowSummary, error)
	List(context.Context, string, int) (WorkflowPage, error)
	ListVersions(context.Context, string, string, int) (VersionPage, error)
}

type workflowRepository interface {
	CreateDefinition(context.Context, workflow.WorkflowDefinition, string, string, string) (postgres.DefinitionRecord, error)
	CreateVersion(context.Context, string, workflow.WorkflowDefinition, string, string, string) (postgres.DefinitionRecord, error)
	LoadDefinition(context.Context, string, int) (workflow.WorkflowDefinition, error)
	LoadWorkflowSummary(context.Context, string) (postgres.WorkflowRecord, error)
	ListWorkflows(context.Context, string, int) ([]postgres.WorkflowRecord, bool, error)
	ListVersions(context.Context, string, int, int) ([]postgres.VersionRecord, bool, error)
}

type workflowService struct {
	repository workflowRepository
	mu         sync.RWMutex
	cache      map[DefinitionRef]*workflow.CompiledWorkflow
}

// NewWorkflowService 创建定义编译器和版本缓存。
func NewWorkflowService(repository workflowRepository) WorkflowService {
	return &workflowService{repository: repository, cache: make(map[DefinitionRef]*workflow.CompiledWorkflow)}
}

func (s *workflowService) Create(ctx context.Context, principal, key string, definition workflow.WorkflowDefinition) (DefinitionRef, error) {
	compiled, err := workflow.Compile(definition)
	if err != nil {
		return DefinitionRef{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	canonical := compiled.Definition()
	record, err := s.repository.CreateDefinition(ctx, canonical, principal, key, definitionHash(canonical))
	if err != nil {
		return DefinitionRef{}, err
	}
	ref := DefinitionRef{WorkflowID: record.WorkflowID, Version: record.Version}
	s.putCache(ref, compiled)
	return ref, nil
}

func (s *workflowService) CreateVersion(ctx context.Context, principal, workflowID, key string, definition workflow.WorkflowDefinition) (DefinitionRef, error) {
	if definition.ID != workflowID {
		return DefinitionRef{}, fmt.Errorf("%w: definition ID %q does not match workflow ID %q", ErrInvalidArgument, definition.ID, workflowID)
	}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		return DefinitionRef{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	canonical := compiled.Definition()
	record, err := s.repository.CreateVersion(ctx, workflowID, canonical, principal, key, definitionHash(canonical))
	if err != nil {
		return DefinitionRef{}, err
	}
	ref := DefinitionRef{WorkflowID: record.WorkflowID, Version: record.Version}
	s.putCache(ref, compiled)
	return ref, nil
}

func (s *workflowService) Get(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	return s.repository.LoadDefinition(ctx, workflowID, version)
}

func (s *workflowService) GetSummary(ctx context.Context, workflowID string) (WorkflowSummary, error) {
	record, err := s.repository.LoadWorkflowSummary(ctx, workflowID)
	if err != nil {
		return WorkflowSummary{}, err
	}
	return workflowSummary(record), nil
}

func (s *workflowService) List(ctx context.Context, cursor string, limit int) (WorkflowPage, error) {
	size, err := normalizeLimit(limit)
	if err != nil {
		return WorkflowPage{}, err
	}
	afterID := ""
	if cursor != "" {
		decoded, err := decodeWorkflowCursor(cursor)
		if err != nil {
			return WorkflowPage{}, err
		}
		afterID = decoded.WorkflowID
	}
	items, more, err := s.repository.ListWorkflows(ctx, afterID, size)
	if err != nil {
		return WorkflowPage{}, err
	}
	page := WorkflowPage{Items: make([]WorkflowSummary, 0, len(items))}
	for _, item := range items {
		page.Items = append(page.Items, workflowSummary(item))
	}
	if more && len(items) > 0 {
		page.NextCursor, err = encodeWorkflowCursor(items[len(items)-1].WorkflowID)
		if err != nil {
			return WorkflowPage{}, err
		}
	}
	return page, nil
}

func (s *workflowService) ListVersions(ctx context.Context, workflowID, cursor string, limit int) (VersionPage, error) {
	size, err := normalizeLimit(limit)
	if err != nil {
		return VersionPage{}, err
	}
	afterVersion := 0
	if cursor != "" {
		decoded, err := decodeVersionCursor(cursor, workflowID)
		if err != nil {
			return VersionPage{}, err
		}
		afterVersion = decoded.AfterVersion
	}
	items, more, err := s.repository.ListVersions(ctx, workflowID, afterVersion, size)
	if err != nil {
		return VersionPage{}, err
	}
	page := VersionPage{Items: make([]VersionSummary, 0, len(items))}
	for _, item := range items {
		page.Items = append(page.Items, VersionSummary{WorkflowID: item.WorkflowID, Version: item.Version, CreatedAt: item.CreatedAt, CreatedBy: item.CreatedBy})
	}
	if more && len(items) > 0 {
		page.NextCursor, err = encodeVersionCursor(workflowID, items[len(items)-1].Version)
		if err != nil {
			return VersionPage{}, err
		}
	}
	return page, nil
}

func workflowSummary(record postgres.WorkflowRecord) WorkflowSummary {
	return WorkflowSummary{WorkflowID: record.WorkflowID, LatestVersion: record.LatestVersion, CreatedAt: record.CreatedAt, CreatedBy: record.CreatedBy}
}

func (s *workflowService) compiled(ctx context.Context, workflowID string, version int) (*workflow.CompiledWorkflow, error) {
	ref := DefinitionRef{WorkflowID: workflowID, Version: version}
	s.mu.RLock()
	compiled := s.cache[ref]
	s.mu.RUnlock()
	if compiled != nil {
		return compiled, nil
	}
	definition, err := s.Get(ctx, workflowID, version)
	if err != nil {
		return nil, err
	}
	compiled, err = workflow.Compile(definition)
	if err != nil {
		return nil, fmt.Errorf("compile workflow %s@%d: %w", workflowID, version, err)
	}
	s.putCache(ref, compiled)
	return compiled, nil
}

func (s *workflowService) putCache(ref DefinitionRef, compiled *workflow.CompiledWorkflow) {
	s.mu.Lock()
	s.cache[ref] = compiled
	s.mu.Unlock()
}

func definitionHash(definition workflow.WorkflowDefinition) string {
	// executor 在模块 4 之前可以省略；默认 mock 不应让同一历史请求产生新的幂等哈希。
	definition.Tasks = append([]workflow.TaskDefinition(nil), definition.Tasks...)
	for index := range definition.Tasks {
		if definition.Tasks[index].Executor == workflow.ExecutorMock {
			definition.Tasks[index].Executor = ""
		}
	}
	body, _ := json.Marshal(definition)
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}
