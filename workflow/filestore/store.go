package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
)

// Store 使用每个 Run 一个 JSON 文件保存工作流快照。
type Store struct {
	// dir 是所有 Run 快照和同目录临时文件的根目录。
	dir string
	// mu 串行化同一进程内的存在性检查和替换，避免检查后写入竞争。
	mu sync.Mutex
	// rename 生产环境使用 os.Rename，保留为字段以注入替换失败测试。
	rename func(string, string) error
}

var _ workflow.Persistence = (*Store)(nil)

// New 创建仅支持 macOS 和 Linux 原子替换语义的文件 Store。
func New(dir string) (*Store, error) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return nil, workflow.ErrAtomicReplaceUnsupported
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	return &Store{dir: dir, rename: os.Rename}, nil
}

// path 拒绝空值和路径分隔符，确保 RunID 只能定位 Store 根目录内的单个 JSON 文件。
func (s *Store) path(id workflow.RunID) (string, error) {
	value := string(id)
	if value == "" || strings.ContainsAny(value, `/\`) || value == "." || value == ".." {
		return "", fmt.Errorf("invalid run id %q", value)
	}
	return filepath.Join(s.dir, value+".json"), nil
}

// Create 创建新的运行快照，已存在的 RunID 不会被覆盖。
func (s *Store) Create(ctx context.Context, snapshot workflow.RunSnapshot) error {
	path, err := s.path(snapshot.Run.ID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 存在性检查与实际创建必须位于同一临界区，才能保证同进程重复 Create 只有一个成功。
	if _, err := os.Stat(path); err == nil {
		return workflow.ErrRunExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.writeLocked(ctx, path, snapshot)
}

// Save 使用同目录临时文件和原子替换更新已有快照。
func (s *Store) Save(ctx context.Context, snapshot workflow.RunSnapshot) error {
	path, err := s.path(snapshot.Run.ID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return workflow.ErrRunNotFound
		}
		return err
	}
	return s.writeLocked(ctx, path, snapshot)
}

// Apply 根据行级变更重建完整文件快照，并以原子替换方式提交。
func (s *Store) Apply(ctx context.Context, change workflow.ChangeSet) error {
	path, err := s.path(change.RunID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, err := s.loadLocked(path, change.RunID)
	if err != nil {
		return err
	}
	if snapshot.Run.Revision != change.ExpectedRevision {
		return workflow.ErrRevisionConflict
	}
	after, err := workflow.ApplyChangeSetForStore(snapshot, change)
	if err != nil {
		return err
	}
	return s.writeLocked(ctx, path, after)
}

// ListNonTerminal 返回当前目录中需要启动恢复的 Run。
func (s *Store) ListNonTerminal(ctx context.Context) ([]workflow.RunID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	ids := make([]workflow.RunID, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := workflow.RunID(strings.TrimSuffix(entry.Name(), ".json"))
		snapshot, err := s.Load(ctx, id)
		if err != nil {
			return nil, err
		}
		if !workflow.IsWorkflowTerminalForStore(snapshot.Run.Status) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// RequestCancel 为非终态 Run 持久记录第一次取消请求。
func (s *Store) RequestCancel(ctx context.Context, id workflow.RunID, at time.Time) (workflow.WorkflowRun, error) {
	if err := ctx.Err(); err != nil {
		return workflow.WorkflowRun{}, err
	}
	path, err := s.path(id)
	if err != nil {
		return workflow.WorkflowRun{}, err
	}

	// 取消的读取、幂等判断和写回必须共用一个临界区，否则并发请求会同时基于旧 revision 提交。
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, err := s.loadLocked(path, id)
	if err != nil {
		return workflow.WorkflowRun{}, err
	}
	if workflow.IsWorkflowTerminalForStore(snapshot.Run.Status) {
		return snapshot.Run, nil
	}
	if snapshot.Run.CancelRequestedAt != nil {
		return snapshot.Run, nil
	}
	snapshot.Run.CancelRequestedAt = &at
	snapshot.Run.Revision++
	if err := s.writeLocked(ctx, path, snapshot); err != nil {
		return workflow.WorkflowRun{}, err
	}
	return snapshot.Run, nil
}

// writeLocked 以原子替换方式写入完整快照；调用方必须持有 s.mu。
func (s *Store) writeLocked(ctx context.Context, target string, snapshot workflow.RunSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	// 临时文件必须位于目标目录，避免跨文件系统 Rename 破坏原子替换语义。
	temp, err := os.CreateTemp(s.dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	// 必须按“写入、文件同步、关闭、Rename”的顺序完成；任一步失败都保留旧目标文件。
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temporary snapshot: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync temporary snapshot: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary snapshot: %w", err)
	}
	if err := s.rename(tempName, target); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}

	// 文件同步后还要同步目录，确保崩溃恢复时目录项更新已经持久化。
	directory, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}

// Load 读取并解码指定 RunID 的完整快照。
func (s *Store) Load(ctx context.Context, id workflow.RunID) (workflow.RunSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunSnapshot{}, err
	}
	path, err := s.path(id)
	if err != nil {
		return workflow.RunSnapshot{}, err
	}
	return s.loadPath(ctx, path, id)
}

func (s *Store) loadPath(ctx context.Context, path string, expectedID workflow.RunID) (workflow.RunSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunSnapshot{}, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return workflow.RunSnapshot{}, workflow.ErrRunNotFound
	}
	if err != nil {
		return workflow.RunSnapshot{}, err
	}
	var snapshot workflow.RunSnapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&snapshot); err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	if snapshot.Run.ID != expectedID {
		return workflow.RunSnapshot{}, fmt.Errorf("stored run id %q does not match requested run %q", snapshot.Run.ID, expectedID)
	}
	return snapshot, nil
}

func (s *Store) loadLocked(path string, expectedID workflow.RunID) (workflow.RunSnapshot, error) {
	return s.loadPath(context.Background(), path, expectedID)
}
