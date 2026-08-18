package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

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
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return workflow.RunSnapshot{}, workflow.ErrRunNotFound
	}
	if err != nil {
		return workflow.RunSnapshot{}, err
	}
	var snapshot workflow.RunSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return workflow.RunSnapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}
