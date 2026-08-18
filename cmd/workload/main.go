package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/filestore"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/mockexec"
)

const usage = "usage: workload run <workflow.json> | workload status <run-id>"

type executorFactory func(workflow.WorkflowDefinition) workflow.Executor

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dataDir := os.Getenv("WORKLOAD_DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join(".workload", "runs")
	}
	os.Exit(run(ctx, os.Args[1:], dataDir, os.Stdout, os.Stderr))
}

// run 执行单次 CLI 调用并返回进程退出码，显式传入输入输出以便自动化测试。
func run(ctx context.Context, args []string, dataDir string, stdout, stderr io.Writer) int {
	return runWithExecutorFactory(ctx, args, dataDir, stdout, stderr, func(definition workflow.WorkflowDefinition) workflow.Executor {
		return mockexec.New(workflow.RealClock{}, successScripts(definition))
	})
}

// runWithExecutorFactory 保持命令路径不变，同时允许取消测试注入可控 Executor。
func runWithExecutorFactory(
	ctx context.Context,
	args []string,
	dataDir string,
	stdout io.Writer,
	stderr io.Writer,
	newExecutor executorFactory,
) int {
	if len(args) != 2 || (args[0] != "run" && args[0] != "status") {
		fmt.Fprintln(stderr, usage)
		return 2
	}

	store, err := filestore.New(dataDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if args[0] == "status" {
		// status 只加载并编码 Run，不调用 Engine，也不保存或改写快照。
		snapshot, err := store.Load(ctx, workflow.RunID(args[1]))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := json.NewEncoder(stdout).Encode(snapshot.Run); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	definition, err := readDefinition(args[1])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	compiled, err := workflow.Compile(definition)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	engine, err := workflow.NewEngine(store, newExecutor(definition), workflow.EngineOptions{})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	id, err := engine.CreateRun(ctx, compiled)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	runState, err := engine.Execute(ctx, id)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if _, err := fmt.Fprintf(stdout, "run_id=%s status=%s\n", id, runState.Status); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return 1
	}
	if runState.Status != workflow.WorkflowSucceeded {
		return 1
	}
	return 0
}

// readDefinition 严格读取单个 JSON 工作流定义，拒绝未知字段和尾随 JSON 值。
func readDefinition(path string) (workflow.WorkflowDefinition, error) {
	file, err := os.Open(path)
	if err != nil {
		return workflow.WorkflowDefinition{}, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var definition workflow.WorkflowDefinition
	if err := decoder.Decode(&definition); err != nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("decode workflow: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return workflow.WorkflowDefinition{}, fmt.Errorf("workflow file must contain exactly one JSON value")
	}
	return definition, nil
}

// successScripts 为公开示例中的每个任务生成一次确定成功的 Mock 执行结果。
func successScripts(definition workflow.WorkflowDefinition) map[mockexec.ScriptKey][]mockexec.Step {
	scripts := make(map[mockexec.ScriptKey][]mockexec.Step, len(definition.Tasks))
	for _, task := range definition.Tasks {
		scripts[mockexec.ScriptKey{DefinitionID: definition.ID, TaskKey: task.Key}] = []mockexec.Step{{
			Kind:   workflow.ResultSuccess,
			Output: "completed:" + task.Action,
		}}
	}
	return scripts
}
