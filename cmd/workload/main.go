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
	"strconv"
	"syscall"

	"github.com/fhtyfgty5-eng/ai-workload-platform/agent"
	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/agentapp"
	"github.com/fhtyfgty5-eng/ai-workload-platform/pkg/workloadclient"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/filestore"
	"github.com/fhtyfgty5-eng/ai-workload-platform/workflow/mockexec"
)

const usage = "usage: workload local run <workflow.json> | workload local status <run-id>"

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
	if len(args) > 0 && args[0] == "agent" {
		return runAgent(ctx, args[1:], stdout, stderr, os.Getenv)
	}
	if len(args) > 0 && args[0] == "local" {
		return runWithExecutorFactory(ctx, args[1:], dataDir, stdout, stderr, func(definition workflow.WorkflowDefinition) workflow.Executor {
			return mockexec.New(workflow.RealClock{}, successScripts(definition))
		})
	}
	if len(args) > 0 && (args[0] == "workflow" || (args[0] == "run" && len(args) > 1 && (args[1] == "start" || args[1] == "status" || args[1] == "cancel"))) {
		return runWithEnvironment(ctx, args, stdout, stderr, os.Getenv)
	}
	// 模块 1 的无前缀 run/status 继续作为兼容入口，公开文档统一使用 local 命名空间。
	return runWithExecutorFactory(ctx, args, dataDir, stdout, stderr, func(definition workflow.WorkflowDefinition) workflow.Executor {
		return mockexec.New(workflow.RealClock{}, successScripts(definition))
	})
}

const agentUsage = "usage: workload agent draft <goal> [--model mock|http] [--output <file>] | workload agent validate <draft.json> [--output <file>] | workload agent confirm <draft.json> --hash <hash> [--output <file>]"

type agentOptions struct {
	model  string
	output string
	hash   string
}

func runAgent(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, agentUsage)
		return 2
	}
	command, input := args[0], args[1]
	options, ok := parseAgentOptions(args[2:])
	if !ok || (command != "draft" && options.model != "") || (command != "confirm" && options.hash != "") {
		fmt.Fprintln(stderr, agentUsage)
		return 2
	}
	if command == "confirm" && options.hash == "" {
		fmt.Fprintln(stderr, "--hash is required")
		return 2
	}
	service, err := newAgentService(options.model, getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	switch command {
	case "draft":
		draft, err := service.GenerateDraft(ctx, input)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", agent.CodeOf(err), err)
			return 1
		}
		return writeAgentJSON(stdout, stderr, options.output, draft)
	case "validate":
		draft, err := readDraft(input)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		validated, err := service.ValidateDraft(ctx, draft)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", agent.CodeOf(err), err)
			return 1
		}
		exit := writeAgentJSON(stdout, stderr, options.output, validated)
		if exit != 0 || len(validated.Validation.Errors) > 0 {
			return 1
		}
		return 0
	case "confirm":
		draft, err := readDraft(input)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		definition, err := service.ConfirmDraft(ctx, draft, options.hash)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", agent.CodeOf(err), err)
			return 1
		}
		return writeAgentJSON(stdout, stderr, options.output, definition)
	default:
		fmt.Fprintln(stderr, agentUsage)
		return 2
	}
}

func parseAgentOptions(args []string) (agentOptions, bool) {
	options := agentOptions{}
	if len(args)%2 != 0 {
		return options, false
	}
	seen := map[string]bool{}
	for index := 0; index < len(args); index += 2 {
		flag, value := args[index], args[index+1]
		if value == "" || seen[flag] {
			return options, false
		}
		seen[flag] = true
		switch flag {
		case "--model":
			if value != "mock" && value != "http" {
				return options, false
			}
			options.model = value
		case "--output":
			options.output = value
		case "--hash":
			options.hash = value
		default:
			return options, false
		}
	}
	return options, true
}

func newAgentService(modelName string, getenv func(string) string) (*agent.Service, error) {
	return agentapp.NewService(modelName, getenv)
}

func readDraft(path string) (agent.WorkflowDraft, error) {
	file, err := os.Open(path)
	if err != nil {
		return agent.WorkflowDraft{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var draft agent.WorkflowDraft
	if err := decoder.Decode(&draft); err != nil {
		return agent.WorkflowDraft{}, fmt.Errorf("decode draft: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return agent.WorkflowDraft{}, fmt.Errorf("draft file must contain exactly one JSON value")
	}
	return draft, nil
}

func writeAgentJSON(stdout, stderr io.Writer, output string, value any) int {
	if output == "" {
		if err := json.NewEncoder(stdout).Encode(value); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	body = append(body, '\n')
	if err := os.WriteFile(output, body, 0o600); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func runWithEnvironment(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	serverURL := getenv("WORKLOAD_SERVER_URL")
	token := getenv("WORKLOAD_TOKEN")
	if serverURL == "" || token == "" {
		fmt.Fprintln(stderr, "WORKLOAD_SERVER_URL and WORKLOAD_TOKEN are required")
		return 2
	}
	client := workloadclient.New(serverURL, token)
	if len(args) == 5 && args[0] == "workflow" && args[1] == "create" {
		key, ok := idempotencyArg(args[3:])
		if !ok {
			fmt.Fprintln(stderr, "--idempotency-key is required")
			return 2
		}
		definition, err := readDefinition(args[2])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ref, err := client.CreateWorkflow(ctx, key, definition)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return encodeResult(stdout, ref)
	}
	if len(args) == 6 && args[0] == "workflow" && args[1] == "add-version" {
		key, ok := idempotencyArg(args[4:])
		if !ok {
			fmt.Fprintln(stderr, "--idempotency-key is required")
			return 2
		}
		definition, err := readDefinition(args[3])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ref, err := client.CreateWorkflowVersion(ctx, args[2], key, definition)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return encodeResult(stdout, ref)
	}
	if len(args) >= 5 && args[0] == "run" && args[1] == "start" {
		version, key, ok := parseStartArgs(args[3:])
		if !ok {
			fmt.Fprintln(stderr, "--version and --idempotency-key are required")
			return 2
		}
		response, err := client.StartRun(ctx, args[2], version, key)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return encodeResult(stdout, response)
	}
	if len(args) == 3 && args[0] == "run" && args[1] == "status" {
		response, err := client.GetRun(ctx, workflow.RunID(args[2]))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return encodeResult(stdout, response)
	}
	if len(args) == 3 && args[0] == "run" && args[1] == "cancel" {
		if err := client.CancelRun(ctx, workflow.RunID(args[2])); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprintln(stderr, "unsupported control-plane command")
	return 2
}

func idempotencyArg(args []string) (string, bool) {
	if len(args) == 2 && args[0] == "--idempotency-key" && args[1] != "" {
		return args[1], true
	}
	return "", false
}

func parseStartArgs(args []string) (int, string, bool) {
	if len(args) != 4 || args[0] != "--version" || args[2] != "--idempotency-key" || args[1] == "" || args[3] == "" {
		return 0, "", false
	}
	version, err := strconv.Atoi(args[1])
	return version, args[3], err == nil && version > 0
}

func encodeResult(stdout io.Writer, value any) int {
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		return 1
	}
	return 0
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
	// CLI 的信号表示用户主动停止本次本地 Run，因此显式转为业务取消；Engine 的父 Context
	// 留给服务进程安全退出使用，不能把基础设施中断误记成用户取消。
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = engine.Cancel(context.WithoutCancel(ctx), id)
		case <-watchDone:
		}
	}()
	runState, err := engine.Execute(context.Background(), id)
	close(watchDone)
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
	decoder.UseNumber()
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
