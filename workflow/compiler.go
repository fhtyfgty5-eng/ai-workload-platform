package workflow

import (
	"fmt"
	"regexp"
	"strings"
)

const maxWorkflowTasks = 10_000

var identifierPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

// CompiledWorkflow 保存定义校验后可由多个运行实例共享的只读索引和 DAG 结构。
type CompiledWorkflow struct {
	// definition 是与调用方输入隔离的只读副本，可被多个 Run 安全共享。
	definition *WorkflowDefinition
	// index 把外部 TaskKey 映射为 Tasks、dependencies 和 successors 共用的数组下标。
	index map[TaskKey]int
	// dependencies[i] 保存第 i 个任务的直接上游任务下标。
	dependencies [][]int
	// successors[i] 保存第 i 个任务的直接下游任务下标。
	successors [][]int
}

// Definition returns an isolated copy of the validated workflow definition.
func (c *CompiledWorkflow) Definition() WorkflowDefinition {
	if c == nil || c.definition == nil {
		return WorkflowDefinition{}
	}
	return cloneDefinition(*c.definition)
}

// Compile 校验工作流定义，并在线性遍历中建立调度所需的索引和邻接表。
func Compile(def WorkflowDefinition) (*CompiledWorkflow, error) {
	// 深拷贝隔离调用方后续修改，确保编译结果能在多个运行实例之间安全共享。
	def = cloneDefinition(def)

	// 第一遍只校验任务自身属性，并建立 TaskKey 到稳定数组下标的唯一映射。
	if def.ID == "" {
		return nil, fmt.Errorf("workflow id is required")
	}
	if !identifierPattern.MatchString(def.ID) {
		return nil, fmt.Errorf("invalid workflow id %q", def.ID)
	}
	if def.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency must be greater than zero")
	}
	if len(def.Tasks) == 0 {
		return nil, fmt.Errorf("workflow must contain at least one task")
	}
	if len(def.Tasks) > maxWorkflowTasks {
		return nil, fmt.Errorf("workflow cannot contain more than 10000 tasks")
	}

	index := make(map[TaskKey]int, len(def.Tasks))
	for i := range def.Tasks {
		task := &def.Tasks[i]
		if !identifierPattern.MatchString(string(task.Key)) {
			return nil, fmt.Errorf("invalid task key %q", task.Key)
		}
		if _, exists := index[task.Key]; exists {
			return nil, fmt.Errorf("duplicate task key %q", task.Key)
		}
		if strings.TrimSpace(task.Action) == "" {
			return nil, fmt.Errorf("task %q action is required", task.Key)
		}
		if task.TimeoutMillis <= 0 {
			return nil, fmt.Errorf("task %q timeout must be greater than zero", task.Key)
		}
		if task.Retry.MaxAttempts == 0 {
			task.Retry.MaxAttempts = 1
		}
		if task.Retry.MaxAttempts < 1 || task.Retry.IntervalMillis < 0 {
			return nil, fmt.Errorf("invalid retry policy for task %q", task.Key)
		}
		index[task.Key] = i
	}

	// 第二遍把外部 TaskKey 依赖编译为整数邻接表，后续调度不再反复查找字符串。
	dependencies := make([][]int, len(def.Tasks))
	successors := make([][]int, len(def.Tasks))
	indegree := make([]int, len(def.Tasks))
	for taskIndex, task := range def.Tasks {
		seen := make(map[int]struct{}, len(task.DependsOn))
		for _, dependencyKey := range task.DependsOn {
			dependencyIndex, exists := index[dependencyKey]
			if !exists {
				return nil, fmt.Errorf("task %q depends on missing task %q", task.Key, dependencyKey)
			}
			if _, duplicate := seen[dependencyIndex]; duplicate {
				return nil, fmt.Errorf("task %q repeats dependency %q", task.Key, dependencyKey)
			}
			seen[dependencyIndex] = struct{}{}
			dependencies[taskIndex] = append(dependencies[taskIndex], dependencyIndex)
			successors[dependencyIndex] = append(successors[dependencyIndex], taskIndex)
			indegree[taskIndex]++
		}
	}

	// 所有入度为零的任务都可作为拓扑遍历起点；indegree 只用于编译期环检测，可在遍历中消耗。
	queue := make([]int, 0, len(def.Tasks))
	for i, degree := range indegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}

	visited := 0
	// Kahn 算法只访问每个任务和每条依赖一次，以 O(V + E) 完成环检测。
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range successors[current] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(def.Tasks) {
		return nil, fmt.Errorf("workflow contains a dependency cycle")
	}

	return &CompiledWorkflow{
		definition:   &def,
		index:        index,
		dependencies: dependencies,
		successors:   successors,
	}, nil
}

// cloneDefinition 深拷贝可变切片，避免调用方修改原定义破坏编译结果的不变性。
func cloneDefinition(def WorkflowDefinition) WorkflowDefinition {
	clone := def
	clone.Tasks = make([]TaskDefinition, len(def.Tasks))
	copy(clone.Tasks, def.Tasks)
	for i := range clone.Tasks {
		clone.Tasks[i].DependsOn = append([]TaskKey(nil), def.Tasks[i].DependsOn...)
	}
	return clone
}
