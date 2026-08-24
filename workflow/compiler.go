package workflow

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

const maxWorkflowTasks = 10_000
const maxTaskInputBytes = 64 * 1024

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
	// 先验证调用方提供的 Input，再执行深拷贝；这也能在复制前拒绝循环引用。
	for _, task := range def.Tasks {
		if err := validateJSONValue(task.Input, make(map[jsonVisit]bool)); err != nil {
			return nil, fmt.Errorf("task %q input must contain only JSON values: %w", task.Key, err)
		}
		body, err := json.Marshal(task.Input)
		if err != nil {
			return nil, fmt.Errorf("task %q input must be a JSON object: %w", task.Key, err)
		}
		if len(body) > maxTaskInputBytes {
			return nil, fmt.Errorf("task %q input cannot exceed %d bytes", task.Key, maxTaskInputBytes)
		}
	}
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

type jsonVisit struct {
	kind reflect.Kind
	ptr  uintptr
}

// validateJSONValue 限制公开 Go API 只能传入 JSON 数据模型，避免自定义编码逻辑绕过校验。
func validateJSONValue(value any, visiting map[jsonVisit]bool) error {
	if value == nil {
		return nil
	}
	if _, ok := value.(json.Marshaler); ok {
		return fmt.Errorf("custom JSON marshaler %T is not supported", value)
	}
	if _, ok := value.(encoding.TextMarshaler); ok {
		return fmt.Errorf("custom text marshaler %T is not supported", value)
	}

	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Interface {
		if reflected.IsNil() {
			return nil
		}
		reflected = reflected.Elem()
	}
	switch reflected.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("map key type %s is not a string", reflected.Type().Key())
		}
		return validateJSONCollection(reflected, visiting, func() error {
			iterator := reflected.MapRange()
			for iterator.Next() {
				if err := validateJSONValue(iterator.Value().Interface(), visiting); err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Slice:
		if reflected.Type().Elem().Kind() == reflect.Uint8 {
			return fmt.Errorf("byte slices are not JSON arrays")
		}
		return validateJSONCollection(reflected, visiting, func() error {
			for index := 0; index < reflected.Len(); index++ {
				if err := validateJSONValue(reflected.Index(index).Interface(), visiting); err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Array:
		for index := 0; index < reflected.Len(); index++ {
			if err := validateJSONValue(reflected.Index(index).Interface(), visiting); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("type %T is not part of the JSON data model", value)
	}
}

func validateJSONCollection(value reflect.Value, visiting map[jsonVisit]bool, validate func() error) error {
	if value.IsNil() {
		return nil
	}
	visit := jsonVisit{kind: value.Kind(), ptr: value.Pointer()}
	if visiting[visit] {
		return fmt.Errorf("cyclic JSON value")
	}
	visiting[visit] = true
	defer delete(visiting, visit)
	return validate()
}

// cloneDefinition 深拷贝可变切片，避免调用方修改原定义破坏编译结果的不变性。
func cloneDefinition(def WorkflowDefinition) WorkflowDefinition {
	clone := def
	clone.Tasks = make([]TaskDefinition, len(def.Tasks))
	copy(clone.Tasks, def.Tasks)
	for i := range clone.Tasks {
		clone.Tasks[i].DependsOn = append([]TaskKey(nil), def.Tasks[i].DependsOn...)
		clone.Tasks[i].Input = cloneTaskInput(def.Tasks[i].Input)
	}
	return clone
}

// cloneTaskInput 通过 JSON 往返复制嵌套对象，避免调用方或 Executor 修改共享定义。
// 调用前输入已经由 Compile 校验，因此编码或解码错误表示内部不变量被破坏。
func cloneTaskInput(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	body, err := json.Marshal(input)
	if err != nil {
		panic("clone validated task input: " + err.Error())
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var cloned map[string]any
	if err := decoder.Decode(&cloned); err != nil {
		panic("decode validated task input: " + err.Error())
	}
	return cloned
}
