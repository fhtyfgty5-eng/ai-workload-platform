package workflow

// TaskKey 是任务在单个工作流定义中的唯一正式标识。
type TaskKey string

// RetryPolicy 描述任务允许的最大尝试次数和固定重试间隔。
type RetryPolicy struct {
	// MaxAttempts 是包含首次执行在内的最大 Attempt 总数；零值在编译时归一为 1。
	MaxAttempts int `json:"max_attempts,omitempty"`
	// IntervalMillis 是一次可重试失败结束后，到下一次 Attempt 可启动前的固定等待时间。
	IntervalMillis int64 `json:"interval_ms,omitempty"`
}

// TaskDefinition 描述一个任务的动作、依赖、重试和超时配置。
type TaskDefinition struct {
	// Key 是任务在当前工作流定义中的唯一正式标识。
	Key TaskKey `json:"key"`
	// Action 是传给 Executor 的不透明动作名称，工作流内核不会把它解释为命令或代码。
	Action string `json:"action"`
	// Input 是传给 Executor 的可选 JSON 对象；内核只校验和复制，不解释其中业务字段。
	Input map[string]any `json:"input,omitempty"`
	// DependsOn 列出当前任务必须等待成功的直接上游任务 Key。
	DependsOn []TaskKey `json:"depends_on,omitempty"`
	// Retry 决定可重试失败或超时后的最大尝试次数与固定间隔。
	Retry RetryPolicy `json:"retry,omitempty"`
	// TimeoutMillis 限制单次 Attempt，而不是整个 Task 或 Workflow 的总运行时间。
	TimeoutMillis int64 `json:"timeout_ms"`
}

// WorkflowDefinition 描述可重复创建运行实例的静态工作流配置。
type WorkflowDefinition struct {
	// ID 是定义级机器标识，不是某次运行实例的 RunID。
	ID string `json:"id"`
	// Concurrency 是同一 Run 内允许同时执行的最大 Attempt 数。
	Concurrency int `json:"concurrency"`
	// Tasks 的顺序在编译后保持稳定，并成为内部数组下标的来源。
	Tasks []TaskDefinition `json:"tasks"`
}
