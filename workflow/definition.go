package workflow

// TaskKey 是任务在单个工作流定义中的唯一正式标识。
type TaskKey string

// RetryPolicy 描述任务允许的最大尝试次数和固定重试间隔。
type RetryPolicy struct {
	MaxAttempts    int   `json:"max_attempts,omitempty"`
	IntervalMillis int64 `json:"interval_ms,omitempty"`
}

// TaskDefinition 描述一个任务的动作、依赖、重试和超时配置。
type TaskDefinition struct {
	Key           TaskKey     `json:"key"`
	Action        string      `json:"action"`
	DependsOn     []TaskKey   `json:"depends_on,omitempty"`
	Retry         RetryPolicy `json:"retry,omitempty"`
	TimeoutMillis int64       `json:"timeout_ms"`
}

// WorkflowDefinition 描述可重复创建运行实例的静态工作流配置。
type WorkflowDefinition struct {
	ID          string           `json:"id"`
	Concurrency int              `json:"concurrency"`
	Tasks       []TaskDefinition `json:"tasks"`
}
