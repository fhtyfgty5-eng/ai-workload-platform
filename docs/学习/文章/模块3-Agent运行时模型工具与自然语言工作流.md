# 从 0 构建 AI Workload Platform（五）：Agent Runtime、工具权限与自然语言工作流

## 摘要

模块 3 在已有的可靠工作流内核和控制面之上，增加了从自然语言目标到结构化工作流草稿的入口。本文先展开一次完整流转，再解释 Agent Runtime、模型适配器、工具调用、权限、预算、草稿校验、内容哈希和人工确认如何协作。

当前成果证明的是“生成可审核草稿，经校验和确认后交给原有可靠执行链路”，不是让模型绕过用户和控制面直接执行真实业务，也不能证明真实模型的规划质量或生产可用性。

## 1. 本篇要解决什么问题

AI Workload Platform 是一个正在从零开发的可靠运行时和调度平台。前面的模块已经能够校验 DAG（Directed Acyclic Graph，有向无环图）、管理任务状态、处理重试与恢复，并通过 HTTP 控制面和 PostgreSQL 保存工作流。到这里，用户仍然必须手写结构化 WorkflowDefinition：任务叫什么、执行什么 Action、依赖谁、输入参数是什么，都要提前确定。

模块 3 增加一条新的入口：用户可以先描述目标，由 Agent 生成结构化工作流草稿；平台再检查任务、参数、依赖、权限、任务超时和 Agent Workflow 并发，最后由用户明确确认。确认后的定义才能进入原有可靠执行链路。

本篇会讲清楚：

- Agent、模型、工具和 Agent Runtime 分别是什么；
- 为什么不能让大模型生成 JSON 后直接执行；
- 如何隔离模型供应商协议；
- 如何用工具注册表限制 Agent 能做什么；
- 为什么要区分事实、假设和待确认问题；
- 草稿校验、内容哈希和人工确认如何协作；
- 超时、取消、预算、重试和审计怎样形成运行边界；
- 当前实现已经验证了什么，还没有解决什么。

## 目录

1. 本篇要解决什么问题
2. 先看一次完整运行过程
3. Agent、模型、工具与 Runtime
4. 为什么不能让模型输出后直接执行
5. 工作流草稿为什么不是普通 WorkflowDefinition
6. 为任务增加结构化 Input
7. 只读工作流目录与工具注册表
8. 草稿校验为什么分层执行
9. 内容哈希与人工确认
10. 预算、重试、超时和取消
11. 稳定错误码与审计事件
12. 为什么选择 Mock Model 和 HTTP 适配器
13. 实际 CLI 流程
14. 测试和性能结果
15. 当前限制
16. 总结

## 2. 先看一次完整运行过程

先用一个固定的文档处理目标看完整链路：

```text
用户输入：先读取 article.md，再清洗内容，最后生成摘要
```

模块 3 当前实现不会让模型直接读文件或执行命令。它只让模型发现平台登记过的任务模板，然后生成一份需要审核的工作流草稿。一次运行按以下顺序推进：

| 阶段 | 负责组件 | 输入 | 输出 | 这一阶段不负责什么 |
| --- | --- | --- | --- | --- |
| 1. 接收目标 | Agent CLI / Runtime | 自然语言目标 | `ModelRequest` | 不创建 Workflow，不启动 Run |
| 2. 发现能力 | Model Adapter + 只读目录工具 | 工具描述、查询参数 | 任务模板和参数规则 | 不执行文件、Shell 或网络操作 |
| 3. 生成草稿 | Model Adapter + Runtime | 目标和目录结果 | `WorkflowDraft` | 不把草稿当作已授权定义 |
| 4. 校验草稿 | Draft Validator + `workflow.Compile` | 任务、Input、依赖、权限和限制 | 错误、警告和新哈希 | 不替用户回答未决问题 |
| 5. 人工确认 | `agent confirm` | 用户检查后的草稿和哈希 | `WorkflowDefinition` | 不写 PostgreSQL |
| 6. 保存版本 | 模块 2 控制面 | 最终定义、幂等 Key | 不可变 Workflow Version | 不由 Agent 绕过 API 写库 |
| 7. 启动 Run | 模块 2 控制面 | Workflow ID、版本号、幂等 Key | 已持久化的 pending Run | 不把“保存定义”当成“开始执行” |
| 8. 执行任务 | 模块 1 Engine | 已创建的 Run | Run、TaskRun、Attempt 状态 | 当前 Mock Executor 不执行真实文档处理 |

因此，模块 3 的“闭环”是从自然语言目标到可确认的结构化定义，并把定义交给原有可靠执行链路；它不是“输入一句话后自动完成真实业务”。后文的每个概念都对应这张表中的一个阶段。

## 3. Agent、模型、工具与 Runtime

### 3.1 什么是 AI Agent

**AI Agent（人工智能智能体）** 是一种围绕目标组织模型调用、上下文、工具和控制流程的软件系统。模型可以根据输入生成下一步建议，Runtime 决定建议是否允许执行、执行哪个工具、什么时候停止，以及如何记录结果。

这一定义包含一个重要边界：Agent 不等于大模型。大模型负责根据输入产生输出；Agent 还需要软件层面的循环、权限、预算、取消和状态管理。

例如用户输入：

```text
先读取 article.md，再清洗内容，最后生成摘要。
```

模型可以建议三个任务及其依赖关系，但模型本身不应该自动获得读取文件、执行命令或修改数据库的权限。是否提供某个工具、工具允许哪些参数、调用失败后怎样处理，都应由 Agent Runtime 控制。

### 3.2 什么是 Model Adapter

**Model Adapter（模型适配器）** 是平台与具体模型服务之间的协议转换层。平台只依赖统一的请求和响应：

```go
type ModelAdapter interface {
    Generate(context.Context, ModelRequest) (ModelResponse, error)
}
```

不同供应商可能使用不同 URL、认证头、模型名称、错误结构和工具调用字段。适配器负责转换这些差异，但不能把供应商 SDK（Software Development Kit，软件开发工具包）的类型扩散到 Runtime、CLI（Command-Line Interface，命令行界面）或工作流内核。

这样做的价值不是让所有模型表现完全相同，而是让上层稳定地处理几类平台语义：

- 模型返回结构化草稿；
- 模型请求调用工具；
- 模型服务暂时不可用；
- 响应格式错误；
- 调用超时或被取消。

### 3.3 什么是 Tool Calling

**Tool Calling（工具调用）** 是模型先返回“希望调用哪个工具以及参数是什么”，再由应用程序决定是否真正执行的机制。模型提出请求，不直接持有工具能力。

一次典型流程如下：

```text
Runtime 把允许的工具描述发给模型
  -> 模型返回工具名和 JSON 参数
  -> Runtime 检查注册、权限、预算和参数
  -> Runtime 调用工具
  -> 工具返回结构化结果
  -> Runtime 把结果交给模型继续生成
```

如果 Runtime 收到未注册工具名，正确行为是拒绝，而不是根据名字猜测要执行什么。

### 3.4 什么是 Agent Runtime

**Agent Runtime（Agent 运行时环境）** 是真正负责执行边界的组件。当前项目中的 Runtime 负责：

- 调用 Model Adapter；
- 只暴露注册过的工具；
- 限制模型轮数、工具次数、响应大小和总时长；
- 传递超时和取消；
- 解析结构化草稿；
- 记录模型、工具、校验和确认事件；
- 把错误转换为稳定机器码。

工作流内核仍然负责 DAG 调度、任务状态、Attempt、重试和恢复。Agent Runtime 不重新实现调度器，工作流内核也不解析自然语言。

## 4. 为什么不能让模型输出后直接执行

模型输出至少有四类不确定性。

第一，格式可能错误。JSON（JavaScript Object Notation，JavaScript 对象表示法）是一种结构化文本数据格式；模型响应可能不是合法 JSON，也可能包含未知字段或超出大小限制。

第二，内容可能不完整。用户说“读取文档”，但没有说明文件来源；模型可能自行补成 `article.md`。这个值可以作为假设展示，不能伪装成用户明确提供的事实。

第三，权限可能越界。模型可能请求一个平台没有注册的工具，或给只读工具传入命令、文件路径和外部 URL。

第四，结构正确不代表业务合法。即使 JSON 能解析，仍可能存在未知 Action、缺少必填参数、循环依赖、任务超时过长或 Agent Workflow 并发超限。

因此模块 3 使用以下强制路径：

```text
自然语言目标
  -> Agent Runtime
  -> Model Adapter
  -> 受控工具调用
  -> Workflow Draft
  -> 参数、权限、任务超时、Agent Workflow 并发和 DAG 校验
  -> 用户确认
  -> WorkflowDefinition
  -> 控制面版本化保存
```

模型输出只是候选草稿。结构校验、用户授权和可靠执行是三个不同阶段。

## 5. 工作流草稿为什么不是普通 WorkflowDefinition

最终的 WorkflowDefinition 只需要描述可执行结构：Workflow ID、并发数、任务、Action、Input、依赖、重试和超时。但人工审核还需要知道“这些内容从哪里来”。因此草稿增加了以下信息：

```text
WorkflowDraft
- draft_id
- goal
- definition
- facts
- assumptions
- questions
- validation
- tool_calls
- status
- content_hash
- created_at
- confirmed_at
```

### 5.1 Facts：用户事实

`facts` 保存直接来自用户输入的陈述。例如：

```text
用户要求读取 article.md，并按读取、清洗、摘要的顺序处理。
```

这里的“事实”只表示来源是用户输入，不表示平台已经访问外部系统验证内容真伪。这个边界必须明确，否则“用户说过”和“系统核实过”会混为一谈。

### 5.2 Assumptions：Agent 假设

`assumptions` 保存模型为了补齐草稿而推断的内容。例如：

```text
清洗模式为 standard，摘要上限为 200 词。
```

假设不会自动导致校验失败，但会产生警告并展示给用户。用户确认完整草稿时，也是在确认这些假设。

### 5.3 Questions：待确认问题

`questions` 保存必须解决的缺失信息。每个问题有稳定 ID、问题文本、回答和是否解决的状态。只要仍有未解决问题，草稿就不能确认。

假设和问题的区别在于：假设有一个可供审核的默认值；问题缺少足以形成可执行定义的信息。哪些内容允许使用默认值，应该由产品规则和任务模板决定，而不是由模型任意决定。

## 6. 为任务增加结构化 Input

模块 1 的任务最初只有 `Action`：

```go
type TaskDefinition struct {
    Key           TaskKey
    Action        string
    DependsOn     []TaskKey
    Retry         RetryPolicy
    TimeoutMillis int64
}
```

仅有 Action 可以表达“执行 read-document”，却不能表达“读取哪个文档”。模块 3 因此增加可选 JSON 对象：

```go
Input map[string]any `json:"input,omitempty"`
```

选择 JSON 对象有三个原因：

1. 工作流定义本来就通过 JSON、HTTP 和 PostgreSQL 传递；
2. 不同 Action 的参数不同，统一固定结构会迫使所有任务携带无关字段；
3. 任务目录可以为每个 Action 单独声明参数类型、必填项和枚举。

代价是 Go 编译器不能在编译期检查每个 Input 字段，因此 Runtime 必须按模板做运行时校验。

Input 只允许 JSON 能表达的对象、数组、字符串、数字、布尔值和空值，不允许函数、通道、结构体、字节切片、自定义 JSON 编码器、脚本或循环引用。这里不能只用“能否调用 `json.Marshal`”作为判断，因为结构体和字节切片也能被 Go 编码，却不属于本项目约定的 Input 数据模型；状态型自定义编码器还可能让两次编码得到不同结果。单个任务输入编码后最多 64 KiB。编译阶段会深拷贝嵌套对象，Engine 再为每次 Executor 调用复制一份，防止调用方或执行器修改共享定义。

JSON 中的数字没有固定整数位宽。Go 默认把解码到 `any` 的数字保存为 `float64`，大整数可能因此失去精度。项目在 CLI、HTTP、Go 客户端、FileStore 和 PostgreSQL 恢复入口统一启用 `json.Number`，使 `9007199254740993` 这样的输入在确认、提交、加载和恢复执行后仍保持原值。

旧定义没有 Input 时字段为空，因此原有模块 1、模块 2 示例保持兼容，调度和状态转换规则没有变化。

## 7. 只读工作流目录与工具注册表

### 7.1 目录里保存什么

第一版工作流目录是进程内静态注册表，每个任务模板包含：

- 模板 ID 和 Action；
- 参数名称、类型、必填项和枚举；
- 所需权限；
- 最大任务超时；
- 是否允许 Agent 使用。

示例目录包含：

| Action | 主要输入 | 权限与超时上限 |
| --- | --- | --- |
| `read-document` | 必填字符串 `source` | 需要 `document:read`，最长 30 秒 |
| `clean-document` | 可选 `mode`，只能是 `standard` 或 `strict` | 最长 30 秒 |
| `summarize-document` | 可选数字 `max_words` | 最长 60 秒 |

### 7.2 为什么目录工具必须只读

当前目标是验证模型如何发现平台能力并生成工作流，不是让模型执行任意操作。因此工具输入只有一个查询字符串：

```json
{
  "query": "document"
}
```

输入 Schema 不提供 `path`、`command`、`url`、HTTP method 或 request body。未知字段会被严格拒绝，查询最长 1024 字节，返回值是目录内容的深拷贝。

这比“在提示词中告诉模型不要执行危险操作”更可靠，因为权限边界存在于代码接口和注册表中。提示词可以影响模型建议，不能代替服务端授权。

### 7.3 工具注册表如何拒绝越权

每个工具注册时绑定所需权限：

```go
type RegisteredTool struct {
    Tool               Tool
    RequiredPermission string
}
```

当前调用顺序是：Runtime 先建立单次工具超时 Context，注册表再查找工具并检查权限，通过后才进入目录工具。目录工具在查询前严格解析 JSON 参数，拒绝未知字段、尾随内容和超长查询；返回 Runtime 后，Runtime 还会限制响应大小并记录审计事件。

因此，未知工具和缺少权限会在进入工具实现前被拒绝；非法参数会进入目录工具的参数校验逻辑，但不会真正执行目录查询。这两个拒绝位置不同，但都不会获得文件、Shell 或网络权限。

第一版工具白名单不是安全沙箱。它只能证明当前接口没有提供高风险能力；未来新增文件、网络或代码工具时，必须重新设计权限、隔离和审批规则。

## 8. 草稿校验为什么分层执行

草稿校验依次检查：

1. 草稿标识和用户目标；
2. Agent 工作流并发是否不超过任务数和 32 的上限；
3. 模块 1 的 `workflow.Compile` 是否接受标识、任务数量、重试和 DAG；
4. 任务 Action 是否存在且允许 Agent 使用；
5. Input 必填项、类型、枚举和未知参数；
6. 任务权限和模板允许的最大超时；
7. 待确认问题是否已经解决；
8. Agent 假设是否需要给出警告。

`workflow.Compile` 仍是 DAG、标识符、任务数量和通用重试规则的唯一编译入口。Agent 校验器没有复制一套环检测算法。这样以后内核规则变化时，结构化定义和自然语言草稿不会出现两套互相冲突的判断。

校验结果一次返回全部错误和警告：

```json
{
  "errors": [],
  "warnings": [
    {
      "code": "assumption_present",
      "path": "assumptions[0]",
      "message": "Agent assumption requires user review"
    }
  ]
}
```

错误阻止确认；警告要求用户注意，但不一定阻止确认。机器读取稳定 `code`，用户阅读 `message`，字段位置由 `path` 指出。

校验失败不是草稿的死路。用户可以修正参数或回答待确认问题，再次执行 `validate`；`validated` 和 `needs_confirmation` 状态都允许回到校验步骤。新报告和新内容哈希会覆盖旧值，已经 `confirmed` 或明确 `rejected` 的草稿则不能回退。

## 9. 内容哈希与人工确认

### 9.1 哈希解决什么问题

如果用户先查看草稿，文件随后被修改，而确认命令仍然接受旧的“是”，那么用户确认的内容和最终输出可能不是同一份。

模块 3 对审核内容的规范化 JSON 计算 SHA-256：

```text
draft_id + goal + definition + facts + assumptions
         + questions + validation + tool_calls + status
  -> canonical JSON
  -> SHA-256
  -> content_hash
```

**SHA-256（Secure Hash Algorithm 256-bit，256 位安全哈希算法）** 会把输入映射为固定长度摘要。这里使用它检测内容变化，不用于加密，也不用于隐藏原文。

确认时比较三项：

- 用户命令传入的哈希；
- 草稿文件保存的哈希；
- 根据当前草稿重新计算的哈希。

三者必须完全相同。之后 Runtime 还会重新运行目录校验和 `workflow.Compile`，防止审核后模板或规则已经变化。

### 9.2 哈希不能证明什么

内容哈希不是数字签名，不能证明确认者身份，也不能阻止有文件权限的人同时修改草稿和哈希。第一版目标是防止审核内容与确认内容无意不一致。

未来进入多用户控制台后，还需要把草稿版本、操作者身份、权限和审批记录保存在控制面数据库中。

### 9.3 确认与数据库事务的边界

**事务（Transaction）** 是数据库把一组读写作为一个不可分割操作提交或回滚的机制。模块 3 的 `confirm` 只在本地重新计算哈希、校验草稿并输出 WorkflowDefinition，不写数据库，因此它本身不是数据库事务。

用户随后调用模块 2 的 `workflow create` 时，控制面才在 PostgreSQL 事务中保存幂等记录、Workflow 和不可变版本。确认成功不等于控制面提交成功；如果提交失败，可以使用相同幂等 Key 重试，而不需要让 Agent 绕过控制面直接写数据库。这个分离使“用户审核了什么”和“平台最终持久化了什么”都能分别验证。

## 10. 预算、重试、超时和取消

Agent 循环如果没有停止条件，模型可能不断请求工具或生成超大响应。第一版使用以下默认限制：

| 限制 | 默认值 | 作用 |
| --- | ---: | --- |
| 模型调用轮数 | 4 | 限制 Agent 循环长度 |
| 工具调用次数 | 8 | 防止工具调用失控 |
| 单次模型或工具响应 | 64 KiB | 限制内存和上下文增长 |
| Runtime 总时长 | 30 秒 | 为整个生成会话设置截止时间 |
| 单次工具调用 | 5 秒 | 隔离卡住的工具 |

`context.Context` 是 Go 标准库用于传递取消信号、截止时间和请求级信息的接口。父 Context 取消后，Runtime 不再发起新的模型或工具调用；HTTP 适配器也使用同一个 Context 取消网络请求。

只有标记为临时的模型传输或服务错误允许重试一次。结构错误、工具拒绝、参数错误、预算耗尽和草稿无效都不重试，因为再次执行不会自动修正这些确定性问题。

Runtime 的模型重试和工作流内核的 Task Attempt 重试不是同一层：

- Runtime 重试处理一次 Agent 生成过程中的模型服务波动；
- 工作流 Attempt 重试处理已经进入执行阶段的任务失败。

把两者混在一起会导致调用次数、费用和状态含义难以解释。

## 11. 稳定错误码与审计事件

Runtime 使用稳定错误码：

```text
model_timeout
model_unavailable
model_invalid_response
tool_not_allowed
tool_invalid_input
tool_timeout
budget_exceeded
draft_invalid
approval_required
draft_changed
canceled
```

这些错误码用于 CLI、测试和未来 HTTP API 判断行为，不能依赖可能变化的人类错误文本。

审计事件记录：

- 草稿生成开始与完成；
- 模型调用完成或重试；
- 工具允许或拒绝；
- 草稿校验；
- 用户确认；
- 内容哈希和非敏感结果分类。

生成失败、取消和预算耗尽会统一记录 `draft.generation_failed`，并保存已经消耗的模型轮数和工具次数。终止事件使用不继承业务取消信号、但带短截止时间的独立 Context，避免原请求取消后连失败原因也无法留下；审计失败不会覆盖原始业务错误码。

审计不是把所有输入原样写入日志。API Key、Authorization、Token 和密码字段会被替换为 `[REDACTED]`。HTTP 适配器不记录完整 Endpoint，避免查询参数中的凭证意外进入日志。

当前审计接收器只保存在内存中，适合测试协议和事件内容；进程退出后不会保留。持久审计、指标和分布式调用链属于后续可观测性模块。

## 12. 为什么选择 Mock Model 和 HTTP 适配器

### 12.1 Mock Model 的作用

Mock Model 用固定逻辑模拟两轮响应：第一轮请求工作流目录，第二轮返回三任务草稿。它使以下行为能够离线复现：

- 工具调用循环；
- 工具权限拒绝；
- 预算耗尽；
- 结构错误；
- 取消和超时；
- 草稿校验和确认。

Mock Model 不评估真实模型的自然语言理解、推理质量或指令遵循能力。它验证的是平台协议和控制边界。

### 12.2 为什么真实适配器先使用标准库 HTTP

第一版真实适配器使用 Go 标准库 `net/http` 实现 Chat Completions 风格的 JSON 协议。Chat Completions 是按消息列表提交对话并接收模型回复的一类 HTTP 接口形式。JSON Schema 是描述 JSON 字段、类型、必填项和嵌套结构的规则；请求用它约束 WorkflowDraft，工具续轮则使用 assistant `tool_calls` 和带 `tool_call_id` 的 `role=tool` 消息。选择标准库实现的原因是：

- 当前只需要一个最小人工演示适配器；
- 可以明确控制超时、响应大小、错误分类和日志；
- 不把供应商 SDK 类型传入项目公开接口；
- 本地 `httptest.Server` 可以验证完整 HTTP 合约。

携带 API Key 的远程 Endpoint 必须使用 HTTPS，本机回环地址才允许 HTTP；客户端不自动跟随重定向，避免凭证被转发到另一个地址。不同供应商对工具调用和结构化输出的支持仍可能不同，所以“本地协议测试通过”不等于“所有兼容供应商已经验证”。

没有优先引入供应商 SDK，是因为 SDK 会增加版本、类型和依赖绑定，而当前没有流式响应、批处理或供应商专属能力需求。如果后续需要供应商特有的流式事件、Token 统计或重试策略，可以在相同 ModelAdapter 后增加独立实现。

### 12.3 为什么没有使用 LangChain 或 LangGraph

LangChain 和 LangGraph 提供模型、工具和 Agent 编排能力，适合快速组合复杂 AI 应用。本项目当前没有优先使用它们，原因不是这些框架能力不足，而是模块 3 需要直接展示并验证：

- 工具白名单在哪里检查；
- 预算在哪一轮消耗；
- 取消如何停止后续调用；
- 错误如何映射为平台语义；
- 草稿为什么不能直接执行。

这些边界使用少量 Go 接口可以清楚实现，并与现有 Go 工作流内核保持同一进程和类型系统。代价是项目需要自行维护模型循环和工具协议。以后出现复杂状态图、模型生态集成或 Python 工具链需求时，应重新比较框架集成成本，而不是永久排斥成熟框架。

### 12.4 为什么没有先做多 Agent

多 Agent 会增加角色分工、消息路由、共享上下文、重复工具调用、死循环和成本归属问题。单 Agent 的权限、预算、确认和审计尚未稳定时，多 Agent 只会扩大不确定性。

当前自然语言草稿由一个 Agent 完成，普通程序任务仍是一等能力。只有真实场景证明单 Agent 无法满足职责隔离或并行协作需求时，才引入多 Agent。

## 13. 实际 CLI 流程

创建本地演示目录并生成草稿：

```bash
mkdir -p .workload/agent-demo

go run ./cmd/workload agent draft \
  '先读取 article.md，再清洗内容，最后生成摘要' \
  --model mock \
  --output .workload/agent-demo/draft.json
```

校验草稿：

```bash
go run ./cmd/workload agent validate \
  .workload/agent-demo/draft.json \
  --output .workload/agent-demo/validated.json
```

读取哈希并确认：

```bash
DRAFT_HASH="$(jq -r '.content_hash' .workload/agent-demo/validated.json)"

go run ./cmd/workload agent confirm \
  .workload/agent-demo/validated.json \
  --hash "$DRAFT_HASH" \
  --output .workload/agent-demo/workflow.json
```

实际输出的最终定义包含：

```json
{
  "id": "agent-document-pipeline",
  "concurrency": 1,
  "tasks": [
    {
      "key": "read",
      "action": "read-document",
      "input": {"source": "article.md"},
      "retry": {"max_attempts": 1},
      "timeout_ms": 30000
    },
    {
      "key": "clean",
      "action": "clean-document",
      "input": {"mode": "standard"},
      "depends_on": ["read"],
      "retry": {"max_attempts": 1},
      "timeout_ms": 30000
    },
    {
      "key": "summarize",
      "action": "summarize-document",
      "input": {"max_words": 200},
      "depends_on": ["clean"],
      "retry": {"max_attempts": 1},
      "timeout_ms": 60000
    }
  ]
}
```

`confirm` 只输出定义，不创建 Workflow，也不启动 Run。在控制面已启动，且 `WORKLOAD_SERVER_URL` 和 operator `WORKLOAD_TOKEN` 已导出的终端中，首次保存这份不可变定义：

```bash
go run ./cmd/workload workflow create \
  .workload/agent-demo/workflow.json \
  --idempotency-key agent-demo-workflow-v1
```

这个命令修改 PostgreSQL，创建 `agent-document-pipeline` 的 Workflow Version 1，但仍不会开始执行。使用相同请求和幂等 Key 重放时会返回同一结果；如果该 Workflow 已由其他请求创建，应直接使用已有版本，不要换一个幂等 Key 重复创建同一 ID。

然后显式创建并启动一次 Run：

```bash
go run ./cmd/workload run start \
  agent-document-pipeline \
  --version 1 \
  --idempotency-key agent-demo-run-1
```

这个命令先在 PostgreSQL 中创建 pending Run，事务提交后再交给 Engine 后台执行。当前控制面使用 Mock Executor，因此 Run 可以展示状态流转，但不会真正读取 `article.md`、清洗内容或生成摘要。

## 14. 测试和性能结果

默认测试不访问外部模型，覆盖模型、工具、草稿、校验、确认、HTTP 合约和 CLI。HTTP 适配器使用本机测试服务器模拟 429、非法响应和取消，不需要真实 API Key。

本机 Apple M4、Go 1.26 环境下运行五轮基准，平均结果为：

| 场景 | 五轮平均 |
| --- | ---: |
| Mock 草稿生成 | 16.726 微秒/次 |
| 草稿校验 | 7.531 微秒/次 |
| 三模板目录查询 | 869.86 纳秒/次 |
| 已取消请求快速返回并记录失败审计 | 923.12 纳秒/次 |

这些结果说明当前本地控制逻辑没有暴露毫秒级固定开销，但不能推导真实 Agent 的响应时间。真实模型延迟主要受推理、网络、供应商排队、限流和输出长度影响，通常远大于这里测量的本地 JSON 和校验成本。

当前分配量仍有优化空间：Mock 生成约 21.8 KiB、180 次分配，草稿校验约 12.0 KiB、123 次分配。已取消请求约 1.9 KiB、15 次分配，增加的开销来自失败审计事件和短时审计 Context；它换来取消原因与预算快照不会丢失。主要分配来源还包括 JSON 编解码、嵌套 Input 深拷贝和草稿哈希。当前路径不在高频任务调度热循环中，因此先保留清晰边界和正确性证据，后续只有在实际负载证明它成为瓶颈时再优化。

## 15. 当前限制

模块 3 已经证明：

- Agent 可以通过受控模型和只读工具生成结构化草稿；
- 平台可以区分事实、假设和待确认问题；
- 未知工具、权限不足、参数错误、任务超时、Agent 并发超限和 DAG 错误会被拒绝；
- 草稿必须校验并通过内容哈希确认；
- 确认后的定义仍由可靠工作流编译器处理；
- 默认测试无需网络和付费模型。

当前仍有以下限制：

- Mock Model 只支持固定文档处理示例，不代表通用自然语言规划能力；
- HTTP 适配器通过了本地合约测试，但尚未记录某个外部供应商的真实调用结果；
- 审计事件没有持久化；
- 人工确认仍是 CLI 和 JSON 文件，不包含多用户身份与数据库审批记录；
- 目录是静态内存数据，不支持动态注册、版本和租户隔离；
- CPU、内存和临时存储尚未进入任务定义，它们要等模块 6 的真实执行环境统一设计和验证；
- 最终控制面仍使用 Mock Executor，不会真的读取、清洗或摘要文档；
- 没有多 Agent、多 Worker、容器任务环境或 Web 控制台；
- 工具白名单不是任意代码安全沙箱。

这些限制决定了当前成果是“可靠 Agent 边界和自然语言草稿闭环”，不是完整商业 Agent 平台。后续模块会继续增加 Worker、租约、可观测性、受限执行环境和最终控制台，但不能绕过本篇建立的工具权限、结构校验和人工确认规则。

## 16. 总结

把模型接入工作流平台，关键工作并不是发出一次 API 请求，而是建立可控制、可替换、可取消、可审计的执行边界。

模块 3 将自然语言目标转换为可审核草稿，用 Model Adapter 隔离供应商，用工具注册表限制能力，用任务目录校验 Action 和 Input，用预算与 Context 限制循环，再用内容哈希和人工确认阻止草稿直接执行。最终定义继续进入原有 Compiler 和控制面，Agent 没有绕过可靠工作流内核。

这条路径保留了模型的生成能力，也保留了工程系统需要的确定性边界。真正的生产 Agent 还需要持久审批、真实工具隔离、外部模型评测和多节点执行；这些能力必须在已有证据上逐步增加，不能用“模型足够聪明”代替权限和可靠性设计。

## 参考资料

- Go `context` 官方文档：<https://pkg.go.dev/context>
- Go `encoding/json` 官方文档：<https://pkg.go.dev/encoding/json>
- Go `net/http` 官方文档：<https://pkg.go.dev/net/http>
- OpenAI Function Calling 官方说明：<https://platform.openai.com/docs/guides/function-calling>
- OpenAI Structured Outputs 官方说明：<https://platform.openai.com/docs/guides/structured-outputs>
