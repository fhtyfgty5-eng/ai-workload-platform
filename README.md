# AI Workload Platform

面向 Agent 与确定性程序任务的可靠运行时和调度平台。

> 当前可运行范围：模块 2 已实现单实例 HTTP 控制面、PostgreSQL 持久化、版本化 Workflow、幂等 Run、Bearer Token、键集分页与 Run 过滤、OpenAPI 3.1、受监督的启动恢复、Go SDK 和控制面 CLI。多 Worker、真实 Agent Runtime 和任务容器执行仍属于后续模块。

## 要解决的问题

脚本、定时器和 Agent 原型可以快速完成一次任务，但当工作包含多个依赖步骤、运行时间较长并且可能失败时，通常还需要处理：

- 前后步骤的依赖和并发；
- 进程重启后的状态恢复；
- 临时失败、超时、取消和重复执行；
- 多个执行节点之间的任务分配；
- Agent 的模型、工具、权限、预算和人工审批；
- 日志、指标、调用链和执行审计。

本项目不替代任务自身的业务逻辑，而是统一管理普通程序和 Agent 的可靠执行过程。

## 核心方向

- **可靠工作流内核：** 使用 DAG（Directed Acyclic Graph，有向无环图）表达依赖，通过状态机管理任务、执行尝试、重试、取消和恢复。
- **普通程序与 Agent：** 二者共用可靠性语义，但保持不同的执行接口、权限和观测数据。
- **可验证的分布式执行：** 使用租约、心跳和故障实验解释节点失联与任务重新分配。
- **受限运行环境：** 逐步加入资源和网络限制，同时明确容器不是绝对安全沙箱。
- **证据优先：** 性能和可靠性结论必须来自自动化测试、可复现实验和原始数据。

## 架构概览

```text
CLI / SDK / 控制台
        |
        v
控制面 API
        |
        v
可靠工作流内核 <-> 持久化存储
        |
        v
Worker
  |-> 普通程序执行器
  `-> Agent Runtime -> 模型 / 工具 / 人工审批
```

Agent Runtime 是 Agent 的运行时环境，负责准备模型、工具、上下文、权限和预算，并记录模型调用、工具调用和人工审批等执行事件。

完整职责和数据流见[架构基线](docs/架构/架构基线.md)。

## 快速开始

环境要求：Go 1.26 或兼容版本；FileStore 的原子替换实现当前只支持 macOS 和 Linux。

运行全部自动化测试（真实 PostgreSQL 测试需要设置 `TEST_DATABASE_URL`）：

```bash
go test ./...
```

启动模块 2 本地控制面：

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://workload:workload_dev_only@localhost:5432/workload?sslmode=disable'
export WORKLOAD_HTTP_ADDR='127.0.0.1:8080'
export WORKLOAD_VIEWER_TOKEN='replace-with-viewer-token'
export WORKLOAD_OPERATOR_TOKEN='replace-with-a-different-operator-token'
go run ./cmd/workload-server migrate up
go run ./cmd/workload-server
```

Docker 登录不是本地公开 PostgreSQL 镜像的前置条件。新电脑环境准备、完整运行步骤和当前推荐演示见[项目本地运行、演示与换电脑手册](docs/部署/本地开发与配置.md)。该手册会随后续模块持续更新，README 只保留当前版本的最短启动入口。

执行三步骤文档处理本地示例：

```bash
go run ./cmd/workload local run examples/document-pipeline.json
```

命令输出包含每次随机生成的 RunID 和最终状态，例如：

```text
run_id=bc7076993d126572b9832e45247a557e status=succeeded
```

使用实际输出的 RunID 查询持久化状态：

```bash
go run ./cmd/workload local status bc7076993d126572b9832e45247a557e
```

默认数据目录是 `.workload/runs`，可以通过 `WORKLOAD_DATA_DIR` 覆盖：

```bash
WORKLOAD_DATA_DIR=/tmp/workload-runs go run ./cmd/workload local run examples/document-pipeline.json
```

`.workload/` 已加入 Git 忽略规则。示例使用 Mock Executor 返回确定成功结果，不会执行 Action 中的本机命令，也不会调用模型或外部服务。

控制面启动后，可在另一个终端导出 `WORKLOAD_SERVER_URL` 和 operator Token，再通过 HTTP 控制面创建并启动同一工作流：

```bash
export WORKLOAD_SERVER_URL='http://127.0.0.1:8080'
export WORKLOAD_TOKEN="$WORKLOAD_OPERATOR_TOKEN"
go run ./cmd/workload workflow create examples/document-pipeline.json --idempotency-key demo-workflow-v1
go run ./cmd/workload run start document-pipeline --version 1 --idempotency-key demo-run-1
go run ./cmd/workload run status <上一步返回的run_id>
```

控制面默认使用确定成功的安全 Mock Executor，只返回模拟结果，不解释或执行 `Action`。

## 当前实现边界

- `workload local` 的本地 FileStore 模式仍在当前终端前台运行；控制面 CLI 通过 `WORKLOAD_SERVER_URL` 和 `WORKLOAD_TOKEN` 调用 HTTP API，写操作必须显式提供 `--idempotency-key`。
- FileStore 使用“每个 Run 一个完整 JSON 快照”，适合模块 1 的单机验证，不支持多进程并发写入或复杂查询。
- 原子替换依赖同目录临时文件和 `os.Rename`，当前明确支持 macOS 和 Linux，不对其他平台作相同保证。
- 恢复语义是“至少执行一次”：崩溃发生在外部动作成功但成功状态保存之前时，任务可能再次执行。未来真实 Executor 必须使用幂等操作、唯一请求 ID 或结果去重控制重复副作用。
- 数据库连接或 Advisory Lock 运行期失效时，原控制面会变为不可就绪、中断本机执行并以错误退出，不在同一进程自动重新加锁。新进程启动后从 PostgreSQL 已提交状态恢复；这种基础设施中断不会被写成用户取消。
- Workflow、Version、Run、Task 和 Event 列表使用不透明键集游标；Run 可按 `workflow_id`、`status` 组合过滤，游标只能在原资源和原过滤条件下继续使用。
- 单个 WorkflowDefinition 最多包含 10,000 个任务；1,000 个 Run 是 Benchmark 输入规模，不是第 1,001 个 Run 的容量拒绝上限。
- 当前没有真实命令执行、真实 Agent、完整日志指标平台、多 Worker、分布式调度或安全沙箱；Mock Executor 不执行本机命令。

首轮环境、输入、五轮原始数据和限制见[模块 1 性能基线](docs/实验/模块1性能基线.md)。这些数据不是生产性能承诺。

模块 2 的单任务 HTTP/PostgreSQL 查询与 Run 创建五轮数据见[模块 2 控制面性能基线](docs/实验/模块2性能基线.md)。该基线使用本机回环请求，不代表生产尾延迟或饱和吞吐。

## 项目路线

项目共分为模块 0 至模块 7：

0. 项目定义与文档基线，已完成；
1. 单机可靠工作流内核，已完成；
2. 控制面 API 与持久化存储，当前模块；
3. Agent Runtime、模型与工具执行；
4. 多 Worker、租约、心跳与故障恢复；
5. 可观测性、故障注入与性能验证；
6. 受限执行环境与 Kubernetes；
7. 真实场景、最小控制台与开源发布。

详细交付物和验收证据见[项目路线图](docs/计划/项目路线图.md)。自然语言生成工作流是模块 3 的必做演示能力，但不阻塞模块 1 的可靠内核和模块 2 的控制面基础。

## 当前可阅读内容

- [产品说明](docs/产品/产品说明.md)：目标用户、适用边界、现有产品和待验证差异。
- [架构基线](docs/架构/架构基线.md)：组件职责、数据流和模块演进。
- [项目范围决策](docs/决策/ADR-0001-项目范围.md)：为什么选择可靠工作流方向。
- [路线调整决策](docs/决策/ADR-0002-产品定位与模块路线调整.md)：为什么收窄定位并重排模块。
- [学习资料规范](docs/学习/学习资料规范.md)：技术文章的知识深度、证据和发布要求。
- [模块 0 项目总览](docs/学习/文章/模块0-项目总览.md)：项目为什么存在、准备构建什么。
- [工作流基础文章](docs/学习/文章/模块0-工作流-DAG-状态机与可靠执行基础.md)：工作流、DAG、状态机与可靠执行基础。
- [模块 1 学习文章](docs/学习/文章/模块1-单机可靠工作流内核设计与验证基础.md)：单机内核的数据结构、调度语义和验证方法。
- [模块 2 学习文章](docs/学习/文章/模块2-从单机工作流内核到可靠控制面.md)：HTTP 控制面、PostgreSQL 事务、幂等、运行监督和崩溃恢复。
- [模块 2 验证报告](docs/实验/模块2验证报告.md)：自动化结果、人工演示状态和当前证据边界。
- [模块 1 性能基线](docs/实验/模块1性能基线.md)：10,000 任务、1,000 Run、恢复和 FileStore 的原始 Benchmark 数据。

当前代码入口和限制见上方“快速开始”和“当前实现边界”。

## 开源边界

仓库计划在条件成熟后公开，但目前尚未完成许可证、贡献指南、安全策略和发布检查，因此当前内容不能视为正式开源版本。

本仓库不得包含当前或过往公司的源代码、配置、业务数据、内部文档、密钥或其他保密信息。路线图中的功能、性能和产品优势在获得可复现证据前都属于计划或假设。

维护者续接项目时使用[项目状态](项目状态.md)和[新窗口启动协议](docs/新窗口启动协议.md)，这两份文件用于协作交接，不代表面向最终用户的产品文档。
