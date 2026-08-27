# 从 0 构建 AI Workload Platform（八）：受限执行环境与 Kubernetes

## 这一模块解决什么问题

AI Workload Platform 要运行的是多步骤工作流。一个工作流可以同时包含普通程序任务和 Agent 任务，例如读取文档、清洗文本、调用模型生成摘要。前面的模块已经完成了工作流内核、PostgreSQL 控制面、独立 Worker、多 Worker 租约、故障恢复和可观测性，但 Worker 仍然可以使用 Mock Executor 返回固定结果。PostgreSQL 是保存工作流和运行状态的关系型数据库。

Mock Executor 适合验证状态机，却不能回答下面这些运行时问题：

- 任务是否真的在隔离环境中执行，而不是直接读写 Worker 主机；
- 任务能否访问外部网络、挂载宿主机目录或获得提权能力；
- CPU、内存、临时存储、进程数和执行时间是否有上限；
- 容器或 Pod 被终止后，错误怎样回到平台已有的 Attempt、租约和重试语义；
- 本地 Docker 执行和 Kubernetes 批处理执行是否可以共用同一个 Worker 协议。

模块 6 的目标不是重写调度器，而是把“任务在哪里运行”替换成受限执行器，并保持上层的状态事实不变。本文会解释固定 Action 注册表、Docker Engine API、Kubernetes Job、资源限制、安全上下文、错误映射和真实实验之间的关系。

本文中的验证范围必须先说清楚：真实 Docker 成功执行、kind 固定 Job、Kubernetes client-go 成功路径和 OOM 映射已经在 Apple Silicon 本机运行；NetworkPolicy 实际阻断、CPU 限流、临时存储超限以及完整 Worker 租约恢复仍没有形成完整证据，不能从清单存在推断这些能力已经被证明。

## 1. 从模块 5 到模块 6

模块 5解决的是“发生了什么以及如何证明”：控制面和 Worker 写入结构化日志、Prometheus 指标和 Trace，故障注入工具可以模拟数据库不可用、租约过期、Worker 终止等情况。它证明了调度与恢复语义，但执行内容仍是 Mock 或普通进程。

模块 6面对的是另一个边界：任务输入可能来自用户，也可能来自模型生成的 Action 参数。只要任务直接运行在 Worker 进程或宿主机 Shell 中，输入就可能改变命令、路径、网络和权限，平台也无法可靠地读取退出原因和资源事件。因此当前模块引入两层适配：

1. `ActionRegistry` 把可执行能力收敛为有限的、经过批准的动作；
2. Docker 和 Kubernetes 执行器把动作放入有资源和安全限制的运行环境。

上层仍然只看到 `workflow.ExecutionRequest` 和 `workflow.ExecutionResponse`。执行器不创建平台 Attempt，也不决定是否重试；这些仍由 Worker 和工作流状态机负责。这样可以把“运行时隔离”和“业务状态转换”分开验证。

## 2. 先理解几个对象

### 2.1 容器和容器运行时

容器是操作系统内核提供的进程隔离与资源控制环境。容器通常与宿主机共享内核，因此不等同于虚拟机。只读根文件系统、非 root 用户、删除 Linux capability 和关闭提权可以降低风险，但不能保证抵御内核漏洞或恶意镜像。

Docker Engine 是负责创建、启动、等待和删除容器的运行时服务。Docker Desktop 在 Mac 上提供一个 Linux 虚拟化环境，Docker CLI 和 Go SDK 通过 Docker Engine API 与它通信。

### 2.2 Docker Engine API 和 Go SDK

Docker Engine API 是 Docker 暴露的程序接口。SDK（Software Development Kit，软件开发工具包）是对程序接口的代码封装。本项目使用 Docker Go SDK 调用 `ContainerCreate`、`ContainerStart`、`ContainerWait`、`ContainerLogs`、`ContainerStop` 和 `ContainerRemove`。

没有选择把用户输入拼接成宿主机 Shell 命令，因为 Shell 会把参数边界、退出状态、取消和清理责任混在一起。结构化 API 可以把镜像、入口、参数、资源和安全字段作为独立字段传递，也可以在单元测试中替换为 Fake client（伪客户端）。

### 2.3 Kubernetes、Pod 和 Job

Kubernetes 是管理容器化工作负载的编排系统。Pod 是 Kubernetes 调度的最小运行单元；Job 表示一个应当完成的批处理任务，并记录其 Pod 是否成功或失败。

Kubernetes 负责创建、调度和回收 Pod，但不负责判断平台租约是否仍然有效，也不应该替代平台的 Attempt 计数。项目把 Job 的 `backoffLimit` 固定为 0，避免 Kubernetes 自己重试而绕过平台的重试和审计。

### 2.4 kind

kind（Kubernetes IN Docker）使用 Docker 容器运行本地 Kubernetes 节点。它适合在 Mac 上创建可删除、可重复的开发集群。本项目把 kind 作为本地验证工具，不把它当成生产集群方案。

### 2.5 资源限制和安全上下文

资源限制是给任务设置的上限。CPU 限制控制计算时间，内存限制控制可使用的内存，临时存储限制控制容器临时文件，PIDs 限制控制进程数，超时限制控制任务最长运行时间。

Kubernetes 的 `resources.requests` 用于调度时预留资源，`resources.limits` 用于限制实际使用量。Docker 通过 `NanoCPUs`、`Memory`、`PidsLimit` 和临时文件系统参数表达类似约束。

安全上下文（SecurityContext）描述容器以哪个用户运行、是否允许提权、是否只读根文件系统以及需要删除哪些 capability。Linux capability 是内核把 root 权限拆成的细粒度能力；模块 6默认删除全部可删除 capability，并关闭自动挂载的 ServiceAccount Token。

`NetworkPolicy` 是 Kubernetes 的网络访问规则。CNI（Container Network Interface，容器网络接口）是连接 Pod 网络的插件接口，只有实际使用的 CNI 执行 NetworkPolicy 时，规则才会阻止流量。因此“清单中有默认拒绝出口”不等于“当前集群已经证明出口被阻断”。

OOM（Out Of Memory）表示进程超过内存限制后被运行时终止。Docker 可以从容器检查结果读取 `OOMKilled`；Kubernetes 执行器会检查 Pod 终止原因，并把它映射为 `oom_killed` 错误码。

## 3. 一条任务如何进入受限环境

### 3.1 用户提交的内容

工作流定义只提交 Action 名称和业务输入，不提交 Shell、镜像、宿主机路径、Docker Socket、Namespace 或网络地址：

```json
{
  "key": "normalize",
  "executor": "container",
  "action": "document.normalize",
  "input": {"source": "  AI   workload   platform  "},
  "timeout_ms": 30000
}
```

`TaskDefinition` 是描述一个工作流任务的结构。`action` 只是注册表中的能力名称，不是命令字符串；`input` 只能包含该 Action Schema（数据结构约束）声明的业务字段。

### 3.2 执行数据流

| 阶段 | 组件 | 处理内容 | 产生的结果 |
| --- | --- | --- | --- |
| 1 | Workflow Compiler | 检查任务 key、依赖、超时和执行器 | 可执行的工作流定义 |
| 2 | Worker | 领取带租约的 Attempt | `ExecutionRequest` |
| 3 | ActionRegistry | 根据 Action 查找固定镜像、入口、Schema、资源和输出上限 | 注册动作规格 |
| 4 | Docker/Kubernetes Executor | 把规格转换为 `ContainerSpec` 或 `JobSpec` | 运行时对象 |
| 5 | 运行时 | 执行固定入口，等待退出并读取有限输出 | 退出码、日志、资源原因 |
| 6 | Executor | 把运行时结果转换为封闭的响应类型 | `ExecutionResponse` |
| 7 | Worker | 使用当前租约提交 Complete | Attempt 终态 |
| 8 | 状态机 | 根据成功、失败、取消或租约丢失解锁、重试或终止 | 下游 TaskRun 状态 |

这张表体现了模块 6的核心边界：执行器只负责运行时适配，不直接写 PostgreSQL，不创建下一次 Attempt，也不决定下游任务是否解锁。

### 3.3 Action 注册表为什么重要

`internal/containerexec.ActionRegistry` 把有限动作映射到固定镜像、入口、输入规则、资源上限和输出上限。仓库示例包括：

- `document.normalize`：对输入文本做确定性空白规范化；
- `document.summarize`：按字数上限截取文本，模拟确定性摘要；
- `resource.cpu-burn`、`resource.memory-burn`、`resource.output-burn`：只用于资源和输出限制实验。

注册表在创建容器前检查动作名称、镜像来源、入口、输入字段类型、CPU、内存、临时存储、进程数、超时和网络策略。生产环境应使用镜像 digest（根据镜像内容计算的不可变摘要），本地演示才允许固定的 `workload-action:local` 标签。

动作镜像由 `deploy/module6-action/Dockerfile` 构建，并以 `scratch` 作为基础镜像。`scratch` 不包含 Shell、包管理器或系统命令，镜像里只有静态编译的 `workload-action` 二进制。这减少了任务环境中的可用工具，但不应被误解为绝对安全边界。

## 4. Docker 执行器

### 4.1 ContainerSpec 的来源

`BuildContainerSpec` 把注册表中的动作规格转换成运行时无关的 `ContainerSpec`。下面的字段说明了哪些内容是平台固定的：

```go
ContainerSpec{
    Image:            spec.Image,
    Entrypoint:       spec.Entrypoint,
    Arguments:        []string{spec.Name, jsonInput},
    User:             "65532:65532",
    ReadOnlyRootFS:   true,
    NoNewPrivileges:   true,
    Privileged:       false,
    CapDrop:           []string{"ALL"},
    NetworkMode:       "none",
    Mounts:            nil,
}
```

动作名和 JSON 参数是两个独立参数，因此输入不会经过 Shell 解析。`RunID` 和 `TaskKey` 只用于请求身份和日志关联，不能变成宿主机绝对路径或命令片段。用户输入只能改变 Action 允许的业务字段，不能改写镜像、入口、网络、挂载和 capability。

### 4.2 生命周期和错误映射

Docker 执行器依次完成：校验注册表、创建容器、启动容器、等待退出、读取有限日志、在取消时停止容器、最后删除容器。Go 的 `context.Context` 用来传递取消信号和截止时间；Attempt 超时或 Worker 关闭时，Worker 取消 Context，执行器据此停止等待和清理。

结果映射保持有限且稳定：

| 运行时现象 | ExecutionResponse | 之后由谁决定 |
| --- | --- | --- |
| 退出码为 0 | `success` | Worker 提交完成并解锁下游 |
| Action 不存在或输入非法 | `permanent_failure` | 状态机终止或记录不可重试失败 |
| 创建、启动或等待 API 暂时失败 | `temporary_failure` | 状态机按重试策略创建下一次 Attempt |
| Context 被取消 | `canceled` | Run 取消或 Worker 关闭语义 |
| `OOMKilled` | `temporary_failure / oom_killed` | 是否重试由平台策略决定 |
| 输出超过上限 | `permanent_failure / output_limit_exceeded` | 记录失败，不能无限扩大输出 |

清理失败只进入日志和观测数据，不能把已经失败的任务伪装成成功。容器 ID 只用于诊断，不作为 PostgreSQL 中的业务事实。

## 5. Kubernetes 执行器

### 5.1 JobSpec 和 Kubernetes API

`BuildJobSpec` 生成与 client-go 解耦的 Job 描述。client-go 是 Kubernetes 官方 Go 客户端库，提供创建、查询和删除 Kubernetes API 对象的代码。Job 固定在 `workload-tasks` Namespace（命名空间）中，名称由 Run、Task 和 Attempt 组成，并带有稳定标签。

Pod 的关键安全字段包括：

- 非 root 用户和 UID 65532；UID（User ID，用户标识）65532 是镜像中的动作用户编号；
- `readOnlyRootFilesystem`；
- `allowPrivilegeEscalation: false` 和 `privileged: false`；
- 删除全部 capability；
- `automountServiceAccountToken: false`；
- CPU、内存和临时存储的 requests/limits；
- `activeDeadlineSeconds`；
- `restartPolicy: Never`。

Job 的 `backoffLimit: 0` 很重要：如果 Pod 失败，Kubernetes 不会在 Job 内部创建隐式重试。平台可以根据 `ExecutionResponse` 和当前 Attempt 策略决定是否重试，这样重试次数、租约和审计仍由同一套状态机管理。

### 5.2 Kubernetes 生命周期

`internal/kubeexec.Client` 通过 `KubernetesClient` 接口注入 API 客户端。执行器创建 Job，轮询 Job 状态，读取 Pod 日志，检查终止原因，最后删除 Job。创建、等待、日志读取和删除必须使用同一个固定 Namespace，否则可能出现创建成功、查询不到或清理错对象的问题。

Docker 和 Kubernetes 共享 Action 注册表和 `ExecutionResponse`，但一个 Attempt 只选择一个执行器。Worker 通过 `WORKLOAD_WORKER_RUNTIME` 选择 `mock`、`docker` 或 `kubernetes`；未知值会报错，不会悄悄降级为 Mock。

| 项目 | Docker 执行器 | Kubernetes 执行器 |
| --- | --- | --- |
| 运行对象 | Docker container | Kubernetes Job，再创建 Pod |
| 资源表达 | `NanoCPUs`、`Memory`、`PidsLimit`、tmpfs | requests/limits、`activeDeadlineSeconds` |
| 重试归属 | 平台 Attempt | 平台 Attempt，Job `backoffLimit=0` |
| 清理 | 停止并删除 container | 删除 Job，连带清理实验 Pod |
| 适用场景 | 本地快速执行 | 集群编排和 Job/Pod 验证 |

执行器的差异只发生在运行对象和生命周期适配层，上层状态机不需要知道底层是 Docker 还是 Kubernetes。

## 6. 方案选择和代价

选择 Docker Engine API，是因为 Docker Desktop 已经是本地开发环境的一部分，Go SDK 可以直接表达容器生命周期和资源字段，也容易通过接口替换 Fake。没有选择宿主机 Shell，是因为 Shell 拼接扩大了输入注入、路径访问和清理风险。

选择 Kubernetes Job，是因为平台需要把批处理任务交给编排系统，Job/Pod 天然提供资源、安全上下文和生命周期对象。没有在项目中自研调度器、Operator 或 Helm 平台，是因为那会重复实现 Kubernetes 能力，并把项目从可靠工作流平台扩大成集群产品。

选择 kind，是因为它复用 Docker Desktop，能够创建可删除的本地集群并适合脚本化验证。minikube 和 k3d 也能运行本地 Kubernetes，但项目已经以 Docker Desktop 为基础环境，再增加一套默认集群工具会增加安装和排障路径。

本模块的代价也必须明确：容器和 Kubernetes 增加了镜像构建、集群启动、网络插件、资源调度和清理失败等故障边界；容器共享宿主机内核，不能单独作为恶意代码的绝对安全沙箱。当前没有开放任意用户镜像、任意命令、宿主机挂载、Docker Socket、GPU 或跨区域调度。

## 7. 可复现实验

### 7.1 Docker 成功执行

在项目根目录运行：

```bash
bash scripts/run-module6-docker.sh
```

脚本编译 Linux ARM64 静态动作二进制，构建 `workload-action:local`，再以无网络、只读根文件系统、非 root、删除 capability、无提权、CPU/内存/PIDs/tmpfs 限制运行 `document.normalize`。本机曾观察到：

```text
module six
image=sha256:0f9f4012834e6de824e63820a69a1cd13ac192fd2e78929e332823e6149c8432
```

这证明的是固定动作在本机 Docker Engine 中成功启动并完成，不证明所有资源超限和网络阻断行为。

设置 `WORKLOAD_MODULE6_DOCKER_E2E=1` 后，`internal/e2e` 中的测试通过真实 Docker Engine API 创建同一个动作，并检查 `DockerExecutor` 返回 `success` 和规范化文本。默认测试不依赖 Docker，避免把本机运行时带入普通 CI。

### 7.2 kind 和 Kubernetes Job

先确认 `kind`、`kubectl` 和 Docker Desktop 可用，再运行：

```bash
bash scripts/run-module6-kubernetes.sh
```

脚本创建或复用 `workload-local` 集群，加载本地动作镜像，应用 `workload-tasks` Namespace 和默认拒绝出口的 NetworkPolicy，运行带安全上下文和资源限制的 Job，读取日志后只删除本脚本创建的 Job。

本机验证过的证据包括：kind 集群控制节点为 `Ready`，固定 Job 成功输出 `kubernetes action`，`KubernetesExecutor` 通过 client-go 真实创建 Job、等待完成、读取输出并清理；实验结束时 `workload-tasks` 中没有残留 Job 或 Pod。这证明成功执行和清理链路，不等同于证明 NetworkPolicy 实际阻断外部出口。

### 7.3 OOM 和 Pod 删除边界

内存实验把限制设为 32 MiB，同时请求动作分配 64 MiB。真实 Pod 以 `OOMKilled`、退出码 137 结束，client-go 执行器把结果映射为 `temporary_failure / oom_killed`。另一个 60 秒 CPU 动作在运行中删除 Pod 后，Job 因 `backoffLimit=0` 进入 Failed，没有由 Kubernetes 创建隐式业务重试。

这些证据说明运行时错误可以被识别并保持平台重试边界，但还没有证明完整控制面会在 Kubernetes Worker 终止后回收租约、创建下一次 Attempt 并拒绝迟到结果。NetworkPolicy 实际阻断、CPU 限流、临时存储超限也需要单独实验。

## 8. 测试如何分层

模块 6测试分为三层：

1. 注册表、Schema、安全默认值、JobSpec/ContainerSpec 和错误映射的单元测试，不需要 Docker 或 Kubernetes；
2. Fake client 测试 Docker/Kubernetes 生命周期、取消、日志读取和清理分支；
3. 显式开启环境变量后的真实 Docker 和 kind/client-go E2E，验证本机运行时的成功路径和 OOM 映射。

推荐的定向命令是：

```bash
go test ./internal/containerexec ./internal/kubeexec ./internal/workerconfig ./cmd/workload-worker ./internal/module6action -count=1
go test ./internal/e2e -run TestModule6DockerActionRunsWithApprovedImage -count=1
go test ./internal/e2e -run TestModule6KubernetesActionRunsInKind -count=1
go test ./internal/e2e -run TestModule6KubernetesMapsOOMKilled -count=1
bash scripts/verify-k8s-manifests.sh
bash -n scripts/run-module6-docker.sh scripts/run-module6-kubernetes.sh
```

真实 Docker/Kubernetes 命令需要本机 Docker Desktop、kind 集群和相应环境变量；普通单元测试不应因为本机没有这些依赖而失败。

## 9. 当前结论和未完成内容

模块 6已经完成：固定 Action 注册表、Docker `ContainerSpec`、Docker Engine API 执行器、Kubernetes `JobSpec`、client-go 执行器、容器安全默认值、真实 Docker 动作、kind 固定 Job、OOM 映射和 Pod 删除后的 Job 失败边界。

模块 6尚未证明：

- 默认 kind CNI 是否真正执行 NetworkPolicy 出口拒绝；
- CPU 限流和临时存储超限时的真实 Pod 事件；
- Kubernetes Worker 被终止后，平台租约回收、重新分配和迟到结果拒绝；
- 控制面、Worker、PostgreSQL 和 Kubernetes 同时发生故障时的完整恢复链路。

下一模块进入浏览器控制台，并不是因为 Kubernetes 已经替代了控制面，而是因为前六个模块的能力仍需要用户通过 JSON 和多个终端操作。模块 7会把自然语言草稿、人工确认、可靠运行和观测结果组织成一个可演示的产品流程，同时继续保留本模块的 Action 和执行器边界。

## 10. 参考资料

- [Docker Engine API](https://docs.docker.com/engine/api/)
- [Docker Go SDK](https://pkg.go.dev/github.com/docker/docker/client)
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Kubernetes Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)
- [Kubernetes Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [kind 文档](https://kind.sigs.k8s.io/)

## 项目源码

本文对应模块 6。完整源码、Docker/Kubernetes 清单和验证报告见 [AI Workload Platform GitHub 仓库](https://github.com/fhtyfgty5-eng/ai-workload-platform)。
