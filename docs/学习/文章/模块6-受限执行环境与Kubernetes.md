# 从 0 构建 AI Workload Platform（八）：受限执行环境与 Kubernetes

本文继续记录 AI Workload Platform 的实际开发。这个项目要构建的是一个可靠运行时和调度平台：用户提交由普通程序或 Agent 组成的多步骤工作流，平台负责依赖调度、状态持久化、重试、取消、恢复、分布式执行和审计。

本文中，Workflow（工作流）是一组有依赖关系的任务；Task（任务）是其中一个可执行步骤；Run（运行实例）是一次完整工作流的执行；Attempt（执行尝试）是某个任务的一次具体执行。租约（Lease）是控制面暂时授予某个 Worker 的执行权，过期后旧 Worker 不能再提交结果。DAG（Directed Acyclic Graph，有向无环图）表示任务依赖关系中不能出现循环。

模块 5 已经解决了“任务发生了什么、如何观察和验证”的问题，但 Worker 仍然只返回 Mock 结果。模块 6 解决的是另一个边界：任务实际在哪个受限环境中运行，以及任务超时、超出资源、访问网络或执行节点失联时，如何继续沿用已有的 Attempt、租约和恢复语义。

本文只讨论已经写入仓库的实现和已经运行的验证。Docker 受限执行、kind 固定 Job、client-go 执行器和 Kubernetes OOM 映射均已在 Apple Silicon 本机真实运行；NetworkPolicy 实际阻断、CPU/临时存储限流和 Pod/Worker 故障恢复仍需单独实验，不能由清单存在直接推断。

阅读本文时只需要跟住三个对象：

- `Action`：用户可选择的、经过平台批准的动作名称，例如 `document.normalize`；
- `ExecutionRequest`：Worker 交给执行器的一次任务请求，包含 Run、Task、Attempt、Action 和结构化输入；
- `ExecutionResponse`：执行器返回给 Worker 的有限结果，例如成功、取消或带错误码的失败。

一条完整执行链路是：任务定义选择 `container` 执行器和 Action -> 注册表补齐固定镜像、入口和资源限制 -> Worker 生成 Docker `ContainerSpec` 或 Kubernetes `JobSpec` -> 运行时执行并返回退出状态和有限日志 -> 执行器转换为 `ExecutionResponse` -> Worker 只在租约有效时向控制面提交 Complete -> 工作流状态机决定下游解锁、重试或终止。模块 6 只替换“任务在哪里执行”这一层，不改变最后的状态机判断。

## 1. 从 Mock 执行进入受限执行

前五个模块形成了这样的链路：工作流被编译为 DAG，控制面把 Run 和 Attempt 保存到 PostgreSQL，Worker 通过租约领取任务，模块 5 再用日志、指标、Trace 和故障夹具记录过程。Mock Executor 的优点是确定、快速且不接触外部系统，但它无法证明以下问题：

- 进程是否真的受到 CPU、内存、临时存储和进程数限制；
- 任务是否能读取宿主机文件、连接外部网络或尝试提权；
- Worker 停止后，容器或 Pod 的生命周期如何结束；
- 运行时错误如何进入已有的重试和恢复状态机。

因此模块 6 没有重写工作流内核，而是增加两层适配：固定的 Action 注册表，以及可以替换为 Fake 的 Docker/Kubernetes 运行时客户端。Fake client（伪客户端）是实现同一调用接口、但不连接真实 Docker 或 Kubernetes 的测试对象。上层仍然只接收 `workflow.ExecutionRequest` 并返回封闭的 `workflow.ExecutionResponse`。

## 2. 术语和边界

### 2.1 容器

容器是由操作系统内核和容器运行时提供的进程隔离与资源控制环境。容器通常共享宿主机内核，不等同于虚拟机。`read-only root filesystem`、非 root 用户和 capability 限制可以减少风险，但不能保证能够安全运行任意恶意代码。

### 2.2 Docker Engine API

Docker Engine API 是 Docker 提供的程序接口。SDK（Software Development Kit，软件开发工具包）是对程序接口的代码封装；模块 6 使用 Docker Go SDK 调用 `ContainerCreate`、`ContainerStart`、`ContainerWait`、`ContainerLogs`、`ContainerStop` 和 `ContainerRemove`，而不是把输入拼接成宿主机 Shell 命令。这样可以把参数作为结构化字段传递，并让生命周期操作在测试中替换为 Fake 客户端。

### 2.3 Kubernetes、Pod 与 Job

Kubernetes 是管理容器化应用的编排系统。Pod 是 Kubernetes 调度的最小运行单元；Job 表示一个应完成的批处理任务，并记录其 Pod 是否成功或失败。Kubernetes 负责创建和回收 Pod，但不负责判断平台租约是否仍然有效，也不替代平台的 Attempt 重试规则。

### 2.4 kind

kind（Kubernetes IN Docker）使用 Docker 容器运行本地 Kubernetes 节点。它适合在 Apple Silicon Mac 上做可重复的本地实验。本项目把 kind 作为开发验证工具，不把它当成生产集群方案。

### 2.5 资源限制和安全上下文

资源限制是对 CPU、内存、临时存储、进程数和执行时间设置的上限。Kubernetes 的 `resources.requests` 用于调度，`resources.limits` 用于限制；Docker 则通过 `NanoCPUs`、`Memory`、`PidsLimit` 和临时文件系统参数表达相同约束。

安全上下文（SecurityContext）描述容器以哪个用户运行、是否允许提权、是否只读根文件系统以及要删除哪些 Linux capabilities。Linux capability 是内核把 root 权限拆分出的细粒度能力；模块 6 默认删除全部可删除 capability。

`NetworkPolicy` 是 Kubernetes 的网络访问规则。CNI（Container Network Interface，容器网络接口）是负责连接 Pod 网络的插件接口；只有 CNI 实现实际执行 NetworkPolicy 时，规则才会产生预期的网络效果。模块 6 的任务命名空间默认拒绝外部出口，但它不是对所有 CNI 实现和所有网络路径的绝对安全保证，真实效果仍需在目标集群中验证。

OOM（Out Of Memory）表示进程使用内存超过限制后被运行时终止。Docker 通过容器检查结果读取 `OOMKilled`，Kubernetes 执行器在 Job 失败时检查 Pod 的终止原因并映射为 `oom_killed`。

## 3. Action 注册表：固定能力而不是任意命令

工作流中的 `Action` 仍然只是动作名称。模块 6 的 Worker 不接受请求中的 Shell、镜像、宿主机路径、Docker Socket、Namespace 或网络地址。`internal/containerexec.ActionRegistry` 把有限动作名称映射到固定镜像、固定入口、输入字段类型、资源上限和输出上限。

Schema（数据结构约束）描述输入必须有哪些字段、每个字段是什么类型以及允许的范围。镜像 digest（摘要）是根据镜像内容计算出的不可变标识，例如 `sha256:...`；相比可变标签，它能避免同一个名称在不同时间指向不同内容。开发演示允许仓库内固定的 `workload-action:local` 标签，生产配置应使用 digest。

仓库提供的示例动作包括：

- `document.normalize`：对输入中的文本做确定性空白规范化；
- `document.summarize`：按字数上限截取文本，模拟确定性摘要；
- `resource.cpu-burn`、`resource.memory-burn`、`resource.output-burn`：只用于资源和输出限制实验。

动作镜像由 `deploy/module6-action/Dockerfile` 构建。镜像使用 `scratch` 作为基础。`scratch` 是 Docker 提供的空基础镜像，不包含 Shell、包管理器或系统命令；里面只有仓库编译出的静态 `workload-action` 二进制。这样本地构建不需要从 Docker Hub 下载基础镜像，也不会把系统命令带入任务环境。

注册表在启动容器前校验：

1. 动作名称符合固定格式且没有换行；
2. 镜像使用 digest，或仅在本地演示中使用仓库内固定的 `workload-action:local` 标签；
3. 入口非空；
4. 输入字段存在于 Schema 且类型正确；
5. CPU、内存、临时存储、进程数和超时不超过平台上限；
6. 网络策略只能是 `none`。

因此输入只能改变动作允许的业务参数，不能改变执行环境本身。

### 3.1 一个最小执行例子

`TaskDefinition` 是工作流中描述单个任务的结构。下面的任务只提交 Action 名称和业务输入；执行环境字段由平台注册表和执行器补齐：

```json
{
  "key": "normalize",
  "executor": "container",
  "action": "document.normalize",
  "input": {"source": "  AI   workload   platform  "},
  "timeout_ms": 30000
}
```

Worker 收到这个任务后，不会把 `action` 当成命令执行，而是按下面的顺序处理：

1. 在注册表中查找 `document.normalize`，得到固定镜像、入口、输入规则和资源上限；
2. 校验 `source` 是允许的字符串字段，且超时没有超过平台上限；
3. 根据注册表生成 `ContainerSpec` 或 `JobSpec`。用户不能通过任务输入改写镜像、入口、网络和挂载；
4. Docker 容器或 Kubernetes Job 执行固定动作，输出 `AI workload platform`；
5. 执行器把退出码 0 和有限日志转换成 `ExecutionResponse{Kind: success, Output: ...}`；
6. Worker 使用当前 Attempt 的租约向控制面提交结果，控制面再决定是否解锁下游任务。

如果输入不符合 Schema，执行器在创建容器前返回 `permanent_failure`；如果运行时 API 暂时不可用，返回 `temporary_failure`，是否创建下一次 Attempt 仍由工作流状态机负责。

## 4. Docker 执行器的实现

`BuildContainerSpec` 把注册动作转换为运行时无关的描述。`ContainerSpec` 是执行器需要的容器配置对象，不是用户可以自由提交的请求对象：

```go
ContainerSpec{
    Image:            spec.Image,
    Entrypoint:       spec.Entrypoint,
    Arguments:        []string{spec.Name, jsonInput},
    User:             "65532:65532",
    ReadOnlyRootFS:   true,
    NoNewPrivileges:  true,
    Privileged:       false,
    CapDrop:          []string{"ALL"},
    NetworkMode:      "none",
    Mounts:           nil,
}
```

动作名和 JSON 参数是两个独立参数，避免 Shell 解析。`RunID` 和 `TaskKey` 只用于请求身份校验，不能变成宿主机绝对路径或命令片段。`User`、网络模式、挂载和 capability 等字段由注册表和执行器固定，用户输入只能改变 Action 允许的业务字段。

`DockerExecutor` 的生命周期是：校验注册表、创建容器、启动、等待退出、读取有限日志、必要时停止、最后删除。Context 是 Go 用来传递取消信号和截止时间的对象；Attempt 超时或 Worker 进入关闭流程时，Worker 取消 Context，执行器据此停止等待和容器。容器 ID 只用于诊断，不作为 PostgreSQL 中的业务事实。成功响应必须在容器完成后返回；清理失败只进入日志，不把失败伪装成成功。

结果映射使用有限错误码：

| 运行时现象 | ExecutionResponse |
| --- | --- |
| 退出码为 0 | `success` |
| 动作不存在或输入非法 | `permanent_failure` |
| 创建、启动或等待 API 失败 | `temporary_failure` |
| Context 被取消 | `canceled` |
| OOMKilled | `temporary_failure / oom_killed` |
| 输出超过上限 | `permanent_failure / output_limit_exceeded` |
| Attempt 截止时间到达 | 由 Worker 取消 Context，按已有重试语义处理 |

是否重试仍由工作流状态机决定，Docker 不会自行创建平台 Attempt。

## 5. Kubernetes 执行器的实现

`BuildJobSpec` 生成与 client-go 解耦的 Job 描述。client-go 是 Kubernetes 官方 Go 客户端库，提供创建和查询 Kubernetes API 对象的代码。Job 固定在 `workload-tasks` 命名空间。Namespace（命名空间）是 Kubernetes 中隔离和组织资源的逻辑边界；名称由 Run、Task 和 Attempt 组成，并带有稳定标签。`backoffLimit` 固定为 0，避免 Kubernetes 自己重试而绕过平台 Attempt 计数。

Pod 的安全字段包括：

- `runAsNonRoot` 和 UID 65532；UID（User ID，用户标识）65532 是镜像中用于运行动作的非 root 用户编号；
- `readOnlyRootFilesystem`；
- `allowPrivilegeEscalation: false`；
- `privileged: false`；
- 删除全部 capability；
- `automountServiceAccountToken: false`；
- CPU、内存和临时存储 requests/limits；在 Docker 路径中，tmpfs 是以内存为后端的临时文件系统，本项目只把它挂载到受限的 `/tmp`，容器删除后内容随之消失；
- `activeDeadlineSeconds`；
- `restartPolicy: Never`。

`internal/kubeexec.Client` 是 client-go 的适配器。它创建 Job、轮询 Job 状态、读取 Pod 日志并删除 Job。Job 创建、等待、删除和日志读取通过 `KubernetesClient` 接口注入，因此单元测试不需要真实集群。Job 的 API 命名空间、等待和清理使用同一个固定 Namespace，避免创建成功后查询另一个命名空间。

两种执行器都共享同一个 Action 注册表和执行结果协议，但不会在同一个 Attempt 中同时运行。Worker 根据 `WORKLOAD_WORKER_RUNTIME` 选择 `mock`、`docker` 或 `kubernetes`；不支持的执行器不会静默降级为 Mock。

两条运行路径的差异可以归纳为：

| 项目 | Docker 执行器 | Kubernetes 执行器 |
| --- | --- | --- |
| 创建对象 | Docker container | Kubernetes Job，再由 Job 创建 Pod |
| 资源字段 | `NanoCPUs`、`Memory`、`PidsLimit`、tmpfs | requests/limits、`activeDeadlineSeconds` |
| 重试归属 | 平台 Attempt | 平台 Attempt；Job `backoffLimit=0` |
| 清理 | 停止并删除 container | 删除 Job，连带清理实验 Pod |
| 适用场景 | 本地快速执行和 Docker Engine 验证 | 编排适配和 Job/Pod 资源验证 |

两种执行器都返回同一种 `ExecutionResponse`，因此上层状态机不需要知道底层是 Docker 还是 Kubernetes。差异只发生在创建运行对象、等待生命周期和读取运行时错误的适配层。

## 6. 为什么选择 Docker Engine API、Kubernetes API 和 kind

选择 Docker Engine API，是因为本地 Docker Desktop 已经是项目开发环境的一部分，Go SDK 可以直接表达容器生命周期和资源配置，并且便于通过接口替换 Fake。没有选择宿主机 Shell，是因为 Shell 拼接会把输入边界、退出状态和清理责任混在一起，也不适合验证宿主机访问限制。

选择 Kubernetes API，是因为平台最终需要把任务交给编排系统调度，而 Job/Pod 的资源、安全上下文和生命周期与批处理任务直接对应。没有选择在项目中自研调度器、Operator 或 Helm 平台，是因为那会重复实现 Kubernetes 能力并扩大项目范围。

选择 kind，是因为它能复用 Docker Desktop、创建可删除的本地集群并适合自动化验证。minikube 和 k3d 也能运行本地 Kubernetes，但本项目已经以 Docker Desktop 作为基础环境，增加第二套本地集群工具会让安装和排障路径变长，因此不作为默认方案。Rust 也没有在本模块引入：模块 6 的核心问题是执行边界和编排适配，而不是更换 Worker 语言。

## 7. 可复现实验

### 7.1 Docker 实验

在项目根目录运行：

```bash
bash scripts/run-module6-docker.sh
```

脚本先以 `GOOS=linux GOARCH=arm64 CGO_ENABLED=0` 编译动作二进制，再构建 `workload-action:local`，最后使用无网络、只读根文件系统、非 root、删除 capability、无提权、CPU/内存/PID/tmpfs 限制运行 `document.normalize`。本机实际运行输出为：

```text
module six
image=sha256:0f9f4012834e6de824e63820a69a1cd13ac192fd2e78929e332823e6149c8432
```

此外，设置 `WORKLOAD_MODULE6_DOCKER_E2E=1` 后，`internal/e2e` 中的 Go 测试会通过真实 Docker Engine API 执行同一个动作，并验证 `DockerExecutor` 返回 `success` 和规范化文本。

### 7.2 Kubernetes 实验

先安装并验证 `kind` 和 `kubectl`，再运行：

```bash
bash scripts/run-module6-kubernetes.sh
```

脚本创建或复用名为 `workload-local` 的 kind 集群，加载本地动作镜像，应用命名空间和默认拒绝出口的 NetworkPolicy，运行带安全上下文和资源限制的 Job，读取日志后只删除本脚本创建的 Job。

本机最初从 Docker Hub 获取 `kindest/node:v1.36.1` 时返回 EOF，随后从可访问的镜像代理下载同版本节点镜像并加上 kind 默认标签。集群最终成功创建，Kubernetes 客户端和服务端版本均为 v1.36.1，控制节点状态为 `Ready`。

脚本中的固定 Job 成功完成并输出 `kubernetes action`，随后只删除该 Job；命名空间中没有残留 Job 或 Pod。设置 `WORKLOAD_MODULE6_KUBERNETES_E2E=1` 和 `KUBECONFIG` 后，仓库中的 `KubernetesExecutor` 也通过 client-go 真实创建 Job、等待完成、读取输出并清理，测试约 2.8 秒通过。这证明的范围是 Job 成功执行链路，不等同于已经证明 NetworkPolicy 实际阻断、OOM 或 Pod/Worker 故障恢复。

## 8. 测试边界和后续工作

模块 6 的单元测试覆盖注册表、输入边界、容器安全默认值、Docker 生命周期、Kubernetes Job 描述、状态映射和清理。真实 Docker 动作、Docker Go 执行器、kind 固定 Job 和 Kubernetes client-go 执行器已经在本机通过。

资源实验把固定动作的内存上限设为 32 MiB，并请求分配 64 MiB。Pod 以 `OOMKilled`、退出码 137 结束，真实 client-go 测试进一步确认执行器返回 `temporary_failure / oom_killed`。另一个 60 秒 CPU 动作在运行中被删除 Pod；由于 `backoffLimit=0`，Job 直接失败，没有由 Kubernetes 创建隐式业务重试。这证明 Kubernetes 故障不会绕过平台 Attempt 计数，但尚未证明完整控制面会回收租约并创建下一次 Attempt。

尚未完成的集群证据包括：默认 kindnet CNI 是否真正执行 NetworkPolicy、CPU 限流和临时存储超限事件，以及终止 Worker 后平台租约如何回收并拒绝迟到结果。这些场景不会影响已经验证的成功执行和 OOM 映射，但在完成前不能宣传为生产级 Kubernetes 安全隔离或故障恢复。

容器不是绝对安全沙箱。本模块没有承诺抵御内核漏洞、容器逃逸、恶意镜像、多租户攻击或生产级 Kubernetes 高可用。平台也没有开放任意用户镜像、任意命令、宿主机挂载、Docker Socket、GPU 或跨区域调度。已经真实验证的是成功执行链路和 OOM 结果映射；NetworkPolicy 实际阻断、CPU 限流、临时存储超限以及完整平台故障恢复仍需要后续实验。

下一模块将把自然语言草稿、人工确认、可靠执行和观测结果组织成轻量 Web 控制台。模块 7 会继续复用本模块已经确定的 Action 和执行器边界，不会通过界面重新开放任意命令或任意镜像。

## 9. 参考资料

- [Docker Engine API](https://docs.docker.com/engine/api/)
- [Docker Go SDK](https://pkg.go.dev/github.com/docker/docker/client)
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Kubernetes Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)
- [Kubernetes Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [kind 文档](https://kind.sigs.k8s.io/)
