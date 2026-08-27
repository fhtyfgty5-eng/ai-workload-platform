# 从 0 构建 AI Workload Platform（六）：多 Worker、租约、心跳与故障恢复

## 摘要

我正在从零开发 AI Workload Platform：一个面向 Agent 与确定性程序任务的可靠运行时和调度平台。它接收包含多个步骤的工作流，管理任务依赖、状态、重试、取消、恢复和执行节点。

模块 3 已经能把自然语言目标转换为经过校验和人工确认的结构化工作流，但确认后的任务仍由控制面进程自己执行。**控制面（Control Plane）** 是接收请求、保存运行状态并决定任务何时可以执行的服务部分。一个进程既负责 HTTP（Hypertext Transfer Protocol，超文本传输协议）请求、数据库协调，又负责实际任务，会留下三个直接问题：执行能力不能通过增加节点扩展；任务可能被控制面故障一起中断；旧执行者恢复后可能与新执行者同时上报结果。

模块 4 把任务执行拆到独立 Worker（主动领取并执行任务的工作进程），并使用 PostgreSQL 关系数据库保存 Dispatch（任务分发记录）。Worker 通过 HTTP 主动领取任务，以租约记录有期限的执行权，以心跳报告存活并续期，再由隔离令牌识别和拒绝过期执行者。本文从这些对象的关系开始解释事务与状态变化，再结合真实多进程测试说明 Worker 崩溃、网络响应丢失、迟到结果、优雅退出和控制面重启如何处理。文章最后给出方案取舍、性能证据和当前限制。

> **复习路线：** 先阅读第 1 至 4 节，理解为什么需要 Worker，以及 Dispatch、Attempt、租约分别记录什么；再阅读第 5 至 8 节，掌握领取、心跳、完成、回收和取消的数据流；第 9 至 11 节解释一致性、方案取舍和真实验证；最后用第 12 节检查当前方案不能保证什么。

## 目录

1. 为什么单进程执行不够
2. Worker、Dispatch、Attempt 各自是什么
3. 租约为什么是临时所有权
4. 心跳同时维护会话和租约
5. 一次任务怎样从 Ready 走到 Succeeded
6. 背压与并发限制在哪里生效
7. Worker 崩溃后怎样恢复
8. 迟到结果、重复请求与取消
9. PostgreSQL 事务为什么重要
10. 为什么选择 PostgreSQL 加 HTTP 拉取
11. 真实验证与性能结果
12. 当前限制与下一步
13. 总结

## 1. 为什么单进程执行不够

模块 2 建立了 HTTP 控制面、PostgreSQL 持久化和进程重启恢复；模块 3 增加 Agent Runtime、工具权限与自然语言草稿。**Run（运行实例）** 是某个不可变 Workflow 版本的一次实际运行。模块 2 和模块 3 证明了“怎样可靠保存和控制一个 Run”，但还没有建立多执行节点之间的所有权协议。

如果继续在控制面进程中执行任务：

- 增加控制面副本会产生多个协调者，不能直接等于增加执行能力；
- 一个长任务会占用控制面进程中的 CPU、内存和 goroutine（Go 的轻量级并发执行单元）；
- 控制面升级或故障会同时影响 API 与全部执行；
- 无法描述某次真实执行尝试（Attempt）当前属于哪个执行节点；
- 任务重新执行后，旧执行者的迟到结果可能覆盖新结果。

模块 4 的目标不是让所有工作流自动变快，而是先建立一个可恢复、可验证的多进程执行边界：控制面决定哪些任务可以执行，Worker 决定自己何时有空领取，PostgreSQL 保存双方共同认可的所有权事实。

## 2. Worker、Dispatch、Attempt 各自是什么

模块 4 没有重新定义前面模块的工作流对象，而是在既有对象外增加执行分发层。先统一四个基础对象：

| 对象 | 表示什么 | 在模块 4 中是否改变 |
| --- | --- | --- |
| Workflow | 包含任务、依赖、重试和超时规则的静态定义 | 不改变，仍使用不可变版本 |
| Run | 某个 Workflow 版本的一次实际运行 | 增加分布式执行产生的状态和归属记录 |
| Task | Workflow 中一个可调度的工作单元 | 增加 `queued` 状态和执行器类型 |
| Action | 交给指定执行器解释的动作标识 | 仍是标识，不是 Shell 命令或文件路径 |
| Input | 传给 Action 的结构化 JSON 参数 | 由控制面从不可变定义中读取并交给 Worker |

模块 4 新增的 Worker、Dispatch 和租约回答“任务由谁执行、执行权何时失效”；Attempt 继续回答“这个任务实际执行了第几次”。这些对象不是同一个 ID 的不同叫法。

### 2.1 Worker

**Worker（执行节点）** 是独立运行、主动从控制面领取任务并执行的进程。每个 Worker 注册时声明：

- 显示名称；
- 协议版本；
- 支持的 ExecutorKind；
- 最大并发槽位。

**ExecutorKind（执行器类型）** 表示 Worker 能处理哪类任务。模块 4 只有 `mock`，它返回确定的模拟结果，不会把 Action 解释为命令、文件、URL 或动态程序。类型匹配由服务端完成，Worker 不能靠修改请求领取不支持的任务。

### 2.2 Dispatch

**Dispatch（分发记录）** 是“一个 Task 已经可以交给兼容 Worker”的持久化数据库事实。它有 `pending`、`leased`、`completed`、`expired`、`canceled` 等状态。

Task 从 `ready` 进入 `queued` 时创建 pending Dispatch。`queued` 只表示已经进入分发队列，此时：

- 不创建 Attempt；
- 不消耗重试次数；
- 不开始任务超时；
- 不代表任何 Worker 已经获得执行权。

如果没有兼容 Worker，Task 保持或返回 `ready`。这样不会因为系统暂时没有执行能力而提前消耗一次业务尝试。

### 2.3 Attempt

**Attempt（执行尝试）** 是 Task 的一次真实执行记录。Worker 成功领取 Dispatch 后，事务才创建 Attempt，并把 Task 从 `queued` 改为 `running`。

一个 Task 可能有多个 Attempt。例如 Worker A 领取 Attempt 1 后失联，租约回收把 Attempt 1 记为 `interrupted`；如果仍有重试次数，Worker B 随后领取 Attempt 2。Attempt 历史回答的是“实际执行过几次”，Dispatch 回答的是“每次执行权怎样分配”。

## 3. 租约为什么是临时所有权

**租约（Lease）** 是带过期时间的任务执行权。领取成功后，控制面返回：

- DispatchID；
- 只返回给当前 Worker 的租约令牌；
- RunID、TaskKey、Action 和 Input；
- Attempt 编号；
- Attempt 截止时间；
- 当前租约过期时间。

数据库只保存租约令牌的 SHA-256 摘要，不保存明文。**SHA-256** 是把任意输入转换成固定长度摘要的哈希算法，服务端可以比较摘要，但不能靠摘要直接还原明文令牌。Worker 提交心跳或结果时必须同时提供 WorkerID、DispatchID 和明文租约令牌；服务端计算摘要并使用常量时间比较。常量时间比较尽量让比较耗时不随第一个不同字节的位置变化，降低通过时间差推测令牌内容的风险。

租约有效必须同时满足：

```text
Worker 会话有效
AND Dispatch 仍为 leased
AND WorkerID 匹配
AND 租约令牌匹配
AND 数据库当前时间早于 lease_expires_at
AND 数据库当前时间早于 attempt_deadline
```

任一条件不满足，服务端都不能接受这个执行者代表当前 Attempt 更新结果。

租约不能阻止 Worker 在外部系统产生副作用。Worker A 的租约过期后，它已经发送的邮件、扣款或模型调用不会自动撤销。因此模块 4 仍是 **at-least-once（至少执行一次）**：平台保证任务不会因节点失联而静默丢失，但故障窗口内可能重复执行。真实任务必须使用幂等业务键、唯一请求 ID、结果去重或补偿机制；**幂等（Idempotency）** 表示同一个操作重复执行多次，最终业务效果仍与执行一次相同。

## 4. 心跳同时维护会话和租约

**心跳（Heartbeat）** 是 Worker 周期发送的存活请求。请求可以携带当前活动租约列表，服务端会：

1. 更新 Worker 的 `last_heartbeat_at`；
2. 检查每个 Dispatch 和租约令牌；
3. 为仍合法的租约返回 `renewed` 并延长过期时间；
4. 为已经取消或过期的租约返回 `revoked`；
5. 为未知或不匹配的租约返回 `unknown`。

空闲 Worker 也必须发送心跳。否则一个长时间没有任务的健康 Worker 会因为 `last_heartbeat_at` 不更新而被标记为 offline，真正出现任务时又无法领取。模块 4 的多进程恢复测试实际发现并修复了这个问题。

租约续期和 Worker 离线判定都使用 PostgreSQL 的当前时间，而不是 Worker 本机时间。原因是不同机器的时钟可能有偏差，Worker 也不能自行声明“我的租约仍有效”。Worker 本地只设置一个比服务端截止时间更早的安全期限：初始期限取租约过期时间与 Attempt 截止时间中较早者，再减去安全余量。合法心跳续租后可以延长本地计时器，但不能越过 Attempt 截止时间。如果网络或 PostgreSQL 长时间不可用，本地 Context（上下文，用于在 Go 调用链中传递取消和截止时间）会在安全期限到达时取消 Executor，降低无所有权执行时间。

## 5. 一次任务怎样从 Ready 走到 Succeeded

正常数据流如下：

```text
Task ready
  -> Dispatch Coordinator 检查兼容 Worker 容量和全局上限
  -> 在一个 PostgreSQL 事务中创建 pending Dispatch
  -> Task ready -> queued
  -> Worker 按空闲槽位发送 claim
  -> 事务锁定 Run 和 Dispatch
  -> 创建 Attempt，Task queued -> running
  -> 返回租约明文和执行输入
  -> Worker 执行并周期心跳
  -> Worker 提交结构化结果
  -> 事务校验当前租约并推进状态机
  -> Dispatch completed，Task succeeded
  -> 解锁下游任务或结束 Run
```

**Dispatch Coordinator（分发协调器）** 是控制面中周期扫描可运行任务、创建 Dispatch、回收过期租约并监督 Advisory Lock 的组件。**Advisory Lock（建议锁）** 是 PostgreSQL 提供、由应用自行约定含义的锁；这里用它保证同一数据库只有一个活动 Coordinator。`Wake` 只是降低新 Run 的等待时间；即使内存唤醒丢失，周期扫描仍会从 PostgreSQL 重新发现任务。

每个扫描轮次对每个 Run 最多推进一个 Task，避免一个大 Run 一次占满全部 Dispatch。只要上一轮仍创建了 Dispatch，Coordinator 会立即开始下一轮公平扫描；返回 0 才等待下次唤醒或周期扫描时刻。这样既保留跨 Run 轮换，也不会让单个大 Run 每个任务都额外等待一个完整扫描周期。

只限制“单轮一个 Task”仍不够。假设全局 Dispatch 上限为 1，每次容量释放后都从最早创建的 Run 重新排序，同一个旧大 Run 仍可能连续赢得每一轮。当前实现把公平依据保存在 PostgreSQL：从未创建过 Dispatch 的 Run 优先，其余按最近一次 Dispatch 创建时间从早到晚选择。这样进程重启后也不依赖内存游标；代价是候选查询需要读取 Dispatch 历史，因此数据库增加了 `(run_id, created_at)` 索引。它仍是基础公平性，不包含租户权重、任务优先级或严格的等待时间保证。

## 6. 背压与并发限制在哪里生效

**背压（Backpressure）** 是下游处理能力不足时，上游停止继续积累工作的机制。本模块有三层限制：

1. Workflow 的 `concurrency` 限制同一 Run 中 `queued + running` 的任务数；
2. Worker 的 `max_concurrency` 限制该会话同时持有的租约数；
3. 控制面的 Dispatch limit 限制全局 `pending + leased` 数。

Coordinator 只有在存在兼容活动 Worker 且还有全局容量时才创建 Dispatch。Worker 领取时，PostgreSQL 再检查 `max_concurrency - active_leases`，不能信任客户端自己上报的 slots。

Worker 空闲领取返回空 `leases` 是正常结果，不是服务错误。运行时使用指数退避：从较短等待开始，连续空领取时逐步增加到上限，领取成功后重置。它还加入随机抖动，让多个 Worker 的下一次请求不集中在同一时刻。随机抖动不能消除所有同时请求，但能减少固定周期造成的同步峰值。

Worker 的并发槽位释放也需要唤醒本地领取循环。否则任务已经完成，领取循环却仍在等待上一次退避计时器，顺序任务会在每个步骤之间增加无意义延迟。模块 4 在活动租约结束时发送一个合并的容量通知；控制面接受成功结果后也唤醒 Coordinator，使新解锁的下游任务尽快创建 Dispatch。通知只用于降低延迟，真正状态仍以 PostgreSQL 为准。

## 7. Worker 崩溃后怎样恢复

假设 Worker A 已经领取 Attempt 1：

```text
Worker A 持有租约并执行
  -> Worker A 进程被强制终止
  -> 心跳停止
  -> PostgreSQL 当前时间超过 lease_expires_at
  -> Reaper 锁定 Run 和 Dispatch
  -> Attempt 1 -> interrupted
  -> 仍有重试次数时 Task 回到 waiting_retry/ready
  -> 创建新的 pending Dispatch
  -> Worker B 领取 Attempt 2
  -> Worker B 提交成功
```

**Reaper（过期回收器）** 是 Coordinator 周期执行的数据库回收逻辑。租约先到期时，Attempt 记为 `interrupted`；任务自己的 Attempt deadline 先到期时，Attempt 记为 `timed_out`。两者都会消耗一次 Attempt，并复用工作流内核原有的重试或最终失败规则。

超过三个心跳周期没有更新的 Worker 会被标记为 `offline`。旧进程重新联网后不能恢复该会话或过期租约；控制面返回永久的会话错误后，Worker 进程会取消本地执行并退出，由人工或进程管理器重新启动，再注册新的 WorkerID 和会话令牌。临时网络错误和服务端 5xx 不会触发该退出，仍按原轮询策略重试。

## 8. 迟到结果、重复请求与取消

### 8.1 迟到结果

Worker A 可能在租约过期后恢复网络，并提交自己之前算出的成功结果。此时即使结果内容正确，也不能覆盖 Worker B 的新 Attempt。控制面检查 Dispatch 状态、WorkerID、租约摘要、数据库时间和当前 Attempt 归属，任一不匹配都返回 `lease_lost`，Run 的 revision 不变化。**revision（修订号）** 是 Run 每次成功提交状态后递增的版本号，用于发现基于旧状态产生的并发写入。

当前方案实现了 **fencing（隔离旧持有者）** 的效果：存储层只接受当前租约对应的随机令牌，旧持有者的迟到写入被隔离。严格意义上的 **fencing token（隔离令牌）** 通常还带有单调递增的 generation（代次编号），使下游系统能够直接比较新旧。模块 4 的随机租约令牌必须和 PostgreSQL 当前状态一起校验，不能单独传播到任意下游作为顺序编号。

### 8.2 重复完成请求

Worker 可能已经提交成功，但 HTTP 响应在返回途中丢失。它会使用同一个租约令牌和相同结果重试。服务端对受限结果字段生成规范化 SHA-256：

- 同一租约、相同结果返回首次成功；
- 同一租约、不同结果返回 `result_conflict`；
- 不会重复递增 revision 或重复解锁下游。

### 8.3 运行取消

取消请求先持久化 `cancel_requested_at`，再由 Coordinator 在事务中撤销 pending/leased Dispatch、取消未开始 Task 和运行中 Attempt。两步不能假设总在同一进程生命周期内完成：服务可能在取消意图提交后、状态收敛前崩溃。因此 Coordinator 的启动扫描和周期扫描都会重新处理“已经请求取消但仍未终止”的 Run。后续心跳返回 `revoked`，迟到完成返回 `lease_lost`。

“进程关闭”和“用户取消”不是同一件事。Worker 优雅退出时先进入 `draining`，停止领取新任务，并在关闭期限内继续心跳和完成已有租约；活动租约清空后再次调用 drain，最终进入 `stopped` 并记录停止时间。没有活动租约的 Worker 可以直接进入 `stopped`。这不会把用户 Run 改成 canceled。

## 9. PostgreSQL 事务为什么重要

**数据库事务（Transaction）** 把一组数据库读写作为一个整体提交：全部成功才生效，任一步失败就回滚。模块 4 必须让以下变化处于同一个事务：

- 签发租约与创建 Attempt；
- Task 状态与 Run revision；
- StateEvent 序号；StateEvent 是按顺序记录每次状态变化的持久化事件；
- Dispatch 状态；
- 结果哈希。

如果先返回租约再创建 Attempt，进程可能在两步之间崩溃，Worker 已经开始执行但数据库不知道；如果先把 Task 改成 succeeded 再更新 Dispatch，失败重试可能让两份事实矛盾。

并发事务还必须使用一致的加锁顺序。假设完成事务先锁 Dispatch，再准备锁 Run；同时取消事务已经锁住 Run，又准备锁同一个 Dispatch。两个事务各自等待对方释放锁，就会形成数据库死锁。

模块 4 因此统一采用“先锁 Run，再锁 Dispatch”的顺序。完成、回收、取消和孤儿清理都遵守这个顺序。领取路径需要同时选择可运行的 Run 和可领取的 Dispatch，因此使用一条 `FOR UPDATE OF r, d SKIP LOCKED` 查询同时锁定两类记录。`FOR UPDATE` 表示其他事务不能同时修改选中的行；`SKIP LOCKED` 表示遇到已经被其他事务锁住的候选时直接跳过，继续查找其他任务，而不是让所有 Worker 排队等待同一行。

Reaper 也必须在筛选过期候选时锁定 Dispatch，并在结算前重新读取最新租约期限和数据库实时时钟。否则 Heartbeat 可能已经返回 `renewed`，Reaper 却仍按更早的查询结果把同一租约回收。`SKIP LOCKED` 让 Reaper 跳过正在续租的 Dispatch，下一轮再依据最新事实判断。

首轮多 Worker 基准确实发现并修复了相反锁顺序导致的循环等待风险。修复后的并发测试和正式基准没有再次出现已知超时，但这只覆盖当前测试输入，数据库锁等待仍需要在后续指标中持续观测。

revision 和连续事件序号可以拒绝基于旧快照的写入，但也意味着同一个 Run 的状态事务最终要串行提交。这是当前正确性边界，也是性能基准中多 Worker 无法让单个 Run 线性加速的主要原因。

## 10. 为什么选择 PostgreSQL 加 HTTP 拉取

### 10.1 PostgreSQL 同时保存状态和分发

模块 2 已经使用 PostgreSQL 保存 Workflow、Run、Task、Attempt 和事件。模块 4 继续用它保存 Worker、Dispatch 和租约，可以在一个事务中校验执行所有权并提交状态，不需要先解决数据库与消息队列之间的双写一致性。

代价是领取、心跳、完成和回收都会增加数据库负载；单 Run revision 还是串行提交点。模块 5 后续基准显示，多 Run 可以通过独立 revision 提高并行度，而单 Run 仍受串行提交限制；当前证据还不足以证明必须引入第二套队列事实源。因此只有后续负载证明 PostgreSQL 分发成为不可接受的瓶颈时，才评审消息队列、Outbox 或状态分片。**Outbox（事务发件箱）** 是把待发布消息与业务状态写入同一个数据库事务，再由后台过程异步转发消息的模式，用于避免数据库与消息系统直接双写不一致。**状态分片（State Sharding）** 是按 Run 或其他稳定键把状态分散到多个独立存储分区，使不同分区可以并行处理；它会增加路由、跨分片查询和一致性复杂度。

### 10.2 HTTP 主动拉取

Worker 通过 HTTP 主动请求任务，控制面不需要知道 Worker 的可访问地址，也不需要穿透防火墙主动连接执行节点。已有 HTTP、认证、错误结构和日志边界可以继续使用。**OpenAPI** 是用机器可读文件描述 HTTP 路径、参数、认证和响应结构的接口规范；模块 4 在模块 2 的 OpenAPI 契约上继续增加 Worker 路由。

没有优先选择控制面推送，因为推送需要管理 Worker 地址、连接状态、失败重投和控制面侧容量视图；Worker 本地空闲槽位反而最直接。没有优先选择 gRPC，因为当前消息较小、不需要双向流和跨语言高吞吐证据；引入 Protocol Buffers（用于定义并生成结构化消息代码的序列化协议）、代码生成和流生命周期会增加学习与运维成本。若后续需要大量流式日志、长连接心跳或多语言 Worker，再基于测量结果评审 gRPC。

### 10.3 为什么没有引入 Kafka、RabbitMQ、NATS 或 Redis

成熟消息系统能提供高吞吐队列、消费组和投递机制，但不能自动替代 Run 状态机、Attempt 归属、租约截止时间和迟到结果检查。当前规模没有证据要求额外系统，而且引入后必须处理消息与 PostgreSQL 状态的原子发布、重复消费和运维。

Redis 可以实现短期队列和租约，但当前项目仍需要 PostgreSQL 保存不可变版本、关系查询、revision 和事件。增加 Redis 会形成缓存或第二事实来源。只有实际负载证明 PostgreSQL 分发查询不能满足目标，并且能明确缓存失效与恢复策略时，才值得增加。

### 10.4 为什么没有直接使用 Kubernetes Job

Kubernetes 能创建和重启容器，但它不知道一个工作流 Task 的重试策略、Run revision、用户取消和旧结果是否仍可接受。模块 4 先验证平台自己的执行所有权协议；模块 6 再把 Worker 或受限任务运行到 Kubernetes。**Pod** 是 Kubernetes 调度和运行一个或多个紧密关联容器的最小部署单元；Pod 故障仍必须遵守同一套租约和状态规则。

## 11. 真实验证与性能结果

自动化验证使用真实 PostgreSQL，并覆盖：

- 一个控制面、两个 Worker 执行并行 DAG（Directed Acyclic Graph，有向无环图）；
- 强制终止持有租约的 Worker，由第二个进程创建 Attempt 2；
- 新 Attempt 创建后通过 HTTP 重放旧租约，返回 `lease_lost`；
- 完整重启控制面后，原 Worker 会话继续心跳和完成；
- PostgreSQL 不可用时，本地安全期限取消执行；
- 运行中取消、Worker 优雅退出和空闲心跳；
- 没有 Worker 时保持 Ready，Worker 后注册后恢复；
- 全局背压、跨轮 Run 公平性和并发领取唯一性；
- 取消意图提交后由周期扫描收敛、Heartbeat 与 Reaper 的受控锁竞争；
- 永久 Worker 会话错误退出，以及临时网络错误继续重试；
- 默认 `mock` 执行器的历史幂等哈希兼容和 OpenAPI 状态枚举契约。

验证实际发现并修复了十三类问题：

1. 空闲 Worker 不发送心跳，导致健康会话被误判离线；
2. 优雅退出过早取消已有任务，没有留出 draining 完成窗口；
3. Worker 配置时长与控制面数据库中的时间事实不一致；
4. 不同事务的锁顺序相反，存在循环等待风险；
5. 任务完成后，控制面和 Worker 都没有及时触发下一轮分发与领取；
6. 本地安全期限没有同时受租约过期时间和 Attempt 截止时间限制；
7. Worker 会话进入 `draining` 后没有在租约清空时收敛到 `stopped`；
8. 取消意图已经提交但即时收敛失败时，周期扫描没有继续处理；
9. 新增默认 `mock` 执行器后，相同历史请求的幂等哈希发生变化；
10. Heartbeat 续租与 Reaper 的旧候选结果竞争，可能出现先返回续租、后回收租约；
11. 每轮最多推进一个 Task 不能保证跨轮公平，旧大 Run 仍可能长期占用唯一容量；
12. OpenAPI 漏写 `queued`，同时错误允许 Worker 提交控制面不接受的 `canceled` 结果；
13. Worker 忽略永久会话错误后不会重新工作，也不会退出。

这些问题都不会被只覆盖单 Worker 成功结果的测试发现。

1、4、16 Worker 处理单个 1,000 任务 Run 的五轮正式基准均值如下：

| Worker | 平均耗时 | 平均吞吐 | 平均空领取次数 |
| ---: | ---: | ---: | ---: |
| 1 | 54.94 秒 | 18.34 tasks/s | 1015.0 |
| 4 | 36.46 秒 | 27.59 tasks/s | 384.0 |
| 16 | 37.61 秒 | 30.02 tasks/s | 1558.6 |

`tasks/s` 表示每秒完成的任务数。基准为缩短 15 个样本的总耗时，使用 25 毫秒至 500 毫秒的压力轮询配置和 64 个数据库连接上限；生产 Worker 默认轮询范围仍是 250 毫秒至 5 秒。4 Worker 比 1 Worker 的平均吞吐提高约 50%，16 Worker 比 4 Worker 只提高约 9%，而且 16 Worker 有一轮降到 14.33 tasks/s。这说明增加 Worker 不会让单 Run 线性加速：每次任务变化都要串行更新同一个 Run revision，Worker 越多还会增加领取竞争和空轮询。当前数据用于暴露正确性设计的性能代价，不是生产容量承诺。

多进程故障测试使用 2 秒租约、50 毫秒心跳和 5 毫秒扫描周期。这个测试值必须明显大于 Worker 的 1 秒本地安全余量，否则 Worker 会在领取后立即取消执行，测到的就不再是真实的崩溃接管。修正参数后连续五轮均通过，其中一次带详细日志的运行从强杀 Worker 到 Run 成功约为 2.04 秒。默认配置是 15 秒租约和 1 秒扫描，因此默认人工演示会明显更慢。恢复速度、失联误判概率和心跳数据库压力相互制约，不能只为了得到更小数字而任意缩短租约。

模块 4 当时的完整环境、命令、原始结果和限制见模块 4 验证报告。模块 5 后续已经补充连接池指标、队列聚合、多 Run 吞吐、进程资源与四种观测配置对照；这些新证据见模块 5 验证报告，不倒写成模块 4 当时已经掌握的数据。

## 12. 当前限制与下一步

模块 4 已经证明：

- 执行进程可以独立扩展和注册；
- 任务领取、续租、完成与回收有持久化事实；
- Worker 崩溃后任务可以重新分配；
- 旧租约、重复结果和取消后的完成不会覆盖当前状态；
- 控制面重启不会丢失 Worker 会话和未过期租约；
- Worker 不能通过 Action 执行任意代码。

模块 4 完成时仍然没有解决：

- 控制面只有一个协调者，没有高可用切换；高可用表示部分实例故障后，其他实例能够自动接管并继续提供服务；
- 单 Run revision 限制并发提交扩展性；
- 指标、Trace、告警和故障注入平台当时尚未建立；
- Worker 只支持 Mock Executor；
- 没有真实资源限制、容器隔离和 Kubernetes 部署；
- Bearer Token 没有企业身份、证书和租户边界；
- 至少执行一次仍要求真实业务处理重复副作用。

因此模块 4 完成后的下一步进入模块 5，不是继续堆叠 Worker 数量，而是建立可观测性和持续故障实验：测量队列深度、领取空转、数据库事务延迟、租约回收时间、单 Run 与多 Run 吞吐，定位 revision 串行点，并让日志、指标和 Trace 能从 Run 关联到 Worker 和 Attempt。**Trace（分布式调用链）** 记录一次请求或任务跨组件经过的步骤和耗时，用于定位延迟或失败发生在哪一段。模块 5 已经完成这些观测与实验，并确认单 Run revision 是明显串行点；这为模块 6 的受限执行环境保留了可比较基线。

## 13. 总结

多 Worker 的核心不是启动多个进程，而是定义哪一个执行者在什么时间内有权代表某个 Attempt 写入结果。

模块 4 使用持久化 Dispatch 表达待分发事实，领取时才创建 Attempt，用租约和数据库时间限制临时所有权，用心跳续租和发现失联，用隔离令牌拒绝迟到写入，再通过 PostgreSQL 事务同时提交 Dispatch、Task、Run revision 和事件。Worker 主动 HTTP 拉取让容量决策靠近执行节点，指数退避和背压控制空轮询与待处理数量。

这套设计保证任务不会因 Worker 崩溃静默丢失，也不会让旧租约覆盖新结果，但不承诺只执行一次。模块 5 已经补充可观测性并量化单 Run 串行瓶颈；真实副作用的幂等和受限执行环境仍需要后续模块继续解决。

## 参考资料

- Go `context` 官方文档：<https://pkg.go.dev/context>
- Go `net/http` 官方文档：<https://pkg.go.dev/net/http>
- PostgreSQL 显式锁官方文档：<https://www.postgresql.org/docs/current/explicit-locking.html>
- PostgreSQL `SELECT` 与 `SKIP LOCKED` 官方文档：<https://www.postgresql.org/docs/current/sql-select.html>
- PostgreSQL 日期与时间函数官方文档：<https://www.postgresql.org/docs/current/functions-datetime.html>
- RFC 6750 Bearer Token 规范：<https://www.rfc-editor.org/rfc/rfc6750>

## 项目源码

本文对应模块 4。完整源码、多 Worker 故障实验和后续模块见 [AI Workload Platform GitHub 仓库](https://github.com/fhtyfgty5-eng/ai-workload-platform)。
