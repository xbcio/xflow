# 部署拓扑指导

> Status: 现状与规划混合标注，见文中「已实现 / MVP 已实现 / 规划」标签与 [§7](#7-现状-vs-规划)。

xflow 是一个通用、可嵌入的工作流引擎 SDK：用户用 DAG 编排节点（内置或自定义），SDK 本身即分布式载体。本文梳理 SDK 的三种运行模式与未来的 server + runner 集群架构，帮助使用者和开发者理解每种拓扑的定位、依赖、执行者、适用场景与限制。

> 阅读前提：引擎核心 `engine/` 由两个接口装配 —— `StateStore`（状态存储）与 `TaskQueue`（任务入队）。通用执行边界在 `execution/`：`Dispatcher` 构建 `TaskLease`，`Executor` 执行或转发，`Runner` 是进程内执行器。`backend.Provider` 抽象出可复用后端装配契约，SDK 负责组装核心能力：`NewLocal` 使用 `backend/providers/local`，`NewCluster` 使用 `backend/providers/distributed`，`NewServer` 委托 `service/apiserver` 暴露可嵌入控制面。底层包与 SDK 工厂均按部署模式命名（`backend/providers/local` ↔ `NewLocal`，`backend/providers/distributed` ↔ `NewCluster`）；包内的状态存储与队列实现名仍按技术命名（`memoryState` / `memoryQueue` 为进程内实现，`rstate` 为 Redis 实现）。详见 [ARCHITECTURE.md](./ARCHITECTURE.md)。

---

## 1. 拓扑总览

| 模式 / 角色 | 谁提交 | 谁执行节点 | 状态存哪 | 外部依赖 | 典型场景 | 现状 |
|---|---|---|---|---|---|---|
| **local** | 本进程 | 本进程（goroutine 池） | 进程内存 | 无 | 开发、测试、单进程嵌入 | 已实现 |
| **cluster** | 每个对等进程 | 每个对等进程（自带 Asynq server） | Redis（+ 可选持久化 Store） | Redis | 多副本对等部署、需要持久化与挂起/信号 | 已实现 |
| **remote** | SDK 瘦客户端 | 远端 runner 集群 | 远端（server 管理） | 网络可达的 server | 轻量嵌入、客户端不愿引入 Redis/执行负载 | 规划 |
| **server**（Control Plane） | 接受外部提交 | 不执行 handler | 通过 StateStore | Redis / 持久化 Store / Asynq | 集群控制面、调度权威 | MVP 已实现 |
| **embedded server** (`xflow.NewServer`) | 宿主进程 HTTP/gRPC 或 mounted handler | 不执行 handler | 内存或 Redis-backed server state | 无或 Redis / 持久化 Store / Asynq | 应用进程内嵌控制面，但执行仍交给 runner | MVP 已实现 |
| **runner protocol**（执行协议） | 不提交 | 不执行 handler | server 持有最终状态 | HTTP+JSON long poll / gRPC | server 与 runner 的统一执行协议 | MVP 已实现（HTTP/gRPC）；streaming / credit-flow 为实验性 |
| **runner**（执行面） | 不提交 | 通过 Runner Protocol 执行 handler | 通过 Runner Protocol 回报 server | 网络可达的 server 或 Relay Gateway | 集群执行面，横向扩缩容 / 网络隔离执行 | MVP 已实现 |
| **Relay Gateway**（可选中继） | 不提交 | 不执行 handler | 本地只缓存 pending/inflight；最终状态在 server | 网络可达的 server + 本地 runner 可达的 Relay Gateway | runner 无法直连 server 时延伸执行通道，不暴露 Redis | 规划 |

「现状 vs 规划」的完整说明见 [§7](#7-现状-vs-规划)。

---

## 2. SDK 的运行入口

### 2.1 local

进程内、内存态、零外部依赖。提交、执行、状态查询全在本地完成。

| 维度 | 说明 |
|---|---|
| 工厂 | `xflow.NewLocal(opts...)` |
| Backend | `backend/providers/local` |
| StateStore | `memoryState`（进程内存，位于 `backend/providers/local`） |
| TaskQueue | `memoryQueue`（goroutine 池，默认并发 4，位于 `backend/providers/local`） |
| Registry | `execution.Registry`（支持 direct node handler 与 type/version lookup） |
| 外部依赖 | 无 |

**执行模型**：`NewLocal` 内部 `backend/providers/local.New()` 组装内存组件，`Bind(eng)` 把 `execution.NewEmbeddedDispatcher` 挂到内存队列并启动 queue consumer pool。提交与执行在同一进程、同一组 goroutine 内闭环，但执行边界仍经过 `TaskLease -> Runner -> TaskResult`，与 cluster / Control Plane + Execution Plane 模型保持一致。`memory.Backend` 实现了 `backend.Provider` 和 `backend.Waiter`，因此 `Wait()` 是事件驱动而非轮询。

**direct ActionHandler 支持**：local 模式是唯一支持「内联直挂 handler」的模式。当使用 `wf.LocalNode(name, h)` 时，该 handler 被存入 builder 的 direct map，并在 `AddWorkflow` 时按节点名注册进 `execution.Registry`。生产/分布式自定义节点统一使用 `node.Define(...).New(params)`，consumer 进程通过 `xflow.WithNodes(...)` 声明可执行能力。

**Trigger 入口**：`AddWorkflow` 注册 workflow 定义并激活 trigger 节点；`Invoke` 从一个显式 entry 创建 execution。Timer/cron/pubsub 使用 backend trigger primitives 做 lock/dedup，webhook 使用 route fan-in + event ID dedup。

**何时用**：单元测试、本地开发调试、把工作流能力嵌入单进程应用且不需要跨进程分发。

**限制**：
- 状态仅存内存，进程退出即丢失，无持久化、无副本容灾。
- direct handler 是内存中的函数实例，无法序列化跨进程，因此只在 local 可用 —— 切到 cluster 必须改用 `node.Define` + `xflow.WithNodes`。

### 2.2 cluster

每个进程是对等节点，自带 Redis（Asynq）状态后端与任务队列；进程内既提交工作流又执行节点。

| 维度 | 说明 |
|---|---|
| 工厂 | `xflow.NewCluster(ClusterConfig{RedisAddr, Store}, opts...)` |
| Backend | `backend/providers/distributed` |
| StateStore | `redisState`（Redis；可叠加 `store.ClusterStore` 做持久化投影） |
| TaskQueue | `asynqQueue`（Asynq over Redis） |
| Registry | `execution.Registry`（按类型解析，依赖 `node.Register` 全局注册） |
| 外部依赖 | Redis（`New` 时会 `Ping` 探活，失败即报错） |

**执行模型（关键事实）**：`Bind(eng)` 在**本进程内**总是启动一个 `asynq.Server`（并发数 = `WithConcurrency`，默认 10），由 `execution.Dispatcher` 消费 Asynq task、构建 `TaskLease`，再交给 embedded `execution.Runner` 执行 action 或 suspending node，并通过带 `LeaseToken` 的 `CommitTaskResult` 推进调度；同时启动 `TimeoutMonitor`。这意味着：

> **每个 `NewCluster` 进程仍既是提交方、又是执行方，但内部执行边界已对齐 Control Plane + Execution Plane 模型。** 只要 `Bind` 被调用（`NewCluster` 内部必调），本进程就会消费队列并执行节点；未来 server / runner 拆分时，可以复用 `execution.Dispatcher`、`TaskLease`、runner result commit 语义，把 embedded runner 替换成远端 Runner Protocol executor。

**何时用**：需要多副本对等部署、需要 Redis 持久化与挂起/信号（审批流程）、且能接受「每个副本都参与执行」的部署形态。

**限制**：
- 不支持 direct ActionHandler。所有 handler 必须通过 `node.Register` 注册类型，由 `execution.Registry` 按类型解析（handler 需可在各进程独立构造）。
- 无 `Waiter` 实现，`Wait()` 退化为按 500ms 轮询 `StateStore`。
- 角色无法分离（见上），控制面与执行面耦合在同一进程。

### 2.3 remote（规划）

SDK 作为**瘦客户端**：自己不执行节点、不需要 Redis，通过网络把工作流提交给远端的 server / runner 集群，只做提交 + 查询状态 + 投递信号。这是「轻量嵌入」的目标形态。

| 维度 | 规划说明 |
|---|---|
| 工厂 | 待定（如 `xflow.NewRemote(endpoint, opts...)`） |
| StateStore | 无本地状态；状态由远端 server 持有，客户端经 API 查询 |
| TaskQueue | 无本地队列；提交即转为对 server 的网络调用 |
| Registry | 客户端无需 handler；handler 由 runner 侧持有 |
| 外部依赖 | 网络可达的 server（不依赖 Redis、不承担执行负载） |

**执行模型（规划）**：客户端 `AddWorkflow` 把 `WorkflowDef` 注册到 server，`Invoke` 指定 `xflow.start` 或 trigger entry 创建 execution；server 编译、派发；runner 执行；客户端通过 `Status` / `Wait` 查询、通过 `Signal` 投递信号。SDK 的 remote 模式即 server / runner 集群的客户端。

**何时用**：嵌入方不愿引入 Redis 依赖、不愿承担节点执行负载，只想把工作流「托管」给集群运行。

**当前状态**：本地内存后端位于 `backend/providers/local/`；当前 embedded cluster backend 位于 `backend/providers/distributed/`；**没有 remote 实现**；代码中也无 remote 工厂或客户端实现。属于规划，尚未落地。后续实现 remote 时应按客户端能力命名，不必延续 `adapter` 叫法。

### 2.4 embedded server

`xflow.NewServer(ServerConfig, opts...)` 是 SDK 支持的嵌入式控制面入口：
调用方可以把 server 的 HTTP handler 挂到自己的 `http.ServeMux`，或者让
`Server.Run` 自行监听 HTTP/gRPC/metrics 地址。它与 `cmd/server` 共享
`service/apiserver` / `service/control` 的模块表面。

| 维度 | 说明 |
|---|---|
| 工厂 | `xflow.NewServer(xflow.ServerConfig{RedisAddr, Store}, opts...)` |
| Backend | RedisAddr 为空时使用 `backend/providers/local`；非空时使用 `backend/providers/distributed` |
| 执行者 | 不执行 handler；通过 Runner Protocol 分配给 runner |
| API | `Handler()` / `Start()` / `Shutdown()` / `Run()` / `RegisterGRPC()` / `IsLeader()` |
| 典型场景 | 宿主应用希望内嵌控制面、复用自身 HTTP/gRPC 生命周期，但仍采用 server/runner 执行拆分 |

**边界**：`NewServer` 是 `sdk/xflow` 允许导入 `service/apiserver` 与
`service/control` 的唯一原因。SDK 只暴露 facade；control-plane 状态机、
runner matching、Runner Protocol、lease handoff 和 auth 逻辑仍归
`service/` 所有，不能复制到 SDK 包内。

---

## 3. server + runner 集群架构（MVP 已实现）

面向「用户通过 UI 定义工作流」的集群服务，由两个角色构成。当前已落地第一版 MVP：`cmd/server` 或 `xflow.NewServer` 提供 HTTP/gRPC 控制面，`cmd/runner` 或嵌入式 runner 通过 Runner Protocol（HTTP+JSON long polling，另有 gRPC 通道，见 `service/protocol/grpc_client.go`）领取 lease、执行 handler、回传 result。SDK 的 remote 模式仍是后续计划。

### 3.1 角色职责

| 角色 | 职责 | 不做什么 |
|---|---|---|
| **server**（Control Plane） | 接受工作流提交（HTTP/gRPC）、把 `WorkflowDef` 编译成 Graph IR、把节点任务派发到 Asynq、跟踪执行生命周期、提供查询 API、投递信号、超时清扫（TimeoutSweep） | **不执行节点 handler**，不暴露 Redis 给 runner |
| **Task Dispatcher**（server 内部组件） | 作为 Asynq worker 消费 server 内部任务，持久化为 runner assignment；由 Core 在 runner claim 后签发/恢复 lease，并管理结果回收 | 不执行 handler，不把 Redis / Asynq 细节泄漏给 runner |
| **Runner Protocol**（控制面-执行面协议） | 定义 runner 注册、心跳、容量、lease、result、cancel 的网络协议 | 不保存最终状态，不决定 DAG 调度 |
| **runner**（执行面） | 连接 server 的 Runner Protocol，通过 registry 解析 handler、执行、回报结果 | 不接受外部 API 请求，不连接 Redis / Asynq |
| **Relay Gateway**（可选） | 当 runner 无法直连 server 时中继 Runner Protocol，提供网络域延伸、本地连接聚合、短暂缓冲 | 不成为状态权威，不直接访问 server Redis / DB |

runner 可横向扩缩容：跑多个 runner 实例即可线性扩展执行吞吐。

**MVP 现有边界**：
- Runner Protocol 已提供 HTTP+JSON long polling 与 gRPC 通道；**streaming / credit-flow control 仍是实验性传输优化**，不应视为生产级可靠性承诺。
- 没有 Relay Gateway；runner 必须能直接访问 server。
- 没有 remote SDK；提交、查询、信号 API 先由 `cmd/server` 或 `xflow.NewServer` 暴露的 server HTTP handler 提供。
- **已实现** runner bearer token、mTLS、runner policy allowlist 与 dry-run rollout；workflow-level authorization、租户隔离和生产级审计仍需单独设计。
- **已实现** runner matching 的 `node_type` / `node_version` 精确匹配、runner policy 过滤与容量 gating；tags / env / region / 权重调度仍在规划。
- **已实现** Redis-backed durable assignment / claim / leased handoff、claim expiry 回收、重连 lease replay，以及 lease TTL + sweeper 回收与 re-enqueue。handler 与协议响应仍是 at-least-once，必须使用业务幂等键。
- 当前 leader election 只协调 leader-only maintenance；它不是完整 control-plane HA 或 failover SLO 的替代品。生产就绪仍依赖 Redis 高可用部署和 kill/restart/failover 验证。
- Loop/Split 仍是实验性扩展路径，未纳入静态 DAG completion 或 server/runner production-ready 保证。
- **已实现** Redis 作为权威状态、store/sqlstore 作为 best-effort audit trail 的 dual-write contract；审计 reconciliation CLI 仍在规划。

### 3.2 与 engine 两接口的对应

server 和 runner 共享同一套 `engine/` 核心语义，但不共享 Redis 连接。`StateStore` 与 `TaskQueue` 都是 server 内部能力；runner 只通过 Runner Protocol 接收任务和回报结果。

| engine 接口 / 通道 | server | runner |
|---|---|---|
| `StateStore` | 写入提交、读生命周期、记录 runner lease/result、超时清扫 | 不直接访问 |
| `TaskQueue` / Asynq | enqueue 节点调度任务；Task Dispatcher 作为 Asynq worker 消费任务 | 不直接访问 |
| Runner Protocol | 下发 lease、接收 heartbeat/result/cancel ack | 连接 server，执行 handler，回报 result |

对照当前 cluster 适配器：cluster 的 `Bind` 在一个进程里同时做了「enqueue + consume + 超时监控」。server / runner 拆分后，server 端保留 Asynq / Redis / 生命周期 / 超时，runner 端只保留 handler 执行。二者之间的适配层是 Task Dispatcher，而不是让 runner 直接成为 Asynq worker。

**核心实现边界**：`engine/` 仍保持 IO-free，不引入 Redis / Asynq / TCP / gRPC。为支持统一 Control Plane + Execution Plane 模型，engine 对外提供 runner-style 的两个阶段 API：

- `BuildTaskLease(ctx, task)`：server / dispatcher 根据队列任务构建 runner 所需的 `TaskLease`，写入新的 lease token，但不执行 handler。
- `CommitTaskResult(ctx, lease, result)`：server / dispatcher 接收 runner result 后，校验 `LeaseToken`，写入节点结果并推进 DAG。

`execution/` 承接 `TaskLease -> Executor -> TaskResult` 的通用运行时。SDK `local` / `cluster` 已切到 embedded `execution.Dispatcher` / `execution.Runner`；未来 `remote` 的差异应收敛到 protocol / backend / registry 装配方式，而不是分叉 engine 调度语义。

### 3.3 与 SDK remote 的关系

```
[ SDK remote 瘦客户端 ] --提交/查询/信号--> [ server 管理面 ]
                                                  │ enqueue
                                                  ▼
                                          [ TaskQueue (Redis) ]
                                                  │ consume by Task Dispatcher
                                                  ▼
                                          [ Runner Protocol ]
                                                  │ lease/result
                                                  ▼
                                          [ runner 执行面 × N ]
```

remote 客户端不碰 Redis、不执行节点，所有「提交 + 查询状态 + 投递信号」都打到 server。

---

## 4. Asynq 与 Task Dispatcher

Asynq 是 server 内部的任务调度和可靠队列核心。runner 不直接连接 Redis / Asynq；Task Dispatcher 是 Asynq 和 Runner Protocol 之间的适配层，MVP 已实现（`service/control.Dispatcher`）。

```
Scheduler / Engine
    │ enqueue node task
    ▼
Asynq / Redis（server 内部）
    │ consume
    ▼
Task Dispatcher（server 内部）
    │ match runner + create lease
    ▼
Runner Protocol（gRPC / HTTP long poll）
    │ assign / heartbeat / result / cancel
    ▼
xflow-runner
```

### 4.1 Task Dispatcher 职责

- 作为 Asynq worker 消费 server 内部任务，并通过无副作用的
  `engine.TaskRouting` 取得路由元数据。
- 在确认该任务仍可路由后，把确定性 `Assignment` 写入
  `RunnerDirectory`；Redis-backed server 使用 `RedisRunnerDirectory`。此
  durable enqueue 是 Asynq ack 前的交接点，而不是把已经构建的 lease
  留在某个 server 进程内存中。
- runner 以当前 session、capacity、capability、selector 和 policy 主动
  `ClaimForRunner`。目录把 assignment 从 `queued` 原子变为有 TTL 的
  `claimed`，并记录该 runner/session 的容量占用。
- server 只在 claim 成功后调用 `engine.BuildTaskLease`。成功时
  `FinalizeClaim` 将完整 lease（含 ID、token、attempt、input 和 deadline）
  持久化为 `leased`，再通过 Runner Protocol 返回给 runner。
- `engine.CommitTaskResult` 以 lease token 作为 fencing token；结果处理
  完成后，`ReleaseLeased` 再以 assignment、lease ID 和 token 身份释放
  容量并清除可重投递标记。

### 4.2 状态边界与恢复

Asynq 仍是 server 内部的可靠队列，但 runner handoff 另有独立的 durable
状态机；它不取代 Engine 的权威 node/execution 状态：

```text
Asynq task
  -> RunnerDirectory assignment: queued -> claimed -> leased -> released
  -> Engine node: pending -> running -> terminal/suspended/waiting
```

`claimed` 有 TTL。server 在 claim 后、`BuildTaskLease` 前崩溃时，过期
claim 会回到 durable assignment queue。若 `BuildTaskLease` 已经写入
running lease、但 server 在 `FinalizeClaim` 前崩溃，重试会恢复该精确的
fenced lease 并完成 finalization，而不是签发第二个 owner。若 finalization
已完成但响应尚未到 runner，runner reconnect 或同 runner ID 的重新注册会
replay 同一完整 lease；因此 runner 必须把 handler 执行视为 at-least-once。

节点 terminal commit、下游计数和后续调度 intent 由 Engine 的 atomic
`CommitNode` 与 durable outbox 处理。TaskQueue 暂时不可用时，outbox 保留
ready intent；恢复后 dispatcher 重新 handoff。lease expiry discovery 和
repair、runner claim expiry recovery，以及 outbox replay 都是独立的恢复
循环，且均以 Redis 中的权威状态为准。

### 4.3 重试与责任分层

| 阶段 | 负责方 | 重试/恢复含义 |
|---|---|---|
| engine scheduling handoff | durable outbox dispatcher | enqueue 失败保留 intent；达到阈值后转 dead-letter 并告警 |
| Asynq → RunnerDirectory | Task Dispatcher | assignment 已持久化后可安全 ack Asynq；暂时性目录错误由队列重试 |
| claim / finalize / response loss | RedisRunnerDirectory + Core | claim TTL 回队；已构建 lease 可恢复 finalization；已 final 的 lease 可 replay |
| expired execution lease | LeaseSweeper | token-fenced revoke/requeue，竞争中的 result commit 优先 |
| handler business failure | Runner + Engine | 按 workflow retry/on-error policy 产生新的 fenced attempt |

Task Dispatcher 不会等待 runner 执行完成；它只完成 durable handoff。最终
node/execution 状态仍由 server 的 `CommitTaskResult` 推进，runner 不直接
访问 Redis 或 Asynq。

### 4.4 当前 HA 与传输边界

Redis 的 durable state 允许任一 control-plane 进程在恢复后继续 claim、
replay 或 sweep 既有工作；这不等同于完整的 control-plane HA。当前 leader
election 只用于 leader-only maintenance（例如 lease/index repair 和 claim
reclaim）的互斥，尚未提供 Raft 元数据复制、跨副本 API ownership 或经过
完整 kill/restart/failover soak 验证的 production-ready 承诺。Redis 可用性
与其自身高可用部署仍是前提。

HTTP long poll 与 gRPC Runner Protocol 均可用。**streaming 与 credit-flow
control 仍为实验性传输优化**：它们不得绕过 durable assignment、lease 或
outbox 语义，也不构成 release gate 已满足的证据。Loop/Split 也仍是实验性
扩展路径，未纳入静态 DAG completion 与 server/runner production-ready
保证。

---

## 5. 网络隔离 Relay Gateway 拓扑（规划）

Relay Gateway 用于 runner 无法直连 server、不能互相直连或需要本地聚合的场景。跨云、跨 VPC、跨数据中心、用户内网、本地开发机、受限测试环境都只是这种网络隔离模型的具体实例。Relay Gateway 不是状态权威，也不直连 server 内部 Redis / DB，而是中继 Runner Protocol，把执行通道延伸到 runner 所在网络域。

典型场景之一：阿里云部署完整 `xflow-server`，腾讯云部署 `xflow-gateway`，腾讯云测试环境部署 `xflow-runner`。本质不是"跨云"，而是 runner 无法直接连接 server：runner 只访问腾讯云本地 Relay Gateway，Relay Gateway 再与阿里云 server 建立受控连接。

```
阿里云
┌──────────────────────────────────────────────┐
│ xflow-server                                 │
│ - API / 编译 / 调度 / 状态权威                │
│ - Redis / DB / TaskQueue                      │
│ - Task Dispatcher / Runner Protocol           │
└──────────────────────┬───────────────────────┘
                       │ mTLS / token / stream
腾讯云                  ▼
┌──────────────────────────────────────────────┐
│ xflow-gateway（Relay Gateway）                │
│ - 不直连 Redis / DB                           │
│ - 中继 Runner Protocol                        │
│ - 本地维护短暂 pending / inflight 缓冲          │
│ - 将 runner result 回传 server                │
└──────────────────────┬───────────────────────┘
                       │ HTTP Long Poll
腾讯云测试环境          ▼
┌──────────────────────────────────────────────┐
│ xflow-runner                                 │
│ - 注册 capability / env / tags                │
│ - 执行测试环境内的节点 handler                 │
│ - 只连接腾讯云本地 Relay Gateway              │
└──────────────────────────────────────────────┘
```

### 5.1 职责边界

| 组件 | 职责 | 不做什么 |
|---|---|---|
| `xflow-server` | 编译 DAG、调度节点、维护 Execution / Task 最终状态、决定重试 / 超时 / 下游推进、通过 Task Dispatcher 给 runner 分配任务 | 不要求网络隔离域内的 runner 直连 Redis |
| `xflow-gateway`（Relay Gateway） | 注册到 server、维持心跳、中继 runner lease/result/cancel、断线后恢复短暂 pending/inflight | 不成为最终状态源，不直接访问 server 的 Redis / DB，不执行 handler |
| `xflow-runner` | 通过 server 或 Relay Gateway 连接 Runner Protocol、执行 handler、上报结果 | 不接受外部 API，不直连 server 内部 Redis / Asynq |

### 5.2 任务流

```
1. Client / UI 提交 WorkflowDef 到阿里云 xflow-server
2. server 编译 DAG，调度到需要腾讯云测试环境执行的节点
3. Task Dispatcher 根据 placement 选择 runner 或 Relay Gateway：
   cloud=tencent, env=test, capability=xflow.http, tags=[...]
4. 腾讯云 xflow-gateway 中继 Runner Protocol，将任务写入本地短暂 pending
5. 腾讯云 xflow-runner 从 Relay Gateway 领取任务
6. runner 执行测试环境内的 handler
7. runner POST complete / fail 到 gateway
8. Relay Gateway 中继 result 给 server
9. server 更新 Task / Execution 状态并推进下游调度
```

### 5.3 关键约束

- Relay Gateway **不得直连 server 内部 Redis / DB / Asynq**。Redis 和 Asynq 是 server 的内部实现细节，暴露到隔离网络域会放大安全、延迟、连接抖动和故障恢复问题。
- Execution / Task 的最终状态以 server 为准。Relay Gateway 的 pending / inflight 只是传输层缓冲，可丢弃、可重建。
- server 给 runner 分配任务必须使用 lease / ack 语义。Gateway 断线或 lease 超时后，server 可重新分配任务。
- 任务路由需要显式 placement 元数据，例如 `cloud`、`region`、`env`、`gateway_id`、`capabilities`、`tags`、`user_scope`。
- runner 与 server 的协议由 Runner Protocol 定义；Relay Gateway 只中继该协议，或在协议允许时降级为 HTTP long poll。

### 5.4 与直接连接的关系

| 模式 | 部署位置 | 是否直连 Redis | 适用场景 |
|---|---|---|---|
| direct runner protocol | runner 直接连接 server | 否 | server 与 runner 网络可达，推荐主路径 |
| Relay Gateway | 独立进程，部署在 runner 所在网络域 / 中转网络域 | 否 | runner 无法直连 server，需要中继 |

---

## 6. 命名约定

执行面这一角色，早期文档曾叫 **worker**，当前代码入口统一使用 **runner**（见 `cmd/runner/`）。新设计使用以下专业术语，避免 `worker`、`transport`、`gateway / relay` 混用。

| 语义 | 避免使用 | 标准名称 |
|---|---|---|
| 控制面服务 | Master | **server / Control Plane** |
| 执行面进程 | worker | **runner / Execution Plane** |
| Asynq 到 runner 的适配组件 | Runner Dispatcher | **Task Dispatcher** |
| 控制面-执行面协议 | Runner Transport | **Runner Protocol** |
| 网络隔离中继 | gateway / relay | **Relay Gateway** |
| 命令目录 | `cmd/runner/` | 保持 **`cmd/runner/`** |

本文一律使用 "runner" 指代执行面。涉及 Asynq worker、goroutine worker pool 等第三方或实现级术语时，保留其原始名称以便定位；它们不代表 xflow 的执行面角色。

---

## 7. 现状 vs 规划

| 拓扑 / 组件 | 状态 | 依据 |
|---|---|---|
| local 模式 | **已实现** | `backend/providers/local/` 完整：内存 state/queue + reusable `execution.Registry` + Waiter |
| cluster 模式 | **已实现** | `backend/providers/distributed/` 完整：Redis state + Asynq queue + reusable `execution.Dispatcher` / `execution.Runner` / `execution.Registry` + TimeoutMonitor |
| remote 模式 | **规划** | 无 remote SDK client/backend，无 remote 工厂 |
| server 管理面 | **MVP 已实现** | `cmd/server` 可启动 HTTP 控制面，支持 workflow invoke、inspect、signal、cancel、runner register/heartbeat/poll/result |
| runner 执行面 | **MVP 已实现** | `cmd/runner` 可连接 server，注册 capability，通过 Runner Protocol 执行 lease 并上报 result |
| Task Dispatcher + durable handoff | **MVP 已实现** | `service/control.Dispatcher` 将 queued task 持久化为 `RedisRunnerDirectory` assignment；claim TTL、finalize、reconnect replay 与 fenced release 支持跨 server 进程恢复 |
| Runner Protocol | **MVP 已实现（传输）** | `service/protocol` 提供 HTTP+JSON DTO、路由常量和 client，另有 gRPC 通道（`grpc_client.go`、`runnerpb/`）；streaming / credit-flow control 仍为实验性 |
| Loop/Split | **实验性** | 扩展/子执行路径未进入静态 DAG completion 与 server/runner production-ready 保证 |
| Relay Gateway | **规划** | 网络隔离中继拓扑已定义，尚无独立进程实现 |

一句话：**local / cluster 已可用；server / runner 的 durable handoff MVP 已落地；remote SDK、Relay Gateway、Loop/Split 正式版与 streaming / credit-flow control 仍在规划或实验阶段。完整 control-plane HA 仍需独立验证与设计。**

---

## 8. 选型指南

| 你的场景 | 推荐 |
|---|---|
| 单元测试、本地调试 | **local** |
| 嵌入单进程应用、无外部依赖要求 | **local** |
| 需要内联直挂 handler（闭包/匿名函数） | **local**（仅此模式支持） |
| 需要持久化、挂起/信号、多副本对等部署 | **cluster** |
| 嵌入方不想引入 Redis、不想承担执行负载 | **remote**（待落地；当前只能退回 cluster 或 local） |
| 面向 UI 定义工作流、控制面与执行面分离扩缩容 | **server + Task Dispatcher + Runner Protocol + runner**（MVP 已实现） |
| runner 能访问 server，但不应直连 Redis | **server + Runner Protocol + runner**（MVP 已实现，推荐主路径） |
| runner 无法直连 server，且不应直连 Redis | **server + Relay Gateway + runner**（规划） |

决策树：

```
需要跨进程分发 / 持久化吗？
├─ 否 ──────────────────────────────► local
└─ 是
   ├─ 客户端能接受引入 Redis 且参与执行？
   │   ├─ 是 ─────────────────────► cluster
   │   └─ 否 ─────────────────────► remote（规划，暂不可用）或 server HTTP API
   └─ 需要控制面 / 执行面独立扩缩容？ ─► server + runner（MVP）
```

> 提醒：从 local 切到 cluster / remote 时，凡是用 direct ActionHandler（内联闭包）的节点都必须改写为 `node.Register` 注册的类型化 handler —— 内联函数实例无法序列化跨进程。
