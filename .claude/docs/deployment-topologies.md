# 部署拓扑指导

xflow 是一个通用、可嵌入的工作流引擎 SDK：用户用 DAG 编排节点（内置或自定义），SDK 本身即分布式载体。本文梳理 SDK 的三种运行模式与未来的 server + runner 集群架构，帮助使用者和开发者理解每种拓扑的定位、依赖、执行者、适用场景与限制。

> 阅读前提：引擎核心 `engine/` 只依赖两个接口 —— `StateBackend`（状态存储）与 `TaskQueue`（任务入队）。adapter 把这两个接口映射到具体 IO。所有拓扑差异本质上都是「这两个接口接到哪里」的差异。详见 [architecture.md](architecture.md)。

---

## 1. 拓扑总览

| 模式 / 角色 | 谁提交 | 谁执行节点 | 状态存哪 | 外部依赖 | 典型场景 | 现状 |
|---|---|---|---|---|---|---|
| **local** | 本进程 | 本进程（goroutine 池） | 进程内存 | 无 | 开发、测试、单进程嵌入 | 已实现 |
| **cluster** | 每个对等进程 | 每个对等进程（自带 Asynq server） | Redis（+ 可选持久化 Store） | Redis | 多副本对等部署、需要持久化与挂起/信号 | 已实现 |
| **remote** | SDK 瘦客户端 | 远端 runner 集群 | 远端（server 管理） | 网络可达的 server | 轻量嵌入、客户端不愿引入 Redis/执行负载 | 规划 |
| **server**（管理面 / Master） | 接受外部提交 | 不执行 | 通过 StateBackend | Redis / 持久化 Store | 集群控制面 | 规划 |
| **runner**（执行面） | 不提交 | 消费队列执行 handler | 通过 StateBackend | Redis | 集群执行面，横向扩缩容 | 规划 |

「现状 vs 规划」的完整说明见 [§5](#5-现状-vs-规划)。

---

## 2. SDK 的三种模式

### 2.1 local

进程内、内存态、零外部依赖。提交、执行、状态查询全在本地完成。

| 维度 | 说明 |
|---|---|
| 工厂 | `xflow.NewLocal(opts...)` |
| StateBackend | `memoryState`（进程内存） |
| TaskQueue | `memoryQueue`（goroutine 池，默认并发 4） |
| Registry | `LocalRegistry` |
| 外部依赖 | 无 |

**执行模型**：`NewLocal` 内部 `local.New()` 组装内存组件，`Bind(eng)` 把 `eng.ExecuteNode` 挂到内存队列并启动 worker 池。提交与执行在同一进程、同一组 goroutine 内闭环。`Adapter` 实现了 `Waiter` 接口（`WaitDone` 基于 channel 事件通知），因此 `Wait()` 是事件驱动而非轮询。

**direct TaskHandler 支持**：local 模式是唯一支持「内联直挂 handler」的模式。当 `AddNode(name, h)` 的第二个参数 `h` 实现 `node.TaskHandler`（而非 `node.Builder`）时，该 handler 被存入 builder 的 direct map，并在 `Submit` 时按节点名注册进 `LocalRegistry`。也可用 `Engine.RegisterHandler(nodeType, h)` 注册全局类型 handler。

**何时用**：单元测试、本地开发调试、把工作流能力嵌入单进程应用且不需要跨进程分发。

**限制**：
- 状态仅存内存，进程退出即丢失，无持久化、无副本容灾。
- direct handler 是内存中的函数实例，无法序列化跨进程，因此只在 local 可用 —— 切到 cluster 必须改用 `node.Register` + `node.New`。

### 2.2 cluster

每个进程是对等节点，自带 Redis（Asynq）状态后端与任务队列；进程内既提交工作流又执行节点。

| 维度 | 说明 |
|---|---|
| 工厂 | `xflow.NewCluster(ClusterConfig{RedisAddr, Store}, opts...)` |
| StateBackend | `redisState`（Redis；可叠加 `store.ClusterStore` 做持久化投影） |
| TaskQueue | `asynqQueue`（Asynq over Redis） |
| Registry | `clusterRegistry`（按类型解析，依赖 `node.Register` 全局注册） |
| 外部依赖 | Redis（`New` 时会 `Ping` 探活，失败即报错） |

**执行模型（关键事实）**：`Bind(eng)` 在**本进程内**总是启动一个 `asynq.Server`（并发数 = `WithConcurrency`，默认 10），把 `eng.ExecuteNode` 注册为任务处理函数，同时启动 `TimeoutMonitor`。这意味着：

> **每个 `NewCluster` 进程既是提交方、又是执行方。当前 cluster 模式下无法干净地分离「只提交」和「只执行」两种角色** —— 只要 `Bind` 被调用（`NewCluster` 内部必调），本进程就会消费队列并执行节点。这正是未来 server / runner 拆分要解决的问题（见 [§3](#3-server--runner-集群架构规划)）。

**何时用**：需要多副本对等部署、需要 Redis 持久化与挂起/信号（审批流程）、且能接受「每个副本都参与执行」的部署形态。

**限制**：
- 不支持 direct TaskHandler。所有 handler 必须通过 `node.Register` 注册类型，由 `clusterRegistry` 按类型解析（handler 需可在各进程独立构造）。
- 无 `Waiter` 实现，`Wait()` 退化为按 500ms 轮询 `StateBackend`。
- 角色无法分离（见上），控制面与执行面耦合在同一进程。

### 2.3 remote（规划）

SDK 作为**瘦客户端**：自己不执行节点、不需要 Redis，通过网络把工作流提交给远端的 server / runner 集群，只做提交 + 查询状态 + 投递信号。这是「轻量嵌入」的目标形态。

| 维度 | 规划说明 |
|---|---|
| 工厂 | 待定（如 `xflow.NewRemote(endpoint, opts...)`） |
| StateBackend | 无本地状态；状态由远端 server 持有，客户端经 API 查询 |
| TaskQueue | 无本地队列；提交即转为对 server 的网络调用 |
| Registry | 客户端无需 handler；handler 由 runner 侧持有 |
| 外部依赖 | 网络可达的 server（不依赖 Redis、不承担执行负载） |

**执行模型（规划）**：客户端 `Submit` 把 `WorkflowDef` 通过网络发给 server；server 编译、派发；runner 执行；客户端通过 `Status` / `Wait` 查询、通过 `Signal` 投递信号。SDK 的 remote 模式即 server / runner 集群的客户端。

**何时用**：嵌入方不愿引入 Redis 依赖、不愿承担节点执行负载，只想把工作流「托管」给集群运行。

**当前状态**：`sdk/internal/adapter/` 下仅有 `local/` 与 `cluster/` 两个适配器，**没有 remote adapter**；代码中也无 remote 工厂或客户端实现。属于规划，尚未落地。

---

## 3. server + runner 集群架构（规划）

未来面向「用户通过 UI 定义工作流」的集群服务，由两个角色构成。SDK 的 remote 模式是这套集群的客户端，SDK 只是其中一种轻量级嵌入方案。

### 3.1 角色职责

| 角色 | 职责 | 不做什么 |
|---|---|---|
| **server**（管理面 / Master） | 接受工作流提交（HTTP/gRPC）、把 `WorkflowDef` 编译成 Graph IR、把节点任务派发到队列、跟踪执行生命周期、提供查询 API、投递信号、超时清扫（TimeoutSweep） | **不执行节点 handler** |
| **runner**（执行面） | 消费队列里的节点任务、通过 registry 解析 handler、执行、回报结果、触发下游调度 | 不接受外部 API 请求 |

runner 可横向扩缩容：跑多个 runner 实例即可线性扩展执行吞吐。

### 3.2 与 engine 两接口的对应

server 与 runner 共享同一套 `engine/` 核心与同一份 Redis 状态，差别只在「接哪个接口、是否消费队列」：

| engine 接口 | server | runner |
|---|---|---|
| `StateBackend` | 写入提交、读生命周期、超时清扫 | 读输入、回报结果、推进调度 |
| `TaskQueue` | 只 enqueue（派发任务） | 只 consume（执行任务） |

对照当前 cluster 适配器：cluster 的 `Bind` 在一个进程里同时做了「enqueue + consume + 超时监控」。server / runner 拆分，本质就是把这一坨能力按接口职责切到两类进程 —— server 端保留 enqueue 与生命周期/超时，runner 端只保留 consume + 执行。

### 3.3 与 SDK remote 的关系

```
[ SDK remote 瘦客户端 ] --提交/查询/信号--> [ server 管理面 ]
                                                  │ enqueue
                                                  ▼
                                          [ TaskQueue (Redis) ]
                                                  │ consume
                                                  ▼
                                          [ runner 执行面 × N ]
```

remote 客户端不碰 Redis、不执行节点，所有「提交 + 查询状态 + 投递信号」都打到 server。

---

## 4. 命名约定：worker → runner

执行面这一角色，代码里目前叫 **worker**（见 `cmd/worker/`），文档与未来代码统一改叫 **runner**。

| 项 | 现状 | 建议 |
|---|---|---|
| 执行面角色称呼 | worker | **runner** |
| 命令目录 | `cmd/worker/` | 建议重命名为 **`cmd/runner/`** |

本文一律使用 "runner" 指代执行面。涉及现有代码引用时（如 `cmd/worker/main.go`、cluster 适配器内的 Asynq worker goroutine）保留其原始名称以便定位，但概念上等同于 runner。

---

## 5. 现状 vs 规划

| 拓扑 / 组件 | 状态 | 依据 |
|---|---|---|
| local 模式 | **已实现** | `sdk/internal/adapter/local/` 完整：内存 state/queue/registry + Waiter |
| cluster 模式 | **已实现** | `sdk/internal/adapter/cluster/` 完整：Redis state + Asynq queue + TimeoutMonitor |
| remote 模式 | **规划** | `sdk/internal/adapter/` 下无 remote adapter，无 remote 工厂 |
| server 管理面 | **规划** | `cmd/server/main.go` 仅有职责说明注释 + `// TODO`，`main` 为空 |
| runner 执行面 | **规划** | `cmd/worker/main.go` 仅有职责说明注释 + `// TODO`，`main` 为空 |

一句话：**local / cluster 已可用；remote / server / runner 是方向，尚未落地。**

---

## 6. 选型指南

| 你的场景 | 推荐 |
|---|---|
| 单元测试、本地调试 | **local** |
| 嵌入单进程应用、无外部依赖要求 | **local** |
| 需要内联直挂 handler（闭包/匿名函数） | **local**（仅此模式支持） |
| 需要持久化、挂起/信号、多副本对等部署 | **cluster** |
| 嵌入方不想引入 Redis、不想承担执行负载 | **remote**（待落地；当前只能退回 cluster 或 local） |
| 面向 UI 定义工作流、控制面与执行面分离扩缩容 | **server + runner**（规划） |

决策树：

```
需要跨进程分发 / 持久化吗？
├─ 否 ──────────────────────────────► local
└─ 是
   ├─ 客户端能接受引入 Redis 且参与执行？
   │   ├─ 是 ─────────────────────► cluster
   │   └─ 否 ─────────────────────► remote（规划，暂不可用）
   └─ 需要控制面 / 执行面独立扩缩容？ ─► server + runner（规划）
```

> 提醒：从 local 切到 cluster / remote 时，凡是用 direct TaskHandler（内联闭包）的节点都必须改写为 `node.Register` 注册的类型化 handler —— 内联函数实例无法序列化跨进程。
