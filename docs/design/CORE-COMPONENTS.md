# XFlow 核心组件设计

> **Status: 目标设计（非当前实现）。** 本文档及其三个子文档描述 server 集群化的目标架构（多节点控制面、Relay Gateway 等），用于指导后续演进方向。当前已实现的架构见 [ARCHITECTURE.md](./ARCHITECTURE.md)（engine/execution/backend 分层）；当前 server/runner MVP 的现状与规划边界见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md) §7。
>
> **注意：早期版本曾把 HA 写成 Raft 方案，这是错的。** 实际选主是 `backend/providers/distributed/leader.go` 的 `RedisLeaderElector`（Redis 租约，TTL 15s），代码中没有、也不计划引入 Raft。

> 本文档已拆分为三个子文档，请按角色查阅对应文档。

## 文档索引

| 文档 | 内容 |
|------|------|
| **[MASTER-COMPONENTS.md](./MASTER-COMPONENTS.md)** | Server Control Plane：WorkflowEngine、Scheduler、StateManager、Task Dispatcher、HA 方案、核心数据结构与接口 |
| **[GATEWAY-COMPONENTS.md](./GATEWAY-COMPONENTS.md)** | Relay Gateway：Runner Protocol 中继、runner 会话管理、Long Poll API、断线恢复 |
| **[WORKER-COMPONENTS.md](./WORKER-COMPONENTS.md)** | Runner Execution Plane：Direct Runner、Relay Runner、ActionHandler 接口、插件系统 |

## 当前代码边界

当前实现按「核心可复用 + SDK 轻量组装」划分：

| 包 | 职责 | 不包含 |
|------|------|------|
| `engine/` | DAG 调度、状态推进、错误策略、挂起/信号、`TaskLease` / `TaskResult` 语义、提交级元数据（如 Execution TTL hint），以及 engine-owned tracing span 入口 | Redis、Asynq、MySQL、网络协议、具体观测导出器 |
| `execution/` | 通用执行边界：`Dispatcher`、`Executor`、embedded `Runner`、embedded handler `Registry` | 具体队列、持久化、TCP/gRPC/WebSocket 实现 |
| `backend/` | 可复用后端抽象：`Provider`、可选能力如 `Waiter` | 具体存储、具体队列、SDK API |
| `backend/providers/local` | 可复用内存后端：内存 `StateStore`、内存 `TaskQueue`、embedded 生命周期、Waiter | Redis、Asynq、server 控制面、网络协议 |
| `backend/providers/distributed` | 可复用 Redis + Asynq 后端：Redis `StateStore`、Asynq `TaskQueue`、TimeoutMonitor、embedded 生命周期 | runner 协议、server 专属控制面、远端 runner 连接实现 |
| `sdk/xflow` | 面向用户的 SDK API，组装 local / cluster 后端，并通过 `NewServer` 暴露可嵌入 control-plane facade | 业务调度算法、server 专属状态机、runner protocol 内部实现 |

`cmd/server` / `cmd/runner` / `remote` 应复用 `engine/` 与
`execution/` / `backend/*`，只新增服务层状态、协议和部署适配；不能反向依赖
`sdk/internal`。`sdk/xflow.NewServer` 是例外的嵌入式 server facade：它可以
委托 `service/apiserver` / `service/control`，但不得把 server 状态机或 runner
protocol 实现复制进 SDK 包。

> 命名约束：底层包与 SDK 工厂均按部署模式命名（`local`/`distributed` ↔ `NewLocal`/`NewCluster`）；包内的状态/队列实现名仍按技术命名（内存态、Redis 态）。
> 因此 SDK 保留 `NewLocal` / `NewCluster` / `NewServer`，但底层实现是
> `backend/providers/local` / `backend/providers/distributed`。如果后续能力只属于 Control Plane，
> 则放入服务层包，而不是放入通用 backend 包。

## 架构一览

```
┌─────────────────────────────────────────────────────────────┐
│   XFlow Server Control Plane（Redis 租约选主，非 Raft）        │
│                                                             │
│   WorkflowEngine · Scheduler · StateManager · Monitor       │  ← 共享基础设施
│   EntryActivationReconciler · LeaseSweeper                  │  ← Leader-only
│   AuditReconcileWorker · timeout.Monitor                    │  ← Leader-only
│   API Server · RunnerProtocol · TaskDispatcher              │  ← 全节点
└─────────────────────────┬───────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 │  Asynq / Redis  │
                 └────────┬────────┘
          ┌───────────────┴────────────────┐
          │                               │
┌─────────▼──────────┐      ┌─────────────▼──────────────────┐
│  Task Dispatcher   │      │  Relay Gateway （planned）      │
│  （server 内部）    │      │  ┌──────────────────────────┐  │
│                    │      │  │ System Runner             │  │
└────────────────────┘      │  │ User Runner               │  │
                            │  │ （HTTP Long Poll）         │  │
                            │  └──────────────────────────┘  │
                            └────────────────────────────────┘
```

## Runner 类型速查

| 类型 | 接入方式 | 作用域 | 典型场景 |
|------|---------|--------|---------|
| Direct Runner | Runner Protocol（直连 server） | 系统级 | server 与 runner 网络可达 |
| System Relay Runner | Runner Protocol via Relay Gateway（planned） | 系统级 | 跨 DC、跨云、受限网络域 |
| User Relay Runner | Runner Protocol via Relay Gateway（planned） | 用户私有 | 用户本机、浏览器 WASM |
