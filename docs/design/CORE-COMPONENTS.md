# XFlow 核心组件设计

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
| `engine/` | DAG 调度、状态推进、错误策略、挂起/信号、`TaskLease` / `TaskResult` 语义、提交级元数据（如 Execution TTL hint） | Redis、Asynq、MySQL、网络协议 |
| `execution/` | 通用执行边界：`Dispatcher`、`Executor`、embedded `Runner`、embedded handler `Registry` | 具体队列、持久化、TCP/gRPC/WebSocket 实现 |
| `backend/` | 可复用后端抽象：`Provider`、可选能力如 `Waiter` | 具体存储、具体队列、SDK API |
| `backend/memory` | 可复用内存后端：内存 `StateStore`、内存 `TaskQueue`、embedded 生命周期、Waiter | Redis、Asynq、server 控制面、网络协议 |
| `backend/asynq` | 可复用 Redis + Asynq 后端：Redis `StateStore`、Asynq `TaskQueue`、TimeoutMonitor、embedded 生命周期 | runner 协议、server 专属控制面、远端 runner 连接实现 |
| `sdk/xflow` | 面向用户的 SDK API，组装 local / cluster 后端 | 业务调度算法、server 专属状态机 |

后续 `cmd/server` / `cmd/runner` / `remote` 应复用 `engine/` 与
`execution/` / `backend/*`，只新增服务层状态、协议和部署适配；不能反向依赖
`sdk/internal`。

> 命名约束：底层包按实现能力命名，SDK 工厂按用户部署模式命名。
> 因此 SDK 保留 `NewLocal` / `NewCluster`，但底层实现是
> `backend/memory` / `backend/asynq`。如果后续能力只属于 Control Plane，
> 则放入服务层包，而不是放入通用 backend 包。

## 架构一览

```
┌─────────────────────────────────────────────────────────────┐
│              XFlow Server Control Plane (Raft)               │
│                                                             │
│   WorkflowEngine · Scheduler · StateManager · Monitor       │  ← 共享基础设施
│   GlobalReconciler · TimeoutMonitor · Archiver              │  ← Leader-only
│   API Server · RunnerProtocol · TaskDispatcher · Reconciler │  ← Follower-only
└─────────────────────────┬───────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 │  Asynq / Redis  │
                 └────────┬────────┘
          ┌───────────────┴────────────────┐
          │                               │
┌─────────▼──────────┐      ┌─────────────▼──────────────────┐
│  Task Dispatcher   │      │  Relay Gateway                  │
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
| System Relay Runner | Runner Protocol via Relay Gateway | 系统级 | 跨 DC、跨云、受限网络域 |
| User Relay Runner | Runner Protocol via Relay Gateway | 用户私有 | 用户本机、浏览器 WASM |
