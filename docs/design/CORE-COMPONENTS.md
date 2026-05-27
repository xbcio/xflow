# XFlow 核心组件设计

> 本文档已拆分为三个子文档，请按角色查阅对应文档。

## 文档索引

| 文档 | 内容 |
|------|------|
| **[MASTER-COMPONENTS.md](./MASTER-COMPONENTS.md)** | Master 调度集群：WorkflowEngine、Scheduler、StateManager、Executor、Reconciler、HA 方案、核心数据结构与接口 |
| **[GATEWAY-COMPONENTS.md](./GATEWAY-COMPONENTS.md)** | Gateway Edge Worker 接入层：Worker 类型（System/User）、注册流程、Long Poll API、任务路由、断线恢复 |
| **[WORKER-COMPONENTS.md](./WORKER-COMPONENTS.md)** | Worker 执行层：Internal Worker（Asynq）、Edge Worker（HTTP Poll）、TaskHandler 接口、插件系统 |

## 架构一览

```
┌─────────────────────────────────────────────────────────────┐
│                    Master Cluster (Raft)                     │
│                                                             │
│   WorkflowEngine · Scheduler · StateManager · Monitor       │  ← 共享基础设施
│   CronManager · TriggerRegistry · GlobalReconciler          │  ← Leader-only
│   API Server · Gateway(:8081) · Executor · LocalReconciler  │  ← Follower-only
└─────────────────────────┬───────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 │  Asynq / Redis  │
                 └────────┬────────┘
          ┌───────────────┴────────────────┐
          │                               │
┌─────────▼──────────┐      ┌─────────────▼──────────────────┐
│  Internal Worker   │      │  Gateway                        │
│  （直连 Redis）     │      │  ┌──────────────────────────┐  │
│                    │      │  │ System Edge Worker        │  │
└────────────────────┘      │  │ User Edge Worker          │  │
                            │  │ （HTTP Long Poll）         │  │
                            │  └──────────────────────────┘  │
                            └────────────────────────────────┘
```

## Worker 类型速查

| 类型 | 接入方式 | 作用域 | 典型场景 |
|------|---------|--------|---------|
| Internal Worker | Asynq（直连 Redis） | 系统级 | 平台内网 |
| System Edge Worker | Gateway Long Poll | 系统级 | 跨 DC、补充算力 |
| User Edge Worker | Gateway Long Poll | 用户私有 | 用户本机、浏览器 WASM |
