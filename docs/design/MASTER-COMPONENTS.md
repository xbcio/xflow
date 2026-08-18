# XFlow Server Control Plane 组件设计

> **Status: 目标设计（非当前实现）。** 本文描述的 Leader-Follower HA 集群架构尚未完整落地。当前 server 是单实例 Control Plane MVP，见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md) §3、§7。

> Server Control Plane 负责工作流的调度与编排，采用 Redis 租约选主的 Leader-Follower 高可用架构。Asynq / Redis 是 server 内部任务调度队列；Task Dispatcher 将 Asynq task 转换成 Runner Protocol lease，runner 不直连 Redis / Asynq。Relay Gateway 仅在 runner 无法直连 server 时作为可选中继。当前代码中的通用调度语义位于 `engine/`，通用执行边界位于 `execution/`；server 层只应新增协议、lease state、runner matching 等服务能力。

## 目录

1. [架构概览](#1-架构概览)
2. [Workflow Engine](#2-workflow-engine)
3. [Scheduler](#3-scheduler)
4. [State Manager](#4-state-manager)
5. [API Server](#5-api-server)
6. [执行循环（Execution Loop）](#6-执行循环execution-loop)
7. [Monitor](#7-monitor)
8. [高可用（HA）](#8-高可用ha)
9. [核心数据结构](#9-核心数据结构)
10. [核心接口](#10-核心接口)
11. [类型定义](#11-类型定义)
12. [配置规范](#12-配置规范)

---

## 1. 架构概览

### 1.1 整体架构

Server Control Plane 集群采用 **Redis 租约选主**（`RedisLeaderElector`，SETNX + TTL 15s）模型，组件分三层：共享基础设施（所有节点）、Leader-only（全局唯一）、Follower-only（实际调度）。单 server 部署时自选举为 Leader，三层全部启动。

```
                ┌──────────────────────────────────────────────┐
                │         Server Control Plane (Redis 租约选主)  │
                │                                              │
                │  ┌──────────────────────────────────────┐    │
                │  │  共享基础设施（所有节点持有）           │    │
                │  │  WorkflowEngine · Scheduler (Asynq)  │    │
                │  │  TaskDispatcher · RunnerProtocol     │    │
                │  │  StateManager · Monitor              │    │
                │  └──────────────────────────────────────┘    │
                │                                              │
                │  ┌──────────────────────────────────────┐    │
                │  │  Leader-only（Redis 租约保护）         │    │
                │  │  EntryActivationReconciler           │    │
                │  │  LeaseSweeper · timeout.Monitor      │    │
                │  │  AuditReconcileWorker                │    │
                │  └──────────────────────────────────────┘    │
                │                                              │
                │  ┌──────────────┐    ┌──────────────┐        │
                │  │ Follower-1   │    │ Follower-N   │        │
                │  │ API Server   │    │ API Server   │        │
                │  │ RelayGateway │    │ RelayGateway │        │
                │  │ Executor     │    │ Executor     │        │
                │  └──────────────┘    └──────────────┘        │
                └──────────────────────┬───────────────────────┘
                                       │
                             ┌─────────┴─────────┐
                             │   Asynq Queue     │
                             │   (Redis)         │
                             └─────────┬─────────┘
                                       │
                ┌──────────────────────┼──────────────────────────┐
                │                Runner Pool                       │
                │  ┌──────────────┐   ┌──────────────────────┐   │
                │  │Direct Runner │   │Relay Runner          │   │
                │  │HTTP / gRPC   │   │(via Relay Gateway)   │   │
                │  └──────────────┘   └──────────────────────┘   │
                └─────────────────────────────────────────────────┘
```

### 1.2 技术栈

| 组件 | 技术选型 |
|------|---------|
| Leader 选举 | `RedisLeaderElector`（Redis SETNX 租约，TTL 15s）—— `backend/providers/distributed/leader.go` |
| 服务发现 | 静态配置（目前）|
| 任务调度 | Asynq（Redis-based）|
| 定时任务 | `robfig/cron/v3`，Leader-only |
| 表达式引擎 | Expr（expr-lang/expr）|
| 状态存储 | Redis（热）+ MySQL（冷）|
| API | HTTP（`net/http.ServeMux`）+ gRPC |
| 监控 | Prometheus + OpenTelemetry |

### 1.3 组件分层

| 层 | 组件 | 说明 |
|----|------|------|
| **共享基础设施** | WorkflowEngine | DSL 解析、编译、Build、`Start()` 创建 Execution + Enqueue 起始节点 |
| | Scheduler（asynq.Client）| 幂等任务入队 |
| | Task Dispatcher | 消费 Asynq task，创建 runner lease，匹配 runner，处理 result / timeout / cancel |
| | Runner Protocol | 控制面-执行面协议，维护 runner 注册、心跳、容量、lease、result（transport: HTTP 主要，gRPC 实验性）|
| | StateManager | Redis + DB 状态读写、分布式锁 |
| | Monitor | 指标采集、健康检查 |
| **Leader-only** | EntryActivationReconciler | 扫描 trigger 入口激活期望态，分配 runner，管理 activation lease |
| | LeaseSweeper | 扫描过期任务 lease，令牌围栏重入队 |
| | timeout.Monitor | 扫描 per-namespace 超时 ZSET，向引擎投递超时信号 |
| | AuditReconcileWorker | 从 SQL audit 日志对账未结算的 admission 记录 |
| **Follower-only** | API Server（HTTP + gRPC）| 对外服务，工作流/执行 CRUD，触发调用 `engine.Start()` |
| | RelayGateway | 可选 Runner Protocol 中继（详见 GATEWAY-COMPONENTS.md）|
| | Executor | 接收 runner result，推进 DAG |

---

## 2. Workflow Engine

**职责**：DSL 解析、工作流编译与 Build、工作流启动、上下文管理、已加载工作流缓存。

WorkflowEngine 是**共享基础设施**，当前通过 API/SDK 提交启动工作流。

关键方法：

```go
// Start 启动工作流（共享能力，Leader 和 Follower 均可调用）
// 1. 从缓存获取已 Build 的 Workflow（未命中则从 DB 加载并 Build）
// 2. 创建 Execution 写入 Redis
// 3. Enqueue 起始节点到 Asynq 队列
func (e *Engine) Start(ctx context.Context, workflowID string, input map[string]any) (*Execution, error)

// GetWorkflow 获取已加载的工作流运行时实例
func (e *Engine) GetWorkflow(workflowID string) (*Workflow, error)
```

---

## 3. Scheduler

**职责**：基于 Asynq 的任务分发、优先级管理、延迟调度、队列管理。Scheduler 封装 `asynq.Client`，提供幂等入队能力（`asynq.TaskID(execID:nodeName)` + `asynq.Unique`）。

---

## 4. State Manager

**职责**：工作流状态追踪、任务状态持久化、状态变更通知、断点续传支持。

进程内事件路由：Runner 通过 Runner Protocol 回报 Follower，Follower 内部将事件路由给 Executor；执行级分布式锁保证同一 Execution 在同一时刻只有一个 Follower 驱动调度。

---

## 5. API Server

**职责**：RESTful HTTP API、gRPC 服务、工作流管理接口。

HTTP 层使用 `net/http.ServeMux`（非 gin），见 `service/apiserver/apiserver.go`。

**Runner 上报路径**（以 `service/protocol/server.go` 中的常量为权威来源）：

```
POST /v1/runners/register         Runner 注册
POST /v1/runners/heartbeat        心跳
POST /v1/runners/poll             拉取任务
POST /v1/runners/result           任务结果上报（携带 lease_token 做 fencing）
POST /v1/runners/lease/renew      租约续约
POST /v1/runners/activation/ack   激活确认
POST /v1/runners/metrics          runner 指标上报（server 代理并入 /metrics；gRPC 不支持）
```

**工作流/执行面 API 路径**（user-facing。权威来源是 `service/apiserver/paths.go` 的 `Path*` 常量与 `UserFacingPaths` 枚举；两者必须同步，有 dead-constant 守卫）：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/workflows` | 创建工作流（仅注册 POST；GET 列表未实现） |
| GET/PUT/DELETE | `/v1/workflows/{id}` | 查询/更新/删除工作流 |
| POST | `/v1/workflows/execute` | 按内联定义触发执行 |
| POST | `/v1/workflows/{id}/execute` | 按已注册工作流触发执行 |
| GET  | `/v1/executions/{id}` | 查询执行 |
| POST | `/v1/executions/{id}/cancel` | 取消执行 |
| POST | `/v1/executions/{id}/signals` | 发送信号 |
| GET  | `/v1/executions/{id}/signals/{name}` | 查询信号 |
| GET  | `/v1/executions/{id}/wait` | 等待执行终态 |
| GET  | `/v1/supplies/{name}` | 取回 supply |
| GET  | `/v1/artifacts/{digest}` | 取回脚本产物（server 代理） |
| —    | `/v1/management/leader`、`/v1/management/runners/{id}`、`/v1/management/executions/{id}`、`/v1/management/dead-letters/{execID}[/replay]` | 管理面（需 `--management`） |
| GET  | `/healthz`、`/readyz` | 健康与就绪探针 |

> `POST /v1/executions` 是 runner protocol 面的 entry-seed 端点，不属于 user face，故不在 `UserFacingPaths` 与 OpenAPI 契约内。

---

## 6. 执行循环（Execution Loop）

**Server ↔ Runner 通信方式**：

| 方向 | 协议 | 说明 |
|------|------|------|
| server 内部 | Asynq（Redis 队列）| Scheduler 入队，Task Dispatcher 消费 |
| Server → Runner | Runner Protocol HTTP（主要）/ gRPC（实验性，非目标形态）| 下发 lease |
| Server → Relay Runner | Runner Protocol via RelayGateway | Gateway 只中继，不访问 Redis / Asynq |
| Runner → Server | `POST /v1/runners/result` | 任务完成/失败回报，携带 `lease_token` + fencing |

**推进流程**：

```
Runner 执行完毕
    │
    └─ POST /v1/runners/result → TaskDispatcher
              │
          Executor.OnTaskCompleted(event)
              │
          ① 获取执行级分布式锁
          ② 查找以该节点为依赖的所有下游节点
          ③ 检查每个下游节点依赖满足条件：
             - 普通节点：所有上游连接均已 success/skipped
             - Merge(wait_all)：原子计数，所有命名输入端口均已到达
             - Merge(wait_any)：任一输入端口到达（原子标记，防止重复触发）
          ④ 满足条件的节点加入 Asynq 队列（幂等入队）

    失败（result 重试耗尽或 lease 超时）→ LeaseSweeper / EntryActivationReconciler 兜底
```

**Transactional Outbox 机制**：

锁内只写 Redis（WriteOutbox + node status 同一 Pipeline），锁外由 `OutboxPublisher` goroutine 异步读取 Outbox 并调用 `Asynq.Enqueue`（幂等 TaskID）。崩溃时 LeaseSweeper 扫描超期 lease 重入队。

**Redis key 格式**（namespace 隔离，hash tag 保证 Redis Cluster 同槽）：

```
xflow:ns:<namespace>:exec:{<id>}:<suffix>
```

示例：
```
xflow:ns:default:exec:{abc123}:outbox:ready
xflow:ns:default:exec:{abc123}:node:my_node:status
xflow:ns:default:exec:{abc123}:leases          # 租约过期 ZSET
xflow:ns:default:exec:{abc123}:timeouts        # 超时 ZSET
```

**信号投递（xflow.wait 节点）**：

```
xflow:ns:<namespace>:exec:{<id>}:signal:<signal_name>
```

---

## 7. Monitor

**职责**：Prometheus 指标注册与暴露、OpenTelemetry 链路追踪、健康检查（`HealthCheck` 接口）。

---

## 8. 高可用（HA）

### 8.1 Leader 选举

Server Control Plane 集群基于 **Redis SETNX 租约**进行 Leader 选举，实现在 `backend/providers/distributed/leader.go`（`RedisLeaderElector`）。

- 租约 TTL：**15s**（`defaultLeaderLeaseTTL`）
- 续约频率：TTL/3
- 续约脚本：`PEXPIRE`（token-fenced，过期后其他副本 SETNX 接管）
- 放弃领导权：`leaderReleaseScript`（`DEL` if token matches）

```go
// RedisLeaderElector 通过 Redis SETNX 协调多副本选主。
type RedisLeaderElector struct {
    rdb redis.UniversalClient
    key string
    ttl time.Duration  // 默认 15s
    // ...
}

func (e *RedisLeaderElector) Campaign(ctx context.Context) error  // 阻塞直到获得租约
func (e *RedisLeaderElector) IsLeader() bool                      // 本地无网络轮询，惰性过期
func (e *RedisLeaderElector) Resign(ctx context.Context) error    // 主动让位
func (e *RedisLeaderElector) Notify() <-chan bool                  // 订阅领导权变更
```

后端不支持真实选举时（如 in-memory）使用 `backend.AlwaysLeader{}`。

### 8.2 Server 启动流程（planned）

共享基础设施组件在所有节点常驻启动（`apiServer`、`grpcServer`、`runnerProtocol`、`taskDispatcher`、`executor`）。Leader-only 组件（`entryReconciler`、`sweeper`、`timeoutMonitor`、`auditWorker`）通过订阅 `elector.Notify()` 通道动态门控：收到 `true` 时启动，收到 `false` 时停止。

### 8.3 Leader-only 组件

**`control.EntryActivationReconciler`**（`service/control/entry_activation_reconciler.go`）：
扫描 trigger 入口激活的期望态记录，分配 runner、管理 activation lease、检测过期并重派。
默认扫描周期 `DefaultEntryActivationReconcilePeriod = 10s`，lease TTL `DefaultEntryActivationLeaseTTL = 60s`。

**`control.LeaseSweeper`**（`service/control/lease_sweeper.go`）：
通过 `engine.StateStore.ListExpiredLeases()` 发现过期任务 lease，token-fenced 原子撤销并重入队（at-least-once，racing commit 仍可胜出）。
默认扫描周期 `DefaultSweepPeriod = 10s`，修复索引周期 `DefaultLeaseRepairPeriod = 1min`。

**`timeout.Monitor`**（`backend/providers/distributed/internal/timeout/monitor.go`）：
SCAN 各 namespace 的 per-execution 超时 ZSET（`xflow:ns:<namespace>:exec:{<id>}:timeouts`），对到期成员向引擎投递超时信号（at-least-once：先 peek，交付成功后 ZREM）。

**`control.AuditReconcileWorker`**（`service/control/audit_reconcile_worker.go`）：
对账 SQL audit log 中未结算的 admission 记录，探测引擎权威状态后补写 outcome 行。
默认扫描周期 `DefaultReconcilePeriod = 15s`，积压年龄阈值 `DefaultReconcileBacklog = 30s`，批量上限 `DefaultReconcileBatch = 256`。

### 8.4 分布式锁

| 锁 key | 粒度 | TTL | 用途 |
|--------|------|-----|------|
| `xflow:ns:<ns>:exec:{<id>}:...` 各子键共享 hash tag | 执行实例 | 随 Execution TTL | 保护调度决策原子性 |

LeaseSweeper 的 `IsAlive` 判定基于 Redis 租约，而非 Raft 心跳（无 Raft 实现）。

### 8.5 崩溃恢复

**Leader 崩溃**：Redis 租约 TTL（15s）自然过期 → 其他副本 SETNX 接管 → Leader-only 组件重新启动。运行中 Execution 不受影响。

**Follower 崩溃**：
1. 任务 lease 超期
2. LeaseSweeper 检测到过期 lease，token-fenced 重入队
3. EntryActivationReconciler 检测到 trigger activation 过期，重分配 runner

最坏恢复窗口：**~20s**（LeaseSweeper 10s 扫描周期 + EntryActivationReconciler 10s 扫描周期）。

---

## 9. 核心数据结构

### 9.1 WorkflowDef（工作流模板）

运行时定义只保留影响执行语义的字段。编辑器专属字段（`position`、`ui`、`notes`）和 `description` 单独存放在 `WorkflowEditorMetadata` 中；详见 [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

```go
// 以 types/workflow.go 为权威定义
type WorkflowDef struct {
    ID              string                    `json:"id,omitempty"`
    Namespace       string                    `json:"namespace,omitempty"` // 服务端权威，客户端无法跨 namespace
    Name            string                    `json:"name,omitempty"`
    Version         string                    `json:"version,omitempty"`
    Description     string                    `json:"description,omitempty"`
    Spec            string                    `json:"spec,omitempty"`
    RunnerSelector  *RunnerSelector           `json:"runner_selector,omitempty"` // struct tag 注意下划线
    Context         *WorkflowContext          `json:"context,omitempty"`
    Settings        *WorkflowSettings         `json:"settings,omitempty"`
    Options         *WorkflowOptions          `json:"options,omitempty"`
    Credentials     map[string]CredentialDef  `json:"credentials,omitempty"`
    Params          map[string]ParamDef       `json:"params,omitempty"`
    NodeTemplates   map[string]NodeTemplate   `json:"node_templates,omitempty"`
    Nodes           []NodeDef                 `json:"nodes,omitempty"`
    Groups          []GroupDef                `json:"groups,omitempty"`          // 节点分组（co-location）
    Connections     Connections               `json:"connections,omitempty"`
    Outputs         map[string]WorkflowOutput `json:"outputs,omitempty"`
    PinData         map[string]any            `json:"pin_data,omitempty"`
    DependencyEdges []DependencyEdge          `json:"dependency_edges,omitempty"` // supply 依赖边
}

type RunnerSelector struct {
    Mode        RunnerSelectorMode `json:"mode,omitempty"`
    MatchLabels map[string]string  `json:"match_labels,omitempty"` // 注意下划线
}

type NodeDef struct {
    ID             string          `json:"id,omitempty"`
    Name           string          `json:"name,omitempty"`
    Type           string          `json:"type,omitempty"`
    Kind           NodeKind        `json:"kind,omitempty"`
    Version        int             `json:"version,omitempty"`
    Template       string          `json:"template,omitempty"`
    Position       *Position       `json:"position,omitempty"`
    Disabled       bool            `json:"disabled,omitempty"`
    OnError        string          `json:"on_error,omitempty"`
    RunnerSelector *RunnerSelector `json:"runner_selector,omitempty"`
    Notes          string          `json:"notes,omitempty"`
    Inputs         []PortDecl      `json:"inputs,omitempty"`
    OutputSchema   map[string]any  `json:"output_schema,omitempty"`
    Parameters     map[string]any  `json:"parameters,omitempty"`
    UI             map[string]any  `json:"ui,omitempty"`
    Retry          *RetrySettings  `json:"retry,omitempty"`
    // Timeout bounds a single execution of this node. Zero → engine.DefaultNodeTimeout (30min)；
    // 负值 → 无限制，须显式写入。范围是单次调用，不含重试累计。
    Timeout        time.Duration   `json:"timeout,omitempty"`
}

// GroupDef 声明节点分组（co-location 单元）。见 types/group.go。
type GroupDef struct {
    Name           string          `json:"name,omitempty"`
    Members        []string        `json:"members,omitempty"`
    RunnerSelector *RunnerSelector `json:"runner_selector,omitempty"`
    OnError        string          `json:"on_error,omitempty"` // stop | continue（组级不支持 error_output / main_output）
    Retry          *RetrySettings  `json:"retry,omitempty"`
    Timeout        time.Duration   `json:"timeout,omitempty"`
    Mode           string          `json:"mode,omitempty"` // "" = durable；"transient" = 短 TTL
}

// DependencyEdge 声明 Node 依赖 supply 节点 Supply 的共享数据。
type DependencyEdge struct {
    Node   string `json:"node"`
    Supply string `json:"supply"`
}
```

### 9.2 执行状态（types/execution.go）

```go
// ExecutionStatus 执行状态机：pending → running → [success|failed|canceling→canceled|timeout]
type ExecutionStatus string

const (
    ExecutionStatusPending   ExecutionStatus = "pending"
    ExecutionStatusRunning   ExecutionStatus = "running"
    ExecutionStatusSuccess   ExecutionStatus = "success"
    ExecutionStatusFailed    ExecutionStatus = "failed"
    ExecutionStatusCanceling ExecutionStatus = "canceling" // 取消进行中
    ExecutionStatusCanceled  ExecutionStatus = "canceled"
    ExecutionStatusTimeout   ExecutionStatus = "timeout"
    // 注：无 "paused" 状态
)

// NodeStatus 节点状态机
type NodeStatus string

const (
    NodeStatusPending    NodeStatus = "pending"
    NodeStatusRunning    NodeStatus = "running"
    NodeStatusCommitting NodeStatus = "committing"
    NodeStatusSuccess    NodeStatus = "success"
    NodeStatusFailed     NodeStatus = "failed"
    NodeStatusSkipped    NodeStatus = "skipped"
    NodeStatusSuspended  NodeStatus = "suspended"
    NodeStatusContinued  NodeStatus = "continued"
    NodeStatusCanceled   NodeStatus = "canceled"
    NodeStatusWaiting    NodeStatus = "waiting"
)
```

---

## 10. 核心接口

```go
type Engine interface {
    LoadWorkflow(source WorkflowSource) (*Workflow, error)
    GetWorkflow(workflowID string) (*Workflow, error)

    // Start 启动工作流（共享能力，Leader 和 Follower 均可调用）
    Start(ctx context.Context, workflowID string, input map[string]any) (*Execution, error)

    Cancel(ctx context.Context, executionID string) error
    GetExecution(ctx context.Context, executionID string) (*Execution, error)
    ListExecutions(ctx context.Context, query *Query) ([]*Execution, error)

    // SendSignal 向指定执行实例发送外部信号，唤醒 xflow.wait 节点
    SendSignal(ctx context.Context, executionID string, signal *Signal) error
}
```

**表达式求值约定**：Server 在调度时仅将运行时原始数据（`$nodes`、`$input`、`$vars`、`$config`、`$inputs`）序列化到 `TaskPayload.Context`，不对 `Parameters` 中的任何表达式求值。Runner 收到 TaskPayload 后，用 Context 构建 Expr 环境，统一对 `${{ expr }}` / `{{ expr }}` 求值。

---

## 11. 类型定义

```go
// NodeKind 节点运行时角色（types/workflow.go）
type NodeKind string

const (
    NodeKindAction  NodeKind = "action"
    NodeKindTrigger NodeKind = "trigger"
    NodeKindSupply  NodeKind = "supply"  // 长生命周期共享数据节点
)

// ErrorStrategy 节点/工作流错误处理策略（engine/errorpolicy.go）
type ErrorStrategy string

const (
    ErrorStrategyStop        ErrorStrategy = "stop"         // 默认；执行中止
    ErrorStrategyErrorOutput ErrorStrategy = "error_output" // 路由到 "error" 端口
    ErrorStrategyMainOutput  ErrorStrategy = "main_output"  // 路由到 "main" 端口
    ErrorStrategyContinue    ErrorStrategy = "continue"     // 路由到 "main" 端口，节点状态为 "continued"
)

// PinDataMode
type PinDataMode string

const (
    PinDataModeTestOnly PinDataMode = "test_only"
    PinDataModeAlways   PinDataMode = "always"
    PinDataModeDisabled PinDataMode = "disabled"
)
```

---

## 12. 配置规范

```yaml
server:
  host: "0.0.0.0"
  http_port: 8080
  grpc_port: 9090

# Redis 租约选主（取代 Raft）
leader_election:
  redis_key: "xflow:leader"
  ttl: 15s          # defaultLeaderLeaseTTL

redis:
  addr: "localhost:6379"
  password: ""
  db: 0
  pool_size: 100

database:
  driver: "mysql"
  dsn: "xflow:xflow@tcp(localhost:3306)/xflow?parseTime=true"
  max_open_conns: 100
  max_idle_conns: 10

scheduler:
  concurrency: 100
  queues:
    high: 10
    default: 5
    low: 1

runner_dispatcher:
  enabled: true
  concurrency: 100
  lease_ttl: 60s
  pending_limit: 10000

runner_transport:
  protocol: "http"          # "http"（主要）| "grpc"（实验性，非目标形态）
  bind_addr: "0.0.0.0:9091"
  heartbeat_timeout: 60s

cron:
  timezone: "Asia/Shanghai"
  sync_interval: 30s

gateway:                    # 可选 Runner Protocol 中继（详见 GATEWAY-COMPONENTS.md）
  enabled: false
  port: 8081
  poll:
    max_wait: 30s
  dispatch:
    task_ttl: 15m
    inflight_ttl: 10m

entry_activation_reconciler:
  enabled: true
  period: 10s               # DefaultEntryActivationReconcilePeriod
  lease_ttl: 60s            # DefaultEntryActivationLeaseTTL

lease_sweeper:
  enabled: true
  period: 10s               # DefaultSweepPeriod
  repair_period: 60s        # DefaultLeaseRepairPeriod

timeout_monitor:
  enabled: true

audit_reconcile_worker:
  enabled: true
  period: 15s               # DefaultReconcilePeriod
  backlog: 30s              # DefaultReconcileBacklog
  batch_size: 256           # DefaultReconcileBatch

monitor:
  enabled: true
  prometheus_port: 9091

tracing:
  enabled: true
  endpoint: "localhost:4317"
  sample_rate: 0.1

logging:
  level: "info"
  format: "json"
```

### 默认值（engine/engine.go）

```go
const (
    DefaultNodeTimeout = 30 * time.Minute  // 单次节点调用超时（engine.DefaultNodeTimeout）
    DefaultLeaseTTL    = 60 * time.Second  // 任务 lease TTL（engine.DefaultLeaseTTL）
    // 无 DefaultWorkflowTimeout（超时是节点级语义）
    // 无 DefaultTaskTimeout（已改名为 DefaultNodeTimeout）
)
```
