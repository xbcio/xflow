# XFlow Server Control Plane 组件设计

> **Status: 目标设计（非当前实现）。** 本文描述的 Raft Leader-Follower HA 架构尚未落地。当前 server 是单实例 Control Plane MVP，见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md) §3、§7。

> Server Control Plane 负责工作流的调度与编排，采用 Raft Leader-Follower 高可用架构。Asynq / Redis 是 server 内部任务调度队列；Task Dispatcher 将 Asynq task 转换成 Runner Protocol lease，runner 不直连 Redis / Asynq。Relay Gateway 仅在 runner 无法直连 server 时作为可选中继。当前代码中的通用调度语义位于 `engine/`，通用执行边界位于 `execution/`；server 层只应新增协议、lease state、runner matching 等服务能力。

## 目录

1. [架构概览](#1-架构概览)
2. [Workflow Engine](#2-workflow-engine)
3. [Scheduler](#3-scheduler)
4. [State Manager](#4-state-manager)
5. [API Server](#5-api-server)
6. [执行循环（Execution Loop）](#6-执行循环execution-loop)
7. [Version Controller](#7-version-controller)
8. [Monitor](#8-monitor)
9. [高可用（HA）](#9-高可用ha)
10. [核心数据结构](#10-核心数据结构)
11. [核心接口](#11-核心接口)
12. [类型定义](#12-类型定义)
13. [配置规范](#13-配置规范)

---

## 1. 架构概览

### 1.1 整体架构

Server Control Plane 集群采用 **Raft Leader-Follower** 模型，组件分三层：共享基础设施（所有节点）、Leader-only（全局唯一）、Follower-only（实际调度）。单 server 部署时自选举为 Leader，三层全部启动。

```
                ┌──────────────────────────────────────────────┐
                │            Server Control Plane (Raft)             │
                │                                              │
                │  ┌──────────────────────────────────────┐    │
                │  │  共享基础设施（所有节点持有）           │    │
                │  │  WorkflowEngine · Scheduler (Asynq)  │    │
                │  │  TaskDispatcher · RunnerProtocol   │    │
                │  │  StateManager · Monitor · VersionCtrl │    │
                │  └──────────────────────────────────────┘    │
                │                                              │
                │  ┌──────────────────────────────────────┐    │
                │  │  Leader-only (1 台，Raft 选举)        │    │
                │  │  GlobalReconciler · TimeoutMonitor   │    │
                │  │  Archiver · DeadLetterProcessor      │    │
                │  └──────────────────────────────────────┘    │
                │                                              │
                │  ┌──────────────┐    ┌──────────────┐        │
                │  │ Follower-1   │    │ Follower-N   │        │
                │  │ API Server   │    │ API Server   │        │
                │  │ RelayGateway │    │ RelayGateway │        │
                │  │ Executor     │    │ Executor     │        │
                │  │ LocalRecon   │    │ LocalRecon   │        │
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
                │  │Server conn   │   │(via Relay Gateway)   │   │
                │  └──────────────┘   └──────────────────────┘   │
                └─────────────────────────────────────────────────┘
```

### 1.2 技术栈

| 组件 | 技术选型 |
|------|---------|
| Leader 选举 | hashicorp/raft |
| 服务发现 | 静态配置 / Consul（可选） |
| 任务调度 | Asynq (Redis-based) |
| 定时任务 | gocron (go-co-op/gocron)，Leader-only |
| 表达式引擎 | Expr (expr-lang/expr) |
| 状态存储 | Redis（热）+ MySQL（冷） |
| 监控 | Prometheus + Grafana |
| 链路追踪 | OpenTelemetry |
| API | gRPC + HTTP |

### 1.3 组件分层

| 层 | 组件 | 说明 |
|----|------|------|
| **共享基础设施** | WorkflowEngine | DSL 解析、编译、Build、`Start()` 创建 Execution + Enqueue 起始节点 |
| | Scheduler (asynq.Client) | 幂等任务入队 |
| | Task Dispatcher (`TaskDispatcher`) | 作为 Asynq worker 消费内部任务，创建 runner lease，匹配 runner，处理 result / timeout / cancel |
| | Runner Protocol (`RunnerProtocol`) | 控制面-执行面协议，维护 runner 注册、心跳、容量、lease、result |
| | StateManager | Redis + DB 状态读写、分布式锁 |
| | Monitor | 指标采集、健康检查 |
| | VersionController | 工作流版本管理 |
| **Leader-only** | GlobalReconciler | 扫描孤儿 Execution，补发调度 |
| | TimeoutMonitor | 扫描超时 Execution，标记 `timeout` |
| | Archiver | 终态 Execution 从 Redis 归档到 DB |
| | DeadLetterProcessor | Asynq 死信队列处理，触发错误策略 |
| **Follower-only** | API Server (HTTP + gRPC) | 对外服务，手动触发调用 `engine.Start()` |
| | RelayGateway | 可选的 Runner Protocol 中继层（详见 GATEWAY-COMPONENTS.md） |
| | Executor | `OnTaskCompleted()` — 接收 runner result，推进 DAG |
| | EventBus | 进程内事件路由（runner result → Executor） |
| | LocalReconciler | 扫描本 Follower 持锁的 Execution，补发漏调度 |

---

## 2. Workflow Engine

**职责**：DSL 解析、工作流编译与 Build、工作流启动、上下文管理、已加载工作流缓存。

> WorkflowEngine 是**共享基础设施**，当前通过 API/SDK 提交启动工作流；外部触发器后续重新设计。

```go
type WorkflowEngine struct {
    parser     *Parser
    compiler   *Compiler
    contextMgr *ContextManager
    exprEngine *expression.Engine
    scheduler  *Scheduler
    stateMgr   *StateManager
    workflows  map[string]*Workflow // key = workflowID:version
    mu         sync.RWMutex
}

// Parser DSL 解析器
type Parser struct {
    yamlParser *yaml.Parser
    jsonParser *json.Parser
}

// Compiler 编译器（校验 DSL 并调用 Workflow.Build() 填充运行时字段）
type Compiler struct {
    validator *Validator
}

// Start 启动工作流（共享能力，Leader 和 Follower 均可调用）
// 1. 从缓存获取已 Build 的 Workflow（未命中则从 DB 加载并 Build）
// 2. 创建 Execution 写入 Redis
// 3. Enqueue 起始节点到 Asynq 队列
func (e *WorkflowEngine) Start(ctx context.Context, workflowID string, input map[string]interface{}) (*Execution, error)

// GetWorkflow 获取已加载的工作流运行时实例（Executor / Reconciler 使用）
func (e *WorkflowEngine) GetWorkflow(workflowID string) (*Workflow, error)
```

---

## 3. Scheduler

**职责**：基于 Asynq 的任务分发、优先级管理、延迟调度、队列管理。

> Scheduler 是**共享基础设施**，封装 `asynq.Client`，提供幂等入队能力。

```go
type Scheduler struct {
    client    *asynq.Client
    inspector *asynq.Inspector
    queueMgr  *QueueManager
}

type QueueManager struct {
    queues map[string]*Queue
}

type Queue struct {
    Name        string
    Priority    int
    Concurrency int
}

// Enqueue 幂等入队（同一节点不会重复入队）
func (s *Scheduler) Enqueue(ctx context.Context, exec *Execution, node *Node) error {
    task := asynq.NewTask(string(node.Type), payload,
        asynq.Queue(s.selectQueue(node)),
        asynq.TaskID(fmt.Sprintf("%s:%s", exec.ID, node.Name)), // 幂等 key
        asynq.Unique(10*time.Minute),
    )
    _, err := s.client.EnqueueContext(ctx, task)
    if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
        return nil // 已入队，幂等跳过
    }
    return err
}
```

## 4. State Manager

**职责**：工作流状态追踪、任务状态持久化、状态变更通知、断点续传支持。

```go
type StateManager struct {
    redis    *redis.Client
    db       *sql.DB
    eventBus EventBus
}

// EventBus server 内部进程间事件总线（进程内 channel，非跨进程消息队列）
// Runner 通过 Runner Protocol 回报 Follower，Follower 的 EventBus 在进程内将事件路由给 Executor
// 执行级分布式锁保证同一 Execution 在同一时刻只有一个 Follower 驱动调度
type EventBus interface {
    Publish(event ExecutionEvent) error
    Subscribe(eventType EventType, handler EventHandler) error
    Unsubscribe(eventType EventType, handler EventHandler) error
}

type EventType string

const (
    EventTypeTaskCompleted  EventType = "task.completed"
    EventTypeTaskFailed     EventType = "task.failed"
    EventTypeTaskSkipped    EventType = "task.skipped"
    EventTypeTaskTimeout    EventType = "task.timeout"
    EventTypeSignalReceived EventType = "signal.received"
)

type ExecutionEvent struct {
    Type        EventType  `json:"type"`
    ExecutionID string     `json:"execution_id"`
    TaskID      string     `json:"task_id"`
    Status      TaskStatus `json:"status"`
    Output      interface{} `json:"output,omitempty"`
    Error       *TaskError  `json:"error,omitempty"`
    Timestamp   time.Time  `json:"timestamp"`
}
```

### 4.1 CheckpointManager

```go
// CheckpointManager 检查点管理
//
// 创建时机：每个节点成功完成后自动创建
// 存储内容：当前 Execution 全量快照
// 清理策略：每个 Execution 默认保留最近 10 个 Checkpoint；
//   Execution 终态后，Checkpoint 设置 TTL（默认 7 天）自动过期
// 幂等性保证：at-least-once（节点可能被重复执行）
type CheckpointManager struct {
    store StateStore
}

func (cm *CheckpointManager) CreateCheckpoint(ctx context.Context, executionID string) (*Checkpoint, error)
func (cm *CheckpointManager) RestoreCheckpoint(ctx context.Context, executionID, checkpointID string) (*Execution, error)

type Checkpoint struct {
    ID          string     `json:"id"`
    ExecutionID string     `json:"execution_id"`
    Snapshot    *Execution `json:"snapshot"`
    CreatedAt   time.Time  `json:"created_at"`
}
```

---

## 5. API Server

**职责**：RESTful API、gRPC 服务、WebSocket 推送、工作流管理接口。

```go
type HTTPServer struct {
    engine *gin.Engine
}

type GRPCServer struct {
    server *grpc.Server
}
```

**gRPC Service 定义**：

```protobuf
service WorkflowService {
    rpc CreateWorkflow(CreateWorkflowRequest) returns (Workflow);
    rpc GetWorkflow(GetWorkflowRequest) returns (Workflow);
    rpc ListWorkflows(ListWorkflowsRequest) returns (ListWorkflowsResponse);
    rpc StartExecution(StartExecutionRequest) returns (Execution);
    rpc GetExecution(GetExecutionRequest) returns (Execution);
    rpc CancelExecution(CancelExecutionRequest) returns (Empty);
}
```

**Runner result 接口**（内部接口，通过 token / mTLS 鉴权）：

```
POST /internal/tasks/:task_id/complete   Runner 任务完成回报
POST /internal/tasks/:task_id/fail       Runner 任务失败回报

Request body:
{
  "execution_id": "string",
  "output": <任意类型>,
  "error": { "code": "string", "message": "string", "retryable": bool }
}
```

---

## 6. 执行循环（Execution Loop）

**Server ↔ Runner 通信方式**：

| 方向 | 协议 | 说明 |
|------|------|------|
| server 内部 | Asynq（Redis 队列） | Scheduler 入队，Task Dispatcher 作为 Asynq worker 消费 |
| Server → Direct Runner | Runner Protocol | TCP / gRPC stream / WebSocket / HTTP long poll，下发 lease |
| Server → Relay Runner | Runner Protocol via RelayGateway | Gateway 只中继，不访问 Redis / Asynq |
| Runner → Server | Runner Protocol result | 任务完成/失败回报，携带 `task_id + attempt + lease_id + fencing_token` |

**推进流程**：

```
Runner 执行完毕
    │
    ├─ ① 写任务结果到 Redis（StateStore.UpdateTaskStatus）
    └─ ② Runner Protocol result → Server API / TaskDispatcher
              │
          EventBus.Publish(task.completed)
              │
          Executor.OnTaskCompleted(event)
              │
          ① 获取执行级分布式锁（防止多 Follower 重复调度）
          ② 查找以该节点为依赖的所有下游节点
          ③ 检查每个下游节点依赖满足条件：
             - 普通节点：所有上游连接均已 success/skipped
             - Merge(wait_all)：原子计数，所有命名输入端口均已到达
             - Merge(wait_any)：任一输入端口到达（原子标记，防止重复触发）
          ④ 满足条件的节点加入 Asynq 队列（幂等入队）

    失败（result 重试耗尽或 lease 超时）→ TaskDispatcher / Reconciler 兜底
```

```go
// Executor 核心推进方法（Follower-only）
func (e *Executor) OnTaskCompleted(event ExecutionEvent) error {
    // LockValue 携带 instanceID，Reconciler 可据此判断持锁方是否存活
    lock, err := e.stateMgr.AcquireLock(ctx, "exec:"+event.ExecutionID, 30*time.Second,
        LockValue(e.instanceID))
    if err != nil {
        return nil // 其他 Follower 持锁（存活），安全跳过
    }
    defer lock.Release()

    // CAS：防止同一 taskID 的重复回调重复推进 DAG
    ok, err := e.stateMgr.CompareAndSetTaskStatus(ctx,
        event.ExecutionID, event.TaskID,
        TaskStatusRunning, TaskStatusSuccess)
    if err != nil || !ok {
        return nil // 已处理过，幂等跳过
    }

    execution, _ := e.stateMgr.LoadState(ctx, event.ExecutionID)
    rt, _ := e.engine.GetWorkflow(execution.WorkflowID)
    readyNodes := rt.FindReadyNodes(execution, event.TaskID)

    // 写入 Transactional Outbox（与 Checkpoint 同一 Redis Pipeline，原子提交）
    // OutboxPublisher goroutine 在锁外异步读取 Outbox → Asynq.Enqueue（幂等）
    e.stateMgr.WriteOutbox(ctx, event.ExecutionID, readyNodes, execution)
    e.stateMgr.CreateCheckpoint(ctx, event.ExecutionID)
    return nil
}
```

**Transactional Outbox 机制**：

锁内只写 Redis（WriteOutbox + CreateCheckpoint 同一 Pipeline），锁外由 `OutboxPublisher` goroutine 异步读取 Outbox 并调用 `Asynq.Enqueue`（幂等 TaskID）。Follower 崩溃时，LocalReconciler 扫描 `xflow:outbox:*` 补发未完成的入队操作。

```
xflow:outbox:{execution_id}   Hash（field=node_name, value=TaskPayload JSON，TTL=5min）
```

**Merge 节点扇入竞态处理**（Redis Lua 原子操作）：

```lua
-- KEYS[1]: xflow:merge:{execution_id}:{merge_node_name}
-- ARGV[1]: expected（上游分支总数）
-- ARGV[2]: taskID（Asynq 幂等 key，格式 execID:nodeName）
-- ARGV[3]: execution timeout seconds（用于设置 TTL 防止 Set 泄漏）

redis.call("SADD", KEYS[1], ARGV[2])           -- 幂等：同一 taskID 重复 SADD 不增加计数
redis.call("EXPIRE", KEYS[1], ARGV[3])
local arrived = redis.call("SCARD", KEYS[1])
local expected = tonumber(ARGV[1])

if arrived >= expected then
    redis.call("DEL", KEYS[1])
    return 1  -- 由本次调用负责触发 Merge 节点
end
return 0      -- 等待其他分支
```

> **与旧方案的关键区别**：用 `SADD taskID` 替换 `INCR`，同一上游节点 Asynq 重试后多次触发 `OnTaskCompleted` 时，`SADD` 幂等不增加计数，彻底消除 Merge 重复触发风险。

**信号投递（xflow.wait 节点）**：

```go
func (e *WorkflowEngine) SendSignal(ctx context.Context, executionID string, signal *Signal) error {
    key := fmt.Sprintf("xflow:signal:%s:%s", executionID, signal.Name)
    data, _ := json.Marshal(signal.Payload)
    return e.stateMgr.Redis().Set(ctx, key, data, 0).Err()
}
```

---

## 7. Version Controller

```go
type VersionController struct {
    store VersionStore
}

type VersionStore interface {
    SaveVersion(ctx context.Context, workflow *WorkflowDef) error
    GetVersion(ctx context.Context, workflowID, version string) (*WorkflowDef, error)
    ListVersions(ctx context.Context, workflowID string) ([]*WorkflowVersion, error)
}
```

---

## 8. Monitor

```go
type Monitor struct {
    prometheus *prometheus.Registry
    tracer     trace.Tracer
    alertMgr   *AlertManager
}

type MetricsCollector struct {
    workflowCounter prometheus.Counter
    taskDuration    prometheus.Histogram
    queueSize       prometheus.Gauge
}

type HealthChecker struct {
    checks []HealthCheck
}

type HealthCheck interface {
    Name() string
    Check(ctx context.Context) error
}
```

---

## 9. 高可用（HA）

### 9.1 Leader-Follower 架构

Server Control Plane 集群基于 **Raft 共识协议**（hashicorp/raft）进行 Leader 选举。Raft 日志和稳定存储使用 MySQL（复用业务数据库），通过 `raft-mdb`（MySQL-backed Raft store）实现。

节点发现支持**静态配置**和 **Consul** 两种模式。

```go
type PeerDiscovery interface {
    GetPeers(ctx context.Context) ([]raft.Server, error)
    Watch(ctx context.Context) (<-chan PeerChangeEvent, error)
    Register(ctx context.Context, id string, addr string) error
    Deregister(ctx context.Context) error
}

type RaftManager struct {
    raft       *raft.Raft
    instanceID string
    bindAddr   string
    db         *sql.DB
    discovery  PeerDiscovery
    leaderCh   <-chan bool
}

func (rm *RaftManager) IsLeader() bool {
    return rm.raft.State() == raft.Leader
}
```

### 9.2 Server 启动流程

```go
type Server struct {
    // ── 共享基础设施（所有节点持有） ──
    raftMgr     *RaftManager
    engine      Engine
    scheduler   *Scheduler
    stateMgr    *StateManager
    monitor     *Monitor
    versionCtrl *VersionController

    // ── Leader-only（动态启停） ──
    globalReconciler    *GlobalReconciler
    timeoutMonitor      *TimeoutMonitor
    archiver            *Archiver
    deadLetterProcessor *DeadLetterProcessor

    // ── Follower-only（常驻运行） ──
    apiServer       *HTTPServer
    grpcServer      *GRPCServer
    taskDispatcher *TaskDispatcher // Asynq → Runner Protocol 适配层
    runnerProtocol  *RunnerProtocol  // 控制面-执行面协议
    relayGateway     *RelayGateway     // 可选中继层
    executor        *Executor
    localReconciler *LocalReconciler
}

func (s *Server) Run(ctx context.Context) error {
    s.stateMgr.Connect()
    s.engine.Init()
    s.raftMgr.Start(ctx)

    // Follower 组件（所有节点常驻）
    s.apiServer.Start()
    s.grpcServer.Start()
    s.runnerProtocol.Start(ctx)
    s.taskDispatcher.Start(ctx)
    s.executor.Start()
    s.localReconciler.Start(ctx)

    // RelayGateway 可选启动（复用 RunnerProtocol，不直连 Redis/Asynq）
    if s.cfg.Gateway.Enabled {
        go s.relayGateway.Start(ctx)
    }

    // 监听 Leader 状态变更，动态启停 Leader-only 组件
    go s.watchLeadership(ctx)

    <-ctx.Done()
    return s.shutdown()
}

func (s *Server) onBecomeLeader(ctx context.Context) {
    s.globalReconciler.Start(ctx)
    s.timeoutMonitor.Start(ctx)
    s.archiver.Start(ctx)
    s.deadLetterProcessor.Start(ctx)
}

func (s *Server) onBecomeFollower() {
    s.deadLetterProcessor.Stop()
    s.archiver.Stop()
    s.timeoutMonitor.Stop()
    s.globalReconciler.Stop()
}
```

### 9.3 Reconciler 分层

**GlobalReconciler（Leader-only）**：扫描所有 `running` 状态的 Execution，发现没有任何 Follower 持锁的孤儿 Execution，重新触发调度。

```go
type GlobalReconciler struct {
    stateMgr  *StateManager
    engine    Engine
    scheduler *Scheduler
    interval  time.Duration // 默认 30s
    batchSize int           // 默认 100
}

func (r *GlobalReconciler) reconcile(ctx context.Context) {
    executions, _ := r.stateMgr.ListByStatus(ctx, ExecutionStatusRunning, r.batchSize)
    for _, exec := range executions {
        lock, err := r.stateMgr.AcquireLock(ctx, "exec:"+exec.ID, 30*time.Second)
        if err != nil {
            continue // Follower 正在处理，跳过
        }
        exec, _ = r.stateMgr.LoadState(ctx, exec.ID)
        for taskID, task := range exec.Tasks {
            if task.Status != TaskStatusSuccess && task.Status != TaskStatusFailed {
                continue
            }
            rt, _ := r.engine.GetWorkflow(exec.WorkflowID)
            for _, node := range rt.FindReadyNodes(exec, taskID) {
                r.scheduler.Enqueue(ctx, exec, node)
            }
        }
        lock.Release()
    }
}
```

**LocalReconciler（Follower 组件）**：只扫描自己当前持锁的 Execution。

```go
type LocalReconciler struct {
    stateMgr   *StateManager
    engine     Engine
    scheduler  *Scheduler
    interval   time.Duration // 默认 15s
    instanceID string
}
```

### 9.4 Leader-only 辅助组件

**TimeoutMonitor**：定期扫描 `running` 状态的 Execution，检查是否超过 `Settings.Timeout`，标记为 `timeout`。

**Archiver**：Execution 到达终态后，从 Redis 异步归档到 DB，归档完成后设置 Redis TTL（默认 24h）。

**DeadLetterProcessor**：Asynq 任务重试耗尽后进入 dead queue，DeadLetterProcessor 定期扫描，对每个死信任务触发工作流错误处理策略（stop / error_output / main_output）。

### 9.5 分布式锁

| 锁 key | 粒度 | TTL | 用途 |
|--------|------|-----|------|
| `xflow:lock:exec:{execution_id}` | 执行实例 | 30s | 保护调度决策原子性 |

**锁 value 格式**：`"{instanceID}"` — Reconciler 获锁失败时读取 value，通过 Raft 心跳判断持锁方是否存活，避免将「GC 停顿」误判为「崩溃」后盲目接管。

```go
// AcquireLock value 选项
func LockValue(instanceID string) LockOption { ... }

// Reconciler 检查逻辑
func (r *GlobalReconciler) shouldTakeOver(instanceID string) bool {
    // 通过 RaftManager 判断该 instanceID 是否仍在集群中在线
    return !r.raftMgr.IsAlive(instanceID)
}
```

### 9.6 崩溃恢复

**Leader 崩溃**：Raft 心跳超时（约 10s）→ 新 Leader 选举 → `onBecomeLeader()` 重启 Leader-only 组件。已运行 Execution 完全不受影响，Follower 独立驱动。

**Follower 崩溃**：
1. 执行锁 30s 后过期
2. GlobalReconciler 发现锁已过期或持锁方已离线
3. 扫描 `xflow:outbox:{execution_id}`，补发未完成的 Asynq 入队
4. 若 Outbox 已清空，走原有 `FindReadyNodes` 补发路径

最坏恢复窗口：**~60s**（锁 TTL 30s + GlobalReconciler 扫描间隔 30s），满足 < 2 分钟 SLA。

**崩溃恢复幂等保证**：
- Outbox 补发的 `Asynq.Enqueue` 使用幂等 TaskID（`execID:nodeName`），重复入队安全
- Executor 接管后通过 `CompareAndSetTaskStatus` 跳过已完成的节点，不重复执行

---

## 10. 核心数据结构

### 10.1 WorkflowDef（工作流模板）

> 运行时定义只保留影响执行语义的字段。编辑器专属字段（`position`、`ui`、`notes`）和 `description` 已从 `WorkflowDef` / `NodeDef` 中拆分出来，单独存放在 `WorkflowEditorMetadata` 中；详见 [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

```go
type WorkflowDef struct {
    ID        string                    `json:"id,omitempty"`
    Namespace string                    `json:"namespace,omitempty"`
    // TenantID 由服务端从认证主体注入，客户端提供的值会被忽略。
    TenantID       string                    `json:"-"`
    Name           string                    `json:"name,omitempty"`
    Version        string                    `json:"version,omitempty"`
    Description    string                    `json:"description,omitempty"` // 编辑器/展示用，不参与运行时 hash
    Spec           string                    `json:"spec,omitempty"`        // DSL schema 版本
    RunnerSelector *RunnerSelector           `json:"runnerSelector,omitempty"`
    Context        *WorkflowContext          `json:"context,omitempty"`
    Settings       *WorkflowSettings         `json:"settings,omitempty"`
    Options        *WorkflowOptions          `json:"options,omitempty"`
    Credentials    map[string]CredentialDef  `json:"credentials,omitempty"`
    Params         map[string]ParamDef       `json:"params,omitempty"`
    NodeTemplates  map[string]NodeTemplate   `json:"node_templates,omitempty"`
    Nodes          []NodeDef                 `json:"nodes,omitempty"`
    Connections    Connections               `json:"connections,omitempty"` // key = 源节点名
    Outputs        map[string]WorkflowOutput `json:"outputs,omitempty"`
    PinData        map[string]any            `json:"pin_data,omitempty"`    // 运行时语义，参与运行时 hash
}

type WorkflowSettings struct {
    Timeout     int            `json:"timeout,omitempty"`
    Concurrency int            `json:"concurrency,omitempty"`
    Timezone    string         `json:"timezone,omitempty"`
    OnError     string         `json:"on_error,omitempty"`
    PinDataMode string         `json:"pin_data_mode,omitempty"`
    Retry       *RetrySettings `json:"retry,omitempty"`
}

type WorkflowContext struct {
    Vars   map[string]any `json:"vars,omitempty"`   // 业务逻辑常量，$vars
    Config map[string]any `json:"config,omitempty"` // 环境配置，$config
}

type WorkflowOptions struct {
    AllowCycles        bool `json:"allow_cycles,omitempty"`        // 是否允许循环执行
    MaxAutoDepth       int  `json:"max_auto_depth,omitempty"`      // 循环模式最大自动调度深度
    ExperimentalExpand bool `json:"experimental_expand,omitempty"` // 是否启用实验性 loop/split 展开
}

type RunnerSelector struct {
    Mode        RunnerSelectorMode `json:"mode,omitempty"`
    MatchLabels map[string]string  `json:"matchLabels,omitempty"`
}

type RunnerSelectorMode string

const (
    RunnerSelectorModeDefault  RunnerSelectorMode = "default"
    RunnerSelectorModeRequired RunnerSelectorMode = "required"
)

type CredentialDef struct {
    Name string `json:"name,omitempty"`
    Type string `json:"type,omitempty"`
}

type ParamDef struct {
    Type        string         `json:"type,omitempty"`
    Required    bool           `json:"required,omitempty"`
    DisplayName string         `json:"display_name,omitempty"`
    Default     any            `json:"default,omitempty"`
    Validation  map[string]any `json:"validation,omitempty"`
}

type NodeTemplate struct {
    Type       string         `json:"type,omitempty"`
    Parameters map[string]any `json:"parameters,omitempty"`
}

type WorkflowOutput struct {
    Value       any    `json:"value,omitempty"`
    DisplayName string `json:"display_name,omitempty"`
}

type NodeDef struct {
    ID             string          `json:"id,omitempty"` // 稳定编辑器标识，不参与运行时 hash
    Name           string          `json:"name,omitempty"`
    Type           string          `json:"type,omitempty"`
    Kind           NodeKind        `json:"kind,omitempty"`
    Version        int             `json:"version,omitempty"`
    Template       string          `json:"template,omitempty"`
    Position       *Position       `json:"position,omitempty"` // 编辑器专属，不参与运行时 hash
    Disabled       bool            `json:"disabled,omitempty"`
    OnError        string          `json:"on_error,omitempty"`
    RunnerSelector *RunnerSelector `json:"runnerSelector,omitempty"`
    Notes          string          `json:"notes,omitempty"` // 编辑器专属，不参与运行时 hash
    Inputs         []PortDecl      `json:"inputs,omitempty"`
    OutputSchema   map[string]any  `json:"output_schema,omitempty"`
    Parameters     map[string]any  `json:"parameters,omitempty"`
    UI             map[string]any  `json:"ui,omitempty"` // 编辑器专属，不参与运行时 hash
    Retry          *RetrySettings  `json:"retry,omitempty"`
}

type NodeKind string

const (
    NodeKindAction  NodeKind = "action"
    NodeKindTrigger NodeKind = "trigger"
)

type Position struct {
    X float64 `json:"x,omitempty"`
    Y float64 `json:"y,omitempty"`
}

type PortDecl struct {
    Name     string `json:"name,omitempty"`
    Required bool   `json:"required,omitempty"`
}

type Connections map[string]map[string][]Connection // key1 = 源节点名, key2 = 输出端口名

type Connection struct {
    Node  string `json:"node,omitempty"`
    Input string `json:"input,omitempty"` // 目标输入端口，默认 main
}

type RetrySettings struct {
    Enabled         bool    `json:"enabled,omitempty"`
    MaxAttempts     int     `json:"max_attempts,omitempty"`
    Strategy        string  `json:"strategy,omitempty"` // fixed/exponential
    InitialInterval int     `json:"initial_interval,omitempty"`
    MaxInterval     int     `json:"max_interval,omitempty"`
    Multiplier      float64 `json:"multiplier,omitempty"`
}
```

### 10.2 Execution（执行实例）

```go
type Execution struct {
    ID          string                    `json:"id"`
    WorkflowID  string                    `json:"workflow_id"`
    WorkflowVer string                    `json:"workflow_ver"`
    Status      ExecutionStatus           `json:"status"`
    Mode        ExecutionMode             `json:"mode"`
    Context     map[string]interface{}    `json:"context"`
    Input       map[string]interface{}    `json:"input"`
    Output      map[string]interface{}    `json:"output"`
    Tasks       map[string]*TaskExecution `json:"tasks"` // key = 节点名
    StartTime   time.Time                 `json:"start_time"`
    EndTime     *time.Time                `json:"end_time,omitempty"`
    Error       *ExecutionError           `json:"error,omitempty"`
}

type TaskExecution struct {
    TaskID    string      `json:"task_id"`
    Status    TaskStatus  `json:"status"`
    Attempts  int         `json:"attempts"`
    Input     interface{} `json:"input"`
    Output    interface{} `json:"output"`
    StartTime time.Time   `json:"start_time"`
    EndTime   *time.Time  `json:"end_time,omitempty"`
    Error     *TaskError  `json:"error,omitempty"`
    Logs      []LogEntry  `json:"logs"`
}
```

### 10.3 Workflow / Node（运行时实例）

```go
// Node 运行时节点（在 NodeDef 基础上增加图索引）
type Node struct {
    *NodeDef
    InEdges  []*Edge
    OutEdges []*Edge
}

// Workflow 工作流运行时实例（从 WorkflowDef 模板 Build 而来）
type Workflow struct {
    Def        *WorkflowDef
    Nodes      map[string]*Node
    Edges      []*Edge
    StartNodes []*Node
    EndNodes   []*Node
}

type Edge struct {
    From     string // 源节点名
    FromPort string // 源输出端口
    To       string // 目标节点名
    ToPort   string // 目标输入端口（默认 main）
}

func Build(def *WorkflowDef) (*Workflow, error)

func (w *Workflow) FindReadyNodes(exec *Execution, completedTaskID string) []*Node
```

---

## 11. 核心接口

### 11.1 Engine 接口

```go
type Engine interface {
    LoadWorkflow(source WorkflowSource) (*Workflow, error)
    GetWorkflow(workflowID string) (*Workflow, error)

    // Start 启动工作流（共享能力，Leader 和 Follower 均可调用）
    Start(ctx context.Context, workflowID string, input map[string]interface{}) (*Execution, error)

    Pause(ctx context.Context, executionID string) error
    Resume(ctx context.Context, executionID string) error
    Cancel(ctx context.Context, executionID string) error
    GetExecution(ctx context.Context, executionID string) (*Execution, error)
    ListExecutions(ctx context.Context, query *Query) ([]*Execution, error)

    // SendSignal 向指定执行实例发送外部信号，唤醒 xflow.wait 节点
    SendSignal(ctx context.Context, executionID string, signal *Signal) error
}

type Signal struct {
    Name    string                 `json:"name"`
    Payload map[string]interface{} `json:"payload"`
}
```

### 11.2 StateManager 接口

```go
type StateManager interface {
    SaveState(ctx context.Context, execution *Execution) error
    LoadState(ctx context.Context, executionID string) (*Execution, error)
    UpdateTaskStatus(ctx context.Context, executionID, taskID string, status TaskStatus) error

    // CompareAndSetTaskStatus 原子 CAS 任务状态，用于 Executor 幂等推进
    // 返回 (true, nil) 表示 CAS 成功；(false, nil) 表示状态不匹配（幂等跳过）
    CompareAndSetTaskStatus(ctx context.Context, executionID, taskID string, expected, target TaskStatus) (bool, error)

    // WriteOutbox 在锁内原子写入待 Enqueue 节点列表（与 CreateCheckpoint 同一 Pipeline）
    WriteOutbox(ctx context.Context, executionID string, nodes []*Node, exec *Execution) error

    // ScanOutbox 扫描未完成的 Outbox 条目（Reconciler 崩溃恢复用）
    ScanOutbox(ctx context.Context, executionID string) ([]*TaskPayload, error)

    CreateCheckpoint(ctx context.Context, executionID string) error
    RestoreCheckpoint(ctx context.Context, executionID, checkpointID string) error
    ListByStatus(ctx context.Context, status ExecutionStatus, limit int) ([]*Execution, error)
    ListLockedBy(ctx context.Context, instanceID string, status ExecutionStatus) ([]*Execution, error)
    AcquireLock(ctx context.Context, resource string, ttl time.Duration, opts ...LockOption) (*Lock, error)
    RenewLock(ctx context.Context, resource string, ttl time.Duration) error
    ReleaseAllLocks(ctx context.Context, instanceID string) error
    CompareAndSetStatus(ctx context.Context, executionID string, expected, target ExecutionStatus) error
    ArchiveToDB(ctx context.Context, execution *Execution) error
    MarkArchived(ctx context.Context, executionID string) error
    SetRedisTTL(ctx context.Context, executionID string, ttl time.Duration) error
    Redis() *redis.Client
}
```

**表达式求值约定**：

> **Server 注入数据，Runner 求值表达式**——server 在调度时仅将运行时原始数据（`$nodes`、`$input`、`$vars`、`$config`、`$inputs`）序列化到 `TaskPayload.Context`，不对 `Parameters` 中的任何表达式求值。Runner 收到 TaskPayload 后，用 Context 构建 Expr 环境，统一对 Parameters 中的 `${{ expr }}` / `{{ expr }}` 进行求值。

> **IF/Switch 路由**：Runner 求值条件表达式后，通过 output port 名称（`"true_branch"` / `"false_branch"` / `"case_x"`）回报给 server，server 根据 port 名在 DAG 中查找对应下游节点入队，无需理解表达式语义。
```

## 12. 类型定义

```go
// NodeType 节点类型
type NodeType string

const (
    NodeTypeHTTP         NodeType = "xflow.http"
    NodeTypeGRPC         NodeType = "xflow.grpc"
    NodeTypeFunction     NodeType = "xflow.function"
    NodeTypeDatabase     NodeType = "xflow.database"
    NodeTypeIF           NodeType = "xflow.if"
    NodeTypeSwitch       NodeType = "xflow.switch"
    NodeTypeLoop         NodeType = "xflow.loop"
    NodeTypeWait         NodeType = "xflow.wait"
    NodeTypeMerge        NodeType = "xflow.merge"
    NodeTypeSplit        NodeType = "xflow.split"
)

// NodeKind 节点运行时角色
type NodeKind string

const (
    NodeKindAction  NodeKind = "action"
    NodeKindTrigger NodeKind = "trigger"
)

// ExecutionStatus 执行状态机：pending → running → [success|failed|canceled|timeout|paused]
type ExecutionStatus string

const (
    ExecutionStatusPending  ExecutionStatus = "pending"
    ExecutionStatusRunning  ExecutionStatus = "running"
    ExecutionStatusSuccess  ExecutionStatus = "success"
    ExecutionStatusFailed   ExecutionStatus = "failed"
    ExecutionStatusCanceled ExecutionStatus = "canceled"
    ExecutionStatusTimeout  ExecutionStatus = "timeout"
    ExecutionStatusPaused   ExecutionStatus = "paused"
)

// TaskStatus 任务状态机：pending → running → [success|failed|skipped|retrying|pinned]
type TaskStatus string

const (
    TaskStatusPending  TaskStatus = "pending"
    TaskStatusRunning  TaskStatus = "running"
    TaskStatusSuccess  TaskStatus = "success"
    TaskStatusFailed   TaskStatus = "failed"
    TaskStatusSkipped  TaskStatus = "skipped"
    TaskStatusPinned   TaskStatus = "pinned"   // pin_data 替代执行，视为 success
    TaskStatusRetrying TaskStatus = "retrying"
)

type ErrorStrategy string

const (
    ErrorStrategyStop        ErrorStrategy = "stop"
    ErrorStrategyErrorOutput ErrorStrategy = "error_output"
    ErrorStrategyMainOutput  ErrorStrategy = "main_output"
)

type PinDataMode string

const (
    PinDataModeTestOnly PinDataMode = "test_only"
    PinDataModeAlways   PinDataMode = "always"
    PinDataModeDisabled PinDataMode = "disabled"
)

type RetrySettings struct {
    Enabled         bool    `json:"enabled,omitempty"`
    MaxAttempts     int     `json:"max_attempts,omitempty"`
    Strategy        string  `json:"strategy,omitempty"` // fixed/exponential
    InitialInterval int     `json:"initial_interval,omitempty"`
    MaxInterval     int     `json:"max_interval,omitempty"`
    Multiplier      float64 `json:"multiplier,omitempty"`
}

type ExecutionError struct {
    Code       string                 `json:"code"`
    Message    string                 `json:"message"`
    Type       string                 `json:"type"`
    Retryable  bool                   `json:"retryable"`
    Details    map[string]interface{} `json:"details,omitempty"`
    StackTrace string                 `json:"stack_trace,omitempty"` // 仅 debug 模式，生产必须关闭
    Timestamp  time.Time              `json:"timestamp"`
}
```

---

## 13. 配置规范

```yaml
# config/master.yaml

server:
  host: "0.0.0.0"
  http_port: 8080
  grpc_port: 9090

raft:
  bind_addr: "0.0.0.0:7000"
  discovery: static              # static | consul
  peers: []                      # static 模式，空则单节点
  # consul:
  #   addr: "localhost:8500"
  #   service_name: "xflow-master"
  #   token: "${CONSUL_TOKEN}"

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
  backpressure:
    enabled: true
    min_available_runner_capacity: 1

runner_transport:
  protocol: "grpc_stream"          # grpc_stream | websocket | http_long_poll | tcp
  bind_addr: "0.0.0.0:9091"
  heartbeat_timeout: 60s
  auth:
    token_expiry: 0
    mtls_enabled: true

cron:
  timezone: "Asia/Shanghai"
  sync_interval: 30s

gateway:                         # 可选 Runner Protocol 中继层（详见 GATEWAY-COMPONENTS.md）
  enabled: true
  port: 8081
  auth:
    heartbeat_timeout: 60s
    token_expiry: 0
  poll:
    max_wait: 30s
  dispatch:
    task_ttl: 15m
    inflight_ttl: 10m
  reconciler:
    enabled: true
    interval: 30s

reconciler:
  global:
    enabled: true
    interval: 30s
    batch_size: 100
  local:
    enabled: true
    interval: 15s

timeout_monitor:
  enabled: true
  interval: 30s
  batch_size: 100

archiver:
  enabled: true
  interval: 60s
  redis_ttl: 24h
  batch_size: 50

dead_letter:
  enabled: true
  interval: 30s
  batch_size: 50

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

### 默认值

```go
const (
    DefaultWorkflowTimeout = 24 * time.Hour
    DefaultTaskTimeout     = 5 * time.Minute
    DefaultMaxRetries      = 3
    DefaultRetryInterval   = 5 * time.Second
    DefaultRetryMultiplier = 2.0
    DefaultMaxConcurrency  = 10
    DefaultQueue           = "default"
    HighPriorityQueue      = "high"
    LowPriorityQueue       = "low"
)
```
