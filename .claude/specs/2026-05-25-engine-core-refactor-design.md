# XFlow SDK 引擎核心重构设计（修订版）

- 日期：2026-05-25
- 范围：引擎调度算法去重 + SuspendingHandler 抽象 + Store 接口拆分 + 性能修复 + 单 module 合并 + `cmd/server`、`cmd/worker` 最小入口
- 不在范围：Raft / Gateway / Edge Worker / 多租户 / Cron / Trigger / 鉴权

---

## 1. 目标与非目标

### 1.1 目标

1. **消除 LocalRunner 与 ClusterRunner 的算法重复** —— 调度推进、port 路由、skip 级联、OnError 策略抽为共享函数，两个 runner 调用同一份实现
2. **Wait 节点去硬编码** —— 引入 `SuspendingHandler` 可选接口，`xflow.wait` 退化为该接口的 builtin 实现；引擎不再 `if nd.Type == "xflow.wait"` 特判
3. **修复已知性能/正确性缺陷**
   - cluster.go `propagateNext` 每次重建 `nodeSet/inEdges`（O(E) × N）
   - `extendExecTTL` 逐个 EXPIRE 无 pipeline（4+4N 次 RTT）
   - `if r.store != nil` 双路径导致代码复杂度翻倍
   - `engine.go` 对 runner 类型的硬断言
   - Store 接口 13 方法混 4 类职责
4. **Store 接口拆分** —— 按职责拆为 3 个接口（ExecutionStore / NodeStore / SignalStore），降低自定义实现门槛
5. **Cluster 模式 Store 必选** —— 删除 `if r.store != nil` 双路径
6. **单 module 合并** —— `sdk/`、`node/`、`types/` 三个独立 module 合并为单 module，引擎实现收入 `engine/` 子树
7. **cmd/server + cmd/worker 最小可运行入口** —— 能跨进程联调

### 1.2 非目标

- 不实现 Raft、CronManager、Gateway、Edge Worker、Reconciler
- 不实现多租户、鉴权、Prometheus 指标
- 不替换 Asynq、不替换 MySQL
- 不预建空目录占位（无 `server/raft/`、`server/cron/`、`client/` stub）
- 不引入 `Clock` 抽象接口（可测性通过传入 `now time.Time` 参数解决）
- 不引入 `Capability` 反向索引（运行时用接口类型断言即可）
- 不引入 `RemoteAdapter`（无调用方，需要时再加）

### 1.3 验收标准

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | 所有现有 examples（basic_test、cluster_test）改写后行为不变 | `go test ./examples/...` |
| 2 | LocalRunner / ClusterRunner 旧文件被删除 | `git diff --stat` 确认 |
| 3 | Engine Core 不依赖 redis/asynq/mysql/sql 包 | `go list -deps ./engine/core/` 无这些包 |
| 4 | `go test ./... -race -count=1` 全过 | CI |
| 5 | `cmd/server` + `cmd/worker` 跨进程跑通 cluster_test 场景 | `scripts/dev-up.sh` + HTTP 断言脚本 |
| 6 | OnError 四策略实现只存在一份 | `grep -rn "func ApplyOnError" engine/` 只命中 `engine/core/errorpolicy.go` |

### 1.4 方案对比（为什么选彻底分层）

| 方案 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| A. 逐个修补 | 7 个独立 PR 各修一个缺陷 | 风险低、每步可验证 | 调度算法仍在两处；Wait 仍硬编码；目录结构不统一 |
| B. 抽共享函数 | 把 applyOnError / findReady / skipCascade 抽到 `internal/runner/shared.go` | 中等工作量 | local/cluster 仍各自维护状态机循环；新增 runner 仍需复制骨架 |
| C. 彻底分层（本方案）| Engine Core 纯算法 + Adapter 负责 IO | 算法只写一遍；新增部署模式只需新 Adapter；可纯单元测试 | 工作量大；API 破坏性变更 |

选 C 的理由：当前 cluster.go 1963 行、local.go 861 行，其中调度/OnError/Wait 逻辑占 ~60%。方案 B 能消除函数级重复，但两个 runner 的主循环（`runDAG` / `handleNodeTask` + `propagateNext`）结构差异大，共享函数无法覆盖主循环本身。方案 C 把主循环也统一。

---

## 2. 当前问题清单

| # | 缺陷 | 位置 | 影响 |
|---|---|---|---|
| 1 | 调度算法重复 | local.go:407-450, cluster.go:1476-1553 | 任何调度行为变更要双改 |
| 2 | OnError 四策略重复 | local.go:713-763, cluster.go:1714-1789 | 双改 |
| 3 | Wait 四模式重复 | local.go:604-707, cluster.go:843-1172 | 双改、引擎硬编码 `if nd.Type == "xflow.wait"` |
| 4 | 图索引重复重建 | cluster.go:1483-1484 每次 propagateNext 都 `buildNodeSet` + `buildInEdges` | O(E) × 完成节点数 |
| 5 | `if r.store != nil` 双路径 | cluster.go 全文 ~15 处 | 代码复杂度翻倍 |
| 6 | Engine 类型硬断言 | engine.go:215-227 `mr, ok := e.runner.(*runner.LocalRunner)` | LocalRunner-only 路径泄漏 |
| 7 | TTL 续期无 pipeline | cluster.go:1891-1906 | 4+4N 次 Redis RTT |
| 8 | Store 接口 13 方法混 4 类职责 | store.go:69-126 | 自定义实现门槛高 |
| 9 | Builder vs TaskHandler 双路径 | build.go + engine.go:215 | 类型断言泄漏 |
| 10 | handleNodeTask 单函数 ~70 行 | cluster.go:714-781 | 状态机+IO+hook+错误处理糅合 |

---

## 3. 整体架构

```
                    ┌────────────────────────────┐
                    │     types.WorkflowDef       │
                    │   （UI / Builder / YAML）    │
                    └─────────────┬──────────────┘
                                  │  Compile()
                                  ▼
                    ┌────────────────────────────┐
                    │      core.Graph (IR)        │
                    │  不可变；邻接表、反向边、     │
                    │  port 路由表、拓扑序          │
                    └─────────────┬──────────────┘
                                  │
        ┌─────────────────────────▼─────────────────────────┐
        │              core.Engine（纯算法）                 │
        │                                                   │
        │  Scheduler       OnNodeComplete / Skip / FanIn    │
        │  ErrorPolicy     ApplyOnError → Outcome           │
        │  SuspendCtrl     Suspend / Resume / Timeout       │
        │  Lifecycle       Submit / Cancel / Complete        │
        │                                                   │
        │  依赖 2 个接口：                                    │
        │    StateBackend   TaskQueue                      │
        └────────────────────────┬──────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                                     │
    ┌─────────▼──────────┐             ┌────────────▼───────────┐
    │   LocalAdapter     │             │   ClusterAdapter       │
    │                    │             │                        │
    │ memoryState        │             │ redisState + Store     │
    │ memoryQueue        │             │ asynqQueue             │
    └────────────────────┘             └────────────────────────┘
              ▲                                     ▲
              │                                     │
    ┌─────────┴──────────┐             ┌────────────┴───────────┐
    │ embedded SDK       │             │ cmd/server + cmd/worker │
    └────────────────────┘             └────────────────────────┘
```

**关键边界**：
- Engine Core **永远不导入** redis/asynq/mysql/sql 包；只依赖标准库 + `types` + `node`
- Adapter 负责把 Engine Core 的 2 个接口映射到具体 IO
- Engine Core 只有 2 个接口（StateBackend + TaskQueue），不是 4 个——`Hooks` 通过 Engine 构造参数注入但不算接口（它不影响算法正确性）；时间通过 `now time.Time` 参数传入（不需要 Clock 接口）

---

## 4. 核心概念

### 4.1 Graph IR（不可变运行时图）

`core.Graph` 是 `WorkflowDef` 的编译产物。一次编译，反复调度。

```go
package core

type Graph struct {
    Name     string
    Nodes    []NodeMeta           // 拓扑序排列
    Index    map[string]int       // name → Nodes 下标
    OutEdges [][]Edge             // [nodeIdx] → 出边列表
    InEdges  [][]Edge             // [nodeIdx] → 入边列表
    InDegree []int                // [nodeIdx] → 初始入度
    Vars     map[string]any
    Config   map[string]any
}

type NodeMeta struct {
    Name       string
    Type       string
    OnError    string
    Parameters map[string]any
    PortOuts   []string           // 该节点声明的所有输出端口
}

type Edge struct {
    SrcIdx, DstIdx     int
    SrcPort, DstPort   string
}

func Compile(def *types.WorkflowDef) (*Graph, error)
```

**设计决策**：
- 节点用整数下标索引——调度热路径全是数组访问，不再每次 `buildNodeSet`
- 编译时执行 cycle 检测（Kahn）+ 未知节点类型校验
- 编译完即只读，指针在 Engine 与 Adapter 之间共享，免锁

### 4.2 SuspendingHandler

```go
package node

// 普通同步节点（绝大多数）——签名不变
type TaskHandler interface {
    Execute(ctx context.Context, input *Input) (*Output, error)
}

// 可挂起节点——显式可选接口
type SuspendingHandler interface {
    PrepareSuspend(ctx context.Context, input *Input) (*SuspendSpec, error)
    OnResume(ctx context.Context, input *Input, signal *SignalPayload) (*Output, error)
}

type SuspendSpec struct {
    Mode    SuspendMode           // ModeSignal / ModeTimer / ModeMultiSignal
    Signals []string
    Quorum  int
    Timer   time.Duration
    Timeout time.Duration         // 任何 Mode 均可附加 Timeout（非零时启用超时）
}
```

**旧四模式到新三枚举的映射**：旧代码的 4 种 wait 模式（signal / timer / multi-signal / signal-with-timeout）在新设计中用 3 个枚举值 + `Timeout` 字段组合表达。`ModeSignal + Timeout > 0` 即旧的 "signal-with-timeout" 模式，无需独立枚举值。

type SignalPayload struct {
    Triggered SignalTrigger        // SignalReceived / TimeoutFired / TimerFired
    Name      string
    Data      map[string]any
    All       map[string]map[string]any
}
```

Engine Core 判断挂起能力：`if h, ok := handler.(SuspendingHandler); ok`——无字符串硬编码、无 Capability 索引。

`xflow.wait` 退化为 ~60 行 builtin handler，实现 `SuspendingHandler`。任何用户节点也可实现该接口（人工审批、sub-workflow 回调、async-callback）。

### 4.3 Engine Core 的 2 个接口

```go
package core

type StateBackend interface {
    // ── 生命周期组：Execution CRUD ──
    CreateExecution(ctx context.Context, e *ExecutionSnapshot) error
    UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error
    GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error)

    // ── 节点状态组：Node CRUD ──
    UpsertNode(ctx context.Context, n *NodeSnapshot) error
    GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error)

    // ── 调度组：原子操作，驱动 DAG 推进 ──
    // DecrementInDegree 原子递减目标节点的入度计数。
    //   portActive: 该入边是否来自上游的活跃输出端口（非 skip 路径）
    //   返回 remainingInDeg: 剩余入度；arrivedActiveIn: 已到达的活跃入边数
    DecrementInDegree(ctx context.Context, id types.ExecutionID, nodeIdx int, portActive bool) (remainingInDeg, arrivedActiveIn int, err error)
    CheckCompletion(ctx context.Context, id types.ExecutionID, totalNodes int) (allDone bool, hasFailed bool, err error)

    // ── 挂起/信号组：管理节点挂起与恢复 ──
    SuspendOrConsume(ctx context.Context, id types.ExecutionID, name string, spec *node.SuspendSpec) (*node.SignalPayload, error)
    DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) (resumeNode string, err error)
    AcquireResumeLock(ctx context.Context, id types.ExecutionID, name string) (bool, error)

    // ── 数据组：节点输出读写 ──
    PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
    GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error)
}

type TaskQueue interface {
    Enqueue(ctx context.Context, t *Task) error
    EnqueueDelayed(ctx context.Context, t *Task, delay time.Duration) error
}
```

**为什么只有 2 个接口而不是 4 个**：
- `Hooks` 是观察者，不影响算法正确性，通过构造参数注入即可
- `Clock` 不需要——需要当前时间的地方（TimeoutSweep）由调用方传入 `now time.Time`

**为什么 StateBackend 不再拆成 4 个子接口**：
- 拆接口的目的是降低自定义实现门槛。但 `SuspendOrConsume / DecrementInDegree / CheckCompletion` 要求原子语义（Redis 用 Lua、内存用 Mutex），任何实现者都必须理解全部语义。拆成 4 个接口只是把 13 个方法分到 4 个文件，实现者仍然要全部实现。
- 改为：StateBackend 是 Engine Core 唯一依赖的接口；**外部 Store**（MySQL 持久化）是 Adapter 内部的实现细节，不暴露给 Engine Core。

### 4.4 ErrorPolicy

```go
package core

type OnErrorOutcome struct {
    Output       map[string]any
    RoutePort    string          // "main" / "error"
    NodeStatus   string          // "success" / "continued" / "failed"
    ExecFatal    bool
    ErrorMessage string
}

func ApplyOnError(strategy string, sysErr error, bizErr *node.Error, output *node.Output) OnErrorOutcome
```

四种策略集中在这一个函数内，两个 Adapter 消费同一个 outcome。

### 4.5 Scheduler

```go
package core

// OnNodeComplete 在节点完成时被调用：
//   1. 对每条出边调用 state.DecrementInDegree
//   2. inDeg 归零 + activeIn > 0 → enqueue
//   3. inDeg 归零 + activeIn == 0 → skip 级联
//   4. 无新节点入队 → state.CheckCompletion
func (e *Engine) OnNodeComplete(ctx context.Context, id types.ExecutionID, g *Graph, completedIdx int, activePort string, output map[string]any) error
```

`DecrementInDegree` 是 StateBackend 的原子操作：Cluster 用 Lua（即现 `propagateLua`），Local 用 mutex 计数器。

### 4.6 Engine 入口

```go
// HandlerRegistry 是 Engine Core 解析 handler 的唯一入口。
// Core 不区分 handler 来源（全局注册 vs per-execution 闭包）。
type HandlerRegistry interface {
    Get(executionID types.ExecutionID, nodeName string, nodeType string) (node.TaskHandler, error)
}

type Engine struct {
    state    StateBackend
    queue    TaskQueue
    hooks    Hooks
    registry HandlerRegistry
    graphs   map[types.ExecutionID]*Graph  // Submit 时注册，Complete/Cancel 时移除
    logger   Logger
}

func NewEngine(state StateBackend, queue TaskQueue, opts ...EngineOption) *Engine

// 调度入口
func (e *Engine) Submit(ctx context.Context, g *Graph, params map[string]any) (types.ExecutionID, error)
func (e *Engine) ExecuteNode(ctx context.Context, t *Task) error   // TaskQueue 回调
func (e *Engine) DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error
func (e *Engine) TimeoutSweep(ctx context.Context, now time.Time) error
```

**HandlerRegistry 设计说明**：
- Engine Core 通过 `registry.Get(execID, nodeName, nodeType)` 获取 handler，不关心来源
- LocalAdapter 实现：先查 per-execution 闭包 map，未命中则查全局 type→handler map
- ClusterAdapter 实现：只查全局 type→handler map（闭包不可序列化，Submit 时拒绝）
- 这样 Core 内部无 `if local then ...` 分支，handler 解析策略完全由 Adapter 注入

**Task 结构体定义**：

```go
type TaskType int

const (
    TaskTypeNodeExec   TaskType = iota  // 首次执行节点
    TaskTypeNodeResume                  // 信号/超时触发恢复
)

type Task struct {
    ExecutionID types.ExecutionID
    NodeName    string
    NodeIdx     int
    Type        TaskType
    Payload     *node.SignalPayload  // 仅 TaskTypeNodeResume 时非 nil
}
```

**Resume 数据流**：
```
DeliverSignal(execID, signalName, data)
  → state.DeliverSignal → 返回 resumeNode
  → state.AcquireResumeLock(execID, resumeNode) → 抢占成功
  → queue.Enqueue(&Task{
        ExecutionID: execID,
        NodeName:    resumeNode,
        Type:       TaskTypeNodeResume,
        Payload:    &SignalPayload{Triggered: SignalReceived, Name: signalName, Data: data},
    })
```

`ExecuteNode` 内部流程（替代 cluster.go `handleNodeTask` + local.go `executeNode`）：

```
1. g := e.graphs[t.ExecutionID]（Submit 时注册）
   state.GetNode → 若 execution 已 canceled / 已终态 → return
2. hooks.OnNodeStart
3. nodeType := g.Nodes[t.NodeIdx].Type
   handler := e.registry.Get(t.ExecutionID, t.NodeName, nodeType)
4. 构建 node.Input
5. 分支：
     A. 普通 handler → output, err := handler.Execute(ctx, input)
     B. SuspendingHandler + TaskTypeNodeExec:
          spec := handler.PrepareSuspend(ctx, input)
          payload := state.SuspendOrConsume(ctx, id, name, spec)
          payload != nil → handler.OnResume(ctx, input, payload)
          payload == nil → hooks.OnNodeSuspended → return（释放 worker 槽）
     C. SuspendingHandler + TaskTypeNodeResume:
          handler.OnResume(ctx, input, task.Payload)
6. err/output.Error → ApplyOnError → outcome
7. state.PutOutput + state.UpsertNode
8. hooks.OnNodeComplete
9. outcome.ExecFatal → state.UpdateExecutionStatus(failed) → delete(e.graphs, id)
   否则 → e.OnNodeComplete(ctx, id, g, t.NodeIdx, outcome.RoutePort, output)
```

---

## 5. Adapter 实现

### 5.1 LocalAdapter

```go
package local

func New(opts ...Option) (*core.Engine, error) {
    state := newMemoryState()
    queue := newMemoryQueue(concurrency)
    registry := newLocalRegistry(globalRegistry)
    eng := core.NewEngine(state, queue, core.WithRegistry(registry), coreOpts...)
    queue.SetHandler(eng.ExecuteNode)
    return eng, nil
}
```

`memoryState` 实现 `core.StateBackend`：
- `DecrementInDegree`：`sync.Mutex` 保护的 `map[string]int`
- `SuspendOrConsume`：mutex 内检查 pending signal map，有则消费返回，无则标记 suspended
- `CheckCompletion`：遍历内存 node status map
- `UpsertNode`：CAS 语义——mutex 内检查当前状态，仅 running→终态 有效

`localRegistry` 实现 `core.HandlerRegistry`：
- `Get(execID, nodeName, nodeType)`：先查 per-execution 闭包 map，未命中则查全局 type→handler map
- Submit 时调用 `registry.Register(execID, closureHandlers)` 注入闭包

`memoryQueue` 实现 `core.TaskQueue`：
- channel + goroutine pool
- `EnqueueDelayed`：`time.AfterFunc` 后投入 channel

### 5.2 ClusterAdapter

```go
package cluster

func New(redisAddr string, store ClusterStore, opts ...Option) (*core.Engine, error) {
    state := newRedisState(redisAddr, store, execTTL)
    queue := newAsynqQueue(redisAddr, concurrency)
    registry := newClusterRegistry(globalRegistry)
    eng := core.NewEngine(state, queue, core.WithRegistry(registry), coreOpts...)
    queue.SetHandler(eng.ExecuteNode)
    go state.RunTimeoutMonitor(eng)
    return eng, nil
}
```

`redisState` 实现 `core.StateBackend`：
- Redis 热缓存 + 双写 ClusterStore（MySQL）
- Lua 脚本沿用现有 6 个：`suspendOrConsumeLua` / `signalOrStoreLua` / `propagateLua` / `quorumCheckLua` / `resumeNodeLua` / `checkCompletionLua`
- `UpsertNode`：Lua CAS——`HGET status`，仅当前状态为 `running` 时 `HSET` 新状态，否则返回 no-op
- **Store 必选**——删除 `if r.store != nil` 双路径

`clusterRegistry` 实现 `core.HandlerRegistry`：
- `Get(execID, nodeName, nodeType)`：只查全局 type→handler map
- Submit 时若传入闭包 handler 则返回错误（闭包不可序列化，跨进程无法传递）

`asynqQueue` 实现 `core.TaskQueue`：
- 包装 `asynq.Client.EnqueueContext`
- `EnqueueDelayed` → `asynq.ProcessIn(delay)`

**性能修复**（在 Adapter 内完成）：
- `propagateNext` 不再重建 `nodeSet/inEdges`——直接从 `Graph.OutEdges[completedIdx]` 读
- `extendExecTTL` 改 `r.rdb.Pipeline()`，一次 RTT
- `checkCompletion` 的 KEYS 数量 >1000 时切分批次

### 5.3 ClusterStore（Adapter 内部依赖，非 Engine Core 接口）

```go
package store

type ExecutionStore interface {
    Create(ctx context.Context, rec *ExecutionRecord) error
    UpdateStatus(ctx context.Context, id types.ExecutionID, status types.Status, errMsg string) error
    Get(ctx context.Context, id types.ExecutionID) (*ExecutionRecord, error)
}

type NodeStore interface {
    Upsert(ctx context.Context, rec *NodeRecord) error
    Get(ctx context.Context, id types.ExecutionID, name string) (*NodeRecord, error)
    List(ctx context.Context, id types.ExecutionID) ([]*NodeRecord, error)
    ListSuspendedBySignal(ctx context.Context, id types.ExecutionID, signal string) ([]*NodeRecord, error)
    ListExpiredSuspensions(ctx context.Context, now time.Time) ([]*NodeRecord, error)
}

type SignalStore interface {
    Save(ctx context.Context, rec *SignalRecord) error
    Consume(ctx context.Context, id types.ExecutionID, name string) (*SignalRecord, error)
    CountByNames(ctx context.Context, id types.ExecutionID, names []string) (int, error)
}

// ClusterStore 是 3 个接口的并集；MySQL 内置实现一行满足。
type ClusterStore interface {
    ExecutionStore
    NodeStore
    SignalStore
}
```

注意：这 3 个接口是 **ClusterAdapter 的内部依赖**，不是 Engine Core 的接口。Engine Core 只看到 `StateBackend`。用户自定义持久化只需实现 `ClusterStore`（或用内置 MySQL）。

---

## 6. 公共 API

```go
package xflow

// 构造入口——两个工厂函数，不是一个 Config struct
func NewLocal(opts ...LocalOption) (*Engine, error)
func NewCluster(redisAddr string, store ClusterStore, opts ...ClusterOption) (*Engine, error)

type Engine struct { core *core.Engine }

// 引擎 API
func (e *Engine) Start(ctx context.Context) error
func (e *Engine) Stop()
func (e *Engine) Submit(ctx context.Context, wf *WorkflowBuilder, params map[string]any) (types.ExecutionID, error)
func (e *Engine) Wait(ctx context.Context, id types.ExecutionID) (types.Result, error)
func (e *Engine) Result(ctx context.Context, id types.ExecutionID) (types.Result, error)
func (e *Engine) Status(ctx context.Context, id types.ExecutionID) (types.Status, error)
func (e *Engine) Signal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error   // 内部调用 core.DeliverSignal
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error

// Query API（Cluster only，直接查 ClusterStore）
func (e *Engine) ListExecutions(ctx context.Context, f ExecutionFilter) ([]*ExecutionRecord, error)
func (e *Engine) ListPendingApprovals(ctx context.Context) ([]*NodeRecord, error)
```

**为什么用两个工厂而不是三工厂 Config**：
- `NewLocal()` 和 `NewCluster(redis, store)` 的参数集完全不同，强行统一到一个 Config 只增加间接层
- 用户代码一眼看出部署模式，不需要读 Config 内部字段

**用户视角**：
```go
// 单进程
eng, _ := xflow.NewLocal()
eng.Start(ctx)
defer eng.Stop()

// 多进程
s := mysql.New(dsn)
eng, _ := xflow.NewCluster(redisAddr, s,
    xflow.WithExecTTL(7*24*time.Hour),
    xflow.WithCredential("github_token", map[string]any{"value": tok}),
)
```

---

## 7. 并发与原子性保证

| 场景 | 风险 | 保证机制 |
|---|---|---|
| 多 Worker 并发完成同一节点 | 双重推进 | `StateBackend.DecrementInDegree` 原子（Lua / mutex）|
| Signal 与 Suspend 的 race | signal 早于 suspend 到达 | `SuspendOrConsume` Lua 原子 |
| 信号 + Timeout 同时触发 | 双重路由 | `AcquireResumeLock` 抢占 |
| 节点超时与正常完成的 race | TimeoutSweep 标记超时的同时 Worker 完成节点 | `StateBackend.UpsertNode` 使用 CAS（compare-and-swap）：仅当当前状态为 `running` 时才写入新状态；Cluster 用 Lua `HSETNX` 语义，Local 用 mutex + 状态前置检查。先到者胜，后到者 no-op |
| Submit 后立即 Wait | execution 未注册 | Local 用 sync.Cond；Cluster 用 Pub/Sub + 30s 兜底轮询 |
| Hook 阻塞 worker | 节点完成被卡 | 5s timeout + WaitGroup |
| 节点 panic | worker 退出 | `ExecuteNode` 顶层 defer recover → sysErr |

---

## 8. 持久化模型（Cluster 必选）

**写路径**：
```
Engine Core → StateBackend.UpsertNode(...)
                ↓ (redisState 内部)
Redis SET (热) → ClusterStore.NodeStore.Upsert (冷, retry 3 次)
```

**读路径**（按需 rehydrate）：
```
StateBackend.GetNode(id, name)
  → Redis GET → 命中返回
  → 未命中 → ClusterStore.NodeStore.Get → Redis 写回 → 返回
```

**failure 处理**：
- 临界写（CreateExecution、SuspendOrConsume）失败 → fail node
- 非临界写（UpsertNode、PutOutput）失败 → 日志告警，不阻塞流程

---

## 9. cmd/server 与 cmd/worker

### 9.1 cmd/server

- 读 `configs/server.yaml`
- 启动 ClusterAdapter Engine
- HTTP API（chi）：
  ```
  POST /v1/workflows              上传 WorkflowDef JSON
  POST /v1/executions             Submit
  GET  /v1/executions/:id         Result
  GET  /v1/executions/:id/status  Status
  POST /v1/executions/:id/signal  Signal
  POST /v1/executions/:id/cancel  Cancel
  GET  /v1/executions             ListExecutions
  GET  /v1/pending-approvals      ListPendingApprovals
  ```
- Graceful shutdown
- 不带鉴权、不带 Prometheus（标记 TODO）

### 9.2 cmd/worker

- 读 `configs/worker.yaml`
- 启动 Asynq Server，注册 `eng.ExecuteNode` 为 handler
- 用户在自己的 worker 里 `import _ "myhandlers"` 注入全局 Registry

### 9.3 联调脚本

`scripts/dev-up.sh`：docker-compose 起 Redis + MySQL，cmd/server :8080，cmd/worker 后台，验证脚本 POST workflow 断言 success。

---

## 10. 文件布局

单 module：`go.mod` 路径 `github.com/xbcio/xflow`。

```
xflow/
├── go.mod
├── go.sum
├── engine/
│   ├── core/
│   │   ├── graph.go              // Graph IR + Compile
│   │   ├── engine.go             // Engine struct + ExecuteNode
│   │   ├── scheduler.go          // OnNodeComplete / Skip / FanIn
│   │   ├── suspend.go            // SuspendController
│   │   ├── errorpolicy.go        // ApplyOnError
│   │   ├── interfaces.go          // StateBackend / TaskQueue 接口
│   │   └── *_test.go
│   ├── adapter/
│   │   ├── local/
│   │   │   ├── adapter.go
│   │   │   ├── memory_state.go
│   │   │   └── memory_queue.go
│   │   └── cluster/
│   │       ├── adapter.go
│   │       ├── redis_state.go    // 含 6 个 Lua
│   │       ├── asynq_queue.go
│   │       ├── timeout_monitor.go
│   │       └── rehydrate.go
│   └── store/
│       ├── store.go              // 3 个接口 + 类型
│       └── mysql/
│           └── store.go
│
├── types/                        // 共享类型（普通子包）
├── node/                         // 节点契约（普通子包）
│
├── sdk/                          // 公共面，包名 xflow
│   ├── xflow.go                  // NewLocal / NewCluster
│   ├── builder.go                // WorkflowBuilder
│   ├── workflow.go               // NodeRef / Run convenience
│   ├── option.go                 // LocalOption / ClusterOption
│   └── hook.go                   // ExecutionHook / BaseHook
│
├── cmd/
│   ├── server/main.go
│   └── worker/main.go
│
├── examples/
│   ├── basic_test.go
│   ├── cluster_test.go
│   └── suspend_test.go
│
├── configs/
│   ├── server.yaml
│   └── worker.yaml
│
├── scripts/
│   └── dev-up.sh
│
├── db/
│   └── xflow_schema.sql
└── docs/
```

**迁移映射**：
| 旧位置 | 新位置 |
|---|---|
| `sdk/xflow/internal/runner/local.go` | `engine/core/*` + `engine/adapter/local/` |
| `sdk/xflow/internal/runner/cluster.go` | `engine/core/*` + `engine/adapter/cluster/` |
| `sdk/xflow/internal/runner/runner.go` | 删除（被 `engine/core/interfaces.go` 替代）|
| `sdk/xflow/internal/store/*` | `engine/store/*` |
| `sdk/xflow/{engine,build,workflow}.go` | `sdk/{xflow,builder,workflow}.go` |
| `sdk/examples/*` | `examples/*` |
| `node/`（独立 module）| `node/`（普通子包）|
| `types/`（独立 module）| `types/`（普通子包）|

---

## 11. 测试策略

### 11.1 Engine Core 单元测试（零 IO 依赖）

用 fake StateBackend + fake TaskQueue：
- `scheduler_test.go`：单链 / 扇出 / 扇入 / port 路由 / skip 级联 / 多节点同时 ready
- `errorpolicy_test.go`：四种策略
- `suspend_test.go`：signal 早到/后到/timer/timeout/multi-signal quorum
- `graph_test.go`：Compile 校验、cycle 检测

fake StateBackend 的复杂度说明：`DecrementInDegree` 和 `SuspendOrConsume` 需要原子语义，fake 用 mutex 实现（~100 行）。这是不可避免的——测试调度算法必须模拟并发竞争。

### 11.2 Adapter 集成测试

- `adapter/local/` —— 真 memoryState + memoryQueue，端到端
- `adapter/cluster/` —— testcontainers 起 Redis + MySQL，跑全部场景
- 共用一组 `compat_test.go` 用例（同一组 workflow 在 local/cluster 都跑）

### 11.3 现有 examples 回归

- `examples/basic_test.go` 用新 API 重写
- `examples/cluster_test.go` 用新 API 重写
- 新增 `examples/suspend_test.go`

---

## 12. 实施阶段（7 个 PR）

每个 PR 独立可合并、可回滚、有明确验收。

| Phase | 范围 | 变动估算 | 验收 |
|---|---|---|---|
| P1 | 三 module 合并单 module（mechanical：删 `sdk/go.mod`、`node/go.mod`、`types/go.mod`，统一 import path） | 0 行逻辑变更 | `go build ./...` 通过；现有测试不变 |
| P2 | `engine/core/graph.go` + `Compile` + `graph_test.go` | ~300 行新增 | 单元测试通过；老代码不动 |
| P3 | `engine/core/errorpolicy.go` + 测试 | ~150 行新增 | 测试通过；老代码不动 |
| P4 | `engine/core/interfaces.go` + `engine.go` + `scheduler.go` + `suspend.go` + fake 测试 | ~800 行新增 | core 单测全过；老代码不动 |
| P5 | `engine/adapter/local/` + `engine/adapter/cluster/` + `engine/store/` | ~1200 行新增 | adapter 集成测试通过 |
| P6 | `sdk/` 重写 + `examples/` 重写；删除旧 `sdk/xflow/internal/runner/`、旧 store | ~600 行新增，~2800 行删除 | `go test ./... -race` 全过 |
| P7 | `cmd/server` + `cmd/worker` + configs + scripts | ~500 行新增 | 跨进程 e2e 通过 |

**为什么 P1 先做 module 合并**：P2-P7 的新代码全部使用统一 import path（`github.com/xbcio/xflow/engine/core` 等）。如果 module 合并放在最后，P2-P6 期间新代码的 import path 在合并时全部要改，增加无谓的 rebase 冲突。先做 mechanical 合并（diff 大但无逻辑变更，review 容易），后续 PR 直接写最终路径。

**P4 是核心风险点**：Engine Core 的 `ExecuteNode` 主循环必须在 P5 接入真实 Adapter 前通过 fake 测试覆盖所有分支。P4 完成后做一次 dry-run：用 cluster_test 的所有用例手动走一遍 Engine Core 接口契约，确认无遗漏。

---

## 13. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Engine Core API 漏掉某种 wait 语义 | P4 完成后用全部现有用例 dry-run 接口契约 |
| Cluster 集成测试需要 Docker | testcontainers + CI 有 Docker；本地无 Docker 时 skip |
| Lua 脚本迁移出错 | redisState 单测覆盖每条 Lua 所有分支；语义不改 |
| 闭包 handler 在 Engine Core 表达 | HandlerRegistry 接口统一解析；LocalAdapter 实现内部区分闭包/全局，Core 无感知 |
| P6 删除量大 | P6 前确保 P5 adapter 测试已全过；P6 只做"接线"不做逻辑 |
| fake StateBackend 复杂度 | 控制在 ~100 行；只实现 Engine Core 实际调用的方法 |
| P1 module 合并 diff 大 | 纯 mechanical 变更（删 go.mod、改 import path），无逻辑修改，review 低风险 |
| 节点超时与正常完成 race | UpsertNode 使用 CAS 语义，仅 running→终态 转换有效，后到者 no-op |

---

## 14. 设计决策摘要

| 决策 | 选择 | 理由 |
|---|---|---|
| 重构方式 | 彻底分层 | local/cluster 主循环结构差异大，共享函数无法覆盖主循环 |
| Engine Core 接口数量 | 2（StateBackend + TaskQueue）| Hooks 不影响正确性；Clock 用参数传入 |
| StateBackend 是否拆子接口 | 不拆（按职责分组注释） | 原子操作语义要求实现者理解全部方法，拆接口不降低门槛 |
| 外部 Store 接口 | 3 个（Execution/Node/Signal）| 这是 Adapter 内部依赖，不是 Core 接口 |
| 挂起机制 | SuspendingHandler 可选接口 | 无 Capability 索引、无字符串硬编码 |
| 公共 API | 两个工厂函数 | 参数集不同，不强行统一 |
| 预留扩展 | 不预建空目录/stub | 需要时再加，避免 dead code |
| 包结构 | engine/{core,adapter,store} + sdk + cmd | 边界清晰 |
| 闭包 handler | HandlerRegistry 接口统一解析 | Core 不区分 handler 来源，策略由 Adapter 注入 |
| 单 module 合并 | 第一个 PR（P1）| 先统一 import path，后续 PR 直接写最终路径，避免 rebase 冲突 |
| 节点状态写入 | CAS 语义（仅 running→终态） | 防止超时与正常完成的 race 导致双重状态写入 |
