# XFlow Runner 组件设计

> Runner 负责实际执行工作流节点任务。XFlow 支持两种接入方式：Direct Runner 直接连接 server 的 Runner Protocol；Relay Runner 通过 Relay Gateway 中继连接。两者共享相同的 ActionHandler 接口，执行逻辑完全一致，差异仅在网络接入方式。Runner 永远不直连 server 内部 Redis / Asynq。当前代码中的 embedded `execution.Runner` 是本地实现，未来独立 `xflow-runner` 应复用同一执行语义，仅替换连接方式。

## 目录

1. [Runner 类型概览](#1-runner-类型概览)
2. [Direct Runner](#2-direct-runner)
3. [Relay Runner](#3-relay-runner)
4. [ActionHandler 接口](#4-actionhandler-接口)
5. [Plugin Manager](#5-plugin-manager)
6. [Retry Engine](#6-retry-engine)
7. [配置规范](#7-配置规范)

---

## 1. Runner 类型概览

```
                       ┌─────────────────────────────┐
                       │         Task 执行层          │
                       │                             │
                       │  ┌──────────────────────┐   │
                       │  │  ActionHandler 接口     │   │
                       │  │  （两种 Runner 共用）  │   │
                       │  └──────────┬───────────┘   │
                       └────────────┼────────────────┘
                                    │
               ┌────────────────────┴───────────────────┐
               │                                        │
  ┌────────────▼─────────────┐          ┌───────────────▼──────────┐
  │      Direct Runner       │          │       Relay Runner        │
  │                          │          │                           │
  │  • Runner Protocol      │          │  • Relay Gateway        │
  │  • 直连 server            │          │  • 中继 Runner Protocol  │
  │  • 不直连 Redis/Asynq     │          │  • 不直连 Redis/Asynq     │
  │  • 结果回报 server        │          │  • 结果经 relay 回 server │
  └──────────────────────────┘          └───────────────────────────┘
```

| | Direct Runner | Relay Runner |
|--|-----------------|-------------|
| **接入方式** | Runner Protocol（TCP / gRPC stream / WebSocket / HTTP long poll） | Runner Protocol via Relay Gateway |
| **部署场景** | runner 能访问 server | 浏览器 WASM、用户本机、跨 DC、跨云测试环境、受限内网环境 |
| **连接发起方** | Runner 主动连接 server | Runner 主动连接 Gateway，Gateway 再连 server |
| **结果上报** | Runner Protocol result | Gateway 中继 result |
| **作用域** | 系统级（所有用户） | 系统级或用户级 |
| **NAT 穿透** | 只需能访问 server Runner Protocol 端口 | 只需能访问 Relay Gateway 端口 |

---

## 2. Direct Runner

### 2.1 Task Executor（任务执行器）

**职责**：
- 连接 server 的 Runner Protocol，注册 runner 能力、标签、并发容量
- 接收 Task Dispatcher 下发的 task lease
- 反序列化 TaskPayload，构建表达式上下文
- 调用对应 ActionHandler 执行任务
- 执行完成后通过 Runner Protocol 回报 result

**表达式求值**：节点参数中的表达式（如 `${{ $input.xxx }}`）在 Runner 侧求值。Server 调度任务时将所需上下文（`$input`、`$vars`、`$config`、`$nodes` 等）序列化到 TaskPayload，Runner 反序列化后构建 Expr 环境并求值。

```go
type TaskExecutor struct {
    transport    RunnerProtocolClient
    handlers     map[string]ActionHandler
    exprEngine   *expression.Engine
    limiter      *ResourceLimiter
    hookRegistry *HookRegistry
}

// RunnerProtocolClient Runner → Server 执行协议客户端
// 底层可实现为 gRPC stream、TCP framed protocol、WebSocket 或 HTTP long poll。
type RunnerProtocolClient interface {
    Register(ctx context.Context, req RegisterRunnerRequest) error
    Receive(ctx context.Context) (*TaskLease, error)
    Ack(ctx context.Context, leaseID string) error
    ReportResult(ctx context.Context, result *TaskResult) error
    Heartbeat(ctx context.Context, status RunnerStatus) error
}
```

### 2.2 Direct Runner 启动流程

```go
func (r *Runner) Run(ctx context.Context) error {
    if err := r.transport.Register(ctx, r.registration()); err != nil {
        return err
    }
    go r.heartbeatLoop(ctx)

    for i := 0; i < r.concurrency; i++ {
        go r.receiveLoop(ctx)
    }
    <-ctx.Done()
    return ctx.Err()
}

func (r *Runner) receiveLoop(ctx context.Context) {
    for {
        lease, err := r.transport.Receive(ctx)
        if err != nil {
            r.backoff.Wait(ctx)
            continue
        }
        go r.executeLease(ctx, lease)
    }
}
```

---

## 3. Relay Runner

### 3.1 概述

Relay Runner 通过 Relay Gateway 接入，内部仍使用与 Direct Runner 相同的 Runner Protocol 语义，与 Direct Runner 共享相同的 ActionHandler 注册机制。Gateway 可以是 embedded 模式（随 server 部署）或 remote 模式（独立部署在 runner 所在网络域 / 中转网络域）。

当 runner 无法直连 server 或需要本地网络域聚合时，runner 仍不直连 server 的 Redis / DB / Asynq。推荐拓扑是 server 部署在控制面网络域，remote Gateway 部署在 runner 所在网络域或中转网络域，runner 只访问本地 Gateway：

```
阿里云 xflow-server
    │  Runner Protocol relay（mTLS / token / lease）
    ▼
腾讯云 xflow-gateway（remote mode）
    │  HTTP Long Poll
    ▼
腾讯云测试环境 xflow-runner
```

这种模式下：
- `xflow-server` 仍是 Execution / Task 的最终状态权威。
- `xflow-gateway` 只做 Runner Protocol 中继和短暂传输缓冲，不直接访问 server 内部 Redis / Asynq。
- `xflow-runner` 使用与 Direct Runner 相同的 `lease / ack / result / heartbeat` 语义。
- 任务必须带有 placement 信息，例如 `cloud=tencent`、`env=test`、`gateway_id=gw-tencent-test`、`capabilities=[xflow.http]`。

```go
type RelayRunner struct {
    transportURL string
    runnerToken  string
    concurrency  int
    handlers     map[string]ActionHandler
    exprEngine   *expression.Engine
    hookRegistry *HookRegistry
    sem          chan struct{} // 并发信号量
}
```

### 3.2 Relay Poll 循环

```go
func (r *RelayRunner) Run(ctx context.Context) error {
    // 启动心跳 goroutine
    go r.heartbeatLoop(ctx)

    // 并发执行 N 个 poll goroutine（每个 goroutine 负责一个并发槽）
    var wg sync.WaitGroup
    for i := 0; i < r.concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            r.pollLoop(ctx)
        }()
    }

    wg.Wait()
    return nil
}

func (r *RelayRunner) pollLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        task, err := r.poll(ctx)
        if err != nil {
            time.Sleep(5 * time.Second) // 网络错误，短暂等待后重试
            continue
        }
        if task == nil {
            continue // 204，立即重新 poll
        }

        r.execute(ctx, task)
    }
}

func (r *RelayRunner) poll(ctx context.Context) (*TaskPayload, error) {
    req, _ := http.NewRequestWithContext(ctx, "GET",
        r.transportURL+"/gateway/tasks/poll?timeout=30s", nil)
    req.Header.Set("Authorization", "Bearer "+r.runnerToken)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusNoContent {
        return nil, nil // 无任务
    }

    var task TaskPayload
    json.NewDecoder(resp.Body).Decode(&task)
    return &task, nil
}

func (r *RelayRunner) execute(ctx context.Context, payload *TaskPayload) {
    handler, ok := r.handlers[payload.NodeType]
    if !ok {
        r.reportFail(ctx, payload, &TaskError{
            Code:    "HANDLER_NOT_FOUND",
            Message: fmt.Sprintf("no handler for node type: %s", payload.NodeType),
        })
        return
    }

    task := buildTask(ctx, payload, r.exprEngine)

    r.hookRegistry.RunBeforeTask(ctx, task)
    output, err := handler.Execute(ctx, task, task.Input)
    r.hookRegistry.RunAfterTask(ctx, task, output, err)

    if err != nil {
        r.reportFail(ctx, payload, toTaskError(err))
        return
    }
    r.reportComplete(ctx, payload, output)
}
```

### 3.3 心跳

```go
func (r *RelayRunner) heartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            w.sendHeartbeat(ctx)
        }
    }
}
```

---

## 4. ActionHandler 接口

所有节点类型通过实现 `ActionHandler` 接口注册，Direct Runner 和 Relay Runner 共用同一套接口定义。

```go
// ActionHandler 任务处理器接口
type ActionHandler interface {
    // 节点描述符（声明端口、元信息，用于编辑器渲染和编译期校验）
    Descriptor() NodeDescriptor

    // 执行任务
    Execute(ctx context.Context, task *Task, input interface{}) (interface{}, error)

    // 任务类型
    Type() TaskType

    // 验证输入
    ValidateInput(input interface{}) error
}

// DynamicOutputHandler 动态输出端口处理器
// 用于 switch 等节点类型，输出端口由用户配置的 parameters 决定
type DynamicOutputHandler interface {
    ActionHandler
    GetOutputPorts(parameters map[string]interface{}) []PortSpec
}

// NodeDescriptor 节点描述符（声明节点元信息和端口）
type NodeDescriptor struct {
    Type        NodeType   `json:"type"`
    DisplayName string     `json:"display_name"`
    Group       string     `json:"group"`
    Description string     `json:"description"`
    Inputs      []PortSpec `json:"inputs"`
    Outputs     []PortSpec `json:"outputs"`
}

// PortSpec 端口定义
type PortSpec struct {
    Name        string `json:"name"`
    DisplayName string `json:"display_name"`
}

// Task 运行时任务上下文（Runner 执行时使用）
type Task struct {
    ID          string
    ExecutionID string
    NodeName    string
    NodeType    TaskType
    Parameters  map[string]interface{} // 表达式已在此处求值完毕
    Input       interface{}
    Context     *ExprContext
    TraceID     string
    Timeout     time.Duration
    Placement   *TaskPlacement
}

// TaskPlacement 描述任务应被路由到哪个执行域。
type TaskPlacement struct {
    Cloud        string
    Region       string
    Env          string
    GatewayID    string
    Capabilities []string
    Tags         []string
}

// ExprContext 表达式求值上下文（Server 序列化后注入 payload）
type ExprContext struct {
    Params  map[string]interface{}            `json:"$params"`
    Input   interface{}                        `json:"$input"`
    Inputs  map[string]interface{}            `json:"$inputs"`
    Nodes   map[string]interface{}            `json:"$nodes"`
    Vars    map[string]interface{}            `json:"$vars"`
    Config  map[string]interface{}            `json:"$config"`
    Workflow map[string]interface{}           `json:"$workflow"`
    Execution map[string]interface{}          `json:"$execution"`
}
```

---

## 5. Plugin Manager

```go
// PluginManager 插件管理器（Direct Runner 和 Relay Runner 均使用）
type PluginManager struct {
    plugins  map[string]Plugin
    handlers map[string]ActionHandler
}

// Plugin 节点任务插件接口
type Plugin interface {
    Name() string
    Type() TaskType
    Init() error
    CreateHandler() ActionHandler
    Shutdown() error
}

// ExecutionHook 执行钩子接口（横切逻辑：审计、监控、权限拦截等）
type ExecutionHook interface {
    BeforeTask(ctx context.Context, task *Task) error
    AfterTask(ctx context.Context, task *Task, output interface{}, err error)
    OnWorkflowStart(ctx context.Context, execution *Execution)
    OnWorkflowEnd(ctx context.Context, execution *Execution)
}

// HookRegistry 钩子注册表（支持多个 Hook 按注册顺序串行调用）
type HookRegistry struct {
    hooks []ExecutionHook
}

func (r *HookRegistry) Register(hook ExecutionHook)
func (r *HookRegistry) RunBeforeTask(ctx context.Context, task *Task) error
func (r *HookRegistry) RunAfterTask(ctx context.Context, task *Task, output interface{}, err error)
```

---

## 6. Retry Engine

Runner 执行重试由 server state + Task Dispatcher 统一管理。Asynq 只负责 server 内部 dispatch 阶段的重试，不直接代表 handler 执行重试；Relay Runner 的网络失败由 Relay Gateway 将任务重新放回 pending，并最终由 server lease 超时兜底。

```go
// RetryEngine 重试引擎（Runner 执行失败策略）
type RetryEngine struct {
    strategy RetryStrategy
}

type RetryStrategy interface {
    NextRetry(attempt int) time.Duration
    ShouldRetry(err error, attempt int) bool
}

// ExponentialBackoff 指数退避
type ExponentialBackoff struct {
    InitialInterval time.Duration
    MaxInterval     time.Duration
    Multiplier      float64
    MaxAttempts     int
}

func (e *ExponentialBackoff) NextRetry(attempt int) time.Duration {
    interval := float64(e.InitialInterval) * math.Pow(e.Multiplier, float64(attempt-1))
    if interval > float64(e.MaxInterval) {
        interval = float64(e.MaxInterval)
    }
    return time.Duration(interval)
}

func (e *ExponentialBackoff) ShouldRetry(err error, attempt int) bool {
    if attempt >= e.MaxAttempts {
        return false
    }
    var taskErr *TaskError
    if errors.As(err, &taskErr) {
        return taskErr.Retryable
    }
    return true
}
```

---

## 7. 配置规范

### 7.1 Direct Runner 配置

```yaml
# config/runner.yaml

runner:
  id: "runner-1"
  concurrency: 50

transport:
  url: "https://xflow-server.example.com:9090"
  protocol: "grpc_stream"          # grpc_stream | websocket | http_long_poll | tcp
  token: "${RUNNER_TOKEN}"         # Runner Token，从环境变量注入
  timeout: 10s
  reconnect:
    max_attempts: 3
    initial_interval: 1s
    max_interval: 10s
    multiplier: 2.0

task_handlers:
  - type: "xflow.http"
    max_concurrency: 20
  - type: "xflow.grpc"
    max_concurrency: 20
  - type: "xflow.function"
    max_concurrency: 50
  - type: "xflow.database"
    max_concurrency: 20

monitor:
  enabled: true
  port: 9091

logging:
  level: "info"
  format: "json"
```

### 7.2 Relay Runner 本地配置

```toml
# ~/.xflow/runner.toml  （xflow-runner register 后自动生成）

[transport]
url   = "https://gateway.tencent-test.example.com:8081"
protocol = "http_long_poll"
token = "WKR-yyyyyy"

[runner]
name         = "tencent-test-runner-01"
scope        = "system"        # 服务端下发，只读
user_id      = ""              # system runner 为空
concurrency  = 8               # 同时执行任务数
capabilities = ["xflow.function", "xflow.http"]
tags         = ["test-env"]

[placement]
cloud      = "tencent"
region     = "ap-guangzhou"
env        = "test"
gateway_id = "gw-tencent-test"

[poll]
timeout = "30s"                # Long Poll 等待时间

[logging]
level  = "info"
format = "text"                # Runner 日志输出到本地终端
```

### 7.3 默认值

```go
const (
    // 默认超时
    DefaultTaskTimeout     = 5 * time.Minute

    // 默认重试（Runner 执行）
    DefaultMaxRetries      = 3
    DefaultRetryInterval   = 5 * time.Second
    DefaultRetryMultiplier = 2.0

    // 默认并发
    DefaultDirectRunnerConcurrency = 50
    DefaultRelayRunnerConcurrency  = 2

    // 队列名称
    DefaultQueue      = "default"
    HighPriorityQueue = "high"
    LowPriorityQueue  = "low"

    // Relay Runner poll
    DefaultPollTimeout    = 30 * time.Second
    DefaultHeartbeatInterval = 30 * time.Second
)
```
