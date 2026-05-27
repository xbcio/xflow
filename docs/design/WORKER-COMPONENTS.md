# XFlow Worker 组件设计

> Worker 负责实际执行工作流节点任务。XFlow 支持两种 Worker 类型：Internal Worker（直连 Redis）和 Edge Worker（通过 Gateway 接入）。两者共享相同的 TaskHandler 接口，执行逻辑完全一致，差异仅在接入方式。

## 目录

1. [Worker 类型概览](#1-worker-类型概览)
2. [Internal Worker](#2-internal-worker)
3. [Edge Worker](#3-edge-worker)
4. [TaskHandler 接口](#4-taskhandler-接口)
5. [Plugin Manager](#5-plugin-manager)
6. [Retry Engine](#6-retry-engine)
7. [配置规范](#7-配置规范)

---

## 1. Worker 类型概览

```
                       ┌─────────────────────────────┐
                       │         Task 执行层          │
                       │                             │
                       │  ┌──────────────────────┐   │
                       │  │  TaskHandler 接口     │   │
                       │  │  （两种 Worker 共用）  │   │
                       │  └──────────┬───────────┘   │
                       └────────────┼────────────────┘
                                    │
               ┌────────────────────┴───────────────────┐
               │                                        │
  ┌────────────▼─────────────┐          ┌───────────────▼──────────┐
  │     Internal Worker      │          │       Edge Worker         │
  │                          │          │                           │
  │  • Asynq Worker          │          │  • HTTP Long Poll         │
  │  • 直连 Redis             │          │  • 通过 Gateway 接入      │
  │  • 平台内网部署           │          │  • 浏览器/桌面/跨 DC      │
  │  • 结果 HTTP 回调 Master  │          │  • 结果 POST Gateway      │
  └──────────────────────────┘          └───────────────────────────┘
```

| | Internal Worker | Edge Worker |
|--|-----------------|-------------|
| **接入方式** | Asynq（直连 Redis） | HTTP Long Poll via Gateway |
| **部署场景** | 平台内网、同 VPC | 浏览器 WASM、用户本机、跨 DC |
| **连接发起方** | Worker 拉取 Redis 任务 | Worker 主动连接 Gateway |
| **结果上报** | HTTP 回调 Master | POST Gateway（Gateway 代理转发） |
| **作用域** | 系统级（所有用户） | 系统级或用户级 |
| **NAT 穿透** | 需要能访问 Redis | 只需能访问 Gateway HTTP 端口 |

---

## 2. Internal Worker

### 2.1 Task Executor（任务执行器）

**职责**：
- 作为 Asynq Worker 订阅 Redis 任务队列
- 反序列化 TaskPayload，构建表达式上下文
- 调用对应 TaskHandler 执行任务
- 执行完成后 HTTP 回调 Master

**表达式求值**：节点参数中的表达式（如 `${{ $input.xxx }}`）在 Worker 侧求值。Master 调度任务时将所需上下文（`$input`、`$vars`、`$config`、`$nodes` 等）序列化到 Asynq task payload，Worker 反序列化后构建 Expr 环境并求值。

```go
type TaskExecutor struct {
    server       *asynq.Server
    handlers     map[string]TaskHandler
    exprEngine   *expression.Engine
    limiter      *ResourceLimiter
    masterClient *MasterClient
    hookRegistry *HookRegistry
}

// MasterClient Worker → Master HTTP 回调客户端
// 回调失败时自动重试（指数退避），重试耗尽后依赖 Master 侧 Reconciler 兜底
type MasterClient struct {
    baseURL     string        // Follower LB 地址，不经过 Leader
    httpClient  *http.Client
    token       string        // 内部调用鉴权 token（从配置注入，不硬编码）
    retryPolicy *RetryPolicy  // 默认 3 次，指数退避
}

func (c *MasterClient) ReportCompleted(ctx context.Context, result *TaskResult) error
func (c *MasterClient) ReportFailed(ctx context.Context, result *TaskResult) error
```

### 2.2 Internal Worker 启动流程

```go
func (w *InternalWorker) Run(ctx context.Context) error {
    // 1. 连接 Redis（Asynq）
    srv := asynq.NewServer(asynq.RedisClientOpt{Addr: w.cfg.Redis.Addr}, asynq.Config{
        Concurrency: w.cfg.Concurrency,
        Queues: map[string]int{
            "high":    10,
            "default": 5,
            "low":     1,
        },
        RetryDelayFunc: w.retryEngine.NextRetry,
    })

    // 2. 注册各类型 TaskHandler
    mux := asynq.NewServeMux()
    for nodeType, handler := range w.handlers {
        mux.Handle(string(nodeType), w.wrapHandler(handler))
    }

    return srv.Run(mux)
}

// wrapHandler 包装 TaskHandler，注入公共逻辑（hook、超时、结果上报）
func (w *InternalWorker) wrapHandler(handler TaskHandler) asynq.HandlerFunc {
    return func(ctx context.Context, t *asynq.Task) error {
        var payload TaskPayload
        json.Unmarshal(t.Payload(), &payload)

        // 构建 Task 上下文
        task := buildTask(ctx, &payload, w.exprEngine)

        // BeforeTask hook
        if err := w.hookRegistry.RunBeforeTask(ctx, task); err != nil {
            return err
        }

        // 执行
        output, err := handler.Execute(ctx, task, task.Input)

        // AfterTask hook
        w.hookRegistry.RunAfterTask(ctx, task, output, err)

        // 上报结果给 Master
        if err != nil {
            return w.masterClient.ReportFailed(ctx, &TaskResult{
                TaskID:      payload.TaskID,
                ExecutionID: payload.ExecutionID,
                Error:       toTaskError(err),
            })
        }
        return w.masterClient.ReportCompleted(ctx, &TaskResult{
            TaskID:      payload.TaskID,
            ExecutionID: payload.ExecutionID,
            Output:      output,
        })
    }
}
```

---

## 3. Edge Worker

### 3.1 概述

Edge Worker 通过 Gateway HTTP API 接入，内部实现 Long Poll 循环，与 Internal Worker 共享相同的 TaskHandler 注册机制。

```go
type EdgeWorker struct {
    gatewayURL   string
    workerToken  string
    concurrency  int
    handlers     map[string]TaskHandler
    exprEngine   *expression.Engine
    hookRegistry *HookRegistry
    sem          chan struct{} // 并发信号量
}
```

### 3.2 Poll 循环

```go
func (w *EdgeWorker) Run(ctx context.Context) error {
    // 启动心跳 goroutine
    go w.heartbeatLoop(ctx)

    // 并发执行 N 个 poll goroutine（每个 goroutine 负责一个并发槽）
    var wg sync.WaitGroup
    for i := 0; i < w.concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            w.pollLoop(ctx)
        }()
    }

    wg.Wait()
    return nil
}

func (w *EdgeWorker) pollLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        task, err := w.poll(ctx)
        if err != nil {
            time.Sleep(5 * time.Second) // 网络错误，短暂等待后重试
            continue
        }
        if task == nil {
            continue // 204，立即重新 poll
        }

        w.execute(ctx, task)
    }
}

func (w *EdgeWorker) poll(ctx context.Context) (*TaskPayload, error) {
    req, _ := http.NewRequestWithContext(ctx, "GET",
        w.gatewayURL+"/gateway/tasks/poll?timeout=30s", nil)
    req.Header.Set("Authorization", "Bearer "+w.workerToken)

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

func (w *EdgeWorker) execute(ctx context.Context, payload *TaskPayload) {
    handler, ok := w.handlers[payload.NodeType]
    if !ok {
        w.reportFail(ctx, payload, &TaskError{
            Code:    "HANDLER_NOT_FOUND",
            Message: fmt.Sprintf("no handler for node type: %s", payload.NodeType),
        })
        return
    }

    task := buildTask(ctx, payload, w.exprEngine)

    w.hookRegistry.RunBeforeTask(ctx, task)
    output, err := handler.Execute(ctx, task, task.Input)
    w.hookRegistry.RunAfterTask(ctx, task, output, err)

    if err != nil {
        w.reportFail(ctx, payload, toTaskError(err))
        return
    }
    w.reportComplete(ctx, payload, output)
}
```

### 3.3 心跳

```go
func (w *EdgeWorker) heartbeatLoop(ctx context.Context) {
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

## 4. TaskHandler 接口

所有节点类型通过实现 `TaskHandler` 接口注册，Internal Worker 和 Edge Worker 共用同一套接口定义。

```go
// TaskHandler 任务处理器接口
type TaskHandler interface {
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
    TaskHandler
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

// Task 运行时任务上下文（Worker 执行时使用）
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
}

// ExprContext 表达式求值上下文（Master 序列化后注入 payload）
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
// PluginManager 插件管理器（Internal Worker 和 Edge Worker 均使用）
type PluginManager struct {
    plugins  map[string]Plugin
    handlers map[string]TaskHandler
}

// Plugin 节点任务插件接口
type Plugin interface {
    Name() string
    Type() TaskType
    Init() error
    CreateHandler() TaskHandler
    Shutdown() error
}

// TriggerPlugin 触发器插件接口（仅 Master 侧使用）
type TriggerPlugin interface {
    Name() string
    Init() error
    CreateTriggerHandler() TriggerHandler
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

Internal Worker 的重试由 Asynq 框架驱动（基于 `asynq.Config.RetryDelayFunc`），Edge Worker 的重试由 Gateway 将任务重新入 pending 队列实现。

```go
// RetryEngine 重试引擎（Internal Worker 使用）
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

### 7.1 Internal Worker 配置

```yaml
# config/worker.yaml

worker:
  id: "worker-1"
  concurrency: 50
  queues:
    - "high"
    - "default"
    - "low"

redis:                              # Asynq 任务队列（必须直连 Redis）
  addr: "localhost:6379"
  password: ""
  db: 0

master:
  url: "http://follower-lb:8080"   # Follower LB 地址（不经过 Leader）
  token: "${MASTER_INTERNAL_TOKEN}" # 内部调用鉴权 token，从环境变量注入
  timeout: 10s
  callback_retry:                   # 回调重试策略（失败后依赖 Reconciler 兜底）
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

### 7.2 Edge Worker 本地配置

```toml
# ~/.xflow/worker.toml  （xflow-worker register 后自动生成）

[gateway]
url   = "https://master.example.com:8081"
token = "WKR-yyyyyy"

[worker]
name         = "my-macbook"
scope        = "user"          # 服务端下发，只读
user_id      = "usr-abc"       # 服务端下发，只读
concurrency  = 2               # 同时执行任务数
capabilities = ["xflow.function", "xflow.http"]
tags         = []

[poll]
timeout = "30s"                # Long Poll 等待时间

[logging]
level  = "info"
format = "text"                # Edge Worker 日志输出到本地终端
```

### 7.3 默认值

```go
const (
    // 默认超时
    DefaultTaskTimeout     = 5 * time.Minute

    // 默认重试（Internal Worker）
    DefaultMaxRetries      = 3
    DefaultRetryInterval   = 5 * time.Second
    DefaultRetryMultiplier = 2.0

    // 默认并发
    DefaultInternalWorkerConcurrency = 50
    DefaultEdgeWorkerConcurrency     = 2

    // 队列名称
    DefaultQueue      = "default"
    HighPriorityQueue = "high"
    LowPriorityQueue  = "low"

    // Edge Worker poll
    DefaultPollTimeout    = 30 * time.Second
    DefaultHeartbeatInterval = 30 * time.Second
)
```
