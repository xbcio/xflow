# XFlow Runner 组件设计

> **Status: 混合。** Direct Runner（`cmd/runner` 经 Runner Protocol 直连 server）已是 MVP 实现；Relay Runner 依赖尚未实现的 Relay Gateway，属于目标设计（planned）。当前实现细节见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md) §3、§7。

> Runner 负责实际执行工作流节点任务。XFlow 支持两种接入方式：Direct Runner 直接连接 server 的 Runner Protocol；Relay Runner 通过 Relay Gateway 中继连接（planned）。两者共享相同的 `ActionHandler` 接口，执行逻辑完全一致，差异仅在网络接入方式。Runner 永远不直连 server 内部 Redis / Asynq。当前代码中的 embedded `execution.Runner` 是本地实现，独立 `xflow-runner` 复用同一执行语义，仅替换连接方式。

## 目录

1. [Runner 类型概览](#1-runner-类型概览)
2. [Direct Runner](#2-direct-runner)
3. [Relay Runner](#3-relay-runner)
4. [ActionHandler 接口](#4-actionhandler-接口)
5. [节点注册机制](#5-节点注册机制)
6. [重试与容错](#6-重试与容错)
7. [配置规范](#7-配置规范)

---

## 1. Runner 类型概览

| | Direct Runner | Relay Runner |
|--|---------------|--------------|
| **接入方式** | Runner Protocol（HTTP 主要传输；gRPC 实验性，非目标形态） | Runner Protocol via Relay Gateway（planned） |
| **部署场景** | runner 能访问 server | 浏览器 WASM、用户本机、跨 DC、跨云测试环境、受限内网 |
| **连接发起方** | Runner 主动连接 server | Runner 主动连接 Gateway，Gateway 再连 server |
| **结果上报** | `POST /v1/runners/result` | Gateway 中继回 server |
| **跨云接入** | 不适用 | Relay Gateway（planned） |

**传输选择**：`RunnerTransportHTTP = "http"`（默认）| `RunnerTransportGRPC = "grpc"`（实验性）。跨云场景走 Relay Gateway（planned），gRPC 不作为目标形态。

---

## 2. Direct Runner

### 2.1 职责

- 连接 server 的 Runner Protocol，注册 runner 能力、标签、并发容量
- 轮询 `POST /v1/runners/poll` 获取 task lease
- 反序列化 TaskPayload，构建表达式上下文，调用对应 `ActionHandler` 执行
- 执行完成后通过 `POST /v1/runners/result` 回报结果
- 在 handler 执行期间续约 lease（`POST /v1/runners/lease/renew`）

### 2.2 ProtocolClient 接口

定义在 `service/runner/runner.go`：

```go
type ProtocolClient interface {
    Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error)
    Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error)
    Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error)
    ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error)
}
```

可选扩展接口（由具体 client 实现）：

- `leaseRenewClient`：`RenewLease` → `POST /v1/runners/lease/renew`
- `activationAckClient`：`ActivationAck` → `POST /v1/runners/activation/ack`
- `MetricsReportClient`：指标上报 → `POST /v1/runners/metrics`（仅 HTTP transport）

### 2.3 Runner Protocol 路径常量

权威来源：`service/protocol/server.go`、`service/protocol/group.go`、`service/protocol/activation.go`、`service/protocol/metrics.go`：

| 常量 | 路径 |
|------|------|
| `RegisterRunnerPath` | `POST /v1/runners/register` |
| `HeartbeatPath` | `POST /v1/runners/heartbeat` |
| `PollTaskPath` | `POST /v1/runners/poll` |
| `ReportResultPath` | `POST /v1/runners/result` |
| `RenewLeasePath` | `POST /v1/runners/lease/renew` |
| `ActivationAckPath` | `POST /v1/runners/activation/ack` |
| `ReportMetricsPath` | `POST /v1/runners/metrics` |

`/gateway/*` 前缀仅在明确讨论 Relay Gateway 时使用，且 Relay Gateway 尚未实现（见 [GATEWAY-COMPONENTS.md](./GATEWAY-COMPONENTS.md)）。

### 2.4 启动流程

```
Register → 建立 session → 启动 heartbeat goroutine（默认 5s）
       → 启动 N 个 worker goroutine
         └── pollLoop: POST /v1/runners/poll
               └── 拿到 lease → executeAndReport
                     ├── execution.Runner.Execute → ActionHandler.Execute
                     └── POST /v1/runners/result
```

重连退避：min 2s，max 30s（`sdk/xflow/runner.go`）。

---

## 3. Relay Runner

### 3.1 概述（planned）

Relay Runner 通过 Relay Gateway 接入，内部使用与 Direct Runner 相同的 Runner Protocol 语义。Relay Gateway 可以是 embedded 模式（随 server 部署）或 remote 模式（独立部署在 runner 所在网络域）。

推荐拓扑：

```
阿里云 xflow-server
    │  Runner Protocol relay（mTLS / token）
    ▼
腾讯云 xflow-gateway（remote mode，planned）
    │  HTTP Long Poll
    ▼
腾讯云测试环境 xflow-runner
```

语义不变：server 仍是 Execution / Task 的最终状态权威；Gateway 只做协议中继，不直连 Redis / Asynq；runner 的 lease / heartbeat / result 语义与 Direct Runner 一致。任务需携带 placement 信息（`cloud`、`region`、`env`、`gateway_id`、`capabilities`）供调度器路由。

---

## 4. ActionHandler 接口

核心接口定义在 `types/node.go`：

```go
// ActionHandler 所有 action 节点处理器必须实现的接口。
// 实现必须无状态、并发安全。
type ActionHandler interface {
    Handler  // 包含 Descriptor() Descriptor
    Execute(ctx context.Context, input *Input) (*Output, error)
}
```

**可挂起节点**（可选接口，`types/suspend.go`）：

```go
// SuspendingHandler 由支持挂起/恢复的处理器实现，独立于 ActionHandler。
type SuspendingHandler interface {
    PrepareSuspend(ctx context.Context, input *Input) (*SuspendSpec, error)
    OnResume(ctx context.Context, input *Input, signal *SignalPayload) (*Output, error)
}
```

挂起节点同时实现 `ActionHandler` 和 `SuspendingHandler`；引擎在 `Execute` 前检测该接口，决定走挂起路径还是普通执行路径。

**Input / Output 结构**（`types/node.go`）：

```go
type Input struct {
    Params      map[string]any  // 表达式已求值的节点参数
    Data        map[string]any  // 主输入端口数据（$input）
    Inputs      map[string]any  // 多端口输入（$inputs）
    Vars        map[string]any  // workflow 变量（$vars）
    Config      map[string]any  // workflow 配置（$config）
    Runtime     *Runtime        // 执行期上下文（$runtime）
    Nodes       map[string]any  // 依赖节点输出（$nodes）
    ExecutionID string
    NodeName    string
    TraceID     string
    SpanID      string
}
```

---

## 5. 节点注册机制

xflow 的扩展机制是全局注册表，**没有 PluginManager 或 HookRegistry**。

**进程级全局注册**（`node/registry/registry.go`）：

```go
// Register 将 handler 注册到全局注册表；type 字段不能为空。
// 同 type 多版本均可注册；最大版本号作为默认 handler。
func Register(h types.ActionHandler)

// RegisterTrigger 注册 trigger handler（可同时实现 ActionHandler）。
func RegisterTrigger(h types.TriggerHandler)

// Lookup / LookupVersion 按 type / (type, version) 查找。
func Lookup(nodeType string) (types.ActionHandler, bool)
func LookupVersion(nodeType string, version int) (types.ActionHandler, bool)
```

**执行期注册表**（`execution/registry.go`）：

```go
// Registry 管理 embedded execution 的 handler 查找，支持：
// - RegisterGlobal(nodeType, handler)：process-wide handler
// - RegisterNodeHandler(nodeName, handler)：execution 内单节点 override
// - RegisterExecutionHandler(id, nodeName, handler)：execution 级动态 handler
// - Get(id, nodeName, nodeType, version)：按优先级解析
type Registry struct { ... }
```

handler 在 `init()` 或进程启动时调用 `node.Register`，server 端通过 `execution.Registry.RegisterGlobal` / `RegisterNodeHandler` 按优先级解析；embedded runner 直接使用同一套注册表。

---

## 6. 重试与容错

**重试由 server 侧引擎负责，runner 侧无独立 RetryEngine。**

- 执行失败时，runner 通过 `POST /v1/runners/result` 上报错误，server engine（`engine/commit.go`：`tryRetryWithAttempt`）判断是否重试。
- 重试策略（`RetrySettings.MaxAttempts`）在 workflow 定义中声明，由 server 管理。
- at-least-once 语义由 server 侧 lease 超时 + durable outbox 兜底：lease 超时未续约时 server 自动重新分发任务。
- `Permanent` 类型错误（`types.NewPermanentError`）不触发重试，`Transient` 错误可重试。
- runner 连接层的重连（transport failure）由 `sdk/xflow/runner.go` 的 `runWithReconnect` 循环处理，退避 2s–30s，与 handler 执行重试无关。

---

## 7. 配置规范

### 7.1 Runner 配置字段（`sdk/xflow/RunnerConfig`）

| 字段 | 说明 | 默认值 |
|------|------|--------|
| `ServerURL` | server HTTP origin（必填） | — |
| `Transport` | `"http"`（默认）或 `"grpc"` | `"http"` |
| `GRPCTarget` | gRPC transport 时必填 | — |
| `RunnerID` | runner 标识，空则 server 生成 | — |
| `Concurrency` | 并发 lease 数，0 使用 runner service 默认 | 1 |
| `Capabilities` | 声明可处理的 node type 列表 | — |
| `Labels` | 用于 RunnerSelector 路由匹配 | — |
| `Namespaces` | 限制服务的 namespace，空不限制 | — |
| `Token` | runner bearer token | — |
| `HeartbeatInterval` | 心跳间隔，0 使用默认 | 5s |
| `PollWait` | 无任务时 poll 等待，0 使用默认 | 1s |
| `ReportMetrics` | 是否向 server 上报 Prometheus 指标 | false |

### 7.2 关键默认常量

```go
// engine/engine.go
const DefaultNodeTimeout = 30 * time.Minute  // 节点未设置 timeout 时的默认上限

// service/runner/runner.go
const defaultRunnerShutdownTimeout = 10 * time.Second
const defaultReportTimeout         = 15 * time.Second
// HeartbeatInterval 默认 5s（config.HeartbeatInterval <= 0 时）

// sdk/xflow/runner.go
const reconnectMinBackoff = 2 * time.Second
const reconnectMaxBackoff = 30 * time.Second
```

### 7.3 Direct Runner 配置示例

```yaml
runner:
  id: "runner-1"
  concurrency: 50

transport:
  url: "https://xflow-server.example.com"
  protocol: "http"              # http（默认）| grpc（实验性）
  token: "${RUNNER_TOKEN}"

capabilities:
  - "xflow.http"
  - "xflow.function"
  - "xflow.database"

monitor:
  enabled: true
  port: 9091
```
