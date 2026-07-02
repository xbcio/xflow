# XFlow Relay Gateway 组件设计

> **Status: 目标设计（非当前实现）。** Relay Gateway 作为独立进程尚未实现；当前 runner 必须直连 server，见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md) §5、§7。

> Relay Gateway 是 Runner Protocol 的可选中继层。默认路径是 runner 直接连接 server 暴露的 Runner Protocol；当 runner 无法直连 server、需要网络域延伸或本地连接聚合时，才部署 Relay Gateway。Gateway 不执行 handler，不直连 server 内部 Redis / DB / Asynq，也不成为 Execution / Task 的最终状态源。

## 目录

1. [架构定位](#1-架构定位)
2. [Runner 接入类型](#2-runner-接入类型)
3. [注册流程](#3-注册流程)
4. [核心数据结构](#4-核心数据结构)
5. [Gateway HTTP API](#5-gateway-http-api)
6. [任务路由与分发](#6-任务路由与分发)
7. [Redis 数据结构](#7-redis-数据结构)
8. [Runner 断线恢复](#8-runner-断线恢复)
9. [Runner 注册表](#9-runner-注册表)
10. [配置规范](#10-配置规范)

---

## 1. 架构定位

Gateway 的核心职责是中继 Runner Protocol，而不是直接消费 server 内部队列。Asynq 是 server 内部调度队列；Task Dispatcher 在 server 内部消费 Asynq，并通过 Runner Protocol 给 runner 下发 lease。Gateway 只在网络需要时中继 Runner Protocol。

| 模式 | 部署位置 | server 连接方式 | runner 连接方式 | 状态边界 | 适用场景 |
|---|---|---|---|---|---|
| `direct` | 无 Gateway，runner 直接连 server | Runner Protocol | Runner Protocol | 状态全部在 server | server 与 runner 网络可达，推荐主路径 |
| `embedded` | Server 进程内 | 进程内调用 Task Dispatcher / Runner Protocol | HTTP Long Poll / WebSocket / gRPC stream | 状态全部在 server；本地队列只是进程内等待队列 | 浏览器、本机、同 VPC Runner 接入 |
| `remote` | 独立进程，部署在 runner 所在网络域 / 中转网络域 | Runner Protocol relay（mTLS/token） | HTTP Long Poll / WebSocket / gRPC stream | 本地 pending/inflight 只是传输缓冲；最终状态在 server | server 与 runner 网络不通时接入 |

### 1.1 Direct Runner Protocol（推荐主路径）

```
┌──────────────────────────────────────────────┐
│ xflow-server                                 │
│ - Engine / Scheduler                         │
│ - Asynq / Redis（server 内部）                │
│ - Task Dispatcher                          │
│ - Runner Protocol                           │
└──────────────────────┬───────────────────────┘
                       │ TCP / gRPC stream / WebSocket / HTTP long poll
                       ▼
┌──────────────────────────────────────────────┐
│ xflow-runner                                 │
│ - 注册 capabilities / tags / env             │
│ - 接收 lease，执行 handler                    │
│ - 回报 result / heartbeat                    │
└──────────────────────────────────────────────┘
```

**关键设计决策**：
- runner 直接连接 server 暴露的 Runner Protocol。
- runner 不连接 Redis / Asynq，不感知 server 内部队列和 payload 结构。
- server 通过 Task Dispatcher 把 Asynq task 转成 runner lease。
- Relay Gateway 不是默认路径，只在网络需要时出现。

### 1.2 Embedded Gateway（Server 内嵌）

```
                    ┌─────────────────────────────────────────────┐
                    │              Server Process                  │
                    │                                             │
                    │  ┌─────────────────┐  ┌──────────────────┐ │
                    │  │  API Server     │  │    Gateway       │ │
                    │  │  :8080          │  │    :8081         │ │
                    │  └────────┬────────┘  └────────┬─────────┘ │
                    │           │  共享                │           │
                    │  ┌────────┴──────────────────────┴───────┐  │
                    │  │ WorkflowEngine / Task Dispatcher / State │
                    │  └───────────────────┬───────────────────┘  │
                    └──────────────────────┼──────────────────────┘
                                           │
                                  ┌────────┴────────┐
                                  │  Asynq Queue    │
                                  │  (Redis)        │
                                  └────────┬────────┘
                                           │ consume by Task Dispatcher
                                           ▼
                                  ┌──────────────────┐
                                  │ Runner Protocol │
                                  │ / Gateway API    │
                                  └────────┬─────────┘
                                           │ HTTP Long Poll / WebSocket
                                      ┌──────────┼──────────────┐
                                      │          │              │
                               ┌──────▼──┐ ┌─────▼───┐ ┌───────▼──┐
                               │Browser  │ │桌面 App  │ │ 跨 DC    │
                               │  WASM   │ │(NAT 后)  │ │ Runner   │
                               └─────────┘ └─────────┘ └──────────┘
```

**高可用部署拓扑**（推荐 K8s 环境）：

```
Runner
    │  HTTPS :8081（单一 LB 地址）
    ▼
Load Balancer（K8s Service / Nginx / HAProxy）
    │  会话亲和（hash by runner_id）
    ├──▶ Server-1 Gateway :8081
    ├──▶ Server-2 Gateway :8081
    └──▶ Server-N Gateway :8081
```

会话亲和保证同一 runner 优先路由到同一 Gateway 实例（减少 inflight 状态分散），Gateway 崩溃时 LB 自动切流到其他实例。

**embedded 模式关键设计决策**：
- Gateway 与 server **同进程启动**，复用 server 内部 Task Dispatcher / Runner Protocol，无跨进程通信开销。
- Asynq 队列**保持 server 内部实现**；Gateway 不作为独立 Asynq worker 暴露给 runner。
- Runner 通过 **LB 单一入口**连接，server 实例增减对 runner 透明。

### 1.3 Remote Relay Gateway（网络隔离独立部署）

remote 模式用于 runner 无法直连 server、不能互相直连或需要本地聚合的场景。典型部署可以是跨云：阿里云部署完整 `xflow-server`，腾讯云部署 `xflow-gateway`，腾讯云测试环境部署 `xflow-runner`。但核心抽象不是云厂商，而是网络域隔离：runner 只访问本地 Gateway，Gateway 与 server 建立受控 Runner Protocol relay。

```
阿里云
┌──────────────────────────────────────────────┐
│ xflow-server                                 │
│ - API / 编译 / 调度 / 状态权威                │
│ - Redis / DB / Asynq（内部）                  │
│ - Task Dispatcher / Runner Protocol        │
└──────────────────────┬───────────────────────┘
                       │ mTLS / token / stream
腾讯云                  ▼
┌──────────────────────────────────────────────┐
│ xflow-gateway（relay mode）                   │
│ - 不直连 Redis / DB / Asynq                   │
│ - 中继 Runner Protocol                       │
│ - 本地短暂 pending / inflight 缓冲             │
│ - 回传 runner 执行结果                         │
└──────────────────────┬───────────────────────┘
                       │ HTTP Long Poll
腾讯云测试环境          ▼
┌──────────────────────────────────────────────┐
│ xflow-runner                                 │
│ - 注册 capability / env / tags                │
│ - 执行测试环境内的节点 handler                 │
│ - 只连接腾讯云本地 gateway                    │
└──────────────────────────────────────────────┘
```

**remote 模式关键设计决策**：
- remote Gateway **不得直连 server 的 Redis / DB / Asynq**。Redis 和 Asynq 是 server 内部实现细节，暴露到隔离网络域会放大安全、延迟、连接抖动和故障恢复问题。
- server 仍是唯一状态权威。Gateway 的 pending / inflight 只是传输层缓冲，断线后可由 server 基于 lease 超时重新分配。
- Gateway 与 server 之间中继 Runner Protocol 的 lease / ack / nack / heartbeat 语义。所有请求必须幂等，至少包含 `gateway_id`、`lease_id`、`task_id`、`execution_id`。
- 任务路由依赖 placement 元数据，例如 `cloud`、`region`、`env`、`gateway_id`、`capabilities`、`tags`、`user_scope`。
- runner 可使用 gRPC stream、WebSocket、TCP framed protocol 或 HTTP long poll；网络隔离带来的复杂性集中在 Gateway 与 server 之间。

---

## 2. Runner 接入类型

### 2.1 Direct Runner（推荐）

直接连接 server 的 Runner Protocol，适用于 server 与 runner 网络可达的部署。Direct Runner 不连接 Redis / Asynq，也不通过 Gateway。

| 属性 | 说明 |
|------|------|
| 连接方式 | Runner Protocol（TCP / gRPC stream / WebSocket / HTTP long poll） |
| 部署场景 | 平台内网、同 VPC、跨网络但 server 可达 |
| 管理方 | 平台运维 |
| 任务范围 | 所有用户工作流（系统级） |
| 结果上报 | Runner Protocol result |

### 2.2 Relay Runner（经 Relay Gateway）

通过 Relay Gateway 接入 Runner Protocol，不依赖 Redis 直连，适用于浏览器 WASM、用户本机、跨数据中心、跨云测试环境、受限内网环境等场景。Relay Runner 分为两个作用域：

| | System Runner | User Runner |
|--|-------------------|-----------------|
| **注册者** | 管理员 | 普通用户 |
| **任务范围** | 所有用户的工作流 | 仅注册者自己的工作流 |
| **注册令牌** | 管理员后台生成 | 用户个人设置页生成 |
| **连接方式** | Runner Protocol via Gateway | Runner Protocol via Gateway |
| **典型场景** | 平台补充执行能力、跨 DC Runner、网络隔离测试 Runner | 用户本机跑私有数据、访问内网服务 |
| **类比** | GitLab Shared Runner | GitLab Specific Runner |

**安全边界**：User Runner 由 server 强制过滤任务归属，只能接收本用户工作流产生的任务，无法访问其他用户数据。Gateway 只能中继，不负责最终授权决策。

---

## 3. 注册流程

与 GitLab Runner 相同的两阶段 Token 设计：

```
管理员后台生成「注册令牌」（Registration Token）
  └─ scope=system，可限制 max_uses、expires_at

用户个人设置页生成「注册令牌」（Registration Token）
  └─ scope=user，自动绑定当前 user_id

                          ↓

Runner 启动，调用 POST /gateway/runners/register 或直接连接 server 的 Runner Protocol register
  └─ 消耗 Registration Token
  └─ 服务端创建 RegisteredRunner 记录，scope 由 Token 决定，不可伪造

                          ↓

服务端返回 Runner Token（长期有效）
  └─ 写入本地配置文件 ~/.xflow/runner.toml

                          ↓

Runner 后续所有请求（poll / complete / fail / heartbeat 或 stream message）使用 Runner Token
```

### 3.1 CLI 操作

```bash
# 管理员：注册系统级 Runner
xflow-runner register \
  --url https://gateway.tencent-test.example.com \
  --token SYS-REG-xxxxxxxx \
  --name "dc2-runner-01" \
  --tags production,dc2 \
  --capabilities xflow.http,xflow.function,xflow.grpc

# 普通用户：在自己电脑上注册用户级 Runner
xflow-runner register \
  --url https://gateway.example.com \
  --token USR-REG-xxxxxxxx \
  --name "my-macbook" \
  --capabilities xflow.function,xflow.http

# 其他命令
xflow-runner run        # 读取 runner.toml，连接 server 或 gateway
xflow-runner verify     # 验证 token 有效性，检查 gateway 连通性
xflow-runner unregister # 注销当前 Runner
```

### 3.2 本地配置文件

```toml
# ~/.xflow/runner.toml  （注册后自动生成，后续 run 读取此文件）

[transport]
url   = "https://xflow.example.com:9090"  # server Runner Protocol；经 gateway 时填 gateway 地址
token = "WKR-yyyyyy"

[runner]
name         = "my-macbook"
scope        = "user"          # 服务端下发，只读
user_id      = "usr-abc"       # 服务端下发，只读
concurrency  = 2               # 同时执行任务数
capabilities = ["xflow.function", "xflow.http"]
tags         = []

[reconnect]
initial_interval = "1s"        # 首次断线重连等待
max_interval     = "30s"       # 最大重连间隔（指数退避封顶）
multiplier       = 2.0         # 退避乘数：1s → 2s → 4s → ... → 30s
```

---

## 4. 核心数据结构

```go
// RunnerScope Runner 作用域
type RunnerScope string

const (
    RunnerScopeSystem RunnerScope = "system" // 全局共享，管理员管理
    RunnerScopeUser   RunnerScope = "user"   // 用户私有，只处理该用户任务
)

// RunnerStatus Runner 在线状态
type RunnerStatus string

const (
    RunnerStatusOnline  RunnerStatus = "online"
    RunnerStatusOffline RunnerStatus = "offline"
)

// GatewayStatus remote Gateway 在线状态
type GatewayStatus string

const (
    GatewayStatusOnline  GatewayStatus = "online"
    GatewayStatusOffline GatewayStatus = "offline"
)

// RegisteredRunner 已注册的 Runner（持久化到 DB）
type RegisteredRunner struct {
    ID            string       `db:"id"`             // UUID
    Name          string       `db:"name"`           // 友好名称，如 "my-macbook"
    TokenHash     string       `db:"token_hash"`     // Runner Token 的 bcrypt hash，不存明文
    Scope         RunnerScope  `db:"scope"`
    UserID        string       `db:"user_id"`        // system: 空；user: 所有者 ID
    Capabilities  []string     `db:"capabilities"`   // 支持的节点类型
    Tags          []string     `db:"tags"`           // 路由标签
    Description   string       `db:"description"`
    RunnerVersion string       `db:"runner_version"`
    Status        RunnerStatus `db:"status"`
    LastSeenAt    time.Time    `db:"last_seen_at"`
    RegisteredAt  time.Time    `db:"registered_at"`
}

// RegisteredGateway 已注册的 remote gateway（持久化到 server DB）
type RegisteredGateway struct {
    ID           string       `db:"id"`
    Name         string       `db:"name"`
    TokenHash    string       `db:"token_hash"`
    Cloud        string       `db:"cloud"`        // 如 aliyun / tencent
    Region       string       `db:"region"`       // 如 cn-shenzhen
    Env          string       `db:"env"`          // 如 test / prod
    Tags         []string     `db:"tags"`
    Status       GatewayStatus `db:"status"`
    LastSeenAt   time.Time    `db:"last_seen_at"`
    RegisteredAt time.Time    `db:"registered_at"`
}

// RegistrationToken 注册令牌（一次性或有限次数）
type RegistrationToken struct {
    Token       string      `db:"token"`       // 明文存储，仅用于注册，注册后可销毁
    Scope       RunnerScope `db:"scope"`
    UserID      string      `db:"user_id"`     // system token: 空
    MaxUses     int         `db:"max_uses"`    // 0 = 无限次
    UsedCount   int         `db:"used_count"`
    ExpiresAt   time.Time   `db:"expires_at"`
    CreatedBy   string      `db:"created_by"`
    Description string      `db:"description"` // 如 "用于生产环境 DC2 Runner"
}

// ConnectedRunner 当前在线的 Runner（内存中，非持久化）
type ConnectedRunner struct {
    RunnerID     string
    Scope        RunnerScope
    UserID       string
    Capabilities []string
    Tags         []string
    // 当前领取但未完成的任务 ID 列表（用于断线恢复）
    InflightTasks map[string]time.Time // taskID → assignedAt
    LastHeartbeat time.Time
    mu            sync.Mutex
}

// TaskPayload Relay Gateway 下发给 Runner 的任务载荷（由 server Task Dispatcher 生成）
type TaskPayload struct {
    TaskID      string                 `json:"task_id"`
    ExecutionID string                 `json:"execution_id"`
    WorkflowID  string                 `json:"workflow_id"`
    UserID      string                 `json:"user_id"`
    NodeName    string                 `json:"node_name"`
    NodeType    string                 `json:"node_type"`
    Parameters  map[string]interface{} `json:"parameters"`
    // 表达式求值所需上下文（Server 调度时序列化注入）
    Context     *ExprContext           `json:"context"`
    TraceID     string                 `json:"trace_id"`
    Timeout     time.Duration          `json:"timeout"`
    Placement   *TaskPlacement         `json:"placement,omitempty"`
}

// TaskPlacement 描述任务应被路由到哪个执行域。
type TaskPlacement struct {
    Cloud        string   `json:"cloud,omitempty"`
    Region       string   `json:"region,omitempty"`
    Env          string   `json:"env,omitempty"`
    GatewayID    string   `json:"gateway_id,omitempty"`
    Capabilities []string `json:"capabilities,omitempty"`
    Tags         []string `json:"tags,omitempty"`
}

// TaskResult Runner 上报的任务结果
type TaskResult struct {
    TaskID      string      `json:"task_id"`
    ExecutionID string      `json:"execution_id"`
    Output      interface{} `json:"output,omitempty"`
    Error       *TaskError  `json:"error,omitempty"`
}
```

---

## 5. Relay Gateway API

Gateway 暴露两组 API：runner-facing API 给 runner 使用；server-facing API 只在 remote relay 模式下启用，用于 Gateway 与 server 中继 Runner Protocol。Direct Runner 不经过本章 API，而是直接连接 server 的 Runner Protocol。

### 5.1 Runner-facing API（relay 模式）

HTTP long poll relay 的 runner-facing 接口以 `/gateway` 为前缀，监听独立端口（默认 `:8081`）。如果 relay 选择 gRPC stream / WebSocket，则消息语义保持一致。

#### 5.1.1 Runner 注册与管理

```
POST   /gateway/runners/register     注册 Runner（消耗 Registration Token）
GET    /gateway/runners/me           查看当前 Runner 信息
DELETE /gateway/runners/me           注销当前 Runner
POST   /gateway/runners/heartbeat    心跳保活
```

**注册请求/响应**：

```json
// POST /gateway/runners/register
// Request
{
  "registration_token": "REG-abc123",
  "name": "my-macbook",
  "capabilities": ["xflow.http", "xflow.function"],
  "tags": ["personal", "mac"],
  "description": "本地开发机",
  "runner_version": "0.1.0"
}

// Response 201
{
  "runner_id": "wkr-uuid-xxxx",
  "runner_token": "WKR-yyyyyy",
  "scope": "user",
  "user_id": "usr-abc",
  "transport_url": "https://gateway.tencent-test.example.com:8081"
}
```

**心跳**（Runner 每 30s 发送一次）：

```json
// POST /gateway/runners/heartbeat
// Header: Authorization: Bearer WKR-yyyyyy
// Request
{
  "current_tasks": 2,
  "capabilities": ["xflow.http", "xflow.function"]
}
// Response 200
{ "ok": true }
```

#### 5.1.2 任务接口

```
GET  /gateway/tasks/poll              拉取任务（Long Poll）
POST /gateway/tasks/:task_id/complete 上报成功
POST /gateway/tasks/:task_id/fail     上报失败
```

**拉取任务**：

```
GET /gateway/tasks/poll?timeout=30s
Header: Authorization: Bearer WKR-yyyyyy

Response 200（有任务）：
{
  "task_id": "exec-123:node-abc",
  "execution_id": "exec-123",
  "node_name": "call_api",
  "node_type": "xflow.http",
  "parameters": { ... },
  "context": { "$vars": {...}, "$config": {...}, "$nodes": {...}, "$input": {...} },
  "trace_id": "traceXXX",
  "timeout": 30000000000
}

Response 204（无任务，超时后返回，Runner 应立即重新 poll）
```

**上报结果**：

```json
// POST /gateway/tasks/exec-123:node-abc/complete
// Header: Authorization: Bearer WKR-yyyyyy
{
  "execution_id": "exec-123",
  "output": { "status": "ok", "data": { ... } }
}

// POST /gateway/tasks/exec-123:node-abc/fail
// Header: Authorization: Bearer WKR-yyyyyy
{
  "execution_id": "exec-123",
  "error": {
    "code": "HTTP_ERROR",
    "message": "upstream returned 500",
    "retryable": true
  }
}
```

### 5.2 Server-facing API（remote relay 模式）

remote Gateway 与 server 之间推荐使用 gRPC bidirectional stream；如果先落 HTTP，实现也必须保留 Runner Protocol 的 lease / ack / result / cancel 语义。

```
POST /gateways/register              注册 remote Relay Gateway
POST /gateways/heartbeat             上报存活、容量、runner 统计
POST /gateways/tasks/lease           中继 server 侧 runner lease
POST /gateways/tasks/:task_id/ack    确认任务已交付并完成
POST /gateways/tasks/:task_id/nack   拒绝或放弃任务，server 可重派
POST /gateways/tasks/:task_id/result 回传 runner 执行结果
```

**lease 请求/响应**：

```json
// POST /gateways/tasks/lease
// Header: Authorization: Bearer GW-yyyyyy
// Request
{
  "gateway_id": "gw-tencent-test",
  "capacity": 20,
  "capabilities": ["xflow.http", "xflow.function"],
  "placement": {
    "cloud": "tencent",
    "region": "ap-guangzhou",
    "env": "test",
    "tags": ["test-env"]
  }
}

// Response 200
{
  "lease_id": "lease-abc",
  "expires_in": "60s",
  "tasks": [
    {
      "task_id": "exec-123:node-abc",
      "execution_id": "exec-123",
      "node_name": "call_test_api",
      "node_type": "xflow.http",
      "parameters": {},
      "placement": {
        "cloud": "tencent",
        "env": "test",
        "gateway_id": "gw-tencent-test"
      }
    }
  ]
}
```

**server 侧重派规则**：`lease_id` 到期前未收到 `ack` 或 `result`，server 将任务视为未交付，可重新 lease 给同一 runner、同一 Gateway 或其他匹配 runner/Gateway。Gateway 重试 `result` 时必须携带相同 `lease_id`、`attempt`、`fencing_token` 和 `task_id`，server 以幂等方式处理。

---

## 6. 任务路由与分发

### 6.1 Server 内部：Asynq → Task Dispatcher

Gateway 不直接消费 Asynq。server 内部的 Task Dispatcher 作为 Asynq worker 消费任务，先持久化 runner 执行状态，再通过 Runner Protocol 下发 lease。

```go
func (d *TaskDispatcher) handleAsynqTask(ctx context.Context, t *asynq.Task) error {
    var payload TaskPayload
    if err := json.Unmarshal(t.Payload(), &payload); err != nil {
        return err
    }

    // 1. 持久化为 server state，成功后才能 ack Asynq
    lease, err := d.state.CreateRunnerLease(ctx, payload)
    if err != nil {
        return err // 让 Asynq 重试 server 内部 dispatch
    }

    // 2. 通过 Runner Protocol 下发；若暂无 runner，保留在 server pending
    err = d.transport.Assign(ctx, lease)
    if err != nil {
        return d.state.MarkRunnerPending(ctx, lease.ID)
    }

    // 3. Asynq 只负责 dispatch intent；runner 执行结果异步回到 server
    return nil
}
```

### 6.2 Relay 模式：Runner Protocol → Gateway pending

remote Gateway 不消费 Asynq，也不访问 Redis。它中继 server 的 Runner Protocol，把拿到的 lease 写入本地短暂 pending 队列，等待 runner poll 或 stream 接收。

```go
func (gw *RemoteGateway) relayLoop(ctx context.Context) {
    ticker := time.NewTicker(gw.cfg.LeaseInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            capacity := gw.localQueue.AvailableCapacity()
            if capacity <= 0 {
                continue
            }
            tasks, leaseID, err := gw.server.LeaseRunnerTasks(ctx, LeaseRequest{
                GatewayID:    gw.cfg.GatewayID,
                Capacity:     capacity,
                Capabilities: gw.registry.Capabilities(),
                Placement: TaskPlacement{
                    Cloud:  gw.cfg.Cloud,
                    Region: gw.cfg.Region,
                    Env:    gw.cfg.Env,
                    Tags:   gw.cfg.Tags,
                },
            })
            if err != nil {
                gw.backoff.Wait(ctx)
                continue
            }
            gw.localQueue.PushLease(leaseID, tasks)
        }
    }
}
```

### 6.3 Runner Poll 时的路由

```go
func (gw *Gateway) PollTask(w http.ResponseWriter, r *http.Request) {
    runner := gw.auth.VerifyRunnerToken(r)
    if runner == nil {
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    }

    timeout := parseDuration(r.URL.Query().Get("timeout"), 30*time.Second)
    ctx, cancel := context.WithTimeout(r.Context(), timeout)
    defer cancel()

    var task *TaskPayload

    switch runner.Scope {
    case RunnerScopeSystem:
        // System Runner 可领取任意用户的任务（user 无人认领时兜底）
        task = gw.queue.PopAny(ctx, runner.Capabilities, runner.Tags)

    case RunnerScopeUser:
        // User Runner 只能领取自己 user_id 下的任务（服务端强制过滤）
        task = gw.queue.PopForUser(ctx, runner.UserID, runner.Capabilities, runner.Tags)
    }

    if task == nil {
        w.WriteHeader(http.StatusNoContent) // 204
        return
    }

    // 标记任务被哪个 Runner 领走（断线恢复用）
    gw.registry.AssignTask(runner.RunnerID, task.TaskID)

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(task)
}
```

### 6.4 路由优先级

```
Runner lease 到达 Gateway
    │
    ├─ 1. 优先：该 user_id 下是否有在线 User Runner？
    │       且 capabilities ⊇ {node_type}，tags 匹配？
    │       └─ 是 → 放入 user 分桶，等待该 User Runner poll
    │
    └─ 2. 兜底：分发给 System Runner（capability 匹配）
               └─ 无可用 Runner → 任务保持 pending，等待 server lease 超时或 gateway 重试
```

---

## 7. Relay 缓冲数据结构

Relay Gateway 可以使用进程内内存、嵌入式 KV 或 relay 所在网络域的本地 Redis 存储 pending/inflight 传输缓冲。它不得使用 server 内部 Redis，也不得依赖 Asynq 数据结构。这些状态不能作为最终状态源；server state 仍是 Execution / Task 的唯一权威。

| Key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `xflow:gw:task:{task_id}` | String | TaskPayload JSON | 15min |
| `xflow:gw:pending:sys:{node_type}` | List | task_id 列表 | 无 |
| `xflow:gw:pending:usr:{user_id}:{node_type}` | List | task_id 列表 | 无 |
| `xflow:gw:inflight:{task_id}` | String | runner_id | 10min（续期） |
| `xflow:gw:result:{task_id}` | List | TaskResult JSON | 1min |

**Pending 分桶**（Runner poll 时按自己的 scope 和 user_id 选择对应的 key）：

```go
// System Runner poll keys（按能力顺序，优先高优先级）
pendingKeys = [
    "xflow:gw:pending:sys:xflow.http",
    "xflow:gw:pending:sys:xflow.function",
    ...
]

// User Runner poll keys（只看自己 user_id 的分桶）
pendingKeys = [
    "xflow:gw:pending:usr:usr-abc:xflow.http",
    "xflow:gw:pending:usr:usr-abc:xflow.function",
    ...
]
```

**任务流转**：

```
server Task Dispatcher 创建 lease
    │
    ├─ gw:task:{task_id} ← TaskPayload（TTL 15min）
    └─ gw:pending:{scope}:{...}:{node_type} ← LPUSH task_id
              │
    Runner BLPOP（poll）
              │
              ├─ gw:inflight:{task_id} ← runner_id（TTL 10min，执行期间续期）
              │
    Runner POST /complete 或 /fail
              │
              └─ gw:result:{task_id} ← LPUSH result
                         │
    Gateway relay 上报 server → server 校验 lease/attempt/fencing_token → 推进状态
```

remote Gateway 的本地缓冲至少需要表达以下状态：

| 状态 | 内容 | 恢复策略 |
|---|---|---|
| `pending` | 已从 server Task Dispatcher lease、尚未被 runner 领取的 TaskPayload | Gateway 重启后可丢弃，由 server lease 超时重派 |
| `inflight` | 已交付给 runner、尚未收到 result 的 task_id / runner_id / lease_id | Gateway 断线或 TTL 超时后 nack 给 server；无法 nack 时由 server lease 超时兜底 |
| `result_retry` | runner 已返回但尚未成功回传 server 的 TaskResult | 按 `task_id + lease_id` 幂等重试 |

---

## 8. Runner 断线恢复

### 8.1 Gateway 内部协调器（GatewayReconciler）

Runner 在执行中途断线（浏览器关闭、网络中断、进程崩溃），Gateway 识别并重新调度孤儿任务。

```go
// GatewayReconciler Gateway 内部协调器（独立 goroutine 运行）
type GatewayReconciler struct {
    registry *RunnerRegistry
    queue    *TaskQueue
    redis    *redis.Client
    interval time.Duration // 默认 30s
}

func (r *GatewayReconciler) reconcile(ctx context.Context) {
    // 扫描所有 inflight 任务
    keys, _ := r.redis.Keys(ctx, "xflow:gw:inflight:*").Result()

    for _, key := range keys {
        taskID := strings.TrimPrefix(key, "xflow:gw:inflight:")
        runnerID, _ := r.redis.Get(ctx, key).Result()

        if r.registry.IsAlive(runnerID) {
            // Runner 在线，续期 inflight key
            r.redis.Expire(ctx, key, 10*time.Minute)
            continue
        }

        // Runner 已断线，重新入队
        r.redis.Del(ctx, key)
        payload := r.queue.LoadPayload(ctx, taskID)
        if payload != nil {
            r.queue.Push(ctx, payload) // 重新放回 pending 分桶
        }
    }
}
```

**心跳检测**：Runner 每 30s 发送心跳，Gateway 更新 `ConnectedRunner.LastHeartbeat`。GatewayReconciler 检测超过 `heartbeat_timeout`（默认 60s）无心跳的 runner 视为断线，触发任务重新入队。remote Gateway 还必须向 server 发送 Gateway heartbeat，包含当前容量、在线 runner 数、pending 数和 inflight 数。

### 8.2 Gateway 高可用与 Runner 故障转移

**Gateway 崩溃时的处理流程**：

```
Gateway（Follower-N）崩溃
    │
    ├─ LB 健康检查失败 → 自动将新连接路由到其他 Gateway 实例
    │
    ├─ inflight 任务：xflow:gw:inflight:{task_id} TTL 10min 到期
    │       └─ 其他 Gateway 实例的 GatewayReconciler 扫描到孤儿任务 → 重新入队
    │
    └─ Runner Long Poll 断开
            └─ 客户端指数退避重连 LB（1s→2s→4s→...→30s 封顶）
                    └─ LB 路由到存活的 Gateway 实例
                            └─ Runner 重新 poll，自动恢复工作
```

**Runner 客户端重连策略**：

```
断线检测：HTTP 请求返回错误 / 连接超时
重连等待：1s → 2s → 4s → 8s → 16s → 30s（封顶，无限重试）
重连后验证：POST /gateway/runners/heartbeat
  - 200 OK → 正常 poll
  - 401 Unauthorized → 提示用户重新注册（Token 失效）
  - 503 Service Unavailable → 继续重试
```

### 8.3 Gateway 优雅下线

```go
func (gw *Gateway) Shutdown(ctx context.Context) {
    // 1. 停止接受新的 poll 请求（返回 503）
    gw.httpServer.Shutdown(ctx)

    // 2. 广播在线 Runner：Gateway 即将下线，请重连
    //    Runner 收到广播后立即触发重连逻辑，不等 30s 超时
    gw.registry.BroadcastShutdown()
    // Broadcast payload: {"type":"shutdown","reconnect_after_ms":1000}

    // 3. 等待所有 inflight 任务完成或超时（30s）
    //    超时后依赖其他实例的 GatewayReconciler 重新入队
    gw.waitInflight(30 * time.Second)
}
```

---

## 9. Runner 注册表

```go
// RunnerRegistry 在线 Runner 注册表（内存，非持久化）
type RunnerRegistry struct {
    runners map[string]*ConnectedRunner // runnerID → ConnectedRunner
    mu      sync.RWMutex
}

func (r *RunnerRegistry) Register(runner *ConnectedRunner)
func (r *RunnerRegistry) Unregister(runnerID string)
func (r *RunnerRegistry) UpdateHeartbeat(runnerID string)
func (r *RunnerRegistry) IsAlive(runnerID string) bool
func (r *RunnerRegistry) AssignTask(runnerID, taskID string)
func (r *RunnerRegistry) ReleaseTask(runnerID, taskID string)

// OnlineRunners 查询满足条件的在线 Runner 列表
func (r *RunnerRegistry) OnlineRunners(scope RunnerScope, userID string, nodeType string) []*ConnectedRunner

// Stats 统计（用于监控）
func (r *RunnerRegistry) Stats() RunnerStats

type RunnerStats struct {
    TotalOnline   int
    SystemRunners int
    UserRunners   int
    InflightTasks int
}
```

---

## 10. 配置规范

### 10.1 Embedded Relay Gateway 配置

Gateway 配置内嵌在 `config/server.yaml` 中：

```yaml
gateway:
  enabled: true            # 默认 true，随 server 启动
  port: 8081               # 独立监听端口

  auth:
    heartbeat_timeout: 60s   # 超过此时间无心跳视为 offline
    token_expiry: 0          # Runner Token 有效期（0 = 永不过期）

  poll:
    max_wait: 30s            # Long Poll 最长等待时间
    max_tasks_per_poll: 1    # 每次 poll 返回任务数（固定为 1，保证顺序性）

  dispatch:
    task_ttl: 15m            # 任务在 pending 队列的最长等待时间，超时后由 server lease 重派
    inflight_ttl: 10m        # inflight 标记 TTL，期间 Runner 应定期续期

  reconciler:
    enabled: true
    interval: 30s            # 扫描孤儿任务的间隔

  rate_limit:
    register_per_ip: 10      # 每 IP 每分钟最大注册次数（防暴力注册）
    poll_per_runner: 120     # 每 Runner 每分钟最大 poll 次数
```

### 10.2 Remote Relay Gateway 配置

remote Gateway 使用独立配置文件，例如 `config/gateway.yaml`：

```yaml
gateway:
  mode: remote
  id: "gw-tencent-test"
  name: "Tencent Cloud Test Relay"
  listen: "0.0.0.0:8081"

placement:
  cloud: "tencent"
  region: "ap-guangzhou"
  env: "test"
  tags:
    - "test-env"

server:
  url: "https://xflow-server.example.com"
  token: "${XFLOW_GATEWAY_TOKEN}"
  tls:
    enabled: true
    ca_file: "/etc/xflow/ca.pem"
    cert_file: "/etc/xflow/gateway.pem"
    key_file: "/etc/xflow/gateway-key.pem"

lease:
  interval: 1s
  batch_size: 20
  ttl: 60s
  retry:
    initial_interval: 1s
    max_interval: 30s
    multiplier: 2.0

runner:
  heartbeat_timeout: 60s
  poll_max_wait: 30s
  max_inflight: 100

buffer:
  type: memory              # memory | redis | badger
  redis_addr: ""            # 仅 type=redis 时需要，必须是远端云本地 Redis
```

### 10.3 内嵌于 server 启动流程

```go
func (s *Server) Run(ctx context.Context) error {
    // ... 现有启动逻辑 ...

    // Relay Gateway 随 server 一同启动（复用 Runner Protocol）
    if s.cfg.Gateway.Enabled {
        gw := gateway.New(gateway.Config{
            Port:       s.cfg.Gateway.Port,
            Protocol:   s.runnerProtocol,
            Dispatcher: s.taskDispatcher,
        })
        go gw.Start(ctx)
    }

    // ... 其余启动逻辑 ...
}
```

### 10.4 优雅下线

```go
func (gw *Gateway) Shutdown(ctx context.Context) {
    // 1. 停止接收新的 poll 请求
    gw.httpServer.Shutdown(ctx)

    // 2. 广播在线 Runner：Gateway 即将下线，请重新连接其他实例
    gw.registry.BroadcastShutdown()

    // 3. 等待所有 inflight 任务完成或超时
    //    超时后依赖 GatewayReconciler（在其他实例上）重新入队
    gw.waitInflight(30 * time.Second)
}
```
