# XFlow Gateway 组件设计

> Gateway 是 Master 内嵌的 Edge Worker 接入层，随 Master 默认启动，允许无法直连 Redis 的 Worker（浏览器 WASM、用户本机、跨数据中心）通过 HTTP Long Poll 接入任务系统。

## 目录

1. [架构定位](#1-架构定位)
2. [Worker 类型](#2-worker-类型)
3. [注册流程](#3-注册流程)
4. [核心数据结构](#4-核心数据结构)
5. [Gateway HTTP API](#5-gateway-http-api)
6. [任务路由与分发](#6-任务路由与分发)
7. [Redis 数据结构](#7-redis-数据结构)
8. [Worker 断线恢复](#8-worker-断线恢复)
9. [Worker 注册表](#9-worker-注册表)
10. [配置规范](#10-配置规范)

---

## 1. 架构定位

```
                    ┌─────────────────────────────────────────────┐
                    │              Master Process                  │
                    │                                             │
                    │  ┌─────────────────┐  ┌──────────────────┐ │
                    │  │  API Server     │  │    Gateway       │ │
                    │  │  :8080          │  │    :8081         │ │
                    │  └────────┬────────┘  └────────┬─────────┘ │
                    │           │  共享                │           │
                    │  ┌────────┴──────────────────────┴───────┐  │
                    │  │  WorkflowEngine / Scheduler / StateManager │
                    │  └───────────────────┬───────────────────┘  │
                    └──────────────────────┼──────────────────────┘
                                           │
                                  ┌────────┴────────┐
                                  │  Asynq Queue    │
                                  │  (Redis)        │
                                  └────────┬────────┘
                        ┌──────────────────┼───────────────────────┐
                        │                  │                        │
             ┌──────────▼──────────┐  ┌───▼──────────────────────────┐
             │  Internal Worker     │  │  Gateway (Asynq Worker 身份)  │
             │  （内网，直连 Redis） │  │  代理 Edge Worker 任务        │
             │  现有逻辑，不变      │  └──────────┬───────────────────┘
             └─────────────────────┘             │ HTTP Long Poll
                                      ┌──────────┼──────────────┐
                                      │          │              │
                               ┌──────▼──┐ ┌─────▼───┐ ┌───────▼──┐
                               │Browser  │ │桌面 App  │ │ 跨 DC    │
                               │  WASM   │ │(NAT 后)  │ │ Worker   │
                               └─────────┘ └─────────┘ └──────────┘
```

**高可用部署拓扑**（推荐 K8s 环境）：

```
Edge Worker
    │  HTTPS :8081（单一 LB 地址）
    ▼
Load Balancer（K8s Service / Nginx / HAProxy）
    │  会话亲和（hash by worker_id）
    ├──▶ Master-1 Gateway :8081
    ├──▶ Master-2 Gateway :8081
    └──▶ Master-N Gateway :8081
```

会话亲和保证同一 Worker 优先路由到同一 Gateway 实例（减少 inflight 状态分散），Gateway 崩溃时 LB 自动切流到其他实例。

**关键设计决策**：
- Gateway 与 Master **同进程启动**，共享 Redis 连接和 Asynq Client，无跨进程通信开销
- Asynq 队列**保持不变**，Gateway 以普通 Asynq Worker 身份消费任务，再转发给 Edge Worker
- Internal Worker（内网直连 Redis）**无需改动**，两种 Worker 并存
- Edge Worker 通过 **LB 单一入口**连接，Master 实例增减对 Edge Worker 透明

---

## 2. Worker 类型

### 2.1 Internal Worker（内网 Worker）

直接连接 Redis，作为 Asynq Worker 消费任务队列，适用于平台内网部署。

| 属性 | 说明 |
|------|------|
| 连接方式 | 直连 Redis（Asynq） |
| 部署场景 | 平台内网、同 VPC |
| 管理方 | 平台运维 |
| 任务范围 | 所有用户工作流（系统级） |
| 结果上报 | HTTP 回调 Master |

### 2.2 Edge Worker（外部 Worker）

通过 Gateway HTTP API 接入，不依赖 Redis 直连，适用于浏览器 WASM、用户本机、跨数据中心等场景。Edge Worker 分为两个作用域：

| | System Edge Worker | User Edge Worker |
|--|-------------------|-----------------|
| **注册者** | 管理员 | 普通用户 |
| **任务范围** | 所有用户的工作流 | 仅注册者自己的工作流 |
| **注册令牌** | 管理员后台生成 | 用户个人设置页生成 |
| **连接方式** | HTTP Long Poll via Gateway | HTTP Long Poll via Gateway |
| **典型场景** | 平台补充执行能力、跨 DC Worker | 用户本机跑私有数据、访问内网服务 |
| **类比** | GitLab Shared Runner | GitLab Specific Runner |

**安全边界**：User Edge Worker 服务端强制过滤任务归属，只能接收本用户工作流产生的任务，无法访问其他用户数据。

---

## 3. 注册流程

与 GitLab Runner 相同的两阶段 Token 设计：

```
管理员后台生成「注册令牌」（Registration Token）
  └─ scope=system，可限制 max_uses、expires_at

用户个人设置页生成「注册令牌」（Registration Token）
  └─ scope=user，自动绑定当前 user_id

                          ↓

Edge Worker 启动，调用 POST /gateway/workers/register
  └─ 消耗 Registration Token
  └─ 服务端创建 RegisteredWorker 记录，scope 由 Token 决定，不可伪造

                          ↓

服务端返回 Worker Token（长期有效）
  └─ 写入本地配置文件 ~/.xflow/worker.toml

                          ↓

Worker 后续所有请求（poll / complete / fail / heartbeat）使用 Worker Token
```

### 3.1 CLI 操作

```bash
# 管理员：注册系统级 Edge Worker
xflow-worker register \
  --url https://master.example.com \
  --token SYS-REG-xxxxxxxx \
  --name "dc2-worker-01" \
  --tags production,dc2 \
  --capabilities xflow.http,xflow.function,xflow.grpc

# 普通用户：在自己电脑上注册用户级 Edge Worker
xflow-worker register \
  --url https://master.example.com \
  --token USR-REG-xxxxxxxx \
  --name "my-macbook" \
  --capabilities xflow.function,xflow.http

# 其他命令
xflow-worker run        # 读取 worker.toml，开始 poll
xflow-worker verify     # 验证 token 有效性，检查 gateway 连通性
xflow-worker unregister # 注销当前 Worker
```

### 3.2 本地配置文件

```toml
# ~/.xflow/worker.toml  （注册后自动生成，后续 run 读取此文件）

[gateway]
url   = "https://xflow.example.com:8081"  # LB 统一入口，单一地址
token = "WKR-yyyyyy"

[worker]
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
// WorkerScope Worker 作用域
type WorkerScope string

const (
    WorkerScopeSystem WorkerScope = "system" // 全局共享，管理员管理
    WorkerScopeUser   WorkerScope = "user"   // 用户私有，只处理该用户任务
)

// WorkerStatus Worker 在线状态
type WorkerStatus string

const (
    WorkerStatusOnline  WorkerStatus = "online"
    WorkerStatusOffline WorkerStatus = "offline"
)

// RegisteredWorker 已注册的 Edge Worker（持久化到 DB）
type RegisteredWorker struct {
    ID            string       `db:"id"`             // UUID
    Name          string       `db:"name"`           // 友好名称，如 "my-macbook"
    TokenHash     string       `db:"token_hash"`     // Worker Token 的 bcrypt hash，不存明文
    Scope         WorkerScope  `db:"scope"`
    UserID        string       `db:"user_id"`        // system: 空；user: 所有者 ID
    Capabilities  []string     `db:"capabilities"`   // 支持的节点类型
    Tags          []string     `db:"tags"`           // 路由标签
    Description   string       `db:"description"`
    RunnerVersion string       `db:"runner_version"`
    Status        WorkerStatus `db:"status"`
    LastSeenAt    time.Time    `db:"last_seen_at"`
    RegisteredAt  time.Time    `db:"registered_at"`
}

// RegistrationToken 注册令牌（一次性或有限次数）
type RegistrationToken struct {
    Token       string      `db:"token"`       // 明文存储，仅用于注册，注册后可销毁
    Scope       WorkerScope `db:"scope"`
    UserID      string      `db:"user_id"`     // system token: 空
    MaxUses     int         `db:"max_uses"`    // 0 = 无限次
    UsedCount   int         `db:"used_count"`
    ExpiresAt   time.Time   `db:"expires_at"`
    CreatedBy   string      `db:"created_by"`
    Description string      `db:"description"` // 如 "用于生产环境 DC2 Worker"
}

// ConnectedWorker 当前在线的 Edge Worker（内存中，非持久化）
type ConnectedWorker struct {
    WorkerID     string
    Scope        WorkerScope
    UserID       string
    Capabilities []string
    Tags         []string
    // 当前领取但未完成的任务 ID 列表（用于断线恢复）
    InflightTasks map[string]time.Time // taskID → assignedAt
    LastHeartbeat time.Time
    mu            sync.Mutex
}

// TaskPayload Gateway 下发给 Edge Worker 的任务载荷（与 Internal Worker 的 Asynq payload 结构相同）
type TaskPayload struct {
    TaskID      string                 `json:"task_id"`
    ExecutionID string                 `json:"execution_id"`
    WorkflowID  string                 `json:"workflow_id"`
    UserID      string                 `json:"user_id"`
    NodeName    string                 `json:"node_name"`
    NodeType    string                 `json:"node_type"`
    Parameters  map[string]interface{} `json:"parameters"`
    // 表达式求值所需上下文（Master 调度时序列化注入）
    Context     *ExprContext           `json:"context"`
    TraceID     string                 `json:"trace_id"`
    Timeout     time.Duration          `json:"timeout"`
}

// TaskResult Edge Worker 上报的任务结果
type TaskResult struct {
    TaskID      string      `json:"task_id"`
    ExecutionID string      `json:"execution_id"`
    Output      interface{} `json:"output,omitempty"`
    Error       *TaskError  `json:"error,omitempty"`
}
```

---

## 5. Gateway HTTP API

所有接口以 `/gateway` 为前缀，监听独立端口（默认 `:8081`）。

### 5.1 Worker 注册与管理

```
POST   /gateway/workers/register     注册 Edge Worker（消耗 Registration Token）
GET    /gateway/workers/me           查看当前 Worker 信息
DELETE /gateway/workers/me           注销当前 Worker
POST   /gateway/workers/heartbeat    心跳保活
```

**注册请求/响应**：

```json
// POST /gateway/workers/register
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
  "worker_id": "wkr-uuid-xxxx",
  "worker_token": "WKR-yyyyyy",
  "scope": "user",
  "user_id": "usr-abc",
  "gateway_url": "https://master.example.com:8081"
}
```

**心跳**（Edge Worker 每 30s 发送一次）：

```json
// POST /gateway/workers/heartbeat
// Header: Authorization: Bearer WKR-yyyyyy
// Request
{
  "current_tasks": 2,
  "capabilities": ["xflow.http", "xflow.function"]
}
// Response 200
{ "ok": true }
```

### 5.2 任务接口

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

Response 204（无任务，超时后返回，Edge Worker 应立即重新 poll）
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

---

## 6. 任务路由与分发

### 6.1 Asynq → Gateway 的桥接

Gateway 以 Asynq Worker 身份注册，拦截发往 Edge Worker 的任务类型。Handler 挂起等待 Edge Worker 来领取任务，收到结果后再向 Master 回调：

```go
// Gateway 启动时注册 Asynq Handler
func (gw *Gateway) registerAsynqHandlers(mux *asynq.ServeMux) {
    // 拦截所有需要 Edge Worker 处理的任务类型
    // Internal Worker 处理它们自己订阅的队列，互不干扰
    mux.HandleFunc("*", gw.handleTask)
}

func (gw *Gateway) handleTask(ctx context.Context, t *asynq.Task) error {
    var payload TaskPayload
    if err := json.Unmarshal(t.Payload(), &payload); err != nil {
        return err
    }

    // 1. 写入 pending 分桶（按 scope + user_id + node_type）
    gw.queue.Push(ctx, &payload)

    // 2. 阻塞等待 Edge Worker 领取并返回结果（BLPOP）
    result, err := gw.queue.WaitResult(ctx, payload.TaskID, gw.cfg.MaxWaitTime)
    if err != nil {
        // 超时，让 Asynq 按重试策略重试
        return fmt.Errorf("no edge worker available: %w", asynq.SkipRetry)
    }

    // 3. 转发结果给 Master 回调接口
    return gw.reportToMaster(ctx, &payload, result)
}
```

### 6.2 Edge Worker Poll 时的路由

```go
func (gw *Gateway) PollTask(w http.ResponseWriter, r *http.Request) {
    worker := gw.auth.VerifyWorkerToken(r)
    if worker == nil {
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    }

    timeout := parseDuration(r.URL.Query().Get("timeout"), 30*time.Second)
    ctx, cancel := context.WithTimeout(r.Context(), timeout)
    defer cancel()

    var task *TaskPayload

    switch worker.Scope {
    case WorkerScopeSystem:
        // System Worker 可领取任意用户的任务（user 无人认领时兜底）
        task = gw.queue.PopAny(ctx, worker.Capabilities, worker.Tags)

    case WorkerScopeUser:
        // User Worker 只能领取自己 user_id 下的任务（服务端强制过滤）
        task = gw.queue.PopForUser(ctx, worker.UserID, worker.Capabilities, worker.Tags)
    }

    if task == nil {
        w.WriteHeader(http.StatusNoContent) // 204
        return
    }

    // 标记任务被哪个 Worker 领走（断线恢复用）
    gw.registry.AssignTask(worker.WorkerID, task.TaskID)

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(task)
}
```

### 6.3 路由优先级

```
Asynq 任务到达 Gateway
    │
    ├─ 1. 优先：该 user_id 下是否有在线 User Worker？
    │       且 capabilities ⊇ {node_type}，tags 匹配？
    │       └─ 是 → 放入 user 分桶，等待该 User Worker poll
    │
    └─ 2. 兜底：分发给 System Worker（capability 匹配）
               └─ 无可用 Worker → 任务挂起，等待 Asynq 重试
```

---

## 7. Redis 数据结构

Gateway 使用 Redis 存储 pending/inflight 任务状态，与 Asynq 数据结构互不干扰（使用独立 key 前缀）。

| Key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `xflow:gw:task:{task_id}` | String | TaskPayload JSON | 15min |
| `xflow:gw:pending:sys:{node_type}` | List | task_id 列表 | 无 |
| `xflow:gw:pending:usr:{user_id}:{node_type}` | List | task_id 列表 | 无 |
| `xflow:gw:inflight:{task_id}` | String | worker_id | 10min（续期） |
| `xflow:gw:result:{task_id}` | List | TaskResult JSON | 1min |

**Pending 分桶**（Edge Worker BLPOP 时按自己的 scope 和 user_id 选择对应的 key）：

```go
// System Worker poll keys（按能力顺序，优先高优先级）
pendingKeys = [
    "xflow:gw:pending:sys:xflow.http",
    "xflow:gw:pending:sys:xflow.function",
    ...
]

// User Worker poll keys（只看自己 user_id 的分桶）
pendingKeys = [
    "xflow:gw:pending:usr:usr-abc:xflow.http",
    "xflow:gw:pending:usr:usr-abc:xflow.function",
    ...
]
```

**任务流转**：

```
Asynq 下发
    │
    ├─ gw:task:{task_id} ← TaskPayload（TTL 15min）
    └─ gw:pending:{scope}:{...}:{node_type} ← LPUSH task_id
              │
    Edge Worker BLPOP（poll）
              │
              ├─ gw:inflight:{task_id} ← worker_id（TTL 10min，执行期间续期）
              │
    Edge Worker POST /complete 或 /fail
              │
              └─ gw:result:{task_id} ← LPUSH result
                         │
    Gateway Handler BLPOP 被唤醒 → 上报 Master → Asynq Handler 返回
```

---

## 8. Worker 断线恢复

### 8.1 Gateway 内部协调器（GatewayReconciler）

Edge Worker 在执行中途断线（浏览器关闭、网络中断、进程崩溃），Gateway 识别并重新调度孤儿任务。

```go
// GatewayReconciler Gateway 内部协调器（独立 goroutine 运行）
type GatewayReconciler struct {
    registry *WorkerRegistry
    queue    *TaskQueue
    redis    *redis.Client
    interval time.Duration // 默认 30s
}

func (r *GatewayReconciler) reconcile(ctx context.Context) {
    // 扫描所有 inflight 任务
    keys, _ := r.redis.Keys(ctx, "xflow:gw:inflight:*").Result()

    for _, key := range keys {
        taskID := strings.TrimPrefix(key, "xflow:gw:inflight:")
        workerID, _ := r.redis.Get(ctx, key).Result()

        if r.registry.IsAlive(workerID) {
            // Worker 在线，续期 inflight key
            r.redis.Expire(ctx, key, 10*time.Minute)
            continue
        }

        // Worker 已断线，重新入队
        r.redis.Del(ctx, key)
        payload := r.queue.LoadPayload(ctx, taskID)
        if payload != nil {
            r.queue.Push(ctx, payload) // 重新放回 pending 分桶
        }
    }
}
```

**心跳检测**：Edge Worker 每 30s 发送心跳，Gateway 更新 `ConnectedWorker.LastHeartbeat`。GatewayReconciler 检测超过 `heartbeat_timeout`（默认 60s）无心跳的 Worker 视为断线，触发任务重新入队。

### 8.2 Gateway 高可用与 Edge Worker 故障转移

**Gateway 崩溃时的处理流程**：

```
Gateway（Follower-N）崩溃
    │
    ├─ LB 健康检查失败 → 自动将新连接路由到其他 Gateway 实例
    │
    ├─ inflight 任务：xflow:gw:inflight:{task_id} TTL 10min 到期
    │       └─ 其他 Gateway 实例的 GatewayReconciler 扫描到孤儿任务 → 重新入队
    │
    └─ Edge Worker Long Poll 断开
            └─ 客户端指数退避重连 LB（1s→2s→4s→...→30s 封顶）
                    └─ LB 路由到存活的 Gateway 实例
                            └─ Worker 重新 poll，自动恢复工作
```

**Edge Worker 客户端重连策略**：

```
断线检测：HTTP 请求返回错误 / 连接超时
重连等待：1s → 2s → 4s → 8s → 16s → 30s（封顶，无限重试）
重连后验证：POST /gateway/workers/heartbeat
  - 200 OK → 正常 poll
  - 401 Unauthorized → 提示用户重新注册（Token 失效）
  - 503 Service Unavailable → 继续重试
```

### 8.3 Gateway 优雅下线

```go
func (gw *Gateway) Shutdown(ctx context.Context) {
    // 1. 停止接受新的 poll 请求（返回 503）
    gw.httpServer.Shutdown(ctx)

    // 2. 广播在线 Worker：Gateway 即将下线，请重连
    //    Edge Worker 收到广播后立即触发重连逻辑，不等 30s 超时
    gw.registry.BroadcastShutdown()
    // Broadcast payload: {"type":"shutdown","reconnect_after_ms":1000}

    // 3. 等待所有 inflight 任务完成或超时（30s）
    //    超时后依赖其他实例的 GatewayReconciler 重新入队
    gw.waitInflight(30 * time.Second)
}
```

---

## 9. Worker 注册表

```go
// WorkerRegistry 在线 Edge Worker 注册表（内存，非持久化）
type WorkerRegistry struct {
    workers map[string]*ConnectedWorker // workerID → ConnectedWorker
    mu      sync.RWMutex
}

func (r *WorkerRegistry) Register(worker *ConnectedWorker)
func (r *WorkerRegistry) Unregister(workerID string)
func (r *WorkerRegistry) UpdateHeartbeat(workerID string)
func (r *WorkerRegistry) IsAlive(workerID string) bool
func (r *WorkerRegistry) AssignTask(workerID, taskID string)
func (r *WorkerRegistry) ReleaseTask(workerID, taskID string)

// OnlineWorkers 查询满足条件的在线 Worker 列表
func (r *WorkerRegistry) OnlineWorkers(scope WorkerScope, userID string, nodeType string) []*ConnectedWorker

// Stats 统计（用于监控）
func (r *WorkerRegistry) Stats() WorkerStats

type WorkerStats struct {
    TotalOnline   int
    SystemWorkers int
    UserWorkers   int
    InflightTasks int
}
```

---

## 10. 配置规范

Gateway 配置内嵌在 `config/master.yaml` 中：

```yaml
gateway:
  enabled: true            # 默认 true，随 Master 启动
  port: 8081               # 独立监听端口

  auth:
    heartbeat_timeout: 60s   # 超过此时间无心跳视为 offline
    token_expiry: 0          # Worker Token 有效期（0 = 永不过期）

  poll:
    max_wait: 30s            # Long Poll 最长等待时间
    max_tasks_per_poll: 1    # 每次 poll 返回任务数（固定为 1，保证顺序性）

  dispatch:
    task_ttl: 15m            # 任务在 pending 队列的最长等待时间，超时后 Asynq 重试
    inflight_ttl: 10m        # inflight 标记 TTL，期间 Worker 应定期续期

  reconciler:
    enabled: true
    interval: 30s            # 扫描孤儿任务的间隔

  rate_limit:
    register_per_ip: 10      # 每 IP 每分钟最大注册次数（防暴力注册）
    poll_per_worker: 120     # 每 Worker 每分钟最大 poll 次数
```

### 内嵌于 Master 启动流程

```go
func (m *Master) Run(ctx context.Context) error {
    // ... 现有启动逻辑 ...

    // Gateway 随 Master 一同启动（共享 Redis、DB 连接）
    if m.cfg.Gateway.Enabled {
        gw := gateway.New(gateway.Config{
            Port:      m.cfg.Gateway.Port,
            Redis:     m.redis,
            DB:        m.db,
            Scheduler: m.scheduler,
            Master:    m.masterClient, // 用于转发回调
        })
        go gw.Start(ctx)
    }

    // ... 其余启动逻辑 ...
}
```

### 优雅下线

```go
func (gw *Gateway) Shutdown(ctx context.Context) {
    // 1. 停止接收新的 poll 请求
    gw.httpServer.Shutdown(ctx)

    // 2. 广播在线 Worker：Gateway 即将下线，请重新连接其他实例
    gw.registry.BroadcastShutdown()

    // 3. 等待所有 inflight 任务完成或超时
    //    超时后依赖 GatewayReconciler（在其他实例上）重新入队
    gw.waitInflight(30 * time.Second)
}
```
