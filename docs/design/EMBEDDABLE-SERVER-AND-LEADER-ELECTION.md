# 可嵌入 Server + Leader Election 抽象设计

Status: 已批准（2026-07-02）

## 背景

现状评估（见对话记录，未落盘为单独文档）：

- `cmd/server/main.go` 的 `runServer(cfg serverConfig)` 已经是纯组合逻辑——调用
  `memory.New` / `asynq.New`、`control.NewServer`、`control.NewDispatcher`、
  `control.NewGRPCServer` 等构造函数，没有全局状态，理论上具备被嵌入的条件。
  但今天没有导出的"嵌入友好"入口：`serverConfig` 是包私有的，也没有聚合的
  `Shutdown(ctx)` 生命周期方法。嵌入者只能照抄一遍 `runServer` 的组装代码。
- `docs/design/CORE-COMPONENTS.md` 的目标架构图标注了
  `XFlow Server Control Plane (Raft)`，但当前代码完全没有 Raft：没有 leader
  election、没有 log replication。`cmd/server` 是无状态 HTTP+gRPC 服务，状态权威
  在 Redis（`StateStore` + `TaskQueue`）。
- `LeaseSweeper` 等 leader-only 任务（以及规划中的 `GlobalReconciler` /
  `Archiver`，见 `CORE-COMPONENTS.md` 架构图）如果多副本同时跑，不是正确性
  问题（幂等），但会重复扫描浪费资源。多副本部署时需要一种方式选出"谁来跑
  这一轮"。

结论：不需要现在引入 Raft——Redis 已经是单点状态权威，副本之间不存在"必须
选出 leader 才能正确工作"的硬需求。真正 leader-only 的任务用 Redis
lease-based election 就够。但要把选举逻辑做成可替换的接口，未来若架构演进到
需要真正的强一致共识（例如去掉 Redis 强依赖），能新增一个 Raft 实现而不改
调用方。

## 目标

1. 让 xflow 的控制面（server）可以被嵌入到宿主 Go 程序里，而不是只能作为独立
   CLI 二进制运行。
2. 给 leader-only 的后台任务（当前是 `LeaseSweeper`，未来可能是
   `GlobalReconciler` / `Archiver`）引入 leader election 抽象，现在用 Redis
   lease 实现，接口层面为未来的 Raft 实现留口子。
3. 所有新增能力必须通过 `sdk/xflow` 的用户门面暴露，用户不直接接触
   `service/control` 包的类型。`service/control` 内部组件（`ControlPlane`、
   `LeaderElector`）是核心库内部的可复用组件，供 `cmd/server` 和
   `sdk/xflow` 门面共同复用。

## 非目标

- 不实现 Raft。只做 `LeaderElector` 接口 + Redis 实现，接口本身不引入
  `hashicorp/raft` 或任何 Raft 相关依赖、占位代码。
- 不支持同一进程内多个 `ControlPlane` / `xflow.Server` 实例的隔离（lease
  key 不做实例前缀）。同一进程只跑一个 server 实例是当前唯一支持的场景。
- 不改变现有 Runner Protocol、Task Dispatcher、认证机制的行为，只重新组织
  它们的生命周期管理入口。

## 架构

```
用户代码
   │  xflow.NewServer(cfg, opts...)
   ▼
sdk/xflow.Server                     ← 用户门面，唯一暴露给用户的类型
   │  内部持有一个 *control.ControlPlane
   ▼
service/control.ControlPlane         ← 核心库内部可复用组件
   ├─ backend.Provider (memory/asynq)
   ├─ control.NewServer / NewDispatcher / NewGRPCServer（现有逻辑，不变）
   └─ control.LeaderElector           ← 新增抽象
        ├─ RedisLeaderElector（现在实现）
        └─ （未来）RaftLeaderElector

cmd/server/main.go                   ← 改为调用 xflow.NewServer，与用户走同一条路径
```

## 组件

### 1. `service/control.ControlPlane`（核心库内部）

聚合现有 `runServer` 里散落的组装逻辑，提供 Handler + 生命周期分层（类似
`net/http` 的 Handler vs Server）：

```go
// service/control/controlplane.go

type Config struct {
    Backend  backend.Provider  // memory.New(...) 或 asynq.New(...) 的结果
    Auth     Authenticator     // 可选，默认 DisabledAuthenticator
    Logger   engine.Logger
    Metrics  *metrics.Metrics
    PollWait time.Duration
}

func NewControlPlane(cfg Config) (*ControlPlane, error)

func (cp *ControlPlane) Handler() http.Handler
func (cp *ControlPlane) GRPCServer() runnerpb.RunnerProtocolServer
func (cp *ControlPlane) Start(ctx context.Context) error
func (cp *ControlPlane) Shutdown(ctx context.Context) error
```

`NewControlPlane` 内部完成今天 `runServer` 里从组装 backend 到
`control.NewServer(...).Handler()` 的全部逻辑；`Start` 负责启动 backend
consumer 和 `LeaseSweeper`（选举后的条件执行，见下）；`Shutdown` 负责按 LIFO
停止 sweeper、backend、（若启用）主动 `Resign` leader。

`cmd/server/main.go` 改为解析 CLI flag 得到 `Config`，交给 `NewControlPlane`，
不再自己维护 `stop`/`sweeperStop`/`grpcStop` 三个局部变量。

### 2. `service/control.LeaderElector`（核心库内部）

```go
// service/control/leader.go

type LeaderElector interface {
    // Campaign blocks until this instance becomes leader or ctx is cancelled.
    Campaign(ctx context.Context) error
    // IsLeader reports current leadership without blocking.
    IsLeader() bool
    // Resign releases leadership voluntarily (e.g. graceful shutdown).
    Resign(ctx context.Context) error
    // Notify returns a channel that emits on every leadership change (true=acquired, false=lost).
    Notify() <-chan bool
}
```

- `RedisLeaderElector`（`service/control/leader_redis.go`）：用
  `SETNX key value EX ttl` + 续期 goroutine 实现。key 固定为
  `xflow:leader:control-plane`（不做实例隔离，见非目标）。
- 仅当 `Config.Backend` 是 Redis/Asynq 后端时，`ControlPlane` 才构造
  `RedisLeaderElector`；memory 后端场景下不可能有第二个副本竞争同一份状态，
  用一个恒为 `IsLeader() == true` 的 no-op 实现（`control.alwaysLeader`）。
- `LeaseSweeper.Run(ctx)` 内部 select `elector.Notify()`，只有当前是 leader
  才执行扫描逻辑。默认启用，不加开关（与现有单/多副本部署行为兼容：只有一个
  副本时，该副本自然当选 leader，行为和今天完全一致）。
- Redis 连接异常时 `IsLeader()` 保守返回 `false`（宁可漏跑一轮 sweep，不可
  多副本同时抢跑）。续期连续失败超过阈值后主动降级为非 leader 并记录日志，
  不 panic。

不引入 `hashicorp/raft` 依赖或任何占位代码；接口本身与实现无关，未来新增
`RaftLeaderElector` 只是新增一个实现文件。

### 3. `sdk/xflow.Server`（用户门面，新增）

新增 `sdk/xflow/server.go`，风格对齐现有 `NewLocal(opts...)` /
`NewCluster(cfg, opts...)`：

```go
// sdk/xflow/server.go

// ServerConfig configures an embedded xflow control-plane server.
type ServerConfig struct {
    // RedisAddr is the Redis address for the Asynq/Redis backend. Empty means
    // an in-memory backend (single-process / test use).
    RedisAddr string
    // Store is an optional durable metadata store (see ClusterConfig.Store).
    Store store.Store
}

type ServerOption func(*serverConfig)

func WithServerAuth(auth control.Authenticator) ServerOption
func WithServerLogger(l engine.Logger) ServerOption
func WithServerMetrics(m *metrics.Metrics) ServerOption

// Server is the embeddable xflow control-plane server.
type Server struct { /* 内部持有 *control.ControlPlane */ }

func NewServer(cfg ServerConfig, opts ...ServerOption) (*Server, error)

func (s *Server) Handler() http.Handler
func (s *Server) Start(ctx context.Context) error
func (s *Server) Shutdown(ctx context.Context) error
```

`WithServerAuth` / `WithServerLogger` / `WithServerMetrics` 直接复用
`control.Authenticator` / `engine.Logger` / `metrics.Metrics` 现有类型，不做
额外包装类型——这些类型本身已经是稳定、独立可用的公开接口/结构体，和现有
`WithLogger(l engine.Logger)` 的处理方式一致（YAGNI：不为了"隔离用户"而重新
定义一遍相同语义的类型）。

用户唯一接触的类型是 `xflow.Server` / `xflow.ServerConfig` /
`xflow.ServerOption`；`control.ControlPlane`、`control.LeaderElector`、
`control.RunnerPool` 等内部类型对用户不可见。

用户使用示例：

```go
srv, err := xflow.NewServer(xflow.ServerConfig{RedisAddr: "localhost:6379"})
if err != nil { ... }
if err := srv.Start(ctx); err != nil { ... }
mux.Handle("/xflow/", srv.Handler())
// 宿主自己的 http.Server.ListenAndServe()
defer srv.Shutdown(ctx)
```

## 数据流 / 生命周期

```
xflow.Server.Start(ctx)
 └─ ControlPlane.Start(ctx)
     ├─ backend consumer 启动（沿用现有 BindHandler 逻辑）
     ├─ elector.Campaign(ctx) 异步跑起来（不阻塞 Start 返回）
     └─ sweeper.Run(ctx) 内部 select elector.Notify()，仅 leader 期间执行扫描

xflow.Server.Shutdown(ctx)
 └─ ControlPlane.Shutdown(ctx)
     ├─ elector.Resign(ctx)（主动放弃，加速下个副本抢占，避免等 TTL 过期）
     ├─ sweeper 停
     └─ backend stop（现有 stop() 逻辑）
```

## 错误处理

- `NewControlPlane` / `xflow.NewServer` 校验必填字段（如 backend 非空），
  构造失败直接返回 error，不 panic。
- `Campaign` 内部续期失败会重试；连续失败超过阈值后主动降级为非 leader，
  写日志，不让整个 `ControlPlane` 挂掉。
- `IsLeader()` 在选举状态不确定时保守返回 `false`。

## 测试策略

- `controlplane_test.go`：验证 `Handler()` 路由行为与现有
  `control.NewServer(...).Handler()` 等价（改造现有 `server_test.go` 用例）；
  验证 `Start`/`Shutdown` 幂等和 goroutine 正确退出（`-race`）。
- `leader_redis_test.go`：验证双实例竞争时只有一个拿到 leadership、`Resign`
  后另一个能立刻抢到、TTL 过期后自动转移。
- `lease_sweeper_test.go` 补一个"非 leader 时不执行清扫"的用例。
- `sdk/xflow/server_test.go`：验证 `xflow.NewServer` 的门面行为（内存后端、
  Redis 后端两种配置），以及 `Handler()` 可以被挂载到宿主 `http.ServeMux`。
- `cmd/server` 现有测试（`main_test.go`）改为验证 CLI 组装出的 `Config`
  正确传给 `xflow.NewServer` / `NewControlPlane`，行为不变。

## 迁移影响

- `cmd/server/main.go` 的 `runServer` 逻辑迁移到
  `control.NewControlPlane` + `xflow.NewServer` 之后，CLI 对外行为
  （flag、路由、日志格式）不变。
- 现有 `control.NewServer` / `NewDispatcher` / `NewGRPCServer` 函数保持不变，
  被 `ControlPlane` 内部调用，不破坏现有直接使用这些函数的代码路径（如果
  测试或其他调用方还在用）。
