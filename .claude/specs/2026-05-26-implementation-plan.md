# 实施计划 — Cluster 适配器增强

基于 `2026-05-26-cluster-enhancement-design.md` spec，按 8 个 phase 实施。

---

## Phase P0-1：finalizeNode port 路由修复 + Output 结构扩展

**修改文件：**
- `node/node.go` — Output 结构体增加 Port 和 Resuspend 字段
- `engine/engine.go` — finalizeNode 使用 output.Port

**变更：**

### node/node.go

```go
// 现有 Output 只有 Data + Error，扩展为：
type Output struct {
    Data      map[string]any
    Error     *Error
    Port      string   // 活跃输出端口（默认 "main"）
    Resuspend bool     // true = 不输出，重新进入 suspended 状态
}
```

### engine/engine.go

```go
// finalizeNode 方法中，替换硬编码 "main"

port := "main"
if output != nil && output.Port != "" {
    port = output.Port
}

_ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, data)
_ = e.state.UpsertNode(ctx, &NodeSnapshot{
    ExecutionID: t.ExecutionID,
    Name:        t.NodeName,
    NodeIdx:     t.NodeIdx,
    Status:      "success",
    Output:      data,
    Port:        port,
})

if e.hooks != nil {
    e.hooks.OnNodeComplete(ctx, t.ExecutionID, t.NodeName, "success")
}

return e.OnNodeComplete(ctx, t.ExecutionID, g, t.NodeIdx, port, data)
```

**测试：**
- `engine/engine_test.go` — 新增测试：handler 返回 Port="approved" 时，下游只有 "approved" 边的节点被调度

**依赖：** 无

---

## Phase P0-2：Resuspend 引擎逻辑 + ResuspendAtomic

**修改文件：**
- `engine/interfaces.go` — StateBackend 新增方法
- `engine/engine.go` — executeSuspending + doResuspend
- `sdk/internal/adapter/cluster/redis_state.go` — ResuspendAtomic Lua + suspendOrConsumeLua 修改 + signalOrStoreLua 修改
- `sdk/internal/adapter/local/memory_state.go` — ResuspendAtomic 内存实现

### engine/interfaces.go

```go
// StateBackend 新增方法
ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
```

### engine/engine.go

新增/修改方法：
1. `executeSuspending` — OnResume 返回后检查 `output.Resuspend`
2. `doResuspend(ctx, t, sh, input, oldSignalName)` — 原子 resuspend 逻辑
3. `resuspendDepthFromCtx(ctx)` / `withResuspendDepth(ctx, depth)` — context 深度计数

关键逻辑：
- `doResuspend` 中先 PutOutput + 更新 input.Data，再调用 ResuspendAtomic
- maxResuspendDepth = 10

### sdk/internal/adapter/cluster/redis_state.go

**新增 Lua 脚本 `resuspendAtomicLua`：**

```lua
-- KEYS[1] = resume_lock
-- KEYS[2] = old_waiter
-- KEYS[3] = new_signal
-- KEYS[4] = new_waiter
-- KEYS[5] = suspended_nodes SET
-- ARGV[1] = node name
-- ARGV[2] = ttl seconds
redis.call('DEL', KEYS[1])
if KEYS[2] ~= KEYS[4] then
    redis.call('DEL', KEYS[2])
end
local signal = redis.call('GET', KEYS[3])
if signal then
    redis.call('DEL', KEYS[3])
    redis.call('SREM', KEYS[5], ARGV[1])
    return signal
end
redis.call('SET', KEYS[4], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SADD', KEYS[5], ARGV[1])
return nil
```

**修改现有 `suspendOrConsumeLua`：** 增加第 4 个 KEY（suspended_nodes SET），挂起时 SADD：

```lua
-- 现有脚本末尾（节点挂起分支）追加：
redis.call('SADD', KEYS[4], ARGV[1])  -- KEYS[4] = suspended_nodes SET, ARGV[1] = node name
```

Go 调用处需要传入第 4 个 key：`suspendedNodesKey(id)` = `xflow:exec:{id}:suspended_nodes`

**修改现有 `signalOrStoreLua`：** 增加第 4 个 KEY（suspended_nodes SET），唤醒时 SREM：

```lua
-- 现有脚本中，找到 waiter 并返回 node name 时追加：
redis.call('SREM', KEYS[4], waiterNodeName)  -- KEYS[4] = suspended_nodes SET
```

Go 调用处需要传入第 4 个 key：`suspendedNodesKey(id)`

**新增 Go 方法：**

```go
func (s *redisState) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
```

### sdk/internal/adapter/local/memory_state.go

```go
func (s *memoryState) ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
// mutex 保护：
// 1. delete(s.resumed, execID+"/"+nodeName)  ← 释放 resume lock
// 2. 删除旧 waiter（s.suspended 中旧信号名的条目）
// 3. 检查新信号是否已预投递（s.signals）
// 4. 如果有 → 消费并返回 payload
// 5. 如果没有 → 注册新 waiter，SADD suspended set
```

**测试：**
- `engine/engine_test.go` — Resuspend 3 次后通过
- `engine/engine_test.go` — Resuspend 不触发下游
- `engine/engine_test.go` — maxResuspendDepth 超限返回 error
- `engine/engine_test.go` — 信号名变化时旧 waiter 被清理

**依赖：** P0-1

---

## Phase P0-3：信号撤回

**修改文件：**
- `engine/interfaces.go` — StateBackend.RevokeSignal
- `engine/engine.go` — Engine.RevokeSignal 方法 + ErrSignalConsumed
- `sdk/internal/adapter/cluster/redis_state.go` — revokeSignalLua + Go 方法
- `sdk/internal/adapter/local/memory_state.go` — RevokeSignal 内存实现
- `store/store.go` — ClusterStore.RevokeSignal
- `store/mysql/store.go` — MySQL 实现（UPDATE status='revoked'）

### engine/interfaces.go

```go
RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error)
```

### engine/engine.go

```go
var ErrSignalConsumed = errors.New("signal already consumed or not found")

func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error
```

### redis_state.go — revokeSignalLua

```lua
-- KEYS[1] = signal key, KEYS[2] = resume_lock key
local signal = redis.call('GET', KEYS[1])
if not signal then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('DEL', KEYS[1])
return 1
```

### store/store.go

```go
RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error)
```

### store/mysql/store.go

```go
func (s *mysqlStore) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error) {
    // UPDATE xflow_signals SET status='revoked' WHERE execution_id=? AND signal_name=? AND status='active'
}
```

**测试：**
- 撤回未消费信号 → 成功
- 撤回已消费信号 → ErrSignalConsumed
- 撤回后投递同名信号 → 节点正常唤醒

**依赖：** 无

---

## Phase P0-4：execTTL 自动续期

**修改文件：**
- `sdk/internal/adapter/cluster/redis_state.go` — extendExecTTL 方法修改
- `sdk/option.go` — SubmitOption / submitConfig / WithExecutionTTL
- `sdk/xflow.go` — Submit 方法签名增加 `...SubmitOption`，传递 execTTL 到 adapter

### redis_state.go

```go
func (s *redisState) extendExecTTL(ctx context.Context, id types.ExecutionID, nodeName string, ttl time.Duration) {
    pipe := s.rdb.Pipeline()
    prefix := fmt.Sprintf("xflow:exec:{%s}", id)
    pipe.Expire(ctx, prefix+":status", ttl)
    pipe.Expire(ctx, prefix+":params", ttl)
    pipe.Expire(ctx, prefix+":graph", ttl)
    pipe.Expire(ctx, prefix+":node:"+nodeName+":status", ttl)
    pipe.Expire(ctx, prefix+":output:"+nodeName, ttl)
    pipe.Expire(ctx, prefix+":suspended_nodes", ttl)
    pipe.Exec(ctx)
}
```

在 `SuspendOrConsume` 和 `ResuspendAtomic` 成功挂起后调用。

### sdk/option.go

```go
type SubmitOption func(*submitConfig)

type submitConfig struct {
    execTTL time.Duration
}

func WithExecutionTTL(d time.Duration) SubmitOption {
    return func(c *submitConfig) { c.execTTL = d }
}
```

**测试：**
- 挂起后 key TTL 被正确延长
- spec.Timeout > defaultTTL 时使用 spec.Timeout + 1h

**依赖：** P0-2

---

## Phase P0-5：xflow.approval 节点

**修改文件：**
- `node/approval.go`（新建）— ApprovalHandler 实现（含 init() 注册）

### node/approval.go

完整实现：
- `ApprovalNodeType = "xflow.approval"`
- `ApprovalMode` 类型 + 常量（any/all/sequential）
- `ApprovalParams` 结构体
- `ApprovalHandler` 实现 `SuspendingHandler` 接口
- `Descriptor()` — 声明 params 和 ports
- `Execute()` — stub
- `PrepareSuspend()` — 按 mode 返回不同 SuspendSpec
- `OnResume()` — 处理 approve/reject/return 动作
- `handleApprove()` — 按 mode 聚合审批结果或 Resuspend
- 辅助函数：`approvalSignal()`, `approverSignal()`, `parseApprovalParams()`, `getApproverIndex()`, `getDecisions()`

**测试：**
- `node/approval_test.go`
  - any 模式：approve → "approved" port
  - any 模式：reject → "rejected" port
  - any 模式：return → Resuspend
  - all 模式：2 人审批，第 1 人 approve → Resuspend，第 2 人 approve → "approved"
  - all 模式：第 1 人 reject → 立即 "rejected"
  - sequential 模式：3 人依次 approve → 每次 Resuspend 直到最后一人
  - timeout → 按 timeout_action 路由

**依赖：** P0-2

---

## Phase P1-1：Hooks 扩展

**修改文件：**
- `engine/interfaces.go` — Hooks 接口新增 3 个方法
- `engine/hooks.go`（新建）— BaseHooks 实现 + safeHook 包装器
- `engine/engine.go` — DeliverSignal / RevokeSignal 中调用新 hook

### engine/interfaces.go

```go
type Hooks interface {
    OnNodeStart(ctx context.Context, id types.ExecutionID, name string)
    OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status string)
    OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string)
    OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.Status)
    // 新增
    OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any)
    OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string)
    OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string)
}
```

### engine/hooks.go（新建）

```go
type BaseHooks struct{}
// 所有方法空实现

// safeHook 包装 hook 调用：5s context timeout + panic recover
func safeHook(ctx context.Context, logger Logger, fn func(ctx context.Context)) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    defer func() {
        if r := recover(); r != nil {
            if logger != nil {
                logger.Error("hook panic recovered", "panic", r)
            }
        }
    }()
    fn(ctx)
}
```

### engine/engine.go

- `DeliverSignal` 成功后：`safeHook(ctx, e.logger, func(ctx) { e.hooks.OnSignalDelivered(ctx, id, name, data) })`
- `RevokeSignal` 成功后：`safeHook(ctx, e.logger, func(ctx) { e.hooks.OnSignalRevoked(ctx, id, signalName) })`
- `OnNodeTimeout` 调用点在 P1-3 的 Timeout Monitor 中（不在此 phase）

**测试：**
- hook 被正确调用
- hook panic 不影响引擎（不 crash）
- hook 超时不阻塞引擎

**依赖：** P0-3

---

## Phase P1-2：Cancel 传播

**修改文件：**
- `engine/interfaces.go` — StateBackend.ListSuspendedNodes
- `engine/engine.go` — Cancel 方法重写
- `sdk/internal/adapter/cluster/redis_state.go` — ListSuspendedNodes + ZSET 清理
- `sdk/internal/adapter/local/memory_state.go` — ListSuspendedNodes
- `types/execution.go` — StatusCanceling 常量

### engine/interfaces.go

```go
ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error)
```

### types/execution.go

```go
const StatusCanceling Status = "canceling"
```

### sdk/internal/adapter/local/memory_state.go

```go
func (s *memoryState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
    // 遍历 s.suspended map，过滤 execID 前缀，返回 node name 列表
}
```

```go
const StatusCanceling Status = "canceling"
```

### redis_state.go

```go
func (s *redisState) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
    return s.rdb.SMembers(ctx, fmt.Sprintf("xflow:exec:{%s}:suspended_nodes", id)).Result()
}

func (s *redisState) cleanupOnCancel(ctx context.Context, id types.ExecutionID, suspendedNodes []string) {
    pipe := s.rdb.Pipeline()
    for _, name := range suspendedNodes {
        member := string(id) + "\x00" + name
        pipe.ZRem(ctx, "xflow:timeouts", member)
    }
    pipe.Exec(ctx)
}
```

**测试：**
- Cancel 后所有 suspended 节点状态为 canceled
- Cancel 后 ZSET 中无残留 timeout 条目
- Cancel 时正在执行的节点不受影响（CAS 保护）

**依赖：** P1-1

---

## Phase P1-3：Timeout Monitor ZSET 重构

**修改文件：**
- `sdk/internal/adapter/cluster/timeout_monitor.go` — 重写为 ZSET 模式
- `sdk/internal/adapter/cluster/redis_state.go` — SuspendOrConsume/ResuspendAtomic 中写入 ZSET + cleanupOnResume

### timeout_monitor.go

重写：
- `TimeoutMonitor` struct（rdb, engine, hooks, interval, stop）
- `Run()` — ticker 循环
- `processTimeouts(now)` — popExpiredLua 弹出 + `safeHook(hooks.OnNodeTimeout)` + DeliverSignal
- `popExpiredLua` Lua 脚本

### redis_state.go

SuspendOrConsume 成功挂起后：
```go
if spec.Timeout > 0 {
    member := string(id) + "\x00" + name
    s.rdb.ZAdd(ctx, "xflow:timeouts", redis.Z{
        Score:  float64(time.Now().Add(spec.Timeout).Unix()),
        Member: member,
    })
}
```

ResuspendAtomic 成功挂起后同理。

cleanupOnResume：
```go
func (s *redisState) cleanupOnResume(ctx context.Context, id types.ExecutionID, nodeName string) {
    member := string(id) + "\x00" + nodeName
    s.rdb.ZRem(ctx, "xflow:timeouts", member)
}
```

在 DeliverSignal 成功唤醒节点后调用。

**popExpiredLua：**
```lua
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #expired > 0 then
    redis.call('ZREM', KEYS[1], unpack(expired))
end
return expired
```

**测试：**
- 节点超时后被正确唤醒
- Resume 后 ZSET 无残留
- 多 Monitor 并发不重复处理
- Resuspend 后旧 timeout 被清理，新 timeout 注册

**依赖：** P1-1

---

## 实施顺序总结

```
P0-1 (port 修复)
  ↓
P0-2 (Resuspend) ←── P0-3 (信号撤回，独立)
  ↓                      ↓
P0-4 (TTL 续期)      P1-1 (Hooks)
  ↓                      ↓
P0-5 (approval)      P1-2 (Cancel) + P1-3 (Timeout ZSET)
```

P0-3 和 P0-1/P0-2 可以并行开发。P1-2 和 P1-3 可以并行开发。
