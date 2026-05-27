# Cluster 适配器增强设计 — 审批流程支持

- 日期：2026-05-26
- 范围：P0（Resuspend + 审批节点 + 信号撤回 + TTL 续期）+ P1（Cancel 传播 + 信号 Hook + Timeout Monitor 优化）
- 前置：依赖 engine-core-refactor 完成后的架构
- 不在范围：业务自定义节点、UI、多租户、鉴权

---

## 0. 核心架构决策：一个漏洞 = 一个 Execution

### 0.1 背景

漏洞处理流程存在"驳回→重新提交"的循环（审核有误重审、复测不通过重测、验收不通过重验）。DAG 引擎不允许环路。

### 0.2 方案：Resuspend

引入 `Output.Resuspend` 语义 — 节点在 OnResume 后可以选择不输出结果，而是重新进入 suspended 状态等待下一次信号。

所有"回退"都是节点自身 Resuspend，不存在跨节点回跳。整个漏洞生命周期用一个 DAG + 一个 execution ID 覆盖。

| 业务动作 | 节点 | 引擎行为 |
|----------|------|----------|
| 审核有误 | 待审核 | Resuspend |
| 复测不通过 | 待复测 | Resuspend |
| 验收不通过 | 待验收 | Resuspend |
| 审批驳回（延期/风险） | 各审批节点 | Resuspend |

### 0.3 漏洞流程 DAG 结构

```
创建漏洞 → 待审核(suspend) → 待分配(suspend) → 待修复(suspend) → 待复测(suspend) → 待发布(suspend) → 待验收(suspend) → 已修复
                                                      ↘ 风险接受审批(suspend) → 风险接受(终态)
                                                      ↘ 风险报备审批(suspend) → 风险报备(终态)
                                                      ↘ 延期修复审批(suspend) → 已延期(终态)
```

---

## 1. 目标与验收标准

### 1.1 P0

1. `Output.Resuspend` 语义 — 节点 OnResume 后可重新挂起
2. `xflow.approval` 通用审批节点 — 或签/会签/依次审批、超时路由
3. 信号撤回 — `RevokeSignal`
4. execTTL 自动续期 — suspended 节点自动延长 Redis key TTL
5. `finalizeNode` 支持 `output.Port` 路由（现有 bug 修复）

### 1.2 P1

6. Cancel 传播 — Cancel 时标记所有 suspended 节点为 canceled
7. 信号相关 Hooks — `OnSignalDelivered` / `OnSignalRevoked` / `OnNodeTimeout`
8. Timeout Monitor 优化 — Redis ZSET 替代 MySQL 全表扫描

### 1.3 验收标准

| # | 标准 | 验证方式 |
|---|---|---|
| 1 | Resuspend 后节点重新进入 suspended，可再次被信号唤醒 | 单元测试（3 次连续 resuspend + 最终通过） |
| 2 | Resuspend 不触发下游调度 | 调度测试 |
| 3 | approval 节点支持 any/all/sequential 三种模式 | 单元测试 + 集成测试 |
| 4 | 信号撤回后节点保持 suspended | 竞态测试 |
| 5 | suspended 节点 Redis key 不会在审批期间过期 | TTL 验证测试 |
| 6 | Cancel 后所有 suspended 节点状态变为 canceled | 集成测试 |
| 7 | Timeout Monitor 使用 ZSET，扫描复杂度 O(expired) | 基准测试 |
| 8 | approval 节点 output.Port 正确路由到下游 | 端到端测试 |

---

## 2. P0 详细设计

### 2.1 `finalizeNode` Port 路由修复

现有 `finalizeNode` 硬编码 `Port: "main"`，忽略 handler 返回的 `output.Port`。修复：

```go
// engine/engine.go — finalizeNode

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

### 2.2 `Output.Resuspend` 语义

#### 2.2.1 Output 结构扩展

```go
// node/node.go

type Output struct {
    Data      map[string]any
    Error     *Error
    Port      string
    Resuspend bool   // true = 不输出，重新进入 suspended 状态
}
```

`Resuspend` 与 `Port` 互斥。`Resuspend=true` 时 `Port` 被忽略，引擎不触发下游调度。

#### 2.2.2 Engine 处理逻辑

核心变更：`doResuspend` 使用原子 Lua 脚本，避免 `ReleaseResumeLock` → `SuspendOrConsume` 之间的竞态。

```go
// engine/engine.go

func (e *Engine) executeSuspending(ctx context.Context, t *Task, sh node.SuspendingHandler, input *node.Input) (*node.Output, error) {
    if t.Type == TaskTypeNodeResume {
        output, err := sh.OnResume(ctx, input, t.Payload)
        if err != nil {
            return nil, err
        }
        if output != nil && output.Resuspend {
            return e.doResuspend(ctx, t, sh, input, t.Payload.Name)
        }
        return output, err
    }

    // 首次执行
    spec, err := sh.PrepareSuspend(ctx, input)
    if err != nil {
        return nil, err
    }

    payload, err := e.state.SuspendOrConsume(ctx, t.ExecutionID, t.NodeName, spec)
    if err != nil {
        return nil, err
    }

    if payload != nil {
        output, err := sh.OnResume(ctx, input, payload)
        if err != nil {
            return nil, err
        }
        if output != nil && output.Resuspend {
            return e.doResuspend(ctx, t, sh, input, payload.Name)
        }
        return output, err
    }

    // 挂起
    _ = e.state.UpsertNode(ctx, &NodeSnapshot{
        ExecutionID: t.ExecutionID,
        Name:        t.NodeName,
        NodeIdx:     t.NodeIdx,
        Status:      "suspended",
    })
    if e.hooks != nil {
        e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
    }
    return nil, nil
}

const maxResuspendDepth = 10

// doResuspend 原子地释放锁 + 清理旧 waiter + 重新挂起或消费预投递信号。
// oldSignalName 是上一次挂起时注册的信号名，用于清理旧 waiter key。
func (e *Engine) doResuspend(ctx context.Context, t *Task, sh node.SuspendingHandler, input *node.Input, oldSignalName string) (*node.Output, error) {
    depth := resuspendDepthFromCtx(ctx)
    if depth >= maxResuspendDepth {
        return nil, fmt.Errorf("resuspend depth exceeded (%d)", maxResuspendDepth)
    }
    ctx = withResuspendDepth(ctx, depth+1)

    // 获取新的 suspend spec
    spec, err := sh.PrepareSuspend(ctx, input)
    if err != nil {
        return nil, err
    }

    newSignalName := ""
    if len(spec.Signals) > 0 {
        newSignalName = spec.Signals[0]
    }

    // 原子操作：释放锁 + 清理旧 waiter + 检查新信号或重新挂起
    payload, err := e.state.ResuspendAtomic(ctx, t.ExecutionID, t.NodeName, oldSignalName, newSignalName, spec)
    if err != nil {
        return nil, err
    }

    if payload != nil {
        output, err := sh.OnResume(ctx, input, payload)
        if err != nil {
            return nil, err
        }
        if output != nil && output.Resuspend {
            return e.doResuspend(ctx, t, sh, input, newSignalName)
        }
        return output, err
    }

    // 重新挂起
    _ = e.state.UpsertNode(ctx, &NodeSnapshot{
        ExecutionID: t.ExecutionID,
        Name:        t.NodeName,
        NodeIdx:     t.NodeIdx,
        Status:      "suspended",
    })
    if e.hooks != nil {
        e.hooks.OnNodeSuspended(ctx, t.ExecutionID, t.NodeName)
    }
    return nil, nil
}
```

#### 2.2.3 StateBackend 新增方法

```go
type StateBackend interface {
    // ... existing ...

    // ResuspendAtomic 原子执行：释放 resume lock + 删除旧 waiter + 检查新信号或重新挂起。
    // 返回 *SignalPayload 表示有预投递信号（立即 resume），nil 表示已重新挂起。
    ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
}
```

#### 2.2.4 Redis Lua 实现

```lua
-- resuspendAtomicLua
-- KEYS[1] = resume_lock:       xflow:exec:{id}:node:{name}:resume_lock
-- KEYS[2] = old_waiter:        xflow:exec:{id}:waiter:{oldSignalName}
-- KEYS[3] = new_signal:        xflow:exec:{id}:signal:{newSignalName}
-- KEYS[4] = new_waiter:        xflow:exec:{id}:waiter:{newSignalName}
-- KEYS[5] = suspended_nodes:   xflow:exec:{id}:suspended_nodes
-- ARGV[1] = node name
-- ARGV[2] = ttl seconds
-- Returns: signal payload JSON or nil

-- 1. 释放 resume lock
redis.call('DEL', KEYS[1])

-- 2. 清理旧 waiter（防止旧信号名的迟到信号错误唤醒）
if KEYS[2] ~= KEYS[4] then
    redis.call('DEL', KEYS[2])
end

-- 3. 检查新信号是否已预投递
local signal = redis.call('GET', KEYS[3])
if signal then
    redis.call('DEL', KEYS[3])
    redis.call('SREM', KEYS[5], ARGV[1])
    return signal
end

-- 4. 注册新 waiter，重新挂起
redis.call('SET', KEYS[4], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SADD', KEYS[5], ARGV[1])
return nil
```

#### 2.2.5 Local 适配器实现

memoryState 用 mutex 保护同样的逻辑：删除旧 waiter、检查新信号、注册新 waiter。

---

### 2.3 `xflow.approval` — 通用审批节点

#### 2.3.1 Parameters

```go
const ApprovalNodeType = "xflow.approval"

type ApprovalMode string

const (
    ApprovalAny        ApprovalMode = "any"        // 任一人通过即通过
    ApprovalAll        ApprovalMode = "all"        // 会签（每人独立决策，即时响应）
    ApprovalSequential ApprovalMode = "sequential" // 按顺序依次审批
)

type ApprovalParams struct {
    Approvers     []string      `json:"approvers"`
    Mode          ApprovalMode  `json:"mode"`
    Timeout       time.Duration `json:"timeout"`
    TimeoutAction string        `json:"timeout_action"` // "reject" | "escalate" | "route"
}
}
```

#### 2.3.2 Output Ports

```
"approved"  — 审批通过
"rejected"  — 审批最终拒绝
"timeout"   — 超时路由
```

驳回（退回修改）不走 output port，而是 Resuspend。

#### 2.3.3 三种模式的实现策略

**ApprovalAny（或签）：**
- `ModeSignal`，单信号 `{nodeName}/approval`
- 任一人投递信号即触发 OnResume

**ApprovalAll（会签）：**
- `ModeSignal`，单信号 `{nodeName}/approval`（不用 ModeMultiSignal）
- 每次信号到达都触发 OnResume
- handler 内部维护已审批人列表（存在 output.Data 中，通过 input 传回）
- 如果还有人未审批 → Resuspend
- 如果所有人都审批了 → 聚合结果输出
- 如果任一人 reject → 立即输出 rejected（不等其他人）

**ApprovalSequential（依次）：**
- `ModeSignal`，信号名 `{nodeName}/approval/{currentApprover}`
- 每次只等当前审批人
- 通过后 Resuspend，PrepareSuspend 返回下一个审批人的信号名
- 全部通过 → 输出 approved
- 任一人 reject → 输出 rejected

#### 2.3.4 Handler 实现

```go
type ApprovalHandler struct{}

func (h *ApprovalHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
    return &Output{Data: map[string]any{"_type": ApprovalNodeType, "_stub": true}}, nil
}

func (h *ApprovalHandler) PrepareSuspend(ctx context.Context, input *Input) (*SuspendSpec, error) {
    params, err := parseApprovalParams(input.Params)
    if err != nil {
        return nil, err
    }

    switch params.Mode {
    case ApprovalAny:
        return &SuspendSpec{
            Mode:    ModeSignal,
            Signals: []string{approvalSignal(input.NodeName)},
            Timeout: params.Timeout,
        }, nil

    case ApprovalAll:
        return &SuspendSpec{
            Mode:    ModeSignal,
            Signals: []string{approvalSignal(input.NodeName)},
            Timeout: params.Timeout,
        }, nil

    case ApprovalSequential:
        // 从已有状态中获取当前审批人索引
        idx := getApproverIndex(input.Data)
        return &SuspendSpec{
            Mode:    ModeSignal,
            Signals: []string{approverSignal(input.NodeName, params.Approvers[idx])},
            Timeout: params.Timeout,
        }, nil
    }
    return nil, fmt.Errorf("unknown approval mode: %s", params.Mode)
}

func (h *ApprovalHandler) OnResume(ctx context.Context, input *Input, signal *SignalPayload) (*Output, error) {
    params, _ := parseApprovalParams(input.Params)

    if signal.Triggered == TimeoutFired {
        switch params.TimeoutAction {
        case "reject":
            return &Output{Data: map[string]any{"approved": false, "reason": "timeout"}, Port: "rejected"}, nil
        default:
            return &Output{Data: map[string]any{"reason": "timeout"}, Port: "timeout"}, nil
        }
    }

    action := signal.Data["action"].(string)

    switch action {
    case "approve":
        return h.handleApprove(params, input, signal)
    case "reject":
        return &Output{
            Data: map[string]any{"approved": false, "approver": signal.Data["approver"], "comment": signal.Data["comment"]},
            Port: "rejected",
        }, nil
    case "return":
        return &Output{Resuspend: true}, nil
    }
    return nil, fmt.Errorf("unknown action: %s", action)
}

func (h *ApprovalHandler) handleApprove(params *ApprovalParams, input *Input, signal *SignalPayload) (*Output, error) {
    switch params.Mode {
    case ApprovalAny:
        return &Output{
            Data: map[string]any{"approved": true, "approver": signal.Data["approver"], "comment": signal.Data["comment"]},
            Port: "approved",
        }, nil

    case ApprovalAll:
        // 聚合已审批人
        decisions := getDecisions(input.Data)
        decisions = append(decisions, map[string]any{
            "approver": signal.Data["approver"],
            "action":   "approve",
            "comment":  signal.Data["comment"],
        })
        if len(decisions) < len(params.Approvers) {
            // 还有人未审批 → Resuspend，携带已有决策
            return &Output{
                Resuspend: true,
                Data:      map[string]any{"_decisions": decisions},
            }, nil
        }
        // 全部通过
        return &Output{
            Data: map[string]any{"approved": true, "decisions": decisions},
            Port: "approved",
        }, nil

    case ApprovalSequential:
        idx := getApproverIndex(input.Data) + 1
        if idx < len(params.Approvers) {
            // 还有下一个审批人 → Resuspend，更新索引
            return &Output{
                Resuspend: true,
                Data:      map[string]any{"_approver_idx": idx},
            }, nil
        }
        // 全部通过
        return &Output{
            Data: map[string]any{"approved": true},
            Port: "approved",
        }, nil
    }
    return nil, nil
}

func approvalSignal(nodeName string) string  { return nodeName + "/approval" }
func approverSignal(nodeName, approver string) string { return nodeName + "/approval/" + approver }
```

#### 2.3.5 Resuspend 时的状态传递

Resuspend 时 `Output.Data` 中的数据会被持久化为节点输出，并立即更新 `input.Data`，使下次 `PrepareSuspend` / `OnResume` 能读到最新状态。

引擎在 `doResuspend` 中，调用 `ResuspendAtomic` 前：

```go
// 持久化状态 + 更新 input 引用
if output.Data != nil {
    _ = e.state.PutOutput(ctx, t.ExecutionID, t.NodeName, output.Data)
    input = &node.Input{
        Params:      input.Params,
        Vars:        input.Vars,
        Config:      input.Config,
        ExecutionID: input.ExecutionID,
        NodeName:    input.NodeName,
        Data:        output.Data,
    }
}
```

这确保 `getApproverIndex(input.Data)` 和 `getDecisions(input.Data)` 能读到上一次 Resuspend 保存的状态。

#### 2.3.6 信号命名约定

| 模式 | 信号名格式 |
|------|-----------|
| any / all | `{nodeName}/approval` |
| sequential | `{nodeName}/approval/{approverID}` |

---

### 2.4 信号撤回

#### 2.4.1 StateBackend 扩展

```go
type StateBackend interface {
    // ... existing ...
    RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error)
}
```

#### 2.4.2 Engine API

```go
func (e *Engine) RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error {
    revoked, err := e.state.RevokeSignal(ctx, id, signalName)
    if err != nil {
        return err
    }
    if !revoked {
        return ErrSignalConsumed
    }
    if e.hooks != nil {
        e.hooks.OnSignalRevoked(ctx, id, signalName)
    }
    return nil
}

var ErrSignalConsumed = errors.New("signal already consumed or not found")
```

#### 2.4.3 Redis Lua

```lua
-- revokeSignalLua
-- KEYS[1] = signal key
-- KEYS[2] = resume_lock key
-- Returns: 1 = revoked, 0 = not found or already consumed

local signal = redis.call('GET', KEYS[1])
if not signal then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
```

---

### 2.5 execTTL 自动续期

每次 SuspendOrConsume / ResuspendAtomic 成功挂起时，自动续期所有相关 key：

```go
func (s *redisState) extendExecTTL(ctx context.Context, id types.ExecutionID, nodeName string, ttl time.Duration) {
    pipe := s.rdb.Pipeline()
    prefix := fmt.Sprintf("xflow:exec:{%s}", id)
    // execution 级
    pipe.Expire(ctx, prefix+":status", ttl)
    pipe.Expire(ctx, prefix+":params", ttl)
    pipe.Expire(ctx, prefix+":graph", ttl)
    // node 级（当前挂起节点）
    pipe.Expire(ctx, prefix+":node:"+nodeName+":status", ttl)
    pipe.Expire(ctx, prefix+":output:"+nodeName, ttl)
    // suspended_nodes set
    pipe.Expire(ctx, prefix+":suspended_nodes", ttl)
    pipe.Exec(ctx)
}
```

TTL 计算：`max(defaultExecTTL, spec.Timeout + 1h)`。

Per-Execution TTL：

```go
// sdk/option.go — SubmitOption 是 SDK 层的提交选项，传递给 cluster adapter

type SubmitOption func(*submitConfig)

type submitConfig struct {
    execTTL time.Duration
}

func WithExecutionTTL(d time.Duration) SubmitOption {
    return func(c *submitConfig) { c.execTTL = d }
}
```

---

## 3. P1 详细设计

### 3.1 Cancel 传播

Cancel 时标记所有 suspended 节点为 canceled，清理 ZSET：

```go
func (e *Engine) Cancel(ctx context.Context, id types.ExecutionID) error {
    e.mu.RLock()
    g := e.graphs[id]
    e.mu.RUnlock()

    if err := e.state.UpdateExecutionStatus(ctx, id, types.StatusCanceling, ""); err != nil {
        return err
    }

    if g != nil {
        suspendedNodes, _ := e.state.ListSuspendedNodes(ctx, id)
        for _, nodeName := range suspendedNodes {
            _ = e.state.UpsertNode(ctx, &NodeSnapshot{
                ExecutionID: id,
                Name:        nodeName,
                NodeIdx:     g.Index[nodeName],
                Status:      "canceled",
            })
        }
    }

    _ = e.state.UpdateExecutionStatus(ctx, id, types.StatusCanceled, "")
    if e.hooks != nil {
        e.hooks.OnExecutionComplete(ctx, id, types.StatusCanceled)
    }

    e.mu.Lock()
    delete(e.graphs, id)
    e.mu.Unlock()
    return nil
}
```

StateBackend 扩展：

```go
ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error)
```

Redis 实现：SET `xflow:exec:{id}:suspended_nodes`，suspend 时 SADD，resume 时 SREM。需要修改现有 `suspendOrConsumeLua` 增加 SADD（当前只有 `resuspendAtomicLua` 维护了此 SET）：

```lua
-- suspendOrConsumeLua 末尾追加（节点挂起时）
redis.call('SADD', KEYS[5], ARGV[1])  -- KEYS[5] = suspended_nodes SET, ARGV[1] = node name
```

`DeliverSignal` 成功唤醒节点时 SREM：

```lua
-- signalOrStoreLua 中，找到 waiter 并返回 node name 时
redis.call('SREM', KEYS[4], waiterNodeName)  -- KEYS[4] = suspended_nodes SET
```

Cancel 时清理 ZSET：

```go
func (s *redisState) cleanupOnCancel(ctx context.Context, id types.ExecutionID, suspendedNodes []string) {
    pipe := s.rdb.Pipeline()
    for _, name := range suspendedNodes {
        member := string(id) + "\x00" + name
        pipe.ZRem(ctx, "xflow:timeouts", member)
    }
    pipe.Exec(ctx)
}
```

types.Status 扩展：

```go
const (
    StatusRunning   Status = "running"
    StatusSuccess   Status = "success"
    StatusFailed    Status = "failed"
    StatusCanceled  Status = "canceled"
    StatusCanceling Status = "canceling"
)
```

---

### 3.2 信号相关 Hooks

```go
type Hooks interface {
    // Lifecycle (existing)
    OnNodeStart(ctx context.Context, id types.ExecutionID, name string)
    OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status string)
    OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string)
    OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.Status)

    // Signal events (new)
    OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any)
    OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string)
    OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string)
}

type BaseHooks struct{}
// 所有方法空实现，嵌入即可满足接口
```

| Hook | 调用位置 | 用途 |
|------|----------|------|
| `OnSignalDelivered` | `Engine.DeliverSignal` 成功后 | 审批已提交通知 |
| `OnSignalRevoked` | `Engine.RevokeSignal` 成功后 | 审批已撤回通知 |
| `OnNodeTimeout` | Timeout Monitor 投递超时信号前 | 告警、升级通知 |

Hook 执行保证：5s context timeout，panic recover 不影响引擎。

---

### 3.3 Timeout Monitor 优化

Redis Sorted Set 替代 MySQL 全表扫描：

```
Key:    xflow:timeouts
Score:  timeout_at (Unix timestamp)
Member: {execution_id}\x00{node_name}
```

ZSET member 使用 `\x00` 作为分隔符（node name 不可能包含 null byte），避免 `:` 歧义。

写入时机：节点挂起且 `spec.Timeout > 0` 时写入 timeout ZSET。Resuspend 时旧条目在 ResuspendAtomic 中清理，新条目在挂起后写入。

原子弹出（多 Worker 安全）：

```lua
-- popExpiredLua
-- KEYS[1] = zset key
-- ARGV[1] = now (unix timestamp)
-- ARGV[2] = batch size
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #expired > 0 then
    redis.call('ZREM', KEYS[1], unpack(expired))
end
return expired
```

多个 Monitor 实例安全并发运行，无需 leader 选举。

Resume 时清理：

```go
func (s *redisState) cleanupOnResume(ctx context.Context, id types.ExecutionID, nodeName string) {
    member := string(id) + "\x00" + nodeName
    pipe := s.rdb.Pipeline()
    pipe.ZRem(ctx, "xflow:timeouts", member)
    pipe.Exec(ctx)
}
```

---

## 4. 接口变更汇总

### 4.1 node.Output

```go
Resuspend bool  // 新增
```

### 4.2 StateBackend 新增

```go
ResuspendAtomic(ctx context.Context, id types.ExecutionID, nodeName string, oldSignalName string, newSignalName string, spec *node.SuspendSpec) (*node.SignalPayload, error)
RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) (bool, error)
ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error)
```

### 4.3 Engine 新增

```go
RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error
```

### 4.4 Hooks 新增

```go
OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any)
OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string)
OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string)
```

### 4.5 ClusterStore 新增

```go
RevokeSignal(ctx context.Context, id types.ExecutionID, name string) (bool, error)
```

---

## 5. 实施阶段

| Phase | 范围 | 依赖 | 估算 |
|---|---|---|---|
| P0-1 | `finalizeNode` port 路由修复 | 无 | ~30 行 |
| P0-2 | `Output.Resuspend` + `ResuspendAtomic` Lua + engine 逻辑 + 测试 | P0-1 | ~400 行 |
| P0-3 | 信号撤回（Lua + Store + Engine API） | 无 | ~250 行 |
| P0-4 | execTTL 自动续期（含 node 级 key） | P0-2 | ~150 行 |
| P0-5 | `xflow.approval` 节点（三种模式 + Resuspend 状态传递）+ 测试 | P0-2 | ~500 行 |
| P1-1 | Hooks 扩展 + BaseHooks + 调用点 | P0-3 | ~200 行 |
| P1-2 | Cancel 传播 + ListSuspendedNodes + ZSET 清理 | P1-1 | ~250 行 |
| P1-3 | Timeout Monitor ZSET 重构 + popExpiredLua | P1-1 | ~300 行 |

总计：~2080 行

---

## 6. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Resuspend 无限递归 | maxResuspendDepth=10 |
| ReleaseResumeLock→SuspendOrConsume 竞态 | 合并为 ResuspendAtomic 单 Lua 脚本 |
| 信号名变化时旧 waiter 残留 | ResuspendAtomic 中原子清理旧 waiter key |
| 信号撤回与 resume 竞态 | Lua 原子检查 resume_lock |
| 单 execution 跨周 Redis 内存 | TTL 续期覆盖 execution + node 级 key |
| Cancel 时节点正在执行 | UpsertNode CAS：仅 suspended→canceled 有效 |
| 多 Monitor 竞争 | popExpiredLua 原子弹出 |
| ZSET member 解析歧义 | 使用 \x00 分隔符 |
| Hooks 接口变更向后兼容 | BaseHooks 嵌入式基类 |

---

## 7. 漏洞流程完整示例

```go
wf := xflow.NewWorkflowBuilder("vuln_lifecycle").
    Node("classify", "vuln.classify").
    Node("review", "xflow.approval").
        Param("approvers", []string{"${security_lead}"}).
        Param("mode", "any").
        Param("timeout", "48h").
        Param("timeout_action", "escalate").
    Node("assign", "vuln.assign").
    Node("fix", "xflow.approval").
        Param("approvers", []string{"${fixer}"}).
        Param("mode", "any").
        Param("timeout", "168h").
    Node("retest", "xflow.approval").
        Param("approvers", []string{"${reporter}"}).
        Param("mode", "any").
    Node("publish", "vuln.publish").
    Node("accept", "xflow.approval").
        Param("approvers", []string{"${reporter}"}).
        Param("mode", "any").
    Node("done", "xflow.function").
    Connect("classify", "main", "review", "main").
    Connect("review", "approved", "assign", "main").
    Connect("assign", "main", "fix", "main").
    Connect("fix", "approved", "retest", "main").
    Connect("retest", "approved", "publish", "main").
    Connect("publish", "main", "accept", "main").
    Connect("accept", "approved", "done", "main").
    Connect("review", "rejected", "done", "main").
    Connect("review", "timeout", "done", "main")

execID, _ := eng.Submit(ctx, wf.Build(),
    map[string]any{"vuln_id": "VULN-2026-001", "severity": "high"},
    xflow.WithExecutionTTL(30*24*time.Hour),
)

// 审核通过
eng.DeliverSignal(ctx, execID, "review/approval", map[string]any{
    "action": "approve", "approver": "sec_lead_01", "comment": "确认",
})

// 审核有误（退回）→ review 节点 Resuspend
eng.DeliverSignal(ctx, execID, "review/approval", map[string]any{
    "action": "return", "approver": "sec_lead_01", "comment": "信息不完整",
})

// 重新审核通过
eng.DeliverSignal(ctx, execID, "review/approval", map[string]any{
    "action": "approve", "approver": "sec_lead_01", "comment": "补充完整，通过",
})

// 复测不通过 → retest 节点 Resuspend
eng.DeliverSignal(ctx, execID, "retest/approval", map[string]any{
    "action": "return", "approver": "reporter_01", "comment": "漏洞仍可复现",
})

// 撤回误操作
eng.RevokeSignal(ctx, execID, "fix/approval")
```
