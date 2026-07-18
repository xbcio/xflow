# 高吞吐采集分离架构（D1）

> 审批工作流与高吞吐采集采用**独立架构**，不混入审批生产门槛。本文档定义两条采集路径、配置示例和容量基线方法。
> 门槛定义见 [RELEASE-GATES.md](./RELEASE-GATES.md)，部署拓扑见 [DEPLOYMENT-TOPOLOGIES.md](./DEPLOYMENT-TOPOLOGIES.md)。

## 1. 边界声明

- server/runner 当前约 13 wf/s 只是单次环境基线，**不能外推为容量承诺**。
- `ExecutionModeTransient` 禁止 signal/suspend/inspect，**不能用于审批**。
- 审批工作流始终使用 durable/default mode，保留 signal/suspend/inspect/audit。
- 高吞吐采集不属于 G0/G1/G2 的放宽条件。

## 2. 两条独立路径

```
路径 A — server workflow API（审批 / 可控采集）
  外部 Kafka consumer/ingress ──削峰/批量/幂等──> server submit/invoke API
  server 编译 Graph → durable 调度 → runner 执行
  （server 不激活 SDK trigger runtime）

路径 B — SDK cluster transient（短生命周期采集编排）
  node.KafkaTrigger + NewCluster(..., ExecutionModeTransient)
  进程内消费 Kafka → emit Invoke → 短 TTL runtime state
  （禁止 signal/suspend/inspect，受限可观测性）
```

两条路径的 Kafka 消费与 workflow 执行**均为 at-least-once**；业务副作用须幂等。

## 3. 路径 A：server workflow API + 外部 ingress

### 3.1 适用场景

- 采集流量需要削峰、批量、幂等控制，与审批工作流共享同一 server 控制面。
- 业务侧已有 Kafka consumer 基础设施，希望把工作流「托管」给 server。

### 3.2 架构

```
[ Kafka topic ]
       │ consume
       ▼
[ 外部 ingress service ]   ← 削峰 / 批量 / 幂等键 / 限流
       │ POST /v1/workflows  或  POST /v1/workflows/invoke
       ▼
[ xflow-server ]
   - 编译 WorkflowDef → Graph IR
   - durable 调度（durable assignment/lease/outbox）
   - runner 执行 handler
   - 保留 signal/suspend/inspect/audit
```

### 3.3 关键事实

- **server 不激活 SDK trigger runtime**：`cmd/server` 的 `apiserver.Config` 无 ExecutionMode 字段，`service/apiserver/` 无 trigger 代码。server 只接受 submit/invoke API 调用。
- ingress service 负责 Kafka 消费、削峰、批量聚合、幂等键生成，再调用 server HTTP API。
- server 侧工作流使用 durable/default mode（Redis 权威状态 + outbox）。

### 3.4 配置示例

ingress service（伪代码，宿主自实现）：

```go
// 宿主 ingress：消费 Kafka，批量聚合，带幂等键调用 server
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers:  brokers,
    Topic:    "ingest-events",
    GroupID:  "ingest-to-xflow",
    MinBytes: 1, MaxBytes: 10 << 20, // 批量拉取
})
for {
    batch, err := readBatch(ctx, reader, 100, 500*time.Millisecond) // 批量 + 超时
    for _, ev := range batch {
        // 幂等键：用 Kafka (topic,partition,offset) 或业务唯一 ID
        workflow := buildWorkflowDef(ev)        // 动态或预定义 workflow
        _, err := xflowClient.Invoke(ctx, workflow, "start", ev.Input)
        if err != nil { /* 重试 / dead-letter，at-least-once */ }
    }
    reader.CommitMessages(ctx, batch...)           // emit 后 commit
}
```

server 启动按 [deployment-examples.md](../references/deployment-examples.md)，使用 durable mode（`--redis`，非 `--memory`）。

## 4. 路径 B：SDK cluster transient

### 4.1 适用场景

- 短生命周期采集编排，不需要持久化、signal、suspend、inspect。
- 嵌入方自带 Kafka 消费能力，愿意引入 Redis 并参与执行。
- 允许短 TTL runtime state 和受限可观测性。

### 4.2 关键事实（实现位置）

| 能力 | 位置 | 说明 |
|---|---|---|
| `ExecutionModeTransient` | `sdk/xflow/execution_mode.go:18` | 常量，禁用 signal/suspend/inspect |
| `NewCluster` 接受 mode | `sdk/xflow/cluster.go:70-72` | transient 时传 `distributed.WithTransientMode(ttl, completionTTL)` |
| `WithTransientTTL` | `sdk/xflow/execution_mode.go:57` | 活跃 runtime state TTL，默认 10min |
| `WithTransientCompletionTTL` | `sdk/xflow/execution_mode.go:65` | 完成后结果 TTL，默认 30s |
| transient 禁用 | `sdk/xflow/engine_control.go:68/80/101`、`engine.go:62` | signal/revoke/inspect/suspend 全部拒绝 |
| `NewLocal` 拒绝 transient | `sdk/xflow/local.go:31` | transient 要求 cluster，返回 `ErrTransientRequiresCluster` |
| backend transient | `backend/distributed/backend.go:84/250/315/326` | transient 不启动 TimeoutMonitor，短 TTL 状态 |
| `KafkaTrigger` | `node/internal/trigger/kafka.go` | per-message 与 aggregate 两种运行时 |
| trigger runtime 激活 | `sdk/xflow/engine.go:88`、`workflow_registry.go:81` | `AddWorkflow` 时 `ReconcileWorkflow` 激活 trigger |

### 4.3 配置示例

```go
eng, err := xflow.NewCluster(
    xflow.ClusterConfig{RedisAddr: redisAddr},
    xflow.WithExecutionMode(xflow.ExecutionModeTransient),
    xflow.WithTransientTTL(10*time.Minute),             // 活跃 state TTL
    xflow.WithTransientCompletionTTL(30*time.Second),    // 完成后结果 TTL
    xflow.WithNodes(/* 采集 handler 节点 */),
)
// 注册带 Kafka trigger 的工作流
eng.AddWorkflow(ctx, &types.WorkflowRecord{
    WorkflowDef: kafkaIngestWorkflow,  // 含 node.KafkaTrigger 节点
})
```

KafkaTrigger 节点配置（aggregate 模式，高吞吐）：

```go
node.KafkaTrigger().
    Brokers(brokers...).
    Topic("ingest-events").
    Group("xflow-collect").
    StartOffset("latest").
    AggregateByPartition(100, 50*time.Millisecond)  // MaxSize=100, FlushInterval=50ms
```

### 4.4 限制

- **禁止 signal/suspend/inspect**：调用返回 `ErrTransientSignalsUnsupported` / `ErrTransientSuspendUnsupported` / `ErrTransientInspectionUnavailable`。
- 短 TTL runtime state：活跃 10min、完成后 30s，过期自动清理。
- 受限可观测性：无长期 inspect 表面。
- per-message 模式每分区串行；aggregate 模式按 MaxSize/FlushInterval 批量 emit。
- 空闲分区 worker 5 分钟后退出（处理 rebalance 分区撤销）。

## 5. 审批工作流独立 durable mode

审批工作流**始终使用 durable/default mode**，不与 transient 混用：

- 保留 signal/suspend/inspect/audit。
- Redis 权威状态 + sqlstore audit projection。
- durable assignment/lease/outbox 调度恢复。
- 会签、超时、取消、循环退回、重复 signal 按 G1/G2 门槛验证。

> 不得用 transient 路径的容量数据承诺审批工作流的生产能力。

## 6. 容量基线方法

在**受控 host** 上分别对两条路径建立多样本基线，再设定回归阈值。

### 6.1 记录维度

| 维度 | 说明 |
|---|---|
| 拓扑 | server+runner / SDK cluster transient |
| 消息大小 | 字节 |
| 节点数 | workflow 节点数 |
| 并发 | `--concurrency` / `WithConcurrency` |
| Redis 配置 | 单节点/sentinel/cluster，持久化 |
| Kafka 配置 | partition 数、consumer group、MaxSize、FlushInterval |
| 延迟分位 | p50/p95/p99 |

### 6.2 现有 perf 基线

| 基准 | 文件 | 覆盖 |
|---|---|---|
| KafkaTrigger aggregate | `test/perf/kafka_trigger_bench_test.go` | MaxSize=1/10/100，真实 Kafka，aggregate 稳态延迟 |
| KafkaTrigger 4000 msg | `node/internal/trigger/kafka_benchmark_test.go` | 4000 消息聚合 |
| scheduler / statestore / asynq queue / e2e load | `test/perf/*_bench_test.go` | 调度、状态存储、队列、端到端负载 |
| 可靠性（队列故障注入） | `test/perf/reliability_bench_test.go` | `perfSwitchableQueue` 注入队列不可用 |

### 6.3 CI 采样脚本

`scripts/perf-sample.sh`（见仓库）运行 perf bench **与 E2E load 测试**，记录 p50/p95/p99 + ns/op + allocs/op，并在结果文件尾部追加结构化 `perf.metric` 行，用于回归监控：

```bash
# 需要真实 Redis + Kafka（test/env/docker-compose.yml）
./scripts/perf-sample.sh
```

CI 采样 job（`.github/workflows/perf-sample.yml`）每日与手动触发，运行 `make perf-sample` 并上传 `perf-sample-results.txt` 为 artifact（保留 90 天）。

> CI perf 采样受环境差异影响大，**不作为硬性门槛**；`continue-on-error: true`，用于发现量级回归与趋势对比。容量承诺须在受控 host 多样本后用 [capacity-report-template.md](../references/capacity-report-template.md) 单独出具报告，且不得用 transient 数据承诺 durable 产能。

## 7. 选型决策

| 场景 | 推荐路径 |
|---|---|
| 审批工作流（会签/超时/取消/循环退回/signal） | **durable mode**（server+runner，G1/G2 门槛） |
| 采集需要削峰/批量/幂等，复用 server 控制面 | **路径 A**（外部 ingress + server workflow API） |
| 短生命周期采集编排，允许短 TTL、无 inspect | **路径 B**（SDK cluster transient + KafkaTrigger） |
| 需要跨进程分发 + 持久化但非采集 | **cluster durable mode**（NewCluster，非 transient） |

> 不得用路径 B（cluster transient）的容量数据承诺 server/runner 的生产能力。

## 8. 非目标

- Relay Gateway / remote SDK 瘦客户端：仍是规划，不作为 D1 前置。
- streaming / credit-flow control 转生产：实验性传输优化，不作为容量证据。
- handler exactly-once：当前不承诺；由业务幂等或未来 transactional inbox 解决。
