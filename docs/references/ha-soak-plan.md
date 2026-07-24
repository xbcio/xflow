# B2 控制面 HA 与 Redis HA Soak 方案

> 本文档定义 B2（G2 门槛）的 soak 验证方法论、故障矩阵、不变量和 SLO 目标。
> 门槛定义见 [RELEASE-GATES.md](../design/RELEASE-GATES.md)。B2 未完成前不得宣称完整 control-plane HA。

## 1. 前置依赖

B2 soak 需要**真实**分布式环境，非单机可完整覆盖：

- **多副本 control-plane**：≥2 个 `xflow-server` 实例，共享同一 Redis。
- **Redis HA**：sentinel 管理的主从，或 Redis Cluster。
- **可控故障注入**：进程 kill/restart、网络分区、Redis 主从切换、响应丢失。
- **runner 集群**：≥2 个 `xflow-runner`，验证执行面横向扩缩容。

## 2. 当前代码侧状态

| 能力 | 状态 | 位置 |
|---|---|---|
| Leader election | ✅ | `backend/distributed/leader.go` `RedisLeaderElector`，SETNX + Lua 续约/释放，TTL 15s |
| Leader-only maintenance 门控 | ✅ | `service/control/lease_sweeper.go` SweepOnce 在非 leader 时跳过 |
| Graceful shutdown（Resign + unbind） | ✅ | `service/apiserver/run.go` + `service/control/controlplane.go:Shutdown` |
| Health/readiness/leader 端点 | ✅ | `module_management.go`，`--management` 启用，`/v1/management/*` 由 `ManagementAuthMiddleware` 门控 |
| Redis key cluster-ready（hash tag） | ✅ | `backend/distributed/internal/rstate/keys.go` 全部 key 用 `{id}` hash tag |
| **Redis HA 客户端（sentinel/cluster）** | ✅ 代码完成 | Task 1.1–4.1：`redis.UniversalClient` 宽化 + `RedisConfig`（single/sentinel/cluster）+ `WithRedisConfig` Option + asynq `AsAsynqConnOpt` 三模式映射 + `cmd/server --redis-mode` flag + `apiserver.Config.RedisConfig` 透传 + sentinel 认证字段（commits `9a39996`..`c292d7d`）。**真实 sentinel/cluster 环境连通性/failover 验收仍 ENV-GATED**（见 §3） |
| **Soak 框架 / 故障注入** | ✅ 代码完成 | Task 5.1–5.3：in-process harness + 7 故障注入器（leader kill/restart 真实执行；RedisFailover/NetworkPartition/RunnerKill/ReportResponseLoss/OutboxFlushFail 后 5 类返回 `ErrEnvGated`）+ SLO 报告类型 + 报告模板（commits `dc13301`..`065974a`）。**填实报告仍 ENV-GATED** |

## 3. Redis HA 客户端改造（代码完成，验收 ENV-GATED）

代码层改造已完成（Task 1.1–4.1，commits `9a39996`..`c292d7d`）：
1. ✅ `backend/distributed/backend.go:New` — 已接受 `redis.UniversalClient`（Task 1.1 宽化 rdb 类型 + Task 1.2 `RedisConfig`（single/sentinel/cluster）+ `WithRedisConfig` Option）。
2. ✅ `backend/distributed/internal/queue/asynq/transport.go` — asynq transport 已接 `RedisConnOpt`，经 `AsAsynqConnOpt` 三模式映射（Task 3.1）。
3. ✅ `cmd/server/main.go` — `--redis-mode` flag + `apiserver.Config.RedisConfig` 透传 + sentinel 认证字段（Task 4.1）。
4. ✅ `RedisLeaderElector`、`workflowreg`、`triggerRuntime` 均使用注入的 client。`workflowreg` 的 key 已加 `{<key>}` hash tag 使 bykey/byid 共置同 slot（G2 Phase 2 Task 2.1，cluster-safe）；`triggerRuntime` 单 key 操作无需 hash tag（G2 Phase 2 Task 2.2 核查确认）。

**仍 ENV-GATED**（miniredis 不能模拟）：
- sentinel 模式下主从切换期间 leader election 在 TTL 内转移；
- cluster 模式下 hash tag 保证 key 共置不触发 CROSSSLOT 错误（真实 cluster 回归）；
- single/sentinel/cluster 三模式连通性 + failover 行为验收。

## 4. 故障矩阵

每个故障注入点记录：注入时刻、预期不变量、恢复时间、重复 invocation/commit 次数。

| 故障 | 注入方式 | 预期不变量 | 验证 |
|---|---|---|---|
| **leader kill** | 终止 leader 进程 | 非 leader 在 ≤ TTL（15s）内接管；leader-only maintenance 单一持有 | `/v1/management/leader` 切换；同一时刻只有一个 maintenance leader |
| **leader restart** | kill 后重启同一进程 | 重启进程重新 Campaign；既有 assignment/lease/outbox 不丢失 | 提交的 workflow 继续推进到终态 |
| **Redis 主从切换** | sentinel 触发 failover | 切换期间短暂失败可重试；切换后 leader 重新获取；无数据丢失 | ready intent/assignment/lease/terminal result 完整 |
| **网络分区（server↔Redis）** | iptables 阻断 | 分区侧进程失去 leadership（lease 过期）；恢复后重新 Campaign | 无双 leader |
| **响应丢失（runner→server）** | 丢弃 report response | runner reconnect replay 同一 lease；fenced commit 不重复推进 DAG | 同一 activation 只被一个有效 lease 接受，下游只推进一次 |
| **runner kill** | 终止 runner 进程 | lease 过期被 sweeper 回收/重投递；另一 runner 接管 | 工作流最终收敛 |
| **outbox flush 前故障** | commit 成功后、flush 前 kill server | 后台 dispatcher 重启后重放 outbox | 下游调度 intent 不丢失 |

## 5. 不变量

soak 期间始终成立：

1. **ready intent / assignment / lease / terminal result 不丢失**——durable 状态全在 Redis。
2. **同一 fenced result 不重复推进 DAG**——`CommitTaskResult` 以 lease token 为 fencing token，重复 commit 返回 `DuplicateTerminal`。
3. **同一时刻只有一个有效 maintenance leader**——`RedisLeaderElector` SETNX + token 释放。
4. **claim expiry / lease replay / outbox replay / leader maintenance 最终收敛**——独立恢复循环，以 Redis 权威状态为准。
5. **允许 handler invocation / queue delivery 重复，但业务副作用只产生一次**——宿主幂等键。

## 6. SLO 目标（待 soak 量化）

soak 报告须量化以下指标，阈值为示例基线，受控 host 多样本后设定：

| 指标 | 目标 | 说明 |
|---|---|---|
| API 可用性（多副本） | ≥ 99.9%（维护窗口外） | 任一副本可服务读；写入路由 leader |
| leader 切换时间 | ≤ 3 × TTL（45s） | kill 到新 leader 接管 |
| 恢复时间（outbox/lease 重放） | ≤ 30s | dispatcher 扫描间隔默认 1s |
| 重复 invocation 率 | 记录，不设上限 | at-least-once 允许重复；记录用于幂等键验证 |
| 错误率 | < 1%（稳态） | 5xx 占比 |

> 单副本 G1 部署**不**适用此 SLO——单副本无 failover，需维护窗口。

## 7. 业务幂等验证

soak 在重复 invocation 下验证业务副作用只产生一次：

- 审批副作用（外部 API 调用、状态写入）使用宿主幂等键。
- 重复 commit 返回 `DuplicateTerminal`，不触发下游重复推进。
- 终态后 replay（outbox / dead-letter）是 no-op 或 `rejected_terminal`。

> xflow 不承诺 handler exactly-once。若业务要求 exactly-once，需另立 transactional inbox / idempotency contract。

## 8. 现有可靠性测试基线

soak 前应已通过的真实 Redis 测试：

| 测试 | 文件 | 覆盖 |
|---|---|---|
| 原子可靠性 | `test/integration/atomic_reliability_real_test.go` | 队列中断期间持久 commit/outbox 恢复 |
| 循环可靠性 | `test/integration/cyclic_reliability_real_test.go` | reject/re-entry/重建/重复提交/事件循环 |
| leader 选举 | `test/integration/leader_real_test.go` | kill 后 TTL 内 leader 切换 |

soak 在此基线上扩展为多副本 + Redis HA + 长时间运行的故障矩阵。

## 9. 交付物

B2 完成须产出：

- [x] Redis HA 客户端（sentinel/cluster）代码改造完成（Task 1.1–4.1，commits `9a39996`..`c292d7d`）— single/sentinel/cluster 三模式真实环境连通性验收 **ENV-GATED**
- [ ] soak 报告：故障矩阵（§4）、注入时刻、重复 delivery/commit outcome、恢复时间 — 框架与模板已就绪（Task 5.1–5.3），填实报告 **ENV-GATED**
- [ ] SLO 量化报告（§6）— 采集代码已就绪（Task 5.3），量化数据 **ENV-GATED**
- [x] HA 边界声明：明确 control-plane HA 的承诺与不承诺（不承诺 exactly-once）— 见 [RELEASE-GATES.md](../design/RELEASE-GATES.md) §2 G2 段「G2 control-plane HA 承诺与边界声明」
- [ ] 业务幂等验证：重复 invocation 下副作用只产生一次的证据 — 采集位已就绪（Task 5.2/5.3），真实样本 **ENV-GATED**
