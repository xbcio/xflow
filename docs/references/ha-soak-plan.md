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
| **Redis HA 客户端（sentinel/cluster）** | ❌ 待办 | `backend/distributed/backend.go:239` 仅 `redis.NewClient`，无 `NewFailoverClient`/`NewClusterClient`；asynq transport 同样单节点 |
| **Soak 框架 / 故障注入** | ❌ 待办 | 现有可靠性测试为手动注入（`test/integration/`），无系统化 soak |

## 3. Redis HA 客户端改造（待办）

当前 `redis.NewClient(&redis.Options{Addr: addr})` 是单节点。数据模型已 cluster-ready（hash tag 使 key 共置），但客户端层不支持 sentinel/cluster。

改造范围：
1. `backend/distributed/backend.go:New` — 接受 `redis.UniversalClient` 或 sentinel 配置，替代 `redis.NewClient`。
2. `backend/distributed/internal/queue/asynq/transport.go` — asynq 的 `RedisConnOpt` 需支持 sentinel/cluster（asynq 支持 `asynq.RedisFailoverClientOpt` / `asynq.RedisClusterClientOpt`）。
3. `cmd/server/main.go` — 新增 `--redis-sentinel-master` / `--redis-cluster` flag，区分单节点/sentinel/cluster 模式。
4. `RedisLeaderElector`、`workflowreg`、`triggerRuntime` 均使用注入的 client。`workflowreg` 的 key 已加 `{<key>}` hash tag 使 bykey/byid 共置同 slot（G2 Phase 2 Task 2.1，cluster-safe）；`triggerRuntime` 命名空间 cluster-safety 见 G2 Phase 2 Task 2.2。

验收：sentinel 模式下主从切换期间 leader election 在 TTL 内转移；cluster 模式下 hash tag 保证 key 共置不触发 CROSSSLOT 错误。

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

- [ ] Redis HA 客户端（sentinel/cluster）改造并通过单节点/sentinel/cluster 三种模式测试
- [ ] soak 报告：故障矩阵（§4）、注入时刻、重复 delivery/commit outcome、恢复时间
- [ ] SLO 量化报告（§6）
- [ ] HA 边界声明：明确 control-plane HA 的承诺与不承诺（不承诺 exactly-once）
- [ ] 业务幂等验证：重复 invocation 下副作用只产生一次的证据
