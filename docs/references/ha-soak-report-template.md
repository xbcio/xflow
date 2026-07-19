# xflow HA Soak 报告模板

> **本文件由 Task 5.3 完成（G2 Wave 4 Phase 5 Task 5.3）。**
>
> **填实数据是环境门控（ENV-GATED）**：在真实多副本 control-plane（≥2 xflow-server 实例）+ Redis HA（sentinel/cluster）拓扑中执行 soak 之前，本模板仅包含结构与占位值，**不得作为 HA 生产可用的证据**。
>
> G2 状态见 [RELEASE-GATES.md](../design/RELEASE-GATES.md) §2：B2 control-plane HA + Redis HA soak **⏳ 未完成**。在本门槛达成前，禁止宣称 xflow 已具备多副本 HA 生产可用性。
>
> 详细方法论、故障矩阵与 SLO 目标见 [ha-soak-plan.md](ha-soak-plan.md)。

---

## 1. 执行摘要

| 项 | 模板值 / 占位说明 |
|---|---|
| 报告版本 | `0.1`（template，Task 5.3） |
| 执行日期 | <!-- SOAK_DATA: ISO-8601 date --> |
| 执行拓扑 | <!-- SOAK_DATA: e.g. 3 xflow-server + sentinel Redis + 2 xflow-runner --> |
| xflow 版本 / commit | <!-- SOAK_DATA: git commit hash --> |
| 记录人 | <!-- SOAK_DATA: operator --> |
| 总体结论 | <!-- SOAK_DATA: PASS / FAIL / ENV-GATED（未执行） --> |

> 模板阶段结论固定为 **ENV-GATED（未执行）**。真实 soak 完成后替换为 PASS/FAIL 并附证据 artifact。

## 2. 环境配置

| 组件 | 配置 | 备注 |
|---|---|---|
| control-plane 副本数 | <!-- SOAK_DATA: N --> | 必须 ≥2 |
| Redis 模式 | <!-- SOAK_DATA: single / sentinel / cluster --> | HA soak 要求 sentinel 或 cluster |
| Redis 版本 | <!-- SOAK_DATA: e.g. 7.2 --> | |
| runner 数量 | <!-- SOAK_DATA: M --> | 必须 ≥2 |
| 网络注入工具 | <!-- SOAK_DATA: iptables / tc / netns --> | 用于 NetworkPartition |
| soak 持续时间 | <!-- SOAK_DATA: e.g. 4h --> | |
| 工作流负载 | <!-- SOAK_DATA: e.g. 100 approval workflows/min --> | |

## 3. 故障矩阵

每行对应 [ha-soak-plan.md](ha-soak-plan.md) §4 的一个故障类。注入时刻使用 ISO-8601；恢复时间从注入开始到 SLO 收敛；commit outcome 描述 fenced commit 结果（once / duplicate-terminal / 等）。

| 故障 | 注入时刻 | 预期不变量 | 实际恢复时间 | duplicate delivery | commit outcome | 结果 |
|---|---|---|---|---|---|---|
| **leader kill** | <!-- SOAK_DATA: time --> | §5.3：非 leader 在 ≤ TTL 内接管；单一 maintenance leader | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **leader restart** | <!-- SOAK_DATA: time --> | §5.1：重启后重新 Campaign；assignment/lease/outbox 不丢失 | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **Redis 主从切换** | <!-- SOAK_DATA: time --> | §5.1：切换期间可重试；切换后 leader 重新获取；无数据丢失 | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **网络分区（server↔Redis）** | <!-- SOAK_DATA: time --> | §5.3/§5.4：分区侧失去 leadership；恢复后重新 Campaign；无双 leader | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **runner kill** | <!-- SOAK_DATA: time --> | §5.4/§5.5：lease 过期被回收重投递；另一 runner 收敛至终态 | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **响应丢失（runner→server）** | <!-- SOAK_DATA: time --> | §5.2/§5.5：runner reconnect replay 同一 lease；fenced commit 不重复推进 DAG | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |
| **outbox flush 前故障** | <!-- SOAK_DATA: time --> | §5.1/§5.4：server 重启后 outbox dispatcher 重放 pending intent | <!-- SOAK_DATA: duration --> | <!-- SOAK_DATA: count --> | <!-- SOAK_DATA: once / duplicate-terminal --> | <!-- SOAK_DATA: PASS / FAIL / TBD --> |

### 3.1 故障注入日志索引

<!-- SOAK_DATA: 链接到外部日志 / tracing / metrics artifact，例如 Grafana explore、Jaeger trace、S3 artifact。模板阶段为空。 -->

## 4. SLO 指标

指标与阈值来自 [ha-soak-plan.md](ha-soak-plan.md) §6。

| 指标 | 目标 | 实测值 | 达标判定 |
|---|---|---|---|
| **API 可用性（多副本）** | ≥ 99.9%（维护窗口外） | <!-- SOAK_DATA: e.g. 99.95% --> | <!-- SOAK_DATA: PASS / FAIL --> |
| **leader 切换时间** | ≤ 3 × TTL（45s） | <!-- SOAK_DATA: e.g. 4.2s (worst) --> | <!-- SOAK_DATA: PASS / FAIL --> |
| **恢复时间（outbox/lease 重放）** | ≤ 30s | <!-- SOAK_DATA: e.g. 2.1s (worst) --> | <!-- SOAK_DATA: PASS / FAIL --> |
| **重复 invocation 率** | 记录，不设上限 | <!-- SOAK_DATA: e.g. 0.12% --> | 仅记录 |
| **错误率（稳态 5xx）** | < 1% | <!-- SOAK_DATA: e.g. 0.05% --> | <!-- SOAK_DATA: PASS / FAIL --> |

> 单副本 G1 部署**不**适用上述 SLO；G1 需维护窗口，详见 [RELEASE-GATES.md](../design/RELEASE-GATES.md) §3。

## 5. 业务幂等证据

在重复 invocation 下验证业务副作用只产生一次（[ha-soak-plan.md](ha-soak-plan.md) §7）：

| 验证项 | 方法 | 结果 |
|---|---|---|
| 宿主幂等键生效 | <!-- SOAK_DATA: 抽查重复 invocation 的宿主副作用日志 / idempotency-key 去重记录 --> | <!-- SOAK_DATA: PASS / FAIL / N/A --> |
| `CommitTaskResult` 返回 `DuplicateTerminal` | <!-- SOAK_DATA: 在 report-response-loss / runner-kill 场景中观察重复 commit 返回值 --> | <!-- SOAK_DATA: PASS / FAIL / N/A --> |
| 终态后 replay 为 no-op 或 `rejected_terminal` | <!-- SOAK_DATA: 检查 outbox / dead-letter replay 在终态后的行为 --> | <!-- SOAK_DATA: PASS / FAIL / N/A --> |

> xflow 承诺 **at-least-once** delivery / invocation，**不承诺 handler exactly-once**。业务 side effect 的 exactly-once 由宿主幂等键兜底。

## 6. 已知限制与诚实声明

1. **Redis HA 客户端（sentinel/cluster）**：截至本模板生成时，`backend/distributed/backend.go` 仍使用单节点 `redis.NewClient`；sentinel/cluster 模式未完成。真实 Redis 主从切换 soak 须等待该改造完成，详见 [ha-soak-plan.md](ha-soak-plan.md) §3。
2. **in-process harness 限制**：`test/soak/` 中的 in-process 故障注入仅验证 leader graceful transfer（LeaderKill / LeaderRestart），其余 5 类故障（RedisFailover、NetworkPartition、RunnerKill、ReportResponseLoss、OutboxFlushFail）返回 `ErrEnvGated`，不在 miniredis 中伪装执行。
3. **重复 invocation 率分母**：`SLOReport.DuplicateInvocationRate` 当前以 API 样本数为分母；真实 soak 应结合宿主 idempotency-key 日志修正为“重复 delivery / 总 delivery”。
4. **模板数据门控**：本文件所有 `<!-- SOAK_DATA: ... -->` 占位值在真实 soak 完成前不得替换为伪造数值。

## 7. 结论与签核

| 角色 | 签名 | 日期 |
|---|---|---|
| 测试执行人 | <!-- SOAK_DATA: sign --> | <!-- SOAK_DATA: date --> |
| SRE / 发布负责人 | <!-- SOAK_DATA: sign --> | <!-- SOAK_DATA: date --> |
| 架构负责人 | <!-- SOAK_DATA: sign --> | <!-- SOAK_DATA: date --> |

**最终声明**：

> 在 [RELEASE-GATES.md](../design/RELEASE-GATES.md) §2 将 G2 状态更新为“已完成”之前，xflow **不得**对外宣称具备多副本 HA 生产可用性。本模板只是 G2 交付物之一，填实数据需真实环境 soak 并经过本签核流程。
