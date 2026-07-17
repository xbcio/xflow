# 发布门槛（Release Gates）

> 本文档定义 xflow server/runner 的分层生产就绪门槛、各层达成状态和部署承诺。
> 门槛定义源自 `.claude/specs/2026-07-17-server-production-readiness-design.md`，本文档与其保持一致，用于快速了解当前可宣称的生产能力。

## 1. 门槛分层

xflow 采用分层发布门槛，不再用单个测试替代完整 release gate。语义约束：handler 与 Runner Protocol 保持 **at-least-once**；生产承诺是不丢调度意图、重复提交不重复推进 DAG，业务副作用由宿主幂等键兜底，**不承诺 handler exactly-once**。

| 门槛 | 适用场景 | 要求 |
|---|---|---|
| **G0 — 调度可靠性候选** | 非生产承诺 | A0～A3：真实 Redis cyclic 恢复、control-plane binder fail-closed、dead-letter 安全处置、跨进程错误语义一致。G0 证明核心调度恢复路径具备候选可靠性，不包含 HA、业务 API 安全或租户承诺。 |
| **G1 — 受限生产（可信单租户）** | 可信网络、单租户、允许控制面维护窗口的复杂审批部署 | G0 + B1（OTel）+ B3（API authz）。workflow/control API 强制认证授权，生产配置不默认匿名；审批工单、权限、不可变审批事件、审计和幂等键由宿主持有；Redis 可恢复部署并完成备份/恢复演练；dead-letter 告警/处置/审计可操作；tracing/metrics/logging 具备值班关联能力。若 control plane 为单副本，明确「不提供无中断 HA SLO」和维护窗口。 |
| **G2 — 多副本 HA / 多租户生产** | 多副本 HA、多租户 | G1 + B2。多租户部署还必须完成 tenant boundary 实现与安全测试。验收包含 Redis HA failover、control-plane kill/restart、重复执行下的业务幂等验证和量化 SLO。 |

> 高吞吐采集不属于 G0/G1/G2 的放宽条件。D1 采用独立 ingress 或 SDK cluster transient 路径，审批工作流始终使用 durable/default mode。

## 2. 达成状态

### G0 — 调度可靠性候选 ✅ 已完成

| 项 | 状态 | Commit | 证据 |
|---|---|---|---|
| A0 真实 Redis cyclic 崩溃恢复 | ✅ | `4a04057` | `test/integration/cyclic_reliability_real_test.go` 覆盖 review reject 后 durable outbox、queue handoff 失败保留 outbox、backend 重建后台重放、重复 delivery 但单一 fenced lease |
| A1 control-plane binder fail-closed | ✅ | `5ee3414` | `service/control/controlplane.go:bindDispatcher()` 缺 `TaskHandlerBinder` 能力时返回配置错误，不回退 `backend.Bind` |
| A2 dead-letter 原子处置契约 | ✅ | `fe15676` | `DeadLetterStore` 能力 + `xflow dead-letter list/replay` CLI，单 Lua 原子 dead→ready，`replay_total{outcome}` 指标。见 [dead-letter-runbook](../references/dead-letter-runbook.md) |
| A3 跨进程错误分类与兼容语义 | ✅ | `1995148`、`6f16260` | 稳定 wire error DTO（kind/code/message/retryable/permanent），保留 `types.ErrPermanent` 分类；`ClassifiedError` 将 IO 错误分类为 permanent/transient |

### G1 — 受限单租户生产 ✅ 已完成

| 项 | 状态 | Commit | 证据 |
|---|---|---|---|
| G0 基线 | ✅ | 见上 | — |
| B1 OTel 端到端接线 | ✅ | `aa5143f` | `observability/tracing/` provider（disabled/stdout/otlp）+ W3C carrier 贯通 server→runner→server；`xflow.task.commit` span；HTTP middleware 为最外层 |
| B3 API authz + fail-closed | ✅ | `c238e1d` | `WorkflowAuthenticator` 接口 + `BearerTokenAuth`（constant-time）；`--api-auth-token`/`--require-api-auth`；生产缺配置启动失败 |

### G2 — 多副本 HA / 多租户 ⏳ 未完成

| 项 | 状态 | 说明 |
|---|---|---|
| B2 control-plane HA + Redis HA soak | ⏳ | 需多副本 + Redis HA 环境，执行 kill/restart、网络中断、主从切换 soak，产出故障矩阵与 SLO。代码侧 leader election/graceful shutdown 已具备，缺 Redis HA 客户端（sentinel/cluster）和 soak 框架 |
| 多租户 tenant boundary | ⏳ | G2 多租户需覆盖 Redis key/索引、workflow registry、runner placement、credential、metrics/log/trace、dead-letter 和审计数据隔离 |

### C — 内部抽象加固（非门槛，已完成）

| 项 | 状态 | Commit | 证据 |
|---|---|---|---|
| C1 store GORM tags 下沉 | ✅ | `4724eec` | `store/models.go` 不携带 ORM schema；`store/sqlstore/models.go` 维护 `dbExecution`/`dbNode`/`dbSignal` 内部持久化类型与转换 |
| C2 Graph 深层不可变性 | ✅ | `b05b9a5` | `engine/graph.Graph` 字段私有化 + 17 个只读 accessor；defensive copy；无 setter |

## 3. G1 部署承诺与限制

**可宣称**：
- 在可信网络、单租户、允许控制面维护窗口的部署下，复杂审批工作流（会签、超时、取消、循环退回、重复 signal）生产可用。
- 调度意图不丢失：durable assignment/lease/outbox 在 Redis 中持久化，控制面重启后恢复；重复提交不重复推进 DAG（fencing）。
- handler 与协议 at-least-once；业务副作用由宿主幂等键保证。

**不可宣称**（见 §4 反声明）：
- 无中断 HA SLO（单副本 G1 部署需维护窗口）。
- handler exactly-once。
- 多租户隔离（G1 是单租户边界）。

## 4. 文档反声明（Release Check）

以下声明**禁止**出现在任何 release 文档中，均已证伪：

- ❌「P1-0 即生产可用」—— G0 只是调度可靠性候选，必须通过 G1 或 G2 才能宣称生产可用。
- ❌「failover 不重执行」—— 系统是 at-least-once，failover 可能重复 invocation，由业务幂等键兜底；不承诺不重执行。
- ❌「trace_id/span_id 可直接充当 OTel parent」—— `ExecutionSnapshot.TraceID/SpanID` 只是审计/检索字段，不是可跨进程恢复的 OTel `SpanContext`；远端 parent 必须用 W3C `traceparent` carrier。
- ❌「只读 replay」—— dead-letter replay 是特权写操作（dead→ready 原子转移），不是只读；必须有权限控制和审计。
- ❌「leader election 等于 control-plane HA」—— leader election 只协调 leader-only maintenance，不提供完整 HA SLO。
- ❌「namespace 是安全边界」—— namespace 和 runner labels 不是安全边界；多租户需独立 tenant boundary 实现。

## 5. 配置要求清单

G1 生产部署必须配置以下能力，详细示例见 [deployment-examples.md](../references/deployment-examples.md)：

| 能力 | 配置 | runbook |
|---|---|---|
| workflow API 鉴权 | `--api-auth-token` + `--require-api-auth` | — |
| runner 协议鉴权 | `--auth-policy runners.yaml` | — |
| 传输安全 | `--tls-cert`/`--tls-key`/`--tls-client-ca`（mTLS） | — |
| 分布式 tracing | `--trace otlp --trace-endpoint ...` | — |
| metrics | `--metrics-addr :9090 --metrics-path /metrics` | [dead-letter-runbook](../references/dead-letter-runbook.md) |
| structured logging | `--log-format json` | — |
| 维护窗口 | SIGTERM graceful shutdown | [maintenance-window-runbook](../references/maintenance-window-runbook.md) |
| Redis 备份/恢复演练 | 部署侧 | [maintenance-window-runbook](../references/maintenance-window-runbook.md) |
