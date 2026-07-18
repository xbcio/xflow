# 发布门槛（Release Gates）

> 本文档定义 xflow server/runner 的分层生产就绪门槛、各层达成状态和部署承诺。
> 门槛定义源自 `.claude/specs/2026-07-17-server-production-readiness-design.md`，本文档与其保持一致，用于快速了解当前可宣称的生产能力。

> **2026-07-18 事实纠正**：经生产就绪复核（见
> `.claude/specs/2026-07-18-sdk-server-production-readiness-remediation-design.md`），
> 此前 G0/G1/C2 标记"已完成"与代码事实不符，已回退。状态以本节为准；
> 任一条目恢复"已完成"必须满足该 spec §2 的三层证据（实现 + 测试 + 运行 artifact）。

## 1. 门槛分层

xflow 采用分层发布门槛，不再用单个测试替代完整 release gate。语义约束：handler 与 Runner Protocol 保持 **at-least-once**；生产承诺是不丢调度意图、重复提交不重复推进 DAG，业务副作用由宿主幂等键兜底，**不承诺 handler exactly-once**。

| 门槛 | 适用场景 | 要求 |
|---|---|---|
| **G0 — 调度可靠性候选** | 非生产承诺 | A0～A3：真实 Redis cyclic 恢复、control-plane binder fail-closed、dead-letter 安全处置、跨进程错误语义一致。G0 证明核心调度恢复路径具备候选可靠性，不包含 HA、业务 API 安全或租户承诺。 |
| **G1 — 受限生产（可信单租户）** | 可信网络、单租户、允许控制面维护窗口的复杂审批部署 | G0 + B1（OTel）+ B3（API authz）。workflow/control API 强制认证授权，生产配置不默认匿名；审批工单、权限、不可变审批事件、审计和幂等键由宿主持有；Redis 可恢复部署并完成备份/恢复演练；dead-letter 告警/处置/审计可操作；tracing/metrics/logging 具备值班关联能力。若 control plane 为单副本，明确「不提供无中断 HA SLO」和维护窗口。 |
| **G2 — 多副本 HA / 多租户生产** | 多副本 HA、多租户 | G1 + B2。多租户部署还必须完成 tenant boundary 实现与安全测试。验收包含 Redis HA failover、control-plane kill/restart、重复执行下的业务幂等验证和量化 SLO。 |

> 高吞吐采集不属于 G0/G1/G2 的放宽条件。D1 采用独立 ingress 或 SDK cluster transient 路径，审批工作流始终使用 durable/default mode。

## 2. 达成状态

### G0 — 调度可靠性候选 🛠 修复中

> 此前标记"已完成"已回退。A0/A1/A2/A3 均为部分完成，存在已知缺口，详见
> [sdk/server 生产就绪验收修复设计](../../.claude/specs/2026-07-18-sdk-server-production-readiness-remediation-design.md) §6。

| 项 | 状态 | 缺口 | 既有 commit |
|---|---|---|---|
| A0 真实 Redis cyclic 崩溃恢复 | 🟡 部分完成 | 测试代码存在但本地因 Redis 不可用被 Skip；无独立 OS 进程 kill/restart 恢复报告；CI 未强制 require-real-Redis | `4a04057` |
| A1 control-plane binder fail-closed | 🟡 部分完成 | control 已不回退 `Provider.Bind`；但 distributed 吞掉 `StartConsumer` 错误，dispatcher/monitor 仍启动并报 ready；无逆序回滚与 goroutine 等待 | `5ee3414` |
| A2 dead-letter 原子处置契约 | 🟡 部分完成 | 原子 dead→ready 已有；缺 node/activation guard、不可变 replay receipt、统一 manager、真正 cursor 分页、CLI 绕过 Engine | `fe15676` |
| A3 跨进程错误分类与兼容语义 | 🟡 部分完成 | wire DTO + `ClassifiedError` 完成；HTTP 408/429 transient、Database 按 SQLState/number 分类、gRPC codes 表完成；**三拓扑 parity 矩阵已完成**（local embedded + server-runner，真 Redis）：覆盖 HTTP 4xx/408/429/5xx/connection、gRPC 全 status codes 表（no-pool/NotFound/Unavailable/connection + InvalidArgument/PermissionDenied/Unauthenticated/AlreadyExists/Unimplemented/FailedPrecondition/OutOfRange/DeadlineExceeded/ResourceExhausted/Aborted/Canceled/Unknown/Internal/DataLoss）、DB no-pool/bad-conn（server-runner 真实 mysql driver 判为 `database.unknown`，与 local `database.connection_lost` 属同 transient 分支，仅 code 字面不同）/deadlock(1205)/constraint、OnError stop/error_output/main_output/continue、script/function config/timeout/user-error。cluster-transient 因无 handler retry 为 collection-path 排除。**生产 runner ResourcePool/credential resolver 接线已完成**（`execution.WithCredentialResolver` + `runnersvc.Config` 透传 + `cmd/runner` YAML `credentials`/`resource_pool` 段 + `${VAR}` env 展开 fail-closed；无 public API break；生产路径 parity 测试变体已覆盖 db+grpc）；no-pool 契约（无 credentials/db/grpc 时不构造 pool）保留。清单关闭前 A3 不标 G0 完成 | `1995148`、`6f16260`、`1815635`、`987a8ff`、`31338fb`、`681ba6f`、`6bdc794`、`bdf0ad3`、`81d6b58`、`1430bda`、`8cc51d3`、`8abb1c2`、`bfb1e9f`、`e38a1c5`、`d0a2352`、`aae23ef`、`472a16a`、`e007c97`、`69fdcde` |

> G0 退出条件见修复设计 §12。在 A0–A3 全部满足三层证据前，G0 不得恢复"已完成"。

### G1 — 受限单租户生产 ⛔ 未满足

> G1 依赖 G0。G0 修复中故 G1 阻断；B1/B3 为 G1 阻断项，亦未满足。

| 项 | 状态 | 缺口 | 既有 commit |
|---|---|---|---|
| G0 基线 | ⛔ | G0 修复中 | 见上 |
| B1 OTel 端到端接线 | ✅ 修复完成（G0 阻断 G1） | provider 可配 sampler（默认 parentbased）+ baggage opt-in + 幂等 shutdown；runner extract `TaskLease.TraceCarrier` 建 execute span、report inject carrier（`WithoutCancel` 保留 SpanContext 不再裸 `context.Background()` 断链）；dispatch span；cmd flags；gRPC unary interceptor（W3C metadata extraction + server span）；runner trace-graph + grpc interceptor 测试 green。server-runner e2e（HTTP + gRPC）已修复并真 Redis 跑通（此前 `control.NewServer` 不再 serve `/v1/workflows` 致 404，且 Redis 在 :6380 非 :6379 长期 skip 掩盖）。⚠️ gRPC runner concurrency>1 credit-flow 路径 double-dispatch 为实验性后续（测试 docstring 已标注，非 release gate） | `0b21125`、`1815635` |
| B3 API authz + fail-closed | 🟡 部分完成 | 仅有静态 bearer 认证 + fail-closed；无 principal、无资源/操作级 authz、无不可变可 reconcile 审计；所有 operation 共享 allow-all | `c238e1d` |

> G1 退出条件见修复设计 §12。即便 B1/B3 单项完成，G0 未完成前不得宣称 G1。

### G2 — 多副本 HA / 多租户 ⏳ 未完成

| 项 | 状态 | 说明 |
|---|---|---|
| B2 control-plane HA + Redis HA soak | ⏳ | 代码侧：leader election/graceful shutdown/management 端点已具备；缺 Redis HA 客户端（sentinel/cluster）和 soak 框架。详见 [ha-soak-plan](../references/ha-soak-plan.md) |
| 多租户 tenant boundary | ⏳ | G2 多租户需覆盖 Redis key/索引、workflow registry、runner placement、credential、metrics/log/trace、dead-letter 和审计数据隔离 |

> leader election、hash tag 或 namespace 单独存在都不等同于 control-plane HA 或多租户隔离。本轮 G0/G1 修复不得顺带宣称 G2。

### C — 内部抽象加固（非门槛）

| 项 | 状态 | 缺口 | Commit |
|---|---|---|---|
| C1 store GORM tags 下沉 | ✅ 完成 | `store/models.go` 已无 `gorm:` tag 与 `TableName()`；ORM schema 完全下沉至 `store/sqlstore` 的 `dbExecution`/`dbNode`/`dbSignal` | `a36f19b` |
| C2 Graph 深层不可变性 | ✅ 完成 | 公开 accessor 全部深层不可变：`NodeAt` 递归拷贝 `Parameters`/`PortOuts`/`RunnerSelector`/`Retry`；`Vars()`/`Config()` 递归深拷贝；`NodeOutEdges`/`NodeInEdges` 返回新 slice。Compile 阶段 value-domain 校验拒绝 pointer/func/chan/非字符串键 map。热路径 `NodeName` 零拷贝。深层 mutation + race + 值域拒绝 + accessor benchmark 通过（per-dispatch 隔离拷贝 ~0.9µs，handler 不可信不可避免） | `7355570` |

### D — 高吞吐路径（独立架构，不阻塞审批 release gate）

| 项 | 状态 | 说明 |
|---|---|---|
| D1 采集与审批工作流分离 | 🟡 架构/采样就绪，容量基线未完成 | 两条独立参考架构 + `make perf-sample` 采样脚本就绪；E2E load 输出 p50/p95/p99 + 结构化 `perf.metric` 行；CI 采样 job `.github/workflows/perf-sample.yml`（每日 + 手动，`continue-on-error`，非门槛，上传 90 天 artifact）已接入；受控 host 多样本报告模板 [capacity-report-template.md](../references/capacity-report-template.md) 已就绪。**受控 host 多样本报告未填实前只能称"架构与采样准备完成"，不得称"容量基线完成"**。详见 [HIGH-THROUGHPUT-INGESTION](HIGH-THROUGHPUT-INGESTION.md)。审批工作流始终使用 durable mode；不得用 transient 容量承诺审批产能 |

## 3. G1 部署承诺与限制

> 以下为 G1 达成后的目标承诺。**当前 G1 未满足**（见 §2），以下承诺在 G0 + B1 + B3 全部满足三层证据前不得对外宣称。

**达成后可宣称**：
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
- ❌「测试被 skip 即 integration 已通过」—— integration test 因依赖不可用 Skip 只能记为"未执行"，不能记为通过；CI 缺依赖时必须 fail-fast。
- ❌「字段 private 即 Graph 深层不可变」—— 字段私有化只是第一步；accessor 浅拷贝仍会泄漏可变引用，必须深层 defensive copy + Compile 校验才可称 deep immutable。
- ❌「单一共享 token 即 API authz 完成」—— 静态 bearer 只是单租户参考实现；必须有 principal + 资源/操作级 authz + 不可变审计，不得 allow-all。

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
