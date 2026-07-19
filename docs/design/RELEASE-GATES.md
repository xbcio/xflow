# 发布门槛（Release Gates）

> 本文档定义 xflow server/runner 的分层生产就绪门槛、各层达成状态和部署承诺。
> 门槛定义源自 `.claude/specs/2026-07-17-server-production-readiness-design.md`，本文档与其保持一致，用于快速了解当前可宣称的生产能力。

> **2026-07-18 事实纠正**：经生产就绪复核（见
> `.claude/specs/2026-07-18-sdk-server-production-readiness-remediation-design.md`），
> 此前 G0/G1/C2 标记"已完成"与代码事实不符，已回退。状态以本节为准；
> 任一条目恢复"已完成"必须满足该 spec §2 的三层证据（实现 + 测试 + 运行 artifact）。
>
> **2026-07-19 验收纠正**：对 HEAD `fdc0206` 的复验确认 G0/G1 仍不通过、G2 仍未完成，
> 并再次证伪 B1“修复完成”、A3“三拓扑完成”和 C2“深层不可变完成”的声明。
> 详细证据见 `.claude/specs/2026-07-19-sdk-server-production-readiness-acceptance.md`；
> 后续任务见 `.claude/plans/2026-07-19-sdk-server-production-readiness-followup.md`。

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
| A0 真实 Redis cyclic 崩溃恢复 | 🟡 部分完成 | require-real-Redis helper、真实 Redis cyclic test 和双进程 recovery test 已存在；但 CI 仍调用可 Skip 的 `make test-integration`，没有强制 required 模式或上传 A0 artifact。process test 仅覆盖 commit 后 flush 前退出/后台恢复，未完成 response-loss、真实 queue handoff、OS kill 矩阵；报告字段未填全且只写入临时目录。2026-07-19 本机强制运行因 `localhost:6379` 不可用而 fail-fast，不能计为通过 | `4a04057`；2026-07-19 验收 |
| A1 control-plane binder fail-closed | 🟡 部分完成 | control-plane 已只走 `TaskHandlerBinder`，`BindTaskHandler` 会传播 consumer start error，正常 stop/race 测试存在；但 SDK `Provider.Bind`/兼容 `BindHandler` 仍只记录 bind 错误并返回 cleanup，`NewCluster` 调用方不可观察失败。timeout monitor 启动后的 rollback 分支未 `Stop`/等待，正常 stop 也未先阻止新消费 | `5ee3414`；2026-07-19 验收 |
| A2 dead-letter 原子处置契约 | 🟡 部分完成 | 结构化请求、成功 receipt、并发幂等、execution/node/activation guard 和 manager 基础已实现；但 legacy/missing meta 会跳过 guard，拒绝 outcome 无权威 receipt。cursor 暴露 raw entry ID 且真实 Redis 稳定分页未证明；API/CLI manager 的 metrics 为 nil，CLI 仍直连 Redis并自行注入身份，Engine 仍有 manager 外 replay 路径；Redis receipt→SQL crash-safe reconcile 未实现 | `fe15676`；2026-07-19 验收 |
| A3 跨进程错误分类与兼容语义 | 🟡 部分完成 | wire DTO、HTTP/gRPC/MySQL/script/function classifier、local embedded + server-runner parity 及生产 runner ResourcePool/credential wiring 已实现；**当前只有两种拓扑**，`test/integration/action_parity_test.go` 明确排除 SDK cluster，故不得称“三拓扑完成”。同 fixture 的 structured kind/code/port/DAG 断言仍不完整，PostgreSQL classifier/运行范围未决，且没有附本轮真实 Redis parity artifact | `1995148`..`69fdcde`；2026-07-19 验收 |

> G0 退出条件见修复设计 §12。在 A0–A3 全部满足三层证据前，G0 不得恢复"已完成"。

### G1 — 受限单租户生产 ⛔ 未满足

> G1 依赖 G0。G0 修复中故 G1 阻断；B1/B3 为 G1 阻断项，亦未满足。

| 项 | 状态 | 缺口 | 既有 commit |
|---|---|---|---|
| G0 基线 | ⛔ | G0 修复中 | 见上 |
| B1 OTel 端到端接线 | 🟡 部分完成 | provider sampler、TraceContext、baggage opt-in、runner execute span、`WithoutCancel` report context、HTTP middleware 和 gRPC unary interceptor 已实现；但 gRPC `ReportResultRequest` proto/converter 不携带 `TraceCarrier`，report→commit 会回退到 dispatch carrier。缺 workflow submit/invoke、task report、outbox spans，dispatch 也未继承 submit/invoke 因果链；无 stream interceptor、baggage allowlist和 HTTP/gRPC 完整 graph artifact。2026-07-19 round-trip 探针确认 carrier 丢失 | `0b21125`、`1815635`；2026-07-19 验收 |
| B3 API authz + fail-closed | 🟡 部分完成 | Principal、operation/resource Authorizer、默认 deny、tenant IDOR、mutation admission audit 和 SQL append-only sink 已实现；但 execution 子路由整体被固定成 `execution.read` 非 mutation，signal/revoke/cancel 未按操作授权审计，management leader/runner 未统一 authz。无 MySQL 时 server 仍可使用内存 audit 启动，单 token 自动拥有全部 scope；post-handler defer 不是 crash-safe reconcile，dead-letter receipt 也未投影/对账到 SQL | `d3bf6ba`、tenant boundary commits；2026-07-19 验收 |

> G1 退出条件见修复设计 §12。即便 B1/B3 单项完成，G0 未完成前不得宣称 G1。

### G2 — 多副本 HA / 多租户 ⏳ 未完成

| 项 | 状态 | 说明 |
|---|---|---|
| B2 control-plane HA + Redis HA soak | ⏳ | 代码侧：leader election/graceful shutdown/management 端点已具备；**Redis HA 客户端代码已完成**（Task 1.1–4.1：`redis.UniversalClient` 宽化 + `RedisConfig` single/sentinel/cluster + `WithRedisConfig` Option + asynq `AsAsynqConnOpt` 三模式映射 + `cmd/server --redis-mode` flag + `apiserver.Config.RedisConfig` 透传 + sentinel 认证字段，commits `9a39996`..`c292d7d`）；**workflowreg/trigger cluster-safety 已修**（Task 2.1 hash tag + Task 2.2 单 key 核查，commits `aed4db3`/`1c54be0`）；**soak 脚手架与报告类型已完成**（Task 5.1 harness + 5.2 in-process graceful 场景/ENV-GATED stub + 5.3 SLO 报告类型与模板，commits `dc13301`..`065974a`）；Redis failover、network partition、runner kill、report response loss、outbox flush fault 仍未在真实环境执行。**仍缺（ENV-GATED）**：真实 sentinel/cluster Redis 环境连通性/failover 验收、多副本 soak 报告填实、SLO 量化达标判定、真实 cluster 下 CROSSSLOT 回归。详见 [ha-soak-plan](../references/ha-soak-plan.md) 与 [ha-soak-report-template](../references/ha-soak-report-template.md) |
| 多租户 tenant boundary | ⏳ 代码与测试完成 | tenant boundary 全链路代码已完成（Phase 6-8）：`backend/tenant` context 原语 + `WorkflowDef.TenantID`（`json:"-"` 编译期禁止不可信客户端，commit `726e6df`）；rstate/workflowreg/trigger key 全部 tenant 前缀（`xflow:t<tenant>:...`，无花括号保 hash tag，Task 7.1/7.2）；API 层 principal 签发 + `TenantAwareAuthorizer` + IDOR（请求体 tenant 忽略，跨 tenant → 404 不泄漏存在性，Task 7.3）；audit/metrics/trace tenant 标签 + 高基数防护（Task 7.4）；runner placement + credential tenant scope + asynq payload tenant（Task 7.5）；dead-letter/outbox manager 双保险 + CLI `--tenant`（Task 7.6）；越权测试矩阵 `test/security/tenant_isolation_test.go` 10 场景全绿（Task 8.1，miniredis，非 ENV-GATED）。**仍缺（ENV-GATED）**：真实多租户部署的端到端验收（多 principal 并发、跨 tenant 性能隔离基线、真实 Redis Cluster 下 tenant key 分布）。详见 [tenant-boundary-design](../../.claude/specs/2026-07-19-tenant-boundary-design.md) |

> leader election、hash tag 或 namespace 单独存在都不等同于 control-plane HA 或多租户隔离。本轮 G0/G1 修复不得顺带宣称 G2。

#### G2 control-plane HA 承诺与边界声明

G2 control-plane HA 的承诺范围与限制（映射 §4 反声明，在 G2 整体达成前不得对外宣称）：

- **承诺 at-least-once，不承诺 exactly-once**：handler 与 Runner Protocol 保持 at-least-once；failover 或 lease replay 可能重复 invocation，业务副作用只产生一次依赖宿主幂等键兜底（映射 §4「failover 不重执行」反声明）。
- **leader election 仅协调 leader-only maintenance**：`RedisLeaderElector` SETNX + Lua 续约/释放只用于 gate `lease_sweeper` 等维护任务，不提供完整 HA SLO；「leader election 等于 control-plane HA」是反声明（§4）。
- **Redis HA 客户端代码就绪 ≠ control-plane HA 已验收**：`UniversalClient` 宽化与 sentinel/cluster 构造代码落地（Task 1.1–4.1）只保证"可配置 sentinel/cluster 模式"，真实多副本 soak + SLO 量化是 ENV-GATED；未填实 [ha-soak-report-template](../references/ha-soak-report-template.md) 前 G2 不得标完成。
- **hash tag / namespace / leader election 单独存在 ≠ HA 或多租户隔离**：hash tag 只保证 Redis Cluster 下 key 共置同 slot（不触发 CROSSSLOT），不是 HA 承诺；namespace 是命名空间隔离，不是安全边界；leader election 不等于 control-plane HA。三者均映射 §4 反声明。

### C — 内部抽象加固（非门槛）

| 项 | 状态 | 缺口 | Commit |
|---|---|---|---|
| C1 store GORM tags 下沉 | ✅ 完成 | `store/models.go` 已无 `gorm:` tag 与 `TableName()`；ORM schema 完全下沉至 `store/sqlstore` 的 `dbExecution`/`dbNode`/`dbSignal` | `a36f19b` |
| C2 Graph 深层不可变性 | 🛠 修复中 | 常规 map/slice accessor defensive copy、value-domain gate、race 与 benchmark 已实现；但 validator 跳过 struct 未导出字段，cloner 先浅拷贝 struct，未导出 map/slice/pointer 可保留 alias。2026-07-19 外部探针同时复现 Compile 输入修改和 `NodeAt` 返回值修改会改变 Graph 内部值，故不得声明 deep immutable | `7355570`；2026-07-19 验收 |

### D — 高吞吐路径（独立架构，不阻塞审批 release gate）

| 项 | 状态 | 说明 |
|---|---|---|
| D1 采集与审批工作流分离 | 🟡 架构/采样就绪，容量基线未完成 | 两条独立参考架构、`make perf-sample`、p50/p95/p99 和结构化 `perf.metric` 已实现；`.github/workflows/perf-sample.yml` 已配置每日/手动非门槛采样与 artifact 上传，但当前 release evidence 未附可核验成功 run/artifact。受控 host 报告仍是空模板；**两拓扑多样本报告填实前只能称“架构与采样准备完成”**。审批始终使用 durable mode，不得用 transient 数据承诺审批产能 |

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

### 4.1 tenant boundary 诚实性声明

tenant boundary 全链路代码与越权测试已完成（Phase 6-8，详见 §2 G2 行），但在 G2 整体达成前不得对外宣称"多租户隔离已完成"。以下边界声明在真实多租户部署验收（ENV-GATED）前不得放宽：

- **tenant 前缀 ≠ 加密隔离**：Redis key 前缀 `xflow:t<tenant>:...` 只是命名空间隔离，**不是密码学隔离**。跨 tenant 隔离依赖**服务端签发 TenantID（principal，忽略请求体 tenant）+ 全链路 context 校验 + 越权测试**，Redis 层是命名空间隔离不是密码学边界。任何能直接访问 Redis 的组件（如运维 CLI 直连）不受 tenant boundary 保护。
- **runner labels 不是安全边界**：runner placement 用**显式 tenant 归属**（`Assignment.TenantID` + runner 注册的 tenant 列表 + `ClaimForRunner` 过滤），**不得用 `RunnerSelector.MatchLabels` 兜底承载 tenant**。runner label 是调度提示，非隔离机制。
- **store key 前缀是主防线，manager/API 校验是双保险**：跨 tenant execID 在 rstate key 层因前缀不同直接 NotFound；API 层 `Inspect` 兜底 + deadletter manager 显式 `principal.TenantID == tenant.FromContext(ctx)` 校验是 fail-closed 第二防线。任一防线被绕过不构成隔离失效（纵深防御）。
- **跨 tenant → 404，不返回 403**：跨 tenant 资源访问一律 NotFound，不泄漏资源存在性（映射组织安全策略 §1「不泄漏 user 是否存在」）。
- **tenant 是低基数维度**：tenant 作为 metric label / span 属性，基数 = tenant 数量（G2 规模数十），可接受；execution ID / node name 组合 / lease token / runner ID **禁止作为 metric label**（高基数）。tenant **不得放入 W3C baggage** 跨进程传播（跨 tenant 泄漏风险），只作 span 属性。
- **default tenant 向后兼容**：未配置 tenant 时所有请求归 `default`，行为等价 G1 单租户；`default` 始终在 `xflow:tenants` SET 中（append-only，不回收，防 sweeper 漏扫孤儿 key）。
- **leader 全局不 per-tenant**：control-plane leader 保持全局单 key `xflow:leader:control-plane`，避免 N 倍 election 开销；leader-only maintenance 按 tenant 迭代（`xflow:tenants` SET）。
- **代码完成 ≠ G2 验收完成**：tenant boundary 代码 + 越权测试完成只解除 G2 退出清单中"tenant boundary 全链路 + 越权测试"一项；G2 整体达成仍依赖 G1 全满足（当前 ⛔）+ Redis HA + 多副本 SLO（ENV-GATED）。

## 5. 配置要求清单

G1 生产部署必须配置以下能力，详细示例见 [deployment-examples.md](../references/deployment-examples.md)：

| 能力 | 配置 | runbook |
|---|---|---|
| workflow API 鉴权 | token→principal/scopes 映射 + `--require-api-auth`；单一全 scope token 不满足最终 G1 | — |
| durable audit | `--mysql-dsn ...` 或等价持久 sink；内存 audit 仅开发使用 | — |
| audit reconciliation | crash-safe admission/outcome 与 dead-letter receipt reconcile worker；**当前未实现，阻断 G1** | — |
| runner 协议鉴权 | `--auth-policy runners.yaml` | — |
| 传输安全 | `--tls-cert`/`--tls-key`/`--tls-client-ca`（mTLS） | — |
| 分布式 tracing | `--trace otlp --trace-endpoint ...` | — |
| metrics | `--metrics-addr :9090 --metrics-path /metrics` | [dead-letter-runbook](../references/dead-letter-runbook.md) |
| structured logging | `--log-format json` | — |
| 维护窗口 | SIGTERM graceful shutdown | [maintenance-window-runbook](../references/maintenance-window-runbook.md) |
| Redis 备份/恢复演练 | 部署侧 | [maintenance-window-runbook](../references/maintenance-window-runbook.md) |
