# Tenant Boundary 设计文档

> **日期**: 2026-07-19
> **状态**: 设计 / 待实施
> **范围**: G2 Wave 4 Phase 6 Task 6.1 —— 多租户隔离的全链路设计。这是其后 Phase 7（7.1–7.6 全链路改造）与 Phase 8（8.1 越权测试、8.2 诚实性声明）的前置设计。
> **门槛映射**: `docs/design/RELEASE-GATES.md` §2 G2 行「多租户 tenant boundary」+ §4 反声明「namespace 是安全边界」（已证伪）。
> **前置上下文**: `.claude/specs/2026-07-19-g2-ha-multitenant-design.md` §2.2 发现 7、`docs/references/ha-soak-plan.md` §4 反声明「runner labels 不是安全边界」。
> **代码可做**: tenant boundary 全链路代码 + 越权测试可由 subagent 完成（miniredis 可执行，非 ENV-GATED）。
> **不等于 G2 完成**: G2 整体完成仍依赖 G1 全满足 + HA soak（ENV-GATED）；tenant boundary 完成只是 G2 退出清单中的一项。

---

## 0. 诚实性前置声明（贯穿本文）

1. **tenant 前缀 ≠ 加密隔离**：Redis key 前缀只是命名空间隔离。映射 RELEASE-GATES §4 反声明「namespace 是安全边界」（已证伪）。跨 tenant 隔离依赖**服务端签发 TenantID + 全链路校验 + 越权测试**，Redis 层是命名空间隔离不是密码学隔离。
2. **runner labels 不是安全边界**：映射 ha-soak-plan §4 反声明。runner placement 必须用**显式 tenant 归属**，不能用 `RunnerSelector.MatchLabels` 兜底承载 tenant（见 §4.7）。
3. **subagent 能完成**：tenant boundary 全链路代码 + miniredis 越权测试可由 subagent 独立实现 + 评审。**不 ENV-GATED**（miniredis 嵌入即可）。
4. **不等于 G2 完成**：G2 退出清单（修复设计 §12）= G1 全满足 + Redis HA + HA soak + 多副本 SLO + **tenant boundary 全链路 + 越权测试**。前三项中 HA soak + 多副本 SLO 是 ENV-GATED；tenant boundary 完成不解除其余项。

---

## 1. 现状核对（实际读代码后的确认）

### 1.1 与 G2 设计 §2.2 发现 7 一致的部分

G2 设计 §2.2 发现 7 列出 tenant 仅有零散桩字段。核对代码确认：

| 桩字段 | 位置 | 现状 |
|---|---|---|
| `WorkflowDef.Namespace` | `types/workflow.go:8` | 仅 JSON 字段，未参与 Redis key/路由。⚠️ 语义混淆风险：`Namespace` 在工作流 DSL 里是「namespace/name@version」的 namespace 部分（registry 的 `key` 由它派生，见 `workflowreg/registry.go:58`），**不是 tenant**。Task 6.2 必须新增显式 `TenantID` 字段，不能复用 `Namespace` 承载 tenant。 |
| `AuditRecord.TenantID` | `store/models.go:72`、`store/sqlstore/audit.go:22`（`dbAuditEvent.TenantID`） | 字段存在，DB 列 `tenant_id varchar(128)` 已就绪。但写入路径 `apiserver.AuditEvent.TenantID`（`authz.go:182`）目前恒空——`authz_wrap.go:55,68,103,135` 写 audit 时取 `principal.TenantID`，而 `BearerPrincipalAuth`（`authz.go:142-163`）构造 Principal 时**不填 TenantID**（见 §1.2 不符点）。 |
| `Principal.TenantID` | `service/apiserver/authz.go:21` | 类型字段存在，但 `BearerPrincipalAuth.Authenticate`（`authz.go:149-163`）返回 `Principal{Subject: a.subject, Scopes: a.scopes}`，**TenantID 始终为零值**。 |
| `DeadLetterReplayPrincipal.TenantID` | `service/control/deadletter_manager.go:24` | 字段存在，`module_management.go:239-243` replay 时从 `principal.TenantID` 注入。但因 Principal.TenantID 恒空，实际 replay principal 也无 tenant。 |
| `apiserver.AuditEvent.TenantID` | `service/apiserver/authz.go:182` | 字段存在，所有 audit 写入点已取 `principal.TenantID`（`authz_wrap.go` 4 处）。贯通只差「Principal 签发时填入 TenantID」这一步。 |

### 1.2 与 G2 设计 §2.2 发现 7 不符 / 需补充之处（核对代码新发现）

**不符点 A — `BearerPrincipalAuth` 当前是单 token→单 principal，不支持 token→tenant 映射。**
`service/apiserver/authz.go:142-163` `BearerPrincipalAuth` 持有单个 `tokenHash + subject + scopes`，`Authenticate` 命中即返回固定 principal，**无 tenant 维度**。`cmd/server/main.go:324-325` 装配处只传 `(token, "xflow-operator", scopes)`，subject 固定为 `xflow-operator`。要支持多租户必须扩展为「token→(subject, tenant, scopes)」映射或多 token 注册表。G2 设计 §2.2 发现 7 措辞「B3 principal 扩展 token→tenant 映射」准确，本设计 §2 给出具体方案。

**不符点 B — runner directory key 当前全局，非 per-tenant。**
`service/control/redis_runner_directory.go:21` `redisRunnerDirectoryKeyPrefix = "xflow:runner-directory:{control}"`，所有 runner 注册到单一 hash-tagged slot。runner placement 的 tenant 隔离**不能靠改这个 key 前缀**（runner directory 是 control-plane 全局目录，runner 本身跨 tenant 共享进程池时需在路由层做 tenant 归属，而非在 directory key 层）。G2 设计 §2.2 发现 6 提到「runner 不连 Redis」——核对确认 runner 仅通过 Runner Protocol 连 server，runner directory 在 server 侧，runner 自身无 tenant 感知。§4.7 给出方案。

**不符点 C — `WorkflowDef.Namespace` 语义混淆，不能复用为 tenant。**
G2 设计 §2.2 发现 7 仅说「未参与 key/路由」，但未指出 `Namespace` 已被用作「namespace/name@version」的 namespace 部分（`workflowreg/registry.go:58` 的 `{<key>}` hash tag 锚定的是包含 namespace 的 key）。本设计明确：**新增 `TenantID` 字段，不挪用 `Namespace`**（见 §4.8）。

**不符点 D — trigger 包已预留 tenant 前缀位置但未实现。**
`backend/distributed/internal/trigger/trigger.go:36-40` 注释明确「Tenant prefix is reserved for Task 7.2 (Phase 6/7). When a tenant prefix is added, the expected shape is `xflow:t<tenant>:trigger:dedup:<key>`」。这是 G2 设计未提及的良好基础，本设计直接采用此无花括号形状（tenant 前缀必须无花括号，原因见 §4.1）。

**不符点 E — `ScopeAuthorizer`（`authz.go:105-119`）不做 per-resource / per-tenant 校验。**
注释明说「G1 single-tenant does not enforce per-execution ownership, which is the host's responsibility」。G2 必须新增 tenant-aware authorizer：`Authorize` 校验 `req.Principal.TenantID == resource.tenant`，否则 Deny。`AuthorizationRequest`（`authz.go:39-45`）目前无 TenantID 字段，需补。

**不符点 F — management 端点 `/v1/management/executions/{id}`（`module_management.go:91-107`）无 tenant 校验。**
`handleExecution` 直接 `Inspect(r.Context(), types.ExecutionID(id))`，不校验 execID 所属 tenant。`handleDeadLetterList`/`handleDeadLetterReplay`（`module_management.go:186,217`）虽经 `authzWrap`，但 `ScopeAuthorizer` 只查 scope 不查 tenant 归属。IDOR 缺口明确，§5 给出修复。

**不符点 G — metrics labels 无 tenant 维度。**
`observability/metrics/engine.go:76-81` `nodeLabels` 只含 `node` + `status`；`control.go:93` lease sweep labels 只含 `result`。无 tenant 标签。§4.9 给出加标签方案与高基数风险声明。

### 1.3 Redis key schema 现状（tenant 前缀注入点）

| 子系统 | 现状 key schema | 文件:行 | hash tag |
|---|---|---|---|
| rstate exec | `xflow:exec:{<id>}:<suffix>` | `rstate/keys.go:17-69` | `{<id>}` ✅ |
| rstate outbox/dead | `xflow:exec:{<id>}:outbox:ready\|dead\|dead:body\|dead:meta:<eid>` | `rstate/atomic_state.go:26-37` | `{<id>}` ✅ |
| rstate SCAN pattern | `xflow:exec:{*}:outbox:ready\|dead`、`xflow:exec:{*}:node:*:status`、`xflow:exec:{*}:leases` | `atomic_state.go:816,859,1001`、`lease_repair.go:72`、`state_lease.go:144` | `{*}` glob |
| workflowreg | `xflow:workflow:{<key>}:bykey`、`xflow:workflow:{<key>}:byid:<id>`、`xflow:workflow:idmap:<id>` | `workflowreg/registry.go:58-77` | `{<key>}` ✅（Task 2.1 已修） |
| trigger | `xflow:trigger:dedup:<key>`、`xflow:trigger:lock:<key>`、`xflow:trigger:state:<scope>:<key>` | `trigger/trigger.go:41-47` | 无（单 key，无需） |
| leader | `xflow:leader:control-plane` | `backend/distributed/backend.go:298` | 无（全局单 key，**不 per-tenant**，见 §3） |
| runner directory | `xflow:runner-directory:{control}` | `redis_runner_directory.go:21` | `{control}` 全局 |

---

## 2. TenantID 定义与来源

### 2.1 类型定义

```go
// backend/tenant/tenant.go (Task 6.2 新建)
type TenantID string

const DefaultTenant TenantID = "default"
```

- `TenantID` 为 `string` 类型别名，服务端签发，**不可信客户端**。
- 保留 `default` 作为单租户默认值，向后兼容（未配置 tenant 时所有请求归 `default`，行为与 G1 单租户等价）。

### 2.2 不可信客户端原则（IDOR 防护，映射组织安全策略 §1）

**核心原则：tenant 必须来自服务端认证后的 principal，忽略请求体任何 tenant 字段。**

- 请求体中出现的 `tenant` / `tenant_id` / `namespace` 字段一律**忽略不读**。
- TenantID 由 `PrincipalAuthenticator`（`service/apiserver/authz.go:125`）在认证后注入 `Principal.TenantID`。
- 所有下游（backend、rstate、workflowreg、trigger、deadletter、audit、metrics、trace）从 `context.Context` 取 TenantID，**不信任请求体**。
- 这映射组织安全策略 §1a「identity must come from the server, never from the client」与 §1c「batch operations check ownership per item」。

### 2.3 TenantID 来源方案

#### 方案 A（推荐）：B3 principal 扩展 token→tenant 映射

`BearerPrincipalAuth` 扩展为多 token 注册表：

```
TokenAuthRegistry {
  tokens: map[tokenHash] -> {Subject, TenantID, Scopes}
}
```

- 配置侧（`cmd/server/main.go`）支持多 token 注册：每个 token 绑定 `(subject, tenant, scopes)`。
- `Authenticate` 命中 token 后返回 `Principal{Subject, TenantID, Scopes}`，TenantID 非空。
- 单租户兼容：未配置多 token 时，单 token 映射到 `TenantID=default`。

**权衡**：
- 优点：与现有 B3 bearer 认证路径一致（`authz.go:142-163`），改动集中在 authenticator + cmd flag，不引入新依赖。
- 缺点：token 是长静态 bearer，需配合 token 轮转策略；不适合大规模租户（每租户一 token，token 数量受管理成本限制）。

#### 方案 B：mTLS client cert → tenant

- `cmd/server/main.go:385-387` 已支持 `--tls-client-ca`。扩展：从 client cert 的 Subject CN/SAN 映射到 TenantID。
- `PrincipalAuthenticator` 新增 mTLS 实现，按 cert subject → tenant 映射表（server 配置）签发 Principal。

**权衡**：
- 优点：适合 B2B 多租户（每租户独立 client cert），凭证生命周期由 PKI 管理；符合组织安全策略 §3「token 须有 timeout」+ §6「传输加密」。
- 缺点：需 PKI 基础设施 + cert 轮转；映射表维护成本。

#### 推荐

- **G2 默认采用方案 A**（最小改动，覆盖 G2 验收需求）。
- **方案 B 作为部署选项并行支持**（mTLS 已具备，`cmd/server` 加 `--mtls-tenant-map` flag）。两者通过 `PrincipalAuthenticator` 接口可共存（chain authenticator）。
- 单租户默认：两种方案均未配置时，所有请求归 `TenantID=default`，行为等价 G1。

---

## 3. Leader 是否 per-tenant（设计决策）

### 3.1 决策：control-plane leader 全局，不 per-tenant

**理由**：
- 避免 N 倍 leader election 开销（每租户一个 leader = N 个 SETNX + 续约 goroutine，Redis 负载线性增长）。
- leader election 在本系统只协调 leader-only maintenance（`lease_sweeper` / `lease_repair`），不协调状态写入（`leader.go:197-204` 注释明确「leadership here only gates background maintenance, not state mutations」）。
- leader key 保持 `xflow:leader:control-plane`（`backend.go:298`），**不改**，全局单一。

### 3.2 leader-only maintenance 需扫所有 tenant

全局 leader 持有 maintenance，但 `lease_sweeper` / `lease_repair` / outbox dispatcher 当前 SCAN 全局 `xflow:exec:{*}:...`（见 §1.3）。加 tenant 前缀后，SCAN pattern 变为 `xflow:t<tenant>:exec:{*}:...`，**全局 leader 需迭代所有 tenant**。

**方案：tenant 注册表 + 按 tenant 迭代**

- 新增 `xflow:tenants` Redis SET，记录所有曾出现过的 tenant（tenant 首次写 key 时 `SADD`）。
- `lease_sweeper` / `lease_repair` / outbox dispatcher 先 `SMEMBERS xflow:tenants`，再按 tenant 迭代 SCAN `xflow:t<tenant>:exec:{*}:...`。
- `default` tenant 始终在集合中（启动时 `SADD`）。
- **不变式（重要）**：tenant 一经 SADD 即**只增多不回收**——不支持 tenant 删除/下线。理由：(1) 删除 tenant 后若有在途 key（exec/outbox/dead-letter）未被 sweeper 扫净，会形成「孤儿 key 永久漏扫」，违背 lease_repair/outbox 的可靠性契约；(2) SET 元素回收会引入「sweeper 已迭代过该 tenant 后才被 SADD 新 key」的竞态。保持 append-only，确保 sweeper 不会漏扫任何 tenant 的 key。`default` tenant 在 server 启动时 `SADD`，其余 tenant 首次出现时懒发现并 SADD，最终收敛。

**权衡**：
- 优点：SCAN 范围明确可控，单 tenant 的 key 不被其他 tenant 的 sweeper 扫到（即便 sweeper 有 bug 也只影响遍历顺序，不跨 tenant）。
- 缺点：`SMEMBERS` 在 tenant 极多时是 O(N) 全量返回；G2 规模（数十 tenant）可接受。若未来上千 tenant，改为 `SSCAN` 游标。
- 备选方案（不推荐）：SCAN `xflow:*:exec:{*}:...` 跨 tenant glob。缺点：SCAN 游标跨 slot 不可靠（Redis Cluster 下 SCAN 是 per-node，glob 跨 tenant 会扫到非 exec key），且无法隔离 sweeper 故障域。采用 tenant 注册表方案。

### 3.3 runner directory 仍全局

`xflow:runner-directory:{control}`（`redis_runner_directory.go:21`）保持全局。runner 归属 tenant 在 **dispatch 路由层**实现（assignment 携带 tenant，dispatcher 按 tenant 路由），不在 directory key 层。见 §4.7。

---

## 4. 子系统改动点清单（文件级，附 file:line）

共 **9 个子系统改动点**。每项标注对应 Phase 7 Task。

### 4.1 rstate key tenant 前缀（Task 7.1）

**改动**：key 函数增加 tenant 参数，前缀注入 tenant。tenant 前缀采用**无花括号**形式 `xflow:t<tenant>:exec:{<id>}:...`（`t` 前缀可读无歧义）。

- `backend/distributed/internal/rstate/keys.go:17-69`：所有 `xflow:exec:{<id>}:...` → `xflow:t<tenant>:exec:{<id>}:...`。
  - `execKey(id, suffix)` → `execKey(tenant tenant.TenantID, id, suffix)`。
  - 同理 `nodeStatusKey`/`nodeMetaKey`/`outputKey`/`signalKey`/`waiterKey`/`waiterSpecKey`/`signalBatchKey`/`inDegreeKey`/`activeInputsKey`/`resumeLockKey`/`suspendedNodesKey`/`leaseExpiryZSetKey`/`timeoutZSetKey`。
- **关键论证（tenant 前缀必须无花括号）**：Redis Cluster hash tag 规则是「首个 `{` 到首个 `}`」之间的子串作为 slot 计算输入。若 tenant 前缀写成 `{tenant}`（带花括号），则 `xflow:{tenant}:exec:{<id>}:node:...` 的首个 `{...}` 是 `{tenant}`，hash tag = `tenant`，导致同一 tenant 的所有 exec key 全部落到单一 slot——既制造热租户热点，又破坏「exec 内 key 共置 + per-exec slot 分布」的设计意图。因此 tenant 前缀必须**无花括号**。采用 `xflow:t<tenant>:exec:{<id>}:node:...` 后，首个 `{` 出现在 `<id>` 前，hash tag = `{<id>}`：exec 内所有 key 共置同 slot（Lua CROSSSLOT 不触发），同时不同 exec 仍按 `<id>` 分布到不同 slot（per-exec slot 分布保留，避免单 tenant 全部坍缩到单 slot），tenant 仅起命名空间隔离作用。SCAN pattern `xflow:t<tenant>:exec:{*}:...` 同理以 `{*}` 作 glob，不引入 hash tag。
- `backend/distributed/internal/rstate/state.go:17` `Store` 持有 tenant 或从 context 取；所有调用点传 tenant。
- `backend/distributed/internal/rstate/atomic_state.go:26-37` outbox/dead key 函数加 tenant 参数。
- `backend/distributed/internal/rstate/atomic_state.go:816,859,1001` SCAN pattern：`xflow:exec:{*}:outbox:ready` → `xflow:t<tenant>:exec:{*}:outbox:ready`，sweeper 按 tenant 迭代（§3.2）。
- `backend/distributed/internal/rstate/lease_repair.go:72` SCAN `xflow:exec:{*}:node:*:status` → `xflow:t<tenant>:exec:{*}:node:*:status`。
- `backend/distributed/internal/rstate/state_lease.go:144` SCAN `xflow:exec:{*}:leases` → `xflow:t<tenant>:exec:{*}:leases`。
- `backend/distributed/internal/rstate/state_lease.go`、`state_suspend.go`：所有 key 构造点加 tenant。
- **越权断言**：tenant A 的 sweeper 扫不到 tenant B 的 key（miniredis 单测）。

### 4.2 workflowreg tenant scope（Task 7.2，与 Task 2.1 hash tag 修复合并）

**改动**：workflow key 加 tenant 前缀（无花括号，理由同 §4.1）。

- `backend/distributed/internal/workflowreg/registry.go:58-77`：
  - `workflowByKeyKey(key)` → `workflowByKeyKey(tenant, key)` = `xflow:t<tenant>:workflow:{<key>}:bykey`
  - `workflowByIDKey(key, id)` → `workflowByIDKey(tenant, key, id)` = `xflow:t<tenant>:workflow:{<key>}:byid:<id>`
  - `workflowByIDKeyPrefix(key)` → 加 tenant 前缀
  - `workflowIDMapKey(id)` → `xflow:t<tenant>:workflow:idmap:<id>` 或保持全局 idmap（见下权衡）
  - `KeyByID`/`KeyByKey`/`KeyIDMap`（`registry.go:83-85`）同步加 tenant 参数
- **与 Task 2.1 合并**：Task 2.1 已加 `{<key>}` hash tag，本任务在 hash tag 之外再加无花括号的 `t<tenant>` 前缀。两者在同一 key 改动里完成，形状 `xflow:t<tenant>:workflow:{<key>}:...`。**tenant 前缀必须无花括号**：若写成 `xflow:{tenant}:workflow:{<key>}:...`，则 Redis hash tag 规则取「首个 `{...}`」= `{tenant}`，会令同 tenant 所有 workflow key 坍缩到单 slot，并破坏 `{<key>}` 的共置语义。无花括号形式下首个 `{` 在 `<key>` 前，hash tag = `{<key>}`，bykey/byid 共置保留、per-key slot 分布保留、tenant 命名空间隔离保留。
- `AddWorkflow`/`GetWorkflow`/`RemoveWorkflow`（`registry.go:110,153,177`）签名加 tenant 或从 context 取。
- **idmap 权衡**：`workflowIDMapKey`（`registry.go:75`）是 id→key 反向索引。若保持全局，则跨 tenant 的 id 碰撞需避免（id 是 UUID，碰撞概率可忽略，但 GetWorkflow(id) 需知道 tenant 才能定位 byid key——这会破坏「仅凭 id 取」语义）。**决策：idmap 也加 tenant 前缀**，`GetWorkflow(tenant, id)` 必须带 tenant。management 端点 `/v1/management/executions/{id}` 先从已认证 principal 取 tenant，再 `GetWorkflow(tenant, id)`（见 §5.1 鸡生蛋问题的解析顺序）。

### 4.3 trigger tenant scope（Task 7.2）

**改动**：trigger key 加 tenant 前缀（无花括号 `xflow:t<tenant>:trigger:...`，理由同 §4.1），采用 `trigger.go:36-40` 注释已预留的形状。

- `backend/distributed/internal/trigger/trigger.go:41-47`：
  - `triggerDedupKey(key)` → `triggerDedupKey(tenant, key)` = `xflow:t<tenant>:trigger:dedup:<key>`
  - `triggerLockKey(key)` → `xflow:t<tenant>:trigger:lock:<key>`
  - `triggerStateKey(scope, key)` → `xflow:t<tenant>:trigger:state:<scope>:<key>`
- trigger 仍是单 key 操作（`trigger.go:22-34` 注释确认无跨 key Lua），tenant 前缀不影响 cluster-safety；tenant 无花括号也避免了「首个 `{...}` 被 tenant 抢占为 hash tag」的隐患（即便当前 trigger 无多 key Lua，保持 key schema 一致性）。
- `Primitives` 方法签名加 tenant 或从 context 取。

### 4.4 leader（Task 7.x —— 全局不变）

**改动**：**无**。leader key `xflow:leader:control-plane`（`backend.go:298`）保持全局。

- `backend/distributed/leader.go:42-65` `RedisLeaderElector` 不变。
- 但 leader-only maintenance（`lease_sweeper` / `lease_repair` / outbox dispatcher）按 tenant 迭代（§3.2），改动在 sweeper 侧（§4.1 SCAN + §4.6）。
- `service/control/controlplane.go:168-176` leader 取用 + sweeper 门控不变。

### 4.5 dead-letter key tenant 隔离（Task 7.6）

**改动**：dead-letter key 共享 exec 的 tenant 前缀（已随 §4.1 完成，此处贯通 manager + API）。

- `backend/distributed/internal/rstate/atomic_state.go:280-294` `replayDeadLetterLua` 的 KEYS 全部带 tenant 前缀（随 §4.1 key 函数改造自动生效）。
- `service/control/deadletter_manager.go:65-67` `List` 签名加 tenant（或从 context 取），传给 `store.ListDeadLetters`。
- `service/control/deadletter_manager.go:75-104` `Replay` 校验 `req.ExecutionID` 所属 tenant == `principal.TenantID`：replay 调 `store.ReplayDeadLetter` 前，先从 store 读 exec 的 tenant（或 exec key 前缀解析 tenant），比对 `principal.TenantID`，不匹配返回 `ReplayUnauthorized`。
- `service/apiserver/module_management.go:186,217` `handleDeadLetterList`/`handleDeadLetterReplay` 从 principal 取 tenant 注入 manager 调用。
- CLI 走 manager 时注入 cli principal + tenant（`deadletter_manager.go:20-21` 已支持 cli principal 注入路径）。

### 4.6 outbox tenant 隔离（Task 7.6）

**改动**：outbox SCAN 与 dispatcher 按 tenant 隔离。

- `backend/distributed/internal/rstate/atomic_state.go:816,859,1001` outbox SCAN pattern 加 tenant（随 §4.1）。
- outbox dispatcher（后台重放）按 tenant 迭代（§3.2 tenant 注册表）。
- outbox body / attempts / dead:meta key 全部随 exec tenant 前缀（随 §4.1 key 函数改造）。

### 4.7 runner placement / credential tenant scope（Task 7.5）

**改动**：runner 注册带 tenant 归属，dispatch 按 execution tenant 路由。

- `service/control/redis_runner_directory.go:21` `xflow:runner-directory:{control}` 保持全局（runner directory 是 control-plane 全局目录，runner 可服务多 tenant，但 dispatch 按 tenant 路由）。
- `service/control/runner_directory.go:24-32` `Assignment` 结构加 `TenantID` 字段：assignment 入队时携带 execution 的 tenant。
- `service/control/controlplane.go` dispatch 路径：按 execution tenant 路由 assignment。runner 消费 assignment 时校验自身 tenant 归属是否允许服务该 tenant。
- `execution/runner.go:33` `WithCredentialResolver(fn func(name string) map[string]any)` → `WithCredentialResolver(fn func(tenant tenant.TenantID, name string) map[string]any)`：credential resolver 按 tenant scope 凭证（不同 tenant 的同 credential name 解析到不同凭证）。
- `service/runner/runner.go:56` `CredentialResolver` 签名同步加 tenant。
- `cmd/runner/config.go`：runner 注册时声明可服务的 tenant 列表（`tenants: [...]`），server 侧 dispatch 校验。
- asynq 任务携带 tenant：asynq task payload 或 queue 命名空间带 tenant（`backend/distributed/internal/queue/asynq/transport.go` producer enqueue 时注入 tenant 到 payload；`consumer.go:27` 解出 tenant 注入 context）。
- **显式 tenant placement，不能用 runner label 兜底**：`types/workflow.go:83-86` `RunnerSelector.MatchLabels` 不能用于承载 tenant（ha-soak-plan §4 反声明）。runner tenant 归属是 server 端 dispatch 决策，不信任 workflow DSL 里的 label。

### 4.8 API 层签发 + authz（Task 7.3）

**改动**：principal 签发 tenant + authz tenant 校验 + IDOR 防护。

- `service/apiserver/authz.go:19-23` `Principal` 已有 `TenantID`，签发处填入（§2.3 方案 A/B）。
- `service/apiserver/authz.go:39-45` `AuthorizationRequest` 加 `TenantID` 字段（principal 的 tenant）。
- `service/apiserver/authz.go:105-119` `ScopeAuthorizer` 扩展或新增 `TenantAwareAuthorizer`：`Authorize` 校验 `req.Principal.TenantID == resource.tenant`，不匹配 Deny。资源 tenant 由资源解析层（route handler 从 execID/workflowID 解析所属 tenant）提供。
- `service/apiserver/authz_wrap.go:24-89` `authzWrap` 在 `Authorize` 前从 principal 取 tenant 注入 context（`r.WithContext(context.WithValue(..., tenantKey, principal.TenantID))`），handler 下游从 context 取。
- `service/apiserver/module_management.go:91-107` `handleExecution` 加 tenant 校验：`Inspect` 前校验 execID 所属 tenant == principal.TenantID，不匹配 404（不泄漏存在性）。
- `cmd/server/main.go:318-326` principal 装配：按 §2.3 方案 A 配置多 token→tenant 映射，或方案 B mTLS map。
- `types/workflow.go:8` `WorkflowDef` 新增 `TenantID` 字段（不复用 `Namespace`，见 §1.2 不符点 C）。

### 4.9 audit / metrics / trace tenant 标签（Task 7.4）

**改动**：audit 贯通 tenant；metrics/log/trace 加 tenant 维度。

- `store/sqlstore/audit.go:22` `dbAuditEvent.TenantID` 列已就绪，写入路径 `apiserver.AuditEvent.TenantID`（`authz.go:182`）已取 `principal.TenantID`——只要 §4.8 签发处填入 TenantID，audit 自动贯通。
- `store/models.go:72` `AuditRecord.TenantID` 已就绪。
- `observability/metrics/engine.go:76-81` `nodeLabels` 加 `"tenant": string(tenant)` 维度。
- `observability/metrics/control.go:93` lease sweep labels 加 tenant 维度。
- `observability/tracing/tracing.go:77` span `WithAttributes` 加 `tenant` 属性（dispatch span / execute span / inspect span）。
- `observability/tracing/provider.go:54,118` baggage 注意事项：tenant 作为 span 属性 OK，但**不要**放入 baggage 跨进程传播（避免高基数 + 跨 tenant 泄漏，`provider.go` 已有 baggage opt-in 警告）。
- **高基数风险声明**：tenant 作为 metrics label 维度，基数 = tenant 数量（G2 规模数十），可接受。但 tenant × node × status 组合基数需评估；若 tenant 上百，考虑只在不暴露给外部 Prometheus 的高基数 histogram 上保留，或采样。文档化此权衡。

---

## 5. API 层 IDOR 防护（映射组织安全策略 §1）

### 5.1 管理端点 tenant 校验

- `/v1/management/executions/{id}`（`module_management.go:91-107`）：
  - **解析顺序（避免鸡生蛋）**：handler 先从已认证 principal 取 `TenantID`（来自 §4.8 签发，不读请求体），再以 `(principal.TenantID, execID)` 调 `Inspect`/`GetWorkflow`。下游 `GetWorkflow(tenant, id)` 因 idmap 已加 tenant 前缀（§4.2），可直接在已知 tenant 命名空间内反查 id→key，不存在「需先知 tenant 才能反查、又需反查才能知 tenant」的循环。
  - **execID 所属 tenant 校验**：`Inspect` 前校验 execID 所属 tenant == `principal.TenantID`。exec 所属 tenant 通过 exec key 前缀解析：exec key 形如 `xflow:t<tenant>:exec:{<id>}:...`，从 key 中取出 `t<tenant>` 段与 `principal.TenantID` 比对。若 key 不存在或前缀 tenant 不匹配，一律返回 404。
  - 不匹配返回 **404**（不返回 403，避免泄漏 execID 存在性——映射安全策略「Return a generic error on authentication failure; do not reveal whether the user exists」）。
- `/v1/management/dead-letters/{execID}` list/replay（`module_management.go:186,217`）：
  - 同样校验 execID.tenant == principal.TenantID，不匹配 404。tenant 同样从 principal 取，再按 exec key 前缀 `xflow:t<tenant>:exec:...` 校验 execID 归属。
  - replay 操作额外在 `deadletter_manager.go` Replay 内校验（§4.5），双保险。

### 5.2 批量操作 per-item tenant 校验

- 当前 management 端点无批量 list（`module_management.go:22-24` 注释明确「intentionally provides no listing endpoints for runners/executions」）。dead-letter list 是单 exec 内分页，tenant 校验在 exec 级（§5.1）。
- 若未来引入批量端点，必须 per-item 校验所有 item 所属 tenant == principal.TenantID，任一不匹配拒绝整批（映射安全策略 §1c）。

### 5.3 不可预测 ID（映射安全策略 §1e）

- **ExecutionID（已核实，满足要求）**：`engine/engine.go:134,155` 生成路径为 `id := types.ExecutionID("exec-" + uuid.New().String())`，使用 `github.com/google/uuid` 的 `uuid.New()`（UUID v4，122 位随机熵）。`exec-` 仅为可读前缀，熵源为 UUID v4。**非自增、不可预测**，满足组织安全策略 §1e「使用 UUID 或 snowflake，非自增，防枚举」。Task 6.2/7.x 无需改动 ExecutionID 生成路径。子执行 ID（`engine/expand.go:130`）形如 `<parentExecID>/sub/<node>/<leaseID>/<batchIndex>`，父 ID 已是 UUID，碰撞概率可忽略。
- `types.WorkflowID`（`types/workflow.go:3`）当前 `workflowreg/registry.go:227` 已用 `uuid.NewString()`，符合 §1e。
- 结论：ExecutionID 与 WorkflowID 均为 UUID v4 派生，已满足不可预测要求；本设计在 6.1 闭合此项，不再下推至 Task 6.2。后续若引入新 ID 类型，须同样使用 UUID/snowflake，禁止自增。

---

## 6. 实施分阶段（与 G2 设计 Phase 6-8 对齐）

### 6.1 顺序与依赖

```
Phase 6 (tenant 原语)
  6.1 (本文档, 设计) ──► 6.2 (context 原语 + TenantID 类型 + WorkflowDef.TenantID)
                              │
                              ▼
Phase 7 (全链路改造)
  7.1 (rstate keys) ──┐
  7.2 (workflowreg+trigger, 合并 Task 2.1) ──┤
  7.4 (audit/metrics/trace 标签) ──┤         (大体并行)
  7.5 (runner placement/credential) ◄── 7.3 (API 签发+authz) 前置于 7.5/7.6
  7.6 (dead-letter/outbox) ◄── 7.3
                              │
                              ▼
Phase 8 (安全测试)
  8.1 (越权测试套件, 依赖 7.x 全部完成)
  8.2 (诚实性声明, RELEASE-GATES §4 补充)
```

### 6.2 前置关系明细

- **6.1 → 6.2**：本文档评审通过后，Task 6.2 新建 `backend/tenant/tenant.go`（context 携带 TenantID）+ 改 `types/workflow.go` 加 `TenantID` 字段。
- **6.2 → 7.x**：context 原语就位后，全链路改造可开始。
- **7.3 前置于 7.5/7.6**：API 层签发 Principal.TenantID 是 runner placement（7.5）与 dead-letter replay 校验（7.6）的前提——后者需从 principal 取 tenant。
- **7.x 全部 → 8.1**：越权测试需全链路就位才能断言跨 tenant 隔离。

### 6.3 可并行

- 7.1（rstate）、7.2（workflowreg+trigger）、7.4（audit/metrics/trace）大体并行，互不依赖。
- 7.5（runner placement）与 7.6（dead-letter/outbox）依赖 7.3 的 principal 签发，但 7.5 与 7.6 之间可并行。
- 7.2 与 Task 2.1 hash tag 修复合并实施（G2 设计 §4 依赖图已标注）。

### 6.4 环境门控

- Phase 6-8 全部**非 ENV-GATED**：miniredis 可执行越权测试，不依赖真实 Redis HA / 多副本。
- 越权测试（8.1）在 `XFLOW_REQUIRE_REDIS_INTEGRATION=1` 下执行（miniredis 嵌入即可）。

---

## 7. 越权测试矩阵（Task 8.1 前瞻）

新增 `test/security/tenant_isolation_test.go`，矩阵：

| 场景 | 期望 |
|---|---|
| tenant A 提交 workflow | tenant A 可 list/get/exec |
| tenant B 尝试 get tenant A 的 workflow | NotFound |
| tenant B 尝试 exec tenant A 的 workflow | Forbidden / NotFound |
| tenant B 尝试 inspect tenant A 的 execution | 404 |
| tenant B 尝试 list/replay tenant A 的 dead-letters | 404 |
| tenant A 提交 workflow，tenant B 提交同名 workflow | 不冲突（不同 tenant 前缀） |
| 重复提交跨 tenant | fencing 不跨 tenant 误判（lease token per exec，tenant 隔离） |
| tenant A 的 sweeper SCAN | 扫不到 tenant B 的 key |
| runner 归属 tenant A 消费 assignment | 不接收 tenant B 的 assignment |
| 请求体伪造 `tenant: "B"`（principal 是 A） | 忽略，按 A 执行 |

- miniredis 可执行，不 ENV-GATED。

---

## 8. 与 G2 设计 §2.2 发现 7 的核对结论

G2 设计 §2.2 发现 7 的 5 项桩字段全部核对确认存在（见 §1.1）。本设计补充 6 项不符/需澄清之处（见 §1.2）：

1. `BearerPrincipalAuth` 不支持 token→tenant 映射（不符点 A）——本设计 §2.3 方案 A 扩展。
2. runner directory key 全局，runner placement tenant 隔离在路由层（不符点 B）——本设计 §4.7。
3. `WorkflowDef.Namespace` 语义混淆，不能复用为 tenant（不符点 C）——本设计 §4.8 新增 `TenantID` 字段。
4. trigger 包已预留 tenant 前缀位置（不符点 D，正向）——本设计 §4.3 直接采用。
5. `ScopeAuthorizer` 不做 tenant 校验（不符点 E）——本设计 §4.8 新增 TenantAwareAuthorizer。
6. management 端点无 tenant 校验（不符点 F，IDOR 缺口）——本设计 §5。
7. metrics labels 无 tenant 维度（不符点 G）——本设计 §4.9。

G2 设计 §2.2 发现 7 整体准确，本设计在其基础上细化了实现路径与 IDOR 防护细节。

---

## 9. 诚实性声明（贯穿实施）

1. **tenant 前缀 ≠ 加密隔离**：Redis key 前缀是命名空间隔离，不是密码学隔离。跨 tenant 隔离依赖服务端签发 TenantID + 全链路校验 + 越权测试。映射 RELEASE-GATES §4 反声明「namespace 是安全边界」（已证伪）。
2. **runner labels 不是安全边界**：runner placement 用显式 tenant 归属，不用 `RunnerSelector.MatchLabels` 兜底。映射 ha-soak-plan §4 反声明。
3. **subagent 能完成**：Phase 6-8 全部代码 + 越权测试可由 subagent 独立实现 + 评审，miniredis 可执行，非 ENV-GATED。
4. **不等于 G2 完成**：G2 整体完成仍依赖 G1 全满足（当前 ⛔ 未满足）+ HA soak + 多副本 SLO（ENV-GATED）。tenant boundary 完成只解除 G2 退出清单中「tenant boundary 全链路 + 越权测试」一项，不解除其余。
5. **leader 全局不 per-tenant**：避免 N 倍 leader 开销；leader-only maintenance 按 tenant 迭代（§3.2 tenant 注册表）。
6. **高基数风险**：tenant 作为 metrics label 基数 = tenant 数量，G2 规模可接受；tenant × node × status 组合需评估，文档化权衡（§4.9）。
