# Tenant Boundary 设计文档

> **日期**: 2026-07-19
> **状态**: 设计 / 待实施
> **范围**: G2 Wave 4 Phase 6 Task 6.1 —— 多租户隔离的全链路设计。这是其后 Phase 7（7.1–7.6 全链路改造）与 Phase 8（8.1 越权测试、8.2 诚实性声明）的前置设计。
> **门槛映射**: `docs/design/RELEASE-GATES.md` §2 G2 行「多租户 tenant boundary」+ §4 反声明「namespace 是安全边界」（已证伪）。
> **前置上下文**: 代码核查确认——**全仓 Go 代码中已无任何 `TenantID` 标识符**（`grep -rn TenantID --include=*.go` 零命中）。隔离原语统一是 `Namespace`：`types/workflow.go` `WorkflowDef.Namespace`（已贯通）、`service/apiserver/authz.go` `Principal.Namespace`（已实现，`NewBearerPrincipalAuthMulti` 多 namespace token 注册表）、`AuditEvent.Namespace`、`DeadLetterReplayPrincipal.Namespace`、`store/sqlstore` `dbAuditEvent.Namespace`（DB 列 `namespace varchar(128)`）。早期设计里的 "TenantID 字段" 已随 tenant→namespace 整体重命名移除（`backend/tenant` 包在 `0deb675` 一并删除），也不再是目标——不要据此新增字段。runner labels 不是安全边界（详见 `docs/references/ha-soak-plan.md` §4 反声明）。
> **代码可做**: namespace boundary 全链路代码 + 越权测试可由 subagent 完成（miniredis 可执行，非 ENV-GATED）。
> **不等于 G2 完成**: G2 整体完成仍依赖 G1 全满足 + HA soak（ENV-GATED）；namespace boundary 完成只是 G2 退出清单中的一项。

---

## 0. 诚实性前置声明（贯穿本文）

1. **namespace 前缀 ≠ 加密隔离**：Redis key 前缀只是命名空间隔离。映射 RELEASE-GATES §4 反声明「namespace 是安全边界」（已证伪）。跨 namespace 隔离依赖**服务端签发 Namespace + 全链路校验 + 越权测试**，Redis 层是命名空间隔离不是密码学隔离。
2. **runner labels 不是安全边界**：映射 ha-soak-plan §4 反声明。runner placement 必须用**显式 namespace 归属**，不能用 `RunnerSelector.MatchLabels` 兜底承载 namespace（见 §4.7）。
3. **subagent 能完成**：namespace boundary 全链路代码 + miniredis 越权测试可由 subagent 独立实现 + 评审。**不 ENV-GATED**（miniredis 嵌入即可）。
4. **不等于 G2 完成**：G2 退出清单（修复设计 §12）= G1 全满足 + Redis HA + HA soak + 多副本 SLO + **namespace boundary 全链路 + 越权测试**。前三项中 HA soak + 多副本 SLO 是 ENV-GATED；namespace boundary 完成不解除其余项。

---

## 1. 现状核对（实际读代码后的确认）

### 1.1 与 G2 设计 §2.2 发现 7 一致的部分

G2 设计 §2.2 发现 7 列出 tenant 仅有零散桩字段。核对代码确认：

| 桩字段 | 位置 | 现状 |
|---|---|---|
| `WorkflowDef.Namespace` | `types/workflow.go:8` | 服务端权威命名空间。`authz_wrap.go` 通过 `namespace.WithNamespace` 注入 context；`apiserver.RegisterWorkflow`/`ReplaceWorkflow` 从 context 读取并校验（`service/apiserver/apiserver.go:431,460`）。workflowreg key 已含 namespace 前缀（`workflowreg/registry.go:58`，形如 `xflow:ns:<namespace>:workflow:{<key>}:...`）。**这就是隔离原语**，不需要另建 TenantID 字段。 |
| `AuditRecord` 的 namespace 维度 | `store/models.go`、`store/sqlstore/audit.go`（`dbAuditEvent.Namespace`，列 `namespace varchar(128)`） | 已就绪。写入路径 `apiserver.AuditEvent.Namespace`（`service/apiserver/authz.go`）由 `authz_wrap.go` 从 `principal.Namespace` 取。**注意**：早期文档称此处字段名为 `TenantID`，该名称已随 tenant→namespace 重命名移除，当前代码中无此字段。 |
| `Principal.Namespace` | `service/apiserver/authz.go:21-25` | 已实现并已填充：`BearerPrincipalAuth.Authenticate` 返回 `Principal{Subject, Namespace, Scopes}`；`NamespaceAwareAuthorizer` 已实现并已在生产路由使用。结构体只有 `Subject`/`Namespace`/`Scopes` 三个字段。 |
| `DeadLetterReplayPrincipal.Namespace` | `service/control/deadletter_manager.go` | 已实现。字段为 `Subject`/`Namespace`/`Scopes`，与 `apiserver.Principal` 同构。replay 时从 `principal.Namespace` 注入。 |

### 1.2 与 G2 设计 §2.2 发现 7 不符 / 需补充之处（核对代码新发现）

**不符点 A — `BearerPrincipalAuth` 已扩展为多 namespace 注册表（已实现）。**
`service/apiserver/authz.go` `BearerPrincipalAuth` 已持有 `principalByHash` 映射表，`NewBearerPrincipalAuthMulti` 接受 `[]TokenPrincipalMapping`，每条记录绑定 `(Subject, Namespace, Scopes)`，`Authenticate` 命中即返回带 Namespace 的 `Principal`（`authz.go:279-316`）。单 namespace 兼容由 `NewBearerPrincipalAuth` 包装（`authz.go:267`）。G2 设计 §2.2 发现 7 措辞「B3 principal 扩展 token→namespace 映射」已落地，本设计 §2 给出的具体方案已实现。

**不符点 B — runner directory key 当前全局，非 per-tenant。**
`service/control/redis_runner_directory.go:21` `redisRunnerDirectoryKeyPrefix = "xflow:runner-directory:{control}"`，所有 runner 注册到单一 hash-tagged slot。runner placement 的 tenant 隔离**不能靠改这个 key 前缀**（runner directory 是 control-plane 全局目录，runner 本身跨 tenant 共享进程池时需在路由层做 tenant 归属，而非在 directory key 层）。G2 设计 §2.2 发现 6 提到「runner 不连 Redis」——核对确认 runner 仅通过 Runner Protocol 连 server，runner directory 在 server 侧，runner 自身无 tenant 感知。§4.7 给出方案。

**不符点 C — `WorkflowDef.Namespace` 是隔离原语本身，无需新增 TenantID（已澄清）。**
G2 设计 §2.2 发现 7 仅说「未参与 key/路由」。实际代码确认：`Namespace` 已作为命名空间前缀贯入 workflowreg key（`workflowreg/registry.go:58`，形如 `xflow:ns:<namespace>:workflow:{<key>}:...`）、rstate key（`rstate/keys.go`）、trigger key（`trigger/trigger.go`）。`NamespaceAwareAuthorizer` 以 `Principal.Namespace` 做跨命名空间访问校验（`authz.go:179-214`）。**`namespace.Namespace` 就是 xflow 的多租户隔离原语，不需要另建 `TenantID` 字段。** §4.8 关于「新增 `TenantID` 字段」的描述已作废，实际以 `Namespace` 为准。

**不符点 D — trigger 包已预留 tenant 前缀位置但未实现。**
`backend/providers/distributed/internal/trigger/trigger.go:36-40` 注释明确「Tenant prefix is reserved for Task 7.2 (Phase 6/7). When a tenant prefix is added, the expected shape is `xflow:ns:<namespace>:trigger:dedup:<key>`」。这是 G2 设计未提及的良好基础，本设计直接采用此无花括号形状（tenant 前缀必须无花括号，原因见 §4.1）。

**不符点 E — `NamespaceAwareAuthorizer` 已实现（已完成）。**
`NamespaceAwareAuthorizer`（`authz.go:179-214`）已实现并在生产路由使用：`Authorize` 校验 `req.Principal.Namespace != ""` + scope + `req.ResourceNamespace == req.Principal.Namespace`（当 ResourceNamespace 非空时）。`AuthorizationRequest.ResourceNamespace` 字段已就绪（`authz.go:57`）。

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
| trigger | `xflow:ns:rigger:dedup:<key>`、`xflow:ns:rigger:lock:<key>`、`xflow:ns:rigger:state:<scope>:<key>` | `trigger/trigger.go:41-47` | 无（单 key，无需） |
| leader | `xflow:leader:control-plane` | `backend/providers/distributed/backend.go:298` | 无（全局单 key，**不 per-tenant**，见 §3） |
| runner directory | `xflow:runner-directory:{control}` | `redis_runner_directory.go:21` | `{control}` 全局 |

---

## 2. Namespace 定义与来源（已实现）

### 2.1 类型定义

```go
// namespace/namespace.go（已实现）
type Namespace string

const Default Namespace = "default"
```

- `Namespace` 为 `string` 类型别名，服务端签发，**不可信客户端**。
- 保留 `"default"` 作为单命名空间默认值，向后兼容（未配置 namespace 时所有请求归 `"default"`，行为与 G1 单租户等价）。

### 2.2 不可信客户端原则（IDOR 防护，映射组织安全策略 §1）

**核心原则：namespace 必须来自服务端认证后的 principal，忽略请求体任何 namespace 字段。**

- 请求体中出现的 `namespace` 字段一律**忽略不读**。
- Namespace 由 `PrincipalAuthenticator`（`service/apiserver/authz.go:125`）在认证后注入 `Principal.Namespace`。
- 所有下游（backend、rstate、workflowreg、trigger、deadletter、audit、metrics、trace）从 `context.Context` 取 Namespace（`namespace.FromContext`），**不信任请求体**。
- 这映射组织安全策略 §1a「identity must come from the server, never from the client」与 §1c「batch operations check ownership per item」。

### 2.3 Namespace 来源方案

#### 方案 A（推荐，已实现）：B3 principal 扩展 token→namespace 映射

`BearerPrincipalAuth` 已扩展为多 token 注册表：

```
BearerPrincipalAuth {
  principalByHash: map[tokenHash] -> principalEntry{subject, namespaceID, scopes}
}
```

- 配置侧（`cmd/server/main.go`）支持多 token 注册：每个 token 绑定 `(subject, namespace, scopes)`（`TokenPrincipalMapping`，`authz.go:234-242`）。
- `Authenticate` 命中 token 后返回 `Principal{Subject, Namespace, Scopes}`，Namespace 非空（`authz.go:316`）。
- 单命名空间兼容：未配置多 token 时，单 token 映射到 `namespace.Default`。

**权衡**：
- 优点：与现有 B3 bearer 认证路径一致（`authz.go:253-316`），改动集中在 authenticator + cmd flag，不引入新依赖。
- 缺点：token 是长静态 bearer，需配合 token 轮转策略；不适合大规模命名空间（每命名空间一 token，token 数量受管理成本限制）。

#### 方案 B：mTLS client cert → namespace（planned）

- `cmd/server/main.go:385-387` 已支持 `--tls-client-ca`。扩展：从 client cert 的 Subject CN/SAN 映射到 Namespace。
- `PrincipalAuthenticator` 新增 mTLS 实现，按 cert subject → namespace 映射表（server 配置）签发 Principal。

**权衡**：
- 优点：适合 B2B 多租户（每命名空间独立 client cert），凭证生命周期由 PKI 管理；符合组织安全策略 §3「token 须有 timeout」+ §6「传输加密」。
- 缺点：需 PKI 基础设施 + cert 轮转；映射表维护成本。

#### 推荐

- **G2 默认采用方案 A**（最小改动，已实现，覆盖 G2 验收需求）。
- **方案 B 作为部署选项并行支持**（planned，mTLS 已具备，`cmd/server` 加 `--mtls-namespace-map` flag）。两者通过 `PrincipalAuthenticator` 接口可共存（chain authenticator）。
- 单命名空间默认：两种方案均未配置时，所有请求归 `namespace.Default`，行为等价 G1。

---

## 3. Leader 是否 per-tenant（设计决策）

### 3.1 决策：control-plane leader 全局，不 per-tenant

**理由**：
- 避免 N 倍 leader election 开销（每租户一个 leader = N 个 SETNX + 续约 goroutine，Redis 负载线性增长）。
- leader election 在本系统只协调 leader-only maintenance（`lease_sweeper` / `lease_repair`），不协调状态写入（`leader.go:197-204` 注释明确「leadership here only gates background maintenance, not state mutations」）。
- leader key 保持 `xflow:leader:control-plane`（`backend.go:298`），**不改**，全局单一。

### 3.2 leader-only maintenance 需扫所有 namespace

全局 leader 持有 maintenance，但 `lease_sweeper` / `lease_repair` / outbox dispatcher 当前 SCAN 全局 `xflow:ns:<namespace>:exec:{*}:...`（见 §1.3）。加 namespace 前缀后，**全局 leader 需迭代所有 namespace**。

**方案：namespace 注册表 + 按 namespace 迭代**（planned）

- 新增 `xflow:ns:namespaces` Redis SET，记录所有曾出现过的 namespace（namespace 首次写 key 时 `SADD`）。
- `lease_sweeper` / `lease_repair` / outbox dispatcher 先 `SMEMBERS xflow:ns:namespaces`，再按 namespace 迭代 SCAN `xflow:ns:<namespace>:exec:{*}:...`。
- `"default"` namespace 始终在集合中（启动时 `SADD`）。

### 3.3 runner directory 仍全局

`xflow:runner-directory:{control}`（`redis_runner_directory.go:21`）保持全局。runner 归属 tenant 在 **dispatch 路由层**实现（assignment 携带 tenant，dispatcher 按 tenant 路由），不在 directory key 层。见 §4.7。

---

## 4. 子系统改动点清单（文件级，附 file:line）

共 **9 个子系统改动点**。每项标注对应 Phase 7 Task。

### 4.1 rstate key namespace 前缀（Task 7.1，已实现）

**改动（已完成）**：key 函数增加 namespace 参数，前缀注入 namespace。namespace 前缀采用**无花括号**形式 `xflow:ns:<namespace>:exec:{<id>}:...`。

- `backend/providers/distributed/internal/rstate/keys.go`：所有 `execKey`/`nodeStatusKey`/`nodeMetaKey`/`outputKey` 等函数已接受 `namespace.Namespace` 参数（`rstate/keys.go:21-131`），形如 `xflow:ns:<namespace>:exec:{<id>}:...`。
  - `execScanPattern(t namespace.Namespace, suffix string)` 已实现（`keys.go:38`）。
- **关键论证（namespace 前缀必须无花括号）**：同原设计，已在实现中验证。采用 `xflow:ns:<namespace>:exec:{<id>}:node:...` 后，hash tag = `{<id>}`：exec 内所有 key 共置同 slot，不同 exec 按 `<id>` 分布到不同 slot，namespace 仅起命名空间隔离作用。
- **越权断言**：namespace A 的 sweeper 扫不到 namespace B 的 key（miniredis 单测，planned）。

### 4.2 workflowreg namespace scope（Task 7.2，已实现）

**改动（已完成）**：workflow key 已加 namespace 前缀（无花括号，理由同 §4.1）。

- `backend/providers/distributed/internal/workflowreg/registry.go`：
  - `workflowByKeyKey(ns, key)` = `xflow:ns:<namespace>:workflow:{<key>}:bykey`（已实现）
  - `workflowByIDKey(ns, key, id)` = `xflow:ns:<namespace>:workflow:{<key>}:byid:<id>`（已实现）
- `AddWorkflow`/`GetWorkflow`/`RemoveWorkflow` 签名已从 context 取 namespace。
- **idmap 决策**：`workflowIDMapKey`（`registry.go:75`）已加 namespace 前缀，`GetWorkflow` 必须带 namespace。

### 4.3 trigger namespace scope（Task 7.2，已实现）

**改动（已完成）**：trigger key 已加 namespace 前缀（无花括号 `xflow:ns:<namespace>:trigger:...`），采用 `trigger.go:29-31` 注释已预留的形状。

- `backend/providers/distributed/internal/trigger/trigger.go`：
  - `triggerDedupKey(t, key)` = `xflow:ns:<namespace>:trigger:dedup:<key>`（`trigger.go:43`）
  - `triggerLockKey(t, key)` = `xflow:ns:<namespace>:trigger:lock:<key>`（`trigger.go:47`）
  - `triggerStateKey(t, scope, key)` = `xflow:ns:<namespace>:trigger:state:<scope>:<key>`（`trigger.go:51`）

### 4.4 leader（Task 7.x —— 全局不变）

**改动**：**无**。leader key `xflow:leader:control-plane`（`backend.go:298`）保持全局。

- `backend/providers/distributed/leader.go:42-65` `RedisLeaderElector` 不变。
- 但 leader-only maintenance（`lease_sweeper` / `lease_repair` / outbox dispatcher）按 namespace 迭代（§3.2），改动在 sweeper 侧（§4.1 SCAN + §4.6）。
- `service/control/controlplane.go:168-176` leader 取用 + sweeper 门控不变。

### 4.5 dead-letter key namespace 隔离（Task 7.6，部分已实现）

**改动**：dead-letter key 共享 exec 的 namespace 前缀（随 §4.1 完成，此处贯通 manager + API）。

- `backend/providers/distributed/internal/rstate/atomic_state.go:280-294` `replayDeadLetterLua` 的 KEYS 全部带 namespace 前缀（随 §4.1 key 函数改造自动生效）。
- `service/control/deadletter_manager.go:65-67` `List` 签名加 namespace（或从 context 取），传给 `store.ListDeadLetters`（planned）。
- `service/control/deadletter_manager.go:75-104` `Replay` 校验 `req.ExecutionID` 所属 namespace == `principal.Namespace`（planned）。
- `service/apiserver/module_management.go:186,217` `handleDeadLetterList`/`handleDeadLetterReplay` 从 principal 取 namespace 注入 manager 调用（planned）。

### 4.6 outbox namespace 隔离（Task 7.6，部分已实现）

**改动**：outbox SCAN 与 dispatcher 按 namespace 隔离。

- `backend/providers/distributed/internal/rstate/atomic_state.go` outbox SCAN pattern 已加 namespace（随 §4.1）。
- outbox dispatcher（后台重放）按 namespace 迭代（§3.2 namespace 注册表，planned）。
- outbox body / attempts / dead:meta key 全部随 exec namespace 前缀（随 §4.1 key 函数改造）。

### 4.7 runner placement / credential namespace scope（Task 7.5，planned）

**改动**：runner 注册带 namespace 归属，dispatch 按 execution namespace 路由。

- `service/control/redis_runner_directory.go:21` `xflow:runner-directory:{control}` 保持全局（runner directory 是 control-plane 全局目录，runner 可服务多 namespace，但 dispatch 按 namespace 路由）。
- `service/control/runner_directory.go` `Assignment` 结构加 `Namespace` 字段（planned）：assignment 入队时携带 execution 的 namespace。
- `execution/runner.go:33` `WithCredentialResolver` → credential resolver 按 namespace scope 凭证（planned）。
- `cmd/runner/config.go`：runner 注册时声明可服务的 namespace 列表（planned）。
- `cmd/runner/config.go`：runner 注册时声明可服务的 tenant 列表（`tenants: [...]`），server 侧 dispatch 校验。
- asynq 任务携带 tenant：asynq task payload 或 queue 命名空间带 tenant（`backend/providers/distributed/internal/queue/asynq/transport.go` producer enqueue 时注入 tenant 到 payload；`consumer.go:27` 解出 tenant 注入 context）。
- **显式 tenant placement，不能用 runner label 兜底**：`types/workflow.go:83-86` `RunnerSelector.MatchLabels` 不能用于承载 tenant（ha-soak-plan §4 反声明）。runner tenant 归属是 server 端 dispatch 决策，不信任 workflow DSL 里的 label。

### 4.8 API 层签发 + authz（Task 7.3，已实现）

**改动（已完成）**：principal 签发 namespace + authz namespace 校验 + IDOR 防护。

- `service/apiserver/authz.go:21-23` `Principal.Namespace` 已实现，签发处填入（§2.3 方案 A 已完成，`authz.go:316`）。
- `service/apiserver/authz.go:57` `AuthorizationRequest.ResourceNamespace` 已就绪。
- `service/apiserver/authz.go:179-214` `NamespaceAwareAuthorizer` 已实现：`Authorize` 校验 `Principal.Namespace != ""` + scope + `ResourceNamespace == Principal.Namespace`（当 ResourceNamespace 非空时）。
- `service/apiserver/authz_wrap.go` `authzWrap` 从 principal 取 namespace 注入 context（`namespace.WithNamespace`），handler 下游通过 `namespace.FromContext` 读取。
- `service/apiserver/module_management.go:91-107` `handleExecution` tenant 校验（planned）：Inspect 前校验 execID 所属 namespace == `principal.Namespace`，不匹配 404。
- `cmd/server/main.go` principal 装配：按 §2.3 方案 A 配置多 token→namespace 映射已就绪（`NewBearerPrincipalAuthMulti`）。
- `types/workflow.go:8` `WorkflowDef.Namespace` 是隔离原语本身，无需新增 `TenantID` 字段（见 §1.2 不符点 C 已澄清）。

### 4.9 audit / metrics / trace namespace 标签（Task 7.4，部分 planned）

**改动**：audit 贯通 namespace；metrics/log/trace 加 namespace 维度。

- `store/sqlstore/audit.go` `dbAuditEvent.Namespace`（列 `namespace varchar(128)`）已就绪；写入路径 `apiserver.AuditEvent.Namespace` 由 `authz_wrap.go` 从 `principal.Namespace` 取，audit 的 namespace 维度已贯通。
- `store/models.go` `AuditRecord` 的 namespace 字段已就绪。
- `observability/metrics/engine.go:76-81` `nodeLabels` 加 `"namespace"` 维度（planned）。
- `observability/metrics/control.go:93` lease sweep labels 加 namespace 维度（planned）。
- `observability/tracing/tracing.go:77` span `WithAttributes` 加 `namespace` 属性（planned）。
- **高基数风险声明**：namespace 作为 metrics label 维度，基数 = namespace 数量（G2 规模数十），可接受。

---

## 5. API 层 IDOR 防护（映射组织安全策略 §1）

### 5.1 管理端点 namespace 校验

- `/v1/management/executions/{id}`（`module_management.go:91-107`）：
  - **解析顺序（避免鸡生蛋）**：handler 先从已认证 principal 取 `Namespace`（来自 §4.8 签发，不读请求体），再以 `(principal.Namespace, execID)` 调 `Inspect`/`GetWorkflow`。
  - **execID 所属 namespace 校验**（planned）：`Inspect` 前校验 execID 所属 namespace == `principal.Namespace`。exec key 形如 `xflow:ns:<namespace>:exec:{<id>}:...`，从 key 中取出 namespace 段与 `principal.Namespace` 比对。若不匹配，一律返回 404。
  - 不匹配返回 **404**（不返回 403，避免泄漏 execID 存在性——映射安全策略「Return a generic error on authentication failure; do not reveal whether the user exists」）。
- `/v1/management/dead-letters/{execID}` list/replay（`module_management.go:186,217`）：
  - 同样校验 execID.namespace == principal.Namespace（planned），不匹配 404。

### 5.2 批量操作 per-item tenant 校验

- 当前 management 端点无批量 list（`module_management.go:22-24` 注释明确「intentionally provides no listing endpoints for runners/executions」）。dead-letter list 是单 exec 内分页，tenant 校验在 exec 级（§5.1）。
- 若未来引入批量端点，必须 per-item 校验所有 item 所属 namespace == `principal.Namespace`，任一不匹配拒绝整批（映射安全策略 §1c）。

### 5.3 不可预测 ID（映射安全策略 §1e）

- **ExecutionID（已核实，满足要求）**：`engine/engine.go:134,155` 生成路径为 `id := types.ExecutionID("exec-" + uuid.New().String())`，使用 `github.com/google/uuid` 的 `uuid.New()`（UUID v4，122 位随机熵）。`exec-` 仅为可读前缀，熵源为 UUID v4。**非自增、不可预测**，满足组织安全策略 §1e「使用 UUID 或 snowflake，非自增，防枚举」。Task 6.2/7.x 无需改动 ExecutionID 生成路径。子执行 ID（`engine/expand.go:130`）形如 `<parentExecID>/sub/<node>/<leaseID>/<batchIndex>`，父 ID 已是 UUID，碰撞概率可忽略。
- `types.WorkflowID`（`types/workflow.go:3`）当前 `workflowreg/registry.go:227` 已用 `uuid.NewString()`，符合 §1e。
- 结论：ExecutionID 与 WorkflowID 均为 UUID v4 派生，已满足不可预测要求；本设计在 6.1 闭合此项，不再下推至 Task 6.2。后续若引入新 ID 类型，须同样使用 UUID/snowflake，禁止自增。

---

## 6. 实施分阶段（与 G2 设计 Phase 6-8 对齐）

### 6.1 顺序与依赖

```
Phase 6 (namespace 原语)
  6.1 (本文档, 设计) ──► 6.2 (context 原语已就位：namespace.WithNamespace/FromContext；
                              Principal.Namespace 已签发；无需新建 TenantID 类型)
                              │
                              ▼
Phase 7 (全链路改造)
  7.1 (rstate keys) ──┐         ← 已实现
  7.2 (workflowreg+trigger, 合并 Task 2.1) ──┤    ← 已实现
  7.4 (audit/metrics/trace 标签) ──┤         (大体并行，planned)
  7.5 (runner placement/credential) ◄── 7.3 (API 签发+authz) 前置于 7.5/7.6
  7.6 (dead-letter/outbox) ◄── 7.3
                              │
                              ▼
Phase 8 (安全测试)
  8.1 (越权测试套件, 依赖 7.x 全部完成)
  8.2 (诚实性声明, RELEASE-GATES §4 补充)
```

### 6.2 前置关系明细

- **6.1 → 6.2**：context 原语（`namespace.WithNamespace`/`FromContext`）与 `Principal.Namespace` 已就位，无需新建 `TenantID` 类型或改 `types/workflow.go`（§1.2 不符点 C 已澄清）。
- **6.2 → 7.x**：context 原语就位后，全链路改造可开始。
- **7.3 前置于 7.5/7.6**：API 层签发 `Principal.Namespace` 是 runner placement（7.5）与 dead-letter replay 校验（7.6）的前提——后者需从 principal 取 namespace。
- **7.x 全部 → 8.1**：越权测试需全链路就位才能断言跨 namespace 隔离。

### 6.3 可并行

- 7.1（rstate）、7.2（workflowreg+trigger）、7.4（audit/metrics/trace）大体并行，互不依赖。
- 7.5（runner placement）与 7.6（dead-letter/outbox）依赖 7.3 的 principal 签发，但 7.5 与 7.6 之间可并行。
- 7.2 与 Task 2.1 hash tag 修复合并实施（G2 设计 §4 依赖图已标注）。

### 6.4 环境门控

- Phase 6-8 全部**非 ENV-GATED**：miniredis 可执行越权测试，不依赖真实 Redis HA / 多副本。
- 越权测试（8.1）在 `XFLOW_REQUIRE_REDIS_INTEGRATION=1` 下执行（miniredis 嵌入即可）。

---

## 7. 越权测试矩阵（Task 8.1 前瞻）

新增 `test/security/namespace_isolation_test.go`（planned），矩阵：

| 场景 | 期望 |
|---|---|
| namespace A 提交 workflow | namespace A 可 list/get/exec |
| namespace B 尝试 get namespace A 的 workflow | NotFound |
| namespace B 尝试 exec namespace A 的 workflow | Forbidden / NotFound |
| namespace B 尝试 inspect namespace A 的 execution | 404 |
| namespace B 尝试 list/replay namespace A 的 dead-letters | 404 |
| namespace A 提交 workflow，namespace B 提交同名 workflow | 不冲突（不同 namespace 前缀） |
| 重复提交跨 namespace | fencing 不跨 namespace 误判（lease token per exec，namespace 隔离） |
| namespace A 的 sweeper SCAN | 扫不到 namespace B 的 key |
| runner 归属 namespace A 消费 assignment | 不接收 namespace B 的 assignment |
| 请求体伪造 `namespace: "B"`（principal 是 A） | 忽略，按 A 执行 |

- miniredis 可执行，不 ENV-GATED。

---

## 8. 与 G2 设计 §2.2 发现 7 的核对结论

G2 设计 §2.2 发现 7 的 5 项桩字段全部核对确认存在（见 §1.1）。本设计补充 6 项不符/需澄清之处（见 §1.2）：

1. `BearerPrincipalAuth` 不支持 token→namespace 映射（不符点 A）——已实现（§2.3 方案 A，`NewBearerPrincipalAuthMulti`）。
2. runner directory key 全局，runner placement namespace 隔离在路由层（不符点 B）——本设计 §4.7（planned）。
3. `WorkflowDef.Namespace` 就是隔离原语，无需新增 `TenantID` 字段（不符点 C 已澄清）——见 §1.2 不符点 C。
4. trigger 包已预留 namespace 前缀位置（不符点 D，正向）——已实现（§4.3）。
5. `NamespaceAwareAuthorizer` 已实现（不符点 E 已关闭）——`authz.go:179-214`。
6. management 端点无 namespace 校验（不符点 F，IDOR 缺口，planned）——本设计 §5。
7. metrics labels 无 namespace 维度（不符点 G，planned）——本设计 §4.9。

---

## 9. 诚实性声明（贯穿实施）

1. **namespace 前缀 ≠ 加密隔离**：Redis key 前缀是命名空间隔离，不是密码学隔离。跨 namespace 隔离依赖服务端签发 Namespace + 全链路校验 + 越权测试。映射 RELEASE-GATES §4 反声明「namespace 是安全边界」（已证伪）。
2. **runner labels 不是安全边界**：runner placement 用显式 namespace 归属，不用 `RunnerSelector.MatchLabels` 兜底。映射 ha-soak-plan §4 反声明。
3. **subagent 能完成**：Phase 6-8 全部代码 + 越权测试可由 subagent 独立实现 + 评审，miniredis 可执行，非 ENV-GATED。
4. **不等于 G2 完成**：G2 整体完成仍依赖 G1 全满足（当前 ⛔ 未满足）+ HA soak + 多副本 SLO（ENV-GATED）。namespace boundary 完成只解除 G2 退出清单中「namespace boundary 全链路 + 越权测试」一项，不解除其余。
5. **leader 全局不 per-namespace**：避免 N 倍 leader 开销；leader-only maintenance 按 namespace 迭代（§3.2 namespace 注册表，planned）。
6. **高基数风险**：namespace 作为 metrics label 基数 = namespace 数量，G2 规模可接受；namespace × node × status 组合需评估，文档化权衡（§4.9）。
