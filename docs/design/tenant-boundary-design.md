# Tenant Boundary（namespace 隔离）设计文档

> **日期**: 2026-07-19；2026-08-27 按实现更新
> **状态**: namespace boundary 已实现并具备越权测试。2026-07-24 的历史 G1 已在当时的 clean SHA 闭合；当前候选仍须在同一 clean SHA 重签 G0/G1，历史闭合不撤销、当前候选也不自动继承。
> **范围**: G2 多租户产品能力的 namespace 隔离边界，包括认证来源、持久化 key、runner placement、management API、dead-letter/outbox、审计与可观测性。
> **技术原语**: 当前隔离原语统一为 `namespace.Namespace`；“tenant/多租户”仅用于产品语境或历史说明，不代表另有 `TenantID` 安全边界。
> **G2 边界**: 本文所述 namespace boundary 的代码与本地/集成测试已闭合；真实 Redis HA、多副本 SLO、HA soak 与真实多 namespace 环境验收仍是 ENV-GATED，不因本文闭合而自动完成。

---

## 0. 诚实性前置声明

1. **namespace 前缀不是密码学隔离**：隔离成立依赖服务端认证 principal、context 传播、全链路 namespace-scoped 读写以及越权测试。
2. **客户端不是身份来源**：请求体、workflow DSL 或 runner label 都不能声明调用者的 namespace；HTTP 控制面以认证后的 `Principal.Namespace` 为准。
3. **runner labels 不是安全边界**：runner 通过显式 `Namespaces` 注册，assignment/claim 在服务端按 namespace 过滤。
4. **实现闭合不等于候选签署或 G2 完成**：历史证据、当前候选 clean-SHA 证据和真实环境验收是三个不同事实，不能互相替代。

---

## 1. 当前实现核对

### 1.1 隔离原语与传播

| 能力 | 当前实现 |
|---|---|
| namespace 类型 | `namespace.Namespace`，缺省值为 `namespace.Default`（`"default"`） |
| HTTP 身份 | `Principal.Namespace` 由 authenticator 签发，`authzWrap` 注入 context |
| workflow | 注册/替换路径使用认证 principal 的 namespace 覆盖并校验定义，不能靠请求体越权 |
| engine/store | 下游通过 `namespace.FromContext` 或显式 `namespace.Namespace` 参数传递 |
| runner | 注册 `Namespaces`，assignment 带 `Namespace`，claim 按 namespace 过滤 |
| audit/dead-letter | 审计记录、dead-letter list/replay 均使用服务端 namespace context |
| metrics/tracing | metrics 加 namespace label；span 自动加 `namespace` attribute，且不写入 baggage |

全仓不需要另建 `TenantID` 类型。历史设计中的 tenant 字段已被当前 namespace 原语取代；兼容 schema、测试夹具和产品示例中的 `tenant` 文字不构成第二套身份来源。

### 1.2 Redis key schema

| 子系统 | 当前 key / scope | 说明 |
|---|---|---|
| rstate execution、lease、outbox、dead-letter | `xflow:ns:<namespace>:exec:{<id>}:...` | `{<id>}` 保持单 execution 的 Redis Cluster 共槽语义 |
| workflow registry | `xflow:wfreg:v2:{ns:<sha256(namespace)>}:...` | v2 digest authority；namespace hash tag 隔离 registry authority |
| trigger | `xflow:ns:<namespace>:trigger:...` | dedup、lock、state 均按 namespace 隔离 |
| namespace registry | `xflow:namespaces` | `ListNamespaces` 在返回结果中隐式保证 `default`，不要求 Redis SET 实际存有该成员 |
| leader | `xflow:leader:control-plane` | 全局 leader，只门控 maintenance，不作为 namespace 安全边界 |
| runner directory | `xflow:runner-directory:{control}` | 目录全局；namespace 约束位于注册、assignment 与 claim 路由层 |

### 1.3 已有安全证据

以下测试覆盖不同层次的 namespace 隔离：

- `test/security/namespace_isolation_test.go`：跨 namespace workflow/execution/maintenance 行为；
- `service/apiserver/namespace_idor_test.go`：management API 跨 namespace 返回 404；
- `backend/providers/distributed/internal/rstate/namespace_isolation_test.go`：Redis key 与扫描隔离；
- `service/control` 的 runner directory namespace 测试：注册与 claim 过滤；
- `observability/metrics`、`observability/tracing` 的 namespace 测试：label/span attribute 传播且不进入 baggage。

---

## 2. Namespace 定义与身份来源

### 2.1 类型与默认值

```go
type Namespace string

const Default Namespace = "default"
```

`default` 保持单 namespace 部署的向后兼容。空值在服务端边界归一化；它不是允许调用者绕过隔离的通配符。

### 2.2 认证来源

当前 bearer 路径通过 `NewBearerPrincipalAuthMulti` 接受 token 到 `(subject, namespace, scopes)` 的映射；`cmd/server` 的 auth-token 配置可为不同 namespace 签发不同 principal。未配置多 namespace 映射时落到 `namespace.Default`。

mTLS client certificate 到 namespace 的独立映射仍可作为后续部署选项，但它不是当前 boundary 是否成立的前提；现有 bearer principal 已提供服务端可信身份来源。

### 2.3 不可信客户端原则

- namespace 必须来自认证后的 principal，而不是请求 JSON、header 中自报字段或 workflow DSL；
- workflow 注册/替换会以 principal namespace 作为权威值；
- backend、rstate、workflow registry、trigger、dead-letter、audit 与可观测性从 context 或显式强类型参数取得 namespace；
- 跨 namespace 查找对外统一表现为 NotFound/404，避免泄露目标是否存在。

---

## 3. Leader、namespace registry 与 runner directory

### 3.1 全局 leader

control-plane leader 保持全局，不按 namespace 创建 leader。leader election 只门控 lease/outbox/timeout 等后台 maintenance，不协调普通状态写入，因此按 namespace 扩张 leader 会增加开销而不增加隔离强度。

### 3.2 namespace registry 与 maintenance

namespace registry 已实现为 `xflow:namespaces`。`ListNamespaces` 读取注册值并在内存结果中隐式加入 `default`。leader-only maintenance 通过该接口迭代 namespace，覆盖 lease scan/repair、outbox discovery/metrics、receipt scan 与 timeout monitor；不存在只扫描旧全局 key 或只处理 `default` 的设计前提。

### 3.3 runner directory

runner directory 仍是 control-plane 全局目录。安全约束由 `RegisterRunnerRequest.Namespaces`、`Assignment.Namespace` 和 `ClaimForRunner` 的 namespace 过滤共同完成，而不是通过拆分 directory key 或信任 `RunnerSelector.MatchLabels` 实现。

---

## 4. 子系统闭合快照

### 4.1 rstate execution / lease key

rstate 的 execution、node、lease、receipt、outbox 与 dead-letter key 均位于 `xflow:ns:<namespace>:exec:{<id>}:...`。namespace 前缀不使用 Redis hash-tag 花括号，因此 execution 内 key 仍以 `{<id>}` 共槽，不同 execution 仍可分布到不同 slot。

### 4.2 workflow registry 与 trigger

workflow registry 已切换到 namespace digest 隔离的 v2 authority：`xflow:wfreg:v2:{ns:<sha256(namespace)>}:...`。trigger 的 dedup/lock/state key 已使用 `xflow:ns:<namespace>:trigger:...`。两者都以服务端 context 中的 namespace 为准。

### 4.3 namespace registry 与后台扫描

`RegisterNamespace`/`ListNamespaces` 已落地，registry key 为 `xflow:namespaces`。lease scan/repair、outbox discovery/metrics、receipt scan 和 timeout monitor 都按 `ListNamespaces` 结果遍历；`default` 由 API 隐式补齐。

### 4.4 dead-letter 与 outbox

Dead-letter store 从 context 取得 namespace。manager list/replay 使用 principal/context 的 namespace，并在 replay 前保持 namespace 一致性；API 的跨 namespace 请求返回 404。outbox body、attempt、dead metadata 与 discovery 都沿用 namespace-scoped execution key，后台 dispatcher 按 registry 迭代。

### 4.5 runner placement、credential 与 queue payload

- `Assignment.Namespace` 已实现，`ClaimForRunner` 只返回 runner 已声明 namespace 内的 assignment；
- runner 配置字段为 `namespaces`，CLI 使用可重复的 `--namespace`；
- credential resolver 的签名包含 `namespace.Namespace`，凭证解析不会退化为全局查找；
- queue payload 携带 `_namespace` 并在消费侧恢复 context；
- runner label 只用于能力匹配，不承载 namespace 身份。

### 4.6 API 签发与 IDOR 防护

`Principal.Namespace`、`NamespaceAwareAuthorizer` 与 context 注入均已用于生产路由。Management execution/dead-letter handler 在 principal namespace 的 store 视图中查询；另一个 namespace 的相同 ID 自然得到 NotFound，并统一映射为 404，不从 execution ID 或 Redis key 文本反向解析 namespace。

### 4.7 audit、metrics 与 tracing

审计事件和 SQL audit record 已包含 namespace。metrics 通过 `withNamespace` 追加 namespace label；tracing `Start` 自动写入 `namespace` span attribute。namespace 明确不进入 baggage，避免跨进程把不可信 baggage 当成授权依据。

### 4.8 默认 namespace 与真实多 namespace 入口

`default` 是兼容默认值，不代表生产只能运行单 namespace。HTTP workflow 注册会使用认证 principal 的 namespace，server 支持 token→namespace 映射，runner 也支持 `--namespace`/`Namespaces` 注册；是否直接调用 `WorkflowBuilder.Namespace` 不能推导整条生产链路只会落到 `default`。

### 4.9 证据边界

本地/miniredis 与 API 测试证明代码级隔离语义；它们不能替代真实 Redis HA、多 control-plane 副本、runner 重连和真实多 namespace 部署下的 G2 验收。环境证据必须在候选 SHA 上单独采集并归档。

### 4.10 artifact namespace scope（本节 2026-08-27 新增，此前 artifact 在本文件零命中）

**隔离边界已实现且有测试，不是待办**：

- `service/apiserver/module_artifact.go:88-98` `handleArtifact`：取 `namespace.FromContext(r.Context())`，
  经 `artifacts.HasReference(ctx, ns, digest)` 判定，不通过一律 404。
- `store/sqlstore/artifact.go:219-227` `HasReference` 是
  `SELECT 1 FROM xflow_artifacts WHERE namespace = ? AND content_hash = ?`，精确匹配，无例外通道。
- `module_artifact.go:83-85` 注释声明「拒绝一律答 404 而非 403」，避免用状态码泄露
  「该 digest 是否存在于别的租户」。
- 进程内第二道：`node/internal/code/script/artifact_cache.go:43-47` 缓存键是
  `(namespace, digest, language)`，namespace 来自 `types.Input.Namespace()`（引擎侧注入，
  非节点参数），防止缓存跨租户串味。
- 测试：`service/apiserver/artifact_endpoint_test.go:137`（`TestArtifactEndpointTenantIsolation`）、
  `test/integration/artifact_endpoint_test.go:67`（MySQL 版）实测 tenant-a 拿 tenant-b 的 digest 得 404。

**待办 1（延后到租户专项讨论）：跨 namespace 引用的可控放开。**

多租户下需要「平台方发布一份官方 wasm，各租户直接引用」的形态，今天做不到——上面那条边界是硬的。
放开时的约束：

- **只加一个字段**：`xflow_artifacts` 加 `visibility ENUM('private','public') DEFAULT 'private'`。
  **不建 grants 表**（「只授权给租户 X、Y」目前是想象出来的需求，没有场景驱动）；
  真出现细粒度需求时 `visibility` 退化为 grant 表的特例，迁移干净。
- **默认私有**，放开必须是一次显式且被记录的动作；「未设置」不得等于公开。
- **「谁在引用」的身份只能来自服务端**（workflow 注册记录 / principal），
  **绝不能**从 workflow 定义里的任何字段取——否则租户 B 自称租户 A 即越权（组织安全策略 §1a）。
  被引用方可以写在定义里，引用方不行。
- **撤销必须真生效**。若校验只在注册时做一次，撤销后已激活的引用照跑，
  形成「以为关了实际还开着」的状态。至少需要反向引用查询（给定 artifact 列出引用它的 workflow），
  但 xflow 今天连 `GET /v1/workflows` 列表路由都没有（`service/apiserver/module_control.go:90-94,156-161` 零命中），
  registry 也无前缀/条件查询（`backend/workflow_registry.go:232-247`）。
  够不着时的诚实退化是「撤销只对新激活生效」并在页面写明，不是假装立即生效。

**待办 2：artifact 存储层没有防御性 namespace 归一化。**

supply 侧有对称的 `normSupplyNS`（`store/sqlstore/supply.go:26-33`，读写双向调用，
注释自述是为修「写 default、读空串 → 恒 not found」打的补丁）；
**artifact 侧搜不到等价函数**——`store/sqlstore/artifact.go` 的 `Put`/`Bind`/`HasReference`
对传入字符串原样精确匹配，不做空串兜底。当前不出问题只因两侧调用方在上层各自归一化好了
（写路径 `sdk/xflow/artifact_resolve.go:55,119` 用已被 `builder.build()` 兜底的 `def.Namespace`；
读路径用 `namespace.FromContext`，两者都恒非空）。
**任何绕过这两个入口、直接拿裸字符串调 `ArtifactStore.Put`/`HasReference` 的新代码都会重演 supply 那个 bug。**
是结构隐患，不是已发生故障。

**现状备注**：生产代码里没有一处调用 `WorkflowBuilder.Namespace(...)`
（`sdk/xflow/builder.go:167` 是唯一入口，非测试文件零命中），
所有 workflow / artifact / runner token 都落在 `builder.build()` 兜底出的 `"default"`
（`sdk/xflow/builder.go:353-356`）。多 namespace 目前只存在于测试。
即三者今天能互相找到，是「从未设置过非默认值」的必然结果，不是设计出来的一致性。


> **2026-08-27 事实更正（不改变本节 artifact 隔离结论与两个待办）**：上文“所有 workflow / artifact / runner token 都落到 `default`、多 namespace 只存在于测试”的推断不成立。HTTP workflow 注册会以认证 principal 的 namespace 覆盖定义；生产 server 支持 auth-token 到 namespace 的映射，runner 也支持 `--namespace`/`Namespaces`。没有直接调用 `WorkflowBuilder.Namespace(...)`，不能证明生产控制面只使用 `default`。

---

## 5. API 层 IDOR 防护

### 5.1 execution 与 dead-letter

Management handler 先从认证 principal 注入的 context 取得 namespace，再调用 namespace-scoped `Inspect`、dead-letter list/replay。实现不从 `execID` 文本解析 namespace，也不先做全局查找；目标仅存在于其他 namespace 时，当前 namespace 的 store 查询自然返回 NotFound，对外统一为 404。

### 5.2 批量操作

当前 management API 没有跨 execution 的批量列表。未来若加入批量操作，必须逐项以 principal namespace 查询并校验；不能因批次中某一项合法而放行其他 namespace 的 item。

### 5.3 ID 不可预测性

ExecutionID 与 WorkflowID 使用 UUID v4 派生，不依赖自增序列。不可预测 ID 只能降低枚举概率，不能替代上述 namespace-scoped 授权与 404 行为。

---

## 6. Phase 6–8 历史闭合快照

原 Phase 6–8 计划已转为实现闭合记录：

1. **Phase 6**：`namespace.Namespace`、context 传播和 `Principal.Namespace` 已完成；
2. **Phase 7**：rstate、workflow registry、trigger、API/authz、runner placement/credential、dead-letter/outbox、audit/metrics/tracing 已完成；
3. **Phase 8**：跨 namespace store、API、runner 与可观测性测试已存在。

这些测试本身不依赖真实 HA 环境；真实多 namespace 部署、Redis HA、control-plane 多副本和 SLO/soak 仍按 G2 ENV-GATED 流程验收。2026-07-24 历史 G1 闭合是历史事实；当前候选须在同一 clean SHA 重新签署 G0/G1，不能引用历史签署代替。

---

## 7. 越权测试矩阵（当前证据）

| 场景 | 当前预期 |
|---|---|
| namespace A/B 注册同名 workflow | authority 与 key 空间互不冲突 |
| B 查询或执行仅存在于 A 的 workflow | NotFound / 404 |
| B inspect A 的 execution | 404，不泄露存在性 |
| B list/replay A 的 dead-letter | 404 |
| A 的 sweeper/repair/outbox scan | 不处理 B 的 execution key |
| 仅声明 A 的 runner claim assignment | 不接收 B 的 assignment |
| 请求体伪造 namespace B、principal 为 A | 忽略伪造值，按 A 执行 |
| metrics/span 记录 | 有 namespace label/attribute；namespace 不进入 baggage |

核心证据分布于 `test/security/namespace_isolation_test.go`、`service/apiserver/namespace_idor_test.go`、rstate namespace isolation 测试、runner directory namespace 测试以及 metrics/tracing namespace 测试。

---

## 8. 与早期 G2 设计的核对结论

早期发现现按当前实现解释：

1. `WorkflowDef.Namespace` 已参与注册与隔离，但认证 principal 才是 HTTP 调用者身份权威；
2. runner directory key 保持全局，namespace 隔离由 `Namespaces`/assignment/claim 路由实现；
3. trigger 已使用 namespace-scoped key，而非仅预留前缀；
4. `NamespaceAwareAuthorizer`、management 404、dead-letter replay 校验均已落地；
5. metrics/tracing 已有 namespace 维度；
6. namespace registry 与 leader-only maintenance 的按 namespace 迭代已落地。

因此这些条目不再是 Phase 7/8 的代码待办。G2 剩余项是候选 clean-SHA 证据与真实 HA、多副本、真实多 namespace 环境验收。

---

## 9. 诚实性声明

1. **namespace 是服务端授权边界，但 Redis 前缀不是加密机制**；绕过服务端直接访问 Redis 不在此授权模型内。
2. **runner labels 不是安全边界**；只能使用显式 namespace 注册和服务端 claim 过滤。
3. **`default` 是 API 隐式保证**；不能假设它一定实际存在于 `xflow:namespaces` SET。
4. **namespace label 有基数成本**；规模评估必须考虑 namespace × node × status，但不能为降低基数而删除授权审计维度。
5. **历史 G1 已闭合，当前候选待重签**；两者不矛盾，也不能互相替代。
6. **namespace boundary 闭合不等于 G2 完成**；真实 HA、多副本 SLO、HA soak 与真实多 namespace 部署证据仍是 ENV-GATED。
