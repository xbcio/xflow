# xflow API 接口规范

> Status: **规范已定，实现待迁移**。本文件是 xflow 全部 HTTP 接口的唯一命名与
> 形状依据。新增端点必须先满足本规范；与本规范冲突的既有端点列在
> [§9 待迁移清单](#9-待迁移清单)，迁移完成后从该节删除。

## 0. 总纲：两族规则

xflow 的 HTTP 端点服务于两类性质不同的调用方，用同一套规则套两者必然有一边被
削足适履。因此规范分为两族：

| | 用户面 / 管理面 / server↔server | runner 协议面 |
| --- | --- | --- |
| 范围 | `/v1/workflows*`、`/v1/executions*`、`/v1/supplies*`、`/v1/artifacts*`、`/v1/management/*` | `/v1/runners/*`（7 条） |
| 风格 | REST | RPC |
| 响应体 | 统一信封（§3） | 裸结构体，不包信封 |
| 版本 | URL 前缀 `/v1` | `ProtocolVersion` 字段 |
| 调用方 | 浏览器前端、SDK、未来的对端 xflow | runner 进程 |

**为什么 runner 面不包信封。** runner 协议的 register/heartbeat/poll/result 四条
路由，HTTP 与 gRPC 共用同一套 `protocol.XxxRequest/Response` Go 类型（经
`FromProto`/`ToProto` 转换，见 `service/control/grpc_server.go`）。给 HTTP 加信封
只有两个结果：要么 gRPC 也带上 `success`/`code`——而 gRPC 有原生 status，这是反
模式；要么两侧类型分叉，滚动升级窗口随之破裂。此外 entry-seed 的 offset 安全判
别依赖 HTTP 状态码与响应体形状（§8.2），信封化会迫使它重写。收益方面，runner
客户端由本仓库自己编写，「统一形状」带来的互操作收益接近零。

**server↔server 归入用户面**，不设第三族。未来的对端 xflow 通过 Relay Gateway
或 SDK 调用，其身份是「另一个调用方」而非「runner 进程」，走对外契约正好。

---

## 1. 路径语法（用户面）

### 1.1 资源命名

- 资源段一律**复数、kebab-case**：`/v1/workflows`、`/v1/dead-letters`
- 「操作单个资源」由路径参数 `{id}` 表达，**不去复数化集合名**。单数路径段在
  REST 惯例里表示 *singleton* 资源（如 GitHub `/user`、K8s `/version`），xflow
  没有这类资源
- 全仓已统一为复数：`/v1/executions`、`/v1/runners/*`、`/v1/supplies/`、
  `/v1/artifacts/`、`/v1/management/dead-letters/`

### 1.2 动词

- **动词不做路径层级。** `register`、`invoke` 这类必须消除：要么变成 HTTP 方法，
  要么变成子资源，要么变成动作后缀
- **禁止把中间路径段当资源层级。** `DELETE /v1/workflows/register/{id}` 是反例
  ——`register` 是动词，不是 workflow 的下级资源
- **允许动作后缀**：`POST /v1/{资源}/{id}/{动作}`，动作用祈使动词
  （`execute`、`cancel`、`publish`），且**只在 REST 语义确实表达不了时使用**。
  这与 n8n（`POST /workflows/:id/execute`）一致

### 1.3 路由注册

全部路由使用 **Go 1.22 mux pattern**：

```go
mux.HandleFunc("POST /v1/workflows/{id}/execute", h)
```

禁止手工 `strings.TrimPrefix` + `Split` 解析路径。现状是全仓零处使用 mux
pattern，导致 `/v1/executions/` 与 `/v1/management/dead-letters/` 的路径解析各自
实现了两遍（`handleExecution` 与 `resolveExecutionRoute`；`handleDeadLetters` 与
`deadLetterExecID`），必须逐处同步，是 drift 源头。

---

## 2. 路径常量

### 2.1 集中声明

- runner 面路径已全部是 `service/protocol/*.go` 里的常量
- 用户面路径**不得散落字面量**，集中到 `service/apiserver/paths.go`

### 2.2 死常量守卫

**每个导出的路径常量必须能在 mux 路由表中找到对应注册。** 配守卫测试。

这条规则针对已发生的事故：`service/protocol/activation.go` 声明了
`ActivatePath = "/v1/runners/activate"`、`DeactivatePath`、`ActivationListPath =
"/v1/activations"` 三个常量，三者**零注册、零 handler、零调用**。真实的激活机制
是 heartbeat 响应携带 `Activations` 指令，runner 收到后本地调用
`TriggerActivationHandler.Activate/Deactivate`，根本不经过 HTTP。照着这三个常量
去调的人得到 404，且会误以为存在一套 HTTP 激活协议。

同类问题也存在于 Op 常量（§6.1）。

---

## 3. 响应信封（用户面）

### 3.1 形状

```json
{
  "success": true,
  "code": "200",
  "message": "",
  "data": {},
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

| 字段 | 类型 | 约定 |
| --- | --- | --- |
| `success` | bool | 与 HTTP 状态码 2xx **严格一致**，不得出现 200 + `success:false` |
| `code` | string | 成功固定 `"200"`；失败为**稳定的业务错误码**，snake_case |
| `message` | string | 人读文案，可变更、可本地化。成功时为 `""` |
| `data` | any | 成功时的载荷；失败时为 `null` |
| `trace_id` | string | 见 §5 |

### 3.2 `code` 的稳定性契约

`code` 是**机读标识**，一旦发布不得随文案改动而变化。客户端据此做错误分支与本地
化，`message` 仅供直接展示。

现状是同一层里三种风格并存：`"workflow not found"`（带空格的英文短语）、
`"workflow_unknown"`、`"stale_generation"`。全部收敛为 snake_case 业务码。

### 3.3 集合响应

列表**永远**是对象包裹，不是裸数组：

```json
{
  "success": true,
  "code": "200",
  "message": "",
  "data": { "list": [], "total": 0 },
  "trace_id": "..."
}
```

- `list` 为空时返回 `[]`，**不得返回 `null`**（前端 `.map` 会抛异常）
- `total` 是**过滤后**的总数，不是全表总数

请求参数：

```
GET /v1/workflows?page=1&page_size=20
```

- `page` 从 **1** 开始。前端表格组件（含 antd `Table`/`Pagination`）默认 1-based，
  0-based 会全局踩坑
- `page_size` 默认 20，**服务端强制上限 200**。安全规范要求敏感数据枚举端点不得
  全表遍历，这个上限是那道闸
- 本规范当前只为**面向页面的列表**定义 offset 分页。深翻页性能与翻页期间的漏读/
  重读是已知代价，对页面列表可接受

**既有的游标端点不改。** `GET /v1/management/dead-letters/{execID}` 已经是游标分
页（请求 `cursor` + `limit`，响应 `next_cursor`）。它服务的是运维对账这类机器扫
描——要求全量遍历不重不漏，正是 offset 分页做不到的。改成 offset 是倒退。

因此规则是：**页面列表用 offset，机器扫描用游标，同一端点不混用两套**。新增端点
默认 offset；确有全量扫描需求时按 dead-letters 的形状定义游标，并在本文件登记。

### 3.4 非 JSON 端点的例外

以下端点成功时返回裸流，**不包信封**：

| 端点 | Content-Type | 原因 |
| --- | --- | --- |
| `GET`/`HEAD` `/v1/artifacts/{digest}` | `application/octet-stream` | 多 MB 产物包进 JSON 需 base64，体积翻 1.33 倍 |
| `GET /v1/supplies/...` | 裸字节（含密文分支） | 同上 |
| `GET /metrics` | Prometheus 文本 | 格式由抓取端规定，promhttp 直接持有 ResponseWriter |

**但失败时仍返回 JSON 信封。** 这是唯一自洽的分法——否则错误信息无处安放。

### 3.5 安全约束（org policy §7 + 本分支特化）

`message` 与 `data` **绝不**包含：节点输出内容、凭证、token、`ulp-token`、
`access_token`、`refresh_token`、AK/SK、私钥、数据库连接串、SQL 语句、调用栈、
服务器路径、完整手机号、身份证号、银行卡号。

本分支的特化规则，逐字保留：

> 编译期错误/警告只带节点名、被引用的节点名、参数名。运行期取件失败的错误只带
> execution ID、节点名、被引用节点名——**绝不带节点输出内容**（上游节点的输出经
> 常就是含 token 的 HTTP 响应体）。

---

## 4. HTTP 状态码

### 4.1 真实反映结果

信封里有 `success`/`code` **不代表状态码可以恒为 200**。两者分工不同：状态码是
传输层的粗分类，供网关、负载均衡、监控、重试策略、`curl` 使用；`success`/`code`
是业务层的细分标识，供应用代码使用。

行业实现一致采用真实状态码：n8n（`res.status(404).json(...)`，见
`docs/references/n8n/core-components.md`）、GitHub REST、Stripe、Kubernetes
（`Status` 对象带 `code` 字段，同时状态码真实）、Temporal、Airflow 2。

**禁止用 200 携带错误。**

### 4.2 状态码语义

| 码 | 用途 |
| --- | --- |
| 200 | 读取成功、动作执行成功 |
| 201 | 创建成功，**必须**带 `Location` 响应头 |
| 202 | 已受理，尚未完成 |
| 204 | 成功且无响应体 |
| 400 | 请求体格式错误、必填字段缺失 |
| 401 | 未认证 |
| 403 | 已认证但无权限 |
| 404 | 资源不存在**或调用方无权得知其存在**（默认拒绝，不泄露存在性） |
| 409 | 状态冲突、幂等重放、栅栏拒绝 |
| 412 / 428 | 条件请求失败 / 要求条件请求 |
| 413 | 请求体超限 |
| 429 | 限流 |
| 503 | 后端依赖不可用 |

---

## 5. trace_id 与 X-Request-Id

两个字段各司其职，**不是同一个东西的两种来源**。

### 5.1 `trace_id`（信封内，服务端权威）

取值优先级：

1. 入站请求带 W3C `traceparent` → 提取其中的 trace ID
2. 否则 → 服务端新建

**必为 W3C 32-hex 格式。** 理由：`observability/tracing/tracing.go` 的
`TraceIDFromContext` 已经在向审计表提供 `trace_id`，且已支持从入站 `traceparent`
提取；OTel propagator 已装配（`observability/tracing/provider.go`）。同名字段必须
共享同一取值空间，否则查链路时语义分叉。

### 5.2 `X-Request-Id`（请求头/响应头，客户端权威）

- 客户端**可选**提供
- 服务端校验：长度 ≤ 128，字符集限 `[A-Za-z0-9._-]`
- 校验通过 → **原样回显到响应头**，不进信封
- 校验不通过 → 视同未提供，**不报错**（这不是业务失败）

**校验是强制的。** 该值会进入日志与审计，不校验等于允许外部注入换行伪造日志行。
这与 org policy §7 方向相反但同等重要：§7 防止内部敏感数据外泄，本条防止外部数
据注入内部记录。

### 5.3 两者如何共同满足「优先透传客户端的」

- 已接入 OTel 的调用方走 `traceparent`——带完整链路（trace ID + span ID + 采样决
  策），比孤立字符串更完整，且能与 SAS 侧的跨系统链路对齐
- 未接入 OTel 的调用方塞 `X-Request-Id`——在响应头拿回自己的 ID 用于关联重试
- 两者都没有——服务端生成

### 5.4 与既有 `RequestID` 的区分

`service/apiserver/module_management.go` 中 dead-letter 的 `RequestID` 字段
**不是** HTTP 请求 ID，而是重放操作的**业务幂等键**——它在一次重放请求的整个生
命周期内保持稳定，跨多次 HTTP 调用不变（见
`service/control/deadletter_projector.go` 的 `AuditID` 说明）。不要与本节的两个字
段混淆。

### 5.5 runner 面

runner 协议**不加 `trace_id` 字段**，链路透传走 OTel `traceparent` 请求头（已在
运行）。

---

## 6. 认证与授权

### 6.1 Op 登记是强制的

每条新路由**必须**在 `service/apiserver/authz.go` 的 `scopeForOperation` 中登记
其 Op。该函数 `default` 分支返回 `""`，而空 scope 被 `ScopeAuthorizer` 与
`NamespaceAwareAuthorizer` 双双拒绝——**漏登记的路由是静默不可达的**，不会报错，
只会 403。

配守卫测试：**所有已注册路由都有对应 Op，且所有 Op 都有路由消费**。

反向的死代码同样要防：`OpWorkflowDefinitionCreate`、`OpWorkflowDefinitionRead`、
`OpWorkflowDefinitionUpdate`、`OpWorkflowDefinitionValidate`、
`OpWorkflowDefinitionPublish`、`OpWorkflowExecutionInvoke`、`OpWorkflowRead` 这 7
个 Op 常量除 `authz.go` 自身外**没有任何路由消费**——它们是为一份从未实现的契约
准备的（§9）。`OpManagementWrite` 同样零引用。唯一的例外是
`OpWorkflowRead` 在 `sqlaudit_test.go` 里被当作任意占位值使用，那不是消费点。

### 6.2 namespace 只能从认证主体取

namespace **只能**由 authz 包装器从认证主体注入 request context，handler 通过
`namespace.FromContext` 读取，**绝不从请求体取**。

entry-seed 端点（`handleSeedExecution`）目前做对了这一点，本规范将其固化：请求体
携带的 `admission_key` 是幂等键，其命名空间归属由服务端裁定，防止伪造跨命名空间
的 admission key。

### 6.3 两套认证体系不混

| 面 | 认证机制 |
| --- | --- |
| 用户面 / 管理面 | apiserver 的 principal/authz 体系，Bearer token，Op + scope |
| runner 协议面 | `Core.authn()` 的 runner directory 认证，`AuthToken` 字段 + SessionID |

**跨界端点必须显式记录选了哪套及理由。** 现存唯一的跨界端点是
`POST /v1/executions`（entry-seed）：它由 runner 调用，却挂在 principal/authz 下
（`OpExecutionSeed`）。理由是它需要 namespace 从认证主体注入这道防伪造机制，而
runner directory 认证不提供。这个选择本规范予以保留（§7 说明）。

---

## 7. 用户面路由表（目标形态）

```
POST   /v1/workflows                        注册定义 → workflow_id
GET    /v1/workflows                        列表（分页）
GET    /v1/workflows/{id}                   读取
PUT    /v1/workflows/{id}                   全量更新
DELETE /v1/workflows/{id}                   注销
POST   /v1/workflows/{id}/execute           执行已注册的工作流
POST   /v1/workflows/execute                内联定义直跑

GET    /v1/executions                       列表（分页）
GET    /v1/executions/{id}                  查询
POST   /v1/executions/{id}/cancel           取消
POST   /v1/executions/{id}/signals          发信号
DELETE /v1/executions/{id}/signals/{name}   撤销信号
GET    /v1/executions/{id}/wait             等待完成

POST   /v1/executions                       entry-seed（runner 调用）

GET    /v1/supplies/{name}                  取 supply 内容（裸流）
PUT    /v1/supplies/{name}                  写 supply 内容
GET    /v1/artifacts/{digest}               取产物字节（裸流，另支持 HEAD）

GET    /v1/management/leader                leader 状态
GET    /v1/management/runners/{id}          单 runner 查询
GET    /v1/management/executions/{id}       execution 查询（管理视角）
GET    /v1/management/dead-letters/{execID}          列表（游标分页，保持不动）
POST   /v1/management/dead-letters/{execID}/replay   重放

GET    /healthz                             存活探针（无信封、无认证）
GET    /readyz                              就绪探针（无信封、无认证）
```

`/healthz`、`/readyz` 是 K8s 探针契约，不进信封也不需要认证；判据在 HTTP 状态码
上，响应体（`{"ready":...,"leader":...}`）仅供人看。

### 7.1 为什么内联直跑不占用 `POST /v1/executions`

「内联定义直跑」语义上确实是「创建一个 execution」，放 `POST /v1/executions` 更
REST。但该路径已被 entry-seed 占用，而 entry-seed 迁移的代价不是改路径，是**重新
设计一条端点的认证归属**：

- 它现在走 principal/authz，namespace 由包装器从认证主体注入以防伪造
- `/v1/runners/*` 全部走 runner directory 认证，不提供 namespace 注入
- 挂 runner 认证 → 丢失防伪造；挂 principal → runner 路由表里出现一条异类

为路径美观去动一条承重的安全机制，不划算。因此内联直跑放
`POST /v1/workflows/execute`，与 `POST /v1/workflows/{id}/execute` 形成「无 id 走
内联、有 id 走已注册」的对称，语义清晰。

### 7.2 management 与 executions 的重复

`GET /v1/management/executions/{id}` 与 `GET /v1/executions/{id}` 目前是两份近乎
相同的 handler 实现（`module_management.go` 的 `handleExecution` 与
`module_control.go` 的 `handleInspect`），都调用 `eng.Inspect`，却挂在不同 Op
（`OpManagementRead` vs `OpExecutionRead`）下。

**合并为一个 handler，保留两个 Op。** 两个 Op 是有意义的——运维 token 与业务
token 应当可以分别授予；两份实现则纯属 drift 风险。

---

## 8. runner 协议面规则

### 8.1 路由表（保持不变）

| 路径 | 常量 |
| --- | --- |
| `POST /v1/runners/register` | `protocol.RegisterRunnerPath` |
| `POST /v1/runners/heartbeat` | `protocol.HeartbeatPath` |
| `POST /v1/runners/poll` | `protocol.PollTaskPath` |
| `POST /v1/runners/result` | `protocol.ReportResultPath` |
| `POST /v1/runners/lease/renew` | `protocol.RenewLeasePath` |
| `POST /v1/runners/activation/ack` | `protocol.ActivationAckPath` |
| `POST /v1/runners/metrics` | `protocol.ReportMetricsPath` |

动词端点在这一族是**合法**的——它本来就是 RPC，不是坏掉的 REST。gRPC 覆盖其中
4 条（register/heartbeat/poll/result）加一条 `Connect` 双向流；renew/ack/metrics
无 gRPC 对应物，其中 metrics **明确设计为永不做**（跨云走 Relay Gateway，gRPC 非
目标部署形态）。

### 8.2 承重的形状约定

entry-seed 的 409 响应有**两种不同 body**，客户端据此决定是否提交 Kafka offset
（`service/protocol/entry_seed_runtime.go`）：

| 情形 | body | 客户端行为 |
| --- | --- | --- |
| 真实 admission 冲突 | `{"state":"conflict","execution_id":...}` | 提交 offset（消息已被另一 runner 处理） |
| generation 栅栏拒绝 | `{"error":"stale_generation"}`（无 `state` 字段） | **不提交 offset**（新 owner 尚未处理，必须重投） |

判错方向会导致 **generation 升级期间静默丢消息**。

这个判别目前依赖「`state` 字段存不存在」这一隐式契约。若该端点将来信封化，判别
必须改为**看 `code` 值**（`"stale_generation"` vs 冲突码）——这比现状更可靠。在
迁移发生前，本约定不得改动。

### 8.3 纪律要求

- **零死常量**：删除 `ActivatePath`、`DeactivatePath`、`ActivationListPath`
- **零半接线**：`/v1/runners/lease/renew` 服务端 handler 齐全，但生产 runner 客户
  端从未接入——`protocol.Client` 没有 `RenewLease` 方法，`service/runner/
  group_renew.go` 的续约循环只被测试驱动，`runner.go` 主循环未接。**要么补齐调用
  链，要么连同 handler 一并删除**，不允许停在半成品状态
- 常量表与守卫测试要求同 §2

---

## 9. 待迁移清单

本节列出与本规范冲突的既有实现。迁移完成后从本节删除条目——**本节为空意味着实
现与规范完全一致**。

### 9.1 路径

| 现状 | 目标 | 依据 |
| --- | --- | --- |
| `POST /v1/workflows`（编译并立即执行） | `POST /v1/workflows/execute` | §1.2 语义倒置：POST 集合应当是创建 |
| `POST /v1/workflows/invoke` | 并入 `POST /v1/workflows/execute` | §1.2 动词层级 |
| `POST /v1/workflows/register` | `POST /v1/workflows` | §1.2 动词层级 |
| `DELETE /v1/workflows/register/{id}` | `DELETE /v1/workflows/{id}` | §1.2 中间段当资源 |
| `POST /v1/executions/{id}/signal` | `POST /v1/executions/{id}/signals` | §1.1 复数资源 |
| `POST /v1/executions/{id}/revoke-signal` | `DELETE /v1/executions/{id}/signals/{name}` | §1.2 动词粘在路径段里 |
| 全仓手工 `TrimPrefix` 路径解析 | Go 1.22 mux pattern | §1.3 |

### 9.2 缺失的端点

前端 `web/packages/xflow-api/src/index.ts` 已在调用、服务端**不存在**的路由：

- `GET /workflows`（列表）
- `GET /workflows/{id}`（读取）
- `PUT /workflows/{id}`（保存）
- `POST /workflows/{id}/runs` → 目标为 `POST /v1/workflows/{id}/execute`
- `GET /workflows/{id}/runtime` → **废除**，无服务端对应概念

### 9.3 响应形状

- 65 处 `writeError` 调用产出 `{"error": "..."}`，与 §3.1 信封不符
- `writeJSON`/`writeError` 在 `service/apiserver` 与 `service/control` 各有一份实
  现，必须合并为一份
- 前端 `web/packages/xflow-api/src/index.ts` 读 `body.message`，服务端发
  `body.error`——**当前前端拿到的每一条服务端错误消息都被丢弃**，一律降级为
  `statusText`。信封落地后自然修复
- 无任何端点实现分页（§3.3）

### 9.4 字段命名

全面 **snake_case**（与 Go wire 主流 112:10、YAML DSL 规范 `on_error` /
`allow_cycles` / `node_templates` 一致）。需修正的越界：

- `types/workflow.go`：`runnerSelector` → `runner_selector`
- `types/workflow.go`：`matchLabels` → `match_labels`

### 9.5 死代码

| 对象 | 状态 |
| --- | --- |
| `protocol.ActivatePath` / `DeactivatePath` / `ActivationListPath` | 零注册零调用 |
| `OpWorkflowDefinition{Create,Read,Update,Validate,Publish}`、`OpWorkflowExecutionInvoke`、`OpWorkflowRead` | 无路由消费（`OpWorkflowRead` 仅被一处测试当占位值） |
| `OpManagementWrite` | 零引用 |
| `api/openapi/xflow-v1.yaml` 的 `/workflow-definitions` 全套（8 条路径） | 零实现，且名称已被否决（过长） |
| `types/workflow_management.go`（61 行） | 零引用 |
| `/v1/runners/lease/renew` 的生产调用链 | 半接线，见 §8.3 |

---

## 10. 文档与实现的绑定

- **OpenAPI 只描述用户面。** runner 协议面用 Go 常量 + `service/protocol` 包文档
  描述，不进 OpenAPI——它不是给外部调用的契约
- **CI 校验：契约声明的路径 ⊆ 已注册路由。** 「契约里有、实现里没有」不允许存在
  ——`/workflow-definitions` 全套就是这个状态的产物：一份 CI 校验通过、还生成过 TS
  类型（`web/.../openapi-types.ts`，已随 `605c4bb` 删除）、却零实现的契约，与真实
  实现和前端客户端三方互不相认
- `docs/design/` 必须与实现一致（既有约束）
