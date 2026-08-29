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

### 0.1 「面」由响应契约决定，不由挂载模块决定

一条端点属于哪一族，判据是**它的响应形状对谁负责**，不是它注册在哪个 module 下，
也不是它用哪套认证。

现存唯一需要这条判据的端点是 `POST /v1/executions`（entry-seed）：它注册在
apiserver 的用户面模块里、走 principal/authz 认证，但它的响应类型是
`protocol.SeedExecutionResponse`、请求带 `ProtocolVersion` 字段、消费者是
`service/protocol/entry_seed_runtime.go` 里的 runner 客户端。

**判定：entry-seed 属于 runner 协议面，不包信封。** 三条理由：

1. 它的响应类型定义在 `service/protocol` 而非 apiserver，与其余 runner RPC 同源
2. 它自带 `ProtocolVersion`，用的是 runner 面的版本机制（§0 表格）而非 URL 版本
3. 它的 409 判别是 Kafka offset 安全的承重契约（§8.2）；信封化会同时改变两种
   409 body 的形状，而这个判别写错的后果是**滚动升级期间静默丢消息**

认证与「面」正交：entry-seed 用 principal/authz 是因为它需要 namespace 防伪造
（§6.3），这不影响它的响应契约归属。

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

这条规则针对已发生的事故：`service/protocol/activation.go` 曾声明
`ActivatePath = "/v1/runners/activate"`、`DeactivatePath`、`ActivationListPath =
"/v1/activations"` 三个常量，三者**零注册、零 handler、零调用**，现已删除（`activation.go:15`
留有墓碑注释）。真实的激活机制
是 heartbeat 响应携带 `Activations` 指令，runner 收到后本地调用
`TriggerActivationHandler.Activate/Deactivate`，根本不经过 HTTP。守卫测试
（`TestDeadConstantsGuard`）防止同类死常量重现。

同类问题也曾存在于 Op 常量（§6.1），已随 §9.5 一并关闭。

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
| `data` | any | 成功时的载荷；失败时**省略该键**（不是 `data: null`，见 `envelope.Data` 的 `omitempty`） |
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

这一条落地时**不得改变现有客户端的判据**。`service/runner/supply_client.go` 与
`store/objectstore/httpstore.go` 目前只看状态码（200 / 404 / 其他），**刻意不解析
错误 body**——注释写明了理由：响应体可能携带不属于本层的服务端细节。给失败分支加
信封不影响它们，但反过来说，这两个客户端也不会因为信封落地而变得更能诊断。若要让
它们读 `code`，那是独立的改动，须单独评估「读服务端错误文本」与 §3.5 的关系。

按 §0.1 的判据，这两条端点的消费者同样是 runner 客户端。它们之所以留在用户面而非
划归 runner 协议面，是因为**未来 xflow-admin 也要读它们**（查看某个 supply 的当前
内容、下载某个产物），而 entry-seed 永远只有 runner 会调。裸流 + 失败信封这个组合
同时满足两类调用方：runner 看状态码，页面读 `message`。

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
`OpWorkflowDefinitionValidate`、`OpWorkflowDefinitionPublish`、
`OpWorkflowExecutionInvoke` 这 5 个 Op 常量除 `authz.go` 自身外**没有任何路由
消费**——它们是为一份从未实现的契约准备的（§9）。`OpManagementWrite` 同样零
引用。（`OpWorkflowDefinitionUpdate` 被 PUT `/v1/workflows/{id}` 消费，
`OpWorkflowRead` 被 GET `/v1/workflows/{id}` 消费——这两个是活的，不要删。
`OpWorkflowRead` 在 `sqlaudit_test.go` 里还被当作任意占位值使用，那不是消费点。）

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

**跨界端点必须显式记录选了哪套及理由。** 现存三条跨界端点——runner 用同一个
`--token` 走 principal/authz 打它们：

| 端点 | 调用方 | 选 principal/authz 的理由 |
| --- | --- | --- |
| `POST /v1/executions`（entry-seed） | runner 的 Kafka trigger | 需要 namespace 从认证主体注入这道防伪造机制，runner directory 认证不提供 |
| `GET /v1/supplies/{name}` | runner 的 supply fetcher | 内容按 namespace 隔离，且加密分支依赖 principal 解析出的租户 |
| `GET`/`HEAD` `/v1/artifacts/{digest}` | runner 的 objectstore HTTP origin | 取件前须校验 `HasReference(callerNamespace, digest)`，同样依赖 principal |

三者共同的理由是一个：**它们都需要「调用方属于哪个 namespace」这个事实**，而
runner directory 认证只回答「这是不是一台已注册的 runner」。这不是历史包袱，是正
确的划分——因此本规范予以保留。

推论：runner 进程同时持有两种身份（runner session + principal token），这是设计
使然，不是配置错误。任何简化认证的提案必须先解释这三条端点的 namespace 从哪来。

---

## 7. 用户面路由表（目标形态）

```
POST   /v1/workflows                        注册定义 → workflow_id
GET    /v1/workflows                        列表（分页）——未实现，见 §9.6
GET    /v1/workflows/{id}                   读取
PUT    /v1/workflows/{id}                   全量更新
DELETE /v1/workflows/{id}                   注销
POST   /v1/workflows/{id}/execute           执行已注册的工作流
POST   /v1/workflows/execute                内联定义直跑

GET    /v1/executions                       列表（分页）——未实现，见 §9.6
GET    /v1/executions/{id}                  查询
POST   /v1/executions/{id}/cancel           取消
POST   /v1/executions/{id}/signals          发信号
DELETE /v1/executions/{id}/signals/{name}   撤销信号
GET    /v1/executions/{id}/wait             等待完成

POST   /v1/executions                       entry-seed（runner 调用）

GET    /v1/supplies/{name}                  取 supply 内容（裸流）
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

这个判别目前依赖「`state` 字段存不存在」这一隐式契约。**本规范明确该端点不信封化**
（理由见 §0.1），因此这个判别保持原样。任何要给它加信封的提案，必须同时把判别改为
看 `code` 值，并在真实的 generation 升级场景下验证——不能只跑单测。

### 8.3 纪律要求

- **零死常量**：`ActivatePath`、`DeactivatePath`、`ActivationListPath` 已删除
  （`service/protocol/activation.go:15` 留有墓碑注释；守卫测试已覆盖）
- **零半接线**：`/v1/runners/lease/renew` 生产调用链已接线——`protocol.Client.RenewLease`
  （`service/protocol/client.go:126`）、`leaseRenewClient` 接口与 `protocolLeaseRenewer`
  适配器（`service/runner/lease_renew.go`），`runner.go:415` 运行时类型断言后在
  `runner.go:419` 启动 `renewLeaseLoop` goroutine
- 常量表与守卫测试要求同 §2

---

## 9. 待迁移清单

本节列出与本规范冲突的既有实现。迁移完成后从本节删除条目——**本节为空意味着实
现与规范完全一致**。

### 9.1 路径

| 现状 | 目标 | 依据 |
| --- | --- | --- |
| 全仓手工 `TrimPrefix` 路径解析 | Go 1.22 mux pattern | §1.3（workflows 面与 executions 面已迁移；management dead-letters 面待 Task 5 迁移） |

`POST /v1/executions/{id}/signal` → `POST /v1/executions/{id}/signals`（§1.1 复数资源）
与 `POST /v1/executions/{id}/revoke-signal` → `DELETE /v1/executions/{id}/signals/{name}`（§1.2
动词粘在路径段里）已完成迁移，从本表移除。

### 9.2 缺失的端点

前端 `web/packages/xflow-api/src/index.ts` 已在调用、服务端**不存在**的路由：

- `GET /workflows`（列表）
- `GET /workflows/{id}/runtime` → **废除**，无服务端对应概念

（`GET /workflows/{id}`、`PUT /workflows/{id}`、`POST /workflows/{id}/runs`→`POST /v1/workflows/{id}/execute`
已在 workflows 路由迁移中实现。）

### 9.3 响应形状

- workflows 族（register/deregister/execute/read/replace）已迁移到信封
  `writeData`/`writeFail` 并带稳定 snake_case code（§3.2）。executions 族
  （inspect/signal/cancel/revoke/wait）亦已迁移完成。management / supply /
  artifact 三族（Task 5b）已迁移：所有失败站点改走 `writeFail`（带稳定
  snake_case code + `trace_id` + `X-Request-Id` 回显），management 的成功
  body 一并信封化（leader / runner / dead-letters list / dead-letters
  replay）；CLI `apiDeadLetterClient.do()`
  解信封再取 `data` 以保持对齐。`PUT /v1/supplies/{name}` 其后已整体下线
  （spec appendix Z.5）：supply 写路径只剩进程内调用
  `sdk/xflow.Server.UpdateSupply`/`UpdateSupplyIfMatch`，不再有 HTTP 写动词、
  也就不再有对应的成功信封。§3.4 的裸流例外（supply GET、artifact
  GET/HEAD）只对**成功流**有效，失败分支仍返回 JSON 信封。`/healthz` 与
  `/readyz` 不信封化（§7）。`service/apiserver` 中的 `writeError`/
  `writeEngineError` 过渡 shim 已删除，apiserver 现仅留 `writeJSON`
  （`module_control.go`，用于 entry-seed 的 §0 例外裸 body 与
  `/healthz`、`/readyz`）；用户面成功/失败均走 `writeData`/`writeFail`。
  `service/control/server.go` 仍各自实现 `writeJSON` 与 `writeError`
  （runner 面，§0 第二列），是另一份 `writeJSON` 的所在；两份 `writeJSON`
  （apiserver 与 control）签名一致但服务于不同面，合并为一份共享 helper
  仍是后续待办
- 前端 `web/packages/xflow-api/src/index.ts` 读 `body.message`，服务端发
  `body.error`——**当前前端拿到的每一条服务端错误消息都被丢弃**，一律降级为
  `statusText`。信封落地后自然修复（workflows 族已修复）
- 无任何列表端点实现分页（§3.3）。**参数层已落地**：`pageParams`
  （`service/apiserver/pagination.go`，1-based、默认 20、服务端强制上限 200）
  与 `writeList`（`service/apiserver/envelope.go`，`{list,total}` 载荷形状）
  已实现并有单测，**但生产调用点为零**——阻塞在两处缺失的数据源，见 §9.6

### 9.4 字段命名

全面 **snake_case**（与 Go wire 主流 112:10、YAML DSL 规范 `on_error` /
`allow_cycles` / `node_templates` 一致）。

`types.RunnerSelector` 的 `runnerSelector` → `runner_selector`、`matchLabels` →
`match_labels` 已完成（含 TS mirror `web/packages/xflow-core/src/index.ts`）。
runtime hash 已通过 hash-local 镜像（`runtimeSelectorHashPayload`）与 wire 标签解耦，
标签冻结在前重命名字节，详见 `sdk/xflow/workflow_identity.go`。

### 9.5 死代码

以下三项已全部关闭：

| 对象 | 结论 |
| --- | --- |
| `protocol.ActivatePath` / `DeactivatePath` / `ActivationListPath` | 已删除（`activation.go:15` 留墓碑注释，守卫测试覆盖） |
| `OpWorkflowDefinition{Create,Read,Validate,Publish}`、`OpWorkflowExecutionInvoke`、`OpManagementWrite` | 已删除，零残留（`authz.go` 只保留 `OpWorkflowDefinitionUpdate`，由 PUT `/v1/workflows/{id}` 消费） |
| `/v1/runners/lease/renew` 的生产调用链 | 已接线（`runner.go:415-420`，见 §8.3） |

### 9.6 列表端点未接线（分页参数层已落地，数据源缺失）

`GET /v1/workflows` 与 `GET /v1/executions` 在 §7 路由表中标注为「未实现」。
**不是分页没做，是列举能力本身不存在**。分页参数层已就位（见 §9.3 末段），
但两个端点的数据源都不具备列举条件，注册一个返回空列表的 handler 只会复刻
`/workflow-definitions` 的老毛病——契约描述一个不存在的端点。两条阻塞如下：

**1. `GET /v1/workflows`：`workflowreg` 无 per-namespace 索引。**

`backend.WorkflowRegistry` 接口只有 `AddWorkflow`/`GetWorkflow`/
`GetWorkflowByKey`/`UpdateDefinitionHash`/`RemoveWorkflow`（`backend/
workflow_registry.go:26-41`）。唯一实现 `workflowreg.Registry` 是纯 Redis KV，
**零索引**（无 `SAdd`/`ZAdd`/`SCAN`），无法按 namespace 枚举。补这条端点需要：

- 在 `workflowreg` 加一个 per-namespace 索引（`xflow:ns:<ns>:workflow:index`
  之类的 ZSET），并处理与 `AddWorkflow`/`RemoveWorkflow` 的原子性
- registry 现在的 Lua 脚本按 `{<key>}` 打 hash tag，**索引键不在同一 slot**，
  Redis Cluster 下无法与记录同事务写，需要单独设计补偿（两阶段 + 校验，或
  hash tag 扩展到索引键）

不先做索引直接 `SCAN xflow:workflow:*` 是全表遍历，违反 org policy §2
「敏感数据枚举端点不得全表遍历」，也跨租户泄漏键名。

**2. `GET /v1/executions`：`store.ExecutionRecord` 无 namespace 字段。**

`store.Executions` 接口只有 `CreateExecution`/`UpdateExecutionStatus`/
`GetExecution`（`store/interfaces.go:11-15`）。全仓 `ListExecutions`/
`CountExecutions` 零命中。更根本地，`store.ExecutionRecord` 与其 DB 投影
`dbExecution` **没有 namespace 列**（只有 `dbSupply`/`dbArtifact` 带 namespace）。
即便加了 `ListExecutions`，也**无法按 namespace 过滤**——那是一个跨租户
列举端点，违反 org policy §1a 与 §2。补这条端点需要一次 schema 迁移：
给 `xflow_executions` 加 namespace 列、回填历史行、再加索引与查询。

两条都不在本次 rollout 范围内。完成本节列出的两件事后，从本节删除对应条目并
解除 §7 路由表的「未实现」标注。

---

## 10. 文档与实现的绑定

- **OpenAPI 只描述用户面。** runner 协议面用 Go 常量 + `service/protocol` 包文档
  描述，不进 OpenAPI——它不是给外部调用的契约
- **CI 校验：契约声明的路径 ⊆ 已注册路由。** 「契约里有、实现里没有」不允许存在
  ——`/workflow-definitions` 全套就是这个状态的产物：一份 CI 校验通过、还生成过 TS
  类型（`web/.../openapi-types.ts`，已随 `605c4bb` 删除）、却零实现的契约，与真实
  实现和前端客户端三方互不相认。该校验已落地为
  `api/openapi/openapi_test.go` 的 `TestContractPathsAreAllRegistered`：契约每条
  path 必须出现在 `service/apiserver.UserFacingPaths` 集合中（前半，子集关系）；
  「`UserFacingPaths` 每条都有 mux 注册」是后半守卫，合起来才是 §10 的完整链条
- `docs/design/` 必须与实现一致（既有约束）
