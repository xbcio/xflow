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

第二个需要这条判据的端点是 server 托管的 webhook 入口
（`POST|{method} /v1/hooks/{namespace}/{workflow_id}/{entry_unit}`，§6.3）：它由
apiserver 托管、注册在用户面模块里，认证却是入口自己的 HMAC 签名（外部发送方没有
平台 principal 可出示），而响应类型仍是 `protocol.SeedExecutionResponse`。按同一条
判据，它同样属于 runner 协议面、不包信封——「用哪套认证」从来不是判据，这个端点是把
这句话逼到极限的例子。

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
| 429 | 限流：API 准入层的 `rate_limited`，或工作流 `settings.concurrency` 的 `workflow_concurrency_*`（§4.3） |
| 503 | 后端依赖不可用；或本副本过载 `server_overloaded`（§4.3） |

### 4.3 API 准入（限速与背压）

变更类路由在认证之后、admission 审计之前过两道闸。被拒的请求**不写审计行、
不触达 store、不执行**，两种拒绝都带整数秒 `Retry-After`（≥1）：

| 闸 | 状态码 / code | 维度 | 默认 | `Retry-After` |
| --- | --- | --- | --- | --- |
| inflight 上限 | 503 `server_overloaded` | 每副本，全部覆盖路由共用一个上限；webhook 类另有 1/4 子上限（至少 1） | 1024（`--api-max-inflight`；负数关闭） | 1，下限而非承诺 |
| 令牌桶限速 | 429 `rate_limited` | 每 namespace（认证主体的）；webhook 为 namespace+workflow | 关闭（`--api-rate` 为 0） | 桶补出一个令牌的时间，向上取整 |

先查 inflight 再查速率：过载副本不必多一次 Redis 往返就能卸载。非阻塞——满了
立即拒绝，不排队。

覆盖范围：execute、execute-by-id、signals、revoke-signal、cancel、pause、resume、
rerun、schedule backfill（backfill 会发起执行）、dead-letter replay（replay 会重投），
以及 webhook 入口。**不覆盖**：`POST /v1/executions`（entry-seed，trigger 已有
自己的 429 与重投，再限会叠加退避）、`GET .../wait`（长轮询会占满 inflight）、读接口、
注册与 schedule 管理。覆盖集合不是手写清单：`admission_routes_test.go` 枚举生产 mux
上每个 POST/PUT/PATCH/DELETE 可达的路由，要求逐条要么归入覆盖（并驱动到拒绝），
要么豁免并给出理由；新路由未归类会让该守卫直接失败。webhook 只对**签名正确**的请求
计费，否则知道路径的人就能花掉
合法发送方的额度；未签名洪峰在签名校验**之前**就占 inflight 槽，所以 webhook 类另有
1/4 上限（派生，至少 1）：unsigned 洪峰被子上限削掉，认证过的 execute 仍拿得到容量。
webhook 的拒绝沿用其裸 `{"error": code}` 形状。

`rate_limited` 与 `workflow_concurrency_*` 同为 429 但互不相干：前者保护控制面，
在请求被准入之前判定，与工作流无关；后者是工作流自己声明的业务容量，在准入之后
判定。客户端按 `code` 区分，二者都可重试。

计数位置（`--api-rate-limit-mode`）：

- `local`（默认）：每副本内存令牌桶，配置值是**单副本**额度，全舰队放行
  rate×副本数。不增加任何网络往返。
- `redis`：每个键一个 Redis 令牌桶（Lua 原子脚本，时钟取 Redis `TIME`），全部
  副本共享一份额度，代价是每个被限速的请求多一次 Redis 往返（250ms 超时）。
  Redis 不可达时**放行到本地桶**（fail open：限流器故障不得拖垮 API），
  `xflow_api_rate_limit_fallback_total` 按 reason 计数，告警日志每 30 秒至多一条。
  连 Redis 都黑掉（连得上却不回话）时失败会等满超时：连续 3 次失败后**熔断 5 秒**，
  期间直接本地判定、不再调用 Redis，也不让每个请求占着 inflight 槽等满 250ms；
  冷却结束后的第一个请求探测 Redis，再失败重新计数。没有
  Redis 后端的部署选 `redis` 会在启动时失败。

指标：`xflow_api_rate_limited_total{route,reason}`（reason=`rate`|`inflight`，
route 为 `execute`|`execution`|`webhook`）、`xflow_api_inflight{route_class}`、
`xflow_api_limiter_buckets`（本地桶数，观察空闲清扫）、
`xflow_api_rate_limit_fallback_total{reason}`（reason=`redis_error`|`breaker_trip`|
`breaker_open`，仅 redis 模式）。

### 4.4 幂等提交（`Idempotency-Key`）

网络重试不得变成第二次执行。三条发起执行的变更路由接受 `Idempotency-Key`
请求头（IETF draft 命名）：`POST /v1/workflows/execute`（内联）、
`POST /v1/workflows/{id}/execute`（已注册）、`POST /v1/executions/{id}/rerun`。
键形状为 1–255 个 `[A-Za-z0-9._:-]`；越界**拒绝而非忽略**：
400 `idempotency_key_invalid`。静默忽略一个键，等于对调用方的重试安全承诺
不作数。

| 情形 | 应答 |
| --- | --- |
| 首次请求 | 现有 2xx 不变（`duplicate` 缺省） |
| 同键 + 同请求 | 200 `{execution_id, status, duplicate: true}`，**与 webhook duplicate 同形状**：给出首次创建的执行 id 与其**当前**状态（可能已 `queued`/`paused`/终态），不新建执行 |
| 同键 + 不同请求 | 409 `idempotency_key_conflict`（调用方 bug，重试不解决） |
| 同键 + 同请求并发 | 409 `idempotency_key_in_progress` + `Retry-After: 1`：首个请求仍在创建，等一拍重试即可 |
| 后端不支持键 | 503 `idempotency_unsupported`（fail closed：静默忽略会让调用方误以为重试安全） |

「同请求」由服务端指纹判定：op、定义版本与 digest、entry、params、test 标志，
rerun 另含 parent 与 `from_node`/`include_success`；params 的 map 键顺序无关，
trace_id 等运行时值不参与。键的作用域随路由：已注册工作流
（`workflow:<id>`）、内联定义（`definition:<ns>/<name>`）、rerun
（`rerun:<parent>`）——同一把键在不同工作流下互不相干。

键的 claim 窗口默认 **24h**（`engine.DefaultIdempotencyTTL`，SDK 可调；且被钳制
为**不长于执行保留期**——claim 比执行活得久会指向已删执行）。窗口过后键并不
立即自由：只要它创建的那个执行还在保留期内，同键请求仍答 duplicate（后端以
执行记录兜底——在原处重建等于覆写它），claim 与执行都消失后同键才开启新执行。
键的认领**先于**
持久化：claim 带 30s 租约，进程在创建中途崩溃后，同键重试会接管那个 claim
而不是永久卡在 in-progress；提交在什么都没持久化前被拒（如工作流并发队列
拒绝）则释放 claim，其重试不会被误判为 duplicate。§4.3 的准入拒绝发生在键
被认领之前，429/503 后的重试同样安全。`FireAndForget` 不产生执行，携带键时
直接拒绝而不是忽略。

契约测试：`backend/internal/statestoretest` 的幂等契约对 local 与 rstate 两个
后端各跑一遍（含过期 claim 释放、崩溃接管、并发只建一个）；路由面在
`service/apiserver/idempotency_test.go`，openapi 描述见
`api/openapi/xflow-v1.yaml` 的 `IdempotencyKey` 参数。

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

例外只有一条，且必须是显式的：server 托管的 webhook 入口
（`/v1/hooks/{namespace}/{workflow_id}/{entry_unit}`）**不在 authz 包装器之内**，
因此没有 Op，也不进 `scopeForOperation`——它的认证是入口自己的 HMAC 签名（§6.3）。
`TestEveryProtectedUserPathRejectsUnauthenticated` 对这唯一一条路径显式豁免，并配了
反向测试 `TestWebhookEntryRejectsBadSignature`：缺失、格式错、签名不匹配一律 401。
豁免与「漏登记」在语句上无法区分，能区分它们的是那条反向测试；删掉它，这个豁免就
退化成 §6.1 要防的那个洞。

反向的死代码同样要防：`OpWorkflowDefinitionCreate`、`OpWorkflowDefinitionRead`、
`OpWorkflowDefinitionValidate`、`OpWorkflowDefinitionPublish`、
`OpWorkflowExecutionInvoke` 这 5 个 Op 常量除 `authz.go` 自身外**没有任何路由
消费**——它们是为一份从未实现的契约准备的（§9）。`OpManagementWrite` 同样零
引用。（`OpWorkflowDefinitionUpdate` 被 PUT `/v1/workflows/{id}` 消费，
`OpWorkflowRead` 被 GET `/v1/workflows/{id}` 消费——这两个是活的，不要删。
`OpWorkflowRead` 在 `sqlaudit_test.go` 里还被当作任意占位值使用，那不是消费点。）

**新增运维操作使用独立 scope，需要显式补授权**：`execution.control`（pause / resume）
与 `execution.rerun`（rerun）都不在既有的 `execution` scope 内。对既有调用方而言，这意味着
**只持有 `execution` 的 token 调用 pause 会得到 403**（rerun 同理），必须显式补授权——这是
有意为之：暂停一条正在运行的流水线、从终态执行派生新执行，都比「读 / 信号 / 取消」更重，
不应随 `execution` 一起被默认授予。

### 6.2 namespace 只能从认证主体取

namespace **只能**由 authz 包装器从认证主体注入 request context，handler 通过
`namespace.FromContext` 读取，**绝不从请求体取**。

entry-seed 端点（`handleSeedExecution`）目前做对了这一点，本规范将其固化：请求体
携带的 `admission_key` 是幂等键，其命名空间归属由服务端裁定，防止伪造跨命名空间
的 admission key。

webhook 入口是唯一的例外，且是有意的：路径里的 namespace 是**声明**而不是来源，
服务端把它与记录比对、不一致按 404 处理，准入跑在记录的 namespace 下（§6.3）。
新增端点不得以它为范例——这条例外成立的前提是「外部发送方没有平台身份可出示」，
没有这个前提的端点必须走 §6.2 的主规则。

### 6.3 认证体系不混（三套，各有适用面）

| 面 | 认证机制 |
| --- | --- |
| 用户面 / 管理面 | apiserver 的 principal/authz 体系，Bearer token，Op + scope |
| runner 协议面 | `Core.authn()` 的 runner directory 认证，`AuthToken` 字段 + SessionID |
| server 托管的 webhook 入口 | 入口自己的 HMAC 签名（`X-XFlow-Signature: sha256=<hex>`，常量时间比较），密钥取自 `secret_supply` 指向的 supply 资源；不在 authz 包装器内 |

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

**第三套认证：server 托管的 webhook 入口（§7）。** 它既不是 principal/authz 也不是
runner directory——发送方是外部系统（代码托管、支付回调），没有平台身份可出示。
它的认证是**入口自己的 HMAC 签名**：密钥由工作流定义里的 `secret_supply` 指定、服务端
从 supply 读取，签名覆盖原始 body 并用常量时间比较；请求既不能携带密钥，也不能选择
用哪个密钥，未配密钥的定义在注册期就被拒。因为没有认证主体，它拿不到上面那条
「namespace 从认证主体注入」的防伪造机制：路径里的 namespace 只是**声明**，服务端把
它与记录里的 namespace 比对，不一致按 404 处理，而准入始终跑在**记录的** namespace 下。

审计上的差别要写清楚：entry-seed 的准入不写每请求账本，那是被显式豁免的吞吐决定
（见 `auditedPerRequest`），但它仍由包装器认证授权，**拒绝照旧入账**；webhook 入口在
包装器之外，认证由 handler 自己完成，因此**签名被拒这条路径没有任何审计行**，只有一条
不含密钥、不含签名的错误日志。这是「用签名换掉平台身份」必须付的代价，部署方若要
审计这个入口，目前只能靠 apiserver 的日志。

---

## 7. 用户面路由表（目标形态）

```
POST   /v1/workflows                        注册定义 → workflow_id
GET    /v1/workflows                        列表（分页）——已实现，见 §9.6
GET    /v1/workflows/{id}                   读取
PUT    /v1/workflows/{id}                   全量更新
DELETE /v1/workflows/{id}                   注销
POST   /v1/workflows/{id}/execute           执行已注册的工作流
POST   /v1/workflows/execute                内联定义直跑

GET    /v1/executions                       列表（分页）——已实现，见 §9.6
GET    /v1/executions/{id}                  查询
POST   /v1/executions/{id}/cancel           取消
POST   /v1/executions/{id}/signals          发信号
DELETE /v1/executions/{id}/signals/{name}   撤销信号
GET    /v1/executions/{id}/wait             等待完成

POST   /v1/executions                       entry-seed（runner 调用）

POST|{method} /v1/hooks/{namespace}/{workflow_id}/{entry_unit}
                                            server 托管 webhook 入口——已实现；
                                            方法取自入口节点自己的 method 参数；
                                            响应形状与 entry-seed 同面（§0.1），不包信封

GET    /v1/node-types                       已注册节点类型的 NodeFormSchema 列表
GET    /v1/node-types/{type}                单个节点类型（?version= 选版本，缺省取最新）
GET    /v1/current-user                     调用方自身的已验证身份（仅 principal 认证路径注册）

GET    /v1/supplies/{name}                  取 supply 内容（裸流）
GET    /v1/artifacts/{digest}               取产物字节（裸流，另支持 HEAD）

GET    /v1/management/leader                leader 状态
GET    /v1/management/runners               runner 列表
GET    /v1/management/runners/{id}          单 runner 查询
POST   /v1/management/runners/{id}/drain    排空（不远程停进程，不强取消已租任务）
POST   /v1/management/runners/{id}/resume   恢复接单
POST   /v1/management/runners/{id}/revoke-identity  吊销该 runner 已签发的身份
POST   /v1/management/runner-pools          创建 runner 池
GET    /v1/management/runner-pools          池列表
GET    /v1/management/runner-pools/{id}     读取池
PATCH  /v1/management/runner-pools/{id}     更新池
DELETE /v1/management/runner-pools/{id}     删除池
POST   /v1/management/runner-pools/{id}/tokens                    签发注册 token
GET    /v1/management/runner-pools/{id}/tokens                    token 列表
DELETE /v1/management/runner-pools/{id}/tokens/{token_id}         吊销 token
GET    /v1/management/runner-pools/{id}/tokens/{token_id}/audit   token 审计
GET    /v1/management/runner-pools/{id}/runners                   池内 runner 实例
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

同一个端点还有第三种承重形状：**429 拒绝**。工作流自己的并发策略（`settings.concurrency`）
拒绝了这次 seed，服务端什么都没持久化，entry-unit 的结果仍然属于 runner：

| 情形 | body | 客户端行为 |
| --- | --- | --- |
| REJECT 模式已达 limit | `{"state":"over_limit","error":"workflow_concurrency_limit"}` | **不提交 offset**（本次未持久化，结果仍属调用方，退避后重试） |
| QUEUE 模式积压达 `max_queued` | `{"state":"queue_full","error":"workflow_concurrency_queue_full"}` | **不提交 offset**（同上；等的是队列排空而非槽位） |

此外还有 503（**不可重试**，与上表两种 429 相反）：seed 的工作流声明了并发上限、
而本部署的状态存储无法执行它——`{"error":"workflow_concurrency_unavailable"}`，
裸 errorResponse，与用户面 execute 的同码 503 同义。引擎在这里 fail-closed（放行
等于废掉作者用来保护下游的上限），因此没有 `Retry-After`：答案随部署改变，不随
runner 重试改变。**调用方必须重试这一条记录直到它被接受**：一次 429 拒绝不提交
offset，若直接跳过、让后面的记录提交，就等于静默丢掉它（见 §8.2 下方对逐条路径的
说明）。

两种状态分开是必要的：客户端动作相同（不提交、退避、重试），但要等的东西不同
（等槽位 vs 等队列）。`Retry-After` 取值 1 秒，与用户面 execute 的同两种拒绝一致
——它是**下限**而非释放时间的承诺（槽位何时空出不可知）。仓库内自带的 runner 只读
状态码、不读该头，两条路径都自己重投，且共用同一套节奏（`aggregateRetryDelay`：
前八次按 flush 间隔，entry-seed 形态下为 1 秒；之后翻倍，上限 30 秒）。差别只在
**手里攥着什么**：批次路径把整批留在手里重推整批；逐条路径（未开 `aggregate` 的
默认形态）的 worker 在同一个分区上原地重试**同一条记录**，重试携带同一个 admission
key——key 由记录自身身份（topic/partition/offset）构造，所以第二次尝试是重试，
不是一次新准入。阻塞这条路径是**要害**而非副作用：worker 每分区串行，等待期间该
分区不会准入后面的记录，也就没有任何后续 commit 能越过被拒的那条——这正是「不提交」
等于「会被重投」而不是「被跳过」的原因。反过来，两条路径都只重投**拒绝**：传输错误
与未知状态立即让位给 Kafka 重投，因为其中有些（generation 栅栏、协议分歧）重试永远
不会成功，无界重试比一次重投更糟。拒绝本身必须**被计数和记录**
（`xflow_trigger_batch_admissions_total{state="refused"}`，其 `reason` 区分
`concurrency_limit` 与 `queue_full`；分区停摆由 `xflow_trigger_consumption_blocked`
报出），否则一次限流读起来和「这个 topic 没流量」完全一样。客户端必须把这个 429
判成**决定**而不是故障——判成故障会把它记进错误率，掩盖「配置正确、正在限流」这一
事实；判成成功则会为一条从不存在的 execution 提交 offset，等于静默丢消息。

这个判别目前依赖「`state` 字段存不存在」这一隐式契约。**本规范明确该端点不信封化**
（理由见 §0.1），因此 409 的那个判别保持原样。任何要给它加信封的提案，必须同时把
判别改为看 `code` 值，并在真实的 generation 升级场景下验证——不能只跑单测。

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

- `GET /workflows/{id}/runtime` → **废除**，无服务端对应概念

（`GET /workflows`（列表，见 §9.6）、`GET /workflows/{id}`、`PUT /workflows/{id}`、
`POST /workflows/{id}/runs`→`POST /v1/workflows/{id}/execute` 已在 workflows 路由迁移中实现。）

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
- 分页参数层已落地：`pageParams`（`service/apiserver/pagination.go`，1-based、
  默认 20、服务端强制上限 200）与 `writeList`（`service/apiserver/envelope.go`，
  `{list,total}` 载荷形状）已实现并有单测；**两个列表端点已成为其生产调用点**
  （`GET /v1/workflows`、`GET /v1/executions`，见 §9.6）

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

### 9.6 列表端点（已接线）

`GET /v1/workflows` 与 `GET /v1/executions` **均已实现**，§7 路由表已同步。
本节原先记录的两条阻塞（`workflowreg` 无 per-namespace 索引、`store.ExecutionRecord`
无 namespace 字段）**都已消除**，下面是实现方式与必须知道的语义后果。

**1. `GET /v1/workflows` —— `workflowreg` 增加了 per-namespace 索引。**

`backend.WorkflowRegistry` 增加 `ListWorkflows`（`backend/workflow_registry.go:277`），
实现在 `backend/providers/distributed/internal/workflowreg/registry.go:640`。
索引就是本节原先建议的那把键 `xflow:wfreg:v2:{ns:<sha256(namespace)>}:index`
（`registry.go:71`、`:145`），写入是既有 Lua mutation 里的 `ZADD`
（`registry.go:223/274/313/426/601`），因此**不是**非原子的旁路写入；
读取走 `ZRange`（`registry.go:680`，**不** `SCAN`，故不违反 org policy §2 的「不得全表遍历」）。
`ns` 是必需 scope 且只作为键来源，**context 里的 namespace 故意不被采纳**，
使调用者无法拿到与它声明的不同的 scope；跨 namespace 枚举是**结构性不可能**，
而不只是「未实现」。

**该索引对早于它存在的数据是自愈的**：当某页从 offset 0 开始且解析不到任何 live id 时，
索引会与该 namespace 的 live byid 记录对账并重试一次
（`registry.go:657-661` 调用 `reconcileWorkflowIndexLua`，脚本定义在 `registry.go:573`）。
**因此索引建立之前注册的 workflow 仍然可枚举**，且热路径不扫描。

**2. `GET /v1/executions` —— `store.ExecutionRecord` 增加了 namespace 列。**

`store.Executions` 增加 `ListExecutions`/`CountExecutions`（`store/interfaces.go`；
实现 `store/sqlstore/execution.go:85`）。`xflow_executions` 经 `db/xflow_schema.sql`
加上 namespace 列与列举索引（`db/xflow_schema.sql:906-948`：`INFORMATION_SCHEMA`
守卫式补列，因 MySQL 无 `ADD COLUMN IF NOT EXISTS`；纯 `ADD COLUMN` + 常量缺省，
可走 INSTANT DDL，历史行不重写）。排序恒定 `created_at DESC, id DESC`
（`store/execution.go:136`）——`id` tiebreak 是分页正确性的承担者，故 offset 分页
**不会重复或跳过**。scope 为空或非法时 `store.ErrInvalidNamespace` **fail closed**
（`store/execution.go:37-45`），绝不回退成「全部 namespace」——那正是无 scope 的
`GET /v1/executions` 会成为的跨租户枚举。

**⚠ 必须写进发布说明的语义后果：历史 execution 不做回填。**

本节原处方要求「回填历史行」，**实现刻意没有回填**：历史行保持 `''`（未归属），
而 store 层对空 scope fail closed，因此**未归属行在任何 scope、任何过滤组合下都不可达**
（设计取向：宁可少列，绝不借错列把 A 租户的行塞给 B 租户）。其后果是：

> **本改动之前产生的 execution 不会出现在 `GET /v1/executions` 中，直到有人把它们
> 人为归属；本次改动不做任何归属。** 未归属行仍可按精确 id 经 `GET /v1/executions/{id}`
> 读取。真正的回填/归属是一次**离线 operator 决策**，不是本次交付的一部分。

**注意两个端点在此处并不对称**：workflow 索引会自愈、历史记录可枚举；
execution 不回填、历史记录不可枚举。不要假定两者行为一致。

**过滤能力**：executions 广告 `status`、`workflow_id`、`created_after`、
`created_before`。`workflow_id` 是服务端签发、提交时盖戳的工作流身份
（`xflow_executions.workflow_id`，`VARCHAR(64)`），按**精确相等**匹配而非名称或
前缀，并同时收窄列表与 `total`；参数缺省或为空表示不过滤，超过 64 字符的值返回
400 `execution_workflow_id_invalid` 而不是被忽略。**历史行不回填**：该列加入之前
产生的行、以及提交时没有 definition id 的行保持 `''`（未归属），它们不匹配任何
非空的 `workflow_id` 过滤——这是与 namespace 相同的 fail-closed 取向，绝不猜测
归属。按 workflow **名称**过滤不暴露（名称是显示串、不是身份：跨 namespace 可重名，
同一 id 重新注册时可变）；按 **runner** 过滤仍**不被数据模型支持**（`runner_id`
不是该表列，可靠的 per-execution 投影尚未建立）。
分页按 §3.3 的 **offset** 语义（非 cursor），默认 20、硬上限 200 作为有文档的安全控制。

契约侧：`api/openapi/xflow-v1.yaml` 已描述两条 `get`（`listWorkflows`、
`listExecutions`），并含 `WorkflowListResponse` / `ExecutionListResponse` 与各自的 item schema。

---

## 10. 文档与实现的绑定

- **OpenAPI 只描述用户面。** runner 协议面用 Go 常量 + `service/protocol` 包文档
  描述，不进 OpenAPI——它不是给外部调用的契约
- **CI 校验：契约声明的路径 ⊆ 已注册路由。** 「契约里有、实现里没有」不允许存在
  ——`/workflow-definitions` 全套就是这个状态的产物：一份 CI 校验通过、还生成过 TS
  类型（`web/.../openapi-types.ts`，已随 `fed9980` 删除）、却零实现的契约，与真实
  实现和前端客户端三方互不相认。该校验已落地为
  `api/openapi/openapi_test.go` 的 `TestContractPathsAreAllRegistered`：契约每条
  path 必须出现在 `service/apiserver.UserFacingPaths` 集合中（前半，子集关系）；
  「`UserFacingPaths` 每条都有 mux 注册」是后半守卫，合起来才是 §10 的完整链条
- `docs/design/` 必须与实现一致（既有约束）
