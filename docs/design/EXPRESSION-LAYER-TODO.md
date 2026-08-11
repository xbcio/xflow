# 表达式与模板求值层遗留项

`docs/design/DSL-SPECIFICATION.md` §4 描述的表达式引擎与实现有系统性分歧。
本文件记录**实测状态**，不含修复方案——方案要先走 brainstorm。

调查日期 2026-08-11，两次独立调查 + 四组实测探针（探针为一次性，未留仓库）。
基线 `8556fd8`。

## 一句话

`${{ }}` 模板语法零实现；`$nodes` 等四个根不存在；多数节点根本不求值参数。
照 spec 写出的工作流跑不出预期结果，且 **`xflow.http` 的 header/body 会静默
把模板字面量发给对端**。

## 三层缺口

### 第 1 层：`${{ }}` / `{{ }}` 语法零实现

spec §4.1 用 80 行描述了两种模式与三条解析规则，还规定「`${{ }}` 前后有文本要
编译报错」。全仓库**没有任何代码**剥离 `${{`/`}}`、检测插值模式，或实现那条
编译期校验。

`exprx.EvalExpr` 直接把参数值交给 expr-lang 编译，所以 spec 的写法是语法错误：

| 写法 | 实测结果 |
|---|---|
| `${{ $params.amount > 1000 }}` | `compile expression: unexpected token Bracket("{") (1:2)` |
| `{{ $params.order_id }}` | `compile expression: a map key must be a quoted string…` |
| `$params.amount > 1000`（实现期望的裸形态） | `true` |

即会求值的节点也只认裸表达式。spec 通篇 110 处 `${{ }}`，无一可用。

### 第 2 层：多数节点不求值任何参数

**没有统一的参数模板求值层。** `engine/input.go:29` 把 `g.NodeAt(...).Parameters`
原样拷进 `Input.Params`，引擎不做任何预处理；是否求值由每个节点各自决定。

| 求值 | 节点 | 求值的参数 |
|---|---|---|
| 是 | `xflow.if` | `condition` |
| 是 | `xflow.switch` | `rules[].condition`、`expression` |
| 是 | `xflow.map` | `items`、`expression` |
| 是 | `xflow.split` | `items` |
| 是 | `xflow.function` | `code`（expr 模式） |
| 是 | `xflow.script` | code 跑在 exprx env 上 |
| 是 | `xflow.transform.set` | `expressions` 各值 |
| 是 | `xflow.transform.filter` | `items`、逐项 `condition` |
| 是 | `xflow.transform.{sort,limit,aggregate,remove_duplicates}` | `items` |
| **否** | `xflow.http` | url / headers / body / query 全字面 |
| **否** | `xflow.grpc` | host / service / method / request / metadata |
| **否** | `xflow.database` | operation / table / credential / where / data |
| **否** | `xflow.notification` | channel / to / subject / message |
| **否** | `xflow.approval` | approvers / mode / timeout |
| **否** | `xflow.wait` | signal_name / duration / timeout |
| **否** | `xflow.transform.{pick,rename}` | — |
| **否** | 全部 trigger（webhook/cron/kafka/timer） | — |

`xflow.if` 与 `xflow.http` 的差别就是一行：前者把 `Params["condition"]` 交给
`exprx.EvalExpr`（`flow/if.go:54-55`），后者把 `Params["url"]` 直接交给
`url.Parse`（`action/http.go:169-174`）。

### 第 3 层：四个根不存在

`exprx.BuildExprEnv`（`node/internal/utils/exprx/exprx.go:93-124`）提供七个根：
`$input`、`$inputs`、`$vars`、`$config`、`$params`、`$runtime`、`$supplies`
（外加 `Data` 顶层键的展开，以及 map 逐项的 `$item`/`$index`/`$items`）。

spec 用到但 env 里没有的：`$nodes`、`$execution`、`$workflow`、`$env`。
所以**即便剥离了 `${{ }}`**，`$nodes['validate_order'].is_valid` 仍会以
`unknown name $nodes` 失败（实测）。

`$nodes` 在生产代码里唯一的出现处是 `engine/graph/group_portability.go:11` 的
正则 `\$nodes\[['"]([^'"]+)['"]\]`——它扫描参数字符串以**拒绝**跨组引用，
从不把 `$nodes` 作为运行期值提供。这是一个只有否定语义的实现。

内置函数的情况比想象的好，但 spec 有两处名字错：

| spec 写法 | 实测 |
|---|---|
| `upper` / `lower` / `trim` / `now` / `date` | 可用（expr-lang 内置） |
| `??`、三元 `? :` | 可用 |
| `parseJson` | **不存在**，内置名是 `fromJSON` |
| `dateFormat` | 不存在 |
| `sprintf` | 不存在 |
| `getCredential` | 不存在（凭证走声明式注入 `$credentials`） |

## 为什么这比看起来严重：失败不对称

同一个节点的不同参数，失败响亮程度完全不同（`xflow.http` 实测）：

| 模板位置 | 结果 |
|---|---|
| `url` | 响亮失败：`unsupported protocol scheme ""` |
| `headers` | **请求照发，HTTP 200，执行成功**，对端收到 `X-Order: ${{ $params.order_id }}` |
| `body` | **同上**，对端收到 `{"order_id":"${{ $params.order_id }}"}` |

零日志、零指标、无任何诊断。作者本地测通了（url 是字面量所以没报错），生产上
每一条请求都在发字面模板。`xflow.database` 的 `where`/`data` 与
`xflow.notification` 的 `to` 同类——前者把模板字面量写进 SQL 条件，后者往字面
地址发通知。

## 测试覆盖：零

全仓库用到 `${{ }}` 的测试只有五处，全部是**结构性**测试（编译/图/可移植性）：

- `engine/graph/body_compile_test.go:37`
- `engine/graph/snapshot_body_failclosed_test.go:87`
- `engine/graph/subgraph_body_criterion_test.go:235,252`
- `engine/graph/group_portability_test.go:15,45,308`

它们验证的是图能编译、`$nodes` 正则能扫出跨组引用，从不真正求值。
**没有任何测试断言 `${{ }}` 形态的参数产出了计算结果。** 这就是这个缺口能存在
到今天的原因——`group_portability_test.go` 甚至把
`"url": "${{ $nodes['D'].json.result }}"` 当作合法夹具用，而那个值在运行期是
纯字面量。

## 决定这件事时要先回答的问题

不写方案，但下面几点不定就没法动手：

1. **求值发生在哪一层。** 引擎统一预处理 `Params`（所有节点一次性获得能力，
   但要处理「哪些参数不该求值」——`xflow.function` 的 `code`、`xflow.script`
   的脚本本体、`xflow.http` 的 `body` 里合法的花括号文本），还是各节点显式声
   明可求值参数（工作量线性于节点数，但语义精确）。
2. **~~`$nodes` 要不要建。~~ 已答：建，且比预想便宜。** 见下方「已推翻的判断」。
3. **spec 是收窄还是实现追平。** 110 处示例里有多少是真实需求，多少是照抄
   n8n 表达式风格。收窄 spec 比实现三层缺口便宜一个数量级。
4. **静默失败先止血。** 无论最终选哪条路，「参数里含 `${{` 却不会被求值」在
   编译期是可判定的——这一条独立于上面三问，且能把最坏形态从静默降为响亮。

## 已推翻的判断（2026-08-11 实测更正）

### `$nodes` 不需要扩张数据流模型

本文件先前把 `$nodes` 记为「一次数据流模型的扩张」，理由是「当前 `Input` 只带
上游直连数据」。**该理由不成立。**

`CommitNodeRequest.StoreOutput` 在生产代码里只有三个赋值点，**全部是 `true`**
（`engine/commit.go:237`、`engine/atomic_commit.go:106,177`）——每个节点的输出
本来就全量持久化。`StateStore.GetOutput(ctx, execID, nodeName)` 早已是接口方法
（`engine/interfaces.go:82`），`buildInput` 自己就在用它读上游。

所以 `$nodes['x']` 要的数据已经躺在 state 里，缺的只是一个按名字惰性取的访问器，
不是新的数据通路。这把 `$nodes` 从架构级决策降为一个 accessor 加一层缓存。

**它也不该被砍。** `$nodes` 在 spec 里出现 85 次，高于 `$params`（45）与
`$input`（45）——是 spec 表达能力的主干，不是照抄 n8n 的装饰。

与 group 隔离的交互也不是障碍，而是配套：`group_portability.go` 那条正则已经在
编译期保证组内 `$nodes` 只指向同组成员，所以内层引擎的惰性访问器只需要看内层
state。两者本来是一套设计的两半，至今只落地了否定那半。

