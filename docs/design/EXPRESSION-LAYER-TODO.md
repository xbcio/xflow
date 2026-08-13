# 表达式与模板求值层遗留项

`docs/design/DSL-SPECIFICATION.md` §4 描述的表达式引擎曾与实现有系统性分歧。
本文件记录**实测状态**。

原始调查 2026-08-11（基线 `8556fd8`），三层缺口在 Task 0～3 与后续几个分支里
陆续关闭；**2026-08-13 在 `d0236f9` 上逐条复验**，原「三层缺口」六条断言全部
不再成立，已改写为下方的现状描述，历史结论移入「已关闭」一节。

## 一句话

`${{ }}` / `{{ }}` 两种模式都已实现并有边界求值层兜底，`$nodes`/`$execution`/
`$workflow` 三个根已建（`$env` 是**刻意不建**）。**三层缺口与 trigger 激活期
参数不求值均已关闭**，本文件现在只是实测记录，无仍开的口子。

## 现状（2026-08-13 复验）

| 曾经的缺口 | 现在 | 证据 |
|---|---|---|
| `${{ }}` 语法零实现 | 已实现三条渲染规则 | `exprx/template.go:28-75` |
| 「`${{ }}` 前后有文本」无编译校验 | 编译期拒绝 | `engine/graph/template_reject.go:23-70` |
| 无统一参数求值层 | handler 边界统一求值 | `execution/params.go:65-117`，接线于 `execution/runner.go:115-119` |
| `xflow.http` 等节点不求值 | 全部求值 | `engine/graph/evaluable_params.go:65,79-87`（空条目＝全部求值） |
| 四个根不存在 | 三个已建 | `exprx/exprx.go:133,140-146` |
| `sprintf` 不存在 | 已注册 | `exprx/functions.go:44-56` |
| 无任何测试断言 `${{ }}` 产出计算结果 | 有，含端到端 | `exprx/template_test.go`、`execution/params_test.go:31-69` |

注：`engine/input.go` 仍然把 `Parameters` 原样拷进 `Input.Params`——这一句
描述本身没过时，但求值挪到了上一层（`execution/runner.go` 的 handler 边界），
所以「无统一层」的结论不再成立。

求值层的两条承重约束（改动前必读，注释写在 `evaluable_params.go` 里）：

1. **豁免集是 `evaluableParams` 取反**。handler 自己求值的参数边界必须跳过，
   否则双求值把条件求成 bool → `cast.ToString` → `"true"` → switch 恒走第一条
   规则，零诊断。
2. **键必须是 `(nodeType, paramName)`，不能只用 paramName**。
   `xflow.trigger.cron` 有个叫 `expression` 的参数装的是 cron 式子
   （`0 */5 * * *`），只按名字建表会把它误判成可求值。

`$env` 与 `getCredential()` 是**刻意不实现**，不是遗留：前者等于在用户可提交的
工作流定义上开「读任意环境变量」的口子，而 runner 持有凭证与云密钥；后者与已落地
的声明式凭证注入冲突，且 spec 自带示例把 token 拼进参数串，参数会随执行记录落库。
详见下方「spec 收窄已完成」。

## 已关闭

### trigger 激活参数不求值（2026-08-12 实测发现，2026-08-13 按方案 A 关闭）

原状：`graph.Compile` **接受** `xflow.trigger.kafka` 上的
`topic: "{{ $config.topic }}"`，而下游没有任何环节求值它，consumer 订阅的是字面
主题名。这条不会被 `execution/params.go` 的边界层兜住——trigger 参数走激活链路，
根本不经过 `Runner.Execute`；`evaluable_params.go` 里几个 trigger 的空条目只对
执行期有意义，对激活期是空头支票。

现在：`engine/graph/activation_params.go` 的 `EvaluateActivationParams` 用只含
`$config`/`$vars` 的受限环境渲染，三条激活路径共用它——独立 trigger
（`service/control/entry_activation_manager.go` 的 `UnitNode` 分支）、trigger
group（`engine/graph/subgraph_package.go` 的 `ProjectGroupPackage`）、进程内内联
（`sdk/xflow/trigger_runtime.go`）。

四处**改动前必读**的约束，都是实测撞出来的：

1. **求值必须落在 `engine/graph`，不能只写在控制面。** `assignPackageHashes` 在
   `graph.Compile` 里算包哈希，`projectPackageForGroup` 在指令构建期**重算**并
   在不一致时拒发。只在两个调用点之一渲染，会让每一次 group 激活都 fail closed。
2. **独立路径的哈希算在渲染后的参数上。** `$config` 改动导致渲染值变化时哈希必须
   变（触发重投），而模板不同但渲染结果相同的两个版本必须**不**变。
3. **根可用性守卫不能交给 expr。** `exprx.CompileExpr` 按 `(code, asBool)` 缓存并
   在命中时忽略 env。实测：`topic-{{ $execution }}` 冷缓存报
   `unknown name $execution`，任何一次节点执行编译过该源码之后就不报错了，直接
   渲染成 `"topic-<nil>"`。测这一层的用例**必须先预热缓存**，否则打到的是 expr
   自己的冷缓存拒绝，守卫删掉了测试照样绿。
4. **group 只渲染入口成员。** 其余成员都会被内层引擎当作任务执行，走
   `execution/params.go`；在这里渲染既是双求值，又会因为成员引用 `$input` 而中止
   整个投影——由于 `assignPackageHashes` 在 `Compile` 内，那等于中止整个编译。

`$supplies` 被拒绝，尽管 `BuildExprEnv` 无条件提供它且在控制面能解析出值：supply
内容由 runner 门控在指令下发**之后**取回，这里渲染出的值会被冻结、永不刷新。

### 三层缺口（Task 0～3，2026-08-11 ~ 08-12）

原始调查记录的三层：`${{ }}` 语法零实现 / 多数节点不求值任何参数 / 四个根不存在。
当时最严重的表现是**失败不对称**——`xflow.http` 的 `url` 写模板会响亮失败
（`unsupported protocol scheme ""`），而 `headers`/`body` 写模板则**请求照发、
HTTP 200、执行成功**，对端收到字面量 `X-Order: ${{ $params.order_id }}`，零日志
零指标。`xflow.database` 的 `where`/`data` 与 `xflow.notification` 的 `to` 同类。

这正是边界求值层要消灭的故障，现已消灭（见上表）。

### 编译期可达性闸的定向撤销（2026-08-12）

Task 0（`3680159`）建过一条编译期规则：参数含 `{{` 但「不会被求值」→ 编译拒绝。
Task 1（`7173668`、`a142dcf`）建了边界求值层：**所有非豁免参数都会被求值**。

两者共存时互相抵消：`execution/params_test.go` 断言 `{{ $params.order_id }}` 在
`xflow.http` headers 中被正确求值，而同一形态被编译期以「parameter is never
evaluated」拒绝，spec 的目标形态部署不上去。

**裁决：拆掉可达性闸，保留畸形形态闸。** 理由是 Task 0 注释自己写明它存在的意义
是防「ships the literal template in a header and still reports HTTP 200」——
Task 1 消灭了这个静默，闸门保护的故障消失后，闸门本身变成对正确用法的误拒。

### `$nodes` 引用（Task 2，2026-08-12）

runner 侧**无 state 访问**——`GetOutput` 只在 control plane 的 `Engine` 上，
handler 拿到的只有 `types.Input`。所以 `$nodes['x']` 不能做成惰性访问器，必须
**编译期抽引用集 + 运行期预取**，与 `$supplies` 完全同构：编译期
`buildNodesRefs` 抽引用集存进 `g.nodesRefs[i]` → `buildInput` 逐个 `GetOutput`
预取填进 `Input.Nodes` → `BuildExprEnv` 注入 `env["$nodes"]`。

两处容易写错、都由测试钉住：

**未执行节点必须是 typed nil map。** spec §4.2 推荐
`$nodes['optional_step'].name ?? 'default'`——带成员访问的 `??`：

| `$nodes["x"]` 的值 | `$nodes['x'] ?? 'D'` | `$nodes['x'].f ?? 'D'` |
|---|---|---|
| 键不存在 | `"D"` | **ERROR** |
| untyped nil | `"D"` | **ERROR** `cannot fetch f from <nil>` |
| `map[string]any(nil)` | `"D"` | `"D"` ✓ |

只有第三行能让 spec 的推荐写法工作。`GetOutput` miss 返回 `nil, nil`，静态类型
已是 `map[string]any`，直接赋值就是 typed nil。

**body 子树必须跳过。** `extractNodeRefs`（`group_portability.go`）递归扫全树
包括 body，而 body 内的 `$nodes['innerA']` 解析的是**内层图**。记到外层会误拒
合法工作流（实测：S→M(map, body 内 innerB 引用 innerA)→T，顶层无 innerA →
`$nodes reference "innerA" does not exist`）。`deriveNodesRefs`
（`nodes_refs.go`）用 `declaresSubgraphBody` 按值判定跳过 body 子树，
`xflow.http` 的 `body` 参数不会被误跳。

body **跨域**引用外层节点是后来单独建的通道（`VisibleOuterNodes` + 随批下发的
快照），见 SUBGRAPH-ENGINE-TODO.md 与 DSL-SPECIFICATION.md 的跨域引用两节。

### spec 收窄已完成（Task 3，2026-08-12）

`DSL-SPECIFICATION.md` §4 已改写至与实现一致（`72b1561`）。删除的不是「未实现」
而是**不该实现**的两项：`$env` 与 `getCredential()`（理由见上）。另删
`$workflow.id`（`WorkflowDef.ID` 无生产写入点，且被 `runtimeHash` 当作 runtime
实例指针排除）与 `$execution.mode`（从未存在）。

写进 spec 的每个表达式示例都先经真实 `EvalExpr` 跑过。由此查出两处 spec 自己在
推荐的**静默错误写法**：`sortBy(arr, 'field')` 原样返回**未排序**数组且不报错
（expr-lang 把 string 参数当逐项常量键，所有项比较相等，稳定排序保序）；
`#{...}` 是语法错误，管道谓词里的对象字面量必须整体加括号 `map(({id: .id}))`。

spec 仍有两处函数名与实现不符属**实现侧没有**、spec 也已不再推荐：`parseJson`
（内置名是 `fromJSON`）、`dateFormat`（spec §「不提供的函数」已明列不提供）。

### 已推翻的判断：`$nodes` 需要扩张数据流模型（2026-08-11 更正）

本文件先前把 `$nodes` 记为「一次数据流模型的扩张」，理由是「当前 `Input` 只带
上游直连数据」。**该理由不成立。** `CommitNodeRequest.StoreOutput` 在生产代码里
三个赋值点全是 `true`（`engine/commit.go`、`engine/atomic_commit.go`）——每个
节点的输出本来就全量持久化，`StateStore.GetOutput` 早已是接口方法且 `buildInput`
自己就在用它读上游。所以 `$nodes['x']` 要的数据已经躺在 state 里，缺的只是取用
方式，不是新的数据通路。

它也不该被砍：`$nodes` 在 spec 里出现 85 次，高于 `$params`（45）与 `$input`
（45），是表达能力的主干。
