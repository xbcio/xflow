# 子图引擎与 map body 遗留项

`feat/subgraph-engine-and-map-body`（30 commits，2026-08-04 ~ 08-06）交付后未做的事。
按「不做会怎样」排序，不按工作量。

背景见 [DSL-SPECIFICATION.md §xflow.map](./DSL-SPECIFICATION.md)（body 语法与结果结构）
与 [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)（group 侧的同族机制）。
本文件只列**待办**。

## P0 — 用之前必须修

## P1 — 规模上去会疼

### 4. 无 `max_concurrency` 节流

设计时显式排除（见 spec §10.1），当时 body 还是 pass-through stub。

**本条原先写的理由是错的，2026-08-11 实测推翻。** 原文说风险是「全部批次一次性对
下游发起真实 I/O」，把爆炸半径描述成对下游系统的并发外呼。实际不成立：并发度由
worker 池上限决定，与批次数无关——local 后端是 `memoryQueue` 的 `concurrency` 个
worker，分布式后端是 runner 侧的池。批次多只让队列变深，不让并发变宽。

真实的故障是另一回事，而且比这严重得多：`FlushOutbox` 跑在 queue worker 协程上
（`engine/atomic.go` 把 `TaskTypeNodeBatch` 直接派进 `ExecuteBatch`），而
`memoryQueue.Enqueue` 满了会阻塞。于是**扇出宽过队列缓冲就是永久死锁**——每个
worker 都停在一次只有它们自己能腾出空间的 send 上。默认 `concurrency=4` 下四个
400 批次的并行 map 就够（1600 > 1024 缓冲）。已在 8180d58 修复：`FlushOutbox`
改走可选的 `TryEnqueue`，满了就把剩余意图留在 outbox 里等下一轮，且**不消耗投递
预算**（走失败路径会 `Attempts++` 并最终进死信，等于静默丢活）。回归测试见
`backend/providers/local/fanout_backpressure_test.go`。

所以 `max_concurrency` 现在纯粹是**吞吐整形**需求（限制单个 map 占用队列的份额，
避免一个大 map 饿死同执行内的其他分支），不再是正确性缺口。优先级相应下调。

无测试、无告警。合并时无证据表明造成过真实事故。

### 9. 并发 `FlushOutbox` 会重复投递同一条意图

多个 worker 可以同时对同一 execution 调 `FlushOutbox`，各自 `ListOutbox` 到同一条
未 ack 的条目、各自 enqueue、各自 ack。body 因此被多跑。实测：800 项的 map 在
`concurrency=4` 下 body 跑了 826～1110 次，`concurrency=1` 下精确 800 次。

**这在契约内**，不是缺陷：`engine.OutboxEntry` 的注释写明投递是 at-least-once，
`engine/expand.go` 也要求「有副作用的 body 节点必须按 `$item` 里的业务键幂等」。
`fanout_backpressure_test.go` 的多 worker 用例因此断言「≥ 期望数」而非精确相等。

记在这里是因为**扇出死锁修复（8180d58）测量上放大了这个窗口**——每次 flush 变短，
两个 worker 撞上同一批条目的机会变多（同样 800 项从 826 涨到 1004～1110）。要收窄
的话，路子是给 `ListOutbox` 加投递租约（列出即标记 in-flight，超时才可再列），
但这会把 outbox 从「无状态列表」变成「有租约的队列」，成本不小。当前无需求驱动。

## P2 — 命名与死代码

### 7. `types/transform.go` 的 `TransformSpec` 尚无消费者（保留）

T11 声明它，本打算由 T12 消费，T12 没有消费。**明确保留不删**：它描述的
`{expression | body}` 二选一形态正是 filter/reduce 落地时要用的，删掉等于丢掉一份
已写好的设计意图。

P2-5 修完后，编译期已经按这个形状执行了：`transformNodeTypes` 就是它说的
「transform-style node」集合，`validateNodeBody` 对集合里每个类型执行同两条规则
（二选一 + 声明了的 body 必须是子图）。2026-08-11 之后 `xflow.map` 的
**两种形态也都真的能跑**（expression 形态见下方「已修复」一节），所以这个形状
不再只是编译期的形式约束。缺的只是**结构体本身仍无人反序列化到**——各节点仍从
`Parameters` 里逐键取 `expression`/`body`。落地 filter/reduce 时把取参改走
`TransformSpec` 即可闭合。

### 8. `engine/graph/subgraph_package.go` 的 `ProjectSubgraphPackage` 名字有歧义

它投影的是 **group** 包（入参是 `unitIdx`，断言 `Kind == UnitGroup`），与
`xflow.subgraph` 这个节点类型无关——后者的投影入口是 `ProjectNodeBodyPackage`。

`xflow.subgraph` 是作者手写的 body 容器节点类型；`xflow.group` 是编译器从顶层
`groups:` 生成的调度单元对外自称的合成路由类型，没有任何 handler 注册它。两者最终
汇合在同一个 `execution/subgraph.Executor`（该执行器分不出 group 和 map body），
但来源与用途不同。改名会动到公开 API，未做。

## 已修复

### group / batch 租约根本不带 W3C TraceCarrier（2026-08-13 修复）

上一节收尾时以为缺的只是「把 `ExecutionSnapshot.TraceCarrier` 也转发给内层子执行」。
查证发现缺口在更上游一层：**group 与 batch 的租约压根就没有 carrier**。

`service/control/core.go` 的轮询路径上，注入点
（`lease.TraceCarrier = tracing.InjectCarrier(dispatchCtx)`）位于 `xflow.task.dispatch`
span 块内，而 `isGroupTask` / `isBatchTask` 两个分支在到达该块**之前就 return 了**——
它们各自走 `dispatchGroupLease` / `dispatchSubgraphLease`，两处都不开 span、不注入。

于是 runner 侧 `service/runner/runner.go:367` 的
`execCtx := tracing.ExtractCarrier(ctx, lease.TraceCarrier)` 拿到空 map，
`xflow.task.execute` 以**根 span** 起头。所以修复前的真实状态不是「内层成员断链、
外层 group span 还在」，而是**整个远端 group / map 批次任务从工作流 trace 上整体脱落**。

修法是把三条路径的取 carrier + 开 span 收敛成一个 `Core.startDispatchSpan`，
两个 dispatch 函数各自改用它并在成功分支注入 carrier。`dispatchGroupLease` 的
`ErrGroupLeaseAlreadyActive` 恢复分支同样注入：恢复出来的租约原本那份 carrier
随着建它的进程一起没了，而即将收到它的 runner 无论如何都要新起一个 execute span。

**实测**（`service/control/lease_trace_carrier_test.go`，三条）：每条给 fake 引擎
配一个**各不相同**的 submit 侧 trace id，断言租约 carrier 里的 trace id 等于它——
不是断言「非空」。这个区别是承重的：`startDispatchSpan` 取不到 submit carrier 时会
回落到轮询上下文起一个新根，**照样产出非空 carrier**，只是挂在一棵自己的树上。
node 那条是正对照（修复前即绿），保证 group/batch 两条不是在拿两个空 map 比相等。

反向探针三处各自实测变红：去掉 batch 注入 / 去掉 group 注入 / 去掉
`startDispatchSpan` 里的 submit carrier 提取（第三处让三条同时变红，且报的是
「trace id 是另一棵树的」而非「空」）。

**遗留**：`SubgraphLeasePayload.TraceID`/`SpanID`（上一节加的）与本次的 carrier
现在是两条并行通道。前者供审计/检索，后者供 OTel 父子关系，按
[RELEASE-GATES.md §4](./RELEASE-GATES.md) 两者不可互相顶替，所以并存是对的；但
runner 收到 carrier 后**只用它起 `xflow.task.execute`**，没有再往内层子执行的
submit 上下文里转发。也就是说 group / batch 任务本身现在挂回了工作流 trace，
而 body 内每个成员节点的 span 仍是 execute span 的本地子孙、不是跨进程 parent 链。
对当前部署形态（内层引擎在 runner 进程内跑）这已经够用，记在此处备将来内层再跨进程时查。

### body / group 成员的 trace 身份在子执行处断掉（2026-08-13 修复）

**实测的修复前下场**：两条路径各写一个探针，成员节点收到的 `Input.TraceID`
与 `SpanID` 都是空串——外层 execution 带着 `trace-outer`/`span-outer` 提交，
成员一个都没看见。原因是子执行是**以自己的 execution ID 全新提交**的：
`execution/subgraph.Executor` 往 `submitCtx` 上只挂了 execution ID 和 scope，
于是内层 snapshot 的 trace 字段为空，`buildInput` 给每个成员发的都是空对。
链路正好断在 map / group 节点上——恰恰是最需要它接下去的地方，因为逐项与逐成员
的活都发生在那之后。

修复让两条路径**汇合而不是分叉**：

- **group**：租约本来就带一整个 `*types.Input`，`TraceID`/`SpanID` 已在上面，
  什么都不用新传，只缺 submit 时的交接。
- **map body**：批次租约带的是 items 不是 Input，所以 `BatchBodyRequest` 与
  `SubgraphLeasePayload` 各加一对字段（与 `Runtime` 同构——都是批次任务自己读不到、
  只能由父节点展开时从 snapshot 转发的提交期值）。`bodyItemInput` 把这对值落到
  **group 租约已经在用的同两个字段**上，于是 `Executor.Execute` 只有一个读取点，
  没有 map 专属分支。

`executionRuntime` 相应扩宽成 `executionSubmissionContext`（两个调用点都要
runtime + trace，再加一个读取者等于把同一份 snapshot 读两遍）。

安全上两者性质不同，代码注释里写明了：`OuterNodes` 带的是真实业务输出（上游常常
是含凭证的 HTTP 响应体），绝不可入日志；`TraceID`/`SpanID` 是 tracing 后端铸的
关联标识，**可以**出现在日志行里。

覆盖：`backend/providers/local/local_subgraph_trace_test.go`（map body 与 group
各一条端到端）、`engine/subgraph_lease_test.go` 的 payload 断言（保证新线上字段
不是死重量）。三处反向探针各自实测变红：去掉 submitCtx 交接 / 还原
`bodyItemInput` / 去掉 payload 赋值。

**后续闭合（2026-08-13 同日）**：上一段原先记录「没有转发 `TraceCarrier`，远端
body 的 span 仍没有真正的 OTel parent」——查证时发现缺口比记的还大一层，且已一并
修掉，见下一节。

### `xflow.map` 的 expression 形态从未实现 + 无 body 的 map 编译通过（2026-08-11 修复）

**实测的修复前下场**：`{items, expression}` 的 map 编译通过、`BodyAt == nil`，
提交后 `WaitDone` 以 `context deadline exceeded` 挂死——handler 完全无视
`expression` 参数，无条件发出扇出描述符，于是每个批次撞 `ErrNoMapBody`。
参数无 body 也无 expression 的 map（含裸 `{Name, Type}`）同样如此。

修复分两半，缺一不可：

1. **实现 expression 形态**（`node/internal/flow/map.go` 的 `evalItemsInline`）：
   逐项求值、就地产出 `{results, count}`，不发描述符。它必须与 body 形态给下游
   同一份契约，否则同一个节点的两种写法会有两套下游语义——`count` 恒等于输入长度、
   失败项以 `{_error, _index}` 占位、`continue_on_error` 只在「至少一项成功」时
   放行（全失败两种设置都失败），逐项根用带 `$` 前缀的 `$item`/`$index`/`$items`
   与 `execution/subgraph` 的 `bodyItemScope` 对齐（无前缀的 `item`/`index` 归
   filter 节点，会静默遮蔽）。`batch_size` 对它无意义且**不**影响 `$index`。
2. **编译期拒绝两者皆无**（`validateNodeBody` 的 fan-out 规则 + `fanOutNodeTypes`）。
   这一半是**判据下沉的前提**而不只是整洁：判据一旦改成 `BodyAt != nil`，无 body
   的 map 会被判「不扩展」，扇出描述符被当成节点的普通输出提交下去——比修复前的
   挂死更糟，是静默的错答案。

覆盖：`node/internal/flow/map_expression_test.go`（六条，含 continue_on_error
的两种设置与全失败）、`engine/graph/expansion_requires_body_test.go`
（四种被拒形状 + 两条正对照）。反向探针五处各自实测变红：编译规则去掉 expression
豁免 / handler 不走 inline 分支 / `continue_on_error` 恒 true / 失败项被丢弃 /
逐项根去掉 `$` 前缀。

`ErrNoMapBody` 因此只剩一条可达路径——`compileTrusted`（投影包走的受信路径，
不跑 `validateNodeBody`）。判据下沉之后这条路径连「可达」都不再成立，守卫随之
上移到编译期（见下一节），`ErrNoMapBody` 本身已删除。

### 扩展判据从嗅 payload 改为读编译期投影的 body + 标记键彻底移除（原 P2-6，2026-08-11 修复）

原条目说这是「纯命名不对称，无功能后果」，且「修它要动 wire / 持久化状态格式」。
两句都不对：`_loop`/`_split` 从来不只是名字，它是**运行期的扩展判据**
（`isLoopSplitOutput` 嗅 `result.Output.Data` 里有没有这两个键）；而正因为它是
判据而不是数据，删掉它不动任何持久化格式——判据换源即可。

**功能后果**：任何 handler 只要在输出里用了 `_loop` 这个字段名，就把自己变成了
扇出节点。它没有 body，扩展出的每个批次撞 `ErrNoMapBody`、重试、整条执行挂到
deadline。`xflow.split` 的编译期拒绝（`split_rejection_test.go`）实测过这个下场。

判据改为 `g.BodyAt(nodeIdx) != nil`：一个节点扩展，当且仅当编译器给它投影了子图
body。这是图的结构性质，payload 无权回答。`BodyAt` 的权威性由快照守卫兜底——
声明了 body 却没带投影包的节点解码不出来。

**判据下沉把一个已知缺陷的症状从响亮改成了静默**，这是本次最容易漏的一环：
`compileTrusted` 是 `Compile` 的 pass 列表的手写平行实现，本文件上方记录过它漂移
（曾不跑 `projectNodeBodies`）。旧判据下这次漂移每个批次以 `ErrNoMapBody` 响亮
失败；新判据下没有 body 就不扩展，批次根本不产生，handler 发的扇出描述符会被当成
节点的普通输出提交下去，body 跑零次且无任何诊断。所以守卫必须同时上移到编译期，
且必须落在 `projectNodeBodies` 这个**两条编译路径共用**的 pass 上
（`assertFanOutNodesResolved`），而不是只在 `Compile` 侧的 `validateNodeBody` 里。

标记键随之从 `node/internal/flow/map.go` 与 `split.go` 移除，不改名。一个「必须存在
才正确、却没有任何东西能校验它存在」的键，叫什么名字都是负债。

覆盖：`engine/expansion_criterion_test.go`（无 body 的节点带满标记键也不扩展 /
expression 形态不扩展 / 有 body 的 map 无标记键也扩展 / 失败的 map 走普通错误路径）、
`engine/batch_body_test.go` 的 `TestTheTrustedCompilePathAlsoRejectsAFanOutNodeWithNoBody`。
反向探针实测：判据退回嗅 payload → 前两条红；判据退成裸 `Type == "xflow.map"` →
expression 那条红；摘掉 `assertFanOutNodesResolved` → 受信路径那条红。

**一条负结果如实记录**：`taskResultExpands` 里「只有成功才扩展」的收窄摘掉后，
全仓一条测试都不红。原因是失败此时改走 `commitLegacyTaskResult`，其错误分支跑同一
套重试与 OnError，提交时又在 `!AllowCycles` 处折回 `commitAcyclicNode`——两条路在
失败处理上是收敛的。收窄予以保留（失败不该进扩展路径），但它防的是将来两条路分叉，
不是今天的缺陷。

### 三处 map 专属判断写死 `xflow.map` + 带请求体的 HTTP 节点存下去读不回来（原 P2-5，2026-08-11 修复）

body 存储本来就是通用的：`NodeMeta.Body` 随节点整体走 wire 与 hash，执行器拿到的
`NodeBodyPackage` 也与投影者无关。卡住的只有编译期那几处字面量。

#### 判据：看值的形状，不看节点类型

**「这个 body 是不是子图」这个问题现在由 `declaresSubgraphBody`
（`engine/graph/compile.go`）一处回答，判据是值的形状**：`params["body"]` 能解成
一个 `types.NodeDef` 且其 `Type == "xflow.subgraph"`。编译期的投影、快照解码期的
fail-closed 守卫、嵌套禁令的递归检查，三处调的是同一个函数。

先前的两个候选都不对：

- **嗅参数名**（`params["body"]` 存在与否）不行——`xflow.http` 也有一个叫 `body`
  的参数，那是请求体。
- **类型白名单**也不行——它让每个新的带 body 节点类型都要回来改这个包，而且它会
  与别处的判据漂移。

值判据对本仓库出现过的每一种 body 取值都能区分（已实测）：http 的 object body
解出空 `Type`，string / array body 根本解不动，只有真 body 到得了 `"xflow.subgraph"`。
而 `xflow.subgraph` 在顶层是被拒绝的（`compile.go:144`），作者不刻意写在 body 里
就产不出这个形状。

#### 顺手关掉的 P0：`xflow.http` 带 JSON 请求体的图存下去读不回来

判据漂移不是假想的。修复前 `snapshot.go` 的 fail-closed 守卫嗅
`Parameters["body"]` **是否存在**，而 `compile.go` 按**节点类型集**判断。于是任何
带 JSON 请求体的 `xflow.http` 节点：编译通过（不在类型集里 → 不投影 body）、
持久化成功，然后每一次 `Graph.UnmarshalJSON` 都以
「declares a body but carries no projected package」失败。

两条解码路径的爆炸半径不同：`workflowreg` 那条捕获错误后回落到
`graph.Compile(record.Definition)`（自愈）；**`rstate.LoadGraph`
（`state_node.go:36-39`）没有回落**——错误一路传到 `backend.go:666`，任务就地失败。
object 与 string 两种请求体都会触发，走的内部路径还不一样。

让守卫改调 `declaresSubgraphBody` 即闭合：两侧不可能再对「哪些节点有 body」有分歧。

#### `transformNodeTypes` 的职责收窄为两条规则

它不再决定什么被投影为子图，只界定 transform 契约（`types/transform.go` 的
`TransformSpec` 描述的形状）本身的两条：expression 与 body 二选一；声明了的 body
**必须**是子图（于是畸形 body 在编译期报错，而不是当成不透明参数带给 handler，
运行期每个批次以 `ErrNoMapBody` 失败）。

这两条对 wrapper 式 body 节点（retry / timeout / try-catch，body 是被守护的东西）
都是错的。那类节点无需在这里加条目——它的 body 凭自身形状被投影。

#### `bannedBodyMemberTypes` 退回字面量，但禁令的可扩展那半移到了成员判定

先前它「从 transform 集派生」，那是错的：`xflow.map` 的 expression 形态**根本没有
body 给值判据看**。所以这张表是三个字面量，各有一条值看不见的理由：
`xflow.subgraph` 是容器本身，`xflow.split` 到处被拒，`xflow.map` 在 body 形态下
无条件扩展（expression 形态不扩展、本可豁免，但禁令刻意停在类型一级：成员的形态
只差一次参数改动，一条改个参数就能悄悄解除的禁令不算禁令）。

**真正需要可扩展的那半在 `validateNodeBody` 里**：逐个成员用
`declaresSubgraphBody(inner.Parameters)` 检查。这才是「将来某个类型长出了 body」时
仍然成立的那条——类型表对它一无所知。

#### 回归测试与反向探针

六条测试在 `engine/graph/subgraph_body_criterion_test.go`，五处反向探针各自实测过：

| 反向探针 | 变红的测试 |
|---|---|
| 快照守卫退回嗅 `Parameters["body"]` 存在性（修复前形态） | `TestSubgraphBodyCriterionIsSharedByCompileAndSnapshot`（object / string 两例） |
| `declaresSubgraphBody` 退化成 `_, ok := params["body"]` | `TestDeclaresSubgraphBody_KeysOffTheValueNotTheName` + 上一条 |
| 删掉成员级 `declaresSubgraphBody` 递归检查 | `TestBodyMemberDeclaringItsOwnSubgraphBodyIsRejected` |
| transform 的「body 必须是子图」检查短路 | `TestEveryTransformTypeEnforcesExpressionXorBody/*/body_that_is_not_a_sub-graph` |
| 从 `bannedBodyMemberTypes` 去掉 `xflow.map` | `TestReservedTypesAreRejectedAsBodyMembers/xflow.map` |

`TestSubgraphBodyIsProjectedAndSurvivesSnapshot` 是正对照——没有它，一个恒 `false`
的判据能让上面每一条「不该投影」的断言全绿。
`TestEveryTransformTypeEnforcesExpressionXorBody` 对集合里的**每个**类型跑一遍，
将来加 filter / reduce / sort 时它们各自的「二选一」自动被覆盖。

### TS 侧 `experimental_expand?` 声明滞后（原 P2-8，2026-08-06 修复）

Go 侧编译门控已在 `52cd7c4` 移除，`web/packages/xflow-core/src/index.ts:44` 的
`WorkflowOptions.experimental_expand?` 是唯一残留声明（`dist/` 为构建产物，重新
构建即消失）。已删除。无运行时影响。

### `engine/graph/dependency.go` 的 `_ = supplyIdx`（原 P2-9，2026-08-06 修复）

已验证确为纯装饰而非漏掉的校验：`compile.go:285` 在构造 `depPorts` 之前就强制了
`Kind == NodeKindSupply`，所以 `buildDependencyEdges` 里那次查找只需存在性。
改为丢弃返回值并加注释说明「为何此分支无 Kind 校验而下方旧式分支有」——旧式形态的
supply 名直接来自定义、未经校验，两者不对称是有理由的。

### `cmd/runner/run.go` 从未装配 `GroupRuntime`（原 P0-1，2026-08-06 修复）

**本文件先前记录的故障机制是错的，实测后更正。** 原记录说「生产 runner 二进制收到
group lease 必然失败」，理由是 `runner.go:359` 的 `r.config.GroupRuntime != nil`
永远为假。实测（`service/control` 内的一次性探针）表明**根本走不到那一行**：

- group 单元的路由要求含 `{NodeType: "xflow.group", Feature: "group.exec.v1"}`
  （`engine.RequirementsFromGraphPackage`）。
- `parseCapabilities` 只能产出 `{NodeType: <名字>}`——`--cap` 没有任何语法能声明
  feature。于是 `MatchCapabilities` 恒为 false。
- 两个 runner 目录（memory / redis）在 claim 时用的都是 `MatchCapabilities`。

真实故障形态因此是**静默饥饿**而非失败：group 任务分配不到任何生产 runner，
永远排队，两侧都不打日志。比原记录的形态更难排查。

另有一个更阴的形态：运维凭直觉加 `--cap xflow.group` 会造成两条路径分歧——
`canRunRouting`（不看 Features）返回 true，`MatchCapabilities`（看）返回 false。
命令行上看起来已经配对，任务照样分配不出去。

修法：`cmd/runner` 无条件装配 `GroupRuntime` 并在能力表中补全
`{xflow.group, group.exec.v1}`；运维已声明 `xflow.group` 时**补全其 Features 而非
让位**（让位会保留恰好失败的那个形态，且重复条目会在
`hasCapabilityForRequirement` 的首个 NodeType 匹配处遮蔽真条目）。

无条件自报不会吸引跑不动的任务：group 的 requirements 同时列出每个成员节点类型，
缺 handler 的 runner 仍被 `MatchCapabilities` 拒绝（已实测）。

零读取点的死字段 `runner.Config.EnableGroupExec` 一并删除——它注释里承诺的
"advertises group.exec.v1 capability" 从未实现，留着即是留一条假线索。
`GroupNodeType` 提为 `engine` 包常量，让握手两侧共享同一字面量。

### trigger-group 的 runner 侧本地执行不存在 + 唯一的 e2e 自己伪造 exits（原 P0-1/P0-2，2026-08-09 修复）

[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) §5 第 4 步写的
"Each batch triggers local group execution (embedded engine, same as normal
group)" 此前没有实现：`seedKafkaEntryBatchMessages` 把原始 Kafka 消息直接构造
成 exits 提交控制面，成员节点一个都不跑；`service/runner/trigger_activation_handler.go`
整个文件不认识 group，控制面为 trigger-group 派发的 directive 带
`NodeType: "xflow.group"`，该合成类型从无 handler 注册，必然 fail closed。

按 [2026-08-07 SAS 流量打标 spec](../superpowers/specs/2026-08-07-sas-traffic-tagging-runner-group-design.md) §3
与 [2026-08-09 trigger-group-local-execution 计划](../superpowers/plans/2026-08-09-trigger-group-local-execution.md)
补齐了三处接线：

| 缺口 | 位置 | 修法 |
|---|---|---|
| `ActivateDirective` 不携带 package | `service/protocol/activation.go` | 加 `Package *graph.SubgraphPackage` 字段 |
| 控制面丢弃已投影的包 | `service/control/entry_activation_manager.go` | 重新投影并挂到 directive 上，哈希以 `ProjectSubgraphPackage` 返回值为准 |
| runner 拿 `"xflow.group"` 查 trigger handler | `service/runner/trigger_activation_handler.go` | 新增 `activateGroup`：定位包内自身的 trigger 入口节点，用 `groupExecTriggerRuntime` 承接 `ExecuteGroup`，走 `GroupRuntime.ExecuteRequest` 真跑内层引擎 |

`cmd/runner/run.go` 的构造顺序缺陷（`runnerServiceConfig` 先建 handler 再建
`GroupRuntime`，导致 `WithGroupRuntime` 传空）在同一计划的 Task 8 里一并修掉:
现在先建 `GroupRuntime` 再传给 handler。

原 P0-2 记录的问题——唯一的 trigger-group e2e
（`test/integration/h_trigger_group_e2e_test.go`）手工构造 exits，只验证控制面
admission 语义，不验证成员节点是否被执行——同一计划的 Task 9 补了
`test/integration/j_trigger_group_local_execution_e2e_test.go`：
`TestTriggerGroupLocalExecution_RealMemberNodeRuns` 起真实的
apiserver+control plane+runner 三进程路径，用真实成员节点 handler
（`groupLocalMemberHandler`，非 mock）验证批次真的经
`ExecuteGroup`→内层引擎→成员节点 Execute→真实 exits→
`SeedExecutionFromEntry`→下游 fan-out 全程跑通，断言下游节点的输出里带着
成员节点自己盖的 `seen_by_member=true` 标记，而非任何手工构造的 exits。

范围之外：该 e2e 的 workflow 定义里没有 supply 节点，所以它不验证 group 成员的
`$supplies`。这**不代表** group 内的 supply 有问题——见下方「已澄清的误记」。

### `xflow.map` 不能作为 group 成员（原 P1-3，2026-08-10 修复）

原记录说「把 `xflow.map` 放进 `SubgraphPackage` 会在包校验阶段被拒，报
`handler not available: type=xflow.map version=1`」，并建议「加显式的编译期拒绝
比递归接线便宜得多」。**该建议未被采纳，实际走了递归接线那条路**（`20fa9c6`），
因为实测发现挡路的是两个各自独立、单独一个就足以让 group 挂住的缺陷，而非
一个能力表问题：

1. `execution/subgraph/subgraph.go` 的 `Executor.Execute` **每次调用现场组装**
   内层引擎的 option 列表，那份列表里没有 batch body executor。调用方侧的
   `WithBatchBodyExecutor`（sdk/xflow、`GroupRuntime`）永远到不了这次调用新建
   的引擎，成员 map 的批次必然撞上 `ErrNoBatchBodyExecutor`。修法是把同一个
   `Executor` 递归接回去——这是唯一可接线的位置——并转发 `req.Deadline`，
   使嵌套的逐项执行不会超出外层 group 自身的 deadline。
2. `compileTrusted`（`CompileProjectedPackage` 对投影出的 group 包所走的路径）
   是 `Compile` pass 列表的手写平行实现，已经漂移：它跑了 `buildEdges` 与
   `buildDependencyEdges`，却从不跑 `projectMapBodies`（该 pass 已于 2026-08-11
   随 P2-5 更名为 `projectNodeBodies`）。于是成员 map 编译干净
   通过但 `NodeMeta.Body == nil`，运行期才失败。

递归**不会**重新打开「嵌套 map 子执行树无界」那个 P2-5 在防的问题：map 自己的
body 里仍然不允许出现 `xflow.map`（`compile.go` 的 `bannedBodyMemberTypes` 在
编译期拒绝，与该 map 是否为 group 成员无关），所以这条接线启用的递归深度不会
超过「成员 map」本来就有的那一层。

覆盖：`execution/subgraph/map_in_group_test.go` 的
`TestExecutor_RunsAMapMemberBodyInsideAGroup`（运行期）与
`engine/graph/map_member_body_test.go` 的
`TestCompileProjectedPackage_ProjectsAMapMemberBody`（编译期）各钉一个缺陷。

## 已澄清的误记（2026-08-10 实测更正）

### group 成员的 `$supplies` 并不为空

本文件先前记录「内层 group 引擎不做 supply 注入，成员节点看到的 `$supplies`
为空」。**实测推翻。** 该结论是从「`j_trigger_group_local_execution_e2e_test.go`
里看不到 supply 内容」一般化而来，但那个 e2e 的 workflow 定义里**根本没有 supply
节点，也没有任何依赖边**——`Admit` 的 reqs 天然为空，内容从未被取过。这与
「注入链路断了」是两类问题，修法完全不同。

真实机制：`$supplies` 读的是**进程全局单例** `supply.Default`
（`exprx/exprx.go:105` 的 `BuildExprEnv`），不经执行上下文传递。
内层引擎（`execution/subgraph`）对 supply 确实零引用——但这恰恰意味着**不需要引用
即可工作**，因为消费方不是通过引擎传参读 supply，而是直接读全局单例，跟节点是不是
group 成员、跑在内层还是外层引擎无关。

链路（每一跳均已实测）：

| 跳 | 位置 |
|---|---|
| 成员的 supply 依赖进入 group 激活的 reqs | `service/control/entry_activation_manager.go:90` `SuppliesForEntryUnit`，对 `UnitGroup` 用 `GroupMetaAt(unitIdx).Members` 做 BFS seed |
| reqs 随 directive 下发 | `service/protocol/activation.go:52` `ActivateDirective.Supplies` |
| runner 据此 fetch 并 Apply 进全局单例 | `service/runner/supply_gate.go` `Admit`；`NewSupplyGate` 默认 `reg = supply.Default` |
| 成员求值时读到内容 | `exprx.go:117` |

`buildDependencyEdges` 在 `compile.go` 里排在 `compileGroups` **之前**，是针对父图
逐节点算的——group 成员与非 group 成员在这一步完全对称，这是链路成立的根因。

唯一会读到空的情形是 workflow 里**从未声明**指向该 supply 的依赖边。而这一情形对
表达式消费者是**编译期硬失败**，不是无声：

- 静态引用 `$supplies.rules` 却无依赖边 → `validateSupplyUsage`
  （`engine/graph/dependency.go:205`）报 "references $supplies.x but has no
  dependency edge to it"
- 动态引用 `$supplies[x]` → 同一处直接拒绝，因为名字无法在编译期解析
- 边已声明但内容取不到 + `require_ready: true` → 门控拒绝激活，而不是带着空规则跑

## 已知且接受的代价（不打算改）

### item 级 `_error` 可被 body 输出伪造

若 body 的终止节点自己产出名为 `_error` 的字段，该项在 `results` 数组中与一次真实
失败**结构上完全无法区分**。终审逐条验证过分层是对的：**批级** `_error` 由
`BatchResultForCommit` 自行计算，从不从 body 返回的 data map 拷贝，因此不可伪造；
可伪造的只有 item 级。

这是无 schema 系统里任何保留前缀约定的通病，不是框架自身记账的正确性缺陷。
已在 [DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) 的 `xflow.map` 一节写明警告。

### `ErrGroupLeaseAlreadyActive` 经 `recoverGroupLease` 后可能以
`ErrGroupLeaseNotActive` 重新传播

仅在 `BuildGroupLease` 报告 lease 存活时才可达，当前测试覆盖不到。先于本分支存在
（`8570bf4`）。group 重试落地时再处理。

### `buildVisibleSupplies` 自身不做授权检查

它依赖「`ProjectGroupPackage` 只在父图 `Compile()` 成功后才可达」这一不变量，而
`Compile()` 已跑过逐成员授权检查。C1 的修复为它加了第二个调用方
（`ProjectNodeBodyPackage`），该不变量对新调用方同样成立——**但仍然没有强制手段**。
将来若有调用方从未完全校验的图上做投影，会重新引入跨成员 supply 泄漏且无测试拦截。
