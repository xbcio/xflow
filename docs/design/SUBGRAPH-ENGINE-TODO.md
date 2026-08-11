# 子图引擎与 map body 遗留项

`feat/subgraph-engine-and-map-body`（30 commits，2026-08-04 ~ 08-06）交付后未做的事。
按「不做会怎样」排序，不按工作量。

背景见 [DSL-SPECIFICATION.md §xflow.map](./DSL-SPECIFICATION.md)（body 语法与结果结构）
与 [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)（group 侧的同族机制）。
本文件只列**待办**。

## P0 — 用之前必须修

## P1 — 规模上去会疼

### 4. 无 `max_concurrency` 节流

设计时显式排除（见 spec §10.1），当时 body 还是 pass-through stub。**T12/T13 之后
风险画像变了**：一个很大的 `items` 数组现在会把「全部批次一次性灌进队列」变成
「全部批次一次性对下游发起真实 I/O」（HTTP 调用、脚本执行）。爆炸半径从队列深度
升级为对下游系统的并发外呼。

无测试、无告警。合并时无证据表明造成过真实事故。

## P2 — 命名与死代码

### 5. 给第二种节点类型加 body 时要放宽三处 map 专属判断

body 存储已经是通用的：`NodeMeta.Body` 随节点整体走 wire 与 hash，fail-closed
守卫只看 `Parameters["body"]` 是否存在、不看节点类型，执行器拿到的
`NodeBodyPackage` 也与投影者无关。**新增一种带 body 的节点类型不需要碰 wire、
hash、序列化或反序列化。**

仍然写死 `xflow.map` 的只有三处，都在编译期：

- `projectMapBodies`（`engine/graph/compile.go`）的 `nd.Type != "xflow.map"` 判断
- `validateMapBody`（同文件）的形态校验
- `bannedBodyMemberTypes`：禁止 body 内再嵌 map/split/subgraph，防止子执行树无界

transform 类节点（filter、reduce）在逻辑超出单个表达式时天然会需要 body——
规范当前写的 `condition: expression` 只是最简形态，不是上限。届时是「在一个 pass
里加 case」，不是重新接一遍线。

### 6. `xflow.map` 仍然对外自称 `"_loop"`

`node/internal/flow/map.go:89`、`engine/expand.go:17` 及 5 个测试文件里的标记键仍是 `_loop`。
设计文档（§「xflow.map 改名后标记键跟着改叫 _map」）把改名派给了扩展工作，实际没做。

纯命名不对称，无功能后果。**修它要动 wire / 持久化状态格式**——这是它没在本分支
修掉的原因，也是越晚修越贵的原因。

### 7. `types/transform.go` 的 `TransformSpec` 尚无消费者（保留）

T11 声明它，本打算由 T12 消费，T12 没有消费。**明确保留不删**：它描述的
`{expression | body}` 二选一形态正是 filter/reduce 落地时要用的，删掉等于丢掉一份
已写好的设计意图。第 5 条放宽三处 map 专属判断时一并消费它。

### 8. `engine/graph/subgraph_package.go` 的 `ProjectSubgraphPackage` 名字有歧义

它投影的是 **group** 包（入参是 `unitIdx`，断言 `Kind == UnitGroup`），与
`xflow.subgraph` 这个节点类型无关——后者的投影入口是 `ProjectNodeBodyPackage`。

`xflow.subgraph` 是作者手写的 body 容器节点类型；`xflow.group` 是编译器从顶层
`groups:` 生成的调度单元对外自称的合成路由类型，没有任何 handler 注册它。两者最终
汇合在同一个 `execution/subgraph.Executor`（该执行器分不出 group 和 map body），
但来源与用途不同。改名会动到公开 API，未做。

## 已修复

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
   `buildDependencyEdges`，却从不跑 `projectMapBodies`。于是成员 map 编译干净
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
（`node/internal/utils/exprx/exprx.go:117` 的 `BuildExprEnv`），不经执行上下文传递。
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
