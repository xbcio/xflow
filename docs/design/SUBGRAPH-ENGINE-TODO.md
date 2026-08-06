# 子图引擎与 map body 遗留项

`feat/subgraph-engine-and-map-body`（30 commits，2026-08-04 ~ 08-06）交付后未做的事。
按「不做会怎样」排序，不按工作量。

背景见 [DSL-SPECIFICATION.md §xflow.map](./DSL-SPECIFICATION.md)（body 语法与结果结构）
与 [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)（group 侧的同族机制）。
本文件只列**待办**。

## P0 — 用之前必须修

（本节当前为空。原第 1 条已修复，见下方「已修复」。）

## P1 — 规模上去会疼

### 2. 无 `max_concurrency` 节流

设计时显式排除（见 spec §10.1），当时 body 还是 pass-through stub。**T12/T13 之后
风险画像变了**：一个很大的 `items` 数组现在会把「全部批次一次性灌进队列」变成
「全部批次一次性对下游发起真实 I/O」（HTTP 调用、脚本执行）。爆炸半径从队列深度
升级为对下游系统的并发外呼。

无测试、无告警。合并时无证据表明造成过真实事故。

## P2 — 命名与死代码

### 3. 给第二种节点类型加 body 时要放宽三处 map 专属判断

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

### 4. `xflow.map` 仍然对外自称 `"_loop"`

`node/internal/flow/map.go:89`、`engine/expand.go:17` 及 5 个测试文件里的标记键仍是 `_loop`。
设计文档（§「xflow.map 改名后标记键跟着改叫 _map」）把改名派给了扩展工作，实际没做。

纯命名不对称，无功能后果。**修它要动 wire / 持久化状态格式**——这是它没在本分支
修掉的原因，也是越晚修越贵的原因。

### 5. `types/transform.go` 的 `TransformSpec` 尚无消费者（保留）

T11 声明它，本打算由 T12 消费，T12 没有消费。**明确保留不删**：它描述的
`{expression | body}` 二选一形态正是 filter/reduce 落地时要用的，删掉等于丢掉一份
已写好的设计意图。第 3 条放宽三处 map 专属判断时一并消费它。

### 6. `engine/graph/subgraph_package.go` 的 `ProjectSubgraphPackage` 名字有歧义

它投影的是 **group** 包（入参是 `unitIdx`，断言 `Kind == UnitGroup`），与
`xflow.subgraph` 这个节点类型无关——后者的投影入口是 `ProjectNodeBodyPackage`。

`xflow.subgraph` 是作者手写的 body 容器节点类型；`xflow.group` 是编译器从顶层
`groups:` 生成的调度单元对外自称的合成路由类型，没有任何 handler 注册它。两者最终
汇合在同一个 `execution/subgraph.Executor`（该执行器分不出 group 和 map body），
但来源与用途不同。改名会动到公开 API，未做。

## 已修复

### TS 侧 `experimental_expand?` 声明滞后（原 P2-6，2026-08-06 修复）

Go 侧编译门控已在 `52cd7c4` 移除，`web/packages/xflow-core/src/index.ts:44` 的
`WorkflowOptions.experimental_expand?` 是唯一残留声明（`dist/` 为构建产物，重新
构建即消失）。已删除。无运行时影响。

### `engine/graph/dependency.go` 的 `_ = supplyIdx`（原 P2-7，2026-08-06 修复）

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
