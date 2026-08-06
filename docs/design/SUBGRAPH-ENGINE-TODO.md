# 子图引擎与 map body 遗留项

`feat/subgraph-engine-and-map-body`（30 commits，2026-08-04 ~ 08-06）交付后未做的事。
按「不做会怎样」排序，不按工作量。

背景见 [DSL-SPECIFICATION.md §xflow.map](./DSL-SPECIFICATION.md)（body 语法与结果结构）
与 [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)（group 侧的同族机制）。
本文件只列**待办**。

## P0 — 用之前必须修

### 1. `cmd/runner/run.go` 从未装配 `GroupRuntime`

**这条先于本分支存在，本分支不负责，但现在是唯一已知的同类缺口。**

本分支的终审在 T14 抓到 `SubgraphRuntime` 在生产 runner 二进制里没接线（e2e 自己
搭了一个运行时，生产二进制从来没有），已于本分支修复。**并列的 group 侧原样未动**：

- `cmd/runner/run.go` 只设 `serviceCfg.SubgraphRuntime`，从没设过 `GroupRuntime`
  或 `EnableGroupExec`。`git log -S EnableGroupExec` 显示该字段唯一一次改动是
  `89a3bb0`，早于本分支 base。
- `runner.Config.EnableGroupExec` **全仓库零读取点**（只有
  `test/integration/group_binary_e2e_test.go:81` 写过一次）。它注释里承诺的
  "advertises group.exec.v1 capability" 从来没有实现过。
- `runner.go:359` 的分发条件是 `lease.GroupPayload != nil && r.config.GroupRuntime != nil`。
  生产二进制里后半永远为 nil，于是每一个 group lease 都静默走不到 group 分支。

后果：**生产 runner 二进制收到 group lease 必然失败**。group 执行在生产中被启用
之前必须修掉。集成测试看不到，因为 `group_binary_e2e_test.go` 自己组装配置。

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

### 5. `types/transform.go` 的 `TransformSpec` 零消费者

T11 声明它，本打算由 T12 消费，T12 没有消费。当前是分支上的死代码。

### 6. `web/packages/xflow-core/src/index.ts:44` 仍声明 `experimental_expand?`

Go 侧的编译门控已在 `52cd7c4` 移除。TS 声明滞后，无运行时影响。

### 7. `engine/graph/dependency.go` 的 `_ = supplyIdx`

从 brief 的伪代码里带进来的空语句。纯装饰。

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
