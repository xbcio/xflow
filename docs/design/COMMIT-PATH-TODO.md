# 提交路径遗留项：acyclic 与 legacy 两条路的重复与分叉

2026-08-11 在移除扩展标记键（见 [SUBGRAPH-ENGINE-TODO.md](./SUBGRAPH-ENGINE-TODO.md)）时
顺带查实的结构问题。**未修**，本文件只记录事实与待决问题。

触发它变重要的外部事件：**漏洞审批流将对接 cyclic 模式，且走分布式部署**。在此之前
cyclic 是有测试无生产流量的路径；对接之后它承重。

## 由来

`5125549`（2026-07-16，"add atomic commit, outbox, and observers"）引入原子提交 +
outbox，把提交路径一分为二：新的 `commitAcyclicTaskResult` 让终态写入与下游投递意图
落在一个受栅栏的事务里；旧路径原封不动改名为 `commitLegacyTaskResult`。

分流条件当时是「无环 且 非 suspend 且 非扩展」。三类东西没迁移：cyclic 图、suspend、
loop/split 扩展。suspend 后来自己拆了出去（`commitSuspendedTaskResult`），所以留在
legacy 上的只剩 cyclic 与扩展。

名字里的 "legacy" 因此是**字面意义**的（老实现的残骸），但今天已经误导：这条路径
承载的是扩展这个活跃功能，真正的 legacy 只有 cyclic 那一支。

## 实测的重复量

两条路径逐字 diff（2026-08-11，除函数名外）：

| 函数对 | 差异 |
|---|---|
| `commitAcyclicNodeError` / `commitLegacyNodeError` | **逐字相同，零差异** |
| `commitAcyclicTaskResult` / `commitLegacyTaskResult` | 仅扩展分支不同（一边 claim + `expandLoopSplit`，一边报错做 backstop） |
| `commitAcyclicNodeWithClassification` / `commitLegacyNodeWithClassification` | 真的不同，见下 |

而且结构本身在自证重复：`commitLegacyNodeWithClassification`（`engine/commit.go:189`）
第一行就是

```go
if !g.AllowCycles() {
    return e.commitAcyclicNode(...)   // 折回新路径
}
```

即**走进 legacy 的无环图，提交那一步又回到 acyclic**。所谓两条路，实际是「一条主路 +
一个 cyclic 分叉」，外面却各包了一套完整且逐字重复的前置逻辑。

这个折回还解释了一个观察到的现象：把 `taskResultExpands` 的「只有成功才扩展」收窄
摘掉，全仓一条测试都不红——失败改走 legacy 后跑的是同一套重试与 OnError，提交时又
折回 acyclic，两条路在失败上收敛。收敛是这个结构的产物，不是设计意图。

## ~~真正的分叉：`CommitNodeRequest.Fatal` 一字段两义~~ ✓ 2026-08-14 已关闭

`...WithClassification` 那一对是唯一真有分歧的：

- **acyclic**：带 `AdvanceTask`，`Fatal` 是**终局信号**——置位则后端在同一事务里
  finalize execution。
- **cyclic**：`planCyclicDownstream` 算出 `CyclicOutbox`，`Fatal` **恒为 false**，
  终局改由 `CyclicComplete` + `CyclicFinalStatus` 承载。`Fatal` 在这一侧的含义变成
  「后端跳过 cyclic 下游」的守卫。

第二义（跳过 cyclic 下游的守卫）之所以存在，是因为**后端不知道图的类型**，只能拿
`Fatal` 当代理信号。`CommitNodeRequest` 现在带 `AllowCycles`，两个协议由它选择，
`Fatal` 因此只剩第一义（无环终局），并已在 `engine/atomic.go` 上写明。

`CommitNodeRequest.Validate()` 交叉校验四组字段（`Fatal`/`AdvanceTask` 属无环侧，
`CyclicOutbox`/`CyclicComplete` 属有环侧），使 bool 的零值不能悄悄把有环提交退回
无环协议——那正是下面第 2 条修掉的缺陷形态。两个后端在 `CommitNode` 入口各调一次。

## 待决问题（重构前必须先答）

1. ~~**cyclic 图的终局该由 `Fatal` 承载，还是保持 `CyclicComplete`？**~~
   **2026-08-14 已答：保持 `CyclicComplete`，并把图类型显式放进请求。** 两个协议不共享
   任何状态（无环靠 `remaining` 计数器，有环靠 `CyclicOutbox`/`CyclicComplete`），合并
   到一个字段上只会让「哪一半在生效」再次取决于后端猜测。
2. ~~**分布式 × cyclic 的实际成色未验证。**~~ **2026-08-13 已实测。** 两个集成测试在
   真 Redis（6380）下真跑真绿，无静默 skip：`TestCyclicReliabilityProcessRecovery`
   7.66s、`TestCyclicReliabilityRealRedis` 5.18s（5 个子测试）。**重构可以由这些测试
   兜底。** 遗留的那条「每次提交都 `LoadGraph` 判 `allowCycles`」的观察，**性能定性
   写错了**（`LoadGraph` 先查 `s.graphs` 内存缓存，命中时只是一次 RLock，不是 Redis
   往返），但它藏着一个真缺陷，已于 2026-08-14 修掉，见下节。
3. ~~**审批流用到的是 cyclic × suspend 的组合，不是 cyclic 本身。**~~
   **2026-08-13 已实测：缺口是真的，现已补上。** 见下节。

## 后端猜图类型：图不可用时静默丢下游（2026-08-14 实测并修复）

原代码两个后端各自重新推导图类型，都会在图拿不到时**默默落到「无环」**：

```go
// rstate/state_commit.go，修复前
allowCycles := 0
if g, err := s.LoadGraph(ctx, req.ExecutionID); err == nil && g != nil && g.AllowCycles() {
    allowCycles = 1
}
```

任何失败——Redis 报错、反序列化失败、key 不存在——都被 `err == nil` 吞掉判成无环。
local 侧同形：`entry.snap.Graph != nil && !entry.snap.Graph.AllowCycles()`，快照没带
图时两个分支**双双落空**（既不计数也不落 outbox）。

图确实会拿不到：`exec:<id>:graph` 带执行的 TTL，而内存缓存不跨进程重启。一条长命的
有环执行（**返工审批环挂在信号上等几天**正是这个形状）重启后提交，就落进这个窗口。

后果两条，都是静默且不可恢复的：

- `CyclicOutbox` 里的下游投递意图**被丢弃**——Lua 里持久化它们的分支由
  `ARGV[16]`（allowCycles）门控。没有任何东西会重新推导一份丢掉的投递意图。
- 走了无环分支，去 `DECR` 一个**从未播种**的完成计数器。`remaining`/`failed` 只在无环
  图上播种（local `memory_state.go:117-120`；rstate `state_execution.go:151` 与
  `entry_admission.go:162`），Redis 于是把它建成 -1 → `remaining <= 0` → **误判执行完成**。

修法：`engine` 本来就握着图（`commit.go:53`/`:80`/`:189` 三处路由都在读
`g.AllowCycles()`），把它直接放进请求，后端不再推导。承重测试各后端一条，均已反向复验
（改回旧推导即红，失败形态正是「OutboxIDs 为空」）：

- `rstate/commit_graph_type_test.go:TestCommitLeasedNodeDoesNotGuessTheGraphType`
  ——把图从**内存缓存与 Redis 双双删除**后提交，仍要求 cyclic 意图落库。
- `local/commit_graph_type_test.go:TestMemoryCommitNodeUsesTheRequestGraphType`
  ——用**不带 Graph 的快照**驱动，即原先让两个分支同时落空的那个形状。

顺带对齐了 group 提交：rstate 早就硬编码 `allowCycles = 0`（groups 与 AllowCycles
由 `validateGroupsAllowCyclesExclusion` 编译期互斥），local 却在读快照的图——同一个
「快照无图则不计数」的洞。现改为无条件走无环协议，与 rstate 一致。


## cyclic × suspend：曾经零覆盖（2026-08-13 实测并关闭）

这个组合在本次之前**分布式下一条测试都没有**：

- 所有分布式 cyclic 用例（`g1RunCyclicReset`、`cyclic_reliability_*`）驱动的图**没有
  suspend 节点**。
- 所有审批/等待用例（`g1RunApprovalMultiSignal`、`g1RunApprovalTimer`）驱动的图
  **无环**。
- 唯一同时具备两种形状的样例 `sdk/examples/cyclic_vulnerability_approval_test.go`
  走 `xflow.NewLocal`，**在进程内**。

缺口不是理论上的。反向探针实测（把 `rstate/state_lua.go` 里 `stampActivation` 改成
直接返回原 body，使 resume outbox 条目不再带活的 `activation_id`）：

| 用例 | 注入下的表现 |
|---|---|
| 新增 `TestCyclicWithSuspendDistributed` | **红** — 执行卡在 running，24.19s 超时 |
| 既有 `TestG1ProductionE2E/ApprovalMultiSignal` | 绿（4.038s）——抓不到 |
| 既有单测 `TestRedisDeliverSignalWithOutboxStampsLiveActivation` | 红 |

即：单测层有钉子，**分布式 e2e 层没有**。只有「环里的 suspend」才会走到这段 stamping
——只有那里，resume 任务的 activation 必须匹配一个 `activation_id` 会递增的节点
（`engine/lease.go:243-256` 的 `classifyNodeForTask`：`AllowCycles() &&
t.ActivationID <= 0` 直接判 `ErrExecutionInactive`）。

已补 `test/integration/cyclic_suspend_distributed_test.go`：`start → review`，
`review --reject→ wait(signal) --main→ start`（返工环里挂审批），`review --main→ end`。
除终态成功外还断言 `review` 被调用 ≥2 次——否则「收到信号就直接结束、从不回环」的实现
也能过。

## 现有 cyclic 覆盖（2026-08-13 复核）

- `sdk/examples/cyclic_vulnerability_approval_test.go` — 恰好就是漏洞审批返工环：
  `AllowCycles(20)`、多方审批（security-lead / app-owner / change-manager / sre-owner）、
  驳回后重来。走 `xflow.NewLocal`，**不是分布式**。
- `backend/providers/distributed/internal/rstate/cyclic_commit_test.go`
- `test/integration/cyclic_reliability_process_test.go` — 实测真跑，7.66s
- `test/integration/cyclic_reliability_real_test.go` — 实测真跑，5.18s
- `test/integration/g1_production_e2e_test.go` — cyclic 侧只有 `g1RunCyclicReset`，无 suspend
- `test/integration/cyclic_suspend_distributed_test.go` — 本次新增，唯一覆盖
  cyclic × suspend 的分布式用例

## ~~失败原因的读回面：只有 SQL 审计行~~ ✓ 2026-08-14 已关闭（本节按时间顺序记录）

与「`Fatal` 一字段两义」同源的一个可观测性缺口。cyclic 深度超限失败时，**触限的那个
节点是 success 的**（下游激活被 `engine/scheduler.go:43-45` 拒绝），所以没有任何失败
节点携带原因，`CyclicFinalError` 是唯一载体。

实测（`backend/providers/local`，`AllowCycles(true)` + `MaxAutoDepth: 2` 的自环图，
与无环 fatal 失败做正对照）。**下表是 2026-08-13 修复前的观测，已全部作废**，保留是为了
记录缺口的形状；当前行为见本节末尾的「已完全关闭」：

| 观察点 | cyclic 深度超限 | 无环 fatal 失败（对照） |
|---|---|---|
| `types.Result.Error` | `""` | `""` |
| `ExecutionDetail.Error` | `""` | `""` |
| `ExecutionSnapshot` | 无 Error 字段 | 无 Error 字段 |
| 节点级 `NodeSnapshot.Error` | `start`/`loop` 均 success、均 `""` | `boom`: failed、`"boom from the business node"` |

即：**执行级的原因读回在两种情况下都缺**（`Result`/`Inspect` 全域问题，非 cyclic 特有），
而 cyclic 深度超限因为没有失败节点，原因**彻底丢失**。

Redis 侧同样：`commitNodeLua` 会把 `CyclicFinalError` 写进 `execKey(..,"error")`，但
当时只有写者没有读者——`GetExecution` 只加载 status/params/runtime/scope/trace，
`engine.ExecutionSnapshot` 根本没有 `Error` 字段。

**2026-08-13 已部分关闭**：三处 Lua 内终态转换现在都投影到 SQL（见
[STORAGE-CONTRACT.md](./STORAGE-CONTRACT.md) 的「Terminal transitions inside a Lua
script」），且 `terminalExecutionError` 让 `CyclicFinalError` 优先于节点错误。所以
**SQL 审计行 `executions.error_msg` 是目前唯一能读回该原因的地方**，回归测试见
`rstate/sql_execution_projection_test.go:TestCommitLeasedNodeProjectsCyclicFinalErrorToSQL`。

**2026-08-14 已完全关闭**：在线读回面四处全部接上，两个后端各自镜像自己对应的 Lua 规则。

- `engine.ExecutionSnapshot.Error` 新增字段；`Inspect` 在执行级赋 `detail.Error`。
- `types.Result.Error` 两条读回路径都通：SDK 的 `resultFromDetail` 经 `Inspect` 透传（分布式
  走这条），local 的 `Backend.WaitDone` 直读快照（**绕过 `Inspect`**，需单独接）。只接一边会
  造成「内嵌模式看得见、分布式看不见」或反之。
- `rstate.GetExecution` 读回 `execKey(..,"error")`（不存在是常态，容忍 `redis.Nil`）。
- local 的 `finishExecutionLocked` 收 errMsg 参数，四处调用点（`atomic_state.go` 两处、
  `group_state.go`、`entry_admission.go`）各自传入。
- 新增 `engine.TerminalExecutionError(status, nodeErr, cyclicErr)`：非 failed 一律为空；
  cyclic 错误优先于节点错误，因为深度超限时**没有失败节点**（`engine/scheduler.go:43-45`
  拒绝的是下游激活，触发限制的节点本身是 success）。

**两条 Lua 规则不同，local 必须分别对齐，不能统一**：

| Lua | 条件 | local 对应处 |
|---|---|---|
| `updateExecutionStatusLua`（`state_lua.go:276`） | `ARGV[2] ~= ''`，**不看 status**，从不删除 | `memory_state.go` 的 `UpdateExecutionStatus`：`if errMsg != ""` |
| `commitGroupLua`（`group_state.go:146`） | `finalStatus == 'failed' and ARGV[8] ~= ''` 双条件 | `finishExecutionLocked`：`status == failed && errMsg != ""` |

把前者也加上 status 门会让 local 清掉 Redis 保留的原因——这个分歧曾被写进代码，靠反向
探针**返回绿**才暴露出来（探针无区分力 = 规则本身写错了）。

曾有第三条路径 `CancelSuspendedGroup`：取消动作直接终结整个 group unit，没有任何成员节点
提交失败，所以连调用方都拿不到原因，两个后端为此共用常量 `engine.CanceledSuspendedGroupError`，
`cancelSuspendedGroupLua` 也为此加了第 6 个 KEY 和第 2 个 ARGV。durable group suspend 子系统
整体移除后这条路径不复存在（详见 `NODE-GROUP-COLOCATION.md` §6），常量与 Lua 一并删除。

回归覆盖落在共享契约里（唯一同时约束两个后端的地方），且都带真 Redis 运行器：
`statestoretest.runExecutionErrorRoundTrip`（`UpdateExecutionStatus` 路径，含成功不留原因的
反面半边）与 `RunGroupStateContract` 的 `FatalGroupCommitStoresExecutionError` /
`SuccessfulGroupCommitLeavesExecutionErrorEmpty`（commit 路径，`finishExecutionLocked` 实际
所在处）。`TestRedisStateStoreContract` 此前只跑 miniredis，本次补了真 Redis 运行器——
断言落在 Lua 内部，而 miniredis 的 Lua 不是 Redis 的 Lua。

端到端一例：`local/cyclic_depth_error_readback_test.go` 真跑一次深度超限，同时断言
`WaitDone` 与 `Inspect` 两条路径给出**相同**原因，并反向断言没有任何节点携带 error
（那正是本场景成立的前提）。

## 可能的收敛形状（未决，仅备忘）

三层：入口合一，扩展分支用 `taskResultExpands` 分流（判据已在 2026-08-11 下沉为编译期
可答，前提具备）；错误分支删掉零差异的那一份；`...WithClassification` 保留两个实现，
因为 `Fatal` 语义确实分叉。

问题 1 已于 2026-08-14 答出且**没有推翻这个形状**：`Fatal` 的第二义消失后，
`...WithClassification` 那一对的分歧收窄成「两个完成协议各一套字段」，`Validate` 把
边界钉住了。也就是说这个候选现在可以直接做，不再被待决问题挡住。
