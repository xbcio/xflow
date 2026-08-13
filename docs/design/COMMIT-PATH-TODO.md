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

## 真正的分叉：`CommitNodeRequest.Fatal` 一字段两义

`...WithClassification` 那一对是唯一真有分歧的：

- **acyclic**：带 `AdvanceTask`，`Fatal` 是**终局信号**——置位则后端在同一事务里
  finalize execution。
- **cyclic**：`planCyclicDownstream` 算出 `CyclicOutbox`，`Fatal` **恒为 false**，
  终局改由 `CyclicComplete` + `CyclicFinalStatus` 承载。`Fatal` 在这一侧的含义变成
  「后端跳过 cyclic 下游」的守卫。

同一个字段两种含义。这是合并的核心障碍，也是将来给审批流加分支时最容易踩的地方。
`CommitNodeRequest` 是跨 `backend/providers/local` 与 `.../distributed/internal/rstate`
两个实现的契约，所以这个决定不是 engine 内部的。

## 待决问题（重构前必须先答）

1. **cyclic 图的终局该由 `Fatal` 承载，还是保持 `CyclicComplete`？**（**仍开**）决定
   合并往哪个方向收，且会动后端接口。
2. ~~**分布式 × cyclic 的实际成色未验证。**~~ **2026-08-13 已实测。** 两个集成测试在
   真 Redis（6380）下真跑真绿，无静默 skip：`TestCyclicReliabilityProcessRecovery`
   7.66s、`TestCyclicReliabilityRealRedis` 5.18s（5 个子测试）。**重构可以由这些测试
   兜底。** 遗留的性能观察不变：`rstate/state_commit.go:146-148` 每次提交都要
   `LoadGraph` 一次来判 `allowCycles`——无环侧不需要的额外读。
3. ~~**审批流用到的是 cyclic × suspend 的组合，不是 cyclic 本身。**~~
   **2026-08-13 已实测：缺口是真的，现已补上。** 见下节。

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

## 失败原因的读回面：只有 SQL 审计行（2026-08-13 实测）

与「`Fatal` 一字段两义」同源的一个可观测性缺口。cyclic 深度超限失败时，**触限的那个
节点是 success 的**（下游激活被 `engine/scheduler.go:43-45` 拒绝），所以没有任何失败
节点携带原因，`CyclicFinalError` 是唯一载体。

实测（`backend/providers/local`，`AllowCycles(true)` + `MaxAutoDepth: 2` 的自环图，
与无环 fatal 失败做正对照）：

| 观察点 | cyclic 深度超限 | 无环 fatal 失败（对照） |
|---|---|---|
| `types.Result.Error` | `""` | `""` |
| `ExecutionDetail.Error` | `""` | `""` |
| `ExecutionSnapshot` | 无 Error 字段 | 无 Error 字段 |
| 节点级 `NodeSnapshot.Error` | `start`/`loop` 均 success、均 `""` | `boom`: failed、`"boom from the business node"` |

即：**执行级的原因读回在两种情况下都缺**（`Result`/`Inspect` 全域问题，非 cyclic 特有），
而 cyclic 深度超限因为没有失败节点，原因**彻底丢失**。

Redis 侧同样：`commitNodeLua` 会把 `CyclicFinalError` 写进 `execKey(..,"error")`，但
全仓只有写者没有读者——`GetExecution` 只加载 status/params/runtime/scope/trace，
`engine.ExecutionSnapshot` 根本没有 `Error` 字段。

**2026-08-13 已部分关闭**：三处 Lua 内终态转换现在都投影到 SQL（见
[STORAGE-CONTRACT.md](./STORAGE-CONTRACT.md) 的「Terminal transitions inside a Lua
script」），且 `terminalExecutionError` 让 `CyclicFinalError` 优先于节点错误。所以
**SQL 审计行 `executions.error_msg` 是目前唯一能读回该原因的地方**，回归测试见
`rstate/sql_execution_projection_test.go:TestCommitLeasedNodeProjectsCyclicFinalErrorToSQL`。

**仍开**：在线读回面（`ExecutionSnapshot.Error` 字段、`GetExecution` 读 error 键、
`Inspect` 在执行级赋 `detail.Error`、local 的 `finishExecutionLocked` 收 errMsg 参数）
四处都没做。这是一次跨 engine + 两个后端的接口改动，与上面问题 1 的答案耦合，未单独
立项。

## 可能的收敛形状（未决，仅备忘）

三层：入口合一，扩展分支用 `taskResultExpands` 分流（判据已在 2026-08-11 下沉为编译期
可答，前提具备）；错误分支删掉零差异的那一份；`...WithClassification` 保留两个实现，
因为 `Fatal` 语义确实分叉。

这只是一个候选，问题 1 的答案可能推翻它。
