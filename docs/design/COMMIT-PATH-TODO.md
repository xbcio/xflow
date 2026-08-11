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

1. **cyclic 图的终局该由 `Fatal` 承载，还是保持 `CyclicComplete`？** 决定合并往哪个
   方向收，且会动后端接口。
2. **分布式 × cyclic 的实际成色未验证。** 已知 `rstate/state_commit.go:146-148` 每次
   提交都要 `LoadGraph` 一次来判 `allowCycles`——无环侧不需要的额外读。而
   `rstate/cyclic_commit_test.go`、`test/integration/cyclic_reliability_{process,real}_test.go`、
   `g1_production_e2e_test.go` 覆盖到哪一步尚未查。**先重构就意味着正确性由这些测试
   兜底**，动手前必须先跑一遍（Redis 在 6380）确认哪些真跑了、哪些静默 skip。
3. **审批流用到的是 cyclic × suspend 的组合，不是 cyclic 本身**——返工环里挂着待审批
   节点。suspend 已从两条路径拆出，这个组合在分布式下有没有被测到，比重构本身更值得
   先确认。

## 现有 cyclic 覆盖（2026-08-11 清点，未验证其中多少真跑）

- `sdk/examples/cyclic_vulnerability_approval_test.go` — 恰好就是漏洞审批返工环：
  `AllowCycles(20)`、多方审批（security-lead / app-owner / change-manager / sre-owner）、
  驳回后重来。走 `xflow.NewLocal`，**不是分布式**。
- `backend/providers/distributed/internal/rstate/cyclic_commit_test.go`
- `test/integration/cyclic_reliability_process_test.go`
- `test/integration/cyclic_reliability_real_test.go`
- `test/integration/g1_production_e2e_test.go`

## 可能的收敛形状（未决，仅备忘）

三层：入口合一，扩展分支用 `taskResultExpands` 分流（判据已在 2026-08-11 下沉为编译期
可答，前提具备）；错误分支删掉零差异的那一份；`...WithClassification` 保留两个实现，
因为 `Fatal` 语义确实分叉。

这只是一个候选，问题 1 的答案可能推翻它。
