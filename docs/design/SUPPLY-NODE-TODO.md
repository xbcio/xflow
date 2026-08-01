# SUPPLY-NODE 遗留项

P3（`feat/supply-node-p3`，20 tasks，已合入 main）交付后未做的事。按「不做会怎样」排序，不按工作量。

背景与设计取舍见 [SUPPLY-NODE.md](./SUPPLY-NODE.md)；本文件只列**待办**。§9 是「知道且接受的代价」，这里是「打算改的东西」——
一条缺口同时出现在两处不矛盾：§9 说明现状为何可接受，这里说明何时不再可接受。

## P0 — 合并前已知、但影响面最大

### 1. 门控拒绝的 activation 不自愈

见 [SUPPLY-NODE.md §9(a)](./SUPPLY-NODE.md#9-known-gaps-and-costs)。

`EntryActivationReconciler.reconcileOne` 的 already-assigned 分支只看
`!expired && ownerMatches`，从不检查该 runner 的 `Activate`（进而
`SupplyGate.Admit`）是否真的成功过，因此永远续租、永不重试。今天唯一的恢复
路径是**重启 runner**。

危险的地方不在行为本身（拒绝并等待是安全方向），而在于它**推翻了一个很自然的
直觉前提**：「下一轮 reconcile 会重试」。任何基于该前提做的运维预案或后续设计
都是错的。

- 已定义但零接线：`protocol.ActivationAck` / `ActivationAckPath`
  （`service/protocol/activation.go:9,71-79`），全仓无 `service/control`、
  `service/runner` 或路由注册处的生产调用点。
- 两条路可选：接线 `ActivationAck`，或让 reconciler 感知 admit 结果。
- 现状已被测试钉死，改动会让它们失败，这是**预期**，不是回归：
  `test/integration/supply_gating_test.go` 的 `TestSupplyGateLosesNoMessages`
  断言拒绝期间 `Generation` 不前进；`TestSupplyGateRecoversOnRestart` 单独隔离
  同一断言。修复时必须同步改这两处断言，并在 §9(a) 记录。

## P1 — 可观测性缺失，出事时会瞎

### 2. runner 侧 supply metrics 无生产落地槽

`cmd/runner/run.go` 里没有 `*metrics.Metrics` 实例，因此 runner 侧那几个
supply metric 在生产中无处上报。单测里有 observer 打点，**不代表生产能看到**。

后果：第 1 项那个「拒绝并静默等待」的状态，生产环境目前**没有任何指标能直接
看出来**，只能靠 Kafka consumer-group lag 间接推断。这两条叠加起来比各自单独
更糟——先修哪条都行，但不该只修 1 不修 2。

### 3. gRPC 传输不携带 hint 与 activation

见 [SUPPLY-NODE.md §9(b)](./SUPPLY-NODE.md#9-known-gaps-and-costs) 与
[DEPLOYMENT-TOPOLOGIES.md §4.5](./DEPLOYMENT-TOPOLOGIES.md#45-传输差异gRPC-心跳不携带控制载荷)。

`runnerpb.HeartbeatResponse` 只有 `server_time` 一个字段。**这个缺口先于本分支
存在**（gRPC 连 activation directive 都不带），不是 supply 引入的。

对 supply 而言只是延迟退化，不是正确性问题：丢 hint 只会让内容刷新延迟从「一个
心跳周期」退化为「一个 TTL 轮询周期」，永远不会退化为「永不刷新」。TTL 轮询才是
正确性保证，hint 只是优化。

## P2 — 17 条 deferred minor

最终评审逐条判定**无一需在合并前修**，并自查了风险最高的两条：

- Task 6 `DependsOn` 不去重 → 由 `dependency.go:58-61` 编译期 map 去重兜住，
  只多占 hash 载荷，不改语义。
- Task 15 两条（`UnregisterConsumer` 不翻转 accepted、`Registry.Ready` 与
  `IsReady` 语义分叉）→ 均偏保守方向。其中 `Registry.Ready` 当前**无生产调用
  点**，但若将来心跳上报改用它，会与门控口径不一致——那时才是必须修的时刻。

完整清单见 ledger（已随 worktree 清理删除，可从 git 历史或本文件的提交记录回溯）。
其余多为测试覆盖窄于其欲保护的不变量（如断言只查两个字段而非整个结构体、hash
稳定性测试只查前缀不钉死字面值），修的价值在于**将来加字段时能被检出**，而非
当下有错。

## 待验证（非缺陷）

合并时发现 `TestDrainPoolNotifiesObserverPoolSwapped` 与引擎自身的 drainer 抢同
一个 channel 而死锁（已修，`79f519a`）。该缺陷**单跑必过、八包并行必挂**，暴露出
一个流程问题：分支上那次「全绿」里 wasm 包是 `(cached)`，根本没真跑。

- 尚未跑过 `-race -count=3` 的加压验证（合并时起了但主动中止）。
- 值得排查是否还有同类「只在并行下暴露」的测试。判据不是「跑过绿」，而是
  「在缓存未命中的情况下跑过绿」。
