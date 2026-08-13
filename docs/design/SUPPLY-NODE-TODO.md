# SUPPLY-NODE 遗留项

P3（`feat/supply-node-p3`，20 tasks，已合入 main）交付后未做的事。按「不做会怎样」排序，不按工作量。

背景与设计取舍见 [SUPPLY-NODE.md](./SUPPLY-NODE.md)；本文件只列**待办**。§9 是「知道且接受的代价」，这里是「打算改的东西」——
一条缺口同时出现在两处不矛盾：§9 说明现状为何可接受，这里说明何时不再可接受。

## P0 — 合并前已知、但影响面最大

### ~~1. 门控拒绝的 activation 不自愈~~ ✓ 已关闭

分支 `fix/activation-ack-retry` 实现了 ActivationAck → Fence → 退避 → 重派
闭环。详见 [SUPPLY-NODE.md §9(a)](./SUPPLY-NODE.md#9-known-gaps-and-costs)。

测试变更：`TestSupplyGateLosesNoMessages` 与 `TestSupplyGateRecoversOnRestart`
的 Generation 断言已按预期更新；新增 `TestSupplyGateRetriesWithoutRestart` 证明
完整自愈闭环。

新引入的已知代价（已写入 §9(a)）：gRPC-only 部署下 ack 无处可发，自愈能力完全
缺失——此缺口与 P1 §3（gRPC 传输不携带 hint/activation）同源但严重性更高。

### ~~4. supply 加密是死代码~~ ✓ 已关闭

KEK/DEK/传输 key 三层已实现并接线。`EnableSupplyEncryption` 现由
`apiserver.Config` 传递；传输 key 经 Redis `SET NX` 在副本间共享；轮换的调度、
跨副本传播与投递已补齐。设计见
[SUPPLY-NODE.md §10](./SUPPLY-NODE.md#10-supply-内容加密) 与
[§10.1 传输 key 轮换](./SUPPLY-NODE.md#101-传输-key-轮换)。

**接线曾断在最后一环**（已修，`fix(apiserver): wire cp SupplyEncryptor into
supply module`）：`cmd/server` 设的是 `EnableSupplyEncryption`，它只流向
`control.Config`；而 `module_supply.go` 的 GET 分支检查的是
`apiserver.Config.SupplyEncryptor`，从没人设过。control plane 建好的
encryptor 只装进了自己的 core（runner 注册/心跳通道），没到 HTTP 端点。
症状是 production 对 `Accept: application/x-xflow-encrypted` **静默返回明文**，
无错误、无告警。

七个 task 的单元测试全绿却漏掉了它：当时的 e2e 探针在 helper 里直接
`m.encryptor = fixedEncryptor{...}` 给字段赋值，绕过了它本该证明的装配路径。
真起 server 进程的那条验证（两副本 + 真 Redis）才抓到。现在
`supply_encryption_wiring_test.go` 走 `apiserver.New` + `WithControlPlane`
真实装配，不碰任何依赖字段。

**轮换的三个缺口已一并关闭**（原表述「仍未做」已过期）：`Rotate()` 现由
`ControlPlane.Start` 起的轮换协程按 `SET NX` 租约周期性调用（`--supply-key-rotation`
配置，负值关闭）；`Rotate` 先写回 Redis 再换本地 key，别的副本经 `Refresh`
采纳，重启不丢；`ConsumeRotation` 已删除，改为 runner 心跳上报持有的 key ID、
server 的 `RotationForHolder` 只在 ID 不同时下发——「每个 runner 各自收到一次」
由此在结构上成立，不需要 per-runner 跟踪集合。宕机/重启/后加入的 runner 在下次
心跳自愈。

**仍未做**：无 KMS 集成，KEK 由部署方注入。

## P1 — 可观测性缺失，出事时会瞎

### ~~2. runner 指标只能自曝，跨网络域采集不到~~ ✓ 已关闭

上报/代理通道已实现：runner `--report-metrics` 把整个 registry 上报到
`POST /v1/runners/metrics`，server 经 `prometheus.Gatherers` 并入自己的 `/metrics`。
设计见 [runner 指标代理通道 spec](../superpowers/specs/2026-08-09-runner-metrics-proxy-design.md)，
拓扑见 [DEPLOYMENT-TOPOLOGIES.md §4.7](./DEPLOYMENT-TOPOLOGIES.md#47-跨网络域的指标采集runner-上报--server-代理)。

本条原表述「`cmd/runner/run.go` 里没有 `*metrics.Metrics` 实例」在 2026-08-09 已更正
为过期——实例早已存在（`9cc879a`），真缺口是「自曝、等人来抓」。实现时顺带解开了
`metrics.New()` 与 `--metrics-addr` 的绑定：registry 与 observer 装配现在无条件执行，
两个 flag 各控一条出口，跨域 runner 因此可以在不开监听端口的前提下上报。

**仍未做**：`xflow_runner_up` 之外没有跨 runner 的聚合视图（各 runner 的 histogram
bucket 由同一份代码决定故可聚合，但若某 runner 装了 bucket 不同的第三方 collector，
`histogram_quantile` 跨 runner 求和会失真，本设计不做检测）；gRPC 传输仍无此能力
（见 [DEPLOYMENT-TOPOLOGIES.md §4.6](./DEPLOYMENT-TOPOLOGIES.md)，既有取舍）。

### 3. gRPC 传输不携带 hint 与 activation

见 [SUPPLY-NODE.md §9(b)](./SUPPLY-NODE.md#9-known-gaps-and-costs) 与
[DEPLOYMENT-TOPOLOGIES.md §4.6](./DEPLOYMENT-TOPOLOGIES.md#46-传输差异gRPC-心跳不携带控制载荷)。

`runnerpb.HeartbeatResponse` 只有 `server_time` 一个字段。**这个缺口先于本分支
存在**（gRPC 连 activation directive 都不带），不是 supply 引入的。

对 supply 而言只是延迟退化，不是正确性问题：丢 hint 只会让内容刷新延迟从「一个
心跳周期」退化为「一个 TTL 轮询周期」，永远不会退化为「永不刷新」。TTL 轮询才是
正确性保证，hint 只是优化。

### ~~4. wasm supply 热更新无生产接线，规则永不到达 guest~~ ✓ 已关闭

分支 `fix/wasm-supply-wiring` 补上了消费者注册这一环。原缺口是两条路径根本不
相交：依赖边只走到 `control.SuppliesForEntryUnit` → runner 的
`SupplyRequirement`，驱动**准入门控**与 `$supplies` 注入，从不触达
`supply.Registry.RegisterConsumer`；于是 `configFromSource` 恒为 false，回落
到 legacy `globals["$config"]` 路径，而 `$config` 在生产中被 workflow 级
Config 占据，reactor 永远拿不到规则——且空规则集不报错，流量原样放行。

「谁消费谁」这个配对必须在**服务端**推导：指令只带扁平的 supply 名字列表，
group 包投影更是把每个成员的 `supplyRefs` 压平成一个去重名字集合，内容抵达
runner 时配对早已丢失。`DeriveEntryActivations` 现在沿同一批依赖边多保留消费
者身份，产出 `SupplyConsumerBinding{ModuleDigest, SupplyNode}` 随激活指令下发，
由 runner 注册。只传 digest 不传 inline code（多 MB base64 上激活链路是已知禁
忌；服务端另算 digest 会造出与 host 侧 `moduleKeyOf` 可能漂移的第二身份来源），
内联代码节点不产生 binding 并记 Warn。

**注册前必须先编译**：runner 上引擎只在首次 Execute 时建。若对着不存在的引擎
注册，通知回调返回 nil → registry 记为「内容已接受」→ 补投条件永不成立；而注
册本身已把模块标记为 source-driven，无池即**拒绝所有消息**——比原缺陷更糟（从
静默放行变成永久卡死）。激活期取件并编译关掉了这个空窗，顺带把 ~7MB 模块的编译
挪出首条消息的 deadline。

Generation 升级用差集而非「清空再注册」：注册键由 (digest, supply node) 决定，
重发相同 binding 时后者会删掉刚建立的注册，症状是热更新静默失效。

全链路 fail closed：无 resolver、取件失败、digest 畸形都让激活失败，而不是让
一个拿不到规则的模块上线。错误只带 digest 与 supply 名，不带 Params/token
（组织策略 §7）。

`test/integration/sas_tagging_e2e_test.go` 的手工注册**保留**：该 workflow 不设
`RunnerSelector`，走 in-process 内联路径，根本不经过激活链路。注释已改为说明它
是内联模式的等价物，并指向 `TestSupplyConsumerBindingReachesRunner`（分布式模式
的端到端覆盖）。

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

## ~~待验证~~ ✓ 已完成（非缺陷排查）

合并时发现 `TestDrainPoolNotifiesObserverPoolSwapped` 与引擎自身的 drainer 抢同
一个 channel 而死锁（已修，`79f519a`）。该缺陷**单跑必过、八包并行必挂**，暴露出
一个流程问题：分支上那次「全绿」里 wasm 包是 `(cached)`，根本没真跑。

加压验证已完成，**确实还有同类缺陷**，且触发条件比预期宽——不需要 `-race`、也不
需要多包并行，**整包 `-count=2` 即稳定复现**。日常 `-count=1` 永远看不到。

根因同一族：**进程级全局状态在测试间泄漏**。全仓库逐包 `-count=2` 扫描确认只有两处，
均为单点遗漏（非系统性）：

1. `wasm/supply_consumer_test.go:185` 用了 `b64(reactorWasm)` 而非本包已有的
   `testReactorCode(t)`（同文件其余 6 处都用对了）。`RegisterSupplyConsumer` 会把该
   code 的引擎**永久**翻成 source-driven（`configFromSource` 单向不可逆，这是正确的
   产品设计），于是共享 fixture 的其他测试第二轮走上 source-driven 分支、`$config`
   被 `stripConfig` 丢弃、拿到上一轮遗留的 `r2` 池 —— 表现为
   `TestReactor_EmptyConfigValid` 报 `empty config should match nothing, got map[r2:true]`。
2. `script/wasm_supply_seam_test.go` 两个测试对**同名** supply `rules` Apply 不同
   revision（11 与 3）。`supply.Default` 是进程级单例，而 `UnregisterConsumer` 只摘
   consumer、**不清除已 Apply 的 Snapshot**，于是 revision 3 残留 —— 表现为
   `config_generation = 0x3, want 11`。已改为 helper 内按 `t.Name()` 派生唯一 supply 名，
   新增测试自动获得隔离。

**修复仅动测试，产品代码零改动**：`configFromSource` 单向不可逆与 `stripConfig` 都是
有意的生产行为，为测试便利加「翻回 legacy」的路径会在产品里开危险的口子。

两个受影响的守护测试已用删除注入复验**确实承重**（改坏 `stripConfig` / 让 `swapConfig`
用固定规则集建池，二者立即失败）——此前它们可能靠读到别的测试残留而通过。

`-race -count=3` 全仓库另有两条失败，判定为 **race 假阳性，勿改**：
`TestDiskCache_SurvivesRuntimeRecreation`（<500ms 门槛）与 `TestP2_ColdStartBudget`
（<100ms 预算）无 race 单跑仅 1.74s/2.08s，门槛按无 race 性能定，`-race` 给 wazero
编译的开销让其必然超标。改门槛就是让判据迁就工具开销。

判据仍是：不是「跑过绿」，而是「在缓存未命中的情况下跑过绿」——现在还要加一条
**`-count=2` 下跑过绿**。
