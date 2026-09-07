# SUPPLY-NODE 遗留项

P3（`feat/supply-node-p3`，20 tasks，已合入 main）交付后未做的事。按「不做会怎样」排序，不按工作量。

背景与设计取舍见 [SUPPLY-NODE.md](./SUPPLY-NODE.md)；本文件只列**待办**。§9 是「知道且接受的代价」，这里是「打算改的东西」——
一条缺口同时出现在两处不矛盾：§9 说明现状为何可接受，这里说明何时不再可接受。

## 待办

**空。** P0/P1/P2 的全部具名条目均已关闭（逐条见下方「已关闭」节）。

P2 里有 **10 条永久丢失**的 deferred minor：清单只存在于已删除的 `feat/supply-node-p3`
worktree 中，从未进 git，穷尽检索后确认无法恢复。这 10 条按 TODO 自身描述属同一类型——
**测试断言覆盖的字段少于其欲保护的不变量**（如只查结构体两个字段而非全部），价值在「将来
加字段时能被检出」而非当下有错。后续在 supply 相关测试里遇到窄断言，就地补齐即可，不必
再尝试恢复清单。具体文件与函数无从确定，不单独列项。

## 已知且接受的代价（不打算改）

- **跨 runner 聚合视图**：`xflow_runner_up` 之外没有跨 runner 的聚合视图。若某 runner
  装了 bucket 不同的第三方 collector，`histogram_quantile` 跨 runner 求和会失真；本设计
  不做检测，也不计划改。
- **gRPC runner 无法上报指标**：上报只走 HTTP `POST /v1/runners/metrics`，gRPC 传输没有
  对应 RPC，gRPC-only runner 无法上报指标。gRPC 不是目标形态（跨网络域走 Relay Gateway），
  此缺口不单独立项。

## P2 — 已定位 7 条（全部已修）

最终评审逐条判定**无一需在合并前修**：

| # | 内容 | 处置 |
|---|---|---|
| 1 | Task 6 `DependsOn` 不去重 | 编译期 map 兜住（`dependency.go:58-61`），不改语义 |
| 2 | Task 15 `UnregisterConsumer` 不翻转 accepted | `c00c35a` 把 readiness 改成派生合取而消除 |
| 3 | Task 15 `Registry.Ready` 与 `IsReady` 分叉 | `80856d8` 把 `Ready` 重写为基于 `IsReady` |
| 4 | Task 17 doom 的 timeout 分类无承重测试 | `b2606a4` 已修 |
| 5 | Task 17 `OnPoolSwap` 注释列了不存在的 `source_error` | `b2606a4` 已修 |
| 6 | Task 17 `node/wasm_observer_test.go` gofmt | `b2606a4` 已修 |
| 7 | hash 稳定性测试只查前缀不钉死字面值 | 已修：`engine/graph/snapshot_supply_test.go:73`（`graphHashPayload`）与 `sdk/xflow/supply_identity_test.go:30`（`runtimeHashPayload`）均改为钉死字面值 |

## 已关闭（保留索引）

| 条目 | 关闭位置 | 说明 |
|---|---|---|
| P0-1：门控拒绝的 activation 不自愈 | `test/integration/supply_gating_test.go:313,546,711` | `fix/activation-ack-retry`：ActivationAck→Fence→退避→重派闭环 |
| P0-4：supply 加密是死代码 | `service/apiserver/supply_encryption_wiring_test.go:22` | KEK/DEK/传输 key 三层接线；`fix(apiserver): wire cp SupplyEncryptor into supply module` |
| P1-2：runner 指标只能自曝 | `service/runner/metrics_reporter.go`，`service/control/server.go`，`test/integration/runner_metrics_proxy_e2e_test.go` | `--report-metrics` → `POST /v1/runners/metrics` → 并入 `/metrics`；已实现 |
| P1-3：gRPC 传输不携带 hint 与 activation | `service/protocol/runnerpb/runner.proto:78-86`，`service/control/grpc_server.go:110` | `HeartbeatResponse` 补全四个字段；gRPC `ActivationAck` RPC 缺失归入「已知代价」 |
| P1-4：wasm supply 热更新无生产接线 | `service/control/supply_consumer_binding_test.go`，`test/integration/wasm_supply_binding_e2e_test.go:159` | `fix/wasm-supply-wiring`：`DeriveEntryActivations` 产出 `SupplyConsumerBinding` 随激活下发 |
| 待验证（全包 `-count=2` 竞态） | `wasm/supply_consumer_test.go:185`，`script/wasm_supply_seam_test.go`，`wasm/latency_budget_test.go` | 两处全局状态泄漏已修；预算测试改为检测污染并拒判 |
