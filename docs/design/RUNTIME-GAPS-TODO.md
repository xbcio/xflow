# 运行时缺口 遗留项

2026-10-03 一轮缺口收口（KEK 轮换、server/runner 凭据热加载、ADR-D4 编辑器元数据、
Node Group 组级 `on_error`、gRPC `AckActivation`、G1 clean-SHA 重签 `c39a271`）之后
仍开放的条目。按「不做会怎样」排序，不按工作量。

每条都写明出处：缺口本身的现状说明留在所属设计文档里，这里只记「打算改的东西」。
关闭一条时，把它移到文末「已关闭」并写上 commit。

范围：只收运行时缺口。Relay Gateway / remote SDK（D1 支持矩阵已排除）与已被显式
否决的设计（如 supply 的 `default: <bytes>`）不在此列；出处文档与实现之间的纯陈旧
偏差也不单独立项，在关闭对应条目时顺带标注。

## 待办

### P1 — 会造成重复执行、静默失效或发布门不稳

1. **gRPC 传输不续租约。** `GRPCClient` 不实现 `leaseRenewClient`
   （`service/runner/doc.go` 的 optional capabilities 清单），gRPC runner 从不调用
   `RenewLease`，崩溃后只能等 server 侧 sweeper 回收。执行超过 lease TTL 的任务会被
   回收并重新派发——代码路径成立（sweeper 经 `ReclaimLease` 撤销并重新入队；gRPC 侧
   只有结果提交处的 token fencing，防不住第二次执行），但缺端到端补测。修法是给
   `runner.proto` 加 `RenewLease` RPC，服务端复用 HTTP 路径的同一个 core 函数
   （`Core.renewLease`；`AckActivation` 已是这个形状），客户端补
   `GRPCClient.RenewLease` 后续租环即经类型断言自动启用。

2. **`make web-ci` 的 admin e2e 偶发超时。** `web/e2e/admin-smoke.ts` 等待
   `style-probe-heading` 的 `toBeVisible`（超时取 Playwright 配置的 20s）在主机负载高时
   失败；单独重跑 3/3 通过，整门重试通过（`c39a271` 重签的确认运行，记录见
   RELEASE-GATES）。原因推测是 dev server 就绪时序，未查实。

3. **`TestSubgraphBinaryE2E/KillRunnerMidExpansionThenRestart` 偶发不收敛。**
   `test/integration/subgraph_binary_e2e_test.go`：map 展开中途 SIGKILL runner 后，
   执行在 240s 内停在 `running`、map 节点 `pending`（约 2/15，为运行观察的估计，仓库内
   无归档出处；均在高负载下）。测试自己的失败信息指向「死掉的 generation 的批次挡住了
   新 generation」，可能是真实的恢复缺陷而不只是时序问题，需要先定性。

4. **`pin_data` / `settings.pin_data_mode` 没有运行时消费者。** 类型上存在
   （`types.WorkflowDef.PinData`、`types.WorkflowSettings.PinDataMode`），也参与工作流
   指纹计算，但引擎、runner、SDK 没有代码读它：被钉住的节点照样入队、照样发起真实
   调用（[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) 已以「⚠️ 未实现」标记该节）。
   要么实现，要么从 DSL 面移除或显式拒绝，避免静默失效。

### P2 — 功能不完整，现有部署可绕开

5. **gRPC 传输不代理指标。** `GRPCClient` 不实现 `MetricsReportClient`
   （同上 `service/runner/doc.go`），跨域无法直接抓取 runner 时只能走 HTTP。
   注：[SUPPLY-NODE-TODO.md](./SUPPLY-NODE-TODO.md) 把它记为「已知且接受的代价，
   不单独立项」——两处裁定需对齐，否则本条应改为已知限制或移除。

6. **runner 凭据热加载没有覆盖全部出站客户端。** `SIGHUP` / `Runner.Reload` 只换
   Runner Protocol HTTP 客户端的 token、客户端证书与 CA
   （`sdk/xflow/runner_credential_reload.go`；gRPC 传输只换 token）；artifact 拉取、
   entry-seed 与 supply 拉取用的是静态 HTTP 客户端（entry-seed 与 supply 共用一个
   client），token 与 CA 都不重载。gRPC 的 server CA 池在构造时被快照，`Reload` 后
   **重连也拿不到新 CA**，需重启进程或重建客户端——代码注释与
   [credential-key-rotation-runbook.md](../references/credential-key-rotation-runbook.md)
   §4.3 仍写「下次重连生效」，是错误表述，需一并更正；runbook 未覆盖上述静态客户端。

7. **runner 的 Runner Protocol HTTP 传输不走环境代理。** 可重载 transport 有意置空
   `Proxy`（经代理的 HTTPS 隧道会绕过可重载 CA 池，`0551269`）。必须经 HTTP 代理
   访问控制面的部署目前无法使用；需要的话改为自建 CONNECT 隧道并在隧道上用实时 CA
   握手。附注：出站代理行为目前不一致——gRPC 传输默认读环境代理，静态客户端装 TLS 时
   不走代理。

8. **entry-seed 托管只支持 Kafka。** 其他 trigger 类型接到 entry-seed 路径上会
   fail-closed（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)「Non-Kafka
   trigger types are fail-closed」一条）。

9. **MAP 的 runner 级资源治理只剩配置面与证据缺口。** `body_concurrency` 只限制单批
   body worker，不是 runner 全局预算（[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md)
   `xflow.map` 当前实现状态）。limiter/queue/wiring 与 cancel/deadline 判定已实现接线
   （`execution/subgraph/map_concurrency_limiter.go`，`5520a01`；`sdk/xflow/runner.go`
   接线 `8e1aa3e`；cancel/deadline `fbc71f8`/`7c1f2cf`，均在 clean 候选 `c39a271` 内），
   但：独立 runner CLI/YAML 未暴露 `MapBatchConcurrency`/`MapItemConcurrency`（全仓
   非测试引用只在 `sdk/xflow/runner.go`），只吃默认值；发布证据按 RELEASE-GATES
   「SUBGRAPH / Node Group 口径」仍记为 dirty-tree validation，不能当作 clean-SHA
   证据。Node Group 在此收口前仍为 Experimental。

10. **pull 模式的 `xflow.supply.http` 未实现。** runner 主动按计划拉取 supply 的路径
    不存在，只能用 `xflow.supply.external` / `static`
    （[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) Supply 一节已标记「尚未实现，
    不要在 DSL 里使用」）。

11. **子工作流复用（`xflow.subworkflow`）未实现。**
    [DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) §6.4 标为「计划扩展，当前版本未实现」。

12. **没有 Redis 与 sqlstore 的执行状态比对工具。** 目前只能靠审计失败计数与
    observer 发现投影偏差（[STORAGE-CONTRACT.md](./STORAGE-CONTRACT.md) 的 planned 项）。

### P3 — 已知限制，记录在案

13. **组级 `error_output` 的错误载荷没有结构化的失败成员名。** 现有 wire 协议里没有
    承载它的字段（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) §12.2）。

14. **嵌入式 `Server.ReplaceWorkflow` 会清空编辑器元数据。** SDK 注册路径不带
    `editor_metadata`，替换后之前经 HTTP 保存的布局丢失；行为已由
    `TestEmbeddedReplaceWorkflowClearsEditorMetadata` 固定。若嵌入式宿主也要保留布局，
    需要给嵌入式 API 加元数据参数或改为「未提供则保留」。

15. **ADR-D4 刻意延后的两项：** 草稿与发布分离（`WorkflowDraft`）、定义版本历史
    （`WorkflowDefinitionVersion`）。见
    [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

### 需要真实环境或人工批准，不能靠改代码关闭

16. **G2：多副本 control-plane HA soak 与真实多 namespace 隔离验收。** 要求见
    [RELEASE-GATES.md](./RELEASE-GATES.md) 的 G2 定义；`make test-soak` 不是该证据
    （见 README 的说明）。

17. **RELEASE-GATES §6 的 D1–D8** 全部为「OPEN — 未批准」，需要对应负责人裁定。

18. **KEK 轮换（`xflow supply reseal`）与 server/runner 凭据热加载都没有在真实环境
    演练过**，runbook 的 owner 与期限属于 D6。

## 已关闭

- **entry unit 的显式 activation 副本扩展**（原 P2-8）—— `5520a01`（2026-09-01）：
  `ActivationReplicas` 类型/SDK、`FeatureEntryActivationReplicaV1` 能力位、reconciler
  多副本与端到端测试；该 commit 在 clean 候选 `c39a271` 之内。待同步出处：
  [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) §12.2 仍写「未实现」。
