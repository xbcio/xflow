# 运行时缺口 遗留项

2026-10-03 一轮缺口收口（KEK 轮换、server/runner 凭据热加载、ADR-D4 编辑器元数据、
Node Group 组级 `on_error`、gRPC `AckActivation`、G1 clean-SHA 重签 `c39a271`）之后
仍开放的条目。按「不做会怎样」排序，不按工作量。

每条都写明出处：缺口本身的现状说明留在所属设计文档里，这里只记「打算改的东西」。
关闭一条时，把它移到文末「已关闭」并写上 commit。

## 待办

### P1 — 会造成重复执行、静默失效或发布门不稳

1. **gRPC 传输不续租约。** `GRPCClient` 不实现 `leaseRenewClient`
   （`service/runner/doc.go` 的 optional capabilities 一节），gRPC runner 从不调用
   `RenewLease`，崩溃后只能等 server 侧 sweeper 回收。待核实并补测：执行时间超过
   lease TTL 的任务在 gRPC 下是否会被回收后重复派发。修法是给 `runner.proto` 加
   `RenewLease` RPC，服务端复用 HTTP 路径的同一个 core 函数（`AckActivation` 已是
   这个形状）。

2. **`make web-ci` 的 admin e2e 偶发超时。** `web/e2e/admin-smoke.ts` 等待
   `style-probe-heading` 的 `toBeVisible`（20s）在主机负载高时失败；单独重跑 3/3 通过，
   整门重试通过（`c39a271` 重签的确认运行，记录见 RELEASE-GATES）。原因推测是 dev
   server 就绪时序，未查实。

3. **`TestSubgraphBinaryE2E/KillRunnerMidExpansionThenRestart` 偶发不收敛。**
   `test/integration/subgraph_binary_e2e_test.go`：map 展开中途 SIGKILL runner 后，
   执行在 240s 内停在 `running`、map 节点 `pending`（约 2/15，均在高负载下）。测试
   自己的失败信息指向「死掉的 generation 的批次挡住了新 generation」，可能是真实的
   恢复缺陷而不只是时序问题，需要先定性。

### P2 — 功能不完整，现有部署可绕开

4. **gRPC 传输不代理指标。** `GRPCClient` 不实现 `MetricsReportClient`
   （同上 `service/runner/doc.go`），跨域无法直接抓取 runner 时只能走 HTTP。

5. **runner 凭据热加载没有覆盖全部出站客户端。** `SIGHUP` / `Runner.Reload` 只换
   Runner Protocol 客户端的 token、客户端证书与 CA（`sdk/xflow/runner_credential_reload.go`）；
   artifact 拉取、entry-seed、supply 拉取用的独立 HTTP 客户端不重载。gRPC 的新 CA
   要到 grpc-go 下次重连才生效。见
   [credential-key-rotation-runbook.md](../references/credential-key-rotation-runbook.md)。

6. **runner 的 Runner Protocol HTTP 传输不走环境代理。** 可重载 transport 有意置空
   `Proxy`（经代理的 HTTPS 隧道会绕过可重载 CA 池，`0551269`）。必须经 HTTP 代理
   访问控制面的部署目前无法使用；需要的话改为自建 CONNECT 隧道并在隧道上用实时 CA
   握手。

7. **entry-seed 托管只支持 Kafka。** 其他 trigger 类型接到 entry-seed 路径上会
   fail-closed（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)「Non-Kafka
   trigger types are fail-closed」一条）。

8. **每个 entry unit 只有一个 activation 副本。** spec §11.6 的显式副本扩展未做
   （[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)「Activation replica count
   > 1」一条）。

9. **MAP 的 runner 级资源治理。** `body_concurrency` 只限制单批 body worker，不是
   runner 全局预算；limiter/queue/wiring 与 cancel/deadline 判定仍在进行中，目前只有
   dirty-tree 验证（[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) `xflow.map` 当前
   实现状态、RELEASE-GATES「SUBGRAPH / Node Group 口径」）。Node Group 在此收口前仍
   为 Experimental。

10. **没有 Redis 与 sqlstore 的执行状态比对工具。** 目前只能靠审计失败计数与
    observer 发现投影偏差（[STORAGE-CONTRACT.md](./STORAGE-CONTRACT.md) 的 planned 项）。

### P3 — 已知限制，记录在案

11. **组级 `error_output` 的错误载荷没有结构化的失败成员名。** 现有 wire 协议里没有
    承载它的字段（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) §12.2）。

12. **嵌入式 `Server.ReplaceWorkflow` 会清空编辑器元数据。** SDK 注册路径不带
    `editor_metadata`，替换后之前经 HTTP 保存的布局丢失；行为已由
    `TestEmbeddedReplaceWorkflowClearsEditorMetadata` 固定。若嵌入式宿主也要保留布局，
    需要给嵌入式 API 加元数据参数或改为「未提供则保留」。

13. **ADR-D4 刻意延后的两项：** 草稿与发布分离（`WorkflowDraft`）、定义版本历史
    （`WorkflowDefinitionVersion`）。见
    [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

### 需要真实环境或人工批准，不能靠改代码关闭

14. **G2：多副本 control-plane HA soak 与真实多 namespace 隔离验收。** 要求见
    [RELEASE-GATES.md](./RELEASE-GATES.md) 的 G2 定义；`make test-soak` 不是该证据。

15. **RELEASE-GATES §6 的 D1–D8** 全部为「OPEN — 未批准」，需要对应负责人裁定。

16. **KEK 轮换（`xflow supply reseal`）与 server/runner 凭据热加载都没有在真实环境
    演练过**，runbook 的 owner 与期限属于 D6。

## 已关闭

（暂无）
